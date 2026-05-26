from __future__ import annotations

import argparse
import asyncio
from collections import deque
from dataclasses import dataclass, field
import logging
from pathlib import Path
import socket
import struct
import time
from typing import Deque, Sequence

from .checksum import PacketError
from .protocol import (
    CLS_ASYNC,
    CLS_CONTROL,
    CLS_SYNC,
    CMD_ENTER,
    CMD_GAMEDATA,
    CMD_GAMETYPE,
    CMD_GAMESTATE,
    CMD_JOINFAIL,
    CMD_PING,
    CMD_PLAYER,
    CMD_PONG,
    CMD_QUIT,
    CMD_REQUESTJOIN,
    CMD_REQUESTJOIN2,
    CMD_REQUESTJOINOK,
    CMD_STATSCODE,
    DEFAULT_HOST_NAME,
    DEFAULT_MAP_FILE_NAME,
    DEFAULT_ROOM_NAME,
    PRODUCT_SEXP,
    SCGP_CHANGERACE,
    SCGP_JOINEDGAME,
    SCGP_LOBBYCHAT,
    SCGP_MAP,
    SCGP_MAPPERCENT,
    SCGP_NEWNETPLAYER,
    SCGP_NOP,
    SCGP_PLAYERJOIN,
    SCGP_RIGHT_CLICK,
    SCGP_ROOMDATA,
    SCGP_STARTGAME,
    SCGP_SELECT,
    SCGP_SEED,
    SCGP_SLOTUPDATE,
    SCGP_SYNC,
    SCGP_TRAIN,
    SCGP_UNKNOWNREQUEST,
    STATUS_NORMAL,
    STATUS_RESEND_REQUEST,
    STATUS_VERIFY,
    VERSION_CODE,
    LanPacket,
    RoomAdvertisement,
    StormPacket,
    first_scgp,
    gamedata_payload,
    game_state_payload,
    gametype_payload,
    lobbychat_payload,
    make_stat_string,
    map_info_payload,
    nop_payload,
    parse_enter_payload,
    player_record_payload,
    playerjoin_payload,
    quit_payload,
    roomdata_payload,
    seed_payload,
    slot_sync_payload,
    startgame_payload,
)

Address = tuple[str, int]

LOG = logging.getLogger("sc_host")
VIRTUAL_HOST_ID = 0
DEFAULT_GUEST_PLAYER_NAME = "SunX"
GAME_TRACE_SCGP_NAMES = {
    SCGP_NOP: "NOP",
    SCGP_SELECT: "SELECT",
    SCGP_RIGHT_CLICK: "RIGHT_CLICK",
    SCGP_TRAIN: "TRAIN",
    SCGP_SYNC: "SYNC",
    SCGP_STARTGAME: "STARTGAME",
    SCGP_MAPPERCENT: "MAPPERCENT",
    SCGP_SLOTUPDATE: "SLOTUPDATE",
    SCGP_NEWNETPLAYER: "NEWNETPLAYER",
    SCGP_JOINEDGAME: "JOINEDGAME",
    SCGP_CHANGERACE: "CHANGERACE",
    SCGP_SEED: "SEED",
    SCGP_PLAYERJOIN: "PLAYERJOIN",
    SCGP_ROOMDATA: "ROOMDATA",
    SCGP_LOBBYCHAT: "LOBBYCHAT",
    SCGP_MAP: "MAP",
    SCGP_UNKNOWNREQUEST: "UNKNOWNREQUEST",
}


@dataclass
class ReliableState:
    address: Address
    player_id: int
    transport: asyncio.DatagramTransport
    next_send: dict[int, int] = field(default_factory=lambda: {CLS_CONTROL: 1, CLS_ASYNC: 0, CLS_SYNC: 100})
    last_recv: dict[int, int] = field(default_factory=lambda: {CLS_SYNC: 102})
    history: dict[int, dict[int, bytes]] = field(default_factory=lambda: {CLS_CONTROL: {}, CLS_ASYNC: {}, CLS_SYNC: {}})
    history_order: dict[int, Deque[int]] = field(
        default_factory=lambda: {CLS_CONTROL: deque(), CLS_ASYNC: deque(), CLS_SYNC: deque()}
    )

    def note_recv(self, packet: StormPacket) -> None:
        if packet.status == STATUS_NORMAL:
            # Storm's second sequence field is the next sequence number expected
            # from the peer. Captures show host replies to peer seq=0 with
            # seq_recv=1, and to peer seq=2 with seq_recv=3.
            self.last_recv[packet.cls] = (packet.seq_send + 1) & 0xFFFF

    def send(
        self,
        cls: int,
        *,
        command: int = 0,
        payload: bytes = b"",
        player_id: int = 0,
        status: int = STATUS_NORMAL,
    ) -> bytes:
        seq_send = self.next_send.setdefault(cls, 0)
        seq_recv = self.last_recv.get(cls, 0)
        packet = StormPacket(seq_send, seq_recv, cls, command, player_id, status, payload)
        wire = packet.to_wire()
        self.transport.sendto(wire, self.address)
        self._store(cls, seq_send, wire)
        if status == STATUS_NORMAL:
            self.next_send[cls] = (seq_send + 1) & 0xFFFF
        return wire

    def resend(self, cls: int, seq: int) -> bool:
        wire = self.history.get(cls, {}).get(seq)
        if wire is None:
            return False
        self.transport.sendto(wire, self.address)
        return True

    def send_verify(self, cls: int, *, player_id: int = 0) -> bytes:
        seq = self.last_recv.get(cls, 0)
        packet = StormPacket(seq, seq, cls, 0, player_id, STATUS_VERIFY)
        wire = packet.to_wire()
        self.transport.sendto(wire, self.address)
        return wire

    def _store(self, cls: int, seq: int, wire: bytes) -> None:
        history = self.history.setdefault(cls, {})
        order = self.history_order.setdefault(cls, deque())
        history[seq] = wire
        order.append(seq)
        while len(order) > 256:
            old = order.popleft()
            history.pop(old, None)


@dataclass
class PlayerSession:
    reliable: ReliableState
    name: str = "Player"
    slot_index: int | None = None
    joined: bool = False
    lobby_snapshot_sent: bool = False
    lobby_ready: bool = False
    map_bootstrap_sent: bool = False
    map_percent: int = 0
    last_lobby_activity: float = 0.0
    last_map_request_log: float = 0.0
    last_map_info_resend: float = 0.0
    race: int = 6
    team: int = 1

    @property
    def address(self) -> Address:
        return self.reliable.address

    @property
    def player_id(self) -> int:
        return self.reliable.player_id


class DiscoveryProtocol(asyncio.DatagramProtocol):
    def __init__(self, server: "StarCraftHostServer") -> None:
        self.server = server
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        self.server.discovery_transport = self.transport

    def datagram_received(self, data: bytes, addr: Address) -> None:
        self.server.handle_discovery(data, addr)


class StormProtocol(asyncio.DatagramProtocol):
    def __init__(self, server: "StarCraftHostServer") -> None:
        self.server = server
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        self.server.storm_transport = self.transport

    def datagram_received(self, data: bytes, addr: Address) -> None:
        self.server.handle_storm(data, addr)


class StarCraftHostServer:
    def __init__(
        self,
        *,
        bind: str = "0.0.0.0",
        discovery_port: int = 6111,
        storm_port: int = 6112,
        host_name: str = DEFAULT_HOST_NAME,
        host_player_name: str | None = None,
        guest_player_name: str = DEFAULT_GUEST_PLAYER_NAME,
        room_name: str = DEFAULT_ROOM_NAME,
        map_name: str = DEFAULT_MAP_FILE_NAME,
        auto_start_delay: float = 3.0,
        game_state_delay: float = 0.35,
        seed_delay: float = 5.75,
        advertise_interval: float = 2.0,
        start_stability_delay: float = 1.0,
        broadcast_addresses: Sequence[str] | None = None,
        trace_game: bool = False,
        trace_nop: bool = False,
    ) -> None:
        self.bind = bind
        self.discovery_port = discovery_port
        self.storm_port = storm_port
        self.host_player_name = host_player_name or host_name
        self.guest_player_name = guest_player_name
        self.player_slot_names = (self.host_player_name, self.guest_player_name)
        self.host_name = self.host_player_name
        self.room_name = room_name
        self.map_name = map_name
        self.auto_start_delay = auto_start_delay
        self.game_state_delay = game_state_delay
        self.seed_delay = seed_delay
        self.advertise_interval = advertise_interval
        self.start_stability_delay = start_stability_delay
        self.trace_game = trace_game
        self.trace_nop = trace_nop
        self.broadcast_addresses = list(
            dict.fromkeys([*self._default_broadcast_addresses(bind), *(broadcast_addresses or [])])
        )
        self.stat_string = make_stat_string(self.host_name, room_name)
        self.discovery_transport: asyncio.DatagramTransport | None = None
        self.storm_transport: asyncio.DatagramTransport | None = None
        self.sessions: dict[Address, PlayerSession] = {}
        self.starting = False
        self.started = False
        self.closed = False
        self._auto_start_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._start_nop_task: asyncio.Task[None] | None = None
        self._advertise_task: asyncio.Task[None] | None = None
        self._last_discovery_log: dict[Address, float] = {}

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        discovery_transport, _discovery_protocol = await loop.create_datagram_endpoint(
            lambda: DiscoveryProtocol(self),
            sock=self._make_socket(self.discovery_port, broadcast=True),
        )
        storm_transport, _storm_protocol = await loop.create_datagram_endpoint(
            lambda: StormProtocol(self),
            sock=self._make_socket(self.storm_port, broadcast=False),
        )
        self.discovery_transport = discovery_transport  # type: ignore[assignment]
        self.storm_transport = storm_transport  # type: ignore[assignment]
        self.discovery_port = discovery_transport.get_extra_info("sockname")[1]
        self.storm_port = storm_transport.get_extra_info("sockname")[1]
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        self._advertise_task = asyncio.create_task(self._advertise_loop())
        LOG.info("listening on %s:%s and %s:%s", self.bind, self.discovery_port, self.bind, self.storm_port)
        LOG.info("advertising to: %s", ", ".join(self.broadcast_addresses))

    async def wait_closed(self) -> None:
        while not self.closed:
            await asyncio.sleep(3600)

    async def close(self) -> None:
        self.closed = True
        if self._auto_start_task:
            self._auto_start_task.cancel()
        if self._keepalive_task:
            self._keepalive_task.cancel()
        if self._start_nop_task:
            self._start_nop_task.cancel()
        if self._advertise_task:
            self._advertise_task.cancel()
        if self.discovery_transport:
            self.discovery_transport.close()
        if self.storm_transport:
            self.storm_transport.close()

    def _make_socket(self, port: int, *, broadcast: bool) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if broadcast:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind((self.bind, port))
        sock.setblocking(False)
        return sock

    @staticmethod
    def _default_broadcast_addresses(bind: str) -> list[str]:
        addresses = ["255.255.255.255"]
        candidates: set[str] = set()
        if bind not in ("", "0.0.0.0", "::"):
            candidates.add(bind)
        else:
            try:
                _host, _aliases, ips = socket.gethostbyname_ex(socket.gethostname())
                candidates.update(ip for ip in ips if "." in ip)
            except OSError:
                pass
            try:
                for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
                    candidates.add(info[4][0])
            except OSError:
                pass
        for ip in sorted(candidates):
            parts = ip.split(".")
            if len(parts) != 4 or parts[0] == "127":
                continue
            addresses.append(".".join(parts[:3] + ["255"]))
        return list(dict.fromkeys(addresses))

    def handle_discovery(self, data: bytes, addr: Address) -> None:
        if self.discovery_transport is None:
            return
        try:
            packet = LanPacket.from_wire(data)
        except PacketError as exc:
            LOG.debug("ignoring non-StarCraft discovery packet from %s: %s", addr, exc)
            return
        if packet.product != PRODUCT_SEXP or packet.version != VERSION_CODE:
            return
        if packet.kind != 2:
            return
        response = self._room_advertisement_wire()
        self.discovery_transport.sendto(response, addr)
        current_players = self._real_player_count()
        advertised_players = self._advertised_player_count(current_players)
        now = time.monotonic()
        if now - self._last_discovery_log.get(addr, 0.0) >= 2.0:
            self._last_discovery_log[addr] = now
            LOG.info(
                "discovery request from %s:%s -> advertised real_players=%s advertised_players=%s state=%s",
                addr[0],
                addr[1],
                current_players,
                advertised_players,
                self._advertised_state(),
            )

    def _room_advertisement_wire(self) -> bytes:
        current_players = self._real_player_count()
        return RoomAdvertisement(
            host_name=self.host_name,
            stat_string=self.stat_string,
            current_players=self._advertised_player_count(current_players),
            max_players=2,
            state=self._advertised_state(),
        ).to_wire()

    def _real_player_count(self) -> int:
        return len([s for s in self.sessions.values() if s.joined])

    def _advertised_state(self) -> int:
        return 0 if self._real_player_count() < 2 and not self.starting and not self.started else 0x0E

    @staticmethod
    def _advertised_player_count(real_players: int) -> int:
        # StarCraft LAN listings appear to require at least one player in the
        # room advertisement. A true dedicated host has no real player yet, so
        # advertise the virtual room owner as one occupant while the game is open.
        return max(1, min(real_players, 2))

    def handle_storm(self, data: bytes, addr: Address) -> None:
        try:
            packet = StormPacket.from_wire(data)
        except PacketError as exc:
            LOG.debug("ignoring non-Storm packet from %s: %s", addr, exc)
            return

        session = self.sessions.get(addr)
        if session:
            session.reliable.note_recv(packet)

        if packet.status == STATUS_RESEND_REQUEST:
            self._handle_resend_request(session, packet)
            return

        if packet.cls == CLS_CONTROL:
            self._handle_control(packet, addr)
        elif packet.cls == CLS_ASYNC:
            self._handle_async(packet, addr)
        elif packet.cls == CLS_SYNC:
            self._handle_sync(packet, addr)

    def _handle_resend_request(self, session: PlayerSession | None, packet: StormPacket) -> None:
        if session is None:
            return
        if len(packet.payload) >= 2:
            seq = struct.unpack_from("<H", packet.payload, 0)[0]
        else:
            seq = packet.seq_recv
        if session.reliable.resend(packet.cls, seq):
            return
        session.reliable.send_verify(packet.cls, player_id=VIRTUAL_HOST_ID)
        LOG.debug(
            "verified resend request for player=%s cls=%s requested_seq=%s verify_seq=%s",
            session.player_id,
            packet.cls,
            seq,
            session.reliable.last_recv.get(packet.cls, 0),
        )

    def _handle_control(self, packet: StormPacket, addr: Address) -> None:
        if packet.command == CMD_REQUESTJOIN:
            session = self._session_for_join(addr)
            if session is None:
                self._send_joinfail(addr)
                return
            session.reliable.note_recv(packet)
            session.reliable.send(CLS_CONTROL, command=CMD_REQUESTJOINOK, payload=struct.pack("<I", 1))
            LOG.info(
                "REQUESTJOIN from %s assigned player_id=%s peer_seq=%s ack=%s",
                addr,
                session.player_id,
                packet.seq_send,
                session.reliable.last_recv.get(CLS_CONTROL),
            )
            return

        session = self.sessions.get(addr)
        if session is None:
            return

        if packet.command == CMD_REQUESTJOIN2:
            LOG.info("REQUESTJOIN2 from player=%s", session.player_id)
        elif packet.command == CMD_ENTER:
            name = parse_enter_payload(packet.payload)
            slot_index = self._slot_index_for_name(name)
            if slot_index is None:
                self._reject_enter(session, name, "name is not configured for this 1v1 room")
                return
            if self._slot_taken(slot_index, except_session=session):
                self._reject_enter(session, name, "configured slot is already occupied")
                return
            session.name = name
            session.slot_index = slot_index
            session.team = slot_index + 1
            session.joined = True
            self._mark_lobby_activity(session)
            LOG.info("ENTER from player=%s name=%r slot=%s", session.player_id, session.name, session.slot_index)
            self._send_join_handshake(session)
        elif packet.command == CMD_PING:
            session.reliable.send(CLS_CONTROL, command=CMD_PONG, player_id=VIRTUAL_HOST_ID)
            if session.joined and not session.lobby_snapshot_sent:
                self._send_lobby_snapshot(session)
        elif packet.command == CMD_PONG:
            if session.joined:
                session.reliable.send_verify(CLS_CONTROL, player_id=VIRTUAL_HOST_ID)
        elif packet.command == CMD_QUIT:
            self._disconnect(session, "client quit")

    def _handle_async(self, packet: StormPacket, addr: Address) -> None:
        session = self.sessions.get(addr)
        if session is None or not packet.payload:
            return
        scgp = packet.payload[0]
        if scgp == SCGP_LOBBYCHAT:
            message = packet.payload[1:].split(b"\0", 1)[0].decode("latin1", "replace")
            LOG.info("lobby chat from player=%s: %s", session.player_id, message)
            self._send_to_others(session, CLS_ASYNC, payload=lobbychat_payload(message))
        elif scgp == SCGP_MAP:
            self._handle_map_request(session)
        else:
            self._send_to_others(session, CLS_ASYNC, payload=packet.payload)

    def _handle_map_request(self, session: PlayerSession) -> None:
        now = time.monotonic()
        if now - session.last_map_request_log >= 2.0:
            session.last_map_request_log = now
            LOG.info("MAP request from player=%s ready=%s percent=%s", session.player_id, session.lobby_ready, session.map_percent)
        session.reliable.send_verify(CLS_ASYNC, player_id=VIRTUAL_HOST_ID)
        if session.joined:
            if not session.lobby_ready:
                self._complete_lobby_join(session, "MAP request")
                return
            if session.map_percent < 100 and now - session.last_map_info_resend >= 1.0:
                session.last_map_info_resend = now
                session.reliable.send(CLS_ASYNC, payload=map_info_payload(self.map_name))

    def _handle_sync(self, packet: StormPacket, addr: Address) -> None:
        session = self.sessions.get(addr)
        if session is None or not packet.payload:
            return
        scgp = first_scgp(packet.payload)
        if scgp == SCGP_JOINEDGAME:
            self._complete_lobby_join(session, "JOINEDGAME")
            self._maybe_schedule_auto_start()
            return
        if scgp == SCGP_MAPPERCENT and len(packet.payload) >= 2:
            percent = packet.payload[1]
            if percent == 0 and session.joined and session.lobby_snapshot_sent and not session.lobby_ready:
                self._complete_lobby_join(session, "MAPPERCENT=0")
            if percent != session.map_percent:
                session.map_percent = percent
                self._mark_lobby_activity(session)
            else:
                session.map_percent = percent
            LOG.info("MAPPERCENT from player=%s percent=%s", session.player_id, session.map_percent)
            self._maybe_schedule_auto_start()
            return
        if scgp == SCGP_CHANGERACE and len(packet.payload) >= 3:
            session.race = packet.payload[2]
            self._mark_lobby_activity(session)
            LOG.info("CHANGERACE from player=%s race=%s", session.player_id, session.race)
            self._broadcast_slot_state()
            return
        if scgp == SCGP_STARTGAME:
            LOG.info("ignoring client StartGame from player=%s", session.player_id)
            return
        if self.starting:
            if scgp != SCGP_NOP:
                LOG.debug("ignoring pre-seed sync scgp=0x%02x from player=%s", scgp, session.player_id)
            return
        if self.started:
            self._trace_game_packet("rx", session, packet=packet)
            self._send_to_others(session, CLS_SYNC, payload=packet.payload)

    def _session_for_join(self, addr: Address) -> PlayerSession | None:
        existing = self.sessions.get(addr)
        if existing is not None:
            return existing
        used = {session.player_id for session in self.sessions.values()}
        player_id = next((candidate for candidate in (1, 2) if candidate not in used), None)
        if player_id is None or self.storm_transport is None:
            return None
        reliable = ReliableState(addr, player_id, self.storm_transport)
        session = PlayerSession(reliable=reliable, name=f"Player{player_id + 1}", team=player_id + 1)
        self.sessions[addr] = session
        return session

    def _send_joinfail(self, addr: Address) -> None:
        if self.storm_transport is None:
            return
        packet = StormPacket(0, 0, CLS_CONTROL, CMD_JOINFAIL, 0, STATUS_NORMAL).to_wire()
        self.storm_transport.sendto(packet, addr)

    def _reject_enter(self, session: PlayerSession, name: str, reason: str) -> None:
        LOG.warning("rejecting ENTER from %s name=%r: %s", session.address, name, reason)
        session.reliable.send(CLS_CONTROL, command=CMD_JOINFAIL, player_id=VIRTUAL_HOST_ID)
        self.sessions.pop(session.address, None)

    def _slot_index_for_name(self, name: str) -> int | None:
        for slot_index, configured_name in enumerate(self.player_slot_names):
            if name == configured_name:
                return slot_index
        return None

    def _slot_taken(self, slot_index: int, *, except_session: PlayerSession | None = None) -> bool:
        return any(
            session is not except_session and session.joined and session.slot_index == slot_index
            for session in self.sessions.values()
        )

    def _send_join_handshake(self, session: PlayerSession) -> None:
        r = session.reliable
        visible_host_name = self._visible_host_name_for(session)
        visible_stat_string = make_stat_string(visible_host_name, self.room_name)
        r.send(
            CLS_CONTROL,
            command=CMD_GAMEDATA,
            payload=gamedata_payload(1, visible_host_name, visible_stat_string),
        )
        r.send(
            CLS_CONTROL,
            command=CMD_PLAYER,
            payload=player_record_payload(VIRTUAL_HOST_ID, visible_host_name),
        )
        r.send(CLS_CONTROL, command=CMD_STATSCODE, payload=struct.pack("<I", 0))
        r.send(CLS_CONTROL, command=CMD_GAMETYPE, payload=gametype_payload())
        r.send(CLS_CONTROL, command=CMD_PING)
        r.send(CLS_SYNC, payload=nop_payload())
        LOG.info("sent lobby bootstrap to player=%s name=%r", session.player_id, session.name)

    def _visible_host_name_for(self, session: PlayerSession) -> str:
        if session.slot_index == 0:
            return self.guest_player_name
        return self.host_player_name

    def _send_lobby_snapshot(self, session: PlayerSession) -> None:
        if session.lobby_snapshot_sent:
            return
        session.lobby_snapshot_sent = True
        session.reliable.send(CLS_ASYNC, payload=roomdata_payload())
        session.reliable.send(CLS_ASYNC, payload=bytes([SCGP_UNKNOWNREQUEST]))
        LOG.info("sent ROOMDATA snapshot to player=%s name=%r", session.player_id, session.name)

    def _broadcast_player_join(self, joined_session: PlayerSession) -> None:
        for session in self.sessions.values():
            if not session.joined:
                continue
            if session is not joined_session:
                session.reliable.send(
                    CLS_CONTROL,
                    command=CMD_PLAYER,
                    payload=player_record_payload(VIRTUAL_HOST_ID, joined_session.name),
                )
                session.reliable.send(CLS_ASYNC, payload=playerjoin_payload(VIRTUAL_HOST_ID), player_id=VIRTUAL_HOST_ID)
            else:
                session.reliable.send(CLS_ASYNC, payload=playerjoin_payload(1), player_id=VIRTUAL_HOST_ID)

    def _broadcast_slot_state(self) -> None:
        for session in self.sessions.values():
            if session.joined:
                session.reliable.send(CLS_SYNC, payload=self._slot_sync_payload_for(session))

    def _complete_lobby_join(self, session: PlayerSession, source: str) -> None:
        first_ready = not session.lobby_ready
        session.lobby_ready = True
        self._mark_lobby_activity(session)
        if first_ready:
            if source == "JOINEDGAME":
                LOG.info("JOINEDGAME from player=%s", session.player_id)
            else:
                LOG.info("implicit JOINEDGAME from player=%s via %s", session.player_id, source)
            self._broadcast_player_join(session)
        if not session.map_bootstrap_sent:
            session.map_bootstrap_sent = True
            session.reliable.send(CLS_ASYNC, payload=map_info_payload(self.map_name))
        self._broadcast_slot_state()

    def _slot_sync_payload_for(self, session: PlayerSession) -> bytes:
        opponent = self._opponent_for(session)
        opponent_race = opponent.race if opponent else 6
        if session.slot_index == 0:
            return slot_sync_payload(
                session.race,
                opponent_race,
                player0_id=1,
                player1_id=VIRTUAL_HOST_ID,
                player0_active=True,
                player1_active=True,
            )
        return slot_sync_payload(
            opponent_race,
            session.race,
            player0_id=VIRTUAL_HOST_ID,
            player1_id=1,
            player0_active=True,
            player1_active=True,
        )

    def _opponent_for(self, session: PlayerSession) -> PlayerSession | None:
        return next((other for other in self.sessions.values() if other is not session and other.joined), None)

    def _race_for(self, player_id: int) -> int:
        for session in self.sessions.values():
            if session.player_id == player_id:
                return session.race
        return 6

    def _send_to_others(self, sender: PlayerSession, cls: int, *, payload: bytes) -> None:
        for session in self.sessions.values():
            if session is sender or not session.joined:
                continue
            self._trace_game_packet("tx", sender, target=session, cls=cls, payload=payload)
            session.reliable.send(cls, payload=payload, player_id=VIRTUAL_HOST_ID)

    def _trace_game_packet(
        self,
        direction: str,
        session: PlayerSession,
        *,
        packet: StormPacket | None = None,
        target: PlayerSession | None = None,
        cls: int | None = None,
        payload: bytes | None = None,
    ) -> None:
        if not self.trace_game:
            return
        if packet is not None:
            cls = packet.cls
            payload = packet.payload
        if cls != CLS_SYNC or not payload:
            return
        scgp = first_scgp(payload)
        if scgp == SCGP_NOP and not self.trace_nop:
            return
        scgp_name = GAME_TRACE_SCGP_NAMES.get(scgp, f"0x{scgp:02x}" if scgp is not None else "EMPTY")
        head = payload[:32].hex(" ")
        if packet is not None:
            LOG.info(
                "GAME_TRACE %s player=%s name=%r storm_pid=%s seq=(%s,%s) status=%s scgp=%s len=%s head=%s",
                direction,
                session.player_id,
                session.name,
                packet.player_id,
                packet.seq_send,
                packet.seq_recv,
                packet.status,
                scgp_name,
                len(payload),
                head,
            )
            return
        LOG.info(
            "GAME_TRACE %s from_player=%s from_name=%r to_player=%s to_name=%r scgp=%s len=%s head=%s",
            direction,
            session.player_id,
            session.name,
            target.player_id if target else None,
            target.name if target else None,
            scgp_name,
            len(payload),
            head,
        )

    def _mark_lobby_activity(self, session: PlayerSession) -> None:
        session.last_lobby_activity = time.monotonic()

    def _maybe_schedule_auto_start(self) -> None:
        if self.starting or self.started or self._auto_start_task is not None:
            return
        if not self._ready_to_start():
            return
        self._auto_start_task = asyncio.create_task(self._auto_start())

    def _ready_to_start(self, *, stable: bool = False) -> bool:
        joined = [session for session in self.sessions.values() if session.joined]
        if len(joined) != 2:
            return False
        if self._session_for_slot(0) is None or self._session_for_slot(1) is None:
            return False
        if not all(session.lobby_ready for session in joined):
            return False
        if not all(session.map_percent >= 100 for session in joined):
            return False
        if stable:
            if self._start_stability_remaining(joined) > 0:
                return False
        return True

    def _session_for_slot(self, slot_index: int) -> PlayerSession | None:
        return next(
            (session for session in self.sessions.values() if session.joined and session.slot_index == slot_index),
            None,
        )

    def _start_stability_remaining(self, joined: list[PlayerSession] | None = None) -> float:
        if joined is None:
            joined = [session for session in self.sessions.values() if session.joined]
        last_activity = max((session.last_lobby_activity for session in joined), default=0.0)
        return self.start_stability_delay - (time.monotonic() - last_activity)

    async def _auto_start(self) -> None:
        try:
            while True:
                if not self._ready_to_start():
                    self._auto_start_task = None
                    return
                joined = [session for session in self.sessions.values() if session.joined]
                remaining = self._start_stability_remaining(joined)
                if remaining <= 0:
                    break
                LOG.info("waiting %.1fs for lobby stability before countdown", remaining)
                await asyncio.sleep(max(remaining, 0.05))
        except asyncio.CancelledError:
            self._auto_start_task = None
            return
        if not self._ready_to_start(stable=True):
            self._auto_start_task = None
            self._maybe_schedule_auto_start()
            return
        LOG.info("auto-start scheduled in %.1fs", self.auto_start_delay)
        try:
            await asyncio.sleep(self.auto_start_delay)
        except asyncio.CancelledError:
            self._auto_start_task = None
            return
        if not self._ready_to_start(stable=True):
            self._auto_start_task = None
            self._maybe_schedule_auto_start()
            return
        self.starting = True
        for session in self.sessions.values():
            if session.joined:
                LOG.info(
                    "GAMESTATE to player=%s control_seq=%s",
                    session.player_id,
                    session.reliable.next_send.get(CLS_CONTROL),
                )
                session.reliable.send(
                    CLS_CONTROL,
                    command=CMD_GAMESTATE,
                    payload=game_state_payload(),
                    player_id=VIRTUAL_HOST_ID,
                )
        try:
            await asyncio.sleep(self.game_state_delay)
        except asyncio.CancelledError:
            self.starting = False
            self._auto_start_task = None
            return
        if not self._ready_to_start(stable=True):
            self.starting = False
            self._auto_start_task = None
            self._maybe_schedule_auto_start()
            return
        for session in self.sessions.values():
            if session.joined:
                LOG.info("STARTGAME to player=%s sync_seq=%s", session.player_id, session.reliable.next_send.get(CLS_SYNC))
                session.reliable.send(CLS_SYNC, payload=startgame_payload())
        self._start_nop_task = asyncio.create_task(self._start_transition_nop_loop())
        try:
            await asyncio.sleep(self.seed_delay)
        except asyncio.CancelledError:
            self.starting = False
            if self._start_nop_task:
                self._start_nop_task.cancel()
                self._start_nop_task = None
            self._auto_start_task = None
            return
        if self._start_nop_task:
            self._start_nop_task.cancel()
            self._start_nop_task = None
        seed = seed_payload()
        for session in self.sessions.values():
            if session.joined:
                session.reliable.send(CLS_SYNC, payload=seed)
        self.starting = False
        self.started = True
        self._auto_start_task = None
        LOG.info("game started")

    async def _start_transition_nop_loop(self) -> None:
        try:
            while self.starting and not self.closed:
                await asyncio.sleep(0.25)
                for session in list(self.sessions.values()):
                    if session.joined:
                        session.reliable.send(CLS_SYNC, payload=nop_payload())
        except asyncio.CancelledError:
            return

    async def _keepalive_loop(self) -> None:
        while not self.closed:
            await asyncio.sleep(0.25)
            if self.starting or self.started:
                continue
            for session in list(self.sessions.values()):
                if not session.joined:
                    continue
                session.reliable.send(CLS_SYNC, payload=nop_payload())

    async def _advertise_loop(self) -> None:
        while not self.closed:
            await asyncio.sleep(self.advertise_interval)
            if self.discovery_transport is None:
                continue
            if self.starting or self.started:
                continue
            self._send_room_advertisements()

    def _send_room_advertisements(self) -> None:
        if self.discovery_transport is None:
            return
        packet = self._room_advertisement_wire()
        for address in self.broadcast_addresses:
            self.discovery_transport.sendto(packet, (address, self.discovery_port))

    def _disconnect(self, session: PlayerSession, reason: str) -> None:
        LOG.info("disconnect player=%s reason=%s", session.player_id, reason)
        for other in self.sessions.values():
            if other is not session and other.joined:
                other.reliable.send(CLS_CONTROL, command=CMD_QUIT, payload=quit_payload())
        self.sessions.pop(session.address, None)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Experimental StarCraft LAN non-player host relay")
    parser.add_argument("--bind", default="0.0.0.0")
    parser.add_argument("--discovery-port", type=int, default=6111)
    parser.add_argument("--storm-port", type=int, default=6112)
    parser.add_argument("--host-name", default=DEFAULT_HOST_NAME)
    parser.add_argument("--host-player-name", default=None)
    parser.add_argument("--guest-player-name", default=DEFAULT_GUEST_PLAYER_NAME)
    parser.add_argument("--room-name", default=DEFAULT_ROOM_NAME)
    parser.add_argument("--map-path", default="")
    parser.add_argument("--map-name", default=DEFAULT_MAP_FILE_NAME)
    parser.add_argument("--auto-start-delay", type=float, default=3.0)
    parser.add_argument("--game-state-delay", type=float, default=0.35)
    parser.add_argument("--seed-delay", type=float, default=5.75)
    parser.add_argument("--advertise-interval", type=float, default=2.0)
    parser.add_argument("--start-stability-delay", type=float, default=1.0)
    parser.add_argument("--trace-game", action="store_true", help="Log in-game non-NOP CLS_SYNC packets and forwarding.")
    parser.add_argument("--trace-nop", action="store_true", help="Include in-game NOP packets in trace-game output.")
    parser.add_argument("--log-file", default="logs/sc_host.log", help="Write logs to this local file; set empty to disable.")
    parser.add_argument(
        "--broadcast-address",
        action="append",
        default=None,
        help="Extra LAN broadcast target for room advertisements, e.g. 192.168.137.255. Can be repeated.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def configure_logging(level_name: str, log_file: str = "") -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(level)
    root.addHandler(console)

    if log_file:
        path = Path(log_file)
        if path.parent != Path("."):
            path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        root.addHandler(file_handler)


async def amain(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level, args.log_file)
    server = StarCraftHostServer(
        bind=args.bind,
        discovery_port=args.discovery_port,
        storm_port=args.storm_port,
        host_name=args.host_name,
        host_player_name=args.host_player_name,
        guest_player_name=args.guest_player_name,
        room_name=args.room_name,
        map_name=args.map_name,
        auto_start_delay=args.auto_start_delay,
        game_state_delay=args.game_state_delay,
        seed_delay=args.seed_delay,
        advertise_interval=args.advertise_interval,
        start_stability_delay=args.start_stability_delay,
        broadcast_addresses=args.broadcast_address,
        trace_game=args.trace_game,
        trace_nop=args.trace_nop,
    )
    if args.log_file:
        LOG.info("writing log file: %s", args.log_file)
    if args.map_path:
        LOG.info("map path configured: %s (metadata remains fixed to capture constants)", args.map_path)
    await server.start()
    try:
        await server.wait_closed()
    except (KeyboardInterrupt, asyncio.CancelledError):
        await server.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(amain(argv))
