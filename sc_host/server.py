from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import logging
from pathlib import Path
import socket
import struct
import time
from typing import Sequence

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
    DEFAULT_MAP_FILE_NAME,
    DEFAULT_MAP_SIZE,
    DEFAULT_ROOM_NAME,
    PRODUCT_SEXP,
    SCGP_CHANGERACE,
    SCGP_JOINEDGAME,
    SCGP_LOBBYCHAT,
    SCGP_MAP,
    SCGP_MAPPERCENT,
    SCGP_NOP,
    SCGP_STARTGAME,
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
    map_complete_payload,
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
from .protocol_debug import GAME_TRACE_SCGP_NAMES, _log_protocol_lan, _log_protocol_storm, _scgp_payload_detail
from .reliable import ReliableState
from .session import PlayerSession

Address = tuple[str, int]

LOG = logging.getLogger("sc_host")
DEFAULT_MAIN_HOST_NAME = "Sun"
DEFAULT_SUB_HOST_NAME = "SunX"
WIRE_HOST_ID = 0
MAIN_HOST_PLAYER_ID = 1
SUB_HOST_PLAYER_ID = 2
UNKNOWN_PLAYER_ID = 0xFF
PLAYER_IDS_BY_SLOT = (MAIN_HOST_PLAYER_ID, SUB_HOST_PLAYER_ID)
LOBBY_SYNC_NOP_INTERVAL = 0.25
START_TRANSITION_SYNC_INTERVAL = 0.05
RESEND_NEXT_SEND_SYNC_INTERVAL = 0.25
RESEND_THROTTLE_LOG_INTERVAL = 1.0
JOIN_BOOTSTRAP_SYNC_NOP_COUNT = 2


@dataclass(frozen=True)
class ResendRequestContext:
    session: PlayerSession
    packet: StormPacket
    header_requested_seq: int
    payload_requested_seq: int | str
    chosen_requested_seq: int
    chosen_source: str
    payload_head: str
    next_send: int
    last_recv: int
    history_count: int
    history_min: int | str
    history_max: int | str
    history_tail: str


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
        main_host_name: str = DEFAULT_MAIN_HOST_NAME,
        sub_host_name: str = DEFAULT_SUB_HOST_NAME,
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
        self.main_host_name = main_host_name
        self.sub_host_name = sub_host_name
        self.player_slot_names = (self.main_host_name, self.sub_host_name)
        # For the current MainHost/SubHost model, the first tested join path is
        # Sun joining a room hosted on the wire by SunX.
        self.host_name = self.sub_host_name
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
        self.room_created_at = time.monotonic()
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
        _log_protocol_lan("rx", data, addr, event="discovery")
        if packet.product != PRODUCT_SEXP or packet.version != VERSION_CODE:
            return
        if packet.kind != 2:
            return
        response = self._room_advertisement_wire()
        self.discovery_transport.sendto(response, addr)
        _log_protocol_lan("tx", response, addr, event="room_advertisement")
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
        _log_protocol_storm("rx", packet, addr, event="datagram")

        session = self.sessions.get(addr)
        if session:
            session.reliable.note_recv(packet)

        if packet.status == STATUS_RESEND_REQUEST:
            self._handle_resend_request(session, packet)
            return
        if packet.status == STATUS_VERIFY:
            self._handle_verify(session, packet)
            return

        if packet.cls == CLS_CONTROL:
            self._handle_control(packet, addr)
        elif packet.cls == CLS_ASYNC:
            self._handle_async(packet, addr)
        elif packet.cls == CLS_SYNC:
            self._handle_sync(packet, addr)

    def _handle_verify(self, session: PlayerSession | None, packet: StormPacket) -> None:
        if session is None:
            return
        LOG.debug(
            "VERIFY from player=%s name=%r cls=%s command=0x%02x storm_pid=%s seq_send=%s seq_recv=%s",
            session.player_id,
            session.name,
            packet.cls,
            packet.command,
            packet.player_id,
            packet.seq_send,
            packet.seq_recv,
        )
        if packet.cls == CLS_CONTROL and session.joined and not session.lobby_snapshot_sent:
            self._maybe_send_lobby_snapshot_after_join_ack(session, packet, source="client CONTROL VERIFY")

    def _handle_resend_request(self, session: PlayerSession | None, packet: StormPacket) -> None:
        if session is None:
            return
        context = self._resend_context(session, packet)
        if context.chosen_requested_seq == context.next_send:
            self._handle_next_send_resend(context)
            return
        if session.reliable.resend(packet.cls, context.chosen_requested_seq):
            self._log_resent_packet(context)
            return
        self._send_resend_verify(context)

    def _resend_context(self, session: PlayerSession, packet: StormPacket) -> ResendRequestContext:
        header_requested_seq = packet.seq_recv
        payload_requested_seq: int | str = "-"
        if len(packet.payload) >= 2:
            payload_requested_seq = struct.unpack_from("<H", packet.payload, 0)[0]
            seq = payload_requested_seq
            chosen_source = "payload[0:2]"
        else:
            seq = header_requested_seq
            chosen_source = "header.seq_recv"
        history_count, history_min, history_max, history_tail = self._resend_history_summary(session.reliable, packet.cls)
        payload_head = packet.payload[:64].hex(" ") or "-"
        next_send = session.reliable.next_send.get(packet.cls, 0)
        last_recv = session.reliable.last_recv.get(packet.cls, 0)
        return ResendRequestContext(
            session=session,
            packet=packet,
            header_requested_seq=header_requested_seq,
            payload_requested_seq=payload_requested_seq,
            chosen_requested_seq=seq,
            chosen_source=chosen_source,
            payload_head=payload_head,
            next_send=next_send,
            last_recv=last_recv,
            history_count=history_count,
            history_min=history_min,
            history_max=history_max,
            history_tail=history_tail,
        )

    def _handle_next_send_resend(self, context: ResendRequestContext) -> None:
        session = context.session
        packet = context.packet
        if packet.cls != CLS_SYNC:
            LOG.debug(
                "resend request matched next_send; no history resend for player=%s cls=%s status=%s "
                "command=0x%02x storm_pid=%s seq_send=%s seq_recv=%s payload_len=%s payload_head=%s "
                "chosen_requested_seq=%s chosen_source=%s header_requested_seq=%s payload_requested_seq=%s "
                "next_send=%s last_recv=%s history_count=%s history_min=%s history_max=%s history_tail=%s",
                session.player_id,
                packet.cls,
                packet.status,
                packet.command,
                packet.player_id,
                packet.seq_send,
                packet.seq_recv,
                len(packet.payload),
                context.payload_head,
                context.chosen_requested_seq,
                context.chosen_source,
                context.header_requested_seq,
                context.payload_requested_seq,
                context.next_send,
                context.last_recv,
                context.history_count,
                context.history_min,
                context.history_max,
                context.history_tail,
            )
            return

        now = time.monotonic()
        last_sync_packet_sent = session.last_sync_packet_sent
        sent_tick = self._send_due_sync_tick(
            session,
            fallback_player_id=self._host_player_id_for(session),
            throttle_interval=RESEND_NEXT_SEND_SYNC_INTERVAL,
            now=now,
        )
        if sent_tick is None:
            session.resend_next_send_throttled += 1
            if now - session.last_resend_throttle_log >= RESEND_THROTTLE_LOG_INTERVAL:
                throttled = session.resend_next_send_throttled
                session.resend_next_send_throttled = 0
                session.last_resend_throttle_log = now
                last_sync_age_ms = int((now - last_sync_packet_sent) * 1000) if last_sync_packet_sent else "-"
                LOG.debug(
                    "resend request matched next_send; throttled sync tick for player=%s cls=%s "
                    "seq_send=%s seq_recv=%s chosen_requested_seq=%s next_send=%s last_recv=%s "
                    "throttled_count=%s throttle_ms=%s last_sync_age_ms=%s "
                    "history_count=%s history_min=%s history_max=%s history_tail=%s",
                    session.player_id,
                    packet.cls,
                    packet.seq_send,
                    packet.seq_recv,
                    context.chosen_requested_seq,
                    context.next_send,
                    context.last_recv,
                    throttled,
                    int(RESEND_NEXT_SEND_SYNC_INTERVAL * 1000),
                    last_sync_age_ms,
                    context.history_count,
                    context.history_min,
                    context.history_max,
                    context.history_tail,
                )
            return

        sent_seq, sent_source = sent_tick
        new_history_count, new_history_min, new_history_max, new_history_tail = self._resend_history_summary(
            session.reliable, packet.cls
        )
        throttled = session.resend_next_send_throttled
        session.resend_next_send_throttled = 0
        LOG.debug(
            "resend request matched next_send; sent sync tick packet for player=%s cls=%s status=%s "
            "command=0x%02x storm_pid=%s seq_send=%s seq_recv=%s payload_len=%s payload_head=%s "
            "chosen_requested_seq=%s chosen_source=%s header_requested_seq=%s payload_requested_seq=%s "
            "sent_seq=%s sent_source=%s old_next_send=%s new_next_send=%s last_recv=%s throttled_since_last_send=%s "
            "throttle_ms=%s "
            "history_count=%s history_min=%s history_max=%s history_tail=%s "
            "new_history_count=%s new_history_min=%s new_history_max=%s new_history_tail=%s",
            session.player_id,
            packet.cls,
            packet.status,
            packet.command,
            packet.player_id,
            packet.seq_send,
            packet.seq_recv,
            len(packet.payload),
            context.payload_head,
            context.chosen_requested_seq,
            context.chosen_source,
            context.header_requested_seq,
            context.payload_requested_seq,
            sent_seq,
            sent_source,
            context.next_send,
            session.reliable.next_send.get(packet.cls, 0),
            context.last_recv,
            throttled,
            int(RESEND_NEXT_SEND_SYNC_INTERVAL * 1000),
            context.history_count,
            context.history_min,
            context.history_max,
            context.history_tail,
            new_history_count,
            new_history_min,
            new_history_max,
            new_history_tail,
        )

    def _log_resent_packet(self, context: ResendRequestContext) -> None:
        session = context.session
        packet = context.packet
        LOG.debug(
            "resent packet for player=%s cls=%s status=%s command=0x%02x storm_pid=%s "
            "seq_send=%s seq_recv=%s payload_len=%s payload_head=%s "
            "chosen_requested_seq=%s chosen_source=%s header_requested_seq=%s payload_requested_seq=%s "
            "history_count=%s history_min=%s history_max=%s history_tail=%s",
            session.player_id,
            packet.cls,
            packet.status,
            packet.command,
            packet.player_id,
            packet.seq_send,
            packet.seq_recv,
            len(packet.payload),
            context.payload_head,
            context.chosen_requested_seq,
            context.chosen_source,
            context.header_requested_seq,
            context.payload_requested_seq,
            context.history_count,
            context.history_min,
            context.history_max,
            context.history_tail,
        )

    def _send_resend_verify(self, context: ResendRequestContext) -> None:
        session = context.session
        packet = context.packet
        verify_seq = context.last_recv
        session.reliable.send_verify(packet.cls, player_id=self._host_player_id_for(session))
        LOG.debug(
            "missing resend history; sent verify for player=%s cls=%s status=%s command=0x%02x storm_pid=%s "
            "seq_send=%s seq_recv=%s payload_len=%s payload_head=%s "
            "chosen_requested_seq=%s chosen_source=%s header_requested_seq=%s payload_requested_seq=%s "
            "verify_seq=%s next_send=%s last_recv=%s "
            "history_count=%s history_min=%s history_max=%s history_tail=%s",
            session.player_id,
            packet.cls,
            packet.status,
            packet.command,
            packet.player_id,
            packet.seq_send,
            packet.seq_recv,
            len(packet.payload),
            context.payload_head,
            context.chosen_requested_seq,
            context.chosen_source,
            context.header_requested_seq,
            context.payload_requested_seq,
            verify_seq,
            context.next_send,
            context.last_recv,
            context.history_count,
            context.history_min,
            context.history_max,
            context.history_tail,
        )

    @staticmethod
    def _resend_history_summary(
        reliable: ReliableState, cls: int
    ) -> tuple[int, int | str, int | str, str]:
        history = reliable.history.get(cls, {})
        if not history:
            return 0, "-", "-", "-"
        order = reliable.history_order.get(cls)
        tail_values = [seq for seq in list(order or ()) if seq in history][-8:]
        if not tail_values:
            tail_values = sorted(history)[-8:]
        return len(history), min(history), max(history), ",".join(str(seq) for seq in tail_values)

    @staticmethod
    def _seq_reached(seq: int, target: int) -> bool:
        return ((seq - target) & 0xFFFF) < 0x8000

    def _handle_control(self, packet: StormPacket, addr: Address) -> None:
        if packet.command == CMD_REQUESTJOIN:
            existing = self.sessions.get(addr)
            if (
                existing is not None
                and packet.seq_send == 0
                and packet.player_id == UNKNOWN_PLAYER_ID
                and not existing.lobby_ready
            ):
                LOG.info(
                    "resetting pending join session for %s old_player=%s joined=%s lobby_snapshot_sent=%s",
                    addr,
                    existing.player_id,
                    existing.joined,
                    existing.lobby_snapshot_sent,
                )
                self.sessions.pop(addr, None)
            session = self._session_for_join(addr)
            if session is None:
                self._send_joinfail(addr)
                return
            session.reliable.note_recv(packet)
            session.reliable.send(CLS_CONTROL, command=CMD_REQUESTJOINOK, payload=struct.pack("<I", 1))
            LOG.info(
                "REQUESTJOIN from %s pending player_id=%s peer_seq=%s ack=%s",
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
            session.reliable.player_id = self._player_id_for_slot(slot_index)
            session.team = slot_index + 1
            session.joined = True
            self._mark_lobby_activity(session)
            LOG.info("ENTER from player=%s name=%r slot=%s", session.player_id, session.name, session.slot_index)
            self._send_join_handshake(session)
        elif packet.command == CMD_PING:
            session.reliable.send(CLS_CONTROL, command=CMD_PONG, player_id=self._host_player_id_for(session))
        elif packet.command == CMD_PONG:
            if session.joined:
                session.reliable.send_verify(CLS_CONTROL, player_id=self._host_player_id_for(session))
                self._note_join_ping_ack(session, packet, source="client PONG")
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
            self._handle_map_request(session, packet.payload)
        else:
            self._send_to_others(session, CLS_ASYNC, payload=packet.payload)

    def _handle_map_request(self, session: PlayerSession, payload: bytes) -> None:
        now = time.monotonic()
        length: int | None = None
        kind: int | None = None
        request_value: int | None = None
        file_position: int | None = None
        if len(payload) >= 5:
            length, kind = struct.unpack_from("<HH", payload, 1)
            if kind == 0x0000 and len(payload) >= 11:
                request_value = struct.unpack_from("<H", payload, 5)[0]
                file_position = struct.unpack_from("<I", payload, 7)[0]
        LOG.info(
            "MAP request from player=%s kind=%s length=%s request_value=%s file_position=%s map_size=%s ready=%s percent=%s",
            session.player_id,
            f"0x{kind:04x}" if kind is not None else "-",
            length if length is not None else "-",
            request_value if request_value is not None else "-",
            file_position if file_position is not None else "-",
            DEFAULT_MAP_SIZE,
            session.lobby_ready,
            session.map_percent,
        )
        session.reliable.send_verify(CLS_ASYNC, player_id=self._host_player_id_for(session))
        if not session.joined:
            return
        if kind == 0x0000:
            if file_position is not None and file_position >= DEFAULT_MAP_SIZE:
                session.map_percent = 100
                self._mark_lobby_activity(session)
                if not session.post_map_slot_state_sent:
                    session.post_map_slot_state_sent = True
                    self._send_slot_state(session, source="post-map", include_map_percent=True)
                    LOG.info(
                        "queued post-map MAPPERCENT/SLOTUPDATE/NEWNETPLAYER to player=%s file_position=%s map_size=%s ready=%s",
                        session.player_id,
                        file_position,
                        DEFAULT_MAP_SIZE,
                        session.lobby_ready,
                    )
                else:
                    LOG.debug(
                        "post-map slot state already sent to player=%s file_position=%s map_size=%s ready=%s",
                        session.player_id,
                        file_position,
                        DEFAULT_MAP_SIZE,
                        session.lobby_ready,
                    )
                return
            if now - session.last_map_info_resend >= 1.0:
                session.last_map_info_resend = now
                session.map_bootstrap_sent = True
                session.reliable.send(
                    CLS_ASYNC,
                    payload=map_info_payload(self.map_name),
                    player_id=self._host_player_id_for(session),
                )
                LOG.info(
                    "resent MAP info to player=%s file_position=%s map_size=%s",
                    session.player_id,
                    file_position if file_position is not None else "-",
                    DEFAULT_MAP_SIZE,
                )
            LOG.warning(
                "MAP transfer requested but map block sending is not implemented player=%s file_position=%s map_size=%s",
                session.player_id,
                file_position if file_position is not None else "-",
                DEFAULT_MAP_SIZE,
            )
            return
        LOG.debug("ignoring unsupported MAP event from player=%s kind=%s length=%s", session.player_id, kind, length)

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
            if percent != session.map_percent:
                session.map_percent = percent
                self._mark_lobby_activity(session)
            else:
                session.map_percent = percent
            if percent in (0, 100) and session.joined and session.lobby_snapshot_sent and not session.lobby_ready:
                LOG.info(
                    "MAPPERCENT before JOINEDGAME from player=%s percent=%s",
                    session.player_id,
                    session.map_percent,
                )
            else:
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
        if len(self.sessions) >= 2 or self.storm_transport is None:
            return None
        reliable = ReliableState(addr, UNKNOWN_PLAYER_ID, self.storm_transport)
        session = PlayerSession(reliable=reliable)
        self.sessions[addr] = session
        return session

    def _send_joinfail(self, addr: Address) -> None:
        if self.storm_transport is None:
            return
        packet = StormPacket(0, 0, CLS_CONTROL, CMD_JOINFAIL, WIRE_HOST_ID, STATUS_NORMAL)
        wire = packet.to_wire()
        self.storm_transport.sendto(wire, addr)
        _log_protocol_storm("tx", packet, addr, event="joinfail")

    def _reject_enter(self, session: PlayerSession, name: str, reason: str) -> None:
        LOG.warning("rejecting ENTER from %s name=%r: %s", session.address, name, reason)
        session.reliable.send(CLS_CONTROL, command=CMD_JOINFAIL, player_id=WIRE_HOST_ID)
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

    def _is_main_host_session(self, session: PlayerSession) -> bool:
        return session.slot_index == 0 or session.name == self.main_host_name

    @staticmethod
    def _player_id_for_slot(slot_index: int) -> int:
        if 0 <= slot_index < len(PLAYER_IDS_BY_SLOT):
            return PLAYER_IDS_BY_SLOT[slot_index]
        return UNKNOWN_PLAYER_ID

    def _visible_host_name_for(self, session: PlayerSession) -> str:
        if self._is_main_host_session(session):
            return self.sub_host_name
        return self.main_host_name

    def _visible_host_slot_for(self, session: PlayerSession) -> int:
        return 1 if self._is_main_host_session(session) else 0

    def _host_player_id_for(self, session: PlayerSession) -> int:
        return WIRE_HOST_ID

    def _slot_active(self, slot_index: int) -> bool:
        return self._game_joined_session_for_slot(slot_index) is not None

    def _gamedata_fields_for(self, session: PlayerSession) -> tuple[int, int, int]:
        command2_packet_count = session.reliable.last_recv.get(CLS_SYNC, 0)
        unknown = 0x06
        game_uptime_seconds = max(0, int(time.monotonic() - self.room_created_at))
        return command2_packet_count, unknown, game_uptime_seconds

    def _send_join_handshake(self, session: PlayerSession) -> None:
        r = session.reliable
        session.lobby_snapshot_sent = False
        session.lobby_ready = False
        session.map_bootstrap_sent = False
        session.post_map_slot_state_sent = False
        session.join_ping_ack_seq = None
        session.join_ping_ack_seen = False
        session.map_percent = 0
        session.last_map_info_resend = 0.0
        visible_host_name = self._visible_host_name_for(session)
        visible_host_id = self._player_id_for_slot(self._visible_host_slot_for(session))
        host_storm_id = self._host_player_id_for(session)
        visible_stat_string = make_stat_string(visible_host_name, self.room_name)
        command2_packet_count, gamedata_unknown, game_uptime_seconds = self._gamedata_fields_for(session)
        gamedata = gamedata_payload(
            session.player_id,
            visible_host_name,
            visible_stat_string,
            command2_packet_count=command2_packet_count,
            unknown=gamedata_unknown,
            game_uptime_seconds=game_uptime_seconds,
        )
        wire = r.send(
            CLS_CONTROL,
            command=CMD_GAMEDATA,
            payload=gamedata,
            player_id=host_storm_id,
        )
        self._log_join_packet(
            session,
            "GAMEDATA",
            wire,
            detail=(
                f"assigned_player_id={session.player_id} host_storm_id={host_storm_id} "
                f"visible_host_id={visible_host_id} "
                f"visible_host_name={visible_host_name!r} "
                f"role={'MainHost' if self._is_main_host_session(session) else 'SubHost'} "
                f"command2_packet_count={command2_packet_count} "
                f"unknown={gamedata_unknown} game_uptime_seconds={game_uptime_seconds} "
                f"room_name={self.room_name!r} stat_string={visible_stat_string!r}"
            ),
        )

        player_record_id = WIRE_HOST_ID
        player_record = player_record_payload(player_record_id, visible_host_name, is_host=True)
        wire = r.send(
            CLS_CONTROL,
            command=CMD_PLAYER,
            payload=player_record,
            player_id=host_storm_id,
        )
        self._log_join_packet(
            session,
            "PLAYER",
            wire,
            detail=(
                f"record_player_id={player_record_id} visible_host_id={visible_host_id} "
                f"record_name={visible_host_name!r} is_host=1"
            ),
        )

        statscode = struct.pack("<I", 0)
        wire = r.send(CLS_CONTROL, command=CMD_STATSCODE, payload=statscode, player_id=host_storm_id)
        self._log_join_packet(session, "STATSCODE", wire, detail="stat_code=0")

        gametype = gametype_payload()
        wire = r.send(CLS_CONTROL, command=CMD_GAMETYPE, payload=gametype, player_id=host_storm_id)
        self._log_join_packet(session, "GAMETYPE", wire, detail=f"game_type_payload_len={len(gametype)}")

        for _ in range(JOIN_BOOTSTRAP_SYNC_NOP_COUNT):
            self._send_sync_nop(session, player_id=host_storm_id)

        wire = r.send(CLS_CONTROL, command=CMD_PING, player_id=host_storm_id)
        try:
            ping_packet = StormPacket.from_wire(wire)
            session.join_ping_ack_seq = (ping_packet.seq_send + 1) & 0xFFFF
        except PacketError as exc:
            LOG.warning(
                "failed to decode sent join PING for player=%s name=%r: %s",
                session.player_id,
                session.name,
                exc,
            )
        self._log_join_packet(
            session,
            "PING",
            wire,
            detail=f"empty payload ack_target={session.join_ping_ack_seq}",
        )

        LOG.info("sent lobby bootstrap to player=%s name=%r", session.player_id, session.name)

    def _log_join_packet(self, session: PlayerSession, label: str, wire: bytes, *, detail: str = "") -> None:
        try:
            packet = StormPacket.from_wire(wire)
        except PacketError as exc:
            LOG.warning(
                "sent join %s to player=%s name=%r addr=%s:%s but failed to decode wire packet: %s",
                label,
                session.player_id,
                session.name,
                session.address[0],
                session.address[1],
                exc,
            )
            return
        LOG.info(
            "sent join %s to player=%s name=%r addr=%s:%s seq_send=%s seq_recv=%s "
            "cls=%s command=0x%02x storm_pid=%s status=%s wire_len=%s payload_len=%s "
            "payload_head=%s detail=%s",
            label,
            session.player_id,
            session.name,
            session.address[0],
            session.address[1],
            packet.seq_send,
            packet.seq_recv,
            packet.cls,
            packet.command,
            packet.player_id,
            packet.status,
            len(wire),
            len(packet.payload),
            packet.payload[:64].hex(" ") or "-",
            detail or "-",
        )

    def _send_sync_packet_now(
        self,
        session: PlayerSession,
        *,
        payload: bytes,
        player_id: int = WIRE_HOST_ID,
        source: str,
        now: float | None = None,
    ) -> int:
        now = time.monotonic() if now is None else now
        sent_seq = session.reliable.next_send.get(CLS_SYNC, 0)
        session.reliable.send(CLS_SYNC, payload=payload, player_id=player_id)
        session.last_sync_packet_sent = now
        if first_scgp(payload) != SCGP_NOP:
            LOG.debug(
                "sent sync tick business packet player=%s name=%r seq=%s source=%s detail=%s",
                session.player_id,
                session.name,
                sent_seq,
                source,
                _scgp_payload_detail(payload),
            )
        return sent_seq

    def _send_sync_nop(
        self,
        session: PlayerSession,
        *,
        player_id: int = WIRE_HOST_ID,
        throttle_interval: float = 0.0,
        now: float | None = None,
    ) -> int | None:
        now = time.monotonic() if now is None else now
        if throttle_interval > 0 and now - session.last_sync_packet_sent < throttle_interval:
            return None
        return self._send_sync_packet_now(
            session,
            payload=nop_payload(),
            player_id=player_id,
            source="NOP",
            now=now,
        )

    def _queue_sync_packet(
        self,
        session: PlayerSession,
        *,
        payload: bytes,
        player_id: int,
        source: str,
    ) -> None:
        session.pending_sync_packets.append((payload, player_id, source))
        LOG.debug(
            "queued sync packet player=%s name=%r source=%s pending=%s detail=%s",
            session.player_id,
            session.name,
            source,
            len(session.pending_sync_packets),
            _scgp_payload_detail(payload),
        )

    def _send_due_sync_tick(
        self,
        session: PlayerSession,
        *,
        fallback_player_id: int = WIRE_HOST_ID,
        throttle_interval: float = LOBBY_SYNC_NOP_INTERVAL,
        now: float | None = None,
    ) -> tuple[int, str] | None:
        now = time.monotonic() if now is None else now
        if throttle_interval > 0 and now - session.last_sync_packet_sent < throttle_interval:
            return None
        if session.pending_sync_packets:
            payload, player_id, source = session.pending_sync_packets.popleft()
            sent_seq = self._send_sync_packet_now(
                session,
                payload=payload,
                player_id=player_id,
                source=source,
                now=now,
            )
            return sent_seq, source
        sent_seq = self._send_sync_packet_now(
            session,
            payload=nop_payload(),
            player_id=fallback_player_id,
            source="NOP",
            now=now,
        )
        return sent_seq, "NOP"

    def _note_join_ping_ack(self, session: PlayerSession, packet: StormPacket, *, source: str) -> None:
        if session.lobby_snapshot_sent:
            return
        target = session.join_ping_ack_seq
        if target is None or not self._seq_reached(packet.seq_recv, target):
            return
        if session.join_ping_ack_seen:
            return
        session.join_ping_ack_seen = True
        LOG.info(
            "join PING acknowledged by %s for player=%s name=%r seq_recv=%s "
            "join_ping_ack_target=%s; waiting for client CONTROL VERIFY before ROOMDATA/UNKNOWNREQUEST",
            source,
            session.player_id,
            session.name,
            packet.seq_recv,
            target,
        )

    def _maybe_send_lobby_snapshot_after_join_ack(
        self,
        session: PlayerSession,
        packet: StormPacket,
        *,
        source: str,
    ) -> None:
        if not session.joined or session.lobby_snapshot_sent:
            return
        target = session.join_ping_ack_seq
        if target is None:
            LOG.debug(
                "waiting to send ROOMDATA/UNKNOWNREQUEST to player=%s name=%r via %s: "
                "join PING ack target is not set seq_recv=%s",
                session.player_id,
                session.name,
                source,
                packet.seq_recv,
            )
            return
        if not session.join_ping_ack_seen:
            LOG.debug(
                "waiting to send ROOMDATA/UNKNOWNREQUEST to player=%s name=%r via %s: "
                "join PING ack has not been seen seq_recv=%s join_ping_ack_target=%s",
                session.player_id,
                session.name,
                source,
                packet.seq_recv,
                target,
            )
            return
        if not self._seq_reached(packet.seq_recv, target):
            LOG.debug(
                "waiting to send ROOMDATA/UNKNOWNREQUEST to player=%s name=%r via %s: "
                "seq_recv=%s has not reached join_ping_ack_target=%s",
                session.player_id,
                session.name,
                source,
                packet.seq_recv,
                target,
            )
            return
        LOG.info(
            "lobby snapshot gate satisfied by %s for player=%s name=%r seq_recv=%s "
            "join_ping_ack_target=%s; sending ROOMDATA/UNKNOWNREQUEST",
            source,
            session.player_id,
            session.name,
            packet.seq_recv,
            target,
        )
        self._send_lobby_snapshot(session)

    def _send_lobby_snapshot(self, session: PlayerSession) -> None:
        if session.lobby_snapshot_sent:
            return
        session.lobby_snapshot_sent = True
        host_player_id = self._host_player_id_for(session)
        session.reliable.send(CLS_ASYNC, payload=roomdata_payload(), player_id=host_player_id)
        session.reliable.send(CLS_ASYNC, payload=bytes([SCGP_UNKNOWNREQUEST]), player_id=host_player_id)
        LOG.info("sent initial ROOMDATA/UNKNOWNREQUEST prompt to player=%s name=%r", session.player_id, session.name)

    def _broadcast_player_join(self, joined_session: PlayerSession) -> None:
        for session in self.sessions.values():
            if not session.joined:
                continue
            session.reliable.send(
                CLS_ASYNC,
                payload=playerjoin_payload(joined_session.player_id),
                player_id=self._host_player_id_for(session),
            )

    def _broadcast_slot_state(self) -> None:
        for session in self.sessions.values():
            if session.joined:
                self._send_slot_state(session, source="broadcast")

    def _send_slot_state(
        self,
        session: PlayerSession,
        *,
        source: str,
        include_map_percent: bool = True,
    ) -> None:
        payload = self._slot_sync_payload_for(session, include_map_percent=include_map_percent)
        self._queue_sync_packet(
            session,
            payload=payload,
            player_id=self._host_player_id_for(session),
            source=f"{source} SLOTUPDATE snapshot",
        )
        LOG.info(
            "queued %s SLOTUPDATE snapshot to player=%s name=%r include_map_percent=%s detail=%s",
            source,
            session.player_id,
            session.name,
            include_map_percent,
            _scgp_payload_detail(payload),
        )

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
            session.reliable.send(
                CLS_ASYNC,
                payload=map_info_payload(self.map_name),
                player_id=self._host_player_id_for(session),
            )
            LOG.info("sent PLAYERJOIN/MAP info to player=%s name=%r", session.player_id, session.name)

    def _slot_sync_payload_for(self, session: PlayerSession, *, include_map_percent: bool = True) -> bytes:
        slot0 = self._game_joined_session_for_slot(0)
        slot1 = self._game_joined_session_for_slot(1)
        return slot_sync_payload(
            slot0.race if slot0 else 6,
            slot1.race if slot1 else 6,
            player0_id=self._player_id_for_slot(0),
            player1_id=self._player_id_for_slot(1),
            player0_active=self._slot_active(0),
            player1_active=self._slot_active(1),
            include_map_percent=include_map_percent,
        )

    def _send_to_others(self, sender: PlayerSession, cls: int, *, payload: bytes) -> None:
        for session in self.sessions.values():
            if session is sender or not session.joined:
                continue
            self._trace_game_packet("tx", sender, target=session, cls=cls, payload=payload)
            session.reliable.send(cls, payload=payload, player_id=sender.player_id)

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

    def _game_joined_session_for_slot(self, slot_index: int) -> PlayerSession | None:
        return next(
            (
                session
                for session in self.sessions.values()
                if session.joined and session.lobby_ready and session.slot_index == slot_index
            ),
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
                    player_id=self._host_player_id_for(session),
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
                self._send_sync_packet_now(
                    session,
                    payload=startgame_payload(),
                    player_id=self._host_player_id_for(session),
                    source="STARTGAME",
                )
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
                host_player_id = self._host_player_id_for(session)
                self._send_sync_packet_now(
                    session,
                    payload=seed,
                    player_id=host_player_id,
                    source="SEED",
                )
                session.reliable.send(CLS_ASYNC, payload=map_complete_payload(), player_id=host_player_id)
                LOG.info("MAP complete to player=%s after SEED", session.player_id)
        self.starting = False
        self.started = True
        self._auto_start_task = None
        LOG.info("game started")

    async def _start_transition_nop_loop(self) -> None:
        try:
            while self.starting and not self.closed:
                await asyncio.sleep(START_TRANSITION_SYNC_INTERVAL)
                for session in list(self.sessions.values()):
                    if session.joined:
                        self._send_due_sync_tick(
                            session,
                            fallback_player_id=self._host_player_id_for(session),
                            throttle_interval=START_TRANSITION_SYNC_INTERVAL,
                        )
        except asyncio.CancelledError:
            return

    async def _keepalive_loop(self) -> None:
        while not self.closed:
            await asyncio.sleep(LOBBY_SYNC_NOP_INTERVAL)
            if self.starting or self.started:
                continue
            for session in list(self.sessions.values()):
                if not session.joined:
                    continue
                self._send_due_sync_tick(
                    session,
                    fallback_player_id=self._host_player_id_for(session),
                    throttle_interval=LOBBY_SYNC_NOP_INTERVAL,
                )

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
            addr = (address, self.discovery_port)
            self.discovery_transport.sendto(packet, addr)
            _log_protocol_lan("tx", packet, addr, event="room_advertisement_broadcast")

    def _disconnect(self, session: PlayerSession, reason: str) -> None:
        LOG.info("disconnect player=%s reason=%s", session.player_id, reason)
        for other in self.sessions.values():
            if other is not session and other.joined:
                other.reliable.send(CLS_CONTROL, command=CMD_QUIT, payload=quit_payload())
        self.sessions.pop(session.address, None)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Experimental StarCraft LAN MainHost/SubHost relay")
    parser.add_argument("--bind", default="0.0.0.0")
    parser.add_argument("--discovery-port", type=int, default=6111)
    parser.add_argument("--storm-port", type=int, default=6112)
    parser.add_argument("--main-host-name", default=DEFAULT_MAIN_HOST_NAME)
    parser.add_argument("--sub-host-name", default=DEFAULT_SUB_HOST_NAME)
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
        main_host_name=args.main_host_name,
        sub_host_name=args.sub_host_name,
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
