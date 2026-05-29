from __future__ import annotations

from dataclasses import dataclass
import socket
import struct
import time
from typing import Sequence

from .checksum import PacketError, unwrap_body, wrap_body

PRODUCT_SEXP = b"PXES"
VERSION_CODE = 0xC3

CLS_CONTROL = 0
CLS_ASYNC = 1
CLS_SYNC = 2

STATUS_NORMAL = 0
STATUS_VERIFY = 1
STATUS_RESEND_REQUEST = 2
STATUS_RESEND_RESPONSE = 3

CMD_REQUESTJOIN = 0x01
CMD_REQUESTJOINOK = 0x02
CMD_REQUESTJOIN2 = 0x03
CMD_PING = 0x04
CMD_PONG = 0x05
CMD_PLAYER = 0x06
CMD_ENTER = 0x07
CMD_GAMEDATA = 0x08
CMD_GAMETYPE = 0x09
CMD_JOINFAIL = 0x0A
CMD_QUIT = 0x0B
CMD_GAMESTATE = 0x0E
CMD_STATSCODE = 0x0F

SCGP_NOP = 0x05
SCGP_SELECT = 0x09
SCGP_RIGHT_CLICK = 0x14
SCGP_TRAIN = 0x1F
SCGP_SYNC = 0x37
SCGP_STARTGAME = 0x3C
SCGP_MAPPERCENT = 0x3D
SCGP_SLOTUPDATE = 0x3E
SCGP_NEWNETPLAYER = 0x3F
SCGP_JOINEDGAME = 0x40
SCGP_CHANGERACE = 0x41
SCGP_SEED = 0x48
SCGP_PLAYERJOIN = 0x49
SCGP_ROOMDATA = 0x4A
SCGP_FORCENAMES = 0x4B
SCGP_LOBBYCHAT = 0x4C
SCGP_REJECT = 0x4E
SCGP_MAP = 0x4F
SCGP_UNKNOWNREQUEST = 0x50

DEFAULT_HOST_NAME = "Sun"
DEFAULT_ROOM_NAME = "Challenger"
DEFAULT_MAP_FILE_NAME = "(2)Challenger.scm"
DEFAULT_MAP_SIZE = 0x0000D801
DEFAULT_MAP_CHECKSUM = 0x0947F543


@dataclass(frozen=True)
class LanPacket:
    kind: int
    product: bytes
    version: int
    state: int
    payload: bytes = b""

    @classmethod
    def from_wire(cls, packet: bytes) -> "LanPacket":
        body = unwrap_body(packet)
        if len(body) < 16:
            raise PacketError("LAN body is too short")
        kind, product, version, state = struct.unpack_from("<I4sII", body, 0)
        return cls(kind=kind, product=product, version=version, state=state, payload=body[16:])

    def to_wire(self) -> bytes:
        return wrap_body(struct.pack("<I4sII", self.kind, self.product, self.version, self.state) + self.payload)


@dataclass(frozen=True)
class RoomAdvertisement:
    host_name: str
    stat_string: str
    game_type: int = 12
    current_players: int = 0
    max_players: int = 2
    state: int = 0

    def to_wire(self) -> bytes:
        payload = (
            self.host_name.encode("latin1", "replace")
            + b"\0"
            + self.stat_string.encode("latin1", "replace")
            + b"\0"
            + struct.pack("<III", self.game_type, self.current_players, self.max_players)
        )
        return LanPacket(0, PRODUCT_SEXP, VERSION_CODE, self.state, payload).to_wire()


@dataclass(frozen=True)
class StormPacket:
    seq_send: int
    seq_recv: int
    cls: int
    command: int
    player_id: int
    status: int
    payload: bytes = b""

    @classmethod
    def from_wire(cls, packet: bytes) -> "StormPacket":
        body = unwrap_body(packet)
        if len(body) < 8:
            raise PacketError("Storm body is too short")
        seq_send, seq_recv, packet_cls, command, player_id, status = struct.unpack_from("<HHBBBB", body, 0)
        return cls(seq_send, seq_recv, packet_cls, command, player_id, status, body[8:])

    def to_wire(self) -> bytes:
        body = struct.pack(
            "<HHBBBB",
            self.seq_send & 0xFFFF,
            self.seq_recv & 0xFFFF,
            self.cls & 0xFF,
            self.command & 0xFF,
            self.player_id & 0xFF,
            self.status & 0xFF,
        ) + self.payload
        return wrap_body(body)


def make_stat_string(host_name: str = DEFAULT_HOST_NAME, room_name: str = DEFAULT_ROOM_NAME) -> str:
    return f",34,12,3,1,f,1,d36c842f,,,{host_name}\r{room_name}\r"


def c_string(data: bytes) -> str:
    return data.split(b"\0", 1)[0].decode("latin1", "replace")


def parse_enter_payload(payload: bytes) -> str:
    return c_string(payload) or "Player"


def _roomdata_bytes(values: Sequence[int] | None, fallback: Sequence[int], length: int) -> bytes:
    source = fallback if values is None else values
    if len(source) != length:
        raise ValueError(f"ROOMDATA field must contain {length} values, got {len(source)}")
    return bytes(value & 0xFF for value in source)


def roomdata_payload(
    slot0_force: int = 1,
    slot1_force: int = 1,
    *,
    tileset: int = 0,
    width: int = 0x60,
    height: int = 0x80,
    ownr: Sequence[int] | None = None,
    side: Sequence[int] | None = None,
    ownr_default: Sequence[int] | None = None,
    forc: Sequence[int] | None = None,
    forc_flags: Sequence[int] | None = None,
    race: Sequence[int] | None = None,
) -> bytes:
    default_ownr = (6, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    default_side = (6, 6, 2, 1, 0, 2, 1, 0, 0, 0, 0, 0)
    default_ownr_default = (6, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    default_forc = (slot0_force & 0xFF, slot1_force & 0xFF, 0, 0, 0, 0, 0, 0)
    default_forc_flags = (1, 0, 0, 0)
    default_race = (1, 1, 0, 0, 0, 0, 0, 0)
    return (
        bytes([SCGP_ROOMDATA])
        + struct.pack("<HHH", tileset & 0xFFFF, width & 0xFFFF, height & 0xFFFF)
        + _roomdata_bytes(ownr, default_ownr, 12)
        + _roomdata_bytes(side, default_side, 12)
        + _roomdata_bytes(ownr_default, default_ownr_default, 12)
        + _roomdata_bytes(forc, default_forc, 8)
        + _roomdata_bytes(forc_flags, default_forc_flags, 4)
        + _roomdata_bytes(race, default_race, 8)
    )


def forcenames_payload(names: tuple[str, str, str, str] = ("Team 1", "Team 2", "Team 3", "Team 4")) -> bytes:
    payload = bytearray([SCGP_FORCENAMES])
    for name in names:
        encoded = name.encode("latin1", "replace")[:29]
        payload.extend(encoded)
        payload.extend(b"\0" * (30 - len(encoded)))
    return bytes(payload)


def gametype_payload() -> bytes:
    return bytes.fromhex(
        "0f 00 01 00 01 00 00 00"
        " 01 01 01 02 02 00 01 01"
        " 00 01 00 00 00 00 00 32"
        " 00 00 00 00 00 00 00 00"
    )


def gamedata_payload(
    player_id: int,
    host_name: str,
    stat_string: str,
    max_players: int = 2,
    *,
    command2_packet_count: int = 0x66,
    unknown: int = 0x06,
    game_uptime_seconds: int = 0,
) -> bytes:
    return (
        struct.pack(
            "<IIIII",
            player_id,
            max_players,
            command2_packet_count,
            unknown,
            game_uptime_seconds,
        )
        + host_name.encode("latin1", "replace")
        + b"\0"
        + stat_string.encode("latin1", "replace")
        + b"\0\0"
    )


def player_record_payload(
    player_id: int,
    name: str,
    *,
    is_host: bool = True,
    address: tuple[str, int] | None = None,
    command2_packet_count: int = 0x66,
) -> bytes:
    encoded_name = name.encode("latin1", "replace")
    size = 36 + len(encoded_name) + 2
    # The first dword is the total PLAYER payload size. Captures show 0x29 for
    # "Sun" and 0x2a for "SunX", so it must track the encoded name length.
    if address is None:
        peer_flag = 0
        sockaddr = b"\0" * 16
    else:
        peer_flag = 4
        sockaddr = (
            struct.pack("<H", socket.AF_INET)
            + struct.pack("!H", address[1])
            + socket.inet_aton(address[0])
            + b"\0" * 8
        )
    fixed = struct.pack(
        "<IIIII",
        size,
        player_id,
        1 if is_host else 0,
        peer_flag,
        command2_packet_count,
    ) + sockaddr
    return fixed + encoded_name + b"\0\0"


def playerjoin_payload(player_id: int) -> bytes:
    return bytes([SCGP_PLAYERJOIN]) + struct.pack("<I", player_id)


def map_info_payload(
    map_name: str = DEFAULT_MAP_FILE_NAME,
    *,
    map_size: int = DEFAULT_MAP_SIZE,
    map_checksum: int = DEFAULT_MAP_CHECKSUM,
) -> bytes:
    name = map_name.encode("latin1", "replace") + b"\0"
    event_body = struct.pack("<II", map_size & 0xFFFFFFFF, map_checksum & 0xFFFFFFFF) + name
    return bytes([SCGP_MAP]) + struct.pack("<HH", len(event_body) + 2, 0x0001) + event_body


def map_complete_payload() -> bytes:
    return bytes([SCGP_MAP]) + struct.pack("<HH", 2, 0x0003)


def lobbychat_payload(message: str) -> bytes:
    return bytes([SCGP_LOBBYCHAT]) + message.encode("latin1", "replace") + b"\0"


def map_percent_payload(percent: int = 100) -> bytes:
    return bytes([SCGP_MAPPERCENT, max(0, min(100, percent))])


def slot_update(slot: int, player: int, state: int, race: int, team: int) -> bytes:
    return bytes([SCGP_SLOTUPDATE, slot & 0xFF, player & 0xFF, state & 0xFF, race & 0xFF, team & 0xFF])


def new_net_player(player_id: int) -> bytes:
    return struct.pack("<BBHHH", SCGP_NEWNETPLAYER, player_id & 0xFF, 0, 1, 5)


def slot_sync_payload(
    player0_race: int = 6,
    player1_race: int = 6,
    *,
    player0_id: int = 1,
    player1_id: int = 2,
    player0_active: bool = True,
    player1_active: bool = True,
    player0_team: int = 1,
    player1_team: int = 2,
    include_map_percent: bool = True,
    include_virtual_host: bool = False,
    slot_entries: Sequence[tuple[int, int, int, int, int]] | None = None,
    net_player_ids: Sequence[int] | None = None,
) -> bytes:
    if slot_entries is not None:
        parts = [slot_update(slot, player, state, race, team) for slot, player, state, race, team in slot_entries]
        parts.extend(new_net_player(player_id) for player_id in (net_player_ids or ()))
        if include_virtual_host:
            parts.append(new_net_player(0))
        if include_map_percent:
            parts.insert(0, map_percent_payload(100))
        return b"".join(parts)

    slot0_player = player0_id if player0_active else 0xFF
    slot1_player = player1_id if player1_active else 0xFF
    slot0_state = 2 if player0_active else 6
    slot1_state = 2 if player1_active else 6
    net_players = []
    if player1_active:
        net_players.append(new_net_player(player1_id))
    if player0_active:
        net_players.append(new_net_player(player0_id))
    if include_virtual_host:
        net_players.append(new_net_player(0))
    parts = [
            slot_update(7, 0xFF, 0, 0, 0),
            slot_update(6, 0xFF, 0, 1, 0),
            slot_update(5, 0xFF, 0, 2, 0),
            slot_update(4, 0xFF, 0, 0, 0),
            slot_update(3, 0xFF, 0, 1, 0),
            slot_update(2, 0xFF, 0, 2, 0),
            slot_update(1, slot1_player, slot1_state, player1_race, player1_team),
            slot_update(0, slot0_player, slot0_state, player0_race, player0_team),
            *net_players,
    ]
    if include_map_percent:
        parts.insert(0, map_percent_payload(100))
    return b"".join(parts)


def startgame_payload() -> bytes:
    return bytes([SCGP_STARTGAME])


def game_state_payload() -> bytes:
    return struct.pack("<I", CMD_GAMESTATE)


def seed_payload(seed: int | None = None) -> bytes:
    if seed is None:
        seed = int(time.time())
    return bytes([SCGP_SEED]) + struct.pack("<I", seed & 0xFFFFFFFF) + (b"\x08" * 8)


def nop_payload() -> bytes:
    return bytes([SCGP_NOP])


def quit_payload() -> bytes:
    return struct.pack("<II", 0x00000282, 0x40000001)


def first_scgp(payload: bytes) -> int | None:
    return payload[0] if payload else None
