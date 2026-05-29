from __future__ import annotations

import logging
import struct

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
    LanPacket,
    SCGP_CHANGERACE,
    SCGP_FORCENAMES,
    SCGP_JOINEDGAME,
    SCGP_LOBBYCHAT,
    SCGP_MAP,
    SCGP_MAPPERCENT,
    SCGP_NEWNETPLAYER,
    SCGP_NOP,
    SCGP_PLAYERJOIN,
    SCGP_REJECT,
    SCGP_RIGHT_CLICK,
    SCGP_ROOMDATA,
    SCGP_SELECT,
    SCGP_SEED,
    SCGP_SLOTUPDATE,
    SCGP_STARTGAME,
    SCGP_SYNC,
    SCGP_TRAIN,
    SCGP_UNKNOWNREQUEST,
    STATUS_NORMAL,
    STATUS_RESEND_REQUEST,
    STATUS_RESEND_RESPONSE,
    STATUS_VERIFY,
    StormPacket,
    c_string,
)

Address = tuple[str, int]

LOG = logging.getLogger("sc_host")

CLS_NAMES = {
    CLS_CONTROL: "CONTROL",
    CLS_ASYNC: "ASYNC",
    CLS_SYNC: "SYNC",
}
STATUS_NAMES = {
    STATUS_NORMAL: "NORMAL",
    STATUS_VERIFY: "VERIFY",
    STATUS_RESEND_REQUEST: "RESEND_REQUEST",
    STATUS_RESEND_RESPONSE: "RESEND_RESPONSE",
}
CONTROL_COMMAND_NAMES = {
    CMD_REQUESTJOIN: "REQUESTJOIN",
    CMD_REQUESTJOINOK: "REQUESTJOINOK",
    CMD_REQUESTJOIN2: "REQUESTJOIN2",
    CMD_PING: "PING",
    CMD_PONG: "PONG",
    CMD_PLAYER: "PLAYER",
    CMD_ENTER: "ENTER",
    CMD_GAMEDATA: "GAMEDATA",
    CMD_GAMETYPE: "GAMETYPE",
    CMD_JOINFAIL: "JOINFAIL",
    CMD_QUIT: "QUIT",
    CMD_GAMESTATE: "GAMESTATE",
    CMD_STATSCODE: "STATSCODE",
}
SCGP_NAMES = {
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
    SCGP_FORCENAMES: "FORCENAMES",
    SCGP_LOBBYCHAT: "LOBBYCHAT",
    SCGP_MAP: "MAP",
    SCGP_REJECT: "REJECT",
    SCGP_UNKNOWNREQUEST: "UNKNOWNREQUEST",
}
GAME_TRACE_SCGP_NAMES = SCGP_NAMES


def _hex_head(data: bytes, limit: int = 64) -> str:
    return data[:limit].hex(" ") or "-"


def _name(names: dict[int, str], value: int, prefix: str) -> str:
    known = names.get(value)
    if known is not None:
        return known
    return f"{prefix}_0x{value:02x}"


def _read_cstring(payload: bytes, offset: int) -> tuple[str, int]:
    if offset >= len(payload):
        return "", offset
    end = payload.find(b"\0", offset)
    if end < 0:
        return payload[offset:].decode("latin1", "replace"), len(payload)
    return payload[offset:end].decode("latin1", "replace"), end + 1


def _lan_payload_detail(packet: LanPacket) -> str:
    if packet.kind != 0 or not packet.payload:
        return "-"
    host_name, offset = _read_cstring(packet.payload, 0)
    stat_string, offset = _read_cstring(packet.payload, offset)
    if len(packet.payload) >= offset + 12:
        game_type, current_players, max_players = struct.unpack_from("<III", packet.payload, offset)
        return (
            f"host_name={host_name!r} stat_string={stat_string!r} game_type={game_type} "
            f"current_players={current_players} max_players={max_players}"
        )
    return f"host_name={host_name!r} stat_string={stat_string!r}"


def _log_protocol_lan(direction: str, data: bytes, addr: Address, *, event: str) -> None:
    try:
        packet = LanPacket.from_wire(data)
    except PacketError as exc:
        LOG.debug(
            "PROTOCOL %s LAN event=%s addr=%s:%s decode_error=%s wire_len=%s",
            direction,
            event,
            addr[0],
            addr[1],
            exc,
            len(data),
        )
        return
    product = packet.product.decode("latin1", "replace")
    LOG.debug(
        "PROTOCOL %s LAN event=%s addr=%s:%s kind=%s product=%s version=0x%02x state=0x%08x "
        "payload_len=%s payload_head=%s detail=%s",
        direction,
        event,
        addr[0],
        addr[1],
        packet.kind,
        product,
        packet.version,
        packet.state,
        len(packet.payload),
        _hex_head(packet.payload),
        _lan_payload_detail(packet),
    )


def _control_payload_detail(command: int, payload: bytes) -> str:
    if command == CMD_REQUESTJOINOK and len(payload) >= 4:
        return f"value={struct.unpack_from('<I', payload, 0)[0]}"
    if command == CMD_ENTER:
        return f"name={c_string(payload)!r}"
    if command == CMD_GAMEDATA and len(payload) >= 20:
        player_id, max_players, command2_count, unknown, uptime_seconds = struct.unpack_from("<IIIII", payload, 0)
        host_name, offset = _read_cstring(payload, 20)
        stat_string, _offset = _read_cstring(payload, offset)
        return (
            f"assigned_player_id={player_id} max_players={max_players} "
            f"command2_packet_count={command2_count} unknown={unknown} "
            f"game_uptime_seconds={uptime_seconds} host_name={host_name!r} stat_string={stat_string!r}"
        )
    if command == CMD_PLAYER and len(payload) >= 36:
        size, player_id, is_host = struct.unpack_from("<III", payload, 0)
        name, _offset = _read_cstring(payload, 36)
        return f"record_size={size} record_player_id={player_id} is_host={is_host} name={name!r}"
    if command == CMD_GAMETYPE:
        word_count = min(8, len(payload) // 2)
        words = struct.unpack_from("<" + "H" * word_count, payload, 0) if word_count else ()
        return f"word_count={len(payload) // 2} first_words={list(words)}"
    if command == CMD_STATSCODE and len(payload) >= 4:
        return f"stat_code={struct.unpack_from('<I', payload, 0)[0]}"
    if command == CMD_GAMESTATE and len(payload) >= 4:
        return f"value=0x{struct.unpack_from('<I', payload, 0)[0]:08x}"
    if command == CMD_QUIT and len(payload) >= 8:
        reason0, reason1 = struct.unpack_from("<II", payload, 0)
        return f"reason0=0x{reason0:08x} reason1=0x{reason1:08x}"
    if command in (CMD_REQUESTJOIN, CMD_REQUESTJOIN2, CMD_PING, CMD_PONG, CMD_JOINFAIL):
        return "empty" if not payload else f"payload_len={len(payload)}"
    return f"payload_len={len(payload)}"


def _map_detail(payload: bytes, offset: int, packet_len: int, length: int, kind: int) -> str:
    detail = f"length={length} kind=0x{kind:04x}"
    if kind == 0x0001 and packet_len >= 13:
        map_size, checksum = struct.unpack_from("<II", payload, offset + 5)
        map_name, _offset = _read_cstring(payload, offset + 13)
        detail += f" map_size={map_size} map_checksum=0x{checksum:08x} map_name={map_name!r}"
    return detail


def _scgp_payload_detail(payload: bytes) -> str:
    if not payload:
        return "scgp=[]"
    entries: list[str] = []
    offset = 0
    truncated = False
    while offset < len(payload):
        if len(entries) >= 24:
            truncated = True
            break
        packet_offset = offset
        scgp = payload[offset]
        name = _name(SCGP_NAMES, scgp, "SCGP")
        remain = len(payload) - offset
        if scgp == SCGP_NOP:
            entries.append(f"{packet_offset}:{name}")
            offset += 1
        elif scgp == SCGP_MAPPERCENT and remain >= 2:
            entries.append(f"{packet_offset}:{name}(percent={payload[offset + 1]})")
            offset += 2
        elif scgp == SCGP_SLOTUPDATE and remain >= 6:
            slot, player, state, race, team = payload[offset + 1 : offset + 6]
            entries.append(f"{packet_offset}:{name}(slot={slot} player={player} state={state} race={race} team={team})")
            offset += 6
        elif scgp == SCGP_NEWNETPLAYER and remain >= 8:
            _id, player_id, unknown0, unknown1, unknown2 = struct.unpack_from("<BBHHH", payload, offset)
            entries.append(
                f"{packet_offset}:{name}(player_id={player_id} unknowns={unknown0},{unknown1},{unknown2})"
            )
            offset += 8
        elif scgp == SCGP_JOINEDGAME:
            entries.append(f"{packet_offset}:{name}")
            offset += 1
        elif scgp == SCGP_CHANGERACE and remain >= 3:
            entries.append(f"{packet_offset}:{name}(byte1={payload[offset + 1]} race={payload[offset + 2]})")
            offset += 3
        elif scgp == SCGP_STARTGAME:
            entries.append(f"{packet_offset}:{name}")
            offset += 1
        elif scgp == SCGP_SEED and remain >= 5:
            seed = struct.unpack_from("<I", payload, offset + 1)[0]
            packet_len = min(remain, 13)
            entries.append(f"{packet_offset}:{name}(seed=0x{seed:08x} extra_len={packet_len - 5})")
            offset += packet_len
        elif scgp == SCGP_PLAYERJOIN and remain >= 5:
            player_id = struct.unpack_from("<I", payload, offset + 1)[0]
            entries.append(f"{packet_offset}:{name}(player_id={player_id})")
            offset += 5
        elif scgp == SCGP_ROOMDATA:
            entries.append(f"{packet_offset}:{name}(len={remain})")
            break
        elif scgp == SCGP_FORCENAMES and remain >= 121:
            names = []
            pos = offset + 1
            for _index in range(4):
                names.append(payload[pos : pos + 30].split(b"\0", 1)[0].decode("latin1", "replace"))
                pos += 30
            entries.append(f"{packet_offset}:{name}(names={names!r})")
            offset += 121
        elif scgp == SCGP_LOBBYCHAT:
            message, next_offset = _read_cstring(payload, offset + 1)
            entries.append(f"{packet_offset}:{name}(message={message!r})")
            offset = next_offset if next_offset > offset + 1 else len(payload)
        elif scgp == SCGP_REJECT:
            entries.append(f"{packet_offset}:{name}(len={remain})")
            break
        elif scgp == SCGP_MAP and remain >= 5:
            length, kind = struct.unpack_from("<HH", payload, offset + 1)
            packet_len = 3 + length if length > 0 and 3 + length <= remain else remain
            entries.append(f"{packet_offset}:{name}({_map_detail(payload, offset, packet_len, length, kind)})")
            offset += packet_len
        elif scgp == SCGP_UNKNOWNREQUEST:
            entries.append(f"{packet_offset}:{name}")
            offset += 1
        elif scgp in (SCGP_SELECT, SCGP_RIGHT_CLICK, SCGP_TRAIN, SCGP_SYNC):
            entries.append(f"{packet_offset}:{name}(len={remain} head={_hex_head(payload[offset:], 32)})")
            break
        else:
            entries.append(f"{packet_offset}:{name}(len={remain} head={_hex_head(payload[offset:], 32)})")
            break
    suffix = f" remaining={len(payload) - offset}" if truncated else ""
    return "scgp=[" + "; ".join(entries) + "]" + suffix


def _storm_payload_detail(packet: StormPacket) -> str:
    if packet.cls == CLS_CONTROL:
        return _control_payload_detail(packet.command, packet.payload)
    if packet.cls in (CLS_ASYNC, CLS_SYNC):
        return _scgp_payload_detail(packet.payload)
    return f"payload_len={len(packet.payload)}"


def _log_protocol_storm(direction: str, packet: StormPacket, addr: Address, *, event: str) -> None:
    cls_name = _name(CLS_NAMES, packet.cls, "CLS")
    status_name = _name(STATUS_NAMES, packet.status, "STATUS")
    command_name = _name(CONTROL_COMMAND_NAMES, packet.command, "CMD") if packet.cls == CLS_CONTROL else f"0x{packet.command:02x}"
    LOG.debug(
        "PROTOCOL %s PKT_STORM event=%s addr=%s:%s seq_send=%s seq_recv=%s cls=%s(%s) "
        "command=%s(0x%02x) storm_pid=%s status=%s(%s) payload_len=%s payload_head=%s detail=%s",
        direction,
        event,
        addr[0],
        addr[1],
        packet.seq_send,
        packet.seq_recv,
        packet.cls,
        cls_name,
        command_name,
        packet.command,
        packet.player_id,
        packet.status,
        status_name,
        len(packet.payload),
        _hex_head(packet.payload),
        _storm_payload_detail(packet),
    )
