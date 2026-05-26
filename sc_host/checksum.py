from __future__ import annotations

import struct


class PacketError(ValueError):
    """Raised when a StarCraft/Battle.net UDP packet is malformed."""


def checksum(data: bytes, start: int = 2) -> int:
    """Return the 16-bit Blizzard UDP checksum used by PKT_STORM/LAN packets."""
    a = 0
    b = 0
    for value in reversed(data[start:]):
        b += value
        if b > 0xFF:
            b -= 0xFF
        a += b

    c = ((b << 8) | (a % 0xFF)) & 0xFFFF
    a2 = 0xFF - (((c & 0xFF) + (c >> 8)) % 0xFF)
    b2 = a2 + (c >> 8)
    b2 = 0xFF - (b2 % 0xFF)
    return (b2 | (a2 << 8)) & 0xFFFF


def wrap_body(body: bytes) -> bytes:
    """Wrap a packet body with checksum and length fields."""
    length = len(body) + 4
    if length > 0xFFFF:
        raise PacketError(f"packet too large: {length} bytes")
    packet = bytearray(4 + len(body))
    struct.pack_into("<H", packet, 2, length)
    packet[4:] = body
    struct.pack_into("<H", packet, 0, checksum(packet))
    return bytes(packet)


def unwrap_body(packet: bytes, *, validate: bool = True) -> bytes:
    """Validate and strip checksum/length fields."""
    if len(packet) < 4:
        raise PacketError("packet is shorter than checksum/length header")
    expected_len = struct.unpack_from("<H", packet, 2)[0]
    if expected_len != len(packet):
        raise PacketError(f"length mismatch: header={expected_len} actual={len(packet)}")
    if validate:
        got = struct.unpack_from("<H", packet, 0)[0]
        computed = checksum(packet)
        if got != computed:
            raise PacketError(f"checksum mismatch: got=0x{got:04x} computed=0x{computed:04x}")
    return packet[4:]


def is_valid_packet(packet: bytes) -> bool:
    try:
        unwrap_body(packet)
    except PacketError:
        return False
    return True
