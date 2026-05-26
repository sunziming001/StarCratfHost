from __future__ import annotations

from dataclasses import dataclass
import collections
import struct


@dataclass(frozen=True)
class UdpPacket:
    frame_no: int
    timestamp: int
    src_ip: str
    src_port: int
    dst_ip: str
    dst_port: int
    payload: bytes


def _ip4(data: bytes) -> str:
    return ".".join(str(x) for x in data)


def _extract_ip(packet: bytes, link_type: int) -> bytes | None:
    if link_type == 0:
        return packet[4:] if len(packet) >= 5 else None
    if link_type == 1:
        if len(packet) < 14:
            return None
        ether_type = struct.unpack("!H", packet[12:14])[0]
        offset = 14
        if ether_type == 0x8100 and len(packet) >= 18:
            ether_type = struct.unpack("!H", packet[16:18])[0]
            offset = 18
        return packet[offset:] if ether_type == 0x0800 else None
    if link_type == 101:
        return packet
    return None


def read_udp_packets(path: str) -> list[UdpPacket]:
    with open(path, "rb") as fp:
        raw = fp.read()
    offset = 0
    endian = "<"
    interfaces: list[tuple[int, int, int]] = []
    packets: list[tuple[int, int, bytes]] = []

    while offset + 12 <= len(raw):
        block_type = struct.unpack_from("<I", raw, offset)[0]
        block_len = struct.unpack_from("<I", raw, offset + 4)[0]
        if not (12 <= block_len <= len(raw) - offset):
            block_len = struct.unpack_from(">I", raw, offset + 4)[0]
        if not (12 <= block_len <= len(raw) - offset):
            break

        body = raw[offset + 8 : offset + block_len - 4]
        if block_type == 0x0A0D0D0A and len(body) >= 4:
            magic = struct.unpack_from("<I", body, 0)[0]
            endian = "<" if magic == 0x1A2B3C4D else ">"
        elif block_type == 1 and len(body) >= 8:
            interfaces.append(struct.unpack_from(endian + "HHI", body, 0))
        elif block_type == 6 and len(body) >= 20:
            iface, ts_hi, ts_lo, cap_len, _orig_len = struct.unpack_from(endian + "IIIII", body, 0)
            packets.append((iface, (ts_hi << 32) + ts_lo, body[20 : 20 + cap_len]))

        offset += block_len

    udp_packets: list[UdpPacket] = []
    for frame_no, (iface, timestamp, packet) in enumerate(packets, 1):
        if iface >= len(interfaces):
            continue
        ip = _extract_ip(packet, interfaces[iface][0])
        if not ip or len(ip) < 20 or (ip[0] >> 4) != 4:
            continue
        ihl = (ip[0] & 0x0F) * 4
        total = struct.unpack("!H", ip[2:4])[0]
        if ip[9] != 17:
            continue
        udp = ip[ihl:total]
        if len(udp) < 8:
            continue
        src_port, dst_port, udp_len, _udp_checksum = struct.unpack("!HHHH", udp[:8])
        udp_packets.append(
            UdpPacket(
                frame_no=frame_no,
                timestamp=timestamp,
                src_ip=_ip4(ip[12:16]),
                src_port=src_port,
                dst_ip=_ip4(ip[16:20]),
                dst_port=dst_port,
                payload=udp[8:udp_len],
            )
        )
    return udp_packets


def udp_flow_counts(packets: list[UdpPacket]) -> collections.Counter[tuple[str, int, str, int]]:
    counts: collections.Counter[tuple[str, int, str, int]] = collections.Counter()
    for packet in packets:
        counts[(packet.src_ip, packet.src_port, packet.dst_ip, packet.dst_port)] += 1
    return counts
