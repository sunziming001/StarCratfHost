from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Deque

from .checksum import PacketError
from .protocol import CLS_ASYNC, CLS_CONTROL, CLS_SYNC, STATUS_NORMAL, STATUS_VERIFY, StormPacket
from .protocol_debug import _log_protocol_storm

Address = tuple[str, int]


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
        _log_protocol_storm("tx", packet, self.address, event="send")
        self._store(cls, seq_send, wire)
        if status == STATUS_NORMAL:
            self.next_send[cls] = (seq_send + 1) & 0xFFFF
        return wire

    def send_at_seq(
        self,
        cls: int,
        seq_send: int,
        *,
        command: int = 0,
        payload: bytes = b"",
        player_id: int = 0,
        status: int = STATUS_NORMAL,
    ) -> bytes:
        seq_recv = self.last_recv.get(cls, 0)
        packet = StormPacket(seq_send, seq_recv, cls, command, player_id, status, payload)
        wire = packet.to_wire()
        self.transport.sendto(wire, self.address)
        _log_protocol_storm("tx", packet, self.address, event="send")
        self._store(cls, seq_send, wire)
        if status == STATUS_NORMAL:
            self.next_send[cls] = (seq_send + 1) & 0xFFFF
        return wire

    def resend(self, cls: int, seq: int) -> bool:
        wire = self.history.get(cls, {}).get(seq)
        if wire is None:
            return False
        self.transport.sendto(wire, self.address)
        try:
            packet = StormPacket.from_wire(wire)
        except PacketError:
            return True
        _log_protocol_storm("tx", packet, self.address, event="resend")
        return True

    def send_verify(self, cls: int, *, player_id: int = 0) -> bytes:
        seq = self.last_recv.get(cls, 0)
        packet = StormPacket(seq, seq, cls, 0, player_id, STATUS_VERIFY)
        wire = packet.to_wire()
        self.transport.sendto(wire, self.address)
        _log_protocol_storm("tx", packet, self.address, event="verify")
        return wire

    def _store(self, cls: int, seq: int, wire: bytes) -> None:
        history = self.history.setdefault(cls, {})
        order = self.history_order.setdefault(cls, deque())
        history[seq] = wire
        order.append(seq)
        while len(order) > 256:
            old = order.popleft()
            history.pop(old, None)
