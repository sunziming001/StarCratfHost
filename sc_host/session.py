from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque

from .reliable import Address, ReliableState


@dataclass
class PlayerSession:
    reliable: ReliableState
    name: str = "Player"
    slot_index: int | None = None
    joined: bool = False
    lobby_snapshot_sent: bool = False
    lobby_ready: bool = False
    map_bootstrap_sent: bool = False
    post_map_slot_state_sent: bool = False
    join_ping_ack_seq: int | None = None
    join_ping_ack_seen: bool = False
    map_percent: int = 0
    last_lobby_activity: float = 0.0
    last_map_info_resend: float = 0.0
    last_sync_packet_sent: float = 0.0
    last_resend_throttle_log: float = 0.0
    resend_next_send_throttled: int = 0
    pending_sync_packets: Deque[tuple[bytes, int, str]] = field(default_factory=deque)
    race: int = 6
    team: int = 1

    @property
    def address(self) -> Address:
        return self.reliable.address

    @property
    def player_id(self) -> int:
        return self.reliable.player_id
