from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from src.common.config import NodeConfig, PeerConfig
from src.common.lamport_clock import LamportClock
from src.common.messages import Envelope


class Role(str, Enum):
    LEADER = "leader"
    FOLLOWER = "follower"
    CANDIDATE = "candidate"


@dataclass
class TaskItem:
    id: str
    payload: str
    owner: Optional[int] = None
    acked: bool = False


@dataclass
class NodeState:
    config: NodeConfig
    lamport: LamportClock = field(default_factory=LamportClock)
    role: Role = Role.FOLLOWER
    leader_id: Optional[int] = None
    peers_alive: Dict[int, bool] = field(default_factory=dict)
    last_heartbeat: Dict[int, float] = field(default_factory=dict)
    tasks: List[TaskItem] = field(default_factory=list)
    pending_acks: Dict[str, asyncio.TimerHandle] = field(default_factory=dict)
    peers_by_id: Dict[int, PeerConfig] = field(default_factory=dict)
    log_version: int = 0
    time_offset_ms: float = 0.0
    prepared_tasks: Dict[str, TaskItem] = field(default_factory=dict)  # para 2PC
    prepared_timers: Dict[str, asyncio.TimerHandle] = field(default_factory=dict)
    mailbox: Dict[int, List[Envelope]] = field(default_factory=dict)  # mensagens indiretas

    def set_leader(self, leader_id: int) -> None:
        self.leader_id = leader_id
        self.role = Role.FOLLOWER if leader_id != self.config.id else Role.LEADER
        # Ao trocar de líder, seguidores limpam prepares e timers pendentes
        if self.role == Role.FOLLOWER:
            for handle in self.prepared_timers.values():
                try:
                    handle.cancel()
                except Exception:
                    pass
            self.prepared_timers.clear()
            self.prepared_tasks.clear()

    def next_ts(self) -> int:
        return self.lamport.tick()

    def update_ts(self, other: int) -> int:
        return self.lamport.update(other)

    def alive_peers(self) -> List[PeerConfig]:
        return [p for p in self.config.peers if self.peers_alive.get(p.id, True)]
