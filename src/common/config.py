from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class PeerConfig:
    id: int
    host: str
    port: int


@dataclass
class NodeConfig:
    """Configuração básica de um nó."""

    id: int
    host: str
    port: int
    peers: List[PeerConfig]
    heartbeat_interval: float = 1.0  # segundos
    heartbeat_timeout: float = 3.0
    election_timeout: float = 2.0
    task_ack_timeout: float = 5.0
