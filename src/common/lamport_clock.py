from __future__ import annotations

import threading


class LamportClock:
    """Relógio lógico de Lamport."""

    def __init__(self) -> None:
        self._ts = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        with self._lock:
            self._ts += 1
            return self._ts

    def update(self, other_ts: int) -> int:
        with self._lock:
            self._ts = max(self._ts, other_ts) + 1
            return self._ts

    @property
    def value(self) -> int:
        return self._ts
