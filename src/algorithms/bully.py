from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional

from src.common.logger import log_structured
from src.common.messages import Envelope, MessageType
from src.node.state import Role

SendFunc = Callable[[str, int, Envelope], asyncio.Future]


class BullyCoordinator:
    """Implementa a eleição Bully desacoplada do nó."""

    def __init__(
        self,
        node_id: int,
        peers,
        send_func: SendFunc,
        set_leader_cb: Callable[[int], None],
        next_ts: Callable[[], int],
        peer_by_id: Callable[[int], Optional[object]],
        election_timeout: float = 2.0,
        logger: Optional[logging.Logger] = None,
        is_peer_alive: Optional[Callable[[int], bool]] = None,
    ) -> None:
        self.node_id = node_id
        self.peers = peers
        self._send = send_func
        self._set_leader = set_leader_cb
        self._next_ts = next_ts
        self._peer_by_id = peer_by_id
        self._election_timeout = election_timeout
        self._in_progress = False
        self._timeout_task: Optional[asyncio.Task] = None
        self._logger = logger
        self._is_peer_alive = is_peer_alive or (lambda _pid: True)

    @property
    def in_progress(self) -> bool:
        return self._in_progress

    async def start(self) -> None:
        if self._in_progress:
            return
        self._in_progress = True
        higher = [p for p in self.peers if p.id > self.node_id and self._is_peer_alive(p.id)]
        log_structured(
            self._logger,
            "warning",
            "election_start",
            node=self.node_id,
            higher_ids=[p.id for p in higher],
        )
        if not higher:
            await self._announce_win()
            return
        for peer in higher:
            msg = Envelope(
                type=MessageType.ELECTION,
                source=self.node_id,
                target=peer.id,
                lamport_ts=self._next_ts(),
            )
            await self._send(peer.host, peer.port, msg)
        self._schedule_timeout()

    async def on_election(self, envelope: Envelope) -> None:
        log_structured(self._logger, "warning", "election_received", node=self.node_id, from_node=envelope.source)
        if self.node_id > envelope.source:
            reply = Envelope(
                type=MessageType.ELECTION_OK,
                source=self.node_id,
                target=envelope.source,
                lamport_ts=self._next_ts(),
            )
            peer = self._peer_by_id(envelope.source)
            if peer:
                await self._send(peer.host, peer.port, reply)
            await self.start()

    async def on_election_ok(self, envelope: Envelope) -> None:
        # Somente marca que alguém maior está ativo; aguardamos anúncio.
        self._in_progress = True
        log_structured(self._logger, "info", "election_ok_received", node=self.node_id, from_node=envelope.source)

    def on_leader_announce(self, envelope: Envelope) -> None:
        self._cancel_timeout()
        self._in_progress = False
        # Se um nÃ³ com ID menor anunciar lÃ­der, reinicia a eleiÃ§Ã£o para evitar lÃ­der incorreto
        if envelope.source < self.node_id:
            log_structured(
                self._logger,
                "warning",
                "leader_announce_lower_id",
                node=self.node_id,
                announced=envelope.source,
            )
            if not self._in_progress:
                try:
                    asyncio.get_running_loop().create_task(self.start())
                except RuntimeError:
                    pass
            return
        self._set_leader(envelope.source)
        log_structured(self._logger, "warning", "leader_announce_received", node=self.node_id, leader=envelope.source)

    async def _announce_win(self) -> None:
        self._set_leader(self.node_id)
        self._in_progress = False
        log_structured(self._logger, "warning", "election_win", node=self.node_id)
        for peer in self.peers:
            msg = Envelope(
                type=MessageType.LEADER_ANNOUNCE,
                source=self.node_id,
                target=peer.id,
                lamport_ts=self._next_ts(),
            )
            await self._send(peer.host, peer.port, msg)

    def _schedule_timeout(self) -> None:
        self._cancel_timeout()
        loop = asyncio.get_running_loop()
        self._timeout_task = loop.create_task(self._await_result())

    def _cancel_timeout(self) -> None:
        if self._timeout_task and not self._timeout_task.done():
            self._timeout_task.cancel()
        self._timeout_task = None

    def stop(self) -> None:
        self._cancel_timeout()

    async def _await_result(self) -> None:
        try:
            await asyncio.sleep(self._election_timeout)
            if self._in_progress:
                await self._announce_win()
        except asyncio.CancelledError:
            return
