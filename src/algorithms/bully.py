from __future__ import annotations

import asyncio
from typing import Callable, Iterable, Optional

from src.common.messages import Envelope, MessageType
from src.node.state import Role

SendFunc = Callable[[str, int, Envelope], asyncio.Future]


class BullyCoordinator:
    """Implementa a eleição Bully desacoplada do nó.

    Espera um callback de envio de mensagem (TCP) e um callback para
    alterar o líder local quando a eleição termina.
    Trabalha de forma otimista: se não há pares com ID maior, auto-elege,
    caso contrário aguarda respostas e se autoproclama após timeout.
    """

    def __init__(
        self,
        node_id: int,
        peers,
        send_func: SendFunc,
        set_leader_cb: Callable[[int], None],
        next_ts: Callable[[], int],
        peer_by_id: Callable[[int], Optional[object]],
        election_timeout: float = 2.0,
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

    @property
    def in_progress(self) -> bool:
        return self._in_progress

    async def start(self) -> None:
        if self._in_progress:
            return
        self._in_progress = True
        higher = [p for p in self.peers if p.id > self.node_id]
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

    def on_leader_announce(self, envelope: Envelope) -> None:
        self._cancel_timeout()
        self._in_progress = False
        self._set_leader(envelope.source)

    async def _announce_win(self) -> None:
        self._set_leader(self.node_id)
        self._in_progress = False
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
