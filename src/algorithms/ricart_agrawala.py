from __future__ import annotations

import asyncio
from typing import Callable, Iterable, Optional, Set
import logging

from src.common.messages import Envelope, MessageType
from src.common.logger import log_structured

SendFunc = Callable[[str, int, Envelope], asyncio.Future]


class RicartAgrawalaMutex:
    """Implementa Ricart-Agrawala para exclusão mútua distribuída."""

    def __init__(
        self,
        node_id: int,
        peers,
        send_func: SendFunc,
        next_ts: Callable[[], int],
        peer_by_id: Callable[[int], Optional[object]],
        election_timeout: float = 2.0,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.node_id = node_id
        self.peers = peers
        self._send = send_func
        self._next_ts = next_ts
        self._peer_by_id = peer_by_id
        self._lock = asyncio.Lock()
        self._request_ts: Optional[int] = None
        self._replies: Set[int] = set()
        self._deferred: Set[int] = set()
        self._timeout = election_timeout
        self._logger = logger

    async def enter(self, peers_alive: Iterable[int]) -> None:
        async with self._lock:
            # Dispara REQUEST_CS a todos os pares vivos e aguarda replies
            self._request_ts = self._next_ts()
            self._replies = set()
            targets = [p for p in self.peers if p.id in peers_alive]
            for peer in targets:
                msg = Envelope(
                    type=MessageType.REQUEST_CS,
                    source=self.node_id,
                    target=peer.id,
                    lamport_ts=self._next_ts(),
                    payload={"ts": self._request_ts},
                )
                await self._send(peer.host, peer.port, msg)
            if targets:
                log_structured(
                    self._logger,
                    "warning",
                    "mutex_request",
                    node=self.node_id,
                    ts=self._request_ts,
                    targets=[p.id for p in targets],
                )
            if targets:
                try:
                    await asyncio.wait_for(self._wait_replies(len(targets)), timeout=self._timeout)
                except asyncio.TimeoutError:
                    # Se timeout, seguimos adiante para evitar deadlock local.
                    return

    async def release(self) -> None:
        await self._broadcast(MessageType.RELEASE_CS, {})
        for peer_id in list(self._deferred):
            await self._send_reply(peer_id)
        self._deferred.clear()
        self._request_ts = None

    async def on_request(self, envelope: Envelope) -> None:
        their_ts = envelope.payload.get("ts", 0)
        self_ts = self._request_ts or float("inf")
        should_reply = (self._request_ts is None) or ((their_ts, envelope.source) < (self_ts, self.node_id))
        if should_reply:
            await self._send_reply(envelope.source)
            log_structured(self._logger, "warning", "mutex_reply", node=self.node_id, to=envelope.source, their_ts=their_ts, self_ts=self_ts)
        else:
            self._deferred.add(envelope.source)
            log_structured(self._logger, "info", "mutex_deferred", node=self.node_id, deferred=envelope.source, their_ts=their_ts, self_ts=self_ts)

    def on_reply(self, envelope: Envelope) -> None:
        self._replies.add(envelope.source)
        log_structured(self._logger, "info", "mutex_reply_received", node=self.node_id, from_node=envelope.source, received=len(self._replies))

    async def on_release(self, envelope: Envelope) -> None:
        if envelope.source in self._deferred:
            self._deferred.remove(envelope.source)
            await self._send_reply(envelope.source)

    async def _send_reply(self, target_id: int) -> None:
        peer = self._peer_by_id(target_id)
        if not peer:
            return
        msg = Envelope(
            type=MessageType.REPLY_CS,
            source=self.node_id,
            target=target_id,
            lamport_ts=self._next_ts(),
            payload={},
        )
        await self._send(peer.host, peer.port, msg)
        log_structured(self._logger, "warning", "mutex_reply_sent", node=self.node_id, to=target_id)

    async def _broadcast(self, msg_type: MessageType, payload: dict) -> None:
        for peer in self.peers:
            msg = Envelope(
                type=msg_type,
                source=self.node_id,
                target=peer.id,
                lamport_ts=self._next_ts(),
                payload=payload,
            )
            await self._send(peer.host, peer.port, msg)

    async def _wait_replies(self, needed: int) -> None:
        while len(self._replies) < needed:
            await asyncio.sleep(0.05)
