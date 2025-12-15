from __future__ import annotations

import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Dict, Optional

from src.algorithms.bully import BullyCoordinator
from src.algorithms.ricart_agrawala import RicartAgrawalaMutex
from src.common.config import NodeConfig
from src.common.logger import log_structured, setup_logger
from src.common.messages import Envelope, MessageType
from src.node.state import NodeState, Role, TaskItem
from src.node.transport import AsyncTransport


class Node:
    """Distributed node with Bully election, Ricart-Agrawala mutex, 2PC replication, routing and time sync."""

    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self.state = NodeState(config=config, peers_by_id={p.id: p for p in config.peers})
        self.logger = setup_logger()
        self.transport = AsyncTransport(config.host, config.port, self.handle_message)
        self.bully = BullyCoordinator(
            node_id=config.id,
            peers=config.peers,
            send_func=self._send_raw,
            set_leader_cb=self.state.set_leader,
            next_ts=self.state.next_ts,
            peer_by_id=self._peer_by_id,
            election_timeout=config.election_timeout,
            logger=self.logger,
            is_peer_alive=lambda pid: self.state.peers_alive.get(pid, True),
        )
        self.mutex = RicartAgrawalaMutex(
            node_id=config.id,
            peers=config.peers,
            send_func=self._send_raw,
            next_ts=self.state.next_ts,
            peer_by_id=self._peer_by_id,
            election_timeout=config.election_timeout,
            logger=self.logger,
        )
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._time_sync_task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()
        self._storage_file = Path("data") / f"tasks_{config.id}.json"
        self._prepare_acks: Dict[str, set[int]] = {}

    async def start(self) -> None:
        await self.transport.start()
        self._load_tasks()
        log_structured(self.logger, "info", "node_started", node=self.config.id, port=self.config.port)
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        self._time_sync_task = asyncio.create_task(self._time_sync_loop())
        asyncio.create_task(self.bully.start())

    async def stop(self) -> None:
        self._stopped.set()
        self.bully.stop()
        for task in (self._heartbeat_task, self._monitor_task, self._time_sync_task):
            if task:
                task.cancel()
        await self.transport.stop()
        log_structured(self.logger, "info", "node_stopped", node=self.config.id)

    async def handle_message(self, envelope: Envelope) -> None:
        if self._stopped.is_set():
            return
        self.state.update_ts(envelope.lamport_ts)
        if envelope.type == MessageType.HEARTBEAT:
            self._on_heartbeat(envelope)
        elif envelope.type == MessageType.TIME_REQUEST:
            await self._on_time_request(envelope)
        elif envelope.type == MessageType.TIME_REPLY:
            self._on_time_reply(envelope)
        elif envelope.type == MessageType.ELECTION:
            await self.bully.on_election(envelope)
        elif envelope.type == MessageType.ELECTION_OK:
            await self.bully.on_election_ok(envelope)
        elif envelope.type == MessageType.LEADER_ANNOUNCE:
            self.bully.on_leader_announce(envelope)
        elif envelope.type == MessageType.REQUEST_CS:
            await self.mutex.on_request(envelope)
        elif envelope.type == MessageType.REPLY_CS:
            self.mutex.on_reply(envelope)
        elif envelope.type == MessageType.RELEASE_CS:
            await self.mutex.on_release(envelope)
        elif envelope.type == MessageType.ENQUEUE:
            await self._on_enqueue(envelope)
        elif envelope.type == MessageType.DEQUEUE:
            await self._on_dequeue(envelope)
        elif envelope.type == MessageType.TASK_ACK:
            self._on_task_ack(envelope)
        elif envelope.type == MessageType.REPL_PREPARE:
            await self._on_repl_prepare(envelope)
        elif envelope.type == MessageType.REPL_PREPARE_OK:
            await self._on_repl_prepare_ok(envelope)
        elif envelope.type == MessageType.REPL_COMMIT:
            self._on_repl_commit(envelope)
        elif envelope.type == MessageType.REPL_ABORT:
            self._on_repl_abort(envelope)
        elif envelope.type == MessageType.ROUTE:
            await self._on_route(envelope)
        elif envelope.type == MessageType.MAILBOX_PUSH:
            self._on_mailbox_push(envelope)
        elif envelope.type == MessageType.MAILBOX_DELIVER:
            await self.handle_message(Envelope.model_validate(envelope.payload["inner"]))
        elif envelope.type == MessageType.SYNC_REQUEST:
            await self._on_sync_request(envelope)
        elif envelope.type == MessageType.SYNC_STATE:
            self._on_sync_state(envelope)
        else:
            log_structured(self.logger, "warning", "unknown_message", node=self.config.id, type=envelope.type)

    async def _heartbeat_loop(self) -> None:
        while not self._stopped.is_set():
            if self.state.role == Role.LEADER:
                await self._broadcast(MessageType.HEARTBEAT, {})
            await asyncio.sleep(self.config.heartbeat_interval)

    async def _monitor_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stopped.is_set():
            leader = self.state.leader_id
            if leader and leader != self.config.id:
                last = self.state.last_heartbeat.get(leader)
                if last and loop.time() - last > self.config.heartbeat_timeout:
                    log_structured(self.logger, "warning", "leader_timeout", node=self.config.id, leader=leader)
                    self.state.leader_id = None
                    await self.bully.start()
            elif leader is None:
                await self.bully.start()
            await asyncio.sleep(0.5)

    async def _time_sync_loop(self) -> None:
        while not self._stopped.is_set():
            if self.state.role != Role.LEADER and self.state.leader_id:
                leader = self._peer_by_id(self.state.leader_id)
                if leader:
                    # Cristian: segue o líder como time server para obter offset de relógio físico
                    req = Envelope(
                        type=MessageType.TIME_REQUEST,
                        source=self.config.id,
                        target=leader.id,
                        lamport_ts=self.state.next_ts(),
                    )
                    await self.transport.send(leader.host, leader.port, req)
            await asyncio.sleep(5.0)

    def _on_heartbeat(self, envelope: Envelope) -> None:
        self.state.peers_alive[envelope.source] = True
        self.state.last_heartbeat[envelope.source] = asyncio.get_running_loop().time()
        self.state.peers_by_id.setdefault(envelope.source, self._peer_by_id(envelope.source))
        if self.state.leader_id is None:
            self.state.set_leader(envelope.source)
        # Deliver queued mailbox messages when peer seen alive
        self._deliver_mailbox(envelope.source)
        # Follower pode pedir sincronização ao líder se versão estiver desatualizada
        if self.state.role == Role.FOLLOWER and self.state.leader_id == envelope.source:
            asyncio.create_task(self._maybe_sync_with_leader())

    async def _on_time_request(self, envelope: Envelope) -> None:
        # Cristian: leader responde com timestamp de relógio físico
        now_ms = int(time.time() * 1000)
        peer = self._peer_by_id(envelope.source)
        if not peer:
            return
        reply = Envelope(
            type=MessageType.TIME_REPLY,
            source=self.config.id,
            target=envelope.source,
            lamport_ts=self.state.next_ts(),
            payload={"server_time_ms": now_ms},
        )
        await self.transport.send(peer.host, peer.port, reply)

    def _on_time_reply(self, envelope: Envelope) -> None:
        # Ajusta offset local (não altera relógio do SO)
        server_time = envelope.payload.get("server_time_ms")
        if server_time is None:
            return
        now_ms = int(time.time() * 1000)
        self.state.time_offset_ms = server_time - now_ms
        log_structured(self.logger, "info", "time_sync", node=self.config.id, offset_ms=self.state.time_offset_ms)

    async def _broadcast(self, msg_type: MessageType, payload: Dict) -> None:
        for peer in self.config.peers:
            if not self.state.peers_alive.get(peer.id, True):
                continue
            msg = Envelope(
                type=msg_type,
                source=self.config.id,
                target=peer.id,
                lamport_ts=self.state.next_ts(),
                payload=payload,
            )
            await self.transport.send(peer.host, peer.port, msg)

    async def _on_enqueue(self, envelope: Envelope) -> None:
        if self.state.role != Role.LEADER:
            if self.state.leader_id is not None and self.state.leader_id != self.config.id:
                leader = self._peer_by_id(self.state.leader_id)
                if leader:
                    await self.transport.send(leader.host, leader.port, envelope)
            return
        async with self._critical_section():
            # 2PC para replicar tarefa antes de confirmá-la ao cliente
            task_id = envelope.payload.get("task_id") or str(uuid.uuid4())
            payload = envelope.payload.get("payload", "")
            if not await self._two_phase_replicate(task_id, payload):
                log_structured(self.logger, "error", "enqueue_abort", node=self.config.id, task_id=task_id)
                return
            self.state.tasks.append(TaskItem(id=task_id, payload=payload))
            self.state.log_version += 1
            ack = Envelope(
                type=MessageType.ENQUEUE_ACK,
                source=self.config.id,
                target=envelope.source,
                lamport_ts=self.state.next_ts(),
                payload={"task_id": task_id},
            )
            self._persist_tasks()
            await self._send_to_origin(envelope, ack)

    async def _on_dequeue(self, envelope: Envelope) -> None:
        if self.state.role != Role.LEADER:
            if self.state.leader_id is not None and self.state.leader_id != self.config.id:
                leader = self._peer_by_id(self.state.leader_id)
                if leader:
                    await self.transport.send(leader.host, leader.port, envelope)
            return
        async with self._critical_section():
            task = self._next_pending_task()
            if not task:
                return
            task.owner = envelope.source
            msg = Envelope(
                type=MessageType.TASK,
                source=self.config.id,
                target=envelope.source,
                lamport_ts=self.state.next_ts(),
                payload={"task_id": task.id, "payload": task.payload},
            )
            self._persist_tasks()
            await self._send_to_origin(envelope, msg)
            loop = asyncio.get_running_loop()
            handle = loop.call_later(self.config.task_ack_timeout, self._requeue_task, task.id)
            self.state.pending_acks[task.id] = handle

    def _on_task_ack(self, envelope: Envelope) -> None:
        task_id = envelope.payload.get("task_id")
        if not task_id:
            return
        for task in self.state.tasks:
            if task.id == task_id:
                task.acked = True
                task.owner = envelope.source
                log_structured(self.logger, "info", "task_acked", node=self.config.id, task_id=task_id)
                handle = self.state.pending_acks.pop(task_id, None)
                if handle:
                    handle.cancel()
                break
        self._persist_tasks()

    async def _two_phase_replicate(self, task_id: str, payload: str) -> bool:
        peers_alive = [p for p in self.config.peers if self.state.peers_alive.get(p.id, True)]
        if not peers_alive:
            return True  # nothing to replicate
        self._prepare_acks[task_id] = set()
        total_nodes = len(peers_alive) + 1
        quorum = (total_nodes // 2) + 1
        # Phase 1: prepare
        for peer in peers_alive:
            msg = Envelope(
                type=MessageType.REPL_PREPARE,
                source=self.config.id,
                target=peer.id,
                lamport_ts=self.state.next_ts(),
                payload={"task_id": task_id, "payload": payload},
            )
            await self.transport.send(peer.host, peer.port, msg)
        # Wait for OKs
        async def wait_prepare():
            while len(self._prepare_acks.get(task_id, set())) < (quorum - 1):
                await asyncio.sleep(0.05)

        try:
            await asyncio.wait_for(wait_prepare(), timeout=self.config.task_ack_timeout)
        except asyncio.TimeoutError:
            ok_count = len(self._prepare_acks.get(task_id, set()))
            if ok_count < (quorum - 1):
                await self._broadcast(MessageType.REPL_ABORT, {"task_id": task_id})
                self._prepare_acks.pop(task_id, None)
                return False
        # Phase 2: commit
        await self._broadcast(MessageType.REPL_COMMIT, {"task_id": task_id, "log_version": self.state.log_version + 1})
        self._prepare_acks.pop(task_id, None)
        return True

    async def _on_repl_prepare(self, envelope: Envelope) -> None:
        task_id = envelope.payload["task_id"]
        payload = envelope.payload.get("payload", "")
        # Salva task em prepared; commit virá no passo seguinte do 2PC
        self.state.prepared_tasks[task_id] = TaskItem(id=task_id, payload=payload)
        self._persist_tasks()
        # Se commit não chegar dentro do timeout, aborta automaticamente
        loop = asyncio.get_running_loop()
        handle = loop.call_later(self.config.task_ack_timeout * 2, self._expire_prepared, task_id)
        old = self.state.prepared_timers.pop(task_id, None)
        if old:
            old.cancel()
        self.state.prepared_timers[task_id] = handle
        peer = self._peer_by_id(envelope.source)
        if peer:
            ok = Envelope(
                type=MessageType.REPL_PREPARE_OK,
                source=self.config.id,
                target=envelope.source,
                lamport_ts=self.state.next_ts(),
                payload={"task_id": task_id},
            )
            await self.transport.send(peer.host, peer.port, ok)

    async def _on_repl_prepare_ok(self, envelope: Envelope) -> None:
        # Leader collects OKs
        task_id = envelope.payload.get("task_id")
        if not task_id:
            return
        self._prepare_acks.setdefault(task_id, set()).add(envelope.source)

    def _on_repl_commit(self, envelope: Envelope) -> None:
        task_id = envelope.payload.get("task_id")
        task = self.state.prepared_tasks.pop(task_id, None)
        if task:
            self.state.tasks.append(task)
            self.state.log_version = max(self.state.log_version, envelope.payload.get("log_version", self.state.log_version))
            self._persist_tasks()
        handle = self.state.prepared_timers.pop(task_id, None)
        if handle:
            handle.cancel()

    def _on_repl_abort(self, envelope: Envelope) -> None:
        task_id = envelope.payload.get("task_id")
        if task_id in self.state.prepared_tasks:
            self.state.prepared_tasks.pop(task_id, None)
            self._persist_tasks()
        handle = self.state.prepared_timers.pop(task_id, None)
        if handle:
            handle.cancel()

    async def _on_route(self, envelope: Envelope) -> None:
        target_id = envelope.payload.get("target_id")
        ttl = envelope.payload.get("ttl", 3)
        inner = envelope.payload.get("inner")
        if target_id == self.config.id:
            await self.handle_message(Envelope.model_validate(inner))
            return
        if ttl <= 0:
            return
        for peer in self.config.peers:
            if peer.id == envelope.source:
                continue
            msg = Envelope(
                type=MessageType.ROUTE,
                source=self.config.id,
                target=peer.id,
                lamport_ts=self.state.next_ts(),
                payload={"target_id": target_id, "ttl": ttl - 1, "inner": inner},
            )
            await self.transport.send(peer.host, peer.port, msg)

    def _on_mailbox_push(self, envelope: Envelope) -> None:
        target = envelope.payload.get("target_id")
        inner = envelope.payload.get("inner")
        if target is None or inner is None:
            return
        env = Envelope.model_validate(inner)
        self.state.mailbox.setdefault(target, []).append(env)
        self._deliver_mailbox(target)

    def _deliver_mailbox(self, target_id: int) -> None:
        if not self.state.peers_alive.get(target_id):
            return
        peer = self._peer_by_id(target_id)
        if not peer:
            return
        queue = self.state.mailbox.get(target_id, [])
        send_tasks = []
        for env in queue:
            send_tasks.append(asyncio.create_task(self.transport.send(peer.host, peer.port, env)))
        queue.clear()

    def _peer_by_id(self, peer_id: int):
        for p in self.config.peers:
            if p.id == peer_id:
                return p
        return None

    class _CriticalSection:
        def __init__(self, outer: "Node") -> None:
            self.outer = outer

        async def __aenter__(self):
            await self.outer._enter_cs()
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            await self.outer._release_cs()

    def _critical_section(self):
        return self._CriticalSection(self)

    async def _enter_cs(self) -> None:
        peers_alive_ids = [p.id for p in self.config.peers if self.state.peers_alive.get(p.id, True)]
        await self.mutex.enter(peers_alive_ids)

    async def _release_cs(self) -> None:
        await self.mutex.release()

    async def _send_to_origin(self, envelope: Envelope, msg: Envelope) -> None:
        peer = self._peer_by_id(envelope.source)
        if peer:
            await self.transport.send(peer.host, peer.port, msg)
            return
        reply_host = envelope.payload.get("reply_host")
        reply_port = envelope.payload.get("reply_port")
        if reply_host and reply_port:
            await self.transport.send(reply_host, int(reply_port), msg)

    def _next_pending_task(self) -> Optional[TaskItem]:
        for task in self.state.tasks:
            if not task.acked and task.owner is None:
                return task
        return None

    def _requeue_task(self, task_id: str) -> None:
        for task in self.state.tasks:
            if task.id == task_id and not task.acked:
                task.owner = None
                log_structured(self.logger, "warning", "task_requeued", node=self.config.id, task_id=task_id)
                break
        self.state.pending_acks.pop(task_id, None)
        self._persist_tasks()

    async def _send_raw(self, host: str, port: int, msg: Envelope) -> None:
        try:
            await self.transport.send(host, port, msg)
        except Exception as exc:
            log_structured(self.logger, "warning", "send_failed", node=self.config.id, host=host, port=port, error=str(exc))
            failed_peer = None
            for p in self.config.peers:
                if p.host == host and p.port == port:
                    failed_peer = p
                    break
            if failed_peer:
                self.state.peers_alive[failed_peer.id] = False
            leader_id = self.state.leader_id
            leader_peer = self._peer_by_id(leader_id) if leader_id else None
            failed_leader = False
            if leader_peer and leader_peer.host == host and leader_peer.port == port:
                failed_leader = True
            if msg.target == leader_id:
                failed_leader = True
            if (
                self.state.role != Role.LEADER
                and leader_id is not None
                and failed_leader
                and not self.bully.in_progress
            ):
                self.state.peers_alive[leader_id] = False
                asyncio.create_task(self.bully.start())

    def _persist_tasks(self) -> None:
        self._storage_file.parent.mkdir(parents=True, exist_ok=True)
        data = [
            {"id": t.id, "payload": t.payload, "owner": t.owner, "acked": t.acked}
            for t in self.state.tasks
        ]
        prepared = [{"id": t.id, "payload": t.payload} for t in self.state.prepared_tasks.values()]
        self._storage_file.write_text(json.dumps({"tasks": data, "prepared": prepared, "log_version": self.state.log_version}))

    def _load_tasks(self) -> None:
        if not self._storage_file.exists():
            return
        try:
            raw = json.loads(self._storage_file.read_text())
            tasks_data = raw.get("tasks", [])
            self.state.tasks = [
                TaskItem(id=item["id"], payload=item["payload"], owner=item.get("owner"), acked=item.get("acked", False))
                for item in tasks_data
            ]
            prepared = raw.get("prepared", [])
            self.state.prepared_tasks = {item["id"]: TaskItem(id=item["id"], payload=item["payload"]) for item in prepared}
            self.state.log_version = raw.get("log_version", 0)
            if self.state.tasks:
                log_structured(self.logger, "info", "tasks_recovered", node=self.config.id, count=len(self.state.tasks))
        except Exception as exc:
            log_structured(self.logger, "error", "tasks_load_failed", node=self.config.id, error=str(exc))

    def _expire_prepared(self, task_id: str) -> None:
        # follower descarta prepares antigos sem commit
        if task_id in self.state.prepared_tasks:
            self.state.prepared_tasks.pop(task_id, None)
            log_structured(self.logger, "warning", "prepared_expired", node=self.config.id, task_id=task_id)
            self._persist_tasks()
        self.state.prepared_timers.pop(task_id, None)

    async def _maybe_sync_with_leader(self) -> None:
        leader = self._peer_by_id(self.state.leader_id) if self.state.leader_id else None
        if not leader:
            return
        req = Envelope(
            type=MessageType.SYNC_REQUEST,
            source=self.config.id,
            target=leader.id,
            lamport_ts=self.state.next_ts(),
            payload={"log_version": self.state.log_version},
        )
        await self.transport.send(leader.host, leader.port, req)

    async def _on_sync_request(self, envelope: Envelope) -> None:
        # Somente líder responde com estado se tiver versão mais nova
        if self.state.role != Role.LEADER:
            return
        their_version = envelope.payload.get("log_version", 0)
        if self.state.log_version <= their_version:
            return
        peer = self._peer_by_id(envelope.source)
        if not peer:
            return
        snapshot = [
            {"id": t.id, "payload": t.payload, "owner": t.owner, "acked": t.acked}
            for t in self.state.tasks
        ]
        resp = Envelope(
            type=MessageType.SYNC_STATE,
            source=self.config.id,
            target=envelope.source,
            lamport_ts=self.state.next_ts(),
            payload={"tasks": snapshot, "log_version": self.state.log_version},
        )
        await self.transport.send(peer.host, peer.port, resp)

    def _on_sync_state(self, envelope: Envelope) -> None:
        incoming_version = envelope.payload.get("log_version", 0)
        if incoming_version <= self.state.log_version:
            return
        tasks_data = envelope.payload.get("tasks", [])
        self.state.tasks = [
            TaskItem(id=item["id"], payload=item["payload"], owner=item.get("owner"), acked=item.get("acked", False))
            for item in tasks_data
        ]
        self.state.log_version = incoming_version
        # Clear prepared since state is refreshed
        self.state.prepared_tasks.clear()
        self._persist_tasks()
