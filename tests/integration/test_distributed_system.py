import asyncio
import json
import socket
from pathlib import Path
from typing import List, Tuple

import pytest

from src.common.messages import Envelope, MessageType
from src.node.node import Node
from src.common.config import NodeConfig, PeerConfig
from src.node.state import Role


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _clear_task_files(ids):
    data_dir = Path("data")
    for node_id in ids:
        f = data_dir / f"tasks_{node_id}.json"
        if f.exists():
            try:
                f.unlink()
            except Exception:
                pass


def _build_configs(count: int, **cfg_kwargs) -> List[Tuple[NodeConfig, int]]:
    ports = [_free_port() for _ in range(count)]
    configs = []
    for idx in range(count):
        node_id = idx + 1
        peers = []
        for jdx in range(count):
            if jdx == idx:
                continue
            peers.append(PeerConfig(id=jdx + 1, host="127.0.0.1", port=ports[jdx]))
        cfg = NodeConfig(id=node_id, host="127.0.0.1", port=ports[idx], peers=peers, **cfg_kwargs)
        configs.append((cfg, ports[idx]))
    return configs


async def _start_cluster(count: int = 2, **cfg_kwargs) -> List[Node]:
    configs = _build_configs(count, **cfg_kwargs)
    nodes = [Node(cfg) for cfg, _ in configs]
    for n in nodes:
        await n.start()
    return nodes


async def _stop_cluster(nodes: List[Node]) -> None:
    for n in nodes:
        try:
            await n.stop()
        except Exception:
            pass
    await asyncio.sleep(0.05)
    # Allow pending transport tasks to finish before the loop closes
    await asyncio.sleep(0.05)


async def _wait_for(predicate, timeout: float = 8.0, interval: float = 0.05):
    start = asyncio.get_event_loop().time()
    while True:
        if predicate():
            return
        if asyncio.get_event_loop().time() - start > timeout:
            raise asyncio.TimeoutError("Condition not met in time")
        await asyncio.sleep(interval)


async def _request_task_from_leader(leader: Node, client_id: int = 5000, timeout: float = 3.0) -> Envelope:
    """Send a DEQUEUE to leader and return the received TASK envelope without ACKing."""
    server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    reply_host, reply_port = server.sockets[0].getsockname()
    server.close()
    await server.wait_closed()
    task_fut: asyncio.Future[Envelope] = asyncio.get_event_loop().create_future()

    async def _on_conn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
        writer.close()
        await writer.wait_closed()
        env = Envelope.model_validate(json.loads(raw.decode()))
        if not task_fut.done():
            task_fut.set_result(env)

    server = await asyncio.start_server(_on_conn, "127.0.0.1", reply_port)

    req = Envelope(
        type=MessageType.DEQUEUE,
        source=client_id,
        target=None,
        lamport_ts=0,
        payload={"reply_host": reply_host, "reply_port": reply_port},
    )
    reader, writer = await asyncio.open_connection(leader.config.host, leader.config.port)
    writer.write(req.model_dump_json().encode() + b"\n")
    await writer.drain()
    writer.close()
    await writer.wait_closed()

    async with server:
        await asyncio.wait_for(task_fut, timeout=timeout)
    return task_fut.result()


@pytest.mark.asyncio
async def test_election_and_replication_three_nodes():
    print("\n[scenario] Eleicao Bully + replicacao 2PC com quorum (3 nos)")
    _clear_task_files([1, 2, 3])
    nodes = await _start_cluster(3)
    try:
        leader = max(nodes, key=lambda n: n.config.id)
        follower = min(nodes, key=lambda n: n.config.id)
        mid = sorted(nodes, key=lambda n: n.config.id)[1]
        print(f"[setup] portas: {[n.config.port for n in nodes]}")

        await _wait_for(
            lambda: leader.state.role == Role.LEADER
            and follower.state.leader_id == leader.config.id
            and mid.state.leader_id == leader.config.id,
            timeout=5.0,
        )
        print(f"[election] lider={leader.config.id} follower={follower.config.id} mid={mid.config.id}")

        # send enqueue to leader
        env = Envelope(
            type=MessageType.ENQUEUE,
            source=999,  # client id
            target=None,
            lamport_ts=0,
            payload={"payload": "ping"},
        )
        print("[enqueue] enviando 'ping' para o lider")
        reader, writer = await asyncio.open_connection(leader.config.host, leader.config.port)
        writer.write(env.model_dump_json().encode() + b"\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

        await _wait_for(lambda: len(leader.state.tasks) > 0, timeout=5.0)
        task_id = leader.state.tasks[-1].id

        # follower should have the replicated task committed
        await _wait_for(lambda: any(t.id == task_id for t in follower.state.tasks), timeout=5.0)
        await _wait_for(lambda: any(t.id == task_id for t in mid.state.tasks), timeout=5.0)
        print(f"[replicacao] task_id={task_id} nos seguidores ok")
        assert follower.state.log_version >= 1
        assert mid.state.log_version >= 1

        # simulate follower down during enqueue to test quorum behavior
        await mid.stop()
        leader.state.peers_alive[mid.config.id] = False
        env2 = Envelope(
            type=MessageType.ENQUEUE,
            source=1000,
            target=None,
            lamport_ts=0,
            payload={"payload": "with_quorum"},
        )
        print(f"[quorum] seguidor {mid.config.id} parado; enviando enqueue 'with_quorum'")
        reader2, writer2 = await asyncio.open_connection(leader.config.host, leader.config.port)
        writer2.write(env2.model_dump_json().encode() + b"\n")
        await writer2.drain()
        writer2.close()
        await writer2.wait_closed()

        await _wait_for(lambda: len(leader.state.tasks) > 1, timeout=10.0)
        task_id2 = leader.state.tasks[-1].id
        # quorum (2/3) should allow commit even with mid stopped
        await _wait_for(lambda: any(t.id == task_id2 for t in follower.state.tasks), timeout=5.0)
        print(f"[quorum] task_id={task_id2} replicada com 2/3 vivos")
    finally:
        await _stop_cluster(nodes)


@pytest.mark.asyncio
async def test_sync_request_catches_up_rejoining_follower():
    print("\n[scenario] Seguidor reingressa e sincroniza via SYNC_REQUEST/STATE")
    _clear_task_files([1, 2, 3])
    configs = _build_configs(3)
    nodes = [Node(cfg) for cfg, _ in configs]
    try:
        for n in nodes:
            await n.start()
        leader = max(nodes, key=lambda n: n.config.id)
        follower = min(nodes, key=lambda n: n.config.id)
        mid = sorted(nodes, key=lambda n: n.config.id)[1]

        await _wait_for(
            lambda: leader.state.role == Role.LEADER
            and follower.state.leader_id == leader.config.id
            and mid.state.leader_id == leader.config.id,
            timeout=5.0,
        )
        print(f"[election] lider={leader.config.id}; seguidor offline sera {mid.config.id}")

        # Stop mid follower so it misses the next commit
        mid_id = mid.config.id
        await mid.stop()
        nodes.remove(mid)
        leader.state.peers_alive[mid_id] = False

        # Enqueue while mid is offline; quorum still holds with leader+follower
        env = Envelope(
            type=MessageType.ENQUEUE,
            source=9999,
            target=None,
            lamport_ts=0,
            payload={"payload": "sync_me"},
        )
        print("[enqueue] enviando 'sync_me' com 2 nos ativos (quorum)")
        reader, writer = await asyncio.open_connection(leader.config.host, leader.config.port)
        writer.write(env.model_dump_json().encode() + b"\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

        await _wait_for(lambda: len(leader.state.tasks) > 0, timeout=5.0)
        task_id = leader.state.tasks[-1].id
        await _wait_for(lambda: any(t.id == task_id for t in follower.state.tasks), timeout=5.0)

        # Restart mid and allow leader heartbeats to reach it
        mid_cfg = next(cfg for cfg, _ in configs if cfg.id == mid_id)
        mid_restarted = Node(mid_cfg)
        nodes.append(mid_restarted)
        await mid_restarted.start()
        leader.state.peers_alive[mid_id] = True

        await _wait_for(lambda: any(t.id == task_id for t in mid_restarted.state.tasks), timeout=8.0)
        assert mid_restarted.state.log_version == leader.state.log_version
        print(f"[sync] task_id={task_id} sincronizada apos retorno do no {mid_id}")
    finally:
        await _stop_cluster(nodes)


@pytest.mark.asyncio
async def test_task_requeues_when_ack_missing():
    print("\n[scenario] Reentrega de tarefa apos falta de ACK")
    _clear_task_files([1, 2])
    nodes = await _start_cluster(2, task_ack_timeout=0.6)
    try:
        leader = max(nodes, key=lambda n: n.config.id)
        follower = min(nodes, key=lambda n: n.config.id)
        await _wait_for(
            lambda: leader.state.role == Role.LEADER and follower.state.leader_id == leader.config.id,
            timeout=5.0,
        )
        print(f"[election] lider={leader.config.id}")

        # Enqueue one task
        env = Envelope(
            type=MessageType.ENQUEUE,
            source=777,
            target=None,
            lamport_ts=0,
            payload={"payload": "requeue_me"},
        )
        reader, writer = await asyncio.open_connection(leader.config.host, leader.config.port)
        writer.write(env.model_dump_json().encode() + b"\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

        await _wait_for(lambda: len(leader.state.tasks) > 0, timeout=3.0)
        task_id = leader.state.tasks[-1].id
        print(f"[enqueue] task_id={task_id}")

        # Request task but do NOT ack
        task_env = await _request_task_from_leader(leader, client_id=9001, timeout=3.0)
        assert task_env.payload.get("task_id") == task_id
        print(f"[dequeue] recebido task_id={task_id} sem ACK")

        # Wait for requeue
        await _wait_for(
            lambda: any(t.id == task_id and t.owner is None and not t.acked for t in leader.state.tasks)
            and task_id not in leader.state.pending_acks,
            timeout=3.0,
        )
        print(f"[requeue] task_id={task_id} devolvida por falta de ACK")
    finally:
        await _stop_cluster(nodes)
