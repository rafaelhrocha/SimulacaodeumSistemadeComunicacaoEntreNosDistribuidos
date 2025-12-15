from __future__ import annotations

import argparse
import asyncio
import json
import socket
import statistics
import sys
import time
from pathlib import Path
from typing import List, Tuple

# Ensure local imports work when running as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.messages import Envelope, MessageType
from src.common.config import NodeConfig, PeerConfig
from src.node.node import Node
from tests.integration.test_distributed_system import _clear_task_files  # reuse helper


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _build_configs(count: int, **cfg_kwargs) -> List[NodeConfig]:
    ports = [_free_port() for _ in range(count)]
    configs: List[NodeConfig] = []
    for idx in range(count):
        node_id = idx + 1
        peers = []
        for jdx in range(count):
            if jdx == idx:
                continue
            peers.append(PeerConfig(id=jdx + 1, host="127.0.0.1", port=ports[jdx]))
        cfg = NodeConfig(id=node_id, host="127.0.0.1", port=ports[idx], peers=peers, **cfg_kwargs)
        configs.append(cfg)
    return configs


async def _start_cluster(count: int = 3, **cfg_kwargs) -> List[Node]:
    configs = _build_configs(count, **cfg_kwargs)
    nodes = [Node(cfg) for cfg in configs]
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


async def enqueue_with_ack(host: str, port: int, payload: str, client_id: int, timeout: float = 3.0) -> float:
    """Send ENQUEUE and wait for ACK; returns latency in seconds."""
    server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    reply_host, reply_port = server.sockets[0].getsockname()
    server.close()
    await server.wait_closed()

    ack_future: asyncio.Future[None] = asyncio.get_event_loop().create_future()

    async def _on_conn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
        writer.close()
        await writer.wait_closed()
        if raw and not ack_future.done():
            ack_future.set_result(None)

    server = await asyncio.start_server(_on_conn, "127.0.0.1", reply_port)

    env = Envelope(
        type=MessageType.ENQUEUE,
        source=client_id,
        target=None,
        lamport_ts=0,
        payload={"payload": payload, "reply_host": reply_host, "reply_port": reply_port},
    )
    start = time.perf_counter()
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(env.model_dump_json().encode() + b"\n")
    await writer.drain()
    writer.close()
    await writer.wait_closed()

    async with server:
        await asyncio.wait_for(ack_future, timeout=timeout)
    end = time.perf_counter()
    return end - start


def _percentiles(samples: List[float], points=(50, 90, 95, 99)) -> dict:
    if not samples:
        return {}
    ordered = sorted(samples)
    result = {}
    for p in points:
        k = (len(ordered) - 1) * (p / 100)
        f = int(k)
        c = min(f + 1, len(ordered) - 1)
        if f == c:
            result[p] = ordered[int(k)]
        else:
            d0 = ordered[f] * (c - k)
            d1 = ordered[c] * (k - f)
            result[p] = d0 + d1
    return result


async def run_benchmark(num_messages: int, concurrency: int, task_ack_timeout: float) -> None:
    _clear_task_files([1, 2, 3])
    nodes = await _start_cluster(3, task_ack_timeout=task_ack_timeout)
    try:
        leader = max(nodes, key=lambda n: n.config.id)
        print(f"[bench] leader={leader.config.id} ports={[n.config.port for n in nodes]}")

        sem = asyncio.Semaphore(concurrency)
        latencies: List[float] = []

        async def worker(idx: int):
            async with sem:
                lat = await enqueue_with_ack(leader.config.host, leader.config.port, f"payload-{idx}", client_id=9000 + idx)
                latencies.append(lat)

        start = time.perf_counter()
        await asyncio.gather(*(worker(i) for i in range(num_messages)))
        elapsed = time.perf_counter() - start

        throughput = num_messages / elapsed if elapsed else 0
        pct = _percentiles(latencies)
        print(f"[bench] msgs={num_messages} conc={concurrency} elapsed={elapsed:.3f}s throughput={throughput:.1f} msg/s")
        print(
            "[latency] avg={:.4f}s p50={:.4f}s p90={:.4f}s p95={:.4f}s p99={:.4f}s".format(
                statistics.mean(latencies),
                pct.get(50, 0.0),
                pct.get(90, 0.0),
                pct.get(95, 0.0),
                pct.get(99, 0.0),
            )
        )
    finally:
        await _stop_cluster(nodes)


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark enqueue latency/throughput")
    parser.add_argument("--messages", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--task-ack-timeout", type=float, default=5.0)
    args = parser.parse_args()
    asyncio.run(run_benchmark(args.messages, args.concurrency, args.task_ack_timeout))


if __name__ == "__main__":
    main()
