from __future__ import annotations

import argparse
import asyncio
import json
from typing import List, Optional, Tuple

from src.common.lamport_clock import LamportClock
from src.common.messages import Envelope, MessageType


def parse_peers(peers_str: str, host: str, port: int) -> List[Tuple[str, int]]:
    if peers_str:
        peers = []
        for item in peers_str.split(","):
            h, p = item.split(":")
            peers.append((h, int(p)))
        return peers
    return [(host, port)]


async def send_to_any(peers: List[Tuple[str, int]], payload: bytes) -> None:
    last_err = None
    for host, port in peers:
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(payload)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return
        except Exception as exc:
            last_err = exc
            continue
    if last_err:
        raise last_err


async def run(node_id: int, peers: List[Tuple[str, int]], timeout: float = 5.0) -> None:
    clock = LamportClock()
    task_future: asyncio.Future[Optional[Envelope]] = asyncio.get_running_loop().create_future()

    async def _on_conn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if raw and not task_future.done():
                data = json.loads(raw.decode())
                env = Envelope.model_validate(data)
                clock.update(env.lamport_ts)
                task_future.set_result(env)
        except Exception:
            if not task_future.done():
                task_future.set_result(None)
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(_on_conn, "127.0.0.1", 0)
    reply_host, reply_port = server.sockets[0].getsockname()

    req = Envelope(
        type=MessageType.DEQUEUE,
        source=node_id,
        target=None,
        lamport_ts=clock.tick(),
        payload={"reply_host": reply_host, "reply_port": reply_port},
    )
    await send_to_any(peers, req.model_dump_json().encode() + b"\n")

    try:
        async with server:
            task_env = await asyncio.wait_for(task_future, timeout=timeout)
    except asyncio.TimeoutError:
        print("Nenhuma tarefa recebida (timeout).")
        return

    if not task_env:
        print("Resposta vazia.")
        return

    task_id = task_env.payload.get("task_id")
    payload = task_env.payload.get("payload")
    print(f"Tarefa recebida: id={task_id} payload={payload} (Lamport={clock.value})")

    ack = Envelope(
        type=MessageType.TASK_ACK,
        source=node_id,
        target=None,
        lamport_ts=clock.tick(),
        payload={"task_id": task_id},
    )
    await send_to_any(peers, ack.model_dump_json().encode() + b"\n")
    print("ACK enviado.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Worker: dequeue and ack one task")
    parser.add_argument("--id", type=int, default=9001, help="Client ID for Lamport timestamp")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--peers", type=str, default="", help="Comma list host:port seeds to try in order")
    args = parser.parse_args()
    peers = parse_peers(args.peers, args.host, args.port)
    asyncio.run(run(args.id, peers))


if __name__ == "__main__":
    main()
