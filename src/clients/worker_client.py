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
    server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    reply_host, reply_port = server.sockets[0].getsockname()

    async def wait_task() -> Optional[Envelope]:
        async with server:
            conn = await asyncio.wait_for(server.accept(), timeout=timeout)
            reader, writer = conn
            raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
            writer.close()
            await writer.wait_closed()
            if not raw:
                return None
            data = json.loads(raw.decode())
            env = Envelope.model_validate(data)
            clock.update(env.lamport_ts)
            return env

    req = Envelope(
        type=MessageType.DEQUEUE,
        source=node_id,
        target=None,
        lamport_ts=clock.tick(),
        payload={"reply_host": reply_host, "reply_port": reply_port},
    )
    await send_to_any(peers, req.model_dump_json().encode() + b"\n")

    try:
        task_env = await wait_task()
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
