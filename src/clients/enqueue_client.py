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


async def run(node_id: int, peers: List[Tuple[str, int]], payload: str, timeout: float = 3.0) -> None:
    clock = LamportClock()
    ack_future: asyncio.Future[Optional[str]] = asyncio.get_running_loop().create_future()

    async def _on_conn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if raw and not ack_future.done():
                data = json.loads(raw.decode())
                env = Envelope.model_validate(data)
                clock.update(env.lamport_ts)
                ack_future.set_result(env.payload.get("task_id"))
        except Exception:
            if not ack_future.done():
                ack_future.set_result(None)
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(_on_conn, "127.0.0.1", 0)
    reply_host, reply_port = server.sockets[0].getsockname()

    env = Envelope(
        type=MessageType.ENQUEUE,
        source=node_id,
        target=None,
        lamport_ts=clock.tick(),
        payload={"payload": payload, "reply_host": reply_host, "reply_port": reply_port},
    )
    await send_to_any(peers, env.model_dump_json().encode() + b"\n")

    try:
        async with server:
            task_id = await asyncio.wait_for(ack_future, timeout=timeout)
            if task_id is not None:
                print(f"ACK recebido. task_id={task_id} (Lamport={clock.value})")
            else:
                print("ACK vazio ou invalido.")
    except asyncio.TimeoutError:
        print("ACK nao recebido (timeout)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Producer: enqueue task")
    parser.add_argument("--id", type=int, default=9000, help="Client ID for Lamport timestamp")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--peers", type=str, default="", help="Comma list host:port seeds to try in order")
    parser.add_argument("--payload", type=str, required=True)
    args = parser.parse_args()
    peers = parse_peers(args.peers, args.host, args.port)
    asyncio.run(run(args.id, peers, args.payload))


if __name__ == "__main__":
    main()
