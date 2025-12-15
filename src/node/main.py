from __future__ import annotations

import argparse
import asyncio
from typing import List

from src.common.config import NodeConfig, PeerConfig
from src.node.node import Node


def parse_peers(peers_str: str, self_id: int) -> List[PeerConfig]:
    peers: List[PeerConfig] = []
    if not peers_str:
        return peers
    for item in peers_str.split(","):
        host, port, peer_id = item.split(":")
        pid = int(peer_id)
        if pid == self_id:
            continue
        peers.append(PeerConfig(id=pid, host=host, port=int(port)))
    return peers


async def main() -> None:
    parser = argparse.ArgumentParser(description="Distributed Queue Node")
    parser.add_argument("--id", type=int, required=True, help="Node ID")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", type=str, default="", help="Comma list host:port:id")
    args = parser.parse_args()

    peers = parse_peers(args.peers, args.id)
    cfg = NodeConfig(
        id=args.id,
        host=args.host,
        port=args.port,
        peers=peers,
    )

    node = Node(cfg)
    await node.start()
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
