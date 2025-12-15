from __future__ import annotations

import asyncio
import json
from typing import Awaitable, Callable, Optional, Set

from src.common.messages import Envelope

MessageHandler = Callable[[Envelope], Awaitable[None]]


class AsyncTransport:
    """Transporte TCP simples baseado em asyncio para troca de mensagens JSON."""

    def __init__(self, host: str, port: int, handler: MessageHandler) -> None:
        self.host = host
        self.port = port
        self._handler = handler
        self._server: Optional[asyncio.AbstractServer] = None
        self._client_tasks: Set[asyncio.Task] = set()

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._track_client, self.host, self.port)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        # Cancel client handlers still in flight
        for task in list(self._client_tasks):
            task.cancel()
        if self._client_tasks:
            await asyncio.gather(*self._client_tasks, return_exceptions=True)
        self._client_tasks.clear()

    async def send(self, host: str, port: int, message: Envelope, retries: int = 3, backoff: float = 0.2) -> None:
        """Send with simple retry + exponential backoff."""
        attempt = 0
        while True:
            try:
                reader, writer = await asyncio.open_connection(host, port)
                data = message.model_dump_json()
                writer.write(data.encode() + b"\n")
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return
            except Exception:
                attempt += 1
                if attempt >= retries:
                    raise
                await asyncio.sleep(backoff * (2 ** (attempt - 1)))

    async def _track_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(self._on_client(reader, writer))
        self._client_tasks.add(task)

        def _cleanup(_):
            self._client_tasks.discard(task)

        task.add_done_callback(_cleanup)
        try:
            await task
        finally:
            self._client_tasks.discard(task)

    async def _on_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            raw = await reader.readline()
            if not raw:
                return
            payload = json.loads(raw.decode())
            envelope = Envelope.model_validate(payload)
            await self._handler(envelope)
        except (asyncio.CancelledError, GeneratorExit):
            # Loop is shutting down; best-effort cleanup below.
            return
        except RuntimeError as exc:
            # Ignore loop-closed errors during teardown, re-raise otherwise.
            if "Event loop is closed" in str(exc):
                return
            raise
        finally:
            try:
                if not writer.is_closing():
                    writer.close()
                await writer.wait_closed()
            except (RuntimeError, asyncio.CancelledError, GeneratorExit):
                # Loop already closed or cancellation in progress; ignore.
                pass
            except Exception:
                pass
