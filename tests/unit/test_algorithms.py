import asyncio
from types import SimpleNamespace

import pytest

from src.algorithms.bully import BullyCoordinator
from src.algorithms.ricart_agrawala import RicartAgrawalaMutex
from src.common.messages import Envelope, MessageType


@pytest.mark.asyncio
async def test_bully_elects_self_when_highest_id():
    print("\n[unit] Bully: no com ID 3 deve se eleger e anunciar")
    sent = []
    leader = []
    peers = [
        SimpleNamespace(id=1, host="127.0.0.1", port=8001),
        SimpleNamespace(id=2, host="127.0.0.1", port=8002),
    ]

    async def send(host, port, msg: Envelope):
        sent.append(msg)

    def set_leader(node_id: int):
        leader.append(node_id)

    bully = BullyCoordinator(
        node_id=3,
        peers=peers,
        send_func=send,
        set_leader_cb=set_leader,
        next_ts=iter(range(1, 100)).__next__,
        peer_by_id=lambda pid: next((p for p in peers if p.id == pid), None),
        election_timeout=0.1,
    )
    await bully.start()
    await asyncio.sleep(0.2)

    assert leader == [3]
    assert any(msg.type == MessageType.LEADER_ANNOUNCE for msg in sent)
    print(f"[result] leader={leader} anuncios={len([m for m in sent if m.type == MessageType.LEADER_ANNOUNCE])}")


@pytest.mark.asyncio
async def test_ricart_agrawala_replies_or_defers():
    print("\n[unit] Ricart-Agrawala: responde ou adia conforme timestamp")
    sent = []
    peers = [SimpleNamespace(id=2, host="127.0.0.1", port=8002)]

    async def send(host, port, msg: Envelope):
        sent.append(msg)

    mutex = RicartAgrawalaMutex(
        node_id=1,
        peers=peers,
        send_func=send,
        next_ts=iter(range(1, 100)).__next__,
        peer_by_id=lambda pid: next((p for p in peers if p.id == pid), None),
        election_timeout=0.1,
    )

    await mutex.enter([2])
    # Deve ter enviado REQUEST_CS
    assert any(msg.type == MessageType.REQUEST_CS for msg in sent)
    print(f"[request] enviados={len(sent)}")

    # Recebe pedido com timestamp menor -> responde
    mutex._request_ts = None  # sem requisição local pendente
    req = Envelope(type=MessageType.REQUEST_CS, source=2, target=1, lamport_ts=0, payload={"ts": 1})
    await mutex.on_request(req)
    assert any(msg.type == MessageType.REPLY_CS and msg.target == 2 for msg in sent)

    # Agora segura o recurso: pedido com TS maior deve ser deferido
    mutex._request_ts = 1  # força estado local como se estivéssemos no CS
    later_req = Envelope(type=MessageType.REQUEST_CS, source=2, target=1, lamport_ts=0, payload={"ts": 5})
    await mutex.on_request(later_req)
    assert len([m for m in sent if m.type == MessageType.REPLY_CS]) == 1
    assert 2 in mutex._deferred
    print(f"[defer] replies={len([m for m in sent if m.type == MessageType.REPLY_CS])} deferred={mutex._deferred}")
