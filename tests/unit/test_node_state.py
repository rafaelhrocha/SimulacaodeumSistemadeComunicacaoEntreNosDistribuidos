from src.common.config import NodeConfig, PeerConfig
from src.node.state import NodeState, Role


class _DummyTimer:
    def cancel(self):
        pass


def test_set_leader_clears_prepared_on_follower():
    cfg = NodeConfig(id=1, host="127.0.0.1", port=8000, peers=[PeerConfig(id=2, host="127.0.0.1", port=8001)])
    state = NodeState(config=cfg, peers_by_id={2: cfg.peers[0]})
    state.prepared_tasks["x"] = object()
    state.prepared_timers["x"] = _DummyTimer()

    state.set_leader(2)

    assert state.role == Role.FOLLOWER
    assert state.prepared_tasks == {}
    assert state.prepared_timers == {}
