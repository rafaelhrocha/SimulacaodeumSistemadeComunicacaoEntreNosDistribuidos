from src.common.lamport_clock import LamportClock


def test_lamport_clock_increments_and_updates():
    print("\n[unit] LamportClock: incrementa e faz update corretamente")
    clock = LamportClock()
    assert clock.tick() == 1
    assert clock.tick() == 2
    clock.update(5)
    assert clock.value == 6
    clock.update(3)
    assert clock.value == 7
    print(f"[result] valor final={clock.value}")
