from __future__ import annotations
import types

from app import rate_limit


class DummyRedis:
    """Very small Redis stand-in used for unit testing."""

    def __init__(self) -> None:
        self.store: dict[str, dict[str, float]] = {}

    class _Pipeline:
        def __init__(self, parent: "DummyRedis") -> None:
            self.parent = parent

        # Context manager methods
        def __enter__(self) -> "DummyRedis._Pipeline":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - required by context mgr
            pass

        # Redis pipeline methods used by the limiter
        def watch(self, key: str) -> None:
            self.key = key

        def unwatch(self) -> None:
            pass

        def hmget(self, key: str, *fields: str):
            data = self.parent.store.get(key, {})
            return [data.get(f) for f in fields]

        def hset(self, key: str, mapping: dict[str, float]) -> None:
            self.parent.store.setdefault(key, {}).update(mapping)

        def expire(self, key: str, ttl: int) -> None:
            pass

        def multi(self) -> None:
            pass

        def execute(self) -> None:
            pass

    def pipeline(self) -> "DummyRedis._Pipeline":
        return DummyRedis._Pipeline(self)


def _mock_time(monkeypatch, start: float = 0.0) -> dict[str, float]:
    """Patch ``rate_limit.time`` with a controllable clock."""

    now = {"value": start}

    def time_func() -> float:
        return now["value"]

    def sleep_func(dt: float) -> None:
        now["value"] += dt

    monkeypatch.setattr(
        rate_limit,
        "time",
        types.SimpleNamespace(time=time_func, sleep=sleep_func),
    )
    return now


def test_rate_limit_enforces_limit(monkeypatch) -> None:
    r = DummyRedis()
    clock = _mock_time(monkeypatch)

    rate_limit.acquire(r, "bucket", limit=2, period=1)
    rate_limit.acquire(r, "bucket", limit=2, period=1)

    before = clock["value"]
    rate_limit.acquire(r, "bucket", limit=2, period=1)

    # After consuming the burst capacity we expect the third acquire to wait
    # for at least half a second (rate = 2 tokens/second).
    assert clock["value"] - before >= 0.5


def test_rate_limit_allows_burst_after_refill(monkeypatch) -> None:
    r = DummyRedis()
    clock = _mock_time(monkeypatch)

    rate_limit.acquire(r, "bucket", limit=2, period=1)
    rate_limit.acquire(r, "bucket", limit=2, period=1)

    # Advance time so that the bucket refills to full capacity.
    clock["value"] += 1.0
    before = clock["value"]

    rate_limit.acquire(r, "bucket", limit=2, period=1)
    rate_limit.acquire(r, "bucket", limit=2, period=1)

    # Should not have slept as two tokens were available immediately.
    assert clock["value"] == before

