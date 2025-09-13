import time

from app.rate_limit import token_bucket


class DummyPipeline:
    def __init__(self, store):
        self.store = store

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

    # Redis pipeline methods used by token_bucket
    def watch(self, key):
        self.key = key

    def hgetall(self, key):
        return self.store.get(key, {}).copy()

    def hset(self, key, mapping):
        self.store.setdefault(key, {}).update(mapping)

    def expire(self, key, ttl):
        pass

    def multi(self):
        pass

    def execute(self):
        pass


class DummyRedis:
    def __init__(self):
        self.store = {}

    def pipeline(self):
        return DummyPipeline(self.store)


def test_rate_limit_blocks_after_capacity():
    r = DummyRedis()
    start = time.time()
    # rate: 2 tokens/sec, capacity 2 => third call waits ~0.5s
    for _ in range(3):
        token_bucket(r, "t1", rate=2, capacity=2)
    elapsed = time.time() - start
    assert elapsed >= 0.45


def test_rate_limit_allows_burst_then_blocks():
    r = DummyRedis()
    start = time.time()
    # capacity 5 allows burst of 5 immediately
    for _ in range(5):
        token_bucket(r, "t2", rate=1, capacity=5)
    mid = time.time()
    assert mid - start < 0.5
    # next call should wait about 1 second
    token_bucket(r, "t2", rate=1, capacity=5)
    elapsed = time.time() - mid
    assert elapsed >= 0.9
