"""Redis backed token bucket rate limiter.

This module exposes a tiny helper implementing a token bucket algorithm using
Redis primitives only.  The state of each bucket is stored as a Redis hash with
two fields:

``tokens``
    Current number of tokens left in the bucket.
``ts``
    Timestamp of the last refill operation.

The :func:`acquire` function blocks until a token is available.  The bucket
is configured using ``limit`` and ``period`` which specify how many tokens are
refilled every ``period`` seconds.  The burst capacity is equal to ``limit`` so
that at most ``limit`` requests can happen at once after the bucket has been
idle long enough.
"""

from __future__ import annotations

import math
import time

import redis

__all__ = ["acquire"]


def acquire(redis_client: redis.Redis, key: str, *, limit: int, period: int) -> None:
    """Acquire a token from ``key``'s bucket.

    Parameters
    ----------
    redis_client:
        Redis connection used to store bucket state.
    key:
        Unique identifier for the bucket.  Callers are expected to include any
        namespacing, e.g. ``"polymarket:markets"``.
    limit:
        Maximum number of requests allowed per period.
    period:
        Window size in seconds used together with ``limit``.  The token bucket
        refills at ``limit / period`` tokens per second and has a capacity of
        ``limit`` tokens.
    """

    rate = float(limit) / float(period)
    capacity = float(limit)
    redis_key = f"rl:{key}"

    while True:
        try:
            with redis_client.pipeline() as pipe:
                pipe.watch(redis_key)
                tokens_str, ts_str = pipe.hmget(redis_key, "tokens", "ts")
                now = time.time()
                if tokens_str is None or ts_str is None:
                    tokens = capacity
                    ts = now
                else:
                    tokens = float(tokens_str)
                    ts = float(ts_str)

                # Refill based on elapsed time
                elapsed = max(0.0, now - ts)
                tokens = min(capacity, tokens + elapsed * rate)

                if tokens < 1.0:
                    # Need to wait for more tokens; release watch and sleep
                    wait = (1.0 - tokens) / rate
                    pipe.unwatch()
                    time.sleep(wait)
                    continue

                tokens -= 1.0
                pipe.multi()
                pipe.hset(redis_key, mapping={"tokens": tokens, "ts": now})
                # Expire the key after the bucket could fully refill to keep
                # redis tidy.  Add a small buffer.
                ttl = int(math.ceil(capacity / rate * 2))
                pipe.expire(redis_key, ttl)
                pipe.execute()
                return
        except redis.WatchError:
            # Another client modified the key concurrently; retry.
            continue

