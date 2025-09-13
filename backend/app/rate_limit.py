from __future__ import annotations

import time

import redis


def token_bucket(
    rds: redis.Redis,
    key: str,
    *,
    rate: float,
    capacity: int,
) -> None:
    """Acquire a token from a redis-backed token bucket.

    The bucket identified by ``key`` starts full with ``capacity`` tokens and
    refills at ``rate`` tokens per second up to ``capacity``.  This function
    blocks until a token is available and then consumes it.
    """

    redis_key = f"tb:{key}"
    while True:
        now = time.time()
        try:
            with rds.pipeline() as pipe:
                pipe.watch(redis_key)
                data = pipe.hgetall(redis_key)
                tokens = float(data.get("tokens", capacity))
                ts = float(data.get("ts", now))
                # Refill based on elapsed time
                tokens = min(capacity, tokens + (now - ts) * rate)
                if tokens >= 1:
                    tokens -= 1
                    pipe.multi()
                    pipe.hset(redis_key, mapping={"tokens": tokens, "ts": now})
                    # Expire the key a bit after it would naturally drain to
                    # avoid unbounded growth of keys.
                    ttl = int(capacity / rate * 2)
                    pipe.expire(redis_key, ttl)
                    pipe.execute()
                    return
                wait = (1 - tokens) / rate
            # Nothing available; sleep until at least one token is produced
            time.sleep(max(wait, 0))
        except redis.WatchError:
            # Retry on concurrent updates
            continue
