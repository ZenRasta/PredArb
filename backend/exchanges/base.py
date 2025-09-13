from __future__ import annotations

import os
import time
from abc import ABC, abstractmethod
from typing import Any, Dict

import redis
import requests


class BaseExchange(ABC):
    """Minimal base class for exchange fetchers.

    Provides a shared :class:`requests.Session` and a redis-backed rate
    limiter used by subclasses.  The limiter implements a very small token
    bucket that ensures we do not exceed ``limit`` requests within ``period``
    seconds.  Subclasses should call :meth:`_acquire_token` before making any
    outbound request.
    """

    platform: str = ""
    base_url: str = ""

    def __init__(self, redis_client: redis.Redis | None = None) -> None:
        self.session = requests.Session()
        # Connecting to redis is lazy; this will not fail when redis is not
        # running which keeps unit tests lightweight.
        self.redis = redis_client or redis.Redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True
        )

    # ------------------------------------------------------------------
    # Rate limiting helpers
    # ------------------------------------------------------------------
    def _acquire_token(self, key: str, limit: int, period: int) -> None:
        """Acquire a rate limit token.

        This uses a simple counter in redis with an expiry ``period``.  If the
        counter already reached ``limit`` we sleep until the key expires and
        retry.
        """

        redis_key = f"rl:{self.platform}:{key}"
        while True:
            try:
                with self.redis.pipeline() as pipe:
                    pipe.watch(redis_key)
                    current = pipe.get(redis_key)
                    current_val = int(current) if current else 0
                    if current_val < limit:
                        pipe.multi()
                        if current_val == 0:
                            pipe.set(redis_key, 1, ex=period)
                        else:
                            pipe.incr(redis_key)
                        pipe.execute()
                        return
                # Limit hit; wait for the key to expire
                time.sleep(period)
            except redis.WatchError:
                # Retry on race conditions
                continue

    # ------------------------------------------------------------------
    # Interface to implement
    # ------------------------------------------------------------------
    @abstractmethod
    def fetch_active_markets(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def fetch_orderbook_or_amm_params(self, market_id: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def normalize_market(self, raw: Dict[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def normalize_snapshot(self, market_id: str, raw: Dict[str, Any]) -> Any:
        raise NotImplementedError
