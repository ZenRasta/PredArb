from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Any, Dict

import redis
import requests

from app import rate_limit


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

        ``limit`` specifies the maximum number of requests allowed in
        ``period`` seconds.  The helper in :mod:`app.rate_limit` implements a
        small token bucket which allows short bursts up to ``limit`` and then
        refills at a steady rate.  Calls block using ``time.sleep`` until a
        token becomes available.
        """

        redis_key = f"{self.platform}:{key}"
        rate_limit.acquire(self.redis, redis_key, limit=limit, period=period)

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
