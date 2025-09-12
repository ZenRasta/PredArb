from __future__ import annotations
from typing import Any, Optional
from .settings import settings


class _InMemoryRDS:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    def get(self, key: str) -> Optional[str]:
        return self._store.get(key)

    def set(self, key: str, value: Any) -> None:
        self._store[key] = str(value)

    def incrby(self, key: str, amount: int = 1) -> int:
        cur = int(self._store.get(key, "0"))
        cur += int(amount)
        self._store[key] = str(cur)
        return cur

    def ping(self) -> bool:
        return True


def _make_rds():
    try:
        import redis  # type: ignore
        r = redis.Redis.from_url(settings.redis_url, decode_responses=True)
        # probe
        r.ping()
        return r
    except Exception:
        # Fallback for local smoke tests without Redis
        return _InMemoryRDS()


rds = _make_rds()

# Optional: placeholder for Supabase client to satisfy imports elsewhere
try:
    from supabase import create_client  # type: ignore
    import os
    if settings.supabase_url and settings.supabase_service_role:
        supabase = create_client(settings.supabase_url, settings.supabase_service_role)
    else:
        supabase = None  # type: ignore
except Exception:  # pragma: no cover
    supabase = None  # type: ignore
