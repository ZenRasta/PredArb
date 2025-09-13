from __future__ import annotations

from .celery_app import celery
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List

from .db import rds
from .types import MarketNormalized
from .dao import upsert_markets_and_outcomes, insert_snapshot
from exchanges.polymarket import PolymarketExchange
from exchanges.limitless import LimitlessExchange


def _now_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def _get_exchange(platform: str):
    if platform == "polymarket":
        return PolymarketExchange(rds)
    if platform == "limitless":
        return LimitlessExchange(rds)
    raise ValueError(f"unknown platform {platform}")


# Short task name: ingest.fetch_markets
@celery.task(name="ingest.fetch_markets")
def fetch_markets(platform: str, window_s: int = 300) -> List[Dict[str, Any]]:
    """Fetch and normalize markets for a platform."""
    ex = _get_exchange(platform)
    raw_items = ex.fetch_active_markets()
    items = [asdict(ex.normalize_market(m)) for m in raw_items]
    rds.set(f"metrics:{platform}:last_fetch_ts", _now_ts())
    return items


# Short task name: ingest.write_markets
@celery.task(name="ingest.write_markets")
def write_markets(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Upsert markets and outcomes to Supabase."""
    m_objs = [MarketNormalized(**i) for i in items]
    if not m_objs:
        return items
    platform = m_objs[0].platform
    res = upsert_markets_and_outcomes(m_objs)
    rds.incrby(f"metrics:{platform}:markets_upserted", len(res))
    return [asdict(m) for m, _ in res]


# Short task name: ingest.write_snapshots
@celery.task(name="ingest.write_snapshots")
def write_snapshots(items: List[Dict[str, Any]], window_s: int = 120) -> Dict[str, Any]:
    """Fetch orderbooks and insert snapshots for each market."""
    m_objs = [MarketNormalized(**i) for i in items]
    if not m_objs:
        return {"ok": True, "platform": None, "snapshots": 0}

    platform = m_objs[0].platform
    ex = _get_exchange(platform)
    count = 0
    for m in m_objs:
        try:
            raw = ex.fetch_orderbook_or_amm_params(m.event_id)
            snap = ex.normalize_snapshot(m.event_id, raw)
            fees = snap.fees or {}
            fees["_platform_hint"] = platform
            snap.fees = fees
            insert_snapshot(snap)
            count += 1
        except Exception:
            rds.incrby(f"metrics:{platform}:ob_rate_limited", 1)
            continue

    rds.set(f"metrics:{platform}:last_snapshot_ts", _now_ts())
    rds.incrby(f"metrics:{platform}:snapshots_inserted", count)
    return {"ok": True, "platform": platform, "snapshots": count}

