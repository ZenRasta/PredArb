from __future__ import annotations
from .celery_app import celery
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
from .db import rds


def _now_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


# Short task name: ingest.fetch_markets
@celery.task(name="ingest.fetch_markets")
def fetch_markets(platform: str, window_s: int = 300) -> List[Dict[str, Any]]:
    # For smoke test: simulate N markets fetched
    now = _now_ts()
    rds.set(f"metrics:{platform}:last_fetch_ts", now)
    # return a small list to flow through the pipeline
    items = [
        {"event_id": f"demo-{platform}-1", "title": "Demo Market 1"},
        {"event_id": f"demo-{platform}-2", "title": "Demo Market 2"},
    ]
    return items


# Short task name: ingest.write_markets
@celery.task(name="ingest.write_markets")
def write_markets(items: List[Dict[str, Any]]) -> Tuple[str, int]:
    # For smoke test: just bump markets_upserted counter by len(items)
    platform = "polymarket" if any("polymarket" in (i.get("event_id") or "") for i in items) else "limitless"
    rds.incrby(f"metrics:{platform}:markets_upserted", len(items) or 1)
    return platform, len(items or [])


# Short task name: ingest.write_snapshots
@celery.task(name="ingest.write_snapshots")
def write_snapshots(prev: Tuple[str, int] | None, platform: str, window_s: int = 120) -> Dict[str, Any]:
    # prev is (platform, count) from write_markets
    plat = platform or (prev[0] if prev else "polymarket")
    count = prev[1] if prev else 1
    rds.set(f"metrics:{plat}:last_snapshot_ts", _now_ts())
    rds.incrby(f"metrics:{plat}:snapshots_inserted", max(1, count))
    return {"ok": True, "platform": plat, "snapshots": count}
