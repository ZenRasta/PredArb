# backend/app/main.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .db import rds, supabase
from .settings import settings
from .celery_app import celery
from .tasks_ingest import fetch_markets, write_markets, write_snapshots
from .tasks_analysis import compute_opportunities as compute_opps_task  # tiny API trigger target

app = FastAPI(title="PredArb API", version="0.3.2")

# ------------------------------------------------------------------------------
# CORS (dev-friendly; tighten for prod)
# ------------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # TODO: restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------------------
# Health
# ------------------------------------------------------------------------------
@app.get("/health")
def health() -> Dict[str, Any]:
    def gi(k, default=None):
        v = rds.get(k)
        return v if v is not None else default

    now = int(datetime.now(tz=timezone.utc).timestamp())

    def ago(ts: Optional[str]) -> Optional[int]:
        if ts is None:
            return None
        try:
            return max(0, now - int(ts))
        except Exception:
            return None

    resp = {
        "status": "ok",
        "time": now,
        "ingest": {
            "polymarket": {
                "last_fetch_age_s": ago(gi("metrics:polymarket:last_fetch_ts")),
                "last_snapshot_age_s": ago(gi("metrics:polymarket:last_snapshot_ts")),
                "markets_upserted_24h": int(gi("metrics:polymarket:markets_upserted", "0")),
                "snapshots_inserted_24h": int(gi("metrics:polymarket:snapshots_inserted", "0")),
                "fetch_rate_limited_24h": int(gi("metrics:polymarket:fetch_rate_limited", "0")),
                "ob_rate_limited_24h": int(gi("metrics:polymarket:ob_rate_limited", "0")),
            },
            "limitless": {
                "last_fetch_age_s": ago(gi("metrics:limitless:last_fetch_ts")),
                "last_snapshot_age_s": ago(gi("metrics:limitless:last_snapshot_ts")),
                "markets_upserted_24h": int(gi("metrics:limitless:markets_upserted", "0")),
                "snapshots_inserted_24h": int(gi("metrics:limitless:snapshots_inserted", "0")),
                "fetch_rate_limited_24h": int(gi("metrics:limitless:fetch_rate_limited", "0")),
                "ob_rate_limited_24h": int(gi("metrics:limitless:ob_rate_limited", "0")),
            },
        },
    }
    return resp

# ------------------------------------------------------------------------------
# Ingest: fetch -> write_markets -> write_snapshots (Celery chain; inline fallback)
# ------------------------------------------------------------------------------
@app.post("/ingest")
def ingest(platform: str = Query(..., pattern="^(polymarket|limitless)$")):
    """
    Kick a one-shot ingest for the chosen platform:
      fetch -> write_markets -> write_snapshots
    Returns task id (Celery) or inline summary (fallback).
    """
    try:
        sig_fetch = fetch_markets.s(platform, 300)
        sig_write = write_markets.s()
        sig_snaps = write_snapshots.s()
        chain = (sig_fetch | sig_write | sig_snaps)
        res = chain.apply_async()
        try:
            result = res.get(timeout=30)
        except Exception:
            result = None
        return {"ok": True, "task_id": res.id, "platform": platform, "mode": "celery", "result": result}
    except Exception:
        # Fallback: run inline without Celery/broker
        items = fetch_markets(platform, 300)
        items = write_markets(items)
        result = write_snapshots(items)
        return {"ok": True, "platform": platform, "result": result, "mode": "inline"}

# ------------------------------------------------------------------------------
# Admin override (Session 3) — protect in production!
# ------------------------------------------------------------------------------
class OverrideIn(BaseModel):
    group_id: Optional[str] = Field(default=None, description="Group to target (optional).")
    market_id: str = Field(..., description="Market UUID to include/exclude.")
    action: str = Field(..., pattern="^(include|exclude)$")
    note: Optional[str] = None

@app.post("/admin/group_override")
def admin_group_override(payload: OverrideIn):
    """
    Add an override row. Your grouping job will apply these after scoring.
    NOTE: add auth in production (JWT role check, API key, or IP allow-list).
    """
    try:
        supabase.table("group_overrides").insert({
            "group_id": payload.group_id,
            "market_id": payload.market_id,
            "action": payload.action,
            "note": payload.note,
        }).execute()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"insert failed: {e}")

# ------------------------------------------------------------------------------
# Groups browser for the WebApp
# ------------------------------------------------------------------------------
@app.get("/groups")
def list_groups(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    include_empty: bool = Query(False, description="Include groups that have no market_ids"),
) -> Dict[str, Any]:
    """
    Returns recent groups with `avg_prob` and a computed `group_size`.
    Sorted by updated_at desc (with created_at as a fallback).
    Supports pagination via `limit` and `offset`.
    """
    try:
        # Supabase range is inclusive; compute end index
        start = offset
        end = offset + limit - 1

        q = (
            supabase.table("groups")
            .select("id,title,market_ids,avg_prob,updated_at,created_at")
            .order("updated_at", desc=True, nulls_first=False)
            .order("created_at", desc=True, nulls_first=False)
            .range(start, end)
        )
        res = q.execute()
        rows: List[Dict[str, Any]] = res.data or []

        if not include_empty:
            rows = [r for r in rows if (r.get("market_ids") or [])]

        for r in rows:
            mids = r.get("market_ids") or []
            r["group_size"] = len(mids)

        return {"ok": True, "count": len(rows), "items": rows, "limit": limit, "offset": offset}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"/groups query failed: {e}")

# ------------------------------------------------------------------------------
# Tiny API Trigger: run Session 4 analysis (Celery first, inline fallback)
# ------------------------------------------------------------------------------
@app.post("/analyze/run")
def analyze_run(
    max_groups: int = Query(200, ge=1, le=1000),
    write_dutch: bool = Query(True),
    write_mispricing: bool = Query(True),
    min_ev_usd_alert: float = Query(1.0, ge=0.0),
) -> Dict[str, Any]:
    """
    Trigger analysis.compute_opportunities:
      - Computes Dutch-book arbs across venues per group
      - Computes cross-venue mispricings vs group VWAP
      - Persists rows to arb_opportunities (dedup by hash)
    Returns Celery task id if broker is available; otherwise runs inline and returns the result.
    """
    # Try Celery
    try:
        sig = compute_opps_task.s(
            max_groups=max_groups,
            write_dutch=write_dutch,
            write_mispricing=write_mispricing,
            min_ev_usd_alert=min_ev_usd_alert,
        )
        res = sig.apply_async()
        return {
            "ok": True,
            "mode": "celery",
            "task_id": res.id,
            "params": {
                "max_groups": max_groups,
                "write_dutch": write_dutch,
                "write_mispricing": write_mispricing,
                "min_ev_usd_alert": min_ev_usd_alert,
            },
        }
    except Exception:
        # Inline fallback (no broker / local quick test)
        try:
            result = compute_opps_task.run(
                max_groups=max_groups,
                write_dutch=write_dutch,
                write_mispricing=write_mispricing,
                min_ev_usd_alert=min_ev_usd_alert,
            )
            return {
                "ok": True,
                "mode": "inline",
                "result": result,
                "params": {
                    "max_groups": max_groups,
                    "write_dutch": write_dutch,
                    "write_mispricing": write_mispricing,
                    "min_ev_usd_alert": min_ev_usd_alert,
                },
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"analyze_run failed: {e}")

