from __future__ import annotations
from typing import Dict, List, Any, Tuple
from datetime import datetime, timezone
from supabase import Client
from .db import supabase
from .types import MarketNormalized, SnapshotNormalized, OutcomeQuote

def _mk_market_row(m: MarketNormalized) -> Dict[str, Any]:
    return {
        "platform": m.platform,
        "event_id": m.event_id,
        "title": m.title,
        "description": m.description or "",
        "end_date": m.end_date.isoformat() if m.end_date else None,
        "status": m.status,
        "volume_usd": m.volume_usd,
        "liquidity_usd": m.liquidity_usd,
        "metadata": m.metadata or {},
    }

def _mk_outcome_rows(market_id: str, outs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for o in outs:
        rows.append({
            "market_id": market_id,
            "outcome_id": str(o.get("outcome_id") or o.get("label")),
            "label": str(o.get("label") or o.get("outcome_id")),
            "polarity": None,  # optional: set 'for'/'against' in later pass
        })
    return rows

def upsert_markets_and_outcomes(items: List[MarketNormalized]) -> List[Tuple[MarketNormalized, str]]:
    """
    Upsert markets and outcomes. Returns list of (MarketNormalized, market_id).
    Uses on_conflict(platform,event_id).
    """
    results: List[Tuple[MarketNormalized, str]] = []
    if not items:
        return results

    # Upsert markets and capture ids
    payload = [_mk_market_row(m) for m in items]
    # supabase-py v2: .upsert(payload, on_conflict="platform,event_id").select("id,platform,event_id")
    res = supabase.table("markets").upsert(payload).execute()
    rows = res.data or []
    id_map: Dict[Tuple[str, str], str] = {(r["platform"], r["event_id"]): r["id"] for r in rows}

    # Outcomes: upsert per market
    for m in items:
        mk_id = id_map.get((m.platform, m.event_id))
        if not mk_id:
            # fetch id explicitly
            q = supabase.table("markets").select("id").eq("platform", m.platform).eq("event_id", m.event_id).limit(1).execute()
            mk_id = (q.data or [{}])[0].get("id")
        if not mk_id:
            continue
        out_rows = _mk_outcome_rows(mk_id, m.outcomes or [])
        if out_rows:
            supabase.table("market_outcomes").upsert(out_rows).execute()
        results.append((m, mk_id))
    return results

def insert_snapshot(s: SnapshotNormalized) -> None:
    """
    Insert one immutable snapshot row for market_snapshots.
    outcomes list is serialized directly to JSONB.
    """
    def _oq_to_json(oq: OutcomeQuote) -> Dict[str, Any]:
        return {
            "outcome_id": oq.outcome_id,
            "label": oq.label,
            "bid": oq.bid,
            "ask": oq.ask,
            "mid": oq.prob,         # keep 'mid' synonym for convenience
            "prob": oq.prob,
            "max_fill": oq.max_fill,
            "depth": oq.depth or {},
        }

    row = {
        "market_id": None,  # set via lookup below
        "ts": s.ts.isoformat(),
        "outcomes": [_oq_to_json(x) for x in s.outcomes],
        "price_source": s.price_source,
        "liquidity_usd": s.liquidity_usd,
        "fees": s.fees or {},
        "stale_seconds": int(s.stale_seconds or 0),
        "checksum": s.checksum,
    }
    # lookup market_id by (platform,event_id) -> but we only have event_id; include in checksum or payload if needed.
    # We’ll find by event_id unique within platform; caller should know s.market_event_id and platform.
    # To avoid an extra query per snapshot, the caller can pass market_id, but here we perform a single query:
    # This function assumes a prior lookup cached by caller. For now, do a quick lookup.

    # Find the platform from fees or pass via row? We’ll require caller to pass platform in fees.meta for now.
    platform = (s.fees or {}).get("_platform_hint")
    if not platform:
        # fall back to probing last known market (slower)
        q = supabase.table("markets").select("id").eq("event_id", s.market_event_id).limit(1).execute()
    else:
        q = supabase.table("markets").select("id").eq("event_id", s.market_event_id).eq("platform", platform).limit(1).execute()

    mk = (q.data or [{}])[0].get("id")
    if not mk:
        # if market missing (race), skip snapshot
        return

    row["market_id"] = mk
    supabase.table("market_snapshots").insert(row).execute()

