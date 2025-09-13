# backend/app/tasks_analysis.py
from __future__ import annotations

import hashlib
import json
import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .celery_app import celery
from .db import supabase

log = logging.getLogger(__name__)

# ====================================================================================
# Utilities
# ====================================================================================

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _now_ts() -> int:
    return int(_now_utc().timestamp())

def _bps_to_frac(bps: float) -> float:
    return max(0.0, float(bps or 0.0)) / 10_000.0

def _json_hash(obj: Dict[str, Any]) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()

def _age_seconds(ts: Optional[str | int | float]) -> Optional[float]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return max(0.0, _now_ts() - float(ts))
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return max(0.0, _now_utc().timestamp() - dt.timestamp())
    except Exception:
        return None

# ====================================================================================
# Fees / data access
# ====================================================================================

def _load_platform_fees() -> Dict[str, Dict[str, float]]:
    """
    Reads your existing schema:
      platform_fees(platform, taker_bps, withdrawal_fee_usd, gas_estimate_usd, ...)
    """
    rows = supabase.table("platform_fees").select("platform,taker_bps,withdrawal_fee_usd,gas_estimate_usd").execute().data or []
    out: Dict[str, Dict[str, float]] = {}
    for r in rows:
        out[str(r["platform"])] = {
            "taker_bps": float(r.get("taker_bps") or 0),
            "withdrawal_fee_usd": float(r.get("withdrawal_fee_usd") or 0),
            "gas_estimate_usd": float(r.get("gas_estimate_usd") or 0),
        }
    return out

def _latest_snapshot(market_id: str) -> Optional[Dict[str, Any]]:
    res = (
        supabase.table("market_snapshots")
        .select("ts,outcomes,fees,liquidity_usd")
        .eq("market_id", market_id)
        .order("ts", desc=True)
        .limit(1)
        .execute()
    )
    return (res.data or [None])[0]

def _recent_groups(limit: int = 200) -> List[Dict[str, Any]]:
    res = (
        supabase.table("groups")
        .select("id,market_ids,avg_prob")
        .order("updated_at", desc=True)
        .limit(limit)
        .execute()
    )
    return res.data or []

# ====================================================================================
# Microstructure approximations (MVP; tune later)
# ====================================================================================

def _slippage_mid_to_fill(price_mid: float, size_usd: float) -> float:
    """
    Very simple model:
      - First $100 at mid
      - +5 bps per extra $100, capped at +50 bps total
    """
    if size_usd <= 100:
        bump_bps = 0
    else:
        bump_bps = min(50, int((size_usd - 100) / 100) * 5)
    bump = _bps_to_frac(bump_bps)
    return min(0.9999, max(0.0001, price_mid * (1.0 + bump)))

def _stale_penalty_bps(age_sec: Optional[float]) -> float:
    """
    0 bps if <= 60s; +5 bps per extra 60s, cap +50 bps
    """
    if age_sec is None or age_sec <= 60:
        return 0.0
    steps = min(10, math.ceil((age_sec - 60) / 60.0))
    return 5.0 * steps

def _leg_effective_price(price_mid: float, size_usd: float, taker_bps: float, age_sec: Optional[float]) -> float:
    p = _slippage_mid_to_fill(price_mid, size_usd)
    fee = _bps_to_frac(taker_bps)
    stale = _bps_to_frac(_stale_penalty_bps(age_sec))
    eff = p * (1.0 + fee + stale)
    return min(0.9999, max(0.0001, eff))

def _fillable_usd(snapshot: Dict[str, Any]) -> float:
    """
    Try to read a depth indicator from outcomes; default to $250.
    If your snapshots contain richer depth, parse here for accuracy.
    """
    outs = snapshot.get("outcomes") or []
    for o in outs:
        if isinstance(o, dict) and "depth_usd" in o:
            try:
                return float(o["depth_usd"])
            except Exception:
                pass
    # fall back to market liquidity/10 as a coarse bound (optional)
    try:
        liq = snapshot.get("liquidity_usd")
        if isinstance(liq, (int, float)) and liq > 0:
            return float(max(100.0, min(liq / 10.0, 5000.0)))
    except Exception:
        pass
    return 250.0

# ====================================================================================
# Dutch book EV (binary)
# ====================================================================================

def _dutch_book_ev(yes_eff: float, no_eff: float, size_usd: float) -> Tuple[float, float]:
    """
    Worst-case profit at resolution:
      YES wins: size*(1-yes_eff) - size*no_eff
      NO  wins: size*(1-no_eff)  - size*yes_eff
    Arbitrage if yes_eff + no_eff < 1.0 (after costs) → min(prof_yes, prof_no) > 0
    Returns (EV_usd, edge_bps).
    """
    prof_yes = size_usd * (1.0 - yes_eff) - size_usd * no_eff
    prof_no  = size_usd * (1.0 - no_eff) - size_usd * yes_eff
    ev = min(prof_yes, prof_no)
    edge_bps = 0.0 if size_usd <= 0 else (ev / size_usd) * 10_000.0
    return ev, edge_bps

# ====================================================================================
# Builders: Dutch-book across venues, and cross-venue mispricing vs VWAP
# ====================================================================================

_YES_ALIASES = {"YES", "Y", "TRUE", "WIN", "UP", "OVER"}
_NO_ALIASES  = {"NO", "N", "FALSE", "LOSE", "DOWN", "UNDER"}

def _snap_yes_no(snapshot: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    yes_mid = no_mid = None
    for o in (snapshot.get("outcomes") or []):
        lbl = str(o.get("label", "")).strip().upper()
        prob = o.get("mid") or o.get("prob")
        if not isinstance(prob, (int, float)):
            continue
        if lbl in _YES_ALIASES:
            yes_mid = float(prob)
        elif lbl in _NO_ALIASES:
            no_mid = float(prob)
    return yes_mid, no_mid

def _platform_from_snapshot(snapshot: Dict[str, Any]) -> str:
    return (snapshot.get("fees") or {}).get("_platform_hint") or "unknown"

def _vwap_lookup(avg_prob: List[Dict[str, Any]], label: str) -> Optional[float]:
    for row in (avg_prob or []):
        if str(row.get("label", "")).strip().lower() == label.strip().lower():
            try:
                return float(row.get("prob"))
            except Exception:
                return None
    return None

def _build_dutch_book(group: Dict[str, Any], fees_map: Dict[str, Dict[str, float]], size_candidates=(100.0, 500.0, 1000.0)) -> List[Dict[str, Any]]:
    mids: List[Dict[str, Any]] = []
    for mid in (group.get("market_ids") or []):
        snap = _latest_snapshot(mid)
        if not snap:
            continue
        yes_mid, no_mid = _snap_yes_no(snap)
        mids.append({
            "market_id": mid,
            "platform": _platform_from_snapshot(snap),
            "yes_mid": yes_mid,
            "no_mid": no_mid,
            "snapshot": snap,
        })

    opps: List[Dict[str, Any]] = []
    for a in mids:
        if a["yes_mid"] is None:
            continue
        for b in mids:
            if a["market_id"] == b["market_id"]:
                continue
            if a["platform"] == b["platform"]:
                continue
            if b["no_mid"] is None:
                continue

            fill_usd = min(_fillable_usd(a["snapshot"]), _fillable_usd(b["snapshot"]))
            if fill_usd <= 0:
                continue

            taker_a = fees_map.get(a["platform"], {}).get("taker_bps", 20.0)
            taker_b = fees_map.get(b["platform"], {}).get("taker_bps", 20.0)
            age_a = _age_seconds(a["snapshot"].get("ts"))
            age_b = _age_seconds(b["snapshot"].get("ts"))

            # We’ll store one row per size bucket (to fit your schema: one EV per row)
            for size in size_candidates:
                sz = min(size, float(fill_usd))
                yes_eff = _leg_effective_price(a["yes_mid"], sz, taker_a, age_a)
                no_eff  = _leg_effective_price(b["no_mid"],  sz, taker_b, age_b)
                ev_usd, edge_bps = _dutch_book_ev(yes_eff, no_eff, sz)
                if ev_usd <= 0:
                    continue

                legs = [
                    {"platform": a["platform"], "market_id": a["market_id"], "side": "BUY_YES", "price_mid": a["yes_mid"], "effective": yes_eff, "snapshot_ts": a["snapshot"].get("ts")},
                    {"platform": b["platform"], "market_id": b["market_id"], "side": "BUY_NO",  "price_mid": b["no_mid"],  "effective": no_eff,  "snapshot_ts": b["snapshot"].get("ts")},
                ]
                payload = {
                    "type": "dutch_book",
                    "group_id": group["id"],
                    "legs": legs,
                    "params": {"model": "default_v1"},
                    "metrics": {
                        "size_usd": float(sz),
                        "ev_usd": float(ev_usd),
                        "edge_bps": int(round(edge_bps)),
                    },
                }
                payload["hash"] = _json_hash(payload)
                opps.append(payload)
    return opps

def _build_cross_mispricing(group: Dict[str, Any], size_bucket: float = 500.0) -> List[Dict[str, Any]]:
    """
    Not risk-free; we store a proxy EV: (avg_prob - venue_prob) * size.
    Positive EV implies BUY of the given label; negative implies short/NO, which we skip for now.
    """
    out: List[Dict[str, Any]] = []
    for mid in (group.get("market_ids") or []):
        snap = _latest_snapshot(mid)
        if not snap:
            continue
        platform = _platform_from_snapshot(snap)
        for o in (snap.get("outcomes") or []):
            label = str(o.get("label", ""))
            p = o.get("mid") or o.get("prob")
            if not isinstance(p, (int, float)):
                continue
            avg = _vwap_lookup(group.get("avg_prob") or [], label)
            if not isinstance(avg, (int, float)):
                continue
            edge = avg - float(p)  # if avg>p → BUY label
            edge_bps = int(round(edge * 10_000.0))
            ev_usd = float(edge * size_bucket)
            if ev_usd <= 0:
                continue

            legs = [{"platform": platform, "market_id": mid, "side": f"BUY_{label.upper()}", "price_mid": float(p)}]
            payload = {
                "type": "cross_mispricing",
                "group_id": group["id"],
                "legs": legs,
                "params": {"reference": "group_vwap"},
                "metrics": {
                    "size_usd": float(size_bucket),
                    "ev_usd": ev_usd,
                    "edge_bps": edge_bps,
                },
            }
            payload["hash"] = _json_hash(payload)
            out.append(payload)
    return out

# ====================================================================================
# Persistence against YOUR schema
# ====================================================================================

def _insert_opportunity_row(row: Dict[str, Any]) -> Optional[str]:
    """
    Insert one row into arb_opportunities (your schema). Dedup by unique hash.
    Returns the inserted ID or None if duplicate.
    """
    try:
        result = supabase.table("arb_opportunities").insert({
            "opp_hash": row["hash"],
            "opp_type": row["type"],
            "group_id": row["group_id"],
            "legs": row["legs"],
            "params": row.get("params") or {},
            "metrics": row.get("metrics") or {},
        }).execute()
        if result.data:
            return result.data[0]["id"]
        return None
    except Exception as e:
        # Likely duplicate on unique(hash). Keep quiet for idempotency.
        msg = str(e).lower()
        if "duplicate" in msg or "unique" in msg:
            return None
        log.debug("insert_opportunity failed: %s", e)
        return None

# Optional: if you want to fan out to users now (your alerts_queue requires user_id)
def _fanout_alerts_for_users(arb_id: str, min_ev_usd: float) -> int:
    """
    Example fanout (very conservative; fetch small set).
    Requires users.subscribed = true; you can also add prefs filtering in Python.
    """
    try:
        users = (
            supabase.table("users")
            .select("telegram_id,preferences,subscribed")
            .eq("subscribed", True)
            .limit(1000)
            .execute()
        ).data or []
        cnt = 0
        for u in users:
            try:
                supabase.table("alerts_queue").insert({
                    "user_id": u["telegram_id"],
                    "arb_id": arb_id,
                    "status": "pending",
                }).execute()
                cnt += 1
            except Exception:
                pass
        return cnt
    except Exception as e:
        log.debug("fanout failed: %s", e)
        return 0

# ====================================================================================
# Celery task
# ====================================================================================

@celery.task(name="analysis.compute_opportunities")
def compute_opportunities(max_groups: int = 200,
                          write_dutch: bool = True,
                          write_mispricing: bool = True,
                          min_ev_usd_alert: float = 1.0) -> Dict[str, Any]:
    """
    Scans recent groups, computes:
      - Dutch-book arbs (BUY_YES on venue A, BUY_NO on venue B)
      - Cross-venue mispricings vs group VWAP
    Persists to arb_opportunities with unique hash.
    """
    fees = _load_platform_fees()
    groups = _recent_groups(limit=max_groups)

    inserted = 0
    alerted = 0

    for g in groups:
        if write_dutch:
            for row in _build_dutch_book(g, fees):
                arb_id = _insert_opportunity_row(row)
                if arb_id:
                    inserted += 1
                    # (Optional) enable fanout once you want user alerts here
                    # if row["metrics"]["ev_usd"] >= min_ev_usd_alert:
                    #     alerted += _fanout_alerts_for_users(arb_id, min_ev_usd_alert)

        if write_mispricing:
            for row in _build_cross_mispricing(g):
                arb_id = _insert_opportunity_row(row)
                if arb_id:
                    inserted += 1
                    # typically we don't alert on mispricings yet

    return {"ok": True, "inserted": inserted, "alerted": alerted, "scanned_groups": len(groups)}
