# backend/app/grouping.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Set, Tuple
import re

from rapidfuzz import fuzz

from .db import supabase, rds

# ------------------------------------------------------------------------------
# Lightweight entity/ticker extraction to quickly prune the candidate pool
# ------------------------------------------------------------------------------

ENTITY_RE = re.compile(
    r"\b(Trump|Biden|Harris|Donald Trump|Joe Biden|BTC|ETH)\b", re.I
)
TICKER_RE = re.compile(r"\b[A-Z]{2,6}\b")


def _extract_entities(text: str) -> Set[str]:
    s = text or ""
    ents = {m.group(0).lower() for m in ENTITY_RE.finditer(s)}
    ents |= {m.group(0).lower() for m in TICKER_RE.finditer(s)}
    return ents


# ------------------------------------------------------------------------------
# 1) Heuristic shortlist: title fuzz + simple entity overlap + end_date proximity
# ------------------------------------------------------------------------------

def shortlist_candidates(
    target: Dict[str, Any],
    candidates: List[Dict[str, Any]],
    k: int = 50,
) -> List[Tuple[Dict[str, Any], float]]:
    """
    Returns up to k candidates with a decent fuzzy-title score and some basic
    compatibility checks (entity overlap, end_date within ~60d if both exist).
    """
    t_title = (target.get("title") or "").lower()
    t_ents = _extract_entities((target.get("title") or "") + " " + (target.get("description") or ""))
    t_end = target.get("end_date")

    def _end_ok(c_end: Any) -> bool:
        if not t_end or not c_end:
            return True
        try:
            t_dt = datetime.fromisoformat(str(t_end).replace("Z", "+00:00")) if isinstance(t_end, str) else t_end
            c_dt = datetime.fromisoformat(str(c_end).replace("Z", "+00:00")) if isinstance(c_end, str) else c_end
            return abs((t_dt - c_dt).days) <= 60
        except Exception:
            return True

    scored: List[Tuple[Dict[str, Any], float]] = []
    for c in candidates:
        if c.get("id") == target.get("id"):
            continue
        if not _end_ok(c.get("end_date")):
            continue

        c_title = (c.get("title") or "").lower()
        c_ents = _extract_entities((c.get("title") or "") + " " + (c.get("description") or ""))

        # If both have extracted entities but no overlap, skip (fast negative)
        if t_ents and c_ents and not (t_ents & c_ents):
            continue

        score = max(
            fuzz.token_sort_ratio(t_title, c_title),
            fuzz.partial_ratio(t_title, c_title),
        )
        if score >= 70:
            scored.append((c, float(score)))

    scored.sort(key=lambda x: -x[1])
    return scored[:k]


# ------------------------------------------------------------------------------
# 2) Embedding helpers (pgvector)
# ------------------------------------------------------------------------------

def embedding_for_market_id(market_id: str) -> List[float] | None:
    """
    Load the embedding vector for a market (if present).
    """
    res = (
        supabase.table("embeddings")
        .select("vector")
        .eq("market_id", market_id)
        .limit(1)
        .execute()
    )
    rows = getattr(res, "data", None) or []
    if not rows:
        return None
    return rows[0]["vector"]


def top_by_cosine(vector: List[float], limit: int = 50) -> List[Tuple[str, float]]:
    """
    Call the 'match_markets' RPC you created in Supabase:

      create or replace function public.match_markets(query vector(1536), match_limit int)
      returns table (market_id uuid, cos_dist float)
      ...

    Returns list of (market_id, cosine_distance). Lower distance is closer.
    """
    res = supabase.rpc("match_markets", {"query": vector, "match_limit": int(limit)}).execute()
    out: List[Tuple[str, float]] = []
    for row in (getattr(res, "data", None) or []):
        out.append((row["market_id"], float(row["cos_dist"])))
    return out


# ------------------------------------------------------------------------------
# 3) Compatibility + overrides
# ------------------------------------------------------------------------------

def end_date_within(a: Dict[str, Any], b: Dict[str, Any], max_days: int = 60) -> bool:
    da, db = a.get("end_date"), b.get("end_date")
    if not da or not db:
        return True
    try:
        da = datetime.fromisoformat(str(da).replace("Z", "+00:00")) if isinstance(da, str) else da
        db = datetime.fromisoformat(str(db).replace("Z", "+00:00")) if isinstance(db, str) else db
        return abs((da - db).days) <= max_days
    except Exception:
        return True


def apply_overrides(group_candidate: List[str]) -> List[str]:
    """
    Apply explicit include/exclude overrides from group_overrides.
    """
    forced: Set[str] = set()
    removed: Set[str] = set()
    res = supabase.table("group_overrides").select("market_id,action").execute()
    for row in (getattr(res, "data", None) or []):
        if row["action"] == "include":
            forced.add(row["market_id"])
        elif row["action"] == "exclude":
            removed.add(row["market_id"])

    out = [m for m in group_candidate if m not in removed]
    out = list(set(out) | forced)
    return out


# ------------------------------------------------------------------------------
# 4) Build a group from a seed market
# ------------------------------------------------------------------------------

def compute_group_for_seed(seed_market_id: str) -> List[str]:
    """
    1) Pull seed market
    2) Pull a recent pool (by updated_at desc)
    3) Heuristic shortlist (rapidfuzz)
    4) If seed has embedding, intersect shortlist with top cosine matches
    5) Apply overrides
    """
    seed_q = (
        supabase.table("markets")
        .select("*")
        .eq("id", seed_market_id)
        .limit(1)
        .execute()
    ).data
    if not seed_q:
        return []
    seed = seed_q[0]

    pool = (
        supabase.table("markets")
        .select("id,title,description,end_date,platform,updated_at")
        .order("updated_at", desc=True)  # NOTE: Supabase Python uses nullsfirst/nullslast; none here.
        .limit(1000)
        .execute()
    ).data or []

    # Heuristic shortlist
    short = shortlist_candidates(seed, pool, k=120)

    # Embedding intersection (if vector exists)
    vec = embedding_for_market_id(seed_market_id)
    if vec is None:
        # No embedding yet; return seed only — grouping can re-run once embeddings land
        return [seed_market_id]

    top_cos = top_by_cosine(vec, limit=100)
    top_ids = {mid for (mid, _dist) in top_cos}

    inter: List[str] = []
    for cand, _score in short:
        if cand["id"] in top_ids and end_date_within(seed, cand, 60):
            inter.append(cand["id"])

    group = list(set([seed_market_id] + inter))
    group = apply_overrides(group)
    return group


# ------------------------------------------------------------------------------
# 5) VWAP average probabilities across a group (by market liquidity)
# ------------------------------------------------------------------------------

def _latest_snapshot_for_market(market_id: str) -> Dict[str, Any] | None:
    res = (
        supabase.table("market_snapshots")
        .select("outcomes,ts")
        .eq("market_id", market_id)
        .order("ts", desc=True)
        .limit(1)
        .execute()
    )
    rows = getattr(res, "data", None) or []
    return rows[0] if rows else None


def vwap_across_markets(market_ids: List[str]) -> List[Dict[str, Any]]:
    if not market_ids:
        return []

    rows = (
        supabase.table("markets")
        .select("id,liquidity_usd")
        .in_("id", market_ids)
        .execute()
    ).data or []
    liq_by_id = {r["id"]: float(r.get("liquidity_usd") or 1.0) for r in rows}

    accum: Dict[str, Dict[str, float]] = {}  # label -> {"w": sum_w, "p": sum_w*p}
    for mid in market_ids:
        snap = _latest_snapshot_for_market(mid)
        if not snap:
            continue
        w = float(liq_by_id.get(mid, 1.0))
        for o in (snap.get("outcomes") or []):
            label = str(o.get("label"))
            # Support either 'prob' or 'mid' (normalized snapshot may use either)
            p = o.get("prob") if o.get("prob") is not None else o.get("mid")
            if p is None:
                continue
            d = accum.setdefault(label, {"w": 0.0, "p": 0.0})
            d["w"] += w
            d["p"] += w * float(p)

    out: List[Dict[str, Any]] = []
    for label, d in accum.items():
        if d["w"] <= 0:
            continue
        out.append({"label": label, "prob": d["p"] / d["w"]})
    return out


# ------------------------------------------------------------------------------
# 6) Persist a group row
# ------------------------------------------------------------------------------

def upsert_group(title: str, market_ids: List[str]) -> Dict[str, Any]:
    """
    Insert a new group row with computed VWAP avg_prob.

    NOTE: We purposely use insert() (not upsert) so each recompute can create a
    fresh group row (history). If you prefer a single canonical row per title,
    switch to .upsert(row, on_conflict="title") and add a unique index on title.
    """
    avg_prob = vwap_across_markets(market_ids)
    row = {
        "title": title,
        "market_ids": market_ids,
        "avg_prob": avg_prob,
        "arb_data": {},
        "analysis_data": {},
        # updated_at will default to now() from your schema
    }
    res = supabase.table("groups").insert(row).execute()
    return {"inserted": (getattr(res, "data", None) or [])}

