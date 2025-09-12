# backend/app/grouping.py
from __future__ import annotations
from typing import List, Tuple, Dict, Any, Set
from datetime import datetime, timezone
from rapidfuzz import fuzz
import re

from .db import supabase, rds
# if you put shortlist_candidates in this file earlier, keep it; otherwise paste it now:

ENTITY_RE = re.compile(r"\b(Trump|Biden|Harris|BTC|ETH|Donald Trump|Joe Biden)\b", re.I)
TICKER_RE = re.compile(r"\b[A-Z]{2,6}\b")

def extract_entities(text: str) -> Set[str]:
    ents = set(m.group(0).lower() for m in ENTITY_RE.finditer(text or ""))
    ents |= set(m.group(0).lower() for m in TICKER_RE.finditer(text or ""))
    return ents

def shortlist_candidates(target: Dict, candidates: List[Dict], k: int = 50) -> List[Tuple[Dict, float]]:
    t_title = (target.get("title") or "").lower()
    t_ents  = extract_entities((target.get("title") or "") + " " + (target.get("description") or ""))
    t_end   = target.get("end_date")

    def end_ok(c_end):
        if not t_end or not c_end:
            return True
        try:
            t_dt = datetime.fromisoformat(str(t_end).replace("Z","+00:00")) if isinstance(t_end, str) else t_end
            c_dt = datetime.fromisoformat(str(c_end).replace("Z","+00:00")) if isinstance(c_end, str) else c_end
            return abs((t_dt - c_dt).days) <= 60
        except Exception:
            return True

    scored = []
    for c in candidates:
        if c["id"] == target["id"]:
            continue
        if not end_ok(c.get("end_date")):
            continue
        c_title = (c.get("title") or "").lower()
        c_ents  = extract_entities((c.get("title") or "") + " " + (c.get("description") or ""))
        if t_ents and c_ents and not (t_ents & c_ents):
            continue
        score = max(fuzz.token_sort_ratio(t_title, c_title), fuzz.partial_ratio(t_title, c_title))
        if score >= 70:
            scored.append((c, float(score)))
    scored.sort(key=lambda x: -x[1])
    return scored[:k]

# ---------- 2.2 Cosine via pgvector RPC ----------

def embedding_for_market_id(market_id: str) -> List[float] | None:
    res = supabase.table("embeddings").select("vector").eq("market_id", market_id).limit(1).execute()
    if not res.data:
        return None
    return res.data[0]["vector"]

def top_by_cosine(vector: List[float], limit: int = 50) -> List[Tuple[str, float]]:
    """
    Calls the Supabase RPC function 'match_markets' you already created:
      match_markets(query vector, match_limit int)
    Returns a list of (market_id, cosine_distance).
    """
    res = supabase.rpc("match_markets", {"query": vector, "match_limit": limit}).execute()
    out: List[Tuple[str, float]] = []
    for row in (res.data or []):
        out.append((row["market_id"], float(row["cos_dist"])))
    return out

# ---------- 2.3 Polarity/exclusivity & finalize groups ----------

def is_binary_market(market_id: str) -> bool:
    rows = supabase.table("market_outcomes").select("outcome_id").eq("market_id", market_id).execute().data or []
    return len(rows) == 2

def end_date_within(market_a: Dict, market_b: Dict, max_days=60) -> bool:
    a = market_a.get("end_date"); b = market_b.get("end_date")
    if not a or not b:
        return True
    try:
        a = datetime.fromisoformat(str(a).replace("Z","+00:00")) if isinstance(a, str) else a
        b = datetime.fromisoformat(str(b).replace("Z","+00:00")) if isinstance(b, str) else b
        return abs((a-b).days) <= max_days
    except Exception:
        return True

def apply_overrides(group_candidate: List[str]) -> List[str]:
    forced = set()
    removed = set()
    res = supabase.table("group_overrides").select("market_id,action").execute()
    for row in (res.data or []):
        if row["action"] == "include":
            forced.add(row["market_id"])
        elif row["action"] == "exclude":
            removed.add(row["market_id"])
    out = [m for m in group_candidate if m not in removed]
    out = list(set(out) | forced)
    return out

def compute_group_for_seed(seed_market_id: str) -> List[str]:
    # seed & pool
    seed_q = supabase.table("markets").select("*").eq("id", seed_market_id).limit(1).execute().data
    if not seed_q:
        return []
    seed = seed_q[0]

    pool = supabase.table("markets").select("id,title,description,end_date,platform").order("updated_at", desc=True).limit(1000).execute().data or []
    short = shortlist_candidates(seed, pool, k=120)

    vec = embedding_for_market_id(seed_market_id)
    if vec is None:
        # No embedding yet; return seed only
        return [seed_market_id]

    top_cos = top_by_cosine(vec, limit=100)
    top_ids = {mid for (mid, dist) in top_cos}

    inter: List[str] = []
    for cand, _score in short:
        if cand["id"] in top_ids and end_date_within(seed, cand, 60):
            inter.append(cand["id"])

    group = list(set([seed_market_id] + inter))
    group = apply_overrides(group)
    return group

# ---------- 2.4 VWAP avg_prob across markets ----------

def latest_snapshot_for_market(market_id: str) -> Dict | None:
    res = supabase.table("market_snapshots").select("outcomes,ts").eq("market_id", market_id).order("ts", desc=True).limit(1).execute()
    if not res.data:
        return None
    return res.data[0]

def vwap_across_markets(market_ids: List[str]) -> List[Dict[str, Any]]:
    rows = supabase.table("markets").select("id,liquidity_usd").in_("id", market_ids).execute().data or []
    lid = {r["id"]: (r.get("liquidity_usd") or 1.0) for r in rows}

    accum: Dict[str, Dict[str, float]] = {}  # label -> {"w":sum_w, "p":sum_w*p}
    for mid in market_ids:
        snap = latest_snapshot_for_market(mid)
        if not snap:
            continue
        w = float(lid.get(mid, 1.0))
        for o in (snap["outcomes"] or []):
            label = str(o.get("label"))
            p = o.get("prob") if o.get("prob") is not None else o.get("mid")
            if p is None:
                continue
            d = accum.setdefault(label, {"w": 0.0, "p": 0.0})
            d["w"] += w
            d["p"] += w * float(p)

    out = []
    for label, d in accum.items():
        if d["w"] <= 0:
            continue
        out.append({"label": label, "prob": d["p"]/d["w"]})
    return out

# ---------- 2.5 Persist a group row ----------

def upsert_group(title: str, market_ids: List[str]) -> None:
    avg_prob = vwap_across_markets(market_ids)
    row = {
        "title": title,
        "market_ids": market_ids,
        "avg_prob": avg_prob,
        "arb_data": {},
        "analysis_data": {},
    }
    supabase.table("groups").insert(row).execute()

