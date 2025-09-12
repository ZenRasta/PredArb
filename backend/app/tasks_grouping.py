# backend/app/tasks_grouping.py
from __future__ import annotations
from .celery_app import celery
from .db import supabase
from .grouping import compute_group_for_seed, upsert_group

@celery.task(name="grouping.recompute_for_market")
def recompute_for_market(market_id: str):
    seed = supabase.table("markets").select("title").eq("id", market_id).limit(1).execute().data
    title = (seed[0]["title"] if seed else "Group")
    mids = compute_group_for_seed(market_id)
    if mids:
        upsert_group(title, mids)
    return {"group_size": len(mids)}

@celery.task(name="grouping.recompute_all")
def recompute_all(limit: int = 200):
    seeds = supabase.table("markets").select("id,title").order("updated_at", desc=True).limit(limit).execute().data or []
    built = 0
    for s in seeds:
        mids = compute_group_for_seed(s["id"])
        if mids:
            upsert_group(s["title"], mids)
            built += 1
    return {"groups_built": built}
