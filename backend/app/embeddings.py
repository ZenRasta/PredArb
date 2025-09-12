from __future__ import annotations
from typing import Dict, List, Tuple
from sentence_transformers import SentenceTransformer
from dataclasses import asdict
import numpy as np
import time
from .db import supabase, rds
from .settings import settings

# Lazy-load model
_model = None
def get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer(settings.embeddings_model)
    return _model

def build_embed_text(market_row: Dict) -> str:
    """
    Build a compact textual representation for the embedding.
    """
    title = (market_row.get("title") or "").strip()
    desc  = (market_row.get("description") or "").strip()
    outs  = market_row.get("outcomes") or []
    out_labels = [o.get("label") for o in outs if isinstance(o, dict)]
    end   = market_row.get("end_date") or ""
    # Add a tiny schema for stability
    text = f"query: {title}\nend:{end}\noutcomes:{', '.join(out_labels)}\ndesc:{desc}"
    return text

def fetch_markets_without_embeddings(limit=1000) -> List[Dict]:
    """
    Get markets that don't have an embedding yet or were updated later than last embedding.
    For simplicity here: just fetch recent markets and check if there's an embedding row.
    """
    # Grab recent markets (you may want to page by updated_at)
    res = supabase.table("markets").select("id,platform,event_id,title,description,end_date,updated_at").limit(limit).execute()
    items = res.data or []
    # Filter by no existing embedding
    out = []
    for m in items:
        eid = m["id"]
        e = supabase.table("embeddings").select("id").eq("market_id", eid).limit(1).execute()
        if not e.data:
            out.append(m)
    return out

def embed_and_upsert(markets: List[Dict], batch: int = 128):
    if not markets:
        return 0
    model = get_model()
    texts = [build_embed_text(m) for m in markets]
    vecs = model.encode(texts, batch_size=batch, normalize_embeddings=True)  # cosine-friendly
    # insert
    payload = []
    for m, v in zip(markets, vecs):
        payload.append({
            "market_id": m["id"],
            "vector": list(map(float, v)),
        })
    # use upsert insert-many; supabase-py: pass list directly
    for chunk_idx in range(0, len(payload), 500):
        supabase.table("embeddings").insert(payload[chunk_idx:chunk_idx+500]).execute()
    return len(payload)
