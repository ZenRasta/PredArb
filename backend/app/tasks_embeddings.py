from __future__ import annotations
from .celery_app import celery
from .embeddings import fetch_markets_without_embeddings, embed_and_upsert

@celery.task(name="embeddings.embed_new_markets")
def embed_new_markets(limit: int = 500):
    items = fetch_markets_without_embeddings(limit=limit)
    count = embed_and_upsert(items, batch=128)
    return {"embedded": count}

