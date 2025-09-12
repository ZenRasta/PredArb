# backend/app/tasks.py
from __future__ import annotations

import logging
from .celery_app import celery
from .tasks_analysis import compute_opportunities 

log = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Minimal heartbeat so you can verify the worker is alive
# -------------------------------------------------------------------
@celery.task(name="app.tasks.heartbeat")
def heartbeat():
    return {"ok": True}

# -------------------------------------------------------------------
# Force-import task modules so Celery sees their @celery.task decorators
# -------------------------------------------------------------------
def _safe_import(module_path: str) -> None:
    """
    Import task modules by path (relative to 'app.') and log the outcome.
    This ensures Celery registers the tasks defined in those files.
    """
    try:
        __import__(f"app.{module_path}", fromlist=["*"])
        log.info("Imported task module: app.%s", module_path)
    except Exception as e:  # pragma: no cover
        log.warning("Could not import task module app.%s: %s", module_path, e)

# Import the actual task modules present in this repo
_safe_import("tasks_ingest")       # ingest.fetch_markets/write_markets/write_snapshots
_safe_import("tasks_embeddings")   # embeddings.embed_new_markets
_safe_import("tasks_grouping")     # grouping.recompute_* tasks

# Optional: make linters happy by referencing symbols if present.
# (No runtime effect; tasks are registered by the imports above.)
try:
    from .tasks_ingest import fetch_markets, write_markets, write_snapshots  # noqa: F401
except Exception:
    pass
try:
    from .tasks_grouping import recompute_for_market, recompute_all  # noqa: F401
except Exception:
    pass
try:
    from .tasks_embeddings import embed_new_markets  # noqa: F401
except Exception:
    pass
