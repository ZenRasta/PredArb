# backend/app/celery_app.py
from __future__ import annotations

import os
from celery import Celery

# -------------------------------------------------------------------
# Environment / Defaults
# -------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

# Cadences (seconds) – override in env as needed
PM_FETCH_SEC = int(os.getenv("PM_FETCH_SEC", "60"))
LL_FETCH_SEC = int(os.getenv("LL_FETCH_SEC", "75"))

EMBED_NEW_MARKETS_SEC   = int(os.getenv("EMBED_NEW_MARKETS_SEC", "600"))   # 10 min
GROUP_RECOMPUTE_ALL_SEC = int(os.getenv("GROUP_RECOMPUTE_ALL_SEC", "900")) # 15 min
HEARTBEAT_SEC           = int(os.getenv("HEARTBEAT_SEC", "30"))
ANALYSIS_COMPUTE_SEC    = int(os.getenv("ANALYSIS_COMPUTE_SEC", "90"))
ALERTS_POLL_SEC         = int(os.getenv("ALERTS_POLL_SEC", "60"))

# Tunables for batch sizes
PM_FETCH_LIMIT   = int(os.getenv("PM_FETCH_LIMIT", "300"))
LL_FETCH_LIMIT   = int(os.getenv("LL_FETCH_LIMIT", "300"))
PM_SNAPSHOT_MAX  = int(os.getenv("PM_SNAPSHOT_MAX", "120"))
LL_SNAPSHOT_MAX  = int(os.getenv("LL_SNAPSHOT_MAX", "120"))
EMBED_LIMIT      = int(os.getenv("EMBED_LIMIT", "800"))
GROUP_RECOMPUTE_LIMIT = int(os.getenv("GROUP_RECOMPUTE_LIMIT", "200"))
ANALYSIS_MAX_GROUPS   = int(os.getenv("ANALYSIS_MAX_GROUPS", "200"))

# -------------------------------------------------------------------
# Celery App
# -------------------------------------------------------------------
celery = Celery(
    "predarb",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

# Core config
celery.conf.update(
    timezone="UTC",
    enable_utc=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,                 # 1h result retention
    worker_prefetch_multiplier=1,        # better fairness with external rate limits
    task_acks_late=True,                 # re-queue on crash
    task_reject_on_worker_lost=True,
    task_default_queue="default",
    task_routes={
        # Ingestion IO
        "ingest.fetch_markets":   {"queue": "ingest"},
        "ingest.write_markets":   {"queue": "ingest"},
        "ingest.write_snapshots": {"queue": "snapshots"},
        "ingest.one_shot":        {"queue": "ingest"},  # wrapper chain
        "ingest.kick":            {"queue": "ingest"},  # optional wrapper

        # Embeddings & grouping
        "embeddings.embed_new_markets": {"queue": "embeddings"},
        "grouping.recompute_for_market": {"queue": "grouping"},
        "grouping.recompute_all":        {"queue": "grouping"},

        # Analysis & alerts
        "analysis.compute_opportunities": {"queue": "analysis"},
        "alerts.process_queue":           {"queue": "alerts"},

        # Health
        "app.tasks.heartbeat": {"queue": "default"},
    },
    # Optional: soft rate limits (we also use a Redis token bucket in-code)
    task_annotations={
        "ingest.fetch_markets":   {"rate_limit": "120/m"},
        "ingest.write_snapshots": {"rate_limit": "300/m"},
    },
    # ----------------------------------------------------------------
    # Explicit imports so Celery eagerly registers tasks
    # ----------------------------------------------------------------
    imports=[
        "app.tasks",             # heartbeat
        "app.tasks_ingest",      # fetch/write/snapshots + one_shot/kick
        "app.tasks_embeddings",  # embeddings.embed_new_markets
        "app.tasks_grouping",    # grouping.*
        "app.tasks_analysis",    # analysis.compute_opportunities
        "app.tasks_alerts",      # alerts.process_queue
    ],
)

# -------------------------------------------------------------------
# Beat Schedule
#   IMPORTANT: Use the one-shot wrapper per platform to avoid calling
#   write_markets bare (which requires the fetch payload).
# -------------------------------------------------------------------
celery.conf.beat_schedule = {
    # --- Ingest (one-shot chains) ---
    "ingest-one-shot-polymarket": {
        "task": "ingest.one_shot",
        "schedule": PM_FETCH_SEC,
        "args": ("polymarket", PM_FETCH_LIMIT, PM_SNAPSHOT_MAX),
        "options": {"queue": "ingest"},
    },
    "ingest-one-shot-limitless": {
        "task": "ingest.one_shot",
        "schedule": LL_FETCH_SEC,
        "args": ("limitless", LL_FETCH_LIMIT, LL_SNAPSHOT_MAX),
        "options": {"queue": "ingest"},
    },

    # --- Embeddings (Session 3) ---
    "embed-new-markets": {
        "task": "embeddings.embed_new_markets",
        "schedule": EMBED_NEW_MARKETS_SEC,
        "args": (EMBED_LIMIT,),
        "options": {"queue": "embeddings"},
    },

    # --- Grouping (Session 3) ---
    "grouping-recompute-all": {
        "task": "grouping.recompute_all",
        "schedule": GROUP_RECOMPUTE_ALL_SEC,
        "args": (GROUP_RECOMPUTE_LIMIT,),
        "options": {"queue": "grouping"},
    },

    # --- Analysis (Session 4) ---
    "analysis-compute-opps": {
        "task": "analysis.compute_opportunities",
        "schedule": ANALYSIS_COMPUTE_SEC,
        "args": (ANALYSIS_MAX_GROUPS,),
        "options": {"queue": "analysis"},
    },

    # --- Alerts ---
    "alerts-process-queue": {
        "task": "alerts.process_queue",
        "schedule": ALERTS_POLL_SEC,
        "args": (),
        "options": {"queue": "alerts"},
    },

    # --- Heartbeat / health ---
    "heartbeat": {
        "task": "app.tasks.heartbeat",
        "schedule": HEARTBEAT_SEC,
        "args": (),
        "options": {"queue": "default"},
    },
}

# -------------------------------------------------------------------
# Autodiscover (redundant but harmless)
# -------------------------------------------------------------------
celery.autodiscover_tasks(["app"])

