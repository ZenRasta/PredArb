from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict
import os
import httpx

from .celery_app import celery
from .db import supabase
from .settings import settings

log = logging.getLogger(__name__)

ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "300"))
ALERT_MIN_EV_CHANGE = float(os.getenv("ALERT_MIN_EV_CHANGE", "1.0"))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def send_telegram_message(chat_id: str, text: str, token: str | None = None) -> None:
    """Send a Telegram message using HTTP API."""
    token = token or settings.telegram_bot_token
    if not token:
        log.info("No TELEGRAM_BOT_TOKEN; skipping send")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        httpx.post(url, data={"chat_id": chat_id, "text": text}, timeout=10)
    except Exception as e:  # pragma: no cover - network failure
        log.warning("Telegram send failed: %s", e)


@celery.task(name="alerts.process_queue")
def process_alerts_queue(limit: int = 100,
                         cooldown_sec: int = ALERT_COOLDOWN_SEC,
                         min_ev_change: float = ALERT_MIN_EV_CHANGE) -> Dict[str, Any]:
    """Process pending alert rows and send Telegram notifications."""
    if not supabase:
        log.info("No Supabase client; skipping alerts")
        return {"sent": 0, "skipped": 0}
    rows = (
        supabase
        .table("alerts_queue")
        .select("*")
        .eq("status", "pending")
        .limit(limit)
        .execute()
        .data
        or []
    )
    sent = 0
    skipped = 0
    now = _now()
    for row in rows:
        arb_id = row.get("arb_id")
        user_id = row.get("user_id")
        if not arb_id or not user_id:
            skipped += 1
            continue
        opp = (
            supabase
            .table("arb_opportunities")
            .select("metrics")
            .eq("id", arb_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        if not opp:
            skipped += 1
            continue
        metrics = opp[0].get("metrics") or {}
        ev = float(metrics.get("ev_usd") or 0.0)
        last_sent_str = row.get("sent_at") or row.get("last_sent")
        last_sent = None
        if last_sent_str:
            try:
                last_sent = datetime.fromisoformat(last_sent_str)
            except Exception:
                pass
        last_val = row.get("last_value")
        if last_sent and (now - last_sent).total_seconds() < cooldown_sec:
            continue
        if last_val is not None and abs(ev - float(last_val)) < min_ev_change:
            continue
        text = f"Opportunity EV ${ev:.2f}"
        send_telegram_message(user_id, text)
        supabase.table("alerts_queue").update({
            "status": "sent",
            "sent_at": now.isoformat(),
            "last_value": ev,
        }).eq("id", row.get("id")).execute()
        sent += 1
    return {"sent": sent, "skipped": skipped}
