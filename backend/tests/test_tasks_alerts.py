from __future__ import annotations

import types
from datetime import datetime, timedelta, timezone

import pytest

from app import tasks_alerts as ta


class FakeSupabase:
    def __init__(self, alerts_rows, opps):
        self.alerts_rows = alerts_rows
        self.opps = opps

    def table(self, name):
        if name == "alerts_queue":
            return FakeAlertsTable(self)
        if name == "arb_opportunities":
            return FakeOppTable(self)
        raise KeyError(name)


class FakeAlertsTable:
    def __init__(self, parent):
        self.parent = parent
        self.mode = "select"
        self.filters = {}
        self.payload = None
        self.limit_n = None

    def select(self, *_):
        self.mode = "select"
        return self

    def update(self, payload):
        self.mode = "update"
        self.payload = payload
        return self

    def eq(self, col, val):
        self.filters[col] = val
        return self

    def limit(self, n):
        self.limit_n = n
        return self

    def execute(self):
        if self.mode == "select":
            rows = [r for r in self.parent.alerts_rows if all(r.get(k) == v for k, v in self.filters.items())]
            if self.limit_n is not None:
                rows = rows[: self.limit_n]
            return types.SimpleNamespace(data=rows)
        elif self.mode == "update":
            for r in self.parent.alerts_rows:
                if all(r.get(k) == v for k, v in self.filters.items()):
                    r.update(self.payload)
            return types.SimpleNamespace(data=[])
        raise RuntimeError("invalid mode")


class FakeOppTable:
    def __init__(self, parent):
        self.parent = parent
        self.filters = {}
        self.limit_n = None

    def select(self, *_):
        return self

    def eq(self, col, val):
        self.filters[col] = val
        return self

    def limit(self, n):
        self.limit_n = n
        return self

    def execute(self):
        opp = self.parent.opps.get(self.filters.get("id"))
        rows = [opp] if opp else []
        return types.SimpleNamespace(data=rows)


def test_process_queue_sends_and_updates(monkeypatch):
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(ta, "_now", lambda: now)
    alerts = [{"id": "a1", "user_id": "u1", "arb_id": "o1", "status": "pending"}]
    opps = {"o1": {"metrics": {"ev_usd": 10.0}}}
    fake = FakeSupabase(alerts, opps)
    monkeypatch.setattr(ta, "supabase", fake)
    sent = []
    monkeypatch.setattr(ta, "send_telegram_message", lambda uid, text, token=None: sent.append((uid, text)))
    monkeypatch.setattr(ta, "settings", types.SimpleNamespace(telegram_bot_token="T"))

    res = ta.process_alerts_queue(limit=10, cooldown_sec=0, min_ev_change=0)
    assert res["sent"] == 1
    assert alerts[0]["status"] == "sent"
    assert alerts[0]["last_value"] == 10.0
    assert sent and sent[0][0] == "u1"


def test_process_queue_respects_cooldown(monkeypatch):
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(ta, "_now", lambda: now)
    alerts = [{
        "id": "a1",
        "user_id": "u1",
        "arb_id": "o1",
        "status": "pending",
        "sent_at": (now - timedelta(seconds=30)).isoformat(),
        "last_value": 5.0,
    }]
    opps = {"o1": {"metrics": {"ev_usd": 6.0}}}
    fake = FakeSupabase(alerts, opps)
    monkeypatch.setattr(ta, "supabase", fake)
    sent = []
    monkeypatch.setattr(ta, "send_telegram_message", lambda *args, **kwargs: sent.append(1))
    monkeypatch.setattr(ta, "settings", types.SimpleNamespace(telegram_bot_token="T"))

    res = ta.process_alerts_queue(limit=10, cooldown_sec=60, min_ev_change=0)
    assert res["sent"] == 0
    assert alerts[0]["status"] == "pending"
    assert not sent


def test_process_queue_requires_value_change(monkeypatch):
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(ta, "_now", lambda: now)
    alerts = [{
        "id": "a1",
        "user_id": "u1",
        "arb_id": "o1",
        "status": "pending",
        "sent_at": (now - timedelta(seconds=120)).isoformat(),
        "last_value": 10.0,
    }]
    opps = {"o1": {"metrics": {"ev_usd": 10.2}}}
    fake = FakeSupabase(alerts, opps)
    monkeypatch.setattr(ta, "supabase", fake)
    sent = []
    monkeypatch.setattr(ta, "send_telegram_message", lambda *a, **k: sent.append(1))
    monkeypatch.setattr(ta, "settings", types.SimpleNamespace(telegram_bot_token="T"))

    res = ta.process_alerts_queue(limit=10, cooldown_sec=60, min_ev_change=1.0)
    assert res["sent"] == 0
    assert alerts[0]["status"] == "pending"
    assert not sent
