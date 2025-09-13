from __future__ import annotations

import types
import pytest

from app import tasks_analysis as ta


def test_compute_opportunities_inserts_dutch_book(monkeypatch):
    inserted = []

    class FakeTable:
        def __init__(self, name: str):
            self.name = name
        def insert(self, payload):
            inserted.append(payload)
            return self
        def execute(self):
            return types.SimpleNamespace(data=[{"id": "arb1"}])
    class FakeSupabase:
        def table(self, name: str):
            assert name == "arb_opportunities"
            return FakeTable(name)
    monkeypatch.setattr(ta, "supabase", FakeSupabase())

    monkeypatch.setattr(ta, "_load_platform_fees", lambda: {"A": {"taker_bps": 0}, "B": {"taker_bps": 0}})
    monkeypatch.setattr(ta, "_recent_groups", lambda limit=200: [{"id": "g1", "market_ids": ["m1", "m2"]}])
    now = ta._now_ts()
    def latest(mid: str):
        if mid == "m1":
            return {"ts": now, "outcomes": [{"label": "YES", "mid": 0.4}, {"label": "NO", "mid": 0.6}], "fees": {"_platform_hint": "A"}}
        return {"ts": now, "outcomes": [{"label": "YES", "mid": 0.45}, {"label": "NO", "mid": 0.55}], "fees": {"_platform_hint": "B"}}
    monkeypatch.setattr(ta, "_latest_snapshot", latest)
    monkeypatch.setattr(ta, "_fillable_usd", lambda snap: 100.0)
    monkeypatch.setattr(ta, "_fanout_alerts_for_users", lambda *args, **kwargs: 0)
    orig_build = ta._build_dutch_book
    monkeypatch.setattr(ta, "_build_dutch_book", lambda g, f: orig_build(g, f, size_candidates=(100.0,)))

    res = ta.compute_opportunities(max_groups=1, write_dutch=True, write_mispricing=False, min_ev_usd_alert=9999)
    assert res["inserted"] == 1
    assert len(inserted) == 1
    row = inserted[0]
    assert row["opp_type"] == "dutch_book"
    assert row["metrics"]["ev_usd"] == pytest.approx(5.0)
    assert row["metrics"]["edge_bps"] == 500
