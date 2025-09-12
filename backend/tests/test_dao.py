from __future__ import annotations
from dataclasses import asdict
from datetime import datetime, timezone
import types
import pytest

# Import the functions and dataclasses under test
from app import dao as dao_mod
from app.types import MarketNormalized, SnapshotNormalized, OutcomeQuote

# ---------------------------
# Fake Supabase client
# ---------------------------

class _ExecuteResult:
    def __init__(self, data=None):
        self.data = data or []

class FakeTable:
    def __init__(self, root, name: str):
        self._root = root
        self._name = name
        self._query = {"select": None, "filters": [], "limit": None}
        self._last_upsert_payload = None
        self._last_insert_payload = None
        self._on_conflict = None

    # write operations
    def upsert(self, payload, on_conflict: str | None = None):
        self._on_conflict = on_conflict
        self._last_upsert_payload = payload

        if self._name == "markets":
            # emulate upsert: use (platform,event_id) as uniqueness
            out_rows = []
            for row in payload:
                key = (row["platform"], row["event_id"])
                if key in self._root.markets_index:
                    # update existing
                    mk_id = self._root.markets_index[key]
                    stored = self._root.tables["markets"][mk_id]
                    stored.update(dict(row))  # overwrite fields
                else:
                    mk_id = f"mk_{len(self._root.tables['markets'])+1}"
                    self._root.markets_index[key] = mk_id
                    self._root.tables["markets"][mk_id] = {"id": mk_id, **row}
                out_rows.append({"id": mk_id, "platform": row["platform"], "event_id": row["event_id"]})
            self._root.last_upsert_markets = out_rows
        elif self._name == "market_outcomes":
            for row in payload:
                self._root.tables["market_outcomes"].append(dict(row))
        else:
            # other tables: just store payload for visibility
            pass
        return self

    def insert(self, payload):
        self._last_insert_payload = payload
        if self._name == "market_snapshots":
            if isinstance(payload, list):
                rows = payload
            else:
                rows = [payload]
            for row in rows:
                snap_id = len(self._root.tables["market_snapshots"]) + 1
                self._root.tables["market_snapshots"].append({"id": snap_id, **row})
        elif self._name == "market_outcomes":
            if isinstance(payload, list):
                rows = payload
            else:
                rows = [payload]
            for row in rows:
                self._root.tables["market_outcomes"].append(dict(row))
        return self

    # read/query chain
    def select(self, cols: str):
        self._query["select"] = cols
        return self

    def eq(self, col: str, val):
        self._query["filters"].append(("eq", col, val))
        return self

    def limit(self, n: int):
        self._query["limit"] = n
        return self

    def execute(self):
        name = self._name
        if name == "markets":
            # scan markets and apply filters
            rows = list(self._root.tables["markets"].values())
            for (_, col, val) in self._query["filters"]:
                rows = [r for r in rows if r.get(col) == val]
            if self._query["limit"] is not None:
                rows = rows[: self._query["limit"]]
            # Return only selected columns if asked
            sel = self._query["select"]
            if sel:
                cols = [c.strip() for c in sel.split(",")]
                rows = [{k: r.get(k) for k in cols} for r in rows]
            return _ExecuteResult(rows)
        elif name in {"market_outcomes", "market_snapshots"}:
            # simple echo for tests
            return _ExecuteResult(list(self._root.tables[name]))
        else:
            return _ExecuteResult([])

class FakeSupabase:
    def __init__(self):
        self.tables = {
            "markets": {},             # id -> row
            "market_outcomes": [],     # list of rows
            "market_snapshots": [],    # list of rows
        }
        self.markets_index = {}        # (platform,event_id) -> id
        self.last_upsert_markets = []

    def table(self, name: str) -> FakeTable:
        return FakeTable(self, name)

# ---------------------------
# Fixtures
# ---------------------------

@pytest.fixture(autouse=True)
def patch_supabase(monkeypatch):
    """
    Replace app.dao.supabase with our FakeSupabase for all tests.
    """
    fake = FakeSupabase()
    monkeypatch.setattr(dao_mod, "supabase", fake, raising=True)
    return fake

# ---------------------------
# Helpers to build dataclasses
# ---------------------------

def mk_market(platform="polymarket", event_id="E1", title="Test", outcomes=None):
    return MarketNormalized(
        platform=platform,
        event_id=event_id,
        title=title,
        description="",
        end_date=None,
        status="open",
        volume_usd=123.0,
        liquidity_usd=45.0,
        metadata={"slug": "test-slug"},
        outcomes=outcomes or [
            {"outcome_id": "YES", "label": "YES", "prob": 0.55},
            {"outcome_id": "NO", "label": "NO", "prob": 0.45},
        ],
        raw={"dummy": True},
    )

def mk_snapshot(event_id="E1", platform="polymarket"):
    now = datetime.now(tz=timezone.utc)
    return SnapshotNormalized(
        market_event_id=event_id,
        ts=now,
        price_source="api",
        outcomes=[
            OutcomeQuote(outcome_id="YES", label="YES", prob=0.6, bid=0.59, ask=0.61, max_fill=100.0, depth={"bids": [], "asks": []}),
            OutcomeQuote(outcome_id="NO",  label="NO",  prob=0.4, bid=0.39, ask=0.41, max_fill=110.0, depth={"bids": [], "asks": []}),
        ],
        liquidity_usd=50.0,
        fees={"_platform_hint": platform},
        stale_seconds=2,
        checksum="abc123",
    )

# ---------------------------
# Tests
# ---------------------------

def test_upsert_markets_and_outcomes_inserts_and_updates(patch_supabase):
    # initial upsert
    m1 = mk_market(platform="polymarket", event_id="PM-1", title="Foo")
    m2 = mk_market(platform="limitless", event_id="LL-1", title="Bar",
                   outcomes=[{"outcome_id":"0","label":"YES","prob":0.51},{"outcome_id":"1","label":"NO","prob":0.49}])

    result = dao_mod.upsert_markets_and_outcomes([m1, m2])

    # verify markets stored
    assert len(patch_supabase.tables["markets"]) == 2
    ids = [mk_id for _, mk_id in result]
    assert len(ids) == 2
    # outcomes inserted
    assert len(patch_supabase.tables["market_outcomes"]) == 4
    # repeat upsert with a changed title to ensure update path works
    m1_updated = mk_market(platform="polymarket", event_id="PM-1", title="Foo UPDATED")
    dao_mod.upsert_markets_and_outcomes([m1_updated])
    # confirm the stored row updated
    mk_id = patch_supabase.markets_index[("polymarket", "PM-1")]
    assert patch_supabase.tables["markets"][mk_id]["title"] == "Foo UPDATED"

def test_insert_snapshot_resolves_market_id_and_writes_row(patch_supabase):
    # seed a market so snapshot can resolve market_id
    m = mk_market(platform="polymarket", event_id="PM-2", title="Baz")
    dao_mod.upsert_markets_and_outcomes([m])

    # insert snapshot
    snap = mk_snapshot(event_id="PM-2", platform="polymarket")
    dao_mod.insert_snapshot(snap)

    snaps = patch_supabase.tables["market_snapshots"]
    assert len(snaps) == 1
    stored = snaps[0]
    assert stored["market_id"] is not None
    assert stored["price_source"] == "api"
    assert isinstance(stored["outcomes"], list) and len(stored["outcomes"]) == 2
    # check mid synonym and structure
    yes_row = next(o for o in stored["outcomes"] if o["label"] == "YES")
    assert yes_row["prob"] == pytest.approx(0.6)
    assert yes_row["mid"]  == pytest.approx(0.6)
    assert yes_row["bid"]  == pytest.approx(0.59)
    assert yes_row["ask"]  == pytest.approx(0.61)
    assert yes_row["max_fill"] == pytest.approx(100.0)

def test_insert_snapshot_skips_when_market_missing(patch_supabase, monkeypatch):
    # Do NOT seed market; snapshot should be skipped gracefully
    snap = mk_snapshot(event_id="UNKNOWN", platform="polymarket")
    # Make sure dao won't find a market
    dao_mod.insert_snapshot(snap)
    assert len(patch_supabase.tables["market_snapshots"]) == 0

def test_multiple_outcomes_inserted(patch_supabase):
    # create multi-outcome market
    m = mk_market(platform="limitless", event_id="LL-7", title="Multi",
                  outcomes=[
                      {"outcome_id":"A","label":"A","prob":0.2},
                      {"outcome_id":"B","label":"B","prob":0.5},
                      {"outcome_id":"C","label":"C","prob":0.3},
                  ])
    dao_mod.upsert_markets_and_outcomes([m])
    # outcomes should be 3 for this market
    outs = [o for o in patch_supabase.tables["market_outcomes"] if o["market_id"]]
    assert len(outs) == 3

