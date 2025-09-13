from __future__ import annotations

from exchanges.polymarket import PolymarketExchange
from exchanges.limitless import LimitlessExchange
from app.types import MarketNormalized, SnapshotNormalized, OutcomeQuote


def test_polymarket_normalization():
    raw_market = {
        "id": "pm1",
        "question": "Will it rain tomorrow?",
        "description": "Weather forecast",
        "endDate": "2024-12-31T00:00:00Z",
        "slug": "rain-tomorrow",
        "volume": "1234.5",
        "liquidity": "1000",
        "outcomes": [
            {"id": "0", "name": "YES", "price": 0.6},
            {"id": "1", "name": "NO", "price": 0.4},
        ],
        "isResolved": False,
    }
    ex = PolymarketExchange()
    m = ex.normalize_market(raw_market)
    assert isinstance(m, MarketNormalized)
    assert m.platform == "polymarket"
    assert m.event_id == "pm1"
    assert len(m.outcomes) == 2
    assert m.outcomes[0]["label"] == "YES"

    raw_snapshot = {
        "outcomes": [
            {"id": "0", "name": "YES", "bid": "0.59", "ask": "0.61", "price": 0.6, "max_qty": 100},
            {"id": "1", "name": "NO", "bid": 0.39, "ask": 0.41, "price": 0.4, "max_qty": 110},
        ],
        "liquidity": 500,
        "fees": {"maker": 0.02},
    }
    snap = ex.normalize_snapshot("pm1", raw_snapshot)
    assert isinstance(snap, SnapshotNormalized)
    assert snap.market_event_id == "pm1"
    assert len(snap.outcomes) == 2
    assert isinstance(snap.outcomes[0], OutcomeQuote)
    assert snap.outcomes[0].bid == 0.59


def test_limitless_normalization():
    raw_market = {
        "id": "ll1",
        "question": "Will Team A win?",
        "description": "Sports",
        "resolveDate": "2024-10-01T12:00:00Z",
        "status": "trading",
        "volume": 200,
        "liquidity": 150,
        "category": "sports",
        "outcomes": [
            {"id": "0", "name": "YES", "prob": 0.55},
            {"id": "1", "name": "NO", "prob": 0.45},
        ],
    }
    ex = LimitlessExchange()
    m = ex.normalize_market(raw_market)
    assert isinstance(m, MarketNormalized)
    assert m.platform == "limitless"
    assert m.event_id == "ll1"
    assert m.status == "trading"
    assert len(m.outcomes) == 2

    raw_snapshot = {
        "timestamp": 1700000000,
        "outcomes": [
            {"id": "0", "name": "YES", "bid": 0.54, "ask": 0.56, "prob": 0.55, "liquidity": 80},
            {"id": "1", "name": "NO", "bid": 0.44, "ask": 0.46, "prob": 0.45, "liquidity": 90},
        ],
        "liquidity": 160,
        "fees": {"taker": 0.03},
    }
    snap = ex.normalize_snapshot("ll1", raw_snapshot)
    assert isinstance(snap, SnapshotNormalized)
    assert snap.market_event_id == "ll1"
    assert len(snap.outcomes) == 2
    assert snap.outcomes[1].ask == 0.46
