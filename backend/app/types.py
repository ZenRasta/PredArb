from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class OutcomeQuote:
    outcome_id: str
    label: str
    bid: Optional[float] = None
    ask: Optional[float] = None
    prob: Optional[float] = None
    max_fill: Optional[float] = None
    depth: Optional[Dict[str, Any]] = None


@dataclass
class MarketNormalized:
    platform: str
    event_id: str
    title: str
    description: Optional[str] = None
    end_date: Optional[datetime] = None
    status: Optional[str] = None
    volume_usd: Optional[float] = None
    liquidity_usd: Optional[float] = None
    outcomes: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None
    raw: Optional[Dict[str, Any]] = None


@dataclass
class SnapshotNormalized:
    market_event_id: str
    ts: datetime
    outcomes: List[OutcomeQuote]
    price_source: Optional[str] = None
    liquidity_usd: Optional[float] = None
    fees: Optional[Dict[str, Any]] = None
    stale_seconds: Optional[int] = None
    checksum: Optional[str] = None
