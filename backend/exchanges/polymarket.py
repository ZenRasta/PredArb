from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from .base import BaseExchange
from app.types import MarketNormalized, SnapshotNormalized, OutcomeQuote


class PolymarketExchange(BaseExchange):
    """Exchange adapter for the Polymarket platform."""

    platform = "polymarket"
    base_url = "https://clob.polymarket.com"

    def fetch_active_markets(self) -> List[Dict[str, Any]]:
        """Fetch list of active markets from Polymarket.

        Returns the raw JSON payload from the API.
        """

        self._acquire_token("markets", limit=5, period=1)
        resp = self.session.get(f"{self.base_url}/markets", params={"active": "true"})
        resp.raise_for_status()
        return resp.json()

    def fetch_orderbook_or_amm_params(self, market_id: str) -> Dict[str, Any]:
        """Fetch the orderbook for a given market."""

        self._acquire_token("orderbook", limit=5, period=1)
        resp = self.session.get(f"{self.base_url}/markets/{market_id}/orderbook")
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Normalization helpers
    # ------------------------------------------------------------------
    def normalize_market(self, raw: Dict[str, Any]) -> MarketNormalized:
        outcomes = []
        for out in raw.get("outcomes", []):
            outcomes.append(
                {
                    "outcome_id": str(out.get("id") or out.get("token_id")),
                    "label": out.get("name") or out.get("title"),
                    "prob": _to_float(out.get("price")),
                }
            )

        end_date = _parse_date(raw.get("end_date") or raw.get("endDate"))
        status = raw.get("status")
        if status is None:
            status = "resolved" if raw.get("isResolved") else "open"

        return MarketNormalized(
            platform=self.platform,
            event_id=str(raw.get("id")),
            title=raw.get("question") or raw.get("title", ""),
            description=raw.get("description"),
            end_date=end_date,
            status=status,
            volume_usd=_to_float(raw.get("volume")),
            liquidity_usd=_to_float(raw.get("liquidity")),
            outcomes=outcomes,
            metadata={"slug": raw.get("slug")},
            raw=raw,
        )

    def normalize_snapshot(self, market_id: str, raw: Dict[str, Any]) -> SnapshotNormalized:
        ts = datetime.now(tz=timezone.utc)
        outcomes: List[OutcomeQuote] = []
        for out in raw.get("outcomes", []):
            outcomes.append(
                OutcomeQuote(
                    outcome_id=str(out.get("id")),
                    label=out.get("name") or out.get("label"),
                    bid=_to_float(out.get("bid")),
                    ask=_to_float(out.get("ask")),
                    prob=_to_float(out.get("price") or out.get("prob")),
                    max_fill=_to_float(out.get("max_qty") or out.get("maxQty")),
                    depth=out.get("depth"),
                )
            )

        return SnapshotNormalized(
            market_event_id=str(market_id),
            ts=ts,
            outcomes=outcomes,
            price_source="orderbook",
            liquidity_usd=_to_float(raw.get("liquidity")),
            fees=raw.get("fees"),
            stale_seconds=None,
        )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _parse_date(s: Any) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _to_float(val: Any) -> float | None:
    try:
        if val is None:
            return None
        return float(val)
    except Exception:
        return None
