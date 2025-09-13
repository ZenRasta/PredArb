from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from .base import BaseExchange
from app.types import MarketNormalized, SnapshotNormalized, OutcomeQuote


class LimitlessExchange(BaseExchange):
    """Exchange adapter for the Limitless platform."""

    platform = "limitless"
    base_url = "https://api.limitless.exchange"

    def fetch_active_markets(self) -> List[Dict[str, Any]]:
        """Fetch list of active markets from Limitless."""

        self._acquire_token("markets", limit=5, period=1)
        resp = self.session.get(f"{self.base_url}/v1/markets", params={"status": "active"})
        resp.raise_for_status()
        return resp.json()

    def fetch_orderbook_or_amm_params(self, market_id: str) -> Dict[str, Any]:
        """Fetch orderbook/AMM parameters for a market."""

        self._acquire_token("orderbook", limit=5, period=1)
        resp = self.session.get(f"{self.base_url}/v1/markets/{market_id}/orderbook")
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
                    "outcome_id": str(out.get("id")),
                    "label": out.get("name") or out.get("title"),
                    "prob": _to_float(out.get("prob")),
                }
            )

        end_date = _parse_date(raw.get("resolveDate") or raw.get("end_date"))
        return MarketNormalized(
            platform=self.platform,
            event_id=str(raw.get("id")),
            title=raw.get("question") or raw.get("title", ""),
            description=raw.get("description"),
            end_date=end_date,
            status=raw.get("status"),
            volume_usd=_to_float(raw.get("volume")),
            liquidity_usd=_to_float(raw.get("liquidity")),
            outcomes=outcomes,
            metadata={"category": raw.get("category")},
            raw=raw,
        )

    def normalize_snapshot(self, market_id: str, raw: Dict[str, Any]) -> SnapshotNormalized:
        ts_raw = raw.get("timestamp") or raw.get("ts")
        if isinstance(ts_raw, (int, float)):
            ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
        else:
            ts = datetime.now(tz=timezone.utc)

        outcomes: List[OutcomeQuote] = []
        for out in raw.get("outcomes", []):
            outcomes.append(
                OutcomeQuote(
                    outcome_id=str(out.get("id")),
                    label=out.get("name") or out.get("label"),
                    bid=_to_float(out.get("bid")),
                    ask=_to_float(out.get("ask")),
                    prob=_to_float(out.get("prob")),
                    max_fill=_to_float(out.get("liquidity")),
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
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _to_float(val: Any) -> float | None:
    try:
        if val is None:
            return None
        return float(val)
    except Exception:
        return None
