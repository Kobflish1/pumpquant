"""Bounded public market-data lookups: the SOL/USD rate and a token's supply."""

from __future__ import annotations

import json
import math
import re
import threading
import time
from collections.abc import Callable, Mapping
from urllib.request import Request, urlopen

from pumpquant.dashboard.config import (
    DashboardError,
)
from pumpquant.dashboard.evidence import (
    TokenEvidenceError,
)

_MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


_COINBASE_SOL_USD_URL = "https://api.coinbase.com/v2/prices/SOL-USD/spot"


_SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"


class MarketDataError(DashboardError):
    """A bounded external market-data lookup failed validation or retrieval."""


def _fetch_market_json(url: str, body: bytes | None = None) -> Mapping[str, object]:
    request = Request(
        url,
        data=body,
        method="POST" if body is not None else "GET",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "PumpQuant-dashboard/1",
        },
    )
    with urlopen(request, timeout=4.0) as response:  # noqa: S310 - constant URLs only
        raw = response.read(1_000_001)
    if len(raw) > 1_000_000:
        raise MarketDataError("market-data response exceeds one megabyte")
    payload = json.loads(raw)
    if not isinstance(payload, Mapping):
        raise MarketDataError("market-data response is not an object")
    return payload


class MarketDataProvider:
    """Thread-safe, stale-aware cache for live SOL/USD and SPL token supply."""

    def __init__(
        self,
        *,
        fetch_json: Callable[[str, bytes | None], Mapping[str, object]] | None = None,
        clock: Callable[[], float] | None = None,
        fx_ttl_seconds: float = 30.0,
        supply_ttl_seconds: float = 21_600.0,
    ) -> None:
        self._fetch_json = fetch_json or _fetch_market_json
        self._clock = clock or time.time
        self._fx_ttl = max(1.0, float(fx_ttl_seconds))
        self._supply_ttl = max(60.0, float(supply_ttl_seconds))
        self._lock = threading.RLock()
        self._fx_cache: tuple[float, float] | None = None
        self._supply_cache: dict[str, tuple[float, float, int]] = {}

    def sol_usd(self) -> dict[str, object]:
        now = self._clock()
        with self._lock:
            cached = self._fx_cache
            if cached is not None and now - cached[0] < self._fx_ttl:
                return self._fx_payload(cached, now, stale=False)
            try:
                payload = self._fetch_json(_COINBASE_SOL_USD_URL, None)
                data = payload.get("data")
                if not isinstance(data, Mapping):
                    raise MarketDataError("Coinbase spot response has no data object")
                value = float(data.get("amount", 0.0))
                if not math.isfinite(value) or value <= 0.0:
                    raise MarketDataError("Coinbase SOL/USD spot is invalid")
                cached = (now, value)
                self._fx_cache = cached
                return self._fx_payload(cached, now, stale=False)
            except (OSError, ValueError, TypeError, json.JSONDecodeError, MarketDataError) as exc:
                if cached is None:
                    raise MarketDataError(f"SOL/USD spot unavailable: {exc}") from exc
                return self._fx_payload(cached, now, stale=True)

    @staticmethod
    def _fx_payload(cached: tuple[float, float], now: float, *, stale: bool) -> dict[str, object]:
        fetched_at, value = cached
        return {
            "pair": "SOL-USD",
            "sol_usd": value,
            "fetched_at_epoch": fetched_at,
            "age_seconds": max(0.0, now - fetched_at),
            "stale": stale,
            "source": _COINBASE_SOL_USD_URL,
        }

    def token_supply(self, mint: str) -> dict[str, object]:
        if not isinstance(mint, str) or not _MINT_RE.fullmatch(mint):
            raise TokenEvidenceError("mint must be a 32-44 character base58 public key")
        now = self._clock()
        with self._lock:
            cached = self._supply_cache.get(mint)
            if cached is not None and now - cached[0] < self._supply_ttl:
                return self._supply_payload(mint, cached, now, stale=False)
            request_body = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTokenSupply",
                    "params": [mint, {"commitment": "finalized"}],
                },
                separators=(",", ":"),
            ).encode()
            try:
                payload = self._fetch_json(_SOLANA_RPC_URL, request_body)
                result = payload.get("result")
                if not isinstance(result, Mapping):
                    raise MarketDataError("Solana token-supply response has no result")
                context = result.get("context")
                value = result.get("value")
                if not isinstance(context, Mapping) or not isinstance(value, Mapping):
                    raise MarketDataError("Solana token-supply result is incomplete")
                supply = float(value.get("uiAmountString", 0.0))
                slot = int(context.get("slot", 0))
                if not math.isfinite(supply) or supply <= 0.0 or slot <= 0:
                    raise MarketDataError("Solana token supply or context slot is invalid")
                cached = (now, supply, slot)
                self._supply_cache[mint] = cached
                return self._supply_payload(mint, cached, now, stale=False)
            except (OSError, ValueError, TypeError, json.JSONDecodeError, MarketDataError) as exc:
                if cached is None:
                    raise MarketDataError(f"token supply unavailable: {exc}") from exc
                return self._supply_payload(mint, cached, now, stale=True)

    @staticmethod
    def _supply_payload(
        mint: str, cached: tuple[float, float, int], now: float, *, stale: bool
    ) -> dict[str, object]:
        fetched_at, supply, slot = cached
        return {
            "mint": mint,
            "token_supply": supply,
            "slot": slot,
            "fetched_at_epoch": fetched_at,
            "age_seconds": max(0.0, now - fetched_at),
            "stale": stale,
            "source": _SOLANA_RPC_URL,
        }
