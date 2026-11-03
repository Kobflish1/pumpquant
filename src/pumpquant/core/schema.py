"""Validated, immutable domain records used by the research pipeline.

Timestamps remain nanoseconds. Numeric amounts in this Python slice are
decimal-normalized and source decimals are retained when known; authoritative
atomic integers must remain in the separate immutable raw/canonical archive.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum
from numbers import Real
from typing import TypeAlias

Number: TypeAlias = int | float


class VenuePhase(StrEnum):
    """The market mechanism on which a token is currently trading."""

    BONDING_CURVE = "bonding_curve"
    PUMPSWAP = "pumpswap"
    CLOB = "clob"


class Side(StrEnum):
    """Aggressor side, expressed from the base-token point of view."""

    BUY = "buy"
    SELL = "sell"


def _require_text(name: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _require_int_at_least(name: str, value: object, lower_bound: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < lower_bound:
        comparator = "nonnegative" if lower_bound == 0 else f">= {lower_bound}"
        raise ValueError(f"{name} must be a {comparator} integer")


def _require_finite(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite real number")
    if not math.isfinite(float(value)):
        raise ValueError(f"{name} must be a finite real number")


def _require_positive(name: str, value: object) -> None:
    _require_finite(name, value)
    if value <= 0:  # type: ignore[operator]
        raise ValueError(f"{name} must be positive")


def _require_nonnegative(name: str, value: object) -> None:
    _require_finite(name, value)
    if value < 0:  # type: ignore[operator]
        raise ValueError(f"{name} must be nonnegative")


@dataclass(frozen=True, slots=True)
class SwapEvent:
    """One normalized on-chain swap with its post-swap reserve snapshot.

    ``ordering_key`` is chain-native and deterministic.  Block time is used for
    time bucketing, but not to break ties because several transactions commonly
    share the same timestamp.
    """

    event_id: str
    market_id: str
    mint: str
    quote_mint: str
    slot: int
    tx_index: int
    instruction_index: int
    block_time_ns: int
    observed_at_ns: int
    phase: VenuePhase
    side: Side
    base_amount: Number
    quote_amount: Number
    price: Number
    virtual_base_reserve: Number
    virtual_quote_reserve: Number
    real_base_reserve: Number
    real_quote_reserve: Number
    trader: str | None = None
    venue_fee_bps: Number | None = None
    protocol_fee_quote: Number = 0.0
    creator_fee_quote: Number = 0.0
    lp_fee_quote: Number = 0.0
    cashback_quote: Number = 0.0
    buyback_fee_quote: Number = 0.0
    signature: str | None = None
    program_version: str | None = None
    base_decimals: int | None = None
    quote_decimals: int | None = None

    def __post_init__(self) -> None:
        for name in ("event_id", "market_id", "mint", "quote_mint"):
            _require_text(name, getattr(self, name))
        if self.mint == self.quote_mint:
            raise ValueError("mint and quote_mint must differ")

        for name in (
            "slot",
            "tx_index",
            "instruction_index",
            "block_time_ns",
            "observed_at_ns",
        ):
            _require_int_at_least(name, getattr(self, name), 0)
        if self.observed_at_ns < self.block_time_ns:
            raise ValueError("observed_at_ns cannot precede block_time_ns")

        if not isinstance(self.phase, VenuePhase):
            raise ValueError("phase must be a VenuePhase")
        if not isinstance(self.side, Side):
            raise ValueError("side must be a Side")

        for name in (
            "base_amount",
            "quote_amount",
            "price",
            "virtual_base_reserve",
            "virtual_quote_reserve",
        ):
            _require_positive(name, getattr(self, name))
        for name in ("real_base_reserve", "real_quote_reserve"):
            _require_nonnegative(name, getattr(self, name))

        if self.trader is not None:
            _require_text("trader", self.trader)
        if self.venue_fee_bps is not None:
            _require_nonnegative("venue_fee_bps", self.venue_fee_bps)
            if self.venue_fee_bps >= 10_000:
                raise ValueError("venue_fee_bps must be below 10000")
        for name in (
            "protocol_fee_quote",
            "creator_fee_quote",
            "lp_fee_quote",
            "cashback_quote",
            "buyback_fee_quote",
        ):
            _require_nonnegative(name, getattr(self, name))
        for name in ("signature", "program_version"):
            value = getattr(self, name)
            if value is not None:
                _require_text(name, value)
        for name in ("base_decimals", "quote_decimals"):
            value = getattr(self, name)
            if value is not None:
                _require_int_at_least(name, value, 0)
                if value > 30:
                    raise ValueError(f"{name} is implausibly large")

    @property
    def ordering_key(self) -> tuple[int, int, int, str]:
        """Canonical total-order key within the normalized chain stream."""

        return (self.slot, self.tx_index, self.instruction_index, self.event_id)


@dataclass(frozen=True, slots=True)
class Bar:
    """A non-empty, half-open ``[start_time_ns, end_time_ns)`` swap bar."""

    market_id: str
    mint: str
    quote_mint: str
    start_time_ns: int
    end_time_ns: int
    phase: VenuePhase
    open: Number
    high: Number
    low: Number
    close: Number
    base_volume: Number
    quote_volume: Number
    buy_quote_volume: Number
    sell_quote_volume: Number
    trade_count: int
    unique_traders: int
    virtual_base_reserve: Number
    virtual_quote_reserve: Number
    token_age_seconds: Number | None
    real_base_reserve: Number | None = None
    real_quote_reserve: Number | None = None
    venue_fee_bps: Number | None = None
    available_at_ns: int | None = None

    def __post_init__(self) -> None:
        for name in ("market_id", "mint", "quote_mint"):
            _require_text(name, getattr(self, name))
        if self.mint == self.quote_mint:
            raise ValueError("mint and quote_mint must differ")
        _require_int_at_least("start_time_ns", self.start_time_ns, 0)
        _require_int_at_least("end_time_ns", self.end_time_ns, 0)
        if self.end_time_ns <= self.start_time_ns:
            raise ValueError("end_time_ns must be greater than start_time_ns")
        if not isinstance(self.phase, VenuePhase):
            raise ValueError("phase must be a VenuePhase")

        for name in ("open", "high", "low", "close"):
            _require_positive(name, getattr(self, name))
        if self.low > self.high:
            raise ValueError("low cannot exceed high")
        if not self.low <= self.open <= self.high:
            raise ValueError("open must lie within [low, high]")
        if not self.low <= self.close <= self.high:
            raise ValueError("close must lie within [low, high]")

        for name in ("base_volume", "quote_volume"):
            _require_positive(name, getattr(self, name))
        for name in ("buy_quote_volume", "sell_quote_volume"):
            _require_nonnegative(name, getattr(self, name))
        if not math.isclose(
            float(self.buy_quote_volume + self.sell_quote_volume),
            float(self.quote_volume),
            rel_tol=1e-9,
            abs_tol=1e-12,
        ):
            raise ValueError("buy_quote_volume + sell_quote_volume must equal quote_volume")

        _require_int_at_least("trade_count", self.trade_count, 1)
        _require_int_at_least("unique_traders", self.unique_traders, 0)
        if self.unique_traders > self.trade_count:
            raise ValueError("unique_traders cannot exceed trade_count")

        for name in ("virtual_base_reserve", "virtual_quote_reserve"):
            _require_positive(name, getattr(self, name))
        for name in ("real_base_reserve", "real_quote_reserve"):
            value = getattr(self, name)
            if value is not None:
                _require_nonnegative(name, value)
        if self.venue_fee_bps is not None:
            _require_nonnegative("venue_fee_bps", self.venue_fee_bps)
            if self.venue_fee_bps >= 10_000:
                raise ValueError("venue_fee_bps must be below 10000")
        if self.available_at_ns is not None:
            _require_int_at_least("available_at_ns", self.available_at_ns, 0)
            if self.available_at_ns < self.end_time_ns:
                raise ValueError("available_at_ns cannot precede bar close")
        if self.token_age_seconds is not None:
            _require_nonnegative("token_age_seconds", self.token_age_seconds)

    @property
    def start_ns(self) -> int:
        """Short alias for callers that already encode time in the field name."""

        return self.start_time_ns

    @property
    def end_ns(self) -> int:
        """Short alias for callers that already encode time in the field name."""

        return self.end_time_ns

    @property
    def ordering_key(self) -> tuple[str, int, str]:
        return (self.market_id, self.start_time_ns, self.phase.value)


@dataclass(frozen=True, slots=True)
class FeatureRow:
    """A bar paired with features available when that bar has closed."""

    bar: Bar
    return_1: float | None
    momentum: float | None
    realized_vol: float | None
    quote_volume_zscore: float | None
    buy_imbalance: float
    trade_intensity: float
    liquidity_quote: float
    price_impact_1_quote_bps: float | None
    reserve_progress: float | None

    def __post_init__(self) -> None:
        if not isinstance(self.bar, Bar):
            raise ValueError("bar must be a Bar")
        for name in (
            "return_1",
            "momentum",
            "realized_vol",
            "quote_volume_zscore",
            "reserve_progress",
            "price_impact_1_quote_bps",
        ):
            value = getattr(self, name)
            if value is not None:
                _require_finite(name, value)
        for name in (
            "buy_imbalance",
            "trade_intensity",
            "liquidity_quote",
        ):
            _require_finite(name, getattr(self, name))

        if self.realized_vol is not None and self.realized_vol < 0:
            raise ValueError("realized_vol must be nonnegative")
        if not -1.0 <= self.buy_imbalance <= 1.0:
            raise ValueError("buy_imbalance must lie within [-1, 1]")
        if self.trade_intensity < 0:
            raise ValueError("trade_intensity must be nonnegative")
        if self.liquidity_quote < 0:
            raise ValueError("liquidity_quote must be nonnegative")
        if self.price_impact_1_quote_bps is not None and self.price_impact_1_quote_bps < 0:
            raise ValueError("price_impact_1_quote_bps must be nonnegative")
        if self.reserve_progress is not None and not 0.0 <= self.reserve_progress <= 1.0:
            raise ValueError("reserve_progress must lie within [0, 1]")
