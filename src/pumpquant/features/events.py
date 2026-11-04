"""Causal microstructure features from lossless Pump/PumpSwap trade events.

The bar layer is useful for broad screens, but it cannot recover exact
equal-volume buckets, event durations, or wallet paths.  This module therefore
operates on chain-ordered atomic event records.  Every row contains only state
available after the current event's ``available_at_ns``; warm-up and missing
provenance stay visible instead of being imputed.
"""

from __future__ import annotations

import heapq
import json
import math
from collections import Counter, deque
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import cast

from pumpquant.core.schema import Side, VenuePhase
from pumpquant.decoding.instruction_intent import PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID

_NATIVE_SOL = "11111111111111111111111111111111"
_WRAPPED_SOL = "So11111111111111111111111111111111111111112"
_SOL_ALIASES = frozenset((_NATIVE_SOL, _WRAPPED_SOL, "SOL"))


def _nonempty(name: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _nonnegative_integer(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _optional_positive_integer(name: str, value: int | None) -> None:
    if value is not None and (isinstance(value, bool) or value <= 0):
        raise ValueError(f"{name} must be a positive integer when present")


@dataclass(frozen=True, slots=True)
class EventFeatureConfig:
    """Pre-registered event feature horizons.

    ``vpin_bucket_quote_atomic`` is an integer.  Bucket splitting
    never passes through a float, so a trade can close several buckets without
    rounding drift.
    """

    vpin_bucket_quote_atomic: int = 100_000_000
    vpin_window_buckets: int = 20
    rolling_event_window: int = 50
    arrival_window_seconds: float = 5.0
    hawkes_tau_seconds: float = 1.0
    max_continuity_gap_seconds: float = 30.0

    def __post_init__(self) -> None:
        for name in (
            "vpin_bucket_quote_atomic",
            "vpin_window_buckets",
            "rolling_event_window",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        for name in (
            "arrival_window_seconds",
            "hawkes_tau_seconds",
            "max_continuity_gap_seconds",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0.0:
                raise ValueError(f"{name} must be positive and finite")


@dataclass(frozen=True, slots=True)
class EventFeatureInput:
    """One lossless-enough event used by the causal feature engine."""

    event_id: str
    market_id: str
    mint: str
    quote_mint: str
    slot: int
    tx_index: int
    log_index: int
    event_index: int
    block_time_ns: int
    available_at_ns: int
    phase: VenuePhase
    side: Side
    base_amount_atomic: int
    quote_amount_atomic: int
    base_decimals: int
    quote_decimals: int
    trader: str | None = None
    creator: str | None = None
    fee_quote_atomic: int | None = None
    pre_base_reserve_atomic: int | None = None
    pre_quote_reserve_atomic: int | None = None
    pre_real_base_reserve_atomic: int | None = None
    pre_real_quote_reserve_atomic: int | None = None
    post_base_reserve_atomic: int | None = None
    post_quote_reserve_atomic: int | None = None
    post_virtual_base_reserve_atomic: int | None = None
    post_virtual_quote_reserve_atomic: int | None = None
    post_real_base_reserve_atomic: int | None = None
    post_real_quote_reserve_atomic: int | None = None
    protocol_fee_bps: int | None = None
    lp_fee_bps: int | None = None
    creator_fee_bps: int | None = None
    cashback_fee_bps: int | None = None
    buyback_fee_bps: int | None = None
    protocol_fee_quote_atomic: int | None = None
    lp_fee_quote_atomic: int | None = None
    creator_fee_quote_atomic: int | None = None
    cashback_fee_quote_atomic: int | None = None
    buyback_fee_quote_atomic: int | None = None
    venue_fee_bps: int | None = None
    fee_asset: str | None = None
    fee_mint: str | None = None
    fee_decimals: int | None = None
    protocol_fee_atomic: int | None = None
    lp_fee_atomic: int | None = None
    creator_fee_atomic: int | None = None
    cashback_fee_atomic: int | None = None
    buyback_fee_atomic: int | None = None
    effective_fee_atomic: int | None = None
    protocol_side: str | None = None
    instruction_name: str | None = None
    pool_orientation_flipped: bool | None = None
    coverage_started_at_launch: bool = False
    availability_is_synthetic: bool = False
    force_coverage_gap: bool = False

    def __post_init__(self) -> None:
        for name in ("event_id", "market_id", "mint", "quote_mint"):
            _nonempty(name, getattr(self, name))
        if self.mint == self.quote_mint:
            raise ValueError("mint and quote_mint must differ")
        for name in (
            "slot",
            "tx_index",
            "log_index",
            "event_index",
            "block_time_ns",
            "available_at_ns",
            "base_decimals",
            "quote_decimals",
        ):
            _nonnegative_integer(name, getattr(self, name))
        if self.available_at_ns < self.block_time_ns:
            raise ValueError("available_at_ns cannot precede block_time_ns")
        if self.base_decimals > 30 or self.quote_decimals > 30:
            raise ValueError("asset decimals are implausibly large")
        if not isinstance(self.phase, VenuePhase):
            raise ValueError("phase must be a VenuePhase")
        if not isinstance(self.side, Side):
            raise ValueError("side must be a Side")
        for name in ("base_amount_atomic", "quote_amount_atomic"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.fee_quote_atomic is not None:
            _nonnegative_integer("fee_quote_atomic", self.fee_quote_atomic)
        for name in (
            "pre_base_reserve_atomic",
            "pre_quote_reserve_atomic",
            "post_base_reserve_atomic",
            "post_quote_reserve_atomic",
            "post_virtual_base_reserve_atomic",
            "post_virtual_quote_reserve_atomic",
        ):
            _optional_positive_integer(name, getattr(self, name))
        for name in (
            "post_real_base_reserve_atomic",
            "post_real_quote_reserve_atomic",
            "pre_real_base_reserve_atomic",
            "pre_real_quote_reserve_atomic",
        ):
            value = getattr(self, name)
            if value is not None:
                _nonnegative_integer(name, value)
        for name in (
            "protocol_fee_bps",
            "lp_fee_bps",
            "creator_fee_bps",
            "cashback_fee_bps",
            "buyback_fee_bps",
            "venue_fee_bps",
        ):
            value = getattr(self, name)
            if value is not None:
                _nonnegative_integer(name, value)
                if value > 10_000:
                    raise ValueError(f"{name} must be at most 10000")
        for name in (
            "protocol_fee_quote_atomic",
            "lp_fee_quote_atomic",
            "creator_fee_quote_atomic",
            "cashback_fee_quote_atomic",
            "buyback_fee_quote_atomic",
            "protocol_fee_atomic",
            "lp_fee_atomic",
            "creator_fee_atomic",
            "cashback_fee_atomic",
            "buyback_fee_atomic",
            "effective_fee_atomic",
        ):
            value = getattr(self, name)
            if value is not None:
                _nonnegative_integer(name, value)
        for name in ("trader", "creator"):
            value = getattr(self, name)
            if value is not None:
                _nonempty(name, value)
        if self.fee_asset not in {None, "base", "quote"}:
            raise ValueError("fee_asset must be base, quote, or None")
        if self.fee_mint is not None:
            _nonempty("fee_mint", self.fee_mint)
        if self.fee_decimals is not None:
            _nonnegative_integer("fee_decimals", self.fee_decimals)
            if self.fee_decimals > 30:
                raise ValueError("fee_decimals is implausibly large")
        for name in ("protocol_side", "instruction_name"):
            value = getattr(self, name)
            if value is not None:
                _nonempty(name, value)
        if self.pool_orientation_flipped is not None and not isinstance(
            self.pool_orientation_flipped, bool
        ):
            raise ValueError("pool_orientation_flipped must be boolean or None")
        if not isinstance(self.coverage_started_at_launch, bool):
            raise ValueError("coverage_started_at_launch must be boolean")
        if not isinstance(self.availability_is_synthetic, bool):
            raise ValueError("availability_is_synthetic must be boolean")
        if not isinstance(self.force_coverage_gap, bool):
            raise ValueError("force_coverage_gap must be boolean")

    @property
    def ordering_key(self) -> tuple[int, int, int, int, str]:
        return (
            self.slot,
            self.tx_index,
            self.log_index,
            self.event_index,
            self.event_id,
        )


@dataclass(frozen=True, slots=True)
class EventFeatureRow:
    """Event-time features available immediately after one landed trade."""

    event_id: str
    market_id: str
    mint: str
    quote_mint: str
    slot: int
    tx_index: int
    log_index: int
    event_index: int
    block_time_ns: int
    available_at_ns: int
    phase: VenuePhase
    side: Side
    base_amount_atomic: int
    quote_amount_atomic: int
    base_decimals: int
    quote_decimals: int
    trader: str | None
    creator: str | None
    fee_quote_atomic: int | None
    pre_base_reserve_atomic: int | None
    pre_quote_reserve_atomic: int | None
    post_base_reserve_atomic: int | None
    post_quote_reserve_atomic: int | None
    availability_is_synthetic: bool
    coverage_gap: bool
    seconds_since_previous_event: float | None
    arrival_rate_per_second: float
    buy_arrival_rate_per_second: float
    sell_arrival_rate_per_second: float
    arrival_flow_imbalance: float
    duration_scaled_by_prior_rate: float | None
    global_arrival_rate_per_second_1s: float
    global_arrival_rate_per_second_5s: float
    global_arrival_rate_per_second_30s: float
    global_active_markets_5s: int
    global_active_markets_30s: int
    global_arrival_flow_imbalance_5s: float
    global_arrival_flow_imbalance_30s: float
    global_signed_quote_imbalance_5s: float
    global_signed_quote_imbalance_30s: float
    market_event_share_global_5s: float
    market_quote_share_global_5s: float
    global_activity_ratio_1s_30s: float
    hawkes_pre: float
    hawkes_buy_pre: float
    hawkes_sell_pre: float
    hawkes_signed_quote_pre: float
    hawkes_post: float
    hawkes_signed_quote_post: float
    completed_vpin_buckets: int
    vpin_ready_buckets: int
    vpin: float | None
    signed_vpin: float | None
    vpin_partial_fill_fraction: float
    wallet_hhi: float
    wallet_top1_share: float
    effective_wallets: float
    wallet_entropy: float
    buy_wallet_hhi: float
    sell_wallet_hhi: float
    unknown_trader_flow_share: float
    is_new_since_coverage: bool | None
    first_seen_censored: bool
    new_wallet_flow_share: float
    repeat_wallet_flow_share: float
    new_wallet_count: int
    repeat_wallet_count: int
    creator_missing: bool
    creator_flow_share: float | None
    creator_buy_share: float | None
    creator_sell_share: float | None
    creator_dump_pressure: float | None
    creator_net_sell_share: float | None
    reserve_source: str
    reserve_continuity: bool
    pre_spot_price: float | None
    post_spot_price: float | None
    reserve_mark_return: float | None
    reserve_k_change: float | None
    reserve_quote_change: float | None
    reserve_quote_acceleration: float | None
    gross_directional_impact_bps: float | None
    fee_excluded_directional_impact_bps: float | None
    base_size_fraction: float | None
    quote_size_fraction: float | None
    bonding_progress: float | None
    real_to_virtual_base_ratio: float | None
    real_to_virtual_quote_ratio: float | None


@dataclass(slots=True)
class _WindowEvent:
    trader: str | None
    side: Side
    quote_amount: int
    is_new_since_coverage: bool | None


@dataclass(frozen=True, slots=True)
class _GlobalWindowEvent:
    available_at_ns: int
    market_id: str
    side: Side
    quote_amount_atomic: int


@dataclass(slots=True)
class _GlobalWindow:
    seconds: int
    events: deque[_GlobalWindowEvent] = field(default_factory=deque)
    buy_count: int = 0
    sell_count: int = 0
    buy_quote_atomic: int = 0
    sell_quote_atomic: int = 0
    market_counts: Counter[str] = field(default_factory=Counter)
    market_quote_atomic: Counter[str] = field(default_factory=Counter)

    def update(self, event: EventFeatureInput) -> None:
        cutoff = event.available_at_ns - self.seconds * 1_000_000_000
        while self.events and self.events[0].available_at_ns < cutoff:
            expired = self.events.popleft()
            if expired.side is Side.BUY:
                self.buy_count -= 1
                self.buy_quote_atomic -= expired.quote_amount_atomic
            else:
                self.sell_count -= 1
                self.sell_quote_atomic -= expired.quote_amount_atomic
            self.market_counts[expired.market_id] -= 1
            if self.market_counts[expired.market_id] == 0:
                del self.market_counts[expired.market_id]
            self.market_quote_atomic[expired.market_id] -= expired.quote_amount_atomic
            if self.market_quote_atomic[expired.market_id] == 0:
                del self.market_quote_atomic[expired.market_id]

        item = _GlobalWindowEvent(
            event.available_at_ns,
            event.market_id,
            event.side,
            event.quote_amount_atomic,
        )
        self.events.append(item)
        if event.side is Side.BUY:
            self.buy_count += 1
            self.buy_quote_atomic += event.quote_amount_atomic
        else:
            self.sell_count += 1
            self.sell_quote_atomic += event.quote_amount_atomic
        self.market_counts[event.market_id] += 1
        self.market_quote_atomic[event.market_id] += event.quote_amount_atomic

    def metrics(self, market_id: str) -> tuple[float, int, float, float, float, float]:
        count = self.buy_count + self.sell_count
        gross_quote = self.buy_quote_atomic + self.sell_quote_atomic
        if count < 1 or gross_quote < 1:
            raise AssertionError("the current event must populate every global window")
        return (
            count / self.seconds,
            len(self.market_counts),
            (self.buy_count - self.sell_count) / count,
            (self.buy_quote_atomic - self.sell_quote_atomic) / gross_quote,
            self.market_counts[market_id] / count,
            self.market_quote_atomic[market_id] / gross_quote,
        )


def _xlogx(value: int) -> float:
    return 0.0 if value <= 0 else value * math.log(value)


@dataclass(slots=True)
class _WeightedOwners:
    """Exact rolling owner weights with O(log window) top-share updates."""

    amounts: dict[str, int] = field(default_factory=dict)
    max_heap: list[tuple[int, str]] = field(default_factory=list)
    total: int = 0
    sum_squares: int = 0
    sum_xlogx: float = 0.0

    def change(self, owner: str, delta: int) -> None:
        old = self.amounts.get(owner, 0)
        new = old + delta
        if new < 0:
            raise AssertionError("rolling owner amount became negative")
        self.total += delta
        self.sum_squares += new * new - old * old
        self.sum_xlogx += _xlogx(new) - _xlogx(old)
        if new:
            self.amounts[owner] = new
            heapq.heappush(self.max_heap, (-new, owner))
        else:
            self.amounts.pop(owner, None)
        if len(self.max_heap) > max(128, len(self.amounts) * 8):
            self.max_heap = [(-amount, key) for key, amount in self.amounts.items()]
            heapq.heapify(self.max_heap)

    def metrics(self) -> tuple[float, float, float, float]:
        if self.total <= 0:
            return (0.0, 0.0, 0.0, 0.0)
        while self.max_heap:
            negative, owner = self.max_heap[0]
            if -negative == self.amounts.get(owner, 0):
                break
            heapq.heappop(self.max_heap)
        hhi = self.sum_squares / (self.total * self.total)
        top1 = (-self.max_heap[0][0] / self.total) if self.max_heap else 0.0
        effective = 1.0 / hhi if hhi > 0.0 else 0.0
        owners = len(self.amounts)
        if owners <= 1:
            entropy = 0.0
        else:
            entropy = (math.log(self.total) - self.sum_xlogx / self.total) / math.log(owners)
            entropy = min(1.0, max(0.0, entropy))
        return (hhi, top1, effective, entropy)

    def clear(self) -> None:
        self.amounts.clear()
        self.max_heap.clear()
        self.total = 0
        self.sum_squares = 0
        self.sum_xlogx = 0.0


@dataclass(slots=True)
class _MarketState:
    phase: VenuePhase
    quote_mint: str
    quote_decimals: int
    previous_time_ns: int | None
    hawkes_post: float
    hawkes_buy_post: float
    hawkes_sell_post: float
    hawkes_signed_quote_post: float
    arrivals: deque[tuple[int, Side]]
    arrival_buy_count: int
    arrival_sell_count: int
    window: deque[_WindowEvent]
    wallets: _WeightedOwners
    buy_wallets: _WeightedOwners
    sell_wallets: _WeightedOwners
    unknown_quote: int
    new_quote: int
    repeat_quote: int
    new_wallet_counts: Counter[str]
    repeat_wallet_counts: Counter[str]
    vpin_buy: int
    vpin_sell: int
    vpin_fill: int
    vpin_signed: deque[float]
    completed_vpin_buckets: int
    previous_post_base: int | None
    previous_post_quote: int | None
    previous_quote_change: float | None


def _new_market_state(event: EventFeatureInput) -> _MarketState:
    return _MarketState(
        phase=event.phase,
        quote_mint=event.quote_mint,
        quote_decimals=event.quote_decimals,
        previous_time_ns=None,
        hawkes_post=0.0,
        hawkes_buy_post=0.0,
        hawkes_sell_post=0.0,
        hawkes_signed_quote_post=0.0,
        arrivals=deque(),
        arrival_buy_count=0,
        arrival_sell_count=0,
        window=deque(),
        wallets=_WeightedOwners(),
        buy_wallets=_WeightedOwners(),
        sell_wallets=_WeightedOwners(),
        unknown_quote=0,
        new_quote=0,
        repeat_quote=0,
        new_wallet_counts=Counter(),
        repeat_wallet_counts=Counter(),
        vpin_buy=0,
        vpin_sell=0,
        vpin_fill=0,
        vpin_signed=deque(),
        completed_vpin_buckets=0,
        previous_post_base=None,
        previous_post_quote=None,
        previous_quote_change=None,
    )


def _directional_impact(side: Side, trade_price: float, spot: float) -> float:
    if side is Side.BUY:
        return (trade_price / spot - 1.0) * 10_000.0
    return (1.0 - trade_price / spot) * 10_000.0


def _spot(base_atomic: int, quote_atomic: int, base_decimals: int, quote_decimals: int) -> float:
    base = float(base_atomic) / (10.0**base_decimals)
    quote = float(quote_atomic) / (10.0**quote_decimals)
    return quote / base


def _reset_continuity(state: _MarketState, event: EventFeatureInput) -> None:
    state.phase = event.phase
    state.quote_mint = event.quote_mint
    state.quote_decimals = event.quote_decimals
    state.previous_time_ns = None
    state.hawkes_post = 0.0
    state.hawkes_buy_post = 0.0
    state.hawkes_sell_post = 0.0
    state.hawkes_signed_quote_post = 0.0
    state.arrivals.clear()
    state.arrival_buy_count = 0
    state.arrival_sell_count = 0
    state.window.clear()
    state.wallets.clear()
    state.buy_wallets.clear()
    state.sell_wallets.clear()
    state.unknown_quote = 0
    state.new_quote = 0
    state.repeat_quote = 0
    state.new_wallet_counts.clear()
    state.repeat_wallet_counts.clear()
    state.vpin_buy = 0
    state.vpin_sell = 0
    state.vpin_fill = 0
    state.vpin_signed.clear()
    state.completed_vpin_buckets = 0
    state.previous_post_base = None
    state.previous_post_quote = None
    state.previous_quote_change = None


def _process_vpin(
    state: _MarketState,
    event: EventFeatureInput,
    config: EventFeatureConfig,
) -> None:
    remaining = event.quote_amount_atomic
    while remaining:
        used = min(remaining, config.vpin_bucket_quote_atomic - state.vpin_fill)
        if event.side is Side.BUY:
            state.vpin_buy += used
        else:
            state.vpin_sell += used
        state.vpin_fill += used
        remaining -= used
        if state.vpin_fill == config.vpin_bucket_quote_atomic:
            signed = (state.vpin_buy - state.vpin_sell) / config.vpin_bucket_quote_atomic
            state.vpin_signed.append(signed)
            while len(state.vpin_signed) > config.vpin_window_buckets:
                state.vpin_signed.popleft()
            state.completed_vpin_buckets += 1
            state.vpin_buy = 0
            state.vpin_sell = 0
            state.vpin_fill = 0


def iter_causal_event_features(
    events: Iterable[EventFeatureInput],
    config: EventFeatureConfig | None = None,
) -> Iterator[EventFeatureRow]:
    """Yield prefix-invariant event features in canonical chain order."""

    config = EventFeatureConfig() if config is None else config
    states: dict[str, _MarketState] = {}
    global_windows: dict[tuple[str, int], tuple[_GlobalWindow, _GlobalWindow, _GlobalWindow]] = {}
    seen_wallets: dict[str, set[str]] = {}
    coverage_complete_by_mint: dict[str, bool] = {}
    previous_key: tuple[int, int, int, int, str] | None = None
    previous_available_at: int | None = None
    arrival_window_ns = round(config.arrival_window_seconds * 1_000_000_000)

    for event in events:
        if previous_key is not None and event.ordering_key <= previous_key:
            raise ValueError("event stream must be strictly chain ordered")
        if previous_available_at is not None and event.available_at_ns < previous_available_at:
            raise ValueError("available_at_ns cannot regress in the replay stream")
        previous_key = event.ordering_key
        previous_available_at = event.available_at_ns

        quote_key = (event.quote_mint, event.quote_decimals)
        quote_windows = global_windows.get(quote_key)
        if quote_windows is None:
            quote_windows = (_GlobalWindow(1), _GlobalWindow(5), _GlobalWindow(30))
            global_windows[quote_key] = quote_windows
        for global_window in quote_windows:
            global_window.update(event)
        global_1s = quote_windows[0].metrics(event.market_id)
        global_5s = quote_windows[1].metrics(event.market_id)
        global_30s = quote_windows[2].metrics(event.market_id)

        state = states.get(event.market_id)
        if state is None:
            state = _new_market_state(event)
            states[event.market_id] = state
        incompatible = (
            state.phase is not event.phase
            or state.quote_mint != event.quote_mint
            or state.quote_decimals != event.quote_decimals
        )
        if incompatible:
            _reset_continuity(state, event)

        seconds_since_previous = (
            None
            if state.previous_time_ns is None
            else (event.block_time_ns - state.previous_time_ns) / 1_000_000_000
        )
        time_regression = seconds_since_previous is not None and seconds_since_previous < 0.0
        coverage_gap = (
            event.force_coverage_gap
            or incompatible
            or time_regression
            or (
                seconds_since_previous is not None
                and seconds_since_previous > config.max_continuity_gap_seconds
            )
        )
        if coverage_gap:
            _reset_continuity(state, event)
            seconds_since_previous = None
            coverage_complete_by_mint[event.mint] = False

        while state.arrivals and state.arrivals[0][0] < event.block_time_ns - arrival_window_ns:
            _, expired_side = state.arrivals.popleft()
            if expired_side is Side.BUY:
                state.arrival_buy_count -= 1
            else:
                state.arrival_sell_count -= 1
        prior_arrival_rate = len(state.arrivals) / config.arrival_window_seconds

        dt = 0.0 if seconds_since_previous is None else seconds_since_previous
        decay = math.exp(-dt / config.hawkes_tau_seconds)
        hawkes_pre = state.hawkes_post * decay
        hawkes_buy_pre = state.hawkes_buy_post * decay
        hawkes_sell_pre = state.hawkes_sell_post * decay
        hawkes_signed_pre = state.hawkes_signed_quote_post * decay
        mark = event.quote_amount_atomic / config.vpin_bucket_quote_atomic
        signed_mark = mark if event.side is Side.BUY else -mark
        hawkes_post = hawkes_pre + 1.0
        hawkes_signed_post = hawkes_signed_pre + signed_mark
        state.hawkes_post = hawkes_post
        state.hawkes_buy_post = hawkes_buy_pre + (1.0 if event.side is Side.BUY else 0.0)
        state.hawkes_sell_post = hawkes_sell_pre + (1.0 if event.side is Side.SELL else 0.0)
        state.hawkes_signed_quote_post = hawkes_signed_post
        state.arrivals.append((event.block_time_ns, event.side))
        if event.side is Side.BUY:
            state.arrival_buy_count += 1
        else:
            state.arrival_sell_count += 1
        buy_arrivals = state.arrival_buy_count
        sell_arrivals = state.arrival_sell_count
        arrival_rate = len(state.arrivals) / config.arrival_window_seconds
        arrival_imbalance = (
            (buy_arrivals - sell_arrivals) / len(state.arrivals) if state.arrivals else 0.0
        )

        wallets = seen_wallets.setdefault(event.mint, set())
        if event.mint not in coverage_complete_by_mint:
            coverage_complete_by_mint[event.mint] = event.coverage_started_at_launch
        is_new: bool | None
        if event.trader is None:
            is_new = None
        else:
            is_new = event.trader not in wallets
            wallets.add(event.trader)
        first_seen_censored = bool(is_new and not coverage_complete_by_mint[event.mint])
        window_event = _WindowEvent(
            trader=event.trader,
            side=event.side,
            quote_amount=event.quote_amount_atomic,
            is_new_since_coverage=is_new,
        )
        state.window.append(window_event)
        owner = event.trader or "__UNKNOWN__"
        state.wallets.change(owner, event.quote_amount_atomic)
        side_wallets = state.buy_wallets if event.side is Side.BUY else state.sell_wallets
        side_wallets.change(owner, event.quote_amount_atomic)
        if event.trader is None:
            state.unknown_quote += event.quote_amount_atomic
        elif is_new is True:
            state.new_quote += event.quote_amount_atomic
            state.new_wallet_counts[event.trader] += 1
        else:
            state.repeat_quote += event.quote_amount_atomic
            state.repeat_wallet_counts[event.trader] += 1
        while len(state.window) > config.rolling_event_window:
            expired = state.window.popleft()
            expired_owner = expired.trader or "__UNKNOWN__"
            state.wallets.change(expired_owner, -expired.quote_amount)
            expired_side_wallets = (
                state.buy_wallets if expired.side is Side.BUY else state.sell_wallets
            )
            expired_side_wallets.change(expired_owner, -expired.quote_amount)
            if expired.trader is None:
                state.unknown_quote -= expired.quote_amount
            elif expired.is_new_since_coverage is True:
                state.new_quote -= expired.quote_amount
                state.new_wallet_counts[expired.trader] -= 1
                if state.new_wallet_counts[expired.trader] == 0:
                    del state.new_wallet_counts[expired.trader]
            else:
                state.repeat_quote -= expired.quote_amount
                state.repeat_wallet_counts[expired.trader] -= 1
                if state.repeat_wallet_counts[expired.trader] == 0:
                    del state.repeat_wallet_counts[expired.trader]
        gross_window = state.wallets.total
        wallet_hhi, wallet_top1, effective_wallets, wallet_entropy = state.wallets.metrics()
        buy_hhi = state.buy_wallets.metrics()[0]
        sell_hhi = state.sell_wallets.metrics()[0]

        if event.creator is None:
            creator_flow_share = None
            creator_buy_share = None
            creator_sell_share = None
            creator_dump_pressure = None
            creator_net_sell_share = None
        else:
            creator_buy = state.buy_wallets.amounts.get(event.creator, 0)
            creator_sell = state.sell_wallets.amounts.get(event.creator, 0)
            creator_total = creator_buy + creator_sell
            creator_flow_share = creator_total / gross_window
            creator_buy_share = creator_buy / gross_window
            creator_sell_share = creator_sell / gross_window
            creator_dump_pressure = creator_sell / creator_total if creator_total else 0.0
            creator_net_sell_share = (creator_sell - creator_buy) / gross_window

        _process_vpin(state, event, config)
        ready_buckets = len(state.vpin_signed)
        vpin = (
            sum(abs(value) for value in state.vpin_signed) / ready_buckets
            if ready_buckets >= config.vpin_window_buckets
            else None
        )
        signed_vpin = (
            sum(state.vpin_signed) / ready_buckets
            if ready_buckets >= config.vpin_window_buckets
            else None
        )

        if event.pre_base_reserve_atomic is not None and event.pre_quote_reserve_atomic is not None:
            pre_base = event.pre_base_reserve_atomic
            pre_quote = event.pre_quote_reserve_atomic
            reserve_source = "explicit_raw_pre"
            reserve_continuity = True
        elif (
            state.previous_post_base is not None
            and state.previous_post_quote is not None
            and not coverage_gap
        ):
            pre_base = state.previous_post_base
            pre_quote = state.previous_post_quote
            reserve_source = "prior_post"
            reserve_continuity = True
        else:
            pre_base = None
            pre_quote = None
            reserve_source = "unavailable"
            reserve_continuity = False
        post_base = event.post_base_reserve_atomic
        post_quote = event.post_quote_reserve_atomic
        pre_spot = (
            _spot(pre_base, pre_quote, event.base_decimals, event.quote_decimals)
            if pre_base is not None and pre_quote is not None
            else None
        )
        post_spot = (
            _spot(post_base, post_quote, event.base_decimals, event.quote_decimals)
            if post_base is not None and post_quote is not None
            else None
        )
        trade_price = _spot(
            event.base_amount_atomic,
            event.quote_amount_atomic,
            event.base_decimals,
            event.quote_decimals,
        )
        gross_impact = (
            _directional_impact(event.side, trade_price, pre_spot) if pre_spot is not None else None
        )
        fee_impact: float | None = None
        if pre_spot is not None and event.fee_quote_atomic is not None:
            adjusted_quote = (
                event.quote_amount_atomic - event.fee_quote_atomic
                if event.side is Side.BUY
                else event.quote_amount_atomic + event.fee_quote_atomic
            )
            if adjusted_quote > 0:
                adjusted_price = _spot(
                    event.base_amount_atomic,
                    adjusted_quote,
                    event.base_decimals,
                    event.quote_decimals,
                )
                fee_impact = _directional_impact(event.side, adjusted_price, pre_spot)
        reserve_mark_return = (
            post_spot / pre_spot - 1.0 if pre_spot is not None and post_spot is not None else None
        )
        reserve_k_change = (
            (post_base * post_quote) / (pre_base * pre_quote) - 1.0
            if pre_base is not None
            and pre_quote is not None
            and post_base is not None
            and post_quote is not None
            else None
        )
        quote_change = (
            post_quote / pre_quote - 1.0
            if pre_quote is not None and post_quote is not None
            else None
        )
        quote_acceleration = (
            quote_change - state.previous_quote_change
            if quote_change is not None
            and state.previous_quote_change is not None
            and reserve_continuity
            else None
        )
        base_size_fraction = event.base_amount_atomic / pre_base if pre_base is not None else None
        quote_size_fraction = (
            event.quote_amount_atomic / pre_quote if pre_quote is not None else None
        )
        real_base_ratio = (
            event.post_real_base_reserve_atomic / event.post_virtual_base_reserve_atomic
            if event.post_real_base_reserve_atomic is not None
            and event.post_virtual_base_reserve_atomic is not None
            else None
        )
        real_quote_ratio = (
            event.post_real_quote_reserve_atomic / event.post_virtual_quote_reserve_atomic
            if event.post_real_quote_reserve_atomic is not None
            and event.post_virtual_quote_reserve_atomic is not None
            else None
        )
        bonding_progress = 1.0 - real_base_ratio if real_base_ratio is not None else None

        yield EventFeatureRow(
            event_id=event.event_id,
            market_id=event.market_id,
            mint=event.mint,
            quote_mint=event.quote_mint,
            slot=event.slot,
            tx_index=event.tx_index,
            log_index=event.log_index,
            event_index=event.event_index,
            block_time_ns=event.block_time_ns,
            available_at_ns=event.available_at_ns,
            phase=event.phase,
            side=event.side,
            base_amount_atomic=event.base_amount_atomic,
            quote_amount_atomic=event.quote_amount_atomic,
            base_decimals=event.base_decimals,
            quote_decimals=event.quote_decimals,
            trader=event.trader,
            creator=event.creator,
            fee_quote_atomic=event.fee_quote_atomic,
            pre_base_reserve_atomic=event.pre_base_reserve_atomic,
            pre_quote_reserve_atomic=event.pre_quote_reserve_atomic,
            post_base_reserve_atomic=event.post_base_reserve_atomic,
            post_quote_reserve_atomic=event.post_quote_reserve_atomic,
            availability_is_synthetic=event.availability_is_synthetic,
            coverage_gap=coverage_gap,
            seconds_since_previous_event=seconds_since_previous,
            arrival_rate_per_second=arrival_rate,
            buy_arrival_rate_per_second=(buy_arrivals / config.arrival_window_seconds),
            sell_arrival_rate_per_second=(sell_arrivals / config.arrival_window_seconds),
            arrival_flow_imbalance=arrival_imbalance,
            duration_scaled_by_prior_rate=(
                None
                if seconds_since_previous is None
                else seconds_since_previous * prior_arrival_rate
            ),
            global_arrival_rate_per_second_1s=global_1s[0],
            global_arrival_rate_per_second_5s=global_5s[0],
            global_arrival_rate_per_second_30s=global_30s[0],
            global_active_markets_5s=global_5s[1],
            global_active_markets_30s=global_30s[1],
            global_arrival_flow_imbalance_5s=global_5s[2],
            global_arrival_flow_imbalance_30s=global_30s[2],
            global_signed_quote_imbalance_5s=global_5s[3],
            global_signed_quote_imbalance_30s=global_30s[3],
            market_event_share_global_5s=global_5s[4],
            market_quote_share_global_5s=global_5s[5],
            global_activity_ratio_1s_30s=global_1s[0] / global_30s[0],
            hawkes_pre=hawkes_pre,
            hawkes_buy_pre=hawkes_buy_pre,
            hawkes_sell_pre=hawkes_sell_pre,
            hawkes_signed_quote_pre=hawkes_signed_pre,
            hawkes_post=hawkes_post,
            hawkes_signed_quote_post=hawkes_signed_post,
            completed_vpin_buckets=state.completed_vpin_buckets,
            vpin_ready_buckets=ready_buckets,
            vpin=vpin,
            signed_vpin=signed_vpin,
            vpin_partial_fill_fraction=(state.vpin_fill / config.vpin_bucket_quote_atomic),
            wallet_hhi=wallet_hhi,
            wallet_top1_share=wallet_top1,
            effective_wallets=effective_wallets,
            wallet_entropy=wallet_entropy,
            buy_wallet_hhi=buy_hhi,
            sell_wallet_hhi=sell_hhi,
            unknown_trader_flow_share=state.unknown_quote / gross_window,
            is_new_since_coverage=is_new,
            first_seen_censored=first_seen_censored,
            new_wallet_flow_share=state.new_quote / gross_window,
            repeat_wallet_flow_share=state.repeat_quote / gross_window,
            new_wallet_count=len(state.new_wallet_counts),
            repeat_wallet_count=len(state.repeat_wallet_counts),
            creator_missing=event.creator is None,
            creator_flow_share=creator_flow_share,
            creator_buy_share=creator_buy_share,
            creator_sell_share=creator_sell_share,
            creator_dump_pressure=creator_dump_pressure,
            creator_net_sell_share=creator_net_sell_share,
            reserve_source=reserve_source,
            reserve_continuity=reserve_continuity,
            pre_spot_price=pre_spot,
            post_spot_price=post_spot,
            reserve_mark_return=reserve_mark_return,
            reserve_k_change=reserve_k_change,
            reserve_quote_change=quote_change,
            reserve_quote_acceleration=quote_acceleration,
            gross_directional_impact_bps=gross_impact,
            fee_excluded_directional_impact_bps=fee_impact,
            base_size_fraction=base_size_fraction,
            quote_size_fraction=quote_size_fraction,
            bonding_progress=bonding_progress,
            real_to_virtual_base_ratio=real_base_ratio,
            real_to_virtual_quote_ratio=real_quote_ratio,
        )
        state.previous_time_ns = event.block_time_ns
        state.previous_post_base = post_base
        state.previous_post_quote = post_quote
        state.previous_quote_change = quote_change


def causal_event_features(
    events: Iterable[EventFeatureInput],
    config: EventFeatureConfig | None = None,
) -> tuple[EventFeatureRow, ...]:
    """Materialise event features; convenient for bounded research slices."""

    return tuple(iter_causal_event_features(events, config))


def _required_text(row: Mapping[str, object], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is missing or invalid")
    return value


def _required_int(row: Mapping[str, object], name: str) -> int:
    value = row.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} is missing or invalid")
    return value


def _optional_int(row: Mapping[str, object], name: str) -> int | None:
    value = row.get(name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} is invalid")
    return value


def _same_asset(left: str, right: str) -> bool:
    return left == right or (left in _SOL_ALIASES and right in _SOL_ALIASES)


def event_feature_input_from_raw(
    row: Mapping[str, object],
    *,
    observed_at_ns: int | None = None,
    coverage_started_at_launch: bool = False,
    availability_is_synthetic: bool | None = None,
    force_coverage_gap: bool = False,
) -> EventFeatureInput:
    """Project one canonical typed trade row without discarding atomic fields.

    The sealed historical dataset has block time but not a source receive time.
    When ``observed_at_ns`` is omitted, the fallback is marked synthetic so it
    cannot be mistaken for measured production latency.
    """

    program_id = _required_text(row, "program_id")
    if program_id == PUMP_PROGRAM_ID:
        phase = VenuePhase.BONDING_CURVE
    elif program_id == PUMPSWAP_PROGRAM_ID:
        phase = VenuePhase.PUMPSWAP
    else:
        raise ValueError("unsupported program_id")
    block_time_ns = _required_int(row, "block_time") * 1_000_000_000
    available_at = block_time_ns if observed_at_ns is None else observed_at_ns
    synthetic_availability = (
        observed_at_ns is None if availability_is_synthetic is None else availability_is_synthetic
    )
    values_raw = _required_text(row, "values_json")
    values_value = json.loads(values_raw)
    if not isinstance(values_value, Mapping):
        raise ValueError("values_json must decode to an object")
    values = cast(Mapping[str, object], values_value)
    creator_value = values.get("creator", values.get("coin_creator"))
    creator = creator_value if isinstance(creator_value, str) and creator_value else None
    mint = _required_text(row, "mint")
    source_quote_mint = _required_text(row, "quote_mint")
    quote_mint = "SOL" if source_quote_mint in _SOL_ALIASES else source_quote_mint

    pre_base: int | None = None
    pre_quote: int | None = None
    pool_orientation_flipped: bool | None = None
    if phase is VenuePhase.PUMPSWAP:
        raw_pre_base = values.get("pool_base_token_reserves")
        raw_pre_quote = values.get("pool_quote_token_reserves")
        if isinstance(raw_pre_base, int) and isinstance(raw_pre_quote, int):
            pool_base_mint = row.get("pool_base_mint")
            pool_quote_mint = row.get("pool_quote_mint")
            if isinstance(pool_base_mint, str) and isinstance(pool_quote_mint, str):
                if _same_asset(pool_base_mint, mint) and _same_asset(pool_quote_mint, quote_mint):
                    pre_base, pre_quote = raw_pre_base, raw_pre_quote
                    pool_orientation_flipped = False
                elif _same_asset(pool_quote_mint, mint) and _same_asset(pool_base_mint, quote_mint):
                    pre_base, pre_quote = raw_pre_quote, raw_pre_base
                    pool_orientation_flipped = True

    virtual_base = _optional_int(row, "post_virtual_base_reserve")
    virtual_quote = _optional_int(row, "post_virtual_quote_reserve")
    real_base = _optional_int(row, "post_real_base_reserve")
    real_quote = _optional_int(row, "post_real_quote_reserve")
    if (virtual_base is None) != (virtual_quote is None):
        raise ValueError("partial post virtual reserve pair")
    if (real_base is None) != (real_quote is None):
        raise ValueError("partial post real reserve pair")
    # The executable mark is virtual during the Pump bonding phase and real in
    # PumpSwap.  Never fall back component-by-component across those two state
    # spaces: a mixed virtual/real pair would create a fictitious price.
    if phase is VenuePhase.BONDING_CURVE:
        post_base = virtual_base
        post_quote = virtual_quote
    else:
        post_base = real_base
        post_quote = real_quote
    pre_real_base: int | None = None
    pre_real_quote: int | None = None

    # Pump TradeEvent reserves are emitted after the trade.  Outside Mayhem
    # mode, post +/- canonical curve amounts supplies a candidate predecessor;
    # the ordered materialiser must still prove it equals the immediately prior
    # post-state before exposing it as executable state.  Mayhem can mutate the
    # virtual quote reserve independently, so this projection
    # emits no predecessor and forces a gap for those rows.  Fee components are
    # separate and never enter reserve algebra.
    side = Side(_required_text(row, "side"))
    mayhem_mode = False
    if phase is VenuePhase.BONDING_CURVE:
        mayhem_value = values.get("mayhem_mode")
        if not isinstance(mayhem_value, bool):
            raise ValueError("Pump TradeEvent mayhem_mode is missing or invalid")
        mayhem_mode = mayhem_value
    if (
        phase is VenuePhase.BONDING_CURVE
        and not mayhem_mode
        and virtual_base is not None
        and virtual_quote is not None
    ):
        base_amount = _required_int(row, "base_amount")
        quote_amount = _required_int(row, "user_quote_amount")
        if side is Side.BUY:
            pre_base = virtual_base + base_amount
            pre_quote = virtual_quote - quote_amount
            if real_base is not None and real_quote is not None:
                pre_real_base = real_base + base_amount
                pre_real_quote = real_quote - quote_amount
        else:
            pre_base = virtual_base - base_amount
            pre_quote = virtual_quote + quote_amount
            if real_base is not None and real_quote is not None:
                pre_real_base = real_base - base_amount
                pre_real_quote = real_quote + quote_amount
        if pre_base <= 0 or pre_quote <= 0:
            raise ValueError("invalid non-positive Pump bonding-curve predecessor reserves")
        if (
            pre_real_base is not None
            and pre_real_quote is not None
            and (pre_real_base < 0 or pre_real_quote < 0)
        ):
            raise ValueError("invalid negative Pump real predecessor reserves")
    elif phase is VenuePhase.PUMPSWAP:
        # PumpSwap's event-local pool reserves are the real executable state.
        pre_real_base = pre_base
        pre_real_quote = pre_quote

    fee_quote_atomic: int | None = None
    fee_asset: str | None = None
    fee_mint_value = row.get("fee_mint")
    fee_mint = fee_mint_value if isinstance(fee_mint_value, str) else None
    fee_decimals_value = row.get("fee_decimals")
    fee_decimals = fee_decimals_value if isinstance(fee_decimals_value, int) else None
    quote_decimals = _required_int(row, "quote_decimals")
    base_decimals = _required_int(row, "base_decimals")
    if fee_mint is not None and _same_asset(fee_mint, quote_mint):
        if fee_decimals == quote_decimals:
            fee_asset = "quote"
    elif fee_mint is not None and _same_asset(fee_mint, mint) and fee_decimals == base_decimals:
        fee_asset = "base"

    raw_fee_amounts = {
        "protocol_fee_atomic": _optional_int(row, "protocol_fee_amount"),
        "lp_fee_atomic": _optional_int(row, "lp_fee_amount"),
        "creator_fee_atomic": _optional_int(row, "creator_fee_amount"),
        "cashback_fee_atomic": _optional_int(row, "cashback_amount"),
        "buyback_fee_atomic": _optional_int(row, "buyback_fee_amount"),
    }
    additive_fee_names = (
        "protocol_fee_atomic",
        "lp_fee_atomic",
        "creator_fee_atomic",
        "cashback_fee_atomic",
    )
    effective_fee_atomic = (
        sum(cast(int, raw_fee_amounts[name]) for name in additive_fee_names)
        if all(raw_fee_amounts[name] is not None for name in additive_fee_names)
        else None
    )
    if fee_asset == "quote":
        fee_quote_atomic = effective_fee_atomic

    fee_bps = {
        "protocol_fee_bps": _optional_int(row, "protocol_fee_bps"),
        "lp_fee_bps": _optional_int(row, "lp_fee_bps"),
        "creator_fee_bps": _optional_int(row, "creator_fee_bps"),
        "cashback_fee_bps": _optional_int(row, "cashback_fee_bps"),
        "buyback_fee_bps": _optional_int(row, "buyback_fee_bps"),
    }
    required_fee_bps = (
        "protocol_fee_bps",
        "creator_fee_bps",
        "cashback_fee_bps",
    )
    lp_fee_bps = fee_bps["lp_fee_bps"]
    venue_fee_bps = (
        sum(cast(int, fee_bps[name]) for name in required_fee_bps)
        + (0 if lp_fee_bps is None else lp_fee_bps)
        if all(fee_bps[name] is not None for name in required_fee_bps)
        and (phase is VenuePhase.BONDING_CURVE or lp_fee_bps is not None)
        else None
    )
    fee_amounts: dict[str, int | None] = {
        "protocol_fee_quote_atomic": None,
        "lp_fee_quote_atomic": None,
        "creator_fee_quote_atomic": None,
        "cashback_fee_quote_atomic": None,
        "buyback_fee_quote_atomic": None,
    }
    if fee_quote_atomic is not None:
        fee_amounts = {
            "protocol_fee_quote_atomic": _optional_int(row, "protocol_fee_amount"),
            "lp_fee_quote_atomic": _optional_int(row, "lp_fee_amount"),
            "creator_fee_quote_atomic": _optional_int(row, "creator_fee_amount"),
            "cashback_fee_quote_atomic": _optional_int(row, "cashback_amount"),
            "buyback_fee_quote_atomic": _optional_int(row, "buyback_fee_amount"),
        }

    trader_value = row.get("user")
    trader = trader_value if isinstance(trader_value, str) and trader_value else None
    protocol_side_value = row.get("protocol_side")
    protocol_side = protocol_side_value if isinstance(protocol_side_value, str) else None
    instruction_name_value = row.get("ix_name")
    instruction_name = instruction_name_value if isinstance(instruction_name_value, str) else None
    return EventFeatureInput(
        event_id=_required_text(row, "event_id"),
        market_id=_required_text(row, "market_id"),
        mint=mint,
        quote_mint=quote_mint,
        slot=_required_int(row, "slot"),
        tx_index=_required_int(row, "tx_index"),
        log_index=_required_int(row, "log_index"),
        event_index=_required_int(row, "event_index"),
        block_time_ns=block_time_ns,
        available_at_ns=available_at,
        phase=phase,
        side=side,
        base_amount_atomic=_required_int(row, "base_amount"),
        quote_amount_atomic=_required_int(row, "user_quote_amount"),
        base_decimals=base_decimals,
        quote_decimals=quote_decimals,
        trader=trader,
        creator=creator,
        fee_quote_atomic=fee_quote_atomic,
        pre_base_reserve_atomic=pre_base,
        pre_quote_reserve_atomic=pre_quote,
        pre_real_base_reserve_atomic=pre_real_base,
        pre_real_quote_reserve_atomic=pre_real_quote,
        post_base_reserve_atomic=post_base,
        post_quote_reserve_atomic=post_quote,
        post_virtual_base_reserve_atomic=virtual_base,
        post_virtual_quote_reserve_atomic=virtual_quote,
        post_real_base_reserve_atomic=real_base,
        post_real_quote_reserve_atomic=real_quote,
        **fee_bps,
        **fee_amounts,
        venue_fee_bps=venue_fee_bps,
        fee_asset=fee_asset,
        fee_mint=fee_mint,
        fee_decimals=fee_decimals,
        **raw_fee_amounts,
        effective_fee_atomic=effective_fee_atomic,
        protocol_side=protocol_side,
        instruction_name=instruction_name,
        pool_orientation_flipped=pool_orientation_flipped,
        coverage_started_at_launch=coverage_started_at_launch,
        availability_is_synthetic=synthetic_availability,
        force_coverage_gap=force_coverage_gap or mayhem_mode,
    )


__all__ = [
    "EventFeatureConfig",
    "EventFeatureInput",
    "EventFeatureRow",
    "causal_event_features",
    "event_feature_input_from_raw",
    "iter_causal_event_features",
]
