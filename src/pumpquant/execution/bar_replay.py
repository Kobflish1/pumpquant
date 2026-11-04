"""Deterministic, execution-aware replay for AMM token strategies.

Pump bonding curves and PumpSwap are automated market makers, not central limit
order books.  The fill functions therefore walk the constant-product reserve
curve directly.  Historical reserves remain exogenous after our hypothetical
fill, so the simulator enforces small participation limits; large
counterfactual orders require a full market-response model and are rejected.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from pumpquant.core.schema import Bar, FeatureRow, Side, VenuePhase
from pumpquant.features.bars import causal_features


@dataclass(frozen=True, slots=True)
class ExecutionConfig:
    quote_notional: float = 0.10
    venue_fee_bps: float = 125.0
    adverse_selection_bps: float = 20.0
    fixed_network_cost_quote: float = 0.00001
    priority_cost_quote: float = 0.00010
    latency_bars: int = 1
    max_reserve_fraction: float = 0.005
    max_bar_volume_fraction: float = 0.01
    landing_probability: float = 1.0
    random_seed: int = 7
    use_observed_venue_fee: bool = True

    def __post_init__(self) -> None:
        numeric = (
            self.quote_notional,
            self.venue_fee_bps,
            self.adverse_selection_bps,
            self.fixed_network_cost_quote,
            self.priority_cost_quote,
            self.max_reserve_fraction,
            self.max_bar_volume_fraction,
            self.landing_probability,
        )
        if not all(math.isfinite(value) for value in numeric):
            raise ValueError("execution configuration must contain only finite numbers")
        if self.quote_notional <= 0.0:
            raise ValueError("quote_notional must be positive")
        if not 0.0 <= self.venue_fee_bps < 10_000.0:
            raise ValueError("venue_fee_bps must be in [0, 10000)")
        if self.adverse_selection_bps < 0.0:
            raise ValueError("adverse_selection_bps cannot be negative")
        if self.fixed_network_cost_quote < 0.0 or self.priority_cost_quote < 0.0:
            raise ValueError("network costs cannot be negative")
        if self.latency_bars < 1:
            raise ValueError("latency_bars must be at least one to prevent same-bar fills")
        if not 0.0 < self.max_reserve_fraction <= 1.0:
            raise ValueError("max_reserve_fraction must be in (0, 1]")
        if not 0.0 < self.max_bar_volume_fraction <= 1.0:
            raise ValueError("max_bar_volume_fraction must be in (0, 1]")
        if not 0.0 <= self.landing_probability <= 1.0:
            raise ValueError("landing_probability must be in [0, 1]")

    @property
    def fixed_cost_quote(self) -> float:
        return self.fixed_network_cost_quote + self.priority_cost_quote


@dataclass(frozen=True, slots=True)
class MomentumSpec:
    """A transparent baseline family for the strategy search layer."""

    momentum_lookback: int = 3
    volume_lookback: int = 5
    entry_momentum: float = 0.05
    entry_buy_imbalance: float = 0.15
    min_liquidity_quote: float = 5.0
    min_trade_count: int = 3
    min_age_seconds: float = 5.0
    max_age_seconds: float = 600.0
    exit_momentum: float = -0.02
    take_profit: float = 0.30
    stop_loss: float = 0.15
    trailing_stop: float = 0.12
    max_holding_bars: int = 10

    def __post_init__(self) -> None:
        float_values = (
            self.entry_momentum,
            self.entry_buy_imbalance,
            self.min_liquidity_quote,
            self.min_age_seconds,
            self.max_age_seconds,
            self.exit_momentum,
            self.take_profit,
            self.stop_loss,
            self.trailing_stop,
        )
        if not all(math.isfinite(value) for value in float_values):
            raise ValueError("strategy specification must contain only finite numbers")
        if self.momentum_lookback < 1 or self.volume_lookback < 2:
            raise ValueError("lookbacks are too short")
        if not -1.0 <= self.entry_buy_imbalance <= 1.0:
            raise ValueError("entry_buy_imbalance must be in [-1, 1]")
        if self.min_liquidity_quote < 0.0 or self.min_trade_count < 0:
            raise ValueError("liquidity and trade count thresholds cannot be negative")
        if self.min_age_seconds < 0.0 or self.max_age_seconds <= self.min_age_seconds:
            raise ValueError("invalid token age range")
        if self.take_profit <= 0.0 or not 0.0 < self.stop_loss < 1.0:
            raise ValueError("invalid take-profit or stop-loss")
        if not 0.0 < self.trailing_stop < 1.0:
            raise ValueError("trailing_stop must be in (0, 1)")
        if self.max_holding_bars < 1:
            raise ValueError("max_holding_bars must be positive")


@dataclass(frozen=True, slots=True)
class Fill:
    market_id: str
    mint: str
    quote_mint: str
    time_ns: int
    available_at_ns: int
    side: Side
    base_amount: float
    quote_amount: float
    mark_price: float
    average_price: float
    curve_impact_bps: float
    venue_fee_quote: float
    fixed_cost_quote: float
    latency_bars: int
    model: str


@dataclass(frozen=True, slots=True)
class RoundTrip:
    market_id: str
    mint: str
    quote_mint: str
    entry: Fill
    exit: Fill
    entry_signal_time_ns: int
    exit_signal_time_ns: int
    exit_reason: str
    quote_pnl: float
    return_pct: float
    holding_bars: int
    prior_failure_cost_quote: float


@dataclass(frozen=True, slots=True)
class OrderFailure:
    market_id: str
    mint: str
    quote_mint: str
    time_ns: int
    side: Side
    fixed_cost_quote: float
    reason: str
    position_related: bool


@dataclass(frozen=True, slots=True)
class OpenPosition:
    market_id: str
    mint: str
    quote_mint: str
    entry: Fill
    entry_signal_time_ns: int
    as_of_time_ns: int
    liquidation_value_quote: float
    prior_failure_cost_quote: float
    quote_pnl: float
    return_pct: float
    holding_bars: int
    valuation_reason: str


@dataclass(frozen=True, slots=True)
class BacktestResult:
    spec: MomentumSpec
    config: ExecutionConfig
    trades: tuple[RoundTrip, ...]
    open_positions: tuple[OpenPosition, ...]
    failures: tuple[OrderFailure, ...]
    missed_entry_orders: int
    missed_exit_orders: int

    @property
    def returns(self) -> list[float]:
        denominator = self.config.quote_notional + self.config.fixed_cost_quote
        events = [(trade.exit.time_ns, trade.return_pct, trade.market_id) for trade in self.trades]
        events.extend(
            (position.as_of_time_ns, position.return_pct, position.market_id)
            for position in self.open_positions
        )
        events.extend(
            (failure.time_ns, -failure.fixed_cost_quote / denominator, failure.market_id)
            for failure in self.failures
            if not failure.position_related
        )
        events.sort()
        return [event[1] for event in events]

    @property
    def rejected_orders(self) -> int:
        return len(self.failures)

    @property
    def missed_orders(self) -> int:
        return self.missed_entry_orders + self.missed_exit_orders

    @property
    def failed_order_cost_quote(self) -> float:
        return sum(failure.fixed_cost_quote for failure in self.failures)

    @property
    def total_quote_pnl(self) -> float:
        standalone_failures = sum(
            failure.fixed_cost_quote for failure in self.failures if not failure.position_related
        )
        return (
            sum(trade.quote_pnl for trade in self.trades)
            + sum(position.quote_pnl for position in self.open_positions)
            - standalone_failures
        )

    @property
    def unclosed_positions(self) -> int:
        return len(self.open_positions)

    @property
    def position_outcomes(self) -> int:
        return len(self.trades) + len(self.open_positions)

    @property
    def attempted_capital_quote(self) -> float:
        """Fixed notional budget attached to every attempted entry.

        Landed failed entries spend only their fixed send cost, and non-landed
        entries spend nothing, but the configured quote notional was still the
        budget behind either attempt.  Including it prevents poor landing from
        shrinking the denominator.  Position-related exit attempts do not
        create a second capital unit.
        """
        filled_entries = sum(
            trade.entry.quote_amount + trade.entry.fixed_cost_quote for trade in self.trades
        ) + sum(
            position.entry.quote_amount + position.entry.fixed_cost_quote
            for position in self.open_positions
        )
        failed_entries = sum(
            self.config.quote_notional + failure.fixed_cost_quote
            for failure in self.failures
            if not failure.position_related
        )
        missed_entry_budgets = self.missed_entry_orders * self.config.quote_notional
        return filled_entries + failed_entries + missed_entry_budgets

    @property
    def return_on_attempted_capital(self) -> float:
        attempted = self.attempted_capital_quote
        return self.total_quote_pnl / attempted if attempted > 0.0 else 0.0


@dataclass(frozen=True, slots=True)
class PreparedBacktestBars:
    """Validated feature rows reusable by specs with identical lookbacks."""

    momentum_lookback: int
    volume_lookback: int
    markets: tuple[tuple[str, tuple[FeatureRow, ...]], ...]
    row_count: int
    quote_mint: str | None


@dataclass(slots=True)
class _Position:
    entry: Fill
    entry_signal_time_ns: int
    high_mark: float
    bar_duration_ns: int
    failure_cost_quote: float = 0.0


def _mark_price(bar: Bar) -> float:
    if bar.virtual_base_reserve > 0.0 and bar.virtual_quote_reserve > 0.0:
        return bar.virtual_quote_reserve / bar.virtual_base_reserve
    return bar.close


def _available_at(bar: Bar) -> int:
    return bar.end_time_ns if bar.available_at_ns is None else bar.available_at_ns


def _due_index(rows: Sequence[FeatureRow], index: int, latency_bars: int) -> int | None:
    signal = rows[index].bar
    duration = signal.end_time_ns - signal.start_time_ns
    due_at = _available_at(signal) + latency_bars * duration
    for candidate_index in range(index + 1, len(rows)):
        candidate = rows[candidate_index].bar
        if candidate.end_time_ns >= due_at and _available_at(candidate) >= due_at:
            return candidate_index
    return None


def _holding_bars(position: _Position, bar: Bar) -> int:
    elapsed = max(0, bar.end_time_ns - position.entry.time_ns)
    return elapsed // position.bar_duration_ns


def _lands(market_id: str, time_ns: int, side: Side, config: ExecutionConfig) -> bool:
    if config.landing_probability >= 1.0:
        return True
    if config.landing_probability <= 0.0:
        return False
    payload = f"{config.random_seed}:{market_id}:{time_ns}:{side.value}".encode()
    draw = int.from_bytes(hashlib.sha256(payload).digest()[:8], "big") / 2**64
    return draw < config.landing_probability


def _within_capacity(bar: Bar, quote_equivalent: float, config: ExecutionConfig) -> bool:
    if quote_equivalent > bar.virtual_quote_reserve * config.max_reserve_fraction:
        return False
    if bar.quote_volume <= 0.0:
        return False
    return quote_equivalent <= bar.quote_volume * config.max_bar_volume_fraction


def constant_product_buy(bar: Bar, config: ExecutionConfig) -> Fill | None:
    """Buy base with a fixed gross quote amount against the snapshot reserves."""
    if bar.phase is VenuePhase.CLOB:
        raise ValueError("CLOB bars require an order-book execution adapter")
    quote_in = config.quote_notional
    if not _within_capacity(bar, quote_in, config):
        return None
    base_reserve = bar.virtual_base_reserve
    quote_reserve = bar.virtual_quote_reserve
    if base_reserve <= 0.0 or quote_reserve <= 0.0:
        return None
    fee_bps = (
        float(bar.venue_fee_bps)
        if config.use_observed_venue_fee and bar.venue_fee_bps is not None
        else config.venue_fee_bps
    )
    fee_rate = fee_bps / 10_000.0
    if bar.phase is VenuePhase.BONDING_CURVE:
        # Pump's exact-quote helper interprets the configured quote amount as a
        # gross user budget, solves the pre-fee curve input by inverse fee, and
        # then charges quote-denominated fees.  This floating-point branch
        # preserves that ordering; protocol_math.py owns atomic rounding.
        net_quote = quote_in / (1.0 + fee_rate)
        venue_fee = quote_in - net_quote
        model = "bonding_curve_normalized_pump_fee_order_v2"
    else:
        # PumpSwap replay is still a normalised constant-product approximation
        # until its versioned atomic fee adapter is wired in.
        venue_fee = quote_in * fee_rate
        net_quote = quote_in - venue_fee
        model = "pumpswap_normalized_cp_approx_v2"
    invariant = base_reserve * quote_reserve
    raw_base_out = base_reserve - invariant / (quote_reserve + net_quote)
    if bar.real_base_reserve is not None and raw_base_out > bar.real_base_reserve:
        return None
    adverse_multiplier = max(0.0, 1.0 - config.adverse_selection_bps / 10_000.0)
    base_out = raw_base_out * adverse_multiplier
    if base_out <= 0.0:
        return None
    mark = _mark_price(bar)
    average = quote_in / base_out
    impact = max(0.0, average / mark - 1.0) * 10_000.0
    return Fill(
        market_id=bar.market_id,
        mint=bar.mint,
        quote_mint=bar.quote_mint,
        time_ns=bar.end_time_ns,
        available_at_ns=_available_at(bar),
        side=Side.BUY,
        base_amount=base_out,
        quote_amount=quote_in,
        mark_price=mark,
        average_price=average,
        curve_impact_bps=impact,
        venue_fee_quote=venue_fee,
        fixed_cost_quote=config.fixed_cost_quote,
        latency_bars=config.latency_bars,
        model=model,
    )


def constant_product_sell(bar: Bar, base_amount: float, config: ExecutionConfig) -> Fill | None:
    """Sell a base amount against the snapshot reserve curve."""
    if bar.phase is VenuePhase.CLOB:
        raise ValueError("CLOB bars require an order-book execution adapter")
    if base_amount <= 0.0:
        return None
    base_reserve = bar.virtual_base_reserve
    quote_reserve = bar.virtual_quote_reserve
    if base_reserve <= 0.0 or quote_reserve <= 0.0:
        return None
    mark = _mark_price(bar)
    quote_equivalent = base_amount * mark
    if not _within_capacity(bar, quote_equivalent, config):
        return None
    fee_bps = (
        float(bar.venue_fee_bps)
        if config.use_observed_venue_fee and bar.venue_fee_bps is not None
        else config.venue_fee_bps
    )
    fee_rate = fee_bps / 10_000.0
    invariant = base_reserve * quote_reserve
    if bar.phase is VenuePhase.BONDING_CURVE:
        # The Pump curve walks the full base input first, then subtracts
        # quote-denominated protocol/creator fees from curve output.
        raw_quote_out = quote_reserve - invariant / (base_reserve + base_amount)
        venue_fee = raw_quote_out * fee_rate
        fee_adjusted_quote_out = raw_quote_out - venue_fee
        model = "bonding_curve_normalized_pump_fee_order_v2"
    else:
        # Generic PumpSwap approximation: input fee before the invariant walk.
        net_base = base_amount * (1.0 - fee_rate)
        raw_quote_out = quote_reserve - invariant / (base_reserve + net_base)
        venue_fee = base_amount * fee_rate * mark
        fee_adjusted_quote_out = raw_quote_out
        model = "pumpswap_normalized_cp_approx_v2"
    if bar.real_quote_reserve is not None and raw_quote_out > bar.real_quote_reserve:
        return None
    adverse_multiplier = max(0.0, 1.0 - config.adverse_selection_bps / 10_000.0)
    quote_out = fee_adjusted_quote_out * adverse_multiplier
    if quote_out <= config.fixed_cost_quote:
        return None
    average = quote_out / base_amount
    impact = max(0.0, 1.0 - average / mark) * 10_000.0
    return Fill(
        market_id=bar.market_id,
        mint=bar.mint,
        quote_mint=bar.quote_mint,
        time_ns=bar.end_time_ns,
        available_at_ns=_available_at(bar),
        side=Side.SELL,
        base_amount=base_amount,
        quote_amount=quote_out,
        mark_price=mark,
        average_price=average,
        curve_impact_bps=impact,
        venue_fee_quote=venue_fee,
        fixed_cost_quote=config.fixed_cost_quote,
        latency_bars=config.latency_bars,
        model=model,
    )


def _should_enter(row: FeatureRow, spec: MomentumSpec) -> bool:
    bar = row.bar
    return (
        bar.token_age_seconds is not None
        and row.momentum is not None
        and row.momentum >= spec.entry_momentum
        and row.buy_imbalance >= spec.entry_buy_imbalance
        and row.liquidity_quote >= spec.min_liquidity_quote
        and bar.trade_count >= spec.min_trade_count
        and spec.min_age_seconds <= bar.token_age_seconds <= spec.max_age_seconds
    )


def _exit_reason(row: FeatureRow, position: _Position, spec: MomentumSpec) -> str | None:
    mark = _mark_price(row.bar)
    position.high_mark = max(position.high_mark, mark)
    move = mark / position.entry.average_price - 1.0
    if move >= spec.take_profit:
        return "take_profit"
    if move <= -spec.stop_loss:
        return "stop_loss"
    if mark / position.high_mark - 1.0 <= -spec.trailing_stop:
        return "trailing_stop"
    if _holding_bars(position, row.bar) >= spec.max_holding_bars:
        return "max_holding"
    if row.momentum is not None and row.momentum <= spec.exit_momentum:
        return "momentum_reversal"
    return None


def _terminal_position(position: _Position, bar: Bar, config: ExecutionConfig) -> OpenPosition:
    """Conservatively value inventory without inventing an end-of-sample fill."""
    liquidation = constant_product_sell(bar, position.entry.base_amount, config)
    if liquidation is None:
        liquidation_value = 0.0
        reason = "zero_recovery_not_executable"
    else:
        liquidation_value = max(0.0, liquidation.quote_amount - liquidation.fixed_cost_quote)
        reason = "executable_mark_not_a_fill"
    invested = position.entry.quote_amount + position.entry.fixed_cost_quote
    pnl = liquidation_value - invested - position.failure_cost_quote
    return OpenPosition(
        market_id=bar.market_id,
        mint=bar.mint,
        quote_mint=bar.quote_mint,
        entry=position.entry,
        entry_signal_time_ns=position.entry_signal_time_ns,
        as_of_time_ns=bar.end_time_ns,
        liquidation_value_quote=liquidation_value,
        prior_failure_cost_quote=position.failure_cost_quote,
        quote_pnl=pnl,
        return_pct=pnl / invested,
        holding_bars=_holding_bars(position, bar),
        valuation_reason=reason,
    )


def _run_one_market(
    rows: Sequence[FeatureRow], spec: MomentumSpec, config: ExecutionConfig
) -> tuple[list[RoundTrip], list[OpenPosition], list[OrderFailure], int, int]:
    available_times = [_available_at(row.bar) for row in rows]
    if any(
        current < previous
        for previous, current in zip(available_times, available_times[1:], strict=False)
    ):
        raise ValueError("market snapshots must have monotonic available_at_ns")
    trades: list[RoundTrip] = []
    open_positions: list[OpenPosition] = []
    failures: list[OrderFailure] = []
    missed_entries = 0
    missed_exits = 0
    position: _Position | None = None
    pending_entry: tuple[int, int] | None = None
    pending_exit: tuple[int, int, str] | None = None

    for index, row in enumerate(rows):
        if pending_entry is not None and pending_entry[0] == index and position is None:
            _, signal_time = pending_entry
            landed = _lands(row.bar.market_id, row.bar.end_time_ns, Side.BUY, config)
            entry = constant_product_buy(row.bar, config) if landed else None
            if entry is None:
                if landed:
                    failures.append(
                        OrderFailure(
                            market_id=row.bar.market_id,
                            mint=row.bar.mint,
                            quote_mint=row.bar.quote_mint,
                            time_ns=row.bar.end_time_ns,
                            side=Side.BUY,
                            fixed_cost_quote=config.fixed_cost_quote,
                            reason="capacity_or_slippage",
                            position_related=False,
                        )
                    )
                else:
                    missed_entries += 1
            else:
                position = _Position(
                    entry=entry,
                    entry_signal_time_ns=signal_time,
                    high_mark=_mark_price(row.bar),
                    bar_duration_ns=row.bar.end_time_ns - row.bar.start_time_ns,
                )
            pending_entry = None

        if pending_exit is not None and pending_exit[0] == index and position is not None:
            _, signal_time, reason = pending_exit
            landed = _lands(row.bar.market_id, row.bar.end_time_ns, Side.SELL, config)
            exit_fill = (
                constant_product_sell(row.bar, position.entry.base_amount, config)
                if landed
                else None
            )
            if exit_fill is None:
                if landed:
                    position.failure_cost_quote += config.fixed_cost_quote
                    failures.append(
                        OrderFailure(
                            market_id=row.bar.market_id,
                            mint=row.bar.mint,
                            quote_mint=row.bar.quote_mint,
                            time_ns=row.bar.end_time_ns,
                            side=Side.SELL,
                            fixed_cost_quote=config.fixed_cost_quote,
                            reason="capacity_or_slippage",
                            position_related=True,
                        )
                    )
                else:
                    missed_exits += 1
                pending_exit = None
            else:
                invested = position.entry.quote_amount + position.entry.fixed_cost_quote
                recovered = exit_fill.quote_amount - exit_fill.fixed_cost_quote
                pnl = recovered - invested - position.failure_cost_quote
                trades.append(
                    RoundTrip(
                        market_id=row.bar.market_id,
                        mint=row.bar.mint,
                        quote_mint=row.bar.quote_mint,
                        entry=position.entry,
                        exit=exit_fill,
                        entry_signal_time_ns=position.entry_signal_time_ns,
                        exit_signal_time_ns=signal_time,
                        exit_reason=reason,
                        quote_pnl=pnl,
                        return_pct=pnl / invested,
                        holding_bars=_holding_bars(position, row.bar),
                        prior_failure_cost_quote=position.failure_cost_quote,
                    )
                )
                position = None
                pending_exit = None

        if position is not None and pending_exit is None:
            trigger_reason = _exit_reason(row, position, spec)
            if trigger_reason is not None:
                due = _due_index(rows, index, config.latency_bars)
                if due is not None:
                    pending_exit = (due, _available_at(row.bar), trigger_reason)
        elif position is None and pending_entry is None:
            if _should_enter(row, spec):
                due = _due_index(rows, index, config.latency_bars)
                if due is not None:
                    pending_entry = (due, _available_at(row.bar))

    if position is not None and rows:
        open_positions.append(_terminal_position(position, rows[-1].bar, config))
    return trades, open_positions, failures, missed_entries, missed_exits


def prepare_backtest_bars(
    bars: Iterable[Bar],
    *,
    momentum_lookback: int,
    volume_lookback: int,
) -> PreparedBacktestBars:
    """Validate bars and compute one reusable feature matrix."""

    materialized = bars if isinstance(bars, (list, tuple)) else list(bars)
    quote_mints = {bar.quote_mint for bar in materialized}
    if len(quote_mints) > 1:
        raise ValueError("one backtest cannot mix quote currencies without causal FX")
    if any(bar.phase is VenuePhase.CLOB for bar in materialized):
        raise ValueError("CLOB bars require an order-book execution adapter")
    rows = causal_features(
        materialized,
        momentum_lookback=momentum_lookback,
        volume_lookback=volume_lookback,
    )
    by_market: dict[str, list[FeatureRow]] = {}
    for row in rows:
        by_market.setdefault(row.bar.market_id, []).append(row)
    markets = tuple(
        (
            market_id,
            tuple(
                sorted(
                    by_market[market_id],
                    key=lambda row: (row.bar.end_time_ns, row.bar.phase.value),
                )
            ),
        )
        for market_id in sorted(by_market)
    )
    return PreparedBacktestBars(
        momentum_lookback=momentum_lookback,
        volume_lookback=volume_lookback,
        markets=markets,
        row_count=len(rows),
        quote_mint=next(iter(quote_mints), None),
    )


def run_prepared_backtest(
    prepared: PreparedBacktestBars,
    spec: MomentumSpec,
    config: ExecutionConfig,
) -> BacktestResult:
    """Replay a spec against features prepared for its lookback pair."""

    if (
        spec.momentum_lookback != prepared.momentum_lookback
        or spec.volume_lookback != prepared.volume_lookback
    ):
        raise ValueError("strategy lookbacks do not match prepared feature rows")

    all_trades: list[RoundTrip] = []
    all_open_positions: list[OpenPosition] = []
    all_failures: list[OrderFailure] = []
    missed_entries = 0
    missed_exits = 0
    for _, market_rows in prepared.markets:
        (
            trades,
            open_positions,
            market_failures,
            market_missed_entries,
            market_missed_exits,
        ) = _run_one_market(market_rows, spec, config)
        all_trades.extend(trades)
        all_open_positions.extend(open_positions)
        all_failures.extend(market_failures)
        missed_entries += market_missed_entries
        missed_exits += market_missed_exits
    all_trades.sort(key=lambda trade: (trade.exit.time_ns, trade.market_id))
    all_open_positions.sort(key=lambda position: (position.as_of_time_ns, position.market_id))
    all_failures.sort(key=lambda failure: (failure.time_ns, failure.market_id, failure.side.value))
    return BacktestResult(
        spec=spec,
        config=config,
        trades=tuple(all_trades),
        open_positions=tuple(all_open_positions),
        failures=tuple(all_failures),
        missed_entry_orders=missed_entries,
        missed_exit_orders=missed_exits,
    )


def run_backtest(
    bars: Iterable[Bar], spec: MomentumSpec, config: ExecutionConfig
) -> BacktestResult:
    """Replay independently per market and return fixed-notional outcomes.

    A run must contain one quote currency because this baseline has no causal FX
    conversion.  The result is not a shared-capital portfolio equity curve.
    """

    prepared = prepare_backtest_bars(
        bars,
        momentum_lookback=spec.momentum_lookback,
        volume_lookback=spec.volume_lookback,
    )
    return run_prepared_backtest(prepared, spec, config)
