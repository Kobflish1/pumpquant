"""Generic execution-aware rules over the causal alpha feature registry."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from enum import Enum, StrEnum
from typing import Any

from pumpquant.core.schema import Bar, Side, VenuePhase
from pumpquant.execution.bar_replay import (
    ExecutionConfig,
    Fill,
    OpenPosition,
    OrderFailure,
    RoundTrip,
    constant_product_buy,
    constant_product_sell,
)
from pumpquant.features.alpha import (
    ALPHA_FEATURE_INDEX,
    AlphaFeatureConfig,
    AlphaFeatureRow,
    causal_alpha_features,
)


class RuleOperator(StrEnum):
    """Supported scalar comparisons for transparent signal rules."""

    GREATER_EQUAL = "ge"
    LESS_EQUAL = "le"
    ABS_GREATER_EQUAL = "abs_ge"
    ABS_LESS_EQUAL = "abs_le"


@dataclass(frozen=True, slots=True)
class SignalRule:
    """One auditable comparison against a registered alpha feature."""

    feature: str
    operator: RuleOperator
    threshold: float

    def __post_init__(self) -> None:
        if self.feature not in ALPHA_FEATURE_INDEX:
            raise ValueError(f"unknown alpha feature {self.feature!r}")
        if not isinstance(self.operator, RuleOperator):
            raise ValueError("operator must be a RuleOperator")
        if not math.isfinite(self.threshold):
            raise ValueError("rule threshold must be finite")
        if (
            self.operator
            in {
                RuleOperator.ABS_GREATER_EQUAL,
                RuleOperator.ABS_LESS_EQUAL,
            }
            and self.threshold < 0.0
        ):
            raise ValueError("absolute rule thresholds cannot be negative")

    def matches(self, row: AlphaFeatureRow) -> bool:
        value = row.value(self.feature)
        if value is None:
            return False
        if self.operator is RuleOperator.GREATER_EQUAL:
            return value >= self.threshold
        if self.operator is RuleOperator.LESS_EQUAL:
            return value <= self.threshold
        if self.operator is RuleOperator.ABS_GREATER_EQUAL:
            return abs(value) >= self.threshold
        return abs(value) <= self.threshold


@dataclass(frozen=True, slots=True)
class AlphaStrategySpec:
    """A conjunction of causal entry rules plus common execution exits."""

    name: str
    feature_config: AlphaFeatureConfig
    entry_rules: tuple[SignalRule, ...]
    exit_rule: SignalRule | None = None
    allowed_phases: tuple[VenuePhase, ...] = (
        VenuePhase.BONDING_CURVE,
        VenuePhase.PUMPSWAP,
    )
    min_liquidity_quote: float = 5.0
    min_signal_capacity_multiple: float = 0.0
    min_trade_count: int = 3
    min_age_seconds: float = 5.0
    max_age_seconds: float = 600.0
    take_profit: float = 0.30
    stop_loss: float = 0.15
    trailing_stop: float = 0.12
    max_holding_bars: int = 10

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("strategy name cannot be empty")
        if not isinstance(self.feature_config, AlphaFeatureConfig):
            raise ValueError("feature_config must be an AlphaFeatureConfig")
        if not self.entry_rules:
            raise ValueError("at least one entry rule is required")
        if len(set(self.entry_rules)) != len(self.entry_rules):
            raise ValueError("duplicate entry rules are not allowed")
        if not self.allowed_phases or len(set(self.allowed_phases)) != len(self.allowed_phases):
            raise ValueError("allowed_phases must be non-empty and unique")
        if any(phase is VenuePhase.CLOB for phase in self.allowed_phases):
            raise ValueError("CLOB strategies require an order-book execution adapter")
        numeric = (
            self.min_liquidity_quote,
            self.min_signal_capacity_multiple,
            self.min_age_seconds,
            self.max_age_seconds,
            self.take_profit,
            self.stop_loss,
            self.trailing_stop,
        )
        if not all(math.isfinite(value) for value in numeric):
            raise ValueError("strategy specification must contain finite numbers")
        if (
            self.min_liquidity_quote < 0.0
            or self.min_signal_capacity_multiple < 0.0
            or self.min_trade_count < 0
        ):
            raise ValueError("liquidity, capacity, and trade-count gates cannot be negative")
        if self.min_age_seconds < 0.0 or self.max_age_seconds <= self.min_age_seconds:
            raise ValueError("invalid token-age range")
        if self.take_profit <= 0.0 or not 0.0 < self.stop_loss < 1.0:
            raise ValueError("invalid take-profit or stop-loss")
        if not 0.0 < self.trailing_stop < 1.0:
            raise ValueError("trailing_stop must be in (0, 1)")
        if self.max_holding_bars < 1:
            raise ValueError("max_holding_bars must be positive")


@dataclass(frozen=True, slots=True)
class PreparedAlphaBars:
    """Reusable rich feature rows for specs with one shared configuration."""

    feature_config: AlphaFeatureConfig
    markets: tuple[tuple[str, tuple[AlphaFeatureRow, ...]], ...]
    row_count: int
    quote_mint: str | None


@dataclass(frozen=True, slots=True)
class AlphaBacktestResult:
    """Fixed-notional execution outcomes for a generic alpha rule set."""

    spec: AlphaStrategySpec
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
        return (
            filled_entries + failed_entries + self.missed_entry_orders * self.config.quote_notional
        )

    @property
    def return_on_attempted_capital(self) -> float:
        attempted = self.attempted_capital_quote
        return self.total_quote_pnl / attempted if attempted > 0.0 else 0.0


@dataclass(slots=True)
class _Position:
    entry: Fill
    entry_signal_time_ns: int
    high_mark: float
    bar_duration_ns: int
    failure_cost_quote: float = 0.0


def _mark_price(bar: Bar) -> float:
    return float(bar.virtual_quote_reserve) / float(bar.virtual_base_reserve)


def _available_at(bar: Bar) -> int:
    return bar.end_time_ns if bar.available_at_ns is None else bar.available_at_ns


def _due_index(rows: Sequence[AlphaFeatureRow], index: int, latency_bars: int) -> int | None:
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


def signal_capacity_multiple(bar: Bar, config: ExecutionConfig) -> float:
    """Current observable capacity divided by requested entry notional.

    This is a signal-time risk gate, not a fill promise.  The actual order is
    still replayed against the later latency-eligible snapshot and can fail.
    """

    reserve_capacity = float(bar.virtual_quote_reserve) * config.max_reserve_fraction
    volume_capacity = float(bar.quote_volume) * config.max_bar_volume_fraction
    return min(reserve_capacity, volume_capacity) / config.quote_notional


def _should_enter(row: AlphaFeatureRow, spec: AlphaStrategySpec, config: ExecutionConfig) -> bool:
    bar = row.bar
    liquidity = row.value("liquidity_quote")
    return (
        bar.phase in spec.allowed_phases
        and bar.token_age_seconds is not None
        and spec.min_age_seconds <= float(bar.token_age_seconds) <= spec.max_age_seconds
        and bar.trade_count >= spec.min_trade_count
        and liquidity is not None
        and liquidity >= spec.min_liquidity_quote
        and signal_capacity_multiple(bar, config) >= spec.min_signal_capacity_multiple
        and all(rule.matches(row) for rule in spec.entry_rules)
    )


def _exit_reason(row: AlphaFeatureRow, position: _Position, spec: AlphaStrategySpec) -> str | None:
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
    if spec.exit_rule is not None and spec.exit_rule.matches(row):
        return f"feature_exit:{spec.exit_rule.feature}:{spec.exit_rule.operator.value}"
    return None


def _terminal_position(position: _Position, bar: Bar, config: ExecutionConfig) -> OpenPosition:
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
    rows: Sequence[AlphaFeatureRow], spec: AlphaStrategySpec, config: ExecutionConfig
) -> tuple[list[RoundTrip], list[OpenPosition], list[OrderFailure], int, int]:
    availability = [_available_at(row.bar) for row in rows]
    if any(
        current < previous
        for previous, current in zip(availability, availability[1:], strict=False)
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
        elif position is None and pending_entry is None and _should_enter(row, spec, config):
            due = _due_index(rows, index, config.latency_bars)
            if due is not None:
                pending_entry = (due, _available_at(row.bar))

    if position is not None and rows:
        open_positions.append(_terminal_position(position, rows[-1].bar, config))
    return trades, open_positions, failures, missed_entries, missed_exits


def prepare_alpha_bars(
    bars: Iterable[Bar], feature_config: AlphaFeatureConfig | None = None
) -> PreparedAlphaBars:
    """Validate bars and compute one reusable multi-family feature matrix."""

    selected = feature_config or AlphaFeatureConfig()
    materialized = bars if isinstance(bars, (list, tuple)) else list(bars)
    quote_mints = {bar.quote_mint for bar in materialized}
    if len(quote_mints) > 1:
        raise ValueError("one backtest cannot mix quote currencies without causal FX")
    if any(bar.phase is VenuePhase.CLOB for bar in materialized):
        raise ValueError("CLOB bars require an order-book execution adapter")
    rows = causal_alpha_features(materialized, selected)
    by_market: dict[str, list[AlphaFeatureRow]] = {}
    for row in rows:
        by_market.setdefault(row.bar.market_id, []).append(row)
    markets = tuple((market_id, tuple(by_market[market_id])) for market_id in sorted(by_market))
    return PreparedAlphaBars(
        feature_config=selected,
        markets=markets,
        row_count=len(rows),
        quote_mint=next(iter(quote_mints), None),
    )


def run_prepared_alpha_backtest(
    prepared: PreparedAlphaBars,
    spec: AlphaStrategySpec,
    config: ExecutionConfig,
) -> AlphaBacktestResult:
    """Replay one transparent rule set against a prepared feature matrix."""

    if spec.feature_config != prepared.feature_config:
        raise ValueError("strategy feature configuration does not match prepared rows")
    all_trades: list[RoundTrip] = []
    all_open_positions: list[OpenPosition] = []
    all_failures: list[OrderFailure] = []
    missed_entries = 0
    missed_exits = 0
    for _, rows in prepared.markets:
        trades, positions, failures, market_missed_entries, market_missed_exits = _run_one_market(
            rows, spec, config
        )
        all_trades.extend(trades)
        all_open_positions.extend(positions)
        all_failures.extend(failures)
        missed_entries += market_missed_entries
        missed_exits += market_missed_exits
    all_trades.sort(key=lambda trade: (trade.exit.time_ns, trade.market_id))
    all_open_positions.sort(key=lambda position: (position.as_of_time_ns, position.market_id))
    all_failures.sort(key=lambda failure: (failure.time_ns, failure.market_id, failure.side.value))
    return AlphaBacktestResult(
        spec=spec,
        config=config,
        trades=tuple(all_trades),
        open_positions=tuple(all_open_positions),
        failures=tuple(all_failures),
        missed_entry_orders=missed_entries,
        missed_exit_orders=missed_exits,
    )


def run_alpha_backtest(
    bars: Iterable[Bar], spec: AlphaStrategySpec, config: ExecutionConfig
) -> AlphaBacktestResult:
    """Prepare and replay a generic multi-family alpha rule set."""

    return run_prepared_alpha_backtest(prepare_alpha_bars(bars, spec.feature_config), spec, config)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def alpha_spec_sha256(spec: AlphaStrategySpec) -> str:
    """Canonical content hash for ledgers and deterministic tie breaking."""

    payload = _json_safe(asdict(spec))
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()
