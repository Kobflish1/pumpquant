"""Fail-closed exact execution and shared-capital development survivor gates.

The statistical research grids deliberately use normalized floating-point AMM
labels.  This module is the separate promotion boundary: it re-quotes the
committed development positions with protocol integer math and then replays one
cash-constrained, chronological portfolio.  It has no filesystem or holdout
reader; callers must pass already verified development rows and causal FX data.
"""

from __future__ import annotations

import bisect
import csv
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from pathlib import Path
from typing import Final, cast

from pumpquant.pricing.bonding_curve import pump_buy_exact_quote_in, pump_sell_exact_base
from pumpquant.pricing.pumpswap import (
    PumpSwapFees,
    pumpswap_buy_exact_quote_in,
    pumpswap_sell_exact_base,
)

BPS_DENOMINATOR: Final[int] = 10_000
_NATIVE_SOL: Final[str] = "11111111111111111111111111111111"
_WRAPPED_SOL: Final[str] = "So11111111111111111111111111111111111111112"
_SOL_ALIASES: Final[frozenset[str]] = frozenset(("SOL", _NATIVE_SOL, _WRAPPED_SOL))
LAMPORTS_PER_SOL: Final[int] = 1_000_000_000


class ExactSurvivorGateError(RuntimeError):
    """An input violates the frozen exact-replay contract."""


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ExactSurvivorGateError(f"{label} is missing or invalid")
    return value


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ExactSurvivorGateError(f"{label} is missing or invalid")
    return value


def _optional_integer(value: object, label: str, *, minimum: int = 0) -> int | None:
    if value is None:
        return None
    return _integer(value, label, minimum=minimum)


def _boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise ExactSurvivorGateError(f"{label} is missing or invalid")
    return value


@dataclass(frozen=True, order=True, slots=True)
class ChainKey:
    """Deterministic landed-event order."""

    slot: int
    tx_index: int
    log_index: int
    event_index: int
    event_id: str


@dataclass(frozen=True, slots=True)
class AtomicEventState:
    """The survivor-only state retained for one causal event pre-state."""

    event_id: str
    market_id: str
    mint: str
    key: ChainKey
    available_at_ns: int
    phase: str
    side: str
    protocol_side: str | None
    instruction_name: str | None
    base_decimals: int
    quote_decimals: int
    fee_asset: str
    fee_mint: str
    fee_decimals: int
    pool_orientation_flipped: bool | None
    protocol_fee_bps: int
    lp_fee_bps: int | None
    creator_fee_bps: int
    cashback_fee_bps: int
    buyback_fee_bps: int
    venue_fee_bps: int
    pre_base_atomic: int
    pre_quote_atomic: int
    pre_real_base_atomic: int
    pre_real_quote_atomic: int
    coverage_gap: bool
    reserve_continuity: bool
    real_reserve_continuity: bool
    continuity_segment: int

    @classmethod
    def from_mapping(cls, row: Mapping[str, object]) -> AtomicEventState:
        event_id = _text(row.get("event_id"), "event_id")
        protocol_side_value = row.get("protocol_side")
        instruction_value = row.get("instruction_name")
        flipped_value = row.get("pool_orientation_flipped")
        state = cls(
            event_id=event_id,
            market_id=_text(row.get("market_id"), "market_id"),
            mint=_text(row.get("mint"), "mint"),
            key=ChainKey(
                _integer(row.get("slot"), "slot"),
                _integer(row.get("tx_index"), "tx_index"),
                _integer(row.get("log_index"), "log_index"),
                _integer(row.get("event_index"), "event_index"),
                event_id,
            ),
            available_at_ns=_integer(row.get("available_at_ns"), "available_at_ns"),
            phase=_text(row.get("phase"), "phase"),
            side=_text(row.get("side"), "side"),
            protocol_side=(
                None if protocol_side_value is None else _text(protocol_side_value, "protocol_side")
            ),
            instruction_name=(
                None if instruction_value is None else _text(instruction_value, "instruction_name")
            ),
            base_decimals=_integer(row.get("base_decimals"), "base_decimals"),
            quote_decimals=_integer(row.get("quote_decimals"), "quote_decimals"),
            fee_asset=_text(row.get("fee_asset"), "fee_asset"),
            fee_mint=_text(row.get("fee_mint"), "fee_mint"),
            fee_decimals=_integer(row.get("fee_decimals"), "fee_decimals"),
            pool_orientation_flipped=(
                None
                if flipped_value is None
                else _boolean(flipped_value, "pool_orientation_flipped")
            ),
            protocol_fee_bps=_integer(row.get("protocol_fee_bps"), "protocol_fee_bps"),
            lp_fee_bps=_optional_integer(row.get("lp_fee_bps"), "lp_fee_bps"),
            creator_fee_bps=_integer(row.get("creator_fee_bps"), "creator_fee_bps"),
            cashback_fee_bps=_integer(row.get("cashback_fee_bps"), "cashback_fee_bps"),
            buyback_fee_bps=_integer(row.get("buyback_fee_bps"), "buyback_fee_bps"),
            venue_fee_bps=_integer(row.get("venue_fee_bps"), "venue_fee_bps"),
            pre_base_atomic=_integer(
                row.get("effective_pre_base_atomic"),
                "effective_pre_base_atomic",
                minimum=1,
            ),
            pre_quote_atomic=_integer(
                row.get("effective_pre_quote_atomic"),
                "effective_pre_quote_atomic",
                minimum=1,
            ),
            pre_real_base_atomic=_integer(
                row.get("effective_pre_real_base_atomic"),
                "effective_pre_real_base_atomic",
                minimum=1,
            ),
            pre_real_quote_atomic=_integer(
                row.get("effective_pre_real_quote_atomic"),
                "effective_pre_real_quote_atomic",
                minimum=1,
            ),
            coverage_gap=_boolean(row.get("coverage_gap"), "coverage_gap"),
            reserve_continuity=_boolean(row.get("reserve_continuity"), "reserve_continuity"),
            real_reserve_continuity=_boolean(
                row.get("real_reserve_continuity"), "real_reserve_continuity"
            ),
            continuity_segment=_integer(row.get("continuity_segment"), "continuity_segment"),
        )
        state.validate()
        return state

    def validate(self) -> None:
        if self.phase not in {"bonding_curve", "pumpswap"}:
            raise ExactSurvivorGateError("unsupported phase")
        if self.side not in {"buy", "sell"}:
            raise ExactSurvivorGateError("unsupported canonical side")
        if self.protocol_side not in {None, "buy", "sell"}:
            raise ExactSurvivorGateError("unsupported protocol side")
        if self.base_decimals > 30 or self.quote_decimals != 9:
            raise ExactSurvivorGateError("exact gate requires plausible base and SOL decimals")
        if self.fee_asset not in {"base", "quote"}:
            raise ExactSurvivorGateError("fee asset is not canonical base or quote")
        expected_fee_decimals = (
            self.base_decimals if self.fee_asset == "base" else self.quote_decimals
        )
        if self.fee_decimals != expected_fee_decimals:
            raise ExactSurvivorGateError("fee decimals disagree with fee asset")
        expected_fee_mint = (
            self.fee_mint == self.mint
            if self.fee_asset == "base"
            else self.fee_mint in _SOL_ALIASES
        )
        if not expected_fee_mint:
            raise ExactSurvivorGateError("fee mint disagrees with canonical fee asset")
        for name, value in (
            ("protocol_fee_bps", self.protocol_fee_bps),
            ("creator_fee_bps", self.creator_fee_bps),
            ("cashback_fee_bps", self.cashback_fee_bps),
            ("buyback_fee_bps", self.buyback_fee_bps),
            ("venue_fee_bps", self.venue_fee_bps),
        ):
            if value >= BPS_DENOMINATOR:
                raise ExactSurvivorGateError(f"{name} is implausible")
        if self.creator_fee_bps and self.cashback_fee_bps:
            raise ExactSurvivorGateError("creator and cashback routes are both active")
        if self.phase == "bonding_curve":
            if self.pool_orientation_flipped is not None or self.fee_asset != "quote":
                raise ExactSurvivorGateError("Pump bonding-curve orientation is invalid")
            if self.lp_fee_bps not in {None, 0}:
                raise ExactSurvivorGateError("Pump bonding curve has an LP fee")
            expected = self.protocol_fee_bps + self.creator_fee_bps + self.cashback_fee_bps
        else:
            if self.pool_orientation_flipped is None or self.lp_fee_bps is None:
                raise ExactSurvivorGateError("PumpSwap orientation or LP fee is missing")
            expected_asset = "base" if self.pool_orientation_flipped else "quote"
            if self.fee_asset != expected_asset:
                raise ExactSurvivorGateError("PumpSwap fee asset disagrees with pool orientation")
            expected = (
                self.protocol_fee_bps
                + self.lp_fee_bps
                + self.creator_fee_bps
                + self.cashback_fee_bps
            )
        if expected != self.venue_fee_bps or not 0 <= expected <= 500:
            raise ExactSurvivorGateError("effective fee bps does not reconcile")
        if self.coverage_gap or not self.reserve_continuity or not self.real_reserve_continuity:
            raise ExactSurvivorGateError("event state is gapped or discontinuous")


@dataclass(frozen=True, slots=True)
class PositionSignal:
    """One committed OOS position from a sealed statistical screen."""

    family: str
    candidate_sha256: str
    fold: int
    era: int
    mint: str
    decision_index: int
    decision_event_id: str
    decision_available_at_ns: int
    entry_event_id: str
    entry_available_at_ns: int
    exit_event_id: str
    exit_available_at_ns: int
    holding_events: int

    @classmethod
    def from_mapping(cls, row: Mapping[str, object], *, family: str) -> PositionSignal:
        candidate = _text(row.get("candidate_sha256"), "candidate_sha256")
        if len(candidate) != 64 or any(
            character not in "0123456789abcdef" for character in candidate
        ):
            raise ExactSurvivorGateError("candidate_sha256 is invalid")
        holding = _integer(row.get("holding_events"), "holding_events", minimum=1)
        if holding not in {20, 50}:
            raise ExactSurvivorGateError("holding_events is outside the frozen set")
        return cls(
            family=_text(family, "family"),
            candidate_sha256=candidate,
            fold=_integer(row.get("fold"), "fold", minimum=1),
            era=_integer(row.get("era"), "era", minimum=1),
            mint=_text(row.get("mint"), "mint"),
            decision_index=_integer(row.get("decision_index"), "decision_index"),
            decision_event_id=_text(row.get("decision_event_id"), "decision_event_id"),
            decision_available_at_ns=_integer(
                row.get("decision_available_at_ns"), "decision_available_at_ns"
            ),
            entry_event_id=_text(row.get("entry_event_id"), "entry_event_id"),
            entry_available_at_ns=_integer(
                row.get("entry_available_at_ns"), "entry_available_at_ns"
            ),
            exit_event_id=_text(row.get("exit_event_id"), "exit_event_id"),
            exit_available_at_ns=_integer(row.get("exit_available_at_ns"), "exit_available_at_ns"),
            holding_events=holding,
        )


@dataclass(frozen=True, slots=True)
class ExactGateConfig:
    fixed_screen_quote_atomic: int = 100_000_000
    fixed_cost_per_leg_atomic: int = 160_000
    adverse_haircut_bps: int = 30
    max_reserve_fraction_bps: int = 25
    starting_usd: Decimal = Decimal("10")
    allocation_percent: int = 20
    max_concurrent_positions: int = 5

    def __post_init__(self) -> None:
        for name in ("fixed_screen_quote_atomic", "fixed_cost_per_leg_atomic"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("adverse_haircut_bps", "max_reserve_fraction_bps"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 10_000:
                raise ValueError(f"{name} is invalid")
        if not self.starting_usd.is_finite() or self.starting_usd <= 0:
            raise ValueError("starting_usd must be positive and finite")
        if not 1 <= self.allocation_percent <= 100:
            raise ValueError("allocation_percent must be in [1, 100]")
        if self.max_concurrent_positions < 1:
            raise ValueError("max_concurrent_positions must be positive")


@dataclass(frozen=True, slots=True)
class ExactTrade:
    signal: PositionSignal
    entry_key: ChainKey
    exit_key: ChainKey
    quote_budget_atomic: int
    entry_quote_spent_atomic: int
    entry_base_before_haircut_atomic: int
    entry_base_after_haircut_atomic: int
    exit_quote_before_haircut_atomic: int
    exit_quote_after_haircut_atomic: int
    fixed_cost_atomic: int
    pnl_quote_atomic: int
    entry_fee_asset: str
    exit_fee_asset: str


def _haircut(amount: int, bps: int) -> int:
    if amount < 0:
        raise ExactSurvivorGateError("cannot haircut a negative amount")
    return amount * (BPS_DENOMINATOR - bps) // BPS_DENOMINATOR


def _pumpswap_fees(state: AtomicEventState) -> PumpSwapFees:
    if state.lp_fee_bps is None:
        raise ExactSurvivorGateError("PumpSwap LP fee is missing")
    # Decoded buyback is a routed breakdown in the sealed interval, not an
    # additive trader charge.  It remains in AtomicEventState for audit only.
    return PumpSwapFees(
        lp_fee_bps=state.lp_fee_bps,
        protocol_fee_bps=state.protocol_fee_bps,
        creator_fee_bps=state.creator_fee_bps,
        cashback_fee_bps=state.cashback_fee_bps,
        buyback_fee_bps=0,
    )


def _validate_position_states(
    signal: PositionSignal,
    decision: AtomicEventState,
    entry: AtomicEventState,
    exit_state: AtomicEventState,
) -> None:
    if (
        decision.event_id != signal.decision_event_id
        or entry.event_id != signal.entry_event_id
        or exit_state.event_id != signal.exit_event_id
    ):
        raise ExactSurvivorGateError("position/event join is inconsistent")
    if (
        decision.available_at_ns != signal.decision_available_at_ns
        or entry.available_at_ns != signal.entry_available_at_ns
        or exit_state.available_at_ns != signal.exit_available_at_ns
    ):
        raise ExactSurvivorGateError("position availability timestamp was altered")
    if not (
        decision.key < entry.key < exit_state.key
        and decision.available_at_ns <= entry.available_at_ns <= exit_state.available_at_ns
    ):
        raise ExactSurvivorGateError("position chronology is invalid")
    states = (decision, entry, exit_state)
    if any(state.mint != signal.mint for state in states):
        raise ExactSurvivorGateError("position crosses mints")
    if len({state.market_id for state in states}) != 1:
        raise ExactSurvivorGateError("position crosses markets")
    if len({state.phase for state in states}) != 1:
        raise ExactSurvivorGateError("position crosses venue phases")
    if len({state.pool_orientation_flipped for state in states}) != 1:
        raise ExactSurvivorGateError("position changes pool orientation")
    if len({state.continuity_segment for state in states}) != 1:
        raise ExactSurvivorGateError("position crosses continuity segments")
    if len({(state.base_decimals, state.quote_decimals) for state in states}) != 1:
        raise ExactSurvivorGateError("position asset precision changes")


def quote_position_exact(
    signal: PositionSignal,
    states: Mapping[str, AtomicEventState],
    *,
    gross_quote_budget_atomic: int,
    config: ExactGateConfig | None = None,
) -> ExactTrade:
    """Re-quote one committed entry/exit using only integer arithmetic."""

    controls = ExactGateConfig() if config is None else config
    if isinstance(gross_quote_budget_atomic, bool) or gross_quote_budget_atomic <= 0:
        raise ExactSurvivorGateError("gross quote budget must be positive")
    try:
        decision = states[signal.decision_event_id]
        entry = states[signal.entry_event_id]
        exit_state = states[signal.exit_event_id]
    except KeyError as error:
        raise ExactSurvivorGateError("position state is missing") from error
    _validate_position_states(signal, decision, entry, exit_state)
    if (
        gross_quote_budget_atomic * BPS_DENOMINATOR
        > entry.pre_quote_atomic * controls.max_reserve_fraction_bps
    ):
        raise ExactSurvivorGateError("entry exceeds the frozen reserve-fraction cap")

    try:
        if entry.phase == "bonding_curve":
            pump_entry_quote = pump_buy_exact_quote_in(
                gross_quote_amount=gross_quote_budget_atomic,
                virtual_base_reserve=entry.pre_base_atomic,
                virtual_quote_reserve=entry.pre_quote_atomic,
                real_base_reserve=entry.pre_real_base_atomic,
                protocol_fee_bps=entry.protocol_fee_bps,
                creator_fee_bps=entry.creator_fee_bps + entry.cashback_fee_bps,
                creator_fee_enabled=True,
            )
            entry_spent = pump_entry_quote.user_quote_amount
            base_before_haircut = pump_entry_quote.base_amount
        elif entry.pool_orientation_flipped:
            # Canonical buy(token/SOL) is a protocol sell when the pool stores
            # SOL as base and the token as quote.
            flipped_entry_quote = pumpswap_sell_exact_base(
                base_amount=gross_quote_budget_atomic,
                base_reserve=entry.pre_quote_atomic,
                quote_reserve=entry.pre_base_atomic,
                fees=_pumpswap_fees(entry),
            )
            if flipped_entry_quote.curve_quote_amount > entry.pre_real_base_atomic:
                raise ExactSurvivorGateError("entry exceeds PumpSwap real inventory capacity")
            entry_spent = gross_quote_budget_atomic
            base_before_haircut = flipped_entry_quote.user_quote_amount
        else:
            swap_entry_quote = pumpswap_buy_exact_quote_in(
                gross_quote_amount=gross_quote_budget_atomic,
                base_reserve=entry.pre_base_atomic,
                quote_reserve=entry.pre_quote_atomic,
                fees=_pumpswap_fees(entry),
            )
            if swap_entry_quote.base_amount > entry.pre_real_base_atomic:
                raise ExactSurvivorGateError("entry exceeds PumpSwap real inventory capacity")
            entry_spent = swap_entry_quote.user_quote_amount
            base_before_haircut = swap_entry_quote.base_amount
        base_after_haircut = _haircut(base_before_haircut, controls.adverse_haircut_bps)
        if base_after_haircut <= 0:
            raise ExactSurvivorGateError("entry produces no executable base inventory")
        if (
            base_after_haircut * BPS_DENOMINATOR
            > exit_state.pre_base_atomic * controls.max_reserve_fraction_bps
        ):
            raise ExactSurvivorGateError("exit exceeds the frozen reserve-fraction cap")

        if exit_state.phase == "bonding_curve":
            pump_exit_quote = pump_sell_exact_base(
                base_amount=base_after_haircut,
                virtual_base_reserve=exit_state.pre_base_atomic,
                virtual_quote_reserve=exit_state.pre_quote_atomic,
                real_quote_reserve=exit_state.pre_real_quote_atomic,
                protocol_fee_bps=exit_state.protocol_fee_bps,
                creator_fee_bps=(exit_state.creator_fee_bps + exit_state.cashback_fee_bps),
                creator_fee_enabled=True,
            )
            quote_before_haircut = pump_exit_quote.user_quote_amount
        elif exit_state.pool_orientation_flipped:
            # Canonical sell(token/SOL) is a protocol quote-input buy in the
            # flipped pool.  Any one-atomic-unit remainder is conservatively
            # left as worthless token dust rather than credited to PnL.
            flipped_exit_quote = pumpswap_buy_exact_quote_in(
                gross_quote_amount=base_after_haircut,
                base_reserve=exit_state.pre_quote_atomic,
                quote_reserve=exit_state.pre_base_atomic,
                fees=_pumpswap_fees(exit_state),
            )
            if flipped_exit_quote.base_amount > exit_state.pre_real_quote_atomic:
                raise ExactSurvivorGateError("exit exceeds PumpSwap real inventory capacity")
            quote_before_haircut = flipped_exit_quote.base_amount
        else:
            swap_exit_quote = pumpswap_sell_exact_base(
                base_amount=base_after_haircut,
                base_reserve=exit_state.pre_base_atomic,
                quote_reserve=exit_state.pre_quote_atomic,
                fees=_pumpswap_fees(exit_state),
            )
            if swap_exit_quote.curve_quote_amount > exit_state.pre_real_quote_atomic:
                raise ExactSurvivorGateError("exit exceeds PumpSwap real inventory capacity")
            quote_before_haircut = swap_exit_quote.user_quote_amount
    except (ValueError, OverflowError) as error:
        raise ExactSurvivorGateError("protocol integer quote failed") from error

    if quote_before_haircut <= 0:
        raise ExactSurvivorGateError("exit produces no executable quote inventory")
    quote_after_haircut = _haircut(quote_before_haircut, controls.adverse_haircut_bps)
    total_fixed_cost = 2 * controls.fixed_cost_per_leg_atomic
    pnl = quote_after_haircut - entry_spent - total_fixed_cost
    return ExactTrade(
        signal=signal,
        entry_key=entry.key,
        exit_key=exit_state.key,
        quote_budget_atomic=gross_quote_budget_atomic,
        entry_quote_spent_atomic=entry_spent,
        entry_base_before_haircut_atomic=base_before_haircut,
        entry_base_after_haircut_atomic=base_after_haircut,
        exit_quote_before_haircut_atomic=quote_before_haircut,
        exit_quote_after_haircut_atomic=quote_after_haircut,
        fixed_cost_atomic=total_fixed_cost,
        pnl_quote_atomic=pnl,
        entry_fee_asset=entry.fee_asset,
        exit_fee_asset=exit_state.fee_asset,
    )


@dataclass(frozen=True, slots=True)
class FixedNotionalReplay:
    trades: tuple[ExactTrade, ...]
    total_pnl_quote_atomic: int
    profitable: bool


def replay_fixed_notional(
    signals: Sequence[PositionSignal],
    states: Mapping[str, AtomicEventState],
    *,
    config: ExactGateConfig | None = None,
) -> FixedNotionalReplay:
    controls = ExactGateConfig() if config is None else config
    if not signals:
        raise ExactSurvivorGateError("exact replay has no committed positions")
    trades = tuple(
        quote_position_exact(
            signal,
            states,
            gross_quote_budget_atomic=controls.fixed_screen_quote_atomic,
            config=controls,
        )
        for signal in signals
    )
    total = sum(trade.pnl_quote_atomic for trade in trades)
    return FixedNotionalReplay(trades, total, total > 0)


@dataclass(frozen=True, slots=True)
class FxPoint:
    available_at_ns: int
    close_usd: Decimal


@dataclass(frozen=True, slots=True)
class CausalFxSeries:
    points: tuple[FxPoint, ...]

    def __post_init__(self) -> None:
        if not self.points:
            raise ValueError("causal FX series cannot be empty")
        prior = -1
        for point in self.points:
            if point.available_at_ns <= prior:
                raise ValueError("causal FX availability must be strictly increasing")
            if not point.close_usd.is_finite() or point.close_usd <= 0:
                raise ValueError("causal FX close must be positive and finite")
            prior = point.available_at_ns

    @classmethod
    def from_csv(cls, path: Path) -> CausalFxSeries:
        points: list[FxPoint] = []
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            required = {"available_at_ns", "close_usd"}
            if reader.fieldnames is None or not required.issubset(reader.fieldnames):
                raise ExactSurvivorGateError("causal FX CSV schema is invalid")
            for row in reader:
                try:
                    available = int(cast(str, row["available_at_ns"]))
                    close = Decimal(cast(str, row["close_usd"]))
                except (KeyError, TypeError, ValueError, InvalidOperation) as error:
                    raise ExactSurvivorGateError("causal FX row is invalid") from error
                points.append(FxPoint(available, close))
        try:
            return cls(tuple(points))
        except ValueError as error:
            raise ExactSurvivorGateError(str(error)) from error

    def price_at(self, available_at_ns: int) -> Decimal:
        keys = tuple(point.available_at_ns for point in self.points)
        index = bisect.bisect_right(keys, available_at_ns) - 1
        if index < 0:
            raise ExactSurvivorGateError("causal FX is unavailable at decision time")
        return self.points[index].close_usd


@dataclass(frozen=True, slots=True)
class PortfolioDecision:
    signal: PositionSignal
    accepted: bool
    reason: str
    quote_budget_atomic: int | None
    cash_after_atomic: int
    open_position_count: int


@dataclass(frozen=True, slots=True)
class SharedCapitalReplay:
    decisions: tuple[PortfolioDecision, ...]
    accepted_trades: tuple[ExactTrade, ...]
    starting_quote_atomic: int
    final_quote_atomic: int
    starting_usd: Decimal
    final_usd: Decimal
    minimum_equity_quote_atomic: int
    maximum_concurrent_positions: int
    profitable_in_quote: bool
    profitable_in_usd: bool
    passed: bool


@dataclass(slots=True)
class _OpenPosition:
    trade: ExactTrade
    principal_atomic: int


def _usd_to_lamports(usd: Decimal, sol_usd: Decimal) -> int:
    value = (usd / sol_usd * Decimal(LAMPORTS_PER_SOL)).to_integral_value(rounding=ROUND_FLOOR)
    result = int(value)
    if result <= 0:
        raise ExactSurvivorGateError("starting USD converts to no executable SOL")
    return result


def _lamports_to_usd(lamports: int, sol_usd: Decimal) -> Decimal:
    return Decimal(lamports) * sol_usd / Decimal(LAMPORTS_PER_SOL)


def replay_shared_capital(
    signals: Sequence[PositionSignal],
    states: Mapping[str, AtomicEventState],
    fx: CausalFxSeries,
    *,
    development_start_available_at_ns: int,
    config: ExactGateConfig | None = None,
) -> SharedCapitalReplay:
    """Replay a candidate in one unlevered chronological SOL wallet."""

    controls = ExactGateConfig() if config is None else config
    if not signals:
        raise ExactSurvivorGateError("shared-capital replay has no signals")
    start_price = fx.price_at(development_start_available_at_ns)
    starting_quote = _usd_to_lamports(controls.starting_usd, start_price)
    cash = starting_quote
    open_positions: list[_OpenPosition] = []
    accepted: list[ExactTrade] = []
    decisions: list[PortfolioDecision] = []
    minimum_equity = starting_quote
    maximum_concurrent = 0
    last_exit_ns = development_start_available_at_ns

    def equity() -> int:
        return cash + sum(position.principal_atomic for position in open_positions)

    def settle_through(key: ChainKey) -> None:
        nonlocal cash, minimum_equity, last_exit_ns
        due = sorted(
            (position for position in open_positions if position.trade.exit_key <= key),
            key=lambda position: position.trade.exit_key,
        )
        for position in due:
            open_positions.remove(position)
            cash += (
                position.trade.exit_quote_after_haircut_atomic - controls.fixed_cost_per_leg_atomic
            )
            last_exit_ns = max(last_exit_ns, position.trade.signal.exit_available_at_ns)
            minimum_equity = min(minimum_equity, equity())

    def entry_state(signal: PositionSignal) -> AtomicEventState:
        try:
            return states[signal.entry_event_id]
        except KeyError as error:
            raise ExactSurvivorGateError("entry state is missing") from error

    ordered = sorted(
        signals,
        key=lambda signal: (entry_state(signal).key, signal.candidate_sha256, signal.mint),
    )
    for signal in ordered:
        state = entry_state(signal)
        settle_through(state.key)
        reason: str | None = None
        if any(position.trade.signal.mint == signal.mint for position in open_positions):
            reason = "mint_already_open"
        elif len(open_positions) >= controls.max_concurrent_positions:
            reason = "max_concurrent_positions"
        current_equity = equity()
        budget = current_equity * controls.allocation_percent // 100
        trade: ExactTrade | None = None
        if reason is None and budget <= 0:
            reason = "allocation_rounds_to_zero"
        if reason is None:
            try:
                trade = quote_position_exact(
                    signal,
                    states,
                    gross_quote_budget_atomic=budget,
                    config=controls,
                )
            except ExactSurvivorGateError:
                reason = "exact_quote_or_capacity_rejected"
        required_cash = (
            0
            if trade is None
            else trade.entry_quote_spent_atomic + controls.fixed_cost_per_leg_atomic
        )
        if reason is None and required_cash > cash:
            reason = "insufficient_cash"
        if reason is not None or trade is None:
            decisions.append(
                PortfolioDecision(
                    signal,
                    False,
                    cast(str, reason),
                    budget if budget > 0 else None,
                    cash,
                    len(open_positions),
                )
            )
            continue
        cash -= required_cash
        if cash < 0:
            raise ExactSurvivorGateError("portfolio cash became negative")
        open_positions.append(_OpenPosition(trade, trade.entry_quote_spent_atomic))
        accepted.append(trade)
        maximum_concurrent = max(maximum_concurrent, len(open_positions))
        minimum_equity = min(minimum_equity, equity())
        decisions.append(
            PortfolioDecision(
                signal,
                True,
                "accepted",
                budget,
                cash,
                len(open_positions),
            )
        )

    for position in sorted(open_positions, key=lambda item: item.trade.exit_key):
        cash += position.trade.exit_quote_after_haircut_atomic - controls.fixed_cost_per_leg_atomic
        last_exit_ns = max(last_exit_ns, position.trade.signal.exit_available_at_ns)
        minimum_equity = min(minimum_equity, cash)
    open_positions.clear()
    final_price = fx.price_at(last_exit_ns)
    final_usd = _lamports_to_usd(cash, final_price)
    profitable_quote = cash > starting_quote
    profitable_usd = final_usd > controls.starting_usd
    passed = bool(accepted and minimum_equity >= 0 and profitable_quote and profitable_usd)
    return SharedCapitalReplay(
        decisions=tuple(decisions),
        accepted_trades=tuple(accepted),
        starting_quote_atomic=starting_quote,
        final_quote_atomic=cash,
        starting_usd=controls.starting_usd,
        final_usd=final_usd,
        minimum_equity_quote_atomic=minimum_equity,
        maximum_concurrent_positions=maximum_concurrent,
        profitable_in_quote=profitable_quote,
        profitable_in_usd=profitable_usd,
        passed=passed,
    )


def finite_decimal_text(value: Decimal) -> str:
    """Canonical non-exponent decimal text for JSON reports."""

    if not value.is_finite():
        raise ExactSurvivorGateError("report decimal is not finite")
    result = format(value, "f")
    if not result or not math.isfinite(float(value)):
        raise ExactSurvivorGateError("report decimal cannot be represented")
    return result


__all__ = [
    "AtomicEventState",
    "CausalFxSeries",
    "ChainKey",
    "ExactGateConfig",
    "ExactSurvivorGateError",
    "ExactTrade",
    "FixedNotionalReplay",
    "FxPoint",
    "PortfolioDecision",
    "PositionSignal",
    "SharedCapitalReplay",
    "finite_decimal_text",
    "quote_position_exact",
    "replay_fixed_notional",
    "replay_shared_capital",
]
