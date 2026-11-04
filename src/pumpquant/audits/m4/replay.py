"""Causal, execution-aware replay primitives for the invalidated M4 rule.

Archive PumpSwap events expose integer-second block times, not the local
processed-arrival timestamps used by the live runner.  This module therefore
assigns an event to the *end* of its reported second.  A five-second decision
strictly excludes events becoming available at the deadline, which prevents
the historical replay from seeing the rest of the boundary second.

The simulator mirrors the material live state-machine semantics:

* timeout is measured from the decision, not the entry fill;
* entry and exit use exact PumpSwap integer quotes and a 5% slippage guard;
* positions are observed on a 200ms clock;
* take-profit is revalidated before every exit attempt;
* entry and exit retry at 75ms, independently, up to eight attempts;
* fee-gate skips and paid failed attempts are outcomes.

Latency/attempt templates are empirical inputs.  Keeping them outside the
market path makes uncertainty from historical markets separable from the
small live execution sample.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

from pumpquant.pricing.pumpswap import (
    PumpSwapFees,
    PumpSwapQuote,
    pumpswap_buy_exact_base,
    pumpswap_buy_exact_quote_in,
    pumpswap_sell_exact_base,
)

LAMPORTS_PER_SOL = 1_000_000_000


@dataclass(frozen=True, slots=True)
class ReplayState:
    available_at_ms: int
    slot: int
    order: int
    base_reserve: int
    quote_reserve: int
    protocol_fee_bps: int
    lp_fee_bps: int
    creator_fee_bps: int
    cashback_fee_bps: int = 0
    buyback_fee_bps: int = 0
    virtual_quote_reserve: int = 0

    def __post_init__(self) -> None:
        if (
            self.available_at_ms < 0
            or self.slot < 0
            or self.order < 0
            or self.base_reserve <= 0
            or self.quote_reserve <= 0
            or self.protocol_fee_bps < 0
            or self.lp_fee_bps < 0
            or self.creator_fee_bps < 0
            or self.cashback_fee_bps < 0
            or self.buyback_fee_bps < 0
            or self.quote_reserve + self.virtual_quote_reserve <= 0
        ):
            raise ValueError("invalid replay state")

    @property
    def fees(self) -> PumpSwapFees:
        return PumpSwapFees(
            protocol_fee_bps=self.protocol_fee_bps,
            lp_fee_bps=self.lp_fee_bps,
            creator_fee_bps=self.creator_fee_bps,
            cashback_fee_bps=self.cashback_fee_bps,
            buyback_fee_bps=self.buyback_fee_bps,
        )

    @property
    def effective_quote_reserve(self) -> int:
        return self.quote_reserve + self.virtual_quote_reserve


@dataclass(frozen=True, slots=True)
class ReplayPath:
    mint: str
    first_block_time: int
    decision_at_ms: int
    fx_usd: float
    checkpoint_quote_lamports: int
    states: tuple[ReplayState, ...]

    def __post_init__(self) -> None:
        if (
            not self.mint
            or self.first_block_time < 0
            or self.decision_at_ms < 0
            or self.fx_usd <= 0
            or self.checkpoint_quote_lamports <= 0
            or not self.states
        ):
            raise ValueError("invalid replay path")
        if tuple(sorted(self.states, key=lambda row: (row.available_at_ms, row.order))) != (
            self.states
        ):
            raise ValueError("replay states must be ordered")


@dataclass(frozen=True, slots=True)
class ExecutionTemplate:
    template_id: int
    entry_outcome: Literal["success", "fee_gate_skip", "failed"]
    entry_latency_ms: float
    exit_latency_ms: float
    entry_attempts: int
    exit_attempts: int
    actual_fees_lamports: int
    submitted_transactions: int

    def __post_init__(self) -> None:
        if (
            self.template_id < 0
            or self.entry_latency_ms < 0
            or self.exit_latency_ms < 0
            or not 0 <= self.entry_attempts <= 8
            or not 0 <= self.exit_attempts <= 8
            or self.actual_fees_lamports < 0
            or self.submitted_transactions < 0
        ):
            raise ValueError("invalid execution template")


@dataclass(frozen=True, slots=True)
class ReplayResult:
    mint: str
    status: str
    reason: str
    entered: bool
    closed: bool
    tp_crossed: bool
    tp_captured: bool
    fee_gate_skipped: bool
    entry_attempts: int
    exit_attempts: int
    entry_time_ms: int | None
    exit_time_ms: int | None
    budget_lamports: int
    entry_spend_lamports: int
    exit_credit_lamports: int
    transaction_fees_lamports: int
    net_pnl_lamports: int

    @property
    def return_on_budget(self) -> float:
        return self.net_pnl_lamports / self.budget_lamports


def conservative_available_at_ms(block_time: int) -> int:
    """Return the earliest safe local availability for a second-resolution row."""

    if block_time < 0:
        raise ValueError("block_time must be nonnegative")
    return (block_time + 1) * 1_000


def conservative_decision_at_ms(first_block_time: int, checkpoint_seconds: int) -> int:
    if checkpoint_seconds <= 0:
        raise ValueError("checkpoint_seconds must be positive")
    return conservative_available_at_ms(first_block_time) + checkpoint_seconds * 1_000


def _state_at(path: ReplayPath, target_ms: int) -> ReplayState | None:
    lower = 0
    upper = len(path.states)
    while lower < upper:
        middle = (lower + upper) // 2
        if path.states[middle].available_at_ms <= target_ms:
            lower = middle + 1
        else:
            upper = middle
    if lower == 0:
        return None
    # All events from an available second arrive together.  bisect_right
    # returns the final transaction state in that completed second.
    return path.states[lower - 1]


def _buy_quote(state: ReplayState, gross_quote: int) -> PumpSwapQuote:
    return pumpswap_buy_exact_quote_in(
        gross_quote_amount=gross_quote,
        base_reserve=state.base_reserve,
        quote_reserve=state.effective_quote_reserve,
        fees=state.fees,
    )


def _required_buy(state: ReplayState, base_amount: int) -> PumpSwapQuote:
    return pumpswap_buy_exact_base(
        base_amount=base_amount,
        base_reserve=state.base_reserve,
        quote_reserve=state.effective_quote_reserve,
        fees=state.fees,
    )


def _sell_quote(state: ReplayState, base_amount: int) -> int:
    try:
        quote = pumpswap_sell_exact_base(
            base_amount=base_amount,
            base_reserve=state.base_reserve,
            quote_reserve=state.effective_quote_reserve,
            fees=state.fees,
        )
    except ValueError:
        return 0
    if quote.curve_quote_amount - quote.lp_fee_quote > state.quote_reserve:
        return 0
    return quote.user_quote_amount


def _monitor_time(entry_ms: int, available_ms: int, cadence_ms: int) -> int:
    if available_ms <= entry_ms:
        return entry_ms
    return entry_ms + math.ceil((available_ms - entry_ms) / cadence_ms) * cadence_ms


def _attempt_landings(
    *,
    start_ms: int,
    total_latency_ms: float,
    empirical_attempts: int,
    maximum_attempts: int,
    retry_delay_ms: int,
) -> tuple[int, ...]:
    """Construct retry landing times while retaining measured total latency.

    The ledger identifies full decision/trigger-to-fill latency and attempt
    count, but not a processed timestamp for every pre-send RPC failure.  The
    measured active time is split across observed attempts; if replay slippage
    requires more attempts than the template observed, the median observed
    attempt duration extends the schedule to the live eight-attempt ceiling.
    """

    attempts = max(1, empirical_attempts)
    active = max(float(attempts), total_latency_ms - retry_delay_ms * (attempts - 1))
    duration = max(1.0, active / attempts)
    landings: list[int] = []
    cursor = float(start_ms)
    for _ in range(maximum_attempts):
        cursor += duration
        landings.append(math.ceil(cursor))
        cursor += retry_delay_ms
    return tuple(landings)


def _result(
    *,
    path: ReplayPath,
    status: str,
    reason: str,
    entered: bool,
    closed: bool,
    tp_crossed: bool,
    tp_captured: bool,
    fee_gate_skipped: bool,
    entry_attempts: int,
    exit_attempts: int,
    entry_time_ms: int | None,
    exit_time_ms: int | None,
    budget: int,
    entry_spend: int,
    exit_credit: int,
    fees: int,
) -> ReplayResult:
    return ReplayResult(
        mint=path.mint,
        status=status,
        reason=reason,
        entered=entered,
        closed=closed,
        tp_crossed=tp_crossed,
        tp_captured=tp_captured,
        fee_gate_skipped=fee_gate_skipped,
        entry_attempts=entry_attempts,
        exit_attempts=exit_attempts,
        entry_time_ms=entry_time_ms,
        exit_time_ms=exit_time_ms,
        budget_lamports=budget,
        entry_spend_lamports=entry_spend,
        exit_credit_lamports=exit_credit,
        transaction_fees_lamports=fees,
        net_pnl_lamports=exit_credit - entry_spend - fees,
    )


def simulate_path(
    path: ReplayPath,
    template: ExecutionTemplate,
    *,
    starting_balance_lamports: int = 71_256_637,
    wallet_fraction: float = 0.0695,
    maximum_order_usd: float = 0.70,
    take_profit_multiple: float = 2.5,
    stop_loss_multiple: float = 0.2,
    timeout_seconds: int = 300,
    slippage_bps: int = 500,
    monitor_cadence_ms: int = 200,
    retry_delay_ms: int = 75,
    maximum_attempts: int = 8,
    force_fee_gate_submission: bool = False,
    capture_take_profit: bool = True,
    cap_take_profit_credit_at_trigger: bool = True,
    fee_multiplier: float = 1.0,
) -> ReplayResult:
    """Replay one frozen signal through one empirical execution template."""

    if (
        starting_balance_lamports <= 0
        or not 0 < wallet_fraction < 1
        or maximum_order_usd <= 0
        or take_profit_multiple <= 1
        or not 0 < stop_loss_multiple < 1
        or timeout_seconds <= 0
        or not 0 <= slippage_bps < 10_000
        or monitor_cadence_ms <= 0
        or retry_delay_ms < 0
        or maximum_attempts <= 0
        or fee_multiplier < 0
    ):
        raise ValueError("invalid replay parameters")
    budget = min(
        math.floor(starting_balance_lamports * wallet_fraction),
        math.floor(maximum_order_usd / path.fx_usd * LAMPORTS_PER_SOL),
    )
    budget = max(1, budget)
    actual_fees = math.ceil(template.actual_fees_lamports * fee_multiplier)
    if template.entry_outcome == "fee_gate_skip" and not force_fee_gate_submission:
        return _result(
            path=path,
            status="skipped",
            reason="fee_gate_skip",
            entered=False,
            closed=False,
            tp_crossed=False,
            tp_captured=False,
            fee_gate_skipped=True,
            entry_attempts=template.entry_attempts,
            exit_attempts=0,
            entry_time_ms=None,
            exit_time_ms=None,
            budget=budget,
            entry_spend=0,
            exit_credit=0,
            fees=actual_fees,
        )
    if template.entry_outcome == "failed" and not force_fee_gate_submission:
        return _result(
            path=path,
            status="entry_failed",
            reason="empirical_entry_failure",
            entered=False,
            closed=False,
            tp_crossed=False,
            tp_captured=False,
            fee_gate_skipped=False,
            entry_attempts=max(1, template.entry_attempts),
            exit_attempts=0,
            entry_time_ms=None,
            exit_time_ms=None,
            budget=budget,
            entry_spend=0,
            exit_credit=0,
            fees=actual_fees,
        )

    decision_state = _state_at(path, path.decision_at_ms - 1)
    if decision_state is None:
        return _result(
            path=path,
            status="entry_failed",
            reason="missing_causal_decision_state",
            entered=False,
            closed=False,
            tp_crossed=False,
            tp_captured=False,
            fee_gate_skipped=False,
            entry_attempts=0,
            exit_attempts=0,
            entry_time_ms=None,
            exit_time_ms=None,
            budget=budget,
            entry_spend=0,
            exit_credit=0,
            fees=0,
        )

    entry_landings = _attempt_landings(
        start_ms=path.decision_at_ms,
        total_latency_ms=max(1.0, template.entry_latency_ms),
        empirical_attempts=max(1, template.entry_attempts),
        maximum_attempts=maximum_attempts,
        retry_delay_ms=retry_delay_ms,
    )
    entry_start = path.decision_at_ms
    base_amount = 0
    entry_spend = 0
    entry_time: int | None = None
    used_entry_attempts = 0
    for landing_ms in entry_landings:
        used_entry_attempts += 1
        build_state = _state_at(path, entry_start)
        landing_state = _state_at(path, landing_ms)
        if build_state is not None and landing_state is not None:
            built = _buy_quote(build_state, budget)
            if built.base_amount > 0 and built.base_amount < landing_state.base_reserve:
                required = _required_buy(landing_state, built.base_amount)
                if required.user_quote_amount <= math.floor(
                    built.user_quote_amount * (1 + slippage_bps / 10_000)
                ):
                    base_amount = built.base_amount
                    entry_spend = required.user_quote_amount
                    entry_time = landing_ms
                    break
        entry_start = landing_ms + retry_delay_ms
    if entry_time is None:
        return _result(
            path=path,
            status="entry_failed",
            reason="entry_unfilled_after_eight_attempts",
            entered=False,
            closed=False,
            tp_crossed=False,
            tp_captured=False,
            fee_gate_skipped=False,
            entry_attempts=used_entry_attempts,
            exit_attempts=0,
            entry_time_ms=None,
            exit_time_ms=None,
            budget=budget,
            entry_spend=0,
            exit_credit=0,
            fees=actual_fees,
        )

    timeout_ms = path.decision_at_ms + timeout_seconds * 1_000
    unique_states: list[ReplayState] = []
    for state in path.states:
        if state.available_at_ms < entry_time:
            continue
        if unique_states and unique_states[-1].available_at_ms == state.available_at_ms:
            unique_states[-1] = state
        else:
            unique_states.append(state)
    trigger_reason = "timeout"
    trigger_time = _monitor_time(entry_time, timeout_ms, monitor_cadence_ms)
    tp_crossed = False
    for state in unique_states:
        observed_ms = _monitor_time(entry_time, state.available_at_ms, monitor_cadence_ms)
        if observed_ms >= trigger_time:
            break
        quote = _sell_quote(state, base_amount)
        if quote >= math.ceil(budget * take_profit_multiple):
            tp_crossed = True
            if capture_take_profit:
                trigger_reason = "take_profit"
                trigger_time = observed_ms
                break
        if quote <= math.floor(budget * stop_loss_multiple):
            trigger_reason = "stop_loss"
            trigger_time = observed_ms
            break

    used_exit_attempts = 0
    exit_credit = 0
    exit_time: int | None = None
    current_trigger = trigger_time
    empirical_exit_attempts = max(1, template.exit_attempts)
    exit_landings = _attempt_landings(
        start_ms=current_trigger,
        total_latency_ms=max(1.0, template.exit_latency_ms),
        empirical_attempts=empirical_exit_attempts,
        maximum_attempts=maximum_attempts,
        retry_delay_ms=retry_delay_ms,
    )
    for landing_ms in exit_landings:
        build_state = _state_at(path, current_trigger)
        if build_state is None:
            used_exit_attempts += 1
            current_trigger = landing_ms + retry_delay_ms
            continue
        built_credit = _sell_quote(build_state, base_amount)
        if trigger_reason == "take_profit" and built_credit < math.ceil(
            budget * take_profit_multiple
        ):
            # Live returns to the 200ms monitor without broadcasting.
            next_trigger: int | None = None
            for state in unique_states:
                observed_ms = _monitor_time(entry_time, state.available_at_ms, monitor_cadence_ms)
                if observed_ms <= current_trigger:
                    continue
                if _sell_quote(state, base_amount) >= math.ceil(budget * take_profit_multiple):
                    next_trigger = observed_ms
                    break
                if observed_ms >= timeout_ms:
                    trigger_reason = "timeout"
                    next_trigger = observed_ms
                    break
            if next_trigger is None:
                trigger_reason = "timeout"
                next_trigger = _monitor_time(entry_time, timeout_ms, monitor_cadence_ms)
            current_trigger = next_trigger
            continue
        used_exit_attempts += 1
        landing_state = _state_at(path, landing_ms)
        raw_landing_credit = 0 if landing_state is None else _sell_quote(landing_state, base_amount)
        if raw_landing_credit >= math.floor(built_credit * (1 - slippage_bps / 10_000)):
            exit_credit = (
                min(raw_landing_credit, math.ceil(budget * take_profit_multiple))
                if cap_take_profit_credit_at_trigger
                else raw_landing_credit
            )
            exit_time = landing_ms
            break
        current_trigger = landing_ms + retry_delay_ms

    if exit_time is None:
        return _result(
            path=path,
            status="unclosed",
            reason=f"{trigger_reason}_exit_unfilled",
            entered=True,
            closed=False,
            tp_crossed=tp_crossed,
            tp_captured=False,
            fee_gate_skipped=False,
            entry_attempts=used_entry_attempts,
            exit_attempts=used_exit_attempts,
            entry_time_ms=entry_time,
            exit_time_ms=None,
            budget=budget,
            entry_spend=entry_spend,
            exit_credit=0,
            fees=actual_fees,
        )
    return _result(
        path=path,
        status="closed",
        reason=trigger_reason,
        entered=True,
        closed=True,
        tp_crossed=tp_crossed,
        tp_captured=trigger_reason == "take_profit",
        fee_gate_skipped=False,
        entry_attempts=used_entry_attempts,
        exit_attempts=used_exit_attempts,
        entry_time_ms=entry_time,
        exit_time_ms=exit_time,
        budget=budget,
        entry_spend=entry_spend,
        exit_credit=exit_credit,
        fees=actual_fees,
    )


__all__ = [
    "ExecutionTemplate",
    "ReplayPath",
    "ReplayResult",
    "ReplayState",
    "conservative_available_at_ms",
    "conservative_decision_at_ms",
    "simulate_path",
]
