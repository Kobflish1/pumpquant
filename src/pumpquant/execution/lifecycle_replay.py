"""Counterfactual AMM transitions for full Pump launch-lifecycle research.

The historical event stream remains the source of external order sizes and fee
state, but never overwrites reserves after a research trade is inserted.
External buys preserve their landed gross user-quote amount; external sells
preserve their landed base-token amount.  This is the strongest reconstruction
available from Pump ``TradeEvent`` rows because those events do not retain the
originating instruction's slippage limit.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Literal

from pumpquant.pricing.bonding_curve import (
    AtomicPumpQuote,
    pump_buy_exact_quote_in,
    pump_sell_exact_base,
)
from pumpquant.pricing.pumpswap import (
    PumpSwapFees,
    PumpSwapQuote,
    pumpswap_buy_exact_quote_in,
    pumpswap_sell_exact_base,
)

Side = Literal["buy", "sell"]


@dataclass(frozen=True, slots=True)
class PumpCurveState:
    virtual_base: int
    virtual_quote: int
    real_base: int
    real_quote: int
    protocol_fee_bps: int
    creator_or_cashback_fee_bps: int

    def __post_init__(self) -> None:
        if (
            self.virtual_base <= 0
            or self.virtual_quote <= 0
            or self.real_base < 0
            or self.real_quote < 0
            or self.protocol_fee_bps < 0
            or self.creator_or_cashback_fee_bps < 0
        ):
            raise ValueError("invalid Pump curve state")


@dataclass(frozen=True, slots=True)
class PumpSwapState:
    real_base: int
    real_quote: int
    virtual_quote: int
    fees: PumpSwapFees

    def __post_init__(self) -> None:
        if self.real_base <= 0 or self.real_quote < 0 or self.virtual_quote < 0:
            raise ValueError("invalid PumpSwap state")
        if self.real_quote + self.virtual_quote <= 0:
            raise ValueError("PumpSwap effective quote reserve must be positive")

    @property
    def effective_quote(self) -> int:
        return self.real_quote + self.virtual_quote


@dataclass(frozen=True, slots=True)
class LandedOrder:
    side: Side
    base_amount: int
    user_quote_amount: int
    maximum_quote_amount_in: int | None = None
    minimum_quote_amount_out: int | None = None

    def __post_init__(self) -> None:
        if self.base_amount <= 0 or self.user_quote_amount <= 0:
            raise ValueError("landed order amounts must be positive")
        if self.maximum_quote_amount_in is not None and self.maximum_quote_amount_in < 0:
            raise ValueError("maximum quote limit must be nonnegative")
        if self.minimum_quote_amount_out is not None and self.minimum_quote_amount_out < 0:
            raise ValueError("minimum quote limit must be nonnegative")


@dataclass(frozen=True, slots=True)
class Transition:
    executed: bool
    reason: str
    state: PumpCurveState | PumpSwapState
    quote: AtomicPumpQuote | PumpSwapQuote | None
    adverse_bps: int | None


@dataclass(frozen=True, slots=True)
class TimedPumpOrder:
    available_at_ms: int
    order_index: int
    order: LandedOrder
    protocol_fee_bps: int
    creator_or_cashback_fee_bps: int

    def __post_init__(self) -> None:
        if (
            self.available_at_ms < 0
            or self.order_index < 0
            or self.protocol_fee_bps < 0
            or self.creator_or_cashback_fee_bps < 0
        ):
            raise ValueError("invalid timed Pump order")


@dataclass(frozen=True, slots=True)
class LifecycleProfile:
    take_profit_multiple: float
    stop_loss_multiple: float
    timeout_seconds: int

    def __post_init__(self) -> None:
        if (
            self.take_profit_multiple <= 1
            or not 0 < self.stop_loss_multiple < 1
            or self.timeout_seconds <= 0
        ):
            raise ValueError("invalid lifecycle profile")


@dataclass(frozen=True, slots=True)
class ReferencePumpOutcome:
    status: str
    entry_time_ms: int | None
    exit_time_ms: int | None
    entry_attempts: int
    exit_attempts: int
    entry_spend: int
    exit_credit: int
    base_amount: int
    transaction_cost: int

    @property
    def net_pnl(self) -> int:
        return self.exit_credit - self.entry_spend - self.transaction_cost


def reconstruct_pump_pre_state(
    post: PumpCurveState,
    *,
    side: Side,
    base_amount: int,
    curve_quote_amount: int,
) -> PumpCurveState:
    """Invert one landed Pump reserve transition exactly."""

    if base_amount <= 0 or curve_quote_amount <= 0:
        raise ValueError("landed Pump amounts must be positive")
    if side == "buy":
        values = (
            post.virtual_base + base_amount,
            post.virtual_quote - curve_quote_amount,
            post.real_base + base_amount,
            post.real_quote - curve_quote_amount,
        )
    else:
        values = (
            post.virtual_base - base_amount,
            post.virtual_quote + curve_quote_amount,
            post.real_base - base_amount,
            post.real_quote + curve_quote_amount,
        )
    return PumpCurveState(*values, post.protocol_fee_bps, post.creator_or_cashback_fee_bps)


def reconstruct_pumpswap_pre_state(
    post: PumpSwapState,
    *,
    side: Side,
    base_amount: int,
    pool_quote_amount: int,
) -> PumpSwapState:
    """Invert one landed PumpSwap physical-reserve transition exactly."""

    if base_amount <= 0 or pool_quote_amount <= 0:
        raise ValueError("landed PumpSwap amounts must be positive")
    if side == "buy":
        real_base = post.real_base + base_amount
        real_quote = post.real_quote - pool_quote_amount
    else:
        real_base = post.real_base - base_amount
        real_quote = post.real_quote + pool_quote_amount
    return PumpSwapState(real_base, real_quote, post.virtual_quote, post.fees)


def _adverse_bps(observed: int, counterfactual: int) -> int:
    if observed <= 0 or counterfactual >= observed:
        return 0
    return ((observed - counterfactual) * 10_000 + observed - 1) // observed


def apply_pump_order(
    state: PumpCurveState,
    order: LandedOrder,
    *,
    maximum_adverse_bps: int | None = None,
) -> Transition:
    """Re-quote and apply a landed Pump order to counterfactual reserves."""

    if maximum_adverse_bps is not None and not 0 <= maximum_adverse_bps <= 10_000:
        raise ValueError("maximum adverse bps must be in [0, 10000]")
    try:
        if order.side == "buy":
            quote = pump_buy_exact_quote_in(
                gross_quote_amount=order.user_quote_amount,
                virtual_base_reserve=state.virtual_base,
                virtual_quote_reserve=state.virtual_quote,
                real_base_reserve=state.real_base,
                protocol_fee_bps=state.protocol_fee_bps,
                creator_fee_bps=state.creator_or_cashback_fee_bps,
                creator_fee_enabled=True,
            )
            adverse = _adverse_bps(order.base_amount, quote.base_amount)
            if maximum_adverse_bps is not None and adverse > maximum_adverse_bps:
                return Transition(False, "pump_slippage_sensitivity_drop", state, quote, adverse)
            next_state = replace(
                state,
                virtual_base=state.virtual_base - quote.base_amount,
                virtual_quote=state.virtual_quote + quote.curve_quote_amount,
                real_base=state.real_base - quote.base_amount,
                real_quote=state.real_quote + quote.curve_quote_amount,
            )
        else:
            quote = pump_sell_exact_base(
                base_amount=order.base_amount,
                virtual_base_reserve=state.virtual_base,
                virtual_quote_reserve=state.virtual_quote,
                real_quote_reserve=state.real_quote,
                protocol_fee_bps=state.protocol_fee_bps,
                creator_fee_bps=state.creator_or_cashback_fee_bps,
                creator_fee_enabled=True,
            )
            adverse = _adverse_bps(order.user_quote_amount, quote.user_quote_amount)
            if maximum_adverse_bps is not None and adverse > maximum_adverse_bps:
                return Transition(False, "pump_slippage_sensitivity_drop", state, quote, adverse)
            next_state = replace(
                state,
                virtual_base=state.virtual_base + quote.base_amount,
                virtual_quote=state.virtual_quote - quote.curve_quote_amount,
                real_base=state.real_base + quote.base_amount,
                real_quote=state.real_quote - quote.curve_quote_amount,
            )
    except ValueError:
        return Transition(False, "pump_counterfactual_quote_rejected", state, None, None)
    return Transition(True, "executed", next_state, quote, adverse)


def apply_pumpswap_order(state: PumpSwapState, order: LandedOrder) -> Transition:
    """Re-quote and apply an official PumpSwap order, enforcing event limits."""

    try:
        if order.side == "buy":
            quote = pumpswap_buy_exact_quote_in(
                gross_quote_amount=order.user_quote_amount,
                base_reserve=state.real_base,
                quote_reserve=state.effective_quote,
                fees=state.fees,
            )
            adverse = _adverse_bps(order.base_amount, quote.base_amount)
            if (
                order.maximum_quote_amount_in is not None
                and quote.user_quote_amount > order.maximum_quote_amount_in
            ):
                return Transition(False, "pumpswap_max_quote_rejected", state, quote, adverse)
            next_state = replace(
                state,
                real_base=state.real_base - quote.base_amount,
                real_quote=state.real_quote + quote.curve_quote_amount + quote.lp_fee_quote,
            )
        else:
            quote = pumpswap_sell_exact_base(
                base_amount=order.base_amount,
                base_reserve=state.real_base,
                quote_reserve=state.effective_quote,
                fees=state.fees,
            )
            adverse = _adverse_bps(order.user_quote_amount, quote.user_quote_amount)
            if (
                order.minimum_quote_amount_out is not None
                and quote.user_quote_amount < order.minimum_quote_amount_out
            ):
                return Transition(False, "pumpswap_min_quote_rejected", state, quote, adverse)
            pool_quote_out = quote.curve_quote_amount - quote.lp_fee_quote
            if pool_quote_out > state.real_quote:
                return Transition(False, "pumpswap_real_quote_insolvent", state, quote, adverse)
            next_state = replace(
                state,
                real_base=state.real_base + quote.base_amount,
                real_quote=state.real_quote - pool_quote_out,
            )
    except ValueError:
        return Transition(False, "pumpswap_counterfactual_quote_rejected", state, None, None)
    return Transition(True, "executed", next_state, quote, adverse)


def _apply_own_pump_buy(
    state: PumpCurveState, gross_quote_amount: int
) -> tuple[PumpCurveState, AtomicPumpQuote]:
    quote = pump_buy_exact_quote_in(
        gross_quote_amount=gross_quote_amount,
        virtual_base_reserve=state.virtual_base,
        virtual_quote_reserve=state.virtual_quote,
        real_base_reserve=state.real_base,
        protocol_fee_bps=state.protocol_fee_bps,
        creator_fee_bps=state.creator_or_cashback_fee_bps,
        creator_fee_enabled=True,
    )
    return (
        replace(
            state,
            virtual_base=state.virtual_base - quote.base_amount,
            virtual_quote=state.virtual_quote + quote.curve_quote_amount,
            real_base=state.real_base - quote.base_amount,
            real_quote=state.real_quote + quote.curve_quote_amount,
        ),
        quote,
    )


def _apply_own_pump_sell(
    state: PumpCurveState, base_amount: int
) -> tuple[PumpCurveState, AtomicPumpQuote]:
    quote = pump_sell_exact_base(
        base_amount=base_amount,
        virtual_base_reserve=state.virtual_base,
        virtual_quote_reserve=state.virtual_quote,
        real_quote_reserve=state.real_quote,
        protocol_fee_bps=state.protocol_fee_bps,
        creator_fee_bps=state.creator_or_cashback_fee_bps,
        creator_fee_enabled=True,
    )
    return (
        replace(
            state,
            virtual_base=state.virtual_base + quote.base_amount,
            virtual_quote=state.virtual_quote - quote.curve_quote_amount,
            real_base=state.real_base + quote.base_amount,
            real_quote=state.real_quote - quote.curve_quote_amount,
        ),
        quote,
    )


def simulate_reference_pump_surface(
    *,
    decision_state: PumpCurveState,
    decision_at_ms: int,
    future_orders: Sequence[TimedPumpOrder],
    profile: LifecycleProfile,
    budget_lamports: int,
    entry_latency_ms: int,
    exit_latency_ms: int,
    transaction_fee_per_attempt: int,
    slippage_bps: int = 500,
    monitor_cadence_ms: int = 200,
    retry_delay_ms: int = 75,
    maximum_attempts: int = 8,
    external_maximum_adverse_bps: int | None = None,
) -> ReferencePumpOutcome:
    """Slow, auditable Pump-only reference used to conform faster kernels."""

    if (
        decision_at_ms < 0
        or budget_lamports <= 0
        or entry_latency_ms <= 0
        or exit_latency_ms <= 0
        or transaction_fee_per_attempt < 0
        or not 0 <= slippage_bps < 10_000
        or monitor_cadence_ms <= 0
        or retry_delay_ms < 0
        or maximum_attempts <= 0
    ):
        raise ValueError("invalid reference replay controls")
    ordered = tuple(
        sorted(
            future_orders,
            key=lambda item: (item.available_at_ms, item.order_index),
        )
    )
    if tuple(future_orders) != ordered:
        raise ValueError("future Pump orders must be ordered")
    state = decision_state
    cursor = 0
    now = decision_at_ms
    paid_attempts = 0

    def advance(target_ms: int) -> None:
        nonlocal cursor, state
        while cursor < len(ordered) and ordered[cursor].available_at_ms <= target_ms:
            external = ordered[cursor]
            state = replace(
                state,
                protocol_fee_bps=external.protocol_fee_bps,
                creator_or_cashback_fee_bps=external.creator_or_cashback_fee_bps,
            )
            transition = apply_pump_order(
                state,
                external.order,
                maximum_adverse_bps=external_maximum_adverse_bps,
            )
            if transition.executed:
                assert isinstance(transition.state, PumpCurveState)
                state = transition.state
            cursor += 1

    entry_quote = pump_buy_exact_quote_in(
        gross_quote_amount=budget_lamports,
        virtual_base_reserve=state.virtual_base,
        virtual_quote_reserve=state.virtual_quote,
        real_base_reserve=state.real_base,
        protocol_fee_bps=state.protocol_fee_bps,
        creator_fee_bps=state.creator_or_cashback_fee_bps,
        creator_fee_enabled=True,
    )
    entry_time: int | None = None
    base_amount = 0
    entry_spend = 0
    entry_attempts = 0
    minimum_base = entry_quote.base_amount * (10_000 - slippage_bps) // 10_000
    for attempt in range(1, maximum_attempts + 1):
        landing = now + entry_latency_ms
        advance(landing)
        paid_attempts += 1
        entry_attempts = attempt
        candidate = pump_buy_exact_quote_in(
            gross_quote_amount=budget_lamports,
            virtual_base_reserve=state.virtual_base,
            virtual_quote_reserve=state.virtual_quote,
            real_base_reserve=state.real_base,
            protocol_fee_bps=state.protocol_fee_bps,
            creator_fee_bps=state.creator_or_cashback_fee_bps,
            creator_fee_enabled=True,
        )
        if candidate.base_amount >= minimum_base and candidate.base_amount > 0:
            state, filled = _apply_own_pump_buy(state, budget_lamports)
            entry_time = landing
            base_amount = filled.base_amount
            entry_spend = filled.user_quote_amount
            break
        minimum_base = candidate.base_amount * (10_000 - slippage_bps) // 10_000
        now = landing + retry_delay_ms
    if entry_time is None:
        return ReferencePumpOutcome(
            "entry_failed",
            None,
            None,
            entry_attempts,
            0,
            0,
            0,
            0,
            paid_attempts * transaction_fee_per_attempt,
        )

    timeout_ms = entry_time + profile.timeout_seconds * 1_000
    next_check = entry_time + monitor_cadence_ms
    exit_attempts = 0
    while next_check <= timeout_ms:
        check_at = min(next_check, timeout_ms)
        advance(check_at)
        credit = pump_sell_exact_base(
            base_amount=base_amount,
            virtual_base_reserve=state.virtual_base,
            virtual_quote_reserve=state.virtual_quote,
            real_quote_reserve=state.real_quote,
            protocol_fee_bps=state.protocol_fee_bps,
            creator_fee_bps=state.creator_or_cashback_fee_bps,
            creator_fee_enabled=True,
        ).user_quote_amount
        trigger = (
            "tp"
            if credit >= budget_lamports * profile.take_profit_multiple
            else (
                "sl"
                if credit <= budget_lamports * profile.stop_loss_multiple
                else ("timeout" if check_at >= timeout_ms else None)
            )
        )
        if trigger is None:
            next_check = check_at + monitor_cadence_ms
            continue
        minimum_quote = credit * (10_000 - slippage_bps) // 10_000
        now = check_at
        for _attempt in range(1, maximum_attempts + 1):
            landing = now + exit_latency_ms
            advance(landing)
            paid_attempts += 1
            exit_attempts += 1
            candidate = pump_sell_exact_base(
                base_amount=base_amount,
                virtual_base_reserve=state.virtual_base,
                virtual_quote_reserve=state.virtual_quote,
                real_quote_reserve=state.real_quote,
                protocol_fee_bps=state.protocol_fee_bps,
                creator_fee_bps=state.creator_or_cashback_fee_bps,
                creator_fee_enabled=True,
            )
            still_tp = candidate.user_quote_amount >= (
                budget_lamports * profile.take_profit_multiple
            )
            if candidate.user_quote_amount >= minimum_quote and (trigger != "tp" or still_tp):
                state, filled = _apply_own_pump_sell(state, base_amount)
                return ReferencePumpOutcome(
                    trigger,
                    entry_time,
                    landing,
                    entry_attempts,
                    exit_attempts,
                    entry_spend,
                    filled.user_quote_amount,
                    base_amount,
                    paid_attempts * transaction_fee_per_attempt,
                )
            if trigger == "tp" and not still_tp:
                next_check = landing + monitor_cadence_ms
                break
            minimum_quote = candidate.user_quote_amount * (10_000 - slippage_bps) // 10_000
            now = landing + retry_delay_ms
        else:
            return ReferencePumpOutcome(
                "exit_failed",
                entry_time,
                None,
                entry_attempts,
                exit_attempts,
                entry_spend,
                0,
                base_amount,
                paid_attempts * transaction_fee_per_attempt,
            )
    return ReferencePumpOutcome(
        "timeout_unclosed",
        entry_time,
        None,
        entry_attempts,
        exit_attempts,
        entry_spend,
        0,
        base_amount,
        paid_attempts * transaction_fee_per_attempt,
    )
