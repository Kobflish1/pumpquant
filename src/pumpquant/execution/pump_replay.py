"""Conservative integer-exact Pump bonding-curve replay primitives.

SQD archive rows expose integer-second block times.  A row stamped at second
``t`` is therefore treated as available only at the start of ``t + 1``.  The
module supports a fixed-budget research probe, not wallet sizing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from pumpquant.pricing.bonding_curve import (
    AtomicPumpQuote,
    pump_buy_exact_base,
    pump_buy_exact_quote_in,
    pump_sell_exact_base,
)


@dataclass(frozen=True, slots=True)
class PumpReplayState:
    available_at_ms: int
    order: int
    virtual_base_reserve: int
    virtual_quote_reserve: int
    real_base_reserve: int
    real_quote_reserve: int
    protocol_fee_bps: int
    creator_fee_bps: int
    cashback_fee_bps: int

    def __post_init__(self) -> None:
        if (
            self.available_at_ms < 0
            or self.order < 0
            or self.virtual_base_reserve <= 0
            or self.virtual_quote_reserve <= 0
            or self.real_base_reserve < 0
            or self.real_quote_reserve < 0
            or min(
                self.protocol_fee_bps,
                self.creator_fee_bps,
                self.cashback_fee_bps,
            )
            < 0
        ):
            raise ValueError("invalid Pump replay state")
        if self.creator_fee_bps and self.cashback_fee_bps:
            raise ValueError("creator and cashback routes cannot both be active")

    @property
    def creator_route_fee_bps(self) -> int:
        return self.creator_fee_bps + self.cashback_fee_bps


@dataclass(frozen=True, slots=True)
class PumpReplayPath:
    mint: str
    launch_time: int
    decision_at_ms: int
    states: tuple[PumpReplayState, ...]

    def __post_init__(self) -> None:
        if not self.mint or self.launch_time < 0 or self.decision_at_ms < 0:
            raise ValueError("invalid Pump replay path")
        if (
            tuple(sorted(self.states, key=lambda row: (row.available_at_ms, row.order)))
            != self.states
        ):
            raise ValueError("Pump replay states must be ordered")


@dataclass(frozen=True, slots=True)
class PumpProfile:
    take_profit_multiple: float
    stop_loss_multiple: float
    timeout_seconds: int

    def __post_init__(self) -> None:
        if (
            self.take_profit_multiple <= 1
            or not 0 < self.stop_loss_multiple < 1
            or self.timeout_seconds <= 0
        ):
            raise ValueError("invalid Pump outcome profile")

    @property
    def profile_id(self) -> str:
        return (
            f"tp{self.take_profit_multiple:g}_sl{self.stop_loss_multiple:g}_t{self.timeout_seconds}"
        )


@dataclass(frozen=True, slots=True)
class PumpReplayResult:
    status: str
    reason: str
    entered: bool
    closed: bool
    tp_crossed: bool
    tp_captured: bool
    entry_time_ms: int | None
    exit_time_ms: int | None
    budget_lamports: int
    entry_spend_lamports: int
    exit_credit_lamports: int
    transaction_fees_lamports: int

    @property
    def net_pnl_lamports(self) -> int:
        return (
            self.exit_credit_lamports - self.entry_spend_lamports - self.transaction_fees_lamports
        )

    @property
    def return_on_budget(self) -> float:
        return self.net_pnl_lamports / self.budget_lamports


def conservative_available_at_ms(block_time: int) -> int:
    if block_time < 0:
        raise ValueError("block_time must be nonnegative")
    return (block_time + 1) * 1_000


def conservative_launch_decision_at_ms(launch_block_time: int, checkpoint_seconds: int) -> int:
    if checkpoint_seconds <= 0:
        raise ValueError("checkpoint_seconds must be positive")
    return conservative_available_at_ms(launch_block_time) + checkpoint_seconds * 1_000


def state_at(path: PumpReplayPath, target_ms: int) -> PumpReplayState | None:
    lower = 0
    upper = len(path.states)
    while lower < upper:
        middle = (lower + upper) // 2
        if path.states[middle].available_at_ms <= target_ms:
            lower = middle + 1
        else:
            upper = middle
    return None if lower == 0 else path.states[lower - 1]


def _buy_quote(state: PumpReplayState, budget: int) -> AtomicPumpQuote:
    return pump_buy_exact_quote_in(
        gross_quote_amount=budget,
        virtual_base_reserve=state.virtual_base_reserve,
        virtual_quote_reserve=state.virtual_quote_reserve,
        real_base_reserve=state.real_base_reserve,
        protocol_fee_bps=state.protocol_fee_bps,
        creator_fee_bps=state.creator_route_fee_bps,
        creator_fee_enabled=True,
    )


def _required_buy(state: PumpReplayState, base_amount: int) -> AtomicPumpQuote:
    return pump_buy_exact_base(
        base_amount=base_amount,
        virtual_base_reserve=state.virtual_base_reserve,
        virtual_quote_reserve=state.virtual_quote_reserve,
        real_base_reserve=state.real_base_reserve,
        protocol_fee_bps=state.protocol_fee_bps,
        creator_fee_bps=state.creator_route_fee_bps,
        creator_fee_enabled=True,
    )


def sell_credit(state: PumpReplayState, base_amount: int) -> int:
    try:
        return pump_sell_exact_base(
            base_amount=base_amount,
            virtual_base_reserve=state.virtual_base_reserve,
            virtual_quote_reserve=state.virtual_quote_reserve,
            real_quote_reserve=state.real_quote_reserve,
            protocol_fee_bps=state.protocol_fee_bps,
            creator_fee_bps=state.creator_route_fee_bps,
            creator_fee_enabled=True,
        ).user_quote_amount
    except ValueError:
        return 0


def _monitor_time(entry_ms: int, available_ms: int, cadence_ms: int) -> int:
    if available_ms <= entry_ms:
        return entry_ms
    return entry_ms + math.ceil((available_ms - entry_ms) / cadence_ms) * cadence_ms


def _landing_times(
    start_ms: int,
    latency_ms: int,
    retry_delay_ms: int,
    maximum_attempts: int,
) -> tuple[int, ...]:
    cursor = start_ms
    values: list[int] = []
    for _ in range(maximum_attempts):
        cursor += latency_ms
        values.append(cursor)
        cursor += retry_delay_ms
    return tuple(values)


def simulate_pump_path(
    path: PumpReplayPath,
    profile: PumpProfile,
    *,
    budget_lamports: int = 3_000_000,
    entry_latency_ms: int = 1_858,
    exit_latency_ms: int = 1_194,
    transaction_fees_lamports: int = 23_000,
    slippage_bps: int = 500,
    monitor_cadence_ms: int = 200,
    retry_delay_ms: int = 75,
    maximum_attempts: int = 8,
    capture_take_profit: bool = True,
) -> PumpReplayResult:
    """Replay one fixed-budget Pump path under a registered exit profile."""

    if (
        budget_lamports <= 0
        or entry_latency_ms <= 0
        or exit_latency_ms <= 0
        or transaction_fees_lamports < 0
        or not 0 <= slippage_bps < 10_000
        or monitor_cadence_ms <= 0
        or retry_delay_ms < 0
        or maximum_attempts <= 0
    ):
        raise ValueError("invalid Pump replay controls")
    decision_state = state_at(path, path.decision_at_ms - 1)
    if decision_state is None:
        return PumpReplayResult(
            "skipped",
            "no_causal_checkpoint_state",
            False,
            False,
            False,
            False,
            None,
            None,
            budget_lamports,
            0,
            0,
            0,
        )
    entry_start = path.decision_at_ms
    entry_time: int | None = None
    entry_spend = 0
    base_amount = 0
    for landing_ms in _landing_times(
        entry_start, entry_latency_ms, retry_delay_ms, maximum_attempts
    ):
        build_state = state_at(path, entry_start)
        landing_state = state_at(path, landing_ms)
        if build_state is not None and landing_state is not None:
            try:
                built = _buy_quote(build_state, budget_lamports)
                landed = _required_buy(landing_state, built.base_amount)
            except ValueError:
                built = None
                landed = None
            if (
                built is not None
                and landed is not None
                and built.base_amount > 0
                and landed.base_amount == built.base_amount
                and landed.user_quote_amount
                <= math.floor(built.user_quote_amount * (1 + slippage_bps / 10_000))
            ):
                base_amount = built.base_amount
                entry_spend = landed.user_quote_amount
                entry_time = landing_ms
                break
        entry_start = landing_ms + retry_delay_ms
    if entry_time is None:
        return PumpReplayResult(
            "entry_failed",
            "entry_unfilled_after_eight_attempts",
            False,
            False,
            False,
            False,
            None,
            None,
            budget_lamports,
            0,
            0,
            math.ceil(transaction_fees_lamports / 2),
        )

    unique_states: list[PumpReplayState] = []
    for state in path.states:
        if state.available_at_ms < entry_time:
            continue
        if unique_states and unique_states[-1].available_at_ms == state.available_at_ms:
            unique_states[-1] = state
        else:
            unique_states.append(state)
    timeout_ms = path.decision_at_ms + profile.timeout_seconds * 1_000
    trigger_time = _monitor_time(entry_time, timeout_ms, monitor_cadence_ms)
    trigger_reason = "timeout"
    tp_crossed = False
    for state in unique_states:
        observed_ms = _monitor_time(entry_time, state.available_at_ms, monitor_cadence_ms)
        if observed_ms >= trigger_time:
            break
        credit = sell_credit(state, base_amount)
        if credit >= math.ceil(budget_lamports * profile.take_profit_multiple):
            tp_crossed = True
            if capture_take_profit:
                trigger_time = observed_ms
                trigger_reason = "take_profit"
                break
        if credit <= math.floor(budget_lamports * profile.stop_loss_multiple):
            trigger_time = observed_ms
            trigger_reason = "stop_loss"
            break

    exit_start = trigger_time
    exit_time: int | None = None
    exit_credit = 0
    exit_attempts = 0
    revalidations = 0
    while exit_attempts < maximum_attempts:
        revalidations += 1
        if revalidations > len(unique_states) + maximum_attempts + 1:
            break
        build_state = state_at(path, exit_start)
        if build_state is None:
            exit_attempts += 1
            exit_start += exit_latency_ms + retry_delay_ms
            continue
        built_credit = sell_credit(build_state, base_amount)
        if trigger_reason == "take_profit" and built_credit < math.ceil(
            budget_lamports * profile.take_profit_multiple
        ):
            next_trigger = next(
                (
                    _monitor_time(entry_time, state.available_at_ms, monitor_cadence_ms)
                    for state in unique_states
                    if _monitor_time(entry_time, state.available_at_ms, monitor_cadence_ms)
                    > exit_start
                    and _monitor_time(entry_time, state.available_at_ms, monitor_cadence_ms)
                    < timeout_ms
                    and sell_credit(state, base_amount)
                    >= math.ceil(budget_lamports * profile.take_profit_multiple)
                ),
                None,
            )
            if next_trigger is None:
                trigger_reason = "timeout"
                exit_start = timeout_ms
            else:
                exit_start = next_trigger
            continue
        landing_ms = exit_start + exit_latency_ms
        exit_attempts += 1
        landing_state = state_at(path, landing_ms)
        if landing_state is None:
            exit_start = landing_ms + retry_delay_ms
            continue
        landed_credit = sell_credit(landing_state, base_amount)
        if landed_credit >= math.floor(built_credit * (1 - slippage_bps / 10_000)):
            exit_credit = (
                min(
                    landed_credit,
                    math.ceil(budget_lamports * profile.take_profit_multiple),
                )
                if trigger_reason == "take_profit"
                else landed_credit
            )
            exit_time = landing_ms
            break
        exit_start = landing_ms + retry_delay_ms
    if exit_time is None:
        return PumpReplayResult(
            "unclosed",
            f"{trigger_reason}_exit_unfilled",
            True,
            False,
            tp_crossed,
            False,
            entry_time,
            None,
            budget_lamports,
            entry_spend,
            0,
            transaction_fees_lamports,
        )
    return PumpReplayResult(
        "closed",
        trigger_reason,
        True,
        True,
        tp_crossed,
        trigger_reason == "take_profit",
        entry_time,
        exit_time,
        budget_lamports,
        entry_spend,
        exit_credit,
        transaction_fees_lamports,
    )


def simulate_pump_surface(
    path: PumpReplayPath,
    profiles: tuple[PumpProfile, ...],
    *,
    budget_lamports: int = 3_000_000,
    entry_latency_ms: int = 1_858,
    exit_latency_ms: int = 1_194,
    transaction_fees_lamports: int = 23_000,
    slippage_bps: int = 500,
    monitor_cadence_ms: int = 200,
    retry_delay_ms: int = 75,
    maximum_attempts: int = 8,
    tp_capture_probability: float = 0.6,
) -> dict[str, dict[str, float | bool | str]]:
    """Evaluate a registered profile surface while sharing entry/path work.

    The expected return is the preregistered mixture of a captured-TP replay
    and a replay that observes the same TP crossing but fails to act on it.
    """

    if not profiles:
        raise ValueError("profiles cannot be empty")
    if not 0 <= tp_capture_probability <= 1:
        raise ValueError("invalid TP capture probability")
    decision_state = state_at(path, path.decision_at_ms - 1)
    if decision_state is None:
        return {
            profile.profile_id: {
                "expected_return": 0.0,
                "captured_return": 0.0,
                "missed_return": 0.0,
                "tp_crossed": False,
                "entry_status": "no_causal_checkpoint_state",
            }
            for profile in profiles
        }

    entry_start = path.decision_at_ms
    entry_time: int | None = None
    entry_spend = 0
    base_amount = 0
    for landing_ms in _landing_times(
        entry_start, entry_latency_ms, retry_delay_ms, maximum_attempts
    ):
        build_state = state_at(path, entry_start)
        landing_state = state_at(path, landing_ms)
        if build_state is not None and landing_state is not None:
            try:
                built = _buy_quote(build_state, budget_lamports)
                landed = _required_buy(landing_state, built.base_amount)
            except ValueError:
                built = None
                landed = None
            if (
                built is not None
                and landed is not None
                and built.base_amount > 0
                and landed.base_amount == built.base_amount
                and landed.user_quote_amount
                <= math.floor(built.user_quote_amount * (1 + slippage_bps / 10_000))
            ):
                base_amount = built.base_amount
                entry_spend = landed.user_quote_amount
                entry_time = landing_ms
                break
        entry_start = landing_ms + retry_delay_ms
    if entry_time is None:
        failed_return = -math.ceil(transaction_fees_lamports / 2) / budget_lamports
        return {
            profile.profile_id: {
                "expected_return": failed_return,
                "captured_return": failed_return,
                "missed_return": failed_return,
                "tp_crossed": False,
                "entry_status": "entry_unfilled_after_eight_attempts",
            }
            for profile in profiles
        }

    unique_states: list[PumpReplayState] = []
    for state in path.states:
        if state.available_at_ms < entry_time:
            continue
        if unique_states and unique_states[-1].available_at_ms == state.available_at_ms:
            unique_states[-1] = state
        else:
            unique_states.append(state)
    observations = [
        (
            _monitor_time(entry_time, state.available_at_ms, monitor_cadence_ms),
            sell_credit(state, base_amount),
        )
        for state in unique_states
    ]
    observations.sort()
    tp_first: dict[float, int | None] = {}
    sl_first: dict[float, int | None] = {}
    for multiple in {profile.take_profit_multiple for profile in profiles}:
        threshold = math.ceil(budget_lamports * multiple)
        tp_first[multiple] = next(
            (observed for observed, credit in observations if credit >= threshold),
            None,
        )
    for multiple in {profile.stop_loss_multiple for profile in profiles}:
        threshold = math.floor(budget_lamports * multiple)
        sl_first[multiple] = next(
            (observed for observed, credit in observations if credit <= threshold),
            None,
        )

    exit_cache: dict[tuple[int, str, float, int], float] = {}

    def exit_return(
        trigger_time: int,
        reason: str,
        tp_multiple: float,
        timeout_ms: int,
    ) -> float:
        key = (trigger_time, reason, tp_multiple, timeout_ms)
        if key in exit_cache:
            return exit_cache[key]
        exit_start = trigger_time
        current_reason = reason
        exit_attempts = 0
        revalidations = 0
        while exit_attempts < maximum_attempts:
            revalidations += 1
            if revalidations > len(observations) + maximum_attempts + 1:
                break
            build_state = state_at(path, exit_start)
            if build_state is None:
                exit_attempts += 1
                exit_start += exit_latency_ms + retry_delay_ms
                continue
            built_credit = sell_credit(build_state, base_amount)
            if current_reason == "take_profit" and built_credit < math.ceil(
                budget_lamports * tp_multiple
            ):
                next_trigger = next(
                    (
                        observed
                        for observed, credit in observations
                        if observed > exit_start
                        and observed < timeout_ms
                        and credit >= math.ceil(budget_lamports * tp_multiple)
                    ),
                    None,
                )
                if next_trigger is None:
                    current_reason = "timeout"
                    exit_start = timeout_ms
                else:
                    exit_start = next_trigger
                continue
            landing_ms = exit_start + exit_latency_ms
            exit_attempts += 1
            landing_state = state_at(path, landing_ms)
            if landing_state is None:
                exit_start = landing_ms + retry_delay_ms
                continue
            landed_credit = sell_credit(landing_state, base_amount)
            if landed_credit >= math.floor(built_credit * (1 - slippage_bps / 10_000)):
                if current_reason == "take_profit":
                    landed_credit = min(
                        landed_credit,
                        math.ceil(budget_lamports * tp_multiple),
                    )
                value = (landed_credit - entry_spend - transaction_fees_lamports) / budget_lamports
                exit_cache[key] = value
                return value
            exit_start = landing_ms + retry_delay_ms
        value = (-entry_spend - transaction_fees_lamports) / budget_lamports
        exit_cache[key] = value
        return value

    surface: dict[str, dict[str, float | bool | str]] = {}
    for profile in profiles:
        timeout_ms = path.decision_at_ms + profile.timeout_seconds * 1_000
        # A trigger only counts if it fires before the position times out.
        tp_time = tp_first[profile.take_profit_multiple]
        sl_time = sl_first[profile.stop_loss_multiple]
        if tp_time is not None and tp_time >= timeout_ms:
            tp_time = None
        if sl_time is not None and sl_time >= timeout_ms:
            sl_time = None
        if tp_time is not None and (sl_time is None or tp_time <= sl_time):
            captured_trigger = tp_time
            captured_reason = "take_profit"
            tp_crossed = True
        elif sl_time is not None:
            captured_trigger = sl_time
            captured_reason = "stop_loss"
            tp_crossed = False
        else:
            captured_trigger = timeout_ms
            captured_reason = "timeout"
            tp_crossed = False
        if sl_time is not None:
            missed_trigger = sl_time
            missed_reason = "stop_loss"
        else:
            missed_trigger = timeout_ms
            missed_reason = "timeout"
        captured_return = exit_return(
            captured_trigger,
            captured_reason,
            profile.take_profit_multiple,
            timeout_ms,
        )
        missed_return = exit_return(
            missed_trigger,
            missed_reason,
            profile.take_profit_multiple,
            timeout_ms,
        )
        surface[profile.profile_id] = {
            "expected_return": (
                tp_capture_probability * captured_return
                + (1 - tp_capture_probability) * missed_return
            ),
            "captured_return": captured_return,
            "missed_return": missed_return,
            "tp_crossed": tp_crossed,
            "entry_status": "entered",
        }
    return surface


__all__ = [
    "PumpProfile",
    "PumpReplayPath",
    "PumpReplayResult",
    "PumpReplayState",
    "conservative_available_at_ms",
    "conservative_launch_decision_at_ms",
    "sell_credit",
    "simulate_pump_path",
    "simulate_pump_surface",
    "state_at",
]
