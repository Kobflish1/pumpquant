"""Bridge the exact survivor replay into the capacity-wallet contract.

The capacity ladder owns the wallet and attribution *shape*, but
it must not invent execution fills.  This is the small adapter at that
boundary: each invocation of the returned factory starts a new exact replay at
the requested USD seed, converts only the exact fills into
``capacity_gate.WalletTrade`` values, and returns a
``capacity_gate.SharedWalletReplay``.  The capacity ladder subsequently derives
its four-day attribution from ``wallet.realized_exits``.

No file or holdout reader lives here.  Callers provide already sealed signals,
integer event states, and causal FX evidence.  Inputs outside the requested
four-day half-open window are excluded before exact replay so a fill cannot
cross the capacity boundary.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import replace
from decimal import Decimal
from typing import Final, TypeAlias

from pumpquant.validation.capacity_gate import (
    DAY_NS,
    CapacityGateError,
    SharedWalletReplay,
    WalletDecision,
    WalletTrade,
    replay_shared_wallet,
)
from pumpquant.validation.capacity_gate import (
    CausalFxPoint as CapacityFxPoint,
)
from pumpquant.validation.capacity_gate import (
    CausalFxSeries as CapacityFxSeries,
)
from pumpquant.validation.capacity_gate import (
    ChainKey as CapacityChainKey,
)
from pumpquant.validation.survivor_gate import (
    AtomicEventState,
    ExactGateConfig,
    ExactSurvivorGateError,
    ExactTrade,
    PositionSignal,
    replay_shared_capital,
)
from pumpquant.validation.survivor_gate import (
    CausalFxSeries as ExactFxSeries,
)
from pumpquant.validation.survivor_gate import (
    ChainKey as ExactChainKey,
)
from pumpquant.validation.survivor_gate import (
    FxPoint as ExactFxPoint,
)

FIXED_COST_PER_LEG_ATOMIC: Final[int] = 160_000
FOUR_DAY_WINDOW_NS: Final[int] = 4 * DAY_NS
REGISTERED_FAMILIES: Final[frozenset[str]] = frozenset(
    {
        "shock-recovery",
        "progress-speed",
        "persistent-pressure",
        "amm-impact-ofi",
        "shock-recovery-fresh-v2",
        "progress-speed-fresh-v2",
        "persistent-pressure-fresh-v2",
        "amm-impact-ofi-fresh-v2",
    }
)


ReplayFactory: TypeAlias = Callable[[Decimal, int], SharedWalletReplay]


def _require_nonempty_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise CapacityGateError(f"{label} is missing or invalid")
    return value


def _require_integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CapacityGateError(f"{label} is missing or invalid")
    return value


def _validate_signal(signal: PositionSignal, index: int) -> None:
    """Validate manually-created ``PositionSignal`` instances as well as rows.

    ``PositionSignal`` has no ``__post_init__`` so the exact
    runner can construct it directly.  The bridge is a promotion boundary and
    therefore checks the fields that are otherwise only validated by
    ``PositionSignal.from_mapping``.
    """

    prefix = f"signal {index}"
    if not isinstance(signal, PositionSignal):
        raise CapacityGateError(f"{prefix} has an invalid type")
    for label, value in (
        ("family", signal.family),
        ("candidate_sha256", signal.candidate_sha256),
        ("mint", signal.mint),
        ("decision_event_id", signal.decision_event_id),
        ("entry_event_id", signal.entry_event_id),
        ("exit_event_id", signal.exit_event_id),
    ):
        _require_nonempty_text(value, f"{prefix}.{label}")
    if signal.family not in REGISTERED_FAMILIES:
        raise CapacityGateError(f"{prefix}.family is not registered")
    if len(signal.candidate_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in signal.candidate_sha256
    ):
        raise CapacityGateError(f"{prefix}.candidate_sha256 is invalid")
    _require_integer(signal.fold, f"{prefix}.fold", minimum=1)
    _require_integer(signal.era, f"{prefix}.era", minimum=1)
    _require_integer(signal.decision_index, f"{prefix}.decision_index")
    _require_integer(signal.decision_available_at_ns, f"{prefix}.decision_available_at_ns")
    _require_integer(signal.entry_available_at_ns, f"{prefix}.entry_available_at_ns")
    _require_integer(signal.exit_available_at_ns, f"{prefix}.exit_available_at_ns")
    _require_integer(signal.holding_events, f"{prefix}.holding_events", minimum=1)
    if signal.holding_events not in {20, 50}:
        raise CapacityGateError(f"{prefix}.holding_events is outside the frozen set")


def _trade_id(signal: PositionSignal) -> str:
    """Return the frozen, deterministic identity used by the capacity ledger."""

    # Keep this spelling human-auditable.  It is also independent of a rung's
    # starting capital, so every rung can be compared by trade identity.
    return ":".join(
        (
            signal.candidate_sha256,
            str(signal.fold),
            signal.mint,
            signal.decision_event_id,
            signal.entry_event_id,
        )
    )


def _as_exact_fx(value: ExactFxSeries | CapacityFxSeries) -> ExactFxSeries:
    if isinstance(value, ExactFxSeries):
        return value
    if isinstance(value, CapacityFxSeries):
        return ExactFxSeries(
            tuple(ExactFxPoint(point.available_at_ns, point.close_usd) for point in value.points)
        )
    raise CapacityGateError("exact causal FX evidence has an invalid type")


def _as_capacity_fx(value: ExactFxSeries | CapacityFxSeries) -> CapacityFxSeries:
    if isinstance(value, CapacityFxSeries):
        return value
    if isinstance(value, ExactFxSeries):
        return CapacityFxSeries(
            tuple(CapacityFxPoint(point.available_at_ns, point.close_usd) for point in value.points)
        )
    raise CapacityGateError("capacity causal FX evidence has an invalid type")


def _capacity_chain_key(value: object, label: str) -> CapacityChainKey:
    """Copy an exact chain key into capacity's nominally identical type."""

    # Importing the exact class under an alias avoids accidentally accepting a
    # capacity ChainKey supplied by a caller.  The exact replay is authoritative
    # and every field is copied, including event_id.
    from pumpquant.validation.survivor_gate import ChainKey as ExactChainKey

    if not isinstance(value, ExactChainKey):
        raise CapacityGateError(f"{label} is missing or has an invalid type")
    return CapacityChainKey(
        slot=_require_integer(value.slot, f"{label}.slot"),
        tx_index=_require_integer(value.tx_index, f"{label}.tx_index"),
        log_index=_require_integer(value.log_index, f"{label}.log_index"),
        event_index=_require_integer(value.event_index, f"{label}.event_index"),
        event_id=_require_nonempty_text(value.event_id, f"{label}.event_id"),
    )


def _wallet_trade(exact_trade: ExactTrade, controls: ExactGateConfig) -> WalletTrade:
    """Adapt one exact fill without charging either leg twice."""

    signal = exact_trade.signal
    if exact_trade.fixed_cost_atomic != 2 * controls.fixed_cost_per_leg_atomic:
        raise CapacityGateError("exact trade fixed cost does not reconcile")
    entry_key = _capacity_chain_key(exact_trade.entry_key, "entry_chain_key")
    exit_key = _capacity_chain_key(exact_trade.exit_key, "exit_chain_key")
    if entry_key.event_id != signal.entry_event_id or exit_key.event_id != signal.exit_event_id:
        raise CapacityGateError("exact trade chain key identity disagrees with its signal")
    trade = WalletTrade(
        trade_id=_trade_id(signal),
        mint=_require_nonempty_text(signal.mint, "trade mint"),
        entry_available_at_ns=_require_integer(
            signal.entry_available_at_ns, "entry_available_at_ns"
        ),
        exit_available_at_ns=_require_integer(signal.exit_available_at_ns, "exit_available_at_ns"),
        # ``entry_quote_spent_atomic`` is the actual integer debit, not the
        # dynamic gross budget used to obtain the exact quote.
        entry_quote_atomic=_require_integer(
            exact_trade.entry_quote_spent_atomic, "entry_quote_spent_atomic", minimum=1
        ),
        # This is already the per-leg adverse-haircut output from exact replay.
        exit_quote_atomic=_require_integer(
            exact_trade.exit_quote_after_haircut_atomic, "exit_quote_after_haircut_atomic"
        ),
        entry_cost_quote_atomic=controls.fixed_cost_per_leg_atomic,
        exit_cost_quote_atomic=controls.fixed_cost_per_leg_atomic,
        quote_decimals=9,
        entry_chain_key=entry_key,
        exit_chain_key=exit_key,
    )
    if trade.realized_net_quote_atomic != exact_trade.pnl_quote_atomic:
        raise CapacityGateError("exact trade cost or quote arithmetic does not reconcile")
    if exact_trade.quote_budget_atomic <= 0:
        raise CapacityGateError("exact trade quote budget is invalid")
    if trade.entry_quote_atomic > exact_trade.quote_budget_atomic:
        raise CapacityGateError("exact trade spent quote exceeds its dynamic budget")
    return trade


def _window_signals(
    signals: Sequence[PositionSignal],
    d0: int,
) -> tuple[PositionSignal, ...]:
    end = d0 + FOUR_DAY_WINDOW_NS
    result = tuple(
        signal
        for signal in signals
        if d0 <= signal.entry_available_at_ns < end and d0 <= signal.exit_available_at_ns < end
    )
    return result


def _validate_states_for_signals(
    signals: Iterable[PositionSignal], states: Mapping[str, AtomicEventState]
) -> None:
    for index, signal in enumerate(signals, 1):
        for label, event_id in (
            ("decision", signal.decision_event_id),
            ("entry", signal.entry_event_id),
            ("exit", signal.exit_event_id),
        ):
            try:
                state = states[event_id]
            except KeyError as error:
                raise CapacityGateError(f"signal {index} {label} state is missing") from error
            if not isinstance(state, AtomicEventState):
                raise CapacityGateError(f"signal {index} {label} state has an invalid type")
            if state.event_id != event_id:
                raise CapacityGateError(f"signal {index} {label} state/event identity disagrees")
            try:
                state.validate()
            except ExactSurvivorGateError as error:
                raise CapacityGateError(f"signal {index} {label} state is invalid") from error


def _validate_fx_at_d0(exact_fx: ExactFxSeries, capacity_fx: CapacityFxSeries, d0: int) -> None:
    try:
        exact_price = exact_fx.price_at(d0)
        capacity_price = capacity_fx.price_at(d0)
    except (ExactSurvivorGateError, CapacityGateError) as error:
        raise CapacityGateError("causal FX is unavailable at D0") from error
    if exact_price != capacity_price:
        raise CapacityGateError("exact and capacity causal FX disagree at D0")


def _validate_fx_series(exact_fx: ExactFxSeries, capacity_fx: CapacityFxSeries) -> None:
    """Require one byte-equivalent logical FX series across both kernels."""

    exact_points = tuple((point.available_at_ns, point.close_usd) for point in exact_fx.points)
    capacity_points = tuple(
        (point.available_at_ns, point.close_usd) for point in capacity_fx.points
    )
    if exact_points != capacity_points:
        raise CapacityGateError("exact and capacity causal FX series differ")


def build_exact_capacity_replay_factory(
    signals: Sequence[PositionSignal] | Iterable[PositionSignal],
    states: Mapping[str, AtomicEventState],
    exact_fx: ExactFxSeries | CapacityFxSeries | None = None,
    capacity_fx: CapacityFxSeries | ExactFxSeries | None = None,
    *,
    fx: ExactFxSeries | CapacityFxSeries | None = None,
    config: ExactGateConfig | None = None,
) -> ReplayFactory:
    """Build the authoritative per-rung exact-to-capacity replay callback.

    ``exact_fx`` and ``capacity_fx`` may be supplied separately when callers
    already have both typed representations.  A single ``exact_fx`` (or the
    keyword alias ``fx``) is enough; the other representation is converted
    losslessly.  Every callback call is independent and uses
    ``replace(config, starting_usd=seed)``.
    """

    if exact_fx is not None and fx is not None:
        raise CapacityGateError("exact_fx and fx are duplicate causal FX arguments")
    if fx is not None:
        exact_fx = fx
    if exact_fx is None and capacity_fx is None:
        raise CapacityGateError("causal FX evidence is required")
    if exact_fx is None:
        exact_fx = capacity_fx
    if capacity_fx is None:
        capacity_fx = exact_fx
    if exact_fx is None or capacity_fx is None:  # defensive narrowing for mypy
        raise CapacityGateError("causal FX evidence is required")

    exact_series = _as_exact_fx(exact_fx)
    capacity_series = _as_capacity_fx(capacity_fx)
    _validate_fx_series(exact_series, capacity_series)
    controls = ExactGateConfig() if config is None else config
    if not isinstance(controls, ExactGateConfig):
        raise CapacityGateError("exact replay config has an invalid type")
    if controls.fixed_cost_per_leg_atomic != FIXED_COST_PER_LEG_ATOMIC:
        raise CapacityGateError("exact replay fixed cost must be 160000 lamports per leg")
    if not isinstance(states, Mapping):
        raise CapacityGateError("event states have an invalid type")

    materialized_signals = tuple(signals)
    if not materialized_signals:
        raise CapacityGateError("exact capacity bridge has no positions")
    if not states:
        raise CapacityGateError("exact capacity bridge has no event states")
    for index, signal in enumerate(materialized_signals, 1):
        _validate_signal(signal, index)
    if len({signal.family for signal in materialized_signals}) != 1:
        raise CapacityGateError("exact capacity bridge mixes strategy families")
    if len({signal.candidate_sha256 for signal in materialized_signals}) != 1:
        raise CapacityGateError("exact capacity bridge mixes candidate identities")
    trade_ids = tuple(_trade_id(signal) for signal in materialized_signals)
    if len(set(trade_ids)) != len(trade_ids):
        raise CapacityGateError("exact capacity bridge has duplicate trade IDs")

    # State references are checked at callback time.  The callback can then
    # report a deterministic rejection for a crossing position without ever
    # feeding it to the exact wallet.
    def factory(seed: Decimal, d0: int) -> SharedWalletReplay:
        if isinstance(seed, bool) or not isinstance(seed, Decimal):
            raise CapacityGateError("per-rung seed must be a Decimal")
        if not seed.is_finite() or seed <= 0:
            raise CapacityGateError("per-rung seed must be positive and finite")
        if isinstance(d0, bool) or not isinstance(d0, int) or d0 < 0:
            raise CapacityGateError("D0 must be a non-negative integer timestamp")
        if d0 % DAY_NS:
            raise CapacityGateError("D0 must be a UTC midnight")
        end = d0 + FOUR_DAY_WINDOW_NS
        if end <= d0:
            raise CapacityGateError("four-day capacity window is invalid")

        selected = _window_signals(materialized_signals, d0)
        # Every signal needs its landed entry key for the deterministic
        # decision order, including signals that are rejected at the boundary.
        # Only selected signals need full decision/entry/exit state validation.
        entry_keys: dict[str, ExactChainKey] = {}
        for index, signal in enumerate(materialized_signals, 1):
            try:
                entry_state = states[signal.entry_event_id]
            except KeyError as error:
                raise CapacityGateError(f"signal {index} entry state is missing") from error
            if not isinstance(entry_state, AtomicEventState):
                raise CapacityGateError(f"signal {index} entry state has an invalid type")
            if entry_state.event_id != signal.entry_event_id:
                raise CapacityGateError(f"signal {index} entry state/event identity disagrees")
            try:
                entry_state.validate()
            except ExactSurvivorGateError as error:
                raise CapacityGateError(f"signal {index} entry state is invalid") from error
            entry_keys[_trade_id(signal)] = entry_state.key
        _validate_fx_at_d0(exact_series, capacity_series, d0)
        if not selected:
            # A rung with no in-window opportunities is a valid failed rung,
            # not missing evidence.  It still carries every deterministic
            # boundary rejection, while preserving the fresh D0 seed.
            starting_quote = capacity_series.atomic_from_usd(seed, d0, decimals=9)
            return SharedWalletReplay(
                starting_usd=seed,
                starting_quote_atomic=starting_quote,
                final_quote_atomic=starting_quote,
                final_usd=capacity_series.usd_from_atomic(starting_quote, d0, decimals=9),
                minimum_equity_quote_atomic=starting_quote,
                decisions=tuple(
                    WalletDecision(
                        trade_id=_trade_id(signal),
                        accepted=False,
                        reason="outside_four_day_window",
                        budget_quote_atomic=0,
                        cash_after_atomic=starting_quote,
                        open_position_count=0,
                    )
                    for signal in sorted(
                        materialized_signals,
                        key=lambda signal: (
                            entry_keys[_trade_id(signal)],
                            signal.candidate_sha256,
                            signal.mint,
                            signal.fold,
                            signal.decision_event_id,
                            signal.entry_event_id,
                        ),
                    )
                ),
                accepted_trades=(),
                maximum_concurrent_positions=0,
                profitable_in_quote=False,
                profitable_in_usd=False,
                passed=False,
                final_exit_available_at_ns=d0,
                allocation_percent=controls.allocation_percent,
                max_concurrent_positions=controls.max_concurrent_positions,
            )
        _validate_states_for_signals(selected, states)
        exact_controls = replace(controls, starting_usd=seed)
        try:
            exact = replay_shared_capital(
                selected,
                states,
                exact_series,
                development_start_available_at_ns=d0,
                config=exact_controls,
            )
        except ExactSurvivorGateError as error:
            raise CapacityGateError("exact per-rung replay failed") from error

        adapted = tuple(_wallet_trade(trade, exact_controls) for trade in exact.accepted_trades)
        adapted_ids = tuple(trade.trade_id for trade in adapted)
        if len(set(adapted_ids)) != len(adapted_ids):
            raise CapacityGateError("exact capacity bridge has duplicate accepted trade IDs")

        # Running the capacity kernel over exact fills is an invariant check:
        # it confirms dynamic (per-rung) quote budgets and chain-key settlement
        # without allowing the capacity layer to re-quote or resize a trade.
        exact_by_id = {_trade_id(trade.signal): trade for trade in exact.accepted_trades}
        exact_decisions_by_id = {
            _trade_id(decision.signal): decision for decision in exact.decisions
        }
        capacity_passed = bool(
            adapted and exact.minimum_equity_quote_atomic >= 0 and exact.final_quote_atomic >= 0
        )
        capacity_check: SharedWalletReplay | None = None
        if adapted:
            try:
                capacity_check = replay_shared_wallet(
                    adapted,
                    capacity_series,
                    starting_usd=seed,
                    start_available_at_ns=d0,
                    allocation_percent=exact_controls.allocation_percent,
                    max_concurrent_positions=exact_controls.max_concurrent_positions,
                )
            except CapacityGateError:
                raise
            if tuple(item.trade_id for item in capacity_check.accepted_trades) != adapted_ids:
                raise CapacityGateError("capacity replay changed exact accepted trades")
            for decision in capacity_check.decisions:
                source = exact_by_id.get(decision.trade_id)
                if source is None:
                    raise CapacityGateError("capacity replay produced an unknown trade decision")
                if not decision.accepted or decision.reason != "accepted":
                    raise CapacityGateError("capacity replay rejected an exact accepted trade")
                if decision.budget_quote_atomic != source.quote_budget_atomic:
                    raise CapacityGateError("per-rung dynamic quote budget changed")
                source_decision = exact_decisions_by_id.get(decision.trade_id)
                if source_decision is None or not source_decision.accepted:
                    raise CapacityGateError("capacity decision does not match exact decision")
                if (
                    decision.cash_after_atomic != source_decision.cash_after_atomic
                    or decision.open_position_count != source_decision.open_position_count
                ):
                    raise CapacityGateError("capacity decision ledger differs from exact replay")
            if capacity_check.starting_quote_atomic != exact.starting_quote_atomic:
                raise CapacityGateError("capacity D0 seed differs from exact replay")
            if capacity_check.final_quote_atomic != exact.final_quote_atomic:
                raise CapacityGateError("capacity final quote differs from exact replay")
            if capacity_check.minimum_equity_quote_atomic != exact.minimum_equity_quote_atomic:
                raise CapacityGateError("capacity minimum equity differs from exact replay")
            if capacity_check.maximum_concurrent_positions != exact.maximum_concurrent_positions:
                raise CapacityGateError("capacity concurrency differs from exact replay")
            # Profitability is recomputed below from the adapted quote balance
            # and the capacity FX series; do not trust a caller-claimed flag.

        final_exit_ns = d0
        if exact.accepted_trades:
            final_exit_ns = max(
                trade.signal.exit_available_at_ns for trade in exact.accepted_trades
            )
        try:
            final_usd = capacity_series.usd_from_atomic(
                exact.final_quote_atomic, final_exit_ns, decimals=9
            )
        except CapacityGateError as error:
            raise CapacityGateError("capacity final FX is unavailable") from error
        if capacity_check is not None and final_usd != capacity_check.final_usd:
            raise CapacityGateError("capacity final USD is inconsistent")
        # Exact and capacity FX are expected to be the same sealed series.  A
        # mismatch at an exit would make USD profitability flags ambiguous, so
        # fail closed rather than silently selecting one representation.
        try:
            exact_final_usd = exact.final_usd
        except AttributeError as error:  # pragma: no cover - defensive
            raise CapacityGateError("exact replay final USD is unavailable") from error
        if exact_final_usd != final_usd:
            raise CapacityGateError("exact and capacity final USD disagree")
        profitable_in_quote = exact.final_quote_atomic > exact.starting_quote_atomic
        profitable_in_usd = final_usd > seed
        if capacity_check is not None and (
            capacity_check.profitable_in_quote != profitable_in_quote
            or capacity_check.profitable_in_usd != profitable_in_usd
            or capacity_check.passed != capacity_passed
        ):
            raise CapacityGateError("capacity replay profitability flags are inconsistent")

        # Preserve exact decisions, including exact rejection reasons and the
        # exact dynamic budget/cash-after/open-count values.  Capacity's wallet
        # has one decision for each adapted opportunity; map those accepted
        # decisions back into exact order and retain exact rejections.  A
        # crossing signal is never submitted to exact replay; it is still
        # represented as a deterministic capacity rejection.
        capacity_decisions = (
            {}
            if capacity_check is None
            else {decision.trade_id: decision for decision in capacity_check.decisions}
        )
        exact_decision_ids = set(exact_decisions_by_id)

        def decision_order(signal: PositionSignal) -> tuple[object, ...]:
            return (
                entry_keys[_trade_id(signal)],
                signal.candidate_sha256,
                signal.mint,
                signal.fold,
                signal.decision_event_id,
                signal.entry_event_id,
            )

        decisions: list[WalletDecision] = []
        latest_cash = exact.starting_quote_atomic
        latest_open = 0
        for signal in sorted(materialized_signals, key=decision_order):
            identifier = _trade_id(signal)
            source_decision = exact_decisions_by_id.get(identifier)
            if source_decision is not None and source_decision.accepted:
                converted = capacity_decisions.get(identifier)
                if converted is None:
                    raise CapacityGateError("capacity decision is missing for exact trade")
                decisions.append(converted)
                latest_cash = converted.cash_after_atomic
                latest_open = converted.open_position_count
                continue
            if source_decision is not None:
                decisions.append(
                    WalletDecision(
                        trade_id=identifier,
                        accepted=False,
                        reason=source_decision.reason,
                        budget_quote_atomic=(
                            source_decision.quote_budget_atomic
                            if source_decision.quote_budget_atomic is not None
                            else 0
                        ),
                        cash_after_atomic=source_decision.cash_after_atomic,
                        open_position_count=source_decision.open_position_count,
                    )
                )
                latest_cash = source_decision.cash_after_atomic
                latest_open = source_decision.open_position_count
                continue
            if identifier in exact_decision_ids:  # pragma: no cover - defensive
                raise CapacityGateError("exact decision lookup is inconsistent")
            decisions.append(
                WalletDecision(
                    trade_id=identifier,
                    accepted=False,
                    reason="outside_four_day_window",
                    budget_quote_atomic=0,
                    cash_after_atomic=latest_cash,
                    open_position_count=latest_open,
                )
            )

        # The capacity object is built from exact output fields and the
        # capacity FX conversion above; no caller-supplied wallet or
        # attribution can forge its result.  ``evaluate_starting_capital_ladder``
        # will recompute FourDayAttribution from ``realized_exits``.
        return SharedWalletReplay(
            starting_usd=seed,
            starting_quote_atomic=exact.starting_quote_atomic,
            final_quote_atomic=exact.final_quote_atomic,
            final_usd=final_usd,
            minimum_equity_quote_atomic=exact.minimum_equity_quote_atomic,
            decisions=tuple(decisions),
            accepted_trades=adapted,
            maximum_concurrent_positions=exact.maximum_concurrent_positions,
            profitable_in_quote=profitable_in_quote,
            profitable_in_usd=profitable_in_usd,
            passed=capacity_passed,
            final_exit_available_at_ns=final_exit_ns,
            allocation_percent=exact_controls.allocation_percent,
            max_concurrent_positions=exact_controls.max_concurrent_positions,
        )

    return factory


__all__ = [
    "FIXED_COST_PER_LEG_ATOMIC",
    "FOUR_DAY_WINDOW_NS",
    "REGISTERED_FAMILIES",
    "ReplayFactory",
    "build_exact_capacity_replay_factory",
]
