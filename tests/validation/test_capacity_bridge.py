from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from pumpquant.validation.capacity_bridge import (
    FIXED_COST_PER_LEG_ATOMIC,
    build_exact_capacity_replay_factory,
)
from pumpquant.validation.capacity_gate import (
    DAY_NS,
    CapacityGateError,
    CausalFxPoint,
    CausalFxSeries,
    attribute_realized_net,
)
from pumpquant.validation.survivor_gate import CausalFxSeries as ExactFxSeries
from pumpquant.validation.survivor_gate import ExactGateConfig, FxPoint
from tests.validation.test_survivor_gate import _signal as _base_signal
from tests.validation.test_survivor_gate import _states


def _signal(**kwargs: object):  # type: ignore[no-untyped-def]
    return replace(_base_signal(**kwargs), family="shock-recovery")


def _fx(*, d0_price: str = "100", final_price: str = "100") -> tuple[ExactFxSeries, CausalFxSeries]:
    exact = ExactFxSeries(
        (
            FxPoint(0, Decimal(d0_price)),
            FxPoint(10 * DAY_NS, Decimal(final_price)),
        )
    )
    capacity = CausalFxSeries(
        (
            CausalFxPoint(0, Decimal(d0_price)),
            CausalFxPoint(10 * DAY_NS, Decimal(final_price)),
        )
    )
    return exact, capacity


def test_factory_uses_a_fresh_dynamic_rung_budget_and_d0_seed() -> None:
    states = _states(start_slot=1)
    signal = _signal(start_slot=1)
    exact_fx, capacity_fx = _fx()
    factory = build_exact_capacity_replay_factory((signal,), states, exact_fx, capacity_fx)

    ten = factory(Decimal("10"), 0)
    one_hundred = factory(Decimal("100"), 0)
    assert ten.starting_quote_atomic == 100_000_000
    assert one_hundred.starting_quote_atomic == 1_000_000_000
    assert ten.decisions[0].budget_quote_atomic == 20_000_000
    assert one_hundred.decisions[0].budget_quote_atomic == 200_000_000
    assert (
        ten.accepted_trades[0].entry_quote_atomic
        != one_hundred.accepted_trades[0].entry_quote_atomic
    )


def test_factory_seeds_at_d0_latest_causal_close_not_development_start() -> None:
    states = _states(start_slot=DAY_NS // 1_000_000_000 + 1)
    signal = _signal(start_slot=DAY_NS // 1_000_000_000 + 1)
    exact = ExactFxSeries(
        (
            FxPoint(0, Decimal("50")),
            FxPoint(DAY_NS, Decimal("100")),
            FxPoint(10 * DAY_NS, Decimal("100")),
        )
    )
    capacity = CausalFxSeries(
        (
            CausalFxPoint(0, Decimal("50")),
            CausalFxPoint(DAY_NS, Decimal("100")),
            CausalFxPoint(10 * DAY_NS, Decimal("100")),
        )
    )
    factory = build_exact_capacity_replay_factory((signal,), states, exact, capacity)
    replay = factory(Decimal("10"), DAY_NS)
    assert replay.starting_quote_atomic == 100_000_000
    assert replay.starting_quote_atomic != 200_000_000


def test_crossing_position_is_rejected_and_never_replayed() -> None:
    inside_states = _states(mint="INSIDE", start_slot=1)
    outside_states = _states(mint="OUTSIDE", start_slot=2)
    inside = _signal(mint="INSIDE", start_slot=1)
    outside = _signal(mint="OUTSIDE", start_slot=2)
    outside_states["OUTSIDE-exit"] = replace(
        outside_states["OUTSIDE-exit"], available_at_ns=4 * DAY_NS
    )
    outside = replace(outside, exit_available_at_ns=4 * DAY_NS)
    states = {**inside_states, **outside_states}
    exact_fx, capacity_fx = _fx()
    replay = build_exact_capacity_replay_factory((inside, outside), states, exact_fx, capacity_fx)(
        Decimal("10"), 0
    )
    assert len(replay.accepted_trades) == 1
    rejected = next(item for item in replay.decisions if item.trade_id.endswith("OUTSIDE-entry"))
    assert not rejected.accepted
    assert rejected.reason == "outside_four_day_window"
    assert all(
        0 <= trade.entry_available_at_ns < 4 * DAY_NS
        and 0 <= trade.exit_available_at_ns < 4 * DAY_NS
        for trade in replay.accepted_trades
    )


def test_all_crossing_positions_form_a_failed_zero_trade_rung() -> None:
    states = _states(start_slot=1)
    signal = _signal(start_slot=1)
    states["TOKEN-entry"] = replace(states["TOKEN-entry"], available_at_ns=4 * DAY_NS + 1)
    states["TOKEN-exit"] = replace(states["TOKEN-exit"], available_at_ns=4 * DAY_NS + 2)
    signal = replace(
        signal,
        entry_available_at_ns=4 * DAY_NS + 1,
        exit_available_at_ns=4 * DAY_NS + 2,
    )
    exact_fx, capacity_fx = _fx()
    replay = build_exact_capacity_replay_factory((signal,), states, exact_fx, capacity_fx)(
        Decimal("10"), 0
    )
    assert not replay.accepted_trades
    assert replay.starting_quote_atomic == replay.final_quote_atomic == 100_000_000
    assert replay.minimum_equity_quote_atomic == 100_000_000
    assert replay.decisions[0].reason == "outside_four_day_window"
    assert not replay.passed


def test_costs_are_one_160000_lamport_charge_per_leg_and_chain_keys_transfer() -> None:
    states = _states(start_slot=1)
    signal = _signal(start_slot=1)
    exact_fx, capacity_fx = _fx()
    replay = build_exact_capacity_replay_factory((signal,), states, exact_fx, capacity_fx)(
        Decimal("10"), 0
    )
    trade = replay.accepted_trades[0]
    assert trade.entry_cost_quote_atomic == FIXED_COST_PER_LEG_ATOMIC
    assert trade.exit_cost_quote_atomic == FIXED_COST_PER_LEG_ATOMIC
    assert (
        trade.realized_net_quote_atomic
        == trade.exit_quote_atomic - trade.entry_quote_atomic - 2 * FIXED_COST_PER_LEG_ATOMIC
    )
    assert trade.entry_chain_key is not None
    assert trade.exit_chain_key is not None
    assert trade.entry_chain_key.event_id == signal.entry_event_id
    assert trade.exit_chain_key.event_id == signal.exit_event_id
    assert replay.accepted_trades[0].trade_id == (
        f"{signal.candidate_sha256}:{signal.fold}:{signal.mint}:"
        f"{signal.decision_event_id}:{signal.entry_event_id}"
    )


def test_capacity_daily_attribution_uses_realized_exact_exits() -> None:
    states = _states(start_slot=1)
    signal = _signal(start_slot=1)
    exact_fx, capacity_fx = _fx()
    replay = build_exact_capacity_replay_factory((signal,), states, exact_fx, capacity_fx)(
        Decimal("10"), 0
    )
    attribution = attribute_realized_net(
        replay.realized_exits,
        capacity_fx,
        availability_start_ns=0,
        availability_end_ns=4 * DAY_NS,
    )
    assert len(attribution.days) == 4
    assert attribution.days[0].exit_count == 1
    expected_usd = (
        Decimal(replay.accepted_trades[0].realized_net_quote_atomic)
        / Decimal(1_000_000_000)
        * Decimal("100")
    )
    assert attribution.days[0].net_usd == expected_usd
    assert all(day.exit_count == 0 for day in attribution.days[1:])


def test_empty_or_inconsistent_inputs_fail_closed() -> None:
    exact_fx, capacity_fx = _fx()
    with pytest.raises(CapacityGateError, match="no positions"):
        build_exact_capacity_replay_factory((), {}, exact_fx, capacity_fx)
    with pytest.raises(CapacityGateError, match="fixed cost"):
        build_exact_capacity_replay_factory(
            (_signal(),),
            _states(),
            exact_fx,
            capacity_fx,
            config=replace(
                ExactGateConfig(),
                fixed_cost_per_leg_atomic=1,
            ),
        )


def test_full_fx_series_and_registered_candidate_identity_are_required() -> None:
    exact = ExactFxSeries(
        (
            FxPoint(0, Decimal("100")),
            FxPoint(DAY_NS, Decimal("101")),
            FxPoint(10 * DAY_NS, Decimal("100")),
        )
    )
    capacity = CausalFxSeries(
        (
            CausalFxPoint(0, Decimal("100")),
            CausalFxPoint(DAY_NS, Decimal("999")),
            CausalFxPoint(10 * DAY_NS, Decimal("100")),
        )
    )
    with pytest.raises(CapacityGateError, match="FX series differ"):
        build_exact_capacity_replay_factory((_signal(),), _states(), exact, capacity)

    exact_fx, capacity_fx = _fx()
    with pytest.raises(CapacityGateError, match="family is not registered"):
        build_exact_capacity_replay_factory(
            (replace(_signal(), family="invented-family"),),
            _states(),
            exact_fx,
            capacity_fx,
        )
    with pytest.raises(CapacityGateError, match="mixes candidate"):
        build_exact_capacity_replay_factory(
            (_signal(), replace(_signal(), candidate_sha256="b" * 64)),
            _states(),
            exact_fx,
            capacity_fx,
        )
