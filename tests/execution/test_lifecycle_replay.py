from pumpquant.execution.lifecycle_replay import (
    LandedOrder,
    LifecycleProfile,
    PumpCurveState,
    PumpSwapState,
    TimedPumpOrder,
    apply_pump_order,
    apply_pumpswap_order,
    reconstruct_pump_pre_state,
    reconstruct_pumpswap_pre_state,
    simulate_reference_pump_surface,
)
from pumpquant.pricing.pumpswap import PumpSwapFees


def test_pump_inserted_trade_persists_across_external_flow() -> None:
    state = PumpCurveState(1_000_000, 30_000, 700_000, 0, 95, 30)
    our_buy = LandedOrder("buy", 30_000, 1_000)
    first = apply_pump_order(state, our_buy)
    assert first.executed
    assert first.state != state

    external_buy = LandedOrder("buy", 20_000, 700)
    counterfactual = apply_pump_order(first.state, external_buy)
    observed_without_us = apply_pump_order(state, external_buy)
    assert counterfactual.executed
    assert observed_without_us.executed
    assert counterfactual.state != observed_without_us.state


def test_reserve_transition_inversion_is_exact() -> None:
    pump_post = PumpCurveState(990_000, 30_300, 690_000, 300, 95, 30)
    assert reconstruct_pump_pre_state(
        pump_post, side="buy", base_amount=10_000, curve_quote_amount=300
    ) == PumpCurveState(1_000_000, 30_000, 700_000, 0, 95, 30)

    swap_post = PumpSwapState(190_000, 61_000, 20_000, PumpSwapFees())
    assert reconstruct_pumpswap_pre_state(
        swap_post, side="buy", base_amount=10_000, pool_quote_amount=1_000
    ) == PumpSwapState(200_000, 60_000, 20_000, PumpSwapFees())


def test_pump_slippage_sensitivity_can_drop_worsened_order() -> None:
    state = PumpCurveState(1_000_000, 30_000, 700_000, 0, 95, 30)
    large_prior = apply_pump_order(state, LandedOrder("buy", 300_000, 20_000))
    assert large_prior.executed
    result = apply_pump_order(
        large_prior.state,
        LandedOrder("buy", 20_000, 700),
        maximum_adverse_bps=100,
    )
    assert not result.executed
    assert result.reason == "pump_slippage_sensitivity_drop"


def test_pumpswap_transition_uses_effective_quote_but_real_inventory() -> None:
    state = PumpSwapState(
        real_base=200_000,
        real_quote=60_000,
        virtual_quote=20_000,
        fees=PumpSwapFees(lp_fee_bps=2, protocol_fee_bps=93, cashback_fee_bps=30),
    )
    result = apply_pumpswap_order(state, LandedOrder("buy", 1_000, 1_000, 1_100, None))
    assert result.executed
    assert result.state.real_base < state.real_base
    assert result.state.real_quote > state.real_quote
    assert result.state.virtual_quote == state.virtual_quote


def test_pumpswap_sell_respects_event_minimum() -> None:
    state = PumpSwapState(
        real_base=200_000,
        real_quote=60_000,
        virtual_quote=20_000,
        fees=PumpSwapFees(lp_fee_bps=2, protocol_fee_bps=93, creator_fee_bps=30),
    )
    result = apply_pumpswap_order(
        state,
        LandedOrder("sell", 10_000, 1_000, None, 100_000),
    )
    assert not result.executed
    assert result.reason == "pumpswap_min_quote_rejected"


def test_reference_surface_closes_at_timeout_with_full_costs() -> None:
    result = simulate_reference_pump_surface(
        decision_state=PumpCurveState(1_000_000, 30_000, 700_000, 0, 95, 30),
        decision_at_ms=1_000,
        future_orders=(),
        profile=LifecycleProfile(2.0, 0.5, 10),
        budget_lamports=1_000,
        entry_latency_ms=100,
        exit_latency_ms=100,
        transaction_fee_per_attempt=5,
        maximum_attempts=2,
    )
    assert result.status == "timeout"
    assert result.entry_time_ms == 1_100
    assert result.exit_time_ms == 11_200
    assert result.transaction_cost == 10
    assert result.net_pnl < 0


def test_reference_surface_requotes_external_orders_after_our_entry() -> None:
    external = TimedPumpOrder(
        available_at_ms=1_500,
        order_index=1,
        order=LandedOrder("buy", 100_000, 10_000),
        protocol_fee_bps=95,
        creator_or_cashback_fee_bps=30,
    )
    result = simulate_reference_pump_surface(
        decision_state=PumpCurveState(1_000_000, 30_000, 700_000, 0, 95, 30),
        decision_at_ms=1_000,
        future_orders=(external,),
        profile=LifecycleProfile(1.1, 0.5, 10),
        budget_lamports=1_000,
        entry_latency_ms=100,
        exit_latency_ms=100,
        transaction_fee_per_attempt=5,
        maximum_attempts=2,
    )
    assert result.status == "tp"
    assert result.exit_credit >= 1_100
