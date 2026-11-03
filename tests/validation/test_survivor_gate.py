from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from pumpquant.pricing.pumpswap import (
    PumpSwapFees,
    pumpswap_buy_exact_quote_in,
    pumpswap_sell_exact_base,
)
from pumpquant.validation.survivor_gate import (
    AtomicEventState,
    CausalFxSeries,
    ExactGateConfig,
    ExactSurvivorGateError,
    FxPoint,
    PositionSignal,
    quote_position_exact,
    replay_fixed_notional,
    replay_shared_capital,
)


def _row(
    event_id: str,
    slot: int,
    *,
    mint: str = "TOKEN",
    phase: str = "pumpswap",
    flipped: bool | None = False,
    quote_reserve: int = 100_000_000_000,
    base_reserve: int = 1_000_000_000_000_000,
    creator_bps: int = 5,
    cashback_bps: int = 0,
    buyback_bps: int = 10,
) -> dict[str, object]:
    lp_bps = 20 if phase == "pumpswap" else None
    protocol_bps = 5 if phase == "pumpswap" else 95
    venue_bps = protocol_bps + (lp_bps or 0) + creator_bps + cashback_bps
    fee_asset = "base" if flipped else "quote"
    return {
        "event_id": event_id,
        "market_id": f"market-{mint}",
        "mint": mint,
        "slot": slot,
        "tx_index": 0,
        "log_index": 0,
        "event_index": 0,
        "available_at_ns": slot * 1_000_000_000,
        "phase": phase,
        "side": "sell",
        "protocol_side": "buy" if flipped else "sell",
        "instruction_name": "sell",
        "base_decimals": 6,
        "quote_decimals": 9,
        "fee_asset": fee_asset,
        "fee_mint": mint if fee_asset == "base" else "SOL",
        "fee_decimals": 6 if fee_asset == "base" else 9,
        "pool_orientation_flipped": flipped,
        "protocol_fee_bps": protocol_bps,
        "lp_fee_bps": lp_bps,
        "creator_fee_bps": creator_bps,
        "cashback_fee_bps": cashback_bps,
        "buyback_fee_bps": buyback_bps,
        "venue_fee_bps": venue_bps,
        "effective_pre_base_atomic": base_reserve,
        "effective_pre_quote_atomic": quote_reserve,
        "effective_pre_real_base_atomic": base_reserve,
        "effective_pre_real_quote_atomic": quote_reserve,
        "coverage_gap": False,
        "reserve_continuity": True,
        "real_reserve_continuity": True,
        "continuity_segment": 1,
    }


def _states(
    *,
    mint: str = "TOKEN",
    phase: str = "pumpswap",
    flipped: bool | None = False,
    start_slot: int = 1,
    exit_quote_reserve: int = 200_000_000_000,
    base_reserve: int = 1_000_000_000_000_000,
    quote_reserve: int = 100_000_000_000,
) -> dict[str, AtomicEventState]:
    rows = (
        _row(
            f"{mint}-decision",
            start_slot,
            mint=mint,
            phase=phase,
            flipped=flipped,
            base_reserve=base_reserve,
            quote_reserve=quote_reserve,
        ),
        _row(
            f"{mint}-entry",
            start_slot + 1,
            mint=mint,
            phase=phase,
            flipped=flipped,
            base_reserve=base_reserve,
            quote_reserve=quote_reserve,
        ),
        _row(
            f"{mint}-exit",
            start_slot + 100,
            mint=mint,
            phase=phase,
            flipped=flipped,
            base_reserve=base_reserve,
            quote_reserve=exit_quote_reserve,
        ),
    )
    return {str(row["event_id"]): AtomicEventState.from_mapping(row) for row in rows}


def _signal(*, mint: str = "TOKEN", start_slot: int = 1) -> PositionSignal:
    return PositionSignal(
        family="test-family",
        candidate_sha256="a" * 64,
        fold=1,
        era=1,
        mint=mint,
        decision_index=0,
        decision_event_id=f"{mint}-decision",
        decision_available_at_ns=start_slot * 1_000_000_000,
        entry_event_id=f"{mint}-entry",
        entry_available_at_ns=(start_slot + 1) * 1_000_000_000,
        exit_event_id=f"{mint}-exit",
        exit_available_at_ns=(start_slot + 100) * 1_000_000_000,
        holding_events=20,
    )


def test_exact_quote_handles_normal_quote_fee_orientation_and_routed_buyback() -> None:
    states = _states()
    signal = _signal()
    trade = quote_position_exact(signal, states, gross_quote_budget_atomic=100_000_000)
    assert trade.entry_fee_asset == "quote"
    assert trade.exit_fee_asset == "quote"
    assert trade.entry_quote_spent_atomic <= 100_000_000
    assert trade.entry_base_after_haircut_atomic < trade.entry_base_before_haircut_atomic
    assert trade.exit_quote_after_haircut_atomic < trade.exit_quote_before_haircut_atomic
    assert trade.pnl_quote_atomic > 0

    no_buyback_rows = {
        key: AtomicEventState.from_mapping(
            _row(
                value.event_id,
                value.key.slot,
                quote_reserve=value.pre_quote_atomic,
                buyback_bps=0,
            )
        )
        for key, value in states.items()
    }
    without_routed_breakdown = quote_position_exact(
        signal, no_buyback_rows, gross_quote_budget_atomic=100_000_000
    )
    assert without_routed_breakdown.pnl_quote_atomic == trade.pnl_quote_atomic


def test_exact_quote_handles_flipped_base_fee_orientation() -> None:
    states = _states(flipped=True)
    trade = quote_position_exact(_signal(), states, gross_quote_budget_atomic=100_000_000)
    assert trade.entry_fee_asset == "base"
    assert trade.exit_fee_asset == "base"
    assert trade.entry_quote_spent_atomic == 100_000_000
    assert trade.entry_base_after_haircut_atomic > 0
    assert trade.pnl_quote_atomic > 0


@pytest.mark.parametrize(
    ("flipped", "event_suffix", "real_field"),
    (
        (False, "entry", "pre_real_base_atomic"),
        (False, "exit", "pre_real_quote_atomic"),
        (True, "entry", "pre_real_base_atomic"),
        (True, "exit", "pre_real_quote_atomic"),
    ),
)
def test_pumpswap_exact_quote_enforces_orientation_correct_real_inventory(
    flipped: bool, event_suffix: str, real_field: str
) -> None:
    states = _states(flipped=flipped)
    event_id = f"TOKEN-{event_suffix}"
    states[event_id] = replace(states[event_id], **{real_field: 1})

    with pytest.raises(ExactSurvivorGateError, match="real inventory capacity"):
        quote_position_exact(_signal(), states, gross_quote_budget_atomic=100_000_000)


def test_pumpswap_real_inventory_uses_gross_curve_amount_and_canonical_side() -> None:
    fees = PumpSwapFees(lp_fee_bps=20, protocol_fee_bps=5, creator_fee_bps=5)
    gross_quote = 100_000_000

    normal = _states()
    normal_entry = normal["TOKEN-entry"]
    normal_buy = pumpswap_buy_exact_quote_in(
        gross_quote_amount=gross_quote,
        base_reserve=normal_entry.pre_base_atomic,
        quote_reserve=normal_entry.pre_quote_atomic,
        fees=fees,
    )
    normal["TOKEN-entry"] = replace(
        normal_entry,
        pre_real_base_atomic=normal_buy.base_amount - 1,
        pre_real_quote_atomic=normal_entry.pre_real_quote_atomic,
    )
    with pytest.raises(ExactSurvivorGateError, match="real inventory capacity"):
        quote_position_exact(_signal(), normal, gross_quote_budget_atomic=gross_quote)
    normal["TOKEN-entry"] = replace(
        normal_entry,
        pre_real_base_atomic=normal_buy.base_amount,
        pre_real_quote_atomic=1,
    )
    normal_trade = quote_position_exact(_signal(), normal, gross_quote_budget_atomic=gross_quote)
    normal_exit = normal["TOKEN-exit"]
    normal_sell = pumpswap_sell_exact_base(
        base_amount=normal_trade.entry_base_after_haircut_atomic,
        base_reserve=normal_exit.pre_base_atomic,
        quote_reserve=normal_exit.pre_quote_atomic,
        fees=fees,
    )
    assert normal_sell.curve_quote_amount > normal_sell.user_quote_amount
    normal["TOKEN-exit"] = replace(
        normal_exit,
        pre_real_base_atomic=1,
        pre_real_quote_atomic=normal_sell.curve_quote_amount - 1,
    )
    with pytest.raises(ExactSurvivorGateError, match="real inventory capacity"):
        quote_position_exact(_signal(), normal, gross_quote_budget_atomic=gross_quote)
    normal["TOKEN-exit"] = replace(
        normal_exit,
        pre_real_base_atomic=1,
        pre_real_quote_atomic=normal_sell.curve_quote_amount,
    )
    quote_position_exact(_signal(), normal, gross_quote_budget_atomic=gross_quote)

    flipped = _states(flipped=True)
    flipped_entry = flipped["TOKEN-entry"]
    flipped_sell = pumpswap_sell_exact_base(
        base_amount=gross_quote,
        base_reserve=flipped_entry.pre_quote_atomic,
        quote_reserve=flipped_entry.pre_base_atomic,
        fees=fees,
    )
    assert flipped_sell.curve_quote_amount > flipped_sell.user_quote_amount
    flipped["TOKEN-entry"] = replace(
        flipped_entry,
        pre_real_base_atomic=flipped_sell.user_quote_amount,
        pre_real_quote_atomic=flipped_entry.pre_real_quote_atomic,
    )
    with pytest.raises(ExactSurvivorGateError, match="real inventory capacity"):
        quote_position_exact(_signal(), flipped, gross_quote_budget_atomic=gross_quote)
    flipped["TOKEN-entry"] = replace(
        flipped_entry,
        pre_real_base_atomic=flipped_sell.curve_quote_amount,
        pre_real_quote_atomic=1,
    )
    flipped_trade = quote_position_exact(_signal(), flipped, gross_quote_budget_atomic=gross_quote)
    flipped_exit = flipped["TOKEN-exit"]
    flipped_buy = pumpswap_buy_exact_quote_in(
        gross_quote_amount=flipped_trade.entry_base_after_haircut_atomic,
        base_reserve=flipped_exit.pre_quote_atomic,
        quote_reserve=flipped_exit.pre_base_atomic,
        fees=fees,
    )
    flipped["TOKEN-exit"] = replace(
        flipped_exit,
        pre_real_base_atomic=1,
        pre_real_quote_atomic=flipped_buy.base_amount - 1,
    )
    with pytest.raises(ExactSurvivorGateError, match="real inventory capacity"):
        quote_position_exact(_signal(), flipped, gross_quote_budget_atomic=gross_quote)
    flipped["TOKEN-exit"] = replace(
        flipped_exit,
        pre_real_base_atomic=1,
        pre_real_quote_atomic=flipped_buy.base_amount,
    )
    quote_position_exact(_signal(), flipped, gross_quote_budget_atomic=gross_quote)


def test_exact_quote_rejects_orientation_change_and_zero_exit_output() -> None:
    states = _states()
    states["TOKEN-exit"] = _states(flipped=True)["TOKEN-exit"]
    with pytest.raises(ExactSurvivorGateError, match="orientation"):
        quote_position_exact(_signal(), states, gross_quote_budget_atomic=100_000_000)

    zero_output = _states(exit_quote_reserve=1)
    with pytest.raises(ExactSurvivorGateError, match="no executable quote"):
        quote_position_exact(_signal(), zero_output, gross_quote_budget_atomic=100_000_000)


def test_exact_state_reconciles_fee_mint_with_canonical_fee_asset() -> None:
    wrong_quote_mint = _row("wrong-quote", 1)
    wrong_quote_mint["fee_mint"] = "not-sol"
    with pytest.raises(ExactSurvivorGateError, match="fee mint"):
        AtomicEventState.from_mapping(wrong_quote_mint)

    wrong_base_mint = _row("wrong-base", 1, flipped=True)
    wrong_base_mint["fee_mint"] = "not-the-token"
    with pytest.raises(ExactSurvivorGateError, match="fee mint"):
        AtomicEventState.from_mapping(wrong_base_mint)

    wrapped_sol = _row("wrapped-sol", 1)
    wrapped_sol["fee_mint"] = "So11111111111111111111111111111111111111112"
    AtomicEventState.from_mapping(wrapped_sol)


def test_pump_bonding_curve_uses_virtual_and_real_integer_state() -> None:
    states = _states(phase="bonding_curve", flipped=None)
    trade = quote_position_exact(_signal(), states, gross_quote_budget_atomic=100_000_000)
    assert trade.entry_fee_asset == "quote"
    assert trade.pnl_quote_atomic > 0


def test_exact_replay_fails_closed_on_tamper_gap_and_capacity() -> None:
    states = _states()
    signal = _signal()
    missing = dict(states)
    missing.pop(signal.exit_event_id)
    with pytest.raises(ExactSurvivorGateError, match="missing"):
        quote_position_exact(signal, missing, gross_quote_budget_atomic=100_000_000)

    gapped_row = _row("TOKEN-exit", 101, quote_reserve=200_000_000_000)
    gapped_row["coverage_gap"] = True
    with pytest.raises(ExactSurvivorGateError, match="gapped"):
        AtomicEventState.from_mapping(gapped_row)

    with pytest.raises(ExactSurvivorGateError, match="reserve-fraction"):
        quote_position_exact(signal, states, gross_quote_budget_atomic=1_000_000_000)

    bad_routes = _row("bad", 1, creator_bps=5, cashback_bps=5)
    with pytest.raises(ExactSurvivorGateError, match="both active"):
        AtomicEventState.from_mapping(bad_routes)


def test_fixed_notional_gate_requires_positive_aggregate_exact_pnl() -> None:
    positive = replay_fixed_notional((_signal(),), _states())
    assert positive.profitable
    assert positive.total_pnl_quote_atomic > 0

    losing_states = _states(exit_quote_reserve=50_000_000_000)
    negative = replay_fixed_notional((_signal(),), losing_states)
    assert not negative.profitable
    assert negative.total_pnl_quote_atomic < 0


def test_shared_10usd_portfolio_is_causal_cash_constrained_and_profitable() -> None:
    all_states: dict[str, AtomicEventState] = {}
    signals: list[PositionSignal] = []
    for index in range(6):
        mint = f"TOKEN-{index}"
        start = 10 + index * 2
        all_states.update(
            _states(
                mint=mint,
                start_slot=start,
                base_reserve=100_000_000_000_000_000,
                quote_reserve=10_000_000_000_000,
                exit_quote_reserve=20_000_000_000_000,
            )
        )
        signals.append(_signal(mint=mint, start_slot=start))
    fx = CausalFxSeries(
        (
            FxPoint(1_000_000_000, Decimal("100")),
            FxPoint(1_000_000_000_000, Decimal("100")),
        )
    )
    controls = ExactGateConfig(starting_usd=Decimal("1000"))
    replay = replay_shared_capital(
        signals,
        all_states,
        fx,
        development_start_available_at_ns=1_000_000_000,
        config=controls,
    )
    # Five simultaneous 20% notionals would consume all principal; explicit
    # entry costs therefore make the fifth and sixth signals cash-ineligible.
    assert replay.maximum_concurrent_positions == 4
    assert len(replay.accepted_trades) == 4
    assert [item.reason for item in replay.decisions].count("insufficient_cash") == 2
    assert replay.minimum_equity_quote_atomic >= 0
    assert replay.profitable_in_quote
    assert replay.profitable_in_usd
    assert replay.passed


def test_shared_portfolio_rejects_missing_causal_fx_and_does_not_claim_loss() -> None:
    fx = CausalFxSeries((FxPoint(2_000_000_000, Decimal("100")),))
    with pytest.raises(ExactSurvivorGateError, match="unavailable"):
        replay_shared_capital(
            (_signal(),),
            _states(),
            fx,
            development_start_available_at_ns=1_000_000_000,
        )

    flat_fx = CausalFxSeries(
        (
            FxPoint(1_000_000_000, Decimal("100")),
            FxPoint(1_000_000_000_000, Decimal("100")),
        )
    )
    replay = replay_shared_capital(
        (_signal(),),
        _states(exit_quote_reserve=50_000_000_000),
        flat_fx,
        development_start_available_at_ns=1_000_000_000,
    )
    assert replay.accepted_trades
    assert not replay.profitable_in_quote
    assert not replay.passed
