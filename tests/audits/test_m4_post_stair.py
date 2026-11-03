from __future__ import annotations

from pumpquant.audits.m4.post_stair import (
    distribution,
    replay_fee_skip_schedule,
    summarize_live_execution,
)


def _event(
    *,
    slot: int,
    order: int,
    block_time: int,
    event_name: str = "BuyEvent",
    post_base: int = 1_000_000_000,
    post_quote: int = 1_000_000_000,
    base_amount: int = 1,
    slippage_limit: int = 10_000_000_000,
) -> dict[str, int | str]:
    return {
        "slot": slot,
        "order": order,
        "block_time": block_time,
        "event_name": event_name,
        "base_amount": base_amount,
        "slippage_limit_quote": slippage_limit,
        "post_base_reserves": post_base,
        "post_quote_reserves": post_quote,
        "virtual_quote_reserves": 0,
        "lp_fee_bps": 0,
        "protocol_fee_bps": 0,
        "creator_fee_bps": 0,
    }


def _case() -> dict[str, int | str]:
    return {
        "pool": "pool",
        "mint": "mint",
        "source_slot": 100,
        "decision_time_ns": 1_000_000_000_000,
        "quote_lamports": 10_000_000,
        "estimated_entry_fee_if_submitted_lamports": 10_000,
    }


def test_distribution_uses_conservative_higher_quantiles() -> None:
    result = distribution([1, 2, 3, 4, 100])
    assert result["median"] == 3
    assert result["mean"] == 22
    assert result["p90"] == 100
    assert result["p99"] == 100


def test_fee_skip_replay_can_exhaust_slippage_retries() -> None:
    # The quote reserve is 10x higher by the landing state, so a base amount
    # fixed from the build state cannot be bought within 5% on either attempt.
    path = [
        _event(slot=100, order=0, block_time=1_000, post_quote=1_000_000_000),
        _event(slot=102, order=1, block_time=1_001, post_quote=10_000_000_000),
        _event(slot=105, order=2, block_time=1_002, post_quote=10_000_000_000),
    ]
    result = replay_fee_skip_schedule(_case(), path, [2, 3], quote_to_landing_slots=2)
    assert result["status"] == "no_fill_after_maximum_attempts"
    assert result["submitted_attempts"] == 2
    assert result["failed_entry_fees_lamports"] == 20_000
    assert {row["status"] for row in result["attempts"]} == {"exceeded_slippage"}


def test_fee_skip_replay_inserts_trade_and_replays_external_orders() -> None:
    case = _case()
    path = [
        _event(slot=100, order=0, block_time=1_000),
        _event(slot=102, order=1, block_time=1_001),
        # A large later buy executes against the counterfactual reserves and
        # moves the held base position through its take-profit.
        _event(
            slot=104,
            order=2,
            block_time=1_002,
            base_amount=450_000_000,
        ),
    ]
    result = replay_fee_skip_schedule(case, path, [2], quote_to_landing_slots=2)
    assert result["status"] == "filled"
    assert result["entry_attempt"] == 1
    assert result["external_orders_executed"] == 1
    assert result["external_orders_rejected_by_counterfactual"] == 0
    assert result["exit_reason"] == "take_profit"
    assert result["return_multiple_on_frozen_budget"] >= 1.75


def test_fee_skip_replay_rejects_external_order_that_breaks_its_guard() -> None:
    case = _case()
    path = [
        _event(slot=100, order=0, block_time=1_000),
        _event(slot=102, order=1, block_time=1_001),
        _event(
            slot=104,
            order=2,
            block_time=1_002,
            base_amount=450_000_000,
            slippage_limit=1,
        ),
    ]
    result = replay_fee_skip_schedule(case, path, [2], quote_to_landing_slots=2)
    assert result["status"] == "filled"
    assert result["external_orders_executed"] == 0
    assert result["external_orders_rejected_by_counterfactual"] == 1
    assert result["exit_reason"] == "timeout"
    assert result["return_multiple_on_frozen_budget"] < 1.75


def test_live_economics_use_exact_fee_payer_balance_deltas() -> None:
    rows = [
        {
            "event": "transaction_ready",
            "wall_time_ns": 1,
            "pool": "pool",
            "side": "entry",
            "signature": "entry",
            "localized_p75_micro_lamports": 0,
            "selected_micro_lamports": 50_000,
            "estimated_total_fee_lamports": 10,
        },
        {
            "event": "transaction_landed",
            "wall_time_ns": 2,
            "pool": "pool",
            "side": "entry",
            "signature": "entry",
            "send_to_confirmed_ms": 500,
        },
        {
            "event": "position_opened",
            "wall_time_ns": 3,
            "pool": "pool",
            "entry_signature": "entry",
            "entry_attempt": 1,
        },
        {
            "event": "exit_triggered",
            "wall_time_ns": 4,
            "pool": "pool",
            "reason": "take_profit",
            "executable_quote_lamports": "200",
        },
        {
            "event": "transaction_ready",
            "wall_time_ns": 5,
            "pool": "pool",
            "side": "exit",
            "signature": "exit",
            "localized_p75_micro_lamports": 0,
            "selected_micro_lamports": 50_000,
            "estimated_total_fee_lamports": 10,
        },
        {
            "event": "transaction_landed",
            "wall_time_ns": 6,
            "pool": "pool",
            "side": "exit",
            "signature": "exit",
            "send_to_confirmed_ms": 500,
        },
        {
            "event": "position_closed",
            "wall_time_ns": 7,
            "pool": "pool",
            "reason": "take_profit",
            "exit_signature": "exit",
            "exit_attempt": 1,
        },
    ]
    transactions = {
        "entry": {
            "err": None,
            "fee_lamports": 10,
            "compute_units_consumed": 1,
            "fee_payer_balance_delta_lamports": -150,
            "trade_events": [
                {
                    "event_name": "BuyEvent",
                    "pool": "pool",
                    "user_quote_amount": 100,
                }
            ],
        },
        "exit": {
            "err": None,
            "fee_lamports": 10,
            "compute_units_consumed": 1,
            "fee_payer_balance_delta_lamports": 250,
            "trade_events": [
                {
                    "event_name": "SellEvent",
                    "pool": "pool",
                    "user_quote_amount": 200,
                }
            ],
        },
    }
    summary = summarize_live_execution(rows, transactions)
    record = summary["trade_economics"]["records"][0]
    assert record["economic_pnl_lamports"] == 100
    assert record["event_quote_pnl_after_transaction_fees_lamports"] == 80
    assert (
        summary["trade_economics"]["all_submitted_transaction_balance_delta_lamports"]["total"]
        == 100
    )
