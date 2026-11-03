from dataclasses import dataclass

import pytest

from pumpquant.execution.shared_wallet import (
    DynamicTradeQuote,
    replay_dynamic_shared_wallet,
)


@dataclass(frozen=True)
class _Opportunity:
    mint: str
    signal_time_ns: int
    signal_fx_usd: float = 100.0


def test_replay_settles_positions_and_resizes_from_actual_equity() -> None:
    budgets: list[int] = []

    def quote(opportunity: _Opportunity, budget: int, order_usd: float) -> DynamicTradeQuote:
        budgets.append(budget)
        return DynamicTradeQuote(
            entered=True,
            closed=True,
            reason="take_profit",
            entry_time_ns=opportunity.signal_time_ns + 1,
            exit_time_ns=opportunity.signal_time_ns + 5,
            entry_spend_lamports=budget,
            entry_cost_lamports=0,
            exit_credit_lamports=budget * 2,
            exit_cost_lamports=0,
        )

    result = replay_dynamic_shared_wallet(
        [_Opportunity("a", 10), _Opportunity("b", 20)],
        starting_usd=10.0,
        wallet_fraction=0.10,
        absolute_notional_cap_usd=100.0,
        max_concurrent_positions=5,
        quote_trade=quote,
    )

    assert budgets == [10_000_000, 11_000_000]
    assert result["accepted_trades"] == 2
    assert result["final_cash_lamports"] == 121_000_000
    assert result["maximum_concurrent_positions"] == 1


def test_replay_enforces_shared_cash_and_concurrency_before_quoting() -> None:
    quote_calls = 0

    def quote(opportunity: _Opportunity, budget: int, order_usd: float) -> DynamicTradeQuote:
        nonlocal quote_calls
        quote_calls += 1
        return DynamicTradeQuote(
            entered=True,
            closed=True,
            reason="timeout",
            entry_time_ns=opportunity.signal_time_ns,
            exit_time_ns=100,
            entry_spend_lamports=budget,
            entry_cost_lamports=0,
            exit_credit_lamports=budget,
            exit_cost_lamports=0,
        )

    result = replay_dynamic_shared_wallet(
        [_Opportunity("a", 1), _Opportunity("b", 2), _Opportunity("c", 3)],
        starting_usd=10.0,
        wallet_fraction=0.20,
        absolute_notional_cap_usd=100.0,
        max_concurrent_positions=1,
        quote_trade=quote,
    )

    assert quote_calls == 1
    assert result["accepted_trades"] == 1
    assert result["rejected_signals"] == 2
    assert result["reason_counts"]["max_concurrent_positions"] == 2


def test_non_entered_paid_send_debits_cash_without_opening_position() -> None:
    def quote(opportunity: _Opportunity, budget: int, order_usd: float) -> DynamicTradeQuote:
        return DynamicTradeQuote(
            entered=False,
            closed=False,
            reason="entry_slippage_rejected",
            entry_time_ns=opportunity.signal_time_ns + 1,
            exit_time_ns=None,
            entry_spend_lamports=0,
            entry_cost_lamports=55_000,
            exit_credit_lamports=0,
            exit_cost_lamports=0,
        )

    result = replay_dynamic_shared_wallet(
        [_Opportunity("a", 1)],
        starting_usd=10.0,
        wallet_fraction=0.036,
        absolute_notional_cap_usd=0.38,
        max_concurrent_positions=5,
        quote_trade=quote,
    )

    assert result["accepted_trades"] == 0
    assert result["final_cash_lamports"] == 99_945_000
    assert result["reason_counts"] == {"entry_slippage_rejected": 1}


def test_exact_starting_sol_balance_overrides_historical_fx_conversion() -> None:
    result = replay_dynamic_shared_wallet(
        [_Opportunity("a", 1)],
        starting_usd=2.53,
        starting_quote_lamports=34_190_000,
        wallet_fraction=0.036,
        absolute_notional_cap_usd=0.38,
        max_concurrent_positions=5,
        quote_trade=lambda opportunity, budget, order_usd: DynamicTradeQuote(
            entered=False,
            closed=False,
            reason="no_entry_state",
            entry_time_ns=None,
            exit_time_ns=None,
            entry_spend_lamports=0,
            entry_cost_lamports=0,
            exit_credit_lamports=0,
            exit_cost_lamports=0,
        ),
    )

    assert result["starting_cash_lamports"] == 34_190_000
    assert result["final_cash_lamports"] == 34_190_000


def test_token_account_rent_is_locked_then_reclaimed_after_close_cost() -> None:
    def quote(opportunity: _Opportunity, budget: int, order_usd: float) -> DynamicTradeQuote:
        return DynamicTradeQuote(
            entered=True,
            closed=True,
            reason="timeout",
            entry_time_ns=2,
            exit_time_ns=3,
            entry_spend_lamports=1_000_000,
            entry_cost_lamports=55_000,
            exit_credit_lamports=1_100_000,
            exit_cost_lamports=110_000,
            token_account_rent_lamports=2_074_080,
            token_account_rent_reclaimed=True,
        )

    result = replay_dynamic_shared_wallet(
        [_Opportunity("a", 1)],
        starting_usd=2.53,
        starting_quote_lamports=34_190_000,
        wallet_fraction=0.036,
        absolute_notional_cap_usd=0.38,
        max_concurrent_positions=5,
        quote_trade=quote,
    )

    assert result["final_cash_lamports"] == 34_125_000
    decision = result["decisions"][0]
    assert decision["cash_after_lamports"] == 31_060_920


def test_unreclaimed_token_account_rent_remains_a_loss() -> None:
    def quote(opportunity: _Opportunity, budget: int, order_usd: float) -> DynamicTradeQuote:
        return DynamicTradeQuote(
            entered=True,
            closed=False,
            reason="exit_unfilled",
            entry_time_ns=2,
            exit_time_ns=3,
            entry_spend_lamports=1_000_000,
            entry_cost_lamports=55_000,
            exit_credit_lamports=0,
            exit_cost_lamports=55_000,
            token_account_rent_lamports=2_074_080,
            token_account_rent_reclaimed=False,
        )

    result = replay_dynamic_shared_wallet(
        [_Opportunity("a", 1)],
        starting_usd=2.53,
        starting_quote_lamports=34_190_000,
        wallet_fraction=0.036,
        absolute_notional_cap_usd=0.38,
        max_concurrent_positions=5,
        quote_trade=quote,
    )

    assert result["final_cash_lamports"] == 31_060_920


def test_fixed_wsol_rent_is_reserved_and_recovered_after_cleanup_cost() -> None:
    observed_budget: list[int] = []

    def quote(opportunity: _Opportunity, budget: int, order_usd: float) -> DynamicTradeQuote:
        observed_budget.append(budget)
        return DynamicTradeQuote(
            entered=False,
            closed=False,
            reason="no_entry_state",
            entry_time_ns=None,
            exit_time_ns=None,
            entry_spend_lamports=0,
            entry_cost_lamports=0,
            exit_credit_lamports=0,
            exit_cost_lamports=0,
        )

    result = replay_dynamic_shared_wallet(
        [_Opportunity("a", 1)],
        starting_usd=2.53,
        starting_quote_lamports=34_190_000,
        fixed_recoverable_rent_lamports=2_039_280,
        fixed_rent_close_cost_lamports=105_000,
        wallet_fraction=0.036,
        absolute_notional_cap_usd=0.38,
        max_concurrent_positions=5,
        quote_trade=quote,
    )

    assert observed_budget == [1_230_839]
    assert result["starting_cash_lamports"] == 32_150_720
    assert result["final_cash_lamports"] == 34_085_000
    assert result["quote_return"] == pytest.approx(-105_000 / 34_190_000)
