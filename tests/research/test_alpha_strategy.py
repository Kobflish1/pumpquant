from __future__ import annotations

import unittest
from dataclasses import replace

from pumpquant.core.schema import Bar, VenuePhase
from pumpquant.execution.bar_replay import ExecutionConfig
from pumpquant.features.alpha import AlphaFeatureConfig
from pumpquant.research.alpha_strategy import (
    AlphaStrategySpec,
    RuleOperator,
    SignalRule,
    alpha_spec_sha256,
    prepare_alpha_bars,
    run_alpha_backtest,
    run_prepared_alpha_backtest,
    signal_capacity_multiple,
)


def make_bar(index: int, price: float, *, buy_fraction: float = 0.8) -> Bar:
    quote_reserve = 100.0
    quote_volume = 100.0
    virtual_base = quote_reserve / price
    return Bar(
        market_id="TOKEN-CURVE",
        mint="TOKEN",
        quote_mint="SOL",
        start_time_ns=index * 1_000_000_000,
        end_time_ns=(index + 1) * 1_000_000_000,
        phase=VenuePhase.BONDING_CURVE,
        open=price,
        high=price,
        low=price,
        close=price,
        base_volume=quote_volume / price,
        quote_volume=quote_volume,
        buy_quote_volume=quote_volume * buy_fraction,
        sell_quote_volume=quote_volume * (1.0 - buy_fraction),
        trade_count=10,
        unique_traders=8,
        virtual_base_reserve=virtual_base,
        virtual_quote_reserve=quote_reserve,
        token_age_seconds=float(index + 1),
        real_base_reserve=virtual_base * 0.8,
        real_quote_reserve=100.0,
        venue_fee_bps=0.0,
    )


class AlphaStrategyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.feature_config = AlphaFeatureConfig(
            short_window=1,
            medium_window=2,
            long_window=3,
            opening_range_bars=2,
            vpin_window=2,
        )
        self.spec = AlphaStrategySpec(
            name="signed-flow-continuation",
            feature_config=self.feature_config,
            entry_rules=(
                SignalRule("signed_flow_short", RuleOperator.GREATER_EQUAL, 0.5),
                SignalRule("participant_ratio", RuleOperator.GREATER_EQUAL, 0.5),
            ),
            exit_rule=SignalRule("signed_flow_short", RuleOperator.LESS_EQUAL, 0.0),
            min_liquidity_quote=0.0,
            min_trade_count=1,
            min_age_seconds=0.0,
            max_age_seconds=100.0,
            take_profit=0.10,
            stop_loss=0.50,
            trailing_stop=0.50,
            max_holding_bars=10,
        )
        self.execution = ExecutionConfig(
            quote_notional=0.1,
            venue_fee_bps=0.0,
            adverse_selection_bps=0.0,
            fixed_network_cost_quote=0.0,
            priority_cost_quote=0.0,
            latency_bars=1,
            max_reserve_fraction=0.1,
            max_bar_volume_fraction=0.1,
        )

    def test_combined_rule_replay_waits_for_strictly_later_fill(self) -> None:
        bars = [
            make_bar(index, price) for index, price in enumerate([0.10, 0.12, 0.16, 0.20, 0.22])
        ]
        result = run_alpha_backtest(bars, replace(self.spec, max_age_seconds=3.5), self.execution)
        self.assertEqual(result.position_outcomes, 1)
        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.entry_signal_time_ns, bars[0].end_time_ns)
        self.assertEqual(trade.entry.time_ns, bars[1].end_time_ns)
        self.assertGreater(trade.exit.time_ns, trade.exit_signal_time_ns)
        self.assertGreater(result.total_quote_pnl, 0.0)

    def test_failed_conjunct_prevents_entry(self) -> None:
        bars = [make_bar(index, 0.1 + index * 0.02) for index in range(5)]
        impossible = replace(
            self.spec,
            entry_rules=self.spec.entry_rules
            + (SignalRule("participant_ratio", RuleOperator.GREATER_EQUAL, 0.95),),
        )
        result = run_alpha_backtest(bars, impossible, self.execution)
        self.assertEqual(result.position_outcomes, 0)
        self.assertEqual(result.rejected_orders, 0)

    def test_signal_time_capacity_gate_is_causal_but_not_a_fill_promise(self) -> None:
        bars = [make_bar(index, 0.1 + index * 0.02) for index in range(5)]
        self.assertAlmostEqual(signal_capacity_multiple(bars[0], self.execution), 100.0)
        gated = replace(self.spec, min_signal_capacity_multiple=100.1)
        result = run_alpha_backtest(bars, gated, self.execution)
        self.assertEqual(result.position_outcomes, 0)
        self.assertEqual(result.rejected_orders, 0)

    def test_feature_exit_is_execution_delayed(self) -> None:
        bars = [
            make_bar(0, 0.10),
            make_bar(1, 0.11),
            make_bar(2, 0.12, buy_fraction=0.2),
            make_bar(3, 0.09, buy_fraction=0.2),
        ]
        spec = replace(
            self.spec,
            take_profit=10.0,
            stop_loss=0.9,
            trailing_stop=0.9,
        )
        result = run_alpha_backtest(bars, spec, self.execution)
        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertTrue(trade.exit_reason.startswith("feature_exit:signed_flow_short"))
        self.assertEqual(trade.exit_signal_time_ns, bars[2].end_time_ns)
        self.assertEqual(trade.exit.time_ns, bars[3].end_time_ns)

    def test_prepared_rows_are_reusable_and_config_mismatch_is_rejected(self) -> None:
        bars = [make_bar(index, 0.1 + index * 0.02) for index in range(5)]
        prepared = prepare_alpha_bars(bars, self.feature_config)
        self.assertEqual(
            run_prepared_alpha_backtest(prepared, self.spec, self.execution),
            run_alpha_backtest(bars, self.spec, self.execution),
        )
        mismatch = replace(
            self.spec,
            feature_config=AlphaFeatureConfig(
                short_window=2,
                medium_window=3,
                long_window=4,
                opening_range_bars=2,
                vpin_window=2,
            ),
        )
        with self.assertRaisesRegex(ValueError, "does not match"):
            run_prepared_alpha_backtest(prepared, mismatch, self.execution)

    def test_rule_validation_and_hash_are_deterministic(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown alpha feature"):
            SignalRule("native_l2_microprice", RuleOperator.GREATER_EQUAL, 0.0)
        with self.assertRaisesRegex(ValueError, "absolute"):
            SignalRule("return_1", RuleOperator.ABS_GREATER_EQUAL, -1.0)
        self.assertEqual(alpha_spec_sha256(self.spec), alpha_spec_sha256(self.spec))
        self.assertNotEqual(
            alpha_spec_sha256(self.spec),
            alpha_spec_sha256(replace(self.spec, take_profit=0.2)),
        )


if __name__ == "__main__":
    unittest.main()
