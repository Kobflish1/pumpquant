from __future__ import annotations

import unittest
from dataclasses import replace

from pumpquant.core.schema import Bar, Side, VenuePhase
from pumpquant.execution.bar_replay import (
    ExecutionConfig,
    MomentumSpec,
    constant_product_buy,
    constant_product_sell,
    prepare_backtest_bars,
    run_backtest,
    run_prepared_backtest,
)


def make_bar(index: int, price: float, *, age: float | None = None) -> Bar:
    quote_reserve = 100.0
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
        base_volume=1_000.0,
        quote_volume=100.0,
        buy_quote_volume=80.0,
        sell_quote_volume=20.0,
        trade_count=10,
        unique_traders=8,
        virtual_base_reserve=quote_reserve / price,
        virtual_quote_reserve=quote_reserve,
        token_age_seconds=float(index + 1) if age is None else age,
        real_base_reserve=1_000_000.0,
        real_quote_reserve=100.0,
    )


class FillTests(unittest.TestCase):
    def test_prepared_features_match_direct_replay_and_enforce_lookbacks(self) -> None:
        bars = [make_bar(index, price) for index, price in enumerate([0.1, 0.12, 0.15])]
        spec = MomentumSpec(
            momentum_lookback=1,
            volume_lookback=2,
            min_liquidity_quote=0.0,
            min_trade_count=1,
            min_age_seconds=0.0,
            max_age_seconds=100.0,
        )
        config = ExecutionConfig()
        prepared = prepare_backtest_bars(
            bars,
            momentum_lookback=spec.momentum_lookback,
            volume_lookback=spec.volume_lookback,
        )
        self.assertEqual(
            run_prepared_backtest(prepared, spec, config),
            run_backtest(bars, spec, config),
        )
        with self.assertRaisesRegex(ValueError, "lookbacks"):
            run_prepared_backtest(
                prepared,
                replace(spec, momentum_lookback=2),
                config,
            )

    def test_curve_fill_has_fees_and_directional_impact(self) -> None:
        bar = make_bar(0, 0.1)
        config = ExecutionConfig(
            quote_notional=1.0,
            venue_fee_bps=100.0,
            adverse_selection_bps=0.0,
            fixed_network_cost_quote=0.0,
            priority_cost_quote=0.0,
            max_reserve_fraction=0.1,
            max_bar_volume_fraction=0.1,
        )
        buy = constant_product_buy(bar, config)
        self.assertIsNotNone(buy)
        assert buy is not None
        self.assertGreater(buy.average_price, buy.mark_price)
        sell = constant_product_sell(bar, buy.base_amount, config)
        self.assertIsNotNone(sell)
        assert sell is not None
        self.assertLess(sell.average_price, sell.mark_price)

    def test_capacity_limit_rejects_counterfactual_large_order(self) -> None:
        bar = make_bar(0, 0.1)
        config = ExecutionConfig(quote_notional=10.0, max_reserve_fraction=0.001)
        self.assertIsNone(constant_product_buy(bar, config))

    def test_observed_historical_fee_overrides_fallback(self) -> None:
        bar = replace(make_bar(0, 0.1), venue_fee_bps=500.0)
        observed = constant_product_buy(
            bar,
            ExecutionConfig(
                quote_notional=0.1,
                venue_fee_bps=0.0,
                adverse_selection_bps=0.0,
                max_reserve_fraction=0.1,
                max_bar_volume_fraction=0.1,
            ),
        )
        fallback = constant_product_buy(
            bar,
            ExecutionConfig(
                quote_notional=0.1,
                venue_fee_bps=0.0,
                adverse_selection_bps=0.0,
                max_reserve_fraction=0.1,
                max_bar_volume_fraction=0.1,
                use_observed_venue_fee=False,
            ),
        )
        assert observed is not None and fallback is not None
        self.assertLess(observed.base_amount, fallback.base_amount)

    def test_bonding_curve_uses_pump_fee_ordering(self) -> None:
        bar = make_bar(0, 0.1)
        config = ExecutionConfig(
            quote_notional=1.0,
            venue_fee_bps=100.0,
            adverse_selection_bps=0.0,
            fixed_network_cost_quote=0.0,
            priority_cost_quote=0.0,
            max_reserve_fraction=0.1,
            max_bar_volume_fraction=0.1,
        )
        buy = constant_product_buy(bar, config)
        assert buy is not None
        curve_quote = 1.0 / 1.01
        invariant = bar.virtual_base_reserve * bar.virtual_quote_reserve
        expected_base = bar.virtual_base_reserve - invariant / (
            bar.virtual_quote_reserve + curve_quote
        )
        self.assertAlmostEqual(buy.base_amount, expected_base)
        self.assertAlmostEqual(buy.venue_fee_quote, 1.0 - curve_quote)

        sell = constant_product_sell(bar, buy.base_amount, config)
        assert sell is not None
        curve_output = bar.virtual_quote_reserve - invariant / (
            bar.virtual_base_reserve + buy.base_amount
        )
        self.assertAlmostEqual(sell.venue_fee_quote, curve_output * 0.01)
        self.assertAlmostEqual(sell.quote_amount, curve_output * 0.99)
        self.assertIn("pump_fee_order", sell.model)
        inventory_between_gross_and_net = replace(bar, real_quote_reserve=curve_output * 0.995)
        self.assertIsNone(
            constant_product_sell(inventory_between_gross_and_net, buy.base_amount, config)
        )


class ReplayTests(unittest.TestCase):
    def test_signal_is_filled_only_after_configured_latency(self) -> None:
        bars = [make_bar(i, price) for i, price in enumerate([0.10, 0.11, 0.13, 0.16, 0.20, 0.18])]
        spec = MomentumSpec(
            momentum_lookback=2,
            volume_lookback=2,
            entry_momentum=0.10,
            entry_buy_imbalance=0.1,
            min_liquidity_quote=0.0,
            min_trade_count=1,
            min_age_seconds=0.0,
            max_age_seconds=100.0,
            exit_momentum=-1.0,
            take_profit=0.10,
            stop_loss=0.9,
            trailing_stop=0.9,
            max_holding_bars=10,
        )
        config = ExecutionConfig(
            quote_notional=0.1,
            venue_fee_bps=0.0,
            adverse_selection_bps=0.0,
            fixed_network_cost_quote=0.0,
            priority_cost_quote=0.0,
            latency_bars=1,
            max_reserve_fraction=0.1,
            max_bar_volume_fraction=0.1,
        )
        result = run_backtest(bars, spec, config)
        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        # Momentum becomes known at bar index 2; fill is bar index 3.
        self.assertEqual(trade.entry_signal_time_ns, bars[2].end_time_ns)
        self.assertEqual(trade.entry.time_ns, bars[3].end_time_ns)
        self.assertEqual(trade.entry.side, Side.BUY)
        self.assertEqual(trade.exit.side, Side.SELL)

    def test_unknown_launch_age_cannot_trigger_entry(self) -> None:
        bars = [make_bar(i, 0.1 + i * 0.02, age=None) for i in range(6)]
        bars = [replace(bar, token_age_seconds=None) for bar in bars]
        result = run_backtest(
            bars,
            MomentumSpec(
                momentum_lookback=1,
                volume_lookback=2,
                entry_momentum=0.0,
                min_liquidity_quote=0.0,
                min_trade_count=1,
            ),
            ExecutionConfig(max_bar_volume_fraction=0.1),
        )
        self.assertEqual(result.trades, ())

    def test_landed_failed_exits_pay_cost_and_stay_unclosed(self) -> None:
        bars = [
            replace(make_bar(i, price), real_quote_reserve=0.0)
            for i, price in enumerate([0.10, 0.11, 0.13, 0.16, 0.20, 0.18])
        ]
        spec = MomentumSpec(
            momentum_lookback=2,
            volume_lookback=2,
            entry_momentum=0.1,
            entry_buy_imbalance=0.1,
            min_liquidity_quote=0.0,
            min_trade_count=1,
            min_age_seconds=0.0,
            max_age_seconds=100.0,
            exit_momentum=-1.0,
            take_profit=0.1,
            stop_loss=0.9,
            trailing_stop=0.9,
            max_holding_bars=10,
        )
        config = ExecutionConfig(
            quote_notional=0.1,
            venue_fee_bps=0.0,
            adverse_selection_bps=0.0,
            fixed_network_cost_quote=0.001,
            priority_cost_quote=0.0,
            max_reserve_fraction=0.1,
            max_bar_volume_fraction=0.1,
        )
        result = run_backtest(bars, spec, config)
        self.assertGreater(result.rejected_orders, 0)
        self.assertGreater(result.failed_order_cost_quote, 0.0)
        self.assertEqual(result.unclosed_positions, 1)
        self.assertTrue(all(value < 0.0 for value in result.returns))
        self.assertAlmostEqual(result.total_quote_pnl, -0.102)
        self.assertAlmostEqual(result.attempted_capital_quote, 0.101)

    def test_same_timestamp_phase_row_cannot_satisfy_latency(self) -> None:
        bars = [make_bar(0, 0.10), make_bar(1, 0.13)]
        same_time_next_phase = replace(
            bars[-1],
            phase=VenuePhase.PUMPSWAP,
            virtual_base_reserve=100.0 / 0.14,
            close=0.14,
            open=0.14,
            high=0.14,
            low=0.14,
        )
        bars.extend([same_time_next_phase, make_bar(2, 0.17), make_bar(3, 0.20)])
        result = run_backtest(
            bars,
            MomentumSpec(
                momentum_lookback=1,
                volume_lookback=2,
                entry_momentum=0.1,
                entry_buy_imbalance=0.1,
                min_liquidity_quote=0.0,
                min_trade_count=1,
                min_age_seconds=0.0,
                max_age_seconds=100.0,
                exit_momentum=-1.0,
                take_profit=10.0,
                stop_loss=0.9,
                trailing_stop=0.9,
            ),
            ExecutionConfig(
                quote_notional=0.1,
                venue_fee_bps=0.0,
                adverse_selection_bps=0.0,
                fixed_network_cost_quote=0.0,
                priority_cost_quote=0.0,
                max_reserve_fraction=0.1,
                max_bar_volume_fraction=0.1,
            ),
        )
        self.assertEqual(result.position_outcomes, 1)
        entry = result.open_positions[0].entry
        self.assertGreater(entry.time_ns, bars[1].end_time_ns)

    def test_delayed_snapshot_cannot_fill_against_past_chain_state(self) -> None:
        bars = [
            make_bar(0, 0.10),
            replace(make_bar(1, 0.13), available_at_ns=10_000_000_000),
            replace(make_bar(2, 0.14), available_at_ns=11_000_000_000),
            replace(make_bar(11, 0.15), available_at_ns=12_000_000_000),
        ]
        result = run_backtest(
            bars,
            MomentumSpec(
                momentum_lookback=1,
                volume_lookback=2,
                entry_momentum=0.1,
                entry_buy_imbalance=0.1,
                min_liquidity_quote=0.0,
                min_trade_count=1,
                min_age_seconds=0.0,
                max_age_seconds=100.0,
                exit_momentum=-1.0,
                take_profit=10.0,
                stop_loss=0.9,
                trailing_stop=0.9,
            ),
            ExecutionConfig(
                quote_notional=0.1,
                venue_fee_bps=0.0,
                adverse_selection_bps=0.0,
                fixed_network_cost_quote=0.0,
                priority_cost_quote=0.0,
                max_reserve_fraction=0.1,
                max_bar_volume_fraction=0.1,
            ),
        )
        self.assertEqual(result.position_outcomes, 1)
        position = result.open_positions[0]
        self.assertEqual(position.entry_signal_time_ns, 10_000_000_000)
        self.assertEqual(position.entry.time_ns, 12_000_000_000)
        self.assertGreaterEqual(
            position.entry.time_ns, position.entry_signal_time_ns + 1_000_000_000
        )

    def test_failed_entries_have_nonzero_attempted_capital_return(self) -> None:
        bars = [make_bar(i, 0.10 + i * 0.02) for i in range(6)]
        result = run_backtest(
            bars,
            MomentumSpec(
                momentum_lookback=1,
                volume_lookback=2,
                entry_momentum=0.0,
                entry_buy_imbalance=0.0,
                min_liquidity_quote=0.0,
                min_trade_count=1,
                min_age_seconds=0.0,
                max_age_seconds=100.0,
            ),
            ExecutionConfig(
                quote_notional=1.0,
                fixed_network_cost_quote=0.001,
                priority_cost_quote=0.0,
                max_reserve_fraction=0.0001,
                max_bar_volume_fraction=0.1,
            ),
        )
        self.assertGreater(result.rejected_orders, 0)
        expected = result.rejected_orders * 1.001
        self.assertAlmostEqual(result.attempted_capital_quote, expected)
        self.assertLess(result.return_on_attempted_capital, 0.0)

    def test_non_landed_entries_count_their_attempted_budget(self) -> None:
        bars = [make_bar(i, 0.10 + i * 0.02) for i in range(6)]
        config = ExecutionConfig(
            quote_notional=0.25,
            landing_probability=0.0,
            fixed_network_cost_quote=0.001,
            priority_cost_quote=0.0,
            max_reserve_fraction=0.1,
            max_bar_volume_fraction=0.1,
        )
        result = run_backtest(
            bars,
            MomentumSpec(
                momentum_lookback=1,
                volume_lookback=2,
                entry_momentum=0.0,
                entry_buy_imbalance=0.0,
                min_liquidity_quote=0.0,
                min_trade_count=1,
                min_age_seconds=0.0,
                max_age_seconds=100.0,
            ),
            config,
        )
        self.assertGreater(result.missed_entry_orders, 0)
        self.assertEqual(result.missed_exit_orders, 0)
        self.assertEqual(result.missed_orders, result.missed_entry_orders)
        self.assertAlmostEqual(
            result.attempted_capital_quote,
            result.missed_entry_orders * config.quote_notional,
        )
        self.assertEqual(result.total_quote_pnl, 0.0)
        self.assertEqual(result.return_on_attempted_capital, 0.0)

    def test_mixed_quote_currencies_and_clob_are_rejected(self) -> None:
        sol_bar = make_bar(0, 0.1)
        usdc_bar = replace(
            make_bar(1, 0.11),
            market_id="TOKEN-USDC",
            mint="TOKEN-USDC",
            quote_mint="USDC",
        )
        with self.assertRaisesRegex(ValueError, "mix quote currencies"):
            run_backtest([sol_bar, usdc_bar], MomentumSpec(), ExecutionConfig())
        with self.assertRaisesRegex(ValueError, "order-book"):
            run_backtest(
                [replace(sol_bar, phase=VenuePhase.CLOB)],
                MomentumSpec(),
                ExecutionConfig(),
            )


if __name__ == "__main__":
    unittest.main()
