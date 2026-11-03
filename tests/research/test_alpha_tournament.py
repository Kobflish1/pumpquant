from __future__ import annotations

import unittest
from dataclasses import replace

from pumpquant.core.schema import Bar, VenuePhase
from pumpquant.execution.bar_replay import ExecutionConfig
from pumpquant.features.alpha import AlphaFeatureConfig
from pumpquant.research.alpha_strategy import AlphaStrategySpec, RuleOperator, SignalRule
from pumpquant.research.alpha_tournament import (
    DEFAULT_EXIT_PROFILES,
    DEFAULT_FEATURE_CONFIGS,
    DEFAULT_RISK_GATES,
    DEFAULT_SIGNAL_AXES,
    alpha_walk_forward_tournament,
    capacity_gate_variants,
    default_single_signal_specs,
    pairwise_survivor_combinations,
    risk_gate_variants,
)


def token_bars(token_index: int) -> list[Bar]:
    launch_ns = (token_index + 1) * 100_000_000_000
    mint = f"TOKEN-{token_index}"
    prices = (0.10, 0.12, 0.15, 0.18, 0.21)
    bars: list[Bar] = []
    for index, price in enumerate(prices):
        quote_reserve = 100.0
        quote_volume = 100.0
        virtual_base = quote_reserve / price
        bars.append(
            Bar(
                market_id=f"{mint}-CURVE",
                mint=mint,
                quote_mint="SOL",
                start_time_ns=launch_ns + index * 1_000_000_000,
                end_time_ns=launch_ns + (index + 1) * 1_000_000_000,
                phase=VenuePhase.BONDING_CURVE,
                open=price,
                high=price,
                low=price,
                close=price,
                base_volume=quote_volume / price,
                quote_volume=quote_volume,
                buy_quote_volume=80.0,
                sell_quote_volume=20.0,
                trade_count=10,
                unique_traders=8,
                virtual_base_reserve=virtual_base,
                virtual_quote_reserve=quote_reserve,
                token_age_seconds=float(index + 1),
                real_base_reserve=virtual_base * 0.8,
                real_quote_reserve=100.0,
                venue_fee_bps=0.0,
            )
        )
    return bars


def base_spec(name: str, rule: SignalRule) -> AlphaStrategySpec:
    return AlphaStrategySpec(
        name=name,
        feature_config=AlphaFeatureConfig(
            short_window=1,
            medium_window=2,
            long_window=3,
            opening_range_bars=2,
            vpin_window=2,
        ),
        entry_rules=(rule,),
        min_liquidity_quote=0.0,
        min_trade_count=1,
        min_age_seconds=0.0,
        max_age_seconds=100.0,
        take_profit=0.10,
        stop_loss=0.50,
        trailing_stop=0.50,
        max_holding_bars=10,
    )


class AlphaTournamentTests(unittest.TestCase):
    def test_default_grid_is_bounded_ranked_and_single_signal(self) -> None:
        specs = default_single_signal_specs()
        self.assertEqual(len(DEFAULT_SIGNAL_AXES), 10)
        self.assertEqual(len(specs), 360)
        self.assertTrue(all(len(spec.entry_rules) == 1 for spec in specs))
        self.assertEqual(
            [axis.rank for axis in sorted(DEFAULT_SIGNAL_AXES, key=lambda item: item.rank)],
            sorted(axis.rank for axis in DEFAULT_SIGNAL_AXES),
        )
        capacity_specs = default_single_signal_specs(
            feature_configs=(DEFAULT_FEATURE_CONFIGS[0],),
            exit_profiles=(DEFAULT_EXIT_PROFILES[0],),
            signal_axes=(DEFAULT_SIGNAL_AXES[0],),
            capacity_multiples=(0.5, 1.0),
        )
        self.assertEqual(len(capacity_specs), 8)
        with self.assertRaisesRegex(ValueError, "capacity_multiples"):
            default_single_signal_specs(capacity_multiples=(-1.0,))

    def test_risk_measurements_only_enter_as_survivor_gates(self) -> None:
        survivor = base_spec(
            "survivor",
            SignalRule("signed_flow_short", RuleOperator.GREATER_EQUAL, 0.2),
        )
        variants = risk_gate_variants(survivor)
        self.assertEqual(len(variants), sum(len(gate.thresholds) for gate in DEFAULT_RISK_GATES))
        self.assertTrue(all(len(spec.entry_rules) == 2 for spec in variants))
        self.assertTrue(all(spec.name.startswith("gate:") for spec in variants))
        capacity_variants = capacity_gate_variants(survivor)
        self.assertEqual(
            [spec.min_signal_capacity_multiple for spec in capacity_variants],
            [0.5, 1.0, 2.0, 5.0],
        )
        with self.assertRaisesRegex(ValueError, "capacity multiples"):
            capacity_gate_variants(survivor, (-1.0,))

    def test_pairwise_combinations_require_compatible_distinct_features(self) -> None:
        flow = base_spec(
            "flow",
            SignalRule("signed_flow_short", RuleOperator.GREATER_EQUAL, 0.2),
        )
        intensity = base_spec(
            "intensity",
            SignalRule("intensity_acceleration", RuleOperator.GREATER_EQUAL, 0.5),
        )
        duplicate_feature = base_spec(
            "flow-stricter",
            SignalRule("signed_flow_short", RuleOperator.GREATER_EQUAL, 0.4),
        )
        incompatible = replace(intensity, name="other-exit", take_profit=0.2)
        pairs = pairwise_survivor_combinations((flow, intensity, duplicate_feature, incompatible))
        self.assertEqual(len(pairs), 2)
        self.assertTrue(all(len(spec.entry_rules) == 2 for spec in pairs))
        self.assertTrue(
            all(len({rule.feature for rule in spec.entry_rules}) == 2 for spec in pairs)
        )

    def test_walk_forward_selects_on_past_and_counts_every_trial(self) -> None:
        bars = [bar for token_index in range(6) for bar in token_bars(token_index)]
        viable = base_spec(
            "viable",
            SignalRule("signed_flow_current", RuleOperator.GREATER_EQUAL, 0.5),
        )
        impossible = base_spec(
            "impossible",
            SignalRule("signed_flow_current", RuleOperator.GREATER_EQUAL, 0.99),
        )
        execution = ExecutionConfig(
            quote_notional=0.1,
            venue_fee_bps=0.0,
            adverse_selection_bps=0.0,
            fixed_network_cost_quote=0.0,
            priority_cost_quote=0.0,
            max_reserve_fraction=0.1,
            max_bar_volume_fraction=0.1,
        )
        completed_folds: list[int] = []
        report = alpha_walk_forward_tournament(
            bars,
            (viable, impossible),
            execution,
            min_train_tokens=2,
            test_tokens=1,
            minimum_train_positions=1,
            on_fold=lambda fold: completed_folds.append(fold.number),
        )
        self.assertEqual(report.candidate_count, 2)
        self.assertEqual(len(report.folds), 4)
        self.assertEqual(report.trial_count, 8)
        self.assertEqual(completed_folds, [0, 1, 2, 3])
        self.assertTrue(all(fold.selected is not None for fold in report.folds))
        self.assertTrue(
            all(fold.selected and fold.selected.spec.name == "viable" for fold in report.folds)
        )
        self.assertGreater(report.outcome_diagnostics.observations, 0)
        for fold in report.folds:
            self.assertTrue(set(fold.train_mints).isdisjoint(fold.test_mints))

        rolling_report = alpha_walk_forward_tournament(
            bars,
            (viable,),
            execution,
            min_train_tokens=2,
            max_train_tokens=2,
            test_tokens=1,
            minimum_train_positions=1,
        )
        self.assertTrue(all(len(fold.train_mints) == 2 for fold in rolling_report.folds))
        self.assertEqual(
            rolling_report.folds[-1].train_mints,
            ("TOKEN-3", "TOKEN-4"),
        )
        with self.assertRaisesRegex(ValueError, "max_train_tokens"):
            alpha_walk_forward_tournament(
                bars,
                (viable,),
                execution,
                min_train_tokens=2,
                max_train_tokens=1,
                test_tokens=1,
                minimum_train_positions=1,
            )

        null_report = alpha_walk_forward_tournament(
            bars,
            (impossible,),
            execution,
            min_train_tokens=2,
            test_tokens=1,
            minimum_train_positions=1,
        )
        self.assertTrue(all(fold.selected is None for fold in null_report.folds))
        self.assertEqual(null_report.outcome_diagnostics.observations, 0)


if __name__ == "__main__":
    unittest.main()
