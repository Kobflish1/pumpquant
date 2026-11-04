from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from pumpquant.core.schema import Bar, VenuePhase
from pumpquant.execution.bar_replay import ExecutionConfig, MomentumSpec, prepare_backtest_bars
from pumpquant.research.search import (
    FOLD_MINT_HASH_SCHEME,
    mint_sequence_sha256,
    momentum_grid,
    momentum_grid_stream,
    momentum_low_discrepancy_stream,
    momentum_spec_sha256,
    walk_forward_search,
)


def token_bars(mint_index: int) -> list[Bar]:
    launch = mint_index * 100 * 1_000_000_000
    prices = [0.10, 0.11, 0.13, 0.16, 0.20, 0.18, 0.17]
    bars: list[Bar] = []
    for index, price in enumerate(prices):
        start = launch + index * 1_000_000_000
        bars.append(
            Bar(
                market_id=f"M{mint_index}-CURVE",
                mint=f"M{mint_index}",
                quote_mint="SOL",
                start_time_ns=start,
                end_time_ns=start + 1_000_000_000,
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
                virtual_base_reserve=100.0 / price,
                virtual_quote_reserve=100.0,
                token_age_seconds=float(index + 1),
                real_base_reserve=1_000_000.0,
                real_quote_reserve=100.0,
            )
        )
    return bars


class SearchTests(unittest.TestCase):
    def test_search_prepares_features_once_per_lookback_pair_and_fold(self) -> None:
        bars = [bar for mint in range(8) for bar in token_bars(mint)]
        template = MomentumSpec(
            volume_lookback=2,
            min_liquidity_quote=0.0,
            min_trade_count=1,
            min_age_seconds=0.0,
            max_age_seconds=100.0,
            exit_momentum=-1.0,
            trailing_stop=0.9,
        )
        candidates = momentum_grid(
            momentum_lookbacks=(1, 2),
            entry_momenta=(0.05,),
            entry_imbalances=(0.1,),
            take_profits=(0.1, 0.2),
            stop_losses=(0.9,),
            template=template,
        )
        config = ExecutionConfig(
            quote_notional=0.1,
            venue_fee_bps=0.0,
            adverse_selection_bps=0.0,
            fixed_network_cost_quote=0.0,
            priority_cost_quote=0.0,
            max_reserve_fraction=0.1,
            max_bar_volume_fraction=0.1,
        )
        with patch(
            "pumpquant.research.search.prepare_backtest_bars",
            wraps=prepare_backtest_bars,
        ) as prepare:
            report = walk_forward_search(
                bars,
                candidates,
                config,
                min_train_tokens=3,
                test_tokens=2,
                minimum_train_positions=1,
                candidate_batch_size=16,
            )
        self.assertEqual(prepare.call_count, len(report.folds) * 2)
        self.assertEqual(report.candidate_count, 4)

    def test_lazy_stream_and_content_hash_are_deterministic(self) -> None:
        stream = momentum_grid_stream(
            momentum_lookbacks=(1, 2, 2),
            entry_momenta=(0.05,),
            entry_imbalances=(0.1,),
            take_profits=(0.1,),
            stop_losses=(0.9,),
        )
        self.assertEqual(len(stream), 2)
        self.assertEqual(tuple(stream), tuple(stream))
        self.assertEqual(
            [momentum_spec_sha256(spec) for spec in stream],
            [momentum_spec_sha256(spec) for spec in stream],
        )
        sample_a = tuple(momentum_low_discrepancy_stream(8, seed=17))
        sample_b = tuple(momentum_low_discrepancy_stream(8, seed=17))
        self.assertEqual(sample_a, sample_b)
        self.assertNotEqual(sample_a, tuple(momentum_low_discrepancy_stream(8, seed=18)))

    def test_grid_is_explicit_and_deduplicated(self) -> None:
        grid = momentum_grid(
            momentum_lookbacks=(2, 2),
            entry_momenta=(0.05,),
            entry_imbalances=(0.1,),
            take_profits=(0.1,),
            stop_losses=(0.2,),
            template=MomentumSpec(min_liquidity_quote=0.0),
        )
        self.assertEqual(len(grid), 1)

    def test_walk_forward_selects_on_past_and_counts_every_trial(self) -> None:
        bars = [bar for mint in range(8) for bar in token_bars(mint)]
        template = MomentumSpec(
            volume_lookback=2,
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
        candidates = momentum_grid(
            momentum_lookbacks=(1, 2),
            entry_momenta=(0.05,),
            entry_imbalances=(0.1,),
            take_profits=(0.1,),
            stop_losses=(0.9,),
            template=template,
        )
        config = ExecutionConfig(
            quote_notional=0.1,
            venue_fee_bps=0.0,
            adverse_selection_bps=0.0,
            fixed_network_cost_quote=0.0,
            priority_cost_quote=0.0,
            max_reserve_fraction=0.1,
            max_bar_volume_fraction=0.1,
        )
        report = walk_forward_search(
            reversed(bars),
            candidates,
            config,
            min_train_tokens=3,
            test_tokens=2,
            embargo_ns=1,
            minimum_train_positions=2,
        )
        self.assertEqual(len(report.folds), 2)
        self.assertEqual(report.trial_count, len(candidates) * len(report.folds))
        for fold in report.folds:
            self.assertIsNotNone(fold.selected)
            self.assertTrue(set(fold.train_mints).isdisjoint(fold.test_mints))
            self.assertLess(fold.train_cutoff_ns, fold.test_start_ns)
        self.assertGreater(report.outcome_diagnostics.observations, 0)

    def test_training_fold_excludes_bars_not_available_by_cutoff(self) -> None:
        bars = [bar for mint in range(3) for bar in token_bars(mint)]
        cutoff = 200 * 1_000_000_000
        delayed: list[Bar] = []
        for bar in bars:
            if (
                bar.mint in {"M0", "M1"}
                and bar.token_age_seconds is not None
                and bar.token_age_seconds >= 3.0
            ):
                delayed.append(replace(bar, available_at_ns=cutoff))
                continue
            delayed.append(bar)
        candidate = MomentumSpec(
            momentum_lookback=1,
            volume_lookback=2,
            entry_momentum=0.0,
            entry_buy_imbalance=0.0,
            min_liquidity_quote=0.0,
            min_trade_count=1,
            min_age_seconds=0.0,
            max_age_seconds=100.0,
            exit_momentum=-1.0,
            take_profit=10.0,
            stop_loss=0.9,
            trailing_stop=0.9,
        )
        report = walk_forward_search(
            delayed,
            [candidate],
            ExecutionConfig(
                quote_notional=0.1,
                venue_fee_bps=0.0,
                adverse_selection_bps=0.0,
                fixed_network_cost_quote=0.0,
                priority_cost_quote=0.0,
                max_reserve_fraction=0.1,
                max_bar_volume_fraction=0.1,
            ),
            min_train_tokens=2,
            test_tokens=1,
            minimum_train_positions=1,
        )
        self.assertEqual(len(report.folds), 1)
        self.assertIsNone(report.folds[0].selected)
        self.assertEqual(report.folds[0].candidate_evaluations[0].position_outcomes, 0)

    def test_halving_ledger_has_one_terminal_row_per_candidate_fold(self) -> None:
        bars = [bar for mint in range(8) for bar in token_bars(mint)]
        template = MomentumSpec(
            volume_lookback=2,
            min_liquidity_quote=0.0,
            min_trade_count=1,
            min_age_seconds=0.0,
            max_age_seconds=100.0,
            exit_momentum=-1.0,
            take_profit=0.1,
            stop_loss=0.9,
            trailing_stop=0.9,
        )
        candidates = momentum_grid_stream(
            momentum_lookbacks=(1, 2, 3, 4),
            entry_momenta=(0.05,),
            entry_imbalances=(0.1,),
            take_profits=(0.1,),
            stop_losses=(0.9,),
            template=template,
        )
        config = ExecutionConfig(
            quote_notional=0.1,
            venue_fee_bps=0.0,
            adverse_selection_bps=0.0,
            fixed_network_cost_quote=0.0,
            priority_cost_quote=0.0,
            max_reserve_fraction=0.1,
            max_bar_volume_fraction=0.1,
        )
        with tempfile.TemporaryDirectory() as directory:
            ledger_path = Path(directory) / "trials.jsonl"
            report = walk_forward_search(
                bars,
                candidates,
                config,
                min_train_tokens=3,
                test_tokens=2,
                minimum_train_positions=1,
                halving_rounds=2,
                survivor_count=1,
                ledger=ledger_path,
                experiment_id="halving-test",
            )
            rows = [json.loads(line) for line in ledger_path.read_text().splitlines()]
        self.assertEqual(len(rows), report.trial_count)
        self.assertEqual(report.pruned_trial_count, sum(row["status"] == "pruned" for row in rows))
        self.assertTrue(any(row["status"] == "completed" for row in rows))
        self.assertTrue(any(row["status"] == "pruned" for row in rows))
        for row in rows:
            self.assertEqual(row["ledger_schema_version"], 3)
            self.assertEqual(row["fold_mint_hash_scheme"], FOLD_MINT_HASH_SCHEME)
            self.assertNotIn("train_mints", row)
            self.assertNotIn("test_mints", row)
            fold = report.folds[row["fold"]]
            self.assertEqual(row["train_mint_count"], len(fold.train_mints))
            self.assertEqual(row["test_mint_count"], len(fold.test_mints))
            self.assertEqual(
                row["train_mints_sha256"],
                mint_sequence_sha256(sorted(fold.train_mints)),
            )
            self.assertEqual(
                row["test_mints_sha256"],
                mint_sequence_sha256(sorted(fold.test_mints)),
            )

    def test_resume_reuses_committed_trials_without_training_replay(self) -> None:
        bars = [bar for mint in range(8) for bar in token_bars(mint)]
        template = MomentumSpec(
            volume_lookback=2,
            min_liquidity_quote=0.0,
            min_trade_count=1,
            min_age_seconds=0.0,
            max_age_seconds=100.0,
            exit_momentum=-1.0,
            take_profit=0.1,
            stop_loss=0.9,
            trailing_stop=0.9,
        )
        candidates = momentum_grid_stream(
            momentum_lookbacks=(1, 2, 3, 4),
            entry_momenta=(0.05,),
            entry_imbalances=(0.1,),
            take_profits=(0.1,),
            stop_losses=(0.9,),
            template=template,
        )
        config = ExecutionConfig(
            quote_notional=0.1,
            venue_fee_bps=0.0,
            adverse_selection_bps=0.0,
            fixed_network_cost_quote=0.0,
            priority_cost_quote=0.0,
            max_reserve_fraction=0.1,
            max_bar_volume_fraction=0.1,
        )
        common = {
            "min_train_tokens": 3,
            "test_tokens": 2,
            "minimum_train_positions": 1,
            "halving_rounds": 2,
            "survivor_count": 1,
            "experiment_id": "resume-test",
        }
        with tempfile.TemporaryDirectory() as directory:
            ledger_path = Path(directory) / "trials.jsonl"
            first = walk_forward_search(bars, candidates, config, ledger=ledger_path, **common)
            rows_before = ledger_path.read_text()
            with patch(
                "pumpquant.research.search._evaluate_prepared",
                side_effect=AssertionError("replayed"),
            ):
                resumed = walk_forward_search(
                    bars,
                    candidates,
                    config,
                    ledger=ledger_path,
                    resume=True,
                    **common,
                )
            self.assertEqual(rows_before, ledger_path.read_text())
        self.assertEqual(first.folds, resumed.folds)
        self.assertEqual(first.evaluation_count, resumed.evaluation_count)
        self.assertEqual(resumed.ledger_rows_appended, 0)

    def test_lazy_stream_matches_tiny_exhaustive_grid(self) -> None:
        bars = [bar for mint in range(8) for bar in token_bars(mint)]
        template = MomentumSpec(
            volume_lookback=2,
            min_liquidity_quote=0.0,
            min_trade_count=1,
            min_age_seconds=0.0,
            max_age_seconds=100.0,
            exit_momentum=-1.0,
            take_profit=0.1,
            stop_loss=0.9,
            trailing_stop=0.9,
        )
        kwargs = {
            "momentum_lookbacks": (1, 2),
            "entry_momenta": (0.05,),
            "entry_imbalances": (0.1,),
            "take_profits": (0.1,),
            "stop_losses": (0.9,),
            "template": template,
        }
        config = ExecutionConfig(
            quote_notional=0.1,
            venue_fee_bps=0.0,
            adverse_selection_bps=0.0,
            fixed_network_cost_quote=0.0,
            priority_cost_quote=0.0,
            max_reserve_fraction=0.1,
            max_bar_volume_fraction=0.1,
        )
        common = {
            "min_train_tokens": 3,
            "test_tokens": 2,
            "minimum_train_positions": 1,
        }
        exhaustive = walk_forward_search(bars, momentum_grid(**kwargs), config, **common)
        streamed = walk_forward_search(bars, momentum_grid_stream(**kwargs), config, **common)
        self.assertEqual(exhaustive, streamed)


if __name__ == "__main__":
    unittest.main()
