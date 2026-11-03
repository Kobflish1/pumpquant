from __future__ import annotations

import math
import unittest
from dataclasses import dataclass

from pumpquant.validation.metrics import (
    deflated_sharpe_probability,
    expected_maximum_sharpe,
    performance,
    probabilistic_sharpe_ratio,
)
from pumpquant.validation.splits import embargoed_walk_forward


class MetricsTests(unittest.TestCase):
    def test_performance_compounds_and_draws_down(self) -> None:
        result = performance([0.10, -0.05, 0.02])
        self.assertAlmostEqual(result.compounded_return, 1.10 * 0.95 * 1.02 - 1.0)
        self.assertAlmostEqual(result.sequence_max_drawdown, -0.05)
        self.assertEqual(result.observations, 3)
        self.assertTrue(math.isfinite(result.sharpe))

    def test_probabilistic_sharpe_orders_good_and_bad_samples(self) -> None:
        good = [0.01, 0.02, -0.002, 0.015, 0.007, 0.012]
        bad = [-value for value in good]
        self.assertGreater(probabilistic_sharpe_ratio(good), 0.5)
        self.assertLess(probabilistic_sharpe_ratio(bad), 0.5)

    def test_multiple_trials_raise_required_sharpe(self) -> None:
        self.assertGreater(expected_maximum_sharpe(100, 0.2), 0.0)
        returns = [0.01, 0.02, -0.002, 0.015, 0.007, 0.012]
        plain = probabilistic_sharpe_ratio(returns)
        deflated = deflated_sharpe_probability(returns, [i / 100 for i in range(-20, 21)])
        self.assertLessEqual(deflated, plain)


@dataclass(frozen=True)
class Cohort:
    mint: str
    launch_time_ns: int


class ValidationTests(unittest.TestCase):
    def test_walk_forward_is_ordered_embargoed_and_token_isolated(self) -> None:
        cohorts = [Cohort(f"m{i}", i * 10) for i in range(8)]
        folds = embargoed_walk_forward(
            reversed(cohorts), min_train_size=3, test_size=2, embargo_ns=5
        )
        self.assertEqual(len(folds), 2)
        self.assertEqual([item.mint for item in folds[0].test], ["m3", "m4"])
        self.assertTrue(all(item.launch_time_ns < 25 for item in folds[0].train))
        self.assertTrue(
            set(item.mint for item in folds[0].train).isdisjoint(
                item.mint for item in folds[0].test
            )
        )

    def test_duplicate_cohort_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate"):
            embargoed_walk_forward(
                [Cohort("same", 1), Cohort("same", 2)], min_train_size=1, test_size=1
            )


if __name__ == "__main__":
    unittest.main()
