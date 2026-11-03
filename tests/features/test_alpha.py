from __future__ import annotations

import math
import unittest
from dataclasses import replace

from pumpquant.core.schema import Bar, VenuePhase
from pumpquant.features.alpha import (
    ALPHA_FEATURE_INDEX,
    ALPHA_FEATURES,
    AlphaFeatureConfig,
    alpha_feature_definition,
    causal_alpha_features,
)


def make_bar(
    index: int,
    price: float,
    *,
    quote_volume: float | None = None,
    buy_fraction: float = 0.75,
    market_id: str = "TOKEN-CURVE",
    mint: str = "TOKEN",
) -> Bar:
    volume = float(10 + index) if quote_volume is None else quote_volume
    quote_reserve = 100.0
    virtual_base = quote_reserve / price
    return Bar(
        market_id=market_id,
        mint=mint,
        quote_mint="SOL",
        start_time_ns=index * 1_000_000_000,
        end_time_ns=(index + 1) * 1_000_000_000,
        phase=VenuePhase.BONDING_CURVE,
        open=price,
        high=price * 1.02,
        low=price * 0.98,
        close=price,
        base_volume=volume / price,
        quote_volume=volume,
        buy_quote_volume=volume * buy_fraction,
        sell_quote_volume=volume * (1.0 - buy_fraction),
        trade_count=10 + index,
        unique_traders=5 + index // 2,
        virtual_base_reserve=virtual_base,
        virtual_quote_reserve=quote_reserve,
        token_age_seconds=float(index + 1),
        real_base_reserve=virtual_base * max(0.2, 0.90 - index * 0.03),
        real_quote_reserve=80.0 + index,
        venue_fee_bps=125.0,
    )


class AlphaFeatureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = AlphaFeatureConfig(
            short_window=2,
            medium_window=3,
            long_window=5,
            opening_range_bars=2,
            vpin_window=3,
        )

    def test_registry_is_unique_auditable_and_labels_proxies(self) -> None:
        self.assertEqual(len(ALPHA_FEATURE_INDEX), len(ALPHA_FEATURES))
        self.assertGreaterEqual(len(ALPHA_FEATURES), 40)
        for definition in ALPHA_FEATURES:
            self.assertTrue(definition.primary_source.startswith("https://"))
            self.assertTrue(definition.required_fields)
        self.assertIn("not L2", alpha_feature_definition("ofi_depth_proxy").description)
        self.assertIn("not equal-volume", alpha_feature_definition("vpin_proxy").description)

    def test_prefix_invariance_prevents_future_bar_leakage(self) -> None:
        bars = [make_bar(index, 1.0 + index * 0.08) for index in range(10)]
        full = causal_alpha_features(bars, self.config)
        for prefix_length in range(1, len(bars) + 1):
            prefix = causal_alpha_features(bars[:prefix_length], self.config)
            self.assertEqual(prefix, full[:prefix_length])

    def test_core_flow_vwap_momentum_and_vpin_formulas(self) -> None:
        bars = [
            make_bar(0, 1.0, quote_volume=10.0, buy_fraction=0.8),
            make_bar(1, 1.1, quote_volume=20.0, buy_fraction=0.7),
            make_bar(2, 1.3, quote_volume=30.0, buy_fraction=0.6),
        ]
        row = causal_alpha_features(bars, self.config)[-1]
        self.assertAlmostEqual(row.value("momentum_short") or 0.0, 0.3)
        self.assertAlmostEqual(row.value("signed_flow_current") or 0.0, 0.2)
        expected_short_flow = (8.0 + 6.0) / (20.0 + 30.0)
        self.assertAlmostEqual(row.value("signed_flow_short") or 0.0, expected_short_flow)
        expected_vpin = (6.0 + 8.0 + 6.0) / 60.0
        self.assertAlmostEqual(row.value("vpin_proxy") or 0.0, expected_vpin)
        cumulative_vwap = 60.0 / (10.0 / 1.0 + 20.0 / 1.1 + 30.0 / 1.3)
        self.assertAlmostEqual(
            row.value("anchored_vwap_deviation") or 0.0,
            1.3 / cumulative_vwap - 1.0,
        )
        liquidity = row.value("liquidity_quote")
        self.assertIsNotNone(liquidity)
        assert liquidity is not None
        self.assertAlmostEqual(row.value("ofi_depth_proxy") or 0.0, 14.0 / liquidity)

    def test_warmups_missingness_ranges_and_finiteness(self) -> None:
        bars = [make_bar(index, 1.0 + 0.05 * index) for index in range(8)]
        rows = causal_alpha_features(bars, self.config)
        self.assertIsNone(rows[0].value("return_1"))
        self.assertIsNone(rows[1].value("momentum_short"))
        self.assertIsNone(rows[3].value("momentum_long"))
        self.assertIsNotNone(rows[5].value("momentum_long"))
        for row in rows:
            self.assertTrue(all(value is None or math.isfinite(value) for value in row.values))
            flow = row.value("signed_flow_current")
            self.assertIsNotNone(flow)
            assert flow is not None
            self.assertLessEqual(abs(flow), 1.0)
            progress = row.value("reserve_progress")
            self.assertIsNotNone(progress)
            assert progress is not None
            self.assertGreaterEqual(progress, 0.0)
            self.assertLessEqual(progress, 1.0)

    def test_market_histories_are_isolated_and_output_is_deterministic(self) -> None:
        first = [make_bar(index, 1.0 + index * 0.1) for index in range(4)]
        second = [
            make_bar(
                index,
                2.0 - index * 0.1,
                market_id="OTHER-CURVE",
                mint="OTHER",
            )
            for index in range(4)
        ]
        rows = causal_alpha_features(reversed(first + second), self.config)
        self.assertEqual([row.bar.market_id for row in rows[:4]], ["OTHER-CURVE"] * 4)
        self.assertEqual([row.bar.market_id for row in rows[4:]], ["TOKEN-CURVE"] * 4)
        self.assertIsNone(rows[0].value("return_1"))
        self.assertIsNone(rows[4].value("return_1"))

    def test_non_monotonic_information_arrival_is_rejected(self) -> None:
        bars = [make_bar(index, 1.0 + index * 0.1) for index in range(3)]
        bars[0] = replace(bars[0], available_at_ns=10_000_000_000)
        with self.assertRaisesRegex(ValueError, "non-monotonic available_at_ns"):
            causal_alpha_features(bars, self.config)


if __name__ == "__main__":
    unittest.main()
