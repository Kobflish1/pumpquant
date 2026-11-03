from __future__ import annotations

import math
import unittest

from pumpquant.pricing.depth_ladder import (
    AmmState,
    QuoteSide,
    buy_base_with_quote,
    round_trip_cost_bps,
    sell_base_for_quote,
    synthetic_depth,
)


class MicrostructureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state = AmmState(base_reserve=1_000.0, quote_reserve=100.0, fee_bps=100.0)

    def test_buy_and_sell_walk_opposite_sides_of_curve(self) -> None:
        buy = buy_base_with_quote(self.state, 1.0)
        sell = sell_base_for_quote(self.state, 10.0)
        self.assertEqual(buy.side, QuoteSide.BUY_BASE)
        self.assertGreater(buy.average_price, self.state.marginal_price)
        self.assertLess(sell.average_price, self.state.marginal_price)
        self.assertGreater(buy.marginal_price_after, self.state.marginal_price)
        self.assertLess(sell.marginal_price_after, self.state.marginal_price)

    def test_impact_increases_with_size(self) -> None:
        small = buy_base_with_quote(self.state, 0.1)
        large = buy_base_with_quote(self.state, 5.0)
        self.assertGreater(large.impact_bps, small.impact_bps)

    def test_depth_is_a_set_of_independent_two_sided_quotes(self) -> None:
        levels = synthetic_depth(self.state, [1.0, 0.1])
        self.assertEqual(len(levels), 4)
        self.assertEqual([level.input_amount for level in levels[::2]], [0.1, 1.0])

    def test_round_trip_cost_is_close_to_two_one_way_fees(self) -> None:
        cost = round_trip_cost_bps(self.state, 1.0)
        self.assertGreater(cost, 190.0)
        self.assertLess(cost, 210.0)

    def test_real_inventory_caps_buy(self) -> None:
        state = AmmState(
            base_reserve=1_000.0,
            quote_reserve=100.0,
            fee_bps=100.0,
            real_base_reserve=0.01,
        )
        with self.assertRaisesRegex(ValueError, "inventory"):
            buy_base_with_quote(state, 1.0)

    def test_real_quote_inventory_caps_sell(self) -> None:
        state = AmmState(
            base_reserve=1_000.0,
            quote_reserve=100.0,
            fee_bps=100.0,
            real_quote_reserve=0.001,
        )
        with self.assertRaisesRegex(ValueError, "quote inventory"):
            sell_base_for_quote(state, 10.0)

    def test_nonfinite_state_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "finite"):
            AmmState(base_reserve=math.nan, quote_reserve=100.0, fee_bps=100.0)


if __name__ == "__main__":
    unittest.main()
