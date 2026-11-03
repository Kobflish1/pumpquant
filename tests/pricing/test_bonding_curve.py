from __future__ import annotations

import unittest

from pumpquant.pricing.bonding_curve import (
    ceil_div,
    pump_buy_exact_base,
    pump_buy_exact_quote_in,
    pump_sell_exact_base,
)


class PumpAtomicMathTests(unittest.TestCase):
    def test_ceil_div_matches_fee_rounding(self) -> None:
        self.assertEqual(ceil_div(1, 10_000), 1)
        self.assertEqual(ceil_div(10_000, 10_000), 1)

    def test_exact_quote_buy_uses_inverse_fee_formula(self) -> None:
        quote = pump_buy_exact_quote_in(
            gross_quote_amount=100,
            virtual_base_reserve=1_000,
            virtual_quote_reserve=100,
            real_base_reserve=1_000,
            protocol_fee_bps=100,
            creator_fee_bps=25,
        )
        self.assertEqual(quote.curve_quote_amount, 97)
        self.assertEqual(quote.base_amount, 492)
        self.assertEqual(quote.total_fee_quote, 2)
        self.assertEqual(quote.user_quote_amount, 99)

    def test_exact_base_buy_rounds_curve_and_fee_up(self) -> None:
        quote = pump_buy_exact_base(
            base_amount=100,
            virtual_base_reserve=1_000,
            virtual_quote_reserve=100,
            real_base_reserve=1_000,
            protocol_fee_bps=100,
            creator_fee_bps=25,
        )
        self.assertEqual(quote.curve_quote_amount, 12)
        self.assertEqual(quote.protocol_fee_quote, 1)
        self.assertEqual(quote.creator_fee_quote, 1)
        self.assertEqual(quote.user_quote_amount, 14)

    def test_sell_walks_full_base_then_subtracts_ceil_fees(self) -> None:
        quote = pump_sell_exact_base(
            base_amount=100,
            virtual_base_reserve=1_000,
            virtual_quote_reserve=100,
            real_quote_reserve=100,
            protocol_fee_bps=100,
            creator_fee_bps=25,
        )
        self.assertEqual(quote.curve_quote_amount, 9)
        self.assertEqual(quote.user_quote_amount, 7)
        self.assertEqual(quote.total_fee_quote, 2)

    def test_sell_does_not_invent_partial_fill_at_real_quote_cap(self) -> None:
        with self.assertRaisesRegex(ValueError, "real quote inventory"):
            pump_sell_exact_base(
                base_amount=100,
                virtual_base_reserve=1_000,
                virtual_quote_reserve=100,
                real_quote_reserve=5,
                protocol_fee_bps=100,
                creator_fee_bps=25,
            )


if __name__ == "__main__":
    unittest.main()
