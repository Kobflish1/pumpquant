from __future__ import annotations

import random
import unittest
from dataclasses import FrozenInstanceError

from pumpquant.pricing.pumpswap import (
    U64_MAX,
    PumpSwapFees,
    PumpSwapQuote,
    pumpswap_buy_exact_base,
    pumpswap_buy_exact_quote_in,
    pumpswap_buy_exact_quote_in_rust,
    pumpswap_sell_exact_base,
)


class PumpSwapAtomicMathTests(unittest.TestCase):
    def test_exact_base_buy_matches_sdk_ceil_fee_rounding(self) -> None:
        quote = pumpswap_buy_exact_base(
            base_amount=100,
            base_reserve=1_000,
            quote_reserve=100,
            lp_fee_bps=20,
            protocol_fee_bps=5,
            creator_fee_bps=5,
        )
        self.assertEqual(quote.curve_quote_amount, 12)
        self.assertEqual(quote.lp_fee_quote, 1)
        self.assertEqual(quote.protocol_fee_quote, 1)
        self.assertEqual(quote.creator_fee_quote, 1)
        self.assertEqual(quote.total_fee_quote, 3)
        self.assertEqual(quote.user_quote_amount, 15)

    def test_exact_quote_buy_matches_typescript_guard_and_conserves_budget(self) -> None:
        quote = pumpswap_buy_exact_quote_in(
            gross_quote_amount=100,
            base_reserve=1_000,
            quote_reserve=100,
            lp_fee_bps=20,
            protocol_fee_bps=5,
            creator_fee_bps=5,
        )
        # floor(100 * 10_000 / 10_030) = 99; three independent ceil fees
        # would cost 102, so the SDK subtracts the excess and uses 97.  Its
        # one-atomic-unit guard feeds 96 to the invariant.
        self.assertEqual(quote.curve_quote_amount, 97)
        self.assertEqual(quote.base_amount, 489)
        self.assertEqual(quote.total_fee_quote, 3)
        self.assertEqual(quote.user_quote_amount, 100)
        self.assertLessEqual(quote.user_quote_amount, 100)

    def test_rust_quote_in_variant_is_explicitly_distinct(self) -> None:
        quote = pumpswap_buy_exact_quote_in_rust(
            gross_quote_amount=100,
            base_reserve=1_000,
            quote_reserve=100,
            lp_fee_bps=20,
            protocol_fee_bps=5,
            creator_fee_bps=5,
        )
        self.assertEqual(quote.curve_quote_amount, 99)
        self.assertEqual(quote.base_amount, 497)
        self.assertEqual(quote.user_quote_amount, 102)

    def test_exact_base_sell_subtracts_each_ceil_fee(self) -> None:
        quote = pumpswap_sell_exact_base(
            base_amount=100,
            base_reserve=1_000,
            quote_reserve=100,
            lp_fee_bps=20,
            protocol_fee_bps=5,
            creator_fee_bps=5,
        )
        self.assertEqual(quote.curve_quote_amount, 9)
        self.assertEqual(quote.total_fee_quote, 3)
        self.assertEqual(quote.user_quote_amount, 6)

    def test_cashback_and_buyback_are_explicit_components(self) -> None:
        quote = pumpswap_buy_exact_base(
            base_amount=100,
            base_reserve=1_000,
            quote_reserve=100,
            fees=PumpSwapFees(
                lp_fee_bps=20,
                protocol_fee_bps=5,
                cashback_fee_bps=5,
                buyback_fee_bps=10,
            ),
        )
        self.assertEqual(quote.lp_fee_quote, 1)
        self.assertEqual(quote.protocol_fee_quote, 1)
        self.assertEqual(quote.creator_fee_quote, 0)
        self.assertEqual(quote.cashback_fee_quote, 1)
        self.assertEqual(quote.buyback_fee_quote, 1)
        self.assertEqual(quote.total_fee_quote, 4)
        self.assertEqual(quote.user_quote_amount, 16)

    def test_zero_is_deterministic_for_each_direction(self) -> None:
        kwargs = {
            "base_reserve": 1_000,
            "quote_reserve": 100,
            "lp_fee_bps": 20,
            "protocol_fee_bps": 5,
            "creator_fee_bps": 5,
        }
        self.assertEqual(
            pumpswap_buy_exact_base(base_amount=0, **kwargs),
            PumpSwapQuote(0, 0, 0, 0, 0, 0, 0, 0),
        )
        self.assertEqual(
            pumpswap_buy_exact_quote_in(gross_quote_amount=0, **kwargs),
            PumpSwapQuote(0, 0, 0, 0, 0, 0, 0, 0),
        )
        self.assertEqual(
            pumpswap_sell_exact_base(base_amount=0, **kwargs),
            PumpSwapQuote(0, 0, 0, 0, 0, 0, 0, 0),
        )

    def test_positive_budget_too_small_for_sdk_guard_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "too small"):
            pumpswap_buy_exact_quote_in(
                gross_quote_amount=1,
                base_reserve=1_000,
                quote_reserve=100,
                lp_fee_bps=10_000,
                protocol_fee_bps=10_000,
            )

    def test_reserve_exhaustion_and_excessive_sell_fees_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "exhausts"):
            pumpswap_buy_exact_base(
                base_amount=1_000,
                base_reserve=1_000,
                quote_reserve=100,
            )
        with self.assertRaisesRegex(ValueError, "exceeds"):
            pumpswap_buy_exact_base(
                base_amount=1_001,
                base_reserve=1_000,
                quote_reserve=100,
            )
        with self.assertRaisesRegex(ValueError, "fees exceed"):
            pumpswap_sell_exact_base(
                base_amount=1,
                base_reserve=1,
                quote_reserve=2,
                lp_fee_bps=10_000,
                protocol_fee_bps=10_000,
            )

    def test_strict_u64_validation_rejects_bool_negative_and_overflow(self) -> None:
        with self.assertRaises(ValueError):
            pumpswap_buy_exact_base(
                base_amount=True,
                base_reserve=1_000,
                quote_reserve=100,
            )
        with self.assertRaises(ValueError):
            pumpswap_buy_exact_quote_in(
                gross_quote_amount=-1,
                base_reserve=1_000,
                quote_reserve=100,
            )
        with self.assertRaises(ValueError):
            pumpswap_sell_exact_base(
                base_amount=1,
                base_reserve=U64_MAX + 1,
                quote_reserve=100,
            )
        with self.assertRaises(ValueError):
            PumpSwapFees(lp_fee_bps=10_001)

    def test_result_and_fee_inputs_are_immutable(self) -> None:
        fees = PumpSwapFees(lp_fee_bps=20)
        quote = pumpswap_buy_exact_base(
            base_amount=1,
            base_reserve=100,
            quote_reserve=100,
            fees=fees,
        )
        with self.assertRaises(FrozenInstanceError):
            fees.lp_fee_bps = 30  # type: ignore[misc]
        with self.assertRaises(FrozenInstanceError):
            quote.base_amount = 2  # type: ignore[misc]

    def test_randomized_invariant_and_conservation_properties(self) -> None:
        rng = random.Random(0x50554D50)
        for _ in range(500):
            base_reserve = rng.randint(2, 100_000)
            quote_reserve = rng.randint(1, 100_000)
            base_amount = rng.randint(1, base_reserve - 1)
            rates = {
                "lp_fee_bps": rng.randint(0, 40),
                "protocol_fee_bps": rng.randint(0, 40),
                "creator_fee_bps": rng.randint(0, 40),
            }

            buy = pumpswap_buy_exact_base(
                base_amount=base_amount,
                base_reserve=base_reserve,
                quote_reserve=quote_reserve,
                **rates,
            )
            self.assertEqual(
                buy.user_quote_amount,
                buy.curve_quote_amount + buy.total_fee_quote,
            )
            self.assertGreaterEqual(
                (base_reserve - base_amount) * (quote_reserve + buy.curve_quote_amount),
                base_reserve * quote_reserve,
            )

            sell = pumpswap_sell_exact_base(
                base_amount=base_amount,
                base_reserve=base_reserve,
                quote_reserve=quote_reserve,
                **rates,
            )
            self.assertEqual(
                sell.user_quote_amount,
                sell.curve_quote_amount - sell.total_fee_quote,
            )
            self.assertGreaterEqual(
                (base_reserve + base_amount) * (quote_reserve - sell.curve_quote_amount),
                base_reserve * quote_reserve,
            )

            gross = rng.randint(0, 100_000)
            exact_quote = pumpswap_buy_exact_quote_in(
                gross_quote_amount=gross,
                base_reserve=base_reserve,
                quote_reserve=quote_reserve,
                **rates,
            )
            self.assertLessEqual(exact_quote.user_quote_amount, gross)
            self.assertEqual(
                exact_quote.user_quote_amount,
                exact_quote.curve_quote_amount + exact_quote.total_fee_quote,
            )
            self.assertLessEqual(exact_quote.base_amount, base_reserve)


if __name__ == "__main__":
    unittest.main()
