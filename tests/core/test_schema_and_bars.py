from __future__ import annotations

import unittest
from dataclasses import replace

from pumpquant.core.schema import Bar, Side, SwapEvent, VenuePhase
from pumpquant.features.bars import build_bars, causal_features


def event(
    event_id: str,
    *,
    block_time_ns: int,
    slot: int,
    tx_index: int,
    price: float,
    quote_amount: float,
    side: Side,
    quote_mint: str = "SOL",
    trader: str | None = None,
) -> SwapEvent:
    return SwapEvent(
        event_id=event_id,
        market_id="TOKEN-CURVE",
        mint="TOKEN",
        quote_mint=quote_mint,
        slot=slot,
        tx_index=tx_index,
        instruction_index=0,
        block_time_ns=block_time_ns,
        observed_at_ns=block_time_ns + 5,
        phase=VenuePhase.BONDING_CURVE,
        side=side,
        base_amount=10.0,
        quote_amount=quote_amount,
        price=price,
        virtual_base_reserve=1_000.0 - tx_index,
        virtual_quote_reserve=500.0 + tx_index,
        real_base_reserve=800.0 - tx_index,
        real_quote_reserve=100.0 + tx_index,
        trader=trader,
    )


def bar(index: int, *, mint: str = "TOKEN") -> Bar:
    close = float(index + 1)
    quote_volume = float(10 * (index + 1))
    return Bar(
        market_id=f"{mint}-CURVE",
        mint=mint,
        quote_mint="SOL",
        start_time_ns=index * 1_000_000_000,
        end_time_ns=(index + 1) * 1_000_000_000,
        phase=VenuePhase.BONDING_CURVE,
        open=close,
        high=close,
        low=close,
        close=close,
        base_volume=5.0,
        quote_volume=quote_volume,
        buy_quote_volume=quote_volume * 0.75,
        sell_quote_volume=quote_volume * 0.25,
        trade_count=index + 1,
        unique_traders=1,
        virtual_base_reserve=500.0 / close,
        virtual_quote_reserve=500.0,
        token_age_seconds=float(index + 1),
        real_base_reserve=800.0 - index * 10,
        real_quote_reserve=100.0,
    )


class SchemaTests(unittest.TestCase):
    def test_swap_validation_and_ordering_key(self) -> None:
        earlier = event(
            "earlier",
            block_time_ns=10,
            slot=7,
            tx_index=0,
            price=1.0,
            quote_amount=2.0,
            side=Side.BUY,
        )
        later = event(
            "later",
            block_time_ns=10,
            slot=7,
            tx_index=1,
            price=2.0,
            quote_amount=2.0,
            side=Side.SELL,
        )
        self.assertLess(earlier.ordering_key, later.ordering_key)

        with self.assertRaisesRegex(ValueError, "cannot precede"):
            replace(
                earlier,
                event_id="invalid",
                observed_at_ns=earlier.block_time_ns - 1,
            )

    def test_bar_rejects_inconsistent_ohlc(self) -> None:
        valid = bar(0)
        values = {field: getattr(valid, field) for field in valid.__dataclass_fields__}
        values["high"] = 0.5
        with self.assertRaisesRegex(ValueError, "low cannot exceed|open must lie"):
            Bar(**values)


class AggregationTests(unittest.TestCase):
    def test_aggregation_is_chain_ordered_and_omits_empty_bars(self) -> None:
        first = event(
            "first",
            block_time_ns=100,
            slot=10,
            tx_index=0,
            price=1.0,
            quote_amount=3.0,
            side=Side.BUY,
            trader="alice",
        )
        second = event(
            "second",
            block_time_ns=200,
            slot=10,
            tx_index=1,
            price=2.0,
            quote_amount=5.0,
            side=Side.SELL,
            trader="bob",
        )
        distant = event(
            "distant",
            block_time_ns=2_100,
            slot=11,
            tx_index=0,
            price=1.5,
            quote_amount=7.0,
            side=Side.BUY,
            trader="alice",
        )

        bars = build_bars(
            [second, distant, first],
            interval_ns=1_000,
            launch_times={"TOKEN": 50},
        )

        self.assertEqual(len(bars), 2)
        aggregated = bars[0]
        self.assertEqual((aggregated.open, aggregated.close), (1.0, 2.0))
        self.assertEqual((aggregated.high, aggregated.low), (2.0, 1.0))
        self.assertEqual(aggregated.base_volume, 20.0)
        self.assertEqual(aggregated.quote_volume, 8.0)
        self.assertEqual(aggregated.buy_quote_volume, 3.0)
        self.assertEqual(aggregated.sell_quote_volume, 5.0)
        self.assertEqual(aggregated.trade_count, 2)
        self.assertEqual(aggregated.unique_traders, 2)
        self.assertEqual(aggregated.virtual_base_reserve, 999.0)
        self.assertEqual(aggregated.start_time_ns, 0)
        self.assertEqual(bars[1].start_time_ns, 2_000)

    def test_duplicate_ids_and_quote_changes_are_rejected(self) -> None:
        item = event(
            "same",
            block_time_ns=100,
            slot=1,
            tx_index=0,
            price=1.0,
            quote_amount=1.0,
            side=Side.BUY,
        )
        with self.assertRaisesRegex(ValueError, "duplicate event_id"):
            build_bars([item, item], 1_000)

        other_quote = event(
            "other",
            block_time_ns=200,
            slot=2,
            tx_index=0,
            price=1.0,
            quote_amount=1.0,
            side=Side.BUY,
            quote_mint="USDC",
        )
        with self.assertRaisesRegex(ValueError, "inconsistent mint/quote identity"):
            build_bars([item, other_quote], 1_000)

    def test_distinct_pool_ids_are_never_merged(self) -> None:
        first = event(
            "pool-a",
            block_time_ns=100,
            slot=1,
            tx_index=0,
            price=1.0,
            quote_amount=1.0,
            side=Side.BUY,
        )
        second = replace(
            event(
                "pool-b",
                block_time_ns=100,
                slot=1,
                tx_index=1,
                price=2.0,
                quote_amount=2.0,
                side=Side.BUY,
            ),
            market_id="TOKEN-POOL-B",
        )
        bars = build_bars([first, second], 1_000)
        self.assertEqual(len(bars), 2)
        self.assertEqual({bar.market_id for bar in bars}, {"TOKEN-CURVE", "TOKEN-POOL-B"})

    def test_bar_availability_retains_feed_delay_and_is_monotonic(self) -> None:
        delayed = replace(
            event(
                "delayed",
                block_time_ns=100,
                slot=1,
                tx_index=0,
                price=1.0,
                quote_amount=1.0,
                side=Side.BUY,
            ),
            observed_at_ns=5_000,
        )
        later = event(
            "later-bar",
            block_time_ns=2_100,
            slot=2,
            tx_index=0,
            price=1.1,
            quote_amount=1.0,
            side=Side.BUY,
        )
        bars = build_bars([delayed, later], 1_000)
        self.assertEqual([bar.available_at_ns for bar in bars], [5_000, 5_000])


class FeatureTests(unittest.TestCase):
    def test_features_have_expected_warmup_and_values(self) -> None:
        rows = causal_features([bar(2), bar(0), bar(1)], 2, 2)

        self.assertIsNone(rows[0].return_1)
        self.assertIsNone(rows[0].momentum)
        self.assertIsNone(rows[0].realized_vol)
        self.assertIsNone(rows[0].quote_volume_zscore)
        self.assertAlmostEqual(rows[1].return_1 or 0.0, 1.0)
        self.assertAlmostEqual(rows[1].quote_volume_zscore or 0.0, 1.0)
        self.assertAlmostEqual(rows[2].momentum or 0.0, 2.0)
        self.assertAlmostEqual(rows[2].buy_imbalance, 0.5)
        self.assertEqual(rows[2].trade_intensity, 3.0)
        self.assertAlmostEqual(rows[2].liquidity_quote, 200.0)
        self.assertAlmostEqual(rows[2].price_impact_1_quote_bps or 0.0, 20.0)
        self.assertEqual(rows[2].reserve_progress, 0.0)

    def test_appending_future_bars_cannot_change_past_rows(self) -> None:
        past = [bar(index) for index in range(4)]
        original = causal_features(past, 2, 3)
        extended = causal_features(past + [bar(4), bar(5)], 2, 3)
        self.assertEqual(original, extended[: len(original)])


if __name__ == "__main__":
    unittest.main()
