from __future__ import annotations

import json
import math
import unittest
from dataclasses import replace

from pumpquant.core.schema import Side, VenuePhase
from pumpquant.decoding.instruction_intent import PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID
from pumpquant.features.events import (
    EventFeatureConfig,
    EventFeatureInput,
    causal_event_features,
    event_feature_input_from_raw,
)


def event(
    index: int,
    *,
    side: Side = Side.BUY,
    quote: int = 50,
    base: int = 50,
    trader: str | None = "alice",
    creator: str | None = "alice",
    time_ns: int | None = None,
    coverage_started_at_launch: bool = True,
    pre_base: int | None = None,
    pre_quote: int | None = None,
    post_base: int | None = None,
    post_quote: int | None = None,
    fee: int | None = 0,
) -> EventFeatureInput:
    timestamp = index * 1_000_000_000 if time_ns is None else time_ns
    return EventFeatureInput(
        event_id=f"event-{index}",
        market_id="market",
        mint="mint",
        quote_mint="SOL",
        slot=index,
        tx_index=0,
        log_index=0,
        event_index=0,
        block_time_ns=timestamp,
        available_at_ns=timestamp,
        phase=VenuePhase.BONDING_CURVE,
        side=side,
        base_amount_atomic=base,
        quote_amount_atomic=quote,
        base_decimals=0,
        quote_decimals=0,
        trader=trader,
        creator=creator,
        fee_quote_atomic=fee,
        pre_base_reserve_atomic=pre_base,
        pre_quote_reserve_atomic=pre_quote,
        post_base_reserve_atomic=post_base,
        post_quote_reserve_atomic=post_quote,
        coverage_started_at_launch=coverage_started_at_launch,
    )


def pump_raw_row(
    *,
    side: Side,
    post_base: int,
    post_quote: int,
    base: int = 100,
    quote: int = 100,
) -> dict[str, object]:
    """Minimal normalized Pump TradeEvent row with non-zero fee components."""

    return {
        "event_id": f"pump-{side.value}-{post_base}-{post_quote}",
        "market_id": "pump-pool",
        "mint": "coin",
        "quote_mint": "SOL",
        "slot": 10,
        "tx_index": 0,
        "log_index": 0,
        "event_index": 0,
        "block_time": 1_700_000_000,
        "program_id": PUMP_PROGRAM_ID,
        "protocol_side": side.value,
        "side": side.value,
        "ix_name": side.value,
        "base_amount": base,
        "user_quote_amount": quote,
        "base_decimals": 0,
        "quote_decimals": 0,
        "post_virtual_base_reserve": post_base,
        "post_virtual_quote_reserve": post_quote,
        "post_real_base_reserve": post_base,
        "post_real_quote_reserve": post_quote,
        "fee_mint": "SOL",
        "fee_decimals": 0,
        "protocol_fee_amount": 3,
        "protocol_fee_bps": 5,
        "lp_fee_amount": 0,
        "lp_fee_bps": None,
        "creator_fee_amount": 4,
        "creator_fee_bps": 5,
        "cashback_amount": 5,
        "cashback_fee_bps": 5,
        "buyback_fee_amount": 0,
        "buyback_fee_bps": 0,
        "user": "wallet",
        "values_json": json.dumps({"creator": "creator", "mayhem_mode": False}),
    }


class EventFeatureTests(unittest.TestCase):
    def test_exact_vpin_splits_trades_and_hawkes_respects_chain_ties(self) -> None:
        config = EventFeatureConfig(
            vpin_bucket_quote_atomic=100,
            vpin_window_buckets=2,
            rolling_event_window=10,
            arrival_window_seconds=10.0,
            hawkes_tau_seconds=1.0,
            max_continuity_gap_seconds=30.0,
        )
        rows = causal_event_features(
            (
                event(1, quote=60, base=60, time_ns=1_000_000_000),
                event(
                    2,
                    side=Side.SELL,
                    quote=80,
                    base=80,
                    time_ns=1_000_000_000,
                ),
                event(
                    3,
                    quote=60,
                    base=60,
                    trader="bob",
                    time_ns=2_000_000_000,
                ),
            ),
            config,
        )
        self.assertEqual(rows[0].vpin_partial_fill_fraction, 0.6)
        self.assertEqual(rows[1].completed_vpin_buckets, 1)
        self.assertEqual(rows[1].vpin_partial_fill_fraction, 0.4)
        self.assertIsNone(rows[1].vpin)
        self.assertEqual(rows[2].completed_vpin_buckets, 2)
        self.assertAlmostEqual(rows[2].vpin or 0.0, 0.2)
        self.assertAlmostEqual(rows[2].signed_vpin or 0.0, 0.2)
        self.assertEqual(rows[0].hawkes_pre, 0.0)
        self.assertEqual(rows[0].hawkes_post, 1.0)
        self.assertEqual(rows[1].hawkes_pre, 1.0)
        self.assertEqual(rows[1].hawkes_post, 2.0)
        self.assertAlmostEqual(rows[2].hawkes_pre, 2.0 * math.exp(-1.0))

    def test_wallet_new_repeat_creator_and_concentration_are_causal(self) -> None:
        config = EventFeatureConfig(
            vpin_bucket_quote_atomic=1_000,
            vpin_window_buckets=2,
            rolling_event_window=3,
        )
        inputs = (
            event(1, quote=50, base=50),
            event(2, side=Side.SELL, quote=50, base=50),
            event(3, quote=100, base=100, trader="bob"),
        )
        rows = causal_event_features(inputs, config)
        last = rows[-1]
        self.assertIs(last.is_new_since_coverage, True)
        self.assertFalse(last.first_seen_censored)
        self.assertAlmostEqual(last.new_wallet_flow_share, 0.75)
        self.assertAlmostEqual(last.repeat_wallet_flow_share, 0.25)
        self.assertEqual(last.new_wallet_count, 2)
        self.assertEqual(last.repeat_wallet_count, 1)
        self.assertAlmostEqual(last.wallet_hhi, 0.5)
        self.assertAlmostEqual(last.wallet_top1_share, 0.5)
        self.assertAlmostEqual(last.effective_wallets, 2.0)
        self.assertAlmostEqual(last.wallet_entropy, 1.0)
        self.assertAlmostEqual(last.creator_flow_share or 0.0, 0.5)
        self.assertAlmostEqual(last.creator_buy_share or 0.0, 0.25)
        self.assertAlmostEqual(last.creator_sell_share or 0.0, 0.25)
        self.assertAlmostEqual(last.creator_dump_pressure or 0.0, 0.5)
        self.assertAlmostEqual(last.creator_net_sell_share or 0.0, 0.0)

        censored = causal_event_features((event(1, coverage_started_at_launch=False),), config)[0]
        self.assertTrue(censored.first_seen_censored)
        unknown = causal_event_features((event(1, trader=None),), config)[0]
        self.assertIsNone(unknown.is_new_since_coverage)
        self.assertEqual(unknown.unknown_trader_flow_share, 1.0)

    def test_cross_token_global_flow_and_attention_windows_are_causal(self) -> None:
        first = event(1, quote=100, time_ns=1_000_000_000)
        second = replace(
            event(
                2,
                side=Side.SELL,
                quote=300,
                time_ns=1_000_000_000,
            ),
            market_id="market-b",
            mint="mint-b",
        )
        third = event(3, quote=100, time_ns=7_000_000_000)
        rows = causal_event_features((first, second, third))

        shared = rows[1]
        self.assertEqual(shared.global_arrival_rate_per_second_1s, 2.0)
        self.assertEqual(shared.global_active_markets_5s, 2)
        self.assertEqual(shared.global_arrival_flow_imbalance_5s, 0.0)
        self.assertAlmostEqual(shared.global_signed_quote_imbalance_5s, -0.5)
        self.assertAlmostEqual(shared.market_event_share_global_5s, 0.5)
        self.assertAlmostEqual(shared.market_quote_share_global_5s, 0.75)
        self.assertAlmostEqual(shared.global_activity_ratio_1s_30s, 30.0)

        expired_short = rows[2]
        self.assertEqual(expired_short.global_active_markets_5s, 1)
        self.assertEqual(expired_short.global_arrival_flow_imbalance_5s, 1.0)
        self.assertAlmostEqual(expired_short.global_signed_quote_imbalance_30s, -0.2)
        self.assertEqual(expired_short.global_active_markets_30s, 2)

    def test_reserve_path_impact_gap_reset_and_prefix_invariance(self) -> None:
        config = EventFeatureConfig(
            vpin_bucket_quote_atomic=100,
            vpin_window_buckets=1,
            rolling_event_window=3,
            max_continuity_gap_seconds=5.0,
        )
        inputs = (
            event(
                1,
                quote=100,
                base=100,
                pre_base=1_000,
                pre_quote=1_000,
                post_base=900,
                post_quote=1_100,
                fee=10,
            ),
            event(
                2,
                side=Side.SELL,
                quote=100,
                base=100,
                post_base=800,
                post_quote=1_200,
                fee=10,
            ),
            event(
                100,
                quote=100,
                base=100,
                post_base=700,
                post_quote=1_300,
                fee=10,
            ),
        )
        prefix = causal_event_features(inputs[:2], config)
        full = causal_event_features(inputs, config)
        self.assertEqual(prefix, full[:2])
        first, second, gap = full
        self.assertEqual(first.reserve_source, "explicit_raw_pre")
        self.assertAlmostEqual(first.pre_spot_price or 0.0, 1.0)
        self.assertAlmostEqual(first.post_spot_price or 0.0, 1_100 / 900)
        self.assertAlmostEqual(first.reserve_k_change or 0.0, -0.01)
        self.assertAlmostEqual(first.reserve_quote_change or 0.0, 0.1)
        self.assertAlmostEqual(first.gross_directional_impact_bps or 0.0, 0.0)
        self.assertAlmostEqual(first.fee_excluded_directional_impact_bps or 0.0, -1_000.0)
        self.assertEqual(second.reserve_source, "prior_post")
        self.assertIsNotNone(second.reserve_quote_acceleration)
        self.assertTrue(gap.coverage_gap)
        self.assertEqual(gap.reserve_source, "unavailable")
        self.assertFalse(gap.reserve_continuity)
        self.assertEqual(gap.completed_vpin_buckets, 1)

    def test_explicit_source_discontinuity_resets_market_state(self) -> None:
        config = EventFeatureConfig(
            vpin_bucket_quote_atomic=100,
            vpin_window_buckets=1,
            rolling_event_window=3,
        )
        first = event(
            1,
            quote=100,
            base=100,
            post_base=900,
            post_quote=1_100,
        )
        after_reject = replace(
            event(
                2,
                quote=100,
                base=100,
                post_base=800,
                post_quote=1_200,
            ),
            force_coverage_gap=True,
        )
        rows = causal_event_features((first, after_reject), config)
        self.assertTrue(rows[1].coverage_gap)
        self.assertEqual(rows[1].reserve_source, "unavailable")
        self.assertEqual(rows[1].completed_vpin_buckets, 1)

    def test_rejects_chain_or_availability_regressions(self) -> None:
        with self.assertRaisesRegex(ValueError, "chain ordered"):
            causal_event_features((event(2), event(1)))
        later = replace(event(2), available_at_ns=4_000_000_000)
        regressed = replace(event(3), available_at_ns=3_000_000_000)
        with self.assertRaisesRegex(ValueError, "available_at_ns"):
            causal_event_features((later, regressed))

    def test_raw_projection_keeps_atomic_pre_state_creator_and_fee_provenance(self) -> None:
        row: dict[str, object] = {
            "event_id": "sig:7:disc",
            "market_id": "pool",
            "mint": "coin",
            "quote_mint": "So11111111111111111111111111111111111111112",
            "slot": 10,
            "tx_index": 2,
            "log_index": 7,
            "event_index": 0,
            "block_time": 1_700_000_000,
            "program_id": PUMPSWAP_PROGRAM_ID,
            "protocol_side": "buy",
            "side": "buy",
            "ix_name": "buy",
            "base_amount": 10,
            "user_quote_amount": 20,
            "base_decimals": 6,
            "quote_decimals": 9,
            "pool_base_mint": "coin",
            "pool_quote_mint": "So11111111111111111111111111111111111111112",
            "post_virtual_base_reserve": None,
            "post_virtual_quote_reserve": None,
            "post_real_base_reserve": 990,
            "post_real_quote_reserve": 2_020,
            "fee_mint": "So11111111111111111111111111111111111111112",
            "fee_decimals": 9,
            "protocol_fee_amount": 1,
            "protocol_fee_bps": 5,
            "lp_fee_amount": 2,
            "lp_fee_bps": 20,
            "creator_fee_amount": 3,
            "creator_fee_bps": 5,
            "cashback_amount": 0,
            "cashback_fee_bps": 0,
            "buyback_fee_amount": 0,
            "buyback_fee_bps": 0,
            "user": "wallet",
            "values_json": json.dumps(
                {
                    "coin_creator": "creator",
                    "pool_base_token_reserves": 1_000,
                    "pool_quote_token_reserves": 2_000,
                }
            ),
        }
        projected = event_feature_input_from_raw(row)
        self.assertEqual(projected.pre_base_reserve_atomic, 1_000)
        self.assertEqual(projected.pre_quote_reserve_atomic, 2_000)
        self.assertEqual(projected.post_base_reserve_atomic, 990)
        self.assertEqual(projected.post_quote_reserve_atomic, 2_020)
        self.assertEqual(projected.creator, "creator")
        self.assertEqual(projected.trader, "wallet")
        self.assertEqual(projected.fee_quote_atomic, 6)
        self.assertEqual(projected.protocol_fee_bps, 5)
        self.assertEqual(projected.lp_fee_bps, 20)
        self.assertEqual(projected.creator_fee_bps, 5)
        self.assertEqual(projected.protocol_fee_quote_atomic, 1)
        self.assertEqual(projected.lp_fee_quote_atomic, 2)
        self.assertEqual(projected.creator_fee_quote_atomic, 3)
        self.assertEqual(projected.venue_fee_bps, 30)
        self.assertEqual(projected.fee_asset, "quote")
        self.assertEqual(projected.effective_fee_atomic, 6)
        self.assertFalse(projected.pool_orientation_flipped)
        self.assertEqual(projected.protocol_side, "buy")
        self.assertEqual(projected.instruction_name, "buy")
        self.assertTrue(projected.availability_is_synthetic)

    def test_raw_projection_preserves_flipped_pool_fee_asset(self) -> None:
        row: dict[str, object] = {
            "event_id": "sig:8:disc",
            "market_id": "flipped-pool",
            "mint": "coin",
            "quote_mint": "So11111111111111111111111111111111111111112",
            "slot": 11,
            "tx_index": 2,
            "log_index": 8,
            "event_index": 0,
            "block_time": 1_700_000_001,
            "program_id": PUMPSWAP_PROGRAM_ID,
            "protocol_side": "sell",
            "side": "buy",
            "ix_name": None,
            "base_amount": 10,
            "user_quote_amount": 20,
            "base_decimals": 6,
            "quote_decimals": 9,
            "pool_base_mint": "So11111111111111111111111111111111111111112",
            "pool_quote_mint": "coin",
            "post_virtual_base_reserve": None,
            "post_virtual_quote_reserve": None,
            "post_real_base_reserve": 990,
            "post_real_quote_reserve": 2_020,
            "fee_mint": "coin",
            "fee_decimals": 6,
            "protocol_fee_amount": 1,
            "protocol_fee_bps": 5,
            "lp_fee_amount": 2,
            "lp_fee_bps": 20,
            "creator_fee_amount": 0,
            "creator_fee_bps": 0,
            "cashback_amount": 3,
            "cashback_fee_bps": 5,
            "buyback_fee_amount": 4,
            "buyback_fee_bps": 4,
            "user": "wallet",
            "values_json": json.dumps(
                {
                    "coin_creator": "creator",
                    "pool_base_token_reserves": 2_000,
                    "pool_quote_token_reserves": 1_000,
                }
            ),
        }
        projected = event_feature_input_from_raw(row)
        self.assertEqual(projected.pre_base_reserve_atomic, 1_000)
        self.assertEqual(projected.pre_quote_reserve_atomic, 2_000)
        self.assertEqual(projected.fee_asset, "base")
        self.assertIsNone(projected.fee_quote_atomic)
        self.assertEqual(projected.effective_fee_atomic, 6)
        self.assertEqual(projected.buyback_fee_atomic, 4)
        self.assertEqual(projected.venue_fee_bps, 30)
        self.assertTrue(projected.pool_orientation_flipped)
        self.assertEqual(projected.protocol_side, "sell")

    def test_pump_raw_projection_reconstructs_buy_pre_state_without_fees(self) -> None:
        projected = event_feature_input_from_raw(
            pump_raw_row(side=Side.BUY, post_base=900, post_quote=1_100)
        )

        self.assertEqual(projected.pre_base_reserve_atomic, 1_000)
        self.assertEqual(projected.pre_quote_reserve_atomic, 1_000)
        self.assertEqual(projected.pre_real_base_reserve_atomic, 1_000)
        self.assertEqual(projected.pre_real_quote_reserve_atomic, 1_000)
        self.assertEqual(projected.fee_quote_atomic, 12)
        self.assertEqual(projected.effective_fee_atomic, 12)

    def test_pump_raw_projection_reconstructs_sell_pre_state_without_fees(self) -> None:
        projected = event_feature_input_from_raw(
            pump_raw_row(side=Side.SELL, post_base=1_100, post_quote=900)
        )

        self.assertEqual(projected.pre_base_reserve_atomic, 1_000)
        self.assertEqual(projected.pre_quote_reserve_atomic, 1_000)
        self.assertEqual(projected.pre_real_base_reserve_atomic, 1_000)
        self.assertEqual(projected.pre_real_quote_reserve_atomic, 1_000)
        self.assertEqual(projected.fee_quote_atomic, 12)

    def test_pump_raw_projection_excludes_mayhem_virtual_pre_state(self) -> None:
        row = pump_raw_row(side=Side.BUY, post_base=900, post_quote=1_100)
        row["values_json"] = json.dumps({"creator": "creator", "mayhem_mode": True})

        projected = event_feature_input_from_raw(row)

        self.assertIsNone(projected.pre_base_reserve_atomic)
        self.assertIsNone(projected.pre_quote_reserve_atomic)
        self.assertIsNone(projected.pre_real_base_reserve_atomic)
        self.assertIsNone(projected.pre_real_quote_reserve_atomic)
        self.assertTrue(projected.force_coverage_gap)

    def test_pump_raw_projection_rejects_nonpositive_predecessor(self) -> None:
        with self.assertRaisesRegex(ValueError, "predecessor"):
            event_feature_input_from_raw(pump_raw_row(side=Side.BUY, post_base=900, post_quote=100))
        with self.assertRaisesRegex(ValueError, "predecessor"):
            event_feature_input_from_raw(
                pump_raw_row(side=Side.SELL, post_base=100, post_quote=900)
            )

    def test_pump_raw_projection_never_substitutes_real_for_missing_virtual_state(self) -> None:
        row = pump_raw_row(side=Side.BUY, post_base=900, post_quote=1_100)
        row["post_virtual_base_reserve"] = None
        row["post_virtual_quote_reserve"] = None
        projected = event_feature_input_from_raw(row)
        self.assertIsNone(projected.pre_base_reserve_atomic)
        self.assertIsNone(projected.pre_quote_reserve_atomic)
        self.assertIsNone(projected.post_base_reserve_atomic)
        self.assertIsNone(projected.post_quote_reserve_atomic)

    def test_raw_projection_rejects_partial_reserve_pairs(self) -> None:
        virtual = pump_raw_row(side=Side.BUY, post_base=900, post_quote=1_100)
        virtual["post_virtual_quote_reserve"] = None
        with self.assertRaisesRegex(ValueError, "partial post virtual"):
            event_feature_input_from_raw(virtual)

        real = pump_raw_row(side=Side.BUY, post_base=900, post_quote=1_100)
        real["post_real_quote_reserve"] = None
        with self.assertRaisesRegex(ValueError, "partial post real"):
            event_feature_input_from_raw(real)


if __name__ == "__main__":
    unittest.main()
