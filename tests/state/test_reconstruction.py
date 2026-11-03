from __future__ import annotations

import pytest

from pumpquant.core.schema import Side, SwapEvent, VenuePhase
from pumpquant.pricing.pumpswap import PumpSwapFees, pumpswap_buy_exact_quote_in
from pumpquant.state.reconstruction import (
    MATH_MODE_INTEGER_EXACT,
    AMMStateReconstructor,
    AMMStateSnapshot,
    PumpFeeConfig,
    QuoteStatus,
    iter_amm_state_snapshots,
)


def event(
    event_id: str,
    *,
    slot: int = 1,
    market_id: str = "market",
    mint: str = "TOKEN",
    quote_mint: str = "SOL",
    phase: VenuePhase = VenuePhase.PUMPSWAP,
    virtual_base_reserve: int | float = 1_000,
    virtual_quote_reserve: int | float = 100,
    real_base_reserve: int | float = 1_000,
    real_quote_reserve: int | float = 100,
) -> SwapEvent:
    return SwapEvent(
        event_id=event_id,
        market_id=market_id,
        mint=mint,
        quote_mint=quote_mint,
        slot=slot,
        tx_index=0,
        instruction_index=0,
        block_time_ns=slot,
        observed_at_ns=slot,
        phase=phase,
        side=Side.BUY,
        base_amount=1,
        quote_amount=1,
        price=0.1,
        virtual_base_reserve=virtual_base_reserve,
        virtual_quote_reserve=virtual_quote_reserve,
        real_base_reserve=real_base_reserve,
        real_quote_reserve=real_quote_reserve,
    )


def test_streaming_integer_surface_is_post_event_and_exact() -> None:
    fees = PumpSwapFees(lp_fee_bps=20, protocol_fee_bps=5, creator_fee_bps=5)
    snapshots = iter_amm_state_snapshots([event("one")], quote_notionals=[10], fee_config=fees)
    snapshot = next(snapshots)

    assert isinstance(snapshot, AMMStateSnapshot)
    assert snapshot.ordering_key == (1, 0, 0, "one")
    assert snapshot.source_ordering_key == snapshot.ordering_key
    assert snapshot.phase is VenuePhase.PUMPSWAP
    assert snapshot.quote_status is QuoteStatus.QUOTED
    assert snapshot.labels[1:] == ("not_observed_orderbook", "not_true_l2")
    assert all(level.math_mode == MATH_MODE_INTEGER_EXACT for level in snapshot.quotes)

    direct = pumpswap_buy_exact_quote_in(
        gross_quote_amount=10,
        base_reserve=1_000,
        quote_reserve=100,
        fees=fees,
    )
    buy = snapshot.buy_quotes[0]
    assert buy.output_amount == direct.base_amount
    assert buy.input_amount == direct.user_quote_amount
    assert buy.impact_bps >= 0
    assert snapshot.spread_bps is not None
    assert snapshot.depth == snapshot.quotes


def test_missing_fee_config_is_explicit_and_event_fee_is_not_invented() -> None:
    snapshot = next(iter_amm_state_snapshots([event("one")], quote_notionals=[10]))
    assert snapshot.quote_status is QuoteStatus.UNKNOWN_CONFIG
    assert snapshot.quotes == ()
    assert "fee configuration" in (snapshot.quote_reason or "")


def test_normalized_fallback_requires_explicit_aggregate_fee() -> None:
    snapshot = next(
        iter_amm_state_snapshots(
            [event("one", virtual_base_reserve=1_000.0, virtual_quote_reserve=100.0)],
            quote_notionals=[10.0],
            fee_config=100.0,
        )
    )
    assert snapshot.quote_status is QuoteStatus.QUOTED
    assert {level.math_mode for level in snapshot.quotes} == {"normalized_approximation"}


def test_ordering_duplicate_and_identity_changes_fail_closed() -> None:
    reconstructor = AMMStateReconstructor(quote_notionals=[10])
    list(reconstructor.consume([event("one", slot=2)]))
    with pytest.raises(ValueError, match="strictly increasing"):
        list(reconstructor.consume([event("two", slot=1)]))

    duplicate = AMMStateReconstructor()
    list(duplicate.consume([event("one", slot=1)]))
    with pytest.raises(ValueError, match="duplicate event_id"):
        list(duplicate.consume([event("one", slot=2)]))

    identity = AMMStateReconstructor()
    list(identity.consume([event("one")]))
    with pytest.raises(ValueError, match="inconsistent mint/quote identity"):
        list(identity.consume([event("two", slot=2, quote_mint="USDC")]))


def test_pump_exact_config_and_slippage_buckets() -> None:
    config = PumpFeeConfig(protocol_fee_bps=100, creator_fee_bps=25)
    snapshot = next(
        iter_amm_state_snapshots(
            [event("one", phase=VenuePhase.BONDING_CURVE)],
            slippage_buckets_bps=[10_000],
            fee_config=config,
        )
    )
    assert snapshot.quote_status is QuoteStatus.QUOTED
    assert snapshot.buy_quotes and snapshot.sell_quotes
    assert all(level.target_slippage_bps == 10_000 for level in snapshot.quotes)
