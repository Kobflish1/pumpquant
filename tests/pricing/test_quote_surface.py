from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from pumpquant.pricing.pumpswap import (
    U64_MAX,
    PumpSwapFees,
    pumpswap_buy_exact_quote_in,
    pumpswap_sell_exact_base,
)
from pumpquant.pricing.quote_surface import (
    LAYOUT_REVISION,
    MATH_REVISION,
    NOT_OBSERVED_ORDERBOOK,
    NOT_TRUE_L2,
    RECONSTRUCTED_EXECUTABLE_SURFACE,
    SCHEMA_REVISION,
    SURFACE_LABELS,
    PumpSwapOrderingKey,
    PumpSwapPoolState,
    PumpSwapQuoteSide,
    PumpSwapStateProvenance,
    build_pumpswap_quote_surface,
    pumpswap_buy_level,
    pumpswap_sell_level,
)


def state(**kwargs: object) -> PumpSwapPoolState:
    values: dict[str, object] = {
        "base_reserve": 1_000,
        "quote_reserve": 100,
        "fees": PumpSwapFees(lp_fee_bps=20, protocol_fee_bps=5, creator_fee_bps=5),
        "slot": 7,
        "tx_index": 2,
        "instruction_index": 1,
    }
    values.update(kwargs)
    return PumpSwapPoolState(**values)  # type: ignore[arg-type]


def test_levels_are_exact_direct_math_and_have_rational_prices() -> None:
    pool = state()
    buy = pumpswap_buy_level(pool, 100)
    sell = pumpswap_sell_level(pool, 100)
    direct_buy = pumpswap_buy_exact_quote_in(
        gross_quote_amount=100,
        base_reserve=pool.base_reserve,
        quote_reserve=pool.quote_reserve,
        fees=pool.fees,
    )
    direct_sell = pumpswap_sell_exact_base(
        base_amount=100,
        base_reserve=pool.base_reserve,
        quote_reserve=pool.quote_reserve,
        fees=pool.fees,
    )
    assert buy.base_amount == direct_buy.base_amount
    assert buy.curve_quote_amount == direct_buy.curve_quote_amount
    assert buy.user_quote_amount == direct_buy.user_quote_amount
    assert buy.fee_components == direct_buy.fee_components
    assert buy.average_price == (direct_buy.user_quote_amount, direct_buy.base_amount)
    assert sell.base_amount == direct_sell.base_amount
    assert sell.curve_quote_amount == direct_sell.curve_quote_amount
    assert sell.user_quote_amount == direct_sell.user_quote_amount
    assert sell.fee_components == direct_sell.fee_components
    assert sell.average_price == (direct_sell.user_quote_amount, direct_sell.base_amount)


def test_surface_is_independent_interleaved_and_monotonic() -> None:
    pool = state()
    surface = build_pumpswap_quote_surface(pool, [100, 200, 300])
    assert [level.side for level in surface.levels] == [
        PumpSwapQuoteSide.BUY,
        PumpSwapQuoteSide.SELL,
        PumpSwapQuoteSide.BUY,
        PumpSwapQuoteSide.SELL,
        PumpSwapQuoteSide.BUY,
        PumpSwapQuoteSide.SELL,
    ]
    assert [level.requested_amount for level in surface.buy_levels] == [100, 200, 300]
    assert [level.requested_amount for level in surface.sell_levels] == [100, 200, 300]
    assert [level.output_amount for level in surface.buy_levels] == sorted(
        level.output_amount for level in surface.buy_levels
    )
    assert [level.output_amount for level in surface.sell_levels] == sorted(
        level.output_amount for level in surface.sell_levels
    )
    assert all(level.availability_key == pool.ordering_key for level in surface.levels)


def test_labels_and_revisions_are_explicit() -> None:
    pool = state()
    surface = build_pumpswap_quote_surface(pool, [100])
    assert surface.labels == (
        RECONSTRUCTED_EXECUTABLE_SURFACE,
        NOT_OBSERVED_ORDERBOOK,
        NOT_TRUE_L2,
    )
    assert surface.labels == SURFACE_LABELS
    assert (surface.schema_revision, surface.layout_revision, surface.math_revision) == (
        SCHEMA_REVISION,
        LAYOUT_REVISION,
        MATH_REVISION,
    )


def test_state_is_unavailable_before_source_ordering_key() -> None:
    pool = state()
    assert not pool.is_available_at((7, 2, 0))
    assert not pool.is_available_at((7, 2, 1))
    assert pool.is_available_at((7, 2, 2))
    with pytest.raises(ValueError, match="unavailable"):
        build_pumpswap_quote_surface(pool, [100], as_of=(7, 2, 0))
    with pytest.raises(ValueError, match="unavailable"):
        build_pumpswap_quote_surface(pool, [100], as_of=(7, 2, 1))
    surface = build_pumpswap_quote_surface(pool, [100], as_of=(7, 2, 2))
    assert surface.is_available_at(PumpSwapOrderingKey(7, 2, 2))


def test_explicit_provenance_and_key_are_consistent() -> None:
    key = PumpSwapOrderingKey(8, 4, 2, "swap")
    provenance = PumpSwapStateProvenance(key)
    pool = PumpSwapPoolState(
        base_reserve=1_000,
        quote_reserve=100,
        availability_key=key,
        provenance=provenance,
    )
    assert pool.slot == 8
    assert pool.tx_index == 4
    assert pool.instruction_index == 2
    assert pool.event_id == "swap"
    assert pool.provenance == provenance


def test_inventory_and_unsupported_state_fail_closed() -> None:
    with pytest.raises(ValueError, match="inventory"):
        pumpswap_buy_level(state(real_base_reserve=1), 100)
    with pytest.raises(ValueError, match="quote inventory"):
        pumpswap_sell_level(state(real_quote_reserve=1), 100)
    with pytest.raises(ValueError, match="schema"):
        state(schema_revision="future")
    with pytest.raises(ValueError, match="path"):
        state(path="unsupported")
    with pytest.raises(ValueError, match="fees"):
        state(fees=PumpSwapFees(lp_fee_bps=6_000, protocol_fee_bps=5_000))


def test_amount_type_and_u64_boundaries_are_rejected() -> None:
    with pytest.raises(ValueError):
        pumpswap_buy_level(state(), True)
    with pytest.raises(ValueError):
        pumpswap_sell_level(state(), -1)
    with pytest.raises(ValueError):
        pumpswap_buy_level(state(base_reserve=U64_MAX + 1), 100)
    with pytest.raises(ValueError):
        pumpswap_sell_level(state(), U64_MAX + 1)
    with pytest.raises(ValueError, match="strictly increasing"):
        build_pumpswap_quote_surface(state(), [200, 100])
    with pytest.raises(ValueError, match="strictly increasing"):
        build_pumpswap_quote_surface(state(), [100, 100])


def test_records_are_immutable_and_have_no_float_fields() -> None:
    pool = state()
    surface = build_pumpswap_quote_surface(pool, [100])
    with pytest.raises(FrozenInstanceError):
        pool.base_reserve = 2  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        surface.levels = ()  # type: ignore[misc]
    assert all(
        field.type is not float
        for record in (pool, surface, surface.levels[0])
        for field in record.__dataclass_fields__.values()
    )
