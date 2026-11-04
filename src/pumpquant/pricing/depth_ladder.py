"""Executable AMM quotes: the Pump/PumpSwap analogue of an L2 depth ladder."""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum


class QuoteSide(StrEnum):
    BUY_BASE = "buy_base"
    SELL_BASE = "sell_base"


@dataclass(frozen=True, slots=True)
class AmmState:
    base_reserve: float
    quote_reserve: float
    fee_bps: float
    real_base_reserve: float | None = None
    real_quote_reserve: float | None = None

    def __post_init__(self) -> None:
        values = [self.base_reserve, self.quote_reserve, self.fee_bps]
        values.extend(
            value
            for value in (self.real_base_reserve, self.real_quote_reserve)
            if value is not None
        )
        if not all(math.isfinite(value) for value in values):
            raise ValueError("AMM state must contain only finite numbers")
        if self.base_reserve <= 0.0 or self.quote_reserve <= 0.0:
            raise ValueError("virtual/pool reserves must be positive")
        if not 0.0 <= self.fee_bps < 10_000.0:
            raise ValueError("fee_bps must be in [0, 10000)")
        if self.real_base_reserve is not None and self.real_base_reserve < 0.0:
            raise ValueError("real_base_reserve cannot be negative")
        if self.real_quote_reserve is not None and self.real_quote_reserve < 0.0:
            raise ValueError("real_quote_reserve cannot be negative")

    @property
    def marginal_price(self) -> float:
        return self.quote_reserve / self.base_reserve


@dataclass(frozen=True, slots=True)
class ExecutableQuote:
    side: QuoteSide
    input_amount: float
    output_amount: float
    average_price: float
    marginal_price_before: float
    marginal_price_after: float
    impact_bps: float
    fee_input: float


def buy_base_with_quote(state: AmmState, quote_in: float) -> ExecutableQuote:
    """Generic constant-product exact-quote-in buy in normalised units."""
    if quote_in <= 0.0:
        raise ValueError("quote_in must be positive")
    fee = quote_in * state.fee_bps / 10_000.0
    net_quote = quote_in - fee
    invariant = state.base_reserve * state.quote_reserve
    new_quote = state.quote_reserve + net_quote
    new_base = invariant / new_quote
    base_out = state.base_reserve - new_base
    if state.real_base_reserve is not None and base_out > state.real_base_reserve:
        raise ValueError("quote exceeds real base inventory")
    average_price = quote_in / base_out
    marginal_after = new_quote / new_base
    return ExecutableQuote(
        side=QuoteSide.BUY_BASE,
        input_amount=quote_in,
        output_amount=base_out,
        average_price=average_price,
        marginal_price_before=state.marginal_price,
        marginal_price_after=marginal_after,
        impact_bps=(average_price / state.marginal_price - 1.0) * 10_000.0,
        fee_input=fee,
    )


def sell_base_for_quote(state: AmmState, base_in: float) -> ExecutableQuote:
    """Generic constant-product exact-base-in sell in normalised units."""
    if base_in <= 0.0:
        raise ValueError("base_in must be positive")
    fee = base_in * state.fee_bps / 10_000.0
    net_base = base_in - fee
    invariant = state.base_reserve * state.quote_reserve
    new_base = state.base_reserve + net_base
    new_quote = invariant / new_base
    quote_out = state.quote_reserve - new_quote
    if state.real_quote_reserve is not None and quote_out > state.real_quote_reserve:
        raise ValueError("quote exceeds real quote inventory")
    average_price = quote_out / base_in
    marginal_after = new_quote / new_base
    return ExecutableQuote(
        side=QuoteSide.SELL_BASE,
        input_amount=base_in,
        output_amount=quote_out,
        average_price=average_price,
        marginal_price_before=state.marginal_price,
        marginal_price_after=marginal_after,
        impact_bps=(1.0 - average_price / state.marginal_price) * 10_000.0,
        fee_input=fee,
    )


def synthetic_depth(
    state: AmmState, quote_notionals: Iterable[float]
) -> tuple[ExecutableQuote, ...]:
    """Build size-dependent buy and sell levels from a single reserve snapshot.

    The sell level uses the base amount worth each requested quote notional at
    the pre-trade marginal price.  These are independent hypothetical quotes,
    not sequential mutations of the pool.
    """
    levels: list[ExecutableQuote] = []
    for notional in sorted(set(float(value) for value in quote_notionals)):
        if notional <= 0.0:
            raise ValueError("all quote notionals must be positive")
        levels.append(buy_base_with_quote(state, notional))
        levels.append(sell_base_for_quote(state, notional / state.marginal_price))
    return tuple(levels)


def round_trip_cost_bps(state: AmmState, quote_in: float) -> float:
    """Instantaneous buy-then-sell loss including pool curvature and both fees."""
    buy = buy_base_with_quote(state, quote_in)
    after_buy = AmmState(
        base_reserve=state.base_reserve - buy.output_amount,
        quote_reserve=state.quote_reserve + (quote_in - buy.fee_input),
        fee_bps=state.fee_bps,
        real_base_reserve=(
            None if state.real_base_reserve is None else state.real_base_reserve - buy.output_amount
        ),
        real_quote_reserve=(
            None
            if state.real_quote_reserve is None
            else state.real_quote_reserve + quote_in - buy.fee_input
        ),
    )
    sell = sell_base_for_quote(after_buy, buy.output_amount)
    return (1.0 - sell.output_amount / quote_in) * 10_000.0
