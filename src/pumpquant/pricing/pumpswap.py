"""Exact integer PumpSwap constant-product AMM quotes.

A *quote* primitive: it does not fetch state and
does not select a historical fee tier.  A caller replaying old trades must
resolve the pool's fee configuration first and pass the resulting basis points
explicitly.

The primary quote-in implementation follows the current TypeScript SDK
``@pump-fun/pump-swap-sdk`` **1.18.0** (published source):

* ``fee(amount, bps)`` is ``ceil(amount * bps / 10_000)``;
* exact-base buy uses ``ceil(quote * base / (base_reserve - base))``;
* exact-quote buy first removes all supplied fee components with
  ``floor(gross * 10_000 / (10_000 + total_bps))`` and applies the SDK's
  one-atomic-unit guard before the constant-product quote;
* exact-base sell uses ``floor(quote * base / (base_reserve + base))`` and
  subtracts each fee component rounded up.

The SDK's current source is available at:
https://unpkg.com/@pump-fun/pump-swap-sdk@1.18.0/src/sdk/buy.ts
https://unpkg.com/@pump-fun/pump-swap-sdk@1.18.0/src/sdk/sell.ts
https://unpkg.com/@pump-fun/pump-swap-sdk@1.18.0/src/sdk/util.ts

The official Rust client **0.1.8** exposes the same AMM formulas in
``src/math/amm.rs`` and fee rounding in ``src/math/fees.rs``:
https://docs.rs/crate/pump-rust-client/0.1.8/source/src/math/amm.rs
https://docs.rs/crate/pump-rust-client/0.1.8/source/src/math/fees.rs

That Rust helper's quote-in primitive predates the TypeScript SDK's
one-unit guard and effective-amount correction.  ``pumpswap_buy_exact_quote_in``
intentionally follows the current TypeScript SDK; the
``pumpswap_buy_exact_quote_in_rust`` helper is provided when a caller must
reproduce the Rust helper byte-for-byte.

``cashback`` and ``buyback`` are fee components because they are
present in the PumpSwap IDL's ``BuyEvent``/``SellEvent``.  No component is
inferred: pass the active component's bps (normally creator *or* cashback,
not both) and pass buyback bps only when the replayed state says it is charged.
The SDK's quote helpers default to zero for both because they do not resolve
those state/config destinations themselves.  This module does not implement
fee-tier selection, config replay, recipient selection, cashback claims, or
buyback execution.

All public amounts are validated as non-negative ``u64`` values.  Arithmetic
uses Python integers (equivalent to widening to ``u128`` for the products in
the upstream Rust implementation), and no floating-point operation is used.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

U64_MAX: Final[int] = (1 << 64) - 1
BPS_DENOMINATOR: Final[int] = 10_000


def _u64(name: str, value: int) -> None:
    """Validate a public integer as a Solana ``u64``-like value."""

    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be a nonnegative integer")
    if value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    if value > U64_MAX:
        raise ValueError(f"{name} must fit in u64")


def _positive_reserves(base_reserve: int, quote_reserve: int) -> None:
    _u64("base_reserve", base_reserve)
    _u64("quote_reserve", quote_reserve)
    if base_reserve == 0 or quote_reserve == 0:
        raise ValueError("base_reserve and quote_reserve must be positive")


def ceil_div(numerator: int, denominator: int) -> int:
    """Return ``ceil(numerator / denominator)`` using integer arithmetic."""

    # Products are widened before division (``u128`` in the Rust client), so
    # ``numerator`` is allowed above u64 here.  Public amount
    # arguments and returned amounts still go through ``_u64`` validation.
    if isinstance(numerator, bool) or not isinstance(numerator, int) or numerator < 0:
        raise ValueError("numerator must be a nonnegative integer")
    if isinstance(denominator, bool) or not isinstance(denominator, int):
        raise ValueError("denominator must be a positive integer")
    if denominator <= 0:
        raise ValueError("denominator must be a positive integer")
    return (numerator + denominator - 1) // denominator


def _fee_amount(amount: int, basis_points: int) -> int:
    """PumpSwap's fee rounding: ``ceil(amount * bps / 10_000)``."""

    # ``amount`` can be an intermediate widened integer, while the public
    # boundary and every basis-point input remain u64-like.
    if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
        raise ValueError("amount must be a nonnegative integer")
    _u64("basis_points", basis_points)
    if basis_points > BPS_DENOMINATOR:
        raise ValueError("basis_points must be at most 10000")
    return (amount * basis_points + BPS_DENOMINATOR - 1) // BPS_DENOMINATOR


@dataclass(frozen=True, slots=True)
class PumpSwapFees:
    """PumpSwap fee rates in basis points.

    The protocol's normal fee table has LP, protocol and (for canonical Pump
    pools) coin-creator rates.  Cashback is a destination for the creator-fee
    slice on cashback coins, while buyback is a separate optional component in
    newer IDLs.  This value object keeps all components explicit; callers must
    pass only components that are active for the state being replayed.
    """

    lp_fee_bps: int = 0
    protocol_fee_bps: int = 0
    creator_fee_bps: int = 0
    cashback_fee_bps: int = 0
    buyback_fee_bps: int = 0

    def __post_init__(self) -> None:
        for name, value in (
            ("lp_fee_bps", self.lp_fee_bps),
            ("protocol_fee_bps", self.protocol_fee_bps),
            ("creator_fee_bps", self.creator_fee_bps),
            ("cashback_fee_bps", self.cashback_fee_bps),
            ("buyback_fee_bps", self.buyback_fee_bps),
        ):
            _u64(name, value)
            if value > BPS_DENOMINATOR:
                raise ValueError(f"{name} must be at most 10000")

    @property
    def total_fee_bps(self) -> int:
        """Sum all supplied fee rates (possibly above 10,000)."""

        return (
            self.lp_fee_bps
            + self.protocol_fee_bps
            + self.creator_fee_bps
            + self.cashback_fee_bps
            + self.buyback_fee_bps
        )


@dataclass(frozen=True, slots=True)
class PumpSwapQuote:
    """Quote result shared by buy and sell directions.

    ``curve_quote_amount`` is the quote amount entering the invariant before
    fees.  For an exact-quote buy using the TypeScript SDK guard, the SDK's
    ``effective_quote`` is retained here while the invariant uses
    ``effective_quote - 1``; this mirrors the published source exactly.
    ``user_quote_amount`` is gross quote paid for buys and net quote received
    for sells.  Fee components are all quote-token atomic units.
    """

    base_amount: int
    user_quote_amount: int
    curve_quote_amount: int
    lp_fee_quote: int
    protocol_fee_quote: int
    creator_fee_quote: int
    cashback_fee_quote: int
    buyback_fee_quote: int

    def __post_init__(self) -> None:
        for name, value in (
            ("base_amount", self.base_amount),
            ("user_quote_amount", self.user_quote_amount),
            ("curve_quote_amount", self.curve_quote_amount),
            ("lp_fee_quote", self.lp_fee_quote),
            ("protocol_fee_quote", self.protocol_fee_quote),
            ("creator_fee_quote", self.creator_fee_quote),
            ("cashback_fee_quote", self.cashback_fee_quote),
            ("buyback_fee_quote", self.buyback_fee_quote),
        ):
            _u64(name, value)

    @property
    def total_fee_quote(self) -> int:
        """Total quote-token fee across every component."""

        return (
            self.lp_fee_quote
            + self.protocol_fee_quote
            + self.creator_fee_quote
            + self.cashback_fee_quote
            + self.buyback_fee_quote
        )

    @property
    def fee_components(self) -> tuple[tuple[str, int], ...]:
        """Stable component ordering useful for reconciliation and logging."""

        return (
            ("lp", self.lp_fee_quote),
            ("protocol", self.protocol_fee_quote),
            ("creator", self.creator_fee_quote),
            ("cashback", self.cashback_fee_quote),
            ("buyback", self.buyback_fee_quote),
        )


def _resolve_fees(
    *,
    fees: PumpSwapFees | None,
    lp_fee_bps: int,
    protocol_fee_bps: int,
    creator_fee_bps: int,
    cashback_fee_bps: int,
    buyback_fee_bps: int,
) -> PumpSwapFees:
    if fees is not None:
        if not isinstance(fees, PumpSwapFees):
            raise ValueError("fees must be a PumpSwapFees value")
        # Reject a mixed object/individual call.  Silent precedence here would
        # make historical replay errors very difficult to diagnose.
        if any(
            value != 0
            for value in (
                lp_fee_bps,
                protocol_fee_bps,
                creator_fee_bps,
                cashback_fee_bps,
                buyback_fee_bps,
            )
        ):
            raise ValueError("pass either fees or individual fee basis points")
        return fees
    return PumpSwapFees(
        lp_fee_bps=lp_fee_bps,
        protocol_fee_bps=protocol_fee_bps,
        creator_fee_bps=creator_fee_bps,
        cashback_fee_bps=cashback_fee_bps,
        buyback_fee_bps=buyback_fee_bps,
    )


def _fee_components(amount: int, fees: PumpSwapFees) -> tuple[int, int, int, int, int]:
    return (
        _fee_amount(amount, fees.lp_fee_bps),
        _fee_amount(amount, fees.protocol_fee_bps),
        _fee_amount(amount, fees.creator_fee_bps),
        _fee_amount(amount, fees.cashback_fee_bps),
        _fee_amount(amount, fees.buyback_fee_bps),
    )


def _sum_components(components: tuple[int, int, int, int, int]) -> int:
    return sum(components)


def _quote_result(
    *,
    base_amount: int,
    user_quote_amount: int,
    curve_quote_amount: int,
    components: tuple[int, int, int, int, int],
) -> PumpSwapQuote:
    lp, protocol, creator, cashback, buyback = components
    # The on-chain account boundary is u64.  Python's unbounded sum is useful
    # for intermediate products, but an overflowing result must not be emitted
    # as if it were a valid Solana amount.
    _u64("user_quote_amount", user_quote_amount)
    for name, value in (
        ("lp_fee_quote", lp),
        ("protocol_fee_quote", protocol),
        ("creator_fee_quote", creator),
        ("cashback_fee_quote", cashback),
        ("buyback_fee_quote", buyback),
    ):
        if value > U64_MAX:
            raise OverflowError(f"{name} does not fit in u64")
    return PumpSwapQuote(
        base_amount=base_amount,
        user_quote_amount=user_quote_amount,
        curve_quote_amount=curve_quote_amount,
        lp_fee_quote=lp,
        protocol_fee_quote=protocol,
        creator_fee_quote=creator,
        cashback_fee_quote=cashback,
        buyback_fee_quote=buyback,
    )


def pumpswap_buy_exact_base(
    *,
    base_amount: int,
    base_reserve: int,
    quote_reserve: int,
    lp_fee_bps: int = 0,
    protocol_fee_bps: int = 0,
    creator_fee_bps: int = 0,
    cashback_fee_bps: int = 0,
    buyback_fee_bps: int = 0,
    fees: PumpSwapFees | None = None,
) -> PumpSwapQuote:
    """Quote a buy for an exact amount of base tokens.

    The invariant amount is ``ceil(quote_reserve * base_amount /
    (base_reserve - base_amount))``.  Each fee is rounded up
    independently, matching ``fee`` in the TypeScript SDK and
    ``fee_amount`` in ``pump-rust-client``.
    """

    _positive_reserves(base_reserve, quote_reserve)
    _u64("base_amount", base_amount)
    fee_inputs = _resolve_fees(
        fees=fees,
        lp_fee_bps=lp_fee_bps,
        protocol_fee_bps=protocol_fee_bps,
        creator_fee_bps=creator_fee_bps,
        cashback_fee_bps=cashback_fee_bps,
        buyback_fee_bps=buyback_fee_bps,
    )
    if base_amount == 0:
        return _quote_result(
            base_amount=0,
            user_quote_amount=0,
            curve_quote_amount=0,
            components=(0, 0, 0, 0, 0),
        )
    if base_amount >= base_reserve:
        raise ValueError("base amount exhausts or exceeds the pool base reserve")

    curve_quote = ceil_div(base_amount * quote_reserve, base_reserve - base_amount)
    components = _fee_components(curve_quote, fee_inputs)
    user_quote = curve_quote + _sum_components(components)
    return _quote_result(
        base_amount=base_amount,
        user_quote_amount=user_quote,
        curve_quote_amount=curve_quote,
        components=components,
    )


def _effective_quote_for_sdk(gross_quote: int, fees: PumpSwapFees) -> int:
    """Apply the SDK's inverse fee rounding and conservation correction."""

    effective = gross_quote * BPS_DENOMINATOR // (BPS_DENOMINATOR + fees.total_fee_bps)
    # The published TypeScript helper performs exactly one correction.  Since
    # every fee component is monotone, reducing the effective amount by the
    # original excess cannot create a second excess.
    components = _fee_components(effective, fees)
    excess = effective + _sum_components(components) - gross_quote
    if excess <= 0:
        return effective
    if excess > effective:
        raise ValueError("gross quote amount is too small for the supplied fees")
    return effective - excess


def pumpswap_buy_exact_quote_in(
    *,
    gross_quote_amount: int,
    base_reserve: int,
    quote_reserve: int,
    lp_fee_bps: int = 0,
    protocol_fee_bps: int = 0,
    creator_fee_bps: int = 0,
    cashback_fee_bps: int = 0,
    buyback_fee_bps: int = 0,
    fees: PumpSwapFees | None = None,
) -> PumpSwapQuote:
    """Quote base tokens for a gross quote-token budget.

    This mirrors ``buyQuoteInput`` from SDK 1.18.0.  The returned
    ``curve_quote_amount`` is its post-fee ``effectiveQuote``; the SDK feeds
    ``effectiveQuote - 1`` into the invariant to avoid an atomic-unit
    over-quote.  Any unspent remainder is therefore ``gross_quote_amount -
    user_quote_amount``.
    """

    return _pumpswap_buy_exact_quote_in(
        gross_quote_amount=gross_quote_amount,
        base_reserve=base_reserve,
        quote_reserve=quote_reserve,
        lp_fee_bps=lp_fee_bps,
        protocol_fee_bps=protocol_fee_bps,
        creator_fee_bps=creator_fee_bps,
        cashback_fee_bps=cashback_fee_bps,
        buyback_fee_bps=buyback_fee_bps,
        fees=fees,
        subtract_one=True,
    )


def pumpswap_buy_exact_quote_in_rust(
    *,
    gross_quote_amount: int,
    base_reserve: int,
    quote_reserve: int,
    lp_fee_bps: int = 0,
    protocol_fee_bps: int = 0,
    creator_fee_bps: int = 0,
    cashback_fee_bps: int = 0,
    buyback_fee_bps: int = 0,
    fees: PumpSwapFees | None = None,
) -> PumpSwapQuote:
    """Reproduce ``pump-rust-client`` 0.1.8's raw quote-in primitive.

    The Rust client does not apply the TypeScript SDK's one-unit guard or
    independent-fee conservation correction.  This opt-in function is useful
    only for reproducing that historical helper; normal callers should use
    :func:`pumpswap_buy_exact_quote_in`.
    """

    return _pumpswap_buy_exact_quote_in(
        gross_quote_amount=gross_quote_amount,
        base_reserve=base_reserve,
        quote_reserve=quote_reserve,
        lp_fee_bps=lp_fee_bps,
        protocol_fee_bps=protocol_fee_bps,
        creator_fee_bps=creator_fee_bps,
        cashback_fee_bps=cashback_fee_bps,
        buyback_fee_bps=buyback_fee_bps,
        fees=fees,
        subtract_one=False,
    )


def _pumpswap_buy_exact_quote_in(
    *,
    gross_quote_amount: int,
    base_reserve: int,
    quote_reserve: int,
    lp_fee_bps: int,
    protocol_fee_bps: int,
    creator_fee_bps: int,
    cashback_fee_bps: int,
    buyback_fee_bps: int,
    fees: PumpSwapFees | None,
    subtract_one: bool,
) -> PumpSwapQuote:
    _positive_reserves(base_reserve, quote_reserve)
    _u64("gross_quote_amount", gross_quote_amount)
    fee_inputs = _resolve_fees(
        fees=fees,
        lp_fee_bps=lp_fee_bps,
        protocol_fee_bps=protocol_fee_bps,
        creator_fee_bps=creator_fee_bps,
        cashback_fee_bps=cashback_fee_bps,
        buyback_fee_bps=buyback_fee_bps,
    )
    if gross_quote_amount == 0:
        return _quote_result(
            base_amount=0,
            user_quote_amount=0,
            curve_quote_amount=0,
            components=(0, 0, 0, 0, 0),
        )

    if subtract_one:
        effective_quote = _effective_quote_for_sdk(gross_quote_amount, fee_inputs)
    else:
        effective_quote = (
            gross_quote_amount * BPS_DENOMINATOR // (BPS_DENOMINATOR + fee_inputs.total_fee_bps)
        )
    components = _fee_components(effective_quote, fee_inputs)
    user_quote = effective_quote + _sum_components(components)
    if subtract_one and effective_quote == 0:
        raise ValueError("gross quote amount is too small for a guarded PumpSwap buy")
    invariant_quote = effective_quote - 1 if subtract_one else effective_quote
    base_out = base_reserve * invariant_quote // (quote_reserve + invariant_quote)
    return _quote_result(
        base_amount=base_out,
        user_quote_amount=user_quote,
        curve_quote_amount=effective_quote,
        components=components,
    )


def pumpswap_sell_exact_base(
    *,
    base_amount: int,
    base_reserve: int,
    quote_reserve: int,
    lp_fee_bps: int = 0,
    protocol_fee_bps: int = 0,
    creator_fee_bps: int = 0,
    cashback_fee_bps: int = 0,
    buyback_fee_bps: int = 0,
    fees: PumpSwapFees | None = None,
) -> PumpSwapQuote:
    """Quote net quote tokens for an exact base-token input.

    ``curve_quote_amount = floor(quote_reserve * base_amount /
    (base_reserve + base_amount))``.  Fee components are rounded up
    independently.  As on-chain, a sell is rejected when those fees exceed
    the raw quote output rather than returning a negative amount.
    """

    _positive_reserves(base_reserve, quote_reserve)
    _u64("base_amount", base_amount)
    fee_inputs = _resolve_fees(
        fees=fees,
        lp_fee_bps=lp_fee_bps,
        protocol_fee_bps=protocol_fee_bps,
        creator_fee_bps=creator_fee_bps,
        cashback_fee_bps=cashback_fee_bps,
        buyback_fee_bps=buyback_fee_bps,
    )
    if base_amount == 0:
        return _quote_result(
            base_amount=0,
            user_quote_amount=0,
            curve_quote_amount=0,
            components=(0, 0, 0, 0, 0),
        )

    curve_quote = quote_reserve * base_amount // (base_reserve + base_amount)
    components = _fee_components(curve_quote, fee_inputs)
    total_fee = _sum_components(components)
    if total_fee > curve_quote:
        raise ValueError("fees exceed raw quote output")
    return _quote_result(
        base_amount=base_amount,
        user_quote_amount=curve_quote - total_fee,
        curve_quote_amount=curve_quote,
        components=components,
    )


__all__ = [
    "BPS_DENOMINATOR",
    "U64_MAX",
    "PumpSwapFees",
    "PumpSwapQuote",
    "ceil_div",
    "pumpswap_buy_exact_base",
    "pumpswap_buy_exact_quote_in",
    "pumpswap_buy_exact_quote_in_rust",
    "pumpswap_sell_exact_base",
]
