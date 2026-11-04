"""Atomic-integer Pump bonding-curve quotes matching SDK 1.36.0 semantics.

The caller must supply the historical protocol/creator fee tier and whether the
creator fee applies. Dynamic tier selection belongs to the versioned state
decoder. PumpSwap has different component accounting and is not
represented by these functions.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AtomicPumpQuote:
    """Atomic quote result.

    ``user_quote_amount`` is gross quote paid for buys and net quote received
    for sells; ``curve_quote_amount`` is the pre-fee invariant amount.
    """

    base_amount: int
    user_quote_amount: int
    curve_quote_amount: int
    protocol_fee_quote: int
    creator_fee_quote: int

    @property
    def total_fee_quote(self) -> int:
        return self.protocol_fee_quote + self.creator_fee_quote


def _nonnegative(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")


def _state(
    virtual_base_reserve: int,
    virtual_quote_reserve: int,
    real_base_reserve: int,
) -> None:
    for name, value in (
        ("virtual_base_reserve", virtual_base_reserve),
        ("virtual_quote_reserve", virtual_quote_reserve),
        ("real_base_reserve", real_base_reserve),
    ):
        _nonnegative(name, value)
    if virtual_base_reserve == 0 or virtual_quote_reserve == 0:
        raise ValueError("virtual reserves must be positive")


def ceil_div(numerator: int, denominator: int) -> int:
    _nonnegative("numerator", numerator)
    if isinstance(denominator, bool) or not isinstance(denominator, int) or denominator <= 0:
        raise ValueError("denominator must be a positive integer")
    return (numerator + denominator - 1) // denominator


def _fees(
    amount: int,
    protocol_fee_bps: int,
    creator_fee_bps: int,
    creator_fee_enabled: bool,
) -> tuple[int, int]:
    if not isinstance(creator_fee_enabled, bool):
        raise ValueError("creator_fee_enabled must be boolean")
    for name, value in (
        ("protocol_fee_bps", protocol_fee_bps),
        ("creator_fee_bps", creator_fee_bps),
    ):
        _nonnegative(name, value)
        if value >= 10_000:
            raise ValueError(f"{name} must be below 10000")
    protocol = ceil_div(amount * protocol_fee_bps, 10_000)
    creator = ceil_div(amount * creator_fee_bps, 10_000) if creator_fee_enabled else 0
    return protocol, creator


def pump_buy_exact_quote_in(
    *,
    gross_quote_amount: int,
    virtual_base_reserve: int,
    virtual_quote_reserve: int,
    real_base_reserve: int,
    protocol_fee_bps: int,
    creator_fee_bps: int,
    creator_fee_enabled: bool = True,
) -> AtomicPumpQuote:
    """Return tokens for a gross quote budget using official inverse-fee rounding."""
    _state(virtual_base_reserve, virtual_quote_reserve, real_base_reserve)
    _nonnegative("gross_quote_amount", gross_quote_amount)
    _fees(0, protocol_fee_bps, creator_fee_bps, creator_fee_enabled)
    if gross_quote_amount == 0:
        return AtomicPumpQuote(0, 0, 0, 0, 0)
    total_fee_bps = protocol_fee_bps + (creator_fee_bps if creator_fee_enabled else 0)
    curve_quote = ((gross_quote_amount - 1) * 10_000) // (10_000 + total_fee_bps)
    uncapped_base_out = (curve_quote * virtual_base_reserve) // (
        virtual_quote_reserve + curve_quote
    )
    base_out = min(uncapped_base_out, real_base_reserve)
    if base_out < uncapped_base_out:
        return pump_buy_exact_base(
            base_amount=base_out,
            virtual_base_reserve=virtual_base_reserve,
            virtual_quote_reserve=virtual_quote_reserve,
            real_base_reserve=real_base_reserve,
            protocol_fee_bps=protocol_fee_bps,
            creator_fee_bps=creator_fee_bps,
            creator_fee_enabled=creator_fee_enabled,
        )
    protocol, creator = _fees(
        curve_quote,
        protocol_fee_bps,
        creator_fee_bps,
        creator_fee_enabled,
    )
    return AtomicPumpQuote(
        base_amount=base_out,
        user_quote_amount=curve_quote + protocol + creator,
        curve_quote_amount=curve_quote,
        protocol_fee_quote=protocol,
        creator_fee_quote=creator,
    )


def pump_buy_exact_base(
    *,
    base_amount: int,
    virtual_base_reserve: int,
    virtual_quote_reserve: int,
    real_base_reserve: int,
    protocol_fee_bps: int,
    creator_fee_bps: int,
    creator_fee_enabled: bool = True,
) -> AtomicPumpQuote:
    """Return the gross quote cost for an exact base amount."""
    _state(virtual_base_reserve, virtual_quote_reserve, real_base_reserve)
    _nonnegative("base_amount", base_amount)
    base_out = min(base_amount, real_base_reserve)
    if base_out == 0:
        return AtomicPumpQuote(0, 0, 0, 0, 0)
    if base_out >= virtual_base_reserve:
        raise ValueError("base amount exhausts virtual reserve")
    curve_quote = (base_out * virtual_quote_reserve // (virtual_base_reserve - base_out)) + 1
    protocol, creator = _fees(
        curve_quote,
        protocol_fee_bps,
        creator_fee_bps,
        creator_fee_enabled,
    )
    return AtomicPumpQuote(
        base_amount=base_out,
        user_quote_amount=curve_quote + protocol + creator,
        curve_quote_amount=curve_quote,
        protocol_fee_quote=protocol,
        creator_fee_quote=creator,
    )


def pump_sell_exact_base(
    *,
    base_amount: int,
    virtual_base_reserve: int,
    virtual_quote_reserve: int,
    real_quote_reserve: int,
    protocol_fee_bps: int,
    creator_fee_bps: int,
    creator_fee_enabled: bool = True,
) -> AtomicPumpQuote:
    """Return net quote output; the full base input walks the curve before fees."""
    _state(virtual_base_reserve, virtual_quote_reserve, 0)
    _nonnegative("base_amount", base_amount)
    _nonnegative("real_quote_reserve", real_quote_reserve)
    if base_amount == 0:
        return AtomicPumpQuote(0, 0, 0, 0, 0)
    curve_quote = base_amount * virtual_quote_reserve // (virtual_base_reserve + base_amount)
    if curve_quote > real_quote_reserve:
        raise ValueError("sell exceeds real quote inventory")
    protocol, creator = _fees(
        curve_quote,
        protocol_fee_bps,
        creator_fee_bps,
        creator_fee_enabled,
    )
    net_quote = max(0, curve_quote - protocol - creator)
    return AtomicPumpQuote(
        base_amount=base_amount,
        user_quote_amount=net_quote,
        curve_quote_amount=curve_quote,
        protocol_fee_quote=protocol,
        creator_fee_quote=creator,
    )
