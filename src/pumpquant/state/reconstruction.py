"""Streaming, provenance-aware reconstruction of Pump/PumpSwap AMM state.

The normalized :class:`~pumpquant.schema.SwapEvent` record already carries a
post-swap reserve snapshot.  This module turns a chain-ordered stream of those
records into immutable post-event state snapshots and *hypothetical* executable
AMM quotes.  It is intentionally not an order-book feed: a pool has a quote
function, not resting bids/asks, queue position, cancellations, or observed
L2.

The stream is consumed one event at a time.  No event list is built and no
event is sorted behind the caller's back.  Duplicate event IDs, ordering
regressions, and a changed ``(mint, quote_mint)`` identity for one market fail
closed.  Fee configuration is always supplied by the caller (or reported as
unknown); ``venue_fee_bps`` on an event is deliberately not promoted into a
protocol fee tier because it does not identify the Pump/PumpSwap fee
components or their historical rounding.

When reserves, notionals, and a complete fee configuration are atomic
integers, quotes delegate to :mod:`pumpquant.protocol_math` or
:mod:`pumpquant.pumpswap_math`.  A caller may explicitly request normalized
quotes with ``atomic=False`` and a single aggregate ``fee_bps``; these are
marked as ``normalized_approximation`` and never masquerade as integer
protocol output.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from enum import StrEnum
from numbers import Real
from types import MappingProxyType
from typing import TypeAlias, cast

from pumpquant.core.schema import Number, Side, SwapEvent, VenuePhase
from pumpquant.pricing.bonding_curve import (
    pump_buy_exact_base,
    pump_buy_exact_quote_in,
    pump_sell_exact_base,
)
from pumpquant.pricing.pumpswap import (
    U64_MAX,
    PumpSwapFees,
    pumpswap_buy_exact_base,
    pumpswap_buy_exact_quote_in,
    pumpswap_sell_exact_base,
)
from pumpquant.pricing.quote_surface import (
    NOT_OBSERVED_ORDERBOOK,
    NOT_TRUE_L2,
    RECONSTRUCTED_EXECUTABLE_SURFACE,
    SURFACE_LABELS,
)

AMM_SURFACE_LABELS: tuple[str, str, str] = SURFACE_LABELS
"""Stable capability labels carried by every reconstructed snapshot."""

MATH_MODE_INTEGER_EXACT = "integer_exact"
MATH_MODE_NORMALIZED_APPROXIMATION = "normalized_approximation"


class QuoteStatus(StrEnum):
    """Why a snapshot does or does not contain executable quote levels."""

    QUOTED = "quoted"
    NOT_REQUESTED = "not_requested"
    UNKNOWN_CONFIG = "unknown_config"
    UNSUPPORTED_PHASE = "unsupported_phase"
    UNQUOTABLE = "unquotable"


AMMQuoteStatus = QuoteStatus
"""Long-form alias retained for callers that prefer an AMM prefix."""


def _text(name: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _finite(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite real number")
    if not math.isfinite(float(value)):
        raise ValueError(f"{name} must be a finite real number")


def _positive(name: str, value: object) -> None:
    _finite(name, value)
    if value <= 0:  # type: ignore[operator]
        raise ValueError(f"{name} must be positive")


def _nonnegative(name: str, value: object) -> None:
    _finite(name, value)
    if value < 0:  # type: ignore[operator]
        raise ValueError(f"{name} must be nonnegative")


def _bps(name: str, value: object, *, allow_10000: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer basis-point value")
    upper = 10_000 if allow_10000 else 9_999
    if value > upper:
        comparator = "at most 10000" if allow_10000 else "below 10000"
        raise ValueError(f"{name} must be {comparator}")


def _ratio_float(numerator: Number, denominator: Number) -> float:
    if denominator <= 0:
        raise ValueError("ratio denominator must be positive")
    result = float(numerator) / float(denominator)
    if not math.isfinite(result):
        raise ValueError("ratio must be finite")
    return result


def _is_integer_value(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _strict_amounts(values: Iterable[Number], name: str) -> tuple[Number, ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError(f"{name} must contain numeric amounts")
    result = tuple(values)
    if not result:
        raise ValueError(f"{name} must not be empty")
    previous: Number | None = None
    for value in result:
        _positive(name, value)
        if previous is not None and value <= previous:
            raise ValueError(f"{name} must be strictly increasing")
        previous = value
    return result


@dataclass(frozen=True, slots=True)
class PumpFeeConfig:
    """Complete Pump bonding-curve fee inputs for one historical program tier."""

    protocol_fee_bps: int
    creator_fee_bps: int = 0
    creator_fee_enabled: bool = True
    atomic: bool = True

    def __post_init__(self) -> None:
        _bps("protocol_fee_bps", self.protocol_fee_bps)
        _bps("creator_fee_bps", self.creator_fee_bps)
        if not isinstance(self.creator_fee_enabled, bool):
            raise ValueError("creator_fee_enabled must be boolean")
        if not isinstance(self.atomic, bool):
            raise ValueError("atomic must be boolean")


@dataclass(frozen=True, slots=True)
class PumpSwapAMMConfig:
    """Complete PumpSwap fee components and the amount-unit declaration."""

    fees: PumpSwapFees
    atomic: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.fees, PumpSwapFees):
            raise ValueError("fees must be a PumpSwapFees value")
        if self.fees.total_fee_bps > 10_000:
            raise ValueError("PumpSwap fee components must total at most 10000 bps")
        if not isinstance(self.atomic, bool):
            raise ValueError("atomic must be boolean")


@dataclass(frozen=True, slots=True)
class AMMQuoteConfig:
    """Explicit fee/unit configuration used to build hypothetical quotes.

    ``pumpswap_fees`` is a complete PumpSwap component table.  The individual
    PumpSwap component fields are accepted for convenient config-file loading,
    but all five must be present before they can be used for an exact quote.
    For a normalized approximation, pass only ``fee_bps`` and
    ``atomic=False``.  A missing config is different from a zero-fee config and
    is reported as :attr:`QuoteStatus.UNKNOWN_CONFIG`.
    """

    atomic: bool | None = None
    fee_bps: Number | None = None
    protocol_fee_bps: int | None = None
    creator_fee_bps: int | None = None
    creator_fee_enabled: bool | None = None
    lp_fee_bps: int | None = None
    cashback_fee_bps: int | None = None
    buyback_fee_bps: int | None = None
    pumpswap_fees: PumpSwapFees | None = None
    pump_fee_config: PumpFeeConfig | None = None

    def __post_init__(self) -> None:
        if self.atomic is not None and not isinstance(self.atomic, bool):
            raise ValueError("atomic must be boolean or None")
        if self.fee_bps is not None:
            _finite("fee_bps", self.fee_bps)
            if self.fee_bps < 0 or self.fee_bps >= 10_000:
                raise ValueError("fee_bps must be in [0, 10000)")
        for name in (
            "protocol_fee_bps",
            "creator_fee_bps",
            "lp_fee_bps",
            "cashback_fee_bps",
            "buyback_fee_bps",
        ):
            value = getattr(self, name)
            if value is not None:
                _bps(name, value, allow_10000=True)
        if self.creator_fee_enabled is not None and not isinstance(self.creator_fee_enabled, bool):
            raise ValueError("creator_fee_enabled must be boolean or None")
        if self.pumpswap_fees is not None:
            if not isinstance(self.pumpswap_fees, PumpSwapFees):
                raise ValueError("pumpswap_fees must be a PumpSwapFees value")
            if self.pumpswap_fees.total_fee_bps > 10_000:
                raise ValueError("PumpSwap fee components must total at most 10000 bps")
        if self.pump_fee_config is not None and not isinstance(self.pump_fee_config, PumpFeeConfig):
            raise ValueError("pump_fee_config must be a PumpFeeConfig value")
        if self.pumpswap_fees is not None and any(
            value is not None
            for value in (
                self.protocol_fee_bps,
                self.creator_fee_bps,
                self.creator_fee_enabled,
                self.lp_fee_bps,
                self.cashback_fee_bps,
                self.buyback_fee_bps,
            )
        ):
            raise ValueError("pass pumpswap_fees or individual fee components, not both")
        if self.pump_fee_config is not None and any(
            value is not None
            for value in (
                self.protocol_fee_bps,
                self.creator_fee_bps,
                self.creator_fee_enabled,
            )
        ):
            raise ValueError("pass pump_fee_config or individual Pump fee components, not both")

    @classmethod
    def normalized(cls, fee_bps: Number) -> AMMQuoteConfig:
        """Build an explicitly normalized (non-atomic) aggregate-fee config."""

        return cls(fee_bps=fee_bps, atomic=False)

    @classmethod
    def pump(cls, config: PumpFeeConfig) -> AMMQuoteConfig:
        if not isinstance(config, PumpFeeConfig):
            raise ValueError("config must be a PumpFeeConfig value")
        return cls(pump_fee_config=config, atomic=config.atomic)

    @classmethod
    def pumpswap(cls, config: PumpSwapAMMConfig | PumpSwapFees) -> AMMQuoteConfig:
        if isinstance(config, PumpSwapAMMConfig):
            return cls(pumpswap_fees=config.fees, atomic=config.atomic)
        if isinstance(config, PumpSwapFees):
            return cls(pumpswap_fees=config, atomic=True)
        raise ValueError("config must be PumpSwapAMMConfig or PumpSwapFees")


FeeConfig: TypeAlias = (
    AMMQuoteConfig
    | PumpFeeConfig
    | PumpSwapAMMConfig
    | PumpSwapFees
    | Mapping[str, object]
    | int
    | float
)
FeeConfigResolver: TypeAlias = Callable[[SwapEvent], FeeConfig | None]


def _coerce_config(value: object) -> AMMQuoteConfig | None:
    if value is None:
        return None
    if isinstance(value, AMMQuoteConfig):
        return value
    if isinstance(value, PumpFeeConfig):
        return AMMQuoteConfig.pump(value)
    if isinstance(value, PumpSwapAMMConfig):
        return AMMQuoteConfig.pumpswap(value)
    if isinstance(value, PumpSwapFees):
        return AMMQuoteConfig.pumpswap(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return AMMQuoteConfig.normalized(value)
    if isinstance(value, Mapping):
        values = dict(value)
        if "fees" in values and isinstance(values["fees"], PumpSwapFees):
            return AMMQuoteConfig.pumpswap(
                PumpSwapAMMConfig(
                    values["fees"],
                    atomic=bool(values.get("atomic", True)),
                )
            )
        try:
            return AMMQuoteConfig(**values)
        except TypeError as error:
            raise ValueError("fee config mapping has unsupported fields") from error
    raise ValueError(f"unsupported AMM fee config type: {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class AMMQuoteLevel:
    """One independent hypothetical buy or sell quote from one post-state."""

    side: Side
    requested_quote_amount: Number
    input_amount: Number
    output_amount: Number
    base_amount: Number
    quote_amount: Number
    curve_quote_amount: Number
    fee_quote_amount: Number
    fee_components: tuple[tuple[str, Number], ...]
    average_price_numerator: Number
    average_price_denominator: Number
    marginal_price_before_numerator: Number
    marginal_price_before_denominator: Number
    marginal_price_after_numerator: Number
    marginal_price_after_denominator: Number
    impact_bps: float
    math_mode: str
    target_slippage_bps: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.side, Side):
            raise ValueError("side must be a Side")
        for name in (
            "requested_quote_amount",
            "input_amount",
            "output_amount",
            "base_amount",
            "quote_amount",
            "curve_quote_amount",
            "average_price_numerator",
            "average_price_denominator",
            "marginal_price_before_numerator",
            "marginal_price_before_denominator",
            "marginal_price_after_numerator",
            "marginal_price_after_denominator",
        ):
            _positive(name, getattr(self, name))
        _nonnegative("fee_quote_amount", self.fee_quote_amount)
        _nonnegative("impact_bps", self.impact_bps)
        _text("math_mode", self.math_mode)
        if self.math_mode not in {
            MATH_MODE_INTEGER_EXACT,
            MATH_MODE_NORMALIZED_APPROXIMATION,
        }:
            raise ValueError("unsupported AMM quote math mode")
        if self.target_slippage_bps is not None:
            _nonnegative("target_slippage_bps", self.target_slippage_bps)
        if self.side is Side.BUY:
            if self.input_amount != self.quote_amount:
                raise ValueError("buy input_amount must equal quote_amount")
            if self.output_amount != self.base_amount:
                raise ValueError("buy output_amount must equal base_amount")
        else:
            if self.input_amount != self.base_amount:
                raise ValueError("sell input_amount must equal base_amount")
            if self.output_amount != self.quote_amount:
                raise ValueError("sell output_amount must equal quote_amount")
        _text("fee component names", "fees")
        for name, amount in self.fee_components:
            _text("fee component name", name)
            _nonnegative("fee component amount", amount)

    @property
    def requested_amount(self) -> Number:
        return self.requested_quote_amount

    @property
    def input_quote_amount(self) -> Number:
        return self.input_amount if self.side is Side.BUY else self.output_amount

    @property
    def output_base_amount(self) -> Number:
        return self.output_amount if self.side is Side.BUY else self.input_amount

    @property
    def average_price(self) -> float:
        return _ratio_float(self.average_price_numerator, self.average_price_denominator)

    @property
    def marginal_price_before(self) -> float:
        return _ratio_float(
            self.marginal_price_before_numerator,
            self.marginal_price_before_denominator,
        )

    @property
    def marginal_price_after(self) -> float:
        return _ratio_float(
            self.marginal_price_after_numerator,
            self.marginal_price_after_denominator,
        )

    @property
    def fee_quote(self) -> Number:
        return self.fee_quote_amount

    @property
    def impact(self) -> float:
        return self.impact_bps

    @property
    def price_impact_bps(self) -> float:
        return self.impact_bps

    @property
    def depth_base(self) -> Number:
        return self.output_base_amount

    @property
    def depth_quote(self) -> Number:
        return self.input_quote_amount


def _quote_impact(
    *,
    side: Side,
    average_num: int,
    average_den: int,
    marginal_before_num: int,
    marginal_before_den: int,
) -> float:
    """Calculate impact in bps using integer cross-products when possible."""

    if side is Side.BUY:
        numerator = average_num * marginal_before_den - marginal_before_num * average_den
    else:
        numerator = marginal_before_num * average_den - average_num * marginal_before_den
    denominator = average_den * marginal_before_num
    if denominator <= 0:
        raise ValueError("quote impact has a non-positive denominator")
    result = numerator * 10_000 / denominator
    if result < 0 and result > -1e-9:
        result = 0.0
    if not math.isfinite(result) or result < 0:
        raise ValueError("quote impact must be finite and nonnegative")
    return float(result)


def _normalized_impact(
    side: Side,
    average_price: float,
    marginal_before: float,
) -> float:
    result = (
        average_price / marginal_before - 1.0
        if side is Side.BUY
        else 1.0 - average_price / marginal_before
    ) * 10_000.0
    if result < 0 and result > -1e-8:
        result = 0.0
    if not math.isfinite(result) or result < 0:
        raise ValueError("normalized quote impact must be finite and nonnegative")
    return result


def _exact_input_is_available(
    event: SwapEvent,
    requests: Iterable[Number],
    config: AMMQuoteConfig,
) -> bool:
    if config.atomic is False:
        return False
    values: list[object] = [
        event.virtual_base_reserve,
        event.virtual_quote_reserve,
        event.real_base_reserve,
        event.real_quote_reserve,
    ]
    values.extend(requests)
    return all(_is_integer_value(value) for value in values)


def _pump_config(config: AMMQuoteConfig) -> PumpFeeConfig | None:
    if config.pump_fee_config is not None:
        return config.pump_fee_config
    if (
        config.protocol_fee_bps is not None
        and config.creator_fee_bps is not None
        and config.creator_fee_enabled is not None
    ):
        return PumpFeeConfig(
            protocol_fee_bps=config.protocol_fee_bps,
            creator_fee_bps=config.creator_fee_bps,
            creator_fee_enabled=config.creator_fee_enabled,
            atomic=True if config.atomic is None else config.atomic,
        )
    return None


def _pumpswap_config(config: AMMQuoteConfig) -> PumpSwapFees | None:
    if config.pumpswap_fees is not None:
        return config.pumpswap_fees
    components = (
        config.lp_fee_bps,
        config.protocol_fee_bps,
        config.creator_fee_bps,
        config.cashback_fee_bps,
        config.buyback_fee_bps,
    )
    if all(value is not None for value in components):
        return PumpSwapFees(
            lp_fee_bps=cast(int, components[0]),
            protocol_fee_bps=cast(int, components[1]),
            creator_fee_bps=cast(int, components[2]),
            cashback_fee_bps=cast(int, components[3]),
            buyback_fee_bps=cast(int, components[4]),
        )
    return None


def _aggregate_fee_bps(config: AMMQuoteConfig) -> float | None:
    if config.fee_bps is not None:
        return float(config.fee_bps)
    if config.pumpswap_fees is not None:
        return float(config.pumpswap_fees.total_fee_bps)
    values = (
        config.lp_fee_bps,
        config.protocol_fee_bps,
        config.creator_fee_bps,
        config.cashback_fee_bps,
        config.buyback_fee_bps,
    )
    if any(value is not None for value in values):
        if not all(value is not None for value in values):
            return None
        return float(sum(cast(int, value) for value in values))
    pump = _pump_config(config)
    if pump is not None:
        return float(
            pump.protocol_fee_bps + (pump.creator_fee_bps if pump.creator_fee_enabled else 0)
        )
    return None


def _normal_buy(
    *,
    phase: VenuePhase,
    event: SwapEvent,
    gross_quote: float,
    fee_bps: float,
) -> tuple[float, float, float, float, tuple[tuple[str, Number], ...]]:
    if gross_quote <= 0:
        raise ValueError("gross quote amount must be positive")
    vb = float(event.virtual_base_reserve)
    vq = float(event.virtual_quote_reserve)
    real_base = float(event.real_base_reserve)
    fee = gross_quote * fee_bps / 10_000.0
    curve_quote = gross_quote - fee
    if curve_quote <= 0:
        raise ValueError("fee consumes normalized quote amount")
    base_out = vb * curve_quote / (vq + curve_quote)
    if phase is VenuePhase.BONDING_CURVE:
        base_out = min(base_out, real_base)
        if base_out >= vb:
            raise ValueError("buy exhausts virtual base reserve")
        if base_out <= 0:
            raise ValueError("buy exceeds real base inventory")
        if base_out < vb * curve_quote / (vq + curve_quote):
            curve_quote = base_out * vq / (vb - base_out)
            gross_quote = curve_quote * (1.0 + fee_bps / 10_000.0)
            fee = gross_quote - curve_quote
    return base_out, gross_quote, curve_quote, fee, (("total", fee),)


def _normal_sell(
    *,
    event: SwapEvent,
    base_in: float,
    fee_bps: float,
) -> tuple[float, float, float, float, tuple[tuple[str, Number], ...]]:
    if base_in <= 0:
        raise ValueError("base amount must be positive")
    vb = float(event.virtual_base_reserve)
    vq = float(event.virtual_quote_reserve)
    real_quote = float(event.real_quote_reserve)
    curve_quote = vq * base_in / (vb + base_in)
    if curve_quote > real_quote:
        raise ValueError("sell exceeds real quote inventory")
    fee = curve_quote * fee_bps / 10_000.0
    quote_out = curve_quote - fee
    if quote_out <= 0:
        raise ValueError("fee consumes normalized quote output")
    return base_in, quote_out, curve_quote, fee, (("total", fee),)


def _make_level(
    *,
    event: SwapEvent,
    side: Side,
    requested_quote: Number,
    input_amount: Number,
    output_amount: Number,
    base_amount: Number,
    quote_amount: Number,
    curve_quote_amount: Number,
    fee_quote_amount: Number,
    fee_components: tuple[tuple[str, Number], ...],
    post_base: Number,
    post_quote: Number,
    math_mode: str,
    target_slippage_bps: float | None = None,
) -> AMMQuoteLevel:
    before_base = event.virtual_base_reserve
    before_quote = event.virtual_quote_reserve
    average_num = input_amount if side is Side.BUY else output_amount
    average_den = output_amount if side is Side.BUY else input_amount
    if all(
        _is_integer_value(value)
        for value in (
            average_num,
            average_den,
            before_quote,
            before_base,
            post_quote,
            post_base,
        )
    ):
        impact = _quote_impact(
            side=side,
            average_num=int(average_num),
            average_den=int(average_den),
            marginal_before_num=int(before_quote),
            marginal_before_den=int(before_base),
        )
    else:
        before = _ratio_float(before_quote, before_base)
        impact = _normalized_impact(side, _ratio_float(average_num, average_den), before)
    _positive("post virtual base reserve", post_base)
    _positive("post virtual quote reserve", post_quote)
    return AMMQuoteLevel(
        side=side,
        requested_quote_amount=requested_quote,
        input_amount=input_amount,
        output_amount=output_amount,
        base_amount=base_amount,
        quote_amount=quote_amount,
        curve_quote_amount=curve_quote_amount,
        fee_quote_amount=fee_quote_amount,
        fee_components=fee_components,
        average_price_numerator=average_num,
        average_price_denominator=average_den,
        marginal_price_before_numerator=before_quote,
        marginal_price_before_denominator=before_base,
        marginal_price_after_numerator=post_quote,
        marginal_price_after_denominator=post_base,
        impact_bps=impact,
        math_mode=math_mode,
        target_slippage_bps=target_slippage_bps,
    )


def _exact_buy_level(
    event: SwapEvent,
    phase: VenuePhase,
    config: AMMQuoteConfig,
    gross_quote: int,
) -> AMMQuoteLevel:
    vb = int(event.virtual_base_reserve)
    vq = int(event.virtual_quote_reserve)
    real_base = int(event.real_base_reserve)
    if phase is VenuePhase.BONDING_CURVE:
        pump = _pump_config(config)
        if pump is None:
            raise ValueError("complete Pump protocol and creator fee config is required")
        quote = pump_buy_exact_quote_in(
            gross_quote_amount=gross_quote,
            virtual_base_reserve=vb,
            virtual_quote_reserve=vq,
            real_base_reserve=real_base,
            protocol_fee_bps=pump.protocol_fee_bps,
            creator_fee_bps=pump.creator_fee_bps,
            creator_fee_enabled=pump.creator_fee_enabled,
        )
        base_out = quote.base_amount
        curve_quote = quote.curve_quote_amount
        user_quote = quote.user_quote_amount
        fee_components: tuple[tuple[str, Number], ...] = (
            ("protocol", quote.protocol_fee_quote),
            ("creator", quote.creator_fee_quote),
        )
        fee = quote.total_fee_quote
    else:
        fees = _pumpswap_config(config)
        if fees is None:
            raise ValueError("complete PumpSwap fee component config is required")
        quote_pumpswap = pumpswap_buy_exact_quote_in(
            gross_quote_amount=gross_quote,
            base_reserve=vb,
            quote_reserve=vq,
            fees=fees,
        )
        base_out = quote_pumpswap.base_amount
        if base_out > real_base:
            raise ValueError("buy exceeds real base inventory")
        curve_quote = quote_pumpswap.curve_quote_amount
        user_quote = quote_pumpswap.user_quote_amount
        fee_components = quote_pumpswap.fee_components
        fee = quote_pumpswap.total_fee_quote
    if base_out <= 0 or user_quote <= 0:
        raise ValueError("quote amount produces no executable base output")
    post_base = vb - base_out
    post_quote = vq + curve_quote
    return _make_level(
        event=event,
        side=Side.BUY,
        requested_quote=gross_quote,
        input_amount=user_quote,
        output_amount=base_out,
        base_amount=base_out,
        quote_amount=user_quote,
        curve_quote_amount=curve_quote,
        fee_quote_amount=fee,
        fee_components=fee_components,
        post_base=post_base,
        post_quote=post_quote,
        math_mode=MATH_MODE_INTEGER_EXACT,
    )


def _exact_sell_level(
    event: SwapEvent,
    phase: VenuePhase,
    config: AMMQuoteConfig,
    requested_quote: int,
) -> AMMQuoteLevel:
    vb = int(event.virtual_base_reserve)
    vq = int(event.virtual_quote_reserve)
    # A quote notional is converted to the nearest executable whole base unit
    # at the pre-event marginal price.  Ceil keeps the requested notional from
    # silently becoming a smaller sell because of integer division.
    base_in = (requested_quote * vb + vq - 1) // vq
    if base_in <= 0:
        raise ValueError("quote notional is below one atomic base unit")
    fee_components: tuple[tuple[str, Number], ...]
    if phase is VenuePhase.BONDING_CURVE:
        pump = _pump_config(config)
        if pump is None:
            raise ValueError("complete Pump protocol and creator fee config is required")
        quote = pump_sell_exact_base(
            base_amount=base_in,
            virtual_base_reserve=vb,
            virtual_quote_reserve=vq,
            real_quote_reserve=int(event.real_quote_reserve),
            protocol_fee_bps=pump.protocol_fee_bps,
            creator_fee_bps=pump.creator_fee_bps,
            creator_fee_enabled=pump.creator_fee_enabled,
        )
        quote_out = quote.user_quote_amount
        curve_quote = quote.curve_quote_amount
        fee_components = (
            ("protocol", quote.protocol_fee_quote),
            ("creator", quote.creator_fee_quote),
        )
        fee = quote.total_fee_quote
    else:
        fees = _pumpswap_config(config)
        if fees is None:
            raise ValueError("complete PumpSwap fee component config is required")
        quote_pumpswap = pumpswap_sell_exact_base(
            base_amount=base_in,
            base_reserve=vb,
            quote_reserve=vq,
            fees=fees,
        )
        if quote_pumpswap.curve_quote_amount > int(event.real_quote_reserve):
            raise ValueError("sell exceeds real quote inventory")
        quote_out = quote_pumpswap.user_quote_amount
        curve_quote = quote_pumpswap.curve_quote_amount
        fee_components = quote_pumpswap.fee_components
        fee = quote_pumpswap.total_fee_quote
    if quote_out <= 0:
        raise ValueError("quote amount produces no executable quote output")
    post_base = vb + base_in
    post_quote = vq - curve_quote
    return _make_level(
        event=event,
        side=Side.SELL,
        requested_quote=requested_quote,
        input_amount=base_in,
        output_amount=quote_out,
        base_amount=base_in,
        quote_amount=quote_out,
        curve_quote_amount=curve_quote,
        fee_quote_amount=fee,
        fee_components=fee_components,
        post_base=post_base,
        post_quote=post_quote,
        math_mode=MATH_MODE_INTEGER_EXACT,
    )


def _normal_buy_level(
    event: SwapEvent,
    phase: VenuePhase,
    fee_bps: float,
    gross_quote: float,
) -> AMMQuoteLevel:
    base_out, gross, curve, fee, components = _normal_buy(
        phase=phase,
        event=event,
        gross_quote=gross_quote,
        fee_bps=fee_bps,
    )
    vb = float(event.virtual_base_reserve)
    vq = float(event.virtual_quote_reserve)
    return _make_level(
        event=event,
        side=Side.BUY,
        requested_quote=gross_quote,
        input_amount=gross,
        output_amount=base_out,
        base_amount=base_out,
        quote_amount=gross,
        curve_quote_amount=curve,
        fee_quote_amount=fee,
        fee_components=components,
        post_base=vb - base_out,
        post_quote=vq + curve,
        math_mode=MATH_MODE_NORMALIZED_APPROXIMATION,
    )


def _normal_sell_level(
    event: SwapEvent,
    fee_bps: float,
    requested_quote: float,
) -> AMMQuoteLevel:
    vb = float(event.virtual_base_reserve)
    vq = float(event.virtual_quote_reserve)
    marginal = vq / vb
    base_in = requested_quote / marginal
    base_in, quote_out, curve, fee, components = _normal_sell(
        event=event,
        base_in=base_in,
        fee_bps=fee_bps,
    )
    return _make_level(
        event=event,
        side=Side.SELL,
        requested_quote=requested_quote,
        input_amount=base_in,
        output_amount=quote_out,
        base_amount=base_in,
        quote_amount=quote_out,
        curve_quote_amount=curve,
        fee_quote_amount=fee,
        fee_components=components,
        post_base=vb + base_in,
        post_quote=vq - curve,
        math_mode=MATH_MODE_NORMALIZED_APPROXIMATION,
    )


def _exact_bucket_buy(
    event: SwapEvent,
    phase: VenuePhase,
    config: AMMQuoteConfig,
    target_bps: float,
) -> AMMQuoteLevel:
    vb = int(event.virtual_base_reserve)
    if phase is VenuePhase.BONDING_CURVE:
        pump = _pump_config(config)
        if pump is None:
            raise ValueError("complete Pump fee config is required")
        base_limit = min(vb - 1, int(event.real_base_reserve))
        if base_limit <= 0:
            raise ValueError("no real base inventory for a buy bucket")
        upper = pump_buy_exact_base(
            base_amount=base_limit,
            virtual_base_reserve=vb,
            virtual_quote_reserve=int(event.virtual_quote_reserve),
            real_base_reserve=int(event.real_base_reserve),
            protocol_fee_bps=pump.protocol_fee_bps,
            creator_fee_bps=pump.creator_fee_bps,
            creator_fee_enabled=pump.creator_fee_enabled,
        ).user_quote_amount
    else:
        fees = _pumpswap_config(config)
        if fees is None:
            raise ValueError("complete PumpSwap fee config is required")
        base_limit = min(vb - 1, int(event.real_base_reserve))
        if base_limit <= 0:
            raise ValueError("no real base inventory for a buy bucket")
        upper = pumpswap_buy_exact_base(
            base_amount=base_limit,
            base_reserve=vb,
            quote_reserve=int(event.virtual_quote_reserve),
            fees=fees,
        ).user_quote_amount
    if upper <= 0:
        raise ValueError("buy bucket has no executable notional")
    low, high, best = 1, min(upper, U64_MAX), 0
    while low <= high:
        middle = (low + high) // 2
        try:
            level = _exact_buy_level(event, phase, config, middle)
        except ValueError:
            high = middle - 1
            continue
        if level.impact_bps <= target_bps:
            best = middle
            low = middle + 1
        else:
            high = middle - 1
    if best <= 0:
        raise ValueError("slippage bucket is below the minimum executable buy impact")
    level = _exact_buy_level(event, phase, config, best)
    return replace(level, target_slippage_bps=target_bps)


def _exact_bucket_sell(
    event: SwapEvent,
    phase: VenuePhase,
    config: AMMQuoteConfig,
    target_bps: float,
) -> AMMQuoteLevel:
    low, high, best = 1, U64_MAX, 0
    while low <= high:
        middle = (low + high) // 2
        try:
            level = _exact_sell_level(event, phase, config, middle)
        except ValueError:
            high = middle - 1
            continue
        if level.impact_bps <= target_bps:
            best = middle
            low = middle + 1
        else:
            high = middle - 1
    if best <= 0:
        raise ValueError("slippage bucket is below the minimum executable sell impact")
    level = _exact_sell_level(event, phase, config, best)
    return replace(level, target_slippage_bps=target_bps)


def _normal_bucket(
    event: SwapEvent,
    phase: VenuePhase,
    fee_bps: float,
    side: Side,
    target_bps: float,
) -> AMMQuoteLevel:
    vb = float(event.virtual_base_reserve)
    vq = float(event.virtual_quote_reserve)
    upper = max(vq * 1_000_000.0, 1.0) if side is Side.BUY else max(vb * 1_000_000.0, 1.0)
    low, high, best = 1e-12, upper, 0.0
    for _ in range(96):
        middle = (low + high) / 2.0
        try:
            level = (
                _normal_buy_level(event, phase, fee_bps, middle)
                if side is Side.BUY
                else _normal_sell_level(event, fee_bps, middle)
            )
        except ValueError:
            high = middle
            continue
        if level.impact_bps <= target_bps:
            best = middle
            low = middle
        else:
            high = middle
    if best <= 0:
        raise ValueError("slippage bucket is below the minimum executable impact")
    level = (
        _normal_buy_level(event, phase, fee_bps, best)
        if side is Side.BUY
        else _normal_sell_level(event, fee_bps, best)
    )
    return AMMQuoteLevel(
        side=level.side,
        requested_quote_amount=level.requested_quote_amount,
        input_amount=level.input_amount,
        output_amount=level.output_amount,
        base_amount=level.base_amount,
        quote_amount=level.quote_amount,
        curve_quote_amount=level.curve_quote_amount,
        fee_quote_amount=level.fee_quote_amount,
        fee_components=level.fee_components,
        average_price_numerator=level.average_price_numerator,
        average_price_denominator=level.average_price_denominator,
        marginal_price_before_numerator=level.marginal_price_before_numerator,
        marginal_price_before_denominator=level.marginal_price_before_denominator,
        marginal_price_after_numerator=level.marginal_price_after_numerator,
        marginal_price_after_denominator=level.marginal_price_after_denominator,
        impact_bps=level.impact_bps,
        math_mode=level.math_mode,
        target_slippage_bps=target_bps,
    )


@dataclass(frozen=True, slots=True)
class AMMStateSnapshot:
    """Immutable post-event AMM state and its reconstructed quote surface."""

    event_id: str
    market_id: str
    mint: str
    quote_mint: str
    slot: int
    tx_index: int
    instruction_index: int
    phase: VenuePhase
    virtual_base_reserve: Number
    virtual_quote_reserve: Number
    real_base_reserve: Number
    real_quote_reserve: Number
    ordering_key: tuple[int, int, int, str]
    quotes: tuple[AMMQuoteLevel, ...]
    quote_status: QuoteStatus
    quote_reason: str | None
    unquoted_requests: tuple[Number, ...]
    spread_bps: float | None
    labels: tuple[str, str, str] = AMM_SURFACE_LABELS
    block_time_ns: int = 0
    observed_at_ns: int = 0
    signature: str | None = None
    program_version: str | None = None
    base_decimals: int | None = None
    quote_decimals: int | None = None

    def __post_init__(self) -> None:
        for name in ("event_id", "market_id", "mint", "quote_mint"):
            _text(name, getattr(self, name))
        if self.mint == self.quote_mint:
            raise ValueError("mint and quote_mint must differ")
        if not isinstance(self.phase, VenuePhase):
            raise ValueError("phase must be a VenuePhase")
        for name in ("slot", "tx_index", "instruction_index", "block_time_ns", "observed_at_ns"):
            if isinstance(getattr(self, name), bool) or not isinstance(getattr(self, name), int):
                raise ValueError(f"{name} must be a nonnegative integer")
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if self.observed_at_ns < self.block_time_ns:
            raise ValueError("observed_at_ns cannot precede block_time_ns")
        for name in (
            "virtual_base_reserve",
            "virtual_quote_reserve",
            "real_base_reserve",
            "real_quote_reserve",
        ):
            _nonnegative(name, getattr(self, name))
        _positive("virtual_base_reserve", self.virtual_base_reserve)
        _positive("virtual_quote_reserve", self.virtual_quote_reserve)
        if self.ordering_key != (
            self.slot,
            self.tx_index,
            self.instruction_index,
            self.event_id,
        ):
            raise ValueError("ordering_key does not match snapshot identity")
        if not isinstance(self.quotes, tuple):
            raise ValueError("quotes must be an immutable tuple")
        if not isinstance(self.quote_status, QuoteStatus):
            raise ValueError("quote_status must be a QuoteStatus")
        if self.quote_status is QuoteStatus.QUOTED and not self.quotes:
            raise ValueError("quoted snapshot must contain at least one quote")
        if self.quote_status is not QuoteStatus.QUOTED and self.quotes:
            raise ValueError("unquoted snapshot cannot contain quote levels")
        if self.quote_reason is not None:
            _text("quote_reason", self.quote_reason)
        if self.labels != AMM_SURFACE_LABELS:
            raise ValueError("unsupported AMM surface labels")
        for name in ("signature", "program_version"):
            value = getattr(self, name)
            if value is not None:
                _text(name, value)
        for name in ("base_decimals", "quote_decimals"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 30
            ):
                raise ValueError(f"{name} must be an integer in [0, 30]")
        if self.spread_bps is not None:
            _nonnegative("spread_bps", self.spread_bps)

    @property
    def venue(self) -> VenuePhase:
        return self.phase

    @property
    def venue_name(self) -> str:
        return self.phase.value

    @property
    def source_ordering_key(self) -> tuple[int, int, int, str]:
        return self.ordering_key

    @property
    def ordering_identity(self) -> tuple[int, int, int, str]:
        return self.ordering_key

    @property
    def marginal_price_numerator(self) -> Number:
        return self.virtual_quote_reserve

    @property
    def marginal_price_denominator(self) -> Number:
        return self.virtual_base_reserve

    @property
    def marginal_price(self) -> float:
        return _ratio_float(self.virtual_quote_reserve, self.virtual_base_reserve)

    @property
    def marginal_price_ratio(self) -> tuple[Number, Number]:
        return (self.virtual_quote_reserve, self.virtual_base_reserve)

    @property
    def quote_surface(self) -> tuple[AMMQuoteLevel, ...]:
        return self.quotes

    @property
    def buy_quotes(self) -> tuple[AMMQuoteLevel, ...]:
        return tuple(level for level in self.quotes if level.side is Side.BUY)

    @property
    def sell_quotes(self) -> tuple[AMMQuoteLevel, ...]:
        return tuple(level for level in self.quotes if level.side is Side.SELL)

    @property
    def depth(self) -> tuple[AMMQuoteLevel, ...]:
        return self.quotes

    @property
    def is_quoted(self) -> bool:
        return self.quote_status is QuoteStatus.QUOTED

    @property
    def is_unquotable(self) -> bool:
        return self.quote_status is not QuoteStatus.QUOTED


def _phase_key(value: object) -> str | VenuePhase:
    if isinstance(value, VenuePhase):
        return value
    if isinstance(value, str):
        try:
            return VenuePhase(value)
        except ValueError:
            return value
    raise ValueError("fee config mapping keys must be phase strings or VenuePhase values")


class AMMStateReconstructor:
    """Stateful streaming reconstructor for one or more market IDs.

    ``consume`` can be called repeatedly with successive chain-ordered
    batches.  The ordering cursor and identity index are retained between
    calls, so a later batch cannot silently replay or rewind state.
    """

    def __init__(
        self,
        *,
        quote_notionals: Iterable[Number] | None = None,
        slippage_buckets_bps: Iterable[Number] | None = None,
        fee_config: FeeConfig | None = None,
        fee_configs: Mapping[object, object] | None = None,
        fee_config_resolver: FeeConfigResolver | None = None,
        track_event_ids: bool = True,
        track_market_identities: bool = True,
    ) -> None:
        if quote_notionals is not None and slippage_buckets_bps is not None:
            raise ValueError("pass quote_notionals or slippage_buckets_bps, not both")
        self.quote_notionals = (
            None if quote_notionals is None else _strict_amounts(quote_notionals, "quote_notionals")
        )
        self.slippage_buckets_bps = (
            None
            if slippage_buckets_bps is None
            else tuple(
                float(value)
                for value in _strict_amounts(slippage_buckets_bps, "slippage_buckets_bps")
            )
        )
        self._fee_config = _coerce_config(fee_config)
        self._fee_configs: Mapping[object, object] = MappingProxyType(
            {} if fee_configs is None else dict(fee_configs)
        )
        if fee_config_resolver is not None and not callable(fee_config_resolver):
            raise ValueError("fee_config_resolver must be callable")
        if not isinstance(track_event_ids, bool):
            raise ValueError("track_event_ids must be boolean")
        if not isinstance(track_market_identities, bool):
            raise ValueError("track_market_identities must be boolean")
        self._fee_config_resolver = fee_config_resolver
        self._track_event_ids = track_event_ids
        self._track_market_identities = track_market_identities
        self._previous_ordering_key: tuple[int, int, int, str] | None = None
        self._seen_event_ids: set[str] | None = set() if track_event_ids else None
        self._identities: dict[str, tuple[str, str]] | None = (
            {} if track_market_identities else None
        )

    @property
    def previous_ordering_key(self) -> tuple[int, int, int, str] | None:
        return self._previous_ordering_key

    @property
    def market_identities(self) -> Mapping[str, tuple[str, str]]:
        return MappingProxyType(dict(self._identities or {}))

    def reset(self) -> None:
        """Clear ordering and identity state while retaining quote config."""

        self._previous_ordering_key = None
        if self._seen_event_ids is not None:
            self._seen_event_ids.clear()
        if self._identities is not None:
            self._identities.clear()

    def _resolve_config(self, event: SwapEvent) -> AMMQuoteConfig | None:
        if self._fee_config_resolver is not None:
            return _coerce_config(self._fee_config_resolver(event))
        if not self._fee_configs:
            return self._fee_config
        direct: object | None = None
        for key in (event.market_id, event.phase, event.phase.value, "*"):
            if key in self._fee_configs:
                direct = self._fee_configs[key]
                break
        if direct is None:
            return self._fee_config
        if isinstance(direct, Mapping):
            nested = None
            for key in (event.phase, event.phase.value, "*"):
                if key in direct:
                    nested = direct[key]
                    break
            direct = nested
        return _coerce_config(direct)

    def _build_quotes(
        self,
        event: SwapEvent,
    ) -> tuple[
        tuple[AMMQuoteLevel, ...],
        QuoteStatus,
        str | None,
        tuple[Number, ...],
    ]:
        if self.quote_notionals is None and self.slippage_buckets_bps is None:
            return (), QuoteStatus.NOT_REQUESTED, None, ()
        if event.phase is VenuePhase.CLOB:
            return (
                (),
                QuoteStatus.UNSUPPORTED_PHASE,
                "CLOB events are not AMM state and no observed L2 is reconstructed",
                tuple(self.quote_notionals or self.slippage_buckets_bps or ()),
            )
        config = self._resolve_config(event)
        if config is None:
            return (
                (),
                QuoteStatus.UNKNOWN_CONFIG,
                f"fee configuration is unavailable for {event.phase.value}",
                tuple(self.quote_notionals or self.slippage_buckets_bps or ()),
            )
        # Slippage buckets are targets, not protocol amounts; their generated
        # notionals are searched in atomic units when the state/config allows
        # exact math.  Do not reject an otherwise atomic state merely because
        # the target itself is a float number of basis points.
        requests: Iterable[Number] = self.quote_notionals or ()
        exact = _exact_input_is_available(event, requests, config)
        complete_exact_config = (
            _pump_config(config) is not None
            if event.phase is VenuePhase.BONDING_CURVE
            else _pumpswap_config(config) is not None
        )
        # An aggregate fee is an intentional normalized fallback.  It cannot
        # be mistaken for a complete integer protocol config merely because a
        # normalized event happens to contain whole-valued floats/integers.
        if config.atomic is None and config.fee_bps is not None and not complete_exact_config:
            exact = False
        if config.atomic is True and not exact:
            return (
                (),
                QuoteStatus.UNQUOTABLE,
                "atomic quote config requires integer reserves and notionals",
                tuple(requests),
            )
        if config.atomic is not False and exact:
            mode = MATH_MODE_INTEGER_EXACT
        else:
            mode = MATH_MODE_NORMALIZED_APPROXIMATION
        if mode == MATH_MODE_INTEGER_EXACT:
            if event.phase is VenuePhase.BONDING_CURVE and _pump_config(config) is None:
                return (
                    (),
                    QuoteStatus.UNKNOWN_CONFIG,
                    "complete Pump protocol/creator fee config is unavailable",
                    tuple(requests),
                )
            if event.phase is VenuePhase.PUMPSWAP and _pumpswap_config(config) is None:
                return (
                    (),
                    QuoteStatus.UNKNOWN_CONFIG,
                    "complete PumpSwap fee component config is unavailable",
                    tuple(requests),
                )
        fee_bps = _aggregate_fee_bps(config)
        if mode == MATH_MODE_NORMALIZED_APPROXIMATION and fee_bps is None:
            return (
                (),
                QuoteStatus.UNKNOWN_CONFIG,
                "normalized quotes require an explicit aggregate fee_bps",
                tuple(requests),
            )
        levels: list[AMMQuoteLevel] = []
        errors: list[str] = []
        unquoted: list[Number] = []
        if self.quote_notionals is not None:
            for requested in self.quote_notionals:
                try:
                    if mode == MATH_MODE_INTEGER_EXACT:
                        amount = int(requested)
                        buy = _exact_buy_level(event, event.phase, config, amount)
                        sell = _exact_sell_level(event, event.phase, config, amount)
                    else:
                        amount_float = float(requested)
                        buy = _normal_buy_level(
                            event, event.phase, cast(float, fee_bps), amount_float
                        )
                        sell = _normal_sell_level(event, cast(float, fee_bps), amount_float)
                    levels.extend((buy, sell))
                except (OverflowError, ValueError) as error:
                    unquoted.append(requested)
                    errors.append(f"{requested!r}: {error}")
        else:
            for target in self.slippage_buckets_bps or ():
                try:
                    if mode == MATH_MODE_INTEGER_EXACT:
                        buy = _exact_bucket_buy(event, event.phase, config, target)
                        sell = _exact_bucket_sell(event, event.phase, config, target)
                    else:
                        buy = _normal_bucket(
                            event,
                            event.phase,
                            cast(float, fee_bps),
                            Side.BUY,
                            target,
                        )
                        sell = _normal_bucket(
                            event,
                            event.phase,
                            cast(float, fee_bps),
                            Side.SELL,
                            target,
                        )
                    levels.extend((buy, sell))
                except (OverflowError, ValueError) as error:
                    unquoted.append(target)
                    errors.append(f"slippage {target!r}: {error}")
        if not levels:
            failure_reason = "; ".join(errors) if errors else "all requested levels are unquotable"
            return (), QuoteStatus.UNQUOTABLE, failure_reason, tuple(unquoted)
        partial_reason: str | None = "; ".join(errors) if errors else None
        return tuple(levels), QuoteStatus.QUOTED, partial_reason, tuple(unquoted)

    def _snapshot(self, event: SwapEvent) -> AMMStateSnapshot:
        if not isinstance(event, SwapEvent):
            raise TypeError("events must contain only SwapEvent instances")
        if self._seen_event_ids is not None and event.event_id in self._seen_event_ids:
            raise ValueError(f"duplicate event_id: {event.event_id}")
        key = event.ordering_key
        if self._previous_ordering_key is not None and key <= self._previous_ordering_key:
            raise ValueError(
                "events must be strictly increasing in ordering_key: "
                f"{key!r} follows {self._previous_ordering_key!r}"
            )
        identity = (event.mint, event.quote_mint)
        if self._identities is not None:
            prior = self._identities.get(event.market_id)
            if prior is not None and prior != identity:
                raise ValueError(f"inconsistent mint/quote identity for market {event.market_id}")
            self._identities.setdefault(event.market_id, identity)
        if self._seen_event_ids is not None:
            self._seen_event_ids.add(event.event_id)
        quotes, status, reason, unquoted = self._build_quotes(event)
        spread: float | None = None
        buys = tuple(level for level in quotes if level.side is Side.BUY)
        sells = tuple(level for level in quotes if level.side is Side.SELL)
        if buys and sells:
            pairs = zip(buys, sells, strict=False)
            spreads = [
                ((buy.average_price / sell.average_price) - 1.0) * 10_000.0 for buy, sell in pairs
            ]
            spread = max(spreads)
        self._previous_ordering_key = key
        return AMMStateSnapshot(
            event_id=event.event_id,
            market_id=event.market_id,
            mint=event.mint,
            quote_mint=event.quote_mint,
            slot=event.slot,
            tx_index=event.tx_index,
            instruction_index=event.instruction_index,
            phase=event.phase,
            virtual_base_reserve=event.virtual_base_reserve,
            virtual_quote_reserve=event.virtual_quote_reserve,
            real_base_reserve=event.real_base_reserve,
            real_quote_reserve=event.real_quote_reserve,
            ordering_key=key,
            quotes=quotes,
            quote_status=status,
            quote_reason=reason,
            unquoted_requests=unquoted,
            spread_bps=spread,
            block_time_ns=event.block_time_ns,
            observed_at_ns=event.observed_at_ns,
            signature=event.signature,
            program_version=event.program_version,
            base_decimals=event.base_decimals,
            quote_decimals=event.quote_decimals,
        )

    def consume(self, events: Iterable[SwapEvent]) -> Iterator[AMMStateSnapshot]:
        """Yield one deterministic post-event snapshot per input event."""

        for event in events:
            yield self._snapshot(event)

    def __call__(self, events: Iterable[SwapEvent]) -> Iterator[AMMStateSnapshot]:
        return self.consume(events)


def iter_amm_state_snapshots(
    events: Iterable[SwapEvent],
    *,
    quote_notionals: Iterable[Number] | None = None,
    slippage_buckets_bps: Iterable[Number] | None = None,
    fee_config: FeeConfig | None = None,
    fee_configs: Mapping[object, object] | None = None,
    fee_config_resolver: FeeConfigResolver | None = None,
    track_event_ids: bool = True,
    track_market_identities: bool = True,
) -> Iterator[AMMStateSnapshot]:
    """Stream post-event AMM snapshots without materializing ``events``."""

    reconstructor = AMMStateReconstructor(
        quote_notionals=quote_notionals,
        slippage_buckets_bps=slippage_buckets_bps,
        fee_config=fee_config,
        fee_configs=fee_configs,
        fee_config_resolver=fee_config_resolver,
        track_event_ids=track_event_ids,
        track_market_identities=track_market_identities,
    )
    return reconstructor.consume(events)


def reconstruct_amm_states(
    events: Iterable[SwapEvent],
    *,
    quote_notionals: Iterable[Number] | None = None,
    slippage_buckets_bps: Iterable[Number] | None = None,
    fee_config: FeeConfig | None = None,
    fee_configs: Mapping[object, object] | None = None,
    fee_config_resolver: FeeConfigResolver | None = None,
    track_event_ids: bool = True,
    track_market_identities: bool = True,
) -> list[AMMStateSnapshot]:
    """Materialize a snapshot list for small callers; streaming is authoritative."""

    return list(
        iter_amm_state_snapshots(
            events,
            quote_notionals=quote_notionals,
            slippage_buckets_bps=slippage_buckets_bps,
            fee_config=fee_config,
            fee_configs=fee_configs,
            fee_config_resolver=fee_config_resolver,
            track_event_ids=track_event_ids,
            track_market_identities=track_market_identities,
        )
    )


# Friendly aliases used by callers that spell AMM as ``Amm`` or prefer the
# word ``surface`` in a reconstruction function name.
AmmStateSnapshot = AMMStateSnapshot
AmmQuoteLevel = AMMQuoteLevel
AmmStateReconstructor = AMMStateReconstructor
AmmQuoteConfig = AMMQuoteConfig
AMMReconstructor = AMMStateReconstructor
iter_amm_reconstruction = iter_amm_state_snapshots
reconstruct_amm_state_stream = iter_amm_state_snapshots
build_amm_quote_surface = iter_amm_state_snapshots


__all__ = [
    "AMM_SURFACE_LABELS",
    "AMMQuoteConfig",
    "AMMQuoteLevel",
    "AMMQuoteStatus",
    "AMMReconstructor",
    "AMMStateReconstructor",
    "AMMStateSnapshot",
    "AmmQuoteConfig",
    "AmmQuoteLevel",
    "AmmStateReconstructor",
    "AmmStateSnapshot",
    "FeeConfig",
    "FeeConfigResolver",
    "MATH_MODE_INTEGER_EXACT",
    "MATH_MODE_NORMALIZED_APPROXIMATION",
    "NOT_OBSERVED_ORDERBOOK",
    "NOT_TRUE_L2",
    "PumpFeeConfig",
    "PumpSwapAMMConfig",
    "QuoteStatus",
    "RECONSTRUCTED_EXECUTABLE_SURFACE",
    "build_amm_quote_surface",
    "iter_amm_reconstruction",
    "iter_amm_state_snapshots",
    "reconstruct_amm_state_stream",
    "reconstruct_amm_states",
]
