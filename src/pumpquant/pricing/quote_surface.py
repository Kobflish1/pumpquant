"""Integer-only, provenance-aware PumpSwap executable quote surfaces.

PumpSwap is an AMM, not an order book.  This module exposes a small adapter
for reconstructing *executable* hypothetical levels from one immutable pool
snapshot.  Every amount is a Solana atomic integer and all arithmetic is
delegated to :mod:`pumpquant.pumpswap_math`; there is deliberately no decimal
normalisation or approximate price calculation here.

The state key is the chain ordering key at which a snapshot was obtained.  A
caller replaying a stream may pass an ``as_of`` key to any quote builder; a
key before the snapshot is rejected.  Keeping the key on every level and on
the returned surface makes it difficult to accidentally use post-trade state
for an earlier instruction.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Final, TypeAlias, cast

from pumpquant.pricing.pumpswap import (
    U64_MAX,
    PumpSwapFees,
    pumpswap_buy_exact_quote_in,
    pumpswap_sell_exact_base,
)
from pumpquant.pricing.pumpswap import (
    PumpSwapQuote as MathPumpSwapQuote,
)

# These labels are intentionally stable.  They are capability statements,
# not marketing names: the result is reconstructed from AMM state and does
# not claim an observed order book or true L2 feed.
RECONSTRUCTED_EXECUTABLE_SURFACE: Final[str] = "reconstructed_executable_surface"
NOT_OBSERVED_ORDERBOOK: Final[str] = "not_observed_orderbook"
NOT_TRUE_L2: Final[str] = "not_true_l2"
SURFACE_LABELS: Final[tuple[str, str, str]] = (
    RECONSTRUCTED_EXECUTABLE_SURFACE,
    NOT_OBSERVED_ORDERBOOK,
    NOT_TRUE_L2,
)

# Revision values are part of the replay contract.  A caller may not silently
# use a state decoded with an unknown account layout or math implementation.
SCHEMA_REVISION: Final[str] = "pumpswap_quote_surface_v1"
LAYOUT_REVISION: Final[str] = "pumpswap_pool_state_layout_v1"
MATH_REVISION: Final[str] = "pumpswap_math_v1"
SUPPORTED_SCHEMA_REVISIONS: Final[frozenset[str]] = frozenset({SCHEMA_REVISION})
SUPPORTED_LAYOUT_REVISIONS: Final[frozenset[str]] = frozenset({LAYOUT_REVISION})
SUPPORTED_MATH_REVISIONS: Final[frozenset[str]] = frozenset({MATH_REVISION})

SUPPORTED_PATH: Final[str] = "constant_product"
SUPPORTED_FEE_SOURCE: Final[str] = "explicit"

AvailabilityKey: TypeAlias = (
    "PumpSwapOrderingKey | tuple[int, int, int] | tuple[int, int, int, str]"
)


def _nonnegative_u64(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be a nonnegative u64 integer")
    if value < 0 or value > U64_MAX:
        raise ValueError(f"{name} must be a nonnegative u64 integer")


def _positive_u64(name: str, value: object) -> None:
    _nonnegative_u64(name, value)
    if value == 0:
        raise ValueError(f"{name} must be positive")


def _text(name: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _revision(name: str, value: object, supported: frozenset[str]) -> None:
    _text(name, value)
    if value not in supported:
        raise ValueError(f"unsupported {name}: {value!r}")


@dataclass(frozen=True, slots=True, order=True)
class PumpSwapOrderingKey:
    """Deterministic Solana ordering key for a source state or observation."""

    slot: int
    tx_index: int = 0
    instruction_index: int = 0
    event_id: str = ""

    def __post_init__(self) -> None:
        _nonnegative_u64("slot", self.slot)
        _nonnegative_u64("tx_index", self.tx_index)
        _nonnegative_u64("instruction_index", self.instruction_index)
        _text("event_id", self.event_id) if self.event_id else None

    @property
    def transaction_index(self) -> int:
        """Alias matching data-source terminology."""

        return self.tx_index

    @property
    def source_ordering_key(self) -> tuple[int, int, int, str]:
        return (self.slot, self.tx_index, self.instruction_index, self.event_id)


def _coerce_ordering_key(value: object, name: str = "availability_key") -> PumpSwapOrderingKey:
    if isinstance(value, PumpSwapOrderingKey):
        return value
    if isinstance(value, tuple) and len(value) in (3, 4):
        slot = value[0]
        tx_index = value[1]
        instruction_index = value[2]
        event_id: object = value[3] if len(value) == 4 else ""
        if (
            isinstance(slot, bool)
            or not isinstance(slot, int)
            or isinstance(tx_index, bool)
            or not isinstance(tx_index, int)
            or isinstance(instruction_index, bool)
            or not isinstance(instruction_index, int)
            or not isinstance(event_id, str)
        ):
            raise ValueError(f"{name} must contain integer slot/order fields")
        return PumpSwapOrderingKey(slot, tx_index, instruction_index, event_id)
    raise ValueError(
        f"{name} must be PumpSwapOrderingKey or a "
        "(slot, tx_index, instruction_index[, event_id]) tuple"
    )


@dataclass(frozen=True, slots=True)
class PumpSwapStateProvenance:
    """Pinned source metadata for one immutable post-state snapshot."""

    ordering_key: PumpSwapOrderingKey | tuple[int, int, int] | tuple[int, int, int, str]
    schema_revision: str = SCHEMA_REVISION
    layout_revision: str = LAYOUT_REVISION
    math_revision: str = MATH_REVISION
    source: str = "post_state"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "ordering_key",
            _coerce_ordering_key(self.ordering_key, "ordering_key"),
        )
        _revision("schema_revision", self.schema_revision, SUPPORTED_SCHEMA_REVISIONS)
        _revision("layout_revision", self.layout_revision, SUPPORTED_LAYOUT_REVISIONS)
        _revision("math_revision", self.math_revision, SUPPORTED_MATH_REVISIONS)
        _text("source", self.source)
        if self.source != "post_state":
            raise ValueError("unsupported PumpSwap state provenance source")

    @property
    def availability_key(self) -> PumpSwapOrderingKey:
        return cast(PumpSwapOrderingKey, self.ordering_key)

    @property
    def source_ordering_key(self) -> PumpSwapOrderingKey:
        return cast(PumpSwapOrderingKey, self.ordering_key)


@dataclass(frozen=True, slots=True)
class PumpSwapPoolState:
    """Validated integer PumpSwap reserves, fees, and source ordering."""

    base_reserve: int
    quote_reserve: int
    fees: PumpSwapFees = PumpSwapFees()
    real_base_reserve: int | None = None
    real_quote_reserve: int | None = None
    slot: int = 0
    tx_index: int = 0
    instruction_index: int = 0
    event_id: str = ""
    availability_key: (
        PumpSwapOrderingKey | tuple[int, int, int] | tuple[int, int, int, str] | None
    ) = None
    provenance: PumpSwapStateProvenance | None = None
    schema_revision: str = SCHEMA_REVISION
    layout_revision: str = LAYOUT_REVISION
    math_revision: str = MATH_REVISION
    path: str = SUPPORTED_PATH
    fee_source: str = SUPPORTED_FEE_SOURCE
    fee_config: PumpSwapFees | None = None
    lp_fee_bps: int | None = None
    protocol_fee_bps: int | None = None
    creator_fee_bps: int | None = None
    cashback_fee_bps: int | None = None
    buyback_fee_bps: int | None = None
    # Constructor alias used by some transaction archives.  The canonical
    # stored field remains tx_index, while this accepts a named alias.
    transaction_index: int | None = None
    source_ordering_key: (
        PumpSwapOrderingKey | tuple[int, int, int] | tuple[int, int, int, str] | None
    ) = None

    def __post_init__(self) -> None:
        _positive_u64("base_reserve", self.base_reserve)
        _positive_u64("quote_reserve", self.quote_reserve)
        component_values = (
            self.lp_fee_bps,
            self.protocol_fee_bps,
            self.creator_fee_bps,
            self.cashback_fee_bps,
            self.buyback_fee_bps,
        )
        if self.fee_config is not None and not isinstance(self.fee_config, PumpSwapFees):
            raise ValueError("fee_config must be a PumpSwapFees value")
        if not isinstance(self.fees, PumpSwapFees):
            raise ValueError("fees must be a PumpSwapFees value")
        if self.fee_config is not None:
            if self.fees != PumpSwapFees() and self.fees != self.fee_config:
                raise ValueError("fees and fee_config disagree")
            object.__setattr__(self, "fees", self.fee_config)
        if any(value is not None for value in component_values):
            components = PumpSwapFees(
                lp_fee_bps=self.lp_fee_bps or 0,
                protocol_fee_bps=self.protocol_fee_bps or 0,
                creator_fee_bps=self.creator_fee_bps or 0,
                cashback_fee_bps=self.cashback_fee_bps or 0,
                buyback_fee_bps=self.buyback_fee_bps or 0,
            )
            if self.fees != PumpSwapFees() and self.fees != components:
                raise ValueError("fees and individual fee basis points disagree")
            object.__setattr__(self, "fees", components)
            object.__setattr__(self, "fee_config", components)
        if self.fees.total_fee_bps > 10_000:
            raise ValueError("unsupported PumpSwap fees path: total fee exceeds 10000 bps")
        for name, value in (
            ("real_base_reserve", self.real_base_reserve),
            ("real_quote_reserve", self.real_quote_reserve),
        ):
            if value is not None:
                _nonnegative_u64(name, value)
        for name, value in (
            ("slot", self.slot),
            ("tx_index", self.tx_index),
            ("instruction_index", self.instruction_index),
        ):
            _nonnegative_u64(name, value)
        if self.transaction_index is not None:
            _nonnegative_u64("transaction_index", self.transaction_index)
            if self.tx_index != 0 and self.tx_index != self.transaction_index:
                raise ValueError("tx_index and transaction_index disagree")
            object.__setattr__(self, "tx_index", self.transaction_index)
        if self.event_id:
            _text("event_id", self.event_id)
        _revision("schema_revision", self.schema_revision, SUPPORTED_SCHEMA_REVISIONS)
        _revision("layout_revision", self.layout_revision, SUPPORTED_LAYOUT_REVISIONS)
        _revision("math_revision", self.math_revision, SUPPORTED_MATH_REVISIONS)
        if self.path != SUPPORTED_PATH:
            raise ValueError(f"unsupported PumpSwap quote path: {self.path!r}")
        if self.fee_source != SUPPORTED_FEE_SOURCE:
            raise ValueError(f"unsupported PumpSwap fee source: {self.fee_source!r}")

        direct_key = PumpSwapOrderingKey(
            self.slot,
            self.tx_index,
            self.instruction_index,
            self.event_id,
        )
        supplied_keys = [
            value
            for value in (self.availability_key, self.source_ordering_key)
            if value is not None
        ]
        if supplied_keys:
            key = _coerce_ordering_key(supplied_keys[0])
            if len(supplied_keys) == 2 and key != _coerce_ordering_key(supplied_keys[1]):
                raise ValueError("availability_key and source_ordering_key disagree")
            # Zero-valued direct fields are defaults, so an explicit key may
            # populate them.  Non-default direct ordering must agree exactly.
            if direct_key != PumpSwapOrderingKey(0, 0, 0, "") and direct_key != key:
                raise ValueError("state ordering fields disagree with availability_key")
            if direct_key == PumpSwapOrderingKey(0, 0, 0, ""):
                object.__setattr__(self, "slot", key.slot)
                object.__setattr__(self, "tx_index", key.tx_index)
                object.__setattr__(self, "instruction_index", key.instruction_index)
                object.__setattr__(self, "event_id", key.event_id)
        else:
            key = direct_key
        object.__setattr__(self, "availability_key", key)
        object.__setattr__(self, "source_ordering_key", key)
        if self.provenance is None:
            provenance = PumpSwapStateProvenance(
                ordering_key=key,
                schema_revision=self.schema_revision,
                layout_revision=self.layout_revision,
                math_revision=self.math_revision,
            )
            object.__setattr__(self, "provenance", provenance)
        else:
            if not isinstance(self.provenance, PumpSwapStateProvenance):
                raise ValueError("provenance must be a PumpSwapStateProvenance value")
            if self.provenance.ordering_key != key:
                raise ValueError("provenance ordering key disagrees with state")
            if (
                self.provenance.schema_revision != self.schema_revision
                or self.provenance.layout_revision != self.layout_revision
                or self.provenance.math_revision != self.math_revision
            ):
                raise ValueError("state revisions disagree with provenance revisions")

    @property
    def ordering_key(self) -> PumpSwapOrderingKey:
        return cast(PumpSwapOrderingKey, self.availability_key)

    @property
    def state_available_after(self) -> PumpSwapOrderingKey:
        """The first chain key at which this post-state may be used."""

        return cast(PumpSwapOrderingKey, self.availability_key)

    @property
    def virtual_base_reserve(self) -> int:
        return self.base_reserve

    @property
    def virtual_quote_reserve(self) -> int:
        return self.quote_reserve

    @property
    def schema_version(self) -> str:
        return self.schema_revision

    @property
    def layout_version(self) -> str:
        return self.layout_revision

    @property
    def math_version(self) -> str:
        return self.math_revision

    def is_available_at(self, as_of: AvailabilityKey) -> bool:
        # Provenance is explicitly a post-state snapshot. It cannot inform a
        # decision at the instruction that produced it; only a strictly later
        # chain ordering key may consume it.
        return _coerce_ordering_key(as_of, "as_of") > self.ordering_key

    def require_available_at(self, as_of: AvailabilityKey) -> None:
        if not self.is_available_at(as_of):
            raise ValueError(
                "PumpSwap state is unavailable before its source ordering key "
                f"{self.ordering_key.source_ordering_key!r}"
            )


@dataclass(frozen=True, slots=True)
class PumpSwapFeeBreakdown:
    """Exact quote-token fee components copied from a math quote."""

    lp_fee_quote: int
    protocol_fee_quote: int
    creator_fee_quote: int
    cashback_fee_quote: int
    buyback_fee_quote: int

    def __post_init__(self) -> None:
        for name in (
            "lp_fee_quote",
            "protocol_fee_quote",
            "creator_fee_quote",
            "cashback_fee_quote",
            "buyback_fee_quote",
        ):
            _nonnegative_u64(name, getattr(self, name))

    @property
    def total_fee_quote(self) -> int:
        return sum(
            (
                self.lp_fee_quote,
                self.protocol_fee_quote,
                self.creator_fee_quote,
                self.cashback_fee_quote,
                self.buyback_fee_quote,
            )
        )

    @property
    def total(self) -> int:
        return self.total_fee_quote

    @property
    def fee_components(self) -> tuple[tuple[str, int], ...]:
        return (
            ("lp", self.lp_fee_quote),
            ("protocol", self.protocol_fee_quote),
            ("creator", self.creator_fee_quote),
            ("cashback", self.cashback_fee_quote),
            ("buyback", self.buyback_fee_quote),
        )


class PumpSwapQuoteSide(StrEnum):
    """Base-token aggressor direction."""

    BUY = "buy"
    SELL = "sell"
    BUY_BASE = "buy"
    SELL_BASE = "sell"


def _side(value: object) -> PumpSwapQuoteSide:
    if isinstance(value, PumpSwapQuoteSide):
        return value
    if isinstance(value, str):
        try:
            return PumpSwapQuoteSide(value)
        except ValueError as error:
            raise ValueError("side must be 'buy' or 'sell'") from error
    raise ValueError("side must be a PumpSwapQuoteSide value")


@dataclass(frozen=True, slots=True)
class PumpSwapQuoteLevel:
    """One independent, executable hypothetical level."""

    side: PumpSwapQuoteSide
    requested_amount: int
    input_amount: int
    output_amount: int
    base_amount: int
    curve_quote_amount: int
    user_quote_amount: int
    fee_breakdown: PumpSwapFeeBreakdown
    average_price_numerator: int
    average_price_denominator: int
    availability_key: PumpSwapOrderingKey
    provenance: PumpSwapStateProvenance
    labels: tuple[str, str, str] = SURFACE_LABELS
    schema_revision: str = SCHEMA_REVISION
    layout_revision: str = LAYOUT_REVISION
    math_revision: str = MATH_REVISION

    def __post_init__(self) -> None:
        object.__setattr__(self, "side", _side(self.side))
        for name in (
            "requested_amount",
            "input_amount",
            "output_amount",
            "base_amount",
            "curve_quote_amount",
            "user_quote_amount",
            "average_price_numerator",
            "average_price_denominator",
        ):
            _positive_u64(name, getattr(self, name))
        if not isinstance(self.fee_breakdown, PumpSwapFeeBreakdown):
            raise ValueError("fee_breakdown must be a PumpSwapFeeBreakdown value")
        if self.side is PumpSwapQuoteSide.BUY:
            if (
                self.input_amount != self.user_quote_amount
                or self.output_amount != self.base_amount
            ):
                raise ValueError("buy level input/output does not match its PumpSwap quote")
        else:
            if (
                self.input_amount != self.base_amount
                or self.output_amount != self.user_quote_amount
            ):
                raise ValueError("sell level input/output does not match its PumpSwap quote")
        expected_numerator = (
            self.input_amount if self.side is PumpSwapQuoteSide.BUY else self.output_amount
        )
        expected_denominator = (
            self.output_amount if self.side is PumpSwapQuoteSide.BUY else self.input_amount
        )
        if self.average_price_numerator != expected_numerator:
            raise ValueError("average price numerator must be quote amount")
        if self.average_price_denominator != expected_denominator:
            raise ValueError("average price denominator must be base amount")
        if not isinstance(self.availability_key, PumpSwapOrderingKey):
            raise ValueError("availability_key must be a PumpSwapOrderingKey value")
        if not isinstance(self.provenance, PumpSwapStateProvenance):
            raise ValueError("provenance must be a PumpSwapStateProvenance value")
        if self.provenance.ordering_key != self.availability_key:
            raise ValueError("level provenance ordering key disagrees with availability_key")
        if self.labels != SURFACE_LABELS:
            raise ValueError("unsupported PumpSwap surface labels")
        _revision("schema_revision", self.schema_revision, SUPPORTED_SCHEMA_REVISIONS)
        _revision("layout_revision", self.layout_revision, SUPPORTED_LAYOUT_REVISIONS)
        _revision("math_revision", self.math_revision, SUPPORTED_MATH_REVISIONS)

    @property
    def requested(self) -> int:
        return self.requested_amount

    @property
    def notional(self) -> int:
        return self.requested_amount

    @property
    def quote_amount(self) -> int:
        return self.input_amount if self.side is PumpSwapQuoteSide.BUY else self.output_amount

    @property
    def average_price_num(self) -> int:
        return self.average_price_numerator

    @property
    def average_price_den(self) -> int:
        return self.average_price_denominator

    @property
    def price_numerator(self) -> int:
        return self.average_price_numerator

    @property
    def price_denominator(self) -> int:
        return self.average_price_denominator

    @property
    def average_price(self) -> tuple[int, int]:
        """The exact quote/base ratio as ``(numerator, denominator)``."""

        return (self.average_price_numerator, self.average_price_denominator)

    @property
    def total_fee_quote(self) -> int:
        return self.fee_breakdown.total_fee_quote

    @property
    def fees(self) -> PumpSwapFeeBreakdown:
        return self.fee_breakdown

    @property
    def fee_quote(self) -> int:
        return self.total_fee_quote

    @property
    def lp_fee_quote(self) -> int:
        return self.fee_breakdown.lp_fee_quote

    @property
    def protocol_fee_quote(self) -> int:
        return self.fee_breakdown.protocol_fee_quote

    @property
    def creator_fee_quote(self) -> int:
        return self.fee_breakdown.creator_fee_quote

    @property
    def cashback_fee_quote(self) -> int:
        return self.fee_breakdown.cashback_fee_quote

    @property
    def buyback_fee_quote(self) -> int:
        return self.fee_breakdown.buyback_fee_quote

    @property
    def fee_components(self) -> tuple[tuple[str, int], ...]:
        return self.fee_breakdown.fee_components

    @property
    def source_ordering_key(self) -> PumpSwapOrderingKey:
        return self.availability_key

    @property
    def slot(self) -> int:
        return self.availability_key.slot

    @property
    def tx_index(self) -> int:
        return self.availability_key.tx_index

    @property
    def transaction_index(self) -> int:
        return self.availability_key.tx_index

    @property
    def instruction_index(self) -> int:
        return self.availability_key.instruction_index

    @property
    def event_id(self) -> str:
        return self.availability_key.event_id

    @property
    def provenance_labels(self) -> tuple[str, str, str]:
        return self.labels


def _breakdown(quote: MathPumpSwapQuote) -> PumpSwapFeeBreakdown:
    return PumpSwapFeeBreakdown(
        lp_fee_quote=quote.lp_fee_quote,
        protocol_fee_quote=quote.protocol_fee_quote,
        creator_fee_quote=quote.creator_fee_quote,
        cashback_fee_quote=quote.cashback_fee_quote,
        buyback_fee_quote=quote.buyback_fee_quote,
    )


def _check_as_of(state: PumpSwapPoolState, as_of: AvailabilityKey | None) -> None:
    if as_of is not None:
        state.require_available_at(as_of)


def _buy_level(
    state: PumpSwapPoolState,
    gross_quote_amount: int,
    *,
    as_of: AvailabilityKey | None = None,
) -> PumpSwapQuoteLevel:
    _positive_u64("gross_quote_amount", gross_quote_amount)
    _check_as_of(state, as_of)
    quote = pumpswap_buy_exact_quote_in(
        gross_quote_amount=gross_quote_amount,
        base_reserve=state.base_reserve,
        quote_reserve=state.quote_reserve,
        fees=state.fees,
    )
    if quote.base_amount == 0:
        raise ValueError("gross quote amount produces no executable base output")
    if state.real_base_reserve is not None and quote.base_amount > state.real_base_reserve:
        raise ValueError("buy exceeds real base inventory")
    return PumpSwapQuoteLevel(
        side=PumpSwapQuoteSide.BUY,
        requested_amount=gross_quote_amount,
        input_amount=quote.user_quote_amount,
        output_amount=quote.base_amount,
        base_amount=quote.base_amount,
        curve_quote_amount=quote.curve_quote_amount,
        user_quote_amount=quote.user_quote_amount,
        fee_breakdown=_breakdown(quote),
        average_price_numerator=quote.user_quote_amount,
        average_price_denominator=quote.base_amount,
        availability_key=state.ordering_key,
        provenance=state.provenance,  # type: ignore[arg-type]
        schema_revision=state.schema_revision,
        layout_revision=state.layout_revision,
        math_revision=state.math_revision,
    )


def _sell_level(
    state: PumpSwapPoolState,
    base_amount: int,
    *,
    as_of: AvailabilityKey | None = None,
) -> PumpSwapQuoteLevel:
    _positive_u64("base_amount", base_amount)
    _check_as_of(state, as_of)
    quote = pumpswap_sell_exact_base(
        base_amount=base_amount,
        base_reserve=state.base_reserve,
        quote_reserve=state.quote_reserve,
        fees=state.fees,
    )
    if quote.user_quote_amount == 0:
        raise ValueError("base amount produces no executable quote output")
    if state.real_quote_reserve is not None and quote.curve_quote_amount > state.real_quote_reserve:
        raise ValueError("sell exceeds real quote inventory")
    return PumpSwapQuoteLevel(
        side=PumpSwapQuoteSide.SELL,
        requested_amount=base_amount,
        input_amount=quote.base_amount,
        output_amount=quote.user_quote_amount,
        base_amount=quote.base_amount,
        curve_quote_amount=quote.curve_quote_amount,
        user_quote_amount=quote.user_quote_amount,
        fee_breakdown=_breakdown(quote),
        average_price_numerator=quote.user_quote_amount,
        average_price_denominator=quote.base_amount,
        availability_key=state.ordering_key,
        provenance=state.provenance,  # type: ignore[arg-type]
        schema_revision=state.schema_revision,
        layout_revision=state.layout_revision,
        math_revision=state.math_revision,
    )


def _resolve_amount(
    amount: int | None,
    *,
    requested_amount: int | None,
    alias_amount: int | None,
    name: str,
) -> int:
    values = [value for value in (amount, requested_amount, alias_amount) if value is not None]
    if len(values) != 1:
        raise ValueError(f"pass exactly one {name} amount")
    value = values[0]
    _positive_u64(name, value)
    return value


def pumpswap_buy_level(
    state: PumpSwapPoolState,
    amount: int | None = None,
    *,
    gross_quote_amount: int | None = None,
    requested_amount: int | None = None,
    as_of: AvailabilityKey | None = None,
) -> PumpSwapQuoteLevel:
    """Build one exact-quote-in buy level from ``state``."""

    resolved = _resolve_amount(
        amount,
        requested_amount=requested_amount,
        alias_amount=gross_quote_amount,
        name="gross_quote_amount",
    )
    return _buy_level(state, resolved, as_of=as_of)


def pumpswap_sell_level(
    state: PumpSwapPoolState,
    amount: int | None = None,
    *,
    base_amount: int | None = None,
    requested_amount: int | None = None,
    as_of: AvailabilityKey | None = None,
) -> PumpSwapQuoteLevel:
    """Build one exact-base-in sell level from ``state``."""

    resolved = _resolve_amount(
        amount,
        requested_amount=requested_amount,
        alias_amount=base_amount,
        name="base_amount",
    )
    return _sell_level(state, resolved, as_of=as_of)


def _strict_amounts(values: Iterable[int], name: str) -> tuple[int, ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError(f"{name} must contain integers")
    result = tuple(values)
    if not result:
        raise ValueError(f"{name} must not be empty")
    previous = 0
    for value in result:
        _positive_u64(name, value)
        if value <= previous:
            raise ValueError(f"{name} must be strictly increasing")
        previous = value
    return result


@dataclass(frozen=True, slots=True)
class PumpSwapQuoteSurface:
    """Deterministically ordered independent buy/sell levels."""

    state: PumpSwapPoolState
    levels: tuple[PumpSwapQuoteLevel, ...]
    labels: tuple[str, str, str] = SURFACE_LABELS
    schema_revision: str = SCHEMA_REVISION
    layout_revision: str = LAYOUT_REVISION
    math_revision: str = MATH_REVISION

    def __post_init__(self) -> None:
        if not isinstance(self.state, PumpSwapPoolState):
            raise ValueError("state must be a PumpSwapPoolState value")
        if not isinstance(self.levels, tuple):
            raise ValueError("levels must be an immutable tuple")
        if not self.levels:
            raise ValueError("surface must contain at least one level")
        if self.labels != SURFACE_LABELS:
            raise ValueError("unsupported PumpSwap surface labels")
        _revision("schema_revision", self.schema_revision, SUPPORTED_SCHEMA_REVISIONS)
        _revision("layout_revision", self.layout_revision, SUPPORTED_LAYOUT_REVISIONS)
        _revision("math_revision", self.math_revision, SUPPORTED_MATH_REVISIONS)
        if (
            self.schema_revision != self.state.schema_revision
            or self.layout_revision != self.state.layout_revision
            or self.math_revision != self.state.math_revision
        ):
            raise ValueError("surface revisions disagree with state")
        previous_by_side: dict[PumpSwapQuoteSide, int] = {}
        for level in self.levels:
            if not isinstance(level, PumpSwapQuoteLevel):
                raise ValueError("surface levels must be PumpSwapQuoteLevel values")
            if level.availability_key != self.state.ordering_key:
                raise ValueError("surface level availability differs from state")
            previous = previous_by_side.get(level.side)
            if previous is not None and level.requested_amount <= previous:
                raise ValueError("surface notionals must increase per side")
            previous_by_side[level.side] = level.requested_amount

    @property
    def quotes(self) -> tuple[PumpSwapQuoteLevel, ...]:
        return self.levels

    @property
    def buy_levels(self) -> tuple[PumpSwapQuoteLevel, ...]:
        return tuple(level for level in self.levels if level.side is PumpSwapQuoteSide.BUY)

    @property
    def sell_levels(self) -> tuple[PumpSwapQuoteLevel, ...]:
        return tuple(level for level in self.levels if level.side is PumpSwapQuoteSide.SELL)

    @property
    def availability_key(self) -> PumpSwapOrderingKey:
        return self.state.ordering_key

    @property
    def source_ordering_key(self) -> PumpSwapOrderingKey:
        return self.state.ordering_key

    @property
    def slot(self) -> int:
        return self.state.ordering_key.slot

    @property
    def tx_index(self) -> int:
        return self.state.ordering_key.tx_index

    @property
    def transaction_index(self) -> int:
        return self.state.ordering_key.tx_index

    @property
    def instruction_index(self) -> int:
        return self.state.ordering_key.instruction_index

    @property
    def provenance_labels(self) -> tuple[str, str, str]:
        return self.labels

    def is_available_at(self, as_of: AvailabilityKey) -> bool:
        return self.state.is_available_at(as_of)

    def require_available_at(self, as_of: AvailabilityKey) -> None:
        self.state.require_available_at(as_of)


def build_pumpswap_quote_surface(
    state: PumpSwapPoolState,
    notionals: Iterable[int] | None = None,
    *,
    buy_notionals: Iterable[int] | None = None,
    sell_sizes: Iterable[int] | None = None,
    sell_notionals: Iterable[int] | None = None,
    as_of: AvailabilityKey | None = None,
) -> PumpSwapQuoteSurface:
    """Build independent buy and sell levels from one state snapshot.

    ``notionals`` is interpreted as gross quote input for buys and base input
    for sells.  Separate ``buy_notionals``/``sell_sizes`` may be supplied for
    callers that keep units on separate ladders.  Every sequence must already
    be strictly increasing; no state or reserve is mutated between levels.
    """

    if not isinstance(state, PumpSwapPoolState):
        raise ValueError("state must be a PumpSwapPoolState value")
    _check_as_of(state, as_of)
    if sell_sizes is not None and sell_notionals is not None:
        raise ValueError("pass either sell_sizes or sell_notionals")
    sell_values = sell_sizes if sell_sizes is not None else sell_notionals
    if notionals is not None and (buy_notionals is not None or sell_values is not None):
        raise ValueError("pass notionals or separate buy/sell amounts")
    if notionals is not None:
        buys = sells = _strict_amounts(notionals, "notionals")
    else:
        if buy_notionals is None and sell_values is None:
            raise ValueError("at least one notional sequence is required")
        buys = _strict_amounts(buy_notionals, "buy_notionals") if buy_notionals is not None else ()
        sells = _strict_amounts(sell_values, "sell_sizes") if sell_values is not None else ()
    if not buys and not sells:
        raise ValueError("at least one notional sequence is required")
    levels: list[PumpSwapQuoteLevel] = []
    # Stable side ordering is buy then sell for each size.  Each level is
    # quoted against the same immutable pre-trade state.
    for index in range(max(len(buys), len(sells))):
        if index < len(buys):
            levels.append(_buy_level(state, buys[index], as_of=as_of))
        if index < len(sells):
            levels.append(_sell_level(state, sells[index], as_of=as_of))
    return PumpSwapQuoteSurface(
        state=state,
        levels=tuple(levels),
        labels=SURFACE_LABELS,
        schema_revision=state.schema_revision,
        layout_revision=state.layout_revision,
        math_revision=state.math_revision,
    )


# Naming aliases make the adapter convenient without duplicating behavior.
PumpSwapState = PumpSwapPoolState
PumpSwapPoolStateProvenance = PumpSwapStateProvenance
PumpSwapProvenance = PumpSwapStateProvenance
PoolStateProvenance = PumpSwapStateProvenance
PumpSwapAvailabilityKey = PumpSwapOrderingKey
PumpSwapQuote = PumpSwapQuoteLevel
PumpSwapExecutableQuote = PumpSwapQuoteLevel
PumpSwapLevel = PumpSwapQuoteLevel
PumpSwapSurface = PumpSwapQuoteSurface
PumpSwapExecutableSurface = PumpSwapQuoteSurface
QuoteLevel = PumpSwapQuoteLevel
QuoteSurface = PumpSwapQuoteSurface
QuoteSide = PumpSwapQuoteSide
build_pumpswap_surface = build_pumpswap_quote_surface
pumpswap_quote_surface = build_pumpswap_quote_surface
reconstruct_pumpswap_surface = build_pumpswap_quote_surface
build_executable_surface = build_pumpswap_quote_surface
build_pumpswap_depth_surface = build_pumpswap_quote_surface
quote_surface = build_pumpswap_quote_surface
quote_pumpswap_buy = pumpswap_buy_level
quote_pumpswap_sell = pumpswap_sell_level


__all__ = [
    "AvailabilityKey",
    "LAYOUT_REVISION",
    "MATH_REVISION",
    "NOT_OBSERVED_ORDERBOOK",
    "NOT_TRUE_L2",
    "PumpSwapAvailabilityKey",
    "PumpSwapExecutableQuote",
    "PumpSwapExecutableSurface",
    "PumpSwapFeeBreakdown",
    "PumpSwapLevel",
    "PumpSwapOrderingKey",
    "PumpSwapPoolState",
    "PumpSwapPoolStateProvenance",
    "PumpSwapProvenance",
    "PumpSwapQuote",
    "PumpSwapQuoteLevel",
    "PumpSwapQuoteSide",
    "PumpSwapQuoteSurface",
    "PumpSwapState",
    "PumpSwapStateProvenance",
    "PumpSwapSurface",
    "PoolStateProvenance",
    "QuoteLevel",
    "QuoteSurface",
    "QuoteSide",
    "RECONSTRUCTED_EXECUTABLE_SURFACE",
    "SCHEMA_REVISION",
    "SUPPORTED_FEE_SOURCE",
    "SUPPORTED_PATH",
    "SURFACE_LABELS",
    "build_pumpswap_quote_surface",
    "build_pumpswap_surface",
    "build_executable_surface",
    "build_pumpswap_depth_surface",
    "pumpswap_buy_level",
    "pumpswap_quote_surface",
    "pumpswap_sell_level",
    "quote_surface",
    "quote_pumpswap_buy",
    "quote_pumpswap_sell",
    "reconstruct_pumpswap_surface",
]
