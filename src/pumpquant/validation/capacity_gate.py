"""Pure-Python, fail-closed capacity and capital-gate primitives.

Independent of the research runners and of the
chronological holdout.  It deals only in already sealed development evidence:
integer token/quote amounts, causal FX points, and landed entry/exit records.
No value in this module is a live-trading recommendation.  In particular, a
finite starting-capital ladder is *not* a proof of a global maximum unless the
caller supplies an exhaustive or bounded proof.

Money is represented by :class:`~decimal.Decimal` only at USD boundaries and
by integers at token/quote boundaries.  ``bool`` is rejected at
every integer boundary (``bool`` is an ``int`` subclass in Python).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from enum import StrEnum
from typing import Final, TypeAlias, cast

DAY_NS: Final[int] = 86_400_000_000_000
FOUR_DAYS: Final[int] = 4
SOL_DECIMALS: Final[int] = 9
USD_DAILY_CAPACITY_TARGET: Final[Decimal] = Decimal("20000")

# The experiment's frozen *starting-capital* ladder.  Values are integer USD
# dollars, not floats and not a claim about an unbounded optimum.
STARTING_CAPITAL_LADDER_USD: Final[tuple[int, ...]] = (
    10,
    25,
    50,
    100,
    250,
    500,
    1_000,
    2_500,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
)
# Naming variants are useful to callers loading a preregistered manifest.


class CapacityGateError(ValueError):
    """An input is absent, malformed, non-causal, or violates a gate."""


@dataclass(frozen=True, order=True, slots=True)
class ChainKey:
    """Landed chain order used to settle equal-timestamp events."""

    slot: int
    tx_index: int
    log_index: int
    event_index: int
    event_id: str

    def __post_init__(self) -> None:
        _integer(self.slot, "slot")
        _integer(self.tx_index, "tx_index")
        _integer(self.log_index, "log_index")
        _integer(self.event_index, "event_index")
        _text(self.event_id, "event_id")


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CapacityGateError(f"{label} must be an integer >= {minimum}")
    return value


def _signed_integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CapacityGateError(f"{label} must be an integer")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CapacityGateError(f"{label} must be a non-empty string")
    return value


def _decimal(value: object, label: str, *, positive: bool = False) -> Decimal:
    # Floats are rejected rather than converted through their binary spelling.
    # Strings are accepted for manifest/CSV boundaries and immediately become
    # Decimal values; all arithmetic below remains Decimal arithmetic.
    if isinstance(value, (bool, float)):
        raise CapacityGateError(f"{label} must be a Decimal or decimal string")
    if not isinstance(value, (Decimal, int, str)):
        raise CapacityGateError(f"{label} must be a Decimal or decimal string")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise CapacityGateError(f"{label} is not a valid decimal") from error
    if not result.is_finite() or (positive and result <= 0):
        comparator = "positive and finite" if positive else "finite"
        raise CapacityGateError(f"{label} must be {comparator}")
    return result


def _bool(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise CapacityGateError(f"{label} must be boolean")
    return value


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise CapacityGateError("report decimal is not finite")
    return format(value, "f")


@dataclass(frozen=True, slots=True)
class CausalFxPoint:
    """A sealed USD-per-quote close available at a causal timestamp."""

    available_at_ns: int
    close_usd: Decimal

    def __post_init__(self) -> None:
        _integer(self.available_at_ns, "available_at_ns")
        object.__setattr__(self, "close_usd", _decimal(self.close_usd, "close_usd", positive=True))


@dataclass(frozen=True, slots=True)
class CausalFxSeries:
    """Strictly increasing, positive FX closes with no interpolation."""

    points: tuple[CausalFxPoint, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.points, tuple) or not self.points:
            raise CapacityGateError("causal FX evidence cannot be empty")
        previous = -1
        normalized: list[CausalFxPoint] = []
        for point in self.points:
            if not isinstance(point, CausalFxPoint):
                raise CapacityGateError("causal FX points have an invalid type")
            if point.available_at_ns <= previous:
                raise CapacityGateError("causal FX availability must be strictly increasing")
            previous = point.available_at_ns
            normalized.append(point)
        object.__setattr__(self, "points", tuple(normalized))

    @classmethod
    def from_points(cls, points: Iterable[CausalFxPoint]) -> CausalFxSeries:
        return cls(tuple(points))

    def price_at(self, available_at_ns: int) -> Decimal:
        timestamp = _integer(available_at_ns, "available_at_ns")
        selected: CausalFxPoint | None = None
        for point in self.points:
            if point.available_at_ns > timestamp:
                break
            selected = point
        if selected is None:
            raise CapacityGateError("causal FX is unavailable at requested time")
        return selected.close_usd

    def usd_from_atomic(
        self,
        amount_atomic: int,
        available_at_ns: int,
        *,
        decimals: int = SOL_DECIMALS,
    ) -> Decimal:
        amount = _signed_integer(amount_atomic, "amount_atomic")
        precision = _integer(decimals, "decimals")
        if precision > 30:
            raise CapacityGateError("decimals is implausibly large")
        return Decimal(amount) * self.price_at(available_at_ns) / (Decimal(10) ** precision)

    def atomic_from_usd(
        self,
        usd: Decimal,
        available_at_ns: int,
        *,
        decimals: int = SOL_DECIMALS,
    ) -> int:
        value = _decimal(usd, "usd", positive=True)
        precision = _integer(decimals, "decimals")
        if precision > 30:
            raise CapacityGateError("decimals is implausibly large")
        atomic = (
            value / self.price_at(available_at_ns) * (Decimal(10) ** precision)
        ).to_integral_value(rounding=ROUND_FLOOR)
        result = int(atomic)
        if result <= 0:
            raise CapacityGateError("USD amount converts to no executable quote units")
        return result


@dataclass(frozen=True, slots=True)
class RealizedExit:
    """A landed exit whose net quote amount is available for attribution.

    ``net_quote_atomic`` is output after the entry principal, all leg costs,
    fees, and adverse execution adjustments have already been accounted for by
    the exact execution caller.  A signed value is allowed because a loss is a
    valid realised outcome.
    """

    trade_id: str
    exit_available_at_ns: int
    net_quote_atomic: int
    quote_decimals: int = SOL_DECIMALS

    def __post_init__(self) -> None:
        _text(self.trade_id, "trade_id")
        _integer(self.exit_available_at_ns, "exit_available_at_ns")
        _signed_integer(self.net_quote_atomic, "net_quote_atomic")
        decimals = _integer(self.quote_decimals, "quote_decimals")
        if decimals != SOL_DECIMALS:
            raise CapacityGateError("quote_decimals must be exactly 9 for SOL exits")


@dataclass(frozen=True, slots=True)
class CompleteUtcDay:
    """Evidence that one UTC calendar day is complete in the sealed data."""

    day_start_ns: int
    complete: bool = True

    def __post_init__(self) -> None:
        timestamp = _integer(self.day_start_ns, "day_start_ns")
        if timestamp % DAY_NS:
            raise CapacityGateError("day_start_ns must be a UTC midnight")
        _bool(self.complete, "complete")
        if not self.complete:
            raise CapacityGateError("UTC day is not complete")


@dataclass(frozen=True, slots=True)
class DailyAttribution:
    """Realised net result for one complete UTC day."""

    day_start_ns: int
    net_usd: Decimal
    exit_count: int
    complete: bool = True

    def __post_init__(self) -> None:
        timestamp = _integer(self.day_start_ns, "day_start_ns")
        if timestamp % DAY_NS:
            raise CapacityGateError("day_start_ns must be a UTC midnight")
        object.__setattr__(self, "net_usd", _decimal(self.net_usd, "net_usd"))
        _integer(self.exit_count, "exit_count")
        _bool(self.complete, "complete")
        if not self.complete:
            raise CapacityGateError("daily attribution must cover a complete UTC day")

    @property
    def day(self) -> date:
        return datetime.fromtimestamp(self.day_start_ns // 1_000_000_000, tz=UTC).date()


@dataclass(frozen=True, slots=True)
class FourDayAttribution:
    """Exactly four contiguous complete UTC days and their daily net totals."""

    days: tuple[DailyAttribution, ...]
    target_usd: Decimal = USD_DAILY_CAPACITY_TARGET

    def __post_init__(self) -> None:
        if len(self.days) != FOUR_DAYS:
            raise CapacityGateError("capacity attribution requires exactly four UTC days")
        prior: int | None = None
        for day in self.days:
            if not isinstance(day, DailyAttribution):
                raise CapacityGateError("capacity attribution day has an invalid type")
            if prior is not None and day.day_start_ns != prior + DAY_NS:
                raise CapacityGateError("UTC day coverage is not contiguous")
            prior = day.day_start_ns
        object.__setattr__(
            self, "target_usd", _decimal(self.target_usd, "target_usd", positive=True)
        )

    @property
    def passed(self) -> bool:
        return all(day.net_usd >= self.target_usd for day in self.days)

    @property
    def total_net_usd(self) -> Decimal:
        return sum((day.net_usd for day in self.days), Decimal(0))

    @property
    def daily_net_usd(self) -> tuple[Decimal, ...]:
        return tuple(day.net_usd for day in self.days)


def utc_day_start_ns(timestamp_ns: int) -> int:
    """Return the UTC-midnight bucket for an epoch-nanosecond timestamp."""

    timestamp = _integer(timestamp_ns, "timestamp_ns")
    return timestamp // DAY_NS * DAY_NS


def _complete_day_from_object(value: object, label: str) -> CompleteUtcDay:
    if isinstance(value, CompleteUtcDay):
        return value
    if not isinstance(value, Mapping):
        raise CapacityGateError(f"{label} has an invalid type")
    return CompleteUtcDay(
        day_start_ns=_integer(
            value.get("day_start_ns", value.get("start_ns")), f"{label}.day_start_ns"
        ),
        complete=_bool(value.get("complete", value.get("sealed")), f"{label}.complete"),
    )


def earliest_four_complete_days(
    availability_start_ns: int,
    availability_end_ns: int,
    *,
    complete_days: Iterable[CompleteUtcDay | Mapping[str, object]] | None = None,
) -> tuple[int, ...]:
    """Select the earliest four complete UTC days in a sealed half-open range.

    The bounds themselves are evidence that all nanoseconds in the range are
    available.  Optional day records are checked as an additional
    completeness seal; a missing or false record fails closed.
    """

    start = _integer(availability_start_ns, "availability_start_ns")
    end = _integer(availability_end_ns, "availability_end_ns")
    if end <= start:
        raise CapacityGateError("availability bounds are empty or reversed")
    first = ((start + DAY_NS - 1) // DAY_NS) * DAY_NS
    candidates: list[int] = []
    day = first
    while day + DAY_NS <= end:
        candidates.append(day)
        day += DAY_NS
    if len(candidates) < FOUR_DAYS:
        raise CapacityGateError("fewer than four complete UTC days are available")
    selected = tuple(candidates[:FOUR_DAYS])
    if complete_days is not None:
        evidence: dict[int, CompleteUtcDay] = {}
        evidence_order: list[int] = []
        for index, raw_item in enumerate(complete_days, 1):
            item = _complete_day_from_object(raw_item, f"UTC day evidence {index}")
            if item.day_start_ns in evidence:
                raise CapacityGateError("UTC day completeness evidence is duplicated")
            evidence[item.day_start_ns] = item
            evidence_order.append(item.day_start_ns)
        # A supplied inventory is itself sealed evidence, not a hint from
        # which the kernel may select a convenient subset.  It must contain
        # exactly the four days selected from the availability bounds: no
        # extras, gaps, duplicate days, or out-of-window records.
        if (
            tuple(evidence_order) != selected
            or set(evidence) != set(selected)
            or len(evidence) != FOUR_DAYS
        ):
            raise CapacityGateError(
                "UTC day completeness evidence is missing or must contain exactly "
                "the four selected days"
            )
        if any(not evidence[day].complete for day in selected):
            raise CapacityGateError("UTC day completeness evidence is incomplete")
    return selected


def _exit_from_object(value: object) -> RealizedExit:
    if isinstance(value, RealizedExit):
        return value
    if not isinstance(value, Mapping):
        raise CapacityGateError("realized exit has an invalid type")
    try:
        return RealizedExit(
            trade_id=_text(value.get("trade_id", value.get("id")), "trade_id"),
            exit_available_at_ns=_integer(
                value.get("exit_available_at_ns", value.get("exit_ns")),
                "exit_available_at_ns",
            ),
            net_quote_atomic=_signed_integer(
                value.get("net_quote_atomic", value.get("net_atomic")), "net_quote_atomic"
            ),
            quote_decimals=_integer(value.get("quote_decimals", SOL_DECIMALS), "quote_decimals"),
        )
    except (TypeError, AttributeError) as error:
        raise CapacityGateError("realized exit is missing required fields") from error


def attribute_realized_net(
    exits: Iterable[RealizedExit | Mapping[str, object]],
    fx: CausalFxSeries,
    *,
    availability_start_ns: int,
    availability_end_ns: int,
    complete_days: Iterable[CompleteUtcDay | Mapping[str, object]] | None = None,
    target_usd: Decimal = USD_DAILY_CAPACITY_TARGET,
) -> FourDayAttribution:
    """Convert exits at causal FX and aggregate net USD by UTC exit day.

    Exactly four earliest complete contiguous days are selected.  Every supplied
    exit must land in those days; silently dropping an out-of-range exit would
    turn incomplete evidence into a false pass, so it is rejected.
    """

    if not isinstance(fx, CausalFxSeries):
        raise CapacityGateError("causal FX evidence has an invalid type")
    selected = earliest_four_complete_days(
        availability_start_ns,
        availability_end_ns,
        complete_days=complete_days,
    )
    buckets = {day: Decimal(0) for day in selected}
    counts = {day: 0 for day in selected}
    seen_trade_ids: set[str] = set()
    for raw in exits:
        exit_record = _exit_from_object(raw)
        if exit_record.trade_id in seen_trade_ids:
            raise CapacityGateError("duplicate realized exit trade ID")
        seen_trade_ids.add(exit_record.trade_id)
        day = utc_day_start_ns(exit_record.exit_available_at_ns)
        if day not in buckets:
            raise CapacityGateError("exit day is outside the four-day complete interval")
        buckets[day] += fx.usd_from_atomic(
            exit_record.net_quote_atomic,
            exit_record.exit_available_at_ns,
            decimals=exit_record.quote_decimals,
        )
        counts[day] += 1
    return FourDayAttribution(
        tuple(DailyAttribution(day, buckets[day], counts[day], complete=True) for day in selected),
        target_usd=target_usd,
    )


def passes_daily_target(
    attribution: FourDayAttribution,
    target_usd: Decimal = USD_DAILY_CAPACITY_TARGET,
) -> bool:
    """Return true only when all four complete days meet the target."""

    if not isinstance(attribution, FourDayAttribution):
        raise CapacityGateError("daily attribution has an invalid type")
    target = _decimal(target_usd, "target_usd", positive=True)
    return all(day.net_usd >= target for day in attribution.days)


class Venue(StrEnum):
    PUMP = "pump"
    PUMPSWAP = "pumpswap"


class TradeSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True, slots=True)
class RealInventoryCapacity:
    """Orientation-correct real-inventory predicate and rejection reason."""

    venue: str
    side: str
    pool_orientation_flipped: bool
    required_inventory_atomic: int
    available_inventory_atomic: int
    inventory_asset: str
    accepted: bool
    reason: str

    def __post_init__(self) -> None:
        _text(self.venue, "venue")
        _text(self.side, "side")
        _bool(self.pool_orientation_flipped, "pool_orientation_flipped")
        _integer(self.required_inventory_atomic, "required_inventory_atomic")
        _integer(self.available_inventory_atomic, "available_inventory_atomic")
        _text(self.inventory_asset, "inventory_asset")
        _bool(self.accepted, "accepted")
        _text(self.reason, "reason")
        if self.accepted and self.reason != "accepted":
            raise CapacityGateError("accepted inventory predicate has a rejection reason")
        if not self.accepted and self.reason != "real_inventory_capacity":
            raise CapacityGateError("inventory rejection must be explicit")

    def __bool__(self) -> bool:
        return self.accepted

    @property
    def status(self) -> str:
        """Stable status spelling for report/ledger consumers."""

        return self.reason

    @property
    def rejected(self) -> bool:
        return not self.accepted


def real_inventory_capacity(
    *,
    venue: str | Venue,
    side: str | TradeSide,
    base_amount_atomic: int,
    quote_amount_atomic: int,
    real_base_inventory_atomic: int,
    real_quote_inventory_atomic: int,
    pool_orientation_flipped: bool | None = False,
) -> RealInventoryCapacity:
    """Check the pool's real inventory for one canonical token trade.

    ``base_amount_atomic`` is canonical token output/input and
    ``quote_amount_atomic`` is canonical SOL input/output, regardless of the
    protocol pool orientation.  The sealed feature schema keeps real reserves
    in this canonical token/SOL orientation even when PumpSwap stores SOL as
    protocol base.  Therefore canonical buys always consume real token
    inventory and canonical sells always consume real SOL inventory.  The
    orientation still matters to the exact quote caller, but it must not swap
    the feature's real-reserve columns.  The returned reason is exactly
    ``real_inventory_capacity`` on rejection; callers must not replace it with
    a generic slippage label.
    """

    venue_text = venue.value if isinstance(venue, Venue) else _text(venue, "venue").lower()
    side_text = side.value if isinstance(side, TradeSide) else _text(side, "side").lower()
    if venue_text not in {Venue.PUMP.value, Venue.PUMPSWAP.value}:
        raise CapacityGateError("venue must be pump or pumpswap")
    if side_text not in {TradeSide.BUY.value, TradeSide.SELL.value}:
        raise CapacityGateError("side must be buy or sell")
    base_amount = _integer(base_amount_atomic, "base_amount_atomic")
    quote_amount = _integer(quote_amount_atomic, "quote_amount_atomic")
    real_base = _integer(real_base_inventory_atomic, "real_base_inventory_atomic")
    real_quote = _integer(real_quote_inventory_atomic, "real_quote_inventory_atomic")
    if pool_orientation_flipped is None:
        if venue_text != Venue.PUMP.value:
            raise CapacityGateError("PumpSwap orientation is missing")
        flipped = False
    else:
        flipped = _bool(pool_orientation_flipped, "pool_orientation_flipped")
    # Pump's bonding curve has one canonical orientation.  Treating a Pump
    # state as flipped is an orientation error, not a capacity pass.
    if venue_text == Venue.PUMP.value and flipped:
        raise CapacityGateError("Pump bonding curve cannot use flipped orientation")
    if side_text == TradeSide.BUY.value:
        required, available, asset = base_amount, real_base, "base"
    else:
        required, available, asset = quote_amount, real_quote, "quote"
    accepted = required <= available
    return RealInventoryCapacity(
        venue=venue_text,
        side=side_text,
        pool_orientation_flipped=flipped,
        required_inventory_atomic=required,
        available_inventory_atomic=available,
        inventory_asset=asset,
        accepted=accepted,
        reason="accepted" if accepted else "real_inventory_capacity",
    )


def pump_capacity_predicate(
    *,
    side: str | TradeSide,
    base_amount_atomic: int,
    quote_amount_atomic: int,
    real_base_inventory_atomic: int,
    real_quote_inventory_atomic: int,
    pool_orientation_flipped: bool | None = False,
) -> RealInventoryCapacity:
    return real_inventory_capacity(
        venue=Venue.PUMP,
        side=side,
        base_amount_atomic=base_amount_atomic,
        quote_amount_atomic=quote_amount_atomic,
        real_base_inventory_atomic=real_base_inventory_atomic,
        real_quote_inventory_atomic=real_quote_inventory_atomic,
        pool_orientation_flipped=pool_orientation_flipped,
    )


def pumpswap_capacity_predicate(
    *,
    side: str | TradeSide,
    base_amount_atomic: int,
    quote_amount_atomic: int,
    real_base_inventory_atomic: int,
    real_quote_inventory_atomic: int,
    pool_orientation_flipped: bool | None = False,
) -> RealInventoryCapacity:
    return real_inventory_capacity(
        venue=Venue.PUMPSWAP,
        side=side,
        base_amount_atomic=base_amount_atomic,
        quote_amount_atomic=quote_amount_atomic,
        real_base_inventory_atomic=real_base_inventory_atomic,
        real_quote_inventory_atomic=real_quote_inventory_atomic,
        pool_orientation_flipped=pool_orientation_flipped,
    )


def has_real_inventory_capacity(**kwargs: object) -> bool:
    """Boolean convenience wrapper preserving the predicate reason."""

    # Keep this wrapper narrow at runtime: callers wanting the
    # rejection reason should use ``real_inventory_capacity`` directly.
    result = real_inventory_capacity(
        venue=cast(str | Venue, kwargs.pop("venue")),
        side=cast(str | TradeSide, kwargs.pop("side")),
        base_amount_atomic=cast(int, kwargs.pop("base_amount_atomic")),
        quote_amount_atomic=cast(int, kwargs.pop("quote_amount_atomic")),
        real_base_inventory_atomic=cast(int, kwargs.pop("real_base_inventory_atomic")),
        real_quote_inventory_atomic=cast(int, kwargs.pop("real_quote_inventory_atomic")),
        pool_orientation_flipped=cast(bool | None, kwargs.pop("pool_orientation_flipped", False)),
    )
    if kwargs:
        raise CapacityGateError("unexpected inventory capacity argument")
    return result.accepted


@dataclass(frozen=True, slots=True)
class WalletTrade:
    """One already-quoted opportunity for chronological wallet replay."""

    trade_id: str
    mint: str
    entry_available_at_ns: int
    exit_available_at_ns: int
    entry_quote_atomic: int
    exit_quote_atomic: int
    entry_cost_quote_atomic: int = 0
    exit_cost_quote_atomic: int = 0
    quote_decimals: int = SOL_DECIMALS
    entry_chain_key: ChainKey | None = None
    exit_chain_key: ChainKey | None = None

    def __post_init__(self) -> None:
        _text(self.trade_id, "trade_id")
        _text(self.mint, "mint")
        entry = _integer(self.entry_available_at_ns, "entry_available_at_ns")
        exit_time = _integer(self.exit_available_at_ns, "exit_available_at_ns")
        if exit_time <= entry:
            raise CapacityGateError("wallet trade exit must follow entry")
        _integer(self.entry_quote_atomic, "entry_quote_atomic", minimum=1)
        _integer(self.exit_quote_atomic, "exit_quote_atomic")
        _integer(self.entry_cost_quote_atomic, "entry_cost_quote_atomic")
        _integer(self.exit_cost_quote_atomic, "exit_cost_quote_atomic")
        decimals = _integer(self.quote_decimals, "quote_decimals")
        if decimals != SOL_DECIMALS:
            raise CapacityGateError("quote_decimals must be exactly 9 for SOL trades")
        # Landed chain identity is not optional in the capacity kernel.  A
        # timestamp-only ordering can be ambiguous at equal availability and
        # cannot prove causal chronology at different timestamps.
        if not isinstance(self.entry_chain_key, ChainKey):
            raise CapacityGateError("wallet trade entry/exit chain keys are required")
        if not isinstance(self.exit_chain_key, ChainKey):
            raise CapacityGateError("wallet trade entry/exit chain keys are required")
        if self.exit_chain_key <= self.entry_chain_key:
            raise CapacityGateError("wallet trade exit chain key must follow entry")

    @property
    def total_entry_debit_atomic(self) -> int:
        return self.entry_quote_atomic + self.entry_cost_quote_atomic

    @property
    def realized_net_quote_atomic(self) -> int:
        return (
            self.exit_quote_atomic
            - self.entry_quote_atomic
            - self.entry_cost_quote_atomic
            - self.exit_cost_quote_atomic
        )

    def realized_exit(self) -> RealizedExit:
        return RealizedExit(
            trade_id=self.trade_id,
            exit_available_at_ns=self.exit_available_at_ns,
            net_quote_atomic=self.realized_net_quote_atomic,
            quote_decimals=self.quote_decimals,
        )


@dataclass(frozen=True, slots=True)
class WalletDecision:
    trade_id: str
    accepted: bool
    reason: str
    budget_quote_atomic: int
    cash_after_atomic: int
    open_position_count: int

    def __post_init__(self) -> None:
        _text(self.trade_id, "decision.trade_id")
        _bool(self.accepted, "decision.accepted")
        _text(self.reason, "decision.reason")
        _integer(self.budget_quote_atomic, "decision.budget_quote_atomic")
        _integer(self.cash_after_atomic, "decision.cash_after_atomic")
        _integer(self.open_position_count, "decision.open_position_count")
        if self.accepted and self.reason != "accepted":
            raise CapacityGateError("accepted wallet decision must use reason accepted")
        if not self.accepted and self.reason == "accepted":
            raise CapacityGateError("rejected wallet decision cannot use reason accepted")
        if self.reason not in {
            "accepted",
            "mint_already_open",
            "max_concurrent_positions",
            "allocation_budget",
            "allocation_rounds_to_zero",
            "exact_quote_or_capacity_rejected",
            "insufficient_cash",
            "outside_four_day_window",
        }:
            raise CapacityGateError("wallet decision has an unknown reason")


@dataclass(frozen=True, slots=True)
class SharedWalletReplay:
    starting_usd: Decimal
    starting_quote_atomic: int
    final_quote_atomic: int
    final_usd: Decimal
    minimum_equity_quote_atomic: int
    decisions: tuple[WalletDecision, ...]
    accepted_trades: tuple[WalletTrade, ...]
    maximum_concurrent_positions: int
    profitable_in_quote: bool
    profitable_in_usd: bool
    passed: bool
    final_exit_available_at_ns: int
    allocation_percent: int = 20
    max_concurrent_positions: int = 5

    def __post_init__(self) -> None:
        """Validate the complete wallet ledger and derive its status flags.

        A replay is an accounting result, not a bag of caller assertions.  The
        public dataclass remains useful as a transport object, but every field
        that can be recomputed from the landed trades/decisions is checked
        here.  This prevents a forged ``SharedWalletReplay`` from being used as
        a ladder factory result to bypass the wallet or attribution gates.
        """

        starting_usd = _decimal(self.starting_usd, "starting_usd", positive=True)
        object.__setattr__(self, "starting_usd", starting_usd)
        starting_quote = _integer(self.starting_quote_atomic, "starting_quote_atomic", minimum=1)
        final_quote = _integer(self.final_quote_atomic, "final_quote_atomic")
        minimum_equity = _integer(
            self.minimum_equity_quote_atomic,
            "minimum_equity_quote_atomic",
        )
        final_usd = _decimal(self.final_usd, "final_usd")
        if final_usd < 0:
            raise CapacityGateError("final_usd cannot be negative")
        final_exit_ns = _integer(
            self.final_exit_available_at_ns,
            "final_exit_available_at_ns",
        )
        allocation = _integer(self.allocation_percent, "allocation_percent")
        if allocation < 1 or allocation > 100:
            raise CapacityGateError("allocation_percent must be in [1, 100]")
        concurrency = _integer(self.max_concurrent_positions, "max_concurrent_positions")
        if concurrency < 1:
            raise CapacityGateError("max_concurrent_positions must be positive")
        if not isinstance(self.decisions, tuple) or not self.decisions:
            raise CapacityGateError("shared wallet replay decisions cannot be empty")
        if not isinstance(self.accepted_trades, tuple):
            raise CapacityGateError("shared wallet accepted trades must be a tuple")

        decisions = self.decisions
        for decision in decisions:
            if not isinstance(decision, WalletDecision):
                raise CapacityGateError("shared wallet decision has an invalid type")
            if decision.open_position_count > concurrency:
                raise CapacityGateError("wallet decision exceeds maximum concurrency")
        decision_ids = tuple(decision.trade_id for decision in decisions)
        if len(set(decision_ids)) != len(decision_ids):
            raise CapacityGateError("duplicate wallet decision trade ID")

        accepted = self.accepted_trades
        accepted_ids = tuple(trade.trade_id for trade in accepted)
        if len(set(accepted_ids)) != len(accepted_ids):
            raise CapacityGateError("duplicate accepted wallet trade ID")
        for trade in accepted:
            if not isinstance(trade, WalletTrade):
                raise CapacityGateError("shared wallet accepted trade has an invalid type")
        if any(trade.quote_decimals != SOL_DECIMALS for trade in accepted):
            raise CapacityGateError("shared wallet requires SOL quote precision")
        accepted_decisions = tuple(decision for decision in decisions if decision.accepted)
        if tuple(decision.trade_id for decision in accepted_decisions) != accepted_ids:
            raise CapacityGateError("accepted wallet decisions do not match accepted wallet trades")
        if any(decision.reason != "accepted" for decision in accepted_decisions):
            raise CapacityGateError("accepted wallet decision has an invalid reason")
        if any(decision.trade_id not in accepted_ids for decision in accepted_decisions):
            raise CapacityGateError("accepted wallet decision is missing its trade")

        all_chain_keys: list[ChainKey] = []
        for trade in accepted:
            # WalletTrade performs the per-trade checks; repeat the identity
            # checks here so this object remains fail-closed if construction is
            # ever changed to bypass that initialiser.
            if not isinstance(trade.entry_chain_key, ChainKey) or not isinstance(
                trade.exit_chain_key, ChainKey
            ):
                raise CapacityGateError("accepted wallet trade chain keys are required")
            if trade.exit_chain_key <= trade.entry_chain_key:
                raise CapacityGateError("accepted wallet trade chain chronology is invalid")
            all_chain_keys.extend((trade.entry_chain_key, trade.exit_chain_key))
        if len(all_chain_keys) != len(set(all_chain_keys)):
            raise CapacityGateError("duplicate wallet landed chain key")

        # Accepted rows must be in the same landed entry order produced by the
        # replay primitive.  This is important because decisions carry only
        # post-settlement state; reordering them would otherwise permit a
        # fabricated cash/equity ledger.
        ordered_accepted = tuple(
            sorted(
                accepted,
                key=lambda item: (item.entry_available_at_ns, item.entry_chain_key),
            )
        )
        if ordered_accepted != accepted:
            raise CapacityGateError("accepted wallet trades are not in landed entry order")

        # Recompute cash, open inventory, minimum equity, and maximum open
        # positions from the accepted rows and accepted decisions.  Rejected
        # signals cannot mutate the wallet, so they need only satisfy the
        # structural decision checks above.
        cash = starting_quote
        open_trades: list[WalletTrade] = []
        accepted_index = 0
        recomputed_minimum = starting_quote
        recomputed_max_open = 0

        def equity() -> int:
            return cash + sum(item.entry_quote_atomic for item in open_trades)

        def settle_before(entry: WalletTrade) -> None:
            nonlocal cash, recomputed_minimum
            entry_key = cast(ChainKey, entry.entry_chain_key)
            due = [
                item
                for item in open_trades
                if item.exit_available_at_ns < entry.entry_available_at_ns
                or (
                    item.exit_available_at_ns == entry.entry_available_at_ns
                    and cast(ChainKey, item.exit_chain_key) <= entry_key
                )
            ]
            for item in sorted(
                due,
                key=lambda value: (value.exit_available_at_ns, value.exit_chain_key),
            ):
                open_trades.remove(item)
                cash += item.exit_quote_atomic - item.exit_cost_quote_atomic
                if cash < 0:
                    raise CapacityGateError("wallet cash became negative at settlement")
                recomputed_minimum = min(recomputed_minimum, equity())

        for decision in decisions:
            if not decision.accepted:
                continue
            trade = accepted[accepted_index]
            accepted_index += 1
            settle_before(trade)
            budget = equity() * allocation // 100
            if any(item.mint == trade.mint for item in open_trades):
                raise CapacityGateError("accepted wallet trade violates one-mint-open invariant")
            if len(open_trades) >= concurrency:
                raise CapacityGateError("accepted wallet trade exceeds maximum concurrency")
            if trade.entry_quote_atomic > budget:
                raise CapacityGateError("accepted wallet trade exceeds allocation budget")
            if trade.total_entry_debit_atomic > cash:
                raise CapacityGateError("accepted wallet trade exceeds wallet cash")
            expected_cash = cash - trade.total_entry_debit_atomic
            if decision.budget_quote_atomic != budget:
                raise CapacityGateError("wallet decision budget is inconsistent")
            if decision.cash_after_atomic != expected_cash:
                raise CapacityGateError("wallet decision cash ledger is inconsistent")
            open_trades.append(trade)
            if decision.open_position_count != len(open_trades):
                raise CapacityGateError("wallet decision open-count ledger is inconsistent")
            cash = expected_cash
            recomputed_max_open = max(recomputed_max_open, len(open_trades))
            recomputed_minimum = min(recomputed_minimum, equity())
        if accepted_index != len(accepted):
            raise CapacityGateError("accepted wallet trade ledger is incomplete")

        final_exit_ns_derived = max(
            (trade.exit_available_at_ns for trade in accepted),
            default=0,
        )
        for trade in sorted(
            open_trades,
            key=lambda item: (item.exit_available_at_ns, item.exit_chain_key),
        ):
            open_trades.remove(trade)
            cash += trade.exit_quote_atomic - trade.exit_cost_quote_atomic
            if cash < 0:
                raise CapacityGateError("wallet cash became negative at final settlement")
            recomputed_minimum = min(recomputed_minimum, equity())
        if open_trades:
            raise CapacityGateError("wallet replay has unresolved open positions")
        if not accepted:
            final_exit_ns_derived = final_exit_ns
        if cash != final_quote:
            raise CapacityGateError("final wallet quote balance is inconsistent")
        if minimum_equity != recomputed_minimum:
            raise CapacityGateError("minimum wallet equity is inconsistent")
        if accepted and final_exit_ns != final_exit_ns_derived:
            raise CapacityGateError("final wallet exit timestamp is inconsistent")
        if minimum_equity > starting_quote:
            raise CapacityGateError("minimum wallet equity exceeds starting equity")

        maximum_derived = max(
            (decision.open_position_count for decision in decisions),
            default=0,
        )
        if maximum_derived != recomputed_max_open:
            raise CapacityGateError("maximum concurrent position count is inconsistent")
        if self.maximum_concurrent_positions != maximum_derived:
            raise CapacityGateError("maximum concurrent position count is inconsistent")

        profitable_quote = final_quote > starting_quote
        profitable_usd = final_usd > starting_usd
        # ``passed`` is a wallet-invariant flag only.  The B
        # rung applies the four-day USD target separately; a terminal FX
        # revaluation must never create a false negative for that criterion.
        passed = bool(accepted and minimum_equity >= 0 and final_quote >= 0)
        if self.profitable_in_quote != profitable_quote:
            raise CapacityGateError("wallet quote-profit flag is inconsistent")
        if self.profitable_in_usd != profitable_usd:
            raise CapacityGateError("wallet USD-profit flag is inconsistent")
        if self.passed != passed:
            raise CapacityGateError("wallet pass flag is inconsistent")

        object.__setattr__(self, "starting_quote_atomic", starting_quote)
        object.__setattr__(self, "final_quote_atomic", final_quote)
        object.__setattr__(self, "minimum_equity_quote_atomic", minimum_equity)
        object.__setattr__(self, "final_usd", final_usd)
        object.__setattr__(self, "final_exit_available_at_ns", final_exit_ns)
        object.__setattr__(self, "allocation_percent", allocation)
        object.__setattr__(self, "max_concurrent_positions", concurrency)

    @property
    def realized_exits(self) -> tuple[RealizedExit, ...]:
        return tuple(trade.realized_exit() for trade in self.accepted_trades)


def _trade_from_object(value: object) -> WalletTrade:
    if isinstance(value, WalletTrade):
        return value
    if not isinstance(value, Mapping):
        raise CapacityGateError("wallet opportunity has an invalid type")
    try:

        def chain_key(raw: object, label: str) -> ChainKey | None:
            if raw is None:
                return None
            if isinstance(raw, ChainKey):
                return raw
            if not isinstance(raw, Mapping):
                raise CapacityGateError(f"{label} has an invalid type")
            return ChainKey(
                slot=_integer(raw.get("slot"), f"{label}.slot"),
                tx_index=_integer(raw.get("tx_index"), f"{label}.tx_index"),
                log_index=_integer(raw.get("log_index"), f"{label}.log_index"),
                event_index=_integer(raw.get("event_index"), f"{label}.event_index"),
                event_id=_text(raw.get("event_id"), f"{label}.event_id"),
            )

        return WalletTrade(
            trade_id=_text(value.get("trade_id", value.get("id")), "trade_id"),
            mint=_text(value.get("mint"), "mint"),
            entry_available_at_ns=_integer(
                value.get("entry_available_at_ns", value.get("entry_ns")),
                "entry_available_at_ns",
            ),
            exit_available_at_ns=_integer(
                value.get("exit_available_at_ns", value.get("exit_ns")),
                "exit_available_at_ns",
            ),
            entry_quote_atomic=_integer(
                value.get("entry_quote_atomic", value.get("entry_atomic")),
                "entry_quote_atomic",
            ),
            exit_quote_atomic=_integer(
                value.get("exit_quote_atomic", value.get("exit_atomic")),
                "exit_quote_atomic",
            ),
            entry_cost_quote_atomic=_integer(
                value.get("entry_cost_quote_atomic", 0), "entry_cost_quote_atomic"
            ),
            exit_cost_quote_atomic=_integer(
                value.get("exit_cost_quote_atomic", 0), "exit_cost_quote_atomic"
            ),
            quote_decimals=_integer(value.get("quote_decimals", SOL_DECIMALS), "quote_decimals"),
            entry_chain_key=chain_key(
                value.get("entry_chain_key", value.get("entry_key")), "entry_chain_key"
            ),
            exit_chain_key=chain_key(
                value.get("exit_chain_key", value.get("exit_key")), "exit_chain_key"
            ),
        )
    except (TypeError, AttributeError) as error:
        raise CapacityGateError("wallet opportunity is missing required fields") from error


def replay_shared_wallet(
    trades: Iterable[WalletTrade | Mapping[str, object]],
    fx: CausalFxSeries,
    *,
    starting_usd: Decimal,
    start_available_at_ns: int,
    allocation_percent: int = 20,
    max_concurrent_positions: int = 5,
) -> SharedWalletReplay:
    """Replay one cash-constrained chronological wallet.

    This is the prequoted accounting adapter retained for exact-v2 parity
    plumbing.  It does not perform venue-specific exact quoting; callers must
    not present its prequoted rows as independently requoted capacity evidence.
    Entries are ordered by landed availability and chain key.  Due exits settle
    before an entry at the same timestamp.  The gross entry quote must fit the
    floor of ``allocation_percent`` of cost-basis equity (cash plus principal
    locked in open positions); fixed entry cost is additionally checked against
    cash.  A mint cannot be opened twice concurrently.
    """

    if not isinstance(fx, CausalFxSeries):
        raise CapacityGateError("causal FX evidence has an invalid type")
    capital = _decimal(starting_usd, "starting_usd", positive=True)
    start_ns = _integer(start_available_at_ns, "start_available_at_ns")
    allocation = _integer(allocation_percent, "allocation_percent")
    if allocation < 1 or allocation > 100:
        raise CapacityGateError("allocation_percent must be in [1, 100]")
    concurrency = _integer(max_concurrent_positions, "max_concurrent_positions")
    if concurrency < 1:
        raise CapacityGateError("max_concurrent_positions must be positive")
    parsed = tuple(_trade_from_object(item) for item in trades)
    if not parsed:
        raise CapacityGateError("shared wallet has no opportunities")
    if len({item.trade_id for item in parsed}) != len(parsed):
        raise CapacityGateError("duplicate wallet trade ID")
    if any(item.quote_decimals != SOL_DECIMALS for item in parsed):
        raise CapacityGateError("shared wallet requires SOL quote precision")
    all_chain_keys: list[ChainKey] = []
    for item in parsed:
        entry_key = item.entry_chain_key
        exit_key = item.exit_chain_key
        if not isinstance(entry_key, ChainKey) or not isinstance(exit_key, ChainKey):
            raise CapacityGateError("all wallet trades require landed chain keys")
        if exit_key <= entry_key:
            raise CapacityGateError("wallet trade chain chronology is invalid")
        all_chain_keys.extend((entry_key, exit_key))
    if len(all_chain_keys) != len(set(all_chain_keys)):
        raise CapacityGateError("duplicate wallet landed chain key")

    # Availability chronology and landed chronology must agree.  A delayed
    # row cannot be used to move an earlier landed event behind a later one.
    chronology = sorted(
        [
            (item.entry_available_at_ns, cast(ChainKey, item.entry_chain_key), "entry")
            for item in parsed
        ]
        + [
            (item.exit_available_at_ns, cast(ChainKey, item.exit_chain_key), "exit")
            for item in parsed
        ],
        key=lambda value: (value[0], value[1]),
    )
    for prior, current in zip(chronology, chronology[1:], strict=False):
        if prior[0] < current[0] and prior[1] >= current[1]:
            raise CapacityGateError("landed chain chronology disagrees with availability")
    # Starting quote precision is fixed by the sealed SOL/quote convention.
    starting_quote = fx.atomic_from_usd(capital, start_ns, decimals=SOL_DECIMALS)
    cash = starting_quote
    open_trades: list[WalletTrade] = []
    accepted: list[WalletTrade] = []
    decisions: list[WalletDecision] = []
    minimum_equity = starting_quote
    max_open = 0
    final_exit_ns = start_ns

    def equity() -> int:
        return cash + sum(item.entry_quote_atomic for item in open_trades)

    def settle(timestamp_ns: int, boundary_key: ChainKey | None = None) -> None:
        nonlocal cash, minimum_equity, final_exit_ns

        def is_due(item: WalletTrade) -> bool:
            if item.exit_available_at_ns < timestamp_ns:
                return True
            if item.exit_available_at_ns > timestamp_ns:
                return False
            if boundary_key is None:
                raise CapacityGateError("same-timestamp entry/exit requires landed chain keys")
            return cast(ChainKey, item.exit_chain_key) <= boundary_key

        due = sorted(
            (item for item in open_trades if is_due(item)),
            key=lambda item: (
                item.exit_available_at_ns,
                item.exit_chain_key,
            ),
        )
        for item in due:
            open_trades.remove(item)
            cash += item.exit_quote_atomic - item.exit_cost_quote_atomic
            if cash < 0:
                raise CapacityGateError("wallet cash became negative at settlement")
            final_exit_ns = max(final_exit_ns, item.exit_available_at_ns)
            minimum_equity = min(minimum_equity, equity())

    by_entry_time: dict[int, list[WalletTrade]] = {}
    for trade in parsed:
        by_entry_time.setdefault(trade.entry_available_at_ns, []).append(trade)
    if any(
        len(group) > 1 and any(item.entry_chain_key is None for item in group)
        for group in by_entry_time.values()
    ):
        raise CapacityGateError("same-timestamp entries require landed chain keys")
    entry_keys = [item.entry_chain_key for item in parsed]
    if len(entry_keys) != len(set(entry_keys)):
        raise CapacityGateError("duplicate entry chain key")
    by_exit_time: dict[int, list[WalletTrade]] = {}
    for trade in parsed:
        by_exit_time.setdefault(trade.exit_available_at_ns, []).append(trade)
    if any(
        len(group) > 1 and any(item.exit_chain_key is None for item in group)
        for group in by_exit_time.values()
    ):
        raise CapacityGateError("same-timestamp exits require landed chain keys")
    exit_keys = [item.exit_chain_key for item in parsed]
    if len(exit_keys) != len(set(exit_keys)):
        raise CapacityGateError("duplicate exit chain key")

    def entry_order(item: WalletTrade) -> tuple[object, ...]:
        if item.entry_chain_key is not None:
            return (item.entry_available_at_ns, item.entry_chain_key)
        return (item.entry_available_at_ns, item.trade_id)

    ordered = sorted(parsed, key=entry_order)
    for trade in ordered:
        settle(trade.entry_available_at_ns, trade.entry_chain_key)
        budget = equity() * allocation // 100
        reason: str | None = None
        if any(item.mint == trade.mint for item in open_trades):
            reason = "mint_already_open"
        elif len(open_trades) >= concurrency:
            reason = "max_concurrent_positions"
        elif trade.entry_quote_atomic > budget:
            reason = "allocation_budget"
        elif trade.total_entry_debit_atomic > cash:
            reason = "insufficient_cash"
        if reason is not None:
            decisions.append(
                WalletDecision(
                    trade_id=trade.trade_id,
                    accepted=False,
                    reason=reason,
                    budget_quote_atomic=budget,
                    cash_after_atomic=cash,
                    open_position_count=len(open_trades),
                )
            )
            continue
        cash -= trade.total_entry_debit_atomic
        if cash < 0:
            # This is unreachable after the check above, but keeping the
            # invariant protects future changes to debit accounting.
            raise CapacityGateError("wallet cash became negative")
        open_trades.append(trade)
        accepted.append(trade)
        max_open = max(max_open, len(open_trades))
        minimum_equity = min(minimum_equity, equity())
        decisions.append(
            WalletDecision(
                trade_id=trade.trade_id,
                accepted=True,
                reason="accepted",
                budget_quote_atomic=budget,
                cash_after_atomic=cash,
                open_position_count=len(open_trades),
            )
        )
    # Settle all positions in deterministic exit order.
    for trade in sorted(
        open_trades,
        key=lambda item: (
            item.exit_available_at_ns,
            item.exit_chain_key,
        ),
    ):
        open_trades.remove(trade)
        cash += trade.exit_quote_atomic - trade.exit_cost_quote_atomic
        if cash < 0:
            raise CapacityGateError("wallet cash became negative at final settlement")
        final_exit_ns = max(final_exit_ns, trade.exit_available_at_ns)
        minimum_equity = min(minimum_equity, equity())
    if open_trades:
        raise CapacityGateError("wallet replay has unresolved open positions")
    final_usd = fx.usd_from_atomic(cash, final_exit_ns, decimals=SOL_DECIMALS)
    profitable_quote = cash > starting_quote
    profitable_usd = final_usd > capital
    # This flag describes wallet accounting invariants only.  The ladder adds
    # its independent four-day USD target; terminal FX profitability is not a
    # B-rung pass condition.
    passed = bool(accepted and minimum_equity >= 0 and cash >= 0)
    return SharedWalletReplay(
        starting_usd=capital,
        starting_quote_atomic=starting_quote,
        final_quote_atomic=cash,
        final_usd=final_usd,
        minimum_equity_quote_atomic=minimum_equity,
        decisions=tuple(decisions),
        accepted_trades=tuple(accepted),
        maximum_concurrent_positions=max_open,
        profitable_in_quote=profitable_quote,
        profitable_in_usd=profitable_usd,
        passed=passed,
        final_exit_available_at_ns=final_exit_ns,
        allocation_percent=allocation,
        max_concurrent_positions=concurrency,
    )


@dataclass(frozen=True, slots=True)
class LadderProof:
    """Proof attached before a maximum may be reported."""

    exhaustive: bool = False
    bounded: bool = False
    upper_bound_usd: Decimal | None = None
    statement: str = ""

    def __post_init__(self) -> None:
        _bool(self.exhaustive, "exhaustive")
        _bool(self.bounded, "bounded")
        if self.exhaustive == self.bounded:
            raise CapacityGateError("ladder proof must be exactly exhaustive or bounded")
        if self.bounded:
            if self.upper_bound_usd is None:
                raise CapacityGateError("bounded proof requires upper_bound_usd")
            upper = _decimal(self.upper_bound_usd, "upper_bound_usd", positive=True)
            object.__setattr__(self, "upper_bound_usd", upper)
        if not _text(self.statement, "statement"):
            raise CapacityGateError("ladder proof statement is required")


@dataclass(frozen=True, slots=True)
class LadderRung:
    starting_usd: Decimal
    wallet: SharedWalletReplay
    attribution: FourDayAttribution

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "starting_usd", _decimal(self.starting_usd, "starting_usd", positive=True)
        )
        if not isinstance(self.wallet, SharedWalletReplay):
            raise CapacityGateError("ladder rung wallet has an invalid type")
        if not isinstance(self.attribution, FourDayAttribution):
            raise CapacityGateError("ladder rung attribution has an invalid type")
        if self.starting_usd != self.wallet.starting_usd:
            raise CapacityGateError("ladder rung seed differs from wallet seed")

    @property
    def passed(self) -> bool:
        return self.wallet.passed and self.attribution.passed


ReplayFactory: TypeAlias = Callable[
    [Decimal, int],
    SharedWalletReplay,
]


@dataclass(frozen=True, slots=True)
class LadderSearchResult:
    rungs: tuple[LadderRung, ...]
    proof: LadderProof | None
    search_complete: bool
    smallest_passing_usd: Decimal | None
    maximum_passing_usd: Decimal | None

    def __post_init__(self) -> None:
        if not isinstance(self.rungs, tuple):
            raise CapacityGateError("ladder search rungs must be a tuple")
        if not self.rungs:
            raise CapacityGateError("ladder search has no rungs")
        _bool(self.search_complete, "search_complete")
        # v1 has no proof/max-capacity publication boundary.  Keep the
        # historical fields for import compatibility, but never permit a
        # result object to carry either value.
        if self.search_complete:
            raise CapacityGateError("v1 ladder search cannot claim completeness")
        if self.proof is not None:
            raise CapacityGateError("v1 ladder search cannot expose a proof")
        prior: Decimal | None = None
        for rung in self.rungs:
            if not isinstance(rung, LadderRung):
                raise CapacityGateError("ladder rung has an invalid type")
            if prior is not None and rung.starting_usd <= prior:
                raise CapacityGateError("ladder rungs must be strictly increasing")
            prior = rung.starting_usd
        if self.smallest_passing_usd is not None:
            _decimal(self.smallest_passing_usd, "smallest_passing_usd", positive=True)
        if self.maximum_passing_usd is not None:
            raise CapacityGateError("v1 ladder search cannot expose a maximum")
        passing = [rung.starting_usd for rung in self.rungs if rung.passed]
        expected_smallest = min(passing) if passing else None
        if self.smallest_passing_usd != expected_smallest:
            raise CapacityGateError("smallest passing rung is inconsistent")

    @property
    def passed(self) -> bool:
        return self.smallest_passing_usd is not None


NotionalLadder = tuple[int, ...]


def validate_notional_ladder(ladder: Iterable[int | Decimal]) -> tuple[int, ...]:
    """Validate a strictly increasing, positive integer-valued ladder."""

    values = tuple(ladder)
    if not values:
        raise CapacityGateError("notional ladder cannot be empty")
    converted: list[int] = []
    previous: int | None = None
    for index, raw in enumerate(values):
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise CapacityGateError(f"notional ladder[{index}] must be an integer")
        if raw <= 0:
            raise CapacityGateError(f"notional ladder[{index}] must be positive")
        value = raw
        if previous is not None and value <= previous:
            raise CapacityGateError("notional ladder must be strictly increasing")
        previous = value
        converted.append(value)
    return tuple(converted)


def evaluate_starting_capital_ladder(
    trades: Iterable[WalletTrade | Mapping[str, object]] | None,
    fx: CausalFxSeries,
    *,
    availability_start_ns: int,
    availability_end_ns: int,
    ladder: Iterable[int] = STARTING_CAPITAL_LADDER_USD,
    complete_days: Iterable[CompleteUtcDay | Mapping[str, object]] | None = None,
    allocation_percent: int = 20,
    max_concurrent_positions: int = 5,
    proof: LadderProof | None = None,
    replay_factory: ReplayFactory | None = None,
) -> LadderSearchResult:
    """Run every frozen capital rung and enforce the four-day USD target.

    The ladder is finite by design and is always reported with
    ``search_complete=False``.  A caller that wants to prove a separate bounded
    integer domain must use an independently sealed proof/reporting step; this
    v1 function refuses a proof argument so a finite rung list cannot be
    mislabeled as a global maximum.
    """

    if proof is not None:
        raise CapacityGateError(
            "finite v1 capital ladder cannot claim an exhaustive/bounded maximum"
        )
    if replay_factory is None:
        raise CapacityGateError("per-rung exact replay factory is required")
    ladder_values = validate_notional_ladder(ladder)
    if ladder_values != STARTING_CAPITAL_LADDER_USD:
        raise CapacityGateError("capacity ladder must equal the frozen 19-rung ladder")
    _integer(allocation_percent, "allocation_percent")
    _integer(max_concurrent_positions, "max_concurrent_positions")
    if allocation_percent != 20 or max_concurrent_positions != 5:
        raise CapacityGateError("capacity wallet policy is frozen at 20% allocation and max 5")
    if trades is not None:
        # Optional ledger rows are still validated when supplied, but they are
        # never reused as fills for another rung.  Every rung must come from
        # the exact replay factory below.
        tuple(_trade_from_object(item) for item in trades)
    # Validate day coverage before running any candidate; otherwise a partial
    # candidate could be mistaken for a failed candidate.
    complete_day_evidence = None if complete_days is None else tuple(complete_days)
    selected_days = earliest_four_complete_days(
        availability_start_ns,
        availability_end_ns,
        complete_days=complete_day_evidence,
    )
    window_start = selected_days[0]
    window_end = selected_days[-1] + DAY_NS
    rungs: list[LadderRung] = []
    for starting_usd in ladder_values:
        seed = Decimal(starting_usd)
        try:
            factory_result = replay_factory(seed, window_start)
        except CapacityGateError:
            raise
        except Exception as error:
            raise CapacityGateError("per-rung exact replay failed") from error
        # Attribution is never accepted from a callback.  The callback is
        # required to return exactly one sealed wallet ledger, and this kernel
        # recomputes the four daily nets solely from that wallet's unique
        # realised exits below.
        if type(factory_result) is not SharedWalletReplay:
            raise CapacityGateError("per-rung replay result is invalid")
        wallet = factory_result
        if wallet.starting_usd != seed:
            raise CapacityGateError("per-rung replay seed does not match requested capital")
        expected_starting_quote = fx.atomic_from_usd(seed, window_start, decimals=SOL_DECIMALS)
        if wallet.starting_quote_atomic != expected_starting_quote:
            raise CapacityGateError("per-rung replay did not use causal D0 FX seed")
        expected_final_usd = fx.usd_from_atomic(
            wallet.final_quote_atomic,
            wallet.final_exit_available_at_ns,
            decimals=SOL_DECIMALS,
        )
        if wallet.final_usd != expected_final_usd:
            raise CapacityGateError("per-rung replay final USD is inconsistent with causal FX")
        if (
            wallet.allocation_percent != allocation_percent
            or wallet.max_concurrent_positions != max_concurrent_positions
        ):
            raise CapacityGateError("per-rung replay policy differs from frozen wallet policy")
        for trade in wallet.accepted_trades:
            if not (
                window_start <= trade.entry_available_at_ns < window_end
                and window_start <= trade.exit_available_at_ns < window_end
            ):
                raise CapacityGateError("position crosses the four-day UTC capacity window")
        # A replay can have no exits for a day.  Attribution still includes the
        # day (net zero), preserving the every-day pass condition.  There is no
        # supplied-attribution escape hatch: all USD attribution is recomputed
        # from the wallet's SOL-decimal realised exits.
        attribution = attribute_realized_net(
            wallet.realized_exits,
            fx,
            availability_start_ns=availability_start_ns,
            availability_end_ns=availability_end_ns,
            complete_days=complete_day_evidence,
        )
        if attribution.target_usd != USD_DAILY_CAPACITY_TARGET:
            raise CapacityGateError("per-rung replay changed frozen daily target")
        if tuple(day.day_start_ns for day in attribution.days) != selected_days:
            raise CapacityGateError("ladder day coverage changed between rungs")
        rungs.append(LadderRung(Decimal(starting_usd), wallet, attribution))
    passing = [rung.starting_usd for rung in rungs if rung.passed]
    smallest = min(passing) if passing else None
    complete = False
    maximum = None
    return LadderSearchResult(
        rungs=tuple(rungs),
        proof=proof,
        search_complete=complete,
        smallest_passing_usd=smallest,
        maximum_passing_usd=maximum,
    )


def report_maximum(result: LadderSearchResult, proof: LadderProof) -> Decimal | None:
    """Reject maximum publication in the frozen v1 kernel."""

    if not isinstance(result, LadderSearchResult) or not isinstance(proof, LadderProof):
        raise CapacityGateError("maximum report requires a ladder result and proof")
    raise CapacityGateError("v1 capacity reports never expose a maximum or proof")


# Compatibility spelling for orchestration code that calls the ladder a
# notional ladder.  It is the same pure function and carries the same proof
# requirement; no second implementation is allowed to diverge.


def assemble_capacity_report(
    ladder: LadderSearchResult,
    *,
    development_inputs_complete: bool = True,
    landing_telemetry_complete: bool = False,
    holdout_accessed: bool = False,
    holdout_unlocked: bool = False,
) -> dict[str, object]:
    """Build deterministic development-only status/report payload.

    Holdout flags are assertions, not caller-controlled publication switches:
    any true value is rejected.  Missing development evidence blocks the report
    and empirical landing telemetry is required for deployment readiness, but
    telemetry absence does not erase a valid simulated development result.
    """

    if not isinstance(ladder, LadderSearchResult):
        raise CapacityGateError("ladder result has an invalid type")
    _bool(development_inputs_complete, "development_inputs_complete")
    _bool(landing_telemetry_complete, "landing_telemetry_complete")
    if _bool(holdout_accessed, "holdout_accessed") or _bool(holdout_unlocked, "holdout_unlocked"):
        raise CapacityGateError("holdout access or unlock is forbidden")
    blocked = not development_inputs_complete
    status = (
        "capacity_blocked_missing_evidence"
        if blocked
        else (
            "capacity_passed_development_only" if ladder.passed else "capacity_failed_daily_target"
        )
    )
    rung_payload = []
    for rung in ladder.rungs:
        rung_payload.append(
            {
                "starting_usd": _canonical_decimal(rung.starting_usd),
                "passed": rung.passed,
                "final_usd": _canonical_decimal(rung.wallet.final_usd),
                "final_quote_atomic": rung.wallet.final_quote_atomic,
                "starting_quote_atomic": rung.wallet.starting_quote_atomic,
                "minimum_equity_quote_atomic": rung.wallet.minimum_equity_quote_atomic,
                "accepted_trade_count": len(rung.wallet.accepted_trades),
                "rejected_trade_count": sum(
                    1 for decision in rung.wallet.decisions if not decision.accepted
                ),
                "maximum_concurrent_positions": rung.wallet.maximum_concurrent_positions,
                "daily_net_usd": [_canonical_decimal(day.net_usd) for day in rung.attribution.days],
                "daily_passed": rung.attribution.passed,
            }
        )
    report: dict[str, object] = {
        "kind": "pumpquant.capacity_gate_report",
        "version": 1,
        "status": status,
        "development_only": True,
        "holdout_accessed": False,
        "holdout_unlocked": False,
        # This v1 kernel never confers live/deployment readiness.  Historical
        # landing/MEV telemetry is a separate later authorisation boundary.
        "simulated_only": True,
        "deployment_readiness": False,
        "search_complete": ladder.search_complete,
        "smallest_passing_usd": (
            None
            if ladder.smallest_passing_usd is None
            else _canonical_decimal(ladder.smallest_passing_usd)
        ),
        "smallest_target_meeting_starting_usd": (
            None
            if ladder.smallest_passing_usd is None
            else _canonical_decimal(ladder.smallest_passing_usd)
        ),
        "rungs": rung_payload,
    }
    return cast(
        dict[str, object],
        json.loads(json.dumps(report, sort_keys=True, separators=(",", ":"))),
    )


def deterministic_report_json(report: Mapping[str, object]) -> str:
    """Serialise a report with stable ordering and no floating-point NaN."""

    def reject_float(value: object) -> None:
        if isinstance(value, float):
            raise CapacityGateError("report money may not use floating-point values")
        if isinstance(value, Mapping):
            for nested in value.values():
                reject_float(nested)
        elif isinstance(value, (list, tuple)):
            for nested in value:
                reject_float(nested)

    reject_float(report)
    try:
        return (
            json.dumps(dict(report), sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        )
    except (TypeError, ValueError) as error:
        raise CapacityGateError("report contains non-JSON evidence") from error


__all__ = [
    "CapacityGateError",
    "ChainKey",
    "CausalFxPoint",
    "CausalFxSeries",
    "CompleteUtcDay",
    "DAY_NS",
    "DailyAttribution",
    "FOUR_DAYS",
    "FourDayAttribution",
    "LadderProof",
    "LadderRung",
    "LadderSearchResult",
    "RealInventoryCapacity",
    "RealizedExit",
    "ReplayFactory",
    "SOL_DECIMALS",
    "STARTING_CAPITAL_LADDER_USD",
    "SharedWalletReplay",
    "NotionalLadder",
    "TradeSide",
    "USD_DAILY_CAPACITY_TARGET",
    "Venue",
    "WalletDecision",
    "WalletTrade",
    "assemble_capacity_report",
    "attribute_realized_net",
    "deterministic_report_json",
    "earliest_four_complete_days",
    "evaluate_starting_capital_ladder",
    "has_real_inventory_capacity",
    "pump_capacity_predicate",
    "passes_daily_target",
    "pumpswap_capacity_predicate",
    "real_inventory_capacity",
    "replay_shared_wallet",
    "report_maximum",
    "utc_day_start_ns",
    "validate_notional_ladder",
]
