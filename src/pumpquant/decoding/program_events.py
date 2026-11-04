"""Pinned Pump/PumpSwap Anchor trade, lifecycle, and state-event decoding.

Successful Pump and PumpSwap instructions emit landed amounts, post-trade
reserves, fee components, trader/pool identities, side, and market-lifecycle
records through Anchor ``Program data: <base64>`` log records.  This module
decodes the three trade events, lifecycle events, and state-mutating
fee/config/liquidity events in the official IDLs pinned by
:mod:`pumpquant.decoding.instruction_intent`.  In particular, PumpSwap
``DepositEvent``/``WithdrawEvent`` expose exact post-operation pool reserves
and LP supply, while ``CreateConfigEvent``, ``UpdateFeeConfigEvent``, and
``DisableEvent`` expose configuration schedules.  Pump ``SetParamsEvent`` and
``UpdateMayhemVirtualParamsEvent`` expose the global and curve-reserve changes
that cannot be inferred from a trade alone.

The invocation stack attributes each data record to the program that emitted
it. Unknown event discriminators are ignored by the log iterators; a
recognised discriminator with malformed Borsh or trailing bytes fails closed.
Transaction success is outside this module: callers must reject
non-null ``meta.err`` before promoting an event to a canonical swap.
"""

from __future__ import annotations

import base64
import binascii
import re
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field, make_dataclass
from types import MappingProxyType
from typing import ClassVar, Final, Literal, TypeAlias, cast

from pumpquant.decoding.instruction_intent import (
    PINNED_IDL_REVISION,
    PUMP_PROGRAM_ID,
    PUMPSWAP_PROGRAM_ID,
    encode_base58,
)

PUMP_TRADE_EVENT_DISCRIMINATOR: Final[bytes] = bytes((189, 219, 127, 211, 78, 230, 97, 238))
PUMPSWAP_BUY_EVENT_DISCRIMINATOR: Final[bytes] = bytes((103, 244, 82, 31, 44, 245, 119, 119))
PUMPSWAP_SELL_EVENT_DISCRIMINATOR: Final[bytes] = bytes((62, 47, 55, 10, 165, 3, 220, 42))

# Lifecycle event discriminators from the pinned Pump/PumpSwap IDLs.  Keep
# these as bytes (rather than integer tuples) so they can be compared directly
# with the first eight bytes of an Anchor event payload.
PUMP_CREATE_EVENT_DISCRIMINATOR: Final[bytes] = bytes((27, 114, 169, 77, 222, 235, 99, 118))
PUMP_COMPLETE_EVENT_DISCRIMINATOR: Final[bytes] = bytes((95, 114, 97, 156, 212, 46, 152, 8))
PUMP_COMPLETE_PUMP_AMM_MIGRATION_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (189, 233, 93, 185, 92, 148, 234, 148)
)
PUMPSWAP_CREATE_POOL_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (177, 49, 12, 210, 160, 118, 167, 116)
)

# State/configuration event discriminators from the exact pinned IDLs.  These
# are kept as bytes so callers can use the same discriminator
# values for raw ``Program data`` records and decoded event provenance.
PUMP_SET_PARAMS_EVENT_DISCRIMINATOR: Final[bytes] = bytes((223, 195, 159, 246, 62, 48, 143, 131))
PUMP_UPDATE_MAYHEM_VIRTUAL_PARAMS_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (117, 123, 228, 182, 161, 168, 220, 214)
)
PUMP_SET_CREATOR_EVENT_DISCRIMINATOR: Final[bytes] = bytes((237, 52, 123, 37, 245, 251, 72, 210))
PUMP_MIGRATE_BONDING_CURVE_CREATOR_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (155, 167, 104, 220, 213, 108, 243, 3)
)
PUMP_DISTRIBUTE_CREATOR_FEES_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (165, 55, 129, 112, 4, 179, 202, 40)
)
PUMP_COLLECT_CREATOR_FEE_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (122, 2, 127, 1, 14, 191, 12, 175)
)
PUMP_RESERVED_FEE_RECIPIENTS_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (43, 188, 250, 18, 221, 75, 187, 95)
)
PUMP_UPDATE_GLOBAL_AUTHORITY_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (182, 195, 137, 42, 35, 206, 207, 247)
)

PUMPSWAP_DEPOSIT_EVENT_DISCRIMINATOR: Final[bytes] = bytes((120, 248, 61, 83, 31, 142, 107, 144))
PUMPSWAP_WITHDRAW_EVENT_DISCRIMINATOR: Final[bytes] = bytes((22, 9, 133, 26, 160, 44, 71, 192))
PUMPSWAP_BOOST_BUY_AND_BURN_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (63, 69, 28, 22, 48, 92, 194, 185)
)
PUMPSWAP_CREATE_CONFIG_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (107, 52, 89, 129, 55, 226, 81, 22)
)
PUMPSWAP_UPDATE_FEE_CONFIG_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (90, 23, 65, 35, 62, 244, 188, 208)
)
PUMPSWAP_DISABLE_EVENT_DISCRIMINATOR: Final[bytes] = bytes((107, 253, 193, 76, 228, 202, 27, 104))
PUMPSWAP_UPDATE_ADMIN_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (225, 152, 171, 87, 246, 63, 66, 234)
)
PUMPSWAP_MIGRATE_POOL_COIN_CREATOR_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (170, 221, 82, 199, 147, 165, 247, 46)
)
PUMPSWAP_SET_BONDING_CURVE_COIN_CREATOR_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (242, 231, 235, 102, 65, 99, 189, 211)
)
PUMPSWAP_SET_METAPLEX_COIN_CREATOR_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (150, 107, 199, 123, 124, 207, 102, 228)
)
PUMPSWAP_COLLECT_COIN_CREATOR_FEE_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (232, 245, 194, 238, 234, 218, 58, 89)
)
PUMPSWAP_RESERVED_FEE_RECIPIENTS_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (43, 188, 250, 18, 221, 75, 187, 95)
)
PUMPSWAP_ADMIN_SET_COIN_CREATOR_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (45, 220, 93, 24, 25, 97, 172, 104)
)
PUMPSWAP_EXTEND_ACCOUNT_EVENT_DISCRIMINATOR: Final[bytes] = bytes(
    (97, 97, 215, 144, 93, 146, 22, 124)
)

PUMP_EVENT_IDL_SOURCE: Final[str] = (
    f"github.com/pump-fun/pump-public-docs@{PINNED_IDL_REVISION}:idl/pump.json"
)
PUMPSWAP_EVENT_IDL_SOURCE: Final[str] = (
    f"github.com/pump-fun/pump-public-docs@{PINNED_IDL_REVISION}:idl/pump_amm.json"
)
PUMP_EVENT_IDL_SHA256: Final[str] = (
    "b90bc471327f671449271d5d1d42354d1fae6f5a06502f5834459a3108138e49"
)
PUMPSWAP_EVENT_IDL_SHA256: Final[str] = (
    "cd5c58a5dded23632aedb993b08cc0d2ba91bda2bb084560913464274d3bee50"
)
PUMPSWAP_BOOST_EVENT_IDL_SOURCE: Final[str] = "@pump-fun/pump-swap-sdk@1.19.0:src/idl/pump_amm.json"
PUMPSWAP_BOOST_EVENT_IDL_SHA256: Final[str] = (
    "e18072707bd59afd1fca2245a18b48eee40c64904f2b34aa84100a0f014ddbe0"
)
# Lifecycle spellings make provenance clear to callers while
# retaining the existing source constants used by trade-event consumers.

_PROGRAM_DATA_PREFIX = "Program data: "
_PUBKEY = r"[1-9A-HJ-NP-Za-km-z]{32,44}"
_INVOKE_RE = re.compile(rf"^Program ({_PUBKEY}) invoke \[([1-9][0-9]*)\]$")
_SUCCESS_RE = re.compile(rf"^Program ({_PUBKEY}) success$")
_FAILED_RE = re.compile(rf"^Program ({_PUBKEY}) failed:")
_MAX_EVENT_BYTES = 1024 * 1024

_FieldKind = Literal[
    "u8",
    "u16",
    "u64",
    "i64",
    "bool",
    "pubkey",
    "string",
    "shareholders",
    "pubkeys7",
    "pubkeys8",
    "u128",
    "i128",
]


class ProgramEventDecodeError(ValueError):
    """Base class for pinned trade/lifecycle/state-event decoding failures."""


class UnknownProgramEventError(ProgramEventDecodeError):
    """No pinned event discriminator matches the program and payload."""


class MalformedProgramEventError(ProgramEventDecodeError):
    """A recognised event has malformed, truncated, or trailing Borsh bytes."""


@dataclass(frozen=True, slots=True)
class Shareholder:
    address: str
    share_bps: int


@dataclass(frozen=True, slots=True)
class DecodedTradeEvent:
    program_id: str
    event_name: str
    discriminator: bytes
    values: Mapping[str, object]
    raw_data: bytes
    idl_revision: str = PINNED_IDL_REVISION

    def __post_init__(self) -> None:
        if self.program_id not in {PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID}:
            raise ValueError("trade event program is not Pump or PumpSwap")
        if self.event_name not in {"TradeEvent", "BuyEvent", "SellEvent"}:
            raise ValueError("unsupported trade event name")
        if len(self.discriminator) != 8 or not self.raw_data.startswith(self.discriminator):
            raise ValueError("trade event discriminator does not match raw data")
        if self.idl_revision != PINNED_IDL_REVISION:
            raise ValueError("trade event does not use the pinned IDL revision")
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))

    @property
    def side(self) -> Literal["buy", "sell"]:
        if self.event_name == "BuyEvent":
            return "buy"
        if self.event_name == "SellEvent":
            return "sell"
        value = self.values.get("is_buy")
        if not isinstance(value, bool):  # pragma: no cover - spec invariant
            raise RuntimeError("Pump TradeEvent has no boolean is_buy")
        return "buy" if value else "sell"


@dataclass(frozen=True, slots=True)
class TradeEventLog:
    log_index: int
    invoke_depth: int
    event: DecodedTradeEvent

    def __post_init__(self) -> None:
        if self.log_index < 0:
            raise ValueError("log_index must be non-negative")
        if self.invoke_depth < 1:
            raise ValueError("invoke_depth must be positive")


@dataclass(frozen=True, slots=True)
class LifecycleEventLog:
    """A lifecycle event attributed to its emitting invocation frame."""

    log_index: int
    invoke_depth: int
    event: LifecycleEvent

    def __post_init__(self) -> None:
        if self.log_index < 0:
            raise ValueError("log_index must be non-negative")
        if self.invoke_depth < 1:
            raise ValueError("invoke_depth must be positive")


@dataclass(frozen=True, slots=True)
class StateEventLog:
    """A state/configuration event attributed to its emitting invocation."""

    log_index: int
    invoke_depth: int
    event: DecodedStateEvent

    def __post_init__(self) -> None:
        if self.log_index < 0:
            raise ValueError("log_index must be non-negative")
        if self.invoke_depth < 1:
            raise ValueError("invoke_depth must be positive")


@dataclass(frozen=True, slots=True)
class _EventSpec:
    program_id: str
    name: str
    discriminator: bytes
    fields: tuple[tuple[str, _FieldKind], ...]


class DecodedLifecycleEvent:
    """Common base for the typed Pump/PumpSwap lifecycle event records.

    The concrete dataclasses below expose every IDL field as a typed
    attribute.  ``values`` is provided as a small convenience for consumers
    that handle trade and lifecycle events through one mapping-oriented path;
    it contains only the fields defined by the corresponding pinned IDL.
    """

    __slots__ = ()

    # These annotations describe metadata supplied by each concrete event
    # dataclass.  They are not slots or constructor arguments on the base.
    program_id: str
    event_name: str
    discriminator: bytes
    raw_data: bytes
    idl_revision: str
    _FIELD_NAMES: ClassVar[tuple[str, ...]]

    @property
    def values(self) -> Mapping[str, object]:
        names = type(self)._FIELD_NAMES
        return MappingProxyType({name: getattr(self, name) for name in names})

    @property
    def kind(self) -> str:
        return self.event_name

    @property
    def event_type(self) -> str:
        return self.kind


def _validate_lifecycle_metadata(
    event: DecodedLifecycleEvent,
    expected_program_id: str,
    expected_event_name: str,
    expected_discriminator: bytes,
) -> None:
    """Validate metadata on a concrete lifecycle dataclass.

    Metadata fields are not constructor arguments, but validating them here
    keeps the same fail-closed invariants as :class:`DecodedTradeEvent` if a
    caller ever subclasses or mutates a record through low-level means.
    """

    if event.program_id != expected_program_id:
        raise ValueError("lifecycle event program is not the expected program")
    if event.event_name != expected_event_name:
        raise ValueError("lifecycle event name does not match its IDL")
    discriminator = event.discriminator
    if discriminator != expected_discriminator or len(discriminator) != 8:
        raise ValueError("lifecycle event discriminator does not match its IDL")
    idl_revision = event.idl_revision
    if idl_revision != PINNED_IDL_REVISION:
        raise ValueError("lifecycle event does not use the pinned IDL revision")
    raw_data = event.raw_data
    if raw_data and (len(raw_data) < 8 or raw_data[:8] != expected_discriminator):
        raise ValueError("lifecycle event raw_data discriminator does not match")


@dataclass(frozen=True, slots=True)
class PumpCreateEvent(DecodedLifecycleEvent):
    name: str
    symbol: str
    uri: str
    mint: str
    bonding_curve: str
    user: str
    creator: str
    timestamp: int
    virtual_token_reserves: int
    virtual_sol_reserves: int
    real_token_reserves: int
    token_total_supply: int
    token_program: str
    is_mayhem_mode: bool
    is_cashback_enabled: bool
    quote_mint: str
    virtual_quote_reserves: int
    program_id: str = field(default=PUMP_PROGRAM_ID, init=False, repr=False, compare=False)
    event_name: Literal["CreateEvent"] = field(
        default="CreateEvent", init=False, repr=False, compare=False
    )
    discriminator: bytes = field(
        default=PUMP_CREATE_EVENT_DISCRIMINATOR, init=False, repr=False, compare=False
    )
    raw_data: bytes = field(default=b"", init=False, repr=False, compare=False)
    idl_revision: str = field(default=PINNED_IDL_REVISION, init=False, repr=False, compare=False)

    _FIELD_NAMES: ClassVar[tuple[str, ...]] = (
        "name",
        "symbol",
        "uri",
        "mint",
        "bonding_curve",
        "user",
        "creator",
        "timestamp",
        "virtual_token_reserves",
        "virtual_sol_reserves",
        "real_token_reserves",
        "token_total_supply",
        "token_program",
        "is_mayhem_mode",
        "is_cashback_enabled",
        "quote_mint",
        "virtual_quote_reserves",
    )

    def __post_init__(self) -> None:
        _validate_lifecycle_metadata(
            self, PUMP_PROGRAM_ID, "CreateEvent", PUMP_CREATE_EVENT_DISCRIMINATOR
        )


@dataclass(frozen=True, slots=True)
class PumpCompleteEvent(DecodedLifecycleEvent):
    user: str
    mint: str
    bonding_curve: str
    timestamp: int
    quote_mint: str
    program_id: str = field(default=PUMP_PROGRAM_ID, init=False, repr=False, compare=False)
    event_name: Literal["CompleteEvent"] = field(
        default="CompleteEvent", init=False, repr=False, compare=False
    )
    discriminator: bytes = field(
        default=PUMP_COMPLETE_EVENT_DISCRIMINATOR, init=False, repr=False, compare=False
    )
    raw_data: bytes = field(default=b"", init=False, repr=False, compare=False)
    idl_revision: str = field(default=PINNED_IDL_REVISION, init=False, repr=False, compare=False)

    _FIELD_NAMES: ClassVar[tuple[str, ...]] = (
        "user",
        "mint",
        "bonding_curve",
        "timestamp",
        "quote_mint",
    )

    def __post_init__(self) -> None:
        _validate_lifecycle_metadata(
            self, PUMP_PROGRAM_ID, "CompleteEvent", PUMP_COMPLETE_EVENT_DISCRIMINATOR
        )


@dataclass(frozen=True, slots=True)
class PumpCompletePumpAmmMigrationEvent(DecodedLifecycleEvent):
    user: str
    mint: str
    mint_amount: int
    sol_amount: int
    pool_migration_fee: int
    bonding_curve: str
    timestamp: int
    pool: str
    quote_mint: str
    program_id: str = field(default=PUMP_PROGRAM_ID, init=False, repr=False, compare=False)
    event_name: Literal["CompletePumpAmmMigrationEvent"] = field(
        default="CompletePumpAmmMigrationEvent", init=False, repr=False, compare=False
    )
    discriminator: bytes = field(
        default=PUMP_COMPLETE_PUMP_AMM_MIGRATION_EVENT_DISCRIMINATOR,
        init=False,
        repr=False,
        compare=False,
    )
    raw_data: bytes = field(default=b"", init=False, repr=False, compare=False)
    idl_revision: str = field(default=PINNED_IDL_REVISION, init=False, repr=False, compare=False)

    _FIELD_NAMES: ClassVar[tuple[str, ...]] = (
        "user",
        "mint",
        "mint_amount",
        "sol_amount",
        "pool_migration_fee",
        "bonding_curve",
        "timestamp",
        "pool",
        "quote_mint",
    )

    def __post_init__(self) -> None:
        _validate_lifecycle_metadata(
            self,
            PUMP_PROGRAM_ID,
            "CompletePumpAmmMigrationEvent",
            PUMP_COMPLETE_PUMP_AMM_MIGRATION_EVENT_DISCRIMINATOR,
        )


@dataclass(frozen=True, slots=True)
class PumpSwapCreatePoolEvent(DecodedLifecycleEvent):
    timestamp: int
    index: int
    creator: str
    base_mint: str
    quote_mint: str
    base_mint_decimals: int
    quote_mint_decimals: int
    base_amount_in: int
    quote_amount_in: int
    pool_base_amount: int
    pool_quote_amount: int
    minimum_liquidity: int
    initial_liquidity: int
    lp_token_amount_out: int
    pool_bump: int
    pool: str
    lp_mint: str
    user_base_token_account: str
    user_quote_token_account: str
    coin_creator: str
    is_mayhem_mode: bool
    program_id: str = field(default=PUMPSWAP_PROGRAM_ID, init=False, repr=False, compare=False)
    event_name: Literal["CreatePoolEvent"] = field(
        default="CreatePoolEvent", init=False, repr=False, compare=False
    )
    discriminator: bytes = field(
        default=PUMPSWAP_CREATE_POOL_EVENT_DISCRIMINATOR,
        init=False,
        repr=False,
        compare=False,
    )
    raw_data: bytes = field(default=b"", init=False, repr=False, compare=False)
    idl_revision: str = field(default=PINNED_IDL_REVISION, init=False, repr=False, compare=False)

    _FIELD_NAMES: ClassVar[tuple[str, ...]] = (
        "timestamp",
        "index",
        "creator",
        "base_mint",
        "quote_mint",
        "base_mint_decimals",
        "quote_mint_decimals",
        "base_amount_in",
        "quote_amount_in",
        "pool_base_amount",
        "pool_quote_amount",
        "minimum_liquidity",
        "initial_liquidity",
        "lp_token_amount_out",
        "pool_bump",
        "pool",
        "lp_mint",
        "user_base_token_account",
        "user_quote_token_account",
        "coin_creator",
        "is_mayhem_mode",
    )

    def __post_init__(self) -> None:
        _validate_lifecycle_metadata(
            self,
            PUMPSWAP_PROGRAM_ID,
            "CreatePoolEvent",
            PUMPSWAP_CREATE_POOL_EVENT_DISCRIMINATOR,
        )


class DecodedStateEvent:
    """Common base for pinned state/configuration event records.

    State events are separate from trade and lifecycle events because they do
    not by themselves represent a swap or a venue boundary.  Their fields
    nevertheless remain exact IDL primitives and their ``values`` mapping is
    immutable, making them safe inputs to a causal reserve/config schedule.
    """

    __slots__ = ()

    program_id: str
    event_name: str
    discriminator: bytes
    raw_data: bytes
    idl_revision: str
    _FIELD_NAMES: ClassVar[tuple[str, ...]]

    @property
    def values(self) -> Mapping[str, object]:
        names = type(self)._FIELD_NAMES
        return MappingProxyType({name: getattr(self, _state_internal_name(name)) for name in names})

    @property
    def kind(self) -> str:
        return self.event_name

    @property
    def event_type(self) -> str:
        return self.event_name


def _state_internal_name(name: str) -> str:
    """Return a valid Python attribute for an IDL field spelling."""

    # ``global`` is the sole reserved-keyword field in the pinned event set.
    # Keep ``global`` as the exact mapping key while normalising its typed
    # attribute to ``global_config`` (Python 3.14 rejects keyword dataclass
    # fields during generated ``__init__`` compilation).
    return "global_config" if name == "global" else name


def _validate_state_metadata(
    event: DecodedStateEvent,
    expected_program_id: str,
    expected_event_name: str,
    expected_discriminator: bytes,
) -> None:
    """Apply fail-closed provenance checks to a state-event record."""

    if event.program_id != expected_program_id:
        raise ValueError("state event program is not the expected program")
    if event.event_name != expected_event_name:
        raise ValueError("state event name does not match its IDL")
    if event.discriminator != expected_discriminator or len(event.discriminator) != 8:
        raise ValueError("state event discriminator does not match its IDL")
    if event.idl_revision != PINNED_IDL_REVISION:
        raise ValueError("state event does not use the pinned IDL revision")
    if event.raw_data and (len(event.raw_data) < 8 or event.raw_data[:8] != expected_discriminator):
        raise ValueError("state event raw_data discriminator does not match")


_STATE_ANNOTATIONS: Final[dict[_FieldKind, object]] = {
    "u8": int,
    "u16": int,
    "u64": int,
    "u128": int,
    "i128": int,
    "i64": int,
    "bool": bool,
    "pubkey": str,
    "string": str,
    "shareholders": tuple[Shareholder, ...],
    "pubkeys7": tuple[str, ...],
    "pubkeys8": tuple[str, ...],
}


def _state_event_class(
    class_name: str,
    program_id: str,
    event_name: str,
    discriminator: bytes,
    fields: tuple[tuple[str, _FieldKind], ...],
) -> type[DecodedStateEvent]:
    """Build one immutable, field-typed dataclass from a pinned IDL layout.

    The layouts are data copied from the hash-locked IDL, so generating the
    repetitive metadata fields in one place reduces the chance that a future
    event alias accidentally carries the wrong program/discriminator.  The
    resulting classes are ordinary dataclasses: every IDL field is an
    attribute, has a concrete annotation, and participates in ``values``.
    """

    field_defs: list[tuple[str, object] | tuple[str, object, object]] = [
        (_state_internal_name(name), _STATE_ANNOTATIONS[kind]) for name, kind in fields
    ]
    field_defs.extend(
        [
            ("program_id", str, field(default=program_id, init=False, repr=False, compare=False)),
            (
                "event_name",
                str,
                field(default=event_name, init=False, repr=False, compare=False),
            ),
            (
                "discriminator",
                bytes,
                field(default=discriminator, init=False, repr=False, compare=False),
            ),
            ("raw_data", bytes, field(default=b"", init=False, repr=False, compare=False)),
            (
                "idl_revision",
                str,
                field(
                    default=PINNED_IDL_REVISION,
                    init=False,
                    repr=False,
                    compare=False,
                ),
            ),
        ]
    )

    def __post_init__(event: DecodedStateEvent) -> None:
        _validate_state_metadata(event, program_id, event_name, discriminator)

    namespace: dict[str, object] = {
        "_FIELD_NAMES": tuple(name for name, _ in fields),
        "__post_init__": __post_init__,
    }
    generated = make_dataclass(
        class_name,
        field_defs,
        bases=(DecodedStateEvent,),
        namespace=namespace,
        frozen=True,
        slots=True,
    )
    return cast(type[DecodedStateEvent], generated)


# Typed state/configuration event classes.  The fields are populated from the
# corresponding event type in the pinned official IDL below.  Less central
# account/incentive events use :class:`GenericStateEvent` but remain strictly
# parsed and included in the dispatch table.
PumpSetParamsEvent = _state_event_class(
    "PumpSetParamsEvent",
    PUMP_PROGRAM_ID,
    "SetParamsEvent",
    PUMP_SET_PARAMS_EVENT_DISCRIMINATOR,
    (
        ("initial_virtual_token_reserves", "u64"),
        ("initial_virtual_sol_reserves", "u64"),
        ("initial_real_token_reserves", "u64"),
        ("final_real_sol_reserves", "u64"),
        ("token_total_supply", "u64"),
        ("fee_basis_points", "u64"),
        ("withdraw_authority", "pubkey"),
        ("enable_migrate", "bool"),
        ("pool_migration_fee", "u64"),
        ("creator_fee_basis_points", "u64"),
        ("fee_recipients", "pubkeys8"),
        ("timestamp", "i64"),
        ("set_creator_authority", "pubkey"),
        ("admin_set_creator_authority", "pubkey"),
    ),
)
PumpUpdateMayhemVirtualParamsEvent = _state_event_class(
    "PumpUpdateMayhemVirtualParamsEvent",
    PUMP_PROGRAM_ID,
    "UpdateMayhemVirtualParamsEvent",
    PUMP_UPDATE_MAYHEM_VIRTUAL_PARAMS_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("mint", "pubkey"),
        ("virtual_token_reserves", "u64"),
        ("virtual_sol_reserves", "u64"),
        ("new_virtual_token_reserves", "u64"),
        ("new_virtual_sol_reserves", "u64"),
        ("real_token_reserves", "u64"),
        ("real_sol_reserves", "u64"),
    ),
)
PumpSetCreatorEvent = _state_event_class(
    "PumpSetCreatorEvent",
    PUMP_PROGRAM_ID,
    "SetCreatorEvent",
    PUMP_SET_CREATOR_EVENT_DISCRIMINATOR,
    (("timestamp", "i64"), ("mint", "pubkey"), ("bonding_curve", "pubkey"), ("creator", "pubkey")),
)
PumpMigrateBondingCurveCreatorEvent = _state_event_class(
    "PumpMigrateBondingCurveCreatorEvent",
    PUMP_PROGRAM_ID,
    "MigrateBondingCurveCreatorEvent",
    PUMP_MIGRATE_BONDING_CURVE_CREATOR_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("mint", "pubkey"),
        ("bonding_curve", "pubkey"),
        ("sharing_config", "pubkey"),
        ("old_creator", "pubkey"),
        ("new_creator", "pubkey"),
    ),
)
PumpDistributeCreatorFeesEvent = _state_event_class(
    "PumpDistributeCreatorFeesEvent",
    PUMP_PROGRAM_ID,
    "DistributeCreatorFeesEvent",
    PUMP_DISTRIBUTE_CREATOR_FEES_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("mint", "pubkey"),
        ("bonding_curve", "pubkey"),
        ("sharing_config", "pubkey"),
        ("admin", "pubkey"),
        ("shareholders", "shareholders"),
        ("distributed", "u64"),
        ("quote_mint", "pubkey"),
    ),
)
PumpCollectCreatorFeeEvent = _state_event_class(
    "PumpCollectCreatorFeeEvent",
    PUMP_PROGRAM_ID,
    "CollectCreatorFeeEvent",
    PUMP_COLLECT_CREATOR_FEE_EVENT_DISCRIMINATOR,
    (("timestamp", "i64"), ("creator", "pubkey"), ("creator_fee", "u64"), ("quote_mint", "pubkey")),
)
PumpReservedFeeRecipientsEvent = _state_event_class(
    "PumpReservedFeeRecipientsEvent",
    PUMP_PROGRAM_ID,
    "ReservedFeeRecipientsEvent",
    PUMP_RESERVED_FEE_RECIPIENTS_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("reserved_fee_recipient", "pubkey"),
        ("reserved_fee_recipients", "pubkeys7"),
    ),
)
PumpUpdateGlobalAuthorityEvent = _state_event_class(
    "PumpUpdateGlobalAuthorityEvent",
    PUMP_PROGRAM_ID,
    "UpdateGlobalAuthorityEvent",
    PUMP_UPDATE_GLOBAL_AUTHORITY_EVENT_DISCRIMINATOR,
    (
        ("global", "pubkey"),
        ("authority", "pubkey"),
        ("new_authority", "pubkey"),
        ("timestamp", "i64"),
    ),
)

PumpSwapDepositEvent = _state_event_class(
    "PumpSwapDepositEvent",
    PUMPSWAP_PROGRAM_ID,
    "DepositEvent",
    PUMPSWAP_DEPOSIT_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("lp_token_amount_out", "u64"),
        ("max_base_amount_in", "u64"),
        ("max_quote_amount_in", "u64"),
        ("user_base_token_reserves", "u64"),
        ("user_quote_token_reserves", "u64"),
        ("pool_base_token_reserves", "u64"),
        ("pool_quote_token_reserves", "u64"),
        ("base_amount_in", "u64"),
        ("quote_amount_in", "u64"),
        ("lp_mint_supply", "u64"),
        ("pool", "pubkey"),
        ("user", "pubkey"),
        ("user_base_token_account", "pubkey"),
        ("user_quote_token_account", "pubkey"),
        ("user_pool_token_account", "pubkey"),
    ),
)
PumpSwapWithdrawEvent = _state_event_class(
    "PumpSwapWithdrawEvent",
    PUMPSWAP_PROGRAM_ID,
    "WithdrawEvent",
    PUMPSWAP_WITHDRAW_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("lp_token_amount_in", "u64"),
        ("min_base_amount_out", "u64"),
        ("min_quote_amount_out", "u64"),
        ("user_base_token_reserves", "u64"),
        ("user_quote_token_reserves", "u64"),
        ("pool_base_token_reserves", "u64"),
        ("pool_quote_token_reserves", "u64"),
        ("base_amount_out", "u64"),
        ("quote_amount_out", "u64"),
        ("lp_mint_supply", "u64"),
        ("pool", "pubkey"),
        ("user", "pubkey"),
        ("user_base_token_account", "pubkey"),
        ("user_quote_token_account", "pubkey"),
        ("user_pool_token_account", "pubkey"),
    ),
)
PumpSwapBoostBuyAndBurnEvent = _state_event_class(
    "PumpSwapBoostBuyAndBurnEvent",
    PUMPSWAP_PROGRAM_ID,
    "BoostBuyAndBurnEvent",
    PUMPSWAP_BOOST_BUY_AND_BURN_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("mint", "pubkey"),
        ("bonding_curve", "pubkey"),
        ("pool", "pubkey"),
        ("authority", "pubkey"),
        ("quote_amount_in_requested", "u64"),
        ("quote_amount_in_used", "u64"),
        ("base_amount_burned", "u64"),
        ("virtual_quote_reserves", "i128"),
        ("real_quote_reserves_after", "u64"),
        ("base_reserves_after", "u64"),
        ("boost_vault_remaining", "u64"),
    ),
)
PumpSwapCreateConfigEvent = _state_event_class(
    "PumpSwapCreateConfigEvent",
    PUMPSWAP_PROGRAM_ID,
    "CreateConfigEvent",
    PUMPSWAP_CREATE_CONFIG_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("admin", "pubkey"),
        ("lp_fee_basis_points", "u64"),
        ("protocol_fee_basis_points", "u64"),
        ("protocol_fee_recipients", "pubkeys8"),
        ("coin_creator_fee_basis_points", "u64"),
        ("admin_set_coin_creator_authority", "pubkey"),
    ),
)
PumpSwapUpdateFeeConfigEvent = _state_event_class(
    "PumpSwapUpdateFeeConfigEvent",
    PUMPSWAP_PROGRAM_ID,
    "UpdateFeeConfigEvent",
    PUMPSWAP_UPDATE_FEE_CONFIG_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("admin", "pubkey"),
        ("lp_fee_basis_points", "u64"),
        ("protocol_fee_basis_points", "u64"),
        ("protocol_fee_recipients", "pubkeys8"),
        ("coin_creator_fee_basis_points", "u64"),
        ("admin_set_coin_creator_authority", "pubkey"),
    ),
)
PumpSwapDisableEvent = _state_event_class(
    "PumpSwapDisableEvent",
    PUMPSWAP_PROGRAM_ID,
    "DisableEvent",
    PUMPSWAP_DISABLE_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("admin", "pubkey"),
        ("disable_create_pool", "bool"),
        ("disable_deposit", "bool"),
        ("disable_withdraw", "bool"),
        ("disable_buy", "bool"),
        ("disable_sell", "bool"),
    ),
)
PumpSwapUpdateAdminEvent = _state_event_class(
    "PumpSwapUpdateAdminEvent",
    PUMPSWAP_PROGRAM_ID,
    "UpdateAdminEvent",
    PUMPSWAP_UPDATE_ADMIN_EVENT_DISCRIMINATOR,
    (("timestamp", "i64"), ("admin", "pubkey"), ("new_admin", "pubkey")),
)
PumpSwapMigratePoolCoinCreatorEvent = _state_event_class(
    "PumpSwapMigratePoolCoinCreatorEvent",
    PUMPSWAP_PROGRAM_ID,
    "MigratePoolCoinCreatorEvent",
    PUMPSWAP_MIGRATE_POOL_COIN_CREATOR_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("base_mint", "pubkey"),
        ("pool", "pubkey"),
        ("sharing_config", "pubkey"),
        ("old_coin_creator", "pubkey"),
        ("new_coin_creator", "pubkey"),
    ),
)
PumpSwapSetBondingCurveCoinCreatorEvent = _state_event_class(
    "PumpSwapSetBondingCurveCoinCreatorEvent",
    PUMPSWAP_PROGRAM_ID,
    "SetBondingCurveCoinCreatorEvent",
    PUMPSWAP_SET_BONDING_CURVE_COIN_CREATOR_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("base_mint", "pubkey"),
        ("pool", "pubkey"),
        ("bonding_curve", "pubkey"),
        ("coin_creator", "pubkey"),
    ),
)
PumpSwapSetMetaplexCoinCreatorEvent = _state_event_class(
    "PumpSwapSetMetaplexCoinCreatorEvent",
    PUMPSWAP_PROGRAM_ID,
    "SetMetaplexCoinCreatorEvent",
    PUMPSWAP_SET_METAPLEX_COIN_CREATOR_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("base_mint", "pubkey"),
        ("pool", "pubkey"),
        ("metadata", "pubkey"),
        ("coin_creator", "pubkey"),
    ),
)
PumpSwapCollectCoinCreatorFeeEvent = _state_event_class(
    "PumpSwapCollectCoinCreatorFeeEvent",
    PUMPSWAP_PROGRAM_ID,
    "CollectCoinCreatorFeeEvent",
    PUMPSWAP_COLLECT_COIN_CREATOR_FEE_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("coin_creator", "pubkey"),
        ("coin_creator_fee", "u64"),
        ("coin_creator_vault_ata", "pubkey"),
        ("coin_creator_token_account", "pubkey"),
    ),
)
PumpSwapReservedFeeRecipientsEvent = _state_event_class(
    "PumpSwapReservedFeeRecipientsEvent",
    PUMPSWAP_PROGRAM_ID,
    "ReservedFeeRecipientsEvent",
    PUMPSWAP_RESERVED_FEE_RECIPIENTS_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("reserved_fee_recipient", "pubkey"),
        ("reserved_fee_recipients", "pubkeys7"),
    ),
)
PumpSwapAdminSetCoinCreatorEvent = _state_event_class(
    "PumpSwapAdminSetCoinCreatorEvent",
    PUMPSWAP_PROGRAM_ID,
    "AdminSetCoinCreatorEvent",
    PUMPSWAP_ADMIN_SET_COIN_CREATOR_EVENT_DISCRIMINATOR,
    (
        ("timestamp", "i64"),
        ("admin_set_coin_creator_authority", "pubkey"),
        ("base_mint", "pubkey"),
        ("pool", "pubkey"),
        ("old_coin_creator", "pubkey"),
        ("new_coin_creator", "pubkey"),
    ),
)


# The union is kept separate from the trade-event dataclass:
# callers can use static narrowing on lifecycle event names without changing
# the existing ``DecodedTradeEvent`` mapping API.
LifecycleEvent: TypeAlias = (
    PumpCreateEvent
    | PumpCompleteEvent
    | PumpCompletePumpAmmMigrationEvent
    | PumpSwapCreatePoolEvent
)


_PUMP_TRADE_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("mint", "pubkey"),
    ("sol_amount", "u64"),
    ("token_amount", "u64"),
    ("is_buy", "bool"),
    ("user", "pubkey"),
    ("timestamp", "i64"),
    ("virtual_sol_reserves", "u64"),
    ("virtual_token_reserves", "u64"),
    ("real_sol_reserves", "u64"),
    ("real_token_reserves", "u64"),
    ("fee_recipient", "pubkey"),
    ("fee_basis_points", "u64"),
    ("fee", "u64"),
    ("creator", "pubkey"),
    ("creator_fee_basis_points", "u64"),
    ("creator_fee", "u64"),
    ("track_volume", "bool"),
    ("total_unclaimed_tokens", "u64"),
    ("total_claimed_tokens", "u64"),
    ("current_sol_volume", "u64"),
    ("last_update_timestamp", "i64"),
    ("ix_name", "string"),
    ("mayhem_mode", "bool"),
    ("cashback_fee_basis_points", "u64"),
    ("cashback", "u64"),
    ("buyback_fee_basis_points", "u64"),
    ("buyback_fee", "u64"),
    ("shareholders", "shareholders"),
    ("quote_mint", "pubkey"),
    ("quote_amount", "u64"),
    ("virtual_quote_reserves", "u64"),
    ("real_quote_reserves", "u64"),
)

_PUMPSWAP_IDENTITIES: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("pool", "pubkey"),
    ("user", "pubkey"),
    ("user_base_token_account", "pubkey"),
    ("user_quote_token_account", "pubkey"),
    ("protocol_fee_recipient", "pubkey"),
    ("protocol_fee_recipient_token_account", "pubkey"),
    ("coin_creator", "pubkey"),
)

_PUMPSWAP_BUY_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("timestamp", "i64"),
    ("base_amount_out", "u64"),
    ("max_quote_amount_in", "u64"),
    ("user_base_token_reserves", "u64"),
    ("user_quote_token_reserves", "u64"),
    ("pool_base_token_reserves", "u64"),
    ("pool_quote_token_reserves", "u64"),
    ("quote_amount_in", "u64"),
    ("lp_fee_basis_points", "u64"),
    ("lp_fee", "u64"),
    ("protocol_fee_basis_points", "u64"),
    ("protocol_fee", "u64"),
    ("quote_amount_in_with_lp_fee", "u64"),
    ("user_quote_amount_in", "u64"),
    *_PUMPSWAP_IDENTITIES,
    ("coin_creator_fee_basis_points", "u64"),
    ("coin_creator_fee", "u64"),
    ("track_volume", "bool"),
    ("total_unclaimed_tokens", "u64"),
    ("total_claimed_tokens", "u64"),
    ("current_sol_volume", "u64"),
    ("last_update_timestamp", "i64"),
    ("min_base_amount_out", "u64"),
    ("ix_name", "string"),
    ("cashback_fee_basis_points", "u64"),
    ("cashback", "u64"),
    ("buyback_fee_basis_points", "u64"),
    ("buyback_fee", "u64"),
)

_PUMPSWAP_SELL_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("timestamp", "i64"),
    ("base_amount_in", "u64"),
    ("min_quote_amount_out", "u64"),
    ("user_base_token_reserves", "u64"),
    ("user_quote_token_reserves", "u64"),
    ("pool_base_token_reserves", "u64"),
    ("pool_quote_token_reserves", "u64"),
    ("quote_amount_out", "u64"),
    ("lp_fee_basis_points", "u64"),
    ("lp_fee", "u64"),
    ("protocol_fee_basis_points", "u64"),
    ("protocol_fee", "u64"),
    ("quote_amount_out_without_lp_fee", "u64"),
    ("user_quote_amount_out", "u64"),
    *_PUMPSWAP_IDENTITIES,
    ("coin_creator_fee_basis_points", "u64"),
    ("coin_creator_fee", "u64"),
    ("cashback_fee_basis_points", "u64"),
    ("cashback", "u64"),
    ("buyback_fee_basis_points", "u64"),
    ("buyback_fee", "u64"),
)

_PUMP_CREATE_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("name", "string"),
    ("symbol", "string"),
    ("uri", "string"),
    ("mint", "pubkey"),
    ("bonding_curve", "pubkey"),
    ("user", "pubkey"),
    ("creator", "pubkey"),
    ("timestamp", "i64"),
    ("virtual_token_reserves", "u64"),
    ("virtual_sol_reserves", "u64"),
    ("real_token_reserves", "u64"),
    ("token_total_supply", "u64"),
    ("token_program", "pubkey"),
    ("is_mayhem_mode", "bool"),
    ("is_cashback_enabled", "bool"),
    ("quote_mint", "pubkey"),
    ("virtual_quote_reserves", "u64"),
)

_PUMP_COMPLETE_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("user", "pubkey"),
    ("mint", "pubkey"),
    ("bonding_curve", "pubkey"),
    ("timestamp", "i64"),
    ("quote_mint", "pubkey"),
)

_PUMP_COMPLETE_PUMP_AMM_MIGRATION_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("user", "pubkey"),
    ("mint", "pubkey"),
    ("mint_amount", "u64"),
    ("sol_amount", "u64"),
    ("pool_migration_fee", "u64"),
    ("bonding_curve", "pubkey"),
    ("timestamp", "i64"),
    ("pool", "pubkey"),
    ("quote_mint", "pubkey"),
)

_PUMPSWAP_CREATE_POOL_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("timestamp", "i64"),
    ("index", "u16"),
    ("creator", "pubkey"),
    ("base_mint", "pubkey"),
    ("quote_mint", "pubkey"),
    ("base_mint_decimals", "u8"),
    ("quote_mint_decimals", "u8"),
    ("base_amount_in", "u64"),
    ("quote_amount_in", "u64"),
    ("pool_base_amount", "u64"),
    ("pool_quote_amount", "u64"),
    ("minimum_liquidity", "u64"),
    ("initial_liquidity", "u64"),
    ("lp_token_amount_out", "u64"),
    ("pool_bump", "u8"),
    ("pool", "pubkey"),
    ("lp_mint", "pubkey"),
    ("user_base_token_account", "pubkey"),
    ("user_quote_token_account", "pubkey"),
    ("coin_creator", "pubkey"),
    ("is_mayhem_mode", "bool"),
)

# Non-trade state/configuration layouts copied from the corresponding named
# event types in the pinned Pump/PumpSwap IDLs.  Keeping these layouts beside
# the dispatch table makes review against the hash-locked source mechanical.
_PUMP_ADMIN_SET_CREATOR_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("timestamp", "i64"),
    ("admin_set_creator_authority", "pubkey"),
    ("mint", "pubkey"),
    ("bonding_curve", "pubkey"),
    ("old_creator", "pubkey"),
    ("new_creator", "pubkey"),
)
_PUMP_ADMIN_SET_IDL_AUTHORITY_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("idl_authority", "pubkey"),
)
_PUMP_ADMIN_UPDATE_TOKEN_INCENTIVES_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("start_time", "i64"),
    ("end_time", "i64"),
    ("day_number", "u64"),
    ("token_supply_per_day", "u64"),
    ("mint", "pubkey"),
    ("seconds_in_a_day", "i64"),
    ("timestamp", "i64"),
)
_PUMP_CLAIM_CASHBACK_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("user", "pubkey"),
    ("amount", "u64"),
    ("timestamp", "i64"),
    ("total_claimed", "u64"),
    ("total_cashback_earned", "u64"),
)
_PUMP_CLAIM_TOKEN_INCENTIVES_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("user", "pubkey"),
    ("mint", "pubkey"),
    ("amount", "u64"),
    ("timestamp", "i64"),
    ("total_claimed_tokens", "u64"),
    ("current_sol_volume", "u64"),
)
_PUMP_CLOSE_USER_VOLUME_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("user", "pubkey"),
    ("timestamp", "i64"),
    ("total_unclaimed_tokens", "u64"),
    ("total_claimed_tokens", "u64"),
    ("current_sol_volume", "u64"),
    ("last_update_timestamp", "i64"),
)
_PUMP_EXTEND_ACCOUNT_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("account", "pubkey"),
    ("user", "pubkey"),
    ("current_size", "u64"),
    ("new_size", "u64"),
    ("timestamp", "i64"),
)
_PUMP_INIT_USER_VOLUME_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("payer", "pubkey"),
    ("user", "pubkey"),
    ("timestamp", "i64"),
)
_PUMP_MINIMUM_DISTRIBUTABLE_FEE_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("minimum_required", "u64"),
    ("distributable_fees", "u64"),
    ("can_distribute", "bool"),
)
_PUMP_SET_METAPLEX_CREATOR_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("timestamp", "i64"),
    ("mint", "pubkey"),
    ("bonding_curve", "pubkey"),
    ("metadata", "pubkey"),
    ("creator", "pubkey"),
)
_PUMP_SYNC_USER_VOLUME_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("user", "pubkey"),
    ("total_claimed_tokens_before", "u64"),
    ("total_claimed_tokens_after", "u64"),
    ("timestamp", "i64"),
)

_PUMPSWAP_EXTEND_ACCOUNT_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("timestamp", "i64"),
    ("account", "pubkey"),
    ("user", "pubkey"),
    ("current_size", "u64"),
    ("new_size", "u64"),
)
_PUMPSWAP_SET_RESERVED_FEE_RECIPIENTS_FIELDS: Final[tuple[tuple[str, _FieldKind], ...]] = (
    ("timestamp", "i64"),
    ("reserved_fee_recipient", "pubkey"),
    ("reserved_fee_recipients", "pubkeys7"),
)


@dataclass(frozen=True, slots=True)
class _StateEventSpec:
    program_id: str
    name: str
    discriminator: bytes
    fields: tuple[tuple[str, _FieldKind], ...]
    event_type: Callable[..., DecodedStateEvent]


def _state_spec(
    program_id: str,
    name: str,
    discriminator: tuple[int, ...],
    fields: tuple[tuple[str, _FieldKind], ...],
    event_type: type[DecodedStateEvent] | None = None,
) -> _StateEventSpec:
    disc = bytes(discriminator)
    return _StateEventSpec(
        program_id,
        name,
        disc,
        fields,
        event_type
        or _state_event_class(
            f"{('Pump' if program_id == PUMP_PROGRAM_ID else 'PumpSwap')}{name}",
            program_id,
            name,
            disc,
            fields,
        ),
    )


_STATE_EVENT_SPECS: Final[dict[tuple[str, bytes], _StateEventSpec]] = {
    # Pump global/configuration and creator/fee events.
    (PUMP_PROGRAM_ID, bytes((64, 69, 192, 104, 29, 30, 25, 107))): _state_spec(
        PUMP_PROGRAM_ID,
        "AdminSetCreatorEvent",
        (64, 69, 192, 104, 29, 30, 25, 107),
        _PUMP_ADMIN_SET_CREATOR_FIELDS,
    ),
    (PUMP_PROGRAM_ID, bytes((245, 59, 70, 34, 75, 185, 109, 92))): _state_spec(
        PUMP_PROGRAM_ID,
        "AdminSetIdlAuthorityEvent",
        (245, 59, 70, 34, 75, 185, 109, 92),
        _PUMP_ADMIN_SET_IDL_AUTHORITY_FIELDS,
    ),
    (PUMP_PROGRAM_ID, bytes((147, 250, 108, 120, 247, 29, 67, 222))): _state_spec(
        PUMP_PROGRAM_ID,
        "AdminUpdateTokenIncentivesEvent",
        (147, 250, 108, 120, 247, 29, 67, 222),
        _PUMP_ADMIN_UPDATE_TOKEN_INCENTIVES_FIELDS,
    ),
    (PUMP_PROGRAM_ID, bytes((226, 214, 246, 33, 7, 242, 147, 229))): _state_spec(
        PUMP_PROGRAM_ID,
        "ClaimCashbackEvent",
        (226, 214, 246, 33, 7, 242, 147, 229),
        _PUMP_CLAIM_CASHBACK_FIELDS,
    ),
    (PUMP_PROGRAM_ID, bytes((79, 172, 246, 49, 205, 91, 206, 232))): _state_spec(
        PUMP_PROGRAM_ID,
        "ClaimTokenIncentivesEvent",
        (79, 172, 246, 49, 205, 91, 206, 232),
        _PUMP_CLAIM_TOKEN_INCENTIVES_FIELDS,
    ),
    (PUMP_PROGRAM_ID, bytes((146, 159, 189, 172, 146, 88, 56, 244))): _state_spec(
        PUMP_PROGRAM_ID,
        "CloseUserVolumeAccumulatorEvent",
        (146, 159, 189, 172, 146, 88, 56, 244),
        _PUMP_CLOSE_USER_VOLUME_FIELDS,
    ),
    (PUMP_PROGRAM_ID, bytes((97, 97, 215, 144, 93, 146, 22, 124))): _state_spec(
        PUMP_PROGRAM_ID,
        "ExtendAccountEvent",
        (97, 97, 215, 144, 93, 146, 22, 124),
        _PUMP_EXTEND_ACCOUNT_FIELDS,
    ),
    (PUMP_PROGRAM_ID, PUMP_COLLECT_CREATOR_FEE_EVENT_DISCRIMINATOR): _state_spec(
        PUMP_PROGRAM_ID,
        "CollectCreatorFeeEvent",
        tuple(PUMP_COLLECT_CREATOR_FEE_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("creator", "pubkey"),
            ("creator_fee", "u64"),
            ("quote_mint", "pubkey"),
        ),
        PumpCollectCreatorFeeEvent,
    ),
    (PUMP_PROGRAM_ID, PUMP_DISTRIBUTE_CREATOR_FEES_EVENT_DISCRIMINATOR): _state_spec(
        PUMP_PROGRAM_ID,
        "DistributeCreatorFeesEvent",
        tuple(PUMP_DISTRIBUTE_CREATOR_FEES_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("mint", "pubkey"),
            ("bonding_curve", "pubkey"),
            ("sharing_config", "pubkey"),
            ("admin", "pubkey"),
            ("shareholders", "shareholders"),
            ("distributed", "u64"),
            ("quote_mint", "pubkey"),
        ),
        PumpDistributeCreatorFeesEvent,
    ),
    (PUMP_PROGRAM_ID, bytes((134, 36, 13, 72, 232, 101, 130, 216))): _state_spec(
        PUMP_PROGRAM_ID,
        "InitUserVolumeAccumulatorEvent",
        (134, 36, 13, 72, 232, 101, 130, 216),
        _PUMP_INIT_USER_VOLUME_FIELDS,
    ),
    (PUMP_PROGRAM_ID, PUMP_MIGRATE_BONDING_CURVE_CREATOR_EVENT_DISCRIMINATOR): _state_spec(
        PUMP_PROGRAM_ID,
        "MigrateBondingCurveCreatorEvent",
        tuple(PUMP_MIGRATE_BONDING_CURVE_CREATOR_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("mint", "pubkey"),
            ("bonding_curve", "pubkey"),
            ("sharing_config", "pubkey"),
            ("old_creator", "pubkey"),
            ("new_creator", "pubkey"),
        ),
        PumpMigrateBondingCurveCreatorEvent,
    ),
    (PUMP_PROGRAM_ID, bytes((168, 216, 132, 239, 235, 182, 49, 52))): _state_spec(
        PUMP_PROGRAM_ID,
        "MinimumDistributableFeeEvent",
        (168, 216, 132, 239, 235, 182, 49, 52),
        _PUMP_MINIMUM_DISTRIBUTABLE_FEE_FIELDS,
    ),
    (PUMP_PROGRAM_ID, PUMP_RESERVED_FEE_RECIPIENTS_EVENT_DISCRIMINATOR): _state_spec(
        PUMP_PROGRAM_ID,
        "ReservedFeeRecipientsEvent",
        tuple(PUMP_RESERVED_FEE_RECIPIENTS_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("reserved_fee_recipient", "pubkey"),
            ("reserved_fee_recipients", "pubkeys7"),
        ),
        PumpReservedFeeRecipientsEvent,
    ),
    (PUMP_PROGRAM_ID, PUMP_SET_CREATOR_EVENT_DISCRIMINATOR): _state_spec(
        PUMP_PROGRAM_ID,
        "SetCreatorEvent",
        tuple(PUMP_SET_CREATOR_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("mint", "pubkey"),
            ("bonding_curve", "pubkey"),
            ("creator", "pubkey"),
        ),
        PumpSetCreatorEvent,
    ),
    (PUMP_PROGRAM_ID, bytes((142, 203, 6, 32, 127, 105, 191, 162))): _state_spec(
        PUMP_PROGRAM_ID,
        "SetMetaplexCreatorEvent",
        (142, 203, 6, 32, 127, 105, 191, 162),
        _PUMP_SET_METAPLEX_CREATOR_FIELDS,
    ),
    (PUMP_PROGRAM_ID, PUMP_SET_PARAMS_EVENT_DISCRIMINATOR): _state_spec(
        PUMP_PROGRAM_ID,
        "SetParamsEvent",
        tuple(PUMP_SET_PARAMS_EVENT_DISCRIMINATOR),
        (
            ("initial_virtual_token_reserves", "u64"),
            ("initial_virtual_sol_reserves", "u64"),
            ("initial_real_token_reserves", "u64"),
            ("final_real_sol_reserves", "u64"),
            ("token_total_supply", "u64"),
            ("fee_basis_points", "u64"),
            ("withdraw_authority", "pubkey"),
            ("enable_migrate", "bool"),
            ("pool_migration_fee", "u64"),
            ("creator_fee_basis_points", "u64"),
            ("fee_recipients", "pubkeys8"),
            ("timestamp", "i64"),
            ("set_creator_authority", "pubkey"),
            ("admin_set_creator_authority", "pubkey"),
        ),
        PumpSetParamsEvent,
    ),
    (PUMP_PROGRAM_ID, bytes((197, 122, 167, 124, 116, 81, 91, 255))): _state_spec(
        PUMP_PROGRAM_ID,
        "SyncUserVolumeAccumulatorEvent",
        (197, 122, 167, 124, 116, 81, 91, 255),
        _PUMP_SYNC_USER_VOLUME_FIELDS,
    ),
    (PUMP_PROGRAM_ID, PUMP_UPDATE_GLOBAL_AUTHORITY_EVENT_DISCRIMINATOR): _state_spec(
        PUMP_PROGRAM_ID,
        "UpdateGlobalAuthorityEvent",
        tuple(PUMP_UPDATE_GLOBAL_AUTHORITY_EVENT_DISCRIMINATOR),
        (
            ("global", "pubkey"),
            ("authority", "pubkey"),
            ("new_authority", "pubkey"),
            ("timestamp", "i64"),
        ),
        PumpUpdateGlobalAuthorityEvent,
    ),
    (PUMP_PROGRAM_ID, PUMP_UPDATE_MAYHEM_VIRTUAL_PARAMS_EVENT_DISCRIMINATOR): _state_spec(
        PUMP_PROGRAM_ID,
        "UpdateMayhemVirtualParamsEvent",
        tuple(PUMP_UPDATE_MAYHEM_VIRTUAL_PARAMS_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("mint", "pubkey"),
            ("virtual_token_reserves", "u64"),
            ("virtual_sol_reserves", "u64"),
            ("new_virtual_token_reserves", "u64"),
            ("new_virtual_sol_reserves", "u64"),
            ("real_token_reserves", "u64"),
            ("real_sol_reserves", "u64"),
        ),
        PumpUpdateMayhemVirtualParamsEvent,
    ),
    # PumpSwap liquidity/configuration and creator/fee events.
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_ADMIN_SET_COIN_CREATOR_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "AdminSetCoinCreatorEvent",
        tuple(PUMPSWAP_ADMIN_SET_COIN_CREATOR_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("admin_set_coin_creator_authority", "pubkey"),
            ("base_mint", "pubkey"),
            ("pool", "pubkey"),
            ("old_coin_creator", "pubkey"),
            ("new_coin_creator", "pubkey"),
        ),
        PumpSwapAdminSetCoinCreatorEvent,
    ),
    (PUMPSWAP_PROGRAM_ID, bytes((147, 250, 108, 120, 247, 29, 67, 222))): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "AdminUpdateTokenIncentivesEvent",
        (147, 250, 108, 120, 247, 29, 67, 222),
        _PUMP_ADMIN_UPDATE_TOKEN_INCENTIVES_FIELDS,
    ),
    (PUMPSWAP_PROGRAM_ID, bytes((226, 214, 246, 33, 7, 242, 147, 229))): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "ClaimCashbackEvent",
        (226, 214, 246, 33, 7, 242, 147, 229),
        _PUMP_CLAIM_CASHBACK_FIELDS,
    ),
    (PUMPSWAP_PROGRAM_ID, bytes((79, 172, 246, 49, 205, 91, 206, 232))): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "ClaimTokenIncentivesEvent",
        (79, 172, 246, 49, 205, 91, 206, 232),
        _PUMP_CLAIM_TOKEN_INCENTIVES_FIELDS,
    ),
    (PUMPSWAP_PROGRAM_ID, bytes((146, 159, 189, 172, 146, 88, 56, 244))): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "CloseUserVolumeAccumulatorEvent",
        (146, 159, 189, 172, 146, 88, 56, 244),
        _PUMP_CLOSE_USER_VOLUME_FIELDS,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_COLLECT_COIN_CREATOR_FEE_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "CollectCoinCreatorFeeEvent",
        tuple(PUMPSWAP_COLLECT_COIN_CREATOR_FEE_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("coin_creator", "pubkey"),
            ("coin_creator_fee", "u64"),
            ("coin_creator_vault_ata", "pubkey"),
            ("coin_creator_token_account", "pubkey"),
        ),
        PumpSwapCollectCoinCreatorFeeEvent,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_CREATE_CONFIG_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "CreateConfigEvent",
        tuple(PUMPSWAP_CREATE_CONFIG_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("admin", "pubkey"),
            ("lp_fee_basis_points", "u64"),
            ("protocol_fee_basis_points", "u64"),
            ("protocol_fee_recipients", "pubkeys8"),
            ("coin_creator_fee_basis_points", "u64"),
            ("admin_set_coin_creator_authority", "pubkey"),
        ),
        PumpSwapCreateConfigEvent,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_DEPOSIT_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "DepositEvent",
        tuple(PUMPSWAP_DEPOSIT_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("lp_token_amount_out", "u64"),
            ("max_base_amount_in", "u64"),
            ("max_quote_amount_in", "u64"),
            ("user_base_token_reserves", "u64"),
            ("user_quote_token_reserves", "u64"),
            ("pool_base_token_reserves", "u64"),
            ("pool_quote_token_reserves", "u64"),
            ("base_amount_in", "u64"),
            ("quote_amount_in", "u64"),
            ("lp_mint_supply", "u64"),
            ("pool", "pubkey"),
            ("user", "pubkey"),
            ("user_base_token_account", "pubkey"),
            ("user_quote_token_account", "pubkey"),
            ("user_pool_token_account", "pubkey"),
        ),
        PumpSwapDepositEvent,
    ),
    (
        PUMPSWAP_PROGRAM_ID,
        PUMPSWAP_BOOST_BUY_AND_BURN_EVENT_DISCRIMINATOR,
    ): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "BoostBuyAndBurnEvent",
        tuple(PUMPSWAP_BOOST_BUY_AND_BURN_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("mint", "pubkey"),
            ("bonding_curve", "pubkey"),
            ("pool", "pubkey"),
            ("authority", "pubkey"),
            ("quote_amount_in_requested", "u64"),
            ("quote_amount_in_used", "u64"),
            ("base_amount_burned", "u64"),
            ("virtual_quote_reserves", "i128"),
            ("real_quote_reserves_after", "u64"),
            ("base_reserves_after", "u64"),
            ("boost_vault_remaining", "u64"),
        ),
        PumpSwapBoostBuyAndBurnEvent,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_DISABLE_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "DisableEvent",
        tuple(PUMPSWAP_DISABLE_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("admin", "pubkey"),
            ("disable_create_pool", "bool"),
            ("disable_deposit", "bool"),
            ("disable_withdraw", "bool"),
            ("disable_buy", "bool"),
            ("disable_sell", "bool"),
        ),
        PumpSwapDisableEvent,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_EXTEND_ACCOUNT_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "ExtendAccountEvent",
        (97, 97, 215, 144, 93, 146, 22, 124),
        _PUMPSWAP_EXTEND_ACCOUNT_FIELDS,
    ),
    (PUMPSWAP_PROGRAM_ID, bytes((134, 36, 13, 72, 232, 101, 130, 216))): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "InitUserVolumeAccumulatorEvent",
        (134, 36, 13, 72, 232, 101, 130, 216),
        _PUMP_INIT_USER_VOLUME_FIELDS,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_MIGRATE_POOL_COIN_CREATOR_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "MigratePoolCoinCreatorEvent",
        tuple(PUMPSWAP_MIGRATE_POOL_COIN_CREATOR_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("base_mint", "pubkey"),
            ("pool", "pubkey"),
            ("sharing_config", "pubkey"),
            ("old_coin_creator", "pubkey"),
            ("new_coin_creator", "pubkey"),
        ),
        PumpSwapMigratePoolCoinCreatorEvent,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_RESERVED_FEE_RECIPIENTS_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "ReservedFeeRecipientsEvent",
        tuple(PUMPSWAP_RESERVED_FEE_RECIPIENTS_EVENT_DISCRIMINATOR),
        _PUMPSWAP_SET_RESERVED_FEE_RECIPIENTS_FIELDS,
        PumpSwapReservedFeeRecipientsEvent,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_SET_BONDING_CURVE_COIN_CREATOR_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "SetBondingCurveCoinCreatorEvent",
        tuple(PUMPSWAP_SET_BONDING_CURVE_COIN_CREATOR_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("base_mint", "pubkey"),
            ("pool", "pubkey"),
            ("bonding_curve", "pubkey"),
            ("coin_creator", "pubkey"),
        ),
        PumpSwapSetBondingCurveCoinCreatorEvent,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_SET_METAPLEX_COIN_CREATOR_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "SetMetaplexCoinCreatorEvent",
        tuple(PUMPSWAP_SET_METAPLEX_COIN_CREATOR_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("base_mint", "pubkey"),
            ("pool", "pubkey"),
            ("metadata", "pubkey"),
            ("coin_creator", "pubkey"),
        ),
        PumpSwapSetMetaplexCoinCreatorEvent,
    ),
    (PUMPSWAP_PROGRAM_ID, bytes((197, 122, 167, 124, 116, 81, 91, 255))): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "SyncUserVolumeAccumulatorEvent",
        (197, 122, 167, 124, 116, 81, 91, 255),
        _PUMP_SYNC_USER_VOLUME_FIELDS,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_UPDATE_ADMIN_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "UpdateAdminEvent",
        tuple(PUMPSWAP_UPDATE_ADMIN_EVENT_DISCRIMINATOR),
        (("timestamp", "i64"), ("admin", "pubkey"), ("new_admin", "pubkey")),
        PumpSwapUpdateAdminEvent,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_UPDATE_FEE_CONFIG_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "UpdateFeeConfigEvent",
        tuple(PUMPSWAP_UPDATE_FEE_CONFIG_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("admin", "pubkey"),
            ("lp_fee_basis_points", "u64"),
            ("protocol_fee_basis_points", "u64"),
            ("protocol_fee_recipients", "pubkeys8"),
            ("coin_creator_fee_basis_points", "u64"),
            ("admin_set_coin_creator_authority", "pubkey"),
        ),
        PumpSwapUpdateFeeConfigEvent,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_WITHDRAW_EVENT_DISCRIMINATOR): _state_spec(
        PUMPSWAP_PROGRAM_ID,
        "WithdrawEvent",
        tuple(PUMPSWAP_WITHDRAW_EVENT_DISCRIMINATOR),
        (
            ("timestamp", "i64"),
            ("lp_token_amount_in", "u64"),
            ("min_base_amount_out", "u64"),
            ("min_quote_amount_out", "u64"),
            ("user_base_token_reserves", "u64"),
            ("user_quote_token_reserves", "u64"),
            ("pool_base_token_reserves", "u64"),
            ("pool_quote_token_reserves", "u64"),
            ("base_amount_out", "u64"),
            ("quote_amount_out", "u64"),
            ("lp_mint_supply", "u64"),
            ("pool", "pubkey"),
            ("user", "pubkey"),
            ("user_base_token_account", "pubkey"),
            ("user_quote_token_account", "pubkey"),
            ("user_pool_token_account", "pubkey"),
        ),
        PumpSwapWithdrawEvent,
    ),
}

# Make generated classes discoverable by their stable typed names as well as
# through each dispatch spec (for example ``PumpAdminSetCreatorEvent``).  The
# named high-value classes above already point at these objects;
# this loop only publishes the remaining official event types.
for _state_spec_entry in _STATE_EVENT_SPECS.values():
    globals()[_state_spec_entry.event_type.__name__] = _state_spec_entry.event_type


@dataclass(frozen=True, slots=True)
class _LifecycleEventSpec:
    program_id: str
    name: str
    discriminator: bytes
    fields: tuple[tuple[str, _FieldKind], ...]
    event_type: Callable[..., DecodedLifecycleEvent]


_LIFECYCLE_EVENT_SPECS: Final[dict[tuple[str, bytes], _LifecycleEventSpec]] = {
    (PUMP_PROGRAM_ID, PUMP_CREATE_EVENT_DISCRIMINATOR): _LifecycleEventSpec(
        PUMP_PROGRAM_ID,
        "CreateEvent",
        PUMP_CREATE_EVENT_DISCRIMINATOR,
        _PUMP_CREATE_FIELDS,
        PumpCreateEvent,
    ),
    (PUMP_PROGRAM_ID, PUMP_COMPLETE_EVENT_DISCRIMINATOR): _LifecycleEventSpec(
        PUMP_PROGRAM_ID,
        "CompleteEvent",
        PUMP_COMPLETE_EVENT_DISCRIMINATOR,
        _PUMP_COMPLETE_FIELDS,
        PumpCompleteEvent,
    ),
    (
        PUMP_PROGRAM_ID,
        PUMP_COMPLETE_PUMP_AMM_MIGRATION_EVENT_DISCRIMINATOR,
    ): _LifecycleEventSpec(
        PUMP_PROGRAM_ID,
        "CompletePumpAmmMigrationEvent",
        PUMP_COMPLETE_PUMP_AMM_MIGRATION_EVENT_DISCRIMINATOR,
        _PUMP_COMPLETE_PUMP_AMM_MIGRATION_FIELDS,
        PumpCompletePumpAmmMigrationEvent,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_CREATE_POOL_EVENT_DISCRIMINATOR): _LifecycleEventSpec(
        PUMPSWAP_PROGRAM_ID,
        "CreatePoolEvent",
        PUMPSWAP_CREATE_POOL_EVENT_DISCRIMINATOR,
        _PUMPSWAP_CREATE_POOL_FIELDS,
        PumpSwapCreatePoolEvent,
    ),
}

_EVENT_SPECS: Final[dict[tuple[str, bytes], _EventSpec]] = {
    (PUMP_PROGRAM_ID, PUMP_TRADE_EVENT_DISCRIMINATOR): _EventSpec(
        PUMP_PROGRAM_ID, "TradeEvent", PUMP_TRADE_EVENT_DISCRIMINATOR, _PUMP_TRADE_FIELDS
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_BUY_EVENT_DISCRIMINATOR): _EventSpec(
        PUMPSWAP_PROGRAM_ID,
        "BuyEvent",
        PUMPSWAP_BUY_EVENT_DISCRIMINATOR,
        _PUMPSWAP_BUY_FIELDS,
    ),
    (PUMPSWAP_PROGRAM_ID, PUMPSWAP_SELL_EVENT_DISCRIMINATOR): _EventSpec(
        PUMPSWAP_PROGRAM_ID,
        "SellEvent",
        PUMPSWAP_SELL_EVENT_DISCRIMINATOR,
        _PUMPSWAP_SELL_FIELDS,
    ),
}


class _BorshReader:
    def __init__(self, payload: bytes, *, context: str = "trade event") -> None:
        self._payload = payload
        self._offset = 8
        self._context = context

    @property
    def remaining(self) -> int:
        return len(self._payload) - self._offset

    def take(self, count: int) -> bytes:
        if count < 0 or count > self.remaining:
            raise MalformedProgramEventError(f"{self._context} Borsh payload is truncated")
        start = self._offset
        self._offset += count
        return self._payload[start : start + count]

    def unsigned(self, count: int) -> int:
        return int.from_bytes(self.take(count), "little")

    def signed(self, count: int) -> int:
        return int.from_bytes(self.take(count), "little", signed=True)

    def u8(self) -> int:
        """Read one little-endian unsigned byte with bounds checking."""

        return self.unsigned(1)

    def u16(self) -> int:
        """Read one little-endian unsigned 16-bit integer."""

        return self.unsigned(2)

    def u128(self) -> int:
        """Read one little-endian unsigned 128-bit integer."""

        return self.unsigned(16)

    def boolean(self) -> bool:
        value = self.u8()
        if value not in {0, 1}:
            raise MalformedProgramEventError(f"{self._context} boolean must be encoded as 0 or 1")
        return bool(value)

    # ``bool`` mirrors the IDL field spelling and keeps the reader API
    # discoverable for the lifecycle layouts.  ``boolean`` remains for the
    # existing trade decoder and is retained as the underlying
    # implementation.
    def bool(self) -> bool:
        return self.boolean()

    def pubkey(self) -> str:
        return encode_base58(self.take(32))

    def pubkeys(self, count: int) -> tuple[str, ...]:
        """Read a fixed-size array of public keys from an Anchor event."""

        if count < 0 or count > self.remaining // 32:
            raise MalformedProgramEventError(f"{self._context} public-key array is truncated")
        return tuple(self.pubkey() for _ in range(count))

    def string(self) -> str:
        length = self.unsigned(4)
        try:
            return self.take(length).decode("utf-8")
        except UnicodeDecodeError as error:
            raise MalformedProgramEventError(f"{self._context} string is not UTF-8") from error

    def shareholders(self) -> tuple[Shareholder, ...]:
        count = self.unsigned(4)
        if count > self.remaining // 34:
            raise MalformedProgramEventError(f"{self._context} shareholder vector is truncated")
        return tuple(Shareholder(self.pubkey(), self.unsigned(2)) for _ in range(count))

    def value(self, kind: _FieldKind) -> object:
        if kind == "u8":
            return self.u8()
        if kind == "u16":
            return self.u16()
        if kind == "u64":
            return self.unsigned(8)
        if kind == "u128":
            return self.u128()
        if kind == "i128":
            return self.signed(16)
        if kind == "i64":
            return self.signed(8)
        if kind == "bool":
            return self.bool()
        if kind == "pubkey":
            return self.pubkey()
        if kind == "string":
            return self.string()
        if kind == "shareholders":
            return self.shareholders()
        if kind == "pubkeys7":
            return self.pubkeys(7)
        if kind == "pubkeys8":
            return self.pubkeys(8)
        raise AssertionError(f"unsupported pinned Borsh field kind: {kind}")


def _strict_base64(value: str) -> bytes:
    try:
        encoded = value.encode("ascii")
        payload = base64.b64decode(encoded, validate=True)
    except (UnicodeEncodeError, ValueError, binascii.Error) as error:
        raise MalformedProgramEventError("Program data payload is not canonical base64") from error
    if base64.b64encode(payload).decode("ascii") != value:
        raise MalformedProgramEventError("Program data payload is not canonical base64")
    return payload


def decode_trade_event(program_id: str, payload: bytes | str) -> DecodedTradeEvent:
    """Decode one exact Pump/PumpSwap trade-event payload."""

    if program_id not in {PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID}:
        raise UnknownProgramEventError("program is not Pump or PumpSwap")
    if isinstance(payload, str):
        raw = _strict_base64(payload)
    elif isinstance(payload, bytes):
        raw = payload
    else:
        raise TypeError("trade event payload must be bytes or base64 text")
    if len(raw) > _MAX_EVENT_BYTES:
        raise MalformedProgramEventError("trade event payload exceeds the size limit")
    if len(raw) < 8:
        raise MalformedProgramEventError("trade event payload is shorter than its discriminator")
    spec = _EVENT_SPECS.get((program_id, raw[:8]))
    if spec is None:
        raise UnknownProgramEventError("no pinned trade event matches program and discriminator")
    reader = _BorshReader(raw)
    values = {name: reader.value(kind) for name, kind in spec.fields}
    # PumpSwap added these two fields to BuyEvent and SellEvent after the
    # historical research interval. Accept exactly the legacy payload or the
    # current official 17-byte suffix; every other trailing shape remains a
    # hard decoding error.
    if (
        program_id == PUMPSWAP_PROGRAM_ID
        and spec.name in {"BuyEvent", "SellEvent"}
        and reader.remaining in {17, 25}
    ):
        values["virtual_quote_reserves"] = reader.signed(16)
        values["can_boost"] = reader.bool()
        if reader.remaining == 8:
            values["base_supply"] = reader.unsigned(8)
    if reader.remaining:
        raise MalformedProgramEventError(
            f"trade event contains trailing Borsh bytes: {reader.remaining}"
        )
    return DecodedTradeEvent(
        program_id=program_id,
        event_name=spec.name,
        discriminator=spec.discriminator,
        values=values,
        raw_data=raw,
    )


def decode_lifecycle_event(program_id: str, payload: bytes | str) -> LifecycleEvent:
    """Decode one exact Pump/PumpSwap lifecycle event payload.

    The dispatch key includes both the emitting program and Anchor's complete
    eight-byte discriminator.  A discriminator recognised for the other
    program therefore remains unknown rather than being decoded under the
    wrong wire layout.
    """

    if program_id not in {PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID}:
        raise UnknownProgramEventError("program is not Pump or PumpSwap")
    if isinstance(payload, str):
        raw = _strict_base64(payload)
    elif isinstance(payload, bytes):
        raw = payload
    else:
        raise TypeError("lifecycle event payload must be bytes or base64 text")
    if len(raw) > _MAX_EVENT_BYTES:
        raise MalformedProgramEventError("lifecycle event payload exceeds the size limit")
    if len(raw) < 8:
        raise MalformedProgramEventError(
            "lifecycle event payload is shorter than its discriminator"
        )
    spec = _LIFECYCLE_EVENT_SPECS.get((program_id, raw[:8]))
    if spec is None:
        raise UnknownProgramEventError(
            "no pinned lifecycle event matches program and discriminator"
        )
    reader = _BorshReader(raw, context="lifecycle event")
    values = {name: reader.value(kind) for name, kind in spec.fields}
    if reader.remaining:
        raise MalformedProgramEventError("lifecycle event contains trailing Borsh bytes")
    event = spec.event_type(**values)
    # ``raw_data`` is metadata, not part of the IDL struct and is
    # excluded from the public constructor.  Set it only after strict parsing
    # has succeeded, preserving the result's provenance.
    object.__setattr__(event, "raw_data", raw)
    return cast(LifecycleEvent, event)


def decode_state_event(program_id: str, payload: bytes | str) -> DecodedStateEvent:
    """Decode one strict fee/config/liquidity event from the pinned IDLs.

    State events remain separate from :func:`decode_trade_event`
    and :func:`decode_lifecycle_event`: a deposit, withdraw, or fee update is
    a causal state transition, not a swap.  The complete program/discriminator
    key prevents Pump/PumpSwap collisions from being interpreted under the
    wrong wire layout.
    """

    if program_id not in {PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID}:
        raise UnknownProgramEventError("program is not Pump or PumpSwap")
    if isinstance(payload, str):
        raw = _strict_base64(payload)
    elif isinstance(payload, bytes):
        raw = payload
    else:
        raise TypeError("state event payload must be bytes or base64 text")
    if len(raw) > _MAX_EVENT_BYTES:
        raise MalformedProgramEventError("state event payload exceeds the size limit")
    if len(raw) < 8:
        raise MalformedProgramEventError("state event payload is shorter than its discriminator")
    spec = _STATE_EVENT_SPECS.get((program_id, raw[:8]))
    if spec is None:
        raise UnknownProgramEventError("no pinned state event matches program and discriminator")
    reader = _BorshReader(raw, context="state event")
    values = {name: reader.value(kind) for name, kind in spec.fields}
    if reader.remaining:
        raise MalformedProgramEventError("state event contains trailing Borsh bytes")
    event = spec.event_type(**{_state_internal_name(name): value for name, value in values.items()})
    object.__setattr__(event, "raw_data", raw)
    return event


def decode_program_event(
    program_id: str, payload: bytes | str
) -> DecodedTradeEvent | LifecycleEvent | DecodedStateEvent:
    """Decode any known Pump/PumpSwap event under the pinned IDL revision."""

    if isinstance(payload, str):
        raw = _strict_base64(payload)
    elif isinstance(payload, bytes):
        raw = payload
    else:
        raise TypeError("program event payload must be bytes or base64 text")
    if len(raw) > _MAX_EVENT_BYTES:
        raise MalformedProgramEventError("program event payload exceeds the size limit")
    if len(raw) < 8:
        raise MalformedProgramEventError("program event payload is shorter than its discriminator")
    key = (program_id, raw[:8])
    if key in _EVENT_SPECS:
        return decode_trade_event(program_id, raw)
    if key in _LIFECYCLE_EVENT_SPECS:
        return decode_lifecycle_event(program_id, raw)
    if key in _STATE_EVENT_SPECS:
        return decode_state_event(program_id, raw)
    raise UnknownProgramEventError("no pinned event matches program and discriminator")


def _pop_program(stack: list[str], program_id: str) -> None:
    if stack and stack[-1] == program_id:
        stack.pop()
        return
    for index in range(len(stack) - 1, -1, -1):
        if stack[index] == program_id:
            del stack[index:]
            return


def iter_trade_event_logs(log_messages: Iterable[str]) -> Iterator[TradeEventLog]:
    """Yield pinned trade events with log index and invocation depth."""

    stack: list[str] = []
    for log_index, message in enumerate(log_messages):
        if not isinstance(message, str):
            raise TypeError("transaction log messages must be strings")
        invoke = _INVOKE_RE.fullmatch(message)
        if invoke is not None:
            program_id, raw_depth = invoke.groups()
            depth = int(raw_depth)
            if depth <= len(stack):
                del stack[depth - 1 :]
            elif depth != len(stack) + 1:
                stack.clear()
            stack.append(program_id)
            continue
        success = _SUCCESS_RE.fullmatch(message)
        if success is not None:
            _pop_program(stack, success.group(1))
            continue
        failed = _FAILED_RE.match(message)
        if failed is not None:
            _pop_program(stack, failed.group(1))
            continue
        if not message.startswith(_PROGRAM_DATA_PREFIX) or not stack:
            continue
        program_id = stack[-1]
        if program_id not in {PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID}:
            continue
        encoded = message[len(_PROGRAM_DATA_PREFIX) :]
        raw = _strict_base64(encoded)
        if len(raw) < 8 or (program_id, raw[:8]) not in _EVENT_SPECS:
            continue
        yield TradeEventLog(
            log_index=log_index,
            invoke_depth=len(stack),
            event=decode_trade_event(program_id, raw),
        )


def iter_lifecycle_event_logs(log_messages: Iterable[str]) -> Iterator[LifecycleEventLog]:
    """Yield pinned lifecycle events with log index and invocation depth."""

    stack: list[str] = []
    for log_index, message in enumerate(log_messages):
        if not isinstance(message, str):
            raise TypeError("transaction log messages must be strings")
        invoke = _INVOKE_RE.fullmatch(message)
        if invoke is not None:
            program_id, raw_depth = invoke.groups()
            depth = int(raw_depth)
            if depth <= len(stack):
                del stack[depth - 1 :]
            elif depth != len(stack) + 1:
                stack.clear()
            stack.append(program_id)
            continue
        success = _SUCCESS_RE.fullmatch(message)
        if success is not None:
            _pop_program(stack, success.group(1))
            continue
        failed = _FAILED_RE.match(message)
        if failed is not None:
            _pop_program(stack, failed.group(1))
            continue
        if not message.startswith(_PROGRAM_DATA_PREFIX) or not stack:
            continue
        program_id = stack[-1]
        if program_id not in {PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID}:
            continue
        encoded = message[len(_PROGRAM_DATA_PREFIX) :]
        raw = _strict_base64(encoded)
        if len(raw) < 8 or (program_id, raw[:8]) not in _LIFECYCLE_EVENT_SPECS:
            continue
        yield LifecycleEventLog(
            log_index=log_index,
            invoke_depth=len(stack),
            event=decode_lifecycle_event(program_id, raw),
        )


def extract_lifecycle_event_logs(log_messages: Iterable[str]) -> tuple[LifecycleEventLog, ...]:
    """Materialise :func:`iter_lifecycle_event_logs` into a tuple."""

    return tuple(iter_lifecycle_event_logs(log_messages))


def iter_state_event_logs(
    log_messages: Iterable[str], *, unknown_count: list[int] | None = None
) -> Iterator[StateEventLog]:
    """Yield pinned state/config events in chain-log order.

    Invocation frames are tracked exactly as in the trade/lifecycle iterators;
    a ``Program data`` record is attributed only to the program at the top of
    the stack.  Unknown event discriminators remain ignorable, while a
    recognised state discriminator with malformed/trailing bytes fails closed.
    """

    if unknown_count is not None and not unknown_count:
        unknown_count.append(0)
    stack: list[str] = []
    for log_index, message in enumerate(log_messages):
        if not isinstance(message, str):
            raise TypeError("transaction log messages must be strings")
        invoke = _INVOKE_RE.fullmatch(message)
        if invoke is not None:
            program_id, raw_depth = invoke.groups()
            depth = int(raw_depth)
            if depth <= len(stack):
                del stack[depth - 1 :]
            elif depth != len(stack) + 1:
                stack.clear()
            stack.append(program_id)
            continue
        success = _SUCCESS_RE.fullmatch(message)
        if success is not None:
            _pop_program(stack, success.group(1))
            continue
        failed = _FAILED_RE.match(message)
        if failed is not None:
            _pop_program(stack, failed.group(1))
            continue
        if not message.startswith(_PROGRAM_DATA_PREFIX) or not stack:
            continue
        program_id = stack[-1]
        if program_id not in {PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID}:
            continue
        encoded = message[len(_PROGRAM_DATA_PREFIX) :]
        raw = _strict_base64(encoded)
        if len(raw) < 8:
            if unknown_count is not None:
                unknown_count[0] += 1
            continue
        key = (program_id, raw[:8])
        if key not in _STATE_EVENT_SPECS:
            # Trade/lifecycle payloads are known records in this invocation,
            # not unknown state records.  Any other discriminator is audited
            # as unknown so adapters cannot silently discard a target-program
            # data record while still allowing forward-compatible consumers.
            if (
                unknown_count is not None
                and key not in _EVENT_SPECS
                and key not in _LIFECYCLE_EVENT_SPECS
            ):
                unknown_count[0] += 1
            continue
        yield StateEventLog(
            log_index=log_index,
            invoke_depth=len(stack),
            event=decode_state_event(program_id, raw),
        )


def extract_state_event_logs(
    log_messages: Iterable[str], *, unknown_count: list[int] | None = None
) -> tuple[StateEventLog, ...]:
    """Materialise :func:`iter_state_event_logs` into a tuple."""

    return tuple(iter_state_event_logs(log_messages, unknown_count=unknown_count))


def iter_program_event_logs(
    log_messages: Iterable[str],
) -> Iterator[TradeEventLog | LifecycleEventLog | StateEventLog]:
    """Yield every known event category while preserving log order/depth."""

    # Parse once so the three category iterators cannot disagree about stack
    # repair.  The local parser mirrors their fail-closed rules.
    stack: list[str] = []
    for log_index, message in enumerate(log_messages):
        if not isinstance(message, str):
            raise TypeError("transaction log messages must be strings")
        invoke = _INVOKE_RE.fullmatch(message)
        if invoke is not None:
            program_id, raw_depth = invoke.groups()
            depth = int(raw_depth)
            if depth <= len(stack):
                del stack[depth - 1 :]
            elif depth != len(stack) + 1:
                stack.clear()
            stack.append(program_id)
            continue
        success = _SUCCESS_RE.fullmatch(message)
        if success is not None:
            _pop_program(stack, success.group(1))
            continue
        failed = _FAILED_RE.match(message)
        if failed is not None:
            _pop_program(stack, failed.group(1))
            continue
        if not message.startswith(_PROGRAM_DATA_PREFIX) or not stack:
            continue
        program_id = stack[-1]
        if program_id not in {PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID}:
            continue
        encoded = message[len(_PROGRAM_DATA_PREFIX) :]
        raw = _strict_base64(encoded)
        if len(raw) < 8:
            continue
        key = (program_id, raw[:8])
        if key in _EVENT_SPECS:
            yield TradeEventLog(log_index, len(stack), decode_trade_event(program_id, raw))
        elif key in _LIFECYCLE_EVENT_SPECS:
            yield LifecycleEventLog(log_index, len(stack), decode_lifecycle_event(program_id, raw))
        elif key in _STATE_EVENT_SPECS:
            yield StateEventLog(log_index, len(stack), decode_state_event(program_id, raw))


def extract_program_event_logs(
    log_messages: Iterable[str],
) -> tuple[TradeEventLog | LifecycleEventLog | StateEventLog, ...]:
    """Materialise :func:`iter_program_event_logs` into a tuple."""

    return tuple(iter_program_event_logs(log_messages))


def extract_trade_event_logs(log_messages: Iterable[str]) -> tuple[TradeEventLog, ...]:
    """Materialise :func:`iter_trade_event_logs` into a tuple."""

    return tuple(iter_trade_event_logs(log_messages))


__all__ = [
    "DecodedLifecycleEvent",
    "DecodedStateEvent",
    "DecodedTradeEvent",
    "LifecycleEvent",
    "LifecycleEventLog",
    "MalformedProgramEventError",
    "PUMP_COMPLETE_EVENT_DISCRIMINATOR",
    "PUMP_COMPLETE_PUMP_AMM_MIGRATION_EVENT_DISCRIMINATOR",
    "PUMP_CREATE_EVENT_DISCRIMINATOR",
    "PUMP_COLLECT_CREATOR_FEE_EVENT_DISCRIMINATOR",
    "PUMP_DISTRIBUTE_CREATOR_FEES_EVENT_DISCRIMINATOR",
    "PUMP_EVENT_IDL_SHA256",
    "PUMPSWAP_BUY_EVENT_DISCRIMINATOR",
    "PUMPSWAP_COLLECT_COIN_CREATOR_FEE_EVENT_DISCRIMINATOR",
    "PUMPSWAP_CREATE_CONFIG_EVENT_DISCRIMINATOR",
    "PUMPSWAP_CREATE_POOL_EVENT_DISCRIMINATOR",
    "PUMPSWAP_DEPOSIT_EVENT_DISCRIMINATOR",
    "PUMPSWAP_DISABLE_EVENT_DISCRIMINATOR",
    "PUMPSWAP_EXTEND_ACCOUNT_EVENT_DISCRIMINATOR",
    "PUMPSWAP_EVENT_IDL_SOURCE",
    "PUMPSWAP_EVENT_IDL_SHA256",
    "PUMPSWAP_SELL_EVENT_DISCRIMINATOR",
    "PUMPSWAP_UPDATE_ADMIN_EVENT_DISCRIMINATOR",
    "PUMPSWAP_UPDATE_FEE_CONFIG_EVENT_DISCRIMINATOR",
    "PUMPSWAP_WITHDRAW_EVENT_DISCRIMINATOR",
    "PUMP_EVENT_IDL_SOURCE",
    "PUMP_TRADE_EVENT_DISCRIMINATOR",
    "PUMP_MIGRATE_BONDING_CURVE_CREATOR_EVENT_DISCRIMINATOR",
    "PUMP_RESERVED_FEE_RECIPIENTS_EVENT_DISCRIMINATOR",
    "PUMP_SET_CREATOR_EVENT_DISCRIMINATOR",
    "PUMP_SET_PARAMS_EVENT_DISCRIMINATOR",
    "PUMP_UPDATE_GLOBAL_AUTHORITY_EVENT_DISCRIMINATOR",
    "PUMP_UPDATE_MAYHEM_VIRTUAL_PARAMS_EVENT_DISCRIMINATOR",
    "PUMPSWAP_ADMIN_SET_COIN_CREATOR_EVENT_DISCRIMINATOR",
    "PUMPSWAP_MIGRATE_POOL_COIN_CREATOR_EVENT_DISCRIMINATOR",
    "PUMPSWAP_RESERVED_FEE_RECIPIENTS_EVENT_DISCRIMINATOR",
    "PUMPSWAP_SET_BONDING_CURVE_COIN_CREATOR_EVENT_DISCRIMINATOR",
    "PUMPSWAP_SET_METAPLEX_COIN_CREATOR_EVENT_DISCRIMINATOR",
    "StateEventLog",
    "ProgramEventDecodeError",
    "PumpCompleteEvent",
    "PumpCompletePumpAmmMigrationEvent",
    "PumpCreateEvent",
    "PumpSetParamsEvent",
    "PumpUpdateMayhemVirtualParamsEvent",
    "PumpSetCreatorEvent",
    "PumpMigrateBondingCurveCreatorEvent",
    "PumpDistributeCreatorFeesEvent",
    "PumpCollectCreatorFeeEvent",
    "PumpReservedFeeRecipientsEvent",
    "PumpUpdateGlobalAuthorityEvent",
    "PumpSwapCreatePoolEvent",
    "PumpSwapDepositEvent",
    "PumpSwapWithdrawEvent",
    "PumpSwapCreateConfigEvent",
    "PumpSwapUpdateFeeConfigEvent",
    "PumpSwapDisableEvent",
    "PumpSwapUpdateAdminEvent",
    "PumpSwapMigratePoolCoinCreatorEvent",
    "PumpSwapSetBondingCurveCoinCreatorEvent",
    "PumpSwapSetMetaplexCoinCreatorEvent",
    "PumpSwapCollectCoinCreatorFeeEvent",
    "PumpSwapReservedFeeRecipientsEvent",
    "PumpSwapAdminSetCoinCreatorEvent",
    "Shareholder",
    "TradeEventLog",
    "UnknownProgramEventError",
    "decode_lifecycle_event",
    "decode_program_event",
    "decode_state_event",
    "decode_trade_event",
    "extract_lifecycle_event_logs",
    "extract_program_event_logs",
    "extract_state_event_logs",
    "extract_trade_event_logs",
    "iter_lifecycle_event_logs",
    "iter_program_event_logs",
    "iter_state_event_logs",
    "iter_trade_event_logs",
]
