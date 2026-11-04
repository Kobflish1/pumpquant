"""Seed-only Pump and PumpSwap instruction-intent decoding.

This module stops at instruction arguments.  It does not inspect
accounts, transaction outcomes, logs, or token transfers, and it never turns a
candidate instruction into a :class:`~pumpquant.schema.SwapEvent`.  Callers can
use the immutable :class:`CandidateIntent` as a seed for a later, state-aware
canonicaliser.

The instruction table covers all 40 Pump and 27 PumpSwap instructions in one
official ``pump-public-docs`` commit.  It is still seed-only: decoding an
argument payload does not assert account ownership, transaction success, or a
reserve/configuration write.
The key for every entry includes the program address, the complete eight-byte
Anchor discriminator, and that pinned revision.  In particular, Pump and
PumpSwap retain separate entries for their colliding ``buy`` and
``sell`` discriminators.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, Literal, TypeAlias

# These are the mainnet addresses in idl/pump.json and idl/pump_amm.json at the
# pinned official repository revision below.
PUMP_PROGRAM_ID: Final[str] = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMPSWAP_PROGRAM_ID: Final[str] = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
# Spellings used by callers and by the public docs are both useful.  Keep one
# value so accidental divergence is impossible.

PINNED_IDL_REVISION: Final[str] = "1b822158844a60ca577df6ca122211b595a1a578"

# Wire-layout labels.  ``PINNED_IDL_REVISION`` is used for the
# current direct-bool tuple struct from the official IDL.  The other labels are
# retained in the candidate record whenever a known historical wire shape is
# decoded; they are not alternate, unpinned instruction tables.
LAYOUT_REVISION_LEGACY_NO_TRACK_VOLUME: Final[str] = "legacy_no_track_volume"
LAYOUT_REVISION_HISTORICAL_OPTION_BOOL: Final[str] = "historical_option_bool"
LAYOUT_REVISION_HISTORICAL_TRACK_VOLUME: Final[str] = "historical_track_volume"

DataEncoding: TypeAlias = Literal["base58", "base64"]
IntentValue: TypeAlias = int | bool | str | bytes | tuple[bytes, ...] | None
IntentArguments: TypeAlias = Mapping[str, IntentValue]


class InstructionIntentDecodeError(ValueError):
    """Base error for all fail-closed instruction decoding failures."""


class UnsupportedEncodingError(InstructionIntentDecodeError):
    """The caller did not select one supported transport encoding."""


class UnknownInstructionError(InstructionIntentDecodeError):
    """No pinned instruction exists for the supplied program/discriminator."""


class MalformedInstructionError(InstructionIntentDecodeError):
    """Encoded data or Borsh arguments are malformed or contain trailing bytes."""


# Solana's canonical base58 alphabet (the Bitcoin alphabet, without 0, O, I,
# and l).  Keeping the implementation here avoids importing a third-party
# wallet/key package in the raw-seed path.
BASE58_ALPHABET: Final[str] = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_VALUES: Final[dict[str, int]] = {char: index for index, char in enumerate(BASE58_ALPHABET)}


def encode_base58(value: bytes) -> str:
    """Encode bytes using canonical Solana/Bitcoin base58.

    This small encoder is also used to round-trip-check decoded text.  Empty
    bytes are represented by the empty string; each leading zero byte is
    represented by one leading ``1`` character.
    """

    if not isinstance(value, bytes):
        raise TypeError("value must be bytes")
    if not value:
        return ""

    leading_zeroes = len(value) - len(value.lstrip(b"\x00"))
    number = int.from_bytes(value, "big")
    digits: list[str] = []
    while number:
        number, remainder = divmod(number, 58)
        digits.append(BASE58_ALPHABET[remainder])
    return "1" * leading_zeroes + "".join(reversed(digits))


def decode_base58(value: str | bytes) -> bytes:
    """Decode *value* as canonical base58, rejecting malformed text.

    Canonical validation is important for raw data provenance: invalid
    alphabet characters, non-ASCII bytes, and alternate spellings are all
    rejected rather than silently normalised.
    """

    if isinstance(value, bytes):
        try:
            value = value.decode("ascii")
        except UnicodeDecodeError as exc:
            raise MalformedInstructionError("base58 data must be ASCII") from exc
    if not isinstance(value, str):
        raise TypeError("base58 data must be str or ASCII bytes")

    number = 0
    for char in value:
        digit = _BASE58_VALUES.get(char)
        if digit is None:
            raise MalformedInstructionError(f"invalid base58 character {char!r}")
        number = number * 58 + digit

    leading_ones = len(value) - len(value.lstrip("1"))
    if number:
        byte_length = (number.bit_length() + 7) // 8
        decoded_number = number.to_bytes(byte_length, "big")
    else:
        decoded_number = b""
    decoded = b"\x00" * leading_ones + decoded_number

    # Re-encoding catches every non-canonical spelling, including an
    # unnecessary zero prefix.  (The alphabet loop above catches bad chars.)
    if encode_base58(decoded) != value:
        raise MalformedInstructionError("non-canonical base58 data")
    return decoded


def _decode_base64(value: str | bytes) -> bytes:
    """Decode canonical RFC 4648 base64 with no whitespace or URL alphabet."""

    if isinstance(value, bytes):
        try:
            text = value.decode("ascii")
        except UnicodeDecodeError as exc:
            raise MalformedInstructionError("base64 data must be ASCII") from exc
    elif isinstance(value, str):
        text = value
    else:
        raise TypeError("base64 data must be str or ASCII bytes")
    try:
        raw = base64.b64decode(text.encode("ascii"), validate=True)
    except (UnicodeEncodeError, ValueError, binascii.Error) as exc:
        raise MalformedInstructionError("invalid base64 data") from exc
    # Strict validation still allows non-zero pad bits in some Python versions;
    # requiring the exact round trip closes that alternate representation too.
    if base64.b64encode(raw).decode("ascii") != text:
        raise MalformedInstructionError("non-canonical base64 data")
    return raw


@dataclass(frozen=True, slots=True)
class InstructionSpec:
    """Pinned metadata for one supported instruction variant."""

    program_id: str
    name: str
    discriminator: bytes
    fields: tuple[str, ...]
    field_types: tuple[str, ...]
    # A legacy Pump/PumpSwap wire shape can omit or append the track-volume
    # option while retaining the same Anchor discriminator.  This metadata is
    # explicit; arbitrary trailing bytes are never accepted.
    historical_optional_tail: str | None = None
    idl_revision: str = PINNED_IDL_REVISION

    def __post_init__(self) -> None:
        if not isinstance(self.program_id, str) or not self.program_id:
            raise ValueError("program_id must be a non-empty string")
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("name must be a non-empty string")
        if not isinstance(self.discriminator, bytes) or len(self.discriminator) != 8:
            raise ValueError("discriminator must be exactly eight bytes")
        if len(self.fields) != len(self.field_types):
            raise ValueError("fields and field_types must have equal lengths")
        if self.historical_optional_tail is not None and (
            not self.historical_optional_tail or self.historical_optional_tail in self.fields
        ):
            raise ValueError("historical optional tail must be a distinct non-empty field name")
        if self.idl_revision != PINNED_IDL_REVISION:
            raise ValueError("only the pinned IDL revision is supported")


@dataclass(frozen=True, slots=True)
class CandidateIntent:
    """Labelled candidate instruction intent.

    This is *not* a fill, execution result, or normalised swap event.  The
    values are only the arguments serialised in the instruction's Borsh data;
    ``raw_data`` retains those exact decoded bytes for later reprocessing.
    Pubkeys remain their original 32-byte values so no text encoding is
    mistaken for an on-chain identity; use :func:`encode_base58` when a
    display form is required.
    """

    program_id: str
    instruction_name: str
    discriminator: bytes
    args: IntentArguments
    raw_data: bytes
    layout_revision: str
    idl_revision: str = PINNED_IDL_REVISION
    intent_kind: Literal["candidate_intent"] = "candidate_intent"

    def __post_init__(self) -> None:
        if not isinstance(self.program_id, str) or not self.program_id:
            raise ValueError("program_id must be a non-empty string")
        if not isinstance(self.instruction_name, str) or not self.instruction_name:
            raise ValueError("instruction_name must be a non-empty string")
        if not isinstance(self.discriminator, bytes) or len(self.discriminator) != 8:
            raise ValueError("discriminator must be exactly eight bytes")
        if not isinstance(self.raw_data, bytes) or len(self.raw_data) < 8:
            raise ValueError("raw_data must contain the eight-byte discriminator")
        if self.raw_data[:8] != self.discriminator:
            raise ValueError("raw_data discriminator does not match discriminator")
        if not isinstance(self.layout_revision, str) or not self.layout_revision:
            raise ValueError("layout_revision must be a non-empty string")
        if self.idl_revision != PINNED_IDL_REVISION:
            raise ValueError("only the pinned IDL revision is supported")
        if self.intent_kind != "candidate_intent":
            raise ValueError("intent_kind must be 'candidate_intent'")
        if not isinstance(self.args, Mapping):
            raise TypeError("args must be a mapping")
        # A frozen dataclass does not freeze a dict passed to it.  Copying to a
        # mapping proxy makes the public result immutable even if a caller
        # retains and later mutates its input mapping.
        copied = dict(self.args)
        if any(not isinstance(key, str) or not key for key in copied):
            raise ValueError("argument names must be non-empty strings")
        object.__setattr__(self, "args", MappingProxyType(copied))

    @property
    def kind(self) -> Literal["candidate_intent"]:
        """Short label useful when serialising mixed decoder outputs."""

        return self.intent_kind

    @property
    def intent_type(self) -> Literal["candidate_intent"]:
        """Long-form semantic label for serialisers that use ``type`` names."""

        return self.intent_kind

    @property
    def type(self) -> Literal["candidate_intent"]:
        """Alias for the candidate-intent label."""

        return self.intent_kind

    @property
    def label(self) -> Literal["candidate_intent"]:
        """Semantic label; never an event or fill type."""

        return self.intent_kind

    @property
    def instruction(self) -> str:
        """Alias for ``instruction_name``."""

        return self.instruction_name

    @property
    def program(self) -> str:
        """Alias for the program address that qualified the discriminator."""

        return self.program_id

    @property
    def name(self) -> str:
        """Alias for ``instruction_name``."""

        return self.instruction_name

    @property
    def arguments(self) -> IntentArguments:
        """Alias for the argument mapping."""

        return self.args

    @property
    def raw_payload(self) -> bytes:
        """Decoded instruction bytes, including the discriminator."""

        return self.raw_data

    @property
    def raw_instruction_data(self) -> bytes:
        """Alias for the lossless decoded instruction bytes."""

        return self.raw_data

    @property
    def raw_args(self) -> bytes:
        """Borsh argument bytes after the eight-byte discriminator."""

        return self.raw_data[8:]

    @property
    def discriminator_hex(self) -> str:
        """Anchor discriminator as lowercase hexadecimal for provenance."""

        return self.discriminator.hex()

    @property
    def track_volume(self) -> bool | None:
        """Normalised optional track-volume flag when present in a layout.

        Legacy sell layouts did not name this argument in the pinned IDL, so
        they return ``None`` here without adding a synthetic key
        to ``args``.
        """

        value = self.args.get("track_volume")
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        raise ValueError("track_volume argument is not a bool")


FieldType: TypeAlias = Literal[
    "u8",
    "u16",
    "u64",
    "i64",
    "bool",
    "string",
    "pubkey",
    "pubkey_array8",
    "optional_bool",
    "optional_u64",
]


def _spec(
    program_id: str,
    name: str,
    discriminator: tuple[int, ...],
    fields: tuple[tuple[str, FieldType], ...] = (),
    *,
    historical_optional_tail: str | None = None,
) -> InstructionSpec:
    return InstructionSpec(
        program_id=program_id,
        name=name,
        discriminator=bytes(discriminator),
        fields=tuple(field[0] for field in fields),
        field_types=tuple(field[1] for field in fields),
        historical_optional_tail=historical_optional_tail,
    )


# The values below are copied from the pinned official IDLs, while keeping the
# map itself keyed by (program_id, discriminator, revision) as a guard against
# Pump/PumpSwap discriminator collisions.
_SPEC_LIST: tuple[InstructionSpec, ...] = (
    # Pump bonding curve.
    _spec(
        PUMP_PROGRAM_ID,
        "create",
        (24, 30, 200, 40, 5, 28, 7, 119),
        (("name", "string"), ("symbol", "string"), ("uri", "string"), ("creator", "pubkey")),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "create_v2",
        (214, 144, 76, 236, 95, 139, 49, 180),
        (
            ("name", "string"),
            ("symbol", "string"),
            ("uri", "string"),
            ("creator", "pubkey"),
            ("is_mayhem_mode", "bool"),
            ("is_cashback_enabled", "optional_bool"),
        ),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "buy",
        (102, 6, 61, 18, 1, 218, 235, 234),
        (
            ("amount", "u64"),
            ("max_sol_cost", "u64"),
            ("track_volume", "optional_bool"),
        ),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "buy_v2",
        (184, 23, 238, 97, 103, 197, 211, 61),
        (("amount", "u64"), ("max_sol_cost", "u64")),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "buy_exact_sol_in",
        (56, 252, 116, 8, 158, 223, 205, 95),
        (
            ("spendable_sol_in", "u64"),
            ("min_tokens_out", "u64"),
            ("track_volume", "optional_bool"),
        ),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "buy_exact_quote_in_v2",
        (194, 171, 28, 70, 104, 77, 91, 47),
        (("spendable_quote_in", "u64"), ("min_tokens_out", "u64")),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "sell",
        (51, 230, 133, 164, 1, 127, 131, 173),
        (("amount", "u64"), ("min_sol_output", "u64")),
        historical_optional_tail="track_volume",
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "sell_v2",
        (93, 246, 130, 60, 231, 233, 64, 178),
        (("amount", "u64"), ("min_sol_output", "u64")),
    ),
    _spec(PUMP_PROGRAM_ID, "migrate", (155, 234, 231, 146, 236, 158, 162, 30)),
    _spec(PUMP_PROGRAM_ID, "migrate_v2", (187, 203, 18, 31, 206, 237, 254, 41)),
    _spec(
        PUMP_PROGRAM_ID,
        "set_params",
        (27, 234, 178, 52, 147, 2, 187, 141),
        (
            ("initial_virtual_token_reserves", "u64"),
            ("initial_virtual_sol_reserves", "u64"),
            ("initial_real_token_reserves", "u64"),
            ("token_total_supply", "u64"),
            ("fee_basis_points", "u64"),
            ("withdraw_authority", "pubkey"),
            ("enable_migrate", "bool"),
            ("pool_migration_fee", "u64"),
            ("creator_fee_basis_points", "u64"),
            ("set_creator_authority", "pubkey"),
            ("admin_set_creator_authority", "pubkey"),
        ),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "set_virtual_quote_reserves",
        (101, 135, 191, 104, 9, 88, 20, 96),
        (("initial_virtual_quote_reserves", "u64"),),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "toggle_cashback_enabled",
        (115, 103, 224, 255, 189, 89, 86, 195),
        (("enabled", "bool"),),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "toggle_create_v2",
        (28, 255, 230, 240, 172, 107, 203, 171),
        (("enabled", "bool"),),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "toggle_mayhem_mode",
        (1, 9, 111, 208, 100, 31, 255, 163),
        (("enabled", "bool"),),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "update_buyback_config",
        (251, 224, 171, 146, 160, 26, 113, 233),
        (("buyback_basis_points", "optional_u64"),),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "add_quote_mint",
        (111, 121, 21, 56, 40, 24, 94, 209),
        (("quote_mint", "pubkey"),),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "remove_quote_mint",
        (177, 65, 223, 38, 88, 209, 158, 155),
        (("quote_mint", "pubkey"),),
    ),
    # PumpSwap / pump_amm.
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "create_pool",
        (233, 146, 209, 142, 207, 104, 64, 188),
        (
            ("index", "u16"),
            ("base_amount_in", "u64"),
            ("quote_amount_in", "u64"),
            ("coin_creator", "pubkey"),
            ("is_mayhem_mode", "bool"),
            ("is_cashback_coin", "optional_bool"),
        ),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "buy",
        (102, 6, 61, 18, 1, 218, 235, 234),
        (
            ("base_amount_out", "u64"),
            ("max_quote_amount_in", "u64"),
            ("track_volume", "optional_bool"),
        ),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "buy_exact_quote_in",
        (198, 46, 21, 82, 180, 217, 232, 112),
        (
            ("spendable_quote_in", "u64"),
            ("min_base_amount_out", "u64"),
            ("track_volume", "optional_bool"),
        ),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "sell",
        (51, 230, 133, 164, 1, 127, 131, 173),
        (("base_amount_in", "u64"), ("min_quote_amount_out", "u64")),
        historical_optional_tail="track_volume",
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "deposit",
        (242, 35, 198, 137, 82, 225, 242, 182),
        (
            ("lp_token_amount_out", "u64"),
            ("max_base_amount_in", "u64"),
            ("max_quote_amount_in", "u64"),
        ),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "withdraw",
        (183, 18, 70, 156, 148, 109, 161, 34),
        (
            ("lp_token_amount_in", "u64"),
            ("min_base_amount_out", "u64"),
            ("min_quote_amount_out", "u64"),
        ),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "create_config",
        (201, 207, 243, 114, 75, 111, 47, 189),
        (
            ("lp_fee_basis_points", "u64"),
            ("protocol_fee_basis_points", "u64"),
            ("protocol_fee_recipients", "pubkey_array8"),
            ("coin_creator_fee_basis_points", "u64"),
            ("admin_set_coin_creator_authority", "pubkey"),
        ),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "update_fee_config",
        (104, 184, 103, 242, 88, 151, 107, 20),
        (
            ("lp_fee_basis_points", "u64"),
            ("protocol_fee_basis_points", "u64"),
            ("protocol_fee_recipients", "pubkey_array8"),
            ("coin_creator_fee_basis_points", "u64"),
            ("admin_set_coin_creator_authority", "pubkey"),
        ),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "disable",
        (185, 173, 187, 90, 216, 15, 238, 233),
        (
            ("disable_create_pool", "bool"),
            ("disable_deposit", "bool"),
            ("disable_withdraw", "bool"),
            ("disable_buy", "bool"),
            ("disable_sell", "bool"),
        ),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "update_admin",
        (161, 176, 40, 213, 60, 184, 179, 228),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "set_reserved_fee_recipients",
        (111, 172, 162, 232, 114, 89, 213, 142),
        (("whitelist_pda", "pubkey"),),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "update_buyback_config",
        (251, 224, 171, 146, 160, 26, 113, 233),
        (("buyback_basis_points", "optional_u64"),),
    ),
    # Remaining pinned Pump state/configuration instructions.  These carry no
    # reserve math themselves, but their successful account writes change the
    # fee/creator/config schedule used by later quotes.
    _spec(
        PUMP_PROGRAM_ID,
        "admin_set_creator",
        (69, 25, 171, 142, 57, 239, 13, 4),
        (("creator", "pubkey"),),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "admin_set_idl_authority",
        (8, 217, 96, 231, 144, 104, 192, 5),
        (("idl_authority", "pubkey"),),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "admin_update_token_incentives",
        (209, 11, 115, 87, 213, 23, 124, 204),
        (
            ("start_time", "i64"),
            ("end_time", "i64"),
            ("seconds_in_a_day", "i64"),
            ("day_number", "u64"),
            ("pump_token_supply_per_day", "u64"),
        ),
    ),
    _spec(PUMP_PROGRAM_ID, "claim_cashback", (37, 58, 35, 126, 190, 53, 228, 197)),
    _spec(PUMP_PROGRAM_ID, "claim_cashback_v2", (122, 243, 204, 65, 94, 116, 29, 55)),
    _spec(PUMP_PROGRAM_ID, "claim_token_incentives", (16, 4, 71, 28, 204, 1, 40, 27)),
    _spec(PUMP_PROGRAM_ID, "close_user_volume_accumulator", (249, 69, 164, 218, 150, 103, 84, 138)),
    _spec(PUMP_PROGRAM_ID, "collect_creator_fee", (20, 22, 86, 123, 198, 28, 219, 132)),
    _spec(PUMP_PROGRAM_ID, "collect_creator_fee_v2", (207, 17, 138, 242, 4, 34, 19, 56)),
    _spec(PUMP_PROGRAM_ID, "distribute_creator_fees", (165, 114, 103, 0, 121, 206, 247, 81)),
    _spec(
        PUMP_PROGRAM_ID,
        "distribute_creator_fees_v2",
        (255, 203, 19, 79, 244, 68, 8, 159),
        (("initialize_ata", "bool"),),
    ),
    _spec(PUMP_PROGRAM_ID, "extend_account", (234, 102, 194, 203, 150, 72, 62, 229)),
    _spec(PUMP_PROGRAM_ID, "get_minimum_distributable_fee", (117, 225, 127, 202, 134, 95, 68, 35)),
    _spec(PUMP_PROGRAM_ID, "init_user_volume_accumulator", (94, 6, 202, 115, 255, 96, 232, 183)),
    _spec(PUMP_PROGRAM_ID, "initialize", (175, 175, 109, 31, 13, 152, 155, 237)),
    _spec(
        PUMP_PROGRAM_ID,
        "migrate_bonding_curve_creator",
        (87, 124, 52, 191, 52, 38, 214, 232),
    ),
    _spec(
        PUMP_PROGRAM_ID,
        "set_creator",
        (254, 148, 255, 112, 207, 142, 170, 165),
        (("creator", "pubkey"),),
    ),
    _spec(PUMP_PROGRAM_ID, "set_mayhem_virtual_params", (61, 169, 188, 191, 153, 149, 42, 97)),
    _spec(PUMP_PROGRAM_ID, "set_metaplex_creator", (138, 96, 174, 217, 48, 85, 197, 246)),
    _spec(
        PUMP_PROGRAM_ID,
        "set_reserved_fee_recipients",
        (111, 172, 162, 232, 114, 89, 213, 142),
        (("whitelist_pda", "pubkey"),),
    ),
    _spec(PUMP_PROGRAM_ID, "sync_user_volume_accumulator", (86, 31, 192, 87, 163, 87, 79, 238)),
    _spec(
        PUMP_PROGRAM_ID,
        "update_global_authority",
        (227, 181, 74, 196, 208, 21, 97, 213),
    ),
    # Remaining pinned PumpSwap creator/fee transitions.
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "admin_set_coin_creator",
        (242, 40, 117, 145, 73, 96, 105, 104),
        (("coin_creator", "pubkey"),),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "admin_update_token_incentives",
        (209, 11, 115, 87, 213, 23, 124, 204),
        (
            ("start_time", "i64"),
            ("end_time", "i64"),
            ("seconds_in_a_day", "i64"),
            ("day_number", "u64"),
            ("token_supply_per_day", "u64"),
        ),
    ),
    _spec(PUMPSWAP_PROGRAM_ID, "claim_cashback", (37, 58, 35, 126, 190, 53, 228, 197)),
    _spec(PUMPSWAP_PROGRAM_ID, "claim_token_incentives", (16, 4, 71, 28, 204, 1, 40, 27)),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "close_user_volume_accumulator",
        (249, 69, 164, 218, 150, 103, 84, 138),
    ),
    _spec(PUMPSWAP_PROGRAM_ID, "collect_coin_creator_fee", (160, 57, 89, 42, 181, 139, 43, 66)),
    _spec(PUMPSWAP_PROGRAM_ID, "extend_account", (234, 102, 194, 203, 150, 72, 62, 229)),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "init_user_volume_accumulator",
        (94, 6, 202, 115, 255, 96, 232, 183),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "migrate_pool_coin_creator",
        (208, 8, 159, 4, 74, 175, 16, 58),
    ),
    _spec(PUMPSWAP_PROGRAM_ID, "set_coin_creator", (210, 149, 128, 45, 188, 58, 78, 175)),
    _spec(PUMPSWAP_PROGRAM_ID, "sync_user_volume_accumulator", (86, 31, 192, 87, 163, 87, 79, 238)),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "toggle_cashback_enabled",
        (115, 103, 224, 255, 189, 89, 86, 195),
        (("enabled", "bool"),),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "toggle_mayhem_mode",
        (1, 9, 111, 208, 100, 31, 255, 163),
        (("enabled", "bool"),),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "transfer_creator_fees_to_pump",
        (139, 52, 134, 85, 228, 229, 108, 241),
    ),
    _spec(
        PUMPSWAP_PROGRAM_ID,
        "transfer_creator_fees_to_pump_v2",
        (1, 33, 78, 185, 33, 67, 44, 92),
    ),
)


_spec_map: dict[tuple[str, bytes, str], InstructionSpec] = {
    (entry.program_id, entry.discriminator, entry.idl_revision): entry for entry in _SPEC_LIST
}
INSTRUCTION_SPECS: Mapping[tuple[str, bytes, str], InstructionSpec] = MappingProxyType(_spec_map)


class _BorshReader:
    """Bounds-checked little-endian Borsh reader for the supported field set."""

    __slots__ = ("_data", "_offset")

    def __init__(self, data: bytes, offset: int = 8) -> None:
        self._data = data
        self._offset = offset

    @property
    def remaining(self) -> int:
        return len(self._data) - self._offset

    def _take(self, size: int) -> bytes:
        if size < 0 or self._offset + size > len(self._data):
            raise MalformedInstructionError("truncated Borsh argument data")
        start = self._offset
        self._offset += size
        return self._data[start : start + size]

    def _integer(self, size: int, *, signed: bool = False) -> int:
        return int.from_bytes(self._take(size), "little", signed=signed)

    def u16(self) -> int:
        return self._integer(2)

    def u8(self) -> int:
        return self._integer(1)

    def u64(self) -> int:
        return self._integer(8)

    def pubkey_array(self, count: int) -> tuple[bytes, ...]:
        if count < 0 or self.remaining < count * 32:
            raise MalformedInstructionError("truncated public-key array")
        return tuple(self.pubkey() for _ in range(count))

    def i64(self) -> int:
        return self._integer(8, signed=True)

    def read_bool(self) -> bool:
        value = self._take(1)[0]
        if value not in (0, 1):
            raise MalformedInstructionError("Borsh bool must be 0 or 1")
        return bool(value)

    def optional_bool(self) -> tuple[bool | None, str]:
        """Read one of the supported OptionBool wire layouts.

        The pinned IDLs define ``OptionBool`` as a tuple struct containing a
        direct bool, hence the one-byte current shape.  A historical omitted
        argument (zero bytes) and the older Borsh ``Option<bool>`` shape
        (``Some(value)`` as tag ``1`` followed by one bool byte) are retained
        as named layouts.  A two-byte ``None`` tag is not valid Borsh and is
        rejected rather than guessed.
        """

        if self.remaining == 0:
            return None, LAYOUT_REVISION_LEGACY_NO_TRACK_VOLUME
        if self.remaining == 1:
            return self.read_bool(), PINNED_IDL_REVISION
        if self.remaining == 2:
            tag = self._take(1)[0]
            if tag != 1:
                raise MalformedInstructionError("historical Option<bool> tag must be 1")
            return self.read_bool(), LAYOUT_REVISION_HISTORICAL_OPTION_BOOL
        raise MalformedInstructionError("unrecognized OptionBool payload length")

    def optional_u64(self) -> int | None:
        """Read a canonical Borsh ``Option<u64>`` argument."""

        if self.remaining < 1:
            raise MalformedInstructionError("truncated Option<u64> tag")
        tag = self._take(1)[0]
        if tag == 0:
            return None
        if tag == 1:
            return self.u64()
        raise MalformedInstructionError("Option<u64> tag must be 0 or 1")

    def string(self) -> str:
        size = self._integer(4)
        raw = self._take(size)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MalformedInstructionError("Borsh string is not valid UTF-8") from exc

    def pubkey(self) -> bytes:
        return self._take(32)

    def finish(self) -> None:
        if self._offset != len(self._data):
            raise MalformedInstructionError("trailing bytes after Borsh arguments")


def _decode_args(spec: InstructionSpec, raw: bytes) -> tuple[IntentArguments, str]:
    reader = _BorshReader(raw)
    values: dict[str, IntentValue] = {}
    layout_revision = PINNED_IDL_REVISION
    for name, field_type in zip(spec.fields, spec.field_types, strict=True):
        # IDL ``OptionBool`` is a named struct containing exactly one bool at
        # this pinned revision, so it uses the same one-byte canonical encoding.
        if field_type == "u8":
            values[name] = reader.u8()
        elif field_type == "u16":
            values[name] = reader.u16()
        elif field_type == "u64":
            values[name] = reader.u64()
        elif field_type == "i64":
            values[name] = reader.i64()
        elif field_type == "bool":
            values[name] = reader.read_bool()
        elif field_type == "optional_bool":
            value, layout_revision = reader.optional_bool()
            # OptionBool is a nullable protocol argument.  Keep
            # None in the typed result for the known omitted legacy layout.
            values[name] = value
        elif field_type == "optional_u64":
            values[name] = reader.optional_u64()
        elif field_type == "string":
            values[name] = reader.string()
        elif field_type == "pubkey":
            values[name] = reader.pubkey()
        elif field_type == "pubkey_array8":
            values[name] = reader.pubkey_array(8)
        else:  # pragma: no cover - guarded by the literal field type alias
            raise MalformedInstructionError(f"unsupported pinned Borsh field type {field_type!r}")
    if spec.historical_optional_tail is not None and reader.remaining:
        value, layout_revision = reader.optional_bool()
        values[spec.historical_optional_tail] = value
        if layout_revision != LAYOUT_REVISION_LEGACY_NO_TRACK_VOLUME:
            # The synthetic tail is not part of the pinned sell schemas; both
            # its direct-bool and historical Option<bool> forms are therefore
            # labelled historical even though they share primitive encodings
            # with the buy-side field.
            layout_revision = LAYOUT_REVISION_HISTORICAL_TRACK_VOLUME
    reader.finish()
    return values, layout_revision


def _decode_transport(data: str | bytes, encoding: DataEncoding) -> bytes:
    if encoding == "base58":
        return decode_base58(data)
    if encoding == "base64":
        return _decode_base64(data)
    raise UnsupportedEncodingError("encoding must be explicitly 'base58' or 'base64'")


def decode_instruction_intent(
    program_id: str,
    data: str | bytes,
    *,
    encoding: DataEncoding,
) -> CandidateIntent:
    """Decode one encoded Pump/PumpSwap instruction into a candidate intent.

    ``encoding`` is mandatory and must be exactly ``"base58"`` or
    ``"base64"``.  There is no auto-detection because both
    alphabets can represent plausible text and guessing would make malformed
    seed rows look valid.  Unknown programs/discriminators and every malformed
    Borsh payload raise :class:`InstructionIntentDecodeError` subclasses.
    """

    if not isinstance(program_id, str):
        raise TypeError("program_id must be a string")
    if not isinstance(encoding, str) or encoding not in ("base58", "base64"):
        raise UnsupportedEncodingError("encoding must be explicitly 'base58' or 'base64'")
    raw = _decode_transport(data, encoding)
    if len(raw) < 8:
        raise MalformedInstructionError("instruction data is shorter than its discriminator")
    discriminator = raw[:8]
    spec = INSTRUCTION_SPECS.get((program_id, discriminator, PINNED_IDL_REVISION))
    if spec is None:
        raise UnknownInstructionError("unknown program/discriminator for pinned IDL revision")
    values, layout_revision = _decode_args(spec, raw)
    return CandidateIntent(
        program_id=program_id,
        instruction_name=spec.name,
        discriminator=discriminator,
        args=values,
        raw_data=raw,
        layout_revision=layout_revision,
        idl_revision=PINNED_IDL_REVISION,
    )


__all__ = [
    "BASE58_ALPHABET",
    "CandidateIntent",
    "DataEncoding",
    "INSTRUCTION_SPECS",
    "IntentArguments",
    "IntentValue",
    "InstructionIntentDecodeError",
    "InstructionSpec",
    "LAYOUT_REVISION_HISTORICAL_OPTION_BOOL",
    "LAYOUT_REVISION_HISTORICAL_TRACK_VOLUME",
    "LAYOUT_REVISION_LEGACY_NO_TRACK_VOLUME",
    "MalformedInstructionError",
    "PINNED_IDL_REVISION",
    "PUMP_PROGRAM_ID",
    "PUMPSWAP_PROGRAM_ID",
    "UnknownInstructionError",
    "UnsupportedEncodingError",
    "decode_base58",
    "decode_instruction_intent",
    "encode_base58",
]
