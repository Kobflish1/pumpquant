"""Pinned, candidate-only account-position mapping for Pump instructions.

The instruction decoder in :mod:`pumpquant.decoding.instruction_intent`
stops at instruction arguments.  This module adds one similarly narrow step:
it labels the account keys supplied with a decoded instruction according to
the account arrays in the two official Pump IDLs at one pinned repository
revision.  It does *not* inspect account data or transaction metadata and it
does not claim that an instruction executed.

Only the core instructions exposed by ``instruction_intent`` are listed.  A
layout is selected by all of the following values: program address,
instruction discriminator, instruction name, and the pinned layout/IDL
revision.  In particular, Pump and PumpSwap retain separate
entries for their colliding ``buy`` and ``sell`` discriminators.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, TypeAlias

from pumpquant.decoding.instruction_intent import (
    PINNED_IDL_REVISION,
    PUMP_PROGRAM_ID,
    PUMPSWAP_PROGRAM_ID,
    CandidateIntent,
    decode_base58,
)

# This is the only protocol source used by this module.  Keep the path in the
# identifier so a future IDL update cannot be mistaken for the table below.
PINNED_IDL_SOURCE_REPOSITORY: Final[str] = "github.com/pump-fun/pump-public-docs"
PUMP_IDL_SOURCE: Final[str] = f"{PINNED_IDL_SOURCE_REPOSITORY}@{PINNED_IDL_REVISION}:idl/pump.json"
PUMPSWAP_IDL_SOURCE: Final[str] = (
    f"{PINNED_IDL_SOURCE_REPOSITORY}@{PINNED_IDL_REVISION}:idl/pump_amm.json"
)

# The account layouts in the pinned IDLs are tied to the same revision label
# used by CandidateIntent.  Historical argument layouts are not
# accepted here: account positions may only be labelled when the candidate
# identifies the pinned wire layout exactly.

Pubkey: TypeAlias = bytes | str
RawAccountPubkeys: TypeAlias = Iterable[object]


class InstructionAccountsError(ValueError):
    """Base class for fail-closed account mapping failures."""


class UnknownAccountLayoutError(InstructionAccountsError):
    """The candidate does not identify a pinned account layout."""


class TooFewAccountsError(InstructionAccountsError):
    """The supplied account list is shorter than the pinned layout."""


class MalformedAccountPubkeyError(InstructionAccountsError):
    """A supplied account key is not a canonical 32-byte public key."""


class AccountListTypeError(InstructionAccountsError, TypeError):
    """The account list or one of its values has an unsupported type."""


@dataclass(frozen=True, slots=True)
class InstructionAccountSpec:
    """One account array copied from a pinned official IDL instruction."""

    program_id: str
    instruction_name: str
    discriminator: bytes
    idl_names: tuple[str, ...]
    idl_revision: str
    source_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.program_id, str) or not self.program_id:
            raise ValueError("account spec program_id must be non-empty text")
        if not isinstance(self.instruction_name, str) or not self.instruction_name:
            raise ValueError("account spec instruction_name must be non-empty text")
        if not isinstance(self.discriminator, bytes) or len(self.discriminator) != 8:
            raise ValueError("account spec discriminator must be eight bytes")
        if not isinstance(self.idl_names, tuple) or not self.idl_names:
            raise ValueError("account spec idl_names must be a non-empty tuple")
        if any(not isinstance(name, str) or not name for name in self.idl_names):
            raise ValueError("account spec names must be non-empty text")
        if len(set(self.idl_names)) != len(self.idl_names):
            raise ValueError("account spec names must be unique")
        if self.idl_revision != PINNED_IDL_REVISION:
            raise ValueError("only the pinned IDL revision is supported")
        if not isinstance(self.source_id, str) or not self.source_id:
            raise ValueError("account spec source_id must be non-empty text")

    @property
    def account_names(self) -> tuple[str, ...]:
        """Alias for the ordered account names from the IDL."""

        return self.idl_names

    @property
    def layout_revision(self) -> str:
        """The pinned wire-layout revision for this account array."""

        return self.idl_revision


# Names are flattened in the exact order of each Anchor IDL ``accounts``
# array.  The pinned JSON contains no optional account groups for these core
# instructions, so every name below corresponds to one positional pubkey.
_PUMP_ACCOUNT_NAMES: dict[str, tuple[str, ...]] = {
    "create": (
        "mint",
        "mint_authority",
        "bonding_curve",
        "associated_bonding_curve",
        "global",
        "mpl_token_metadata",
        "metadata",
        "user",
        "system_program",
        "token_program",
        "associated_token_program",
        "rent",
        "event_authority",
        "program",
    ),
    "create_v2": (
        "mint",
        "mint_authority",
        "bonding_curve",
        "associated_bonding_curve",
        "global",
        "user",
        "system_program",
        "token_program",
        "associated_token_program",
        "mayhem_program_id",
        "global_params",
        "sol_vault",
        "mayhem_state",
        "mayhem_token_vault",
        "event_authority",
        "program",
    ),
    "buy": (
        "global",
        "fee_recipient",
        "mint",
        "bonding_curve",
        "associated_bonding_curve",
        "associated_user",
        "user",
        "system_program",
        "token_program",
        "creator_vault",
        "event_authority",
        "program",
        "global_volume_accumulator",
        "user_volume_accumulator",
        "fee_config",
        "fee_program",
    ),
    "buy_v2": (
        "global",
        "base_mint",
        "quote_mint",
        "base_token_program",
        "quote_token_program",
        "associated_token_program",
        "fee_recipient",
        "associated_quote_fee_recipient",
        "buyback_fee_recipient",
        "associated_quote_buyback_fee_recipient",
        "bonding_curve",
        "associated_base_bonding_curve",
        "associated_quote_bonding_curve",
        "user",
        "associated_base_user",
        "associated_quote_user",
        "creator_vault",
        "associated_creator_vault",
        "sharing_config",
        "global_volume_accumulator",
        "user_volume_accumulator",
        "associated_user_volume_accumulator",
        "fee_config",
        "fee_program",
        "system_program",
        "event_authority",
        "program",
    ),
    "buy_exact_sol_in": (
        "global",
        "fee_recipient",
        "mint",
        "bonding_curve",
        "associated_bonding_curve",
        "associated_user",
        "user",
        "system_program",
        "token_program",
        "creator_vault",
        "event_authority",
        "program",
        "global_volume_accumulator",
        "user_volume_accumulator",
        "fee_config",
        "fee_program",
    ),
    "buy_exact_quote_in_v2": (
        "global",
        "base_mint",
        "quote_mint",
        "base_token_program",
        "quote_token_program",
        "associated_token_program",
        "fee_recipient",
        "associated_quote_fee_recipient",
        "buyback_fee_recipient",
        "associated_quote_buyback_fee_recipient",
        "bonding_curve",
        "associated_base_bonding_curve",
        "associated_quote_bonding_curve",
        "user",
        "associated_base_user",
        "associated_quote_user",
        "creator_vault",
        "associated_creator_vault",
        "sharing_config",
        "global_volume_accumulator",
        "user_volume_accumulator",
        "associated_user_volume_accumulator",
        "fee_config",
        "fee_program",
        "system_program",
        "event_authority",
        "program",
    ),
    "sell": (
        "global",
        "fee_recipient",
        "mint",
        "bonding_curve",
        "associated_bonding_curve",
        "associated_user",
        "user",
        "system_program",
        "creator_vault",
        "token_program",
        "event_authority",
        "program",
        "fee_config",
        "fee_program",
    ),
    "sell_v2": (
        "global",
        "base_mint",
        "quote_mint",
        "base_token_program",
        "quote_token_program",
        "associated_token_program",
        "fee_recipient",
        "associated_quote_fee_recipient",
        "buyback_fee_recipient",
        "associated_quote_buyback_fee_recipient",
        "bonding_curve",
        "associated_base_bonding_curve",
        "associated_quote_bonding_curve",
        "user",
        "associated_base_user",
        "associated_quote_user",
        "creator_vault",
        "associated_creator_vault",
        "sharing_config",
        "user_volume_accumulator",
        "associated_user_volume_accumulator",
        "fee_config",
        "fee_program",
        "system_program",
        "event_authority",
        "program",
    ),
    "migrate": (
        "global",
        "withdraw_authority",
        "mint",
        "bonding_curve",
        "associated_bonding_curve",
        "user",
        "system_program",
        "token_program",
        "pump_amm",
        "pool",
        "pool_authority",
        "pool_authority_mint_account",
        "pool_authority_wsol_account",
        "amm_global_config",
        "wsol_mint",
        "lp_mint",
        "user_pool_token_account",
        "pool_base_token_account",
        "pool_quote_token_account",
        "token_2022_program",
        "associated_token_program",
        "pump_amm_event_authority",
        "event_authority",
        "program",
        "rent",
    ),
    "migrate_v2": (
        "global",
        "withdraw_authority",
        "base_mint",
        "quote_mint",
        "bonding_curve",
        "associated_base_bonding_curve",
        "associated_quote_bonding_curve",
        "user",
        "system_program",
        "pump_amm",
        "pool",
        "pool_authority",
        "pool_authority_mint_account",
        "pool_authority_quote_account",
        "amm_global_config",
        "lp_mint",
        "user_pool_token_account",
        "pool_base_token_account",
        "pool_quote_token_account",
        "base_token_program",
        "quote_token_program",
        "token_2022_program",
        "associated_token_program",
        "pump_amm_event_authority",
        "rent",
        "event_authority",
        "program",
    ),
}

_PUMPSWAP_ACCOUNT_NAMES: dict[str, tuple[str, ...]] = {
    "create_pool": (
        "pool",
        "global_config",
        "creator",
        "base_mint",
        "quote_mint",
        "lp_mint",
        "user_base_token_account",
        "user_quote_token_account",
        "user_pool_token_account",
        "pool_base_token_account",
        "pool_quote_token_account",
        "system_program",
        "token_2022_program",
        "base_token_program",
        "quote_token_program",
        "associated_token_program",
        "event_authority",
        "program",
    ),
    "buy": (
        "pool",
        "user",
        "global_config",
        "base_mint",
        "quote_mint",
        "user_base_token_account",
        "user_quote_token_account",
        "pool_base_token_account",
        "pool_quote_token_account",
        "protocol_fee_recipient",
        "protocol_fee_recipient_token_account",
        "base_token_program",
        "quote_token_program",
        "system_program",
        "associated_token_program",
        "event_authority",
        "program",
        "coin_creator_vault_ata",
        "coin_creator_vault_authority",
        "global_volume_accumulator",
        "user_volume_accumulator",
        "fee_config",
        "fee_program",
    ),
    "buy_exact_quote_in": (
        "pool",
        "user",
        "global_config",
        "base_mint",
        "quote_mint",
        "user_base_token_account",
        "user_quote_token_account",
        "pool_base_token_account",
        "pool_quote_token_account",
        "protocol_fee_recipient",
        "protocol_fee_recipient_token_account",
        "base_token_program",
        "quote_token_program",
        "system_program",
        "associated_token_program",
        "event_authority",
        "program",
        "coin_creator_vault_ata",
        "coin_creator_vault_authority",
        "global_volume_accumulator",
        "user_volume_accumulator",
        "fee_config",
        "fee_program",
    ),
    "sell": (
        "pool",
        "user",
        "global_config",
        "base_mint",
        "quote_mint",
        "user_base_token_account",
        "user_quote_token_account",
        "pool_base_token_account",
        "pool_quote_token_account",
        "protocol_fee_recipient",
        "protocol_fee_recipient_token_account",
        "base_token_program",
        "quote_token_program",
        "system_program",
        "associated_token_program",
        "event_authority",
        "program",
        "coin_creator_vault_ata",
        "coin_creator_vault_authority",
        "fee_config",
        "fee_program",
    ),
    "deposit": (
        "pool",
        "global_config",
        "user",
        "base_mint",
        "quote_mint",
        "lp_mint",
        "user_base_token_account",
        "user_quote_token_account",
        "user_pool_token_account",
        "pool_base_token_account",
        "pool_quote_token_account",
        "token_program",
        "token_2022_program",
        "event_authority",
        "program",
    ),
    "withdraw": (
        "pool",
        "global_config",
        "user",
        "base_mint",
        "quote_mint",
        "lp_mint",
        "user_base_token_account",
        "user_quote_token_account",
        "user_pool_token_account",
        "pool_base_token_account",
        "pool_quote_token_account",
        "token_program",
        "token_2022_program",
        "event_authority",
        "program",
    ),
}


# Account discriminators are written out here rather than
# derived from instruction names.  They are the same bytes used by the pinned
# instruction decoder and protect the Pump/PumpSwap collision case.
_PUMP_DISCRIMINATORS: dict[str, bytes] = {
    "create": bytes((24, 30, 200, 40, 5, 28, 7, 119)),
    "create_v2": bytes((214, 144, 76, 236, 95, 139, 49, 180)),
    "buy": bytes((102, 6, 61, 18, 1, 218, 235, 234)),
    "buy_v2": bytes((184, 23, 238, 97, 103, 197, 211, 61)),
    "buy_exact_sol_in": bytes((56, 252, 116, 8, 158, 223, 205, 95)),
    "buy_exact_quote_in_v2": bytes((194, 171, 28, 70, 104, 77, 91, 47)),
    "sell": bytes((51, 230, 133, 164, 1, 127, 131, 173)),
    "sell_v2": bytes((93, 246, 130, 60, 231, 233, 64, 178)),
    "migrate": bytes((155, 234, 231, 146, 236, 158, 162, 30)),
    "migrate_v2": bytes((187, 203, 18, 31, 206, 237, 254, 41)),
}
_PUMPSWAP_DISCRIMINATORS: dict[str, bytes] = {
    "create_pool": bytes((233, 146, 209, 142, 207, 104, 64, 188)),
    "buy": bytes((102, 6, 61, 18, 1, 218, 235, 234)),
    "buy_exact_quote_in": bytes((198, 46, 21, 82, 180, 217, 232, 112)),
    "sell": bytes((51, 230, 133, 164, 1, 127, 131, 173)),
    "deposit": bytes((242, 35, 198, 137, 82, 225, 242, 182)),
    "withdraw": bytes((183, 18, 70, 156, 148, 109, 161, 34)),
}


def _build_specs(
    program_id: str,
    source_id: str,
    account_names: Mapping[str, tuple[str, ...]],
    discriminators: Mapping[str, bytes],
) -> tuple[InstructionAccountSpec, ...]:
    specs: list[InstructionAccountSpec] = []
    for name, names in account_names.items():
        specs.append(
            InstructionAccountSpec(
                program_id=program_id,
                instruction_name=name,
                discriminator=discriminators[name],
                idl_names=names,
                idl_revision=PINNED_IDL_REVISION,
                source_id=source_id,
            )
        )
    return tuple(specs)


_SPEC_LIST: tuple[InstructionAccountSpec, ...] = _build_specs(
    PUMP_PROGRAM_ID, PUMP_IDL_SOURCE, _PUMP_ACCOUNT_NAMES, _PUMP_DISCRIMINATORS
) + _build_specs(
    PUMPSWAP_PROGRAM_ID,
    PUMPSWAP_IDL_SOURCE,
    _PUMPSWAP_ACCOUNT_NAMES,
    _PUMPSWAP_DISCRIMINATORS,
)

# Include every selector component in the public map key.  This makes an
# accidental name/revision substitution fail closed instead of silently
# choosing an account list for a colliding discriminator.
AccountLayoutKey: TypeAlias = tuple[str, bytes, str, str]
_spec_map: dict[AccountLayoutKey, InstructionAccountSpec] = {
    (spec.program_id, spec.discriminator, spec.instruction_name, spec.idl_revision): spec
    for spec in _SPEC_LIST
}
INSTRUCTION_ACCOUNT_SPECS: Mapping[AccountLayoutKey, InstructionAccountSpec] = MappingProxyType(
    _spec_map
)


def _normalize_pubkey(value: object, index: int) -> Pubkey:
    """Validate one raw key while preserving its caller-supplied representation."""

    if isinstance(value, bytes):
        if len(value) != 32:
            raise MalformedAccountPubkeyError(
                f"account pubkey at position {index} must be exactly 32 bytes"
            )
        return value
    if isinstance(value, str):
        try:
            decoded = decode_base58(value)
        except (TypeError, ValueError) as exc:
            raise MalformedAccountPubkeyError(
                f"account pubkey at position {index} is not canonical base58"
            ) from exc
        if len(decoded) != 32:
            raise MalformedAccountPubkeyError(
                f"account pubkey at position {index} must decode to 32 bytes"
            )
        return value
    raise AccountListTypeError(
        f"account pubkey at position {index} must be bytes or canonical base58 text"
    )


@dataclass(frozen=True, slots=True)
class CandidateAccountMapping:
    """Labels for a candidate instruction's positional accounts.

    ``account_positions`` contains exactly the pinned IDL names in order.
    ``extras`` retains every trailing account in its original order; extras
    are not assigned protocol meaning.  Both tuples are provenance only.
    ``candidate_only`` and ``execution_success_unknown`` are fixed true values
    to prevent this record from being mistaken for an observed fill.
    """

    program_id: str
    instruction_name: str
    discriminator: bytes
    account_positions: tuple[tuple[str, Pubkey], ...]
    extras: tuple[Pubkey, ...]
    layout_revision: str
    idl_revision: str
    source_id: str
    candidate_only: bool = True
    execution_success_unknown: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.program_id, str) or not self.program_id:
            raise ValueError("mapping program_id must be non-empty text")
        if not isinstance(self.instruction_name, str) or not self.instruction_name:
            raise ValueError("mapping instruction_name must be non-empty text")
        if not isinstance(self.discriminator, bytes) or len(self.discriminator) != 8:
            raise ValueError("mapping discriminator must be eight bytes")
        if self.layout_revision != PINNED_IDL_REVISION:
            raise UnknownAccountLayoutError("mapping must use the pinned layout revision")
        if self.idl_revision != PINNED_IDL_REVISION:
            raise UnknownAccountLayoutError("mapping must use the pinned IDL revision")
        if not isinstance(self.source_id, str) or not self.source_id:
            raise ValueError("mapping source_id must be non-empty text")
        if self.candidate_only is not True or self.execution_success_unknown is not True:
            raise ValueError("account mappings are always candidate-only with unknown execution")
        if not isinstance(self.account_positions, tuple):
            raise TypeError("account_positions must be an immutable tuple")
        for index, pair in enumerate(self.account_positions):
            if not isinstance(pair, tuple) or len(pair) != 2:
                raise TypeError(f"account_positions entry {index} must be a two-member tuple")
            name, pubkey = pair
            if not isinstance(name, str) or not name:
                raise ValueError(f"account_positions name {index} must be non-empty text")
            _normalize_pubkey(pubkey, index)
        if not isinstance(self.extras, tuple):
            raise TypeError("extras must be an immutable tuple")
        for offset, pubkey in enumerate(self.extras):
            _normalize_pubkey(pubkey, len(self.account_positions) + offset)

    @property
    def accounts(self) -> tuple[tuple[str, Pubkey], ...]:
        """Alias for the ordered IDL account-position pairs."""

        return self.account_positions

    @property
    def ordered_accounts(self) -> tuple[tuple[str, Pubkey], ...]:
        """Alias emphasising that IDL order is preserved."""

        return self.account_positions

    @property
    def account_pairs(self) -> tuple[tuple[str, Pubkey], ...]:
        """Alias for the ordered ``(idl_name, pubkey)`` pairs."""

        return self.account_positions

    @property
    def ordered_pairs(self) -> tuple[tuple[str, Pubkey], ...]:
        """Alias for the ordered ``(idl_name, pubkey)`` pairs."""

        return self.account_positions

    @property
    def idl_accounts(self) -> tuple[tuple[str, Pubkey], ...]:
        """Alias for account pairs labelled by the IDL."""

        return self.account_positions

    @property
    def positions(self) -> tuple[tuple[str, Pubkey], ...]:
        """Alias for callers that use ``positions`` terminology."""

        return self.account_positions

    @property
    def retained_extras(self) -> tuple[Pubkey, ...]:
        """Trailing keys retained without assigning protocol semantics."""

        return self.extras

    @property
    def extra_accounts(self) -> tuple[Pubkey, ...]:
        """Alias for trailing keys retained without protocol semantics."""

        return self.extras

    @property
    def idl_source(self) -> str:
        """Pinned source identifier (repository, revision, and IDL path)."""

        return self.source_id

    @property
    def source_revision(self) -> str:
        """The pinned source revision used for this mapping."""

        return self.idl_revision

    @property
    def source_url(self) -> str:
        """Canonical GitHub URL for the pinned IDL source file."""

        path = "pump.json" if self.source_id == PUMP_IDL_SOURCE else "pump_amm.json"
        return f"https://github.com/pump-fun/pump-public-docs/blob/{self.idl_revision}/idl/{path}"

    @property
    def source(self) -> str:
        """Alias for :attr:`source_id`."""

        return self.source_id

    @property
    def revision(self) -> str:
        """Alias for the pinned account-layout revision."""

        return self.layout_revision

    @property
    def idl_names(self) -> tuple[str, ...]:
        """The ordered IDL names represented by :attr:`account_positions`."""

        return tuple(name for name, _ in self.account_positions)


def _layout_for(intent: CandidateIntent) -> InstructionAccountSpec:
    if not isinstance(intent, CandidateIntent):
        raise TypeError("intent must be a CandidateIntent")
    if intent.idl_revision != PINNED_IDL_REVISION:
        raise UnknownAccountLayoutError("candidate does not use the pinned IDL revision")
    if intent.layout_revision != PINNED_IDL_REVISION:
        raise UnknownAccountLayoutError("candidate does not use the pinned account layout")
    key = (intent.program_id, intent.discriminator, intent.instruction_name, intent.idl_revision)
    spec = INSTRUCTION_ACCOUNT_SPECS.get(key)
    if spec is None:
        raise UnknownAccountLayoutError(
            "no pinned account layout matches program, discriminator, instruction, and revision"
        )
    return spec


def map_candidate_accounts(
    intent: CandidateIntent,
    account_pubkeys: RawAccountPubkeys,
) -> CandidateAccountMapping:
    """Map raw account pubkeys to a pinned candidate instruction layout.

    The function validates every supplied key, rejects a list shorter than the
    layout, and keeps all trailing keys in ``extras``.  It never evaluates
    signer/writable flags, account contents, balances, transfers, logs, or
    transaction outcome.
    """

    spec = _layout_for(intent)
    if isinstance(account_pubkeys, (str, bytes, bytearray, Mapping)):
        raise AccountListTypeError("account_pubkeys must be an iterable of individual pubkeys")
    try:
        raw_values = tuple(account_pubkeys)
    except TypeError as exc:
        raise AccountListTypeError("account_pubkeys must be an iterable of pubkeys") from exc

    expected = len(spec.idl_names)
    if len(raw_values) < expected:
        raise TooFewAccountsError(
            f"{spec.instruction_name} requires at least {expected} accounts; got {len(raw_values)}"
        )
    values = tuple(_normalize_pubkey(value, index) for index, value in enumerate(raw_values))
    positions = tuple(zip(spec.idl_names, values[:expected], strict=True))
    return CandidateAccountMapping(
        program_id=spec.program_id,
        instruction_name=spec.instruction_name,
        discriminator=spec.discriminator,
        account_positions=positions,
        extras=values[expected:],
        layout_revision=spec.idl_revision,
        idl_revision=spec.idl_revision,
        source_id=spec.source_id,
    )


__all__ = [
    "AccountLayoutKey",
    "AccountListTypeError",
    "CandidateAccountMapping",
    "INSTRUCTION_ACCOUNT_SPECS",
    "InstructionAccountSpec",
    "InstructionAccountsError",
    "MalformedAccountPubkeyError",
    "PINNED_IDL_REVISION",
    "PINNED_IDL_SOURCE_REPOSITORY",
    "PUMP_IDL_SOURCE",
    "PUMPSWAP_IDL_SOURCE",
    "Pubkey",
    "RawAccountPubkeys",
    "TooFewAccountsError",
    "UnknownAccountLayoutError",
    "map_candidate_accounts",
]
