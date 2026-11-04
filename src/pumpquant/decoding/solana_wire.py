"""Strict, dependency-free decoding of Solana transaction wire values.

The block and transaction archives keep Solana transactions in the JSON-RPC
``[base64, "base64"]`` representation.  This module decodes
only the parts that are needed to join execution metadata back to accounts:
the first signature and the ordered static account keys in a legacy or v0
message.  It does not attempt to interpret instructions or address-table
contents.  Every length is bounded by the input bytes and Solana compact-u16
values are required to use their canonical shortvec representation.

``map_token_balances`` combines those static keys with the loaded addresses in
``meta`` (writable addresses first, then readonly addresses) and resolves RPC
token-balance account indices to ``(mint, decimals)`` values keyed by account
pubkey.  Conflicting metadata for one account is rejected rather than silently
normalised.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Final, NamedTuple, cast


class SolanaWireError(ValueError):
    """Base class for malformed or unsupported Solana wire values."""


class MalformedTransactionError(SolanaWireError):
    """A serialised transaction is malformed or has trailing bytes."""


class UnsupportedTransactionVersionError(SolanaWireError):
    """The transaction uses a message version other than legacy or v0."""


class MalformedTokenMetadataError(SolanaWireError):
    """RPC token metadata cannot be joined safely to account keys."""


BASE58_ALPHABET: Final[str] = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_VALUES: Final[dict[str, int]] = {
    character: index for index, character in enumerate(BASE58_ALPHABET)
}
_PUBKEY_BYTES: Final[int] = 32
_SIGNATURE_BYTES: Final[int] = 64
_MAX_ACCOUNT_KEYS: Final[int] = 256
_MAX_SIGNATURES: Final[int] = 255


class TokenBalance(NamedTuple):
    """Mint and token precision for one token account.

    A named tuple keeps the result convenient for both attribute access
    (``value.mint`` and ``value.decimals``) and callers that expect a simple
    ``(mint, decimals)`` pair.
    """

    mint: str
    decimals: int


class _Cursor:
    """Bounds-checked reader for one serialised transaction."""

    __slots__ = ("payload", "offset")

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0

    @property
    def remaining(self) -> int:
        return len(self.payload) - self.offset

    def read(self, count: int, label: str) -> bytes:
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise MalformedTransactionError(f"{label} length is invalid")
        end = self.offset + count
        if end > len(self.payload):
            raise MalformedTransactionError(f"serialized transaction is truncated at {label}")
        value = self.payload[self.offset : end]
        self.offset = end
        return value

    def read_u8(self, label: str) -> int:
        return self.read(1, label)[0]

    def read_shortvec(self, label: str) -> int:
        """Read Solana's canonical compact-u16 (little-endian base-128)."""

        value = 0
        shift = 0
        # A u16 has at most three seven-bit groups.  In the third group only
        # the low two bits are legal and its continuation bit must be clear.
        for group in range(3):
            byte = self.read_u8(f"{label} shortvec")
            part = byte & 0x7F
            if group == 2 and (byte & 0x80 or part > 0x03):
                raise MalformedTransactionError(f"{label} shortvec exceeds u16")
            value |= part << shift
            if byte & 0x80 == 0:
                if group > 0 and value < (1 << (7 * group)):
                    raise MalformedTransactionError(f"{label} shortvec is noncanonical")
                return value
            shift += 7
        # The third group always returns or raises above.  This is defensive
        # in case the loop is edited without updating its terminal condition.
        raise MalformedTransactionError(f"{label} shortvec is unterminated")


def _base58_encode(value: bytes) -> str:
    if not isinstance(value, bytes):
        raise TypeError("base58 input must be bytes")
    if not value:
        return ""
    number = int.from_bytes(value, "big")
    digits: list[str] = []
    while number:
        number, remainder = divmod(number, 58)
        digits.append(BASE58_ALPHABET[remainder])
    leading = len(value) - len(value.lstrip(b"\x00"))
    return "1" * leading + "".join(reversed(digits))


def _base58_pubkey(value: object, label: str) -> str:
    """Validate canonical base58 text encoding exactly one 32-byte pubkey."""

    if not isinstance(value, str) or not value:
        raise MalformedTokenMetadataError(f"{label} must be non-empty base58 text")
    # A 32-byte value has a canonical base58 representation between 32 and 44
    # characters (inclusive).  Rejecting outside that range before converting
    # to an integer also prevents an untrusted metadata string from triggering
    # unbounded work.
    if not 32 <= len(value) <= 44:
        raise MalformedTokenMetadataError(f"{label} must encode exactly 32 bytes")
    number = 0
    for character in value:
        digit = _BASE58_VALUES.get(character)
        if digit is None:
            raise MalformedTokenMetadataError(f"{label} is not canonical base58")
        number = number * 58 + digit
    leading = len(value) - len(value.lstrip("1"))
    if number:
        magnitude = (number.bit_length() + 7) // 8
        decoded = number.to_bytes(magnitude, "big")
    else:
        decoded = b""
    decoded = b"\x00" * leading + decoded
    if len(decoded) != _PUBKEY_BYTES or _base58_encode(decoded) != value:
        raise MalformedTokenMetadataError(f"{label} must encode exactly 32 bytes")
    return value


def _decode_transaction_base64(value: object) -> bytes:
    if not isinstance(value, str):
        raise TypeError("serialized transaction must be a base64 string")
    if not value:
        raise MalformedTransactionError("serialized transaction base64 is empty")
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, TypeError, binascii.Error) as error:
        raise MalformedTransactionError("serialized transaction base64 is invalid") from error
    try:
        canonical = base64.b64encode(decoded).decode("ascii")
    except (TypeError, UnicodeError) as error:  # pragma: no cover - bytes are guaranteed above
        raise MalformedTransactionError("serialized transaction base64 is invalid") from error
    if canonical != value:
        raise MalformedTransactionError("serialized transaction base64 is not canonical")
    return decoded


def _read_account_indexes(
    cursor: _Cursor,
    count: int,
    *,
    label: str,
    static_count: int,
) -> tuple[int, ...]:
    if count > _MAX_ACCOUNT_KEYS:
        raise MalformedTransactionError(f"{label} count exceeds u8 account-index bounds")
    indexes = tuple(cursor.read_u8(f"{label} index") for _ in range(count))
    # v0 instructions can refer to loaded accounts.  Their upper bound is
    # checked after address-table lookup entries have been decoded; legacy
    # instructions are checked immediately by the caller.
    if static_count == 0:
        raise MalformedTransactionError("message has no static account keys")
    return indexes


def _parse_serialized(value: str) -> tuple[str, tuple[str, ...]]:
    payload = _decode_transaction_base64(value)
    cursor = _Cursor(payload)

    signature_count = cursor.read_shortvec("signature count")
    if signature_count < 1 or signature_count > _MAX_SIGNATURES:
        raise MalformedTransactionError("signature count is outside Solana transaction bounds")
    signatures = cursor.read(signature_count * _SIGNATURE_BYTES, "signatures")

    first_message_byte = cursor.read_u8("message version/header")
    if first_message_byte & 0x80:
        version = first_message_byte & 0x7F
        if version != 0:
            raise UnsupportedTransactionVersionError(
                f"unsupported Solana message version {version}"
            )
        versioned = True
        num_required_signatures = cursor.read_u8("required signature count")
    else:
        version = None
        versioned = False
        num_required_signatures = first_message_byte
    num_readonly_signed = cursor.read_u8("readonly signed account count")
    num_readonly_unsigned = cursor.read_u8("readonly unsigned account count")

    if num_required_signatures != signature_count:
        raise MalformedTransactionError(
            "signature count does not match message required signature count"
        )
    if num_required_signatures == 0:
        raise MalformedTransactionError("message requires at least one signature")
    if num_readonly_signed > num_required_signatures:
        raise MalformedTransactionError("readonly signed account count exceeds required signatures")

    static_count = cursor.read_shortvec("static account-key count")
    if static_count < num_required_signatures or static_count > _MAX_ACCOUNT_KEYS:
        raise MalformedTransactionError("static account-key count is outside message bounds")
    if num_readonly_unsigned > static_count - num_required_signatures:
        raise MalformedTransactionError(
            "readonly unsigned account count exceeds unsigned static accounts"
        )

    raw_keys = tuple(
        cursor.read(_PUBKEY_BYTES, f"static account key {index}") for index in range(static_count)
    )
    static_keys = tuple(_base58_encode(raw_key) for raw_key in raw_keys)
    cursor.read(_PUBKEY_BYTES, "recent blockhash")

    instruction_count = cursor.read_shortvec("instruction count")
    instruction_indexes: list[int] = []
    for instruction_index in range(instruction_count):
        instruction_indexes.append(cursor.read_u8(f"instruction {instruction_index} program index"))
        account_count = cursor.read_shortvec(f"instruction {instruction_index} account count")
        instruction_indexes.extend(
            _read_account_indexes(
                cursor,
                account_count,
                label=f"instruction {instruction_index} account",
                static_count=static_count,
            )
        )
        data_count = cursor.read_shortvec(f"instruction {instruction_index} data length")
        cursor.read(data_count, f"instruction {instruction_index} data")

    loaded_count = 0
    if versioned:
        lookup_count = cursor.read_shortvec("address-table lookup count")
        for lookup_index in range(lookup_count):
            cursor.read(_PUBKEY_BYTES, f"address-table lookup {lookup_index} account")
            writable_count = cursor.read_shortvec(
                f"address-table lookup {lookup_index} writable index count"
            )
            writable_indexes = cursor.read(
                writable_count,
                f"address-table lookup {lookup_index} writable indexes",
            )
            readonly_count = cursor.read_shortvec(
                f"address-table lookup {lookup_index} readonly index count"
            )
            readonly_indexes = cursor.read(
                readonly_count,
                f"address-table lookup {lookup_index} readonly indexes",
            )
            loaded_count += len(writable_indexes) + len(readonly_indexes)
            if static_count + loaded_count > _MAX_ACCOUNT_KEYS:
                raise MalformedTransactionError("total account keys exceed u8 index bounds")

    total_accounts = static_count + loaded_count
    if any(index >= total_accounts for index in instruction_indexes):
        raise MalformedTransactionError("instruction account index is outside account-key bounds")
    if cursor.remaining:
        raise MalformedTransactionError("serialized transaction has trailing bytes")

    first_signature = _base58_encode(signatures[:_SIGNATURE_BYTES])
    return first_signature, static_keys


def parse_serialized_transaction(value: str) -> tuple[str, tuple[str, ...]]:
    """Return ``(first_signature, static_account_keys)`` from base64 wire text.

    ``value`` must be canonical standard base64 containing one complete legacy
    or v0 transaction.  Unsupported versions, malformed compact-u16 values,
    invalid account references, and trailing bytes raise a
    :class:`SolanaWireError` subclass.
    """

    return _parse_serialized(value)


def parse_transaction_record(value: Mapping[str, object]) -> tuple[str, tuple[str, ...]]:
    """Decode the ``transaction: [base64, "base64"]`` RPC record field.

    The wire parser itself accepts only the serialised base64
    string.  This small adapter keeps JSON-RPC shape validation at the edge for
    callers walking ``getBlock``/``getTransaction`` records.
    """

    record = _mapping_text_keys(value, "transaction record")
    encoded = record.get("transaction")
    if not isinstance(encoded, Sequence) or isinstance(encoded, (str, bytes, bytearray)):
        raise TypeError("transaction record field must be a base64 tuple")
    if len(encoded) != 2 or encoded[1] != "base64" or not isinstance(encoded[0], str):
        raise MalformedTransactionError("transaction record is not a base64 tuple")
    return parse_serialized_transaction(encoded[0])


# Names used by callers that describe the same wire operation more explicitly.


def _account_key_sequence(value: object, label: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise TypeError(f"{label} must be a sequence of base58 pubkeys")
    keys: list[str] = []
    for index, key in enumerate(value):
        try:
            keys.append(_base58_pubkey(key, f"{label}[{index}]"))
        except MalformedTokenMetadataError as error:
            raise MalformedTokenMetadataError(str(error)) from error
    if not keys:
        raise MalformedTokenMetadataError(f"{label} must not be empty")
    return tuple(keys)


def _mapping_text_keys(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise TypeError(f"{label} must be a mapping with text keys")
    return cast(Mapping[str, object], value)


def append_loaded_addresses(
    static_account_keys: Sequence[str], meta: Mapping[str, object]
) -> tuple[str, ...]:
    """Append ``loadedAddresses.writable`` then ``readonly`` to static keys."""

    keys = _account_key_sequence(static_account_keys, "static_account_keys")
    meta = _mapping_text_keys(meta, "meta")
    if "loadedAddresses" not in meta:
        return keys
    raw_loaded = meta["loadedAddresses"]
    try:
        loaded = _mapping_text_keys(raw_loaded, "meta.loadedAddresses")
    except TypeError as error:
        raise MalformedTokenMetadataError("meta.loadedAddresses must be an object") from error
    if "writable" not in loaded or "readonly" not in loaded:
        raise MalformedTokenMetadataError(
            "meta.loadedAddresses must contain writable and readonly arrays"
        )
    writable = loaded["writable"]
    readonly = loaded["readonly"]
    if (
        isinstance(writable, (str, bytes, bytearray))
        or not isinstance(writable, Sequence)
        or isinstance(readonly, (str, bytes, bytearray))
        or not isinstance(readonly, Sequence)
    ):
        raise MalformedTokenMetadataError(
            "meta.loadedAddresses writable and readonly must be arrays"
        )
    loaded_writable = (
        _account_key_sequence(writable, "loadedAddresses.writable") if writable else ()
    )
    loaded_readonly = (
        _account_key_sequence(readonly, "loadedAddresses.readonly") if readonly else ()
    )
    if len(keys) + len(loaded_writable) + len(loaded_readonly) > _MAX_ACCOUNT_KEYS:
        raise MalformedTokenMetadataError("loaded account keys exceed u8 index bounds")
    return keys + loaded_writable + loaded_readonly


def append_loaded_addresses_from_meta(
    meta: Mapping[str, object], static_account_keys: Sequence[str]
) -> tuple[str, ...]:
    """Argument-order variant of :func:`append_loaded_addresses`."""

    return append_loaded_addresses(static_account_keys, meta)


class TokenBalanceIndex(Mapping[str, TokenBalance]):
    """Token metadata indexed by account pubkey.

    ``pre`` and ``post`` preserve the source side of each RPC balance list;
    the mapping interface and ``by_pubkey`` expose their verified union.  The
    optional index maps are useful when a caller needs to retain account-index
    provenance while still normalising amounts by pubkey.
    """

    __slots__ = (
        "account_keys",
        "pre",
        "post",
        "by_pubkey",
        "pre_by_index",
        "post_by_index",
    )

    account_keys: tuple[str, ...]
    pre: Mapping[str, TokenBalance]
    post: Mapping[str, TokenBalance]
    by_pubkey: Mapping[str, TokenBalance]
    pre_by_index: Mapping[int, TokenBalance]
    post_by_index: Mapping[int, TokenBalance]

    def __init__(
        self,
        account_keys: tuple[str, ...],
        pre: Mapping[str, TokenBalance],
        post: Mapping[str, TokenBalance],
        by_pubkey: Mapping[str, TokenBalance],
        pre_by_index: Mapping[int, TokenBalance],
        post_by_index: Mapping[int, TokenBalance],
    ) -> None:
        self.account_keys = account_keys
        self.pre = MappingProxyType(dict(pre))
        self.post = MappingProxyType(dict(post))
        self.by_pubkey = MappingProxyType(dict(by_pubkey))
        self.pre_by_index = MappingProxyType(dict(pre_by_index))
        self.post_by_index = MappingProxyType(dict(post_by_index))

    def __getitem__(self, key: str) -> TokenBalance:
        return self.by_pubkey[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.by_pubkey)

    def __len__(self) -> int:
        return len(self.by_pubkey)


def _balance_entries(value: object, label: str) -> tuple[Mapping[str, object], ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise MalformedTokenMetadataError(f"{label} must be an array or null")
    entries: list[Mapping[str, object]] = []
    for index, entry in enumerate(value):
        if not isinstance(entry, Mapping) or any(not isinstance(key, str) for key in entry):
            raise MalformedTokenMetadataError(f"{label}[{index}] must be an object")
        entries.append(cast(Mapping[str, object], entry))
    return tuple(entries)


def _parse_balance_side(
    entries: Sequence[Mapping[str, object]],
    account_keys: tuple[str, ...],
    label: str,
) -> tuple[dict[str, TokenBalance], dict[int, TokenBalance]]:
    by_pubkey: dict[str, TokenBalance] = {}
    by_index: dict[int, TokenBalance] = {}
    for position, entry in enumerate(entries):
        account_index = entry.get("accountIndex")
        if (
            isinstance(account_index, bool)
            or not isinstance(account_index, int)
            or account_index < 0
            or account_index >= len(account_keys)
        ):
            raise MalformedTokenMetadataError(
                f"{label}[{position}].accountIndex is outside account-key bounds"
            )
        mint = _base58_pubkey(entry.get("mint"), f"{label}[{position}].mint")
        ui_token_amount = entry.get("uiTokenAmount")
        if not isinstance(ui_token_amount, Mapping) or any(
            not isinstance(key, str) for key in ui_token_amount
        ):
            raise MalformedTokenMetadataError(
                f"{label}[{position}].uiTokenAmount must be an object"
            )
        decimals = ui_token_amount.get("decimals")
        if isinstance(decimals, bool) or not isinstance(decimals, int) or not 0 <= decimals <= 255:
            raise MalformedTokenMetadataError(
                f"{label}[{position}].uiTokenAmount.decimals must be a u8"
            )
        balance = TokenBalance(mint, decimals)
        previous_index = by_index.get(account_index)
        if previous_index is not None and previous_index != balance:
            raise MalformedTokenMetadataError(
                f"{label} contains conflicting metadata for account index {account_index}"
            )
        by_index[account_index] = balance
        pubkey = account_keys[account_index]
        previous_pubkey = by_pubkey.get(pubkey)
        if previous_pubkey is not None and previous_pubkey != balance:
            raise MalformedTokenMetadataError(
                f"{label} contains conflicting metadata for account {pubkey}"
            )
        by_pubkey[pubkey] = balance
    return by_pubkey, by_index


def map_token_balances(
    static_account_keys: Sequence[str], meta: Mapping[str, object]
) -> TokenBalanceIndex:
    """Resolve pre/post token balances to ``pubkey -> (mint, decimals)``.

    Static keys are followed by loaded writable keys and then loaded readonly
    keys.  A token account appearing on both sides must retain the same mint
    and decimal precision; conflicting metadata raises
    :class:`MalformedTokenMetadataError`.
    """

    meta = _mapping_text_keys(meta, "meta")
    account_keys = append_loaded_addresses(static_account_keys, meta)
    pre_entries = _balance_entries(meta.get("preTokenBalances"), "preTokenBalances")
    post_entries = _balance_entries(meta.get("postTokenBalances"), "postTokenBalances")
    pre, pre_by_index = _parse_balance_side(pre_entries, account_keys, "preTokenBalances")
    post, post_by_index = _parse_balance_side(post_entries, account_keys, "postTokenBalances")

    by_pubkey = dict(pre)
    for pubkey, balance in post.items():
        previous = by_pubkey.get(pubkey)
        if previous is not None and previous != balance:
            raise MalformedTokenMetadataError(
                f"pre/post token metadata differs for account {pubkey}"
            )
        by_pubkey[pubkey] = balance
    for index, balance in post_by_index.items():
        previous = pre_by_index.get(index)
        if previous is not None and previous != balance:
            raise MalformedTokenMetadataError(
                f"pre/post token metadata differs for account index {index}"
            )

    return TokenBalanceIndex(
        account_keys,
        pre,
        post,
        by_pubkey,
        pre_by_index,
        post_by_index,
    )


def map_token_balances_from_meta(
    meta: Mapping[str, object], static_account_keys: Sequence[str]
) -> TokenBalanceIndex:
    """Argument-order variant of :func:`map_token_balances` for RPC callers."""

    return map_token_balances(static_account_keys, meta)


__all__ = [
    "BASE58_ALPHABET",
    "MalformedTokenMetadataError",
    "MalformedTransactionError",
    "SolanaWireError",
    "TokenBalance",
    "TokenBalanceIndex",
    "UnsupportedTransactionVersionError",
    "append_loaded_addresses",
    "append_loaded_addresses_from_meta",
    "map_token_balances",
    "map_token_balances_from_meta",
    "parse_serialized_transaction",
    "parse_transaction_record",
]
