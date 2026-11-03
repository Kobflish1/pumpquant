from __future__ import annotations

import base64
from collections.abc import Callable

import pytest

from pumpquant.decoding.instruction_intent import encode_base58
from pumpquant.decoding.solana_wire import (
    MalformedTokenMetadataError,
    MalformedTransactionError,
    TokenBalance,
    UnsupportedTransactionVersionError,
    append_loaded_addresses,
    map_token_balances,
    parse_serialized_transaction,
    parse_transaction_record,
)


def _shortvec(value: int) -> bytes:
    result = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            result.append(byte | 0x80)
        else:
            result.append(byte)
            return bytes(result)


def _key(index: int) -> tuple[bytes, str]:
    raw = bytes([index]) * 32
    return raw, encode_base58(raw)


def _serialized(
    *,
    version: int | None = None,
    static_keys: tuple[bytes, ...] = (bytes([1]) * 32, bytes([2]) * 32),
    instructions: tuple[tuple[int, tuple[int, ...], bytes], ...] = (),
    lookups: tuple[tuple[bytes, tuple[int, ...], tuple[int, ...]], ...] = (),
    signature: bytes = bytes([9]) * 64,
) -> bytes:
    message_prefix = b"" if version is None else bytes([0x80 | version])
    message = bytearray(message_prefix)
    message.extend((1, 0, 0))
    message.extend(_shortvec(len(static_keys)))
    for key in static_keys:
        message.extend(key)
    message.extend(bytes([7]) * 32)
    message.extend(_shortvec(len(instructions)))
    for program_index, account_indexes, data in instructions:
        message.append(program_index)
        message.extend(_shortvec(len(account_indexes)))
        message.extend(account_indexes)
        message.extend(_shortvec(len(data)))
        message.extend(data)
    if version is not None:
        message.extend(_shortvec(len(lookups)))
        for lookup_key, writable, readonly in lookups:
            message.extend(lookup_key)
            message.extend(_shortvec(len(writable)))
            message.extend(writable)
            message.extend(_shortvec(len(readonly)))
            message.extend(readonly)
    return b"\x01" + signature + bytes(message)


def _encoded(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


def test_legacy_returns_first_signature_and_ordered_static_keys() -> None:
    first, first_text = _key(1)
    second, second_text = _key(2)
    signature = bytes([9]) * 64

    parsed_signature, keys = parse_serialized_transaction(
        _encoded(_serialized(static_keys=(first, second), signature=signature))
    )

    assert parsed_signature == encode_base58(signature)
    assert keys == (first_text, second_text)


def test_rpc_record_adapter_validates_base64_tuple() -> None:
    payload = _serialized()
    expected = parse_serialized_transaction(_encoded(payload))
    assert parse_transaction_record({"transaction": [_encoded(payload), "base64"]}) == expected
    with pytest.raises(MalformedTransactionError):
        parse_transaction_record({"transaction": [_encoded(payload), "base58"]})


def test_v0_allows_instruction_indices_into_loaded_addresses() -> None:
    first, first_text = _key(1)
    second, second_text = _key(2)
    lookup, _ = _key(3)
    payload = _serialized(
        version=0,
        static_keys=(first, second),
        instructions=((2, (2,), b"\x01"),),
        lookups=((lookup, (4,), (8,)),),
    )

    _signature, keys = parse_serialized_transaction(_encoded(payload))
    assert keys == (first_text, second_text)


@pytest.mark.parametrize(
    ("mutator", "error"),
    [
        (lambda payload: b"\x81\x00" + payload[1:], MalformedTransactionError),
        (lambda payload: payload + b"\x00", MalformedTransactionError),
        (lambda payload: payload[:65] + b"\x02" + payload[66:], MalformedTransactionError),
    ],
)
def test_malformed_wire_values_fail_closed(
    mutator: Callable[[bytes], bytes], error: type[ValueError]
) -> None:
    payload = _serialized()
    with pytest.raises(error):
        parse_serialized_transaction(_encoded(mutator(payload)))


def test_noncanonical_base64_and_unsupported_version_are_rejected() -> None:
    encoded = _encoded(_serialized())
    with pytest.raises(MalformedTransactionError):
        parse_serialized_transaction(encoded.rstrip("="))

    unsupported = _serialized(version=1)
    with pytest.raises(UnsupportedTransactionVersionError):
        parse_serialized_transaction(_encoded(unsupported))


def test_loaded_addresses_and_balances_are_mapped_with_decimals() -> None:
    static_one, static_text = _key(1)
    static_two, _ = _key(2)
    writable, writable_text = _key(3)
    readonly, readonly_text = _key(4)
    mint, mint_text = _key(5)
    meta: dict[str, object] = {
        "loadedAddresses": {
            "writable": [writable_text],
            "readonly": [readonly_text],
        },
        "preTokenBalances": [
            {
                "accountIndex": 2,
                "mint": mint_text,
                "uiTokenAmount": {"decimals": 6},
            }
        ],
        "postTokenBalances": [
            {
                "accountIndex": 2,
                "mint": mint_text,
                "uiTokenAmount": {"decimals": 6},
            },
            {
                "accountIndex": 3,
                "mint": mint_text,
                "uiTokenAmount": {"decimals": 6},
            },
        ],
    }

    assert append_loaded_addresses((static_text, encode_base58(static_two)), meta) == (
        static_text,
        encode_base58(static_two),
        writable_text,
        readonly_text,
    )
    balances = map_token_balances((static_text, encode_base58(static_two)), meta)
    assert balances.account_keys == (
        static_text,
        encode_base58(static_two),
        writable_text,
        readonly_text,
    )
    assert balances[writable_text] == TokenBalance(mint_text, 6)
    assert balances[readonly_text] == (mint_text, 6)
    assert balances.pre_by_index[2] == (mint_text, 6)
    assert balances.post_by_index[3] == (mint_text, 6)
    assert static_one != mint and static_text not in balances


@pytest.mark.parametrize(
    "change",
    [
        lambda mint_text: {"mint": mint_text, "uiTokenAmount": {"decimals": 7}},
        lambda mint_text: {
            "mint": encode_base58(bytes([6]) * 32),
            "uiTokenAmount": {"decimals": 6},
        },
    ],
)
def test_conflicting_pre_post_metadata_is_rejected(
    change: Callable[[str], dict[str, object]],
) -> None:
    _account, account_text = _key(1)
    _mint, mint_text = _key(2)
    meta: dict[str, object] = {
        "preTokenBalances": [
            {
                "accountIndex": 0,
                "mint": mint_text,
                "uiTokenAmount": {"decimals": 6},
            }
        ],
        "postTokenBalances": [
            {
                "accountIndex": 0,
                **change(mint_text),
            }
        ],
    }
    with pytest.raises(MalformedTokenMetadataError):
        map_token_balances((account_text,), meta)


def test_balance_index_bounds_and_key_shape_are_rejected() -> None:
    _account, account_text = _key(1)
    _mint, mint_text = _key(2)
    with pytest.raises(MalformedTokenMetadataError):
        map_token_balances(
            (account_text,),
            {
                "preTokenBalances": [
                    {
                        "accountIndex": 1,
                        "mint": mint_text,
                        "uiTokenAmount": {"decimals": 6},
                    }
                ]
            },
        )
    with pytest.raises(MalformedTokenMetadataError):
        append_loaded_addresses((account_text,), {"loadedAddresses": {"writable": []}})
