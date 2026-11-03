"""Independent golden vectors and fail-closed tests for instruction intents."""

from __future__ import annotations

import base64
from dataclasses import FrozenInstanceError

import pytest

from pumpquant.decoding.instruction_intent import (
    INSTRUCTION_SPECS,
    LAYOUT_REVISION_HISTORICAL_OPTION_BOOL,
    LAYOUT_REVISION_LEGACY_NO_TRACK_VOLUME,
    PINNED_IDL_REVISION,
    PUMP_PROGRAM_ID,
    PUMPSWAP_PROGRAM_ID,
    CandidateIntent,
    MalformedInstructionError,
    UnknownInstructionError,
    UnsupportedEncodingError,
    decode_base58,
    decode_instruction_intent,
    encode_base58,
)

_PUMP_BUY = bytes((102, 6, 61, 18, 1, 218, 235, 234))
_PUMP_SELL = bytes((51, 230, 133, 164, 1, 127, 131, 173))
_PUMPSWAP_BUY = _PUMP_BUY
_PUMPSWAP_SELL = _PUMP_SELL


def _u16(value: int) -> bytes:
    return value.to_bytes(2, "little")


def _u64(value: int) -> bytes:
    return value.to_bytes(8, "little")


def _string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return len(encoded).to_bytes(4, "little") + encoded


def _independent_base58(raw: bytes) -> str:
    """Tiny test-only encoder, intentionally separate from module code."""

    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    if not raw:
        return ""
    zeros = len(raw) - len(raw.lstrip(b"\0"))
    number = int.from_bytes(raw, "big")
    digits: list[str] = []
    while number:
        number, remainder = divmod(number, 58)
        digits.append(alphabet[remainder])
    return "1" * zeros + "".join(reversed(digits))


def _decode(raw: bytes, *, program: str, encoding: str = "base64") -> CandidateIntent:
    encoded = (
        base64.b64encode(raw).decode("ascii") if encoding == "base64" else _independent_base58(raw)
    )
    return decode_instruction_intent(program, encoded, encoding=encoding)  # type: ignore[arg-type]


def test_pump_create_golden_vector_and_raw_provenance() -> None:
    discriminator = bytes((24, 30, 200, 40, 5, 28, 7, 119))
    creator = bytes(range(32))
    raw = discriminator + _string("A") + _string("T") + _string("https://x") + creator

    intent = _decode(raw, program=PUMP_PROGRAM_ID)

    assert intent.intent_kind == "candidate_intent"
    assert intent.label == "candidate_intent"
    assert intent.instruction_name == "create"
    assert intent.layout_revision == PINNED_IDL_REVISION
    assert intent.args == {
        "name": "A",
        "symbol": "T",
        "uri": "https://x",
        "creator": creator,
    }
    assert intent.raw_data == raw
    assert intent.raw_args == raw[8:]


@pytest.mark.parametrize(
    ("program", "raw", "name", "args"),
    [
        (
            PUMP_PROGRAM_ID,
            bytes((214, 144, 76, 236, 95, 139, 49, 180))
            + _string("N")
            + _string("S")
            + _string("U")
            + bytes([7]) * 32
            + b"\x01\x00",
            "create_v2",
            {
                "name": "N",
                "symbol": "S",
                "uri": "U",
                "creator": bytes([7]) * 32,
                "is_mayhem_mode": True,
                "is_cashback_enabled": False,
            },
        ),
        (
            PUMP_PROGRAM_ID,
            _PUMP_BUY + _u64(11) + _u64(22) + b"\x01",
            "buy",
            {"amount": 11, "max_sol_cost": 22, "track_volume": True},
        ),
        (
            PUMP_PROGRAM_ID,
            bytes((184, 23, 238, 97, 103, 197, 211, 61)) + _u64(11) + _u64(22),
            "buy_v2",
            {"amount": 11, "max_sol_cost": 22},
        ),
        (
            PUMP_PROGRAM_ID,
            bytes((56, 252, 116, 8, 158, 223, 205, 95)) + _u64(13) + _u64(14) + b"\x00",
            "buy_exact_sol_in",
            {"spendable_sol_in": 13, "min_tokens_out": 14, "track_volume": False},
        ),
        (
            PUMP_PROGRAM_ID,
            bytes((194, 171, 28, 70, 104, 77, 91, 47)) + _u64(15) + _u64(16),
            "buy_exact_quote_in_v2",
            {"spendable_quote_in": 15, "min_tokens_out": 16},
        ),
        (
            PUMP_PROGRAM_ID,
            _PUMP_SELL + _u64(17) + _u64(18),
            "sell",
            {"amount": 17, "min_sol_output": 18},
        ),
        (
            PUMP_PROGRAM_ID,
            bytes((93, 246, 130, 60, 231, 233, 64, 178)) + _u64(19) + _u64(20),
            "sell_v2",
            {"amount": 19, "min_sol_output": 20},
        ),
        (
            PUMP_PROGRAM_ID,
            bytes((155, 234, 231, 146, 236, 158, 162, 30)),
            "migrate",
            {},
        ),
        (
            PUMP_PROGRAM_ID,
            bytes((187, 203, 18, 31, 206, 237, 254, 41)),
            "migrate_v2",
            {},
        ),
        (
            PUMPSWAP_PROGRAM_ID,
            bytes((233, 146, 209, 142, 207, 104, 64, 188))
            + _u16(2)
            + _u64(21)
            + _u64(22)
            + bytes([8]) * 32
            + b"\x00\x01",
            "create_pool",
            {
                "index": 2,
                "base_amount_in": 21,
                "quote_amount_in": 22,
                "coin_creator": bytes([8]) * 32,
                "is_mayhem_mode": False,
                "is_cashback_coin": True,
            },
        ),
        (
            PUMPSWAP_PROGRAM_ID,
            _PUMPSWAP_BUY + _u64(23) + _u64(24) + b"\x00",
            "buy",
            {"base_amount_out": 23, "max_quote_amount_in": 24, "track_volume": False},
        ),
        (
            PUMPSWAP_PROGRAM_ID,
            bytes((198, 46, 21, 82, 180, 217, 232, 112)) + _u64(25) + _u64(26) + b"\x01",
            "buy_exact_quote_in",
            {"spendable_quote_in": 25, "min_base_amount_out": 26, "track_volume": True},
        ),
        (
            PUMPSWAP_PROGRAM_ID,
            _PUMPSWAP_SELL + _u64(27) + _u64(28),
            "sell",
            {"base_amount_in": 27, "min_quote_amount_out": 28},
        ),
        (
            PUMPSWAP_PROGRAM_ID,
            bytes((242, 35, 198, 137, 82, 225, 242, 182)) + _u64(29) + _u64(30) + _u64(31),
            "deposit",
            {"lp_token_amount_out": 29, "max_base_amount_in": 30, "max_quote_amount_in": 31},
        ),
        (
            PUMPSWAP_PROGRAM_ID,
            bytes((183, 18, 70, 156, 148, 109, 161, 34)) + _u64(32) + _u64(33) + _u64(34),
            "withdraw",
            {"lp_token_amount_in": 32, "min_base_amount_out": 33, "min_quote_amount_out": 34},
        ),
    ],
)
def test_core_golden_vectors(program: str, raw: bytes, name: str, args: dict[str, object]) -> None:
    intent = _decode(raw, program=program)
    assert intent.instruction_name == name
    assert intent.args == args
    assert intent.raw_data == raw


def test_known_optional_layouts_are_explicit_and_normalized() -> None:
    raw = _PUMP_BUY + _u64(1) + _u64(2)
    legacy = _decode(raw, program=PUMP_PROGRAM_ID)
    assert legacy.args["track_volume"] is None
    assert legacy.layout_revision == LAYOUT_REVISION_LEGACY_NO_TRACK_VOLUME

    historical = _decode(raw + b"\x01\x01", program=PUMP_PROGRAM_ID)
    assert historical.args["track_volume"] is True
    assert historical.layout_revision == LAYOUT_REVISION_HISTORICAL_OPTION_BOOL

    with pytest.raises(MalformedInstructionError):
        _decode(raw + b"\x00\x01", program=PUMP_PROGRAM_ID)


def test_pinned_map_has_program_collision_keys() -> None:
    assert (PUMP_PROGRAM_ID, _PUMP_BUY, PINNED_IDL_REVISION) in INSTRUCTION_SPECS
    assert (PUMPSWAP_PROGRAM_ID, _PUMPSWAP_BUY, PINNED_IDL_REVISION) in INSTRUCTION_SPECS
    assert INSTRUCTION_SPECS[(PUMP_PROGRAM_ID, _PUMP_BUY, PINNED_IDL_REVISION)].name == "buy"
    assert (
        INSTRUCTION_SPECS[(PUMPSWAP_PROGRAM_ID, _PUMPSWAP_BUY, PINNED_IDL_REVISION)].name == "buy"
    )


def test_wrong_program_unknown_discriminator_and_malformed_payload_fail_closed() -> None:
    raw = _PUMP_BUY + _u64(1) + _u64(2) + b"\x00"
    # The buy discriminator deliberately collides across programs; the
    # program-qualified table resolves it to PumpSwap rather than Pump.
    collision = _decode(raw, program=PUMPSWAP_PROGRAM_ID)
    assert collision.args == {
        "base_amount_out": 1,
        "max_quote_amount_in": 2,
        "track_volume": False,
    }
    with pytest.raises(UnknownInstructionError):
        _decode(bytes((24, 30, 200, 40, 5, 28, 7, 119)), program=PUMPSWAP_PROGRAM_ID)
    with pytest.raises(UnknownInstructionError):
        _decode(bytes(8) + _u64(1), program=PUMP_PROGRAM_ID)
    with pytest.raises(MalformedInstructionError):
        _decode(_PUMP_BUY + b"\x00", program=PUMP_PROGRAM_ID)
    with pytest.raises(MalformedInstructionError):
        _decode(bytes((155, 234, 231, 146, 236, 158, 162, 30)) + b"\x00", program=PUMP_PROGRAM_ID)


def test_encoding_must_be_explicit_and_canonical() -> None:
    raw = bytes((155, 234, 231, 146, 236, 158, 162, 30))
    encoded58 = _independent_base58(raw)
    encoded64 = base64.b64encode(raw).decode("ascii")
    assert (
        decode_instruction_intent(PUMP_PROGRAM_ID, encoded58, encoding="base58").name == "migrate"
    )
    assert (
        decode_instruction_intent(PUMP_PROGRAM_ID, encoded64, encoding="base64").name == "migrate"
    )
    with pytest.raises(UnsupportedEncodingError):
        decode_instruction_intent(PUMP_PROGRAM_ID, encoded58, encoding="hex")  # type: ignore[arg-type]
    with pytest.raises(MalformedInstructionError):
        decode_instruction_intent(PUMP_PROGRAM_ID, encoded58 + "0", encoding="base58")
    with pytest.raises(MalformedInstructionError):
        decode_instruction_intent(PUMP_PROGRAM_ID, encoded64 + "\n", encoding="base64")


def test_base58_canonical_validation_and_base64_strictness() -> None:
    assert decode_base58(encode_base58(b"\0\0abc")) == b"\0\0abc"
    with pytest.raises(MalformedInstructionError):
        decode_base58("0")
    with pytest.raises(MalformedInstructionError):
        decode_base58("O")
    with pytest.raises(MalformedInstructionError):
        decode_instruction_intent(
            PUMP_PROGRAM_ID,
            base64.b64encode(_PUMP_BUY + _u64(1) + _u64(2) + b"\x01").decode("ascii").rstrip("="),
            encoding="base64",
        )


def test_immutable_result() -> None:
    intent = _decode(_PUMP_BUY + _u64(1) + _u64(2) + b"\x01", program=PUMP_PROGRAM_ID)
    with pytest.raises(TypeError):
        intent.args["amount"] = 3  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        intent.instruction_name = "sell"  # type: ignore[misc]
