from __future__ import annotations

import base64

import pytest

import pumpquant.decoding.program_events as pe
from pumpquant.decoding.instruction_intent import (
    PUMP_PROGRAM_ID,
    PUMPSWAP_PROGRAM_ID,
    decode_instruction_intent,
    encode_base58,
)


def _u64(value: int) -> bytes:
    return value.to_bytes(8, "little")


def _i64(value: int) -> bytes:
    return value.to_bytes(8, "little", signed=True)


def _pubkey(seed: int) -> bytes:
    return bytes((seed,)) * 32


def _deposit_event() -> bytes:
    return b"".join(
        (
            pe.PUMPSWAP_DEPOSIT_EVENT_DISCRIMINATOR,
            _i64(1_700_000_000),
            b"".join(_u64(value) for value in range(1, 11)),
            b"".join(_pubkey(seed) for seed in range(1, 6)),
        )
    )


def _withdraw_event() -> bytes:
    return b"".join(
        (
            pe.PUMPSWAP_WITHDRAW_EVENT_DISCRIMINATOR,
            _i64(1_700_000_001),
            b"".join(_u64(value) for value in range(11, 21)),
            b"".join(_pubkey(seed) for seed in range(6, 11)),
        )
    )


def _fee_config_event() -> bytes:
    return b"".join(
        (
            pe.PUMPSWAP_UPDATE_FEE_CONFIG_EVENT_DISCRIMINATOR,
            _i64(1_700_000_002),
            _pubkey(1),
            _u64(20),
            _u64(5),
            b"".join(_pubkey(seed) for seed in range(2, 10)),
            _u64(5),
            _pubkey(10),
        )
    )


def test_decodes_pumpswap_liquidity_events_with_post_state() -> None:
    deposit = pe.decode_state_event(PUMPSWAP_PROGRAM_ID, _deposit_event())
    withdraw = pe.decode_state_event(PUMPSWAP_PROGRAM_ID, _withdraw_event())

    assert isinstance(deposit, pe.PumpSwapDepositEvent)
    assert (deposit.base_amount_in, deposit.quote_amount_in, deposit.lp_mint_supply) == (8, 9, 10)
    assert deposit.pool_base_token_reserves == 6
    assert isinstance(withdraw, pe.PumpSwapWithdrawEvent)
    assert (withdraw.base_amount_out, withdraw.quote_amount_out, withdraw.lp_mint_supply) == (
        18,
        19,
        20,
    )
    assert deposit.raw_data == _deposit_event()


def test_decodes_fee_config_and_preserves_invocation_order() -> None:
    encoded_deposit = base64.b64encode(_deposit_event()).decode("ascii")
    encoded_config = base64.b64encode(_fee_config_event()).decode("ascii")
    logs = [
        f"Program {PUMP_PROGRAM_ID} invoke [1]",
        f"Program {PUMPSWAP_PROGRAM_ID} invoke [2]",
        f"Program data: {encoded_deposit}",
        f"Program {PUMPSWAP_PROGRAM_ID} success",
        f"Program {PUMPSWAP_PROGRAM_ID} invoke [2]",
        f"Program data: {encoded_config}",
        f"Program {PUMPSWAP_PROGRAM_ID} success",
        f"Program {PUMP_PROGRAM_ID} success",
    ]
    events = pe.extract_state_event_logs(logs)
    assert [event.log_index for event in events] == [2, 5]
    assert [event.invoke_depth for event in events] == [2, 2]
    assert events[1].event.values["lp_fee_basis_points"] == 20
    assert events[1].event.values["protocol_fee_recipients"][-1] == encode_base58(_pubkey(9))


def test_state_decoder_fails_closed_for_trailing_wrong_program_and_bad_bool() -> None:
    with pytest.raises(pe.MalformedProgramEventError, match="trailing"):
        pe.decode_state_event(PUMPSWAP_PROGRAM_ID, _deposit_event() + b"x")
    with pytest.raises(pe.UnknownProgramEventError):
        pe.decode_state_event(PUMP_PROGRAM_ID, _deposit_event())
    disable = bytearray(
        pe.PUMPSWAP_DISABLE_EVENT_DISCRIMINATOR + _i64(1) + _pubkey(1) + b"\x01\x00\x00\x00\x00"
    )
    # The first disable flag starts after the discriminator, timestamp, admin.
    disable[8 + 8 + 32] = 2
    with pytest.raises(pe.MalformedProgramEventError, match="boolean"):
        pe.decode_state_event(PUMPSWAP_PROGRAM_ID, bytes(disable))


def test_config_instruction_decoders_use_pinned_idl_discriminators() -> None:
    raw = bytes((201, 207, 243, 114, 75, 111, 47, 189)) + _u64(20) + _u64(5)
    raw += b"".join(_pubkey(seed) for seed in range(1, 9)) + _u64(5) + _pubkey(9)
    intent = decode_instruction_intent(
        PUMPSWAP_PROGRAM_ID,
        base64.b64encode(raw).decode("ascii"),
        encoding="base64",
    )
    assert intent.instruction_name == "create_config"
    assert intent.args["lp_fee_basis_points"] == 20
    assert len(intent.args["protocol_fee_recipients"]) == 8  # type: ignore[arg-type]

    buyback = bytes((251, 224, 171, 146, 160, 26, 113, 233)) + b"\x01" + _u64(35)
    decoded = decode_instruction_intent(
        PUMP_PROGRAM_ID,
        base64.b64encode(buyback).decode("ascii"),
        encoding="base64",
    )
    assert decoded.instruction_name == "update_buyback_config"
    assert decoded.args["buyback_basis_points"] == 35
