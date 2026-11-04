"""Fail-closed tests for pinned candidate account-position mapping."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import FrozenInstanceError, replace

import pytest

from pumpquant.decoding.instruction_accounts import (
    INSTRUCTION_ACCOUNT_SPECS,
    PINNED_IDL_REVISION,
    PUMP_IDL_SOURCE,
    PUMPSWAP_IDL_SOURCE,
    AccountListTypeError,
    CandidateAccountMapping,
    MalformedAccountPubkeyError,
    TooFewAccountsError,
    UnknownAccountLayoutError,
    map_candidate_accounts,
)
from pumpquant.decoding.instruction_intent import (
    PUMP_PROGRAM_ID,
    PUMPSWAP_PROGRAM_ID,
    CandidateIntent,
    encode_base58,
)


def _intent(program_id: str, name: str) -> CandidateIntent:
    for (program, discriminator, instruction, revision), _spec in INSTRUCTION_ACCOUNT_SPECS.items():
        if program == program_id and instruction == name and revision == PINNED_IDL_REVISION:
            return CandidateIntent(
                program_id=program,
                instruction_name=instruction,
                discriminator=discriminator,
                args={},
                raw_data=discriminator,
                layout_revision=PINNED_IDL_REVISION,
            )
    raise AssertionError(f"missing account spec for {program_id}/{name}")


def _keys(count: int, *, as_text: bool = True) -> list[str | bytes]:
    values = [bytes([index % 256]) * 32 for index in range(1, count + 1)]
    if as_text:
        return [encode_base58(value) for value in values]
    return list(values)


def test_every_core_layout_maps_in_idl_order_and_preserves_extras() -> None:
    for (program, _, name, _), spec in INSTRUCTION_ACCOUNT_SPECS.items():
        keys = _keys(len(spec.idl_names) + 2)
        mapped = map_candidate_accounts(_intent(program, name), keys)

        assert mapped.account_positions == tuple(
            zip(spec.idl_names, keys[: len(spec.idl_names)], strict=True)
        )
        assert mapped.extras == tuple(keys[-2:])
        assert mapped.accounts == mapped.account_positions
        assert mapped.idl_names == spec.idl_names
        assert mapped.layout_revision == PINNED_IDL_REVISION
        assert mapped.idl_revision == PINNED_IDL_REVISION
        assert mapped.source_id in (PUMP_IDL_SOURCE, PUMPSWAP_IDL_SOURCE)
        assert mapped.candidate_only is True
        assert mapped.execution_success_unknown is True


def test_colliding_buy_and_sell_discriminators_are_program_qualified() -> None:
    pump = map_candidate_accounts(_intent(PUMP_PROGRAM_ID, "buy"), _keys(16))
    amm = map_candidate_accounts(_intent(PUMPSWAP_PROGRAM_ID, "buy"), _keys(23))
    assert pump.idl_names[0] == "global"
    assert amm.idl_names[0] == "pool"
    assert pump.source_id == PUMP_IDL_SOURCE
    assert amm.source_id == PUMPSWAP_IDL_SOURCE


def test_bytes_pubkeys_are_accepted_and_preserved() -> None:
    intent = _intent(PUMP_PROGRAM_ID, "create")
    keys = _keys(14, as_text=False)
    mapped = map_candidate_accounts(intent, keys)
    assert mapped.account_positions[0] == ("mint", keys[0])
    assert mapped.extras == ()


@pytest.mark.parametrize(
    "mutator",
    [
        lambda value: replace(value, program_id="11111111111111111111111111111111"),
        lambda value: replace(value, instruction_name="sell"),
        lambda value: replace(value, layout_revision="legacy_no_track_volume"),
    ],
)
def test_wrong_program_instruction_or_layout_fails_closed(
    mutator: Callable[[CandidateIntent], CandidateIntent],
) -> None:
    with pytest.raises(UnknownAccountLayoutError):
        map_candidate_accounts(mutator(_intent(PUMP_PROGRAM_ID, "buy")), _keys(16))


def test_too_few_accounts_and_malformed_pubkeys_fail_closed() -> None:
    intent = _intent(PUMP_PROGRAM_ID, "sell")
    with pytest.raises(TooFewAccountsError):
        map_candidate_accounts(intent, _keys(13))
    with pytest.raises(MalformedAccountPubkeyError):
        map_candidate_accounts(intent, _keys(13) + ["not-a-pubkey"])
    with pytest.raises(MalformedAccountPubkeyError):
        map_candidate_accounts(intent, _keys(13) + [bytes(31)])
    with pytest.raises(AccountListTypeError):
        map_candidate_accounts(intent, _keys(14)[:-1] + [123])
    with pytest.raises(AccountListTypeError):
        map_candidate_accounts(intent, "not-an-account-list")


def test_input_and_result_are_immutable() -> None:
    intent = _intent(PUMP_PROGRAM_ID, "create")
    keys = _keys(14)
    mapped = map_candidate_accounts(intent, keys)
    keys[0] = encode_base58(bytes([99]) * 32)
    assert mapped.account_positions[0][1] != keys[0]
    with pytest.raises(FrozenInstanceError):
        mapped.instruction_name = "sell"  # type: ignore[misc]
    with pytest.raises(TypeError):
        mapped.account_positions[0] = ("other", keys[0])  # type: ignore[index]
    with pytest.raises(AttributeError):
        mapped.accounts.append(("other", keys[0]))  # type: ignore[attr-defined]


def test_public_layout_mapping_is_read_only() -> None:
    key = next(iter(INSTRUCTION_ACCOUNT_SPECS))
    with pytest.raises(TypeError):
        INSTRUCTION_ACCOUNT_SPECS[key] = INSTRUCTION_ACCOUNT_SPECS[key]  # type: ignore[index]


def test_result_constructor_rejects_non_candidate_flags() -> None:
    with pytest.raises(ValueError):
        CandidateAccountMapping(
            program_id=PUMP_PROGRAM_ID,
            instruction_name="create",
            discriminator=bytes((24, 30, 200, 40, 5, 28, 7, 119)),
            account_positions=tuple((name, _keys(1)[0]) for name in ("mint",)),
            extras=(),
            layout_revision=PINNED_IDL_REVISION,
            idl_revision=PINNED_IDL_REVISION,
            source_id=PUMP_IDL_SOURCE,
            candidate_only=False,
        )
