from __future__ import annotations

from pathlib import Path

import pytest

from pumpquant.research.families.shock_recovery import (
    StateProfile,
    default_shock_recovery_candidates,
)
from pumpquant.research.families.shock_recovery_grid import (
    DEVELOPMENT_END_SLOT_EXCLUSIVE,
    DevelopmentResult,
    FittedThresholds,
    ShockRecoveryGridConfig,
    ShockRecoveryGridError,
    _fill_buy,
    _fill_sell,
    evaluate_candidate,
    fit_candidate_thresholds,
    nested_walk_forward_folds,
    validate_development_rows,
    verify_local_report,
    write_local_report,
)


def _row(mint: str, index: int, *, fee_bps: float | None = 100.0) -> dict[str, object]:
    return {
        "mint": mint,
        "market_id": f"market-{mint}",
        "event_id": f"{mint}-{index}",
        "slot": 431_265_461 + index,
        "tx_index": 0,
        "log_index": 0,
        "event_index": index,
        "available_at_ns": 1_000_000_000_000 + index,
        "phase": "bonding_curve",
        "side": "buy",
        "coverage_gap": False,
        "reserve_continuity": True,
        "shock_price_5e": -1.0,
        "shock_flow_5e": -1.0,
        "recovery_price_2e": 1.0,
        "recovery_flow_2e": 1.0,
        "gross_directional_impact_bps": 1.0,
        "sell_excitation_relaxation_5e": 0.1,
        "sell_excitation_relaxation_20e": 0.1,
        "pre_base_reserve": 100.0,
        "pre_quote_reserve": 100.0,
        "fee_bps": fee_bps,
    }


def test_upper_bound_is_fail_closed() -> None:
    with pytest.raises(ShockRecoveryGridError):
        ShockRecoveryGridConfig(development_end_slot_exclusive=DEVELOPMENT_END_SLOT_EXCLUSIVE)
    with pytest.raises(ShockRecoveryGridError):
        validate_development_rows(
            [_row("m", 0) | {"slot": DEVELOPMENT_END_SLOT_EXCLUSIVE}],
        )


def test_nested_folds_are_whole_token_disjoint_and_deterministic() -> None:
    launches = {f"m{i:03d}": 1_000_000_000_000 + i * 1_000_000_000_000 for i in range(100)}
    folds = nested_walk_forward_folds(launches)
    assert len(folds) == 5
    assert all(not set(fold.validation_mints) & set(fold.test_mints) for fold in folds)
    assert all(not set(fold.train_mints) & set(fold.validation_mints) for fold in folds)
    assert all(not set(fold.train_mints) & set(fold.test_mints) for fold in folds)
    assert [len(fold.validation) for fold in folds] == [len(fold.test) for fold in folds]


def test_quantiles_do_not_leak_validation_values() -> None:
    candidate = next(
        item
        for item in default_shock_recovery_candidates()
        if item.shock_length == 5
        and item.state_profile is StateProfile.NONE
        and item.shock_measure.value == "price"
        and item.recovery_measure.value == "price"
    )
    training = [_row("train", i) | {"shock_price_5e": float(i)} for i in range(5)]
    threshold = fit_candidate_thresholds(candidate, training, fold=1)
    tampered_validation = [_row("validation", 0) | {"shock_price_5e": 1_000_000.0}]
    assert threshold == fit_candidate_thresholds(candidate, training, fold=1)
    assert (
        threshold.shock_threshold
        != fit_candidate_thresholds(
            candidate, training + tampered_validation, fold=1
        ).shock_threshold
    )


def test_one_position_per_mint_and_missing_fee_fail_closed() -> None:
    candidate = next(
        item
        for item in default_shock_recovery_candidates()
        if item.shock_length == 5
        and item.holding_events == 20
        and item.state_profile is StateProfile.NONE
    )
    rows = [_row("m", index) for index in range(45)]
    thresholds = FittedThresholds(candidate.sha256, 1, -1.0, 1.0, None, len(rows))
    outcomes = evaluate_candidate(candidate, rows, thresholds, fold=1, split="validation")
    assert len(outcomes) == 1
    assert outcomes[0].entry_index == 2
    assert outcomes[0].exit_index == 22
    missing_fee = [_row("m", index, fee_bps=None) for index in range(45)]
    assert evaluate_candidate(candidate, missing_fee, thresholds) == ()


def test_fee_asset_orientation_changes_the_correct_leg() -> None:
    quote_fee = _row("m", 0) | {"fee_bps": 100.0, "fee_asset": "quote"}
    base_fee = _row("m", 0) | {"fee_bps": 100.0, "fee_asset": "base"}
    quote_buy = _fill_buy(quote_fee, None, 10.0, 1.0, 1.0)
    base_buy = _fill_buy(base_fee, None, 10.0, 1.0, 1.0)
    assert quote_buy is not None and base_buy is not None
    expected_quote_buy = 100.0 - 10_000.0 / (100.0 + 10.0 / 1.01)
    expected_base_buy = (100.0 - 10_000.0 / 110.0) * 0.99
    assert quote_buy[0] == pytest.approx(expected_quote_buy)
    assert base_buy[0] == pytest.approx(expected_base_buy)

    quote_sell = _fill_sell(quote_fee, None, 10.0, 1.0, 1.0)
    base_sell = _fill_sell(base_fee, None, 10.0, 1.0, 1.0)
    expected_quote_sell = (100.0 - 10_000.0 / 110.0) * 0.99
    expected_base_sell = 100.0 - 10_000.0 / (100.0 + 10.0 / 1.01)
    assert quote_sell == pytest.approx(expected_quote_sell)
    assert base_sell == pytest.approx(expected_base_sell)


def test_zero_survivor_manifest_is_valid_and_payload_tamper_is_rejected(tmp_path: Path) -> None:
    result = DevelopmentResult(
        experiment_id="synthetic",
        status="no_survivor",
        candidate_count=192,
        trial_denominator=1143,
        folds=(),
        promoted=(),
    )
    write_local_report(result, tmp_path)
    manifest = verify_local_report(tmp_path)
    assert manifest["valid"] is True
    assert manifest["zero_survivor"] is True
    report_path = tmp_path / "report.json"
    report_path.write_bytes(report_path.read_bytes() + b"tamper")
    with pytest.raises(ShockRecoveryGridError, match="tampered"):
        verify_local_report(tmp_path)
