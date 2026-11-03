from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pumpquant.execution.telemetry import (
    SCHEMA,
    ExecutionAttemptLedger,
    ExecutionTelemetryError,
    summarize_calibration,
    summarize_calibration_arms,
)


def _event(attempt_id: str, event: str, elapsed_ms: float, **extra: object) -> dict:
    return {
        "schema": SCHEMA,
        "attempt_id": attempt_id,
        "event": event,
        "wall_time": datetime.now(UTC).isoformat(),
        "elapsed_ms": elapsed_ms,
        **extra,
    }


def test_append_read_and_summarize(tmp_path) -> None:
    ledger = ExecutionAttemptLedger(tmp_path / "attempts.jsonl")
    rows = [
        _event("a", "attempt_created", 0, purpose="recovery"),
        _event("a", "sent", 20, signature="sig-a"),
        _event(
            "a",
            "simulated",
            10,
            compute_unit_limit=10_000,
            compute_unit_price_micro_lamports=1_000,
        ),
        _event(
            "a",
            "landed",
            540,
            signature="sig-a",
            send_to_land_ms=520,
            err=None,
        ),
        _event(
            "a",
            "reconciled",
            600,
            signature="sig-a",
            actual_fee_lamports=15_010,
            actual_compute_units=1_214,
        ),
        _event("b", "attempt_created", 0, purpose="rejected_simulation"),
        _event("b", "terminal_error", 50, error="simulation failed"),
    ]
    for row in rows:
        ledger.append(row)

    summary = summarize_calibration(ledger.read())
    assert summary.attempt_count == 2
    assert summary.submit_decision_count == 0
    assert summary.pre_send_failure_count == 0
    assert summary.submission_probability is None
    assert summary.sent_count == 1
    assert summary.landed_count == 1
    assert summary.landing_probability == 1
    assert summary.latency_median_ms == 520
    assert summary.latency_mean_ms == 520
    assert summary.latency_p90_ms == 520
    assert summary.latency_p99_ms == 520
    assert summary.total_onchain_fee_lamports == 15_010
    assert summary.compute_median_units == 1_214
    assert ledger.path.stat().st_mode & 0o777 == 0o600


def test_rejects_secret_bearing_fields(tmp_path) -> None:
    ledger = ExecutionAttemptLedger(tmp_path / "attempts.jsonl")
    with pytest.raises(ExecutionTelemetryError, match="secret-bearing"):
        ledger.append(
            _event(
                "a",
                "attempt_created",
                0,
                metadata={"seed_phrase": "must never be written"},
            )
        )


def test_landed_failure_pays_fee() -> None:
    rows = [
        _event("a", "attempt_created", 0),
        _event("a", "sent", 1),
        _event("a", "landed", 2, send_to_land_ms=1, err={"InstructionError": 1}),
        _event("a", "reconciled", 3, actual_fee_lamports=5_000),
    ]
    summary = summarize_calibration(rows)
    assert summary.landed_failure_count == 1
    assert summary.landed_success_count == 0
    assert summary.total_onchain_fee_lamports == 5_000


def test_expired_unlanded_attempt_has_zero_fee() -> None:
    rows = [
        _event("a", "attempt_created", 0),
        _event("a", "sent", 1),
        _event("a", "expired", 2),
    ]
    summary = summarize_calibration(rows)
    assert summary.expired_count == 1
    assert summary.landed_count == 0
    assert summary.total_onchain_fee_lamports == 0


def test_pre_send_endpoint_failure_reduces_submission_availability() -> None:
    rows = [
        _event("a", "attempt_created", 0),
        _event("a", "decision", 1, action="submit"),
        _event("a", "sent", 2),
        _event("a", "landed", 3, send_to_land_ms=1, err=None),
        _event("a", "reconciled", 4, actual_fee_lamports=5_000),
        _event("b", "attempt_created", 0),
        _event("b", "decision", 1, action="submit"),
        _event("b", "pre_send_failure", 2, failure_class="endpoint_rate_limit"),
        _event("b", "terminal_error", 3, error="no broadcast"),
    ]
    summary = summarize_calibration(rows)
    assert summary.submit_decision_count == 2
    assert summary.pre_send_failure_count == 1
    assert summary.submission_probability == 0.5
    assert summary.landing_probability == 1


def test_calibration_arms_separate_entry_exit_and_bid() -> None:
    rows = [
        _event("a", "attempt_created", 0, purpose="pumpswap_calibration_buy"),
        _event(
            "a",
            "decision",
            1,
            action="submit",
            selected_micro_lamports=1_000,
        ),
        _event("a", "sent", 2),
        _event("a", "landed", 3, send_to_land_ms=10, err=None),
        _event("a", "reconciled", 4, actual_fee_lamports=5_100),
        _event("b", "attempt_created", 0, purpose="pumpswap_calibration_buy"),
        _event(
            "b",
            "decision",
            1,
            action="submit",
            selected_micro_lamports=1_000,
        ),
        _event("b", "pre_send_failure", 2, failure_class="endpoint_rate_limit"),
        _event("b", "terminal_error", 3, error="no broadcast"),
        _event("c", "attempt_created", 0, purpose="pumpswap_calibration_sell"),
        _event(
            "c",
            "decision",
            1,
            action="submit",
            selected_micro_lamports=10_000,
        ),
        _event("c", "sent", 2),
        _event("c", "landed", 3, send_to_land_ms=8, err=None),
        _event("c", "reconciled", 4, actual_fee_lamports=6_000),
    ]
    arms = summarize_calibration_arms(rows)
    assert len(arms) == 2
    buy = next(arm for arm in arms if arm.purpose.endswith("buy"))
    assert buy.submit_decisions == 2
    assert buy.pre_send_failures == 1
    assert buy.submission_probability == 0.5
    assert buy.landing_probability == 1
    assert buy.landing_wilson_low_95 is not None
    assert buy.landing_wilson_low_95 < 0.99
