from __future__ import annotations

from datetime import UTC, datetime

from pumpquant.execution.empirical import build_empirical_execution_profile
from pumpquant.execution.telemetry import SCHEMA


def _event(attempt: str, event: str, elapsed: float, **extra: object) -> dict:
    return {
        "schema": SCHEMA,
        "attempt_id": attempt,
        "event": event,
        "wall_time": datetime.now(UTC).isoformat(),
        "elapsed_ms": elapsed,
        **extra,
    }


def _landed_leg(
    attempt: str,
    purpose: str,
    bid: int,
    *,
    route: str = "route-a",
) -> list[dict]:
    return [
        _event(attempt, "attempt_created", 0, purpose=purpose, route=route),
        _event(
            attempt,
            "decision",
            1,
            action="submit",
            selected_micro_lamports=bid,
            global_p75_micro_lamports=0,
            localized_p75_micro_lamports=0,
        ),
        _event(attempt, "built_signed", 1.5, signature=f"sig-{attempt}"),
        _event(attempt, "sent", 2, signature=f"sig-{attempt}"),
        _event(
            attempt,
            "landed",
            3,
            signature=f"sig-{attempt}",
            send_to_land_ms=500,
            websocket_processed_ms=250,
            err=None,
        ),
        _event(
            attempt,
            "reconciled",
            4,
            signature=f"sig-{attempt}",
            actual_fee_lamports=5_100,
            actual_compute_units=90_000,
        ),
    ]


def test_profile_keeps_entry_and_exit_samples_and_closes_small_sample_gate() -> None:
    events = [
        *_landed_leg("buy", "pumpswap_calibration_buy", 1_000),
        *_landed_leg("sell", "pumpswap_calibration_sell", 10_000),
    ]
    profile = build_empirical_execution_profile(events)
    assert len(profile.samples) == 2
    assert {sample.side for sample in profile.samples} == {"entry", "exit"}
    assert not profile.gate.ready
    assert "entry_landed_samples=1<30" in profile.gate.reasons


def test_pre_send_failure_reduces_entry_availability() -> None:
    events = _landed_leg("buy", "pumpswap_calibration_buy", 1_000)
    events.extend(
        [
            _event("failed", "attempt_created", 0, purpose="pumpswap_calibration_buy"),
            _event(
                "failed",
                "decision",
                1,
                action="submit",
                selected_micro_lamports=1_000,
            ),
            _event("failed", "pre_send_failure", 2),
            _event("failed", "terminal_error", 3, error="rate limit"),
        ]
    )
    profile = build_empirical_execution_profile(events)
    assert profile.gate.entry_submit_decisions == 2
    assert profile.gate.entry_pre_send_failures == 1
    assert profile.gate.entry_submission_probability == 0.5


def test_profile_filters_route_and_uses_websocket_processed_latency() -> None:
    events = [
        *_landed_leg("buy-a", "pumpswap_calibration_buy", 1_000, route="route-a"),
        *_landed_leg("buy-b", "pumpswap_calibration_buy", 2_000, route="route-b"),
    ]
    profile = build_empirical_execution_profile(
        events,
        route="route-a",
        minimum_landed_per_leg=0,
        minimum_bids_per_leg=0,
        minimum_regimes_per_leg=0,
        minimum_submission_probability=0,
    )
    assert profile.version == 3
    assert profile.route_filter == "route-a"
    assert len(profile.samples) == 1
    assert profile.samples[0].pre_submit_ms == 1.5
    assert profile.samples[0].send_to_processed_ms == 250
    assert profile.samples[0].attempt_to_processed_ms == 251.5
