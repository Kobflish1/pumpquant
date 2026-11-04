from dataclasses import dataclass

import pytest

from pumpquant.audits.live_rule.revalidation import (
    causal_second_checkpoint_path,
    summarize_end_to_end_latencies,
)


@dataclass(frozen=True)
class _Event:
    available_at_ns: int
    value: int


@dataclass(frozen=True)
class _Path:
    signal_time_ns: int
    signal_available_ns: int
    events: tuple[_Event, ...]


def test_causal_second_checkpoint_delays_availability_and_keeps_deadline_state():
    path = _Path(
        signal_time_ns=1_000_000_000,
        signal_available_ns=1_000_000_000,
        events=(
            _Event(1_000_000_000, 1),
            _Event(6_000_000_000, 2),
            _Event(7_000_000_000, 3),
        ),
    )
    selected = causal_second_checkpoint_path(path, 5)
    assert selected is not None
    assert selected.signal_time_ns == 7_000_000_000
    assert selected.signal_available_ns == 7_000_000_000
    assert selected.events == (
        _Event(7_000_000_000, 2),
        _Event(8_000_000_000, 3),
    )


def test_causal_second_checkpoint_rejects_subsecond_archive_times():
    path = _Path(
        signal_time_ns=1,
        signal_available_ns=1,
        events=(_Event(1_000_000_001, 1),),
    )
    with pytest.raises(ValueError, match="whole-second"):
        causal_second_checkpoint_path(path, 5)


def test_end_to_end_latency_summary_uses_decision_and_trigger_origins():
    rows = [
        {
            "event": "decision",
            "action": "enter",
            "mint": "m",
            "decision_time_ns": 1_000_000_000,
        },
        {
            "event": "transaction_landed",
            "side": "entry",
            "mint": "m",
            "wall_time_ns": 2_500_000_000,
        },
        {
            "event": "exit_triggered",
            "mint": "m",
            "wall_time_ns": 3_000_000_000,
        },
        {
            "event": "transaction_landed",
            "side": "exit",
            "mint": "m",
            "wall_time_ns": 4_250_000_000,
        },
    ]
    result = summarize_end_to_end_latencies(rows)
    assert result["entry"]["median_ms"] == 1_500
    assert result["exit"]["median_ms"] == 1_250
    assert result["entries_without_landing_sample"] == 0
