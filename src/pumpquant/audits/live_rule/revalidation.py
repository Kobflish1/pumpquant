"""Causal checkpoints and end-to-end latency, for revalidating the live rule."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import replace
from typing import Any

NANOSECONDS = 1_000_000_000


def causal_second_checkpoint_path(path: Any, checkpoint_seconds: int) -> Any | None:
    """Return a checkpoint path without treating second timestamps as immediate.

    Archive ``block_time`` values have one-second precision. An event reported
    at second ``t`` is therefore treated as causally available at ``t + 1``.
    The checkpoint begins when the first event is available, matching the live
    engine's observed-arrival clock.
    """

    if checkpoint_seconds <= 0:
        raise ValueError("checkpoint_seconds must be positive")
    events = tuple(path.events)
    if not events:
        return None
    if any(
        int(event.available_at_ns) < 0 or int(event.available_at_ns) % NANOSECONDS
        for event in events
    ):
        raise ValueError("archive event availability must be whole-second time")
    causal_events = tuple(
        replace(
            event,
            available_at_ns=int(event.available_at_ns) + NANOSECONDS,
        )
        for event in events
    )
    decision_ns = int(causal_events[0].available_at_ns) + checkpoint_seconds * NANOSECONDS
    selected_index: int | None = None
    for index, event in enumerate(causal_events):
        if int(event.available_at_ns) > decision_ns:
            break
        selected_index = index
    if selected_index is None:
        return None
    return replace(
        path,
        signal_time_ns=decision_ns,
        signal_available_ns=decision_ns,
        events=causal_events[selected_index:],
    )


def _percentile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    if not 0.0 <= probability <= 1.0:
        raise ValueError("probability must be between zero and one")
    ordered = sorted(values)
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _summary(values: list[float]) -> dict[str, float | int | None]:
    return {
        "samples": len(values),
        "median_ms": _percentile(values, 0.50),
        "p90_ms": _percentile(values, 0.90),
        "p99_ms": _percentile(values, 0.99),
        "maximum_ms": max(values) if values else None,
    }


def summarize_end_to_end_latencies(
    rows: Iterable[dict[str, Any]],
) -> dict[str, object]:
    """Summarise the latency scopes consumed by a causal outcome replay."""

    materialized = list(rows)
    samples = extract_end_to_end_latency_samples(materialized)
    decisions = {
        str(row["mint"]): int(row["decision_time_ns"])
        for row in materialized
        if row.get("event") == "decision" and row.get("action") == "enter"
    }
    exits = {
        str(row["mint"])
        for row in materialized
        if row.get("event") == "transaction_landed" and row.get("side") == "exit"
    }
    sampled_exit_mints = {str(item["mint"]) for item in samples["exit"]}
    return {
        "latency_scope": "full_signal_and_trigger_to_processed",
        "entry": _summary([float(item["latency_ms"]) for item in samples["entry"]]),
        "exit": _summary([float(item["latency_ms"]) for item in samples["exit"]]),
        "eligible_entry_decisions": len(decisions),
        "entries_without_landing_sample": len(decisions) - len(samples["entry"]),
        "landed_exits_without_trigger_sample": len(exits - sampled_exit_mints),
    }


def extract_end_to_end_latency_samples(
    rows: Iterable[dict[str, Any]],
) -> dict[str, list[dict[str, float | str]]]:
    """Return signature-linked latency samples rather than only percentiles."""

    materialized = list(rows)
    decisions = {
        str(row["mint"]): int(row["decision_time_ns"])
        for row in materialized
        if row.get("event") == "decision" and row.get("action") == "enter"
    }
    entry_landed = {
        str(row["mint"]): int(row["wall_time_ns"])
        for row in materialized
        if row.get("event") == "transaction_landed" and row.get("side") == "entry"
    }
    entry_samples: list[dict[str, float | str]] = [
        {
            "mint": mint,
            "latency_ms": (entry_landed[mint] - decision_ns) / 1_000_000,
        }
        for mint, decision_ns in decisions.items()
        if mint in entry_landed and entry_landed[mint] >= decision_ns
    ]

    triggers: dict[str, list[int]] = defaultdict(list)
    exits: dict[str, list[int]] = defaultdict(list)
    for row in materialized:
        if row.get("event") == "exit_triggered":
            triggers[str(row["mint"])].append(int(row["wall_time_ns"]))
        elif row.get("event") == "transaction_landed" and row.get("side") == "exit":
            exits[str(row["mint"])].append(int(row["wall_time_ns"]))
    exit_samples: list[dict[str, float | str]] = []
    for mint, landed_times in exits.items():
        landed = max(landed_times)
        eligible = [value for value in triggers.get(mint, ()) if value <= landed]
        if eligible:
            exit_samples.append(
                {
                    "mint": mint,
                    "latency_ms": (landed - max(eligible)) / 1_000_000,
                }
            )
    return {"entry": entry_samples, "exit": exit_samples}


__all__ = [
    "NANOSECONDS",
    "causal_second_checkpoint_path",
    "extract_end_to_end_latency_samples",
    "summarize_end_to_end_latencies",
]
