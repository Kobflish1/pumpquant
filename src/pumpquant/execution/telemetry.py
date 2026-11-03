"""Append-only telemetry and summaries for live Solana execution attempts.

The ledger deliberately stores public transaction evidence only. Secret keys,
mnemonics, serialized signed transactions, and raw key material are rejected.
"""

from __future__ import annotations

import json
import math
import os
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import fmean, median
from typing import Any

SCHEMA = "pumpquant.execution_attempt.v1"
_FORBIDDEN_KEYS = frozenset(
    {
        "mnemonic",
        "recovery_phrase",
        "secret",
        "secret_key",
        "private_key",
        "seed",
        "seed_phrase",
        "signed_transaction",
    }
)
_TERMINAL_EVENTS = frozenset({"reconciled", "terminal_error", "expired"})


class ExecutionTelemetryError(ValueError):
    """Raised when an execution event or ledger is unsafe or inconsistent."""


@dataclass(frozen=True, slots=True)
class AttemptSummary:
    attempt_id: str
    purpose: str | None
    submit_decision: bool
    pre_send_failure: bool
    sent: bool
    landed: bool
    reconciled: bool
    expired: bool
    landed_error: bool
    signature: str | None
    send_to_land_ms: float | None
    fee_lamports: int
    compute_units: int | None
    compute_unit_limit: int | None
    compute_unit_price_micro_lamports: int | None
    terminal_error: str | None


@dataclass(frozen=True, slots=True)
class CalibrationSummary:
    attempt_count: int
    submit_decision_count: int
    pre_send_failure_count: int
    sent_count: int
    landed_count: int
    landed_success_count: int
    landed_failure_count: int
    expired_count: int
    reconciled_count: int
    submission_probability: float | None
    landing_probability: float | None
    landed_success_probability: float | None
    latency_median_ms: float | None
    latency_mean_ms: float | None
    latency_p90_ms: float | None
    latency_p99_ms: float | None
    total_onchain_fee_lamports: int
    fee_median_lamports: float | None
    compute_median_units: float | None


@dataclass(frozen=True, slots=True)
class CalibrationArmSummary:
    purpose: str
    compute_unit_price_micro_lamports: int
    submit_decisions: int
    pre_send_failures: int
    sent: int
    landed: int
    landed_successes: int
    submission_probability: float
    landing_probability: float | None
    landing_wilson_low_95: float | None
    landing_wilson_high_95: float | None
    latency_median_ms: float | None
    latency_mean_ms: float | None
    latency_p90_ms: float | None
    latency_p99_ms: float | None
    fee_median_lamports: float | None


def _check_safe(value: Any, path: tuple[str, ...] = ()) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = str(key).lower()
            if normalized in _FORBIDDEN_KEYS:
                location = ".".join((*path, str(key)))
                raise ExecutionTelemetryError(f"secret-bearing field is forbidden: {location}")
            _check_safe(nested, (*path, str(key)))
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _check_safe(nested, (*path, str(index)))


def _validate_event(event: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(event)
    _check_safe(row)
    if row.get("schema") != SCHEMA:
        raise ExecutionTelemetryError(f"schema must be {SCHEMA!r}")
    for key in ("attempt_id", "event", "wall_time"):
        if not isinstance(row.get(key), str) or not row[key]:
            raise ExecutionTelemetryError(f"{key} must be a non-empty string")
    try:
        datetime.fromisoformat(row["wall_time"].replace("Z", "+00:00"))
    except ValueError as error:
        raise ExecutionTelemetryError("wall_time must be ISO-8601") from error
    elapsed = row.get("elapsed_ms")
    if not isinstance(elapsed, (int, float)) or not math.isfinite(float(elapsed)) or elapsed < 0:
        raise ExecutionTelemetryError("elapsed_ms must be finite and nonnegative")
    return row


class ExecutionAttemptLedger:
    """Durably append validated JSON events using the operating system's append flag."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def append(self, event: Mapping[str, Any]) -> None:
        row = _validate_event(event)
        payload = (json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n").encode()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(
            self.path,
            os.O_APPEND | os.O_CREAT | os.O_WRONLY,
            0o600,
        )
        try:
            os.write(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def read(self) -> tuple[dict[str, Any], ...]:
        if not self.path.exists():
            return ()
        rows: list[dict[str, Any]] = []
        with self.path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ExecutionTelemetryError(
                        f"invalid JSON on ledger line {line_number}"
                    ) from error
                if not isinstance(raw, Mapping):
                    raise ExecutionTelemetryError(f"ledger line {line_number} is not an object")
                rows.append(_validate_event(raw))
        return tuple(rows)


def _number(event: Mapping[str, Any], key: str) -> float | None:
    value = event.get(key)
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    return None


def _integer(event: Mapping[str, Any], key: str) -> int | None:
    value = _number(event, key)
    return None if value is None else int(value)


def _percentile(values: Iterable[float], quantile: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    position = quantile * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _wilson_interval(
    successes: int,
    total: int,
    z: float = 1.959963984540054,
) -> tuple[float | None, float | None]:
    if total == 0:
        return None, None
    probability = successes / total
    denominator = 1.0 + z * z / total
    center = (probability + z * z / (2.0 * total)) / denominator
    radius = (
        z
        * math.sqrt(probability * (1.0 - probability) / total + z * z / (4.0 * total * total))
        / denominator
    )
    return max(0.0, center - radius), min(1.0, center + radius)


def summarize_attempts(events: Iterable[Mapping[str, Any]]) -> tuple[AttemptSummary, ...]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in events:
        row = _validate_event(raw)
        grouped[row["attempt_id"]].append(row)

    summaries: list[AttemptSummary] = []
    for attempt_id, rows in grouped.items():
        rows.sort(key=lambda row: (float(row["elapsed_ms"]), row["wall_time"]))
        by_event = {row["event"]: row for row in rows}
        created = by_event.get("attempt_created", {})
        decision = by_event.get("decision")
        sent = by_event.get("sent")
        landed = by_event.get("landed")
        reconciled = by_event.get("reconciled")
        terminal = by_event.get("terminal_error")
        expired = "expired" in by_event
        landed_error = bool(landed and landed.get("err") is not None)
        fee = _integer(reconciled or {}, "actual_fee_lamports")
        if fee is None:
            fee = 0
        signature = None
        for candidate in (reconciled, landed, sent, by_event.get("built_signed")):
            if candidate and isinstance(candidate.get("signature"), str):
                signature = candidate["signature"]
                break
        summaries.append(
            AttemptSummary(
                attempt_id=attempt_id,
                purpose=created.get("purpose") if isinstance(created.get("purpose"), str) else None,
                submit_decision=bool(decision and decision.get("action") == "submit"),
                pre_send_failure="pre_send_failure" in by_event,
                sent=sent is not None,
                landed=landed is not None,
                reconciled=reconciled is not None,
                expired=expired,
                landed_error=landed_error,
                signature=signature,
                send_to_land_ms=_number(landed or {}, "send_to_land_ms"),
                fee_lamports=fee,
                compute_units=_integer(reconciled or {}, "actual_compute_units"),
                compute_unit_limit=_integer(by_event.get("simulated", {}), "compute_unit_limit"),
                compute_unit_price_micro_lamports=_integer(
                    by_event.get("simulated", {}),
                    "compute_unit_price_micro_lamports",
                ),
                terminal_error=terminal.get("error")
                if terminal and isinstance(terminal.get("error"), str)
                else None,
            )
        )
    return tuple(sorted(summaries, key=lambda summary: summary.attempt_id))


def summarize_calibration(
    events: Iterable[Mapping[str, Any]],
) -> CalibrationSummary:
    attempts = summarize_attempts(events)
    submit_decisions = [attempt for attempt in attempts if attempt.submit_decision]
    sent = [attempt for attempt in attempts if attempt.sent]
    landed = [attempt for attempt in sent if attempt.landed]
    landed_success = [attempt for attempt in landed if not attempt.landed_error]
    latencies = [
        attempt.send_to_land_ms for attempt in landed if attempt.send_to_land_ms is not None
    ]
    fees = [attempt.fee_lamports for attempt in landed if attempt.fee_lamports > 0]
    compute = [attempt.compute_units for attempt in landed if attempt.compute_units is not None]
    return CalibrationSummary(
        attempt_count=len(attempts),
        submit_decision_count=len(submit_decisions),
        pre_send_failure_count=sum(attempt.pre_send_failure for attempt in attempts),
        sent_count=len(sent),
        landed_count=len(landed),
        landed_success_count=len(landed_success),
        landed_failure_count=len(landed) - len(landed_success),
        expired_count=sum(attempt.expired for attempt in attempts),
        reconciled_count=sum(attempt.reconciled for attempt in attempts),
        submission_probability=(
            sum(attempt.sent for attempt in submit_decisions) / len(submit_decisions)
            if submit_decisions
            else None
        ),
        landing_probability=len(landed) / len(sent) if sent else None,
        landed_success_probability=len(landed_success) / len(sent) if sent else None,
        latency_median_ms=median(latencies) if latencies else None,
        latency_mean_ms=fmean(latencies) if latencies else None,
        latency_p90_ms=_percentile(latencies, 0.90),
        latency_p99_ms=_percentile(latencies, 0.99),
        total_onchain_fee_lamports=sum(attempt.fee_lamports for attempt in attempts),
        fee_median_lamports=median(fees) if fees else None,
        compute_median_units=median(compute) if compute else None,
    )


def summarize_calibration_arms(
    events: Iterable[Mapping[str, Any]],
) -> tuple[CalibrationArmSummary, ...]:
    rows = tuple(_validate_event(event) for event in events)
    attempts = {attempt.attempt_id: attempt for attempt in summarize_attempts(rows)}
    decisions: dict[str, Mapping[str, Any]] = {
        row["attempt_id"]: row
        for row in rows
        if row["event"] == "decision" and row.get("action") == "submit"
    }
    grouped: dict[tuple[str, int], list[AttemptSummary]] = defaultdict(list)
    for attempt_id, decision in decisions.items():
        attempt = attempts[attempt_id]
        if attempt.purpose not in {
            "pumpswap_calibration_buy",
            "pumpswap_calibration_sell",
        }:
            continue
        price = _integer(decision, "selected_micro_lamports")
        if price is None:
            continue
        grouped[(attempt.purpose, price)].append(attempt)

    output: list[CalibrationArmSummary] = []
    for (purpose, price), arm in sorted(grouped.items()):
        sent = [attempt for attempt in arm if attempt.sent]
        landed = [attempt for attempt in sent if attempt.landed]
        successes = [attempt for attempt in landed if not attempt.landed_error]
        latencies = [
            attempt.send_to_land_ms for attempt in landed if attempt.send_to_land_ms is not None
        ]
        fees = [attempt.fee_lamports for attempt in landed if attempt.fee_lamports > 0]
        low, high = _wilson_interval(len(landed), len(sent))
        output.append(
            CalibrationArmSummary(
                purpose=purpose,
                compute_unit_price_micro_lamports=price,
                submit_decisions=len(arm),
                pre_send_failures=sum(attempt.pre_send_failure for attempt in arm),
                sent=len(sent),
                landed=len(landed),
                landed_successes=len(successes),
                submission_probability=len(sent) / len(arm),
                landing_probability=len(landed) / len(sent) if sent else None,
                landing_wilson_low_95=low,
                landing_wilson_high_95=high,
                latency_median_ms=median(latencies) if latencies else None,
                latency_mean_ms=fmean(latencies) if latencies else None,
                latency_p90_ms=_percentile(latencies, 0.90),
                latency_p99_ms=_percentile(latencies, 0.99),
                fee_median_lamports=median(fees) if fees else None,
            )
        )
    return tuple(output)


def terminal_attempt_ids(events: Iterable[Mapping[str, Any]]) -> frozenset[str]:
    return frozenset(
        str(event["attempt_id"]) for event in events if event.get("event") in _TERMINAL_EVENTS
    )


__all__ = [
    "SCHEMA",
    "AttemptSummary",
    "CalibrationSummary",
    "CalibrationArmSummary",
    "ExecutionAttemptLedger",
    "ExecutionTelemetryError",
    "summarize_attempts",
    "summarize_calibration",
    "summarize_calibration_arms",
    "terminal_attempt_ids",
]
