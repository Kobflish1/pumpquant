"""Pure validation for the approved BigQuery discovery backfill artifact.

The approval JSON is evidence produced by a previous dry run.  This module
only parses and validates that evidence; it never imports a cloud client or
performs a network operation.  In particular, the plan hashes are recomputed
with the current offline planner before an artifact is accepted.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TypeAlias

from pumpquant.acquisition.bigquery.source import (
    DEFAULT_MARKET_PROGRAM_IDS,
    MAX_DAILY_BACKFILL_BYTES,
    MAX_SANDBOX_MONTHLY_BYTES,
    QueryPlan,
    build_daily_pump_instruction_plans,
    query_sha256,
    validate_plan_sql_provenance,
)

APPROVAL_ARTIFACT_VERSION = 1
APPROVAL_STATUS = "approved_dry_run_only"
APPROVAL_SOURCE_DATASET = "bigquery-public-data.crypto_solana_mainnet_us"
APPROVAL_SOURCE_LOCATION = "us-central1"
APPROVAL_PLAN_NAME = "pump_instruction_discovery_v1"
APPROVAL_PROGRAM_SCOPE = "Pump and PumpSwap"
APPROVAL_DAY_COUNT = 30
APPROVAL_FINALITY_BUFFER_DAYS = 7
APPROVED_WINDOW_START = date(2026, 6, 7)
APPROVED_WINDOW_END = date(2026, 7, 7)

_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_PROJECT_ID = re.compile(r"[a-z][a-z0-9-]{4,28}[a-z0-9]\Z")
_UTC_CAPTURED_AT = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z\Z"
)
_UTC_DAY = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")

JsonObject: TypeAlias = Mapping[str, object]
WindowValue: TypeAlias = str | date


class BigQueryApprovalError(ValueError):
    """Raised when an approval artifact is malformed or no longer valid."""


class BigQueryApprovalValidationError(BigQueryApprovalError):
    """Explicit name for callers that want to distinguish validation errors."""


@dataclass(frozen=True, slots=True)
class ApprovedDay:
    """One validated daily estimate and its freshly recomputed immutable plan."""

    start: str
    end: str
    estimated_bytes: int
    plan_sha256: str
    plan: QueryPlan


@dataclass(frozen=True, slots=True)
class BigQueryApproval:
    """Typed result returned after a complete artifact passes validation."""

    artifact_version: int
    status: str
    captured_at: str
    billing_project: str
    source_dataset: str
    source_location: str
    source_contract_sha256: str
    plan_name: str
    program_scope: str
    selected_start: str
    selected_end: str
    finality_buffer_days: int
    rejected_latest_window: Mapping[str, object]
    daily_maximum_bytes_billed: int
    cumulative_maximum_bytes_billed: int
    cumulative_estimated_bytes: int
    remaining_cumulative_bytes: int
    combined_plan_sha256: str
    query_executed: bool
    bytes_billed: int
    days: tuple[ApprovedDay, ...]

    @property
    def plans(self) -> tuple[QueryPlan, ...]:
        """Return plans in artifact order for downstream execution checks."""
        return tuple(day.plan for day in self.days)

    @property
    def plan_hashes(self) -> tuple[str, ...]:
        """Return validated daily plan hashes in artifact order."""
        return tuple(day.plan_sha256 for day in self.days)

    @property
    def selected_window(self) -> tuple[str, str]:
        """Return the selected half-open window as canonical UTC dates."""
        return self.selected_start, self.selected_end

    @property
    def window(self) -> tuple[str, str]:
        """Short alias for :attr:`selected_window`."""
        return self.selected_window


# A descriptive alias is convenient for callers that do not need the shorter
# class name.  Keep it as an alias (rather than a second class) for isinstance
# compatibility.
ApprovalValidationResult = BigQueryApproval
ApprovalArtifactError = BigQueryApprovalError
ApprovalArtifactValidationError = BigQueryApprovalValidationError


def _fail(message: str) -> BigQueryApprovalValidationError:
    return BigQueryApprovalValidationError(message)


def _object(value: object, label: str) -> JsonObject:
    if not isinstance(value, Mapping):
        raise _fail(f"{label} must be a JSON object")
    return value


def _exact_keys(value: JsonObject, expected: set[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        details: list[str] = []
        if missing:
            details.append(f"missing {', '.join(missing)}")
        if extra:
            details.append(f"unexpected {', '.join(extra)}")
        suffix = "; ".join(details) if details else "different field set"
        raise _fail(f"{label} has an invalid field set ({suffix})")


def _text(value: object, label: str, *, pattern: re.Pattern[str] | None = None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(f"{label} must be a non-empty string")
    if pattern is not None and pattern.fullmatch(value) is None:
        raise _fail(f"{label} has an invalid format")
    return value


def _integer(value: object, label: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _fail(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise _fail(f"{label} must be at least {minimum}")
    return value


def _boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise _fail(f"{label} must be a boolean")
    return value


def _utc_day(value: object, label: str) -> date:
    text = _text(value, label, pattern=_UTC_DAY)
    try:
        return date.fromisoformat(text)
    except ValueError as error:
        raise _fail(f"{label} is not a valid UTC calendar date") from error


def _window_day(value: WindowValue, label: str) -> tuple[str, date]:
    if isinstance(value, date) and not isinstance(value, datetime):
        parsed = value
        return parsed.isoformat(), parsed
    if not isinstance(value, str):
        raise _fail(f"{label} must be a YYYY-MM-DD date")
    parsed = _utc_day(value, label)
    return value, parsed


def _captured_at(value: object) -> str:
    text = _text(value, "captured_at", pattern=_UTC_CAPTURED_AT)
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as error:
        raise _fail("captured_at is not a valid UTC timestamp") from error
    if parsed.tzinfo != UTC or parsed.utcoffset() != timedelta(0):
        raise _fail("captured_at must use the UTC Z designator")
    return text


def _validate_window(
    selected_window: JsonObject,
    *,
    expected_start: WindowValue,
    expected_end: WindowValue,
) -> tuple[str, str, date, date, int]:
    _exact_keys(
        selected_window,
        {"start", "end", "complete_utc_days", "finality_buffer_days"},
        "selected_window",
    )
    start_text = _text(selected_window["start"], "selected_window.start", pattern=_UTC_DAY)
    end_text = _text(selected_window["end"], "selected_window.end", pattern=_UTC_DAY)
    start = _utc_day(start_text, "selected_window.start")
    end = _utc_day(end_text, "selected_window.end")
    expected_start_text, expected_start_day = _window_day(expected_start, "expected_start")
    expected_end_text, expected_end_day = _window_day(expected_end, "expected_end")
    if (start_text, end_text) != (expected_start_text, expected_end_text):
        raise _fail(
            "selected_window does not match the expected window: "
            f"expected [{expected_start_text}, {expected_end_text}), "
            f"got [{start_text}, {end_text})"
        )
    if end <= start:
        raise _fail("selected_window.end must be later than selected_window.start")
    duration = (end - start).days
    if duration != APPROVAL_DAY_COUNT:
        raise _fail(f"selected_window must contain exactly {APPROVAL_DAY_COUNT} UTC days")
    if (expected_end_day - expected_start_day).days != APPROVAL_DAY_COUNT:
        raise _fail(f"expected window must contain exactly {APPROVAL_DAY_COUNT} UTC days")
    complete_days = _integer(
        selected_window["complete_utc_days"], "selected_window.complete_utc_days", minimum=0
    )
    if complete_days != APPROVAL_DAY_COUNT:
        raise _fail(f"selected_window.complete_utc_days must be {APPROVAL_DAY_COUNT}")
    finality_days = _integer(
        selected_window["finality_buffer_days"], "selected_window.finality_buffer_days", minimum=0
    )
    if finality_days != APPROVAL_FINALITY_BUFFER_DAYS:
        raise _fail(f"selected_window.finality_buffer_days must be {APPROVAL_FINALITY_BUFFER_DAYS}")
    return start_text, end_text, start, end, finality_days


def _validate_rejected_window(
    value: object,
    *,
    selected_start: date,
    selected_end: date,
    cumulative_cap: int,
) -> Mapping[str, object]:
    window = _object(value, "rejected_latest_window")
    _exact_keys(window, {"start", "end", "estimated_bytes", "reason"}, "rejected_latest_window")
    start_text = _text(window["start"], "rejected_latest_window.start", pattern=_UTC_DAY)
    end_text = _text(window["end"], "rejected_latest_window.end", pattern=_UTC_DAY)
    start = _utc_day(start_text, "rejected_latest_window.start")
    end = _utc_day(end_text, "rejected_latest_window.end")
    if end <= start or (end - start).days != APPROVAL_DAY_COUNT:
        raise _fail("rejected_latest_window must contain exactly 30 UTC days")
    # The rejected candidate is the next 30-day window after the approved one.
    if start != selected_start + timedelta(days=7) or end != selected_end + timedelta(days=7):
        raise _fail("rejected_latest_window is not the expected latest candidate window")
    estimate = _integer(
        window["estimated_bytes"], "rejected_latest_window.estimated_bytes", minimum=1
    )
    if estimate <= cumulative_cap:
        raise _fail("rejected_latest_window estimate must exceed the cumulative cap")
    _text(window["reason"], "rejected_latest_window.reason")
    return window


def _validate_plan(plan: QueryPlan, *, start: str, end: str, daily_cap: int) -> None:
    """Validate all immutable plan components, not only the derived hash."""
    if plan.name != APPROVAL_PLAN_NAME:
        raise _fail(f"daily plan has unexpected name {plan.name!r}")
    if plan.maximum_bytes_billed != daily_cap:
        raise _fail("daily plan has a different maximum_bytes_billed")
    if plan.location != APPROVAL_SOURCE_LOCATION:
        raise _fail("daily plan has an unexpected location")
    expected_tables = ("bigquery-public-data.crypto_solana_mainnet_us.Instructions",)
    if plan.referenced_tables != expected_tables:
        raise _fail("daily plan has unexpected referenced tables")
    try:
        actual_tables = validate_plan_sql_provenance(plan)
    except (ValueError, RuntimeError) as error:
        raise _fail(f"daily plan SQL provenance is invalid: {error}") from error
    if actual_tables != expected_tables:
        raise _fail("daily plan SQL provenance has unexpected source tables")
    if "SELECT *" in plan.sql.upper():
        raise _fail("daily plan SQL must not contain SELECT *")
    if plan.sql.count("@start_ts") < 1 or plan.sql.count("@end_ts") < 1:
        raise _fail("daily plan SQL does not bind both half-open UTC boundaries")
    if "program_id IN UNNEST(@program_ids)" not in plan.sql:
        raise _fail("daily plan SQL does not bind the canonical program IDs")
    parameters = plan.parameters
    if tuple(item.name for item in parameters) != ("start_ts", "end_ts", "program_ids"):
        raise _fail("daily plan parameters are not the canonical ordered set")
    if parameters[0].type_name != "TIMESTAMP" or parameters[0].value != f"{start}T00:00:00Z":
        raise _fail("daily plan start_ts parameter is invalid")
    if parameters[1].type_name != "TIMESTAMP" or parameters[1].value != f"{end}T00:00:00Z":
        raise _fail("daily plan end_ts parameter is invalid")
    if (
        parameters[2].type_name != "ARRAY<STRING>"
        or parameters[2].value != DEFAULT_MARKET_PROGRAM_IDS
    ):
        raise _fail("daily plan program_ids parameter is not the canonical default")


def validate_approval_artifact(
    payload: JsonObject,
    *,
    expected_start: WindowValue = APPROVED_WINDOW_START,
    expected_end: WindowValue = APPROVED_WINDOW_END,
    expected_window: tuple[WindowValue, WindowValue] | None = None,
) -> BigQueryApproval:
    """Strictly validate one complete 30-day dry-run approval JSON object.

    Validation is deterministic and offline.  ``expected_start`` and
    ``expected_end`` default to the registered approved window, but callers may
    provide another exact 30-day UTC window when validating a separately
    generated artifact.  The planner is always invoked with the canonical
    default program IDs and the fixed 64 GiB daily cap.
    """
    if expected_window is not None:
        if len(expected_window) != 2:
            raise _fail("expected_window must contain exactly (start, end)")
        if (expected_start, expected_end) != (
            APPROVED_WINDOW_START,
            APPROVED_WINDOW_END,
        ):
            raise _fail("pass either expected_window or expected_start/expected_end, not both")
        expected_start, expected_end = expected_window
    root = _object(payload, "approval artifact")
    _exact_keys(
        root,
        {
            "artifact_version",
            "status",
            "captured_at",
            "billing_project",
            "source_dataset",
            "source_location",
            "source_contract_sha256",
            "plan_name",
            "program_scope",
            "selected_window",
            "rejected_latest_window",
            "daily_maximum_bytes_billed",
            "cumulative_maximum_bytes_billed",
            "cumulative_estimated_bytes",
            "remaining_cumulative_bytes",
            "combined_plan_sha256",
            "query_executed",
            "bytes_billed",
            "days",
        },
        "approval artifact",
    )
    version = _integer(root["artifact_version"], "artifact_version", minimum=0)
    if version != APPROVAL_ARTIFACT_VERSION:
        raise _fail(f"artifact_version must be {APPROVAL_ARTIFACT_VERSION}")
    status = _text(root["status"], "status")
    if status != APPROVAL_STATUS:
        raise _fail(f"status must be {APPROVAL_STATUS!r}")
    captured_at = _captured_at(root["captured_at"])
    billing_project = _text(root["billing_project"], "billing_project", pattern=_PROJECT_ID)
    source_dataset = _text(root["source_dataset"], "source_dataset")
    if source_dataset != APPROVAL_SOURCE_DATASET:
        raise _fail(f"source_dataset must be {APPROVAL_SOURCE_DATASET!r}")
    source_location = _text(root["source_location"], "source_location")
    if source_location != APPROVAL_SOURCE_LOCATION:
        raise _fail(f"source_location must be {APPROVAL_SOURCE_LOCATION!r}")
    source_contract = _text(
        root["source_contract_sha256"], "source_contract_sha256", pattern=_HEX64
    )
    plan_name = _text(root["plan_name"], "plan_name")
    if plan_name != APPROVAL_PLAN_NAME:
        raise _fail(f"plan_name must be {APPROVAL_PLAN_NAME!r}")
    program_scope = _text(root["program_scope"], "program_scope")
    if program_scope != APPROVAL_PROGRAM_SCOPE:
        raise _fail(f"program_scope must be {APPROVAL_PROGRAM_SCOPE!r}")
    selected = _object(root["selected_window"], "selected_window")
    start_text, end_text, start, end, finality_days = _validate_window(
        selected, expected_start=expected_start, expected_end=expected_end
    )
    daily_cap = _integer(
        root["daily_maximum_bytes_billed"],
        "daily_maximum_bytes_billed",
        minimum=1,
    )
    if daily_cap != MAX_DAILY_BACKFILL_BYTES:
        raise _fail(f"daily_maximum_bytes_billed must be {MAX_DAILY_BACKFILL_BYTES}")
    cumulative_cap = _integer(
        root["cumulative_maximum_bytes_billed"],
        "cumulative_maximum_bytes_billed",
        minimum=1,
    )
    if cumulative_cap != MAX_SANDBOX_MONTHLY_BYTES:
        raise _fail(f"cumulative_maximum_bytes_billed must be {MAX_SANDBOX_MONTHLY_BYTES}")
    rejected = _validate_rejected_window(
        root["rejected_latest_window"],
        selected_start=start,
        selected_end=end,
        cumulative_cap=cumulative_cap,
    )
    combined_hash = _text(root["combined_plan_sha256"], "combined_plan_sha256", pattern=_HEX64)
    query_executed = _boolean(root["query_executed"], "query_executed")
    if query_executed:
        raise _fail("approval artifact must record query_executed=false")
    bytes_billed = _integer(root["bytes_billed"], "bytes_billed", minimum=0)
    if bytes_billed != 0:
        raise _fail("approval artifact must record bytes_billed=0")

    raw_days = root["days"]
    if not isinstance(raw_days, list):
        raise _fail("days must be a JSON array")
    if len(raw_days) != APPROVAL_DAY_COUNT:
        raise _fail(f"days must contain exactly {APPROVAL_DAY_COUNT} entries")
    plans = build_daily_pump_instruction_plans(
        start_text,
        end_text,
        maximum_bytes_billed=MAX_DAILY_BACKFILL_BYTES,
        program_ids=DEFAULT_MARKET_PROGRAM_IDS,
    )
    if len(plans) != APPROVAL_DAY_COUNT:  # defensive check if planner changes
        raise _fail("canonical planner did not produce exactly 30 daily plans")

    approved_days: list[ApprovedDay] = []
    estimates: list[int] = []
    seen_starts: set[str] = set()
    seen_hashes: set[str] = set()
    for index, (raw_day, plan) in enumerate(zip(raw_days, plans, strict=True)):
        day = _object(raw_day, f"days[{index}]")
        _exact_keys(day, {"start", "end", "estimated_bytes", "plan_sha256"}, f"days[{index}]")
        day_start_text = _text(day["start"], f"days[{index}].start", pattern=_UTC_DAY)
        day_end_text = _text(day["end"], f"days[{index}].end", pattern=_UTC_DAY)
        day_start = _utc_day(day_start_text, f"days[{index}].start")
        day_end = _utc_day(day_end_text, f"days[{index}].end")
        expected_day_start = start + timedelta(days=index)
        expected_day_end = expected_day_start + timedelta(days=1)
        if (day_start, day_end) != (expected_day_start, expected_day_end):
            raise _fail(
                f"days[{index}] is not the contiguous half-open day "
                f"[{expected_day_start.isoformat()}, {expected_day_end.isoformat()})"
            )
        if day_start_text in seen_starts:
            raise _fail(f"days[{index}] duplicates a prior day")
        seen_starts.add(day_start_text)
        estimate = _integer(day["estimated_bytes"], f"days[{index}].estimated_bytes", minimum=1)
        if estimate > MAX_DAILY_BACKFILL_BYTES:
            raise _fail(f"days[{index}].estimated_bytes exceeds the 64 GiB daily cap")
        plan_hash = _text(day["plan_sha256"], f"days[{index}].plan_sha256", pattern=_HEX64)
        if plan_hash in seen_hashes:
            raise _fail(f"days[{index}] duplicates a prior plan hash")
        seen_hashes.add(plan_hash)
        _validate_plan(plan, start=day_start_text, end=day_end_text, daily_cap=daily_cap)
        current_hash = query_sha256(plan)
        if plan_hash != current_hash:
            raise _fail(f"days[{index}].plan_sha256 does not match the current canonical plan")
        approved_days.append(ApprovedDay(day_start_text, day_end_text, estimate, plan_hash, plan))
        estimates.append(estimate)

    cumulative_estimated = _integer(
        root["cumulative_estimated_bytes"], "cumulative_estimated_bytes", minimum=1
    )
    expected_cumulative = sum(estimates)
    if cumulative_estimated != expected_cumulative:
        raise _fail("cumulative_estimated_bytes does not equal the sum of daily estimates")
    if cumulative_estimated > cumulative_cap:
        raise _fail("cumulative_estimated_bytes exceeds the cumulative cap")
    remaining = _integer(
        root["remaining_cumulative_bytes"], "remaining_cumulative_bytes", minimum=0
    )
    expected_remaining = cumulative_cap - cumulative_estimated
    if remaining != expected_remaining:
        raise _fail("remaining_cumulative_bytes does not match the cumulative cap")

    return BigQueryApproval(
        artifact_version=version,
        status=status,
        captured_at=captured_at,
        billing_project=billing_project,
        source_dataset=source_dataset,
        source_location=source_location,
        source_contract_sha256=source_contract,
        plan_name=plan_name,
        program_scope=program_scope,
        selected_start=start_text,
        selected_end=end_text,
        finality_buffer_days=finality_days,
        rejected_latest_window=rejected,
        daily_maximum_bytes_billed=daily_cap,
        cumulative_maximum_bytes_billed=cumulative_cap,
        cumulative_estimated_bytes=cumulative_estimated,
        remaining_cumulative_bytes=remaining,
        combined_plan_sha256=combined_hash,
        query_executed=query_executed,
        bytes_billed=bytes_billed,
        days=tuple(approved_days),
    )


def load_approval_artifact(
    path: str | Path,
    *,
    expected_start: WindowValue = APPROVED_WINDOW_START,
    expected_end: WindowValue = APPROVED_WINDOW_END,
    expected_window: tuple[WindowValue, WindowValue] | None = None,
) -> BigQueryApproval:
    """Read a local JSON artifact and apply :func:`validate_approval_artifact`."""
    target = Path(path)
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BigQueryApprovalValidationError(
            "approval artifact is not valid readable JSON"
        ) from error
    return validate_approval_artifact(
        _object(raw, "approval artifact"),
        expected_start=expected_start,
        expected_end=expected_end,
        expected_window=expected_window,
    )


# Names suitable for integrations that use a ``bigquery_approval`` prefix.
validate_bigquery_approval = validate_approval_artifact
load_bigquery_approval = load_approval_artifact
validate_bigquery_approval_artifact = validate_approval_artifact
load_bigquery_approval_artifact = load_approval_artifact
validate_approval_file = load_approval_artifact


__all__ = [
    "APPROVAL_ARTIFACT_VERSION",
    "APPROVAL_DAY_COUNT",
    "APPROVAL_FINALITY_BUFFER_DAYS",
    "APPROVAL_PLAN_NAME",
    "APPROVAL_PROGRAM_SCOPE",
    "APPROVAL_SOURCE_DATASET",
    "APPROVAL_SOURCE_LOCATION",
    "APPROVAL_STATUS",
    "ApprovalArtifactError",
    "ApprovalArtifactValidationError",
    "APPROVED_WINDOW_END",
    "APPROVED_WINDOW_START",
    "ApprovalValidationResult",
    "ApprovedDay",
    "BigQueryApproval",
    "BigQueryApprovalError",
    "BigQueryApprovalValidationError",
    "load_approval_artifact",
    "load_bigquery_approval",
    "load_bigquery_approval_artifact",
    "validate_approval_file",
    "validate_approval_artifact",
    "validate_bigquery_approval",
    "validate_bigquery_approval_artifact",
]
