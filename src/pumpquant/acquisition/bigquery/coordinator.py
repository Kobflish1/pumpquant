"""Fail-closed, exactly-once submission coordination for approved BigQuery days.

The normal archive command reads every query result page.  This coordinator is
deliberately a different, bounded phase: it validates the existing approval,
rechecks the source contract, performs *all* non-billable dry runs, reconciles
durable execution state, and only then submits the still-unidentified days.
It never calls ``jobs.getQueryResults`` or ``tabledata.list``.  A successful
submission is followed by a metadata-only ``jobs.get`` so a downstream
Storage Read consumer has the destination table, schema, row count, ETag, and
actual processed-byte evidence it needs.

No POST is retried.  Before a POST is attempted the day is atomically marked
``submission_attempt_started`` in one global ledger.  A crash or transport
failure after that point is therefore a reconciliation condition,
not an invitation to submit the same query a second time.  The stable
``query_request_id`` derived from the exact execution plan is retained for
BigQuery's request-id idempotency window.

The module is dependency-free apart from the already existing planner,
approval, source-contract, and durable-state modules.  Tests can provide a
small fake backend and never need Google credentials.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, TypeAlias, cast

from pumpquant.acquisition.bigquery.approval import (
    ApprovedDay,
    BigQueryApproval,
    load_approval_artifact,
)
from pumpquant.acquisition.bigquery.archive import load_archive_manifest
from pumpquant.acquisition.bigquery.source import (
    BackendDryRun,
    DatasetRef,
    DatasetSnapshot,
    DryRunResult,
    QueryPlan,
    audit_dataset,
    dataset_schema_sha256,
    dry_run,
)
from pumpquant.core.durable_state import atomic_private_json_write

COORDINATOR_VERSION = 1
DEFAULT_COORDINATOR_WORKERS = 8
MAX_COORDINATOR_WORKERS = 32

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_JOB_ID = re.compile(r"^[A-Za-z0-9_-]{1,1024}$")

JSONMapping: TypeAlias = Mapping[str, object]


class CoordinatorError(RuntimeError):
    """Raised when the coordinator cannot prove a safe exactly-once run."""


class CoordinatorPreflightError(CoordinatorError):
    """Raised before any non-dry-run POST is allowed."""


class CoordinatorSubmissionError(CoordinatorError):
    """Raised when one or more already-marked submissions need reconciliation."""


@dataclass(frozen=True, slots=True)
class JobMetadata:
    """Metadata-only evidence for one BigQuery job.

    ``schema`` and ``destination_table`` retain REST mappings;
    no query rows are ever accepted by this type.  ``actual_bytes_processed``
    may be zero for a valid empty result, while all missing values are kept as
    ``None`` until a completed ``jobs.get`` proves them.
    """

    job_id: str
    state: str
    destination_table: Mapping[str, object] | None = None
    schema: Mapping[str, object] | None = None
    num_rows: int | None = None
    etag: str | None = None
    actual_bytes_processed: int | None = None
    total_bytes_billed: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.job_id, str) or _JOB_ID.fullmatch(self.job_id) is None:
            raise ValueError("job_id has an invalid format")
        if not isinstance(self.state, str) or not self.state.strip():
            raise ValueError("job state must be non-empty text")
        for label, value in (
            ("num_rows", self.num_rows),
            ("actual_bytes_processed", self.actual_bytes_processed),
            ("total_bytes_billed", self.total_bytes_billed),
        ):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"{label} must be a non-negative integer or None")
        if self.etag is not None and (not isinstance(self.etag, str) or not self.etag):
            raise ValueError("etag must be non-empty text or None")
        if self.destination_table is not None and not isinstance(self.destination_table, Mapping):
            raise TypeError("destination_table must be a mapping or None")
        if self.schema is not None and not isinstance(self.schema, Mapping):
            raise TypeError("schema must be a mapping or None")

    @property
    def done(self) -> bool:
        return self.state.upper() == "DONE"

    def to_payload(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "state": self.state,
            "destination_table": (
                None if self.destination_table is None else dict(self.destination_table)
            ),
            "schema": None if self.schema is None else dict(self.schema),
            "num_rows": self.num_rows,
            "etag": self.etag,
            "actual_bytes_processed": self.actual_bytes_processed,
            "total_bytes_billed": self.total_bytes_billed,
        }


class CoordinatorBackend(Protocol):
    """Backend surface used by :func:`coordinate_approved_days`.

    ``submit_query`` must issue exactly one read-only ``jobs.query`` POST and
    must not retry it.  ``get_job_metadata`` must use GET-only metadata calls;
    it must never retrieve query rows.
    """

    def inspect_dataset(self, ref: DatasetRef) -> DatasetSnapshot: ...

    def estimate_query(self, plan: QueryPlan) -> BackendDryRun: ...

    def submit_query(self, plan: QueryPlan, approval: DryRunResult) -> str: ...

    def get_job_metadata(
        self, plan: QueryPlan, job_id: str
    ) -> JobMetadata | Mapping[str, object]: ...


ArchiveLookup: TypeAlias = Callable[[ApprovedDay], Mapping[str, object] | None]


@dataclass(frozen=True, slots=True)
class CoordinatedDay:
    """The plan and current durable state for one UTC day."""

    start: str
    end: str
    plan_sha256: str
    request_id: str
    estimated_bytes: int
    maximum_bytes_billed: int
    status: str
    job_id: str | None = None
    job_metadata: JobMetadata | None = None
    actual_bytes_processed: int | None = None
    manifest_sha256: str | None = None

    def to_payload(self) -> dict[str, object]:
        value = asdict(self)
        value["job_metadata"] = (
            None if self.job_metadata is None else self.job_metadata.to_payload()
        )
        return value


@dataclass(frozen=True, slots=True)
class CoordinationResult:
    """Serialisable result of preflight or execution coordination."""

    status: str
    execute: bool
    approval_file_sha256: str
    combined_plan_sha256: str
    billing_project: str
    source_contract_sha256: str
    cumulative_maximum_bytes_billed: int
    cumulative_actual_bytes: int
    cumulative_estimated_pending_bytes: int
    dry_runs: tuple[DryRunResult, ...]
    days: tuple[CoordinatedDay, ...]
    ledger_path: str

    @property
    def submitted(self) -> tuple[CoordinatedDay, ...]:
        return tuple(day for day in self.days if day.status in {"job_identified", "submitted"})

    def to_payload(self) -> dict[str, object]:
        return {
            "status": self.status,
            "execute": self.execute,
            "approval_file_sha256": self.approval_file_sha256,
            "combined_plan_sha256": self.combined_plan_sha256,
            "billing_project": self.billing_project,
            "source_contract_sha256": self.source_contract_sha256,
            "cumulative_maximum_bytes_billed": self.cumulative_maximum_bytes_billed,
            "cumulative_actual_bytes": self.cumulative_actual_bytes,
            "cumulative_estimated_pending_bytes": self.cumulative_estimated_pending_bytes,
            "dry_runs": [asdict(item) for item in self.dry_runs],
            "days": [day.to_payload() for day in self.days],
            "ledger_path": self.ledger_path,
            "submitted_days": [day.plan_sha256 for day in self.submitted],
        }


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _canonical_file_sha256(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise CoordinatorPreflightError("approval artifact must be a regular non-symlink file")
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CoordinatorPreflightError("approval artifact is not readable JSON") from error
    if not isinstance(value, Mapping):
        raise CoordinatorPreflightError("approval artifact must contain a JSON object")
    encoded = (json.dumps(dict(value), sort_keys=True, separators=(",", ":")) + "\n").encode()
    return hashlib.sha256(encoded).hexdigest()


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    atomic_private_json_write(path, payload, exclusive=False)


def _read_json(path: Path, description: str) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise CoordinatorPreflightError(f"{description} must be a regular non-symlink file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CoordinatorPreflightError(f"{description} is not valid JSON") from error
    if not isinstance(value, dict):
        raise CoordinatorPreflightError(f"{description} must contain a JSON object")
    return cast(dict[str, object], value)


@contextmanager
def _coordinator_lock(output_base: Path) -> Iterator[None]:
    lock_path = output_base / ".coordinator" / ".lock"
    lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _validate_workers(workers: int) -> int:
    if isinstance(workers, bool) or not isinstance(workers, int):
        raise ValueError("workers must be an integer")
    if not 1 <= workers <= MAX_COORDINATOR_WORKERS:
        raise ValueError(f"workers must be between 1 and {MAX_COORDINATOR_WORKERS}")
    return workers


def _as_nonnegative_int(value: object, label: str, *, allow_none: bool = False) -> int | None:
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise CoordinatorPreflightError(f"{label} must be a non-negative integer")
    if isinstance(value, str):
        if not value.isdigit():
            raise CoordinatorPreflightError(f"{label} must be a non-negative integer")
        parsed = int(value)
    else:
        parsed = value
    if parsed < 0:
        raise CoordinatorPreflightError(f"{label} must be a non-negative integer")
    return parsed


def _normalize_metadata(
    value: JobMetadata | Mapping[str, object], expected_job_id: str
) -> JobMetadata:
    if isinstance(value, JobMetadata):
        metadata = value
    elif isinstance(value, Mapping):

        def first(*keys: str) -> object:
            for key in keys:
                if key in value:
                    return value[key]
            return None

        raw_job_id = first("job_id", "jobId") or expected_job_id
        raw_state = first("state", "job_state") or "UNKNOWN"
        destination = first("destination_table", "destinationTable")
        schema = first("schema")
        raw_rows = first("num_rows", "numRows")
        raw_etag = first("etag", "eTag", "ETag")
        raw_actual = first("actual_bytes_processed", "total_bytes_processed", "totalBytesProcessed")
        raw_billed = first("total_bytes_billed", "totalBytesBilled")
        if destination is not None and not isinstance(destination, Mapping):
            raise CoordinatorPreflightError("jobs.get destinationTable is invalid")
        if schema is not None and not isinstance(schema, Mapping):
            raise CoordinatorPreflightError("jobs.get schema is invalid")
        metadata = JobMetadata(
            str(raw_job_id),
            str(raw_state),
            None if destination is None else dict(destination),
            None if schema is None else dict(schema),
            _as_nonnegative_int(raw_rows, "jobs.get numRows", allow_none=True),
            None if raw_etag is None else str(raw_etag),
            _as_nonnegative_int(raw_actual, "jobs.get actual bytes", allow_none=True),
            _as_nonnegative_int(raw_billed, "jobs.get billed bytes", allow_none=True),
        )
    else:
        raise CoordinatorPreflightError("jobs.get metadata must be an object")
    if metadata.job_id != expected_job_id:
        raise CoordinatorPreflightError("jobs.get returned a different job ID")
    return metadata


def _require_done_metadata(metadata: JobMetadata, *, maximum_bytes_billed: int) -> None:
    if not metadata.done:
        return
    if metadata.destination_table is None:
        raise CoordinatorPreflightError("completed jobs.get omitted destinationTable")
    if metadata.schema is None:
        raise CoordinatorPreflightError("completed jobs.get omitted schema")
    if metadata.num_rows is None:
        raise CoordinatorPreflightError("completed jobs.get omitted numRows")
    if metadata.etag is None:
        raise CoordinatorPreflightError("completed destination omitted ETag")
    if metadata.actual_bytes_processed is None:
        raise CoordinatorPreflightError("completed jobs.get omitted actual byte statistics")
    if metadata.actual_bytes_processed > maximum_bytes_billed:
        raise CoordinatorPreflightError("completed job exceeded maximum_bytes_billed")


def _execution_cap_and_request_id(
    day: ApprovedDay,
    payload: Mapping[str, object] | None,
) -> tuple[int, str]:
    cap = day.plan.maximum_bytes_billed
    if payload is not None and payload.get("maximum_bytes_billed") is not None:
        raw_cap = payload.get("maximum_bytes_billed")
        if isinstance(raw_cap, bool) or not isinstance(raw_cap, int):
            raise CoordinatorPreflightError("execution ledger has an invalid query byte cap")
        cap = raw_cap
    if cap < day.estimated_bytes or cap > day.plan.maximum_bytes_billed:
        raise CoordinatorPreflightError("execution ledger byte cap is outside approved bounds")
    execution_plan = replace(day.plan, maximum_bytes_billed=cap)
    from pumpquant.acquisition.bigquery.client import query_request_id

    return cap, query_request_id(execution_plan)


def _validate_day_ledger(
    payload: Mapping[str, object],
    day: ApprovedDay,
    *,
    billing_project: str,
    source_contract_sha256: str,
) -> tuple[str, int, str, str | None, JobMetadata | None, str | None]:
    """Validate a per-day ledger without trusting any mutable derived field."""

    if payload.get("plan_sha256") != day.plan_sha256:
        raise CoordinatorPreflightError("execution ledger plan hash differs from approval")
    if payload.get("estimated_bytes") != day.estimated_bytes:
        raise CoordinatorPreflightError("execution ledger estimate differs from approval")
    if (
        payload.get("approved_maximum_bytes_billed", day.plan.maximum_bytes_billed)
        != day.plan.maximum_bytes_billed
    ):
        raise CoordinatorPreflightError("execution ledger approved cap differs from approval")
    if payload.get("billing_project", billing_project) != billing_project:
        raise CoordinatorPreflightError("execution ledger billing project differs from approval")
    if payload.get("source_contract_sha256", source_contract_sha256) != source_contract_sha256:
        raise CoordinatorPreflightError("execution ledger source contract differs from approval")
    cap, request_id = _execution_cap_and_request_id(day, payload)
    if payload.get("request_id") != request_id:
        raise CoordinatorPreflightError(
            "execution ledger request ID is not deterministic for the plan"
        )
    status = payload.get("status")
    if not isinstance(status, str) or not status:
        raise CoordinatorPreflightError("execution ledger has no status")
    if status in {"execution_attempt_started", "submission_attempt_started", "running"}:
        raise CoordinatorPreflightError(
            "execution ledger has an indeterminate submission; reconcile the "
            "BigQuery job before continuing"
        )
    if status not in {"job_identified", "completed"}:
        raise CoordinatorPreflightError("execution ledger has an unsupported status")
    raw_job_id = payload.get("job_id")
    if not isinstance(raw_job_id, str) or _JOB_ID.fullmatch(raw_job_id) is None:
        raise CoordinatorPreflightError("execution ledger has an invalid job ID")
    raw_actual = payload.get("actual_bytes_processed")
    actual = _as_nonnegative_int(raw_actual, "execution ledger actual bytes", allow_none=True)
    if actual is not None and actual > cap:
        raise CoordinatorPreflightError("execution ledger actual bytes exceed its execution cap")
    metadata_value = payload.get("job_metadata")
    metadata = None
    if metadata_value is not None:
        if not isinstance(metadata_value, Mapping):
            raise CoordinatorPreflightError("execution ledger job_metadata must be an object")
        metadata = _normalize_metadata(metadata_value, raw_job_id)
        _require_done_metadata(metadata, maximum_bytes_billed=cap)
        if metadata.actual_bytes_processed is not None:
            if actual is not None and actual != metadata.actual_bytes_processed:
                raise CoordinatorPreflightError(
                    "execution ledger differs from jobs.get byte evidence"
                )
            actual = metadata.actual_bytes_processed
    manifest_sha = payload.get("manifest_sha256")
    if manifest_sha is not None and (
        not isinstance(manifest_sha, str) or not _HEX64.fullmatch(manifest_sha)
    ):
        raise CoordinatorPreflightError("execution ledger has an invalid manifest hash")
    return status, cap, request_id, raw_job_id, metadata, manifest_sha


def _archive_evidence(
    output_base: Path,
    day: ApprovedDay,
    *,
    billing_project: str,
    source_contract_sha256: str,
) -> tuple[str, int, str, str, int] | None:
    """Read a trusted local audit manifest, if one exists.

    The ``.published`` tree is the local audit copy of the remote
    archive.  A day with that manifest is already represented remotely and is
    never submitted again.  Local work-in-progress manifests are
    ignored, so a crash cannot be mistaken for a published result.
    """

    path = output_base / ".published" / day.plan_sha256 / "manifest.json"
    if not path.exists():
        return None
    try:
        manifest = load_archive_manifest(path)
    except (OSError, ValueError, TypeError) as error:
        raise CoordinatorPreflightError("immutable archive audit manifest is invalid") from error
    provenance = manifest.provenance
    if provenance is None:
        raise CoordinatorPreflightError("immutable archive audit manifest has no provenance")
    expected_cap, expected_request_id = _execution_cap_and_request_id(day, None)
    actual = (
        provenance.plan_sha256 == day.plan_sha256
        and provenance.request_id == expected_request_id
        and provenance.plan_name == day.plan.name
        and provenance.billing_project == billing_project
        and provenance.location == day.plan.location
        and provenance.source_tables == day.plan.referenced_tables
        and provenance.source_contract_sha256 == source_contract_sha256
        and provenance.start == str(day.plan.parameters[0].value)
        and provenance.end == str(day.plan.parameters[1].value)
        and provenance.dry_run_estimated_bytes == day.estimated_bytes
        and provenance.maximum_bytes_billed == expected_cap
    )
    if not actual:
        raise CoordinatorPreflightError("immutable archive does not match the approved day")
    if provenance.actual_bytes_processed > expected_cap:
        raise CoordinatorPreflightError("immutable archive actual bytes exceed its execution cap")
    return (
        provenance.job_id,
        provenance.actual_bytes_processed,
        expected_request_id,
        manifest.manifest_sha256,
        provenance.total_rows,
    )


def _global_ledger_path(output_base: Path, approval_sha256: str) -> Path:
    return output_base / ".coordinator" / f"{approval_sha256}.json"


def _day_ledger_path(output_base: Path, plan_sha256: str) -> Path:
    return output_base / ".executions" / f"{plan_sha256}.json"


def _load_existing_global(path: Path, approval_sha256: str) -> dict[str, object] | None:
    if not path.exists():
        return None
    value = _read_json(path, "coordinator ledger")
    if value.get("approval_file_sha256") != approval_sha256:
        raise CoordinatorPreflightError(
            "coordinator ledger belongs to a different approval artifact"
        )
    if value.get("version") != COORDINATOR_VERSION:
        raise CoordinatorPreflightError("unsupported coordinator ledger version")
    days = value.get("days")
    if not isinstance(days, Mapping):
        raise CoordinatorPreflightError("coordinator ledger days must be an object")
    for state in days.values():
        if not isinstance(state, Mapping):
            raise CoordinatorPreflightError("coordinator ledger day state must be an object")
        if state.get("status") in {
            "submission_attempt_started",
            "execution_attempt_started",
            "running",
        }:
            raise CoordinatorPreflightError(
                "coordinator ledger has an indeterminate submission; reconcile before retrying"
            )
    return value


def _day_payload(
    day: ApprovedDay,
    *,
    cap: int,
    request_id: str,
    status: str,
    job_id: str | None = None,
    metadata: JobMetadata | None = None,
    actual: int | None = None,
    manifest_sha256: str | None = None,
) -> dict[str, object]:
    return {
        "start": day.start,
        "end": day.end,
        "plan_sha256": day.plan_sha256,
        "request_id": request_id,
        "estimated_bytes": day.estimated_bytes,
        "maximum_bytes_billed": cap,
        "approved_maximum_bytes_billed": day.plan.maximum_bytes_billed,
        "status": status,
        "job_id": job_id,
        "job_metadata": None if metadata is None else metadata.to_payload(),
        "actual_bytes_processed": actual,
        "manifest_sha256": manifest_sha256,
    }


def _state_payload(
    *,
    approval: BigQueryApproval,
    approval_sha256: str,
    source_contract_sha256: str,
    execute: bool,
    days: Mapping[str, Mapping[str, object]],
    status: str,
    cumulative_actual: int,
    cumulative_pending: int,
) -> dict[str, object]:
    return {
        "version": COORDINATOR_VERSION,
        "status": status,
        "approval_file_sha256": approval_sha256,
        "combined_plan_sha256": approval.combined_plan_sha256,
        "billing_project": approval.billing_project,
        "source_contract_sha256": source_contract_sha256,
        "selected_window": {"start": approval.selected_start, "end": approval.selected_end},
        "daily_maximum_bytes_billed": approval.daily_maximum_bytes_billed,
        "cumulative_maximum_bytes_billed": approval.cumulative_maximum_bytes_billed,
        "execute": execute,
        "cumulative_actual_bytes": cumulative_actual,
        "cumulative_estimated_pending_bytes": cumulative_pending,
        "updated_at": _utc_now(),
        "days": {key: dict(value) for key, value in sorted(days.items())},
    }


def _read_day_ledger(
    output_base: Path,
    day: ApprovedDay,
    *,
    billing_project: str,
    source_contract_sha256: str,
) -> tuple[str, int, str, str | None, JobMetadata | None, str | None] | None:
    path = _day_ledger_path(output_base, day.plan_sha256)
    if not path.exists():
        return None
    payload = _read_json(path, "execution ledger")
    return _validate_day_ledger(
        payload,
        day,
        billing_project=billing_project,
        source_contract_sha256=source_contract_sha256,
    )


def _metadata_call(backend: CoordinatorBackend, plan: QueryPlan, job_id: str) -> JobMetadata:
    try:
        value = backend.get_job_metadata(plan, job_id)
    except CoordinatorError:
        raise
    except Exception as error:
        raise CoordinatorPreflightError("metadata-only jobs.get failed") from error
    return _normalize_metadata(value, job_id)


def _source_contract(backend: CoordinatorBackend, approval: BigQueryApproval) -> str:
    try:
        snapshot = backend.inspect_dataset(DatasetRef())
    except Exception as error:
        raise CoordinatorPreflightError("source-contract metadata inspection failed") from error
    if not isinstance(snapshot, DatasetSnapshot):
        raise CoordinatorPreflightError("source-contract inspection returned invalid metadata")
    issues = audit_dataset(snapshot, required_tables=("Instructions",))
    errors = tuple(issue for issue in issues if issue.severity == "error")
    if errors:
        raise CoordinatorPreflightError("source-contract validation failed")
    actual = dataset_schema_sha256(snapshot)
    if actual != approval.source_contract_sha256:
        raise CoordinatorPreflightError("live source contract differs from approved artifact")
    return actual


def _plan_dry_runs(
    backend: CoordinatorBackend,
    approval: BigQueryApproval,
    ledger_info: Mapping[str, tuple[str, int, str, str | None, JobMetadata | None, str | None]],
    cap_overrides: Mapping[str, int] | None = None,
) -> tuple[tuple[DryRunResult, ...], dict[str, tuple[QueryPlan, DryRunResult, int, str]]]:
    results: list[DryRunResult] = []
    by_hash: dict[str, tuple[QueryPlan, DryRunResult, int, str]] = {}
    for day in approval.days:
        prior = ledger_info.get(day.plan_sha256)
        cap = day.plan.maximum_bytes_billed if prior is None else prior[1]
        if prior is None and cap_overrides is not None:
            cap = cap_overrides.get(day.plan_sha256, cap)
        execution_plan = replace(day.plan, maximum_bytes_billed=cap)
        try:
            result = dry_run(backend, execution_plan)
        except Exception as error:
            if isinstance(error, CoordinatorError):
                raise
            raise CoordinatorPreflightError(f"dry run failed for {day.start}") from error
        if result.estimated_bytes != day.estimated_bytes:
            raise CoordinatorPreflightError(
                f"dry-run estimate drifted for {day.start}: expected {day.estimated_bytes}, "
                f"got {result.estimated_bytes}"
            )
        from pumpquant.acquisition.bigquery.client import query_request_id

        request_id = query_request_id(execution_plan)
        if prior is not None and prior[2] != request_id:
            raise CoordinatorPreflightError("ledger request ID differs from refreshed dry-run plan")
        results.append(result)
        by_hash[day.plan_sha256] = (execution_plan, result, cap, request_id)
    if sum(item.estimated_bytes for item in results) != approval.cumulative_estimated_bytes:
        raise CoordinatorPreflightError(
            "all daily dry-run estimates do not match approved cumulative estimate"
        )
    return tuple(results), by_hash


_LedgerEvidence: TypeAlias = tuple[str, int, str, str | None, JobMetadata | None, str | None]
_ArchiveEvidence: TypeAlias = tuple[str, int, str, str, int]
_Planned: TypeAlias = dict[str, tuple[QueryPlan, DryRunResult, int, str]]


def _lookup_archive(day: ApprovedDay, remote: Mapping[str, object]) -> _ArchiveEvidence:
    """Bind a trusted archive lookup's identity and accounting before using it."""
    job_id = remote.get("job_id")
    if not isinstance(job_id, str) or _JOB_ID.fullmatch(job_id) is None:
        raise CoordinatorPreflightError("immutable archive lookup returned invalid job ID")
    parsed_actual = _as_nonnegative_int(
        remote.get("actual_bytes_processed"), "immutable archive actual bytes"
    )
    if parsed_actual is None or parsed_actual > day.plan.maximum_bytes_billed:
        raise CoordinatorPreflightError("immutable archive lookup has invalid byte accounting")
    manifest_sha = remote.get("manifest_sha256")
    if not isinstance(manifest_sha, str) or not _HEX64.fullmatch(manifest_sha):
        raise CoordinatorPreflightError("immutable archive lookup has invalid manifest hash")
    from pumpquant.acquisition.bigquery.client import query_request_id

    remote_rows = _as_nonnegative_int(remote.get("total_rows", 0), "immutable archive total rows")
    if remote_rows is None:  # pragma: no cover - helper returns int for 0
        raise CoordinatorPreflightError("immutable archive total rows are missing")
    return job_id, parsed_actual, query_request_id(day.plan), manifest_sha, remote_rows


def _collect_evidence(
    output_base: Path,
    approval: BigQueryApproval,
    source_contract_sha256: str,
    archive_lookup: ArchiveLookup | None,
) -> tuple[dict[str, _LedgerEvidence], dict[str, _ArchiveEvidence]]:
    """Validate every per-day ledger and find archives before any request."""
    ledger_info: dict[str, _LedgerEvidence] = {}
    archive_info: dict[str, _ArchiveEvidence] = {}
    for day in approval.days:
        prior = _read_day_ledger(
            output_base,
            day,
            billing_project=approval.billing_project,
            source_contract_sha256=source_contract_sha256,
        )
        if prior is not None:
            ledger_info[day.plan_sha256] = prior
        evidence = _archive_evidence(
            output_base,
            day,
            billing_project=approval.billing_project,
            source_contract_sha256=source_contract_sha256,
        )
        if evidence is not None:
            archive_info[day.plan_sha256] = evidence
        if archive_lookup is not None:
            try:
                remote = archive_lookup(day)
            except Exception as error:
                raise CoordinatorPreflightError("immutable archive lookup failed") from error
            if remote is not None:
                archive_info[day.plan_sha256] = _lookup_archive(day, remote)
    return ledger_info, archive_info


@dataclass(slots=True)
class _Reconciled:
    days: dict[str, CoordinatedDay]
    state_days: dict[str, dict[str, object]]
    cumulative_actual: int = 0
    cumulative_pending: int = 0


def _reconcile_days(
    backend: CoordinatorBackend,
    approval: BigQueryApproval,
    planned: _Planned,
    ledger_info: Mapping[str, _LedgerEvidence],
    archive_info: Mapping[str, _ArchiveEvidence],
) -> _Reconciled:
    """Give each day its status from archive or ledger evidence, and total the bytes."""
    result = _Reconciled({}, {})
    for day in approval.days:
        _, dry_run_result, cap, request_id = planned[day.plan_sha256]
        prior = ledger_info.get(day.plan_sha256)
        archive = archive_info.get(day.plan_sha256)
        metadata = None
        job_id = None
        actual = None
        manifest_sha = None
        status = "ready"
        if archive is not None:
            job_id, actual, archive_request_id, manifest_sha, _ = archive
            if archive_request_id != request_id:
                # A prior run may have used a tightened cap; ledger validation covers
                # that case, so a manifest alone must carry the approved cap.
                raise CoordinatorPreflightError(
                    "immutable archive request ID differs from approved plan"
                )
            status = "completed"
            result.cumulative_actual += actual
        elif prior is not None:
            prior_status, prior_cap, prior_request_id, job_id, prior_metadata, manifest_sha = prior
            if prior_cap != cap or prior_request_id != request_id:
                raise CoordinatorPreflightError("durable day state differs from refreshed plan")
            metadata = prior_metadata
            if metadata is None or not metadata.done:
                metadata = _metadata_call(backend, day.plan, cast(str, job_id))
            _require_done_metadata(metadata, maximum_bytes_billed=cap)
            actual = metadata.actual_bytes_processed
            status = "job_identified" if prior_status == "job_identified" else "completed"
            if metadata.done and actual is not None:
                result.cumulative_actual += actual
            elif not metadata.done:
                # A running job has an unknown cost, which would make the cap unverifiable.
                raise CoordinatorPreflightError(
                    "an identified BigQuery job is not complete; reconcile it before "
                    "submitting more days"
                )
        else:
            result.cumulative_pending += dry_run_result.estimated_bytes
        result.days[day.plan_sha256] = CoordinatedDay(
            start=day.start,
            end=day.end,
            plan_sha256=day.plan_sha256,
            request_id=request_id,
            estimated_bytes=day.estimated_bytes,
            maximum_bytes_billed=cap,
            status=status,
            job_id=job_id,
            job_metadata=metadata,
            actual_bytes_processed=actual,
            manifest_sha256=manifest_sha,
        )
        result.state_days[day.plan_sha256] = _day_payload(
            day,
            cap=cap,
            request_id=request_id,
            status=status,
            job_id=job_id,
            metadata=metadata,
            actual=actual,
            manifest_sha256=manifest_sha,
        )
    return result


def _require_same_jobs(
    previous: Mapping[str, object] | None,
    state_days: Mapping[str, Mapping[str, object]],
    message: str,
) -> None:
    """No identified or completed day may disappear or change its job."""
    old_days = previous.get("days") if previous is not None else None
    if not isinstance(old_days, Mapping):
        return
    for plan_hash, value in old_days.items():
        if isinstance(value, Mapping) and value.get("status") in {"job_identified", "completed"}:
            current = state_days.get(str(plan_hash))
            if current is None or current.get("job_id") != value.get("job_id"):
                raise CoordinatorPreflightError(message)


@dataclass(frozen=True, slots=True)
class _Ledger:
    """The coordinator's global ledger for one approval."""

    approval: BigQueryApproval
    approval_sha256: str
    source_contract_sha256: str
    output_base: Path
    path: Path

    def write(
        self,
        days: Mapping[str, Mapping[str, object]],
        status: str,
        *,
        execute: bool,
        actual: int,
        pending: int,
        check: Callable[[Mapping[str, object] | None], None] | None = None,
    ) -> None:
        with _coordinator_lock(self.output_base):
            if check is not None:
                check(_load_existing_global(self.path, self.approval_sha256))
            _write_json(
                self.path,
                _state_payload(
                    approval=self.approval,
                    approval_sha256=self.approval_sha256,
                    source_contract_sha256=self.source_contract_sha256,
                    execute=execute,
                    days=days,
                    status=status,
                    cumulative_actual=actual,
                    cumulative_pending=pending,
                ),
            )

    def result(
        self,
        status: str,
        *,
        execute: bool,
        actual: int,
        pending: int,
        dry_runs: tuple[DryRunResult, ...],
        days: tuple[CoordinatedDay, ...],
    ) -> CoordinationResult:
        return CoordinationResult(
            status=status,
            execute=execute,
            approval_file_sha256=self.approval_sha256,
            combined_plan_sha256=self.approval.combined_plan_sha256,
            billing_project=self.approval.billing_project,
            source_contract_sha256=self.source_contract_sha256,
            cumulative_maximum_bytes_billed=self.approval.cumulative_maximum_bytes_billed,
            cumulative_actual_bytes=actual,
            cumulative_estimated_pending_bytes=pending,
            dry_runs=dry_runs,
            days=days,
            ledger_path=str(self.path),
        )


def _submit_days(
    backend: CoordinatorBackend,
    ledger: _Ledger,
    planned: _Planned,
    pending_days: list[ApprovedDay],
    started_days: dict[str, dict[str, object]],
    reconciled: _Reconciled,
    workers: int,
) -> None:
    """Submit the reserved days concurrently, recording each job as it is identified."""
    approval = ledger.approval

    def submit(day: ApprovedDay) -> tuple[str, str, JobMetadata]:
        plan, approval_result, cap, _ = planned[day.plan_sha256]
        try:
            job_id = backend.submit_query(plan, approval_result)
        except Exception as error:
            raise CoordinatorSubmissionError(f"query submission failed for {day.start}") from error
        if not isinstance(job_id, str) or _JOB_ID.fullmatch(job_id) is None:
            raise CoordinatorSubmissionError("BigQuery submission returned an invalid job ID")
        metadata = _metadata_call(backend, plan, job_id)
        # A running job is still safely identified; a completed one must carry all
        # downstream metadata and byte accounting now.
        _require_done_metadata(metadata, maximum_bytes_billed=cap)
        return day.plan_sha256, job_id, metadata

    errors: list[BaseException] = []
    with ThreadPoolExecutor(
        max_workers=min(workers, len(pending_days)), thread_name_prefix="pumpquant-submit"
    ) as pool:
        futures: list[Future[tuple[str, str, JobMetadata]]] = [
            pool.submit(submit, day) for day in pending_days
        ]
        for future in as_completed(futures):
            try:
                plan_hash, job_id, metadata = future.result()
            except BaseException as error:
                errors.append(error)
                continue
            current = started_days[plan_hash]
            started_days[plan_hash] = {
                **current,
                "status": "job_identified",
                "job_id": job_id,
                "job_metadata": metadata.to_payload(),
                "actual_bytes_processed": metadata.actual_bytes_processed,
                "request_id": current["request_id"],
            }
            ledger.write(
                started_days,
                "submitting" if not errors else "halted_error",
                execute=True,
                actual=reconciled.cumulative_actual,
                pending=reconciled.cumulative_pending,
            )
            # The per-day ledger lets the archive and Storage Read paths recover
            # this job on their own.
            _write_json(
                _day_ledger_path(ledger.output_base, plan_hash),
                {
                    **started_days[plan_hash],
                    "billing_project": approval.billing_project,
                    "source_contract_sha256": ledger.source_contract_sha256,
                    "approved_maximum_bytes_billed": approval.daily_maximum_bytes_billed,
                },
            )
    if errors:
        ledger.write(
            started_days,
            "halted_error",
            execute=True,
            actual=reconciled.cumulative_actual,
            pending=reconciled.cumulative_pending,
        )
        raise CoordinatorSubmissionError("one or more submissions need reconciliation") from errors[
            0
        ]


def _coordinated_day(day: ApprovedDay, state: Mapping[str, object]) -> CoordinatedDay:
    job_metadata = state.get("job_metadata")
    return CoordinatedDay(
        start=day.start,
        end=day.end,
        plan_sha256=day.plan_sha256,
        request_id=cast(str, state["request_id"]),
        estimated_bytes=day.estimated_bytes,
        maximum_bytes_billed=cast(int, state["maximum_bytes_billed"]),
        status=cast(str, state["status"]),
        job_id=cast(str | None, state.get("job_id")),
        job_metadata=(
            None
            if job_metadata is None
            else _normalize_metadata(
                cast(Mapping[str, object], job_metadata), cast(str, state["job_id"])
            )
        ),
        actual_bytes_processed=cast(int | None, state.get("actual_bytes_processed")),
        manifest_sha256=cast(str | None, state.get("manifest_sha256")),
    )


def coordinate_approved_days(
    approval_file: str | os.PathLike[str],
    output_dir: str | os.PathLike[str],
    backend: CoordinatorBackend,
    *,
    execute: bool = False,
    workers: int = DEFAULT_COORDINATOR_WORKERS,
    archive_lookup: ArchiveLookup | None = None,
) -> CoordinationResult:
    """Preflight and optionally submit every still-unexecuted approved day.

    The default is a pure preflight (apart from private local ledgers).  With
    ``execute=True`` all POSTs are launched concurrently only after the source
    contract, every daily dry run, archive/job reconciliation, and
    cumulative-byte projection have passed.
    """

    _validate_workers(workers)
    if not isinstance(execute, bool):
        raise TypeError("execute must be a bool")
    approval_path = Path(approval_file).expanduser()
    output_base = Path(output_dir).expanduser()
    output_base.mkdir(mode=0o700, parents=True, exist_ok=True)
    approval_sha256 = _canonical_file_sha256(approval_path)
    try:
        approval = load_approval_artifact(approval_path)
    except Exception as error:
        raise CoordinatorPreflightError("backfill approval validation failed") from error
    source_contract_sha256 = _source_contract(backend, approval)
    ledger = _Ledger(
        approval,
        approval_sha256,
        source_contract_sha256,
        output_base,
        _global_ledger_path(output_base, approval_sha256),
    )
    with _coordinator_lock(output_base):
        global_state = _load_existing_global(ledger.path, approval_sha256)
    ledger_info, archive_info = _collect_evidence(
        output_base, approval, source_contract_sha256, archive_lookup
    )
    dry_runs, planned = _plan_dry_runs(backend, approval, ledger_info)
    reconciled = _reconcile_days(backend, approval, planned, ledger_info, archive_info)
    state_days = reconciled.state_days

    # A day in the prior global state but not in this approval could hide a
    # submitted query, so it is not safe to ignore.
    if global_state is not None:
        old_days = global_state.get("days")
        if isinstance(old_days, Mapping) and set(old_days).difference(state_days):
            raise CoordinatorPreflightError("coordinator ledger contains an unknown approved day")
        _require_same_jobs(
            global_state, state_days, "global ledger differs from durable day evidence"
        )
    actual, pending = reconciled.cumulative_actual, reconciled.cumulative_pending
    if actual + pending > approval.cumulative_maximum_bytes_billed:
        raise CoordinatorPreflightError("projected cumulative bytes exceed the approved 1 TiB cap")
    ledger.write(
        state_days,
        "preflight_ready",
        execute=execute,
        actual=actual,
        pending=pending,
        check=lambda current: _require_same_jobs(
            current, state_days, "global ledger job identity changed"
        ),
    )

    days = tuple(reconciled.days[item.plan_sha256] for item in approval.days)
    if not execute:
        return ledger.result(
            "preflight_ready",
            execute=False,
            actual=actual,
            pending=pending,
            dry_runs=dry_runs,
            days=days,
        )
    pending_days = [
        day for day in approval.days if reconciled.days[day.plan_sha256].status == "ready"
    ]
    if not pending_days:
        return ledger.result(
            "nothing_to_submit",
            execute=True,
            actual=actual,
            pending=0,
            dry_runs=dry_runs,
            days=days,
        )

    # Reserve every POST durably before launching any worker. If this process dies
    # now, the next run fails closed instead of guessing what BigQuery accepted.
    started_days = dict(state_days)
    for day in pending_days:
        started_days[day.plan_sha256] = {
            **started_days[day.plan_sha256],
            "status": "submission_attempt_started",
            "query_may_have_executed": True,
            "submission_started_at": _utc_now(),
        }
    ledger.write(
        started_days,
        "submitting",
        execute=True,
        actual=actual,
        pending=pending,
        check=lambda current: None,  # re-validates the ledger on disk before reserving
    )
    _submit_days(backend, ledger, planned, pending_days, started_days, reconciled, workers)

    final_days = tuple(
        _coordinated_day(day, started_days[day.plan_sha256]) for day in approval.days
    )
    newly_submitted = {day.plan_sha256 for day in pending_days}
    final_actual = actual + sum(
        day.actual_bytes_processed or 0
        for day in final_days
        if day.plan_sha256 in newly_submitted and day.status == "job_identified"
    )
    ledger.write(started_days, "submitted", execute=True, actual=final_actual, pending=0)
    return ledger.result(
        "submitted",
        execute=True,
        actual=final_actual,
        pending=0,
        dry_runs=dry_runs,
        days=final_days,
    )


__all__ = [
    "COORDINATOR_VERSION",
    "DEFAULT_COORDINATOR_WORKERS",
    "MAX_COORDINATOR_WORKERS",
    "CoordinatorBackend",
    "CoordinatorError",
    "CoordinatorPreflightError",
    "CoordinatorSubmissionError",
    "CoordinatedDay",
    "CoordinationResult",
    "JobMetadata",
    "coordinate_approved_days",
]
