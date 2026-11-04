"""Archive one approved BigQuery day, or a whole approved batch, under hard byte caps.

Every day is checked against its approval artifact before anything is billed: the plan
hash, the source schema contract, the dry-run estimate and the cumulative byte budget.
A durable ledger records each attempt, so an interrupted run resumes or recovers its
published archive instead of querying twice. Nothing here runs without ``execute``.
"""

from __future__ import annotations

import fcntl
import importlib
import json
import os
import shutil
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pumpquant.acquisition.bigquery.approval import (
    ApprovedDay,
    BigQueryApproval,
    BigQueryApprovalError,
    load_approval_artifact,
    validate_approval_artifact,
)
from pumpquant.acquisition.bigquery.archive import (
    ArchiveManifest,
    OVHS3Uploader,
    QueryProvenance,
    archive_bigquery_pages,
    load_archive_manifest,
    manifest_bytes,
    verify_archive_manifest,
)
from pumpquant.acquisition.bigquery.source import (
    DEFAULT_MARKET_PROGRAM_IDS,
    CostCapExceeded,
    DatasetRef,
    QueryPlan,
    build_daily_pump_instruction_plans,
    dataset_schema_sha256,
    dry_run,
    query_sha256,
    snapshot_report,
)
from pumpquant.core.durable_state import atomic_private_json_write
from pumpquant.core.hashing import sha256_bytes, sha256_file

DEFAULT_PAGE_ROW_CAP = 10_000
ARCHIVE_PREFIX_ROOT = "raw/bigquery/crypto_solana_mainnet_us/v1/instructions"

Connect = Callable[[str], Any]
"""Open a BigQuery backend for a billing project."""
Report = Callable[[dict[str, Any]], None]


@dataclass(frozen=True, slots=True)
class DailyArchiveRequest:
    billing_project: str
    start: str
    end: str
    approval_file: str
    output_dir: str
    s3_config: str | None
    execute: bool
    max_rows_per_shard: int
    max_shard_bytes: int
    min_free_bytes: int
    program_ids: tuple[str, ...] | None = None
    page_row_cap: int = DEFAULT_PAGE_ROW_CAP
    prefetch_one_page: bool = False


@dataclass(frozen=True, slots=True)
class ArchiveOutcome:
    """What a run did, as the JSON it reports, with the command's exit code."""

    code: int
    payload: dict[str, Any]


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _read_json_object(path: Path, description: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(f"{description} is not valid readable JSON") from error
    if not isinstance(value, dict):
        raise RuntimeError(f"{description} must contain a JSON object")
    return value


def load_query_provenance(path_value: str) -> QueryProvenance:
    """Load a strict QueryProvenance object for Storage Read archival."""

    payload = _read_json_object(Path(path_value).expanduser(), "query provenance")
    raw = payload.get("query_provenance", payload)
    if not isinstance(raw, Mapping):
        raise RuntimeError("query provenance must be an object")
    source_tables = raw.get("source_tables")
    if not isinstance(source_tables, (list, tuple)):
        raise RuntimeError("query provenance source_tables must be a list")
    try:
        return QueryProvenance(
            plan_name=raw["plan_name"],
            plan_sha256=raw["plan_sha256"],
            request_id=raw["request_id"],
            job_id=raw["job_id"],
            billing_project=raw["billing_project"],
            location=raw["location"],
            source_tables=tuple(source_tables),
            source_contract_sha256=raw["source_contract_sha256"],
            start=raw["start"],
            end=raw["end"],
            dry_run_estimated_bytes=raw["dry_run_estimated_bytes"],
            maximum_bytes_billed=raw["maximum_bytes_billed"],
            actual_bytes_processed=raw["actual_bytes_processed"],
            total_rows=raw["total_rows"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("query provenance has invalid fields") from error


def _load_ovh_uploader(path_value: str) -> OVHS3Uploader:
    path = Path(path_value).expanduser()
    if path.is_symlink() or not path.is_file():
        raise RuntimeError("S3 configuration must be a regular non-symlink file")
    if path.stat().st_mode & 0o077:
        raise RuntimeError("S3 configuration permissions must be 0600 or stricter")
    payload = _read_json_object(path, "S3 configuration")
    required = {"endpoint_url", "region", "bucket", "access_key", "secret_key"}
    if set(payload) != required or any(not isinstance(payload[key], str) for key in required):
        raise RuntimeError("S3 configuration has an invalid field set")
    return OVHS3Uploader(
        payload["endpoint_url"],
        payload["region"],
        payload["bucket"],
        payload["access_key"],
        payload["secret_key"],
    )


def _load_daily_approval(
    path_value: str,
    *,
    billing_project: str,
    start: str,
    end: str,
) -> tuple[int, int, int, str, str]:
    try:
        approval = load_approval_artifact(Path(path_value).expanduser())
    except BigQueryApprovalError as error:
        raise RuntimeError(f"backfill approval validation failed: {error}") from error

    if approval.billing_project != billing_project:
        raise RuntimeError("backfill approval billing project mismatch")

    matches = [day for day in approval.days if day.start == start and day.end == end]
    if len(matches) != 1:
        raise RuntimeError("requested day is not uniquely present in the backfill approval")
    day = matches[0]
    return (
        approval.daily_maximum_bytes_billed,
        approval.cumulative_maximum_bytes_billed,
        day.estimated_bytes,
        day.plan_sha256,
        approval.source_contract_sha256,
    )


def _archive_prefix(plan_hash: str, start: str) -> str:
    return f"{ARCHIVE_PREFIX_ROOT}/date={start[:10]}/plan={plan_hash}"


def _archive_root(base: Path, plan_hash: str, start: str) -> Path:
    return base / f"date={start[:10]}" / f"plan={plan_hash}"


def _ledger_path(base: Path, plan_hash: str) -> Path:
    return base / ".executions" / f"{plan_hash}.json"


def _published_manifest_path(base: Path, plan_hash: str) -> Path:
    return base / ".published" / plan_hash / "manifest.json"


def _store_published_manifest(
    base: Path,
    plan_hash: str,
    manifest: ArchiveManifest,
) -> Path:
    path = _published_manifest_path(base, plan_hash)
    payload = json.loads(manifest_bytes(manifest))
    if not isinstance(payload, dict):  # pragma: no cover - serialiser invariant
        raise RuntimeError("canonical archive manifest did not serialize to an object")
    atomic_private_json_write(path, payload, exclusive=False)
    loaded = load_archive_manifest(path)
    if loaded != manifest:
        raise RuntimeError("published manifest audit copy failed verification")
    return path


def _cumulative_ledger_bytes(base: Path) -> int:
    ledger_dir = base / ".executions"
    if not ledger_dir.exists():
        return 0
    total = 0
    for path in sorted(ledger_dir.glob("*.json")):
        payload = _read_json_object(path, "execution ledger")
        status = payload.get("status")
        if status != "completed":
            raise RuntimeError(
                "an indeterminate prior BigQuery attempt must be reconciled before continuing"
            )
        actual = payload.get("actual_bytes_processed")
        if isinstance(actual, bool) or not isinstance(actual, int) or actual <= 0:
            raise RuntimeError("completed execution ledger lacks actual byte accounting")
        total += actual
    return total


def _remaining_execution_cap(
    *,
    cumulative_actual_bytes: int,
    approved_estimated_bytes: int,
    daily_cap: int,
    cumulative_cap: int,
) -> int:
    """Return the strict server cap that cannot cross the cumulative ceiling."""

    values = (
        cumulative_actual_bytes,
        approved_estimated_bytes,
        daily_cap,
        cumulative_cap,
    )
    if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
        raise TypeError("BigQuery byte accounting values must be integers")
    if (
        cumulative_actual_bytes < 0
        or approved_estimated_bytes <= 0
        or daily_cap <= 0
        or cumulative_cap <= 0
    ):
        raise ValueError("BigQuery byte accounting values are outside their valid range")
    remaining = cumulative_cap - cumulative_actual_bytes
    if remaining < approved_estimated_bytes:
        raise RuntimeError("durable execution ledger would exceed the approved cumulative cap")
    return min(daily_cap, remaining)


@contextmanager
def _file_lock(lock_path: Path) -> Iterator[None]:
    lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _bind_project_output_root(
    *,
    billing_project: str,
    output_base: Path,
) -> None:
    """Persistently bind all Sandbox accounting for a project to one local root."""

    project_key = sha256_bytes(billing_project.encode())
    binding_path = (
        Path.home()
        / ".config"
        / "pumpquant"
        / "execution-roots"
        / project_key
        / "sandbox-output-root.json"
    )
    binding = {
        "version": 1,
        "billing_project": billing_project,
        "output_root": str(output_base.resolve()),
    }
    try:
        atomic_private_json_write(binding_path, binding, exclusive=True)
    except FileExistsError:
        if _read_json_object(binding_path, "plan output-root binding") != binding:
            raise RuntimeError(
                "BigQuery project is already bound to a different local output root"
            ) from None


def _manifest_matches_approval(
    manifest: ArchiveManifest,
    *,
    plan: QueryPlan,
    estimated_bytes: int,
    billing_project: str,
    source_contract_sha256: str,
) -> None:
    provenance = manifest.provenance
    if provenance is None:
        raise RuntimeError("archive manifest has no query provenance")
    from pumpquant.acquisition.bigquery.client import query_request_id

    execution_cap = provenance.maximum_bytes_billed
    if (
        isinstance(execution_cap, bool)
        or not isinstance(execution_cap, int)
        or execution_cap < estimated_bytes
        or execution_cap > plan.maximum_bytes_billed
    ):
        raise RuntimeError("archive manifest has an invalid execution byte cap")
    execution_plan = replace(plan, maximum_bytes_billed=execution_cap)
    if (
        manifest.table != "Instructions"
        or manifest.row_count != provenance.total_rows
        or (manifest.row_count > 0 and (not manifest.shards or manifest.byte_count <= 0))
        or any(shard.row_count <= 0 or shard.byte_count <= 0 for shard in manifest.shards)
    ):
        raise RuntimeError("archive manifest row or table semantics are inconsistent")

    expected = (
        plan.name,
        query_sha256(plan),
        query_request_id(execution_plan),
        estimated_bytes,
        execution_cap,
        billing_project,
        plan.location,
        plan.referenced_tables,
        source_contract_sha256,
        plan.parameters[0].value,
        plan.parameters[1].value,
    )
    actual = (
        provenance.plan_name,
        provenance.plan_sha256,
        provenance.request_id,
        provenance.dry_run_estimated_bytes,
        provenance.maximum_bytes_billed,
        provenance.billing_project,
        provenance.location,
        provenance.source_tables,
        provenance.source_contract_sha256,
        provenance.start,
        provenance.end,
    )
    if actual != expected:
        raise RuntimeError("archive manifest does not match the approved daily plan")


def _upload_archive(
    uploader: OVHS3Uploader,
    manifest: ArchiveManifest,
    root: Path,
    *,
    max_workers: int = 1,
) -> dict[str, Any]:
    provenance = manifest.provenance
    if provenance is None:
        raise RuntimeError("archive manifest has no query provenance")
    prefix = _archive_prefix(provenance.plan_sha256, provenance.start)
    uploader.preflight(prefix)
    results = uploader.upload_manifest(
        manifest,
        root,
        prefix=prefix,
        max_workers=max_workers,
    )
    return {
        "bucket": uploader.config.bucket,
        "prefix": prefix,
        "objects": len(results),
        "bytes": sum(item.byte_count for item in results),
        "skipped_existing": sum(item.skipped for item in results),
        "manifest_key": results[-1].key,
    }


def _verify_published_archive(
    uploader: OVHS3Uploader,
    manifest: ArchiveManifest,
) -> dict[str, Any]:
    provenance = manifest.provenance
    if provenance is None:
        raise RuntimeError("archive manifest has no query provenance")
    prefix = _archive_prefix(provenance.plan_sha256, provenance.start)
    uploader.preflight(prefix)
    results = uploader.verify_remote_manifest(manifest, prefix=prefix)
    return {
        "bucket": uploader.config.bucket,
        "prefix": prefix,
        "objects": len(results),
        "bytes": sum(item.byte_count for item in results),
        "skipped_existing": len(results),
        "manifest_key": results[-1].key,
        "fully_reverified": True,
    }


def _recover_remote_archive(
    uploader: OVHS3Uploader,
    *,
    plan: QueryPlan,
    expected_bytes: int,
    billing_project: str,
    source_contract_sha256: str,
    output_base: Path,
    ledger: Path,
    prior_attempt: Mapping[str, Any] | None,
) -> tuple[ArchiveManifest, dict[str, Any]] | None:
    """Recover a fully published day before considering any query submission."""

    plan_hash = query_sha256(plan)
    prefix = _archive_prefix(plan_hash, str(plan.parameters[0].value))
    uploader.preflight(prefix)
    remote_manifest = uploader.fetch_manifest(prefix=prefix)
    if remote_manifest is None:
        return None
    _manifest_matches_approval(
        remote_manifest,
        plan=plan,
        estimated_bytes=expected_bytes,
        billing_project=billing_project,
        source_contract_sha256=source_contract_sha256,
    )
    provenance = remote_manifest.provenance
    if provenance is None:  # pragma: no cover - checked by approval matching
        raise RuntimeError("remote archive manifest has no query provenance")
    trusted_job_id = (
        prior_attempt.get("job_id")
        if prior_attempt is not None and isinstance(prior_attempt.get("job_id"), str)
        else None
    )
    audit_path = _published_manifest_path(output_base, plan_hash)
    if not audit_path.is_file():
        raise RuntimeError(
            "remote archive lacks a trusted local audit manifest; reconcile the BigQuery job"
        )
    audit_manifest = load_archive_manifest(audit_path)
    _manifest_matches_approval(
        audit_manifest,
        plan=plan,
        estimated_bytes=expected_bytes,
        billing_project=billing_project,
        source_contract_sha256=source_contract_sha256,
    )
    if audit_manifest != remote_manifest:
        raise RuntimeError("remote archive differs from its trusted local audit manifest")
    audit_provenance = audit_manifest.provenance
    if audit_provenance is None:  # pragma: no cover - checked above
        raise RuntimeError("trusted audit manifest has no query provenance")
    if trusted_job_id is not None and trusted_job_id != audit_provenance.job_id:
        raise RuntimeError("trusted audit manifest differs from the execution ledger")
    trusted_job_id = audit_provenance.job_id
    if trusted_job_id != provenance.job_id:
        raise RuntimeError("remote archive job differs from the execution ledger")
    if prior_attempt is not None and (
        prior_attempt.get("request_id") != provenance.request_id
        or prior_attempt.get("maximum_bytes_billed") != provenance.maximum_bytes_billed
        or prior_attempt.get("estimated_bytes") != provenance.dry_run_estimated_bytes
        or (
            prior_attempt.get("actual_bytes_processed") is not None
            and prior_attempt.get("actual_bytes_processed") != provenance.actual_bytes_processed
        )
        or (
            prior_attempt.get("total_rows") is not None
            and prior_attempt.get("total_rows") != provenance.total_rows
        )
        or (
            prior_attempt.get("manifest_sha256") is not None
            and prior_attempt.get("manifest_sha256") != remote_manifest.manifest_sha256
        )
    ):
        raise RuntimeError("remote archive execution bounds differ from the execution ledger")
    remote = _verify_published_archive(uploader, remote_manifest)
    stored_audit_path = _store_published_manifest(output_base, plan_hash, remote_manifest)
    atomic_private_json_write(
        ledger,
        {
            "status": "completed",
            "plan_sha256": plan_hash,
            "request_id": provenance.request_id,
            "estimated_bytes": expected_bytes,
            "maximum_bytes_billed": provenance.maximum_bytes_billed,
            "approved_maximum_bytes_billed": plan.maximum_bytes_billed,
            "source_contract_sha256": source_contract_sha256,
            "job_id": provenance.job_id,
            "actual_bytes_processed": provenance.actual_bytes_processed,
            "total_rows": provenance.total_rows,
            "manifest_sha256": remote_manifest.manifest_sha256,
            "published_manifest_path": str(stored_audit_path),
            "remote_manifest_key": remote["manifest_key"],
        },
        exclusive=False,
    )
    return remote_manifest, remote


@dataclass(slots=True)
class _Day:
    """One approved day as it moves from approval to a published archive."""

    request: DailyArchiveRequest
    plan: QueryPlan
    plan_hash: str
    total_cap: int
    expected_bytes: int
    source_contract: str
    output_base: Path
    root: Path
    ledger: Path
    payload: dict[str, Any]
    execution_plan: QueryPlan
    request_id: str
    prior_attempt: dict[str, Any] | None = None
    uploader: OVHS3Uploader | None = None
    attempt: dict[str, Any] = field(default_factory=dict)

    def done(self, code: int = 0, **changes: Any) -> ArchiveOutcome:
        self.payload.update(changes)
        return ArchiveOutcome(code, self.payload)

    def matches_approval(self, manifest: ArchiveManifest) -> None:
        _manifest_matches_approval(
            manifest,
            plan=self.plan,
            estimated_bytes=self.expected_bytes,
            billing_project=self.request.billing_project,
            source_contract_sha256=self.source_contract,
        )


def _archive_summary(manifest: ArchiveManifest, *, shards: bool = False) -> dict[str, Any]:
    summary: dict[str, Any] = {"rows": manifest.row_count, "bytes": manifest.byte_count}
    if shards:
        summary["shards"] = len(manifest.shards)
    summary["manifest_sha256"] = manifest.manifest_sha256
    return summary


def _approved_day(request: DailyArchiveRequest) -> _Day:
    """Rebuild the day's query plan and check it is exactly the one approved."""
    daily_cap, total_cap, expected_bytes, expected_plan_hash, source_contract = (
        _load_daily_approval(
            request.approval_file,
            billing_project=request.billing_project,
            start=request.start,
            end=request.end,
        )
    )
    program_ids = (
        DEFAULT_MARKET_PROGRAM_IDS if request.program_ids is None else tuple(request.program_ids)
    )
    plans = build_daily_pump_instruction_plans(
        request.start, request.end, maximum_bytes_billed=daily_cap, program_ids=program_ids
    )
    if len(plans) != 1:
        raise RuntimeError("bq-archive-day accepts exactly one complete UTC day")
    plan = plans[0]
    plan_hash = query_sha256(plan)
    if plan_hash != expected_plan_hash:
        raise RuntimeError("daily query plan hash differs from the approved artifact")
    from pumpquant.acquisition.bigquery.client import query_request_id

    output_base = Path(request.output_dir).expanduser()
    root = _archive_root(output_base, plan_hash, str(plan.parameters[0].value))
    payload: dict[str, Any] = {
        "status": "ready_for_execution",
        "plan_sha256": plan_hash,
        "start": plan.parameters[0].value,
        "end": plan.parameters[1].value,
        "estimated_bytes": expected_bytes,
        "maximum_bytes_billed": plan.maximum_bytes_billed,
        "source_contract_sha256": source_contract,
        "archive_root": str(root),
        "query_executed": False,
    }
    return _Day(
        request=request,
        plan=plan,
        plan_hash=plan_hash,
        total_cap=total_cap,
        expected_bytes=expected_bytes,
        source_contract=source_contract,
        output_base=output_base,
        root=root,
        ledger=_ledger_path(output_base, plan_hash),
        payload=payload,
        execution_plan=plan,
        request_id=query_request_id(plan),
    )


def _existing_local_archive(day: _Day) -> ArchiveOutcome:
    """Verify a day already archived locally, and publish it when executing."""
    manifest = load_archive_manifest(day.root / "manifest.json")
    verify_archive_manifest(manifest, day.root)
    day.matches_approval(manifest)
    provenance = manifest.provenance
    if provenance is None:  # pragma: no cover - checked by approval matching
        raise RuntimeError("local archive manifest has no query provenance")
    if day.ledger.exists():
        existing = _read_json_object(day.ledger, "execution ledger")
        if (
            existing.get("status") not in {"job_identified", "completed"}
            or existing.get("plan_sha256") != day.plan_hash
            or existing.get("request_id") != provenance.request_id
            or existing.get("estimated_bytes") != provenance.dry_run_estimated_bytes
            or existing.get("maximum_bytes_billed") != provenance.maximum_bytes_billed
            or existing.get("job_id") != provenance.job_id
            or (
                existing.get("actual_bytes_processed") is not None
                and existing.get("actual_bytes_processed") != provenance.actual_bytes_processed
            )
            or (
                existing.get("total_rows") is not None
                and existing.get("total_rows") != provenance.total_rows
            )
            or (
                existing.get("manifest_sha256") is not None
                and existing.get("manifest_sha256") != manifest.manifest_sha256
            )
        ):
            raise RuntimeError("local archive differs from the execution ledger")
    if not day.request.execute:
        return day.done(status="existing_archive_verified", archive=_archive_summary(manifest))
    if day.request.s3_config is None:
        raise RuntimeError("--s3-config is required to publish an existing archive")
    remote = _upload_archive(_load_ovh_uploader(day.request.s3_config), manifest, day.root)
    audit_manifest = _store_published_manifest(day.output_base, day.plan_hash, manifest)
    atomic_private_json_write(
        day.ledger,
        {
            "status": "completed",
            "plan_sha256": day.plan_hash,
            "request_id": provenance.request_id,
            "estimated_bytes": day.expected_bytes,
            "maximum_bytes_billed": provenance.maximum_bytes_billed,
            "approved_maximum_bytes_billed": day.plan.maximum_bytes_billed,
            "source_contract_sha256": day.source_contract,
            "job_id": provenance.job_id,
            "actual_bytes_processed": provenance.actual_bytes_processed,
            "total_rows": manifest.row_count,
            "manifest_sha256": manifest.manifest_sha256,
            "published_manifest_path": str(audit_manifest),
            "remote_manifest_key": remote["manifest_key"],
        },
        exclusive=False,
    )
    return day.done(
        status="existing_archive_published",
        query_executed=False,
        archive=_archive_summary(manifest),
        remote=remote,
    )


def _load_prior_attempt(day: _Day) -> None:
    """Read the ledger of an earlier attempt and keep its execution cap."""
    from pumpquant.acquisition.bigquery.client import query_request_id

    prior = _read_json_object(day.ledger, "execution ledger")
    prior_cap = prior.get("maximum_bytes_billed")
    if (
        isinstance(prior_cap, bool)
        or not isinstance(prior_cap, int)
        or prior_cap < day.expected_bytes
        or prior_cap > day.plan.maximum_bytes_billed
    ):
        raise RuntimeError("prior execution ledger has an invalid query byte cap")
    day.execution_plan = replace(day.plan, maximum_bytes_billed=prior_cap)
    day.request_id = query_request_id(day.execution_plan)
    if (
        prior.get("plan_sha256") != day.plan_hash
        or prior.get("request_id") != day.request_id
        or prior.get("estimated_bytes") != day.expected_bytes
        or prior.get("approved_maximum_bytes_billed", day.plan.maximum_bytes_billed)
        != day.plan.maximum_bytes_billed
        or prior.get("source_contract_sha256", day.source_contract) != day.source_contract
    ):
        raise RuntimeError("prior execution ledger does not match the approved plan")
    day.prior_attempt = prior


def _completed_attempt(day: _Day, prior: Mapping[str, Any]) -> ArchiveOutcome:
    """Check a completed ledger against its audit manifest and, if configured, the remote."""
    manifest = load_archive_manifest(_published_manifest_path(day.output_base, day.plan_hash))
    day.matches_approval(manifest)
    provenance = manifest.provenance
    if provenance is None or (
        prior.get("job_id") != provenance.job_id
        or prior.get("request_id") != provenance.request_id
        or prior.get("maximum_bytes_billed") != provenance.maximum_bytes_billed
        or prior.get("estimated_bytes") != provenance.dry_run_estimated_bytes
        or prior.get("actual_bytes_processed") != provenance.actual_bytes_processed
        or prior.get("total_rows") != provenance.total_rows
        or prior.get("manifest_sha256") != manifest.manifest_sha256
    ):
        raise RuntimeError("completed execution ledger differs from its audit manifest")
    day.payload.update(
        status="completed_audit_manifest_verified", archive=_archive_summary(manifest)
    )
    if day.request.s3_config is not None:
        remote = _verify_published_archive(_load_ovh_uploader(day.request.s3_config), manifest)
        return day.done(status="completed_remote_archive_verified", remote=remote)
    if day.request.execute:
        raise RuntimeError("--s3-config is required to verify a completed archive")
    return day.done()


def _preflight(day: _Day, backend: Any) -> ArchiveOutcome | None:
    """Re-check the source schema and the dry-run estimate against the approval."""
    snapshot = backend.inspect_dataset(DatasetRef())
    report = snapshot_report(snapshot, required_tables=("Instructions",))
    if not report["contract_ok"]:
        return ArchiveOutcome(2, {"status": "blocked_source_contract", "source_contract": report})
    current_contract = dataset_schema_sha256(snapshot)
    if current_contract != day.source_contract:
        return ArchiveOutcome(
            7,
            {
                "status": "blocked_source_contract_drift",
                "expected_source_contract_sha256": day.source_contract,
                "current_source_contract_sha256": current_contract,
                "query_executed": False,
            },
        )
    try:
        current_dry_run = dry_run(backend, day.plan)
    except CostCapExceeded as error:
        return ArchiveOutcome(3, {"status": "blocked_cost_cap", "dry_run": asdict(error.result)})
    if current_dry_run.estimated_bytes != day.expected_bytes:
        return ArchiveOutcome(
            6,
            {
                "status": "blocked_estimate_drift",
                "expected_bytes": day.expected_bytes,
                "current_bytes": current_dry_run.estimated_bytes,
                "query_executed": False,
            },
        )
    return None


def _submit_query(day: _Day, backend: Any) -> Iterator[Any]:
    """Submit the query under the remaining cumulative budget, recording the job first."""
    from pumpquant.acquisition.bigquery.client import query_request_id

    with _file_lock(day.output_base / ".executions" / ".lock"):
        cumulative = _cumulative_ledger_bytes(day.output_base)
        execution_cap = _remaining_execution_cap(
            cumulative_actual_bytes=cumulative,
            approved_estimated_bytes=day.expected_bytes,
            daily_cap=day.plan.maximum_bytes_billed,
            cumulative_cap=day.total_cap,
        )
        day.execution_plan = replace(day.plan, maximum_bytes_billed=execution_cap)
        execution_approval = dry_run(backend, day.execution_plan)
        if execution_approval.estimated_bytes != day.expected_bytes:
            raise RuntimeError("execution-cap dry run differs from the approved estimate")
        _bind_project_output_root(
            billing_project=day.request.billing_project, output_base=day.output_base
        )
        day.request_id = query_request_id(day.execution_plan)
        day.attempt = {
            "status": "execution_attempt_started",
            "created_at": _now(),
            "plan_sha256": day.plan_hash,
            "request_id": day.request_id,
            "estimated_bytes": day.expected_bytes,
            "maximum_bytes_billed": execution_cap,
            "approved_maximum_bytes_billed": day.plan.maximum_bytes_billed,
            "cumulative_actual_bytes_before_query": cumulative,
            "source_contract_sha256": day.source_contract,
            "query_may_have_executed": True,
        }

        def record_job(job_id: str) -> None:
            day.attempt.update(status="job_identified", job_id=job_id)
            atomic_private_json_write(day.ledger, day.attempt, exclusive=False)

        pages: Iterator[Any] = backend.execute_query(
            day.execution_plan,
            execution_approval,
            max_results=day.request.page_row_cap,
            prefetch_one_page=day.request.prefetch_one_page,
            on_job_created=record_job,
        )
        atomic_private_json_write(day.ledger, day.attempt, exclusive=True)
    return pages


def _archive_and_publish(day: _Day, pages: Iterator[Any]) -> ArchiveOutcome:
    """Stream the result pages into Parquet shards, upload them and close the ledger."""
    assert day.uploader is not None
    first = next(pages)
    if first.total_rows is None or first.total_bytes_processed is None:
        raise RuntimeError("BigQuery result lacks final row or byte statistics")
    if day.attempt.get("job_id") != first.job_id:
        day.attempt.update(status="job_identified", job_id=first.job_id)
        atomic_private_json_write(day.ledger, day.attempt, exclusive=False)
    provenance = QueryProvenance(
        plan_name=day.plan.name,
        plan_sha256=day.plan_hash,
        request_id=day.request_id,
        job_id=first.job_id,
        billing_project=day.request.billing_project,
        location=day.plan.location,
        source_tables=day.plan.referenced_tables,
        source_contract_sha256=day.source_contract,
        start=str(day.plan.parameters[0].value),
        end=str(day.plan.parameters[1].value),
        dry_run_estimated_bytes=day.expected_bytes,
        maximum_bytes_billed=day.execution_plan.maximum_bytes_billed,
        actual_bytes_processed=first.total_bytes_processed,
        total_rows=first.total_rows,
    )

    def all_pages() -> Iterator[Any]:
        yield first
        yield from pages

    manifest = archive_bigquery_pages(
        all_pages(),
        day.root,
        table="Instructions",
        max_rows_per_shard=day.request.max_rows_per_shard,
        max_bytes_per_shard=day.request.max_shard_bytes,
        provenance=provenance,
    )
    remote = _upload_archive(day.uploader, manifest, day.root)
    audit_manifest = _store_published_manifest(day.output_base, day.plan_hash, manifest)
    atomic_private_json_write(
        day.ledger,
        {
            **day.attempt,
            "status": "completed",
            "actual_bytes_processed": first.total_bytes_processed,
            "total_rows": manifest.row_count,
            "manifest_sha256": manifest.manifest_sha256,
            "published_manifest_path": str(audit_manifest),
            "remote_manifest_key": remote["manifest_key"],
        },
        exclusive=False,
    )
    return day.done(
        status="archive_published",
        query_executed=True,
        job_id=first.job_id,
        actual_bytes_processed=first.total_bytes_processed,
        archive=_archive_summary(manifest, shards=True),
        remote=remote,
    )


def archive_day(request: DailyArchiveRequest, connect: Connect) -> ArchiveOutcome:
    """Validate, and with ``execute`` run, archive and publish one approved day.

    Without ``execute`` no query runs and nothing is written. Callers that may run
    beside a batch should hold the batch lock (:func:`archive_day_locked`).
    """
    day = _approved_day(request)
    if (day.root / "manifest.json").is_file():
        return _existing_local_archive(day)
    if day.root.exists():
        raise RuntimeError("archive root exists without a verified manifest")
    if day.ledger.exists():
        _load_prior_attempt(day)

    if request.execute:
        if request.s3_config is None:
            raise RuntimeError("--s3-config is required before query execution")
        day.uploader = _load_ovh_uploader(request.s3_config)
        recovered = _recover_remote_archive(
            day.uploader,
            plan=day.plan,
            expected_bytes=day.expected_bytes,
            billing_project=request.billing_project,
            source_contract_sha256=day.source_contract,
            output_base=day.output_base,
            ledger=day.ledger,
            prior_attempt=day.prior_attempt,
        )
        if recovered is not None:
            remote_manifest, remote = recovered
            return day.done(
                status="remote_archive_recovered_without_query",
                query_executed=False,
                archive=_archive_summary(remote_manifest, shards=True),
                remote=remote,
            )

    resume_job_id: str | None = None
    prior = day.prior_attempt
    if prior is not None:
        status, job_id = prior.get("status"), prior.get("job_id")
        if status == "job_identified" and isinstance(job_id, str):
            resume_job_id = job_id
            day.payload["status"] = "existing_job_ready_for_resume"
        elif status == "execution_attempt_started":
            raise RuntimeError(
                "the prior query submission outcome is indeterminate; inspect BigQuery jobs"
            )
        elif status == "completed":
            return _completed_attempt(day, prior)
        else:
            raise RuntimeError("execution ledger is inconsistent with the local archive state")

    backend = connect(request.billing_project)
    if resume_job_id is None or not request.execute:
        blocked = _preflight(day, backend)
        if blocked is not None:
            return blocked
    if not request.execute:
        return day.done()
    day.output_base.mkdir(mode=0o700, parents=True, exist_ok=True)
    if shutil.disk_usage(day.output_base).free < request.min_free_bytes:
        raise RuntimeError("insufficient free local disk for the guarded daily archive")
    try:
        importlib.import_module("pyarrow")
    except ImportError as error:
        raise RuntimeError("query execution requires: pip install 'pumpquant[archive]'") from error
    if day.uploader is None:  # pragma: no cover - execute path requires a config above
        raise RuntimeError("S3 uploader was not initialized")
    if resume_job_id is None:
        pages = _submit_query(day, backend)
    else:
        assert prior is not None
        _bind_project_output_root(
            billing_project=request.billing_project, output_base=day.output_base
        )
        day.attempt = prior
        pages = backend.resume_query(
            day.execution_plan,
            resume_job_id,
            max_results=request.page_row_cap,
            prefetch_one_page=request.prefetch_one_page,
        )
    return _archive_and_publish(day, pages)


def archive_day_locked(request: DailyArchiveRequest, connect: Connect) -> ArchiveOutcome:
    """Run :func:`archive_day` while holding the output root's batch lock."""
    output_base = Path(request.output_dir).expanduser()
    with _file_lock(output_base / ".batch" / ".lock"):
        return archive_day(request, connect)


@dataclass(frozen=True, slots=True)
class BatchArchiveRequest:
    billing_project: str
    approval_file: str
    output_dir: str
    s3_config: str | None
    execute: bool
    cleanup_local: bool
    max_rows_per_shard: int
    max_shard_bytes: int
    min_free_bytes: int
    page_row_cap: int = DEFAULT_PAGE_ROW_CAP
    prefetch_one_page: bool = False


class _BatchState:
    """The batch's durable progress file, rewritten after every step."""

    def __init__(self, path: Path, base: dict[str, Any]) -> None:
        self.path = path
        self.base = base
        self.completed: list[str] = []

    def write(self, status: str, **fields: Any) -> dict[str, Any]:
        payload = {**self.base, "status": status, **fields}
        payload.setdefault("completed_plan_sha256", self.completed)
        atomic_private_json_write(self.path, payload, exclusive=False)
        return payload


def _batch_day(
    request: BatchArchiveRequest,
    approval: BigQueryApproval,
    day: ApprovedDay,
    snapshot_path: Path,
    state: _BatchState,
    connect: Connect,
    report: Report,
) -> int:
    """Archive one day of the batch; a non-zero code halts the batch."""
    output_base = Path(request.output_dir).expanduser()
    day_request = DailyArchiveRequest(
        billing_project=request.billing_project,
        start=day.start,
        end=day.end,
        approval_file=str(snapshot_path),
        output_dir=str(output_base),
        s3_config=request.s3_config,
        execute=request.execute,
        max_rows_per_shard=request.max_rows_per_shard,
        max_shard_bytes=request.max_shard_bytes,
        min_free_bytes=request.min_free_bytes,
        page_row_cap=request.page_row_cap,
        prefetch_one_page=request.prefetch_one_page,
    )
    try:
        outcome = archive_day(day_request, connect)
    except Exception as error:
        state.write(
            "halted_error",
            failed_plan_sha256=day.plan_sha256,
            error_type=type(error).__name__,
        )
        raise
    report(outcome.payload)
    if outcome.code != 0:
        state.write(
            "halted_return_code",
            failed_plan_sha256=day.plan_sha256,
            return_code=outcome.code,
        )
        return outcome.code
    cleanup: dict[str, Any] | None = None
    cumulative_actual: int | None = None
    try:
        if request.execute:
            if request.cleanup_local:
                if request.s3_config is None:  # pragma: no cover - checked by the caller
                    raise RuntimeError("missing S3 configuration")
                cleanup = cleanup_verified_local_archive(
                    output_base=output_base,
                    day=day,
                    approval=approval,
                    s3_config=request.s3_config,
                )
            cumulative_actual = _cumulative_ledger_bytes(output_base)
            if cumulative_actual > approval.cumulative_maximum_bytes_billed:
                state.write(
                    "halted_actual_over_cap",
                    failed_plan_sha256=day.plan_sha256,
                    cumulative_actual_bytes=cumulative_actual,
                    completed_plan_sha256=[*state.completed, day.plan_sha256],
                )
                return 8
    except Exception as error:
        state.write(
            "halted_postprocess_error",
            failed_plan_sha256=day.plan_sha256,
            error_type=type(error).__name__,
        )
        raise
    state.completed.append(day.plan_sha256)
    state.write(
        "running",
        last_completed_plan_sha256=day.plan_sha256,
        cumulative_actual_bytes=cumulative_actual,
        last_cleanup=cleanup,
    )
    return 0


def archive_batch(request: BatchArchiveRequest, connect: Connect, report: Report) -> int:
    """Archive every day of an approved backfill in order, halting at the first problem.

    Each day's outcome, then the batch's final state, is passed to ``report``. The
    approval is snapshotted durably first and re-hashed before each day, so the batch
    cannot drift from what was approved while it runs. Returns the exit code.
    """
    approval_path = Path(request.approval_file).expanduser()
    try:
        approval_payload = _read_json_object(approval_path, "backfill approval")
        approval = validate_approval_artifact(approval_payload)
    except BigQueryApprovalError as error:
        raise RuntimeError(f"backfill approval validation failed: {error}") from error
    if approval.billing_project != request.billing_project:
        raise RuntimeError("backfill approval billing project mismatch")
    if request.execute and request.s3_config is None:
        raise RuntimeError("--s3-config is required before batch query execution")

    output_base = Path(request.output_dir).expanduser()
    output_base.mkdir(mode=0o700, parents=True, exist_ok=True)
    encoded_approval = (
        json.dumps(approval_payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    approval_sha256 = sha256_bytes(encoded_approval)
    snapshot_path = output_base / ".batch" / "approvals" / f"{approval_sha256}.json"
    state = _BatchState(
        output_base / ".batch" / f"{approval_sha256}.json",
        {
            "version": 1,
            "approval_file_sha256": approval_sha256,
            "combined_plan_sha256": approval.combined_plan_sha256,
            "billing_project": approval.billing_project,
            "selected_window": {"start": approval.selected_start, "end": approval.selected_end},
            "daily_maximum_bytes_billed": approval.daily_maximum_bytes_billed,
            "cumulative_maximum_bytes_billed": approval.cumulative_maximum_bytes_billed,
            "execute": bool(request.execute),
            "cleanup_local": bool(request.cleanup_local),
            "page_row_cap": request.page_row_cap,
            "prefetch_one_page": request.prefetch_one_page,
        },
    )

    with _file_lock(output_base / ".batch" / ".lock"):
        if request.execute:
            _bind_project_output_root(
                billing_project=request.billing_project, output_base=output_base
            )
        atomic_private_json_write(snapshot_path, approval_payload, exclusive=False)
        if sha256_file(snapshot_path) != approval_sha256:
            raise RuntimeError("durable approval snapshot hash mismatch")
        state.write("running", started_at=_now())
        for day in approval.days:
            if sha256_file(snapshot_path) != approval_sha256:
                raise RuntimeError("durable approval snapshot changed during batch execution")
            code = _batch_day(request, approval, day, snapshot_path, state, connect, report)
            if code != 0:
                return code
        final_payload = state.write(
            "completed" if request.execute else "validation_completed",
            completed_at=_now(),
            cumulative_actual_bytes=(
                _cumulative_ledger_bytes(output_base) if request.execute else None
            ),
        )
        report(final_payload)
        return 0


def cleanup_verified_local_archive(
    *,
    output_base: Path,
    day: ApprovedDay,
    approval: BigQueryApproval,
    s3_config: str,
) -> dict[str, Any]:
    """Remove local shards only after a second complete remote verification."""

    root = _archive_root(output_base, day.plan_sha256, day.start)
    tombstone = output_base / ".cleanup" / day.plan_sha256
    if root.exists() and tombstone.exists():
        raise RuntimeError("both active and tombstoned local archives exist")
    if not root.exists() and not tombstone.exists():
        return {"removed": False, "archive_root": str(root), "bytes_released": 0}
    audit_manifest = load_archive_manifest(_published_manifest_path(output_base, day.plan_sha256))
    _manifest_matches_approval(
        audit_manifest,
        plan=day.plan,
        estimated_bytes=day.estimated_bytes,
        billing_project=approval.billing_project,
        source_contract_sha256=approval.source_contract_sha256,
    )
    provenance = audit_manifest.provenance
    if provenance is None:  # pragma: no cover - checked above
        raise RuntimeError("published audit manifest has no query provenance")
    ledger = _read_json_object(
        _ledger_path(output_base, day.plan_sha256), "completed execution ledger"
    )
    if (
        ledger.get("status") != "completed"
        or ledger.get("plan_sha256") != day.plan_sha256
        or ledger.get("job_id") != provenance.job_id
        or ledger.get("actual_bytes_processed") != provenance.actual_bytes_processed
        or ledger.get("total_rows") != provenance.total_rows
        or ledger.get("manifest_sha256") != audit_manifest.manifest_sha256
    ):
        raise RuntimeError("completed execution ledger differs from the local archive")
    remote = _verify_published_archive(_load_ovh_uploader(s3_config), audit_manifest)
    released = audit_manifest.byte_count
    if root.exists():
        if root.is_symlink() or not root.is_dir():
            raise RuntimeError("local archive root must be a non-symlink directory")
        local_manifest = load_archive_manifest(root / "manifest.json")
        verify_archive_manifest(local_manifest, root)
        if local_manifest != audit_manifest:
            raise RuntimeError("local archive differs from its published audit manifest")
        tombstone.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.replace(root, tombstone)
    if tombstone.is_symlink() or not tombstone.is_dir():
        raise RuntimeError("local archive tombstone must be a non-symlink directory")
    shutil.rmtree(tombstone)
    return {
        "removed": True,
        "archive_root": str(root),
        "bytes_released": released,
        "remote": remote,
    }
