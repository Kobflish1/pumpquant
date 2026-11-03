"""Data commands: plan, dry-run and archive BigQuery days; audit block archives."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib
import json
import os
import re
import shutil
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import asdict, replace
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
from pumpquant.acquisition.bigquery.coordinator import (
    DEFAULT_COORDINATOR_WORKERS,
    MAX_COORDINATOR_WORKERS,
    CoordinatorError,
    coordinate_approved_days,
)
from pumpquant.acquisition.bigquery.source import (
    DEFAULT_MARKET_PROGRAM_IDS,
    MAX_SANDBOX_MONTHLY_BYTES,
    CostCapExceeded,
    DatasetRef,
    DatasetSnapshot,
    DryRunValidationError,
    QueryPlan,
    audit_dataset,
    build_daily_pump_instruction_plans,
    build_pump_instruction_plan,
    build_pump_transaction_plan,
    dataset_schema_sha256,
    dry_run,
    query_sha256,
    referenced_source_table_names,
)
from pumpquant.acquisition.bigquery.storage_archive import archive_completed_query_job
from pumpquant.acquisition.block_inventory_audit import audit_block_inventory
from pumpquant.cli._output import print_json
from pumpquant.core.durable_state import atomic_private_json_write
from pumpquant.core.hashing import sha256_file

_BYTE_UNITS = {
    "": 1,
    "B": 1,
    "KB": 1_000,
    "MB": 1_000**2,
    "GB": 1_000**3,
    "TB": 1_000**4,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
    "TIB": 1024**4,
}

_DEFAULT_PAGE_ROW_CAP = 10_000
_MAX_PAGE_ROW_CAP = 100_000


def _positive_bytes(value: str) -> int:
    match = re.fullmatch(r"\s*([0-9]+)\s*([A-Za-z]*)\s*", value)
    if match is None:
        raise argparse.ArgumentTypeError("use an integer byte count or a suffix such as 512GiB")
    unit = match.group(2).upper()
    if unit not in _BYTE_UNITS:
        raise argparse.ArgumentTypeError(f"unsupported byte unit {match.group(2)!r}")
    result = int(match.group(1)) * _BYTE_UNITS[unit]
    if result <= 0:
        raise argparse.ArgumentTypeError("byte limit must be positive")
    return result


def _positive_page_rows(value: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("page-row cap must be a positive integer") from None
    if result <= 0 or result > _MAX_PAGE_ROW_CAP:
        raise argparse.ArgumentTypeError(f"page-row cap must be between 1 and {_MAX_PAGE_ROW_CAP}")
    return result


def _cmd_block_inventory_audit(args: argparse.Namespace) -> int:
    inventory = args.inventory_path or args.inventory_arg
    if inventory is None:
        raise ValueError("block-audit requires an inventory directory or manifest.json")
    archive_roots = list(args.archive_root or [])
    if args.inventory_path is not None and args.inventory_arg is not None:
        archive_roots.insert(0, args.inventory_arg)
    archive_roots.extend(args.archive_roots_arg or [])
    if not archive_roots:
        raise ValueError("block-audit requires at least one --archive-root")
    result = audit_block_inventory(
        inventory,
        archive_roots,
        batch_size=args.batch_size,
        include_signature_details=args.include_signature_details,
    )
    payload = result.to_dict()
    if args.output is not None:
        output = Path(args.output).expanduser()
        atomic_private_json_write(output, payload, exclusive=True)
    print_json(payload)
    return 0


def _bigquery_plan_from_args(args: argparse.Namespace) -> QueryPlan:
    program_ids = (
        DEFAULT_MARKET_PROGRAM_IDS if args.program_ids is None else tuple(args.program_ids)
    )
    builder = (
        build_pump_instruction_plan
        if args.plan_kind == "instruction-discovery"
        else build_pump_transaction_plan
    )
    return builder(
        args.start, args.end, maximum_bytes_billed=args.max_bytes, program_ids=program_ids
    )


def _query_plan_payload(plan: QueryPlan, *, show_sql: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "plan_name": plan.name,
        "plan_sha256": query_sha256(plan),
        "location": plan.location,
        "maximum_bytes_billed": plan.maximum_bytes_billed,
        "parameters": [asdict(parameter) for parameter in plan.parameters],
        "referenced_tables": plan.referenced_tables,
        "limitations": plan.limitations,
        "execution_enabled": False,
    }
    if show_sql:
        payload["sql"] = plan.sql
    return payload


def _snapshot_payload(
    snapshot: DatasetSnapshot, *, required_tables: tuple[str, ...] | None = None
) -> dict[str, Any]:
    issues = audit_dataset(snapshot, required_tables=required_tables)
    return {
        "dataset": snapshot.ref.full_name,
        "location": snapshot.location,
        "captured_at": snapshot.captured_at,
        "schema_sha256": dataset_schema_sha256(snapshot),
        "required_tables": (
            tuple(sorted(snapshot.table_map))
            if required_tables is None
            else tuple(sorted(required_tables))
        ),
        "contract_ok": not any(issue.severity == "error" for issue in issues),
        "issues": [asdict(issue) for issue in issues],
        "tables": [
            {
                "name": table.name,
                "location": table.location,
                "table_type": table.table_type,
                "partition_field": table.partition_field,
                "partition_granularity": table.partition_granularity,
                "require_partition_filter": table.require_partition_filter,
                "clustering_fields": table.clustering_fields,
                "num_rows": table.num_rows,
                "logical_bytes": table.logical_bytes,
                "created_at": table.created_at,
                "modified_at": table.modified_at,
                "fields": [asdict(field) for field in table.fields],
            }
            for table in snapshot.tables
        ],
    }


def _google_backend(billing_project: str) -> Any:
    from pumpquant.acquisition.bigquery.client import GoogleBigQueryBackend

    return GoogleBigQueryBackend(billing_project)


def _cmd_bq_plan(args: argparse.Namespace) -> int:
    plan = _bigquery_plan_from_args(args)
    print_json(_query_plan_payload(plan, show_sql=args.show_sql))
    return 0


def _cmd_bq_inspect(args: argparse.Namespace) -> int:
    backend = _google_backend(args.billing_project)
    snapshot = backend.inspect_dataset(DatasetRef())
    payload = _snapshot_payload(snapshot)
    print_json(payload)
    return 0 if payload["contract_ok"] else 2


def _cmd_bq_dry_run(args: argparse.Namespace) -> int:
    plan = _bigquery_plan_from_args(args)
    backend = _google_backend(args.billing_project)
    snapshot = backend.inspect_dataset(DatasetRef())
    snapshot_payload = _snapshot_payload(
        snapshot, required_tables=referenced_source_table_names(plan)
    )
    payload = {
        "plan": _query_plan_payload(plan, show_sql=args.show_sql),
        "source_contract": snapshot_payload,
        "query_executed": False,
        "bytes_billed": 0,
    }
    if not snapshot_payload["contract_ok"]:
        payload["status"] = "blocked_source_contract"
        print_json(payload)
        return 2
    try:
        result = dry_run(backend, plan)
    except CostCapExceeded as error:
        payload["status"] = "blocked_cost_cap"
        payload["dry_run"] = asdict(error.result)
        print_json(payload)
        return 3
    except DryRunValidationError as error:
        payload["status"] = "blocked_invalid_dry_run"
        payload["dry_run_error"] = str(error)
        print_json(payload)
        return 4
    payload["status"] = "dry_run_passed"
    payload["dry_run"] = asdict(result)
    print_json(payload)
    return 0


def _cmd_bq_batch_dry_run(args: argparse.Namespace) -> int:
    if args.total_max_bytes > MAX_SANDBOX_MONTHLY_BYTES:
        raise ValueError(f"Sandbox cumulative cap cannot exceed {MAX_SANDBOX_MONTHLY_BYTES} bytes")
    program_ids = (
        DEFAULT_MARKET_PROGRAM_IDS if args.program_ids is None else tuple(args.program_ids)
    )
    plans = build_daily_pump_instruction_plans(
        args.start,
        args.end,
        maximum_bytes_billed=args.daily_max_bytes,
        program_ids=program_ids,
    )
    backend = _google_backend(args.billing_project)
    snapshot = backend.inspect_dataset(DatasetRef())
    snapshot_payload = _snapshot_payload(snapshot, required_tables=("Instructions",))
    payload: dict[str, Any] = {
        "source_contract": snapshot_payload,
        "window": {
            "start": plans[0].parameters[0].value,
            "end": plans[-1].parameters[1].value,
            "complete_utc_days": len(plans),
        },
        "daily_maximum_bytes_billed": args.daily_max_bytes,
        "cumulative_maximum_bytes_billed": args.total_max_bytes,
        "query_executed": False,
        "bytes_billed": 0,
        "days": [],
    }
    if not snapshot_payload["contract_ok"]:
        payload["status"] = "blocked_source_contract"
        print_json(payload)
        return 2

    cumulative = 0
    for plan in plans:
        try:
            result = dry_run(backend, plan)
        except CostCapExceeded as error:
            payload["status"] = "blocked_daily_cost_cap"
            payload["blocked_day"] = {
                "plan": _query_plan_payload(plan, show_sql=False),
                "dry_run": asdict(error.result),
            }
            payload["cumulative_estimated_bytes"] = cumulative
            print_json(payload)
            return 3
        except DryRunValidationError as error:
            payload["status"] = "blocked_invalid_dry_run"
            payload["dry_run_error"] = str(error)
            payload["cumulative_estimated_bytes"] = cumulative
            print_json(payload)
            return 4
        cumulative += result.estimated_bytes
        payload["days"].append(
            {
                "start": plan.parameters[0].value,
                "end": plan.parameters[1].value,
                "plan_sha256": result.plan_sha256,
                "estimated_bytes": result.estimated_bytes,
                "maximum_bytes_billed": result.maximum_bytes_billed,
            }
        )

    payload["cumulative_estimated_bytes"] = cumulative
    payload["remaining_cumulative_bytes"] = max(args.total_max_bytes - cumulative, 0)
    if cumulative > args.total_max_bytes:
        payload["status"] = "blocked_cumulative_cost_cap"
        print_json(payload)
        return 5
    payload["status"] = "batch_dry_run_passed"
    print_json(payload)
    return 0


_ARCHIVE_PREFIX_ROOT = "raw/bigquery/crypto_solana_mainnet_us/v1/instructions"


def _read_json_object(path: Path, description: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(f"{description} is not valid readable JSON") from error
    if not isinstance(value, dict):
        raise RuntimeError(f"{description} must contain a JSON object")
    return value


def _load_query_provenance(path_value: str) -> QueryProvenance:
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


def _private_json_write(path: Path, payload: Mapping[str, Any], *, exclusive: bool) -> None:
    atomic_private_json_write(path, payload, exclusive=exclusive)


def _archive_prefix(plan_hash: str, start: str) -> str:
    return f"{_ARCHIVE_PREFIX_ROOT}/date={start[:10]}/plan={plan_hash}"


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
    if not isinstance(payload, dict):  # pragma: no cover - serializer invariant
        raise RuntimeError("canonical archive manifest did not serialize to an object")
    _private_json_write(path, payload, exclusive=False)
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
def _execution_lock(base: Path) -> Any:
    lock_path = base / ".executions" / ".lock"
    lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


@contextmanager
def _batch_lock(base: Path) -> Any:
    lock_path = base / ".batch" / ".lock"
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

    project_key = hashlib.sha256(billing_project.encode()).hexdigest()
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
        _private_json_write(binding_path, binding, exclusive=True)
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
    _private_json_write(
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


def _run_bq_archive_day(args: argparse.Namespace) -> int:
    page_row_cap = getattr(args, "page_row_cap", _DEFAULT_PAGE_ROW_CAP)
    prefetch_one_page = bool(getattr(args, "prefetch_one_page", False))
    (
        daily_cap,
        total_cap,
        expected_bytes,
        expected_plan_hash,
        expected_source_contract,
    ) = _load_daily_approval(
        args.approval_file,
        billing_project=args.billing_project,
        start=args.start,
        end=args.end,
    )
    program_ids = (
        DEFAULT_MARKET_PROGRAM_IDS if args.program_ids is None else tuple(args.program_ids)
    )
    plans = build_daily_pump_instruction_plans(
        args.start,
        args.end,
        maximum_bytes_billed=daily_cap,
        program_ids=program_ids,
    )
    if len(plans) != 1:
        raise RuntimeError("bq-archive-day accepts exactly one complete UTC day")
    plan = plans[0]
    plan_hash = query_sha256(plan)
    if plan_hash != expected_plan_hash:
        raise RuntimeError("daily query plan hash differs from the approved artifact")

    current_source_contract = expected_source_contract
    output_base = Path(args.output_dir).expanduser()
    root = _archive_root(output_base, plan_hash, str(plan.parameters[0].value))
    ledger = _ledger_path(output_base, plan_hash)
    payload: dict[str, Any] = {
        "status": "ready_for_execution",
        "plan_sha256": plan_hash,
        "start": plan.parameters[0].value,
        "end": plan.parameters[1].value,
        "estimated_bytes": expected_bytes,
        "maximum_bytes_billed": plan.maximum_bytes_billed,
        "source_contract_sha256": current_source_contract,
        "archive_root": str(root),
        "query_executed": False,
    }

    manifest_path = root / "manifest.json"
    if manifest_path.is_file():
        manifest = load_archive_manifest(manifest_path)
        verify_archive_manifest(manifest, root)
        _manifest_matches_approval(
            manifest,
            plan=plan,
            estimated_bytes=expected_bytes,
            billing_project=args.billing_project,
            source_contract_sha256=current_source_contract,
        )
        provenance = manifest.provenance
        if provenance is None:  # pragma: no cover - checked by approval matching
            raise RuntimeError("local archive manifest has no query provenance")
        if ledger.exists():
            existing_ledger = _read_json_object(ledger, "execution ledger")
            if (
                existing_ledger.get("status") not in {"job_identified", "completed"}
                or existing_ledger.get("plan_sha256") != plan_hash
                or existing_ledger.get("request_id") != provenance.request_id
                or existing_ledger.get("estimated_bytes") != provenance.dry_run_estimated_bytes
                or existing_ledger.get("maximum_bytes_billed") != provenance.maximum_bytes_billed
                or existing_ledger.get("job_id") != provenance.job_id
                or (
                    existing_ledger.get("actual_bytes_processed") is not None
                    and existing_ledger.get("actual_bytes_processed")
                    != provenance.actual_bytes_processed
                )
                or (
                    existing_ledger.get("total_rows") is not None
                    and existing_ledger.get("total_rows") != provenance.total_rows
                )
                or (
                    existing_ledger.get("manifest_sha256") is not None
                    and existing_ledger.get("manifest_sha256") != manifest.manifest_sha256
                )
            ):
                raise RuntimeError("local archive differs from the execution ledger")
        if not args.execute:
            payload["status"] = "existing_archive_verified"
            payload["archive"] = {
                "rows": manifest.row_count,
                "bytes": manifest.byte_count,
                "manifest_sha256": manifest.manifest_sha256,
            }
            print_json(payload)
            return 0
        if args.s3_config is None:
            raise RuntimeError("--s3-config is required to publish an existing archive")
        remote = _upload_archive(_load_ovh_uploader(args.s3_config), manifest, root)
        audit_manifest = _store_published_manifest(output_base, plan_hash, manifest)

        _private_json_write(
            ledger,
            {
                "status": "completed",
                "plan_sha256": plan_hash,
                "request_id": provenance.request_id,
                "estimated_bytes": expected_bytes,
                "maximum_bytes_billed": provenance.maximum_bytes_billed,
                "approved_maximum_bytes_billed": plan.maximum_bytes_billed,
                "source_contract_sha256": current_source_contract,
                "job_id": provenance.job_id,
                "actual_bytes_processed": provenance.actual_bytes_processed,
                "total_rows": manifest.row_count,
                "manifest_sha256": manifest.manifest_sha256,
                "published_manifest_path": str(audit_manifest),
                "remote_manifest_key": remote["manifest_key"],
            },
            exclusive=False,
        )
        payload.update(
            status="existing_archive_published",
            query_executed=False,
            archive={
                "rows": manifest.row_count,
                "bytes": manifest.byte_count,
                "manifest_sha256": manifest.manifest_sha256,
            },
            remote=remote,
        )
        print_json(payload)
        return 0
    if root.exists():
        raise RuntimeError("archive root exists without a verified manifest")
    from pumpquant.acquisition.bigquery.client import query_request_id

    execution_plan = plan
    request_id = query_request_id(plan)
    prior_attempt: dict[str, Any] | None = None
    resume_job_id: str | None = None
    if ledger.exists():
        prior_attempt = _read_json_object(ledger, "execution ledger")
        prior_execution_cap = prior_attempt.get("maximum_bytes_billed")
        if (
            isinstance(prior_execution_cap, bool)
            or not isinstance(prior_execution_cap, int)
            or prior_execution_cap < expected_bytes
            or prior_execution_cap > plan.maximum_bytes_billed
        ):
            raise RuntimeError("prior execution ledger has an invalid query byte cap")
        execution_plan = replace(plan, maximum_bytes_billed=prior_execution_cap)
        request_id = query_request_id(execution_plan)
        if (
            prior_attempt.get("plan_sha256") != plan_hash
            or prior_attempt.get("request_id") != request_id
            or prior_attempt.get("estimated_bytes") != expected_bytes
            or prior_attempt.get("approved_maximum_bytes_billed", plan.maximum_bytes_billed)
            != plan.maximum_bytes_billed
            or prior_attempt.get("source_contract_sha256", expected_source_contract)
            != expected_source_contract
        ):
            raise RuntimeError("prior execution ledger does not match the approved plan")

    uploader: OVHS3Uploader | None = None
    if args.execute:
        if args.s3_config is None:
            raise RuntimeError("--s3-config is required before query execution")
        uploader = _load_ovh_uploader(args.s3_config)
        recovered = _recover_remote_archive(
            uploader,
            plan=plan,
            expected_bytes=expected_bytes,
            billing_project=args.billing_project,
            source_contract_sha256=current_source_contract,
            output_base=output_base,
            ledger=ledger,
            prior_attempt=prior_attempt,
        )
        if recovered is not None:
            remote_manifest, remote = recovered
            payload.update(
                status="remote_archive_recovered_without_query",
                query_executed=False,
                archive={
                    "rows": remote_manifest.row_count,
                    "bytes": remote_manifest.byte_count,
                    "shards": len(remote_manifest.shards),
                    "manifest_sha256": remote_manifest.manifest_sha256,
                },
                remote=remote,
            )
            print_json(payload)
            return 0

    if prior_attempt is not None:
        candidate_job_id = prior_attempt.get("job_id")
        if prior_attempt.get("status") == "job_identified" and isinstance(candidate_job_id, str):
            resume_job_id = candidate_job_id
            payload["status"] = "existing_job_ready_for_resume"
        elif prior_attempt.get("status") == "execution_attempt_started":
            raise RuntimeError(
                "the prior query submission outcome is indeterminate; inspect BigQuery jobs"
            )
        elif prior_attempt.get("status") == "completed":
            audit_path = _published_manifest_path(output_base, plan_hash)
            manifest = load_archive_manifest(audit_path)
            _manifest_matches_approval(
                manifest,
                plan=plan,
                estimated_bytes=expected_bytes,
                billing_project=args.billing_project,
                source_contract_sha256=current_source_contract,
            )
            provenance = manifest.provenance
            if provenance is None or (
                prior_attempt.get("job_id") != provenance.job_id
                or prior_attempt.get("request_id") != provenance.request_id
                or prior_attempt.get("maximum_bytes_billed") != provenance.maximum_bytes_billed
                or prior_attempt.get("estimated_bytes") != provenance.dry_run_estimated_bytes
                or prior_attempt.get("actual_bytes_processed") != provenance.actual_bytes_processed
                or prior_attempt.get("total_rows") != provenance.total_rows
                or prior_attempt.get("manifest_sha256") != manifest.manifest_sha256
            ):
                raise RuntimeError("completed execution ledger differs from its audit manifest")
            payload.update(
                status="completed_audit_manifest_verified",
                archive={
                    "rows": manifest.row_count,
                    "bytes": manifest.byte_count,
                    "manifest_sha256": manifest.manifest_sha256,
                },
            )
            if args.s3_config is not None:
                payload.update(
                    status="completed_remote_archive_verified",
                    remote=_verify_published_archive(_load_ovh_uploader(args.s3_config), manifest),
                )
            elif args.execute:
                raise RuntimeError("--s3-config is required to verify a completed archive")
            print_json(payload)
            return 0
        else:
            raise RuntimeError("execution ledger is inconsistent with the local archive state")

    backend = _google_backend(args.billing_project)
    if resume_job_id is None or not args.execute:
        snapshot = backend.inspect_dataset(DatasetRef())
        snapshot_payload = _snapshot_payload(snapshot, required_tables=("Instructions",))
        if not snapshot_payload["contract_ok"]:
            print_json({"status": "blocked_source_contract", "source_contract": snapshot_payload})
            return 2
        current_source_contract = dataset_schema_sha256(snapshot)
        if current_source_contract != expected_source_contract:
            print_json(
                {
                    "status": "blocked_source_contract_drift",
                    "expected_source_contract_sha256": expected_source_contract,
                    "current_source_contract_sha256": current_source_contract,
                    "query_executed": False,
                }
            )
            return 7
        try:
            current_dry_run = dry_run(backend, plan)
        except CostCapExceeded as error:
            print_json({"status": "blocked_cost_cap", "dry_run": asdict(error.result)})
            return 3
        if current_dry_run.estimated_bytes != expected_bytes:
            print_json(
                {
                    "status": "blocked_estimate_drift",
                    "expected_bytes": expected_bytes,
                    "current_bytes": current_dry_run.estimated_bytes,
                    "query_executed": False,
                }
            )
            return 6
    if not args.execute:
        print_json(payload)
        return 0
    output_base.mkdir(mode=0o700, parents=True, exist_ok=True)
    if shutil.disk_usage(output_base).free < args.min_free_bytes:
        raise RuntimeError("insufficient free local disk for the guarded daily archive")
    try:
        importlib.import_module("pyarrow")
    except ImportError as error:
        raise RuntimeError("query execution requires: pip install 'pumpquant[archive]'") from error
    if uploader is None:  # pragma: no cover - execute path requires a config above
        raise RuntimeError("S3 uploader was not initialized")
    if resume_job_id is None:
        with _execution_lock(output_base):
            cumulative = _cumulative_ledger_bytes(output_base)
            execution_cap = _remaining_execution_cap(
                cumulative_actual_bytes=cumulative,
                approved_estimated_bytes=expected_bytes,
                daily_cap=plan.maximum_bytes_billed,
                cumulative_cap=total_cap,
            )
            execution_plan = replace(plan, maximum_bytes_billed=execution_cap)
            execution_approval = dry_run(backend, execution_plan)
            if execution_approval.estimated_bytes != expected_bytes:
                raise RuntimeError("execution-cap dry run differs from the approved estimate")
            _bind_project_output_root(
                billing_project=args.billing_project,
                output_base=output_base,
            )
            request_id = query_request_id(execution_plan)
            attempt = {
                "status": "execution_attempt_started",
                "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "plan_sha256": plan_hash,
                "request_id": request_id,
                "estimated_bytes": expected_bytes,
                "maximum_bytes_billed": execution_cap,
                "approved_maximum_bytes_billed": plan.maximum_bytes_billed,
                "cumulative_actual_bytes_before_query": cumulative,
                "source_contract_sha256": current_source_contract,
                "query_may_have_executed": True,
            }

            def record_job(job_id: str) -> None:
                attempt.update(status="job_identified", job_id=job_id)
                _private_json_write(ledger, attempt, exclusive=False)

            pages = backend.execute_query(
                execution_plan,
                execution_approval,
                max_results=page_row_cap,
                prefetch_one_page=prefetch_one_page,
                on_job_created=record_job,
            )
            _private_json_write(ledger, attempt, exclusive=True)
    else:
        if prior_attempt is None:  # pragma: no cover - guarded above
            raise RuntimeError("missing prior execution ledger")
        _bind_project_output_root(
            billing_project=args.billing_project,
            output_base=output_base,
        )
        attempt = prior_attempt
        pages = backend.resume_query(
            execution_plan,
            resume_job_id,
            max_results=page_row_cap,
            prefetch_one_page=prefetch_one_page,
        )
    first = next(pages)
    if first.total_rows is None or first.total_bytes_processed is None:
        raise RuntimeError("BigQuery result lacks final row or byte statistics")
    if attempt.get("job_id") != first.job_id:
        attempt.update(status="job_identified", job_id=first.job_id)
        _private_json_write(ledger, attempt, exclusive=False)
    provenance = QueryProvenance(
        plan_name=plan.name,
        plan_sha256=plan_hash,
        request_id=request_id,
        job_id=first.job_id,
        billing_project=args.billing_project,
        location=plan.location,
        source_tables=plan.referenced_tables,
        source_contract_sha256=current_source_contract,
        start=str(plan.parameters[0].value),
        end=str(plan.parameters[1].value),
        dry_run_estimated_bytes=expected_bytes,
        maximum_bytes_billed=execution_plan.maximum_bytes_billed,
        actual_bytes_processed=first.total_bytes_processed,
        total_rows=first.total_rows,
    )

    def all_pages() -> Any:
        yield first
        yield from pages

    manifest = archive_bigquery_pages(
        all_pages(),
        root,
        table="Instructions",
        max_rows_per_shard=args.max_rows_per_shard,
        max_bytes_per_shard=args.max_shard_bytes,
        provenance=provenance,
    )
    remote = _upload_archive(uploader, manifest, root)
    audit_manifest = _store_published_manifest(output_base, plan_hash, manifest)
    _private_json_write(
        ledger,
        {
            **attempt,
            "status": "completed",
            "actual_bytes_processed": first.total_bytes_processed,
            "total_rows": manifest.row_count,
            "manifest_sha256": manifest.manifest_sha256,
            "published_manifest_path": str(audit_manifest),
            "remote_manifest_key": remote["manifest_key"],
        },
        exclusive=False,
    )
    payload.update(
        status="archive_published",
        query_executed=True,
        job_id=first.job_id,
        actual_bytes_processed=first.total_bytes_processed,
        archive={
            "rows": manifest.row_count,
            "bytes": manifest.byte_count,
            "shards": len(manifest.shards),
            "manifest_sha256": manifest.manifest_sha256,
        },
        remote=remote,
    )
    print_json(payload)
    return 0


def _cmd_bq_archive_day(args: argparse.Namespace) -> int:
    """Serialize direct day runs against the batch coordinator for this output root."""

    output_base = Path(args.output_dir).expanduser()
    with _batch_lock(output_base):
        return _run_bq_archive_day(args)


def _cmd_bq_archive_storage(args: argparse.Namespace) -> int:
    """Archive a previously completed query job without submitting a query."""

    provenance = _load_query_provenance(args.provenance_file)
    manifest = archive_completed_query_job(
        billing_project=args.billing_project,
        job_id=args.job_id,
        location=args.location,
        output_dir=args.output_dir,
        provenance=provenance,
        max_streams=args.max_streams,
        max_rows_per_shard=args.max_rows_per_shard,
        max_bytes_per_shard=args.max_shard_bytes,
        table=args.table,
    )
    print_json(
        {
            "status": "storage_archive_written",
            "query_executed": False,
            "job_id": provenance.job_id,
            "archive_root": str(Path(args.output_dir).expanduser().resolve()),
            "archive": {
                "rows": manifest.row_count,
                "bytes": manifest.byte_count,
                "shards": len(manifest.shards),
                "manifest_sha256": manifest.manifest_sha256,
            },
        }
    )
    return 0


def _cleanup_verified_local_archive(
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


def _batch_state_path(output_base: Path, approval_sha256: str) -> Path:
    return output_base / ".batch" / f"{approval_sha256}.json"


def _cmd_bq_archive_batch(args: argparse.Namespace) -> int:
    approval_path = Path(args.approval_file).expanduser()
    try:
        approval_payload = _read_json_object(approval_path, "backfill approval")
        approval = validate_approval_artifact(approval_payload)
    except BigQueryApprovalError as error:
        raise RuntimeError(f"backfill approval validation failed: {error}") from error
    if approval.billing_project != args.billing_project:
        raise RuntimeError("backfill approval billing project mismatch")
    if args.execute and args.s3_config is None:
        raise RuntimeError("--s3-config is required before batch query execution")

    output_base = Path(args.output_dir).expanduser()
    page_row_cap = getattr(args, "page_row_cap", _DEFAULT_PAGE_ROW_CAP)
    prefetch_one_page = bool(getattr(args, "prefetch_one_page", False))
    output_base.mkdir(mode=0o700, parents=True, exist_ok=True)
    encoded_approval = (
        json.dumps(approval_payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    approval_sha256 = hashlib.sha256(encoded_approval).hexdigest()
    snapshot_path = output_base / ".batch" / "approvals" / f"{approval_sha256}.json"
    state_path = _batch_state_path(output_base, approval_sha256)
    completed: list[str] = []
    base_state: dict[str, Any] = {
        "version": 1,
        "approval_file_sha256": approval_sha256,
        "combined_plan_sha256": approval.combined_plan_sha256,
        "billing_project": approval.billing_project,
        "selected_window": {
            "start": approval.selected_start,
            "end": approval.selected_end,
        },
        "daily_maximum_bytes_billed": approval.daily_maximum_bytes_billed,
        "cumulative_maximum_bytes_billed": approval.cumulative_maximum_bytes_billed,
        "execute": bool(args.execute),
        "cleanup_local": bool(args.cleanup_local),
        "page_row_cap": page_row_cap,
        "prefetch_one_page": prefetch_one_page,
    }

    with _batch_lock(output_base):
        if args.execute:
            _bind_project_output_root(
                billing_project=args.billing_project,
                output_base=output_base,
            )
        _private_json_write(snapshot_path, approval_payload, exclusive=False)
        if sha256_file(snapshot_path) != approval_sha256:
            raise RuntimeError("durable approval snapshot hash mismatch")
        _private_json_write(
            state_path,
            {
                **base_state,
                "status": "running",
                "started_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "completed_plan_sha256": completed,
            },
            exclusive=False,
        )
        for day in approval.days:
            if sha256_file(snapshot_path) != approval_sha256:
                raise RuntimeError("durable approval snapshot changed during batch execution")
            day_args = argparse.Namespace(
                billing_project=args.billing_project,
                start=day.start,
                end=day.end,
                approval_file=str(snapshot_path),
                output_dir=str(output_base),
                s3_config=args.s3_config,
                program_ids=None,
                max_rows_per_shard=args.max_rows_per_shard,
                max_shard_bytes=args.max_shard_bytes,
                min_free_bytes=args.min_free_bytes,
                execute=args.execute,
                page_row_cap=page_row_cap,
                prefetch_one_page=prefetch_one_page,
            )
            try:
                return_code = _run_bq_archive_day(day_args)
            except Exception as error:
                _private_json_write(
                    state_path,
                    {
                        **base_state,
                        "status": "halted_error",
                        "failed_plan_sha256": day.plan_sha256,
                        "error_type": type(error).__name__,
                        "completed_plan_sha256": completed,
                    },
                    exclusive=False,
                )
                raise
            if return_code != 0:
                _private_json_write(
                    state_path,
                    {
                        **base_state,
                        "status": "halted_return_code",
                        "failed_plan_sha256": day.plan_sha256,
                        "return_code": return_code,
                        "completed_plan_sha256": completed,
                    },
                    exclusive=False,
                )
                return return_code
            cleanup: dict[str, Any] | None = None
            cumulative_actual: int | None = None
            try:
                if args.execute:
                    if args.cleanup_local:
                        if args.s3_config is None:  # pragma: no cover - checked above
                            raise RuntimeError("missing S3 configuration")
                        cleanup = _cleanup_verified_local_archive(
                            output_base=output_base,
                            day=day,
                            approval=approval,
                            s3_config=args.s3_config,
                        )
                    cumulative_actual = _cumulative_ledger_bytes(output_base)
                    if cumulative_actual > approval.cumulative_maximum_bytes_billed:
                        _private_json_write(
                            state_path,
                            {
                                **base_state,
                                "status": "halted_actual_over_cap",
                                "failed_plan_sha256": day.plan_sha256,
                                "cumulative_actual_bytes": cumulative_actual,
                                "completed_plan_sha256": [*completed, day.plan_sha256],
                            },
                            exclusive=False,
                        )
                        return 8
            except Exception as error:
                _private_json_write(
                    state_path,
                    {
                        **base_state,
                        "status": "halted_postprocess_error",
                        "failed_plan_sha256": day.plan_sha256,
                        "error_type": type(error).__name__,
                        "completed_plan_sha256": completed,
                    },
                    exclusive=False,
                )
                raise
            completed.append(day.plan_sha256)
            _private_json_write(
                state_path,
                {
                    **base_state,
                    "status": "running",
                    "last_completed_plan_sha256": day.plan_sha256,
                    "completed_plan_sha256": completed,
                    "cumulative_actual_bytes": cumulative_actual,
                    "last_cleanup": cleanup,
                },
                exclusive=False,
            )

        final_payload = {
            **base_state,
            "status": "completed" if args.execute else "validation_completed",
            "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "completed_plan_sha256": completed,
            "cumulative_actual_bytes": (
                _cumulative_ledger_bytes(output_base) if args.execute else None
            ),
        }
        _private_json_write(state_path, final_payload, exclusive=False)
        print_json(final_payload)
        return 0


def _cmd_bq_coordinate(args: argparse.Namespace) -> int:
    """Preflight, then optionally submit approved days without reading rows."""

    backend = _google_backend(args.billing_project)
    try:
        result = coordinate_approved_days(
            args.approval_file,
            args.output_dir,
            backend,
            execute=bool(args.execute),
            workers=args.workers,
        )
    except CoordinatorError as error:
        print_json(
            {
                "status": "blocked",
                "execute": bool(args.execute),
                "error_type": type(error).__name__,
                "error": str(error),
                "query_executed": False,
            }
        )
        return 2
    payload = result.to_payload()
    payload["query_executed"] = bool(args.execute and result.submitted)
    payload["result_rows_retrieved"] = False
    print_json(payload)
    return 0


def register(subparsers: Any) -> None:
    block_audit_parser = subparsers.add_parser(
        "block-audit",
        help="compare a sealed signature inventory with archived getBlock segments",
    )
    block_audit_parser.add_argument(
        "inventory_arg",
        nargs="?",
        help="inventory directory or manifest.json (may also be supplied with --inventory)",
    )
    block_audit_parser.add_argument(
        "archive_roots_arg",
        nargs="*",
        help="archive roots searched recursively for full-block segment manifests",
    )
    block_audit_parser.add_argument(
        "--inventory",
        "--inventory-path",
        "--inventory-manifest",
        "--inventory-parquet",
        dest="inventory_path",
        help="sealed signature inventory directory or manifest.json",
    )
    block_audit_parser.add_argument(
        "--archive-root",
        "--archive",
        action="append",
        dest="archive_root",
        help="archive root to search recursively (repeatable)",
    )
    block_audit_parser.add_argument(
        "--batch-size",
        type=_positive_page_rows,
        default=4_096,
        help="inventory Parquet rows per bounded read batch",
    )
    block_audit_parser.add_argument(
        "--include-signature-details",
        action="store_true",
        help="include complete sorted intersection/missing/extra signature arrays",
    )
    block_audit_parser.add_argument(
        "--output",
        help="optional path for the canonical compact JSON audit (stdout is always emitted)",
    )
    block_audit_parser.set_defaults(handler=_cmd_block_inventory_audit)

    bq_plan_parser = subparsers.add_parser(
        "bq-plan", help="build a bounded Solana BigQuery plan without authentication"
    )
    bq_plan_parser.add_argument("--start", required=True, help="inclusive UTC timestamp or date")
    bq_plan_parser.add_argument("--end", required=True, help="exclusive UTC timestamp or date")
    bq_plan_parser.add_argument(
        "--plan",
        dest="plan_kind",
        choices=("instruction-discovery", "transaction-bundle"),
        default="transaction-bundle",
        help="clustered discovery rows or the expensive full transaction bundle",
    )
    bq_plan_parser.add_argument(
        "--max-bytes",
        type=_positive_bytes,
        required=True,
        help="mandatory dry-run/execution ceiling, e.g. 512GiB",
    )
    bq_plan_parser.add_argument(
        "--program-id",
        action="append",
        dest="program_ids",
        help="repeat to override the default Pump and PumpSwap program IDs",
    )
    bq_plan_parser.add_argument("--show-sql", action="store_true")
    bq_plan_parser.set_defaults(handler=_cmd_bq_plan)

    bq_inspect_parser = subparsers.add_parser(
        "bq-inspect", help="inspect the live Solana dataset metadata without scanning rows"
    )
    bq_inspect_parser.add_argument("--billing-project", required=True)
    bq_inspect_parser.set_defaults(handler=_cmd_bq_inspect)

    bq_dry_run_parser = subparsers.add_parser(
        "bq-dry-run", help="inspect metadata and run a non-billable query estimate"
    )
    bq_dry_run_parser.add_argument("--billing-project", required=True)
    bq_dry_run_parser.add_argument("--start", required=True, help="inclusive UTC timestamp or date")
    bq_dry_run_parser.add_argument("--end", required=True, help="exclusive UTC timestamp or date")
    bq_dry_run_parser.add_argument(
        "--plan",
        dest="plan_kind",
        choices=("instruction-discovery", "transaction-bundle"),
        default="transaction-bundle",
        help="clustered discovery rows or the expensive full transaction bundle",
    )
    bq_dry_run_parser.add_argument(
        "--max-bytes",
        type=_positive_bytes,
        required=True,
        help="mandatory ceiling; the command blocks when the estimate is larger",
    )
    bq_dry_run_parser.add_argument(
        "--program-id",
        action="append",
        dest="program_ids",
        help="repeat to override the default Pump and PumpSwap program IDs",
    )
    bq_dry_run_parser.add_argument("--show-sql", action="store_true")
    bq_dry_run_parser.set_defaults(handler=_cmd_bq_dry_run)

    bq_batch_dry_run_parser = subparsers.add_parser(
        "bq-batch-dry-run",
        help="dry-run complete UTC days and enforce a cumulative Sandbox ceiling",
    )
    bq_batch_dry_run_parser.add_argument("--billing-project", required=True)
    bq_batch_dry_run_parser.add_argument(
        "--start", required=True, help="inclusive midnight UTC date or timestamp"
    )
    bq_batch_dry_run_parser.add_argument(
        "--end", required=True, help="exclusive midnight UTC date or timestamp"
    )
    bq_batch_dry_run_parser.add_argument(
        "--daily-max-bytes",
        type=_positive_bytes,
        required=True,
        help="server-side ceiling registered for every daily plan",
    )
    bq_batch_dry_run_parser.add_argument(
        "--total-max-bytes",
        type=_positive_bytes,
        required=True,
        help="cumulative ceiling across every daily estimate",
    )
    bq_batch_dry_run_parser.add_argument(
        "--program-id",
        action="append",
        dest="program_ids",
        help="repeat to override the default Pump and PumpSwap program IDs",
    )
    bq_batch_dry_run_parser.set_defaults(handler=_cmd_bq_batch_dry_run)

    bq_archive_day_parser = subparsers.add_parser(
        "bq-archive-day",
        help="execute one pre-approved UTC day and publish a verified raw archive",
    )
    bq_archive_day_parser.add_argument("--billing-project", required=True)
    bq_archive_day_parser.add_argument(
        "--start", required=True, help="approved inclusive midnight UTC timestamp"
    )
    bq_archive_day_parser.add_argument(
        "--end", required=True, help="approved exclusive midnight UTC timestamp"
    )
    bq_archive_day_parser.add_argument(
        "--approval-file",
        required=True,
        help="audited 30-day dry-run approval JSON",
    )
    bq_archive_day_parser.add_argument(
        "--output-dir",
        required=True,
        help="local archive and durable execution-ledger root",
    )
    bq_archive_day_parser.add_argument(
        "--s3-config",
        help="private 0600 JSON with explicit OVH S3 endpoint and credentials",
    )
    bq_archive_day_parser.add_argument(
        "--program-id",
        action="append",
        dest="program_ids",
        help="repeat to override the approved default Pump/PumpSwap IDs",
    )
    bq_archive_day_parser.add_argument("--max-rows-per-shard", type=int, default=100_000)
    bq_archive_day_parser.add_argument(
        "--max-shard-bytes", type=_positive_bytes, default=256 * 1024 * 1024
    )
    bq_archive_day_parser.add_argument(
        "--page-row-cap",
        type=_positive_page_rows,
        default=_DEFAULT_PAGE_ROW_CAP,
        help=(
            "maximum rows requested per BigQuery result page (1-100000); "
            "BigQuery response-size limits may still reduce each page"
        ),
    )
    bq_archive_day_parser.add_argument(
        "--prefetch-one-page",
        action="store_true",
        help="opt in to one bounded REST result-page prefetch",
    )
    bq_archive_day_parser.add_argument(
        "--min-free-bytes", type=_positive_bytes, default=10 * 1024**3
    )
    bq_archive_day_parser.add_argument(
        "--execute",
        action="store_true",
        help="submit the approved query; omitted means revalidate and report only",
    )
    bq_archive_day_parser.set_defaults(handler=_cmd_bq_archive_day)

    bq_archive_storage_parser = subparsers.add_parser(
        "bq-archive-storage",
        help="archive an existing DONE query job through the read-only Storage Read API",
    )
    bq_archive_storage_parser.add_argument("--billing-project", required=True)
    bq_archive_storage_parser.add_argument("--job-id", required=True)
    bq_archive_storage_parser.add_argument("--location", required=True)
    bq_archive_storage_parser.add_argument(
        "--provenance-file",
        required=True,
        help="JSON object containing canonical QueryProvenance fields",
    )
    bq_archive_storage_parser.add_argument(
        "--output-dir",
        required=True,
        help="new local archive root; existing roots and stale staging are rejected",
    )
    bq_archive_storage_parser.add_argument("--table", help="manifest table name override")
    bq_archive_storage_parser.add_argument(
        "--max-streams",
        type=int,
        default=32,
        help="bounded Storage Read stream request (1-128)",
    )
    bq_archive_storage_parser.add_argument("--max-rows-per-shard", type=int, default=100_000)
    bq_archive_storage_parser.add_argument(
        "--max-shard-bytes", type=_positive_bytes, default=256 * 1024 * 1024
    )
    bq_archive_storage_parser.set_defaults(handler=_cmd_bq_archive_storage)

    bq_archive_batch_parser = subparsers.add_parser(
        "bq-archive-batch",
        help="process the complete approved window sequentially with durable recovery",
    )
    bq_archive_batch_parser.add_argument("--billing-project", required=True)
    bq_archive_batch_parser.add_argument(
        "--approval-file",
        required=True,
        help="audited 30-day dry-run approval JSON",
    )
    bq_archive_batch_parser.add_argument(
        "--output-dir",
        required=True,
        help="local archive, audit-manifest, batch-state, and execution-ledger root",
    )
    bq_archive_batch_parser.add_argument(
        "--s3-config",
        help="private 0600 JSON with explicit OVH S3 endpoint and credentials",
    )
    bq_archive_batch_parser.add_argument("--max-rows-per-shard", type=int, default=100_000)
    bq_archive_batch_parser.add_argument(
        "--max-shard-bytes", type=_positive_bytes, default=256 * 1024 * 1024
    )
    bq_archive_batch_parser.add_argument(
        "--page-row-cap",
        type=_positive_page_rows,
        default=_DEFAULT_PAGE_ROW_CAP,
        help=(
            "maximum rows requested per BigQuery result page (1-100000); "
            "BigQuery response-size limits may still reduce each page"
        ),
    )
    bq_archive_batch_parser.add_argument(
        "--prefetch-one-page",
        action="store_true",
        help="opt in to one bounded REST result-page prefetch",
    )
    bq_archive_batch_parser.add_argument(
        "--min-free-bytes", type=_positive_bytes, default=10 * 1024**3
    )
    bq_archive_batch_parser.add_argument(
        "--cleanup-local",
        action="store_true",
        help=(
            "remove a day's local shards only after its OVH archive, audit manifest, "
            "and completed ledger are fully reverified"
        ),
    )
    bq_archive_batch_parser.add_argument(
        "--execute",
        action="store_true",
        help="execute or recover approved days; omitted validates every day without submission",
    )
    bq_archive_batch_parser.set_defaults(handler=_cmd_bq_archive_batch)

    bq_coordinate_parser = subparsers.add_parser(
        "bq-coordinate",
        help=(
            "preflight an approved window and, with --execute, submit remaining days "
            "concurrently without retrieving query rows"
        ),
    )
    bq_coordinate_parser.add_argument("--billing-project", required=True)
    bq_coordinate_parser.add_argument(
        "--approval-file",
        required=True,
        help="audited 30-day dry-run approval JSON",
    )
    bq_coordinate_parser.add_argument(
        "--output-dir",
        required=True,
        help="private durable coordinator and execution-ledger root",
    )
    bq_coordinate_parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_COORDINATOR_WORKERS,
        choices=range(1, MAX_COORDINATOR_WORKERS + 1),
        help=(
            f"maximum concurrent submissions (1-{MAX_COORDINATOR_WORKERS}; "
            f"default {DEFAULT_COORDINATOR_WORKERS})"
        ),
    )
    bq_coordinate_parser.add_argument(
        "--execute",
        action="store_true",
        help="submit still-unidentified approved jobs; omitted means preflight only",
    )
    bq_coordinate_parser.set_defaults(handler=_cmd_bq_coordinate)
