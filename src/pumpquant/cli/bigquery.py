"""Data commands: plan, dry-run and archive BigQuery days; audit block archives."""

from __future__ import annotations

import argparse
import re
from dataclasses import asdict
from pathlib import Path
from typing import Any

from pumpquant.acquisition.bigquery.coordinator import (
    DEFAULT_COORDINATOR_WORKERS,
    MAX_COORDINATOR_WORKERS,
    CoordinatorError,
    coordinate_approved_days,
)
from pumpquant.acquisition.bigquery.daily import (
    DEFAULT_PAGE_ROW_CAP,
    BatchArchiveRequest,
    DailyArchiveRequest,
    archive_batch,
    archive_day_locked,
    load_query_provenance,
)
from pumpquant.acquisition.bigquery.source import (
    DEFAULT_MARKET_PROGRAM_IDS,
    MAX_SANDBOX_MONTHLY_BYTES,
    CostCapExceeded,
    DatasetRef,
    DryRunValidationError,
    QueryPlan,
    build_daily_pump_instruction_plans,
    build_pump_instruction_plan,
    build_pump_transaction_plan,
    dry_run,
    query_sha256,
    referenced_source_table_names,
    snapshot_report,
)
from pumpquant.acquisition.bigquery.storage_archive import archive_completed_query_job
from pumpquant.acquisition.block_inventory_audit import audit_block_inventory
from pumpquant.cli._output import print_json
from pumpquant.core.durable_state import atomic_private_json_write

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
    payload = snapshot_report(snapshot)
    print_json(payload)
    return 0 if payload["contract_ok"] else 2


def _cmd_bq_dry_run(args: argparse.Namespace) -> int:
    plan = _bigquery_plan_from_args(args)
    backend = _google_backend(args.billing_project)
    snapshot = backend.inspect_dataset(DatasetRef())
    snapshot_payload = snapshot_report(
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
    snapshot_payload = snapshot_report(snapshot, required_tables=("Instructions",))
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


def _cmd_bq_archive_day(args: argparse.Namespace) -> int:
    request = DailyArchiveRequest(
        billing_project=args.billing_project,
        start=args.start,
        end=args.end,
        approval_file=args.approval_file,
        output_dir=args.output_dir,
        s3_config=args.s3_config,
        execute=bool(args.execute),
        max_rows_per_shard=args.max_rows_per_shard,
        max_shard_bytes=args.max_shard_bytes,
        min_free_bytes=args.min_free_bytes,
        program_ids=None if args.program_ids is None else tuple(args.program_ids),
        page_row_cap=args.page_row_cap,
        prefetch_one_page=bool(args.prefetch_one_page),
    )
    outcome = archive_day_locked(request, _google_backend)
    print_json(outcome.payload)
    return outcome.code


def _cmd_bq_archive_storage(args: argparse.Namespace) -> int:
    """Archive a previously completed query job without submitting a query."""

    provenance = load_query_provenance(args.provenance_file)
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


def _cmd_bq_archive_batch(args: argparse.Namespace) -> int:
    request = BatchArchiveRequest(
        billing_project=args.billing_project,
        approval_file=args.approval_file,
        output_dir=args.output_dir,
        s3_config=args.s3_config,
        execute=bool(args.execute),
        cleanup_local=bool(args.cleanup_local),
        max_rows_per_shard=args.max_rows_per_shard,
        max_shard_bytes=args.max_shard_bytes,
        min_free_bytes=args.min_free_bytes,
        page_row_cap=args.page_row_cap,
        prefetch_one_page=bool(args.prefetch_one_page),
    )
    return archive_batch(request, _google_backend, report=print_json)


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


def _register_block_audit(subparsers: Any) -> None:
    """Compare a signature inventory with archived blocks."""
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


def _register_query_planning(subparsers: Any) -> None:
    """Plan, inspect and dry-run queries; none of these is billed."""
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


def _register_archiving(subparsers: Any) -> None:
    """Archive approved days under byte caps, one day, a batch, or coordinated."""
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
        default=DEFAULT_PAGE_ROW_CAP,
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
        default=DEFAULT_PAGE_ROW_CAP,
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


def register(subparsers: Any) -> None:
    _register_block_audit(subparsers)
    _register_query_planning(subparsers)
    _register_archiving(subparsers)
