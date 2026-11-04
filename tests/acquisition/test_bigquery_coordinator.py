from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from pumpquant.acquisition.bigquery.approval import (
    APPROVED_WINDOW_END,
    APPROVED_WINDOW_START,
    load_approval_artifact,
)
from pumpquant.acquisition.bigquery.coordinator import (
    CoordinatorPreflightError,
    JobMetadata,
    coordinate_approved_days,
)
from pumpquant.acquisition.bigquery.daily import DailyArchiveRequest, archive_day
from pumpquant.acquisition.bigquery.source import (
    DEFAULT_MARKET_PROGRAM_IDS,
    MAX_DAILY_BACKFILL_BYTES,
    MAX_SANDBOX_MONTHLY_BYTES,
    BackendDryRun,
    DatasetRef,
    DatasetSnapshot,
    DryRunResult,
    QueryPlan,
    build_daily_pump_instruction_plans,
    dataset_schema_sha256,
    query_sha256,
)
from tests.acquisition.test_bigquery_source import _valid_snapshot

GIB = 1024**3
DAILY_ESTIMATE = 2 * GIB


def _approval(path: Path) -> Path:
    """A valid 30-day approval artifact for the registered window, as a dry run writes it."""
    start, end = APPROVED_WINDOW_START.isoformat(), APPROVED_WINDOW_END.isoformat()
    plans = build_daily_pump_instruction_plans(
        start,
        end,
        maximum_bytes_billed=MAX_DAILY_BACKFILL_BYTES,
        program_ids=DEFAULT_MARKET_PROGRAM_IDS,
    )
    days = [
        {
            "start": (APPROVED_WINDOW_START + timedelta(days=index)).isoformat(),
            "end": (APPROVED_WINDOW_START + timedelta(days=index + 1)).isoformat(),
            "estimated_bytes": DAILY_ESTIMATE,
            "plan_sha256": query_sha256(plan),
        }
        for index, plan in enumerate(plans)
    ]
    total = DAILY_ESTIMATE * len(days)
    payload = {
        "artifact_version": 1,
        "status": "approved_dry_run_only",
        "captured_at": "2026-07-13T16:00:00Z",
        "billing_project": "test-project",
        "source_dataset": "bigquery-public-data.crypto_solana_mainnet_us",
        "source_location": "us-central1",
        "source_contract_sha256": dataset_schema_sha256(_valid_snapshot()),
        "plan_name": "pump_instruction_discovery_v1",
        "program_scope": "Pump and PumpSwap",
        "selected_window": {
            "start": start,
            "end": end,
            "complete_utc_days": 30,
            "finality_buffer_days": 7,
        },
        "rejected_latest_window": {
            "start": (APPROVED_WINDOW_START + timedelta(days=7)).isoformat(),
            "end": (date.fromisoformat(end) + timedelta(days=7)).isoformat(),
            "estimated_bytes": MAX_SANDBOX_MONTHLY_BYTES + 1,
            "reason": "over the cumulative cap",
        },
        "daily_maximum_bytes_billed": MAX_DAILY_BACKFILL_BYTES,
        "cumulative_maximum_bytes_billed": MAX_SANDBOX_MONTHLY_BYTES,
        "cumulative_estimated_bytes": total,
        "remaining_cumulative_bytes": MAX_SANDBOX_MONTHLY_BYTES - total,
        "combined_plan_sha256": "a" * 64,
        "query_executed": False,
        "bytes_billed": 0,
        "days": days,
    }
    path.write_text(json.dumps(payload))
    return path


class _Backend:
    """Metadata, dry runs and submissions only; no query rows are ever served."""

    def __init__(self, snapshot: DatasetSnapshot | None = None) -> None:
        self.snapshot = snapshot or _valid_snapshot()
        self.submitted: list[str] = []

    def inspect_dataset(self, ref: DatasetRef) -> DatasetSnapshot:
        assert ref == DatasetRef()
        return self.snapshot

    def estimate_query(self, plan: QueryPlan) -> BackendDryRun:
        return BackendDryRun(DAILY_ESTIMATE, plan.referenced_tables)

    def submit_query(self, plan: QueryPlan, approval: DryRunResult) -> str:
        job_id = f"job_{len(self.submitted)}"
        self.submitted.append(job_id)
        return job_id

    def get_job_metadata(self, plan: QueryPlan, job_id: str) -> JobMetadata:
        return JobMetadata(job_id=job_id, state="RUNNING")


def test_approval_fixture_is_valid(tmp_path: Path) -> None:
    approval = load_approval_artifact(_approval(tmp_path / "approval.json"))
    assert len(approval.days) == 30
    assert approval.cumulative_estimated_bytes == 30 * DAILY_ESTIMATE


def test_preflight_plans_every_day_without_submitting(tmp_path: Path) -> None:
    backend = _Backend()
    result = coordinate_approved_days(
        _approval(tmp_path / "approval.json"), tmp_path / "out", backend
    )
    assert result.status == "preflight_ready"
    assert not result.execute
    assert len(result.days) == 30
    assert {day.status for day in result.days} == {"ready"}
    assert result.cumulative_estimated_pending_bytes == 30 * DAILY_ESTIMATE
    assert backend.submitted == []
    ledger = json.loads(Path(result.ledger_path).read_text())
    assert ledger["status"] == "preflight_ready"
    assert len(ledger["days"]) == 30


def test_execute_submits_each_day_once_and_records_the_jobs(tmp_path: Path) -> None:
    approval = _approval(tmp_path / "approval.json")
    backend = _Backend()
    result = coordinate_approved_days(approval, tmp_path / "out", backend, execute=True, workers=4)
    assert result.status == "submitted"
    assert len(backend.submitted) == 30
    assert {day.status for day in result.days} == {"job_identified"}
    assert sorted(day.job_id or "" for day in result.days) == sorted(backend.submitted)
    ledger = json.loads(Path(result.ledger_path).read_text())
    assert ledger["status"] == "submitted"
    assert ledger["cumulative_estimated_pending_bytes"] == 0

    # A second run must not submit again while those jobs are still running.
    with pytest.raises(CoordinatorPreflightError, match="not complete"):
        coordinate_approved_days(approval, tmp_path / "out", backend, execute=True)
    assert len(backend.submitted) == 30


def test_a_changed_source_schema_blocks_the_run(tmp_path: Path) -> None:
    snapshot = _valid_snapshot()
    drifted = DatasetSnapshot(
        snapshot.ref, snapshot.location, snapshot.tables[1:], snapshot.captured_at
    )
    with pytest.raises(CoordinatorPreflightError):
        coordinate_approved_days(
            _approval(tmp_path / "approval.json"), tmp_path / "out", _Backend(drifted)
        )


def test_daily_archive_validates_a_day_without_running_a_query(tmp_path: Path) -> None:
    approval = _approval(tmp_path / "approval.json")
    backend = _Backend()
    request = DailyArchiveRequest(
        billing_project="test-project",
        start="2026-06-07",
        end="2026-06-08",
        approval_file=str(approval),
        output_dir=str(tmp_path / "out"),
        s3_config=None,
        execute=False,
        max_rows_per_shard=1_000,
        max_shard_bytes=1_000_000,
        min_free_bytes=0,
    )
    outcome = archive_day(request, lambda project: backend)
    assert outcome.code == 0
    assert outcome.payload["status"] == "ready_for_execution"
    assert outcome.payload["estimated_bytes"] == DAILY_ESTIMATE
    assert outcome.payload["query_executed"] is False
    assert backend.submitted == []
    assert not (tmp_path / "out").exists()


def test_daily_archive_blocks_on_estimate_drift(tmp_path: Path) -> None:
    class Drifted(_Backend):
        def estimate_query(self, plan: QueryPlan) -> BackendDryRun:
            return BackendDryRun(DAILY_ESTIMATE + 1, plan.referenced_tables)

    request = DailyArchiveRequest(
        billing_project="test-project",
        start="2026-06-07",
        end="2026-06-08",
        approval_file=str(_approval(tmp_path / "approval.json")),
        output_dir=str(tmp_path / "out"),
        s3_config=None,
        execute=False,
        max_rows_per_shard=1_000,
        max_shard_bytes=1_000_000,
        min_free_bytes=0,
    )
    outcome = archive_day(request, lambda project: Drifted())
    assert outcome.code == 6
    assert outcome.payload["status"] == "blocked_estimate_drift"
