from __future__ import annotations

import json
from pathlib import Path

import pytest

from pumpquant.research.tail_exit_runtime import (
    CheckpointStore,
    ProgressEstimator,
    ProgressPublisher,
    TailExitRuntimeError,
    WorkPlan,
    WorkUnit,
    file_record,
)


def plan(run_id: str = "tail-exit-v2-test") -> WorkPlan:
    return WorkPlan(
        run_id=run_id,
        experiment_id="fresh-tail-exit-v1",
        contract={"feature_sha256": "a" * 64, "shards": 2},
        units=(
            WorkUnit("verify", "prepare", "prepare", 10.0, 10.0),
            WorkUnit("label.0", "labels", "label", 40.0, 50.0),
            WorkUnit("label.1", "labels", "label", 40.0, 50.0),
            WorkUnit("seal", "finalize", "finalize", 10.0, 5.0),
        ),
    )


def test_plan_identity_is_canonical_and_sensitive() -> None:
    first = plan()
    assert first.sha256 == plan().sha256
    assert first.total_weight == 100.0
    assert first.sha256 != plan("tail-exit-v2-other").sha256
    with pytest.raises(ValueError, match="duplicate"):
        WorkPlan(
            run_id="duplicate-test",
            experiment_id="test",
            contract={},
            units=(
                WorkUnit("same", "stage", "kind", 1.0, 1.0),
                WorkUnit("same", "stage", "kind", 1.0, 1.0),
            ),
        )


def test_checkpoint_adopts_only_plan_bound_verified_files(tmp_path: Path) -> None:
    selected_plan = plan()
    store = CheckpointStore(tmp_path / "work", selected_plan)
    payload = store.root / "labels" / "part-0.parquet"
    payload.parent.mkdir()
    payload.write_bytes(b"verified-label-shard")
    store.record(
        "label.0",
        duration_seconds=42.0,
        files=[file_record(store.root, payload)],
        detail={"row_count": 7},
    )

    resumed = CheckpointStore(store.root, selected_plan)
    assert resumed.is_complete("label.0")
    assert resumed.completed_units["label.0"]["detail"] == {"row_count": 7}

    payload.write_bytes(b"corrupt")
    with pytest.raises(TailExitRuntimeError, match="verification failed"):
        CheckpointStore(store.root, selected_plan)
    with pytest.raises(TailExitRuntimeError, match="plan identity"):
        CheckpointStore(tmp_path / "work", plan("different-run"))


def test_weighted_progress_is_monotonic_and_eta_becomes_measured() -> None:
    selected_plan = plan()
    estimator = ProgressEstimator(selected_plan, {})
    estimator.start_unit("verify")
    initial = estimator.snapshot()
    assert initial["overall_percent"] == 0.0
    assert float(initial["eta_seconds"]) == pytest.approx(115.0, abs=0.1)
    assert initial["eta_source"] == "historical_bootstrap"

    completed = {
        "verify": {
            "duration_seconds": 8.0,
            "completed_at_epoch": 1.0,
            "files": [],
        },
        "label.0": {
            "duration_seconds": 30.0,
            "completed_at_epoch": 2.0,
            "files": [],
        },
    }
    estimator.adopt(completed)
    measured = estimator.snapshot("label.1")
    assert measured["overall_percent"] == 50.0
    assert measured["eta_seconds"] == 35.0
    assert measured["eta_source"] == "measured_hybrid"
    assert measured["eta_sample_count"] == 2


def test_progress_publisher_sequences_and_rejects_other_plan(tmp_path: Path) -> None:
    selected_plan = plan()
    root = tmp_path / "progress"
    publisher = ProgressPublisher(root, selected_plan)
    first = publisher.publish({"kind": "pumpquant.fresh_tail_exit_v2_progress"})
    second = publisher.publish({"kind": "pumpquant.fresh_tail_exit_v2_progress"})
    assert first["sequence"] == 1
    assert second["sequence"] == 2
    assert json.loads((root / "current.json").read_text())["sequence"] == 2

    resumed = ProgressPublisher(root, selected_plan)
    assert resumed.publish({"kind": "pumpquant.fresh_tail_exit_v2_progress"})["sequence"] == 3
    with pytest.raises(TailExitRuntimeError, match="plan identity"):
        ProgressPublisher(root, plan("other-run"))
