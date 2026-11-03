"""Durable execution primitives for the v2 tail/exit research runner.

This module deliberately contains no research or DuckDB logic. It owns the
immutable work plan, restart checkpoints, weighted progress, ETA estimation,
and manifest-last current-state publication used by the worker and dashboard.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import median

from pumpquant.core.hashing import sha256_file

RUNTIME_VERSION = 2
CHECKPOINT_MARKER = ".pumpquant-tail-exit-v2-checkpoint.json"
CHECKPOINT_MANIFEST = "checkpoint-manifest.json"
_UNIT_ID = re.compile(r"^[a-z][a-z0-9_.-]*$")


class TailExitRuntimeError(RuntimeError):
    """A v2 plan, checkpoint, or publication invariant failed."""


def canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    )


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _atomic_replace(path: Path, body: bytes, token: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{token}.tmp")
    with temporary.open("xb") as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class WorkUnit:
    """One bounded, restartable execution unit."""

    unit_id: str
    stage: str
    kind: str
    weight: float
    bootstrap_seconds: float

    def __post_init__(self) -> None:
        for label, value in (
            ("unit id", self.unit_id),
            ("stage", self.stage),
            ("kind", self.kind),
        ):
            if not _UNIT_ID.fullmatch(value):
                raise ValueError(f"unsafe {label}: {value!r}")
        if not math.isfinite(self.weight) or self.weight <= 0.0:
            raise ValueError("work-unit weight must be positive and finite")
        if not math.isfinite(self.bootstrap_seconds) or self.bootstrap_seconds <= 0.0:
            raise ValueError("bootstrap duration must be positive and finite")


@dataclass(frozen=True, slots=True)
class WorkPlan:
    """Canonical run identity and ordered computational denominator."""

    run_id: str
    experiment_id: str
    contract: dict[str, object]
    units: tuple[WorkUnit, ...]

    def __post_init__(self) -> None:
        if not _UNIT_ID.fullmatch(self.run_id):
            raise ValueError(f"unsafe run id: {self.run_id!r}")
        if not self.experiment_id.strip():
            raise ValueError("experiment id cannot be empty")
        if not self.units:
            raise ValueError("work plan cannot be empty")
        identities = [unit.unit_id for unit in self.units]
        if len(identities) != len(set(identities)):
            raise ValueError("work plan contains duplicate unit ids")

    @property
    def payload(self) -> dict[str, object]:
        return {
            "kind": "pumpquant.tail_exit_work_plan",
            "version": RUNTIME_VERSION,
            "run_id": self.run_id,
            "experiment_id": self.experiment_id,
            "contract": self.contract,
            "units": [asdict(unit) for unit in self.units],
        }

    @property
    def sha256(self) -> str:
        return sha256_bytes(canonical_json(self.payload))

    @property
    def total_weight(self) -> float:
        return sum(unit.weight for unit in self.units)

    def unit(self, unit_id: str) -> WorkUnit:
        for unit in self.units:
            if unit.unit_id == unit_id:
                return unit
        raise TailExitRuntimeError(f"unknown work unit: {unit_id}")


def _relative_file(root: Path, value: object) -> Path:
    if not isinstance(value, str) or not value:
        raise TailExitRuntimeError("checkpoint file path is invalid")
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise TailExitRuntimeError(f"unsafe checkpoint file path: {value!r}")
    path = root / relative
    if path.is_symlink() or not path.is_file():
        raise TailExitRuntimeError(f"checkpoint file is missing or unsafe: {relative}")
    return path


def file_record(root: Path, path: Path) -> dict[str, object]:
    resolved_root = root.resolve()
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(resolved_root)
    except ValueError as error:
        raise TailExitRuntimeError("checkpoint file is outside its work root") from error
    if path.is_symlink() or not path.is_file():
        raise TailExitRuntimeError(f"checkpoint file is missing or unsafe: {path}")
    return {
        "path": relative.as_posix(),
        "byte_count": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def verify_file_record(root: Path, record: object) -> Path:
    if not isinstance(record, dict):
        raise TailExitRuntimeError("checkpoint file record is not an object")
    path = _relative_file(root, record.get("path"))
    byte_count = record.get("byte_count")
    digest = record.get("sha256")
    if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count < 0:
        raise TailExitRuntimeError("checkpoint byte count is invalid")
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise TailExitRuntimeError("checkpoint SHA-256 is invalid")
    if path.stat().st_size != byte_count or sha256_file(path) != digest:
        raise TailExitRuntimeError(f"checkpoint file verification failed: {path.name}")
    return path


class CheckpointStore:
    """Manifest-last durable unit ledger with strict plan binding."""

    def __init__(self, root: Path, plan: WorkPlan) -> None:
        self.root = root.resolve()
        self.plan = plan
        if self.root.is_symlink():
            raise TailExitRuntimeError("checkpoint root must not be a symlink")
        marker = self.root / CHECKPOINT_MARKER
        if self.root.exists():
            if not self.root.is_dir() or not marker.is_file() or marker.is_symlink():
                raise TailExitRuntimeError("existing checkpoint root is not v2-managed")
            payload = json.loads(marker.read_text(encoding="utf-8"))
            if payload.get("plan_sha256") != plan.sha256:
                raise TailExitRuntimeError("checkpoint plan identity differs")
        else:
            self.root.mkdir(parents=True)
            _atomic_replace(
                marker,
                canonical_json(
                    {
                        "kind": "pumpquant.tail_exit_checkpoint_marker",
                        "version": RUNTIME_VERSION,
                        "plan_sha256": plan.sha256,
                        "run_id": plan.run_id,
                    }
                ),
                "create",
            )
        self._manifest_path = self.root / CHECKPOINT_MANIFEST
        self._completed: dict[str, dict[str, object]] = {}
        if self._manifest_path.exists():
            self._load()
        else:
            self._publish()

    def _load(self) -> None:
        raw = json.loads(self._manifest_path.read_text(encoding="utf-8"))
        if (
            raw.get("kind") != "pumpquant.tail_exit_checkpoint_manifest"
            or raw.get("version") != RUNTIME_VERSION
            or raw.get("plan_sha256") != self.plan.sha256
        ):
            raise TailExitRuntimeError("checkpoint manifest identity is invalid")
        completed = raw.get("completed_units")
        if not isinstance(completed, dict):
            raise TailExitRuntimeError("checkpoint completed-unit ledger is invalid")
        for unit_id, metadata in completed.items():
            self.plan.unit(str(unit_id))
            if not isinstance(metadata, dict):
                raise TailExitRuntimeError("checkpoint unit metadata is invalid")
            files = metadata.get("files", [])
            if not isinstance(files, list):
                raise TailExitRuntimeError("checkpoint unit files are invalid")
            for record in files:
                verify_file_record(self.root, record)
            duration = metadata.get("duration_seconds")
            if (
                not isinstance(duration, (int, float))
                or not math.isfinite(duration)
                or duration < 0
            ):
                raise TailExitRuntimeError("checkpoint unit duration is invalid")
            self._completed[str(unit_id)] = dict(metadata)

    def _publish(self) -> None:
        payload = {
            "kind": "pumpquant.tail_exit_checkpoint_manifest",
            "version": RUNTIME_VERSION,
            "plan_sha256": self.plan.sha256,
            "run_id": self.plan.run_id,
            "updated_at_epoch": time.time(),
            "completed_units": self._completed,
        }
        _atomic_replace(
            self._manifest_path,
            canonical_json(payload),
            f"{time.time_ns()}",
        )

    @property
    def completed_units(self) -> dict[str, dict[str, object]]:
        return {key: dict(value) for key, value in self._completed.items()}

    def is_complete(self, unit_id: str) -> bool:
        self.plan.unit(unit_id)
        return unit_id in self._completed

    def record(
        self,
        unit_id: str,
        *,
        duration_seconds: float,
        files: list[dict[str, object]] | None = None,
        detail: dict[str, object] | None = None,
    ) -> None:
        self.plan.unit(unit_id)
        if not math.isfinite(duration_seconds) or duration_seconds < 0.0:
            raise ValueError("checkpoint duration must be finite and non-negative")
        records = list(files or [])
        for record in records:
            verify_file_record(self.root, record)
        metadata: dict[str, object] = {
            "duration_seconds": duration_seconds,
            "completed_at_epoch": time.time(),
            "files": records,
        }
        if detail:
            metadata["detail"] = dict(detail)
        previous = self._completed.get(unit_id)
        if previous is not None:
            if previous != metadata:
                raise TailExitRuntimeError(f"checkpoint unit already exists: {unit_id}")
            return
        self._completed[unit_id] = metadata
        self._publish()


def _numbers(records: Iterable[dict[str, object]], key: str) -> list[float]:
    """The numeric values recorded under ``key``, skipping records without one."""
    return [float(value) for record in records if isinstance(value := record.get(key), int | float)]


class ProgressEstimator:
    """Monotonic weighted progress with robust per-kind ETA adaptation."""

    def __init__(self, plan: WorkPlan, completed: dict[str, dict[str, object]]) -> None:
        self.plan = plan
        self.started_monotonic = time.perf_counter()
        self.started_epoch = time.time()
        self.current_unit_id: str | None = None
        self.current_unit_started_monotonic: float | None = None
        self.adopt(completed)

    def adopt(self, completed: dict[str, dict[str, object]]) -> None:
        self.completed = dict(completed)
        records = self.completed.values()
        self.prior_elapsed_seconds = sum(_numbers(records, "duration_seconds"))
        self.last_completed_at_epoch = max(_numbers(records, "completed_at_epoch"), default=None)

    def start_unit(self, unit_id: str) -> None:
        self.plan.unit(unit_id)
        if unit_id in self.completed:
            raise TailExitRuntimeError(f"cannot start completed work unit: {unit_id}")
        self.current_unit_id = unit_id
        self.current_unit_started_monotonic = time.perf_counter()

    def finish_unit(self) -> None:
        self.current_unit_id = None
        self.current_unit_started_monotonic = None

    def _kind_samples(self) -> dict[str, list[float]]:
        samples: dict[str, list[float]] = {}
        for unit_id, metadata in self.completed.items():
            duration = metadata.get("duration_seconds")
            if isinstance(duration, (int, float)) and duration > 0.0:
                samples.setdefault(self.plan.unit(unit_id).kind, []).append(float(duration))
        return samples

    def snapshot(self, current_unit_id: str | None = None) -> dict[str, object]:
        current_unit_id = current_unit_id or self.current_unit_id
        completed_ids = set(self.completed)
        completed_weight = sum(
            unit.weight for unit in self.plan.units if unit.unit_id in completed_ids
        )
        total_weight = self.plan.total_weight
        overall_percent = min(100.0, max(0.0, completed_weight / total_weight * 100.0))
        samples = self._kind_samples()
        remaining_seconds = 0.0
        measured_kinds: set[str] = set()
        current_elapsed = (
            time.perf_counter() - self.current_unit_started_monotonic
            if current_unit_id == self.current_unit_id
            and self.current_unit_started_monotonic is not None
            else 0.0
        )
        for unit in self.plan.units:
            if unit.unit_id in completed_ids:
                continue
            kind_samples = samples.get(unit.kind, [])
            if kind_samples:
                estimate = median(kind_samples[-7:])
                measured_kinds.add(unit.kind)
            else:
                estimate = unit.bootstrap_seconds
            if unit.unit_id == current_unit_id:
                # Keep the live ETA numeric and moving. When a unit overruns its
                # prior, expand the estimate instead of pinning the ETA at zero.
                estimate = max(estimate, current_elapsed * 1.25)
                estimate = max(1.0, estimate - current_elapsed)
            remaining_seconds += estimate
        if not completed_ids:
            eta_source = "historical_bootstrap"
        elif measured_kinds:
            eta_source = "measured_hybrid"
        else:
            eta_source = "historical_bootstrap"
        if len(completed_ids) == len(self.plan.units):
            remaining_seconds = 0.0
            eta_source = "complete"
        return {
            "completed_weight": completed_weight,
            "total_weight": total_weight,
            "overall_percent": overall_percent,
            "completed_units": len(completed_ids),
            "total_units": len(self.plan.units),
            "current_unit_id": current_unit_id,
            "eta_seconds": remaining_seconds,
            "eta_source": eta_source,
            "eta_sample_count": sum(len(values) for values in samples.values()),
            "last_completed_at_epoch": self.last_completed_at_epoch,
            "elapsed_seconds": (
                self.prior_elapsed_seconds + time.perf_counter() - self.started_monotonic
            ),
        }


class ProgressPublisher:
    """One manifest-bound current snapshot with a monotonic producer sequence."""

    def __init__(self, root: Path, plan: WorkPlan) -> None:
        self.root = root.resolve()
        self.plan = plan
        if self.root.is_symlink():
            raise TailExitRuntimeError("progress root must not be a symlink")
        self.root.mkdir(parents=True, exist_ok=True)
        self.sequence = 0
        manifest = self.root / "manifest.json"
        current = self.root / "current.json"
        if manifest.exists() or current.exists():
            if not manifest.is_file() or not current.is_file():
                raise TailExitRuntimeError("existing progress root is incomplete")
            raw_manifest = json.loads(manifest.read_text(encoding="utf-8"))
            raw_current = json.loads(current.read_text(encoding="utf-8"))
            if raw_current.get("plan_sha256") != plan.sha256:
                raise TailExitRuntimeError("progress root plan identity differs")
            files = raw_manifest.get("files")
            record = files.get("current.json") if isinstance(files, dict) else None
            if not isinstance(record, dict):
                raise TailExitRuntimeError("progress manifest file record is missing")
            body = current.read_bytes()
            if record.get("byte_count") != len(body) or record.get("sha256") != sha256_bytes(body):
                raise TailExitRuntimeError("existing progress snapshot verification failed")
            self.sequence = int(raw_manifest.get("sequence", 0))

    def publish(self, payload: dict[str, object]) -> dict[str, object]:
        self.sequence += 1
        current_payload = {
            **payload,
            "schema_version": RUNTIME_VERSION,
            "plan_sha256": self.plan.sha256,
            "run_id": self.plan.run_id,
            "sequence": self.sequence,
            "heartbeat_at_epoch": time.time(),
        }
        body = canonical_json(current_payload)
        current = self.root / "current.json"
        _atomic_replace(current, body, f"current-{self.sequence}")
        manifest_payload = {
            "kind": "pumpquant.fresh_tail_exit_progress_manifest",
            "version": RUNTIME_VERSION,
            "status": "complete",
            "plan_sha256": self.plan.sha256,
            "run_id": self.plan.run_id,
            "sequence": self.sequence,
            "files": {
                "current.json": {
                    "byte_count": len(body),
                    "sha256": sha256_bytes(body),
                }
            },
        }
        _atomic_replace(
            self.root / "manifest.json",
            canonical_json(manifest_payload),
            f"manifest-{self.sequence}",
        )
        return current_payload


def process_rss_bytes() -> int | None:
    """Return Linux process RSS without adding a platform dependency."""

    status = Path("/proc/self/status")
    if not status.is_file():
        return None
    for line in status.read_text(encoding="utf-8").splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                return int(parts[1]) * 1024
    return None


__all__ = [
    "CHECKPOINT_MANIFEST",
    "CHECKPOINT_MARKER",
    "CheckpointStore",
    "ProgressEstimator",
    "ProgressPublisher",
    "TailExitRuntimeError",
    "WorkPlan",
    "WorkUnit",
    "canonical_json",
    "file_record",
    "process_rss_bytes",
    "sha256_file",
    "verify_file_record",
]
