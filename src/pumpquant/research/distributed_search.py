"""Deterministic distributed candidate search helpers.

The regular search implementation in :mod:`pumpquant.research.search`
owns the walk-forward split and all execution semantics.  This module only
adds the orchestration boundary around it:

* candidates are assigned by their zero-based stream ordinal (``ordinal %
  worker_count``), so a worker never has to materialise a Cartesian product;
* every worker writes its own fsync-safe JSONL ledger and a small manifest;
* ledgers can be merged in a stable order, with conflicting duplicate trial
  identities rejected before anything is published; and
* an optional local process launcher keeps the number of concurrent workers
  bounded.

Partitioning is order based.  Consequently all hosts must use
the same candidate-stream definition, seed, and ordering.  Walk-forward bars
are *not* partitioned: every worker receives the complete bar input so that
each worker's training and test cohorts remain exactly the same chronology.
This is a deterministic orchestration primitive, not a claim that exhaustive
evaluation of an impractically large candidate space is feasible.
"""

from __future__ import annotations

import json
import multiprocessing
import os
from collections.abc import Iterable, Iterator, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from pumpquant.core.hashing import sha256_file
from pumpquant.core.schema import Bar
from pumpquant.execution.bar_replay import ExecutionConfig, MomentumSpec
from pumpquant.research.search import (
    ExperimentLedger,
    SearchReport,
    momentum_spec_sha256,
    walk_forward_search,
)
from pumpquant.validation.metrics import (
    deflated_sharpe_probability,
    performance,
    probabilistic_sharpe_ratio,
)


class LedgerMergeError(ValueError):
    """Raised when worker ledgers cannot be merged without ambiguity."""


@dataclass(frozen=True, slots=True)
class CandidatePartition(Iterable[MomentumSpec]):
    """A lazy, deterministic view of one worker's candidate positions.

    The source is retained rather than converted to a tuple.  Each iteration
    starts at the beginning of the source and emits only ordinals belonging to
    ``worker_index``.  This makes the wrapper re-iterable whenever the source
    is re-iterable, which is required by multi-fold and racing searches.

    ``deduplicated`` mirrors the source stream's declaration.  Built-in
    ``MomentumGridStream`` and ``MomentumLowDiscrepancyStream`` are already
    content-deduplicated; ordinary iterables retain the historical
    ``walk_forward_search`` behaviour of hash-deduplicating candidates.
    """

    source: Iterable[MomentumSpec]
    worker_count: int
    worker_index: int
    deduplicated: bool = False

    def __post_init__(self) -> None:
        _validate_worker_coordinates(self.worker_count, self.worker_index)

    def __iter__(self) -> Iterator[MomentumSpec]:
        # ``enumerate`` is the only state needed.  In particular, do not use
        # ``list(source)`` or ``itertools.tee``: both can allocate the whole
        # candidate stream.
        seen: set[str] = set()
        effective_ordinal = 0
        for candidate in self.source:
            if not self.deduplicated and isinstance(candidate, MomentumSpec):
                digest = momentum_spec_sha256(candidate)
                if digest in seen:
                    continue
                seen.add(digest)
            if effective_ordinal % self.worker_count == self.worker_index:
                yield candidate
            effective_ordinal += 1

    def __len__(self) -> int:
        """Return the partition size when the source exposes ``len``.

        Calling ``len`` never consumes an iterator.  A source with no finite
        length simply makes ``len(partition)`` raise ``TypeError`` as normal.
        """

        if not self.deduplicated and not isinstance(self.source, Sequence):
            raise TypeError("candidate partition length is unknown for a lazy deduplicated source")
        if self.deduplicated:
            total = len(self.source)  # type: ignore[arg-type]
        else:
            # A concrete sequence can be scanned without materialising a
            # second candidate container.  Lazy generators do
            # not claim a length after content deduplication.
            unique = 0
            seen: set[str] = set()
            for candidate in self.source:
                if isinstance(candidate, MomentumSpec):
                    digest = momentum_spec_sha256(candidate)
                    if digest in seen:
                        continue
                    seen.add(digest)
                unique += 1
            total = unique
        if total < 0:
            raise ValueError("candidate stream length cannot be negative")
        if self.worker_index >= total:
            return 0
        return (total - self.worker_index + self.worker_count - 1) // self.worker_count

    @property
    def total_count_hint(self) -> int | None:
        """The source length, if available without consuming the stream."""

        try:
            return len(self)
        except TypeError:
            return None


def _validate_worker_coordinates(worker_count: int, worker_index: int) -> None:
    if isinstance(worker_count, bool) or not isinstance(worker_count, int):
        raise ValueError("worker_count must be a positive integer")
    if worker_count < 1:
        raise ValueError("worker_count must be a positive integer")
    if isinstance(worker_index, bool) or not isinstance(worker_index, int):
        raise ValueError("worker_index must be an integer in [0, worker_count)")
    if worker_index < 0 or worker_index >= worker_count:
        raise ValueError("worker_index must be an integer in [0, worker_count)")


def partition_candidates(
    candidates: Iterable[MomentumSpec],
    worker_count: int,
    worker_index: int,
) -> CandidatePartition:
    """Assign candidate stream positions deterministically to one worker.

    Position ``i`` is assigned to ``i % worker_count``.  The operation is
    lazy and preserves the source order within each partition.  The union of
    all ``worker_count`` partitions is exactly the source's effective
    candidate sequence.  Ordinary ``MomentumSpec`` iterables retain the
    search layer's canonical-hash deduplication semantics; streams declaring
    ``deduplicated=True`` are never deduplicated again.
    """

    _validate_worker_coordinates(worker_count, worker_index)
    return CandidatePartition(
        source=candidates,
        worker_count=worker_count,
        worker_index=worker_index,
        deduplicated=bool(getattr(candidates, "deduplicated", False)),
    )


def partition_sizes(total_count: int, worker_count: int) -> tuple[int, ...]:
    """Return deterministic modulo-partition sizes for a known stream length."""

    if isinstance(total_count, bool) or not isinstance(total_count, int) or total_count < 0:
        raise ValueError("total_count must be a non-negative integer")
    if isinstance(worker_count, bool) or not isinstance(worker_count, int) or worker_count < 1:
        raise ValueError("worker_count must be a positive integer")
    return tuple(
        (total_count - index + worker_count - 1) // worker_count if index < total_count else 0
        for index in range(worker_count)
    )


def default_worker_ledger_path(ledger_dir: str | os.PathLike[str], worker_index: int) -> Path:
    """Return the conventional private JSONL path for one worker."""

    if isinstance(worker_index, bool) or not isinstance(worker_index, int) or worker_index < 0:
        raise ValueError("worker_index must be a non-negative integer")
    return Path(ledger_dir) / f"worker-{worker_index:05d}.jsonl"


def _ledger_json(value: Any) -> bytes:
    """Ledger rows keep the format they were first written in: sorted, compact, raw UTF-8."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _atomic_json_write(path: Path, payload: Mapping[str, Any]) -> None:
    """Publish one manifest atomically and durably."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="wb", prefix=f".{path.name}.", suffix=".partial", dir=path.parent, delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(_ledger_json(payload) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
        # fsync the containing directory so a power loss cannot lose the
        # rename.  Some filesystems/platforms reject directory fsync; the
        # data file is still durable in that case.
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _report_summary(report: SearchReport) -> dict[str, Any]:
    """Serialise bounded report metadata for a worker manifest."""

    return {
        "candidate_count": report.candidate_count,
        "trial_count": report.trial_count,
        "evaluation_count": report.evaluation_count,
        "failed_trial_count": report.failed_trial_count,
        "pruned_trial_count": report.pruned_trial_count,
        "fold_count": len(report.folds),
        "ledger_rows_appended": report.ledger_rows_appended,
    }


def _empty_search_report() -> SearchReport:
    """Construct the report for an empty modulo partition."""

    returns: list[float] = []
    return SearchReport(
        candidate_count=0,
        trial_count=0,
        folds=(),
        outcome_diagnostics=performance(returns),
        diagnostic_probabilistic_sharpe=probabilistic_sharpe_ratio(returns),
        diagnostic_deflated_sharpe=deflated_sharpe_probability(returns, returns),
    )


@dataclass(frozen=True, slots=True)
class DistributedWorkerResult:
    """Result and provenance for one partitioned search worker."""

    worker_count: int
    worker_index: int
    ledger_path: Path
    manifest_path: Path
    report: SearchReport
    candidate_count_hint: int | None
    partition_candidate_count: int


def run_worker_search(
    bars: Iterable[Bar],
    candidates: Iterable[MomentumSpec],
    config: ExecutionConfig,
    *,
    worker_count: int,
    worker_index: int,
    ledger_path: str | os.PathLike[str] | None = None,
    ledger_dir: str | os.PathLike[str] | None = None,
    ledger: str | os.PathLike[str] | None = None,
    manifest_path: str | os.PathLike[str] | None = None,
    experiment_id: str = "search",
    data_sha256: str | None = None,
    resume: bool = False,
    **search_kwargs: Any,
) -> DistributedWorkerResult:
    """Run one deterministic candidate partition and seal its worker ledger.

    ``bars`` is passed intact to :func:`walk_forward_search`;
    only candidates are partitioned.  ``ledger_path`` takes precedence over
    ``ledger_dir``.  A manifest is written next to the ledger unless an
    path is supplied.  The manifest is advisory provenance; trial
    rows themselves remain the authoritative durable record.
    """

    _validate_worker_coordinates(worker_count, worker_index)
    if ledger is not None:
        if ledger_path is not None or ledger_dir is not None:
            raise ValueError("provide only one of ledger, ledger_path, or ledger_dir")
        ledger_path = ledger
    if ledger_path is not None and ledger_dir is not None:
        raise ValueError("provide ledger_path or ledger_dir, not both")
    if ledger_path is None:
        if ledger_dir is None:
            raise ValueError("ledger_path or ledger_dir is required")
        ledger = default_worker_ledger_path(ledger_dir, worker_index)
    else:
        ledger = Path(ledger_path)
    if manifest_path is None:
        manifest = ledger.with_suffix(ledger.suffix + ".manifest.json")
    else:
        manifest = Path(manifest_path)

    partition = partition_candidates(candidates, worker_count, worker_index)
    total_count_hint = partition.total_count_hint
    try:
        local_count_hint = len(partition)
    except TypeError:
        local_count_hint = None

    # The search layer rejects an empty candidate stream.  Empty modulo
    # partitions are legitimate when worker_count exceeds the stream length;
    # report this instead of silently fabricating trial rows.  A
    # caller can still merge the non-empty workers.
    if local_count_hint == 0:
        report = _empty_search_report()
        ledger.parent.mkdir(parents=True, exist_ok=True)
        with ledger.open("a", encoding="utf-8") as handle:
            handle.flush()
            os.fsync(handle.fileno())
    else:
        report = walk_forward_search(
            bars,
            partition,
            config,
            ledger=ExperimentLedger(ledger),
            experiment_id=experiment_id,
            data_sha256=data_sha256,
            resume=resume,
            **search_kwargs,
        )
    manifest_payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "pumpquant.distributed_search_worker",
        "experiment_id": experiment_id,
        "worker_count": worker_count,
        "worker_index": worker_index,
        "ledger_path": str(ledger),
        "ledger_sha256": sha256_file(ledger),
        "ledger_rows": len(ExperimentLedger(ledger).read_records()),
        "candidate_count_hint": total_count_hint,
        "partition_candidate_count": report.candidate_count,
        "data_sha256": data_sha256,
        "execution_config": asdict(config),
        "report": _report_summary(report),
    }
    _atomic_json_write(manifest, manifest_payload)
    return DistributedWorkerResult(
        worker_count=worker_count,
        worker_index=worker_index,
        ledger_path=ledger,
        manifest_path=manifest,
        report=report,
        candidate_count_hint=total_count_hint,
        partition_candidate_count=report.candidate_count,
    )


def _load_ledger_rows(path: Path) -> tuple[dict[str, Any], ...]:
    try:
        rows = ExperimentLedger(path).read_records()
    except (OSError, ValueError) as error:
        raise LedgerMergeError(f"cannot read worker ledger {path}: {error}") from error
    return rows


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LedgerMergeError(f"cannot read worker manifest {path}: {error}") from error
    if not isinstance(payload, dict):
        raise LedgerMergeError(f"worker manifest {path} is not an object")
    return payload


def _trial_identity(row: Mapping[str, Any], source: Path) -> tuple[str, ...]:
    """Extract a stable identity, rejecting rows too ambiguous to merge."""

    trial_id = row.get("trial_id")
    if isinstance(trial_id, str) and trial_id:
        # The trial_id is the strongest identity contract.  It also
        # lets merge validate generic ledgers that do not expose the expanded
        # experiment/fold/spec fields.
        return ("trial_id", trial_id)
    experiment = row.get("experiment_id")
    fold = row.get("fold")
    digest = row.get("candidate_sha256")
    if not isinstance(digest, str):
        digest = row.get("candidate_id")
    if (
        isinstance(experiment, str)
        and isinstance(fold, int)
        and not isinstance(fold, bool)
        and isinstance(digest, str)
    ):
        return (experiment, str(fold), digest)
    raise LedgerMergeError(
        f"worker ledger {source} contains a terminal row without a trial identity"
    )


@dataclass(frozen=True, slots=True)
class LedgerMergeResult:
    """Summary returned after a deterministic ledger merge."""

    output_path: Path
    input_paths: tuple[Path, ...]
    row_count: int
    duplicate_row_count: int
    experiment_ids: tuple[str, ...]

    @property
    def trial_count(self) -> int:
        """Alias used by search reports for the merged terminal-row count."""

        return self.row_count


def merge_worker_ledgers(
    ledgers: Iterable[str | os.PathLike[str]],
    output: str | os.PathLike[str],
    *,
    expected_worker_count: int | None = None,
    experiment_id: str | None = None,
    require_manifests: bool = False,
) -> LedgerMergeResult:
    """Merge worker JSONL ledgers in a deterministic, conflict-checked order.

    Rows are keyed by ``(experiment_id, fold, candidate_sha256)`` (with a
    ``trial_id`` fallback for generic ledgers).  Exact duplicate rows are
    coalesced; any duplicate key with a different canonical payload raises
    :class:`LedgerMergeError`.  The output is written through a temporary
    file, fsynced, then atomically replaced, so a conflict never leaves a
    partial merged ledger.
    """

    paths = tuple(Path(path) for path in ledgers)
    if not paths:
        raise ValueError("at least one worker ledger is required")
    if len(set(paths)) != len(paths):
        raise ValueError("worker ledger paths must be unique")
    if expected_worker_count is not None:
        if (
            isinstance(expected_worker_count, bool)
            or not isinstance(expected_worker_count, int)
            or expected_worker_count < 1
        ):
            raise ValueError("expected_worker_count must be a positive integer")
        if len(paths) != expected_worker_count:
            raise LedgerMergeError(
                f"expected {expected_worker_count} worker ledgers, received {len(paths)}"
            )

    rows_by_identity: dict[tuple[str, ...], tuple[bytes, dict[str, Any]]] = {}
    duplicate_count = 0
    experiments: set[str] = set()
    manifest_workers: dict[int, Path] = {}
    manifest_counts: set[int] = set()
    for path in paths:
        manifest = path.with_suffix(path.suffix + ".manifest.json")
        if require_manifests and not manifest.exists():
            raise LedgerMergeError(f"worker manifest is missing for {path}")
        if manifest.exists():
            payload = _load_manifest(manifest)
            if payload.get("kind") != "pumpquant.distributed_search_worker":
                raise LedgerMergeError(f"worker manifest {manifest} has an unknown kind")
            manifest_count = payload.get("worker_count")
            manifest_index = payload.get("worker_index")
            if not isinstance(manifest_count, int) or isinstance(manifest_count, bool):
                raise LedgerMergeError(f"worker manifest {manifest} has invalid worker_count")
            if not isinstance(manifest_index, int) or isinstance(manifest_index, bool):
                raise LedgerMergeError(f"worker manifest {manifest} has invalid worker_index")
            if manifest_index < 0 or manifest_index >= manifest_count:
                raise LedgerMergeError(f"worker manifest {manifest} has out-of-range worker_index")
            if expected_worker_count is not None and manifest_count != expected_worker_count:
                raise LedgerMergeError(f"worker manifest {manifest} disagrees on worker_count")
            manifest_counts.add(manifest_count)
            if manifest_index in manifest_workers:
                raise LedgerMergeError(
                    f"worker index {manifest_index} is repeated by "
                    f"{manifest_workers[manifest_index]}"
                )
            manifest_workers[manifest_index] = manifest
            expected_hash = payload.get("ledger_sha256")
            if isinstance(expected_hash, str) and expected_hash != sha256_file(path):
                raise LedgerMergeError(f"worker ledger hash differs from manifest {manifest}")
        for row in _load_ledger_rows(path):
            if experiment_id is not None and row.get("experiment_id") != experiment_id:
                raise LedgerMergeError(f"worker ledger {path} has a different experiment_id")
            identity = _trial_identity(row, path)
            try:
                canonical = _ledger_json(row)
            except (TypeError, ValueError) as error:
                raise LedgerMergeError(
                    f"worker ledger {path} contains a non-canonical JSON value"
                ) from error
            existing = rows_by_identity.get(identity)
            if existing is None:
                rows_by_identity[identity] = (canonical, row)
            elif existing[0] != canonical:
                raise LedgerMergeError("conflicting duplicate trial identity " + repr(identity))
            else:
                duplicate_count += 1
            if isinstance(row.get("experiment_id"), str):
                experiments.add(str(row["experiment_id"]))

    if expected_worker_count is not None and manifest_workers:
        expected_indexes = set(range(expected_worker_count))
        if set(manifest_workers) != expected_indexes:
            missing = sorted(expected_indexes - set(manifest_workers))
            extra = sorted(set(manifest_workers) - expected_indexes)
            raise LedgerMergeError(
                f"worker manifests are incomplete (missing={missing}, extra={extra})"
            )
    if len(manifest_counts) > 1:
        raise LedgerMergeError(
            f"worker manifests disagree on worker_count: {sorted(manifest_counts)}"
        )
    if experiment_id is None and len(experiments) > 1:
        raise LedgerMergeError(
            f"worker ledgers contain multiple experiment_id values: {sorted(experiments)}"
        )

    ordered = [rows_by_identity[key][1] for key in sorted(rows_by_identity)]
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="wb", prefix=f".{target.name}.", suffix=".partial", dir=target.parent, delete=False
        ) as handle:
            temporary = Path(handle.name)
            for row in ordered:
                handle.write(_ledger_json(row) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        temporary = None
        try:
            directory_fd = os.open(target.parent, os.O_RDONLY)
        except OSError:
            pass
        else:
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)

    return LedgerMergeResult(
        output_path=target,
        input_paths=paths,
        row_count=len(ordered),
        duplicate_row_count=duplicate_count,
        experiment_ids=tuple(sorted(experiments)),
    )


_FORK_SHARED_BARS: tuple[Bar, ...] | None = None
_FORK_SHARED_CANDIDATES: Iterable[MomentumSpec] | None = None
_FORK_SHARED_CONFIG: ExecutionConfig | None = None


def _worker_process_entry(payload: Mapping[str, Any]) -> DistributedWorkerResult:
    """Top-level process entry point (spawn/pickling friendly)."""

    return run_worker_search(
        payload["bars"],
        payload["candidates"],
        payload["config"],
        worker_count=int(payload["worker_count"]),
        worker_index=int(payload["worker_index"]),
        ledger_path=payload["ledger_path"],
        experiment_id=str(payload["experiment_id"]),
        data_sha256=payload.get("data_sha256"),
        **dict(payload["search_kwargs"]),
    )


def _fork_shared_worker_process_entry(
    payload: Mapping[str, Any],
) -> DistributedWorkerResult:
    """Run from copy-on-write inputs inherited by a Linux fork worker."""

    if _FORK_SHARED_BARS is None or _FORK_SHARED_CANDIDATES is None or _FORK_SHARED_CONFIG is None:
        raise RuntimeError("fork-shared local worker inputs are unavailable")
    return run_worker_search(
        _FORK_SHARED_BARS,
        _FORK_SHARED_CANDIDATES,
        _FORK_SHARED_CONFIG,
        worker_count=int(payload["worker_count"]),
        worker_index=int(payload["worker_index"]),
        ledger_path=payload["ledger_path"],
        experiment_id=str(payload["experiment_id"]),
        data_sha256=payload.get("data_sha256"),
        **dict(payload["search_kwargs"]),
    )


def launch_local_workers(
    bars: Iterable[Bar],
    candidates: Iterable[MomentumSpec],
    config: ExecutionConfig,
    *,
    worker_count: int,
    ledger_dir: str | os.PathLike[str],
    max_workers: int | None = None,
    experiment_id: str = "search",
    data_sha256: str | None = None,
    resume: bool = False,
    fork_share_bars: bool = False,
    worker_indexes: Sequence[int] | None = None,
    **search_kwargs: Any,
) -> tuple[DistributedWorkerResult, ...]:
    """Launch a bounded set of local worker processes.

    This helper is opt-in and conservative.  Bars are
    materialised once so they can be pickled by the ``spawn`` start method.
    On POSIX, ``fork_share_bars=True`` instead creates workers from that
    materialised parent image.  The bar objects are then shared by
    copy-on-write and are never included in a per-worker task payload.
    The candidate stream is passed as-is and remains lazy inside each child;
    built-in momentum streams are picklable, while an arbitrary one-shot
    generator fails to pickle rather than being silently materialised.
    ``worker_indexes`` can select a disjoint subset of global partitions for
    multi-host launches; it defaults to every index.  ``max_workers`` defaults
    to the smaller of the selected partition count and local CPU count.  Hosts
    with very large bar archives should run
    :func:`run_worker_search` directly and provide their own shared input
    loading rather than copying bars into every local process.
    """

    _validate_worker_coordinates(worker_count, 0)
    if worker_indexes is None:
        selected_indexes = tuple(range(worker_count))
    else:
        selected_indexes = tuple(worker_indexes)
        if not selected_indexes:
            raise ValueError("worker_indexes cannot be empty")
        for index in selected_indexes:
            _validate_worker_coordinates(worker_count, index)
        if len(set(selected_indexes)) != len(selected_indexes):
            raise ValueError("worker_indexes must be unique")
    if max_workers is None:
        max_workers = min(len(selected_indexes), os.cpu_count() or 1)
    if isinstance(max_workers, bool) or not isinstance(max_workers, int) or max_workers < 1:
        raise ValueError("max_workers must be a positive integer")
    if not isinstance(fork_share_bars, bool):
        raise ValueError("fork_share_bars must be a boolean")
    max_workers = min(max_workers, len(selected_indexes))
    if len(selected_indexes) > 1:
        first_iterator = iter(candidates)
        if iter(candidates) is first_iterator:
            raise ValueError("candidate stream must be re-iterable for multiple local workers")
    bar_values = tuple(bars)
    candidate_values = candidates
    ledger_root = Path(ledger_dir)
    ledger_root.mkdir(parents=True, exist_ok=True)
    common_payloads: list[dict[str, Any]] = [
        {
            "worker_count": worker_count,
            "worker_index": index,
            "ledger_path": str(default_worker_ledger_path(ledger_root, index)),
            "experiment_id": experiment_id,
            "data_sha256": data_sha256,
            "search_kwargs": {**search_kwargs, "resume": resume},
        }
        for index in selected_indexes
    ]
    use_fork_sharing = fork_share_bars and "fork" in multiprocessing.get_all_start_methods()
    if use_fork_sharing:
        global _FORK_SHARED_BARS, _FORK_SHARED_CANDIDATES, _FORK_SHARED_CONFIG
        _FORK_SHARED_BARS = bar_values
        _FORK_SHARED_CANDIDATES = candidate_values
        _FORK_SHARED_CONFIG = config
        payloads = common_payloads
        worker_entry = _fork_shared_worker_process_entry
        process_context = multiprocessing.get_context("fork")
    else:
        payloads = [
            {
                **payload,
                "bars": bar_values,
                "candidates": candidate_values,
                "config": config,
            }
            for payload in common_payloads
        ]
        worker_entry = _worker_process_entry
        process_context = None
    try:
        try:
            pool = ProcessPoolExecutor(
                max_workers=max_workers,
                mp_context=process_context,
            )
        except (NotImplementedError, PermissionError):
            # Restricted containers can lack POSIX semaphores.  Preserve the
            # bounded-launcher contract with a deterministic sequential fallback;
            # ordinary worker exceptions are not swallowed.
            results = [worker_entry(payload) for payload in payloads]
        else:
            with pool:
                results = list(pool.map(worker_entry, payloads))
    finally:
        if use_fork_sharing:
            _FORK_SHARED_BARS = None
            _FORK_SHARED_CANDIDATES = None
            _FORK_SHARED_CONFIG = None
    return tuple(sorted(results, key=lambda item: item.worker_index))


__all__ = [
    "CandidatePartition",
    "DistributedWorkerResult",
    "LedgerMergeError",
    "LedgerMergeResult",
    "default_worker_ledger_path",
    "launch_local_workers",
    "merge_worker_ledgers",
    "partition_candidates",
    "partition_sizes",
    "run_worker_search",
]
