"""Leakage-safe global reduction of partitioned training-search ledgers."""

from __future__ import annotations

import json
import math
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from pumpquant.core.hashing import sha256_file
from pumpquant.core.schema import Bar
from pumpquant.execution.bar_replay import ExecutionConfig, MomentumSpec, run_backtest
from pumpquant.research.distributed_search import LedgerMergeError, merge_worker_ledgers
from pumpquant.research.search import (
    FOLD_MINT_HASH_SCHEME,
    ExperimentLedger,
    _cohorts,
    mint_sequence_sha256,
)
from pumpquant.validation.metrics import (
    deflated_sharpe_probability,
    performance,
    probabilistic_sharpe_ratio,
)
from pumpquant.validation.splits import embargoed_walk_forward


class DistributedReductionError(ValueError):
    """Raised when worker ledgers cannot produce one exact global replay."""


@dataclass(frozen=True, slots=True)
class DistributedReductionResult:
    output_path: Path
    output_sha256: str
    merged_ledger_path: Path
    merged_ledger_sha256: str
    fold_count: int
    candidate_count: int
    trial_count: int


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix=f".{path.name}.",
            suffix=".partial",
            dir=path.parent,
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            json.dump(
                _json_safe(payload),
                handle,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _number(value: Any, *, default: float = -math.inf) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return default
    result = float(value)
    return result if math.isfinite(result) else default


def _training_key(record: Mapping[str, Any]) -> tuple[float, float, float, float, str]:
    evaluation = record.get("evaluation")
    if not isinstance(evaluation, Mapping) or evaluation.get("eligible") is not True:
        return (-math.inf, -math.inf, -math.inf, -math.inf, "")
    performance_payload = evaluation.get("performance")
    if not isinstance(performance_payload, Mapping):
        performance_payload = {}
    digest = record.get("candidate_sha256")
    if not isinstance(digest, str):
        raise DistributedReductionError("candidate_sha256 is missing from a trial row")
    return (
        _number(evaluation.get("return_on_attempted_capital")),
        _number(evaluation.get("total_quote_pnl")),
        _number(performance_payload.get("mean_return")),
        -_number(evaluation.get("rejected_orders"), default=math.inf),
        digest,
    )


def _trial_sharpe(record: Mapping[str, Any]) -> float:
    evaluation = record.get("evaluation")
    if not isinstance(evaluation, Mapping):
        return 0.0
    stats = evaluation.get("performance")
    if not isinstance(stats, Mapping):
        return 0.0
    return _number(stats.get("sharpe"), default=0.0)


def _execution_config(record: Mapping[str, Any]) -> ExecutionConfig:
    payload = record.get("execution_config")
    if not isinstance(payload, Mapping):
        raise DistributedReductionError("execution_config is missing from a trial row")
    try:
        return ExecutionConfig(**dict(payload))
    except (TypeError, ValueError) as error:
        raise DistributedReductionError(f"invalid execution_config: {error}") from error


def reduce_distributed_ledgers(
    bars: Iterable[Bar],
    worker_ledgers: Sequence[str | os.PathLike[str]],
    *,
    output_path: str | os.PathLike[str],
    merged_ledger_path: str | os.PathLike[str],
    expected_worker_count: int,
    expected_candidate_count: int,
    experiment_id: str,
    data_sha256: str,
    min_train_tokens: int,
    test_tokens: int,
    embargo_ns: int = 0,
    require_manifests: bool = True,
) -> DistributedReductionResult:
    """Select globally on training rows, then replay each test fold exactly once."""

    if expected_candidate_count < 1:
        raise ValueError("expected_candidate_count must be positive")
    merged_path = Path(merged_ledger_path)
    try:
        merged = merge_worker_ledgers(
            worker_ledgers,
            merged_path,
            expected_worker_count=expected_worker_count,
            experiment_id=experiment_id,
            require_manifests=require_manifests,
        )
    except LedgerMergeError as error:
        raise DistributedReductionError(str(error)) from error

    records = ExperimentLedger(merged_path).read_records()
    if not records:
        raise DistributedReductionError("merged ledger is empty")
    materialized = bars if isinstance(bars, (list, tuple)) else list(bars)
    quote_mints = {bar.quote_mint for bar in materialized}
    if len(quote_mints) != 1:
        raise DistributedReductionError("global replay requires one quote currency")
    folds = embargoed_walk_forward(
        _cohorts(materialized),
        min_train_size=min_train_tokens,
        test_size=test_tokens,
        embargo_ns=embargo_ns,
    )
    if not folds:
        raise DistributedReductionError("bars do not contain one complete fold")

    records_by_fold: dict[int, dict[str, dict[str, Any]]] = {}
    execution_config: ExecutionConfig | None = None
    trial_sharpes: list[float] = []
    local_test_replays_ignored = 0
    for record in records:
        if record.get("ledger_schema_version") != 3:
            raise DistributedReductionError("exact reduction requires ledger schema version 3")
        if record.get("experiment_id") != experiment_id:
            raise DistributedReductionError("trial row has a different experiment_id")
        if record.get("data_sha256") != data_sha256:
            raise DistributedReductionError("trial row has a different data_sha256")
        status = record.get("status")
        if status not in {"completed", "failed"}:
            raise DistributedReductionError(
                "exact reduction rejects pruned or non-terminal candidate rows"
            )
        fold_number = record.get("fold")
        digest = record.get("candidate_sha256")
        if not isinstance(fold_number, int) or isinstance(fold_number, bool):
            raise DistributedReductionError("trial row has an invalid fold number")
        if not isinstance(digest, str):
            raise DistributedReductionError("trial row has no candidate_sha256")
        fold_records = records_by_fold.setdefault(fold_number, {})
        if digest in fold_records:
            raise DistributedReductionError("candidate is duplicated within a fold")
        fold_records[digest] = record
        candidate_config = _execution_config(record)
        if execution_config is None:
            execution_config = candidate_config
        elif candidate_config != execution_config:
            raise DistributedReductionError("execution_config differs between trial rows")
        trial_sharpes.append(_trial_sharpe(record))
        if record.get("test_backtest") is not None:
            local_test_replays_ignored += 1

    if execution_config is None:
        raise DistributedReductionError("no execution_config was recovered")
    if set(records_by_fold) != {fold.number for fold in folds}:
        raise DistributedReductionError("ledger folds differ from reconstructed bar folds")

    bars_by_mint: dict[str, list[Bar]] = {}
    for bar in materialized:
        bars_by_mint.setdefault(bar.mint, []).append(bar)
    reference_candidates: set[str] | None = None
    fold_payloads: list[dict[str, Any]] = []
    oos_returns: list[float] = []
    failed_trial_count = 0
    for fold in folds:
        fold_records = records_by_fold[fold.number]
        candidate_digests = set(fold_records)
        if len(candidate_digests) != expected_candidate_count:
            raise DistributedReductionError(
                f"fold {fold.number} has {len(candidate_digests)} candidates; "
                f"expected {expected_candidate_count}"
            )
        if reference_candidates is None:
            reference_candidates = candidate_digests
        elif candidate_digests != reference_candidates:
            raise DistributedReductionError("candidate set changed between folds")
        expected_train_hash = mint_sequence_sha256(cohort.mint for cohort in fold.train)
        expected_test_hash = mint_sequence_sha256(cohort.mint for cohort in fold.test)
        for record in fold_records.values():
            if (
                record.get("fold_mint_hash_scheme") != FOLD_MINT_HASH_SCHEME
                or record.get("train_mint_count") != len(fold.train)
                or record.get("test_mint_count") != len(fold.test)
                or record.get("train_mints_sha256") != expected_train_hash
                or record.get("test_mints_sha256") != expected_test_hash
                or record.get("test_start_ns") != fold.test_start_ns
                or record.get("train_cutoff_ns") != fold.test_start_ns - embargo_ns
            ):
                raise DistributedReductionError(
                    f"fold identity mismatch in trial {record.get('trial_id')}"
                )
        failed_trial_count += sum(
            record.get("status") == "failed" for record in fold_records.values()
        )
        eligible = [
            record for record in fold_records.values() if _training_key(record)[0] != -math.inf
        ]
        selected = max(eligible, key=_training_key) if eligible else None
        if selected is None:
            test_backtest = None
        else:
            spec_payload = selected.get("spec")
            if not isinstance(spec_payload, Mapping):
                raise DistributedReductionError("selected row has no strategy spec")
            try:
                spec = MomentumSpec(**dict(spec_payload))
            except (TypeError, ValueError) as error:
                raise DistributedReductionError(
                    f"invalid selected strategy spec: {error}"
                ) from error
            test_bars = [bar for cohort in fold.test for bar in bars_by_mint.get(cohort.mint, ())]
            test_backtest = run_backtest(test_bars, spec, execution_config)
            oos_returns.extend(test_backtest.returns)
        fold_payloads.append(
            {
                "number": fold.number,
                "train_mint_count": len(fold.train),
                "train_mints_sha256": expected_train_hash,
                "test_mint_count": len(fold.test),
                "test_mints_sha256": expected_test_hash,
                "test_start_ns": fold.test_start_ns,
                "selected_candidate_sha256": (
                    None if selected is None else selected["candidate_sha256"]
                ),
                "selected_spec": None if selected is None else selected["spec"],
                "training_evaluation": (None if selected is None else selected["evaluation"]),
                "test_backtest": (None if test_backtest is None else asdict(test_backtest)),
            }
        )

    output_diagnostics = performance(oos_returns)
    payload = {
        "schema_version": 1,
        "kind": "pumpquant.exact_distributed_search_reduction",
        "experiment_id": experiment_id,
        "data_sha256": data_sha256,
        "quote_mint": next(iter(quote_mints)),
        "selection_basis": "training-only-global-deterministic-v1",
        "fold_mint_hash_scheme": FOLD_MINT_HASH_SCHEME,
        "worker_count": expected_worker_count,
        "candidate_count": expected_candidate_count,
        "fold_count": len(folds),
        "trial_count": merged.row_count,
        "failed_trial_count": failed_trial_count,
        "local_test_replays_ignored": local_test_replays_ignored,
        "merged_ledger_path": str(merged_path.resolve()),
        "merged_ledger_sha256": sha256_file(merged_path),
        "execution_config": asdict(execution_config),
        "min_train_tokens": min_train_tokens,
        "test_tokens": test_tokens,
        "embargo_ns": embargo_ns,
        "folds": fold_payloads,
        "outcome_diagnostics": asdict(output_diagnostics),
        "diagnostic_probabilistic_sharpe": probabilistic_sharpe_ratio(oos_returns),
        "diagnostic_deflated_sharpe": deflated_sharpe_probability(
            oos_returns,
            trial_sharpes,
        ),
    }
    target = Path(output_path)
    _atomic_json(target, payload)
    return DistributedReductionResult(
        output_path=target,
        output_sha256=sha256_file(target),
        merged_ledger_path=merged_path,
        merged_ledger_sha256=sha256_file(merged_path),
        fold_count=len(folds),
        candidate_count=expected_candidate_count,
        trial_count=merged.row_count,
    )


__all__ = [
    "DistributedReductionError",
    "DistributedReductionResult",
    "reduce_distributed_ledgers",
]
