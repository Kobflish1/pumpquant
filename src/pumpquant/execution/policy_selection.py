"""Joint, distributionally conservative execution-policy selection.

The optimiser does not optimise fees, retries, slippage, routes, and size in
isolation.  Each candidate is a complete executable action bundle.  Its
outcomes must already include fills, misses, failed-attempt fees, AMM impact,
rent, and any realised exit credit.  Entry always competes with the zero-P&L
``skip`` action; exits cannot skip an open risk and therefore select the best
supported loss-control bundle even when all choices have negative utility.
"""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from typing import Literal

from pumpquant.execution.realism import wilson_lower

Side = Literal["entry", "exit"]


@dataclass(frozen=True, slots=True)
class JointExecutionAction:
    action_id: str
    side: Side
    priority_micro_lamports_per_cu: int
    compute_unit_limit: int
    route: str
    maximum_attempts: int
    retry_delay_ms: int
    slippage_bps: int
    order_fraction: float
    maximum_concurrency: int

    def __post_init__(self) -> None:
        if not self.action_id or self.side not in {"entry", "exit"} or not self.route:
            raise ValueError("invalid action identity")
        if (
            min(
                self.priority_micro_lamports_per_cu,
                self.compute_unit_limit,
                self.maximum_attempts,
                self.retry_delay_ms,
                self.slippage_bps,
                self.maximum_concurrency,
            )
            < 0
        ):
            raise ValueError("action controls must be non-negative")
        if self.compute_unit_limit < 1 or self.maximum_attempts < 1:
            raise ValueError("compute and attempt limits must be positive")
        if not 0 <= self.slippage_bps < 10_000:
            raise ValueError("slippage is invalid")
        if not 0 < self.order_fraction <= 1 or self.maximum_concurrency < 1:
            raise ValueError("wallet controls are invalid")

    @property
    def priority_fee_lamports(self) -> int:
        return math.ceil(self.priority_micro_lamports_per_cu * self.compute_unit_limit / 1_000_000)


@dataclass(frozen=True, slots=True)
class JointActionOutcome:
    action_id: str
    chronological_index: int
    regime: str
    net_pnl_lamports: int
    fee_lamports: int
    latency_ms: float
    landed: bool
    closed: bool

    def __post_init__(self) -> None:
        if not self.action_id or self.chronological_index < 0 or not self.regime:
            raise ValueError("invalid outcome identity")
        if self.fee_lamports < 0 or not math.isfinite(self.latency_ms) or self.latency_ms < 0:
            raise ValueError("invalid execution outcome")


@dataclass(frozen=True, slots=True)
class JointOptimizerContract:
    wallet_lamports: int
    minimum_samples: int = 30
    minimum_regimes: int = 2
    minimum_entry_landing_lower_95: float = 0.0
    minimum_exit_landing_lower_95: float = 0.80
    lower_tail_fraction: float = 0.10
    bootstrap_repetitions: int = 1_000
    bootstrap_mean_block_length: float = 4.0
    bootstrap_lower_probability: float = 0.05

    def __post_init__(self) -> None:
        if self.wallet_lamports <= 0 or self.minimum_samples < 2 or self.minimum_regimes < 1:
            raise ValueError("invalid support contract")
        if not 0 <= self.minimum_entry_landing_lower_95 <= 1:
            raise ValueError("entry landing gate is invalid")
        if not 0 <= self.minimum_exit_landing_lower_95 <= 1:
            raise ValueError("exit landing gate is invalid")
        if not 0 < self.lower_tail_fraction <= 0.5:
            raise ValueError("tail fraction is invalid")
        if self.bootstrap_repetitions < 100 or self.bootstrap_mean_block_length < 1:
            raise ValueError("bootstrap controls are invalid")
        if not 0 < self.bootstrap_lower_probability < 0.5:
            raise ValueError("bootstrap probability is invalid")


@dataclass(frozen=True, slots=True)
class JointActionEvaluation:
    action: JointExecutionAction
    samples: int
    regimes: int
    landed: int
    closed: int
    landing_probability: float
    landing_wilson_lower_95: float
    mean_net_pnl_lamports: float
    median_net_pnl_lamports: float
    largest_winner_removed_mean_pnl_lamports: float
    lower_tail_mean_pnl_lamports: float
    mean_log_growth: float
    worst_regime_mean_log_growth: float
    bootstrap_lower_mean_log_growth: float
    largest_winner_removed_mean_log_growth: float
    robust_log_score: float
    mean_fee_lamports: float
    latency_p90_ms: float
    eligible: bool
    rejection_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class JointPolicyDecision:
    side: Side
    action: str
    reason: str
    selected_action_id: str | None
    selected_robust_log_score: float
    evaluations: tuple[JointActionEvaluation, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _higher_quantile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(probability * len(ordered)) - 1)]


def _lower_quantile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, math.floor(probability * len(ordered))))]


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def _log_growth(pnl: int, wallet: int) -> float:
    ending = wallet + pnl
    return math.log(ending / wallet) if ending > 0 else -math.inf


def _uniform(seed: str) -> float:
    digest = hashlib.sha256(seed.encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def _stationary_bootstrap_mean_lower(
    values: Sequence[float],
    *,
    repetitions: int,
    mean_block_length: float,
    lower_probability: float,
    seed: str,
) -> float:
    restart_probability = 1.0 / mean_block_length
    means: list[float] = []
    count = len(values)
    for repetition in range(repetitions):
        current = int(_uniform(f"{seed}:start:{repetition}") * count) % count
        total = 0.0
        for index in range(count):
            total += values[current]
            if _uniform(f"{seed}:restart:{repetition}:{index}") < restart_probability:
                current = int(_uniform(f"{seed}:jump:{repetition}:{index}") * count) % count
            else:
                current = (current + 1) % count
        means.append(total / count)
    return _lower_quantile(means, lower_probability)


def evaluate_joint_action(
    action: JointExecutionAction,
    outcomes: Sequence[JointActionOutcome],
    contract: JointOptimizerContract,
) -> JointActionEvaluation:
    rows = sorted(
        (row for row in outcomes if row.action_id == action.action_id),
        key=lambda row: row.chronological_index,
    )
    if not rows:
        raise ValueError(f"no outcomes for action {action.action_id}")
    pnl = [row.net_pnl_lamports for row in rows]
    logs = [_log_growth(value, contract.wallet_lamports) for value in pnl]
    regimes: dict[str, list[float]] = defaultdict(list)
    for row, value in zip(rows, logs, strict=True):
        regimes[row.regime].append(value)
    landing_count = sum(row.landed for row in rows)
    landing_lower = wilson_lower(landing_count, len(rows))
    ordered_pnl = sorted(pnl)
    tail_count = max(1, math.ceil(contract.lower_tail_fraction * len(pnl)))
    removed_pnl = pnl.copy()
    removed_pnl.remove(max(removed_pnl))
    removed_logs = logs.copy()
    removed_logs.pop(max(range(len(pnl)), key=pnl.__getitem__))
    reasons: list[str] = []
    if len(rows) < contract.minimum_samples:
        reasons.append(f"samples={len(rows)}<{contract.minimum_samples}")
    if len(regimes) < contract.minimum_regimes:
        reasons.append(f"regimes={len(regimes)}<{contract.minimum_regimes}")
    landing_gate = (
        contract.minimum_entry_landing_lower_95
        if action.side == "entry"
        else contract.minimum_exit_landing_lower_95
    )
    if landing_lower < landing_gate:
        reasons.append(f"landing_lower_95={landing_lower:.6f}<{landing_gate:.6f}")
    if any(not math.isfinite(value) for value in logs):
        reasons.append("ruin_or_nonfinite_log_growth")
    bootstrap_lower = (
        _stationary_bootstrap_mean_lower(
            logs,
            repetitions=contract.bootstrap_repetitions,
            mean_block_length=contract.bootstrap_mean_block_length,
            lower_probability=contract.bootstrap_lower_probability,
            seed=action.action_id,
        )
        if all(math.isfinite(value) for value in logs)
        else -math.inf
    )
    worst_regime = min(_mean(values) for values in regimes.values())
    removed_log_mean = _mean(removed_logs) if removed_logs else -math.inf
    robust_score = min(bootstrap_lower, worst_regime, removed_log_mean)
    if action.side == "entry" and robust_score <= 0:
        reasons.append("robust_log_score_not_above_skip")
    if action.side == "entry" and _mean(removed_pnl) <= 0:
        reasons.append("largest_winner_removed_expectancy_not_positive")
    return JointActionEvaluation(
        action=action,
        samples=len(rows),
        regimes=len(regimes),
        landed=landing_count,
        closed=sum(row.closed for row in rows),
        landing_probability=landing_count / len(rows),
        landing_wilson_lower_95=landing_lower,
        mean_net_pnl_lamports=_mean(pnl),
        median_net_pnl_lamports=_higher_quantile(pnl, 0.50),
        largest_winner_removed_mean_pnl_lamports=_mean(removed_pnl),
        lower_tail_mean_pnl_lamports=_mean(ordered_pnl[:tail_count]),
        mean_log_growth=_mean(logs),
        worst_regime_mean_log_growth=worst_regime,
        bootstrap_lower_mean_log_growth=bootstrap_lower,
        largest_winner_removed_mean_log_growth=removed_log_mean,
        robust_log_score=robust_score,
        mean_fee_lamports=_mean([row.fee_lamports for row in rows]),
        latency_p90_ms=_higher_quantile([row.latency_ms for row in rows], 0.90),
        eligible=not reasons,
        rejection_reasons=tuple(reasons),
    )


def select_joint_profit_action(
    actions: Iterable[JointExecutionAction],
    outcomes: Iterable[JointActionOutcome],
    contract: JointOptimizerContract,
    *,
    side: Side,
) -> JointPolicyDecision:
    materialized_outcomes = tuple(outcomes)
    evaluations = tuple(
        evaluate_joint_action(action, materialized_outcomes, contract)
        for action in actions
        if action.side == side
    )
    if not evaluations:
        raise ValueError(f"no {side} actions")
    eligible = [evaluation for evaluation in evaluations if evaluation.eligible]
    if side == "entry" and not eligible:
        return JointPolicyDecision(
            side="entry",
            action="skip",
            reason="no_supported_bundle_beats_zero_pnl_skip_under_robust_log_growth",
            selected_action_id=None,
            selected_robust_log_score=0.0,
            evaluations=evaluations,
        )
    pool = eligible if eligible else list(evaluations)
    selected = max(
        pool,
        key=lambda item: (
            item.robust_log_score,
            item.lower_tail_mean_pnl_lamports,
            -item.mean_fee_lamports,
            -item.latency_p90_ms,
        ),
    )
    return JointPolicyDecision(
        side=side,
        action="submit",
        reason=(
            "maximum_supported_conservative_net_log_growth"
            if eligible
            else "mandatory_exit_least_bad_supported_or_observed_bundle"
        ),
        selected_action_id=selected.action.action_id,
        selected_robust_log_score=selected.robust_log_score,
        evaluations=evaluations,
    )


__all__ = [
    "JointActionEvaluation",
    "JointActionOutcome",
    "JointExecutionAction",
    "JointOptimizerContract",
    "JointPolicyDecision",
    "evaluate_joint_action",
    "select_joint_profit_action",
]
