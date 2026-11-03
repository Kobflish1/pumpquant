"""Exact chronological validation for a frozen wallet-growth candidate.

The module deliberately has no approximate notional scaling. Every simulated
trade is selected from a replayed outcome at an explicit budget and execution
scenario. Validation windows are real consecutive clock intervals.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass

import numpy as np
import numpy.typing as npt
from arch.bootstrap import SPA, optimal_block_length

MILLISECONDS_PER_HOUR = 3_600_000


@dataclass(frozen=True, slots=True)
class Goal:
    starting_wallet_lamports: int
    target_wallet_lamports: int
    ruin_wallet_lamports: int
    horizon_hours: int
    allocation_fraction: float | None = None

    def __post_init__(self) -> None:
        if not 0 < self.ruin_wallet_lamports < self.starting_wallet_lamports:
            raise ValueError("ruin must be between zero and the starting wallet")
        if self.target_wallet_lamports <= self.starting_wallet_lamports:
            raise ValueError("target must exceed the starting wallet")
        if self.horizon_hours <= 0:
            raise ValueError("horizon must be positive")
        if self.allocation_fraction is not None and not 0 < self.allocation_fraction <= 1:
            raise ValueError("allocation fraction must be in (0, 1]")

    @property
    def horizon_ms(self) -> int:
        return self.horizon_hours * MILLISECONDS_PER_HOUR


@dataclass(frozen=True, slots=True)
class ExactOutcome:
    signal_id: str
    mint: str
    decision_time_ms: int
    budget_lamports: int
    scenario: str
    net_pnl_lamports: int
    entry_landing_time_ms: int | None
    exit_landing_time_ms: int | None
    capital_release_time_ms: int


@dataclass(frozen=True, slots=True)
class ValidationWindow:
    start_time_ms: int
    end_time_ms: int

    def __post_init__(self) -> None:
        if self.end_time_ms <= self.start_time_ms:
            raise ValueError("window end must follow its start")


@dataclass(frozen=True, slots=True)
class TimedObservation:
    observation_id: str
    feature_time_ms: int
    label_end_time_ms: int

    def __post_init__(self) -> None:
        if self.label_end_time_ms < self.feature_time_ms:
            raise ValueError("label cannot end before features are observed")


@dataclass(frozen=True, slots=True)
class PurgedWalkForwardFold:
    test_window: ValidationWindow
    train_observation_ids: tuple[str, ...]
    test_observation_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class WindowResult:
    scenario: str
    start_time_ms: int
    end_time_ms: int
    ending_wallet_lamports: int
    reached_target: bool
    reached_ruin: bool
    trades: int
    skipped_while_position_open: int
    skipped_without_exact_budget: int
    unresolved_position_at_horizon: bool


@dataclass(frozen=True, slots=True)
class DecisionContext:
    """Information available when a direct-goal policy sees one signal."""

    signal_id: str
    mint: str
    decision_time_ms: int
    window_end_time_ms: int
    wallet_lamports: int
    target_wallet_lamports: int
    ruin_wallet_lamports: int
    available_budgets_lamports: tuple[int, ...]

    @property
    def remaining_time_ms(self) -> int:
        return self.window_end_time_ms - self.decision_time_ms


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    """Skip the signal with ``None`` or trade one exact replayed budget."""

    budget_lamports: int | None


DirectGoalPolicy = Callable[[DecisionContext], PolicyDecision]


class ExactOutcomeCube:
    """Validated lookup table indexed by scenario, budget and signal."""

    def __init__(self, outcomes: Iterable[ExactOutcome]) -> None:
        lookup: dict[tuple[str, int, str], ExactOutcome] = {}
        signals: dict[str, tuple[int, str]] = {}
        budgets: set[int] = set()
        scenarios: set[str] = set()
        for outcome in outcomes:
            if outcome.budget_lamports <= 0:
                raise ValueError("budgets must be positive")
            if outcome.capital_release_time_ms < outcome.decision_time_ms:
                raise ValueError("capital cannot be released before the signal")
            key = (outcome.scenario, outcome.budget_lamports, outcome.signal_id)
            if key in lookup:
                raise ValueError(f"duplicate exact outcome: {key}")
            identity = (outcome.decision_time_ms, outcome.mint)
            if outcome.signal_id in signals and signals[outcome.signal_id] != identity:
                raise ValueError(f"signal identity changed: {outcome.signal_id}")
            signals[outcome.signal_id] = identity
            budgets.add(outcome.budget_lamports)
            scenarios.add(outcome.scenario)
            lookup[key] = outcome
        if not lookup:
            raise ValueError("outcome cube is empty")

        expected = set(signals)
        for scenario in scenarios:
            for budget in budgets:
                actual = {
                    signal_id
                    for current_scenario, current_budget, signal_id in lookup
                    if current_scenario == scenario and current_budget == budget
                }
                if actual != expected:
                    missing = sorted(expected - actual)[:3]
                    raise ValueError(
                        f"incomplete outcome cube for {scenario}/{budget}; missing={missing}"
                    )
        self._lookup = lookup
        self._signals = signals
        self.budgets = tuple(sorted(budgets))
        self.scenarios = tuple(sorted(scenarios))

    def chronological_signal_ids(self, window: ValidationWindow) -> tuple[str, ...]:
        return tuple(
            signal_id
            for signal_id, _ in sorted(
                self._signals.items(), key=lambda item: (item[1][0], item[1][1], item[0])
            )
            if window.start_time_ms <= self._signals[signal_id][0] < window.end_time_ms
        )

    def outcome(self, scenario: str, budget: int, signal_id: str) -> ExactOutcome:
        return self._lookup[(scenario, budget, signal_id)]


def consecutive_windows(
    start_time_ms: int,
    end_time_ms: int,
    horizon_hours: int,
    *,
    step_hours: int | None = None,
) -> tuple[ValidationWindow, ...]:
    """Build real clock windows; no day duplication or synthetic extension."""
    horizon = horizon_hours * MILLISECONDS_PER_HOUR
    step = (step_hours or horizon_hours) * MILLISECONDS_PER_HOUR
    if horizon <= 0 or step <= 0:
        raise ValueError("window horizon and step must be positive")
    windows = []
    cursor = start_time_ms
    while cursor + horizon <= end_time_ms:
        windows.append(ValidationWindow(cursor, cursor + horizon))
        cursor += step
    return tuple(windows)


def purged_walk_forward_folds(
    observations: Iterable[TimedObservation],
    test_windows: Sequence[ValidationWindow],
    *,
    embargo_ms: int,
    minimum_train_observations: int,
    minimum_test_observations: int,
) -> tuple[PurgedWalkForwardFold, ...]:
    """Create expanding folds without overlapping training labels.

    Training observations are strictly earlier than each test window and their
    labels must finish before the embargo boundary. Test labels must resolve
    inside the test window. Future observations are never used for training.
    """
    if embargo_ms < 0:
        raise ValueError("embargo cannot be negative")
    if minimum_train_observations < 1 or minimum_test_observations < 1:
        raise ValueError("minimum fold sizes must be positive")
    ordered = sorted(
        observations,
        key=lambda item: (item.feature_time_ms, item.label_end_time_ms, item.observation_id),
    )
    if len({item.observation_id for item in ordered}) != len(ordered):
        raise ValueError("observation identifiers must be unique")
    folds = []
    for window in test_windows:
        train_cutoff = window.start_time_ms - embargo_ms
        train = tuple(
            item.observation_id
            for item in ordered
            if item.feature_time_ms < window.start_time_ms and item.label_end_time_ms < train_cutoff
        )
        test = tuple(
            item.observation_id
            for item in ordered
            if window.start_time_ms <= item.feature_time_ms < window.end_time_ms
            and item.label_end_time_ms <= window.end_time_ms
        )
        if len(train) < minimum_train_observations or len(test) < minimum_test_observations:
            continue
        folds.append(PurgedWalkForwardFold(window, train, test))
    return tuple(folds)


def simulate_exact_window(
    cube: ExactOutcomeCube,
    goal: Goal,
    window: ValidationWindow,
    scenario: str,
) -> WindowResult:
    if window.end_time_ms - window.start_time_ms != goal.horizon_ms:
        raise ValueError("validation window does not equal the declared goal horizon")
    if scenario not in cube.scenarios:
        raise ValueError(f"unknown execution scenario: {scenario}")
    if goal.allocation_fraction is None:
        raise ValueError("fixed-allocation replay requires allocation_fraction")

    wallet = goal.starting_wallet_lamports
    busy_until = window.start_time_ms
    trades = 0
    skipped_open = 0
    skipped_budget = 0
    reached_target = False
    reached_ruin = False
    unresolved_position = False
    for signal_id in cube.chronological_signal_ids(window):
        decision_time_ms, _ = cube._signals[signal_id]
        if decision_time_ms < busy_until:
            skipped_open += 1
            continue
        desired_budget = int(wallet * goal.allocation_fraction)
        usable = [budget for budget in cube.budgets if budget <= desired_budget]
        if not usable:
            skipped_budget += 1
            continue
        budget = usable[-1]
        outcome = cube.outcome(scenario, budget, signal_id)
        trades += 1
        busy_until = outcome.capital_release_time_ms
        if busy_until > window.end_time_ms:
            unresolved_position = True
            break
        wallet += outcome.net_pnl_lamports
        if wallet >= goal.target_wallet_lamports:
            reached_target = True
            break
        if wallet <= goal.ruin_wallet_lamports:
            reached_ruin = True
            break
    return WindowResult(
        scenario=scenario,
        start_time_ms=window.start_time_ms,
        end_time_ms=window.end_time_ms,
        ending_wallet_lamports=wallet,
        reached_target=reached_target,
        reached_ruin=reached_ruin,
        trades=trades,
        skipped_while_position_open=skipped_open,
        skipped_without_exact_budget=skipped_budget,
        unresolved_position_at_horizon=unresolved_position,
    )


def simulate_exact_window_policy(
    cube: ExactOutcomeCube,
    goal: Goal,
    window: ValidationWindow,
    scenario: str,
    policy: DirectGoalPolicy,
) -> WindowResult:
    """Replay a dynamic policy whose sole boundary is the final wallet goal.

    The policy may skip or choose any explicitly replayed budget using current
    wallet and remaining time. No intermediate wealth target is imposed.
    """
    if window.end_time_ms - window.start_time_ms != goal.horizon_ms:
        raise ValueError("validation window does not equal the declared goal horizon")
    if scenario not in cube.scenarios:
        raise ValueError(f"unknown execution scenario: {scenario}")

    wallet = goal.starting_wallet_lamports
    busy_until = window.start_time_ms
    trades = 0
    skipped_open = 0
    skipped_budget = 0
    reached_target = False
    reached_ruin = False
    unresolved_position = False
    for signal_id in cube.chronological_signal_ids(window):
        decision_time_ms, mint = cube._signals[signal_id]
        if decision_time_ms < busy_until:
            skipped_open += 1
            continue
        available = tuple(budget for budget in cube.budgets if budget <= wallet)
        if not available:
            skipped_budget += 1
            continue
        decision = policy(
            DecisionContext(
                signal_id=signal_id,
                mint=mint,
                decision_time_ms=decision_time_ms,
                window_end_time_ms=window.end_time_ms,
                wallet_lamports=wallet,
                target_wallet_lamports=goal.target_wallet_lamports,
                ruin_wallet_lamports=goal.ruin_wallet_lamports,
                available_budgets_lamports=available,
            )
        )
        budget = decision.budget_lamports
        if budget is None:
            continue
        if budget not in available:
            raise ValueError("policy selected an unavailable exact budget")
        outcome = cube.outcome(scenario, budget, signal_id)
        trades += 1
        busy_until = outcome.capital_release_time_ms
        if busy_until > window.end_time_ms:
            unresolved_position = True
            break
        wallet += outcome.net_pnl_lamports
        if wallet >= goal.target_wallet_lamports:
            reached_target = True
            break
        if wallet <= goal.ruin_wallet_lamports:
            reached_ruin = True
            break
    return WindowResult(
        scenario=scenario,
        start_time_ms=window.start_time_ms,
        end_time_ms=window.end_time_ms,
        ending_wallet_lamports=wallet,
        reached_target=reached_target,
        reached_ruin=reached_ruin,
        trades=trades,
        skipped_while_position_open=skipped_open,
        skipped_without_exact_budget=skipped_budget,
        unresolved_position_at_horizon=unresolved_position,
    )


def validate_exact_windows(
    cube: ExactOutcomeCube,
    goal: Goal,
    windows: Sequence[ValidationWindow],
) -> dict[str, object]:
    if not windows:
        raise ValueError("no complete consecutive goal-horizon window is available")
    results = [
        simulate_exact_window(cube, goal, window, scenario)
        for window in windows
        for scenario in cube.scenarios
    ]
    grouped: dict[str, list[WindowResult]] = defaultdict(list)
    for result in results:
        grouped[result.scenario].append(result)
    scenario_rates = {
        scenario: sum(result.reached_target for result in rows) / len(rows)
        for scenario, rows in grouped.items()
    }
    return {
        "windows": len(windows),
        "scenarios": list(cube.scenarios),
        "scenario_success_rates": scenario_rates,
        "worst_scenario_success_rate": min(scenario_rates.values()),
        "results": [asdict(result) for result in results],
    }


def automatic_stationary_block_size(values: Sequence[float] | npt.NDArray[np.float64]) -> int:
    array = np.asarray(values, dtype=np.float64)
    if array.ndim != 1 or len(array) < 8 or np.any(~np.isfinite(array)):
        raise ValueError("automatic block selection requires at least eight finite observations")
    if np.ptp(array) == 0:
        return 1
    estimate = optimal_block_length(array)
    stationary = float(estimate.iloc[0]["stationary"])
    if not np.isfinite(stationary):
        return 1
    return max(1, min(len(array), int(round(stationary))))


def hansen_spa(
    benchmark_losses: Sequence[float],
    candidate_losses: Mapping[str, Sequence[float]],
    *,
    bootstrap_repetitions: int = 10_000,
    seed: int = 20260803,
) -> dict[str, object]:
    """Test the full candidate family against one benchmark under dependence."""
    if not candidate_losses:
        raise ValueError("candidate family is empty")
    benchmark = np.asarray(benchmark_losses, dtype=np.float64)
    names = tuple(candidate_losses)
    models = np.column_stack(
        [np.asarray(candidate_losses[name], dtype=np.float64) for name in names]
    )
    if models.shape[0] != len(benchmark):
        raise ValueError("benchmark and candidate histories must have equal length")
    if len(benchmark) < 8 or np.any(~np.isfinite(benchmark)) or np.any(~np.isfinite(models)):
        raise ValueError("SPA requires at least eight finite common time observations")
    block_size = automatic_stationary_block_size(np.min(models - benchmark[:, None], axis=1))
    test = SPA(
        benchmark,
        models,
        block_size=block_size,
        reps=bootstrap_repetitions,
        bootstrap="stationary",
        studentize=True,
        nested=False,
        seed=seed,
    )
    test.compute()
    return {
        "candidate_count": len(names),
        "observations": len(benchmark),
        "stationary_block_size": block_size,
        "pvalues": {key: float(value) for key, value in test.pvalues.items()},
    }


__all__ = [
    "DecisionContext",
    "DirectGoalPolicy",
    "ExactOutcome",
    "ExactOutcomeCube",
    "Goal",
    "PolicyDecision",
    "PurgedWalkForwardFold",
    "TimedObservation",
    "ValidationWindow",
    "WindowResult",
    "automatic_stationary_block_size",
    "consecutive_windows",
    "hansen_spa",
    "purged_walk_forward_folds",
    "simulate_exact_window",
    "simulate_exact_window_policy",
    "validate_exact_windows",
]
