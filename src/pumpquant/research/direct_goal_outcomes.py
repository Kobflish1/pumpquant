"""Contracts and small utilities for direct-goal outcome discovery.

The outcome atlas is deliberately strategy-free.  It describes the executable
future path after a causal decision and budget, including an *oracle* maximum
credit.  That oracle value is useful as a supervised discovery label, but it is
not a realizable exit policy or an authorization-quality P&L estimate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

LAMPORTS_PER_SOL: Final[int] = 1_000_000_000


@dataclass(frozen=True, slots=True)
class DirectGoalContract:
    """Finite-horizon reach-before-ruin objective in atomic wallet units."""

    starting_wallet_lamports: int
    target_wallet_lamports: int
    ruin_wallet_lamports: int
    horizon_ms: int

    def __post_init__(self) -> None:
        if self.ruin_wallet_lamports < 0:
            raise ValueError("ruin boundary must be nonnegative")
        if not (
            self.ruin_wallet_lamports < self.starting_wallet_lamports < self.target_wallet_lamports
        ):
            raise ValueError("require ruin < start < target")
        if self.horizon_ms <= 0:
            raise ValueError("goal horizon must be positive")

    @property
    def required_gain_lamports(self) -> int:
        return self.target_wallet_lamports - self.starting_wallet_lamports

    @property
    def maximum_initial_budget_lamports(self) -> int:
        return self.starting_wallet_lamports - self.ruin_wallet_lamports

    def required_net_return(self, budget_lamports: int) -> float:
        """Net P&L divided by budget for a one-action direct target hit."""

        if budget_lamports <= 0:
            raise ValueError("budget must be positive")
        return self.required_gain_lamports / budget_lamports

    def validate_budgets(self, budgets_lamports: tuple[int, ...]) -> None:
        if not budgets_lamports:
            raise ValueError("at least one budget is required")
        if tuple(sorted(set(budgets_lamports))) != budgets_lamports:
            raise ValueError("budgets must be unique and strictly increasing")
        if budgets_lamports[0] <= 0:
            raise ValueError("budgets must be positive")
        if budgets_lamports[-1] > self.maximum_initial_budget_lamports:
            raise ValueError("a budget crosses the declared initial ruin boundary")


@dataclass(frozen=True, slots=True)
class ExecutionScenario:
    """Frozen latency/cost state used for path-label materialization."""

    name: str
    probability: float
    entry_latency_ms: int
    exit_latency_ms: int
    entry_attempt_fee_lamports: int
    exit_attempt_fee_lamports: int

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("scenario name is required")
        if not 0 <= self.probability <= 1:
            raise ValueError("scenario probability must be in [0, 1]")
        if self.entry_latency_ms < 0 or self.exit_latency_ms < 0:
            raise ValueError("latencies must be nonnegative")
        if self.entry_attempt_fee_lamports < 0 or self.exit_attempt_fee_lamports < 0:
            raise ValueError("attempt fees must be nonnegative")


def validate_scenarios(scenarios: tuple[ExecutionScenario, ...]) -> None:
    if not scenarios:
        raise ValueError("at least one execution scenario is required")
    names = [scenario.name for scenario in scenarios]
    if len(set(names)) != len(names):
        raise ValueError("execution scenario names must be unique")
    if abs(sum(item.probability for item in scenarios) - 1.0) > 1e-12:
        raise ValueError("execution scenario probabilities must sum to one")


def direct_target_hit(
    *,
    starting_wallet_lamports: int,
    target_wallet_lamports: int,
    net_pnl_lamports: int | None,
) -> bool:
    """Return whether one realized action reaches the direct wallet target."""

    return bool(
        net_pnl_lamports is not None
        and starting_wallet_lamports + net_pnl_lamports >= target_wallet_lamports
    )
