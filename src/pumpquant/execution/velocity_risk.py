"""Sizing and risk metrics for high-frequency velocity strategies.

The functions in this module consume *net account returns*.  Venue
fees, AMM impact, failed-transaction costs, latency, and slippage rejection must
already be represented in each observation before Kelly sizing is attempted.
"""

from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RiskLimits:
    """Constraints applied to the otherwise unconstrained Kelly optimum."""

    ruin_equity_fraction: float = 0.25
    maximum_ruin_probability: float = 0.01
    maximum_p95_drawdown: float = 0.60
    bootstrap_paths: int = 2_000
    bootstrap_trades: int = 1_000
    random_seed: int = 17

    def __post_init__(self) -> None:
        if not 0.0 < self.ruin_equity_fraction < 1.0:
            raise ValueError("ruin_equity_fraction must be inside (0, 1)")
        if not 0.0 <= self.maximum_ruin_probability <= 1.0:
            raise ValueError("maximum_ruin_probability must be inside [0, 1]")
        if not 0.0 <= self.maximum_p95_drawdown <= 1.0:
            raise ValueError("maximum_p95_drawdown must be inside [0, 1]")
        if self.bootstrap_paths < 1 or self.bootstrap_trades < 1:
            raise ValueError("bootstrap dimensions must be positive")


@dataclass(frozen=True, slots=True)
class SizingResult:
    fraction: float
    observations: int
    mean_account_return: float
    geometric_multiplier_per_trade: float
    geometric_growth_per_hour: float
    ruin_probability: float
    median_max_drawdown: float
    p95_max_drawdown: float
    admissible: bool


def _quantile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot calculate a quantile of an empty sequence")
    index = (len(ordered) - 1) * probability
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _bootstrap_risk(
    returns: Sequence[float],
    limits: RiskLimits,
    *,
    fraction_seed: int,
) -> tuple[float, float, float]:
    rng = random.Random(limits.random_seed + fraction_seed)
    ruin_count = 0
    drawdowns: list[float] = []
    length = len(returns)
    for _ in range(limits.bootstrap_paths):
        equity = 1.0
        peak = 1.0
        maximum_drawdown = 0.0
        ruined = False
        for _ in range(limits.bootstrap_trades):
            outcome = returns[rng.randrange(length)]
            equity *= 1.0 + outcome
            if equity <= 0.0:
                ruined = True
                maximum_drawdown = 1.0
                break
            peak = max(peak, equity)
            maximum_drawdown = max(maximum_drawdown, 1.0 - equity / peak)
            if equity <= limits.ruin_equity_fraction:
                ruined = True
        ruin_count += int(ruined)
        drawdowns.append(maximum_drawdown)
    return (
        ruin_count / limits.bootstrap_paths,
        _quantile(drawdowns, 0.50),
        _quantile(drawdowns, 0.95),
    )


def evaluate_sizing_grid(
    account_returns_by_fraction: Mapping[float, Sequence[float]],
    *,
    trades_per_hour: float,
    limits: RiskLimits | None = None,
) -> list[SizingResult]:
    """Evaluate Kelly growth and bootstrap risk for precomputed net outcomes."""

    selected_limits = limits or RiskLimits()
    if not math.isfinite(trades_per_hour) or trades_per_hour < 0.0:
        raise ValueError("trades_per_hour must be finite and nonnegative")
    results: list[SizingResult] = []
    for index, (fraction, raw_returns) in enumerate(sorted(account_returns_by_fraction.items())):
        if not math.isfinite(fraction) or not 0.0 < fraction <= 1.0:
            raise ValueError("fractions must be finite and inside (0, 1]")
        returns = tuple(float(value) for value in raw_returns)
        if not returns or not all(math.isfinite(value) and value > -1.0 for value in returns):
            raise ValueError("each fraction requires finite, non-bankrupting returns")
        mean_return = sum(returns) / len(returns)
        mean_log = sum(math.log1p(value) for value in returns) / len(returns)
        geometric = math.exp(mean_log)
        hourly = math.exp(mean_log * trades_per_hour) - 1.0
        ruin, median_drawdown, p95_drawdown = _bootstrap_risk(
            returns,
            selected_limits,
            fraction_seed=index * 1_000_003,
        )
        admissible = (
            ruin <= selected_limits.maximum_ruin_probability
            and p95_drawdown <= selected_limits.maximum_p95_drawdown
        )
        results.append(
            SizingResult(
                fraction=fraction,
                observations=len(returns),
                mean_account_return=mean_return,
                geometric_multiplier_per_trade=geometric,
                geometric_growth_per_hour=hourly,
                ruin_probability=ruin,
                median_max_drawdown=median_drawdown,
                p95_max_drawdown=p95_drawdown,
                admissible=admissible,
            )
        )
    return results


def select_growth_optimum(
    results: Sequence[SizingResult], *, constrained: bool
) -> SizingResult | None:
    """Return the maximum log-growth result, optionally enforcing risk limits."""

    eligible = [result for result in results if result.admissible or not constrained]
    if not eligible:
        return None
    return max(
        eligible,
        key=lambda result: (
            result.geometric_growth_per_hour,
            -result.p95_max_drawdown,
            -result.fraction,
        ),
    )
