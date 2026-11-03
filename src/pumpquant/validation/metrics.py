"""Small-sample and multiple-testing-aware strategy statistics.

All Sharpe ratios in this module are *per return observation*.  Annualising a
memecoin strategy with an arbitrary square-root-of-time factor is usually
misleading because token lifetimes and trade arrivals are highly irregular.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Performance:
    """Diagnostics for one ordered return series, not necessarily a portfolio."""

    observations: int
    compounded_return: float
    mean_return: float
    volatility: float
    sharpe: float
    sortino: float
    sequence_max_drawdown: float
    hit_rate: float
    profit_factor: float
    skewness: float
    excess_kurtosis: float


def _moments(values: list[float]) -> tuple[float, float, float, float]:
    """Return mean, sample stdev, adjusted sample skew, excess kurtosis."""
    n = len(values)
    if n == 0:
        return 0.0, 0.0, 0.0, 0.0
    mean = statistics.fmean(values)
    if n < 2:
        return mean, 0.0, 0.0, 0.0
    variance = sum((x - mean) ** 2 for x in values) / (n - 1)
    stdev = math.sqrt(max(variance, 0.0))
    if stdev == 0.0:
        return mean, 0.0, 0.0, 0.0
    m2 = sum((x - mean) ** 2 for x in values) / n
    m3 = sum((x - mean) ** 3 for x in values) / n
    m4 = sum((x - mean) ** 4 for x in values) / n
    raw_skew = m3 / (m2**1.5)
    skew = math.sqrt(n * (n - 1)) / (n - 2) * raw_skew if n > 2 else 0.0
    if n > 3:
        raw_kurt = m4 / (m2 * m2)
        excess = ((n - 1) / ((n - 2) * (n - 3))) * ((n + 1) * (raw_kurt - 3.0) + 6.0)
    else:
        excess = 0.0
    return mean, stdev, skew, excess


def max_drawdown(returns: list[float]) -> float:
    equity = 1.0
    peak = 1.0
    worst = 0.0
    for value in returns:
        equity *= max(0.0, 1.0 + value)
        peak = max(peak, equity)
        if peak > 0.0:
            worst = min(worst, equity / peak - 1.0)
    return worst


def performance(returns: list[float]) -> Performance:
    clean = [float(value) for value in returns]
    if not all(math.isfinite(value) for value in clean):
        raise ValueError("returns must contain only finite values")
    mean, volatility, skew, excess_kurtosis = _moments(clean)
    sharpe = mean / volatility if volatility > 0.0 else 0.0
    downside = [min(value, 0.0) for value in clean]
    downside_deviation = math.sqrt(statistics.fmean([x * x for x in downside])) if clean else 0.0
    sortino = mean / downside_deviation if downside_deviation > 0.0 else 0.0
    gains = sum(value for value in clean if value > 0.0)
    losses = -sum(value for value in clean if value < 0.0)
    if losses > 0.0:
        profit_factor = gains / losses
    elif gains > 0.0:
        profit_factor = math.inf
    else:
        profit_factor = 0.0
    compounded_return = math.prod(1.0 + value for value in clean) - 1.0 if clean else 0.0
    return Performance(
        observations=len(clean),
        compounded_return=compounded_return,
        mean_return=mean,
        volatility=volatility,
        sharpe=sharpe,
        sortino=sortino,
        sequence_max_drawdown=max_drawdown(clean),
        hit_rate=sum(value > 0.0 for value in clean) / len(clean) if clean else 0.0,
        profit_factor=profit_factor,
        skewness=skew,
        excess_kurtosis=excess_kurtosis,
    )


def probabilistic_sharpe_ratio(returns: list[float], benchmark_sharpe: float = 0.0) -> float:
    """Probability that the population Sharpe exceeds ``benchmark_sharpe``.

    Implements the skew/kurtosis-adjusted statistic from Bailey and Lopez de
    Prado.  It is deliberately not reported for fewer than three observations.
    """
    clean = [float(value) for value in returns]
    if not all(math.isfinite(value) for value in clean):
        raise ValueError("returns must contain only finite values")
    if len(clean) < 3:
        return 0.5
    perf = performance(clean)
    kurtosis = perf.excess_kurtosis + 3.0
    denominator_sq = 1.0 - perf.skewness * perf.sharpe + ((kurtosis - 1.0) / 4.0) * (perf.sharpe**2)
    if denominator_sq <= 0.0:
        return 0.5
    z_score = (
        (perf.sharpe - benchmark_sharpe) * math.sqrt(len(clean) - 1) / math.sqrt(denominator_sq)
    )
    return statistics.NormalDist().cdf(z_score)


def expected_maximum_sharpe(trials: int, sharpe_std: float) -> float:
    """Expected best Sharpe under multiple independent zero-edge trials."""
    if trials <= 1 or sharpe_std <= 0.0:
        return 0.0
    normal = statistics.NormalDist()
    euler_gamma = 0.5772156649015329
    first = normal.inv_cdf(1.0 - 1.0 / trials)
    second = normal.inv_cdf(1.0 - 1.0 / (trials * math.e))
    return sharpe_std * ((1.0 - euler_gamma) * first + euler_gamma * second)


def deflated_sharpe_probability(returns: list[float], candidate_sharpes: list[float]) -> float:
    """PSR against the Sharpe expected from the recorded strategy trials.

    ``candidate_sharpes`` must include every strategy/parameter trial in the
    experiment, not just the finalists.  Omitting failed trials invalidates the
    selection-bias adjustment.
    """
    clean_trials = [float(value) for value in candidate_sharpes]
    if not all(math.isfinite(value) for value in clean_trials):
        raise ValueError("candidate Sharpes must contain only finite values")
    if len(clean_trials) < 2:
        benchmark = 0.0
    else:
        benchmark = expected_maximum_sharpe(len(clean_trials), statistics.stdev(clean_trials))
    return probabilistic_sharpe_ratio(returns, benchmark)
