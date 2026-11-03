"""Deterministic diagnostics for a fixed strategy's signal-level return ledger."""

from __future__ import annotations

import math
import random
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any


def _quantile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot calculate a quantile of an empty sequence")
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def _chronological_blocks(
    rows: Sequence[Mapping[str, Any]],
    block_count: int,
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for index in range(block_count):
        start = len(rows) * index // block_count
        end = len(rows) * (index + 1) // block_count
        group = rows[start:end]
        returns_r = [float(row["net_return_r"]) for row in group]
        blocks.append(
            {
                "block": index + 1,
                "signals": len(group),
                "fills": sum(bool(row["entered"]) for row in group),
                "start_time_ns": int(group[0]["signal_time_ns"]),
                "end_time_ns": int(group[-1]["signal_time_ns"]),
                "expectancy_r": _mean(returns_r),
                "positive": _mean(returns_r) > 0.0,
            }
        )
    return blocks


def _day_blocks(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        timestamp = int(row["signal_time_ns"]) / 1_000_000_000
        day = datetime.fromtimestamp(timestamp, tz=UTC).date().isoformat()
        grouped[day].append(row)
    result: list[dict[str, Any]] = []
    for day, group in sorted(grouped.items()):
        returns_r = [float(row["net_return_r"]) for row in group]
        result.append(
            {
                "day": day,
                "signals": len(group),
                "fills": sum(bool(row["entered"]) for row in group),
                "expectancy_r": _mean(returns_r),
                "net_r": sum(returns_r),
            }
        )
    return result


def _moving_block_bootstrap(
    values: Sequence[float],
    *,
    replicates: int,
    block_length: int,
    seed: int,
) -> dict[str, float | int]:
    if replicates < 1 or block_length < 1:
        raise ValueError("bootstrap dimensions must be positive")
    rng = random.Random(seed)
    count = len(values)
    means: list[float] = []
    for _ in range(replicates):
        sample: list[float] = []
        while len(sample) < count:
            start = rng.randrange(count)
            sample.extend(values[(start + offset) % count] for offset in range(block_length))
        means.append(_mean(sample[:count]))
    return {
        "replicates": replicates,
        "block_length_signals": block_length,
        "mean_expectancy_r": _mean(values),
        "lower_95_expectancy_r": _quantile(means, 0.025),
        "median_expectancy_r": _quantile(means, 0.5),
        "upper_95_expectancy_r": _quantile(means, 0.975),
        "probability_nonpositive": sum(value <= 0.0 for value in means) / replicates,
    }


def _sign_placebo(
    values: Sequence[float],
    *,
    replicates: int,
    seed: int,
) -> dict[str, float | int]:
    rng = random.Random(seed)
    observed = _mean(values)
    exceedances = 0
    for _ in range(replicates):
        placebo = sum(value if rng.randrange(2) else -value for value in values) / len(values)
        exceedances += int(placebo >= observed)
    return {
        "replicates": replicates,
        "observed_expectancy_r": observed,
        "one_sided_p_value": (exceedances + 1) / (replicates + 1),
    }


def _equity_path(account_returns: Sequence[float]) -> dict[str, float]:
    equity = 1.0
    peak = 1.0
    maximum_drawdown = 0.0
    for value in account_returns:
        equity *= 1.0 + value
        peak = max(peak, equity)
        maximum_drawdown = max(maximum_drawdown, 1.0 - equity / peak)
    return {
        "ending_multiplier": equity,
        "ending_balance_from_10_usd": 10.0 * equity,
        "maximum_drawdown": maximum_drawdown,
    }


def validate_fixed_strategy(
    signal_results: Sequence[Mapping[str, Any]],
    *,
    block_count: int = 8,
    bootstrap_replicates: int = 20_000,
    bootstrap_block_length: int = 8,
    placebo_replicates: int = 20_000,
    seed: int = 67_005,
) -> dict[str, Any]:
    """Validate one already-fixed rule without searching or changing parameters."""
    rows = sorted(
        signal_results,
        key=lambda row: (int(row["signal_time_ns"]), str(row["mint"])),
    )
    if len(rows) < block_count:
        raise ValueError("signal ledger is too small for chronological blocks")
    returns_r = [float(row["net_return_r"]) for row in rows]
    account_returns = [float(row["account_return"]) for row in rows]
    if not all(math.isfinite(value) for value in returns_r + account_returns):
        raise ValueError("signal returns must be finite")

    blocks = _chronological_blocks(rows, block_count)
    days = _day_blocks(rows)
    positive_values = sorted((value for value in returns_r if value > 0.0), reverse=True)
    gross_positive_r = sum(positive_values)
    leave_best: dict[str, float] = {}
    for count in (1, 3, 5):
        trimmed = sorted(returns_r, reverse=True)[count:]
        leave_best[str(count)] = _mean(trimmed)
    concentration = {
        f"top_{count}_share_of_gross_positive_r": (
            sum(positive_values[:count]) / gross_positive_r if gross_positive_r else 0.0
        )
        for count in (1, 3, 5)
    }
    bootstrap = _moving_block_bootstrap(
        returns_r,
        replicates=bootstrap_replicates,
        block_length=bootstrap_block_length,
        seed=seed,
    )
    placebo = _sign_placebo(
        returns_r,
        replicates=placebo_replicates,
        seed=seed + 1,
    )
    positive_blocks = sum(bool(block["positive"]) for block in blocks)
    tests = {
        "minimum_30_fills": sum(bool(row["entered"]) for row in rows) >= 30,
        "at_least_6_of_8_positive_chronological_blocks": positive_blocks >= 6,
        "block_bootstrap_lower_95_positive": float(bootstrap["lower_95_expectancy_r"]) > 0.0,
        "sign_placebo_p_below_0_05": float(placebo["one_sided_p_value"]) < 0.05,
        "positive_after_removing_best_3_signals": leave_best["3"] > 0.0,
    }
    return {
        "status": "complete",
        "fixed_rule_only": True,
        "signals": len(rows),
        "fills": sum(bool(row["entered"]) for row in rows),
        "closed_trades": sum(bool(row["closed"]) for row in rows),
        "expectancy_r": _mean(returns_r),
        "mean_account_return": _mean(account_returns),
        "chronological_blocks": blocks,
        "positive_chronological_blocks": positive_blocks,
        "day_blocks": days,
        "moving_block_bootstrap": bootstrap,
        "sign_placebo": placebo,
        "leave_best_signals_out_expectancy_r": leave_best,
        "positive_pnl_concentration": concentration,
        "observed_sequential_equity": _equity_path(account_returns),
        "tests": tests,
        "all_tests_pass": all(tests.values()),
        "truth_boundaries": [
            "The rule and execution profile are fixed; this report performs no search.",
            "The block bootstrap preserves short-range chronological dependence but "
            "cannot reproduce unseen market regimes.",
            "The sign placebo is a diagnostic null, not a complete matched-control test.",
            "Historical availability remains a synthetic block-time proxy.",
            "The untouched chronological holdout is not read.",
        ],
    }


__all__ = ["validate_fixed_strategy"]
