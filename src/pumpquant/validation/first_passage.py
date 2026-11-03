"""First-passage probability estimates for one frozen wallet stair."""

from __future__ import annotations

import math

import numpy as np


def moving_block_indices(
    *,
    observation_count: int,
    paths: int,
    horizon_signals: int,
    block_length: int,
    seed: int,
) -> np.ndarray:
    if min(observation_count, paths, horizon_signals, block_length) < 1:
        raise ValueError("bootstrap dimensions must be positive")
    rng = np.random.default_rng(seed)
    blocks = math.ceil(horizon_signals / block_length)
    starts = rng.integers(0, observation_count, size=(paths, blocks), dtype=np.int32)
    offsets = np.arange(block_length, dtype=np.int32)
    return ((starts[:, :, None] + offsets[None, None, :]) % observation_count).reshape(paths, -1)[
        :, :horizon_signals
    ]


def estimate_first_passage(
    *,
    returns_by_balance: np.ndarray,
    balance_nodes: np.ndarray,
    bootstrap_indices: np.ndarray,
    starting_balance: float,
    target_balance: float,
    half_loss_balance: float,
    ruin_balance: float,
) -> dict[str, float | int]:
    """Estimate target-before-loss probabilities using conservative interpolation.

    Returns at the two exact balance nodes bracketing current equity are looked
    up for the sampled historical signal.  The smaller return is used, avoiding
    optimistic interpolation across slippage/fill discontinuities.
    """

    returns = np.asarray(returns_by_balance, dtype=np.float64)
    nodes = np.asarray(balance_nodes, dtype=np.float64)
    indices = np.asarray(bootstrap_indices, dtype=np.int32)
    if returns.ndim != 2 or indices.ndim != 2:
        raise ValueError("returns and bootstrap indices must be matrices")
    if returns.shape[0] != len(nodes):
        raise ValueError("return surface and balance nodes disagree")
    if len(nodes) < 2 or np.any(np.diff(nodes) <= 0):
        raise ValueError("balance nodes must be strictly increasing")
    if np.any(~np.isfinite(returns)) or np.any(returns <= -1):
        raise ValueError("return surface contains invalid account returns")
    if np.any(indices < 0) or np.any(indices >= returns.shape[1]):
        raise ValueError("bootstrap index is outside the observed signal set")
    if not 0 < ruin_balance < half_loss_balance < starting_balance < target_balance:
        raise ValueError("first-passage boundaries are not ordered")
    if nodes[0] > ruin_balance or nodes[-1] < target_balance:
        raise ValueError("return surface does not span the requested boundaries")

    path_count, horizon = indices.shape
    equity = np.full(path_count, starting_balance, dtype=np.float64)
    peak = equity.copy()
    maximum_drawdown = np.zeros(path_count, dtype=np.float64)
    half_state = np.zeros(path_count, dtype=np.int8)
    ruin_state = np.zeros(path_count, dtype=np.int8)
    target_step = np.zeros(path_count, dtype=np.int32)

    for step in range(horizon):
        active = ruin_state == 0
        if not np.any(active):
            break
        active_rows = np.flatnonzero(active)
        active_equity = equity[active_rows]
        upper = np.searchsorted(nodes, active_equity, side="right")
        upper = np.clip(upper, 1, len(nodes) - 1)
        lower = upper - 1
        signal_indices = indices[active_rows, step]
        conservative_returns = np.minimum(
            returns[lower, signal_indices],
            returns[upper, signal_indices],
        )
        equity[active_rows] *= 1.0 + conservative_returns
        peak[active_rows] = np.maximum(peak[active_rows], equity[active_rows])
        maximum_drawdown[active_rows] = np.maximum(
            maximum_drawdown[active_rows],
            1.0 - equity[active_rows] / peak[active_rows],
        )

        reached_target = active & (equity >= target_balance)
        reached_half = (half_state == 0) & (equity <= half_loss_balance)
        reached_ruin = active & (equity <= ruin_balance)
        half_state[reached_target & (half_state == 0)] = 1
        half_state[reached_half] = -1
        ruin_state[reached_target] = 1
        ruin_state[reached_ruin] = -1
        target_step[reached_target] = step + 1

    target_steps = target_step[target_step > 0]
    return {
        "bootstrap_paths": path_count,
        "horizon_signals": horizon,
        "target_before_50pct_loss_probability": float(np.mean(half_state == 1)),
        "hit_50pct_loss_first_probability": float(np.mean(half_state == -1)),
        "target_before_25pct_ruin_probability": float(np.mean(ruin_state == 1)),
        "hit_25pct_ruin_first_probability": float(np.mean(ruin_state == -1)),
        "unresolved_before_50pct_loss_fraction": float(np.mean(half_state == 0)),
        "unresolved_before_25pct_ruin_fraction": float(np.mean(ruin_state == 0)),
        "conditional_target_signals_median": (
            float(np.median(target_steps)) if len(target_steps) else math.nan
        ),
        "conditional_target_signals_mean": (
            float(np.mean(target_steps)) if len(target_steps) else math.nan
        ),
        "conditional_target_signals_p90": (
            float(np.quantile(target_steps, 0.90, method="higher"))
            if len(target_steps)
            else math.nan
        ),
        "conditional_target_signals_p99": (
            float(np.quantile(target_steps, 0.99, method="higher"))
            if len(target_steps)
            else math.nan
        ),
        "median_maximum_drawdown": float(np.median(maximum_drawdown)),
        "p95_maximum_drawdown": float(np.quantile(maximum_drawdown, 0.95, method="higher")),
    }


__all__ = ["estimate_first_passage", "moving_block_indices"]
