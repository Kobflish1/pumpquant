"""The synthetic end-to-end demo: generate tokens, search a rule, test it on unseen data.

The data is generated, so the result shows how the method works, never whether a
strategy makes money. ``pumpquant demo`` and the dashboard preview both run this.
"""

from __future__ import annotations

from dataclasses import dataclass

from pumpquant.core.schema import Bar
from pumpquant.core.synthetic import generate_synthetic_bars
from pumpquant.execution.bar_replay import ExecutionConfig, MomentumSpec
from pumpquant.research.search import SearchReport, momentum_grid, walk_forward_search


@dataclass(frozen=True, slots=True)
class DemoRun:
    bars: list[Bar]
    report: SearchReport


def run_demo(
    *,
    tokens: int = 36,
    bars_per_token: int = 30,
    seed: int = 7,
    config: ExecutionConfig | None = None,
) -> DemoRun:
    """Search 16 momentum variants with chronological walk-forward folds."""
    bars = generate_synthetic_bars(tokens=tokens, bars_per_token=bars_per_token, seed=seed)
    template = MomentumSpec(
        volume_lookback=3,
        min_liquidity_quote=0.0,
        min_trade_count=2,
        min_age_seconds=2.0,
        max_age_seconds=float(bars_per_token),
        exit_momentum=-0.03,
        trailing_stop=0.14,
        max_holding_bars=8,
    )
    candidates = momentum_grid(
        momentum_lookbacks=(2, 4),
        entry_momenta=(0.03, 0.07),
        entry_imbalances=(0.0, 0.25),
        take_profits=(0.15, 0.30),
        stop_losses=(0.10,),
        template=template,
    )
    # Train on at least half the tokens; each fold then tests the next sixth.
    min_train = max(4, tokens // 2)
    report = walk_forward_search(
        bars,
        candidates,
        config or ExecutionConfig(random_seed=seed),
        min_train_tokens=min_train,
        test_tokens=max(2, tokens // 6),
        minimum_train_positions=max(3, min_train // 3),
    )
    return DemoRun(bars=bars, report=report)
