from __future__ import annotations

import pytest

from pumpquant.execution.velocity_risk import (
    RiskLimits,
    evaluate_sizing_grid,
    select_growth_optimum,
)


def test_growth_optimum_can_differ_from_risk_constrained_optimum() -> None:
    returns = {
        0.1: (0.02, 0.02, -0.01, 0.02, -0.01),
        0.8: (0.80, 0.80, -0.40, 0.80, -0.40),
    }
    limits = RiskLimits(
        ruin_equity_fraction=0.50,
        maximum_ruin_probability=0.0,
        maximum_p95_drawdown=0.30,
        bootstrap_paths=200,
        bootstrap_trades=100,
    )
    results = evaluate_sizing_grid(returns, trades_per_hour=2.0, limits=limits)

    assert select_growth_optimum(results, constrained=False).fraction == pytest.approx(0.8)
    assert select_growth_optimum(results, constrained=True).fraction == pytest.approx(0.1)


def test_hourly_growth_compounds_per_trade_log_growth() -> None:
    results = evaluate_sizing_grid(
        {0.2: (0.01, 0.01)},
        trades_per_hour=3.0,
        limits=RiskLimits(bootstrap_paths=10, bootstrap_trades=10),
    )

    assert results[0].geometric_multiplier_per_trade == pytest.approx(1.01)
    assert results[0].geometric_growth_per_hour == pytest.approx(1.01**3 - 1.0)


def test_invalid_bankrupting_return_is_rejected() -> None:
    with pytest.raises(ValueError, match="non-bankrupting"):
        evaluate_sizing_grid(
            {1.0: (0.1, -1.0)},
            trades_per_hour=1.0,
            limits=RiskLimits(bootstrap_paths=1, bootstrap_trades=1),
        )
