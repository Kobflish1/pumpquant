from pumpquant.validation.strategy_diagnostics import validate_fixed_strategy


def _rows(values: list[float]) -> list[dict[str, object]]:
    return [
        {
            "mint": f"mint-{index:03d}",
            "signal_time_ns": (index + 1) * 1_000_000_000,
            "account_return": value * 0.02,
            "net_return_r": value,
            "entered": True,
            "closed": True,
        }
        for index, value in enumerate(values)
    ]


def test_validation_reports_stability_and_concentration() -> None:
    result = validate_fixed_strategy(
        _rows([0.2] * 64),
        bootstrap_replicates=200,
        placebo_replicates=200,
        seed=7,
    )

    assert result["expectancy_r"] == 0.2
    assert result["positive_chronological_blocks"] == 8
    assert result["moving_block_bootstrap"]["lower_95_expectancy_r"] > 0.0
    assert result["leave_best_signals_out_expectancy_r"]["3"] > 0.0
    assert result["tests"]["minimum_30_fills"]
    assert result["observed_sequential_equity"]["ending_multiplier"] > 1.0


def test_validation_rejects_unstable_alternating_blocks() -> None:
    values = [0.3] * 8 + [-0.4] * 8
    result = validate_fixed_strategy(
        _rows(values * 4),
        bootstrap_replicates=200,
        placebo_replicates=200,
        seed=11,
    )

    assert result["positive_chronological_blocks"] == 4
    assert not result["tests"]["at_least_6_of_8_positive_chronological_blocks"]
    assert not result["all_tests_pass"]
