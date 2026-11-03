from __future__ import annotations

import pytest

from pumpquant.validation.goal import (
    DecisionContext,
    ExactOutcome,
    ExactOutcomeCube,
    Goal,
    PolicyDecision,
    TimedObservation,
    ValidationWindow,
    consecutive_windows,
    hansen_spa,
    purged_walk_forward_folds,
    simulate_exact_window,
    simulate_exact_window_policy,
    validate_exact_windows,
)


def outcome(signal: int, budget: int, scenario: str, pnl: int) -> ExactOutcome:
    decision = signal * 1_000
    return ExactOutcome(
        signal_id=f"s{signal}",
        mint=f"m{signal}",
        decision_time_ms=decision,
        budget_lamports=budget,
        scenario=scenario,
        net_pnl_lamports=pnl,
        entry_landing_time_ms=decision + 10,
        exit_landing_time_ms=decision + 100,
        capital_release_time_ms=decision + 100,
    )


def test_consecutive_windows_never_extend_or_repeat_data() -> None:
    assert consecutive_windows(0, 95 * 3_600_000, 96) == ()
    windows = consecutive_windows(0, 192 * 3_600_000, 96)
    assert [(x.start_time_ms, x.end_time_ms) for x in windows] == [
        (0, 96 * 3_600_000),
        (96 * 3_600_000, 192 * 3_600_000),
    ]


def test_walk_forward_purges_overlapping_labels_and_future_rows() -> None:
    window = ValidationWindow(1_000, 2_000)
    observations = [
        TimedObservation("clean_train", 100, 800),
        TimedObservation("overlap", 200, 950),
        TimedObservation("test", 1_100, 1_500),
        TimedObservation("unresolved_test", 1_200, 2_100),
        TimedObservation("future", 2_100, 2_200),
    ]
    folds = purged_walk_forward_folds(
        observations,
        [window],
        embargo_ms=100,
        minimum_train_observations=1,
        minimum_test_observations=1,
    )
    assert len(folds) == 1
    assert folds[0].train_observation_ids == ("clean_train",)
    assert folds[0].test_observation_ids == ("test",)


def test_cube_rejects_missing_budget_or_scenario_rows() -> None:
    with pytest.raises(ValueError, match="incomplete outcome cube"):
        ExactOutcomeCube(
            [
                outcome(1, 100, "base", 1),
                outcome(2, 100, "base", 1),
                outcome(1, 200, "base", 1),
            ]
        )


def test_exact_budget_lookup_does_not_scale_pnl() -> None:
    cube = ExactOutcomeCube(
        [
            outcome(1, budget, scenario, pnl)
            for scenario in ("base", "stress")
            for budget, pnl in ((100, 10), (200, 15))
        ]
    )
    goal = Goal(1_000, 2_000, 100, 1, 0.25)
    window = ValidationWindow(0, 3_600_000)
    result = simulate_exact_window(cube, goal, window, "base")
    assert result.ending_wallet_lamports == 1_015


def test_all_execution_scenarios_are_kept_separate() -> None:
    cube = ExactOutcomeCube(
        [
            outcome(1, 100, "base", 1_500),
            outcome(1, 100, "stress", -950),
        ]
    )
    goal = Goal(1_000, 2_000, 100, 1, 0.2)
    result = validate_exact_windows(cube, goal, [ValidationWindow(0, 3_600_000)])
    assert result["scenario_success_rates"] == {"base": 1.0, "stress": 0.0}
    assert result["worst_scenario_success_rate"] == 0.0


def test_wrong_horizon_is_rejected() -> None:
    cube = ExactOutcomeCube([outcome(1, 100, "base", 10)])
    goal = Goal(1_000, 2_000, 100, 96, 0.2)
    with pytest.raises(ValueError, match="declared goal horizon"):
        simulate_exact_window(cube, goal, ValidationWindow(0, 24 * 3_600_000), "base")


def test_profit_released_after_horizon_is_not_counted() -> None:
    late = outcome(1, 100, "base", 10_000)
    late = ExactOutcome(
        signal_id=late.signal_id,
        mint=late.mint,
        decision_time_ms=late.decision_time_ms,
        budget_lamports=late.budget_lamports,
        scenario=late.scenario,
        net_pnl_lamports=late.net_pnl_lamports,
        entry_landing_time_ms=late.entry_landing_time_ms,
        exit_landing_time_ms=4_000_000,
        capital_release_time_ms=4_000_000,
    )
    result = simulate_exact_window(
        ExactOutcomeCube([late]),
        Goal(1_000, 2_000, 100, 1, 0.2),
        ValidationWindow(0, 3_600_000),
        "base",
    )
    assert result.ending_wallet_lamports == 1_000
    assert result.reached_target is False
    assert result.unresolved_position_at_horizon is True


def test_direct_goal_policy_uses_wallet_and_time_without_stairs() -> None:
    cube = ExactOutcomeCube(
        [
            outcome(signal, budget, "base", pnl)
            for signal, pnl in ((1, 100), (2, 900))
            for budget in (100, 500)
        ]
    )
    contexts: list[DecisionContext] = []

    def policy(context: DecisionContext) -> PolicyDecision:
        contexts.append(context)
        if context.signal_id == "s1":
            return PolicyDecision(None)
        return PolicyDecision(max(context.available_budgets_lamports))

    result = simulate_exact_window_policy(
        cube,
        Goal(1_000, 1_800, 100, 1),
        ValidationWindow(0, 3_600_000),
        "base",
        policy,
    )
    assert result.reached_target is True
    assert result.trades == 1
    assert contexts[0].wallet_lamports == 1_000
    assert contexts[1].remaining_time_ms == 3_598_000


def test_direct_goal_policy_rejects_unreplayed_budget() -> None:
    cube = ExactOutcomeCube([outcome(1, 100, "base", 10)])
    with pytest.raises(ValueError, match="unavailable exact budget"):
        simulate_exact_window_policy(
            cube,
            Goal(1_000, 2_000, 100, 1),
            ValidationWindow(0, 3_600_000),
            "base",
            lambda _: PolicyDecision(200),
        )


def test_spa_reports_the_complete_candidate_family() -> None:
    benchmark = [0.0] * 20
    result = hansen_spa(
        benchmark,
        {
            "better": [-1.2, -0.8] * 10,
            "same": [-0.1, 0.1] * 10,
        },
        bootstrap_repetitions=100,
        seed=7,
    )
    assert result["candidate_count"] == 2
    assert result["observations"] == 20
    assert set(result["pvalues"]) == {"lower", "consistent", "upper"}
