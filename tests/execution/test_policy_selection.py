from pumpquant.execution.policy_selection import (
    JointActionOutcome,
    JointExecutionAction,
    JointOptimizerContract,
    select_joint_profit_action,
)


def _action(action_id: str, side: str, fee: int) -> JointExecutionAction:
    return JointExecutionAction(
        action_id=action_id,
        side=side,
        priority_micro_lamports_per_cu=fee,
        compute_unit_limit=100_000,
        route="dual",
        maximum_attempts=3,
        retry_delay_ms=75,
        slippage_bps=500,
        order_fraction=0.02,
        maximum_concurrency=2,
    )


def _outcomes(
    action_id: str, values: list[int], landed: int | None = None
) -> list[JointActionOutcome]:
    landed = len(values) if landed is None else landed
    return [
        JointActionOutcome(
            action_id=action_id,
            chronological_index=index,
            regime="quiet" if index % 2 == 0 else "busy",
            net_pnl_lamports=value,
            fee_lamports=10_000,
            latency_ms=500 + index,
            landed=index < landed,
            closed=index < landed,
        )
        for index, value in enumerate(values)
    ]


def _contract(**overrides: object) -> JointOptimizerContract:
    values = dict(
        wallet_lamports=50_000_000,
        minimum_samples=10,
        minimum_regimes=2,
        bootstrap_repetitions=100,
        bootstrap_mean_block_length=2,
    )
    values.update(overrides)
    return JointOptimizerContract(**values)


def test_entry_selects_higher_fee_only_when_net_outcomes_are_better() -> None:
    cheap = _action("cheap", "entry", 10_000)
    fast = _action("fast", "entry", 100_000)
    outcomes = [
        *_outcomes("cheap", [10_000] * 20),
        *_outcomes("fast", [40_000] * 20),
    ]
    decision = select_joint_profit_action([cheap, fast], outcomes, _contract(), side="entry")
    assert decision.action == "submit"
    assert decision.selected_action_id == "fast"


def test_entry_skips_when_fees_and_failures_destroy_edge() -> None:
    action = _action("bad", "entry", 100_000)
    decision = select_joint_profit_action(
        [action], _outcomes("bad", [-20_000] * 20, landed=8), _contract(), side="entry"
    )
    assert decision.action == "skip"
    assert decision.selected_action_id is None


def test_one_large_winner_cannot_authorize_entry() -> None:
    action = _action("tail", "entry", 50_000)
    values = [-50_000] * 19 + [2_000_000]
    decision = select_joint_profit_action(
        [action], _outcomes("tail", values), _contract(), side="entry"
    )
    assert decision.action == "skip"
    assert (
        "largest_winner_removed_expectancy_not_positive"
        in decision.evaluations[0].rejection_reasons
    )


def test_exit_selects_least_bad_action_instead_of_skipping() -> None:
    slow = _action("slow", "exit", 10_000)
    fast = _action("fast", "exit", 100_000)
    outcomes = [
        *_outcomes("slow", [-200_000] * 20),
        *_outcomes("fast", [-50_000] * 20),
    ]
    decision = select_joint_profit_action(
        [slow, fast], outcomes, _contract(minimum_exit_landing_lower_95=0.0), side="exit"
    )
    assert decision.action == "submit"
    assert decision.selected_action_id == "fast"


def test_unsupported_action_cannot_win_entry() -> None:
    supported = _action("supported", "entry", 10_000)
    sparse = _action("sparse", "entry", 50_000)
    outcomes = [
        *_outcomes("supported", [10_000] * 20),
        *_outcomes("sparse", [1_000_000] * 4),
    ]
    decision = select_joint_profit_action([supported, sparse], outcomes, _contract(), side="entry")
    assert decision.selected_action_id == "supported"
    sparse_eval = next(item for item in decision.evaluations if item.action.action_id == "sparse")
    assert not sparse_eval.eligible
