import numpy as np

from pumpquant.validation.first_passage import estimate_first_passage, moving_block_indices


def test_positive_surface_reaches_target_before_boundaries() -> None:
    indices = moving_block_indices(
        observation_count=4,
        paths=100,
        horizon_signals=20,
        block_length=2,
        seed=7,
    )
    result = estimate_first_passage(
        returns_by_balance=np.full((3, 4), 0.10),
        balance_nodes=np.asarray([0.25, 1.0, 2.0]),
        bootstrap_indices=indices,
        starting_balance=1.0,
        target_balance=2.0,
        half_loss_balance=0.5,
        ruin_balance=0.25,
    )

    assert result["target_before_50pct_loss_probability"] == 1.0
    assert result["target_before_25pct_ruin_probability"] == 1.0
    assert result["conditional_target_signals_median"] == 8.0
    assert result["conditional_target_signals_mean"] == 8.0
    assert result["conditional_target_signals_p99"] == 8.0


def test_negative_surface_hits_both_loss_boundaries_first() -> None:
    indices = np.zeros((20, 20), dtype=np.int32)
    result = estimate_first_passage(
        returns_by_balance=np.full((3, 1), -0.20),
        balance_nodes=np.asarray([0.20, 1.0, 2.0]),
        bootstrap_indices=indices,
        starting_balance=1.0,
        target_balance=2.0,
        half_loss_balance=0.5,
        ruin_balance=0.25,
    )

    assert result["hit_50pct_loss_first_probability"] == 1.0
    assert result["hit_25pct_ruin_first_probability"] == 1.0


def test_bracketing_uses_worse_exact_return() -> None:
    indices = np.zeros((1, 1), dtype=np.int32)
    result = estimate_first_passage(
        returns_by_balance=np.asarray([[1.1], [0.0], [1.1]]),
        balance_nodes=np.asarray([0.25, 1.0, 2.0]),
        bootstrap_indices=indices,
        starting_balance=1.0,
        target_balance=2.0,
        half_loss_balance=0.5,
        ruin_balance=0.25,
    )

    assert result["target_before_25pct_ruin_probability"] == 0.0
    assert result["unresolved_before_25pct_ruin_fraction"] == 1.0
