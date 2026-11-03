from pumpquant.execution.realism import (
    build_round_trip_templates,
    deterministic_template_index,
    stationary_block_indices,
    wilson_lower,
)


def _round_trip(mint: str, offset: int = 0) -> list[dict]:
    decision = 1_000_000_000 + offset
    trigger = 3_000_000_000 + offset
    return [
        {
            "event": "decision",
            "action": "enter",
            "mint": mint,
            "decision_time_ns": decision,
            "wall_time_ns": decision,
        },
        {
            "event": "transaction_ready",
            "side": "entry",
            "mint": mint,
            "attempt": 1,
            "wall_time_ns": decision + 100,
            "estimated_total_fee_lamports": 11_000,
            "selected_micro_lamports": 50_000,
            "localized_p75_micro_lamports": 25_000,
        },
        {
            "event": "transaction_landed",
            "side": "entry",
            "mint": mint,
            "attempt": 1,
            "wall_time_ns": decision + 500_000_000,
        },
        {"event": "exit_triggered", "side": "exit", "mint": mint, "wall_time_ns": trigger},
        {
            "event": "transaction_ready",
            "side": "exit",
            "mint": mint,
            "attempt": 1,
            "wall_time_ns": trigger + 100,
            "estimated_total_fee_lamports": 12_000,
            "selected_micro_lamports": 50_000,
            "localized_p75_micro_lamports": 25_000,
        },
        {
            "event": "transaction_landed",
            "side": "exit",
            "mint": mint,
            "attempt": 1,
            "wall_time_ns": trigger + 700_000_000,
        },
    ]


def test_builds_joint_round_trip_and_charges_ready_attempts() -> None:
    templates, support = build_round_trip_templates(_round_trip("mint-a"), minimum_pairs=1)
    assert support.ready_for_empirical_replay
    assert support.paired_round_trips == 1
    assert templates[0].entry.full_latency_ms == 500.0
    assert templates[0].exit.full_latency_ms == 700.0
    assert templates[0].entry.conservative_paid_fee_lamports == 11_000


def test_unlanded_entry_is_kept_in_availability_denominator() -> None:
    rows = _round_trip("mint-a")
    rows.append(
        {
            "event": "decision",
            "action": "enter",
            "mint": "miss",
            "decision_time_ns": 9,
            "wall_time_ns": 9,
        }
    )
    _, support = build_round_trip_templates(rows, minimum_pairs=1)
    assert support.eligible_entries == 2
    assert support.landed_entries == 1
    assert support.entry_landing_probability == 0.5
    assert support.entry_landing_wilson_lower_95 < 0.5


def test_deterministic_sampling_and_stationary_blocks() -> None:
    assert deterministic_template_index("x", 7, 5) == deterministic_template_index("x", 7, 5)
    values = stationary_block_indices(5, 20, mean_block_length=4, seed=11)
    assert values == stationary_block_indices(5, 20, mean_block_length=4, seed=11)
    assert len(values) == 20
    assert all(0 <= value < 5 for value in values)


def test_wilson_lower_is_conservative() -> None:
    assert 0 < wilson_lower(48, 56) < 48 / 56
