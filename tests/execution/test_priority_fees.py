from pumpquant.execution.priority_fees import select_priority_fee


def _samples(side: str, floor: int, latency: float, count: int = 10) -> list[dict]:
    return [
        {
            "side": side,
            "bid_floor_micro_lamports": floor,
            "localized_p75_micro_lamports": 25_000,
            "attempt_to_processed_ms": latency + index,
        }
        for index in range(count)
    ]


def test_selects_cheapest_arm_meeting_tail_latency() -> None:
    samples = [
        *_samples("entry", 1_000, 2_000),
        *_samples("entry", 50_000, 600),
        *_samples("entry", 100_000, 500),
    ]
    decision = select_priority_fee(
        samples,
        side="entry",
        current_localized_p75_micro_lamports=25_000,
        compute_unit_limit=150_000,
    )
    assert decision.action == "submit"
    assert decision.selected_bid_micro_lamports == 50_000
    assert decision.estimated_total_fee_lamports == 12_500


def test_skips_entry_outside_calibrated_context() -> None:
    decision = select_priority_fee(
        _samples("entry", 50_000, 600),
        side="entry",
        current_localized_p75_micro_lamports=60_000,
        compute_unit_limit=150_000,
    )
    assert decision.action == "skip"


def test_exit_can_choose_best_effort_tail_arm() -> None:
    samples = [
        *_samples("exit", 1_000, 1_400),
        *_samples("exit", 50_000, 700),
    ]
    decision = select_priority_fee(
        samples,
        side="exit",
        current_localized_p75_micro_lamports=25_000,
        compute_unit_limit=110_000,
        latency_sla_ms=500,
        allow_best_effort=True,
    )
    assert decision.action == "submit"
    assert decision.selected_bid_micro_lamports == 50_000
