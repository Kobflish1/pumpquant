from __future__ import annotations

from pumpquant.audits.m4.replay import (
    ExecutionTemplate,
    ReplayPath,
    ReplayState,
    conservative_available_at_ms,
    conservative_decision_at_ms,
    simulate_path,
)


def _state(second: int, quote: int, order: int = 0) -> ReplayState:
    return ReplayState(
        available_at_ms=conservative_available_at_ms(second),
        slot=second,
        order=order,
        base_reserve=1_000_000_000_000_000,
        quote_reserve=quote,
        protocol_fee_bps=5,
        lp_fee_bps=20,
        creator_fee_bps=30,
    )


def _template(outcome: str = "success") -> ExecutionTemplate:
    return ExecutionTemplate(
        template_id=0,
        entry_outcome=outcome,
        entry_latency_ms=500,
        exit_latency_ms=500,
        entry_attempts=1,
        exit_attempts=1,
        actual_fees_lamports=23_000,
        submitted_transactions=2,
    )


def test_conservative_checkpoint_excludes_entire_boundary_second() -> None:
    first = 1_000
    decision = conservative_decision_at_ms(first, 5)
    assert decision == 1_006_000
    assert conservative_available_at_ms(first + 4) < decision
    assert conservative_available_at_ms(first + 5) == decision


def test_replay_state_adds_virtual_quote_to_effective_reserve() -> None:
    state = ReplayState(
        available_at_ms=1,
        slot=1,
        order=0,
        base_reserve=1_000,
        quote_reserve=2_000,
        virtual_quote_reserve=3_000,
        protocol_fee_bps=5,
        lp_fee_bps=20,
        creator_fee_bps=30,
    )
    assert state.effective_quote_reserve == 5_000


def test_fee_gate_skip_is_an_explicit_paid_outcome() -> None:
    path = ReplayPath(
        mint="mint",
        first_block_time=1_000,
        decision_at_ms=1_006_000,
        fx_usd=100.0,
        checkpoint_quote_lamports=1_000_000_000,
        states=(_state(1_000, 1_000_000_000), _state(1_004, 1_000_000_000)),
    )
    result = simulate_path(path, _template("fee_gate_skip"))
    assert result.fee_gate_skipped
    assert not result.entered
    assert result.net_pnl_lamports == -23_000


def test_timeout_is_measured_from_decision_not_entry() -> None:
    first = 1_000
    decision = conservative_decision_at_ms(first, 5)
    states = tuple(
        [_state(first, 1_000_000_000), _state(first + 4, 1_000_000_000)]
        + [_state(first + offset, 1_000_000_000) for offset in range(6, 330)]
    )
    path = ReplayPath(
        mint="mint",
        first_block_time=first,
        decision_at_ms=decision,
        fx_usd=100.0,
        checkpoint_quote_lamports=1_000_000_000,
        states=states,
    )
    result = simulate_path(path, _template())
    assert result.entered
    assert result.closed
    assert result.reason == "timeout"
    assert result.exit_time_ms is not None
    assert result.exit_time_ms < result.entry_time_ms + 301_000  # type: ignore[operator]


def test_entry_retry_ceiling_is_eight() -> None:
    first = 1_000
    decision = conservative_decision_at_ms(first, 5)
    # The pool collapses before every landing, so the built exact-base quote
    # always exceeds the fixed 5% guard.
    states = tuple(
        [_state(first, 1_000_000_000), _state(first + 4, 1_000_000_000)]
        + [_state(first + 5 + offset, 1_000_000_000 * 2**offset) for offset in range(20)]
    )
    path = ReplayPath(
        mint="mint",
        first_block_time=first,
        decision_at_ms=decision,
        fx_usd=100.0,
        checkpoint_quote_lamports=1_000_000_000,
        states=states,
    )
    template = ExecutionTemplate(
        template_id=0,
        entry_outcome="success",
        entry_latency_ms=1_500,
        exit_latency_ms=500,
        entry_attempts=1,
        exit_attempts=1,
        actual_fees_lamports=23_000,
        submitted_transactions=2,
    )
    result = simulate_path(path, template)
    assert not result.entered
    assert result.entry_attempts == 8
    assert result.reason == "entry_unfilled_after_eight_attempts"
