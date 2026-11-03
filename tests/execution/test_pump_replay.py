from __future__ import annotations

from pumpquant.execution.pump_replay import (
    PumpProfile,
    PumpReplayPath,
    PumpReplayState,
    conservative_available_at_ms,
    conservative_launch_decision_at_ms,
    simulate_pump_path,
    simulate_pump_surface,
)


def _state(second: int, order: int, *, quote: int = 30_000_000_000) -> PumpReplayState:
    return PumpReplayState(
        available_at_ms=conservative_available_at_ms(second),
        order=order,
        virtual_base_reserve=1_073_000_000_000_000,
        virtual_quote_reserve=quote,
        real_base_reserve=793_100_000_000_000,
        real_quote_reserve=max(0, quote - 30_000_000_000),
        protocol_fee_bps=100,
        creator_fee_bps=0,
        cashback_fee_bps=0,
    )


def test_strict_launch_clock_excludes_deadline_second() -> None:
    assert conservative_available_at_ms(100) == 101_000
    assert conservative_launch_decision_at_ms(100, 5) == 106_000
    assert conservative_available_at_ms(105) == 106_000


def test_no_checkpoint_state_is_a_costless_skip() -> None:
    path = PumpReplayPath(
        "mint",
        100,
        conservative_launch_decision_at_ms(100, 5),
        (_state(105, 0),),
    )
    result = simulate_pump_path(path, PumpProfile(1.5, 0.7, 60))
    assert result.reason == "no_causal_checkpoint_state"
    assert result.return_on_budget == 0


def test_surface_matches_individual_replays() -> None:
    states = (
        _state(100, 0),
        _state(104, 1, quote=31_000_000_000),
        _state(106, 2, quote=34_000_000_000),
        _state(110, 3, quote=38_000_000_000),
        _state(170, 4, quote=35_000_000_000),
    )
    path = PumpReplayPath(
        "mint",
        100,
        conservative_launch_decision_at_ms(100, 5),
        states,
    )
    profile = PumpProfile(1.25, 0.7, 60)
    surface = simulate_pump_surface(path, (profile,), tp_capture_probability=0.6)
    captured = simulate_pump_path(path, profile, capture_take_profit=True)
    missed = simulate_pump_path(path, profile, capture_take_profit=False)
    expected = 0.6 * captured.return_on_budget + 0.4 * missed.return_on_budget
    assert abs(float(surface[profile.profile_id]["expected_return"]) - expected) < 1e-12


def test_cashback_fee_route_is_executable() -> None:
    state = PumpReplayState(
        available_at_ms=101_000,
        order=0,
        virtual_base_reserve=1_073_000_000_000_000,
        virtual_quote_reserve=30_000_000_000,
        real_base_reserve=793_100_000_000_000,
        real_quote_reserve=1_000_000,
        protocol_fee_bps=100,
        creator_fee_bps=0,
        cashback_fee_bps=5,
    )
    assert state.creator_route_fee_bps == 5
