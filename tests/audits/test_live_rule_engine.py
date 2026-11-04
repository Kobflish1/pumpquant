from pumpquant.audits.live_rule.engine import LiveRuleEngine


def test_enters_at_exact_frozen_threshold() -> None:
    engine = LiveRuleEngine()
    engine.observe_pool(pool="p", mint="m", slot=1)
    engine.observe_trade(
        pool="p",
        event_time_ns=1_000_000_000,
        post_quote_lamports=908_108_808,
        cashback_fee_bps=0,
        slot=2,
    )
    decisions = engine.due_decisions(9_000_000_000)
    assert len(decisions) == 1
    assert decisions[0].action == "enter"


def test_uses_last_causal_state_and_rejects_cashback() -> None:
    engine = LiveRuleEngine()
    engine.observe_pool(pool="p", mint="m", slot=1)
    engine.observe_trade(
        pool="p",
        event_time_ns=1_000_000_000,
        post_quote_lamports=700_000_000,
        cashback_fee_bps=0,
        slot=2,
    )
    engine.observe_trade(
        pool="p",
        event_time_ns=8_000_000_000,
        post_quote_lamports=800_000_000,
        cashback_fee_bps=5,
        slot=3,
    )
    engine.observe_trade(
        pool="p",
        event_time_ns=10_000_000_000,
        post_quote_lamports=1,
        cashback_fee_bps=0,
        slot=4,
    )
    decision = engine.due_decisions(9_000_000_000)[0]
    assert decision.action == "skip"
    assert decision.reason == "cashback"
    assert decision.quote_liquidity_sol == 0.8


def test_fails_closed_for_stale_or_unknown_pool() -> None:
    engine = LiveRuleEngine(maximum_decision_lateness_ms=100)
    engine.observe_trade(
        pool="unknown",
        event_time_ns=1,
        post_quote_lamports=1,
        cashback_fee_bps=0,
        slot=1,
    )
    assert engine.due_decisions(10_000_000_000) == ()
    engine.observe_pool(pool="p", mint="m", slot=1)
    engine.observe_trade(
        pool="p",
        event_time_ns=1_000_000_000,
        post_quote_lamports=1,
        cashback_fee_bps=0,
        slot=2,
    )
    decision = engine.due_decisions(9_200_000_000)[0]
    assert decision.action == "skip"
    assert decision.reason == "decision_stale"


def test_duplicate_pool_decision_is_impossible() -> None:
    engine = LiveRuleEngine()
    engine.observe_pool(pool="p", mint="m", slot=1)
    engine.observe_trade(
        pool="p",
        event_time_ns=1_000_000_000,
        post_quote_lamports=1,
        cashback_fee_bps=0,
        slot=2,
    )
    assert len(engine.due_decisions(9_000_000_000)) == 1
    assert engine.due_decisions(10_000_000_000) == ()


def test_nonzero_virtual_quote_reserve_fails_closed() -> None:
    engine = LiveRuleEngine()
    engine.observe_pool(pool="p", mint="m", slot=1)
    engine.observe_trade(
        pool="p",
        event_time_ns=1_000_000_000,
        post_quote_lamports=1,
        cashback_fee_bps=0,
        virtual_quote_reserves=1,
        slot=2,
    )
    decision = engine.due_decisions(9_000_000_000)[0]
    assert decision.action == "skip"
    assert decision.reason == "unmodeled_virtual_quote_reserves"


def test_stair_two_parameters_use_five_second_liquidity_rule() -> None:
    engine = LiveRuleEngine(
        checkpoint_seconds=5,
        maximum_quote_liquidity_sol=1.459006092,
    )
    engine.observe_pool(pool="p", mint="m", slot=1)
    engine.observe_trade(
        pool="p",
        event_time_ns=1_000_000_000,
        post_quote_lamports=1_459_006_092,
        cashback_fee_bps=0,
        slot=2,
    )
    decision = engine.due_decisions(6_000_000_000)[0]
    assert decision.action == "enter"
    assert decision.quote_liquidity_sol == 1.459006092


def test_checkpoint_uses_causal_arrival_time_not_second_timestamp() -> None:
    engine = LiveRuleEngine(
        checkpoint_seconds=5,
        maximum_quote_liquidity_sol=1.0,
    )
    engine.observe_pool(pool="p", mint="m", slot=1)
    engine.observe_trade(
        pool="p",
        event_time_ns=1_000_000_000,
        observed_time_ns=1_800_000_000,
        post_quote_lamports=900_000_000,
        cashback_fee_bps=0,
        slot=2,
    )
    engine.observe_trade(
        pool="p",
        event_time_ns=6_000_000_000,
        observed_time_ns=6_500_000_000,
        post_quote_lamports=800_000_000,
        cashback_fee_bps=0,
        slot=3,
    )
    assert engine.due_decisions(6_000_000_000) == ()
    decision = engine.due_decisions(6_800_000_000)[0]
    assert decision.action == "enter"
    assert decision.quote_liquidity_sol == 0.8
    assert decision.decision_time_ns == 6_800_000_000
    assert decision.first_trade_event_time_ns == 1_000_000_000
    assert decision.first_trade_observed_time_ns == 1_800_000_000
    assert decision.source_event_time_ns == 6_000_000_000


def test_trade_arriving_after_causal_checkpoint_cannot_change_decision() -> None:
    engine = LiveRuleEngine(
        checkpoint_seconds=5,
        maximum_quote_liquidity_sol=1.0,
    )
    engine.observe_pool(pool="p", mint="m", slot=1)
    engine.observe_trade(
        pool="p",
        event_time_ns=1_000_000_000,
        observed_time_ns=1_200_000_000,
        post_quote_lamports=900_000_000,
        cashback_fee_bps=0,
        slot=2,
    )
    engine.observe_trade(
        pool="p",
        event_time_ns=6_000_000_000,
        observed_time_ns=6_300_000_000,
        post_quote_lamports=2_000_000_000,
        cashback_fee_bps=0,
        slot=3,
    )
    decision = engine.due_decisions(6_300_000_000)[0]
    assert decision.action == "enter"
    assert decision.quote_liquidity_sol == 0.9
