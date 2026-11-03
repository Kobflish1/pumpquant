from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from pumpquant.validation.capacity_gate import (
    DAY_NS,
    STARTING_CAPITAL_LADDER_USD,
    CapacityGateError,
    CausalFxPoint,
    CausalFxSeries,
    ChainKey,
    CompleteUtcDay,
    FourDayAttribution,
    LadderProof,
    LadderRung,
    RealizedExit,
    TradeSide,
    Venue,
    WalletTrade,
    assemble_capacity_report,
    attribute_realized_net,
    deterministic_report_json,
    earliest_four_complete_days,
    evaluate_starting_capital_ladder,
    real_inventory_capacity,
    replay_shared_wallet,
    validate_notional_ladder,
)


def _fx(*, price: str = "100") -> CausalFxSeries:
    return CausalFxSeries(
        tuple(CausalFxPoint(index * DAY_NS, Decimal(price)) for index in range(6))
    )


def _trade(
    trade_id: str,
    mint: str,
    entry_day: int,
    exit_day: int,
    *,
    entry: int = 20_000_000,
    exit: int = 21_000_000,
    order: int | None = None,
) -> WalletTrade:
    chain_order = 0 if order is None else order
    return WalletTrade(
        trade_id=trade_id,
        mint=mint,
        entry_available_at_ns=entry_day * DAY_NS + 1,
        exit_available_at_ns=exit_day * DAY_NS + 1,
        entry_quote_atomic=entry,
        exit_quote_atomic=exit,
        entry_chain_key=ChainKey(entry_day, chain_order, 0, 0, f"{trade_id}:entry"),
        exit_chain_key=ChainKey(exit_day, chain_order, 0, 0, f"{trade_id}:exit"),
    )


def test_ladder_is_frozen_integer_and_strictly_increasing() -> None:
    assert STARTING_CAPITAL_LADDER_USD[0] == 10
    assert STARTING_CAPITAL_LADDER_USD[-1] == 10_000_000
    assert validate_notional_ladder(STARTING_CAPITAL_LADDER_USD)
    with pytest.raises(CapacityGateError, match="integer"):
        validate_notional_ladder((10, True))
    with pytest.raises(CapacityGateError, match="increasing"):
        validate_notional_ladder((10, 10))
    with pytest.raises(CapacityGateError, match="integer"):
        validate_notional_ladder((10, Decimal("25")))


def test_causal_fx_rejects_future_and_float_money() -> None:
    fx = _fx()
    with pytest.raises(CapacityGateError, match="unavailable"):
        CausalFxSeries(
            tuple(CausalFxPoint((index + 1) * DAY_NS, Decimal("100")) for index in range(6))
        ).price_at(0)
    with pytest.raises(CapacityGateError, match="Decimal"):
        CausalFxPoint(0, 1.5)  # type: ignore[arg-type]
    assert fx.atomic_from_usd(Decimal("10"), 1) == 100_000_000
    assert fx.usd_from_atomic(-1_000_000, DAY_NS) == Decimal("-0.1")


def test_exactly_four_earliest_complete_contiguous_utc_days() -> None:
    selected = earliest_four_complete_days(1, 5 * DAY_NS)
    assert selected == (DAY_NS, 2 * DAY_NS, 3 * DAY_NS, 4 * DAY_NS)
    with pytest.raises(CapacityGateError, match="four"):
        earliest_four_complete_days(0, 4 * DAY_NS - 1)
    with pytest.raises(CapacityGateError, match="missing"):
        earliest_four_complete_days(
            0,
            4 * DAY_NS,
            complete_days=(CompleteUtcDay(0), CompleteUtcDay(DAY_NS)),
        )


def test_realized_net_is_causally_converted_by_exit_utc_day() -> None:
    exits = (
        RealizedExit("a", DAY_NS + 1, 100_000_000),
        RealizedExit("b", 2 * DAY_NS + 1, -20_000_000),
    )
    result = attribute_realized_net(
        exits,
        _fx(),
        availability_start_ns=0,
        availability_end_ns=4 * DAY_NS,
    )
    assert result.days[1].net_usd == Decimal("10")
    assert result.days[2].net_usd == Decimal("-2")
    assert result.days[0].exit_count == result.days[3].exit_count == 0
    with pytest.raises(CapacityGateError, match="outside"):
        attribute_realized_net(
            (RealizedExit("late", 4 * DAY_NS + 1, 1),),
            _fx(),
            availability_start_ns=0,
            availability_end_ns=4 * DAY_NS,
        )


def test_four_day_target_requires_every_day_not_total_only() -> None:
    days = tuple(
        # A total of $80k is insufficient when one day is below $20k.
        _day(day, Decimal("20000") if day != 2 else Decimal("19999"))
        for day in range(4)
    )
    result = FourDayAttribution(days)
    assert result.total_net_usd == Decimal("79999")
    assert not result.passed


def _day(day: int, net: Decimal):
    from pumpquant.validation.capacity_gate import DailyAttribution

    return DailyAttribution(day * DAY_NS, net, 1)


def test_orientation_correct_real_inventory_predicates_and_explicit_reason() -> None:
    normal_buy = real_inventory_capacity(
        venue=Venue.PUMPSWAP,
        side=TradeSide.BUY,
        base_amount_atomic=11,
        quote_amount_atomic=1001,
        real_base_inventory_atomic=10,
        real_quote_inventory_atomic=1000,
    )
    assert not normal_buy.accepted
    assert normal_buy.reason == "real_inventory_capacity"
    flipped_buy = real_inventory_capacity(
        venue=Venue.PUMPSWAP,
        side=TradeSide.BUY,
        base_amount_atomic=11,
        quote_amount_atomic=100,
        real_base_inventory_atomic=11,
        real_quote_inventory_atomic=100,
        pool_orientation_flipped=True,
    )
    assert flipped_buy.accepted  # canonical token reserve remains pre_real_base
    flipped_sell = real_inventory_capacity(
        venue="pumpswap",
        side="sell",
        base_amount_atomic=11,
        quote_amount_atomic=1001,
        real_base_inventory_atomic=10,
        real_quote_inventory_atomic=1000,
        pool_orientation_flipped=True,
    )
    assert not flipped_sell.accepted
    with pytest.raises(CapacityGateError, match="flipped"):
        real_inventory_capacity(
            venue="pump",
            side="buy",
            base_amount_atomic=1,
            quote_amount_atomic=1,
            real_base_inventory_atomic=1,
            real_quote_inventory_atomic=1,
            pool_orientation_flipped=True,
        )


def test_shared_wallet_is_chronological_cash_constrained_and_single_mint() -> None:
    trades = (
        _trade("a", "M", 0, 2),
        _trade("b", "M", 1, 3, order=0),
        _trade("c", "N", 1, 3, order=1),
    )
    replay = replay_shared_wallet(
        trades,
        _fx(),
        starting_usd=Decimal("10"),
        start_available_at_ns=0,
    )
    assert [decision.reason for decision in replay.decisions] == [
        "accepted",
        "mint_already_open",
        "accepted",
    ]
    assert replay.maximum_concurrent_positions == 2
    assert replay.final_usd == Decimal("10.2")
    assert replay.passed


def test_ladder_does_not_report_maximum_without_proof_and_report_is_locked() -> None:
    def factory(seed: Decimal, d0: int):
        amount = int(seed) * 1_000_000
        return replay_shared_wallet(
            (_trade("a", "M", 0, 1, entry=amount, exit=amount + 1_000_000),),
            _fx(),
            starting_usd=seed,
            start_available_at_ns=d0,
        )

    result = evaluate_starting_capital_ladder(
        None,
        _fx(),
        availability_start_ns=0,
        availability_end_ns=4 * DAY_NS,
        replay_factory=factory,
    )
    assert not result.search_complete
    assert result.maximum_passing_usd is None
    report = assemble_capacity_report(result)
    assert report["development_only"] is True
    assert report["holdout_accessed"] is False
    assert report["holdout_unlocked"] is False
    assert "maximum_passing_usd" not in report
    assert "proof" not in report
    with pytest.raises(CapacityGateError, match="holdout"):
        assemble_capacity_report(result, holdout_accessed=True)


def test_explicit_exhaustive_proof_allows_maximum_and_missing_dev_blocks() -> None:
    proof = LadderProof(exhaustive=True, statement="all integer seeds in the declared bound")
    with pytest.raises(CapacityGateError, match="finite"):
        evaluate_starting_capital_ladder(
            None,
            _fx(),
            availability_start_ns=0,
            availability_end_ns=4 * DAY_NS,
            proof=proof,
            replay_factory=lambda seed, d0: replay_shared_wallet(
                (_trade("a", "M", 0, 1),),
                _fx(),
                starting_usd=seed,
                start_available_at_ns=d0,
            ),
        )


def test_ladder_requires_per_rung_requote_and_uses_d0_seed() -> None:
    fx = CausalFxSeries(
        (
            CausalFxPoint(0, Decimal("100")),
            CausalFxPoint(DAY_NS, Decimal("200")),
            CausalFxPoint(2 * DAY_NS, Decimal("200")),
            CausalFxPoint(3 * DAY_NS, Decimal("200")),
            CausalFxPoint(4 * DAY_NS, Decimal("200")),
        )
    )
    seen: list[tuple[Decimal, int]] = []

    def factory(seed: Decimal, d0: int):
        seen.append((seed, d0))
        amount = int(seed) * 1_000_000
        return replay_shared_wallet(
            (_trade("a", "M", 1, 2, entry=amount, exit=amount + 1),),
            fx,
            starting_usd=seed,
            start_available_at_ns=d0,
        )

    result = evaluate_starting_capital_ladder(
        None,
        fx,
        availability_start_ns=1,
        availability_end_ns=5 * DAY_NS,
        replay_factory=factory,
    )
    assert seen == [(Decimal(value), DAY_NS) for value in STARTING_CAPITAL_LADDER_USD]
    assert [r.wallet.accepted_trades[0].entry_quote_atomic for r in result.rungs] == [
        value * 1_000_000 for value in STARTING_CAPITAL_LADDER_USD
    ]

    with pytest.raises(CapacityGateError, match="replay factory"):
        evaluate_starting_capital_ladder(
            (_trade("a", "M", 0, 1),),
            fx,
            availability_start_ns=0,
            availability_end_ns=4 * DAY_NS,
        )


def test_same_timestamp_entries_require_chain_keys() -> None:
    with pytest.raises(CapacityGateError, match="chain keys"):
        replay_shared_wallet(
            (
                {
                    "trade_id": "a",
                    "mint": "M",
                    "entry_available_at_ns": 1,
                    "exit_available_at_ns": DAY_NS + 1,
                    "entry_quote_atomic": 20_000_000,
                    "exit_quote_atomic": 21_000_000,
                },
                {
                    "trade_id": "b",
                    "mint": "N",
                    "entry_available_at_ns": 1,
                    "exit_available_at_ns": DAY_NS + 1,
                    "entry_quote_atomic": 20_000_000,
                    "exit_quote_atomic": 21_000_000,
                },
            ),
            _fx(),
            starting_usd=Decimal("100"),
            start_available_at_ns=0,
        )


def test_chain_key_settles_exit_before_equal_timestamp_entry() -> None:
    replay = replay_shared_wallet(
        (
            _trade("a", "M", 0, 1, order=0),
            _trade("b", "M", 1, 2, order=1),
        ),
        _fx(),
        starting_usd=Decimal("10"),
        start_available_at_ns=0,
    )
    assert [item.reason for item in replay.decisions] == ["accepted", "accepted"]


def test_ladder_rejects_callback_policy_or_window_tampering() -> None:
    def wrong_policy(seed: Decimal, d0: int):
        return replay_shared_wallet(
            (_trade("a", "M", 0, 1),),
            _fx(),
            starting_usd=seed,
            start_available_at_ns=d0,
            allocation_percent=10,
        )

    with pytest.raises(CapacityGateError, match="policy"):
        evaluate_starting_capital_ladder(
            None,
            _fx(),
            availability_start_ns=0,
            availability_end_ns=4 * DAY_NS,
            replay_factory=wrong_policy,
        )


def test_report_json_rejects_float_money() -> None:
    with pytest.raises(CapacityGateError, match="floating-point"):
        deterministic_report_json({"net_usd": 1.0})


def test_capacity_ladder_and_wallet_policy_are_frozen() -> None:
    def factory(seed: Decimal, d0: int):
        return replay_shared_wallet(
            (_trade("a", "M", 0, 1),),
            _fx(),
            starting_usd=seed,
            start_available_at_ns=d0,
        )

    with pytest.raises(CapacityGateError, match="frozen 19-rung"):
        evaluate_starting_capital_ladder(
            None,
            _fx(),
            availability_start_ns=0,
            availability_end_ns=4 * DAY_NS,
            ladder=STARTING_CAPITAL_LADDER_USD[:-1],
            replay_factory=factory,
        )
    with pytest.raises(CapacityGateError, match="policy"):
        evaluate_starting_capital_ladder(
            None,
            _fx(),
            availability_start_ns=0,
            availability_end_ns=4 * DAY_NS,
            allocation_percent=19,
            replay_factory=factory,
        )


def test_ladder_accepts_only_wallet_and_recomputes_exits() -> None:
    def factory(seed: Decimal, d0: int):
        return replay_shared_wallet(
            (_trade("a", "M", 0, 1),),
            _fx(),
            starting_usd=seed,
            start_available_at_ns=d0,
        )

    wallet = factory(Decimal("10"), 0)
    attribution = attribute_realized_net(
        wallet.realized_exits,
        _fx(),
        availability_start_ns=0,
        availability_end_ns=4 * DAY_NS,
    )

    with pytest.raises(CapacityGateError, match="replay result"):
        evaluate_starting_capital_ladder(
            None,
            _fx(),
            availability_start_ns=0,
            availability_end_ns=4 * DAY_NS,
            replay_factory=lambda _seed, _d0: LadderRung(Decimal("10"), wallet, attribution),
        )
    with pytest.raises(CapacityGateError, match="replay result"):
        evaluate_starting_capital_ladder(
            None,
            _fx(),
            availability_start_ns=0,
            availability_end_ns=4 * DAY_NS,
            replay_factory=lambda _seed, _d0: (wallet, attribution),  # type: ignore[return-value]
        )


def test_duplicate_ids_missing_keys_and_quote_precision_fail_closed() -> None:
    with pytest.raises(CapacityGateError, match="duplicate wallet trade ID"):
        replay_shared_wallet(
            (_trade("a", "M", 0, 1), _trade("a", "N", 1, 2, order=1)),
            _fx(),
            starting_usd=Decimal("10"),
            start_available_at_ns=0,
        )
    with pytest.raises(CapacityGateError, match="chain keys"):
        WalletTrade(
            trade_id="missing",
            mint="M",
            entry_available_at_ns=1,
            exit_available_at_ns=DAY_NS + 1,
            entry_quote_atomic=1,
            exit_quote_atomic=1,
        )
    with pytest.raises(CapacityGateError, match="quote_decimals"):
        WalletTrade(
            trade_id="precision",
            mint="M",
            entry_available_at_ns=1,
            exit_available_at_ns=DAY_NS + 1,
            entry_quote_atomic=1,
            exit_quote_atomic=1,
            quote_decimals=8,
            entry_chain_key=ChainKey(0, 0, 0, 0, "precision:entry"),
            exit_chain_key=ChainKey(1, 0, 0, 0, "precision:exit"),
        )
    with pytest.raises(CapacityGateError, match="duplicate realized exit"):
        attribute_realized_net(
            (
                RealizedExit("same", DAY_NS + 1, 1),
                RealizedExit("same", 2 * DAY_NS + 1, 1),
            ),
            _fx(),
            availability_start_ns=0,
            availability_end_ns=4 * DAY_NS,
        )


def test_complete_day_inventory_must_be_exactly_four_selected_days() -> None:
    with pytest.raises(CapacityGateError, match="exactly"):
        earliest_four_complete_days(
            0,
            5 * DAY_NS,
            complete_days=tuple(CompleteUtcDay(index * DAY_NS) for index in range(5)),
        )


def test_forged_wallet_ledger_is_rejected() -> None:
    wallet = replay_shared_wallet(
        (_trade("a", "M", 0, 1),),
        _fx(),
        starting_usd=Decimal("10"),
        start_available_at_ns=0,
    )
    with pytest.raises(CapacityGateError, match="final wallet quote"):
        replace(wallet, final_quote_atomic=wallet.final_quote_atomic + 1)
    with pytest.raises(CapacityGateError, match="decisions cannot be empty"):
        replace(wallet, decisions=())


def test_b_rung_ignores_terminal_fx_false_negative() -> None:
    fx = CausalFxSeries(
        (
            CausalFxPoint(0, Decimal("100")),
            CausalFxPoint(1, Decimal("1")),
            CausalFxPoint(4 * DAY_NS, Decimal("1")),
        )
    )

    def factory(seed: Decimal, d0: int):
        trades = tuple(
            WalletTrade(
                trade_id=f"day-{day}",
                mint=f"M-{day}",
                entry_available_at_ns=day * DAY_NS + 1,
                exit_available_at_ns=day * DAY_NS + DAY_NS // 2,
                entry_quote_atomic=1,
                exit_quote_atomic=1 + 20_000_000_000_000,
                entry_chain_key=ChainKey(2 * day, 0, 0, 0, f"day-{day}:entry"),
                exit_chain_key=ChainKey(2 * day + 1, 0, 0, 0, f"day-{day}:exit"),
            )
            for day in range(4)
        )
        return replay_shared_wallet(
            trades,
            fx,
            starting_usd=seed,
            start_available_at_ns=d0,
        )

    result = evaluate_starting_capital_ladder(
        None,
        fx,
        availability_start_ns=0,
        availability_end_ns=4 * DAY_NS,
        replay_factory=factory,
    )
    million = next(rung for rung in result.rungs if rung.starting_usd == Decimal("1000000"))
    assert not million.wallet.profitable_in_usd
    assert million.wallet.passed
    assert million.attribution.passed
    assert million.passed


def test_deterministic_json_has_terminal_newline() -> None:
    assert deterministic_report_json({"status": "ok"}).endswith("\n")
