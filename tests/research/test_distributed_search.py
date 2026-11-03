from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import pytest

from pumpquant.core.schema import Bar, VenuePhase
from pumpquant.execution.bar_replay import ExecutionConfig, MomentumSpec
from pumpquant.research.distributed_reducer import reduce_distributed_ledgers
from pumpquant.research.distributed_search import (
    LedgerMergeError,
    launch_local_workers,
    merge_worker_ledgers,
    partition_candidates,
    partition_sizes,
)
from pumpquant.research.search import momentum_grid, momentum_spec_sha256, walk_forward_search


def _token_bars(mint_index: int) -> list[Bar]:
    launch = mint_index * 100 * 1_000_000_000
    prices = [0.10, 0.11, 0.13, 0.16, 0.20, 0.18, 0.17]
    return [
        Bar(
            market_id=f"M{mint_index}-CURVE",
            mint=f"M{mint_index}",
            quote_mint="SOL",
            start_time_ns=launch + index * 1_000_000_000,
            end_time_ns=launch + (index + 1) * 1_000_000_000,
            phase=VenuePhase.BONDING_CURVE,
            open=price,
            high=price,
            low=price,
            close=price,
            base_volume=1_000.0,
            quote_volume=100.0,
            buy_quote_volume=80.0,
            sell_quote_volume=20.0,
            trade_count=10,
            unique_traders=8,
            virtual_base_reserve=100.0 / price,
            virtual_quote_reserve=100.0,
            token_age_seconds=float(index + 1),
            real_base_reserve=1_000_000.0,
            real_quote_reserve=100.0,
        )
        for index, price in enumerate(prices)
    ]


def test_modulo_partition_is_lazy_and_covers_every_position() -> None:
    consumed = 0

    class Stream:
        def __iter__(self):
            nonlocal consumed
            for value in range(11):
                consumed += 1
                yield value  # type: ignore[misc]

    # Values are deliberately not MomentumSpec: this test exercises the
    # orchestration wrapper alone, before the evaluator validates candidates.
    source = Stream()
    partitions = [partition_candidates(source, 4, index) for index in range(4)]
    assert consumed == 0
    assert [list(partition) for partition in partitions] == [
        [0, 4, 8],
        [1, 5, 9],
        [2, 6, 10],
        [3, 7],
    ]
    assert consumed == 44  # one full source pass per re-iterated partition
    assert partition_sizes(11, 4) == (3, 3, 3, 2)


def test_merge_is_stable_and_rejects_conflicting_identity(tmp_path: Path) -> None:
    first = tmp_path / "worker-0.jsonl"
    second = tmp_path / "worker-1.jsonl"
    row = {
        "experiment_id": "e",
        "fold": 0,
        "candidate_sha256": "a" * 64,
        "status": "completed",
    }
    first.write_text(json.dumps(row) + "\n", encoding="utf-8")
    second.write_text(json.dumps(row, sort_keys=True) + "\n", encoding="utf-8")
    output = tmp_path / "merged.jsonl"
    result = merge_worker_ledgers([second, first], output, expected_worker_count=2)
    assert result.row_count == 1
    assert result.duplicate_row_count == 1
    assert json.loads(output.read_text(encoding="utf-8")) == row

    second.write_text(json.dumps({**row, "status": "failed"}) + "\n", encoding="utf-8")
    with pytest.raises(LedgerMergeError, match="conflicting duplicate"):
        merge_worker_ledgers([first, second], tmp_path / "must-not-publish.jsonl")


def test_partition_preserves_declared_stream_deduplication() -> None:
    candidate = MomentumSpec()

    class DeclaredUnique:
        deduplicated = True

        def __iter__(self):
            yield candidate
            yield candidate

    partition = partition_candidates(DeclaredUnique(), 2, 0)
    assert list(partition) == [candidate]


def test_local_launcher_handles_empty_partitions_with_bounded_pool(tmp_path: Path) -> None:
    config = ExecutionConfig(
        quote_notional=0.1,
        venue_fee_bps=0.0,
        adverse_selection_bps=0.0,
        fixed_network_cost_quote=0.0,
        priority_cost_quote=0.0,
        max_reserve_fraction=0.1,
        max_bar_volume_fraction=0.1,
    )
    results = launch_local_workers(
        [],
        (),
        config,
        worker_count=2,
        ledger_dir=tmp_path,
        max_workers=1,
    )
    assert [item.worker_index for item in results] == [0, 1]
    assert all(item.report.candidate_count == 0 for item in results)


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="copy-on-write launcher requires POSIX fork",
)
def test_local_launcher_can_share_bars_through_fork(tmp_path: Path) -> None:
    config = ExecutionConfig(
        quote_notional=0.1,
        venue_fee_bps=0.0,
        adverse_selection_bps=0.0,
        fixed_network_cost_quote=0.0,
        priority_cost_quote=0.0,
        max_reserve_fraction=0.1,
        max_bar_volume_fraction=0.1,
    )
    results = launch_local_workers(
        [],
        (),
        config,
        worker_count=2,
        ledger_dir=tmp_path,
        max_workers=2,
        fork_share_bars=True,
    )
    assert [item.worker_index for item in results] == [0, 1]
    assert all(item.report.candidate_count == 0 for item in results)


def test_local_launcher_can_run_a_disjoint_global_worker_subset(tmp_path: Path) -> None:
    config = ExecutionConfig(
        quote_notional=0.1,
        venue_fee_bps=0.0,
        adverse_selection_bps=0.0,
        fixed_network_cost_quote=0.0,
        priority_cost_quote=0.0,
        max_reserve_fraction=0.1,
        max_bar_volume_fraction=0.1,
    )
    results = launch_local_workers(
        [],
        (),
        config,
        worker_count=4,
        worker_indexes=(1, 3),
        ledger_dir=tmp_path,
        max_workers=2,
        fork_share_bars=True,
    )
    assert [item.worker_index for item in results] == [1, 3]
    assert all(item.worker_count == 4 for item in results)
    with pytest.raises(ValueError, match="unique"):
        launch_local_workers(
            [],
            (),
            config,
            worker_count=4,
            worker_indexes=(1, 1),
            ledger_dir=tmp_path,
        )


def test_global_reducer_matches_monolithic_training_selection(tmp_path: Path) -> None:
    bars = [bar for mint in range(8) for bar in _token_bars(mint)]
    template = MomentumSpec(
        volume_lookback=2,
        min_liquidity_quote=0.0,
        min_trade_count=1,
        min_age_seconds=0.0,
        max_age_seconds=100.0,
        exit_momentum=-1.0,
        trailing_stop=0.9,
    )
    candidates = momentum_grid(
        momentum_lookbacks=(1, 2),
        entry_momenta=(0.04,),
        entry_imbalances=(0.1,),
        take_profits=(0.1, 0.2),
        stop_losses=(0.9,),
        template=template,
    )
    config = ExecutionConfig(
        quote_notional=0.1,
        venue_fee_bps=0.0,
        adverse_selection_bps=0.0,
        fixed_network_cost_quote=0.0,
        priority_cost_quote=0.0,
        max_reserve_fraction=0.1,
        max_bar_volume_fraction=0.1,
    )
    common = {
        "min_train_tokens": 3,
        "test_tokens": 2,
        "minimum_train_positions": 1,
        "halving_rounds": 1,
    }
    monolithic = walk_forward_search(bars, candidates, config, **common)
    ledger_dir = tmp_path / "workers"
    workers = launch_local_workers(
        bars,
        candidates,
        config,
        worker_count=2,
        max_workers=2,
        ledger_dir=ledger_dir,
        experiment_id="exact-reducer-test",
        data_sha256="a" * 64,
        defer_test_replay=True,
        **common,
    )
    assert all(fold.test_backtest is None for worker in workers for fold in worker.report.folds)
    output = tmp_path / "global.json"
    reduction = reduce_distributed_ledgers(
        bars,
        sorted(ledger_dir.glob("worker-*.jsonl")),
        output_path=output,
        merged_ledger_path=tmp_path / "merged.jsonl",
        expected_worker_count=2,
        expected_candidate_count=len(candidates),
        experiment_id="exact-reducer-test",
        data_sha256="a" * 64,
        min_train_tokens=3,
        test_tokens=2,
    )
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert reduction.fold_count == len(monolithic.folds)
    assert payload["local_test_replays_ignored"] == 0
    assert [fold["selected_candidate_sha256"] for fold in payload["folds"]] == [
        None if fold.selected is None else momentum_spec_sha256(fold.selected.spec)
        for fold in monolithic.folds
    ]
    assert payload["outcome_diagnostics"]["observations"] == (
        monolithic.outcome_diagnostics.observations
    )
    assert payload["outcome_diagnostics"]["mean_return"] == pytest.approx(
        monolithic.outcome_diagnostics.mean_return
    )
