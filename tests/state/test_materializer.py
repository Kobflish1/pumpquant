from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pumpquant.core.io import write_parquet
from pumpquant.core.schema import Side, SwapEvent, VenuePhase
from pumpquant.pricing.pumpswap import PumpSwapFees
from pumpquant.state.materializer import (
    FeeScheduleEntry,
    materialize_amm_states,
)


def _event(event_id: str, slot: int) -> SwapEvent:
    return SwapEvent(
        event_id=event_id,
        market_id="market",
        mint="TOKEN",
        quote_mint="SOL",
        slot=slot,
        tx_index=0,
        instruction_index=0,
        block_time_ns=slot,
        observed_at_ns=slot,
        phase=VenuePhase.PUMPSWAP,
        side=Side.BUY,
        base_amount=1,
        quote_amount=1,
        price=0.1,
        virtual_base_reserve=1_000,
        virtual_quote_reserve=100,
        real_base_reserve=1_000,
        real_quote_reserve=100,
        venue_fee_bps=999,
    )


def _input(tmp_path: Path, events: list[SwapEvent]) -> Path:
    root = tmp_path / "normalized"
    root.mkdir()
    swaps = root / "swaps.parquet"
    write_parquet(swaps, events)
    manifest = {
        "kind": "pumpquant.normalized_swap_dataset",
        "version": 1,
        "row_count": len(events),
        "files": {
            "swaps.parquet": {
                "byte_count": swaps.stat().st_size,
                "sha256": hashlib.sha256(swaps.read_bytes()).hexdigest(),
            }
        },
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


def test_materializer_streams_snapshots_and_quotes(tmp_path: Path) -> None:
    source = _input(tmp_path, [_event("one", 1)])
    output = tmp_path / "states"
    manifest = materialize_amm_states(
        source,
        output,
        quote_notionals=[10],
        fee_schedule=[FeeScheduleEntry(0, 10, PumpSwapFees(lp_fee_bps=20))],
        batch_size=1,
    )
    assert manifest["row_count"] == 1
    assert manifest["quote_row_count"] == 2
    assert pq.read_table(output / "snapshots.parquet").num_rows == 1
    assert pq.read_table(output / "quotes.parquet").num_rows == 2


def test_missing_schedule_config_is_unknown_and_never_uses_event_fee(tmp_path: Path) -> None:
    source = _input(tmp_path, [_event("one", 20)])
    output = tmp_path / "states"
    materialize_amm_states(
        source,
        output,
        quote_notionals=[10],
        fee_schedule=[FeeScheduleEntry(0, 10, PumpSwapFees(lp_fee_bps=20))],
    )
    row = pq.read_table(output / "snapshots.parquet").to_pylist()[0]
    assert row["quote_status"] == "unknown_config"
    assert row["quote_count"] == 0


def test_schedule_availability_is_slot_bounded(tmp_path: Path) -> None:
    source = _input(tmp_path, [_event("one", 1), _event("two", 11)])
    output = tmp_path / "states"
    materialize_amm_states(
        source,
        output,
        quote_notionals=[10],
        fee_schedule=[FeeScheduleEntry(0, 10, PumpSwapFees(lp_fee_bps=20))],
    )
    statuses = [
        row["quote_status"] for row in pq.read_table(output / "snapshots.parquet").to_pylist()
    ]
    assert statuses == ["quoted", "unknown_config"]


def test_json_like_schedule_entries_are_accepted(tmp_path: Path) -> None:
    source = _input(tmp_path, [_event("one", 1)])
    output = tmp_path / "states"
    materialize_amm_states(
        source,
        output,
        quote_notionals=[10],
        fee_schedule={
            "version": 1,
            "entries": [
                {
                    "start_slot_inclusive": 0,
                    "end_slot_exclusive": 10,
                    "phase": "*",
                    "config": {"kind": "pumpswap", "lp_fee_bps": 20},
                }
            ],
        },
    )
    assert pq.read_table(output / "quotes.parquet").num_rows == 2


def test_regression_leaves_existing_target_untouched(tmp_path: Path) -> None:
    source = _input(tmp_path, [_event("late", 2), _event("early", 1)])
    output = tmp_path / "states"
    output.mkdir()
    marker = output / "marker"
    marker.write_text("old", encoding="utf-8")
    with pytest.raises(ValueError, match="strictly increasing"):
        materialize_amm_states(source, output, quote_notionals=[10])
    assert marker.read_text(encoding="utf-8") == "old"
    assert not list(tmp_path.glob(".states.*.partial"))


def test_duplicate_ids_across_batches_fail_closed(tmp_path: Path) -> None:
    source = _input(tmp_path, [_event("same", 1), _event("same", 2)])
    output = tmp_path / "states"
    with pytest.raises(ValueError, match="duplicate event_id"):
        materialize_amm_states(source, output, batch_size=1)
    assert not output.exists()


def test_manifest_file_hashes_match_published_bytes(tmp_path: Path) -> None:
    source = _input(tmp_path, [_event("one", 1)])
    output = tmp_path / "states"
    materialize_amm_states(output=output, input_path=source)
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    for name, metadata in manifest["files"].items():
        path = output / name
        assert metadata["byte_count"] == path.stat().st_size
        assert metadata["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_successful_rebuild_atomically_replaces_previous_target(tmp_path: Path) -> None:
    source = _input(tmp_path, [_event("one", 1)])
    output = tmp_path / "states"
    output.mkdir()
    (output / "old").write_text("old", encoding="utf-8")
    materialize_amm_states(source, output)
    assert not (output / "old").exists()
    assert (output / "manifest.json").is_file()
