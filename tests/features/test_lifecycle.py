from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pumpquant.core.io import write_parquet
from pumpquant.core.schema import Side, SwapEvent, VenuePhase
from pumpquant.features.lifecycle import (
    LifecycleFeatureError,
    materialize_lifecycle_features,
)


def _event(event_id: str, slot: int, instruction_index: int) -> SwapEvent:
    return SwapEvent(
        event_id=event_id,
        market_id="CURVE",
        mint="MINT",
        quote_mint="SOL",
        slot=slot,
        tx_index=0,
        instruction_index=instruction_index,
        block_time_ns=slot * 1_000_000_000,
        observed_at_ns=slot * 1_000_000_000,
        phase=VenuePhase.BONDING_CURVE,
        side=Side.BUY,
        base_amount=1,
        quote_amount=2,
        price=2,
        virtual_base_reserve=100,
        virtual_quote_reserve=200,
        real_base_reserve=100,
        real_quote_reserve=200,
    )


def _normalized(tmp_path: Path, events: list[SwapEvent]) -> Path:
    root = tmp_path / "normalized"
    root.mkdir()
    swaps = root / "swaps.parquet"
    write_parquet(swaps, events)
    manifest = {
        "kind": "pumpquant.normalized_swap_dataset",
        "version": 1,
        "row_count": len(events),
        "physical_order": ["slot", "tx_index", "instruction_index", "event_id"],
        "files": {
            "swaps.parquet": {
                "byte_count": swaps.stat().st_size,
                "sha256": hashlib.sha256(swaps.read_bytes()).hexdigest(),
            }
        },
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


_LIFECYCLE_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("lifecycle_event_id", pa.string()),
        pa.field("slot", pa.int64()),
        pa.field("tx_index", pa.int32()),
        pa.field("log_index", pa.int32()),
        pa.field("event_index", pa.int16()),
        pa.field("event_name", pa.string()),
        pa.field("mint", pa.string()),
        pa.field("base_mint", pa.string()),
        pa.field("pool", pa.string()),
        pa.field("block_time", pa.int64()),
    ]
)


def _lifecycle(tmp_path: Path, rows: list[dict[str, object]]) -> Path:
    root = tmp_path / "lifecycle"
    root.mkdir()
    shard = root / "lifecycle.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=_LIFECYCLE_SCHEMA), shard)
    digest = hashlib.sha256(shard.read_bytes()).hexdigest()
    manifest = {
        "kind": "pumpquant.trade_event_dataset",
        "version": 1,
        "lifecycle_row_count": len(rows),
        "lifecycle_shard_count": 1,
        "lifecycle_shards": [
            {
                "path": shard.name,
                "row_count": len(rows),
                "byte_count": shard.stat().st_size,
                "sha256": digest,
            }
        ],
        "files": {shard.name: {"byte_count": shard.stat().st_size, "sha256": digest}},
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


def _row(event_id: str, slot: int, log_index: int, name: str, **extra: object) -> dict[str, object]:
    return {
        "event_id": event_id,
        "lifecycle_event_id": event_id,
        "slot": slot,
        "tx_index": 0,
        "log_index": log_index,
        "event_index": 0,
        "event_name": name,
        "mint": "MINT",
        "base_mint": None,
        "pool": None,
        "block_time": slot,
        **extra,
    }


def test_same_slot_ordering_derives_age_and_graduation_state(tmp_path: Path) -> None:
    swaps = _normalized(tmp_path, [_event("before", 100, 2), _event("after", 100, 4)])
    lifecycle = _lifecycle(
        tmp_path,
        [_row("launch", 100, 1, "CreateEvent"), _row("graduate", 100, 3, "CompleteEvent")],
    )
    output = tmp_path / "features"
    materialize_lifecycle_features(swaps, lifecycle, output, batch_size=1)
    rows = pq.read_table(output / "features.parquet").to_pylist()
    assert rows[0]["age_slots"] == 0
    assert rows[0]["is_pre_graduation"] is True
    assert rows[1]["age_slots"] == 0
    assert rows[1]["is_post_graduation"] is True
    assert rows[0]["launch_transition_id"] == "launch"
    assert rows[1]["graduation_transition_id"] == "graduate"


def test_missing_lifecycle_anchors_are_explicit(tmp_path: Path) -> None:
    swaps = _normalized(tmp_path, [_event("one", 10, 1)])
    lifecycle = _lifecycle(tmp_path, [_row("other", 1, 1, "OtherEvent")])
    output = tmp_path / "features"
    materialize_lifecycle_features(swaps, lifecycle, output)
    row = pq.read_table(output / "features.parquet").to_pylist()[0]
    assert row["age_slots"] is None
    assert row["graduation_state"] == "unknown_graduation"
    assert row["lifecycle_anchor_status"] == "missing_launch_and_pool_creation_and_graduation"
    assert row["missing_launch_anchor"] is True
    assert row["missing_graduation_anchor"] is True


def test_duplicate_lifecycle_ids_fail_closed(tmp_path: Path) -> None:
    swaps = _normalized(tmp_path, [_event("one", 10, 1)])
    lifecycle = _lifecycle(
        tmp_path,
        [_row("same", 1, 1, "CreateEvent"), _row("same", 2, 1, "CompleteEvent")],
    )
    with pytest.raises(LifecycleFeatureError, match="duplicate lifecycle_event_id"):
        materialize_lifecycle_features(swaps, lifecycle, tmp_path / "features")


def test_duplicate_swap_ids_fail_closed(tmp_path: Path) -> None:
    swaps = _normalized(tmp_path, [_event("same", 10, 1), _event("same", 11, 1)])
    lifecycle = _lifecycle(tmp_path, [])
    with pytest.raises(LifecycleFeatureError, match="duplicate swap event_id"):
        materialize_lifecycle_features(swaps, lifecycle, tmp_path / "features", batch_size=1)


def test_failed_restart_leaves_existing_output_and_no_partial(tmp_path: Path) -> None:
    swaps = _normalized(tmp_path, [_event("late", 11, 1), _event("early", 10, 1)])
    lifecycle = _lifecycle(tmp_path, [])
    output = tmp_path / "features"
    output.mkdir()
    marker = output / "marker"
    marker.write_text("old", encoding="utf-8")
    with pytest.raises(LifecycleFeatureError, match="strictly increasing"):
        materialize_lifecycle_features(swaps, lifecycle, output, batch_size=1)
    assert marker.read_text(encoding="utf-8") == "old"
    assert not list(tmp_path.glob(".features.*.partial"))


def test_successful_restart_replaces_existing_output(tmp_path: Path) -> None:
    swaps = _normalized(tmp_path, [_event("one", 10, 1)])
    lifecycle = _lifecycle(tmp_path, [])
    output = tmp_path / "features"
    output.mkdir()
    (output / "old").write_text("old", encoding="utf-8")
    materialize_lifecycle_features(swaps, lifecycle, output)
    assert not (output / "old").exists()
    assert (output / "manifest.json").is_file()


def test_lifecycle_shard_symlink_is_rejected(tmp_path: Path) -> None:
    swaps = _normalized(tmp_path, [_event("one", 10, 1)])
    lifecycle = _lifecycle(tmp_path, [])
    target = lifecycle / "lifecycle.parquet"
    link = lifecycle / "linked.parquet"
    link.symlink_to(target)
    manifest = json.loads((lifecycle / "manifest.json").read_text(encoding="utf-8"))
    manifest["lifecycle_shards"][0]["path"] = link.name
    (lifecycle / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(LifecycleFeatureError, match="symlink"):
        materialize_lifecycle_features(swaps, lifecycle, tmp_path / "features")
