from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from pumpquant.features.lifecycle import LifecycleFeatureError
from pumpquant.state.lifecycle_launch_times import export_lifecycle_launch_times
from tests.features.test_lifecycle import _LIFECYCLE_SCHEMA, _lifecycle, _row


def _dataset(root: Path, rows: list[dict[str, object]]) -> Path:
    root.mkdir()
    return _lifecycle(root, rows)


def test_direct_export_uses_only_true_launch_anchors(tmp_path: Path) -> None:
    first = _dataset(
        tmp_path / "first",
        [
            _row("launch-a", 10, 1, "CreateEvent", mint="MINT_A"),
            _row("other", 11, 1, "CompleteEvent", mint="MINT_A"),
        ],
    )
    second = _dataset(
        tmp_path / "second",
        [_row("launch-b", 20, 1, "CreateEvent", mint="MINT_B")],
    )
    output = tmp_path / "launch_times.json"
    report = export_lifecycle_launch_times([first, second], output, batch_size=1)
    assert json.loads(output.read_text()) == {
        "MINT_A": 10_000_000_000,
        "MINT_B": 20_000_000_000,
    }
    assert report["lifecycle_rows_read"] == 3
    assert report["launch_times_written"] == 2
    assert report["first_observed_fallback"] is False


def test_direct_trade_manifest_prefers_lifecycle_schema_hash(tmp_path: Path) -> None:
    source = _dataset(
        tmp_path / "source",
        [_row("launch", 10, 1, "CreateEvent")],
    )
    manifest_path = source / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["schema_sha256"] = "0" * 64
    manifest["lifecycle_schema_sha256"] = hashlib.sha256(
        _LIFECYCLE_SCHEMA.serialize().to_pybytes()
    ).hexdigest()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    output = tmp_path / "launch_times.json"
    export_lifecycle_launch_times(source, output)
    assert json.loads(output.read_text(encoding="utf-8")) == {
        "MINT": 10_000_000_000,
    }


def test_multiple_launches_for_one_mint_fail_before_replacing_output(tmp_path: Path) -> None:
    source = _dataset(
        tmp_path / "source",
        [
            _row("launch-a", 10, 1, "CreateEvent"),
            _row("launch-b", 11, 1, "CreateEvent"),
        ],
    )
    output = tmp_path / "launch_times.json"
    output.write_text('{"old":1}\n', encoding="utf-8")
    with pytest.raises(LifecycleFeatureError, match="multiple true launch anchors"):
        export_lifecycle_launch_times(source, output)
    assert output.read_text(encoding="utf-8") == '{"old":1}\n'
    assert not list(tmp_path.glob(".launch_times.json.*.partial"))


def test_global_order_regression_and_tampering_fail_closed(tmp_path: Path) -> None:
    late = _dataset(tmp_path / "late", [_row("late", 20, 1, "CreateEvent")])
    early = _dataset(tmp_path / "early", [_row("early", 10, 1, "CreateEvent")])
    with pytest.raises(LifecycleFeatureError, match="strictly increasing"):
        export_lifecycle_launch_times([late, early], tmp_path / "ordered.json")

    shard = early / "lifecycle.parquet"
    with shard.open("ab") as handle:
        handle.write(b"tampered")
    with pytest.raises(LifecycleFeatureError, match="byte/hash verification"):
        export_lifecycle_launch_times(early, tmp_path / "tampered.json")
