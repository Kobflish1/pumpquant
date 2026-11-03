from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pumpquant.cli import main
from pumpquant.state.launch_times import LaunchTimesError, export_launch_times


def _feature_dataset(tmp_path: Path, name: str, rows: list[dict[str, object]]) -> Path:
    root = tmp_path / name
    root.mkdir()
    feature = root / "features.parquet"
    pq.write_table(pa.Table.from_pylist(rows), feature, compression="zstd")
    digest = hashlib.sha256(feature.read_bytes()).hexdigest()
    manifest = {
        "kind": "pumpquant.lifecycle_feature_dataset",
        "version": 1,
        "row_count": len(rows),
        "files": {
            "features.parquet": {
                "byte_count": feature.stat().st_size,
                "sha256": digest,
            }
        },
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


def _row(
    event_id: str, mint: str, launch: int | None, *, status: str | None = None
) -> dict[str, object]:
    launch_status = "present" if launch is not None else "missing"
    return {
        "event_id": event_id,
        "mint": mint,
        "slot": 20,
        "tx_index": 2,
        "instruction_index": 1,
        "block_time_ns": 2_000,
        "launch_slot": 10 if launch is not None else None,
        "launch_tx_index": 1 if launch is not None else None,
        "launch_instruction_index": 0 if launch is not None else None,
        "launch_block_time_ns": launch,
        "launch_anchor_status": launch_status,
        "lifecycle_anchor_status": status
        or ("complete" if launch is not None else "missing_launch"),
        "missing_launch_anchor": launch is None,
    }


def test_export_deduplicates_repeated_true_anchor_and_excludes_missing(tmp_path: Path) -> None:
    features = _feature_dataset(
        tmp_path,
        "features",
        [
            _row("a", "MINT_A", 1, status="missing_pool_creation"),
            _row("b", "MINT_A", 1, status="complete"),
            _row("c", "MINT_B", None),
        ],
    )
    output = tmp_path / "launch_times.json"
    report = export_launch_times(features, output)
    assert json.loads(output.read_text()) == {"MINT_A": 1}
    assert report["mints_excluded_missing_anchor"] == ["MINT_B"]
    assert report["strict"] is False
    with pytest.raises(LaunchTimesError, match="missing launch anchors: MINT_B"):
        export_launch_times(features, tmp_path / "strict.json", strict=True)
    assert not (tmp_path / "strict.json").exists()


def test_export_rejects_conflict_and_preserves_existing_output(tmp_path: Path) -> None:
    features = _feature_dataset(tmp_path, "features", [_row("a", "MINT", 1), _row("b", "MINT", 2)])
    output = tmp_path / "launch_times.json"
    output.write_text('{"old":0}\n', encoding="utf-8")
    with pytest.raises(LaunchTimesError, match="conflicting launch times"):
        export_launch_times(features, output)
    assert output.read_text(encoding="utf-8") == '{"old":0}\n'
    assert not list(tmp_path.glob(".launch_times.json.*.partial"))


@pytest.mark.parametrize(
    ("row", "message"),
    [
        (_row("a", "", 1), "mint"),
        ({**_row("a", "MINT", 1), "launch_anchor_status": "unknown"}, "launch_anchor_status"),
        ({**_row("a", "MINT", 1), "launch_block_time_ns": -1}, "launch_block_time_ns"),
    ],
)
def test_export_rejects_malformed_anchor_rows(
    tmp_path: Path, row: dict[str, object], message: str
) -> None:
    features = _feature_dataset(tmp_path, "features", [row])
    with pytest.raises(LaunchTimesError, match=message):
        export_launch_times(features, tmp_path / "launch_times.json")


def test_export_rejects_duplicate_event_ids_across_inputs(tmp_path: Path) -> None:
    first = _feature_dataset(tmp_path, "first", [_row("same", "MINT_A", 1)])
    second = _feature_dataset(tmp_path, "second", [_row("same", "MINT_A", 1)])
    with pytest.raises(LaunchTimesError, match="duplicate feature event_id"):
        export_launch_times([first, second], tmp_path / "launch_times.json")


def test_export_resolves_chunk_local_missing_anchor_from_another_input(
    tmp_path: Path,
) -> None:
    missing = _feature_dataset(tmp_path, "missing", [_row("a", "MINT", None)])
    anchored = _feature_dataset(tmp_path, "anchored", [_row("b", "MINT", 1)])
    output = tmp_path / "launch_times.json"
    report = export_launch_times([missing, anchored], output, strict=True)
    assert json.loads(output.read_text()) == {"MINT": 1}
    assert report["missing_anchor_mints"] == []
    assert report["mints_resolved_from_other_inputs"] == ["MINT"]


def test_export_rejects_tampered_verified_parquet(tmp_path: Path) -> None:
    features = _feature_dataset(tmp_path, "features", [_row("a", "MINT", 1)])
    with (features / "features.parquet").open("ab") as handle:
        handle.write(b"tampered")
    with pytest.raises(LaunchTimesError, match="hash verification"):
        export_launch_times(features, tmp_path / "launch_times.json")


def test_export_rejects_future_anchor_and_verified_sidecar_output(tmp_path: Path) -> None:
    future = _feature_dataset(
        tmp_path,
        "future",
        [{**_row("a", "MINT", 3_000), "launch_slot": 30}],
    )
    with pytest.raises(LaunchTimesError, match="launch anchor occurs after"):
        export_launch_times(future, tmp_path / "launch_times.json")

    features = _feature_dataset(tmp_path, "features", [_row("b", "MINT", 1)])
    audit = features / "audit.json"
    audit.write_text('{"sealed":true}\n', encoding="utf-8")
    manifest_path = features / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"]["audit.json"] = {
        "byte_count": audit.stat().st_size,
        "sha256": hashlib.sha256(audit.read_bytes()).hexdigest(),
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(LaunchTimesError, match="distinct from every verified feature file"):
        export_launch_times(features, audit)
    assert audit.read_text(encoding="utf-8") == '{"sealed":true}\n'


def test_cli_positional_shorthand(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    features = _feature_dataset(tmp_path, "features", [_row("a", "MINT", 11)])
    output = tmp_path / "launch_times.json"
    assert main(["launch-times", str(features), str(output)]) == 0
    assert json.loads(output.read_text()) == {"MINT": 11}
    assert json.loads(capsys.readouterr().out)["launch_times_written"] == 1
