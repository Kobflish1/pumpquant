from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from pumpquant.acquisition.bigquery.archive import (
    ArchiveConflictError,
    ArchiveManifest,
    archive_bigquery_pages,
)
from pumpquant.acquisition.signature_inventory import (
    INVENTORY_COLUMNS,
    build_signature_inventory,
    is_valid_signature,
)

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


SCHEMA = {
    "fields": [
        {"name": "block_slot", "type": "INTEGER"},
        {"name": "block_hash", "type": "STRING"},
        {"name": "block_timestamp", "type": "TIMESTAMP"},
        {"name": "tx_signature", "type": "STRING"},
        {"name": "source_index", "type": "INTEGER"},
        {"name": "source_parent_index", "type": "INTEGER"},
        {"name": "accounts", "type": "STRING", "mode": "REPEATED"},
        {"name": "data", "type": "STRING"},
        {"name": "parsed", "type": "STRING"},
        {"name": "program", "type": "STRING"},
        {"name": "program_id", "type": "STRING"},
        {"name": "instruction_type", "type": "STRING"},
        {
            "name": "params",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "key", "type": "STRING"},
                {"name": "value", "type": "STRING"},
            ],
        },
        {"name": "chain_identity_complete", "type": "BOOL"},
    ]
}


def _signature(byte: int) -> str:
    """A canonical 64-byte all-zero Solana signature fixture."""

    if byte == 0:
        return "1" * 64
    # Keep fixtures dependency-free while producing a canonical base58 value
    # through the module's validator.  Distinct values are only needed here.
    number = int.from_bytes(bytes([byte]) * 64, "big")
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    result: list[str] = []
    while number:
        number, remainder = divmod(number, 58)
        result.append(alphabet[remainder])
    return "".join(reversed(result))


def _row(
    slot: int,
    timestamp: str | None,
    signature: str,
    program_id: str | None,
) -> dict[str, object]:
    values: list[object] = [
        str(slot),
        "block-hash",
        timestamp,
        signature,
        str(slot),
        None,
        [],
        None,
        None,
        "pump",
        program_id,
        "buy",
        [],
        "true",
    ]
    return {"f": [{"v": value} for value in values]}


def _archive(
    tmp_path: Path,
    rows: list[dict[str, object]],
    name: str = "archive",
) -> ArchiveManifest:
    return archive_bigquery_pages(
        [{"schema": SCHEMA, "rows": rows}],
        tmp_path / name,
        table="Instructions",
        schema=SCHEMA,
        max_rows_per_shard=2,
    )


def test_inventory_deduplicates_filters_half_open_and_preserves_provenance(
    tmp_path: Path,
) -> None:
    first = _signature(0)
    second = _signature(1)
    assert is_valid_signature(first)
    assert is_valid_signature(second)
    archive = _archive(
        tmp_path,
        [
            _row(10, "2026-06-07T00:00:00Z", first, "program-a"),
            _row(11, "2026-06-07T00:00:01Z", first, "program-b"),
            _row(12, "2026-06-08T00:00:00Z", second, "program-c"),
            _row(13, "2026-06-07T00:00:02Z", "not-a-signature", "program-d"),
        ],
    )

    result = build_signature_inventory(
        [tmp_path / "archive"],
        tmp_path / "inventory",
        start="2026-06-07T00:00:00Z",
        end="2026-06-08T00:00:00Z",
        batch_size=1,
        bucket_count=4,
    )

    table = pq.read_table(tmp_path / "inventory" / result.inventory_path)
    assert tuple(table.column_names) == INVENTORY_COLUMNS
    assert table.to_pylist() == [
        {
            "signature": first,
            "first_slot": 10,
            "last_slot": 11,
            "first_block_timestamp": table.to_pylist()[0]["first_block_timestamp"],
            "last_block_timestamp": table.to_pylist()[0]["last_block_timestamp"],
            "instruction_count": 2,
            "program_ids": ["program-a", "program-b"],
            "source_manifests": [archive.manifest_sha256],
        }
    ]
    assert result.inventory_row_count == 1
    assert result.included_row_count == 2
    assert result.duplicate_row_count == 1
    assert result.malformed_signature_count == 1
    assert result.out_of_bounds_row_count == 1
    manifest_payload = json.loads((tmp_path / "inventory" / "manifest.json").read_text())
    assert (
        manifest_payload["inventory_sha256"]
        == hashlib.sha256((tmp_path / "inventory" / result.inventory_path).read_bytes()).hexdigest()
    )


def test_integrity_is_verified_for_all_inputs_before_output(tmp_path: Path) -> None:
    _archive(tmp_path, [_row(1, "2026-06-07T00:00:00Z", _signature(0), "p")])
    second = _archive(
        tmp_path,
        [_row(2, "2026-06-07T00:00:00Z", _signature(1), "p")],
        name="second",
    )
    shard = tmp_path / "second" / second.shards[0].path
    shard.write_bytes(shard.read_bytes() + b"tamper")

    with pytest.raises(ArchiveConflictError):
        build_signature_inventory(
            [tmp_path / "archive", tmp_path / "second"],
            tmp_path / "inventory",
            start="2026-06-07T00:00:00Z",
            end="2026-06-08T00:00:00Z",
        )
    assert not (tmp_path / "inventory").exists()


def test_rerun_is_byte_deterministic_and_cleans_partial_state(tmp_path: Path) -> None:
    _archive(tmp_path, [_row(1, "2026-06-07T00:00:00Z", _signature(0), "p")])
    stale = tmp_path / ".inventory.partial"
    stale.mkdir()
    (stale / "orphan").write_text("stale")

    kwargs = {
        "start": "2026-06-07T00:00:00Z",
        "end": "2026-06-08T00:00:00Z",
        "bucket_count": 2,
    }
    first = build_signature_inventory([tmp_path / "archive"], tmp_path / "inventory", **kwargs)
    inventory_bytes = (tmp_path / "inventory" / first.inventory_path).read_bytes()
    manifest_bytes = (tmp_path / "inventory" / first.manifest_path).read_bytes()
    second = build_signature_inventory([tmp_path / "archive"], tmp_path / "inventory", **kwargs)
    assert second == first
    assert (tmp_path / "inventory" / first.inventory_path).read_bytes() == inventory_bytes
    assert (tmp_path / "inventory" / first.manifest_path).read_bytes() == manifest_bytes
    assert not stale.exists()
    assert sorted(path.name for path in (tmp_path / "inventory").iterdir()) == [
        "manifest.json",
        "signatures.parquet",
    ]
