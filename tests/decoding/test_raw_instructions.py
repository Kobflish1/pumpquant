from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from pumpquant.acquisition.bigquery.archive import (
    ArchiveConflictError,
    QueryProvenance,
    archive_bigquery_pages,
)
from pumpquant.decoding.raw_instructions import (
    DISCOVERY_COLUMNS,
    RawInstructionRow,
    stream_raw_instructions,
)

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


def _row(slot: int, timestamp: str, *, complete: bool = True) -> dict[str, object]:
    values: list[object] = [
        str(slot),
        "block-hash",
        timestamp,
        f"signature-{slot}",
        str(slot),
        None,
        [{"v": "account-a"}, {"v": "account-b"}],
        "00112233",  # Keep raw instruction data as text.
        '{"amount":"1.230000000"}',  # Do not parse this JSON.
        "pump",
        "program-id",
        "buy",
        [
            {
                "v": {
                    "f": [
                        {"v": "amount"},
                        {"v": "001.230000000"},
                    ]
                }
            }
        ],
        "true" if complete else "false",
    ]
    return {"f": [{"v": value} for value in values]}


def _provenance(total_rows: int) -> QueryProvenance:
    return QueryProvenance(
        plan_name="pump_instruction_discovery_v1",
        plan_sha256="a" * 64,
        request_id="00000000-0000-0000-0000-000000000001",
        job_id="job-1",
        billing_project="billing-project",
        location="us-central1",
        source_tables=("bigquery-public-data.crypto_solana_mainnet_us.Instructions",),
        source_contract_sha256="b" * 64,
        start="2026-06-07T00:00:00Z",
        end="2026-06-08T00:00:00Z",
        dry_run_estimated_bytes=1,
        maximum_bytes_billed=2,
        actual_bytes_processed=1,
        total_rows=total_rows,
    )


def _archive(tmp_path: Path, rows: list[dict[str, object]], *, schema: object = SCHEMA):
    pages = [
        {
            "schema": schema,
            "rows": rows,
            "job_id": "job-1",
            "page_number": 0,
            "total_rows": len(rows),
            "total_bytes_processed": 1,
        }
    ]
    return archive_bigquery_pages(
        pages,
        tmp_path / "archive",
        table="Instructions",
        schema=schema,
        max_rows_per_shard=1,
        provenance=_provenance(len(rows)),
    )


def test_streams_all_discovery_columns_without_float_or_swap_conversion(tmp_path: Path) -> None:
    manifest = _archive(
        tmp_path,
        [_row(7, "2026-06-07T12:00:00Z"), _row(8, "2026-06-07T12:01:00Z")],
    )

    rows = list(
        stream_raw_instructions(
            tmp_path / "archive",
            manifest,
            batch_size=1,
            expected_schema_hash=manifest.schema_sha256,
            expected_source_contract=manifest.provenance.source_contract_sha256,  # type: ignore[union-attr]
        )
    )

    assert tuple(RawInstructionRow.__dataclass_fields__) == (
        *DISCOVERY_COLUMNS,
        "source_locator",
    )
    assert [row.block_slot for row in rows] == [7, 8]
    assert rows[0].accounts == ("account-a", "account-b")
    assert rows[0].data == "00112233"
    assert rows[0].parsed == '{"amount":"1.230000000"}'
    assert rows[0].params == (("amount", "001.230000000"),)
    assert rows[0].source_locator.manifest_sha256 == manifest.manifest_sha256
    assert rows[0].source_locator.shard_path == "Instructions/part-000000.parquet"
    assert rows[0].source_locator.shard_index == 0
    assert rows[0].source_locator.row_offset == 0
    assert rows[1].source_locator.shard_index == 1
    assert rows[0].capability_labels == (
        "seed_only",
        "chain_identity_complete",
        "not_true_l1",
        "not_true_l2",
    )
    assert not rows[0].is_true_l1 and not rows[0].is_true_l2
    with pytest.raises(FrozenInstanceError):
        rows[0].data = "changed"  # type: ignore[misc]


def test_integrity_is_verified_before_first_yield(tmp_path: Path) -> None:
    manifest = _archive(tmp_path, [_row(1, "2026-06-07T00:00:00Z")])
    shard_path = tmp_path / "archive" / manifest.shards[0].path
    shard_path.write_bytes(shard_path.read_bytes() + b"tamper")

    iterator = stream_raw_instructions(tmp_path / "archive", manifest)
    with pytest.raises(ArchiveConflictError):
        next(iterator)


def test_schema_and_table_contracts_are_fail_closed(tmp_path: Path) -> None:
    manifest = _archive(tmp_path, [_row(1, "2026-06-07T00:00:00Z")])
    with pytest.raises(ValueError, match="schema hash"):
        next(
            stream_raw_instructions(tmp_path / "archive", manifest, expected_schema_sha256="c" * 64)
        )

    other = archive_bigquery_pages(
        [
            {
                "schema": SCHEMA,
                "rows": [_row(1, "2026-06-07T00:00:00Z")],
            }
        ],
        tmp_path / "other",
        table="Transactions",
        schema=SCHEMA,
    )
    with pytest.raises(ValueError, match="table Instructions"):
        next(stream_raw_instructions(tmp_path / "other", other))


def test_provenance_is_half_open_and_identity_is_required(tmp_path: Path) -> None:
    out_of_range = _archive(tmp_path, [_row(1, "2026-06-08T00:00:00Z")])
    with pytest.raises(ValueError, match="half-open"):
        list(stream_raw_instructions(tmp_path / "archive", out_of_range))

    incomplete_dir = tmp_path / "incomplete"
    pages = [
        {
            "schema": SCHEMA,
            "rows": [_row(1, "2026-06-07T00:00:00Z", complete=False)],
        }
    ]
    incomplete = archive_bigquery_pages(
        pages,
        incomplete_dir,
        table="Instructions",
        schema=SCHEMA,
    )
    with pytest.raises(ValueError, match="complete chain identity"):
        list(stream_raw_instructions(incomplete_dir, incomplete))
