from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pumpquant.state.lifecycle_catalog import (
    LifecycleCatalogError,
    materialize_lifecycle_catalog,
)

SCHEMA = pa.schema(
    (
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
        pa.field("values_json", pa.string()),
    )
)


def _event(
    event_id: str,
    slot: int,
    name: str,
    values: dict[str, object],
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "lifecycle_event_id": event_id,
        "slot": slot,
        "tx_index": 0,
        "log_index": 1,
        "event_index": 0,
        "event_name": name,
        "mint": values.get("mint"),
        "base_mint": values.get("base_mint"),
        "pool": values.get("pool"),
        "block_time": slot,
        "values_json": json.dumps(values),
    }


def _dataset(root: Path, rows: list[dict[str, object]]) -> Path:
    root.mkdir()
    shard = root / "lifecycle.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), shard)
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


def test_materializes_exact_launch_and_lifecycle_outcomes(tmp_path: Path) -> None:
    source = _dataset(
        tmp_path / "source",
        [
            _event(
                "launch",
                10,
                "CreateEvent",
                {
                    "mint": "MINT",
                    "name": "Atlas",
                    "symbol": "ATL",
                    "creator": "CREATOR",
                    "user": "LAUNCHER",
                    "bonding_curve": "CURVE",
                    "quote_mint": "SOL",
                    "token_program": "TOKEN",
                    "token_total_supply": 1_000_000_000_000_000,
                    "virtual_token_reserves": 1_073_000_000_000_000,
                    "virtual_sol_reserves": 30_000_000_000,
                    "real_token_reserves": 793_100_000_000_000,
                    "is_mayhem_mode": False,
                    "is_cashback_enabled": True,
                },
            ),
            _event("complete", 20, "CompleteEvent", {"mint": "MINT"}),
            _event(
                "migration",
                21,
                "CompletePumpAmmMigrationEvent",
                {
                    "mint": "MINT",
                    "pool": "POOL",
                    "user": "MIGRATOR",
                    "mint_amount": 200,
                    "sol_amount": 300,
                    "pool_migration_fee": 4,
                },
            ),
            _event(
                "pool",
                22,
                "CreatePoolEvent",
                {
                    "base_mint": "MINT",
                    "pool": "POOL",
                    "index": 0,
                    "creator": "POOL_CREATOR",
                    "coin_creator": "CREATOR",
                    "base_mint_decimals": 6,
                    "quote_mint_decimals": 9,
                    "pool_base_amount": 200,
                    "pool_quote_amount": 300,
                    "initial_liquidity": 400,
                    "minimum_liquidity": 5,
                    "lp_token_amount_out": 395,
                    "lp_mint": "LP",
                },
            ),
        ],
    )
    output = tmp_path / "catalog"
    manifest = materialize_lifecycle_catalog(source, output, batch_size=1)
    row = pq.read_table(output / "tokens.parquet").to_pylist()[0]

    assert manifest["row_count"] == 1
    assert row["mint"] == "MINT"
    assert row["token_total_supply_atomic"] == 1_000_000_000_000_000
    assert row["launch_time_ns"] == 10_000_000_000
    assert row["completed_bonding_curve"] is True
    assert row["migrated_to_pumpswap"] is True
    assert row["created_pumpswap_pool"] is True
    assert row["migration_pool"] == "POOL"
    assert row["pool"] == "POOL"
    assert row["base_decimals"] == 6
    audit = json.loads((output / "audit.json").read_text(encoding="utf-8"))
    assert audit["lifecycle_rows_read"] == 4
    assert audit["orphan_anchor_counts"] == {"complete": 0, "migration": 0, "pool": 0}


def test_duplicate_launch_and_non_overwrite_fail_closed(tmp_path: Path) -> None:
    source = _dataset(
        tmp_path / "source",
        [
            _event("launch-a", 10, "CreateEvent", {"mint": "MINT"}),
            _event("launch-b", 11, "CreateEvent", {"mint": "MINT"}),
        ],
    )
    output = tmp_path / "catalog"
    with pytest.raises(LifecycleCatalogError, match="multiple launch events"):
        materialize_lifecycle_catalog(source, output)
    assert not output.exists()

    output.mkdir()
    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        materialize_lifecycle_catalog(source, output)


def test_pumpswap_pool_with_sol_as_base_is_oriented_to_token(tmp_path: Path) -> None:
    source = _dataset(
        tmp_path / "source",
        [
            _event("launch", 10, "CreateEvent", {"mint": "TOKEN"}),
            _event(
                "pool",
                18,
                "CreatePoolEvent",
                {
                    "base_mint": "So11111111111111111111111111111111111111112",
                    "quote_mint": "TOKEN",
                    "pool": "POOL",
                    "index": 0,
                    "base_mint_decimals": 9,
                    "quote_mint_decimals": 6,
                    "pool_base_amount": 2_000_000_000,
                    "pool_quote_amount": 300_000_000,
                },
            ),
            _event(
                "migration",
                19,
                "CompletePumpAmmMigrationEvent",
                {"mint": "TOKEN", "pool": "POOL"},
            ),
        ],
    )
    output = tmp_path / "catalog"
    materialize_lifecycle_catalog(source, output)
    row = pq.read_table(output / "tokens.parquet").to_pylist()[0]

    assert row["created_pumpswap_pool"] is True
    assert row["pool_orientation_flipped"] is True
    assert row["base_decimals"] == 6
    assert row["quote_decimals"] == 9
    assert row["pool_base_amount_atomic"] == 300_000_000
    assert row["pool_quote_amount_atomic"] == 2_000_000_000
