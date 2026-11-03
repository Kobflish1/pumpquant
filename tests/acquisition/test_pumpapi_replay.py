from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from pumpquant.acquisition.pumpapi_replay import SCHEMA, iter_hours, materialize_archive

try:
    from compression import zstd  # type: ignore[attr-defined,no-redef]
except ImportError:  # pragma: no cover
    import zstandard as zstd  # type: ignore[import-not-found,no-redef]


def test_materialize_archive_retains_only_pump_venues_and_exact_text(tmp_path: Path) -> None:
    hour = datetime(2026, 7, 22, 22, tzinfo=UTC)
    source = tmp_path / "hour.jsonl.zst"
    events = [
        {
            "signature": "ignore",
            "action": "buy",
            "pool": "meteora-dlmm",
            "block": 1,
            "timestamp": 1784759017000,
        },
        {
            "signature": "boost",
            "action": "buy",
            "pool": "pump-amm",
            "block": 434594090,
            "timestamp": 1784759017536,
            "localTimestamp": 1784759017537,
            "poolId": "pool",
            "mint": "mintpump",
            "quoteMint": "wsol",
            "txSigner": "authority",
            "tokenAmount": 1507931.704547,
            "quoteAmount": 0.659418954,
            "tokensInPool": 199782193.129512,
            "quoteInPool": 70.439722007,
            "tradersInvolved": {"authority": {}},
            "postBalances": {
                "authority": {
                    "sol": 1.25,
                    "mintpump": 123.0,
                    "wsol": 0.5,
                    "unrelated": 999,
                }
            },
            "tokenExtensions": {"metadataPointer": {}},
            "addressLookupTables": ["lookup"],
        },
        {
            "signature": "launch",
            "action": "create",
            "pool": "pump",
            "block": 434594091,
            "timestamp": 1784759018000,
        },
    ]
    with zstd.open(source, "wb") as stream:
        for event in events:
            stream.write(json.dumps(event, separators=(",", ":")).encode() + b"\n")

    output = tmp_path / "part.parquet"
    result = materialize_archive(source, output, hour=hour, batch_rows=1)
    table = pq.read_table(output)

    assert table.schema == SCHEMA
    assert result["archive_lines"] == 3
    assert result["rows"] == 2
    assert result["action_counts"] == {"buy": 1, "create": 1}
    rows = table.to_pylist()
    assert [row["archive_sequence"] for row in rows] == [2, 3]
    assert rows[0]["tokenAmount"] == "1507931.704547"
    assert rows[0]["quoteInPool"] == "70.439722007"
    assert rows[0]["timestamp_ms"] == 1784759017536
    assert json.loads(rows[0]["participant_post_balances_json"]) == {
        "authority": {"quote": "0.5", "sol": "1.25", "token": "123.0"}
    }
    assert json.loads(rows[0]["token_extensions_json"]) == {"metadataPointer": {}}
    assert rows[0]["address_lookup_table_count"] == 1
    assert rows[0]["structural_raw_json"] is None
    assert rows[1]["structural_raw_json"] is not None


def test_iter_hours_requires_aligned_increasing_range() -> None:
    start = datetime(2026, 7, 1, tzinfo=UTC)
    assert list(iter_hours(start, datetime(2026, 7, 1, 2, tzinfo=UTC))) == [
        start,
        datetime(2026, 7, 1, 1, tzinfo=UTC),
    ]
