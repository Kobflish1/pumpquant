from __future__ import annotations

import base64
import gzip
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pumpquant.acquisition.block_archive import BlockArchiveConfig, archive_full_blocks
from pumpquant.acquisition.block_inventory_audit import audit_block_inventory
from pumpquant.acquisition.signature_inventory import (
    INVENTORY_COLUMNS,
    SignatureInventoryManifest,
    SignatureInventorySource,
    inventory_schema_sha256,
    manifest_bytes,
)
from pumpquant.acquisition.transaction_hydrator import HttpResponse, SolanaRpcConfig

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _base58(value: bytes) -> str:
    number = int.from_bytes(value, "big")
    chars: list[str] = []
    while number:
        number, remainder = divmod(number, 58)
        chars.append(ALPHABET[remainder])
    return "1" * (len(value) - len(value.lstrip(b"\x00"))) + "".join(reversed(chars))


def _inventory(tmp_path: Path, signatures: list[str]) -> Path:
    root = tmp_path / "inventory"
    root.mkdir()
    schema = pa.schema(
        [
            pa.field("signature", pa.string(), nullable=False),
            pa.field("first_slot", pa.int64()),
            pa.field("last_slot", pa.int64()),
            pa.field("first_block_timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("last_block_timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("instruction_count", pa.int64(), nullable=False),
            pa.field("program_ids", pa.list_(pa.string()), nullable=False),
            pa.field("source_manifests", pa.list_(pa.string()), nullable=False),
        ]
    )
    parquet_path = root / "signatures.parquet"
    rows = {
        "signature": signatures,
        "first_slot": [None] * len(signatures),
        "last_slot": [None] * len(signatures),
        "first_block_timestamp": [None] * len(signatures),
        "last_block_timestamp": [None] * len(signatures),
        "instruction_count": [1] * len(signatures),
        "program_ids": [[] for _ in signatures],
        "source_manifests": [["a" * 64] for _ in signatures],
    }
    table = pa.Table.from_pydict(rows, schema=schema)
    assert tuple(table.column_names) == INVENTORY_COLUMNS
    pq.write_table(table, parquet_path)
    source = SignatureInventorySource(
        "a" * 64, "source.json", "Instructions", "b" * 64, len(signatures)
    )
    payload = SignatureInventoryManifest(
        start=datetime(2026, 7, 15, tzinfo=UTC),
        end=datetime(2026, 7, 16, tzinfo=UTC),
        sources=(source,),
        inventory_row_count=len(signatures),
        inventory_byte_count=parquet_path.stat().st_size,
        inventory_sha256=hashlib.sha256(parquet_path.read_bytes()).hexdigest(),
        inventory_schema_sha256=inventory_schema_sha256(),
        source_row_count=len(signatures),
        included_row_count=len(signatures),
    )
    (root / "manifest.json").write_bytes(manifest_bytes(payload))
    return root


def _block(slot: int, transaction: str) -> dict[str, object]:
    return {
        "jsonrpc": "2.0",
        "id": f"block-{slot}",
        "result": {
            "blockHeight": slot,
            "blockTime": 1_700_000_000,
            "blockhash": f"hash-{slot}",
            "parentSlot": slot - 1,
            "previousBlockhash": f"hash-{slot - 1}",
            "rewards": [],
            "transactions": [{"meta": None, "transaction": [transaction, "base64"], "version": 0}],
        },
    }


def test_audit_extracts_gzip_wire_signature_and_reports_sets(tmp_path: Path) -> None:
    signature_bytes = bytes([7]) * 64
    signature = _base58(signature_bytes)
    missing = _base58(bytes([8]) * 64)
    serialized = b"\x01" + signature_bytes + b"message"
    body = json.dumps(
        _block(100, base64.b64encode(serialized).decode()), separators=(",", ":")
    ).encode()
    wire = HttpResponse(200, body, {"Content-Encoding": "gzip"}, gzip.compress(body, mtime=0))
    archive_root = tmp_path / "blocks"
    archive_full_blocks(
        [100],
        archive_root,
        SolanaRpcConfig("https://rpc.example.test"),
        BlockArchiveConfig(slots_per_segment=1, max_concurrency=1),
        transport=type("Transport", (), {"post": lambda self, payload, timeout: wire})(),
        sleeper=lambda _: None,
    )
    inventory = _inventory(tmp_path, [signature, missing])

    result = audit_block_inventory(inventory, archive_root, include_signature_details=True)
    payload = result.to_dict()
    assert payload["intersection_signatures"] == [signature]
    assert payload["missing_signatures"] == [missing]
    assert payload["extra_signatures"] == []
    assert payload["found_slots"] == [100]
    assert payload["totals"]["transaction_count"] == 1  # type: ignore[index]
    assert payload["totals"]["meta_null_count"] == 1  # type: ignore[index]


def test_audit_reconciles_missing_retry_with_found(tmp_path: Path) -> None:
    signature_bytes = bytes([9]) * 64
    signature = _base58(signature_bytes)
    serialized = b"\x01" + signature_bytes + b"message"
    block_body = json.dumps(
        _block(200, base64.b64encode(serialized).decode()), separators=(",", ":")
    ).encode()
    found = HttpResponse(200, block_body)
    null = HttpResponse(
        200,
        json.dumps({"jsonrpc": "2.0", "id": "block-200", "result": None}).encode(),
    )
    root = tmp_path / "blocks"
    config = BlockArchiveConfig(slots_per_segment=1, max_concurrency=1)
    rpc = SolanaRpcConfig("https://rpc.example.test")
    null_transport = type("Transport", (), {"post": lambda self, payload, timeout: null})()
    found_transport = type("Transport", (), {"post": lambda self, payload, timeout: found})()
    archive_full_blocks([200], root, rpc, config, transport=null_transport, sleeper=lambda _: None)
    archive_full_blocks([200], root, rpc, config, transport=found_transport, sleeper=lambda _: None)
    inventory = _inventory(tmp_path, [signature])

    payload = audit_block_inventory(inventory, root).to_dict()
    assert payload["found_slots"] == [200]
    assert payload["totals"]["found_slot_count"] == 1  # type: ignore[index]
    assert payload["totals"]["null_result_slot_count"] == 0  # type: ignore[index]
    assert payload["conflicts"]["slot_count"] == 0  # type: ignore[index]
    assert payload["per_slot"][0]["record_count"] == 2  # type: ignore[index]
