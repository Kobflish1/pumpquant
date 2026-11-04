"""Verified one-row-per-launch Pump/PumpSwap lifecycle catalogue.

The lifecycle sidecar is compact but event-shaped.  The opportunity atlas needs
an unconditional launch denominator with exact supply and completion,
migration, and pool anchors.  This module verifies the sealed sidecars through
the existing lifecycle reader, validates global identity/order, and publishes a
small Parquet catalogue without reading swap payloads.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from pumpquant.core.hashing import canonical_json, sha256_file
from pumpquant.features.lifecycle import (
    DEFAULT_BATCH_SIZE,
    LifecycleFeatureError,
    _lifecycle_row_value,
    _LifecycleIndex,
    _resolve_lifecycle_input,
    _row_int,
    _row_text,
    _timestamp_ns,
    _values_json,
)

try:  # Optional data dependency.
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pa = None
    pq = None


KIND = "pumpquant.lifecycle_catalog"
AUDIT_KIND = "pumpquant.lifecycle_catalog_audit"
VERSION = 1


class LifecycleCatalogError(LifecycleFeatureError):
    """A lifecycle identity, source, or publication invariant failed."""


def _slot(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LifecycleCatalogError(f"slot must be an integer, got {value!r}")
    return value


def _require_arrow() -> tuple[Any, Any]:
    if pa is None or pq is None:  # pragma: no cover
        raise LifecycleCatalogError("Parquet support requires: pip install 'pumpquant[data]'")
    return pa, pq


def _schema() -> Any:
    arrow, _ = _require_arrow()
    return arrow.schema(
        (
            arrow.field("mint", arrow.string(), nullable=False),
            arrow.field("name", arrow.string()),
            arrow.field("symbol", arrow.string()),
            arrow.field("uri", arrow.string()),
            arrow.field("creator", arrow.string()),
            arrow.field("launch_user", arrow.string()),
            arrow.field("bonding_curve", arrow.string()),
            arrow.field("quote_mint", arrow.string()),
            arrow.field("token_program", arrow.string()),
            arrow.field("token_total_supply_atomic", arrow.uint64()),
            arrow.field("virtual_token_reserves_atomic", arrow.uint64()),
            arrow.field("virtual_quote_reserves_atomic", arrow.uint64()),
            arrow.field("real_token_reserves_atomic", arrow.uint64()),
            arrow.field("is_mayhem_mode", arrow.bool_()),
            arrow.field("is_cashback_enabled", arrow.bool_()),
            arrow.field("launch_event_id", arrow.string(), nullable=False),
            arrow.field("launch_slot", arrow.int64(), nullable=False),
            arrow.field("launch_tx_index", arrow.int32(), nullable=False),
            arrow.field("launch_log_index", arrow.int32(), nullable=False),
            arrow.field("launch_time_ns", arrow.int64(), nullable=False),
            arrow.field("complete_event_id", arrow.string()),
            arrow.field("complete_slot", arrow.int64()),
            arrow.field("complete_time_ns", arrow.int64()),
            arrow.field("migration_event_id", arrow.string()),
            arrow.field("migration_slot", arrow.int64()),
            arrow.field("migration_time_ns", arrow.int64()),
            arrow.field("migration_user", arrow.string()),
            arrow.field("migration_pool", arrow.string()),
            arrow.field("migration_mint_amount_atomic", arrow.uint64()),
            arrow.field("migration_quote_amount_atomic", arrow.uint64()),
            arrow.field("pool_migration_fee_atomic", arrow.uint64()),
            arrow.field("pool_event_id", arrow.string()),
            arrow.field("pool_slot", arrow.int64()),
            arrow.field("pool_time_ns", arrow.int64()),
            arrow.field("pool", arrow.string()),
            arrow.field("pool_index", arrow.int32()),
            arrow.field("pool_creator", arrow.string()),
            arrow.field("coin_creator", arrow.string()),
            arrow.field("pool_orientation_flipped", arrow.bool_()),
            arrow.field("base_decimals", arrow.int16()),
            arrow.field("quote_decimals", arrow.int16()),
            arrow.field("pool_base_amount_atomic", arrow.uint64()),
            arrow.field("pool_quote_amount_atomic", arrow.uint64()),
            arrow.field("initial_liquidity_atomic", arrow.uint64()),
            arrow.field("minimum_liquidity_atomic", arrow.uint64()),
            arrow.field("lp_token_amount_out_atomic", arrow.uint64()),
            arrow.field("lp_mint", arrow.string()),
            arrow.field("completed_bonding_curve", arrow.bool_(), nullable=False),
            arrow.field("migrated_to_pumpswap", arrow.bool_(), nullable=False),
            arrow.field("created_pumpswap_pool", arrow.bool_(), nullable=False),
        )
    )


CATALOG_SCHEMA = _schema() if pa is not None else None


def _write_bytes(path: Path, body: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())


def _event_name(row: Mapping[str, object]) -> str:
    return (_row_text(row, ("event_name", "event_type")) or "").lower().replace("_", "")


def _value(
    row: Mapping[str, object],
    values: Mapping[str, object],
    *names: str,
) -> object | None:
    return _lifecycle_row_value(row, values, names)


def _text_value(
    row: Mapping[str, object],
    values: Mapping[str, object],
    *names: str,
) -> str | None:
    value = _value(row, values, *names)
    return value if isinstance(value, str) and value else None


def _int_value(
    row: Mapping[str, object],
    values: Mapping[str, object],
    *names: str,
) -> int | None:
    value = _value(row, values, *names)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LifecycleCatalogError(f"lifecycle field {names[0]} must be a non-negative integer")
    return value


def _bool_value(
    row: Mapping[str, object],
    values: Mapping[str, object],
    *names: str,
) -> bool | None:
    value = _value(row, values, *names)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise LifecycleCatalogError(f"lifecycle field {names[0]} must be boolean")
    return value


def _identity(row: Mapping[str, object]) -> tuple[str, int, int, int, int]:
    event_id = _row_text(row, ("lifecycle_event_id", "event_id"))
    slot = _row_int(row, ("slot",), "slot", required=True)
    tx_index = _row_int(row, ("tx_index",), "tx_index", required=True)
    log_index = _row_int(row, ("log_index", "instruction_index"), "log_index")
    event_index = _row_int(row, ("event_index",), "event_index")
    if event_id is None or slot is None or tx_index is None:
        raise LifecycleCatalogError("lifecycle row lacks event identity")
    return event_id, slot, tx_index, log_index or 0, event_index or 0


def _put_once(
    target: dict[str, dict[str, object]],
    mint: str,
    payload: dict[str, object],
    kind: str,
) -> None:
    prior = target.get(mint)
    if prior is not None:
        raise LifecycleCatalogError(
            f"multiple {kind} events for mint {mint}: "
            f"{prior.get(kind + '_event_id')} and {payload.get(kind + '_event_id')}"
        )
    target[mint] = payload


def _launch_payload(row: Mapping[str, object]) -> tuple[str, dict[str, object]]:
    values = _values_json(row)
    mint = _text_value(row, values, "mint")
    if mint is None:
        raise LifecycleCatalogError("CreateEvent has no mint")
    event_id, slot, tx_index, log_index, _event_index = _identity(row)
    timestamp = _timestamp_ns(row)
    if timestamp is None or timestamp < 0:
        raise LifecycleCatalogError(f"CreateEvent has no valid block timestamp: {event_id}")
    return mint, {
        "mint": mint,
        "name": _text_value(row, values, "name"),
        "symbol": _text_value(row, values, "symbol"),
        "uri": _text_value(row, values, "uri"),
        "creator": _text_value(row, values, "creator"),
        "launch_user": _text_value(row, values, "user"),
        "bonding_curve": _text_value(row, values, "bonding_curve"),
        "quote_mint": _text_value(row, values, "quote_mint"),
        "token_program": _text_value(row, values, "token_program"),
        "token_total_supply_atomic": _int_value(row, values, "token_total_supply"),
        "virtual_token_reserves_atomic": _int_value(row, values, "virtual_token_reserves"),
        "virtual_quote_reserves_atomic": _int_value(
            row, values, "virtual_quote_reserves", "virtual_sol_reserves"
        ),
        "real_token_reserves_atomic": _int_value(row, values, "real_token_reserves"),
        "is_mayhem_mode": _bool_value(row, values, "is_mayhem_mode"),
        "is_cashback_enabled": _bool_value(row, values, "is_cashback_enabled"),
        "launch_event_id": event_id,
        "launch_slot": slot,
        "launch_tx_index": tx_index,
        "launch_log_index": log_index,
        "launch_time_ns": timestamp,
    }


def _anchor_payload(
    row: Mapping[str, object],
    kind: str,
    *,
    launch_mints: set[str] | None = None,
) -> tuple[str, dict[str, object]] | None:
    values = _values_json(row)
    pool_flipped = False
    if kind == "pool":
        raw_base_mint = _text_value(row, values, "base_mint")
        raw_quote_mint = _text_value(row, values, "quote_mint")
        known = set() if launch_mints is None else launch_mints
        base_is_launch = raw_base_mint in known
        quote_is_launch = raw_quote_mint in known
        if base_is_launch and quote_is_launch:
            raise LifecycleCatalogError(
                "PumpSwap pool contains two development launch mints: "
                f"{raw_base_mint} and {raw_quote_mint}"
            )
        if quote_is_launch:
            mint = raw_quote_mint
            pool_flipped = True
        elif base_is_launch:
            mint = raw_base_mint
        else:
            # The sidecar also contains pools for tokens outside this
            # development launch denominator (for example established USDC
            # markets). They are valid lifecycle events but not atlas rows.
            return None
    else:
        mint = _text_value(row, values, "mint")
    if mint is None:
        raise LifecycleCatalogError(f"{kind} event has no mint")
    event_id, slot, _tx_index, _log_index, _event_index = _identity(row)
    timestamp = _timestamp_ns(row)
    payload: dict[str, object] = {
        f"{kind}_event_id": event_id,
        f"{kind}_slot": slot,
        f"{kind}_time_ns": timestamp,
    }
    if kind == "migration":
        payload.update(
            {
                "migration_user": _text_value(row, values, "user"),
                "migration_pool": _text_value(row, values, "pool"),
                "migration_mint_amount_atomic": _int_value(row, values, "mint_amount"),
                "migration_quote_amount_atomic": _int_value(
                    row, values, "sol_amount", "quote_amount"
                ),
                "pool_migration_fee_atomic": _int_value(row, values, "pool_migration_fee"),
            }
        )
    elif kind == "pool":
        raw_base_decimals = _int_value(row, values, "base_mint_decimals", "base_decimals")
        raw_quote_decimals = _int_value(row, values, "quote_mint_decimals", "quote_decimals")
        raw_base_amount = _int_value(row, values, "pool_base_amount")
        raw_quote_amount = _int_value(row, values, "pool_quote_amount")
        payload.update(
            {
                "pool": _text_value(row, values, "pool"),
                "pool_index": _int_value(row, values, "index"),
                "pool_creator": _text_value(row, values, "creator"),
                "coin_creator": _text_value(row, values, "coin_creator"),
                "pool_orientation_flipped": pool_flipped,
                "base_decimals": raw_quote_decimals if pool_flipped else raw_base_decimals,
                "quote_decimals": raw_base_decimals if pool_flipped else raw_quote_decimals,
                "pool_base_amount_atomic": raw_quote_amount if pool_flipped else raw_base_amount,
                "pool_quote_amount_atomic": raw_base_amount if pool_flipped else raw_quote_amount,
                "initial_liquidity_atomic": _int_value(row, values, "initial_liquidity"),
                "minimum_liquidity_atomic": _int_value(row, values, "minimum_liquidity"),
                "lp_token_amount_out_atomic": _int_value(row, values, "lp_token_amount_out"),
                "lp_mint": _text_value(row, values, "lp_mint"),
            }
        )
    return mint, payload


def materialize_lifecycle_catalog(
    inputs: Sequence[str | Path] | str | Path,
    output: str | Path,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, object]:
    """Verify lifecycle inputs and atomically publish one row per Pump launch."""

    arrow, parquet_api = _require_arrow()
    values = (inputs,) if isinstance(inputs, (str, Path)) else tuple(inputs)
    if not values:
        raise LifecycleCatalogError("at least one lifecycle input is required")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
        raise LifecycleCatalogError("batch_size must be a positive integer")
    resolved = tuple(_resolve_lifecycle_input(value) for value in values)
    target = Path(output).expanduser().resolve()
    if target.exists():
        raise FileExistsError(f"refusing to overwrite {target}")
    if target.parent.is_symlink():
        raise LifecycleCatalogError("output parent must not be a symlink")
    target.parent.mkdir(parents=True, exist_ok=True)

    launches: dict[str, dict[str, object]] = {}
    completions: dict[str, dict[str, object]] = {}
    migrations: dict[str, dict[str, object]] = {}
    pool_candidates: dict[str, list[dict[str, object]]] = {}
    counts: Counter[str] = Counter()
    rows_read = 0
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.tmp-", dir=target.parent))
    try:
        index = _LifecycleIndex(staging / "identity.sqlite")
        try:
            for source in resolved:
                source_rows = 0
                for shard in source.shards:
                    parquet = parquet_api.ParquetFile(shard.path)
                    try:
                        for batch in parquet.iter_batches(batch_size=batch_size, use_threads=False):
                            for raw in batch.to_pylist():
                                if not isinstance(raw, Mapping):
                                    raise LifecycleCatalogError(
                                        "lifecycle Parquet row must be an object"
                                    )
                                row = cast(Mapping[str, object], raw)
                                index.add_row(row)
                                name = _event_name(row)
                                counts[name or "unknown"] += 1
                                if name == "createevent":
                                    mint, payload = _launch_payload(row)
                                    _put_once(launches, mint, payload, "launch")
                                elif name == "completeevent":
                                    anchor = _anchor_payload(row, "complete")
                                    assert anchor is not None
                                    mint, payload = anchor
                                    _put_once(completions, mint, payload, "complete")
                                elif name == "completepumpammmigrationevent":
                                    anchor = _anchor_payload(row, "migration")
                                    assert anchor is not None
                                    mint, payload = anchor
                                    _put_once(migrations, mint, payload, "migration")
                                elif name == "createpoolevent":
                                    anchor = _anchor_payload(
                                        row,
                                        "pool",
                                        launch_mints=set(launches),
                                    )
                                    if anchor is None:
                                        counts["createpoolevent_outside_launch_population"] += 1
                                        rows_read += 1
                                        source_rows += 1
                                        continue
                                    mint, payload = anchor
                                    # The canonical PumpSwap pool is index zero. Ignore nonzero
                                    # pools rather than allowing them to overwrite that boundary.
                                    if payload.get("pool_index") in (None, 0):
                                        pool_candidates.setdefault(mint, []).append(payload)
                                rows_read += 1
                                source_rows += 1
                    finally:
                        parquet.close()
                if source_rows != source.row_count:
                    raise LifecycleCatalogError(
                        "streamed lifecycle row count differs from manifest"
                    )
            index.commit()
        finally:
            index.close()

        pools: dict[str, dict[str, object]] = {}
        for mint, candidates in pool_candidates.items():
            migration_pool = migrations.get(mint, {}).get("migration_pool")
            matches = [
                candidate
                for candidate in candidates
                if migration_pool is not None and candidate.get("pool") == migration_pool
            ]
            counts["createpoolevent_noncanonical_pool"] += len(candidates) - len(matches)
            if len(matches) > 1:
                raise LifecycleCatalogError(
                    f"multiple canonical pool events for mint {mint}: "
                    + ", ".join(str(match.get("pool_event_id")) for match in matches)
                )
            if matches:
                pools[mint] = matches[0]

        output_rows: list[dict[str, object]] = []
        for mint, launch in sorted(
            launches.items(), key=lambda item: (_slot(item[1]["launch_slot"]), item[0])
        ):
            complete = completions.get(mint, {})
            migration = migrations.get(mint, {})
            pool = pools.get(mint, {})
            for boundary, label in (
                (complete, "complete"),
                (migration, "migration"),
                (pool, "pool"),
            ):
                boundary_slot = boundary.get(f"{label}_slot")
                if boundary_slot is not None and _slot(boundary_slot) < _slot(
                    launch["launch_slot"]
                ):
                    raise LifecycleCatalogError(f"{label} precedes launch for mint {mint}")
            output_rows.append(
                {
                    **launch,
                    **complete,
                    **migration,
                    **pool,
                    "completed_bonding_curve": bool(complete),
                    "migrated_to_pumpswap": bool(migration),
                    "created_pumpswap_pool": bool(pool),
                }
            )

        schema = CATALOG_SCHEMA
        assert schema is not None
        catalog_path = staging / "tokens.parquet"
        parquet_api.write_table(
            arrow.Table.from_pylist(output_rows, schema=schema),
            catalog_path,
            compression="zstd",
        )
        catalog_size = catalog_path.stat().st_size
        catalog_hash = sha256_file(catalog_path)
        orphan_counts = {
            "complete": len(set(completions) - set(launches)),
            "migration": len(set(migrations) - set(launches)),
            "pool": len(set(pools) - set(launches)),
        }
        audit = {
            "kind": AUDIT_KIND,
            "version": VERSION,
            "status": "complete",
            "input_count": len(resolved),
            "lifecycle_rows_read": rows_read,
            "event_counts": dict(sorted(counts.items())),
            "launch_count": len(launches),
            "complete_count": sum(mint in completions for mint in launches),
            "migration_count": sum(mint in migrations for mint in launches),
            "pool_count": sum(mint in pools for mint in launches),
            "orphan_anchor_counts": orphan_counts,
        }
        audit_body = canonical_json(audit)
        _write_bytes(staging / "audit.json", audit_body)
        manifest = {
            "kind": KIND,
            "version": VERSION,
            "status": "complete",
            "row_count": len(output_rows),
            "physical_order": "launch_slot,mint",
            "sources": [
                {
                    "manifest": str(source.manifest_path),
                    "manifest_sha256": source.manifest_sha256,
                    "manifest_kind": source.manifest_kind,
                    "row_count": source.row_count,
                    "schema_sha256": source.schema_sha256,
                    "shards": [
                        {
                            "path": shard.relative_path,
                            "row_count": shard.row_count,
                            "byte_count": shard.byte_count,
                            "sha256": shard.sha256,
                        }
                        for shard in source.shards
                    ],
                }
                for source in resolved
            ],
            "schema_sha256": hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
            "files": {
                "tokens.parquet": {
                    "byte_count": catalog_size,
                    "row_count": len(output_rows),
                    "sha256": catalog_hash,
                },
                "audit.json": {
                    "byte_count": len(audit_body),
                    "sha256": hashlib.sha256(audit_body).hexdigest(),
                },
            },
            "truth_boundaries": [
                "one row is emitted for every decoded Pump CreateEvent",
                "completion, migration, and canonical PumpSwap pool creation are outcomes",
                "no first-observed swap fallback is used",
                "every lifecycle source byte is manifest-verified before use",
            ],
        }
        _write_bytes(staging / "manifest.json", canonical_json(manifest))
        (staging / "identity.sqlite").unlink(missing_ok=True)
        os.replace(staging, target)
        parent_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        return manifest
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


__all__ = [
    "AUDIT_KIND",
    "CATALOG_SCHEMA",
    "KIND",
    "LifecycleCatalogError",
    "VERSION",
    "materialize_lifecycle_catalog",
]
