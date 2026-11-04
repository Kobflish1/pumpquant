"""Bounded launch/lifecycle features for normalised Pump/PumpSwap swaps.

The normaliser and the archive adapters publish different
datasets.  This is the small, deterministic bridge between them: it
indexes only lifecycle anchors in a disk-backed SQLite database and then
streams the normalised swaps in chain order.  No complete 30-day swap table is
materialised in memory.

Lifecycle input may be either an adapter ``pumpquant.trade_event_dataset``
manifest (with ``lifecycle_shards``) or a destination manifest containing the
``lifecycle_event_dataset`` summary.  Every manifested byte is checked before
it is read.  The output is replaced only after Parquet, audit, and manifest
files have all been sealed in a staging directory.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from pumpquant.core.hashing import canonical_json, sha256_bytes, sha256_file
from pumpquant.core.io import iter_swap_event_batches
from pumpquant.core.schema import SwapEvent, VenuePhase

try:  # Optional data dependency; imported lazily by callers that need Parquet.
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - exercised only without pumpquant[data]
    pa = None
    pq = None


FEATURES_KIND = "pumpquant.lifecycle_feature_dataset"
AUDIT_KIND = "pumpquant.lifecycle_feature_audit"
SCHEMA_VERSION = 1
NORMALIZED_KIND = "pumpquant.normalized_swap_dataset"
TRADE_EVENT_KIND = "pumpquant.trade_event_dataset"
PHYSICAL_ORDER = ("slot", "tx_index", "instruction_index", "event_id")
LIFECYCLE_PHYSICAL_ORDER = ("slot", "tx_index", "log_index", "event_index", "event_id")
DEFAULT_BATCH_SIZE = 32_768


class LifecycleFeatureError(RuntimeError):
    """A source, identity, lifecycle, or publication invariant failed."""


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise LifecycleFeatureError(f"{label} must be an object with text keys")
    return cast(Mapping[str, object], value)


def _text(value: object, label: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise LifecycleFeatureError(f"{label} must be non-empty text")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LifecycleFeatureError(f"{label} must be a non-negative integer")
    return value


def _optional_nonnegative_int(value: object, label: str) -> int | None:
    if value is None:
        return None
    return _nonnegative_int(value, label)


def _safe_relative(root: Path, value: object, label: str) -> Path:
    relative = Path(_text(value, label))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise LifecycleFeatureError(f"{label} escapes its dataset")
    unresolved = root / relative
    current = root
    for component in relative.parts:
        current = current / component
        if current.is_symlink():
            raise LifecycleFeatureError(f"{label} traverses a symlink")
    candidate = unresolved.resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as error:
        raise LifecycleFeatureError(f"{label} escapes its dataset") from error
    return candidate


def _require_arrow() -> tuple[Any, Any]:
    if pa is None or pq is None:  # pragma: no cover - optional dependency
        raise LifecycleFeatureError("Parquet support requires: pip install 'pumpquant[data]'")
    return pa, pq


def _schema_hash(schema: Any) -> str:
    return sha256_bytes(schema.serialize().to_pybytes())


def _json_object(path: Path, label: str) -> tuple[Mapping[str, object], str, bytes]:
    try:
        raw = path.read_bytes()
        payload = _mapping(json.loads(raw), label)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LifecycleFeatureError(f"{label} is invalid JSON: {path}") from error
    return payload, sha256_bytes(raw), raw


@dataclass(frozen=True, slots=True)
class _NormalizedInput:
    root: Path
    parquet_path: Path
    manifest_path: Path
    manifest_sha256: str
    swaps_sha256: str
    row_count: int


@dataclass(frozen=True, slots=True)
class _LifecycleShard:
    path: Path
    relative_path: str
    row_count: int
    byte_count: int
    sha256: str


@dataclass(frozen=True, slots=True)
class _LifecycleInput:
    root: Path
    manifest_path: Path
    manifest_sha256: str
    schema_sha256: str | None
    row_count: int
    shards: tuple[_LifecycleShard, ...]
    manifest_kind: str


def _validate_manifest_file(
    root: Path,
    path: Path,
    metadata: Mapping[str, object],
    label: str,
) -> tuple[int, str]:
    byte_count = _nonnegative_int(metadata.get("byte_count"), f"{label} byte_count")
    digest = _text(metadata.get("sha256"), f"{label} sha256")
    if len(digest) != 64:
        raise LifecycleFeatureError(f"{label} sha256 must be SHA-256 hex")
    if path.is_symlink() or not path.is_file():
        raise LifecycleFeatureError(f"{label} is missing or not a regular file: {path}")
    if path.stat().st_size != byte_count or sha256_file(path) != digest:
        raise LifecycleFeatureError(f"{label} failed byte/hash verification")
    try:
        path.relative_to(root)
    except ValueError as error:
        raise LifecycleFeatureError(f"{label} escapes its dataset") from error
    return byte_count, digest


def _files_mapping(manifest: Mapping[str, object]) -> Mapping[str, object]:
    raw_files = manifest.get("files")
    if isinstance(raw_files, Mapping):
        return cast(Mapping[str, object], raw_files)
    # Destination manifests use a list of {path, byte_count, sha256} entries.
    if isinstance(raw_files, Sequence) and not isinstance(raw_files, (str, bytes, bytearray)):
        result: dict[str, object] = {}
        for index, value in enumerate(raw_files):
            item = _mapping(value, f"manifest file {index}")
            relative = _text(item.get("path"), f"manifest file {index} path")
            result[relative] = item
        return result
    return {}


def _manifest_shards(
    root: Path,
    manifest: Mapping[str, object],
    *,
    destination_summary: Mapping[str, object] | None,
) -> tuple[tuple[_LifecycleShard, ...], int, str | None]:
    source = manifest if destination_summary is None else destination_summary
    raw_shards = source.get("lifecycle_shards", source.get("shards"))
    if not isinstance(raw_shards, Sequence) or isinstance(raw_shards, (str, bytes, bytearray)):
        raise LifecycleFeatureError("lifecycle manifest has no shard sequence")
    declared_count = source.get("lifecycle_row_count", source.get("row_count"))
    row_count = _nonnegative_int(declared_count, "lifecycle row_count")
    declared_shards = source.get("lifecycle_shard_count", source.get("shard_count"))
    shard_count = _nonnegative_int(declared_shards, "lifecycle shard_count")
    if shard_count != len(raw_shards):
        raise LifecycleFeatureError("lifecycle shard count differs from shard list")
    files = _files_mapping(manifest)
    shards: list[_LifecycleShard] = []
    total_rows = 0
    for index, raw in enumerate(raw_shards):
        item = _mapping(raw, f"lifecycle shard {index}")
        relative_value = item.get("path")
        relative = _text(relative_value, f"lifecycle shard {index} path")
        path = _safe_relative(root, relative, f"lifecycle shard {index} path")
        if (
            destination_summary is not None
            and not path.exists()
            and not relative.startswith("trade-events/")
        ):
            # The destination wrapper keeps the nested trade-event tree under
            # ``trade-events/`` while retaining adapter-relative shard paths.
            nested_relative = f"trade-events/{relative}"
            nested_path = _safe_relative(root, nested_relative, f"lifecycle shard {index} path")
            if nested_path.exists():
                path = nested_path
        metadata: Mapping[str, object] = item
        file_metadata = next(
            (
                files.get(candidate)
                for candidate in (
                    relative,
                    f"trade-events/{relative}",
                    relative.removeprefix("trade-events/"),
                )
                if files.get(candidate) is not None
            ),
            None,
        )
        if file_metadata is not None:
            metadata = _mapping(file_metadata, f"lifecycle shard {index} file metadata")
        byte_count, digest = _validate_manifest_file(
            root, path, metadata, f"lifecycle shard {index}"
        )
        shard_rows = _nonnegative_int(item.get("row_count"), f"lifecycle shard {index} row_count")
        if "byte_count" in item and item.get("byte_count") != byte_count:
            raise LifecycleFeatureError(f"lifecycle shard {index} byte_count differs")
        if "sha256" in item and item.get("sha256") != digest:
            raise LifecycleFeatureError(f"lifecycle shard {index} sha256 differs")
        parquet = pq.ParquetFile(path)
        try:
            metadata_rows = 0 if parquet.metadata is None else int(parquet.metadata.num_rows)
            if metadata_rows != shard_rows:
                raise LifecycleFeatureError(
                    f"lifecycle shard {index} row_count differs from Parquet"
                )
        finally:
            parquet.close()
        total_rows += shard_rows
        shards.append(
            _LifecycleShard(
                path=path,
                relative_path=relative,
                row_count=shard_rows,
                byte_count=byte_count,
                sha256=digest,
            )
        )
    if total_rows != row_count:
        raise LifecycleFeatureError("lifecycle row_count differs from shard rows")
    # Trade-event manifests carry both the full event-dataset schema hash and
    # the lifecycle sidecar schema hash.  The shard sequence above is the
    # lifecycle sequence, so the lifecycle-specific declaration must win when
    # both fields are present.  Keep the generic key as a compatibility
    # fallback for manifests whose shard sequence is itself the declared
    # dataset (for example a destination summary from an older v1 publisher).
    schema_sha = source.get("lifecycle_schema_sha256")
    if schema_sha is None:
        schema_sha = source.get("schema_sha256")
    if schema_sha is not None:
        schema_sha = _text(schema_sha, "lifecycle schema_sha256")
        if len(schema_sha) != 64:
            raise LifecycleFeatureError("lifecycle schema_sha256 must be SHA-256 hex")
    return tuple(shards), row_count, schema_sha


def _resolve_normalized_input(path: str | Path) -> _NormalizedInput:
    source = Path(path).expanduser().resolve()
    if source.is_dir():
        root = source
        manifest_path = root / "manifest.json"
        parquet_path = root / "swaps.parquet"
    elif source.is_file() and source.suffix.lower() == ".parquet":
        root = source.parent
        manifest_path = root / "manifest.json"
        parquet_path = source
    else:
        raise LifecycleFeatureError(
            f"normalized input is not a directory or Parquet file: {source}"
        )
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise LifecycleFeatureError("normalized input must have a sealed manifest.json")
    manifest, manifest_sha, _ = _json_object(manifest_path, "normalized manifest")
    if manifest.get("kind") != NORMALIZED_KIND or manifest.get("version") != SCHEMA_VERSION:
        raise LifecycleFeatureError("input is not a version-1 normalized swap dataset")
    declared_order = manifest.get("physical_order")
    if declared_order is not None and (
        not isinstance(declared_order, Sequence)
        or isinstance(declared_order, (str, bytes, bytearray))
        or tuple(declared_order) != PHYSICAL_ORDER
    ):
        raise LifecycleFeatureError("normalized input physical order is not canonical")
    raw_files = _files_mapping(manifest)
    metadata: Mapping[str, object] | None = None
    for key in ("swaps.parquet", parquet_path.name):
        candidate = raw_files.get(key)
        if candidate is not None:
            metadata = _mapping(candidate, "normalized swaps file metadata")
            declared_path = metadata.get("path")
            if declared_path is not None:
                parquet_path = _safe_relative(root, declared_path, "normalized swaps path")
            break
    if metadata is None:
        raise LifecycleFeatureError("normalized manifest has no swaps.parquet metadata")
    byte_count, digest = _validate_manifest_file(root, parquet_path, metadata, "normalized swaps")
    parquet = pq.ParquetFile(parquet_path)
    try:
        row_count = 0 if parquet.metadata is None else int(parquet.metadata.num_rows)
        declared_schema = manifest.get("swap_schema_sha256")
        if declared_schema is not None:
            declared_schema = _text(declared_schema, "normalized swap_schema_sha256")
            if len(declared_schema) != 64 or _schema_hash(parquet.schema_arrow) != declared_schema:
                raise LifecycleFeatureError("normalized swap schema hash does not match manifest")
    finally:
        parquet.close()
    declared_rows = _nonnegative_int(manifest.get("row_count"), "normalized row_count")
    if declared_rows != row_count:
        raise LifecycleFeatureError("normalized row_count differs from Parquet")
    if byte_count != parquet_path.stat().st_size or digest != sha256_file(parquet_path):
        raise LifecycleFeatureError("normalized swaps changed while being opened")
    return _NormalizedInput(
        root=root,
        parquet_path=parquet_path,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
        swaps_sha256=digest,
        row_count=row_count,
    )


def _resolve_lifecycle_input(path: str | Path) -> _LifecycleInput:
    source = Path(path).expanduser().resolve()
    if source.is_file() and source.name == "manifest.json":
        root = source.parent
        manifest_path = source
    elif source.is_dir():
        root = source
        manifest_path = root / "manifest.json"
    else:
        raise LifecycleFeatureError(f"lifecycle input is not a dataset directory: {source}")
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise LifecycleFeatureError("lifecycle input must have a sealed manifest.json")
    manifest, manifest_sha, _ = _json_object(manifest_path, "lifecycle manifest")
    kind = _text(manifest.get("kind"), "lifecycle manifest kind")
    summary: Mapping[str, object] | None = None
    identity_manifest_path = manifest_path
    identity_manifest_sha = manifest_sha
    shard_manifest = manifest
    if "lifecycle_event_dataset" in manifest:
        summary = _mapping(manifest.get("lifecycle_event_dataset"), "lifecycle event dataset")
        nested_manifest_value = summary.get("manifest_path")
        if nested_manifest_value is not None:
            nested_manifest_path = _safe_relative(
                root, nested_manifest_value, "lifecycle manifest path"
            )
            if nested_manifest_path.exists():
                nested_manifest, nested_sha, _ = _json_object(
                    nested_manifest_path, "nested lifecycle manifest"
                )
                nested_kind = _text(nested_manifest.get("kind"), "nested lifecycle manifest kind")
                if nested_kind != TRADE_EVENT_KIND:
                    raise LifecycleFeatureError(
                        "nested lifecycle manifest is not a trade-event dataset"
                    )
                declared_nested_sha = summary.get("manifest_sha256")
                if (
                    declared_nested_sha is not None
                    and _text(declared_nested_sha, "nested lifecycle manifest_sha256") != nested_sha
                ):
                    raise LifecycleFeatureError(
                        "nested lifecycle manifest hash does not match wrapper"
                    )
                shard_manifest = nested_manifest
                identity_manifest_path = nested_manifest_path
                identity_manifest_sha = nested_sha
    elif kind not in {
        TRADE_EVENT_KIND,
        "pumpquant.old_faithful_backtester_chunk",
        "pumpquant.rpc_gap_backtester_chunk",
    }:
        raise LifecycleFeatureError(f"unsupported lifecycle manifest kind: {kind}")
    # Destination manifests describe files relative to the destination root;
    # adapter manifests describe files relative to the trade-event root.  A
    # caller may pass the nested ``trade-events`` directory directly, in which
    # case the exact-path behaviour is naturally selected.
    shards, row_count, schema_sha = _manifest_shards(
        root, shard_manifest, destination_summary=summary
    )
    if not shards and row_count != 0:
        raise LifecycleFeatureError("lifecycle manifest has rows but no shards")
    if schema_sha is not None:
        for shard in shards:
            parquet = pq.ParquetFile(shard.path)
            try:
                if _schema_hash(parquet.schema_arrow) != schema_sha:
                    raise LifecycleFeatureError("lifecycle schema hash does not match shard")
            finally:
                parquet.close()
    return _LifecycleInput(
        root=root,
        manifest_path=identity_manifest_path,
        manifest_sha256=identity_manifest_sha,
        schema_sha256=schema_sha,
        row_count=row_count,
        shards=shards,
        manifest_kind=_text(shard_manifest.get("kind"), "lifecycle manifest kind"),
    )


def _feature_schema() -> Any:
    arrow, _ = _require_arrow()
    return arrow.schema(
        (
            arrow.field("schema_version", arrow.int16(), nullable=False),
            arrow.field("record_type", arrow.string(), nullable=False),
            arrow.field("event_id", arrow.string(), nullable=False),
            arrow.field("market_id", arrow.string(), nullable=False),
            arrow.field("mint", arrow.string(), nullable=False),
            arrow.field("quote_mint", arrow.string(), nullable=False),
            arrow.field("slot", arrow.int64(), nullable=False),
            arrow.field("tx_index", arrow.int32(), nullable=False),
            arrow.field("instruction_index", arrow.int32(), nullable=False),
            arrow.field("ordering_key", arrow.string(), nullable=False),
            arrow.field("block_time_ns", arrow.int64(), nullable=False),
            arrow.field("observed_at_ns", arrow.int64(), nullable=False),
            arrow.field("phase", arrow.string(), nullable=False),
            arrow.field("venue_phase", arrow.string(), nullable=False),
            arrow.field("venue", arrow.string(), nullable=False),
            arrow.field("side", arrow.string(), nullable=False),
            arrow.field("base_amount", arrow.float64(), nullable=False),
            arrow.field("quote_amount", arrow.float64(), nullable=False),
            arrow.field("price", arrow.float64(), nullable=False),
            arrow.field("virtual_base_reserve", arrow.float64(), nullable=False),
            arrow.field("virtual_quote_reserve", arrow.float64(), nullable=False),
            arrow.field("real_base_reserve", arrow.float64(), nullable=False),
            arrow.field("real_quote_reserve", arrow.float64(), nullable=False),
            arrow.field("trader", arrow.string()),
            arrow.field("venue_fee_bps", arrow.float64()),
            arrow.field("protocol_fee_quote", arrow.float64(), nullable=False),
            arrow.field("creator_fee_quote", arrow.float64(), nullable=False),
            arrow.field("lp_fee_quote", arrow.float64(), nullable=False),
            arrow.field("cashback_quote", arrow.float64(), nullable=False),
            arrow.field("buyback_fee_quote", arrow.float64(), nullable=False),
            arrow.field("signature", arrow.string()),
            arrow.field("program_version", arrow.string()),
            arrow.field("base_decimals", arrow.int16()),
            arrow.field("quote_decimals", arrow.int16()),
            arrow.field("launch_slot", arrow.int64()),
            arrow.field("launch_tx_index", arrow.int32()),
            arrow.field("launch_instruction_index", arrow.int32()),
            arrow.field("launch_block_time_ns", arrow.int64()),
            arrow.field("launch_transition_id", arrow.string()),
            arrow.field("launch_event_id", arrow.string()),
            arrow.field("pool_creation_slot", arrow.int64()),
            arrow.field("pool_creation_tx_index", arrow.int32()),
            arrow.field("pool_creation_instruction_index", arrow.int32()),
            arrow.field("pool_creation_block_time_ns", arrow.int64()),
            arrow.field("pool_creation_transition_id", arrow.string()),
            arrow.field("pool_creation_event_id", arrow.string()),
            arrow.field("pool_create_transition_id", arrow.string()),
            arrow.field("pool_id", arrow.string()),
            arrow.field("graduation_slot", arrow.int64()),
            arrow.field("graduation_tx_index", arrow.int32()),
            arrow.field("graduation_instruction_index", arrow.int32()),
            arrow.field("graduation_block_time_ns", arrow.int64()),
            arrow.field("graduation_transition_id", arrow.string()),
            arrow.field("graduation_event_id", arrow.string()),
            arrow.field("age_slots", arrow.int64()),
            arrow.field("token_age_slots", arrow.int64()),
            arrow.field("age_seconds", arrow.float64()),
            arrow.field("token_age_seconds", arrow.float64()),
            arrow.field("pool_age_slots", arrow.int64()),
            arrow.field("pool_age_seconds", arrow.float64()),
            arrow.field("is_pre_graduation", arrow.bool_()),
            arrow.field("is_post_graduation", arrow.bool_()),
            arrow.field("pre_graduation", arrow.bool_()),
            arrow.field("post_graduation", arrow.bool_()),
            arrow.field("graduation_state", arrow.string(), nullable=False),
            arrow.field("graduation_phase", arrow.string(), nullable=False),
            arrow.field("lifecycle_state", arrow.string(), nullable=False),
            arrow.field("launch_anchor_status", arrow.string(), nullable=False),
            arrow.field("pool_anchor_status", arrow.string(), nullable=False),
            arrow.field("graduation_anchor_status", arrow.string(), nullable=False),
            arrow.field("lifecycle_anchor_status", arrow.string(), nullable=False),
            arrow.field("lifecycle_anchor_missing_reason", arrow.string()),
            arrow.field("missing_launch_anchor", arrow.bool_(), nullable=False),
            arrow.field("missing_pool_anchor", arrow.bool_(), nullable=False),
            arrow.field("missing_graduation_anchor", arrow.bool_(), nullable=False),
            arrow.field("missing_lifecycle_anchor", arrow.bool_(), nullable=False),
        )
    )


FEATURE_SCHEMA = _feature_schema() if pa is not None else None


@dataclass(frozen=True, slots=True)
class _Anchor:
    kind: str
    event_id: str
    mint: str
    slot: int
    tx_index: int
    instruction_index: int
    block_time_ns: int | None
    pool_id: str | None
    order_key: tuple[int, int, int, int, str]


def _event_key(
    slot: int,
    tx_index: int,
    instruction_index: int,
    event_index: int,
    event_id: str,
) -> tuple[int, int, int, int, str]:
    return (slot, tx_index, instruction_index, event_index, event_id)


def _row_text(row: Mapping[str, object], names: Sequence[str]) -> str | None:
    for name in names:
        value = row.get(name)
        if value is not None:
            if not isinstance(value, str):
                raise LifecycleFeatureError(f"lifecycle {name} must be text")
            if value:
                return value
    return None


def _row_int(
    row: Mapping[str, object],
    names: Sequence[str],
    label: str,
    *,
    required: bool = False,
) -> int | None:
    for name in names:
        value = row.get(name)
        if value is not None:
            return _nonnegative_int(value, label)
    if required:
        raise LifecycleFeatureError(f"lifecycle {label} is missing")
    return None


def _timestamp_ns(row: Mapping[str, object]) -> int | None:
    # ``block_time`` is Unix seconds in the adapter schema; event timestamp is
    # also seconds.  Accept explicit *_ns fields and large values for small
    # synthetic fixtures without silently producing a 1e9 error.
    values = _values_json(row)
    raw = _row_int(row, ("block_time_ns", "event_time_ns"), "block_time_ns")
    if raw is None:
        raw = _row_int(values, ("block_time_ns", "event_time_ns"), "block_time_ns")
    if raw is not None:
        return raw
    raw = _row_int(row, ("block_time", "event_timestamp", "timestamp"), "block_time")
    if raw is None:
        raw = _row_int(values, ("block_time", "event_timestamp", "timestamp"), "block_time")
    if raw is None:
        return None
    return raw if raw >= 10**12 else raw * 1_000_000_000


def _values_json(row: Mapping[str, object]) -> Mapping[str, object]:
    raw = row.get("values_json")
    if raw is None:
        return {}
    if not isinstance(raw, str):
        raise LifecycleFeatureError("lifecycle values_json must be text")
    try:
        return _mapping(json.loads(raw), "lifecycle values_json")
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LifecycleFeatureError("lifecycle values_json is invalid JSON") from error


def _lifecycle_row_value(
    row: Mapping[str, object], values: Mapping[str, object], names: Sequence[str]
) -> object:
    for name in names:
        if row.get(name) is not None:
            return row[name]
        if values.get(name) is not None:
            return values[name]
    return None


def _anchor_from_row(row: Mapping[str, object]) -> _Anchor | None:
    event_id = _row_text(row, ("lifecycle_event_id", "event_id"))
    if event_id is None:
        raise LifecycleFeatureError("lifecycle row has no lifecycle_event_id")
    slot = _row_int(row, ("slot",), "slot", required=True)
    tx_index = _row_int(row, ("tx_index",), "tx_index", required=True)
    log_index = _row_int(row, ("log_index", "instruction_index"), "log_index")
    event_index = _row_int(row, ("event_index",), "event_index")
    assert slot is not None and tx_index is not None
    if log_index is None:
        log_index = event_index if event_index is not None else 0
    if event_index is None:
        event_index = 0
    event_name = (_row_text(row, ("event_name", "event_type")) or "").lower().replace("_", "")
    venue = (_row_text(row, ("venue",)) or "").lower()
    values = _values_json(row)
    mint_value = _lifecycle_row_value(row, values, ("mint", "base_mint", "token_mint"))
    mint = mint_value if isinstance(mint_value, str) and mint_value else None
    if mint is None:
        return None
    pool_value = _lifecycle_row_value(row, values, ("pool", "market_id"))
    pool_id = pool_value if isinstance(pool_value, str) and pool_value else None
    launch = event_name in {"createevent", "create", "launchevent", "launchtoken"}
    pool = event_name in {"createpoolevent", "createpool", "poolevent"} or (
        "pumpswap" in venue and "createpool" in event_name
    )
    graduation = event_name in {
        "completeevent",
        "completepumpammmigrationevent",
        "migrationevent",
        "migrateevent",
        "graduationevent",
        "graduatedevent",
    }
    if not (launch or pool or graduation):
        return None
    # A PumpSwap pool-create row names the mint as base_mint.  Program/venue
    # are retained in the sidecar for audit but are not needed for matching.
    kind = "pool" if pool else "graduation" if graduation else "launch"
    return _Anchor(
        kind=kind,
        event_id=event_id,
        mint=mint,
        slot=slot,
        tx_index=tx_index,
        instruction_index=log_index,
        block_time_ns=_timestamp_ns(row),
        pool_id=pool_id,
        order_key=_event_key(slot, tx_index, log_index, event_index, event_id),
    )


class _LifecycleIndex:
    """SQLite-backed lifecycle identity and anchor index."""

    def __init__(self, path: Path) -> None:
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=DELETE")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA cache_size=-4096")
        self.connection.execute("CREATE TABLE lifecycle_ids (event_id TEXT PRIMARY KEY NOT NULL)")
        self.connection.execute(
            "CREATE TABLE source_ids (namespace TEXT NOT NULL, event_id TEXT NOT NULL, "
            "PRIMARY KEY(namespace,event_id))"
        )
        self.connection.execute(
            "CREATE TABLE anchors ("
            "kind TEXT NOT NULL, mint TEXT NOT NULL, pool_id TEXT NOT NULL DEFAULT '', "
            "event_id TEXT PRIMARY KEY NOT NULL, slot INTEGER NOT NULL, tx_index INTEGER NOT NULL, "
            "instruction_index INTEGER NOT NULL, event_index INTEGER NOT NULL, "
            "block_time_ns INTEGER, "
            "order_key TEXT NOT NULL)"
        )
        self.connection.execute(
            "CREATE INDEX anchors_lookup ON anchors("
            "kind, mint, pool_id, slot, tx_index, instruction_index, event_index, event_id)"
        )
        self.connection.execute(
            "CREATE TABLE market_identities ("
            "market_id TEXT PRIMARY KEY NOT NULL, mint TEXT NOT NULL, quote_mint TEXT NOT NULL)"
        )
        self.connection.commit()
        self.previous_key: tuple[int, int, int, int, str] | None = None

    def add_row(self, row: Mapping[str, object]) -> _Anchor | None:
        anchor = _anchor_from_row(row)
        lifecycle_id = _row_text(row, ("lifecycle_event_id", "event_id"))
        raw_event_id = _row_text(row, ("event_id",))
        event_id = lifecycle_id
        assert event_id is not None
        try:
            self.connection.execute("INSERT INTO lifecycle_ids(event_id) VALUES (?)", (event_id,))
        except sqlite3.IntegrityError as error:
            raise LifecycleFeatureError(f"duplicate lifecycle_event_id: {event_id}") from error
        for namespace, source_id in (
            ("lifecycle_event", lifecycle_id),
            ("event", raw_event_id),
        ):
            if source_id is None:
                continue
            try:
                self.connection.execute(
                    "INSERT INTO source_ids(namespace,event_id) VALUES (?,?)",
                    (namespace, source_id),
                )
            except sqlite3.IntegrityError as error:
                raise LifecycleFeatureError(f"duplicate {namespace} id: {source_id}") from error
        if anchor is None:
            # Non-anchor lifecycle events are still identity-checked and order-
            # checked, but need not be retained after their row is consumed.
            slot = _row_int(row, ("slot",), "slot", required=True)
            tx = _row_int(row, ("tx_index",), "tx_index", required=True)
            log = _row_int(row, ("log_index", "instruction_index"), "log_index") or 0
            idx = _row_int(row, ("event_index",), "event_index") or 0
            assert slot is not None and tx is not None
            key = _event_key(slot, tx, log, idx, event_id)
        else:
            key = anchor.order_key
        if self.previous_key is not None and key <= self.previous_key:
            raise LifecycleFeatureError("lifecycle input order is not strictly increasing")
        self.previous_key = key
        if anchor is not None:
            pool_id = anchor.pool_id or ""
            self.connection.execute(
                "INSERT INTO anchors("
                "kind,mint,pool_id,event_id,slot,tx_index,instruction_index,event_index,"
                "block_time_ns,order_key) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    anchor.kind,
                    anchor.mint,
                    pool_id,
                    anchor.event_id,
                    anchor.slot,
                    anchor.tx_index,
                    anchor.instruction_index,
                    anchor.order_key[3],
                    anchor.block_time_ns,
                    json.dumps(anchor.order_key, separators=(",", ":")),
                ),
            )
        return anchor

    def commit(self) -> None:
        self.connection.commit()

    def check_swap_identity(self, event: SwapEvent) -> None:
        prior = self.connection.execute(
            "SELECT mint,quote_mint FROM market_identities WHERE market_id=?",
            (event.market_id,),
        ).fetchone()
        identity = (event.mint, event.quote_mint)
        if prior is not None and tuple(prior) != identity:
            raise LifecycleFeatureError(
                f"inconsistent mint/quote identity for market {event.market_id}"
            )
        if prior is None:
            self.connection.execute(
                "INSERT INTO market_identities(market_id,mint,quote_mint) VALUES (?,?,?)",
                (event.market_id, event.mint, event.quote_mint),
            )

    def check_swap_id(self, event_id: str) -> None:
        try:
            self.connection.execute(
                "INSERT INTO source_ids(namespace,event_id) VALUES (?,?)",
                ("swap", event_id),
            )
        except sqlite3.IntegrityError as error:
            raise LifecycleFeatureError(f"duplicate swap event_id: {event_id}") from error

    def anchors_for(self, kind: str, mint: str, market_id: str) -> _Anchor | None:
        if kind == "pool":
            rows = self.connection.execute(
                "SELECT event_id,mint,slot,tx_index,instruction_index,event_index,"
                "block_time_ns,pool_id,order_key FROM anchors "
                "WHERE kind=? AND mint=? AND pool_id=? "
                "ORDER BY slot,tx_index,instruction_index,event_index,event_id LIMIT 1",
                (kind, mint, market_id),
            ).fetchone()
            if rows is None:
                rows = self.connection.execute(
                    "SELECT event_id,mint,slot,tx_index,instruction_index,event_index,"
                    "block_time_ns,pool_id,order_key FROM anchors "
                    "WHERE kind=? AND mint=? AND pool_id='' "
                    "ORDER BY slot,tx_index,instruction_index,event_index,event_id LIMIT 1",
                    (kind, mint),
                ).fetchone()
        else:
            rows = self.connection.execute(
                "SELECT event_id,mint,slot,tx_index,instruction_index,event_index,"
                "block_time_ns,pool_id,order_key "
                "FROM anchors WHERE kind=? AND mint=? ORDER BY "
                "CASE WHEN pool_id=? AND pool_id<>'' THEN 0 ELSE 1 END, "
                "slot,tx_index,instruction_index,event_index,event_id LIMIT 1",
                (kind, mint, market_id),
            ).fetchone()
        if rows is None:
            return None
        (
            event_id,
            row_mint,
            slot,
            tx_index,
            instruction_index,
            event_index,
            block_time_ns,
            pool_id,
            raw_key,
        ) = rows
        parsed_key = json.loads(raw_key)
        if (
            not isinstance(parsed_key, list)
            or len(parsed_key) != 5
            or not all(isinstance(item, (int, str)) for item in parsed_key)
        ):
            raise LifecycleFeatureError("stored lifecycle anchor order key is invalid")
        order_key = (
            _nonnegative_int(parsed_key[0], "stored anchor slot"),
            _nonnegative_int(parsed_key[1], "stored anchor tx_index"),
            _nonnegative_int(parsed_key[2], "stored anchor instruction_index"),
            _nonnegative_int(parsed_key[3], "stored anchor event_index"),
            _text(parsed_key[4], "stored anchor event_id"),
        )
        return _Anchor(
            kind=kind,
            event_id=str(event_id),
            mint=str(row_mint),
            slot=int(slot),
            tx_index=int(tx_index),
            instruction_index=int(instruction_index),
            block_time_ns=None if block_time_ns is None else int(block_time_ns),
            pool_id=None if not pool_id else str(pool_id),
            order_key=order_key,
        )

    def close(self) -> None:
        self.connection.close()


def _swap_order_key(event: SwapEvent) -> tuple[int, int, int, int, str]:
    # Lifecycle logs are rank 0 and swaps rank 1 for the impossible-but-
    # deterministic same-log-index tie.  In normal archives log indices differ.
    return (event.slot, event.tx_index, event.instruction_index, 1, event.event_id)


def _number(value: object) -> float:
    result = float(cast(Any, value))
    if not (result == result) or result in (float("inf"), float("-inf")):
        raise LifecycleFeatureError("swap numeric field is not finite")
    return result


def _feature_row(event: SwapEvent, index: _LifecycleIndex) -> dict[str, object]:
    launch = index.anchors_for("launch", event.mint, event.market_id)
    pool = index.anchors_for("pool", event.mint, event.market_id)
    graduation = index.anchors_for("graduation", event.mint, event.market_id)
    swap_key = _swap_order_key(event)

    def after(anchor: _Anchor | None) -> bool:
        return anchor is not None and swap_key > anchor.order_key

    launch_after = after(launch)
    age_slots = None if launch is None or not launch_after else event.slot - launch.slot
    age_seconds: float | None = None
    if launch is not None and launch_after and launch.block_time_ns is not None:
        age_seconds = (event.block_time_ns - launch.block_time_ns) / 1_000_000_000
        if age_seconds < 0:
            age_seconds = None
    pool_age_slots = None if pool is None or not after(pool) else event.slot - pool.slot
    pool_age_seconds: float | None = None
    if pool is not None and after(pool) and pool.block_time_ns is not None:
        pool_age_seconds = (event.block_time_ns - pool.block_time_ns) / 1_000_000_000
        if pool_age_seconds < 0:
            pool_age_seconds = None
    if graduation is None:
        graduation_state = "unknown_graduation"
        is_pre: bool | None = None
        is_post: bool | None = None
    elif swap_key < graduation.order_key:
        graduation_state = "pre_graduation"
        is_pre, is_post = True, False
    else:
        graduation_state = "post_graduation"
        is_pre, is_post = False, True
    missing: list[str] = []
    if launch is None:
        missing.append("launch")
    if pool is None:
        missing.append("pool_creation")
    if graduation is None:
        missing.append("graduation")
    status = "complete" if not missing else "missing_" + "_and_".join(missing)
    launch_status = "present" if launch is not None else "missing"
    pool_status = "present" if pool is not None else "missing"
    graduation_status = "present" if graduation is not None else "missing"
    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "lifecycle_feature",
        "event_id": event.event_id,
        "market_id": event.market_id,
        "mint": event.mint,
        "quote_mint": event.quote_mint,
        "slot": event.slot,
        "tx_index": event.tx_index,
        "instruction_index": event.instruction_index,
        "ordering_key": json.dumps(list(event.ordering_key), separators=(",", ":")),
        "block_time_ns": event.block_time_ns,
        "observed_at_ns": event.observed_at_ns,
        "phase": event.phase.value,
        "venue_phase": event.phase.value,
        "venue": "pump" if event.phase is VenuePhase.BONDING_CURVE else event.phase.value,
        "side": event.side.value,
        "base_amount": _number(event.base_amount),
        "quote_amount": _number(event.quote_amount),
        "price": _number(event.price),
        "virtual_base_reserve": _number(event.virtual_base_reserve),
        "virtual_quote_reserve": _number(event.virtual_quote_reserve),
        "real_base_reserve": _number(event.real_base_reserve),
        "real_quote_reserve": _number(event.real_quote_reserve),
        "trader": event.trader,
        "venue_fee_bps": None if event.venue_fee_bps is None else _number(event.venue_fee_bps),
        "protocol_fee_quote": _number(event.protocol_fee_quote),
        "creator_fee_quote": _number(event.creator_fee_quote),
        "lp_fee_quote": _number(event.lp_fee_quote),
        "cashback_quote": _number(event.cashback_quote),
        "buyback_fee_quote": _number(event.buyback_fee_quote),
        "signature": event.signature,
        "program_version": event.program_version,
        "base_decimals": event.base_decimals,
        "quote_decimals": event.quote_decimals,
        "launch_slot": None if launch is None else launch.slot,
        "launch_tx_index": None if launch is None else launch.tx_index,
        "launch_instruction_index": None if launch is None else launch.instruction_index,
        "launch_block_time_ns": None if launch is None else launch.block_time_ns,
        "launch_transition_id": None if launch is None else launch.event_id,
        "launch_event_id": None if launch is None else launch.event_id,
        "pool_creation_slot": None if pool is None else pool.slot,
        "pool_creation_tx_index": None if pool is None else pool.tx_index,
        "pool_creation_instruction_index": None if pool is None else pool.instruction_index,
        "pool_creation_block_time_ns": None if pool is None else pool.block_time_ns,
        "pool_creation_transition_id": None if pool is None else pool.event_id,
        "pool_creation_event_id": None if pool is None else pool.event_id,
        "pool_create_transition_id": None if pool is None else pool.event_id,
        "pool_id": None if pool is None else pool.pool_id,
        "graduation_slot": None if graduation is None else graduation.slot,
        "graduation_tx_index": None if graduation is None else graduation.tx_index,
        "graduation_instruction_index": (
            None if graduation is None else graduation.instruction_index
        ),
        "graduation_block_time_ns": None if graduation is None else graduation.block_time_ns,
        "graduation_transition_id": None if graduation is None else graduation.event_id,
        "graduation_event_id": None if graduation is None else graduation.event_id,
        "age_slots": age_slots,
        "token_age_slots": age_slots,
        "age_seconds": age_seconds,
        "token_age_seconds": age_seconds,
        "pool_age_slots": pool_age_slots,
        "pool_age_seconds": pool_age_seconds,
        "is_pre_graduation": is_pre,
        "is_post_graduation": is_post,
        "pre_graduation": is_pre,
        "post_graduation": is_post,
        "graduation_state": graduation_state,
        "graduation_phase": graduation_state,
        "lifecycle_state": graduation_state,
        "launch_anchor_status": launch_status,
        "pool_anchor_status": pool_status,
        "graduation_anchor_status": graduation_status,
        "lifecycle_anchor_status": status,
        "lifecycle_anchor_missing_reason": None if not missing else ",".join(missing),
        "missing_launch_anchor": launch is None,
        "missing_pool_anchor": pool is None,
        "missing_graduation_anchor": graduation is None,
        "missing_lifecycle_anchor": bool(missing),
    }
    return payload


def _write_bytes(path: Path, value: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())


def _publish_directory(staging: Path, target: Path) -> None:
    previous: Path | None = None
    if target.exists():
        previous = Path(
            tempfile.mkdtemp(
                prefix=f".{target.name}.previous.", suffix=".partial", dir=target.parent
            )
        )
        previous.rmdir()
        os.replace(target, previous)
    try:
        os.replace(staging, target)
    except BaseException:
        if previous is not None and not target.exists():
            os.replace(previous, target)
        raise
    if previous is not None:
        shutil.rmtree(previous, ignore_errors=True)
    parent_fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)


def materialize_lifecycle_features(
    normalized_input: str | Path,
    lifecycle_input: str | Path,
    output: str | Path,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, object]:
    """Join normalised swaps to lifecycle anchors with bounded memory."""

    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise LifecycleFeatureError("batch_size must be a positive integer")
    arrow, parquet_api = _require_arrow()
    normalized = _resolve_normalized_input(normalized_input)
    lifecycle = _resolve_lifecycle_input(lifecycle_input)
    raw_target = Path(output).expanduser()
    if raw_target.is_symlink():
        raise LifecycleFeatureError("output must not be a symlink")
    target = raw_target.resolve()
    if target == normalized.parquet_path or target == lifecycle.root:
        raise LifecycleFeatureError("output must be distinct from both inputs")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{target.name}.", suffix=".partial", dir=target.parent)
    )
    published = False
    index: _LifecycleIndex | None = None
    writer: Any = None
    output_rows = 0
    status_counts: Counter[str] = Counter()
    anchor_counts: Counter[str] = Counter()
    previous_swap_key: tuple[int, int, int, str] | None = None
    try:
        index = _LifecycleIndex(staging / "identity.sqlite")
        lifecycle_rows_seen = 0
        for shard in lifecycle.shards:
            parquet = parquet_api.ParquetFile(shard.path)
            try:
                names = set(parquet.schema_arrow.names)
                required_any = {"lifecycle_event_id", "event_id"}
                if (
                    not names.intersection(required_any)
                    or "slot" not in names
                    or "tx_index" not in names
                ):
                    raise LifecycleFeatureError("lifecycle shard lacks identity/order columns")
                columns = sorted(
                    names.intersection(
                        {
                            "event_id",
                            "lifecycle_event_id",
                            "slot",
                            "tx_index",
                            "event_index",
                            "log_index",
                            "instruction_index",
                            "block_time",
                            "block_time_ns",
                            "event_timestamp",
                            "timestamp",
                            "event_time_ns",
                            "program_id",
                            "event_name",
                            "event_type",
                            "venue",
                            "mint",
                            "base_mint",
                            "token_mint",
                            "pool",
                            "market_id",
                            "values_json",
                        }
                    )
                )
                for batch in parquet.iter_batches(
                    batch_size=batch_size, columns=columns, use_threads=False
                ):
                    for raw in batch.to_pylist():
                        row = cast(Mapping[str, object], raw)
                        lifecycle_rows_seen += 1
                        anchor = index.add_row(row)
                        if anchor is not None:
                            anchor_counts[anchor.kind] += 1
                    index.commit()
            finally:
                parquet.close()
        if lifecycle_rows_seen != lifecycle.row_count:
            raise LifecycleFeatureError("streamed lifecycle row count differs from manifest")
        feature_schema = FEATURE_SCHEMA
        assert feature_schema is not None
        feature_path = staging / "features.parquet"
        writer = parquet_api.ParquetWriter(feature_path, feature_schema, compression="zstd")
        identity = index
        for batch in iter_swap_event_batches(normalized.parquet_path, batch_size=batch_size):
            rows: list[dict[str, object]] = []
            for event in batch:
                if previous_swap_key is not None and event.ordering_key <= previous_swap_key:
                    raise LifecycleFeatureError(
                        "normalized swap input order is not strictly increasing"
                    )
                previous_swap_key = event.ordering_key
                identity.check_swap_identity(event)
                identity.check_swap_id(event.event_id)
                row = _feature_row(event, identity)
                rows.append(row)
                output_rows += 1
                status_counts[str(row["lifecycle_anchor_status"])] += 1
            if rows:
                writer.write_table(arrow.Table.from_pylist(rows, schema=feature_schema))
            identity.connection.commit()
        if output_rows != normalized.row_count:
            raise LifecycleFeatureError("streamed normalized row count differs from manifest")
        writer.close()
        writer = None
        index.close()
        index = None
        files: dict[str, dict[str, object]] = {}
        for path in (feature_path,):
            files[path.name] = {"byte_count": path.stat().st_size, "sha256": sha256_file(path)}
        audit_payload: dict[str, object] = {
            "kind": AUDIT_KIND,
            "version": SCHEMA_VERSION,
            "input_row_count": normalized.row_count,
            "lifecycle_row_count": lifecycle.row_count,
            "output_row_count": output_rows,
            "normalized_manifest_sha256": normalized.manifest_sha256,
            "normalized_swaps_sha256": normalized.swaps_sha256,
            "lifecycle_manifest_sha256": lifecycle.manifest_sha256,
            "lifecycle_schema_sha256": lifecycle.schema_sha256,
            "anchor_counts": dict(sorted(anchor_counts.items())),
            "status_counts": dict(sorted(status_counts.items())),
            "physical_order": list(PHYSICAL_ORDER),
            "lifecycle_physical_order": list(LIFECYCLE_PHYSICAL_ORDER),
            "input_read_mode": "streaming_parquet_batches_lifecycle_sqlite_index",
            "missing_anchor_policy": "explicit_nullable_fields_and_statuses",
            "age_seconds_source": (
                "normalized_block_time_ns_minus_lifecycle_block_time_or_timestamp"
            ),
            "phase_source": "normalized_swap_phase",
        }
        audit_body = canonical_json(audit_payload)
        _write_bytes(staging / "audit.json", audit_body)
        files["audit.json"] = {"byte_count": len(audit_body), "sha256": sha256_bytes(audit_body)}
        normalized_identity: dict[str, object] = {
            "path": str(normalized.parquet_path),
            "manifest_path": str(normalized.manifest_path),
            "manifest_sha256": normalized.manifest_sha256,
            "swaps_sha256": normalized.swaps_sha256,
            "row_count": normalized.row_count,
        }
        lifecycle_identity: dict[str, object] = {
            "path": str(lifecycle.root),
            "manifest_path": str(lifecycle.manifest_path),
            "manifest_sha256": lifecycle.manifest_sha256,
            "schema_sha256": lifecycle.schema_sha256,
            "shards": [
                {
                    "path": shard.relative_path,
                    "byte_count": shard.byte_count,
                    "sha256": shard.sha256,
                    "row_count": shard.row_count,
                }
                for shard in lifecycle.shards
            ],
            "row_count": lifecycle.row_count,
            "manifest_kind": lifecycle.manifest_kind,
        }
        manifest_payload: dict[str, object] = {
            "kind": FEATURES_KIND,
            "version": SCHEMA_VERSION,
            "inputs": {"normalized": normalized_identity, "lifecycle": lifecycle_identity},
            "source_manifests": {
                "normalized_manifest_sha256": normalized.manifest_sha256,
                "lifecycle_manifest_sha256": lifecycle.manifest_sha256,
            },
            "normalized_manifest_sha256": normalized.manifest_sha256,
            "lifecycle_manifest_sha256": lifecycle.manifest_sha256,
            "normalized_swaps_sha256": normalized.swaps_sha256,
            "lifecycle_schema_sha256": lifecycle.schema_sha256,
            "row_count": output_rows,
            "feature_schema_sha256": _schema_hash(feature_schema),
            "physical_order": list(PHYSICAL_ORDER),
            "lifecycle_physical_order": list(LIFECYCLE_PHYSICAL_ORDER),
            "audit_path": "audit.json",
            "audit_sha256": files["audit.json"]["sha256"],
            "files": files,
        }
        _write_bytes(staging / "manifest.json", canonical_json(manifest_payload))
        directory_fd = os.open(staging, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        _publish_directory(staging, target)
        published = True
        return manifest_payload
    finally:
        if writer is not None:
            writer.close()
        if index is not None:
            index.close()
        if not published and staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


__all__ = [
    "AUDIT_KIND",
    "FEATURES_KIND",
    "FEATURE_SCHEMA",
    "LifecycleFeatureError",
    "LIFECYCLE_PHYSICAL_ORDER",
    "PHYSICAL_ORDER",
    "SCHEMA_VERSION",
    "materialize_lifecycle_features",
]
