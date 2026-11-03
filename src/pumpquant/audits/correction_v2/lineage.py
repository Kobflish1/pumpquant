# ruff: noqa: E501

"""Outcome-blind source-lineage and SOL-quote audit for correction-v2.

The correction runner consumes a v1 feature projection, a separately sealed
Pump market-mode index, and the lifecycle-only mirror from which the index was
derived.  This module is deliberately a verifier, rather than a materializer:
it opens only manifests, inventories, the v1 feature/rejected Parquets, the
market-mode Parquet, and hash-bound lifecycle Parquets.  It never reads a
report, outcome, holdout, or writes an input root.

``verify_correction_lineage_v2`` returns an immutable, deterministic audit
object.  Callers can persist ``canonical_bytes`` in a new output root if a
downstream correction run needs a manifest-last publication; this verifier
does not publish anything itself.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import os
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Final, cast

import pyarrow as pa
import pyarrow.parquet as pq

from pumpquant.core.hashing import sha256_file_with_size
from pumpquant.decoding.instruction_intent import PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID

DEVELOPMENT_START_SLOT: Final[int] = 431_265_461
DEVELOPMENT_END_SLOT: Final[int] = 432_432_000
HOLDOUT_START_SLOT: Final[int] = DEVELOPMENT_END_SLOT
HOLDOUT_END_SLOT: Final[int] = 432_864_000
EXPECTED_SOURCE_CHUNK_COUNT: Final[int] = 19
SOURCE_CHUNK_COUNT: Final[int] = EXPECTED_SOURCE_CHUNK_COUNT

NATIVE_SOL_MINT: Final[str] = "11111111111111111111111111111111"
WRAPPED_SOL_MINT: Final[str] = "So11111111111111111111111111111111111111112"
CANONICAL_SOL_MINT: Final[str] = "SOL"
SOL_ALIASES: Final[frozenset[str]] = frozenset(
    (NATIVE_SOL_MINT, WRAPPED_SOL_MINT, CANONICAL_SOL_MINT)
)
# Names used by a few older callers.  The set is intentionally exact; do not
# add lowercase/upper-case or symbol aliases without a new preregistration.
SOL_MINT_ALIASES: Final[frozenset[str]] = SOL_ALIASES
NATIVE_SOL: Final[str] = NATIVE_SOL_MINT
WRAPPED_SOL: Final[str] = WRAPPED_SOL_MINT
CANONICAL_SOL: Final[str] = CANONICAL_SOL_MINT

V1_FEATURE_KIND: Final[str] = "pumpquant.shock_recovery_feature_dataset"
V1_AUDIT_KIND: Final[str] = "pumpquant.shock_recovery_feature_audit"
MODE_INDEX_KIND: Final[str] = "pumpquant.pump_market_mode_index"
MODE_AUDIT_KIND: Final[str] = "pumpquant.pump_market_mode_audit_v2"
TRADE_EVENT_KIND: Final[str] = "pumpquant.trade_event_dataset"
DATASET_KIND: Final[str] = "pumpquant.fresh_dev_materialized_dataset"
V1_VERSION: Final[int] = 1
MODE_VERSION: Final[int] = 2
SOURCE_VERSION: Final[int] = 1

_HEX = frozenset("0123456789abcdef")
_FORBIDDEN = frozenset({"holdout", "outcome", "outcomes", "report", "reports", "s3"})
_LIFECYCLE_PREFIXES = ("trade-events/lifecycle/", "lifecycle/")
_SUPPORTED_CHUNK_KINDS = frozenset(
    {
        "pumpquant.old_faithful_backtester_chunk",
        "pumpquant.fresh_dev_materialized_chunk",
        "pumpquant.fresh_dev_backtester_chunk",
        "pumpquant.rpc_gap_backtester_chunk",
    }
)
_MODE_REQUIRED_COLUMNS = (
    "schema_version",
    "record_type",
    "mint",
    "is_mayhem_mode",
    "quote_mint",
    "create_slot",
    "create_tx_index",
    "create_log_index",
    "create_event_index",
    "create_event_id",
    "create_signature",
    "create_ordering_key",
)

# This is the exact output schema emitted by ``extract_pump_market_modes_v2``.
# Keep the physical schema binding here rather than trusting only the column
# names: an output-schema digest must detect type/nullability drift too.
MODE_SCHEMA = pa.schema(
    (
        pa.field("schema_version", pa.int16(), nullable=False),
        pa.field("record_type", pa.string(), nullable=False),
        pa.field("mint", pa.string(), nullable=False),
        pa.field("is_mayhem_mode", pa.bool_(), nullable=False),
        pa.field("quote_mint", pa.string()),
        pa.field("create_slot", pa.int64(), nullable=False),
        pa.field("create_tx_index", pa.int32(), nullable=False),
        pa.field("create_log_index", pa.int32(), nullable=False),
        pa.field("create_event_index", pa.int16(), nullable=False),
        pa.field("create_event_id", pa.string(), nullable=False),
        pa.field("create_signature", pa.string()),
        pa.field("create_ordering_key", pa.string(), nullable=False),
    )
)
MODE_OUTPUT_SCHEMA_SHA256: Final[str] = hashlib.sha256(
    MODE_SCHEMA.serialize().to_pybytes()
).hexdigest()


class CorrectionLineageError(RuntimeError):
    """A sealed lineage input or quote proof is invalid."""


# Public spelling used by some deployment code.
CorrectionLineageV2Error = CorrectionLineageError
CorrectionLineageAuditError = CorrectionLineageError


def canonical_json(value: object) -> bytes:
    """Encode one audit/manifest value in the repository's canonical form."""

    try:
        text = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as error:
        raise CorrectionLineageError("value is not canonical-JSON encodable") from error
    return (text + "\n").encode("utf-8")


def normalize_sol_quote(value: object) -> str:
    """Return ``SOL`` for one of the three explicitly frozen SOL spellings.

    A missing or unknown value is an error.  In particular, this helper never
    turns ``None`` into SOL and never performs case-folding or symbol lookup.
    """

    if not isinstance(value, str) or value not in SOL_ALIASES:
        raise CorrectionLineageError("quote mint is missing or is not a canonical SOL alias")
    return CANONICAL_SOL_MINT


# A descriptive alias for callers that use ``canonical_quote_mint`` wording.
canonical_quote_mint = normalize_sol_quote


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> tuple[int, str]:
    try:
        return sha256_file_with_size(path)
    except OSError as error:
        raise CorrectionLineageError(f"cannot hash {path}") from error


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise CorrectionLineageError(f"{label} must be an object with text keys")
    return dict(cast(Mapping[str, object], value))


def _text(value: object, label: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise CorrectionLineageError(f"{label} must be non-empty text")
    return value


def _hash(value: object, label: str) -> str:
    text = _text(value, label)
    if len(text) != 64 or any(char not in _HEX for char in text):
        raise CorrectionLineageError(f"{label} must be lowercase SHA-256 hex")
    return text


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CorrectionLineageError(f"{label} must be an integer >= {minimum}")
    return value


def _forbidden_path(path: Path) -> bool:
    return any(part.casefold() in _FORBIDDEN for part in path.parts)


def _assert_root(path_value: str | Path, label: str) -> Path:
    path = Path(path_value).expanduser()
    if _forbidden_path(path):
        raise CorrectionLineageError(f"{label} points at a forbidden downstream path")
    if path.is_symlink() or not path.is_dir():
        raise CorrectionLineageError(f"{label} must be a regular directory")
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise CorrectionLineageError(f"{label} cannot be resolved safely") from error
    if resolved != path.absolute() or _forbidden_path(resolved):
        raise CorrectionLineageError(f"{label} traverses a symlink or forbidden path")
    return resolved


def _assert_file(path: Path, label: str) -> None:
    if _forbidden_path(path) or path.is_symlink() or not path.is_file():
        raise CorrectionLineageError(f"{label} must be a regular non-symlink file")
    current = path.parent
    while True:
        if current.is_symlink():
            raise CorrectionLineageError(f"{label} traverses a symlink")
        parent = current.parent
        if parent == current:
            break
        current = parent


def _assert_no_symlinks(root: Path, label: str) -> None:
    """Reject symlinked files/directories before any inventory traversal."""

    try:
        for current, directories, files in os.walk(root, followlinks=False):
            current_path = Path(current)
            if current_path.is_symlink():
                raise CorrectionLineageError(f"{label} traverses a symlink")
            for name in (*directories, *files):
                if (current_path / name).is_symlink():
                    raise CorrectionLineageError(f"{label} contains a symlink: {name}")
    except OSError as error:
        raise CorrectionLineageError(f"{label} cannot be inspected safely") from error


def _safe_relative(root: Path, value: object, label: str) -> Path:
    text = _text(value, label)
    pure = PurePosixPath(text)
    if (
        pure.is_absolute()
        or pure.as_posix() != text
        or not pure.parts
        or any(part in {"", ".", ".."} for part in pure.parts)
        or any(part.casefold() in _FORBIDDEN for part in pure.parts)
    ):
        raise CorrectionLineageError(f"{label} is unsafe")
    current = root
    for part in pure.parts:
        current = current / part
        if current.is_symlink():
            raise CorrectionLineageError(f"{label} traverses a symlink")
    candidate = current.resolve(strict=False)
    try:
        candidate.relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as error:
        raise CorrectionLineageError(f"{label} escapes its root") from error
    return candidate


class _DuplicateKey(ValueError):
    pass


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKey(key)
        result[key] = value
    return result


def _json_file(path: Path, label: str) -> tuple[dict[str, object], bytes, str]:
    _assert_file(path, label)
    try:
        body = path.read_bytes()
        parsed = json.loads(body.decode("utf-8"), object_pairs_hook=_pairs)
    except (_DuplicateKey, OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CorrectionLineageError(f"{label} is invalid JSON") from error
    value = _mapping(parsed, label)
    if body != canonical_json(value):
        raise CorrectionLineageError(f"{label} is not canonical JSON")
    return value, body, _sha256_bytes(body)


def _files_inventory(value: object, label: str) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    if isinstance(value, Mapping):
        iterable: Sequence[object] = [
            dict(_mapping(item, f"{label} {key}"), path=key) for key, item in value.items()
        ]
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        iterable = value
    else:
        raise CorrectionLineageError(f"{label} inventory is missing")
    for index, raw in enumerate(iterable):
        item = _mapping(raw, f"{label} entry {index}")
        name = _text(item.get("path"), f"{label} entry {index} path")
        pure = PurePosixPath(name)
        if (
            pure.is_absolute()
            or pure.as_posix() != name
            or not pure.parts
            or any(part in {"", ".", ".."} for part in pure.parts)
            or any(part.casefold() in _FORBIDDEN for part in pure.parts)
        ):
            raise CorrectionLineageError(f"{label} path is unsafe: {name}")
        if name in result:
            raise CorrectionLineageError(f"{label} repeats file path: {name}")
        result[name] = item
    return result


def _verify_inventory_file(
    root: Path,
    relative: str,
    entry: Mapping[str, object],
    label: str,
    *,
    parquet: bool = False,
) -> dict[str, object]:
    path = _safe_relative(root, relative, f"{label} path")
    _assert_file(path, label)
    expected_size = _integer(entry.get("byte_count"), f"{label} byte_count")
    expected_hash = _hash(entry.get("sha256"), f"{label} sha256")
    actual_size, actual_hash = _sha256_file(path)
    if (actual_size, actual_hash) != (expected_size, expected_hash):
        raise CorrectionLineageError(f"{label} differs from its sealed inventory")
    result: dict[str, object] = {
        "path": relative,
        "byte_count": actual_size,
        "sha256": actual_hash,
    }
    if parquet:
        try:
            parquet_file = pq.ParquetFile(path)
            metadata_rows = (
                0 if parquet_file.metadata is None else int(parquet_file.metadata.num_rows)
            )
            parquet_file.close()
        except Exception as error:
            raise CorrectionLineageError(f"{label} cannot be opened as Parquet") from error
        if "row_count" in entry:
            rows = _integer(entry.get("row_count"), f"{label} row_count")
            if metadata_rows != rows:
                raise CorrectionLineageError(f"{label} row_count differs from Parquet metadata")
        else:
            rows = metadata_rows
        result["row_count"] = rows
    elif "row_count" in entry:
        result["row_count"] = _integer(entry.get("row_count"), f"{label} row_count")
    return result


def _reject_downstream_flags(value: Mapping[str, object], label: str) -> None:
    for field in (
        "outcomes_read",
        "reports_read",
        "s3_read",
        "holdout_accessed",
        "holdout_unlocked",
    ):
        if value.get(field) is True:
            raise CorrectionLineageError(f"{label} declares forbidden {field}")
    for field in ("holdout_payload_reads_allowed", "holdout_feature_generation_allowed"):
        if field in value and value.get(field) is not False:
            raise CorrectionLineageError(f"{label} permits holdout access: {field}")


def _manifest_path(root: Path, names: Sequence[str], label: str) -> Path:
    for name in names:
        candidate = root / name
        if candidate.is_file() and not candidate.is_symlink():
            return candidate
    raise CorrectionLineageError(f"{label} manifest is missing")


def _iter_parquet_rows(
    path: Path, columns: Sequence[str], label: str
) -> Iterator[dict[str, object]]:
    _assert_file(path, label)
    try:
        parquet = pq.ParquetFile(path)
        names = set(parquet.schema_arrow.names)
        requested = [column for column in columns if column in names]
        if not requested:
            raise CorrectionLineageError(f"{label} has no requested identity columns")
        for batch in parquet.iter_batches(batch_size=65_536, columns=requested):
            for row in batch.to_pylist():
                yield cast(dict[str, object], row)
        parquet.close()
    except CorrectionLineageError:
        raise
    except Exception as error:
        raise CorrectionLineageError(f"{label} cannot be read as Parquet") from error


def _require_parquet_columns(path: Path, columns: Sequence[str], label: str) -> None:
    _assert_file(path, label)
    try:
        parquet = pq.ParquetFile(path)
        names = set(parquet.schema_arrow.names)
        parquet.close()
    except Exception as error:
        raise CorrectionLineageError(f"{label} cannot be opened as Parquet") from error
    missing = [column for column in columns if column not in names]
    if missing:
        raise CorrectionLineageError(f"{label} is missing required columns: {', '.join(missing)}")


def _row_value(
    row: Mapping[str, object], values: Mapping[str, object], names: Sequence[str]
) -> object:
    for name in names:
        if row.get(name) is not None:
            return row[name]
        if values.get(name) is not None:
            return values[name]
    return None


def _values_json(row: Mapping[str, object], label: str) -> Mapping[str, object]:
    raw = row.get("values_json")
    if raw is None:
        return {}
    if isinstance(raw, Mapping):
        return _mapping(raw, f"{label} values_json")
    if not isinstance(raw, str):
        raise CorrectionLineageError(f"{label} values_json is not text")
    try:
        value = json.loads(raw, object_pairs_hook=_pairs)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CorrectionLineageError(f"{label} values_json is invalid JSON") from error
    return _mapping(value, f"{label} values_json")


def _ordering(row: Mapping[str, object], label: str) -> tuple[int, int, int, int, str]:
    values: list[int] = []
    for name in ("slot", "tx_index", "log_index", "event_index"):
        values.append(_integer(row.get(name), f"{label} {name}"))
    # ``event_id`` is the final canonical tie-break.  A few lifecycle mirrors
    # expose only ``lifecycle_event_id``; use it when the primary spelling is
    # absent (but never collapse two distinct IDs sharing numeric coordinates).
    raw_event_id = row.get("event_id")
    if raw_event_id is None:
        raw_event_id = row.get("lifecycle_event_id")
    event_id = _text(raw_event_id, f"{label} event_id")
    return (values[0], values[1], values[2], values[3], event_id)


@dataclass(frozen=True, slots=True)
class _PumpCreate:
    mint: str
    quote_mint: str
    normalized_quote_mint: str | None
    event_id: str
    ordering: tuple[int, int, int, int, str]
    is_mayhem_mode: bool
    source_path: str


@dataclass(frozen=True, slots=True)
class _PoolCreate:
    pool: str
    base_mint: str
    quote_mint: str
    sol_source_mint: str | None
    normalized_quote_mint: str | None
    event_id: str
    ordering: tuple[int, int, int, int, str]
    source_path: str


@dataclass(slots=True)
class _Lifecycle:
    pump: dict[str, _PumpCreate]
    pools: dict[str, _PoolCreate]
    create_rows: int = 0
    pump_rows: int = 0
    pool_rows: int = 0
    duplicate_count: int = 0
    conflict_count: int = 0
    event_ids: set[str] | None = None

    def __post_init__(self) -> None:
        self.event_ids = set()


@dataclass(frozen=True, slots=True)
class _Chunk:
    start: int
    end: int
    manifest_key: str
    manifest_sha256: str
    manifest_byte_count: int
    manifest_path: Path
    manifest: dict[str, object]
    trade_manifest_sha256: str
    trade_manifest_byte_count: int
    trade_manifest_path: Path
    trade_manifest: dict[str, object]
    shards: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class _Seal:
    root: Path
    manifest_path: Path
    manifest: dict[str, object]
    manifest_sha256: str
    manifest_byte_count: int
    chunks: tuple[_Chunk, ...]
    inventory: tuple[dict[str, object], ...]
    lifecycle: _Lifecycle


def _inventory_sort_key(item: Mapping[str, object]) -> tuple[str, int, str]:
    raw_start = item.get("chunk_start_slot_inclusive")
    start = raw_start if isinstance(raw_start, int) and not isinstance(raw_start, bool) else -1
    return (str(item.get("role")), start, str(item.get("path")))


def _chunk_reference(
    value: object, index: int, *, require_size: bool = False
) -> tuple[int, int, str, str, int | None]:
    item = _mapping(value, f"source chunk {index}")
    start = _integer(item.get("start_slot_inclusive"), f"source chunk {index} start")
    end = _integer(item.get("end_slot_exclusive"), f"source chunk {index} end")
    key = _text(
        item.get("manifest_key", item.get("manifest_path", item.get("path"))),
        f"source chunk {index} manifest_key",
    )
    digest = _hash(item.get("manifest_sha256"), f"source chunk {index} manifest_sha256")
    if require_size and item.get("manifest_byte_count") is None and item.get("byte_count") is None:
        raise CorrectionLineageError(f"source chunk {index} manifest byte_count is missing")
    raw_size = item.get("manifest_byte_count", item.get("byte_count"))
    size = (
        None
        if raw_size is None
        else _integer(raw_size, f"source chunk {index} manifest_byte_count")
    )
    return start, end, key, digest, size


def _resolve_chunk_manifest(root: Path, key: str, start: int, end: int, label: str) -> Path:
    candidates: list[Path] = []
    pure = PurePosixPath(key)
    if (
        pure.is_absolute()
        or ".." in pure.parts
        or any(part.casefold() in _FORBIDDEN for part in pure.parts)
    ):
        raise CorrectionLineageError(f"{label} key is unsafe")
    candidates.append(root.joinpath(*pure.parts))
    if "chunks" in pure.parts:
        candidates.append(root.joinpath(*pure.parts[pure.parts.index("chunks") :]))
    candidates.append(root / "chunks" / f"start={start}" / f"end={end}" / "manifest.json")
    for candidate in candidates:
        try:
            path = candidate.resolve(strict=False)
            path.relative_to(root.resolve(strict=True))
        except (OSError, ValueError):
            continue
        if path.is_file() and not path.is_symlink():
            _assert_file(path, label)
            return path
    raise CorrectionLineageError(f"{label} is not present in the lifecycle mirror")


def _resolve_nested(root: Path, value: object, label: str, *, fallback: str | None = None) -> Path:
    options: list[str] = []
    if isinstance(value, str) and value:
        options.append(value)
        if value.startswith("lifecycle/"):
            options.append(f"trade-events/{value}")
    if fallback:
        options.append(fallback)
        if fallback.startswith("lifecycle/"):
            options.append(f"trade-events/{fallback}")
    for option in options:
        pure = PurePosixPath(option)
        if pure.is_absolute() or ".." in pure.parts:
            raise CorrectionLineageError(f"{label} is unsafe")
        try:
            path = _safe_relative(root, option, label)
        except CorrectionLineageError:
            continue
        if path.is_file() and not path.is_symlink():
            return path
    raise CorrectionLineageError(f"{label} is not present")


def _lifecycle_row_from_parquet(
    path: Path, chunk: _Chunk, shard_path: str, lifecycle: _Lifecycle
) -> None:
    columns = (
        "event_id",
        "lifecycle_event_id",
        "slot",
        "tx_index",
        "log_index",
        "event_index",
        "program_id",
        "event_name",
        "event_type",
        "mint",
        "base_mint",
        "quote_mint",
        "pool",
        "is_mayhem_mode",
        "values_json",
    )
    parquet = pq.ParquetFile(path)
    try:
        names = set(parquet.schema_arrow.names)
        requested = [column for column in columns if column in names]
        if not requested:
            raise CorrectionLineageError(f"lifecycle shard has no identity columns: {shard_path}")
        for batch in parquet.iter_batches(batch_size=65_536, columns=requested):
            for raw in batch.to_pylist():
                row = cast(dict[str, object], raw)
                values = _values_json(row, f"lifecycle {shard_path}")
                program = _row_value(row, values, ("program_id", "program"))
                name = _row_value(row, values, ("event_name", "event_type"))
                if program not in {PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID}:
                    continue
                if program == PUMP_PROGRAM_ID and name != "CreateEvent":
                    continue
                if program == PUMPSWAP_PROGRAM_ID and name != "CreatePoolEvent":
                    continue
                lifecycle.create_rows += 1
                event_id = _text(
                    _row_value(row, values, ("lifecycle_event_id", "event_id")),
                    "lifecycle create event_id",
                )
                if lifecycle.event_ids is None:
                    lifecycle.event_ids = set()
                if event_id in lifecycle.event_ids:
                    lifecycle.duplicate_count += 1
                    raise CorrectionLineageError(f"duplicate lifecycle create event_id: {event_id}")
                lifecycle.event_ids.add(event_id)
                order_row = dict(row)
                for key in ("slot", "tx_index", "log_index", "event_index"):
                    value = _row_value(row, values, (key,))
                    order_row[key] = value
                ordering = _ordering(order_row, "lifecycle create")
                if ordering[0] < chunk.start or ordering[0] >= chunk.end:
                    raise CorrectionLineageError(
                        f"lifecycle create event is outside chunk bounds: {event_id}"
                    )
                if program == PUMP_PROGRAM_ID:
                    mint = _text(
                        _row_value(row, values, ("mint", "base_mint", "token_mint")),
                        "Pump CreateEvent mint",
                    )
                    quote_raw = _row_value(row, values, ("quote_mint",))
                    quote = _text(quote_raw, f"Pump CreateEvent quote_mint for {mint}")
                    normalized = CANONICAL_SOL_MINT if quote in SOL_ALIASES else None
                    mode = _row_value(row, values, ("is_mayhem_mode",))
                    if not isinstance(mode, bool):
                        raise CorrectionLineageError(f"Pump CreateEvent mode for {mint} is missing")
                    record: _PumpCreate | _PoolCreate = _PumpCreate(
                        mint, quote, normalized, event_id, ordering, mode, shard_path
                    )
                    lifecycle.pump_rows += 1
                    prior_pump = lifecycle.pump.get(mint)
                    if prior_pump is not None:
                        lifecycle.conflict_count += int(prior_pump != record)
                        lifecycle.duplicate_count += int(prior_pump == record)
                        raise CorrectionLineageError(
                            f"duplicate/conflicting Pump CreateEvent mint: {mint}"
                        )
                    lifecycle.pump[mint] = cast(_PumpCreate, record)
                else:
                    pool = _text(
                        _row_value(row, values, ("pool",)), "PumpSwap CreatePoolEvent pool"
                    )
                    base = _text(
                        _row_value(row, values, ("base_mint", "mint")),
                        f"PumpSwap pool {pool} base_mint",
                    )
                    quote = _text(
                        _row_value(row, values, ("quote_mint",)), f"PumpSwap pool {pool} quote_mint"
                    )
                    if base == quote:
                        raise CorrectionLineageError(
                            f"PumpSwap pool {pool} has identical base/quote mints"
                        )
                    sol_values = [value for value in (base, quote) if value in SOL_ALIASES]
                    # Non-SOL pools are retained as source identities.  They
                    # become a fail-closed error only if a feature row maps to
                    # them below; unrelated source pools must not disappear.
                    sol_source = sol_values[0] if len(sol_values) == 1 else None
                    normalized = CANONICAL_SOL_MINT if sol_source is not None else None
                    record = _PoolCreate(
                        pool,
                        base,
                        quote,
                        sol_source,
                        normalized,
                        event_id,
                        ordering,
                        shard_path,
                    )
                    lifecycle.pool_rows += 1
                    prior_pool = lifecycle.pools.get(pool)
                    if prior_pool is not None:
                        lifecycle.conflict_count += int(prior_pool != record)
                        lifecycle.duplicate_count += int(prior_pool == record)
                        raise CorrectionLineageError(
                            f"duplicate/conflicting PumpSwap CreatePoolEvent pool: {pool}"
                        )
                    lifecycle.pools[pool] = record
    finally:
        parquet.close()


def _lifecycle_shards(
    chunk_root: Path,
    chunk_manifest: Mapping[str, object],
    nested: Mapping[str, object],
    parent_files: Mapping[str, Mapping[str, object]],
    chunk: _Chunk,
    lifecycle: _Lifecycle,
    inventory: list[dict[str, object]],
) -> tuple[dict[str, object], ...]:
    summary_raw = chunk_manifest.get("lifecycle_event_dataset")
    summary = _mapping(summary_raw, "lifecycle_event_dataset") if summary_raw is not None else {}
    summary_manifest_path = summary.get("manifest_path")
    if summary_manifest_path is not None and summary_manifest_path != "trade-events/manifest.json":
        raise CorrectionLineageError("lifecycle manifest_path must be trade-events/manifest.json")
    summary_manifest_hash = summary.get("manifest_sha256")
    if (
        summary_manifest_hash is not None
        and _hash(summary_manifest_hash, "lifecycle manifest_sha256") != chunk.trade_manifest_sha256
    ):
        raise CorrectionLineageError("lifecycle summary manifest hash differs")
    raw = nested.get("lifecycle_shards")
    if raw is None:
        raw = summary.get("shards")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raise CorrectionLineageError("lifecycle manifest has no shard list")
    declared_count = nested.get("lifecycle_shard_count", summary.get("shard_count"))
    if _integer(declared_count, "lifecycle shard_count") != len(raw):
        raise CorrectionLineageError("lifecycle shard count differs from list")
    declared_rows = nested.get("lifecycle_row_count", summary.get("row_count"))
    total_rows = _integer(declared_rows, "lifecycle row_count")
    if (
        summary.get("row_count") is not None
        and _integer(summary.get("row_count"), "lifecycle summary row_count") != total_rows
    ):
        raise CorrectionLineageError("lifecycle summary row_count differs from nested manifest")
    if summary.get("shard_count") is not None and _integer(
        summary.get("shard_count"), "lifecycle summary shard_count"
    ) != len(raw):
        raise CorrectionLineageError("lifecycle summary shard_count differs from nested manifest")
    declared_schema: str | None = None
    for candidate in (nested.get("lifecycle_schema_sha256"), summary.get("schema_sha256")):
        if candidate is None:
            continue
        digest = _hash(candidate, "lifecycle schema_sha256")
        if declared_schema is not None and digest != declared_schema:
            raise CorrectionLineageError("lifecycle schema hash differs between manifests")
        declared_schema = digest
    rows = 0
    result: list[dict[str, object]] = []
    seen_paths: set[str] = set()
    for index, raw_item in enumerate(raw):
        item = _mapping(raw_item, f"lifecycle shard {index}")
        text_path = _text(item.get("path"), f"lifecycle shard {index} path")
        pure = PurePosixPath(text_path)
        if pure.is_absolute() or ".." in pure.parts:
            raise CorrectionLineageError(f"lifecycle shard {index} path is unsafe")
        normalized = pure.as_posix()
        if not normalized.startswith("trade-events/lifecycle/"):
            normalized = f"trade-events/{normalized.removeprefix('trade-events/')}"
        if normalized in seen_paths:
            raise CorrectionLineageError(f"duplicate lifecycle shard path: {normalized}")
        seen_paths.add(normalized)
        size = _integer(item.get("byte_count"), f"lifecycle shard {index} byte_count")
        digest = _hash(item.get("sha256"), f"lifecycle shard {index} sha256")
        row_count = _integer(item.get("row_count"), f"lifecycle shard {index} row_count")
        parent_entry = parent_files.get(normalized) or parent_files.get(
            normalized.removeprefix("trade-events/")
        )
        if parent_entry is None:
            raise CorrectionLineageError(
                f"lifecycle shard is absent from parent inventory: {normalized}"
            )
        if (
            _integer(parent_entry.get("byte_count"), "parent lifecycle byte_count") != size
            or _hash(parent_entry.get("sha256"), "parent lifecycle sha256") != digest
        ):
            raise CorrectionLineageError(f"parent lifecycle inventory differs: {normalized}")
        shard_path = _resolve_nested(
            chunk_root, normalized, f"lifecycle shard {index}", fallback=text_path
        )
        _verify_inventory_file(
            chunk_root,
            str(shard_path.relative_to(chunk_root)),
            {"byte_count": size, "sha256": digest, "row_count": row_count},
            f"lifecycle shard {index}",
            parquet=True,
        )
        parquet = pq.ParquetFile(shard_path)
        try:
            schema_hash = _sha256_bytes(parquet.schema_arrow.serialize().to_pybytes())
        finally:
            parquet.close()
        if declared_schema is not None and schema_hash != declared_schema:
            raise CorrectionLineageError(
                f"lifecycle shard schema differs from manifest: {normalized}"
            )
        if declared_schema is None:
            declared_schema = schema_hash
        _lifecycle_row_from_parquet(shard_path, chunk, normalized, lifecycle)
        rows += row_count
        item_payload = {
            "path": normalized,
            "byte_count": size,
            "sha256": digest,
            "row_count": row_count,
            "schema_sha256": schema_hash,
        }
        result.append(item_payload)
        inventory.append(
            {"role": "lifecycle_shard", "chunk_start_slot_inclusive": chunk.start, **item_payload}
        )
    if rows != total_rows:
        raise CorrectionLineageError("lifecycle row_count differs from shards")
    return tuple(result)


def _load_lifecycle_mirror(root_value: str | Path) -> _Seal:
    root = _assert_root(root_value, "lifecycle mirror")
    _assert_no_symlinks(root, "lifecycle mirror")
    manifest_path = _manifest_path(
        root, ("dataset-manifest.json", "manifest.json"), "lifecycle mirror"
    )
    manifest, body, digest = _json_file(manifest_path, "lifecycle aggregate manifest")
    if manifest.get("kind") != DATASET_KIND or manifest.get("version") != SOURCE_VERSION:
        raise CorrectionLineageError("lifecycle aggregate is not a v1 fresh development dataset")
    if (
        manifest.get("development_only") is not True
        or manifest.get("holdout_payload_reads_allowed") is not False
    ):
        raise CorrectionLineageError("lifecycle aggregate is not development-only")
    _reject_downstream_flags(manifest, "lifecycle aggregate")
    if (
        manifest.get("start_slot_inclusive") != DEVELOPMENT_START_SLOT
        or manifest.get("end_slot_exclusive") != DEVELOPMENT_END_SLOT
    ):
        raise CorrectionLineageError("lifecycle aggregate bounds differ from frozen development")
    if manifest.get("coverage_contiguous_and_exact") is False:
        raise CorrectionLineageError("lifecycle aggregate coverage is not exact")
    raw_chunks = manifest.get("chunks")
    if (
        not isinstance(raw_chunks, Sequence)
        or isinstance(raw_chunks, (str, bytes, bytearray))
        or len(raw_chunks) != EXPECTED_SOURCE_CHUNK_COUNT
    ):
        raise CorrectionLineageError("lifecycle aggregate must contain all 19 source chunks")
    chunks: list[_Chunk] = []
    inventory: list[dict[str, object]] = [
        {
            "role": "aggregate_manifest",
            "path": manifest_path.name,
            "byte_count": len(body),
            "sha256": digest,
        }
    ]
    lifecycle = _Lifecycle({}, {})
    cursor = DEVELOPMENT_START_SLOT
    seen_keys: set[str] = set()
    for index, raw_ref in enumerate(raw_chunks):
        start, end, key, expected_hash, expected_size = _chunk_reference(
            raw_ref, index, require_size=False
        )
        if start != cursor or end <= start or end > DEVELOPMENT_END_SLOT:
            raise CorrectionLineageError("lifecycle aggregate chunks are not contiguous and exact")
        cursor = end
        if key in seen_keys:
            raise CorrectionLineageError(f"lifecycle aggregate repeats chunk manifest: {key}")
        seen_keys.add(key)
        chunk_path = _resolve_chunk_manifest(root, key, start, end, f"chunk {index} manifest")
        chunk_size, chunk_hash = _sha256_file(chunk_path)
        if (
            expected_size is not None and chunk_size != expected_size
        ) or chunk_hash != expected_hash:
            raise CorrectionLineageError(f"chunk {index} manifest differs from aggregate binding")
        chunk_manifest, chunk_body, _ = _json_file(chunk_path, f"chunk {index} manifest")
        if (
            chunk_manifest.get("kind") not in _SUPPORTED_CHUNK_KINDS
            or chunk_manifest.get("version") != SOURCE_VERSION
        ):
            raise CorrectionLineageError(f"chunk {index} manifest kind/version is unsupported")
        if (
            chunk_manifest.get("start_slot_inclusive") != start
            or chunk_manifest.get("end_slot_exclusive") != end
        ):
            raise CorrectionLineageError(f"chunk {index} manifest bounds differ from aggregate")
        _reject_downstream_flags(chunk_manifest, f"chunk {index} manifest")
        parent_files = _files_inventory(chunk_manifest.get("files"), f"chunk {index} files")
        trade_raw = chunk_manifest.get("trade_event_dataset")
        if not isinstance(trade_raw, Mapping):
            raise CorrectionLineageError(f"chunk {index} has no trade-event manifest binding")
        trade_ref = _mapping(trade_raw, f"chunk {index} trade_event_dataset")
        trade_rel = _text(trade_ref.get("path"), f"chunk {index} trade manifest path")
        if trade_rel != "trade-events/manifest.json":
            raise CorrectionLineageError(f"chunk {index} trade manifest path is unsafe")
        trade_hash = _hash(trade_ref.get("manifest_sha256"), f"chunk {index} trade manifest sha256")
        trade_entry = parent_files.get(trade_rel) or parent_files.get("manifest.json")
        if trade_entry is None:
            raise CorrectionLineageError(f"chunk {index} parent inventory omits trade manifest")
        trade_size = _integer(
            trade_entry.get("byte_count"), f"chunk {index} trade manifest byte_count"
        )
        if (
            trade_ref.get("manifest_byte_count") is not None
            and _integer(trade_ref.get("manifest_byte_count"), "trade manifest byte_count")
            != trade_size
        ):
            raise CorrectionLineageError(f"chunk {index} trade manifest byte count differs")
        if _hash(trade_entry.get("sha256"), "trade manifest inventory sha256") != trade_hash:
            raise CorrectionLineageError(f"chunk {index} trade manifest hash differs")
        trade_path = _resolve_nested(chunk_path.parent, trade_rel, f"chunk {index} trade manifest")
        actual_trade_size, actual_trade_hash = _sha256_file(trade_path)
        if actual_trade_size != trade_size or actual_trade_hash != trade_hash:
            raise CorrectionLineageError(f"chunk {index} trade manifest differs from binding")
        trade_manifest, trade_body, _ = _json_file(trade_path, f"chunk {index} trade manifest")
        if (
            trade_manifest.get("kind") != TRADE_EVENT_KIND
            or trade_manifest.get("version") != SOURCE_VERSION
        ):
            raise CorrectionLineageError(f"chunk {index} trade manifest kind/version mismatch")
        _reject_downstream_flags(trade_manifest, f"chunk {index} trade manifest")
        # A nested lifecycle manifest may carry its own inventory in addition
        # to the parent chunk inventory.  When present, bind every nested
        # entry back to the parent bytes/hash seal.  Older production mirrors
        # omit this optional field, so absence is intentionally accepted.
        nested_files_raw = trade_manifest.get("files")
        if nested_files_raw is not None:
            nested_files = _files_inventory(nested_files_raw, f"chunk {index} trade files")
            for nested_name, nested_entry in nested_files.items():
                candidates = (
                    nested_name,
                    f"trade-events/{nested_name}",
                    "trade-events/manifest.json" if nested_name == "manifest.json" else "",
                )
                parent_name = next(
                    (candidate for candidate in candidates if candidate in parent_files), None
                )
                if parent_name is None:
                    raise CorrectionLineageError(
                        f"chunk {index} nested trade inventory omits parent file: {nested_name}"
                    )
                parent_entry = parent_files[parent_name]
                if _integer(nested_entry.get("byte_count"), "nested trade byte_count") != _integer(
                    parent_entry.get("byte_count"), "parent trade byte_count"
                ) or _hash(nested_entry.get("sha256"), "nested trade sha256") != _hash(
                    parent_entry.get("sha256"), "parent trade sha256"
                ):
                    raise CorrectionLineageError(
                        f"chunk {index} nested trade inventory differs: {nested_name}"
                    )
        chunk_stub = _Chunk(
            start,
            end,
            key,
            expected_hash,
            chunk_size,
            chunk_path,
            chunk_manifest,
            trade_hash,
            trade_size,
            trade_path,
            trade_manifest,
            (),
        )
        inventory.append(
            {
                "role": "chunk_manifest",
                "chunk_start_slot_inclusive": start,
                "path": str(chunk_path.relative_to(root)),
                "byte_count": chunk_size,
                "sha256": chunk_hash,
            }
        )
        inventory.append(
            {
                "role": "trade_manifest",
                "chunk_start_slot_inclusive": start,
                "path": str(trade_path.relative_to(root)),
                "byte_count": trade_size,
                "sha256": trade_hash,
            }
        )
        shards = _lifecycle_shards(
            chunk_path.parent,
            chunk_manifest,
            trade_manifest,
            parent_files,
            chunk_stub,
            lifecycle,
            inventory,
        )
        chunks.append(
            _Chunk(
                start,
                end,
                key,
                expected_hash,
                chunk_size,
                chunk_path,
                chunk_manifest,
                trade_hash,
                trade_size,
                trade_path,
                trade_manifest,
                shards,
            )
        )
    if cursor != DEVELOPMENT_END_SLOT:
        raise CorrectionLineageError("lifecycle aggregate does not reach development end")
    return _Seal(
        root,
        manifest_path,
        manifest,
        digest,
        len(body),
        tuple(chunks),
        tuple(
            sorted(
                inventory,
                key=_inventory_sort_key,
            )
        ),
        lifecycle,
    )


def _load_v1(
    v1_value: str | Path,
) -> tuple[
    Path, Path, dict[str, object], str, dict[str, object], tuple[dict[str, object], ...], int, int
]:
    root = _assert_root(v1_value, "v1 feature root")
    _assert_no_symlinks(root, "v1 feature root")
    manifest_path = root / "manifest.json"
    manifest, body, manifest_hash = _json_file(manifest_path, "v1 feature manifest")
    if manifest.get("kind") != V1_FEATURE_KIND or manifest.get("version") != V1_VERSION:
        raise CorrectionLineageError("v1 feature root kind/version is unsupported")
    if manifest.get("status") != "complete":
        raise CorrectionLineageError("v1 feature root is partial or not complete")
    if (
        manifest.get("development_only") is not True
        or manifest.get("holdout_payload_reads_allowed") is not False
    ):
        raise CorrectionLineageError("v1 feature root is not development-only")
    _reject_downstream_flags(manifest, "v1 feature manifest")
    if (
        manifest.get("start_slot_inclusive") != DEVELOPMENT_START_SLOT
        or manifest.get("end_slot_exclusive") != DEVELOPMENT_END_SLOT
    ):
        raise CorrectionLineageError("v1 feature bounds differ from frozen development")
    raw_sources = manifest.get("sources")
    if (
        not isinstance(raw_sources, Sequence)
        or isinstance(raw_sources, (str, bytes, bytearray))
        or len(raw_sources) != EXPECTED_SOURCE_CHUNK_COUNT
    ):
        raise CorrectionLineageError("v1 manifest must bind all 19 source chunks")
    source_refs: list[dict[str, object]] = []
    cursor = DEVELOPMENT_START_SLOT
    for index, raw in enumerate(raw_sources):
        start, end, key, digest, size = _chunk_reference(raw, index)
        if start != cursor or end <= start or end > DEVELOPMENT_END_SLOT:
            raise CorrectionLineageError("v1 source intervals are not contiguous and exact")
        cursor = end
        source_item: dict[str, object] = {
            "start_slot_inclusive": start,
            "end_slot_exclusive": end,
            "manifest_key": key,
            "manifest_sha256": digest,
        }
        if size is not None:
            source_item["manifest_byte_count"] = size
        source_refs.append(source_item)
    if cursor != DEVELOPMENT_END_SLOT:
        raise CorrectionLineageError("v1 sources do not cover development")
    files = _files_inventory(manifest.get("files"), "v1 feature files")
    feature_name = "features.parquet"
    rejected_name = (
        "rejected-amounts.parquet"
        if "rejected-amounts.parquet" in files
        else "rejected-events.parquet"
    )
    audit_name = _text(manifest.get("audit_path", "audit.json"), "v1 audit_path")
    if feature_name not in files or rejected_name not in files or audit_name not in files:
        raise CorrectionLineageError("v1 feature inventory is missing features/rejected/audit")
    feature_meta = _verify_inventory_file(
        root, feature_name, files[feature_name], "v1 features", parquet=True
    )
    rejected_meta = _verify_inventory_file(
        root, rejected_name, files[rejected_name], "v1 rejected rows", parquet=True
    )
    _require_parquet_columns(
        root / feature_name,
        ("event_id", "market_id", "mint", "slot", "tx_index", "log_index", "event_index"),
        "v1 features",
    )
    _require_parquet_columns(
        root / rejected_name,
        (
            "event_id",
            "market_id",
            "mint",
            "quote_mint",
            "slot",
            "tx_index",
            "log_index",
            "event_index",
        ),
        "v1 rejected rows",
    )
    audit_meta = _verify_inventory_file(root, audit_name, files[audit_name], "v1 audit")
    # v1 ``row_count`` is the accepted feature-stream count (the rejected
    # stream has its own inventory count).  Bind it explicitly before any
    # source rows are audited so a stale top-level declaration cannot pass.
    declared_feature_rows = _integer(manifest.get("row_count"), "v1 row_count")
    if declared_feature_rows != feature_meta.get("row_count"):
        raise CorrectionLineageError("v1 top-level row_count differs from features.parquet")
    audit_path = _safe_relative(root, audit_name, "v1 audit_path")
    audit, _, audit_hash = _json_file(audit_path, "v1 audit")
    if audit.get("version") != V1_VERSION or (audit.get("kind") not in {None, V1_AUDIT_KIND}):
        raise CorrectionLineageError("v1 audit kind/version is unsupported")
    _reject_downstream_flags(audit, "v1 audit")
    if (
        manifest.get("audit_sha256") is not None
        and _hash(manifest.get("audit_sha256"), "v1 audit_sha256") != audit_hash
    ):
        raise CorrectionLineageError("v1 audit hash differs from manifest")
    for name, meta in (
        (feature_name, feature_meta),
        (rejected_name, rejected_meta),
        (audit_name, audit_meta),
    ):
        declared = files[name]
        if declared.get("row_count") is not None and meta.get("row_count") != declared.get(
            "row_count"
        ):
            raise CorrectionLineageError(f"v1 {name} row count differs from inventory")
    # Keep only expected inventory paths.  The manifest is the one allowed
    # unlisted file; hidden partial/holdout sidecars are never accepted.
    listed = set(files)
    actual = {
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }
    if actual - listed - {"manifest.json"} or listed - actual:
        raise CorrectionLineageError("v1 root files differ from its sealed inventory")
    return (
        root,
        root / feature_name,
        manifest,
        manifest_hash,
        audit,
        tuple(source_refs),
        _integer(feature_meta.get("row_count", 0), "v1 feature row_count"),
        _integer(rejected_meta.get("row_count", 0), "v1 rejected row_count"),
    )


def _load_mode_root(
    mode_value: str | Path, lifecycle: _Seal
) -> tuple[
    dict[str, object],
    str,
    dict[str, object],
    str,
    str,
    tuple[dict[str, object], ...],
]:
    root = _assert_root(mode_value, "market-mode root")
    _assert_no_symlinks(root, "market-mode root")
    manifest_path = root / "manifest.json"
    manifest, body, digest = _json_file(manifest_path, "market-mode manifest")
    if (
        manifest.get("kind") != MODE_INDEX_KIND
        or manifest.get("version") != MODE_VERSION
        or manifest.get("status") != "complete"
    ):
        raise CorrectionLineageError("market-mode root is partial or not v2")
    if (
        manifest.get("development_only") is not True
        or manifest.get("holdout_payload_reads_allowed") is not False
        or manifest.get("classification_complete") is not True
    ):
        raise CorrectionLineageError("market-mode root is not complete development-only evidence")
    _reject_downstream_flags(manifest, "market-mode manifest")
    if (
        manifest.get("start_slot_inclusive") != DEVELOPMENT_START_SLOT
        or manifest.get("end_slot_exclusive") != DEVELOPMENT_END_SLOT
    ):
        raise CorrectionLineageError("market-mode root bounds differ from frozen development")
    aggregate_hash = lifecycle.manifest_sha256
    for field in ("source_dataset_manifest_sha256", "source_manifest_sha256"):
        if (
            manifest.get(field) is not None
            and _hash(manifest.get(field), f"market-mode {field}") != aggregate_hash
        ):
            raise CorrectionLineageError(f"market-mode {field} differs from lifecycle aggregate")
    source_chunks = manifest.get("source_chunks")
    if (
        not isinstance(source_chunks, Sequence)
        or isinstance(source_chunks, (str, bytes, bytearray))
        or len(source_chunks) != EXPECTED_SOURCE_CHUNK_COUNT
    ):
        raise CorrectionLineageError("market-mode source_chunks must bind all 19 chunks")
    for index, raw in enumerate(source_chunks):
        item = _mapping(raw, f"market-mode source chunk {index}")
        chunk = lifecycle.chunks[index]
        if (
            item.get("start_slot_inclusive") != chunk.start
            or item.get("end_slot_exclusive") != chunk.end
            or _hash(item.get("manifest_sha256"), "market-mode source manifest_sha256")
            != chunk.manifest_sha256
        ):
            raise CorrectionLineageError(
                f"market-mode source chunk {index} differs from lifecycle mirror"
            )
        if item.get("manifest_key") is not None and item.get("manifest_key") != chunk.manifest_key:
            raise CorrectionLineageError(f"market-mode source manifest key {index} differs")
        if (
            item.get("manifest_byte_count") is not None
            and _integer(item.get("manifest_byte_count"), "market-mode source manifest_byte_count")
            != chunk.manifest_byte_count
        ):
            raise CorrectionLineageError(f"market-mode source chunk {index} byte count differs")
        if (
            item.get("lifecycle_manifest_sha256") is not None
            and _hash(
                item.get("lifecycle_manifest_sha256"), "market-mode lifecycle manifest_sha256"
            )
            != chunk.trade_manifest_sha256
        ):
            raise CorrectionLineageError(f"market-mode lifecycle manifest {index} differs")
        if (
            item.get("lifecycle_manifest_byte_count") is not None
            and _integer(
                item.get("lifecycle_manifest_byte_count"),
                "market-mode lifecycle manifest_byte_count",
            )
            != chunk.trade_manifest_byte_count
        ):
            raise CorrectionLineageError(f"market-mode lifecycle manifest size {index} differs")
        lifecycle_path = item.get("lifecycle_manifest_path")
        if lifecycle_path is not None:
            expected_lifecycle_path = str(chunk.trade_manifest_path.relative_to(lifecycle.root))
            if lifecycle_path != expected_lifecycle_path:
                raise CorrectionLineageError(f"market-mode lifecycle manifest path {index} differs")
        listed_shards = item.get("lifecycle_shards")
        if not isinstance(listed_shards, Sequence) or len(listed_shards) != len(chunk.shards):
            raise CorrectionLineageError(
                f"market-mode lifecycle shard binding {index} is incomplete"
            )
        for shard_raw, expected in zip(listed_shards, chunk.shards, strict=True):
            shard = _mapping(shard_raw, "market-mode lifecycle shard")
            if (
                shard.get("path")
                not in {
                    expected.get("path"),
                    str(expected.get("path", "")).removeprefix("trade-events/"),
                }
                or _hash(shard.get("sha256"), "market-mode lifecycle shard sha256")
                != expected.get("sha256")
                or _integer(shard.get("byte_count"), "market-mode lifecycle shard byte_count")
                != expected.get("byte_count")
            ):
                raise CorrectionLineageError(f"market-mode lifecycle shard binding {index} differs")
    files = _files_inventory(manifest.get("files"), "market-mode files")
    if set(files) != {"modes.parquet", "audit.json"}:
        raise CorrectionLineageError(
            "market-mode inventory must contain modes.parquet and audit.json"
        )
    modes_meta = _verify_inventory_file(
        root, "modes.parquet", files["modes.parquet"], "market modes", parquet=True
    )
    _verify_inventory_file(root, "audit.json", files["audit.json"], "market-mode audit")
    modes_path = root / "modes.parquet"
    audit, _, audit_hash = _json_file(root / "audit.json", "market-mode audit")
    try:
        mode_parquet = pq.ParquetFile(modes_path)
        physical_schema_sha256 = _sha256_bytes(mode_parquet.schema_arrow.serialize().to_pybytes())
        mode_parquet.close()
    except Exception as error:
        raise CorrectionLineageError("market modes schema cannot be inspected") from error
    if physical_schema_sha256 != MODE_OUTPUT_SCHEMA_SHA256:
        raise CorrectionLineageError("market modes output schema differs from supported v2 schema")
    for owner, value in (("manifest", manifest), ("audit", audit)):
        declared_schema = _hash(
            value.get("output_schema_sha256"), f"market-mode {owner} output_schema_sha256"
        )
        if declared_schema != MODE_OUTPUT_SCHEMA_SHA256:
            raise CorrectionLineageError(
                f"market-mode {owner} output_schema_sha256 differs from modes.parquet"
            )
    if (
        audit.get("kind") != MODE_AUDIT_KIND
        or audit.get("version") != MODE_VERSION
        or audit.get("status") != "complete"
        or audit.get("start_slot_inclusive") != DEVELOPMENT_START_SLOT
        or audit.get("end_slot_exclusive") != DEVELOPMENT_END_SLOT
    ):
        raise CorrectionLineageError("market-mode audit is incomplete")
    _reject_downstream_flags(audit, "market-mode audit")
    if (
        audit.get("source_manifest_sha256") is not None
        and _hash(audit.get("source_manifest_sha256"), "market-mode audit source hash")
        != aggregate_hash
    ):
        raise CorrectionLineageError("market-mode audit source hash differs")
    if audit.get("modes_sha256") is not None and _hash(
        audit.get("modes_sha256"), "market-mode modes_sha256"
    ) != cast(str, modes_meta["sha256"]):
        raise CorrectionLineageError("market-mode audit modes hash differs")
    if manifest.get("modes_sha256") is not None and _hash(
        manifest.get("modes_sha256"), "market-mode modes_sha256"
    ) != cast(str, modes_meta["sha256"]):
        raise CorrectionLineageError("market-mode manifest modes hash differs")
    # Verify mode audit source chunks whenever present; it must not be a loose
    # second source binding.
    audit_chunks = audit.get("source_chunks")
    if (
        not isinstance(audit_chunks, Sequence)
        or isinstance(audit_chunks, (str, bytes, bytearray))
        or len(audit_chunks) != EXPECTED_SOURCE_CHUNK_COUNT
    ):
        raise CorrectionLineageError("market-mode audit source_chunks must bind all 19 chunks")
    for index, raw in enumerate(audit_chunks):
        item = _mapping(raw, f"market-mode audit source chunk {index}")
        chunk = lifecycle.chunks[index]
        if (
            item.get("start_slot_inclusive") != chunk.start
            or item.get("end_slot_exclusive") != chunk.end
            or _hash(item.get("manifest_sha256"), "market-mode audit source hash")
            != chunk.manifest_sha256
        ):
            raise CorrectionLineageError(f"market-mode audit source chunk {index} differs")
        if item.get("manifest_key") is not None and item.get("manifest_key") != chunk.manifest_key:
            raise CorrectionLineageError(f"market-mode audit source manifest key {index} differs")
        lifecycle_path = item.get("lifecycle_manifest_path")
        if lifecycle_path is not None:
            expected_lifecycle_path = str(chunk.trade_manifest_path.relative_to(lifecycle.root))
            if lifecycle_path != expected_lifecycle_path:
                raise CorrectionLineageError(
                    f"market-mode audit lifecycle manifest path {index} differs"
                )
    # Parse sealed mode records.  Identity is independently compared with the
    # lifecycle CreateEvent set below; no quote identity is inferred here.
    modes: list[dict[str, object]] = []
    previous: tuple[int, int, int, int, str] | None = None
    seen_ids: set[str] = set()
    seen_mints: set[str] = set()
    required = _MODE_REQUIRED_COLUMNS
    for row in _iter_parquet_rows(modes_path, required, "market modes"):
        for name in required:
            if name not in row:
                raise CorrectionLineageError(f"market modes is missing required column {name}")
        if (
            row.get("schema_version") != MODE_VERSION
            or row.get("record_type") != "pump_create_market_mode"
        ):
            raise CorrectionLineageError("market modes row kind/version is invalid")
        mint = _text(row.get("mint"), "market-mode mint")
        event_id = _text(row.get("create_event_id"), "market-mode create_event_id")
        if not isinstance(row.get("is_mayhem_mode"), bool):
            raise CorrectionLineageError(f"market-mode mode for {mint} is not boolean")
        key_row = {
            "slot": row.get("create_slot"),
            "tx_index": row.get("create_tx_index"),
            "log_index": row.get("create_log_index"),
            "event_index": row.get("create_event_index"),
            "event_id": event_id,
        }
        key = _ordering(key_row, "market-mode ordering")
        if previous is not None and key <= previous:
            raise CorrectionLineageError("market modes are not strictly chain ordered")
        previous = key
        if event_id in seen_ids or mint in seen_mints:
            raise CorrectionLineageError(f"duplicate/conflicting market mode identity: {mint}")
        seen_ids.add(event_id)
        seen_mints.add(mint)
        ordering_raw = row.get("create_ordering_key")
        if not isinstance(ordering_raw, str):
            raise CorrectionLineageError(f"market-mode ordering key for {mint} is missing")
        try:
            parsed = json.loads(ordering_raw)
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise CorrectionLineageError(
                f"market-mode ordering key for {mint} is invalid"
            ) from error
        if parsed != list(key):
            raise CorrectionLineageError(f"market-mode ordering key for {mint} differs")
        quote = row.get("quote_mint")
        if quote is not None:
            _text(quote, f"market-mode quote_mint for {mint}")
        modes.append(row)
    mode_rows = _integer(modes_meta.get("row_count", len(modes)), "market modes row_count")
    if mode_rows != len(modes) or manifest.get("row_count") not in {None, len(modes)}:
        raise CorrectionLineageError("market modes row count differs from manifest")
    listed = set(files)
    actual = {
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }
    if actual - listed - {"manifest.json"} or listed - actual:
        raise CorrectionLineageError("market-mode root files differ from inventory")
    return manifest, digest, audit, cast(str, modes_meta["sha256"]), audit_hash, tuple(modes)


def _same_quote_identity(left: object, right: object) -> bool:
    if not isinstance(left, str) or not isinstance(right, str):
        return left == right
    if left in SOL_ALIASES and right in SOL_ALIASES:
        return True
    return left == right


def _compare_mode_lifecycle(
    mode_rows: Sequence[Mapping[str, object]], lifecycle: _Lifecycle
) -> None:
    by_mint: dict[str, Mapping[str, object]] = {cast(str, row["mint"]): row for row in mode_rows}
    if set(by_mint) != set(lifecycle.pump):
        missing = sorted(set(lifecycle.pump) - set(by_mint))
        extra = sorted(set(by_mint) - set(lifecycle.pump))
        raise CorrectionLineageError(
            f"market-mode CreateEvent coverage differs (missing={missing[:3]}, extra={extra[:3]})"
        )
    for mint, create in lifecycle.pump.items():
        row = by_mint[mint]
        mode_key = (
            _integer(row["create_slot"], "market-mode create_slot"),
            _integer(row["create_tx_index"], "market-mode create_tx_index"),
            _integer(row["create_log_index"], "market-mode create_log_index"),
            _integer(row["create_event_index"], "market-mode create_event_index"),
            cast(str, row["create_event_id"]),
        )
        if mode_key != create.ordering or row.get("is_mayhem_mode") != create.is_mayhem_mode:
            raise CorrectionLineageError(f"market-mode CreateEvent identity differs for {mint}")
        quote = row.get("quote_mint")
        if quote is not None and not _same_quote_identity(quote, create.quote_mint):
            raise CorrectionLineageError(f"market-mode quote identity differs for {mint}")


def _row_identity(
    row: Mapping[str, object], label: str
) -> tuple[str, str, str, str | None, tuple[int, int, int, int, str]]:
    event_id = _text(row.get("event_id"), f"{label} event_id")
    market_id = _text(row.get("market_id"), f"{label} market_id")
    mint = _text(row.get("mint"), f"{label} mint")
    phase_raw = row.get("phase")
    phase = None if phase_raw is None else _text(phase_raw, f"{label} phase")
    key = _ordering(row, label)
    return event_id, market_id, mint, phase, key


def _audit_feature_quotes(
    feature_path: Path,
    rejected_path: Path,
    lifecycle: _Lifecycle,
    expected_feature_rows: int,
    expected_rejected_rows: int,
) -> dict[str, object]:
    accepted_columns = (
        "event_id",
        "market_id",
        "mint",
        "quote_mint",
        "slot",
        "tx_index",
        "log_index",
        "event_index",
        "phase",
    )
    rejected_columns = (
        "event_id",
        "market_id",
        "mint",
        "quote_mint",
        "slot",
        "tx_index",
        "log_index",
        "event_index",
        "phase",
    )
    streams: tuple[tuple[str, Path, Sequence[str], int], ...] = (
        ("accepted", feature_path, accepted_columns, expected_feature_rows),
        ("rejected", rejected_path, rejected_columns, expected_rejected_rows),
    )
    counts: dict[str, dict[str, int]] = {
        stream: {
            "rows": 0,
            "covered_rows": 0,
            "unmatched_rows": 0,
            "duplicate_rows": 0,
            "conflict_rows": 0,
            "sol_quote_rows": 0,
            "mode_exclusion_rows": 0,
            "mode_unclassified_rows": 0,
            "mode_not_applicable_rows": 0,
        }
        for stream, _path, _columns, _expected_rows in streams
    }
    alias_counts: Counter[str] = Counter()
    proof_hash = hashlib.sha256()
    identity_hash = hashlib.sha256()

    def ordered_rows(
        stream: str, path: Path, columns: Sequence[str]
    ) -> Iterator[tuple[tuple[int, int, int, int, str], str, dict[str, object]]]:
        previous: tuple[int, int, int, int, str] | None = None
        for raw in _iter_parquet_rows(path, columns, f"v1 {stream} rows"):
            key = _ordering(raw, f"v1 {stream}")
            if previous is not None and key <= previous:
                raise CorrectionLineageError(f"v1 {stream} rows are not strictly chain ordered")
            previous = key
            yield key, stream, raw

    # Merge the two sealed streams into one canonical chronological audit.  A
    # row's event_id is the final ordering tie-break, so rows sharing all four
    # numeric coordinates remain distinct and deterministic.
    merged = heapq.merge(
        *(ordered_rows(stream, path, columns) for stream, path, columns, _expected_rows in streams),
        key=lambda item: item[0],
    )
    seen_event_ids: set[str] = set()
    previous_global: tuple[int, int, int, int, str] | None = None
    for key, stream, raw in merged:
        counts[stream]["rows"] += 1
        event_id, market_id, mint, phase, identity_key = _row_identity(raw, f"v1 {stream}")
        # ``identity_key`` is recomputed from the same row to bind event_id and
        # all numeric coordinates, rather than trusting a generator-side value.
        if identity_key != key:
            raise CorrectionLineageError(f"v1 {stream} ordering identity drift: {event_id}")
        if key[0] < DEVELOPMENT_START_SLOT or key[0] >= DEVELOPMENT_END_SLOT:
            raise CorrectionLineageError(
                f"v1 {stream} row is outside development bounds: {event_id}"
            )
        if previous_global is not None and key <= previous_global:
            counts[stream]["duplicate_rows"] += 1
            if key == previous_global:
                raise CorrectionLineageError(
                    f"duplicate accepted/rejected feature identity: {event_id}"
                )
            raise CorrectionLineageError("accepted/rejected rows are not strictly chain ordered")
        previous_global = key
        if event_id in seen_event_ids:
            counts[stream]["duplicate_rows"] += 1
            raise CorrectionLineageError(f"duplicate v1 feature event_id: {event_id}")
        seen_event_ids.add(event_id)
        row_quote = raw.get("quote_mint")
        if row_quote is not None:
            _text(row_quote, f"v1 {stream} quote_mint")
        phase_norm = phase.casefold() if phase is not None else None
        pool = lifecycle.pools.get(market_id)
        pump = lifecycle.pump.get(mint)
        anchor: tuple[int, int, int, int, str]
        mode_record: _PumpCreate | None
        mode_not_applicable = False
        # Rejected rows from the v1 sidecar commonly omit ``phase``.  A
        # market_id that resolves to a PumpSwap pool is sufficient and
        # deterministic venue evidence even when the same token also has a
        # Pump CreateEvent; do not call that identity ambiguous.
        if phase_norm in {"pumpswap", "pump_swap", "pump-swap"} or (
            phase_norm is None and pool is not None
        ):
            if pool is None:
                counts[stream]["unmatched_rows"] += 1
                raise CorrectionLineageError(f"unmatched PumpSwap pool for {stream} row {event_id}")
            if mint not in {pool.base_mint, pool.quote_mint} or mint in SOL_ALIASES:
                counts[stream]["conflict_rows"] += 1
                raise CorrectionLineageError(
                    f"PumpSwap pool/mint identity conflicts for {event_id}"
                )
            source_quote = pool.sol_source_mint
            normalized = pool.normalized_quote_mint
            anchor = pool.ordering
            # The feature mint identifies the token whose Pump CreateEvent
            # carries the mode.  For flipped pools SOL may be ``base_mint``
            # and the token may be ``quote_mint``; looking up base_mint would
            # silently lose the Mayhem classification in that orientation.
            mode_record = lifecycle.pump.get(mint)
            mode_not_applicable = mode_record is None
            if source_quote is None or normalized is None:
                counts[stream]["conflict_rows"] += 1
                raise CorrectionLineageError(
                    f"non-SOL PumpSwap pool quote (canonical SOL required) for {stream} row {event_id}"
                )
        elif phase_norm in {"bonding_curve", "bonding", "pump"} or (
            phase_norm is None and pump is not None
        ):
            if pump is None:
                counts[stream]["unmatched_rows"] += 1
                raise CorrectionLineageError(
                    f"unmatched Pump CreateEvent mint for {stream} row {event_id}"
                )
            source_quote = pump.quote_mint
            normalized = pump.normalized_quote_mint
            anchor = pump.ordering
            mode_record = pump
            if normalized is None:
                counts[stream]["conflict_rows"] += 1
                raise CorrectionLineageError(
                    f"non-SOL Pump quote (canonical SOL required) for {stream} row {event_id}"
                )
        elif phase_norm is None:
            counts[stream]["unmatched_rows"] += 1
            raise CorrectionLineageError(f"unmatched feature identity for {stream} row {event_id}")
        else:
            raise CorrectionLineageError(f"unknown phase for {stream} row {event_id}")
        if key <= anchor:
            raise CorrectionLineageError(
                f"v1 {stream} row is not strictly after its lifecycle create anchor: {event_id}"
            )
        if row_quote is not None and (
            row_quote not in SOL_ALIASES or normalize_sol_quote(row_quote) != normalized
        ):
            counts[stream]["conflict_rows"] += 1
            raise CorrectionLineageError(
                f"v1 {stream} quote identity is not canonical SOL for {event_id}"
            )
        if mode_not_applicable:
            counts[stream]["mode_not_applicable_rows"] += 1
        elif mode_record is None:
            counts[stream]["mode_unclassified_rows"] += 1
        elif mode_record.is_mayhem_mode:
            counts[stream]["mode_exclusion_rows"] += 1
        counts[stream]["covered_rows"] += 1
        counts[stream]["sol_quote_rows"] += 1
        alias_counts[source_quote] += 1
        proof = {
            "stream": stream,
            "event_id": event_id,
            "market_id": market_id,
            "mint": mint,
            "source_quote_mint": source_quote,
            "normalized_quote_mint": normalized,
        }
        proof_hash.update(canonical_json(proof))
        identity_hash.update(
            canonical_json(
                {
                    "stream": stream,
                    "event_id": event_id,
                    "market_id": market_id,
                    "mint": mint,
                    "ordering": list(key),
                }
            )
        )
    for stream, _path, _columns, expected_rows in streams:
        if counts[stream]["rows"] != expected_rows:
            raise CorrectionLineageError(f"v1 {stream} row count differs from manifest")
    return {
        "accepted": counts["accepted"],
        "rejected": counts["rejected"],
        "total_rows": counts["accepted"]["rows"] + counts["rejected"]["rows"],
        "total_covered_rows": counts["accepted"]["covered_rows"]
        + counts["rejected"]["covered_rows"],
        "total_unmatched_rows": counts["accepted"]["unmatched_rows"]
        + counts["rejected"]["unmatched_rows"],
        "duplicate_count": sum(item["duplicate_rows"] for item in counts.values()),
        "conflict_count": sum(item["conflict_rows"] for item in counts.values()),
        "canonical_ordering": "slot,tx_index,log_index,event_index,event_id",
        "canonical_ordering_sha256": identity_hash.hexdigest(),
        "source_quote_mint_counts": dict(sorted(alias_counts.items())),
        "normalized_quote_mint_counts": {CANONICAL_SOL_MINT: sum(alias_counts.values())},
        "proof_sha256": proof_hash.hexdigest(),
        "identity_sha256": identity_hash.hexdigest(),
        "mode_exclusion_accepted_rows": counts["accepted"]["mode_exclusion_rows"],
        "mode_exclusion_rejected_rows": counts["rejected"]["mode_exclusion_rows"],
        "mode_unclassified_accepted_rows": counts["accepted"]["mode_unclassified_rows"],
        "mode_unclassified_rejected_rows": counts["rejected"]["mode_unclassified_rows"],
        "mode_not_applicable_accepted_rows": counts["accepted"]["mode_not_applicable_rows"],
        "mode_not_applicable_rejected_rows": counts["rejected"]["mode_not_applicable_rows"],
    }


@dataclass(frozen=True, slots=True)
class CorrectionLineageAudit:
    """Immutable canonical audit returned by :func:`verify_correction_lineage_v2`."""

    payload: Mapping[str, object]
    canonical_bytes: bytes
    sha256: str

    @property
    def digest(self) -> str:
        return self.sha256

    @property
    def inventory(self) -> object:
        return self.payload.get("inventory")

    @property
    def canonical_audit_bytes(self) -> bytes:
        return self.canonical_bytes


def verify_correction_lineage_v2(
    v1_root: str | Path,
    mode_root: str | Path,
    lifecycle_root: str | Path,
) -> CorrectionLineageAudit:
    """Verify source lineage and SOL quote proof without reading outcomes.

    The three roots must already be sealed.  This function is read-only and
    returns a digestable audit; it never creates or modifies a file.
    """

    (
        v1,
        feature_path,
        v1_manifest,
        v1_manifest_hash,
        v1_audit,
        source_refs,
        feature_rows,
        rejected_rows,
    ) = _load_v1(v1_root)
    lifecycle = _load_lifecycle_mirror(lifecycle_root)
    (
        mode_manifest,
        mode_manifest_hash,
        mode_audit,
        mode_modes_sha256,
        mode_audit_sha256,
        mode_rows,
    ) = _load_mode_root(mode_root, lifecycle)
    for index, (source, chunk) in enumerate(zip(source_refs, lifecycle.chunks, strict=True)):
        if (
            source["start_slot_inclusive"] != chunk.start
            or source["end_slot_exclusive"] != chunk.end
            or source["manifest_sha256"] != chunk.manifest_sha256
        ):
            raise CorrectionLineageError(f"v1 source manifest hash drift at chunk {index}")
        if source["manifest_key"] != chunk.manifest_key:
            raise CorrectionLineageError(f"v1 source manifest key drift at chunk {index}")
        if (
            source.get("manifest_byte_count") is not None
            and source["manifest_byte_count"] != chunk.manifest_byte_count
        ):
            raise CorrectionLineageError(f"v1 source manifest byte-count drift at chunk {index}")
    _compare_mode_lifecycle(mode_rows, lifecycle.lifecycle)
    rejected_name = (
        "rejected-amounts.parquet"
        if (v1 / "rejected-amounts.parquet").is_file()
        else "rejected-events.parquet"
    )
    rejected_path = v1 / rejected_name
    quote = _audit_feature_quotes(
        feature_path, rejected_path, lifecycle.lifecycle, feature_rows, rejected_rows
    )
    mode_exclusion = {
        "mode_exclusion_accepted_rows": _integer(
            quote.get("mode_exclusion_accepted_rows"),
            "mode exclusion accepted rows",
        ),
        "mode_exclusion_rejected_rows": _integer(
            quote.get("mode_exclusion_rejected_rows"),
            "mode exclusion rejected rows",
        ),
        "mode_unclassified_accepted_rows": _integer(
            quote.get("mode_unclassified_accepted_rows"),
            "mode unclassified accepted rows",
        ),
        "mode_unclassified_rejected_rows": _integer(
            quote.get("mode_unclassified_rejected_rows"),
            "mode unclassified rejected rows",
        ),
        "mode_not_applicable_accepted_rows": _integer(
            quote.get("mode_not_applicable_accepted_rows"),
            "mode not-applicable accepted rows",
        ),
        "mode_not_applicable_rejected_rows": _integer(
            quote.get("mode_not_applicable_rejected_rows"),
            "mode not-applicable rejected rows",
        ),
    }
    accepted_quote = _mapping(quote.get("accepted"), "accepted quote coverage")
    rejected_quote = _mapping(quote.get("rejected"), "rejected quote coverage")
    v1_audit_sha256 = _sha256_bytes(canonical_json(v1_audit))
    source_chunk_inventory = [
        {
            "index": index,
            "start_slot_inclusive": chunk.start,
            "end_slot_exclusive": chunk.end,
            "manifest_key": chunk.manifest_key,
            "manifest_sha256": chunk.manifest_sha256,
            "manifest_byte_count": chunk.manifest_byte_count,
            "lifecycle_manifest_sha256": chunk.trade_manifest_sha256,
            "lifecycle_manifest_byte_count": chunk.trade_manifest_byte_count,
            "lifecycle_shards": list(chunk.shards),
        }
        for index, chunk in enumerate(lifecycle.chunks)
    ]
    payload: dict[str, object] = {
        "kind": "pumpquant.correction_v2_source_lineage_audit",
        "version": 2,
        "status": "complete",
        "development_only": True,
        "start_slot_inclusive": DEVELOPMENT_START_SLOT,
        "end_slot_exclusive": DEVELOPMENT_END_SLOT,
        "final_holdout_start_slot_inclusive": HOLDOUT_START_SLOT,
        "final_holdout_end_slot_exclusive": HOLDOUT_END_SLOT,
        "holdout_payload_reads_allowed": False,
        "holdout_feature_generation_allowed": False,
        "holdout_accessed": False,
        "outcomes_read": False,
        "reports_read": False,
        "s3_read": False,
        # Explicit top-level bindings are consumed by the downstream
        # attestation validator.  Keep the corresponding nested evidence
        # below for human/audit readability as well.
        "v1_manifest_sha256": v1_manifest_hash,
        "v1_audit_sha256": v1_audit_sha256,
        "mode_index_manifest_sha256": mode_manifest_hash,
        "mode_index_audit_sha256": mode_audit_sha256,
        "mode_index_modes_sha256": mode_modes_sha256,
        "source_rows": feature_rows,
        "rejected_rows": rejected_rows,
        "mode_exclusion_accepted_rows": mode_exclusion["mode_exclusion_accepted_rows"],
        "mode_exclusion_rejected_rows": mode_exclusion["mode_exclusion_rejected_rows"],
        "mode_unclassified_accepted_rows": mode_exclusion["mode_unclassified_accepted_rows"],
        "mode_unclassified_rejected_rows": mode_exclusion["mode_unclassified_rejected_rows"],
        "mode_not_applicable_accepted_rows": mode_exclusion["mode_not_applicable_accepted_rows"],
        "mode_not_applicable_rejected_rows": mode_exclusion["mode_not_applicable_rejected_rows"],
        "v1": {
            "root": str(v1),
            "manifest_sha256": v1_manifest_hash,
            "audit_sha256": v1_audit_sha256,
            "feature_row_count": feature_rows,
            "rejected_row_count": rejected_rows,
            "source_chunk_count": len(source_refs),
        },
        "market_mode": {
            "manifest_sha256": mode_manifest_hash,
            "audit_sha256": mode_audit_sha256,
            "modes_sha256": mode_modes_sha256,
            "row_count": len(mode_rows),
            "pump_create_mint_count": len(lifecycle.lifecycle.pump),
        },
        "lifecycle_mirror": {
            "root": str(lifecycle.root),
            "aggregate_manifest_sha256": lifecycle.manifest_sha256,
            "aggregate_manifest_byte_count": lifecycle.manifest_byte_count,
            "source_chunk_count": len(lifecycle.chunks),
            "lifecycle_shard_count": sum(len(chunk.shards) for chunk in lifecycle.chunks),
            "lifecycle_row_count": sum(
                _integer(
                    chunk.trade_manifest.get(
                        "lifecycle_row_count", chunk.trade_manifest.get("row_count", 0)
                    ),
                    "lifecycle row_count",
                )
                for chunk in lifecycle.chunks
            ),
        },
        "source_bindings": {
            "aggregate_manifest_sha256": lifecycle.manifest_sha256,
            "v1_manifest_sha256": v1_manifest_hash,
            "market_mode_manifest_sha256": mode_manifest_hash,
            "chunks": source_chunk_inventory,
        },
        "lifecycle_identity": {
            "pump_create_event_count": lifecycle.lifecycle.pump_rows,
            "pumpswap_create_pool_event_count": lifecycle.lifecycle.pool_rows,
            "pump_mint_count": len(lifecycle.lifecycle.pump),
            "pumpswap_pool_count": len(lifecycle.lifecycle.pools),
            "duplicate_count": lifecycle.lifecycle.duplicate_count,
            "conflict_count": lifecycle.lifecycle.conflict_count,
            "unmatched_count": 0,
        },
        "quote_proof": quote,
        "coverage": {
            "accepted_rows": _integer(accepted_quote.get("rows"), "accepted rows"),
            "accepted_covered_rows": _integer(
                accepted_quote.get("covered_rows"), "accepted covered rows"
            ),
            "rejected_rows": _integer(rejected_quote.get("rows"), "rejected rows"),
            "rejected_covered_rows": _integer(
                rejected_quote.get("covered_rows"), "rejected covered rows"
            ),
            "duplicate_count": _integer(quote.get("duplicate_count"), "duplicate count"),
            "conflict_count": _integer(quote.get("conflict_count"), "conflict count"),
            "unmatched_count": _integer(quote.get("total_unmatched_rows"), "unmatched count"),
            "mode_exclusion_accepted_rows": mode_exclusion["mode_exclusion_accepted_rows"],
            "mode_exclusion_rejected_rows": mode_exclusion["mode_exclusion_rejected_rows"],
            "mode_unclassified_accepted_rows": mode_exclusion["mode_unclassified_accepted_rows"],
            "mode_unclassified_rejected_rows": mode_exclusion["mode_unclassified_rejected_rows"],
            "mode_not_applicable_accepted_rows": mode_exclusion[
                "mode_not_applicable_accepted_rows"
            ],
            "mode_not_applicable_rejected_rows": mode_exclusion[
                "mode_not_applicable_rejected_rows"
            ],
        },
        "mode_exclusion": mode_exclusion,
        "inventory": sorted(
            list(lifecycle.inventory)
            + [
                {
                    "role": "v1_manifest",
                    "path": str((v1 / "manifest.json").relative_to(v1)),
                    "byte_count": (v1 / "manifest.json").stat().st_size,
                    "sha256": v1_manifest_hash,
                },
                {
                    "role": "mode_manifest",
                    "path": "manifest.json",
                    "byte_count": (Path(mode_root) / "manifest.json").stat().st_size,
                    "sha256": mode_manifest_hash,
                },
            ],
            key=_inventory_sort_key,
        ),
        "publication_contract": "read-only audit; no source root, outcome, report, S3, or holdout payload was opened",
    }
    canonical_bytes = canonical_json(payload)
    return CorrectionLineageAudit(
        payload=payload, canonical_bytes=canonical_bytes, sha256=_sha256_bytes(canonical_bytes)
    )


# Operation-oriented aliases.
verify_lineage = verify_correction_lineage_v2
audit_correction_lineage_v2 = verify_correction_lineage_v2
verify_source_lineage = verify_correction_lineage_v2


__all__ = [
    "CANONICAL_SOL",
    "CANONICAL_SOL_MINT",
    "CorrectionLineageAudit",
    "CorrectionLineageAuditError",
    "CorrectionLineageError",
    "CorrectionLineageV2Error",
    "DEVELOPMENT_END_SLOT",
    "DEVELOPMENT_START_SLOT",
    "EXPECTED_SOURCE_CHUNK_COUNT",
    "MODE_OUTPUT_SCHEMA_SHA256",
    "NATIVE_SOL",
    "NATIVE_SOL_MINT",
    "SOL_ALIASES",
    "SOL_MINT_ALIASES",
    "WRAPPED_SOL",
    "WRAPPED_SOL_MINT",
    "audit_correction_lineage_v2",
    "canonical_json",
    "canonical_quote_mint",
    "normalize_sol_quote",
    "verify_correction_lineage_v2",
    "verify_lineage",
    "verify_source_lineage",
]
