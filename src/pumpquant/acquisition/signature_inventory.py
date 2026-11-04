"""Deterministic transaction-signature inventory for sealed Instructions archives.

The BigQuery ``Instructions`` archive is a discovery source and may contain
many instruction rows for the same transaction.  This module builds a small,
manifest-bound inventory of unique transaction signatures without loading an
entire day (or all days) into memory:

* every source manifest and listed shard is loaded and verified before a data
  row is consumed;
* rows are read through bounded Arrow record batches and filtered with the
  half-open UTC interval ``[start, end)``;
* valid signatures are spilled to deterministic hash buckets, reduced one
  bucket at a time, and externally merged in signature order; and
* the Parquet file and its canonical JSON manifest are published atomically.

``pyarrow`` is optional, just as it is for :mod:`pumpquant.decoding.raw_instructions`.
Importing this module does not require the archive extra; attempting to build
an inventory without it raises an actionable ``RuntimeError``.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import os
import re
import shutil
from collections.abc import Iterator, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO, TextIO, cast

from pumpquant.acquisition.bigquery.archive import (
    ArchiveManifest,
    _fsync_directory,
    load_archive_manifest,
)
from pumpquant.core.hashing import sha256_file
from pumpquant.decoding.raw_instructions import RawInstructionArchiveReader

try:  # pyarrow is an optional archive extra; import it lazily at use time.
    import pyarrow as _pa
    import pyarrow.compute as _pc
    import pyarrow.parquet as _pq
except ImportError:  # pragma: no cover - exercised in minimal installs
    _pa = None
    _pc = None
    _pq = None


_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_BASE58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_CHARS = frozenset(chr(value) for value in _BASE58_ALPHABET)
_DEFAULT_BATCH_SIZE = 4_096
_DEFAULT_BUCKET_COUNT = 256
_DEFAULT_OUTPUT_BATCH_SIZE = 4_096
_DEFAULT_INVENTORY_NAME = "signatures.parquet"
_DEFAULT_MANIFEST_NAME = "manifest.json"
_MISSING = object()
_SCAN_COLUMNS = (
    "block_slot",
    "block_timestamp",
    "tx_signature",
    "program",
    "program_id",
)

# The output schema is explicit.  Arrow types keep the
# inventory stable when the input archive contains only null provenance values.
INVENTORY_COLUMNS: tuple[str, ...] = (
    "signature",
    "first_slot",
    "last_slot",
    "first_block_timestamp",
    "last_block_timestamp",
    "instruction_count",
    "program_ids",
    "source_manifests",
)


class SignatureInventoryError(RuntimeError):
    """Raised when an inventory cannot be safely built or published."""


@dataclass(frozen=True, slots=True)
class SignatureInventorySource:
    """Stable provenance for one verified Instructions manifest."""

    manifest_sha256: str
    manifest_path: str
    table: str
    schema_sha256: str
    row_count: int

    def __post_init__(self) -> None:
        if not _HEX64.fullmatch(self.manifest_sha256):
            raise ValueError("source manifest hash must be lowercase SHA-256 hex")
        if not _HEX64.fullmatch(self.schema_sha256):
            raise ValueError("source schema hash must be lowercase SHA-256 hex")
        if not isinstance(self.manifest_path, str) or not self.manifest_path:
            raise ValueError("source manifest path must be non-empty text")
        if not isinstance(self.table, str) or not self.table:
            raise ValueError("source table must be non-empty text")
        if isinstance(self.row_count, bool) or not isinstance(self.row_count, int):
            raise TypeError("source row_count must be an integer")
        if self.row_count < 0:
            raise ValueError("source row_count cannot be negative")


@dataclass(frozen=True, slots=True)
class SignatureInventoryManifest:
    """Canonical metadata emitted beside a signature inventory.

    ``manifest_sha256`` is calculated over the canonical JSON bytes.  It is
    omitted from those bytes, matching the existing archive
    manifest convention and avoiding a self-referential digest.
    """

    start: datetime
    end: datetime
    sources: tuple[SignatureInventorySource, ...]
    inventory_path: str = _DEFAULT_INVENTORY_NAME
    inventory_row_count: int = 0
    inventory_byte_count: int = 0
    inventory_sha256: str = ""
    inventory_schema_sha256: str = ""
    bucket_count: int = _DEFAULT_BUCKET_COUNT
    batch_size: int = _DEFAULT_BATCH_SIZE
    source_row_count: int = 0
    duplicate_row_count: int = 0
    included_row_count: int = 0
    malformed_signature_count: int = 0
    out_of_bounds_row_count: int = 0
    missing_timestamp_row_count: int = 0
    manifest_path: str = _DEFAULT_MANIFEST_NAME
    manifest_sha256: str = ""

    def __post_init__(self) -> None:
        start = _utc_timestamp(self.start, "start")
        end = _utc_timestamp(self.end, "end")
        if end <= start:
            raise ValueError("end must be later than start")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        if not self.sources:
            raise ValueError("at least one source manifest is required")
        if len({source.manifest_sha256 for source in self.sources}) != len(self.sources):
            raise ValueError("source manifests must be unique")
        for label in ("inventory_path", "manifest_path"):
            value = getattr(self, label)
            if (
                not isinstance(value, str)
                or not value
                or Path(value).is_absolute()
                or Path(value).name != value
                or value in {".", ".."}
            ):
                raise ValueError(f"{label} must be a safe relative basename")
        for label in (
            "inventory_row_count",
            "inventory_byte_count",
            "bucket_count",
            "batch_size",
            "source_row_count",
            "duplicate_row_count",
            "included_row_count",
            "malformed_signature_count",
            "out_of_bounds_row_count",
            "missing_timestamp_row_count",
        ):
            value = getattr(self, label)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{label} must be a non-negative integer")
        if self.bucket_count <= 0 or self.batch_size <= 0:
            raise ValueError("bucket_count and batch_size must be positive")
        for label in ("inventory_sha256", "inventory_schema_sha256", "manifest_sha256"):
            value = getattr(self, label)
            if value and not _HEX64.fullmatch(value):
                raise ValueError(f"{label} must be lowercase SHA-256 hex")
        if self.inventory_row_count > self.source_row_count:
            raise ValueError("inventory row count cannot exceed source row count")
        if self.included_row_count != self.source_row_count - self.malformed_signature_count - (
            self.out_of_bounds_row_count + self.missing_timestamp_row_count
        ):
            raise ValueError(
                "included_row_count must equal source rows minus malformed/out-of-bounds rows"
            )
        if self.duplicate_row_count != max(0, self.included_row_count - self.inventory_row_count):
            raise ValueError(
                "duplicate_row_count must equal valid source rows minus unique signatures"
            )

    @property
    def row_count(self) -> int:
        return self.inventory_row_count

    @property
    def byte_count(self) -> int:
        return self.inventory_byte_count

    @property
    def sources_sha256(self) -> tuple[str, ...]:
        return tuple(source.manifest_sha256 for source in self.sources)


@dataclass(frozen=True, slots=True)
class _VerifiedSource:
    root: Path
    manifest: ArchiveManifest
    source: SignatureInventorySource
    reader: RawInstructionArchiveReader


@dataclass(slots=True)
class _SignatureAggregate:
    signature: str
    first_slot: int | None = None
    last_slot: int | None = None
    first_block_timestamp: datetime | None = None
    last_block_timestamp: datetime | None = None
    instruction_count: int = 0
    program_ids: set[str] | None = None
    source_manifests: set[str] | None = None

    def __post_init__(self) -> None:
        self.program_ids = set()
        self.source_manifests = set()

    def add(
        self,
        *,
        slot: int | None,
        timestamp: datetime | None,
        program_id: str | None,
        source_manifest: str,
    ) -> None:
        self.instruction_count += 1
        if slot is not None:
            self.first_slot = slot if self.first_slot is None else min(self.first_slot, slot)
            self.last_slot = slot if self.last_slot is None else max(self.last_slot, slot)
        if timestamp is not None:
            normalized = _utc_timestamp(timestamp, "block_timestamp")
            self.first_block_timestamp = (
                normalized
                if self.first_block_timestamp is None
                else min(self.first_block_timestamp, normalized)
            )
            self.last_block_timestamp = (
                normalized
                if self.last_block_timestamp is None
                else max(self.last_block_timestamp, normalized)
            )
        if program_id:
            assert self.program_ids is not None
            self.program_ids.add(program_id)
        assert self.source_manifests is not None
        self.source_manifests.add(source_manifest)

    def payload(self) -> dict[str, object]:
        assert self.program_ids is not None
        assert self.source_manifests is not None
        return {
            "signature": self.signature,
            "first_slot": self.first_slot,
            "last_slot": self.last_slot,
            "first_block_timestamp": _iso(self.first_block_timestamp),
            "last_block_timestamp": _iso(self.last_block_timestamp),
            "instruction_count": self.instruction_count,
            "program_ids": sorted(self.program_ids),
            "source_manifests": sorted(self.source_manifests),
        }


def _require_pyarrow() -> tuple[Any, Any, Any]:
    if _pa is None or _pc is None or _pq is None:  # pragma: no cover - optional extra
        raise RuntimeError("signature inventory requires: pip install 'pumpquant[archive]'")
    return _pa, _pc, _pq


def _utc_timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{label} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must include a timezone")
    return value.astimezone(UTC)


def _bound(value: str | datetime, label: str) -> datetime:
    if isinstance(value, datetime):
        return _utc_timestamp(value, label)
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{label} must be an ISO timestamp or datetime")
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise ValueError(f"{label} is not an ISO timestamp") from error
    return _utc_timestamp(parsed, label)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _utc_timestamp(value, "timestamp").isoformat().replace("+00:00", "Z")


def _manifest_path(value: object) -> Path:
    path = Path(value) if isinstance(value, (str, Path)) else None
    if path is None:
        raise TypeError("manifest inputs must be paths to a manifest or archive directory")
    if path.is_dir():
        path = path / _DEFAULT_MANIFEST_NAME
    if path.is_symlink() or not path.is_file():
        raise FileNotFoundError(path)
    return path


def _resolve_sources(
    manifests: Sequence[str | Path] | str | Path,
) -> tuple[_VerifiedSource, ...]:
    if isinstance(manifests, (str, Path)):
        manifests = (manifests,)
    elif isinstance(manifests, (bytes, bytearray)):
        raise TypeError("manifests must be a non-empty sequence of paths")
    if not manifests:
        raise ValueError("at least one Instructions manifest is required")
    resolved: list[_VerifiedSource] = []
    seen_hashes: set[str] = set()
    for item in manifests:
        manifest_file = _manifest_path(item)
        root = manifest_file.parent
        manifest = load_archive_manifest(manifest_file)
        if manifest.table != "Instructions":
            raise ValueError("signature inventory requires Instructions manifests")
        if manifest.manifest_sha256 in seen_hashes:
            raise ValueError("duplicate source manifest hash")
        seen_hashes.add(manifest.manifest_sha256)
        reader = RawInstructionArchiveReader(root, manifest, batch_size=_DEFAULT_BATCH_SIZE)
        # This hashes every listed shard and validates every Arrow schema and
        # row count without yielding a record.  It is completed
        # for every source before any source is scanned.
        reader._preflight()
        resolved.append(
            _VerifiedSource(
                root=root,
                manifest=manifest,
                reader=reader,
                source=SignatureInventorySource(
                    manifest_sha256=manifest.manifest_sha256,
                    manifest_path=manifest.manifest_path,
                    table=manifest.table,
                    schema_sha256=manifest.schema_sha256,
                    row_count=manifest.row_count,
                ),
            )
        )
    # Input order must not alter either inventory bytes or its provenance
    # manifest.  Shards remain in their sealed manifest order while source
    # manifests use their stable digest as the canonical order key.
    resolved.sort(key=lambda source: source.source.manifest_sha256)
    return tuple(resolved)


def _infer_bounds(
    sources: Sequence[_VerifiedSource],
    start: str | datetime | None,
    end: str | datetime | None,
) -> tuple[datetime, datetime]:
    starts: list[datetime] = []
    ends: list[datetime] = []
    for source in sources:
        provenance = source.manifest.provenance
        if provenance is not None:
            starts.append(_bound(provenance.start, "provenance.start"))
            ends.append(_bound(provenance.end, "provenance.end"))
    if start is None:
        if not starts:
            raise ValueError("start is required when a source has no query provenance")
        start_dt = min(starts)
    else:
        start_dt = _bound(start, "start")
    if end is None:
        if not ends:
            raise ValueError("end is required when a source has no query provenance")
        end_dt = max(ends)
    else:
        end_dt = _bound(end, "end")
    if end_dt <= start_dt:
        raise ValueError("end must be later than start")
    return start_dt, end_dt


def _base58_decode(value: str) -> bytes | None:
    """Decode a Solana base58 signature without third-party dependencies."""

    if not value or any(char not in _BASE58_CHARS for char in value):
        return None
    number = 0
    for char in value.encode("ascii"):
        number = number * 58 + _BASE58_ALPHABET.index(char)
    raw = number.to_bytes((number.bit_length() + 7) // 8, "big") if number else b""
    leading = len(value) - len(value.lstrip("1"))
    decoded = b"\x00" * leading + raw
    if len(decoded) != 64:
        return None
    # Reject non-canonical encodings (e.g. unnecessary leading ``1`` values).
    if _base58_encode(decoded) != value:
        return None
    return decoded


def _base58_encode(value: bytes) -> str:
    if not value:
        return ""
    number = int.from_bytes(value, "big")
    chars: list[str] = []
    while number:
        number, remainder = divmod(number, 58)
        chars.append(chr(_BASE58_ALPHABET[remainder]))
    leading = len(value) - len(value.lstrip(b"\x00"))
    return "1" * leading + "".join(reversed(chars))


def is_valid_signature(value: object) -> bool:
    """Return whether ``value`` is a canonical 64-byte Solana base58 signature."""

    return isinstance(value, str) and _base58_decode(value) is not None


def _signature(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text != value or not is_valid_signature(text):
        return None
    return text


def _value(row: Mapping[str, object], name: str) -> object:
    return row.get(name, _MISSING)


def _slot(value: object) -> int | None:
    if value is _MISSING or value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _timestamp(value: object) -> datetime | None:
    if value is _MISSING or value is None:
        return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return None
    return value.astimezone(UTC)


def _program_id(row: Mapping[str, object]) -> str | None:
    for name in ("program_id", "program"):
        value = _value(row, name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _bucket_for(signature: str, bucket_count: int) -> int:
    digest = hashlib.sha256(signature.encode("ascii")).digest()
    return int.from_bytes(digest[:8], "big") % bucket_count


def _occurrence_bytes(
    signature: str,
    slot: int | None,
    timestamp: datetime | None,
    program_id: str | None,
    source_manifest: str,
) -> bytes:
    payload = {
        "signature": signature,
        "slot": slot,
        "timestamp": _iso(timestamp),
        "program_id": program_id,
        "source_manifest": source_manifest,
    }
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _inventory_schema(pa: Any) -> Any:
    return pa.schema(
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


def inventory_schema_sha256() -> str:
    """Return the stable hash of the emitted inventory schema description."""

    payload = {
        "fields": [
            {"name": "signature", "type": "string", "nullable": False},
            {"name": "first_slot", "type": "int64", "nullable": True},
            {"name": "last_slot", "type": "int64", "nullable": True},
            {
                "name": "first_block_timestamp",
                "type": "timestamp[us, tz=UTC]",
                "nullable": True,
            },
            {
                "name": "last_block_timestamp",
                "type": "timestamp[us, tz=UTC]",
                "nullable": True,
            },
            {"name": "instruction_count", "type": "int64", "nullable": False},
            {"name": "program_ids", "type": "list<string>", "nullable": False},
            {"name": "source_manifests", "type": "list<string>", "nullable": False},
        ]
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _aggregate_bucket(path: Path) -> list[dict[str, object]]:
    aggregates: dict[str, _SignatureAggregate] = {}
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as error:  # pragma: no cover - internal corruption
                raise SignatureInventoryError(
                    f"bucket {path.name} has invalid JSON on line {line_number}"
                ) from error
            if not isinstance(raw, Mapping):  # pragma: no cover - internal corruption
                raise SignatureInventoryError(f"bucket {path.name} row is not an object")
            sig = raw.get("signature")
            source = raw.get("source_manifest")
            if not isinstance(sig, str) or not is_valid_signature(sig):
                raise SignatureInventoryError(f"bucket {path.name} contains malformed signature")
            if not isinstance(source, str) or not _HEX64.fullmatch(source):
                raise SignatureInventoryError(f"bucket {path.name} contains malformed source hash")
            timestamp = raw.get("timestamp")
            parsed_timestamp = (
                _bound(timestamp, "bucket timestamp") if isinstance(timestamp, str) else None
            )
            slot = _slot(raw.get("slot"))
            program = raw.get("program_id")
            program_text = program if isinstance(program, str) else None
            aggregate = aggregates.get(sig)
            if aggregate is None:
                aggregate = _SignatureAggregate(sig)
                aggregates[sig] = aggregate
            aggregate.add(
                slot=slot,
                timestamp=parsed_timestamp,
                program_id=program_text,
                source_manifest=source,
            )
    return [aggregates[sig].payload() for sig in sorted(aggregates)]


def _write_run(path: Path, rows: Sequence[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")))
            handle.write("\n")


def _merge_runs(
    run_paths: Sequence[Path],
    target: Path,
    *,
    pa: Any,
    output_batch_size: int,
) -> tuple[int, int, str]:
    """Merge sorted bucket runs into one deterministic Parquet file."""

    schema = _inventory_schema(pa)
    target_partial = Path(f"{target}.partial")
    if target_partial.exists():
        target_partial.unlink()
    handles: list[TextIO] = []
    iterators: list[Iterator[dict[str, object]]] = []
    heap: list[tuple[str, int, dict[str, object]]] = []

    def rows_from(handle: TextIO) -> Iterator[dict[str, object]]:
        for line in handle:
            value = json.loads(line)
            if not isinstance(value, Mapping):  # pragma: no cover - internal corruption
                raise SignatureInventoryError("inventory run row is not an object")
            yield cast(dict[str, object], dict(value))

    def arrow_row(value: Mapping[str, object]) -> dict[str, object]:
        """Decode the canonical JSON run representation for Arrow output."""

        row = dict(value)
        for name in ("first_block_timestamp", "last_block_timestamp"):
            timestamp = row.get(name)
            row[name] = _bound(timestamp, name) if isinstance(timestamp, str) else None
        return row

    try:
        for index, run_path in enumerate(run_paths):
            run_handle = run_path.open("r", encoding="utf-8")
            handles.append(run_handle)
            iterator = rows_from(run_handle)
            iterators.append(iterator)
            try:
                first = next(iterator)
            except StopIteration:
                continue
            signature = first.get("signature")
            if not isinstance(signature, str):  # pragma: no cover - internal corruption
                raise SignatureInventoryError("inventory run has no signature")
            heapq.heappush(heap, (signature, index, first))

        writer = _pq.ParquetWriter(target_partial, schema, compression="zstd")
        row_count = 0
        try:
            buffer: list[dict[str, object]] = []
            while heap:
                _, index, row = heapq.heappop(heap)
                buffer.append(arrow_row(row))
                row_count += 1
                if len(buffer) >= output_batch_size:
                    writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
                    buffer = []
                try:
                    following = next(iterators[index])
                except StopIteration:
                    continue
                signature = following.get("signature")
                if not isinstance(signature, str):  # pragma: no cover - internal corruption
                    raise SignatureInventoryError("inventory run has no signature")
                heapq.heappush(heap, (signature, index, following))
            if buffer:
                writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
            elif row_count == 0:
                # Ensure an empty inventory still has a valid, schema.
                writer.write_table(pa.Table.from_pylist([], schema=schema))
        finally:
            writer.close()
        with target_partial.open("rb") as binary_handle:
            os.fsync(binary_handle.fileno())
        os.replace(target_partial, target)
        _fsync_directory(target.parent)
    finally:
        for closed_handle in handles:
            closed_handle.close()
        if target_partial.exists():
            target_partial.unlink()
    digest = hashlib.sha256()
    byte_count = 0
    with target.open("rb") as binary_handle:
        while chunk := binary_handle.read(1024 * 1024):
            digest.update(chunk)
            byte_count += len(chunk)
    return row_count, byte_count, digest.hexdigest()


def _manifest_payload(manifest: SignatureInventoryManifest) -> dict[str, object]:
    return {
        "version": 1,
        "kind": "pumpquant.signature_inventory",
        "manifest_path": manifest.manifest_path,
        "inventory_path": manifest.inventory_path,
        "inventory_schema_sha256": manifest.inventory_schema_sha256,
        "inventory_row_count": manifest.inventory_row_count,
        "inventory_byte_count": manifest.inventory_byte_count,
        "inventory_sha256": manifest.inventory_sha256,
        "start": _iso(manifest.start),
        "end": _iso(manifest.end),
        "bucket_count": manifest.bucket_count,
        "batch_size": manifest.batch_size,
        "source_row_count": manifest.source_row_count,
        "duplicate_row_count": manifest.duplicate_row_count,
        "included_row_count": manifest.included_row_count,
        "malformed_signature_count": manifest.malformed_signature_count,
        "out_of_bounds_row_count": manifest.out_of_bounds_row_count,
        "missing_timestamp_row_count": manifest.missing_timestamp_row_count,
        "source_manifests": [
            {
                "manifest_sha256": source.manifest_sha256,
                "manifest_path": source.manifest_path,
                "table": source.table,
                "schema_sha256": source.schema_sha256,
                "row_count": source.row_count,
            }
            for source in manifest.sources
        ],
    }


def manifest_bytes(manifest: SignatureInventoryManifest) -> bytes:
    """Serialise an inventory manifest in canonical UTF-8 JSON form."""

    return (
        json.dumps(_manifest_payload(manifest), sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _manifest_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"inventory manifest {label} must be a non-negative integer")
    return value


def _manifest_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"inventory manifest {label} must be non-empty text")
    return value


def load_signature_inventory_manifest(path: str | Path) -> SignatureInventoryManifest:
    """Load a canonical inventory manifest and bind it to its file hash."""

    manifest_file = Path(path)
    if manifest_file.is_symlink() or not manifest_file.is_file():
        raise FileNotFoundError(manifest_file)
    raw = manifest_file.read_bytes()
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("signature inventory manifest is not valid UTF-8 JSON") from error
    if not isinstance(payload, Mapping):
        raise TypeError("signature inventory manifest must be an object")
    if payload.get("version") != 1 or payload.get("kind") != "pumpquant.signature_inventory":
        raise ValueError("unsupported signature inventory manifest version/kind")
    source_payloads = payload.get("source_manifests")
    if not isinstance(source_payloads, Sequence) or isinstance(
        source_payloads, (str, bytes, bytearray)
    ):
        raise TypeError("signature inventory source_manifests must be a sequence")
    sources: list[SignatureInventorySource] = []
    for index, source in enumerate(source_payloads):
        if not isinstance(source, Mapping):
            raise TypeError(f"source manifest {index} must be an object")
        sources.append(
            SignatureInventorySource(
                manifest_sha256=_manifest_text(
                    source.get("manifest_sha256"), "source manifest_sha256"
                ),
                manifest_path=_manifest_text(source.get("manifest_path"), "source manifest_path"),
                table=_manifest_text(source.get("table"), "source table"),
                schema_sha256=_manifest_text(source.get("schema_sha256"), "source schema_sha256"),
                row_count=_manifest_int(source.get("row_count"), "source row_count"),
            )
        )
    manifest_path = _manifest_text(payload.get("manifest_path"), "manifest_path")
    if manifest_file.name != manifest_path:
        raise ValueError("manifest_path does not match loaded file")
    start = _bound(_manifest_text(payload.get("start"), "start"), "start")
    end = _bound(_manifest_text(payload.get("end"), "end"), "end")
    manifest = SignatureInventoryManifest(
        start=start,
        end=end,
        sources=tuple(sources),
        inventory_path=_manifest_text(payload.get("inventory_path"), "inventory_path"),
        inventory_row_count=_manifest_int(
            payload.get("inventory_row_count"), "inventory_row_count"
        ),
        inventory_byte_count=_manifest_int(
            payload.get("inventory_byte_count"), "inventory_byte_count"
        ),
        inventory_sha256=_manifest_text(payload.get("inventory_sha256"), "inventory_sha256"),
        inventory_schema_sha256=_manifest_text(
            payload.get("inventory_schema_sha256"), "inventory_schema_sha256"
        ),
        bucket_count=_manifest_int(payload.get("bucket_count"), "bucket_count"),
        batch_size=_manifest_int(payload.get("batch_size"), "batch_size"),
        source_row_count=_manifest_int(payload.get("source_row_count"), "source_row_count"),
        duplicate_row_count=_manifest_int(
            payload.get("duplicate_row_count"), "duplicate_row_count"
        ),
        included_row_count=_manifest_int(payload.get("included_row_count"), "included_row_count"),
        malformed_signature_count=_manifest_int(
            payload.get("malformed_signature_count"), "malformed_signature_count"
        ),
        out_of_bounds_row_count=_manifest_int(
            payload.get("out_of_bounds_row_count"), "out_of_bounds_row_count"
        ),
        missing_timestamp_row_count=_manifest_int(
            payload.get("missing_timestamp_row_count"), "missing_timestamp_row_count"
        ),
        manifest_path=manifest_path,
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
    )
    if raw != manifest_bytes(manifest):
        raise ValueError("signature inventory manifest is not in canonical form")
    return manifest


def verify_signature_inventory(
    manifest: SignatureInventoryManifest | str | Path,
    output_root: str | Path,
) -> None:
    """Verify the canonical JSON manifest and inventory byte checksum."""

    root = Path(output_root)
    loaded = (
        load_signature_inventory_manifest(root / manifest.manifest_path)
        if isinstance(manifest, SignatureInventoryManifest)
        else load_signature_inventory_manifest(manifest)
    )
    if isinstance(manifest, SignatureInventoryManifest) and loaded != manifest:
        raise ValueError("supplied inventory manifest differs from the on-disk manifest")
    manifest_file = root / loaded.manifest_path
    raw = manifest_file.read_bytes()
    if hashlib.sha256(raw).hexdigest() != loaded.manifest_sha256:
        raise SignatureInventoryError("inventory manifest checksum mismatch")
    inventory = root / loaded.inventory_path
    if inventory.is_symlink() or not inventory.is_file():
        raise FileNotFoundError(inventory)
    digest = hashlib.sha256()
    size = 0
    with inventory.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    if size != loaded.inventory_byte_count or digest.hexdigest() != loaded.inventory_sha256:
        raise SignatureInventoryError("signature inventory failed integrity verification")
    if _pa is not None and _pq is not None:
        parquet = _pq.ParquetFile(inventory)
        try:
            if parquet.metadata is None or parquet.metadata.num_rows != loaded.inventory_row_count:
                raise SignatureInventoryError("signature inventory row count mismatch")
            if tuple(parquet.schema_arrow.names) != INVENTORY_COLUMNS:
                raise SignatureInventoryError("signature inventory schema mismatch")
        finally:
            close = getattr(parquet, "close", None)
            if callable(close):
                close()


def _safe_basename(value: str | Path, label: str) -> str:
    text = str(value)
    if (
        not text
        or Path(text).is_absolute()
        or Path(text).name != text
        or text in {".", ".."}
        or "/" in text
        or "\\" in text
    ):
        raise ValueError(f"{label} must be a safe relative basename")
    return text


def _publish(
    staging: Path,
    root: Path,
    *,
    inventory_name: str,
    manifest_name: str,
) -> None:
    root_is_new = not root.exists()
    if root.exists() and (root.is_symlink() or not root.is_dir()):
        raise FileExistsError(f"output directory is not a real directory: {root}")
    if root_is_new:
        os.replace(staging, root)
        _fsync_directory(root.parent)
        return

    # A published inventory is immutable.  An exact deterministic rerun is a
    # no-op; anything else receives a conflict instead of creating a window in
    # which an old manifest could point at newly replaced Parquet bytes.
    expected = {inventory_name, manifest_name}
    actual = {item.name for item in root.iterdir()}
    if actual != expected:
        raise FileExistsError("existing inventory directory has conflicting contents")
    for name in sorted(expected):
        old = root / name
        new = staging / name
        if not old.is_file() or old.is_symlink() or old.stat().st_size != new.stat().st_size:
            raise FileExistsError("existing inventory differs from deterministic rerun")
        if sha256_file(old) != sha256_file(new):
            raise FileExistsError("existing inventory differs from deterministic rerun")
    shutil.rmtree(staging)


def build_signature_inventory(
    manifests: Sequence[str | Path] | str | Path,
    output_dir: str | Path,
    *,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    bucket_count: int = _DEFAULT_BUCKET_COUNT,
    output_batch_size: int = _DEFAULT_OUTPUT_BATCH_SIZE,
    inventory_name: str = _DEFAULT_INVENTORY_NAME,
    manifest_name: str = _DEFAULT_MANIFEST_NAME,
    strict_signatures: bool = False,
) -> SignatureInventoryManifest:
    """Build and atomically publish a deterministic signature inventory.

    ``manifests`` accepts manifest files or archive directories (a directory
    is resolved to ``manifest.json``).  Bounds are UTC-aware and half-open. If
    omitted, they are inferred from the minimum/maximum query-provenance
    bounds across the inputs.  Invalid signatures are skipped and counted by
    default; ``strict_signatures=True`` raises on the first malformed value.
    """

    pa, pc, pq = _require_pyarrow()
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if isinstance(bucket_count, bool) or not isinstance(bucket_count, int) or bucket_count <= 0:
        raise ValueError("bucket_count must be a positive integer")
    if (
        isinstance(output_batch_size, bool)
        or not isinstance(output_batch_size, int)
        or output_batch_size <= 0
    ):
        raise ValueError("output_batch_size must be a positive integer")
    inventory_name = _safe_basename(inventory_name, "inventory_name")
    manifest_name = _safe_basename(manifest_name, "manifest_name")
    if inventory_name == manifest_name:
        raise ValueError("inventory_name and manifest_name must differ")

    sources = _resolve_sources(manifests)
    start_dt, end_dt = _infer_bounds(sources, start, end)
    root = Path(output_dir)
    if root.is_symlink() or (root.exists() and not root.is_dir()):
        raise FileExistsError(f"output directory is not a real directory: {root}")
    resolved_output = root.resolve()
    for source in sources:
        resolved_source = source.root.resolve()
        if (
            resolved_output == resolved_source
            or resolved_output.is_relative_to(resolved_source)
            or resolved_source.is_relative_to(resolved_output)
        ):
            raise ValueError("output directory must not overlap a source archive")
    root.parent.mkdir(parents=True, exist_ok=True)
    staging = root.parent / f".{root.name}.partial"
    if staging.exists() or staging.is_symlink():
        if staging.is_symlink() or not staging.is_dir():
            raise FileExistsError(f"stale staging path is not a directory: {staging}")
        shutil.rmtree(staging)
    staging.mkdir(mode=0o700)
    buckets = staging / "buckets"
    buckets.mkdir(mode=0o700)
    handles: dict[int, BinaryIO] = {}
    source_row_count = 0
    malformed_signature_count = 0
    out_of_bounds_row_count = 0
    missing_timestamp_row_count = 0

    def bucket_handle(index: int) -> BinaryIO:
        handle = handles.get(index)
        if handle is None:
            path = buckets / f"bucket-{index:08d}.jsonl"
            handle = path.open("ab")
            handles[index] = handle
        return handle

    try:
        # All source verification has completed in _resolve_sources before this
        # loop starts.  Reading can therefore never expose a partially sealed
        # source archive to the output.
        for source in sources:
            path = source.root
            for shard in source.manifest.shards:
                parquet = pq.ParquetFile(path / shard.path)
                try:
                    for batch in parquet.iter_batches(
                        batch_size=batch_size,
                        columns=list(_SCAN_COLUMNS),
                        use_threads=False,
                    ):
                        source_row_count += batch.num_rows
                        timestamp_index = batch.schema.get_field_index("block_timestamp")
                        timestamp_values = batch.column(timestamp_index)
                        missing_timestamp_row_count += timestamp_values.null_count
                        lower = pc.greater_equal(
                            timestamp_values,
                            pa.scalar(start_dt, type=timestamp_values.type),
                        )
                        upper = pc.less(
                            timestamp_values,
                            pa.scalar(end_dt, type=timestamp_values.type),
                        )
                        in_bounds = pc.and_(lower, upper)
                        included_timestamp_count = int(
                            pc.sum(pc.fill_null(in_bounds, False)).as_py() or 0
                        )
                        out_of_bounds_row_count += (
                            batch.num_rows - timestamp_values.null_count - included_timestamp_count
                        )
                        rows = batch.filter(in_bounds).to_pylist()
                        for raw in rows:
                            if not isinstance(raw, Mapping):
                                raise SignatureInventoryError("Parquet row is not an object")
                            sig = _signature(
                                _value(cast(Mapping[str, object], raw), "tx_signature")
                            )
                            if sig is None:
                                malformed_signature_count += 1
                                if strict_signatures:
                                    raise ValueError("encountered malformed transaction signature")
                                continue
                            timestamp = _timestamp(_value(raw, "block_timestamp"))
                            if timestamp is None:  # pragma: no cover - Arrow mask guarantees this
                                raise SignatureInventoryError(
                                    "filtered Parquet row has no UTC timestamp"
                                )
                            slot = _slot(_value(raw, "block_slot"))
                            line = _occurrence_bytes(
                                sig,
                                slot,
                                timestamp,
                                _program_id(raw),
                                source.source.manifest_sha256,
                            )
                            bucket_handle(_bucket_for(sig, bucket_count)).write(line)
                finally:
                    close = getattr(parquet, "close", None)
                    if callable(close):
                        close()
        for handle in handles.values():
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
        handles.clear()

        run_paths: list[Path] = []
        for bucket_index in range(bucket_count):
            bucket_path = buckets / f"bucket-{bucket_index:08d}.jsonl"
            if not bucket_path.exists():
                continue
            rows = _aggregate_bucket(bucket_path)
            if not rows:
                continue
            run_path = staging / f"run-{bucket_index:08d}.jsonl"
            _write_run(run_path, rows)
            run_paths.append(run_path)
        run_paths.sort()

        inventory_target = staging / inventory_name
        unique_count, byte_count, inventory_hash = _merge_runs(
            run_paths,
            inventory_target,
            pa=pa,
            output_batch_size=output_batch_size,
        )
        included_row_count = (
            source_row_count
            - malformed_signature_count
            - out_of_bounds_row_count
            - missing_timestamp_row_count
        )
        duplicate_count = included_row_count - unique_count
        if duplicate_count < 0:  # pragma: no cover - defensive accounting guard
            raise SignatureInventoryError(
                "inventory accounting produced a negative duplicate count"
            )
        manifest = SignatureInventoryManifest(
            start=start_dt,
            end=end_dt,
            sources=tuple(source.source for source in sources),
            inventory_path=inventory_name,
            inventory_row_count=unique_count,
            inventory_byte_count=byte_count,
            inventory_sha256=inventory_hash,
            inventory_schema_sha256=inventory_schema_sha256(),
            bucket_count=bucket_count,
            batch_size=batch_size,
            source_row_count=source_row_count,
            duplicate_row_count=duplicate_count,
            included_row_count=included_row_count,
            malformed_signature_count=malformed_signature_count,
            out_of_bounds_row_count=out_of_bounds_row_count,
            missing_timestamp_row_count=missing_timestamp_row_count,
            manifest_path=manifest_name,
        )
        manifest_target = staging / manifest_name
        manifest_partial = Path(f"{manifest_target}.partial")
        payload = manifest_bytes(manifest)
        with manifest_partial.open("xb") as manifest_handle:
            manifest_handle.write(payload)
            manifest_handle.flush()
            os.fsync(manifest_handle.fileno())
        os.replace(manifest_partial, manifest_target)
        _fsync_directory(staging)
        manifest_hash = hashlib.sha256(payload).hexdigest()
        completed = SignatureInventoryManifest(
            start=manifest.start,
            end=manifest.end,
            sources=manifest.sources,
            inventory_path=manifest.inventory_path,
            inventory_row_count=manifest.inventory_row_count,
            inventory_byte_count=manifest.inventory_byte_count,
            inventory_sha256=manifest.inventory_sha256,
            inventory_schema_sha256=manifest.inventory_schema_sha256,
            bucket_count=manifest.bucket_count,
            batch_size=manifest.batch_size,
            source_row_count=manifest.source_row_count,
            duplicate_row_count=manifest.duplicate_row_count,
            included_row_count=manifest.included_row_count,
            malformed_signature_count=manifest.malformed_signature_count,
            out_of_bounds_row_count=manifest.out_of_bounds_row_count,
            missing_timestamp_row_count=manifest.missing_timestamp_row_count,
            manifest_path=manifest.manifest_path,
            manifest_sha256=manifest_hash,
        )
        # Bucket/run files are private scratch state and must never become
        # part of the published inventory directory.
        shutil.rmtree(buckets)
        for run_path in run_paths:
            if run_path.exists():
                run_path.unlink()
        # Rewrite the staging JSON only if the caller wants the in-memory
        # object hash exposed.  The canonical payload itself is unchanged.
        _publish(staging, root, inventory_name=inventory_name, manifest_name=manifest_name)
        return completed
    except BaseException:
        for handle in handles.values():
            with suppress(Exception):
                handle.close()
        if staging.exists() and not staging.is_symlink():
            shutil.rmtree(staging)
        raise


__all__ = [
    "INVENTORY_COLUMNS",
    "SignatureInventoryError",
    "SignatureInventoryManifest",
    "SignatureInventorySource",
    "build_signature_inventory",
    "inventory_schema_sha256",
    "is_valid_signature",
    "load_signature_inventory_manifest",
    "manifest_bytes",
    "verify_signature_inventory",
]
