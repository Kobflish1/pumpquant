"""Manifest-bound, streaming access to archived BigQuery ``Instructions`` rows.

The archive writer in :mod:`pumpquant.bigquery_archive` intentionally keeps the
BigQuery result values in Parquet.  This module is the small read side of that
contract: it verifies the manifest and every shard before returning the first
row, then decodes one bounded Parquet record batch at a time.  The returned
records are discovery records, not :class:`pumpquant.schema.SwapEvent`
instances.  In particular, strings in ``parsed``/``data`` and the nested
``params`` values are retained as supplied by BigQuery.

The source is a historical BigQuery seed.  It is useful for discovery and for
joining later lossless captures, but it is not a true L1 transaction archive
and it does not contain an observed order book (there is no true L2 for
Pump/PumpSwap).  Those limitations are deliberately represented on both the
reader and each row through capability labels.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from pumpquant.acquisition.bigquery.archive import (
    ArchiveManifest,
    load_archive_manifest,
    verify_archive_manifest,
)

try:  # pyarrow is an optional archive extra; import it lazily at use time.
    import pyarrow as _pa
    import pyarrow.parquet as _pq
except ImportError:  # pragma: no cover - exercised in minimal installs
    _pa = None
    _pq = None


_HEX64 = re.compile(r"^[0-9a-f]{64}$")

# These are the aliases emitted by ``build_pump_instruction_plan``.  They are
# intentionally kept as a tuple so field/order checks are deterministic.
DISCOVERY_COLUMNS: tuple[str, ...] = (
    "block_slot",
    "block_hash",
    "block_timestamp",
    "tx_signature",
    "source_index",
    "source_parent_index",
    "accounts",
    "data",
    "parsed",
    "program",
    "program_id",
    "instruction_type",
    "params",
    "chain_identity_complete",
)


def _require_pyarrow() -> tuple[Any, Any]:
    if _pa is None or _pq is None:  # pragma: no cover - depends on optional extra
        raise RuntimeError(
            "streaming raw BigQuery instructions requires: pip install 'pumpquant[archive]'"
        )
    return _pa, _pq


def _nonempty_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be non-empty text")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TypeError(f"{label} must be a non-negative integer")
    return value


def _utc_timestamp(value: object, label: str) -> datetime:
    """Return an aware UTC timestamp without going through a float."""

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


@dataclass(frozen=True, slots=True)
class RawInstructionSourceLocator:
    """Stable identity of one row in one verified archive.

    ``shard_index`` is the zero-based position in the manifest (rather than a
    number inferred from a file name), and ``row_offset`` is the zero-based
    position in that Parquet shard.  Including the manifest digest makes the
    locator unambiguous if a path is later reused for a different archive.
    """

    manifest_sha256: str
    shard_path: str
    shard_index: int
    row_offset: int

    def __post_init__(self) -> None:
        if not _HEX64.fullmatch(self.manifest_sha256):
            raise ValueError("source locator manifest_sha256 must be lowercase SHA-256 hex")
        if (
            not self.shard_path
            or Path(self.shard_path).is_absolute()
            or ".." in Path(self.shard_path).parts
            or Path(self.shard_path).name == ""
        ):
            raise ValueError("source locator shard_path must be a safe relative path")
        _nonnegative_int(self.shard_index, "source locator shard_index")
        _nonnegative_int(self.row_offset, "source locator row_offset")

    @property
    def manifest_hash(self) -> str:
        return self.manifest_sha256

    @property
    def shard(self) -> str:
        return self.shard_path

    @property
    def row_index(self) -> int:
        return self.row_offset

    @property
    def key(self) -> str:
        """A deterministic, human-readable locator string."""

        return (
            f"manifest_sha256={self.manifest_sha256};"
            f"shard_index={self.shard_index};shard_path={self.shard_path};"
            f"row_offset={self.row_offset}"
        )

    @property
    def uri(self) -> str:
        return self.key

    def __str__(self) -> str:
        return self.key


@dataclass(frozen=True, slots=True)
class RawInstructionCapabilities:
    """Capability/provenance labels attached to a discovery row."""

    provenance: str = "seed_only"
    chain_identity_complete: bool = True
    true_l1: bool = False
    true_l2: bool = False

    def __post_init__(self) -> None:
        if self.provenance != "seed_only":
            raise ValueError("raw Instructions provenance must be seed_only")
        if not isinstance(self.chain_identity_complete, bool):
            raise TypeError("chain_identity_complete capability must be boolean")
        if not isinstance(self.true_l1, bool) or not isinstance(self.true_l2, bool):
            raise TypeError("true_l1 and true_l2 capabilities must be boolean")
        if self.true_l1 or self.true_l2:
            raise ValueError("BigQuery Instructions discovery is not true L1 or true L2")

    @property
    def provenance_label(self) -> str:
        return self.provenance

    @property
    def labels(self) -> tuple[str, ...]:
        labels = [self.provenance]
        if self.chain_identity_complete:
            labels.append("chain_identity_complete")
        labels.extend(("not_true_l1", "not_true_l2"))
        return tuple(labels)

    @property
    def capability_labels(self) -> tuple[str, ...]:
        return self.labels

    @property
    def is_true_l1(self) -> bool:
        return self.true_l1

    @property
    def is_true_l2(self) -> bool:
        return self.true_l2


@dataclass(frozen=True, slots=True)
class RawInstructionRow:
    """One immutable BigQuery discovery row.

    The first fourteen attributes are exactly the columns selected by the
    ``pump_instruction_discovery_v1`` query.  Repeated values are tuples so a
    caller cannot mutate a row after it has been yielded.  ``params`` retains
    each raw key/value pair (including null members) and does not parse JSON or
    coerce numeric-looking text.
    """

    block_slot: int
    block_hash: str
    block_timestamp: datetime
    tx_signature: str
    source_index: int | None
    source_parent_index: int | None
    accounts: tuple[str, ...] | None
    data: str | None
    parsed: str | None
    program: str | None
    program_id: str | None
    instruction_type: str | None
    params: tuple[tuple[str | None, str | None], ...] | None
    chain_identity_complete: bool
    source_locator: RawInstructionSourceLocator

    def __post_init__(self) -> None:
        self._validate_values()

    def _validate_values(self) -> None:
        _nonnegative_int(self.block_slot, "block_slot")
        _nonempty_text(self.block_hash, "block_hash")
        _utc_timestamp(self.block_timestamp, "block_timestamp")
        _nonempty_text(self.tx_signature, "tx_signature")
        if self.source_index is not None:
            _nonnegative_int(self.source_index, "source_index")
        if self.source_parent_index is not None:
            _nonnegative_int(self.source_parent_index, "source_parent_index")
        if self.accounts is not None:
            if not isinstance(self.accounts, tuple):
                raise TypeError("accounts must be an immutable tuple or None")
            for value in self.accounts:
                _nonempty_text(value, "accounts element")
        for name in ("data", "parsed", "program", "program_id", "instruction_type"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{name} must be text or None")
        if self.params is not None:
            if not isinstance(self.params, tuple):
                raise TypeError("params must be an immutable tuple or None")
            for pair in self.params:
                if not isinstance(pair, tuple) or len(pair) != 2:
                    raise TypeError("params must contain two-member immutable pairs")
                if any(item is not None and not isinstance(item, str) for item in pair):
                    raise TypeError("params key/value members must be text or None")
        if not isinstance(self.chain_identity_complete, bool):
            raise TypeError("chain_identity_complete must be boolean")
        if not self.chain_identity_complete:
            raise ValueError("row does not have complete chain identity")

    @property
    def locator(self) -> RawInstructionSourceLocator:
        return self.source_locator

    @property
    def provenance(self) -> str:
        return "seed_only"

    @property
    def provenance_label(self) -> str:
        return self.provenance

    @property
    def capabilities(self) -> RawInstructionCapabilities:
        return RawInstructionCapabilities(
            provenance=self.provenance,
            chain_identity_complete=self.chain_identity_complete,
        )

    @property
    def capability_labels(self) -> tuple[str, ...]:
        return self.capabilities.labels

    @property
    def true_l1(self) -> bool:
        return False

    @property
    def true_l2(self) -> bool:
        return False

    @property
    def is_true_l1(self) -> bool:
        return False

    @property
    def is_true_l2(self) -> bool:
        return False

    @property
    def is_l1(self) -> bool:
        return False

    @property
    def is_l2(self) -> bool:
        return False


def _expected_arrow_types(pa: Any) -> dict[str, Any]:
    """Types emitted by ``bigquery_archive.arrow_schema`` for discovery rows."""

    return {
        "block_slot": pa.int64(),
        "block_hash": pa.string(),
        "block_timestamp": pa.timestamp("us", tz="UTC"),
        "tx_signature": pa.string(),
        "source_index": pa.int64(),
        "source_parent_index": pa.int64(),
        "accounts": pa.list_(pa.string()),
        "data": pa.string(),
        "parsed": pa.string(),
        "program": pa.string(),
        "program_id": pa.string(),
        "instruction_type": pa.string(),
        "params": pa.list_(
            pa.struct(
                [
                    pa.field("key", pa.string()),
                    pa.field("value", pa.string()),
                ]
            )
        ),
        "chain_identity_complete": pa.bool_(),
    }


def _validate_parquet_schema(parquet: Any, pa: Any, path: Path) -> None:
    schema = parquet.schema_arrow
    names = tuple(schema.names)
    missing = [name for name in DISCOVERY_COLUMNS if name not in names]
    if missing:
        raise ValueError(f"Parquet shard {path} is missing discovery columns: {', '.join(missing)}")
    expected = _expected_arrow_types(pa)
    for name in DISCOVERY_COLUMNS:
        field = schema.field(name)
        if not field.type.equals(expected[name]):
            raise TypeError(
                f"Parquet shard {path} field {name!r} has type {field.type}, "
                f"expected {expected[name]}"
            )


def _string_or_none(value: object, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{label} must be text or None")
    return value


def _int_or_none(value: object, label: str) -> int | None:
    if value is None:
        return None
    return _nonnegative_int(value, label)


def _accounts(value: object) -> tuple[str, ...] | None:
    if value is None:
        return None
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError("accounts must be a sequence of strings or None")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item:
            raise TypeError("accounts elements must be non-empty strings")
        result.append(item)
    return tuple(result)


def _params(value: object) -> tuple[tuple[str | None, str | None], ...] | None:
    if value is None:
        return None
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError("params must be a sequence of key/value records or None")
    result: list[tuple[str | None, str | None]] = []
    for item in value:
        if item is None:
            result.append((None, None))
            continue
        if not isinstance(item, Mapping):
            raise TypeError("params elements must be mappings")
        key = _string_or_none(item.get("key"), "params.key")
        val = _string_or_none(item.get("value"), "params.value")
        result.append((key, val))
    return tuple(result)


def _row_from_mapping(
    values: Mapping[str, object],
    locator: RawInstructionSourceLocator,
) -> RawInstructionRow:
    missing = [name for name in DISCOVERY_COLUMNS if name not in values]
    if missing:
        raise ValueError(f"row is missing discovery columns: {', '.join(missing)}")
    identity = values["chain_identity_complete"]
    if not isinstance(identity, bool):
        raise TypeError("chain_identity_complete must be boolean")
    if not identity:
        raise ValueError("row does not have complete chain identity")
    timestamp = _utc_timestamp(values["block_timestamp"], "block_timestamp")
    return RawInstructionRow(
        block_slot=_nonnegative_int(values["block_slot"], "block_slot"),
        block_hash=_nonempty_text(values["block_hash"], "block_hash"),
        block_timestamp=timestamp,
        tx_signature=_nonempty_text(values["tx_signature"], "tx_signature"),
        source_index=_int_or_none(values["source_index"], "source_index"),
        source_parent_index=_int_or_none(values["source_parent_index"], "source_parent_index"),
        accounts=_accounts(values["accounts"]),
        data=_string_or_none(values["data"], "data"),
        parsed=_string_or_none(values["parsed"], "parsed"),
        program=_string_or_none(values["program"], "program"),
        program_id=_string_or_none(values["program_id"], "program_id"),
        instruction_type=_string_or_none(values["instruction_type"], "instruction_type"),
        params=_params(values["params"]),
        chain_identity_complete=identity,
        source_locator=locator,
    )


def _same_manifest(left: ArchiveManifest, right: ArchiveManifest) -> bool:
    return (
        left.table == right.table
        and left.schema_sha256 == right.schema_sha256
        and left.shards == right.shards
        and left.manifest_sha256 == right.manifest_sha256
        and left.manifest_path == right.manifest_path
        and left.provenance == right.provenance
    )


@dataclass(frozen=True, slots=True)
class RawInstructionArchiveReader:
    """Validated streaming reader for one local Instructions archive.

    Construction performs only cheap argument validation.  The first call to
    :meth:`iter_rows` loads the on-disk canonical manifest, hashes every listed
    shard, and checks Parquet metadata before yielding any row.  A Parquet
    record batch is converted to Python values at a time; no whole-day table is
    materialized in memory.
    """

    archive_root: str | Path
    manifest: ArchiveManifest | str | Path
    batch_size: int = 1024
    expected_schema_sha256: str | None = None
    expected_source_contract_sha256: str | None = None
    expected_start: str | datetime | None = None
    expected_end: str | datetime | None = None

    def __post_init__(self) -> None:
        if isinstance(self.batch_size, bool) or not isinstance(self.batch_size, int):
            raise TypeError("batch_size must be a positive integer")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        for value, label in (
            (self.expected_schema_sha256, "expected_schema_sha256"),
            (self.expected_source_contract_sha256, "expected_source_contract_sha256"),
        ):
            if value is not None and not _HEX64.fullmatch(value):
                raise ValueError(f"{label} must be lowercase SHA-256 hex")
        if self.expected_start is not None:
            _bound(self.expected_start, "expected_start")
        if self.expected_end is not None:
            _bound(self.expected_end, "expected_end")
        if (
            self.expected_start is not None
            and self.expected_end is not None
            and _bound(self.expected_end, "expected_end")
            <= _bound(self.expected_start, "expected_start")
        ):
            raise ValueError("expected_end must be later than expected_start")

    @property
    def root(self) -> Path:
        return Path(self.archive_root)

    @property
    def capabilities(self) -> RawInstructionCapabilities:
        return RawInstructionCapabilities()

    @property
    def provenance(self) -> str:
        return "seed_only"

    @property
    def provenance_label(self) -> str:
        return self.provenance

    @property
    def capability_labels(self) -> tuple[str, ...]:
        return self.capabilities.labels

    @property
    def true_l1(self) -> bool:
        return False

    @property
    def true_l2(self) -> bool:
        return False

    def _manifest_on_disk(self) -> ArchiveManifest:
        supplied = self.manifest
        if isinstance(supplied, ArchiveManifest):
            manifest_path = self.root / supplied.manifest_path
        else:
            path = Path(supplied)
            manifest_path = path if path.is_absolute() else self.root / path
        loaded = load_archive_manifest(manifest_path)
        if isinstance(supplied, ArchiveManifest) and not _same_manifest(supplied, loaded):
            raise ValueError("supplied archive manifest differs from the on-disk manifest")
        if loaded.table != "Instructions":
            raise ValueError("raw instruction reader requires manifest table Instructions")
        if self.expected_schema_sha256 is not None and (
            loaded.schema_sha256 != self.expected_schema_sha256
        ):
            raise ValueError("archive schema hash does not match expected_schema_sha256")
        provenance = loaded.provenance
        if self.expected_source_contract_sha256 is not None:
            if provenance is None:
                raise ValueError("archive has no query provenance/source contract")
            if provenance.source_contract_sha256 != self.expected_source_contract_sha256:
                raise ValueError("archive source contract hash does not match expectation")
        if provenance is not None:
            if provenance.total_rows != loaded.row_count:
                raise ValueError("manifest row_count does not match query provenance total_rows")
            start = _bound(provenance.start, "provenance.start")
            end = _bound(provenance.end, "provenance.end")
            if end <= start:
                raise ValueError("provenance end must be later than provenance start")
            if self.expected_start is not None and start != _bound(
                self.expected_start, "expected_start"
            ):
                raise ValueError("archive provenance start does not match expected_start")
            if self.expected_end is not None and end != _bound(self.expected_end, "expected_end"):
                raise ValueError("archive provenance end does not match expected_end")
        elif self.expected_start is not None or self.expected_end is not None:
            raise ValueError("expected bounds require query provenance")
        return loaded

    def _preflight(self) -> ArchiveManifest:
        manifest = self._manifest_on_disk()
        # This is intentionally before opening a Parquet iterator: a caller
        # never receives a row from a partially/tampered archive.
        verify_archive_manifest(manifest, self.root)
        _, pq = _require_pyarrow()
        pa, _ = _require_pyarrow()
        for shard in manifest.shards:
            path = self.root / shard.path
            parquet = pq.ParquetFile(path)
            try:
                _validate_parquet_schema(parquet, pa, path)
                metadata = parquet.metadata
                if metadata is None or metadata.num_rows != shard.row_count:
                    actual = None if metadata is None else metadata.num_rows
                    raise ValueError(
                        f"Parquet shard {shard.path} row count {actual} "
                        f"does not match manifest {shard.row_count}"
                    )
            finally:
                close = getattr(parquet, "close", None)
                if callable(close):
                    close()
        return manifest

    def iter_rows(self) -> Iterator[RawInstructionRow]:
        manifest = self._preflight()
        _, pq = _require_pyarrow()
        provenance_bounds: tuple[datetime, datetime] | None = None
        if manifest.provenance is not None:
            provenance_bounds = (
                _bound(manifest.provenance.start, "provenance.start"),
                _bound(manifest.provenance.end, "provenance.end"),
            )
        yielded = 0
        for shard_index, shard in enumerate(manifest.shards):
            path = self.root / shard.path
            parquet = pq.ParquetFile(path)
            try:
                row_offset = 0
                for batch in parquet.iter_batches(
                    batch_size=self.batch_size,
                    columns=list(DISCOVERY_COLUMNS),
                    use_threads=False,
                ):
                    # ``to_pylist`` is bounded by ``batch_size`` and keeps
                    # Decimal, bytes, and nested values in their Arrow-native
                    # Python representations.  No float conversion occurs.
                    rows = batch.to_pylist()
                    for raw in rows:
                        if not isinstance(raw, Mapping):
                            raise TypeError("Parquet record batch row must be a mapping")
                        locator = RawInstructionSourceLocator(
                            manifest_sha256=manifest.manifest_sha256,
                            shard_path=shard.path,
                            shard_index=shard_index,
                            row_offset=row_offset,
                        )
                        row = _row_from_mapping(cast(Mapping[str, object], raw), locator)
                        if provenance_bounds is not None and not (
                            provenance_bounds[0] <= row.block_timestamp < provenance_bounds[1]
                        ):
                            raise ValueError(
                                f"row {locator} block_timestamp is outside the "
                                "half-open query provenance bounds"
                            )
                        row_offset += 1
                        yielded += 1
                        yield row
                if row_offset != shard.row_count:
                    raise ValueError(
                        f"Parquet shard {shard.path} yielded {row_offset} rows, "
                        f"manifest declares {shard.row_count}"
                    )
            finally:
                close = getattr(parquet, "close", None)
                if callable(close):
                    close()
        if yielded != manifest.row_count:
            raise ValueError(
                f"archive yielded {yielded} rows, manifest declares {manifest.row_count}"
            )
        if manifest.provenance is not None and yielded != manifest.provenance.total_rows:
            raise ValueError("streamed row count does not match query provenance total_rows")

    def rows(self) -> Iterator[RawInstructionRow]:
        return self.iter_rows()

    def __iter__(self) -> Iterator[RawInstructionRow]:
        return self.iter_rows()


# Concise aliases are useful to callers that do not need to retain the reader
# object, while keeping one implementation and one validation path.
RawInstructionsReader = RawInstructionArchiveReader
RawInstructionReader = RawInstructionArchiveReader


def stream_raw_instructions(
    archive_root: str | Path,
    manifest: ArchiveManifest | str | Path,
    *,
    batch_size: int = 1024,
    expected_schema_sha256: str | None = None,
    expected_schema_hash: str | None = None,
    expected_source_contract_sha256: str | None = None,
    expected_source_contract: str | None = None,
    expected_start: str | datetime | None = None,
    expected_end: str | datetime | None = None,
) -> Iterator[RawInstructionRow]:
    """Stream verified raw Instructions rows from ``archive_root``.

    The function is a generator, so archive verification occurs when iteration
    starts and always precedes the first yielded row.  ``expected_schema_hash``
    and ``expected_source_contract`` are spelling aliases for the SHA-256
    parameter names retained for callers that use the shorter terminology.
    """

    if (
        expected_schema_sha256 is not None
        and expected_schema_hash is not None
        and expected_schema_sha256 != expected_schema_hash
    ):
        raise ValueError("expected schema hash aliases disagree")
    if (
        expected_source_contract_sha256 is not None
        and expected_source_contract is not None
        and expected_source_contract_sha256 != expected_source_contract
    ):
        raise ValueError("expected source contract aliases disagree")
    reader = RawInstructionArchiveReader(
        archive_root=archive_root,
        manifest=manifest,
        batch_size=batch_size,
        expected_schema_sha256=expected_schema_sha256 or expected_schema_hash,
        expected_source_contract_sha256=(
            expected_source_contract_sha256 or expected_source_contract
        ),
        expected_start=expected_start,
        expected_end=expected_end,
    )
    yield from reader.iter_rows()


iter_raw_instructions = stream_raw_instructions
read_raw_instructions = stream_raw_instructions
iter_raw_instruction_rows = stream_raw_instructions
stream_raw_instruction_rows = stream_raw_instructions


__all__ = [
    "DISCOVERY_COLUMNS",
    "RawInstructionArchiveReader",
    "RawInstructionCapabilities",
    "RawInstructionReader",
    "RawInstructionRow",
    "RawInstructionSourceLocator",
    "RawInstructionsReader",
    "iter_raw_instructions",
    "iter_raw_instruction_rows",
    "read_raw_instructions",
    "stream_raw_instructions",
    "stream_raw_instruction_rows",
]
