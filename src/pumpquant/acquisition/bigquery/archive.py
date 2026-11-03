"""Lossless-ish raw BigQuery REST response archival.

This module deliberately sits below the query-planning adapter.  It accepts
the JSON shape returned by the BigQuery REST API (pages with a ``schema`` and
``rows`` containing ``f``/``v`` values), decodes values without going through
floating point, and writes bounded, immutable Parquet shards.  BigQuery and
S3 dependencies are optional and loaded only by the operations that need
them.

The archive is intended to preserve the values BigQuery exposes.  It cannot
recover fields omitted by BigQuery itself (for example serialized
transactions); callers should retain the original response when that
distinction matters.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import threading
import time as time_module
from collections.abc import Iterable, Iterator, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol, TypeAlias, cast
from urllib.parse import urlparse

from pumpquant.core.hashing import sha256_file_with_size

JSONMapping: TypeAlias = Mapping[str, object]
PageLike: TypeAlias = Mapping[str, object] | object

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_MAX_TIMESTAMP_MICROSECONDS = Decimal("1000000")
_SUPPORTED_TYPES = {
    "STRING",
    "BYTES",
    "INT64",
    "FLOAT64",
    "BOOL",
    "TIMESTAMP",
    "DATE",
    "TIME",
    "DATETIME",
    "NUMERIC",
    "BIGNUMERIC",
    "RECORD",
}
_ARCHIVED_SCHEMA_KEYS = {"name", "type", "mode", "fields"}
_REMOTE_VERIFY_RANGE_BYTES = 4 * 1024 * 1024
_REMOTE_VERIFY_ATTEMPTS = 8
_REMOTE_UPLOAD_ATTEMPTS = 8


class _HasValue(Protocol):
    def __getattr__(self, name: str) -> object: ...


def _load_pyarrow() -> tuple[Any, Any]:
    """Load pyarrow lazily and provide an actionable optional-extra error."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:  # pragma: no cover - depends on optional package
        raise RuntimeError(
            "raw BigQuery archival requires: pip install 'pumpquant[archive]'"
        ) from error
    return pa, pq


def _read_member(value: PageLike, name: str, default: object = None) -> object:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _require_mapping(value: object, description: str) -> JSONMapping:
    if not isinstance(value, Mapping):
        raise TypeError(f"{description} must be a mapping")
    return cast(JSONMapping, value)


def _normal_type(type_name: object) -> str:
    if not isinstance(type_name, str) or not type_name.strip():
        raise ValueError("BigQuery field type must be a non-empty string")
    normalized = type_name.strip().upper()
    aliases = {
        "INTEGER": "INT64",
        "INT": "INT64",
        "FLOAT": "FLOAT64",
        "BOOLEAN": "BOOL",
        "STRUCT": "RECORD",
    }
    return aliases.get(normalized, normalized)


@dataclass(frozen=True, slots=True)
class BigQueryField:
    """Immutable BigQuery schema field used by the decoder and Arrow writer."""

    name: str
    type_name: str
    mode: str = "NULLABLE"
    fields: tuple[BigQueryField, ...] = ()

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("BigQuery field name must be non-empty")
        normalized_type = _normal_type(self.type_name)
        normalized_mode = self.mode.strip().upper() if isinstance(self.mode, str) else ""
        if not normalized_mode:
            raise ValueError(f"field {self.name!r} mode must be a non-empty string")
        if normalized_mode not in {"NULLABLE", "REQUIRED", "REPEATED"}:
            raise ValueError(f"unsupported BigQuery field mode {self.mode!r}")
        if normalized_type not in _SUPPORTED_TYPES:
            raise ValueError(f"unsupported BigQuery field type {normalized_type!r}")
        object.__setattr__(self, "type_name", normalized_type)
        object.__setattr__(self, "mode", normalized_mode)
        if normalized_type != "RECORD" and self.fields:
            raise ValueError(f"non-RECORD field {self.name!r} cannot have nested fields")
        child_names = [child.name for child in self.fields]
        if len(child_names) != len(set(child_names)):
            raise ValueError(f"RECORD field {self.name!r} has duplicate child names")


@dataclass(frozen=True, slots=True)
class BigQuerySchema:
    """Immutable parsed BigQuery schema and its deterministic hash."""

    fields: tuple[BigQueryField, ...]
    schema_sha256: str

    def __post_init__(self) -> None:
        names = [field.name for field in self.fields]
        if len(names) != len(set(names)):
            raise ValueError("BigQuery schema has duplicate field names")

    @property
    def hash(self) -> str:
        """Alias useful to callers that use ``schema_hash`` terminology."""

        return self.schema_sha256

    @property
    def schema_hash(self) -> str:
        return self.schema_sha256


def _field_from_mapping(value: object) -> BigQueryField:
    field = _require_mapping(value, "BigQuery schema field")
    unsupported = sorted(
        str(key)
        for key, item in field.items()
        if key not in _ARCHIVED_SCHEMA_KEYS and item is not None
    )
    if unsupported:
        raise ValueError(
            "BigQuery schema contains unsupported value-semantic metadata: "
            + ", ".join(unsupported)
        )
    name = field.get("name")
    type_name = field.get("type")
    mode = field.get("mode", "NULLABLE")
    nested_raw = field.get("fields", ())
    if nested_raw is None:
        nested_raw = ()
    if not isinstance(nested_raw, Sequence) or isinstance(nested_raw, (str, bytes, bytearray)):
        raise TypeError(f"field {name!r} nested fields must be a sequence")
    nested = tuple(_field_from_mapping(child) for child in nested_raw)
    if not isinstance(name, str) or not isinstance(type_name, str) or not isinstance(mode, str):
        raise TypeError("BigQuery schema field name, type, and mode must be strings")
    return BigQueryField(name, type_name, mode, nested)


def parse_bigquery_schema(schema: Mapping[str, object] | BigQuerySchema) -> BigQuerySchema:
    """Parse a REST ``schema`` mapping and hash its layout deterministically.

    The REST response uses ``{"fields": [...]}``; passing the field sequence
    directly is intentionally unsupported so accidental schema/row confusion
    is reported early.
    """

    if isinstance(schema, BigQuerySchema):
        return schema
    fields_raw = schema.get("fields")
    if not isinstance(fields_raw, Sequence) or isinstance(fields_raw, (str, bytes, bytearray)):
        raise TypeError("BigQuery schema must contain a fields sequence")
    fields = tuple(_field_from_mapping(item) for item in fields_raw)
    payload = {"fields": [_field_payload(field) for field in fields]}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return BigQuerySchema(fields, hashlib.sha256(encoded).hexdigest())


def _field_payload(field: BigQueryField) -> dict[str, object]:
    return {
        "name": field.name,
        "type": field.type_name,
        "mode": field.mode,
        "fields": [_field_payload(child) for child in field.fields],
    }


def schema_sha256(schema: Mapping[str, object] | BigQuerySchema) -> str:
    """Return the canonical SHA-256 for a REST schema mapping."""

    return parse_bigquery_schema(schema).schema_sha256


def _cell_value(cell: object) -> object:
    if isinstance(cell, Mapping):
        if "v" not in cell:
            raise ValueError("BigQuery REST cell is missing its v member")
        return cell["v"]
    if cell is None:
        raise ValueError("BigQuery REST cell cannot be absent")
    sentinel = object()
    value = getattr(cell, "v", sentinel)
    if value is sentinel:
        raise TypeError("BigQuery REST cell must expose a v member")
    return value


def _parse_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, Decimal)) and not isinstance(value, bool):
        parsed = _timestamp_from_seconds(Decimal(value))
    elif isinstance(value, float):
        # REST timestamps are strings; accepting a float here is only for
        # page-like test doubles and still routes through Decimal(str(...)).
        parsed = _timestamp_from_seconds(Decimal(str(value)))
    elif isinstance(value, str):
        text = value.strip()
        try:
            parsed = _timestamp_from_seconds(Decimal(text))
        except InvalidOperation:
            normalized = text.replace(" ", "T", 1) if "T" not in text else text
            if normalized.endswith("Z"):
                normalized = normalized[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError as error:
                raise ValueError(f"invalid BigQuery TIMESTAMP {value!r}") from error
    else:
        raise TypeError(f"BigQuery TIMESTAMP value must be a string or datetime, got {value!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _timestamp_from_seconds(seconds: Decimal) -> datetime:
    # BigQuery exposes microsecond precision.  Integer arithmetic on Decimal
    # avoids float conversion and therefore preserves large slot-era epochs.
    micros_decimal = seconds * _MAX_TIMESTAMP_MICROSECONDS
    if micros_decimal != micros_decimal.to_integral_value():
        raise ValueError("BigQuery TIMESTAMP has sub-microsecond precision")
    micros = int(micros_decimal)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    try:
        return epoch + timedelta(microseconds=micros)
    except OverflowError as error:
        raise ValueError("BigQuery TIMESTAMP is outside Python datetime range") from error


def _parse_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise TypeError(f"BigQuery DATE value must be a string, got {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"invalid BigQuery DATE {value!r}") from error


def _parse_time(value: object) -> time:
    if isinstance(value, time):
        if value.tzinfo is not None:
            raise ValueError("BigQuery TIME cannot contain a timezone")
        return value
    if not isinstance(value, str):
        raise TypeError(f"BigQuery TIME value must be a string, got {value!r}")
    try:
        parsed = time.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"invalid BigQuery TIME {value!r}") from error
    if parsed.tzinfo is not None:
        raise ValueError("BigQuery TIME cannot contain a timezone")
    return parsed


def _parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise ValueError("BigQuery DATETIME cannot contain a timezone")
        return value
    if not isinstance(value, str):
        raise TypeError(f"BigQuery DATETIME value must be a string, got {value!r}")
    normalized = value.replace(" ", "T", 1) if "T" not in value else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise ValueError(f"invalid BigQuery DATETIME {value!r}") from error
    if parsed.tzinfo is not None:
        raise ValueError("BigQuery DATETIME cannot contain a timezone")
    return parsed


def _parse_decimal(value: object, type_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TypeError(f"BigQuery {type_name} value must be text or Decimal, got {value!r}")
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as error:
        raise ValueError(f"invalid BigQuery {type_name} value {value!r}") from error
    if not parsed.is_finite():
        raise ValueError(f"BigQuery {type_name} value must be finite")
    return parsed


def _decode_scalar(value: object, type_name: str) -> object:
    if value is None:
        return None
    if type_name == "STRING":
        if not isinstance(value, str):
            raise TypeError("BigQuery STRING value must be text")
        return value
    if type_name == "BYTES":
        if isinstance(value, bytes):
            return value
        if not isinstance(value, str):
            raise TypeError(f"BigQuery BYTES value must be base64 text, got {value!r}")
        try:
            return base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as error:
            raise ValueError("invalid BigQuery BYTES base64 value") from error
    if type_name == "INT64":
        if isinstance(value, bool):
            raise TypeError("BigQuery INT64 cannot be boolean")
        try:
            return int(str(value), 10)
        except (TypeError, ValueError) as error:
            raise ValueError(f"invalid BigQuery INT64 value {value!r}") from error
    if type_name == "FLOAT64":
        if isinstance(value, bool):
            raise TypeError("BigQuery FLOAT64 cannot be boolean")
        try:
            return float(str(value))
        except (TypeError, ValueError) as error:
            raise ValueError(f"invalid BigQuery FLOAT64 value {value!r}") from error
    if type_name == "BOOL":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized == "true":
                return True
            if normalized == "false":
                return False
        raise ValueError(f"invalid BigQuery BOOL value {value!r}")
    if type_name == "TIMESTAMP":
        return _parse_timestamp(value)
    if type_name == "DATE":
        return _parse_date(value)
    if type_name == "TIME":
        return _parse_time(value)
    if type_name == "DATETIME":
        return _parse_datetime(value)
    if type_name in {"NUMERIC", "BIGNUMERIC"}:
        return _parse_decimal(value, type_name)
    raise ValueError(f"unsupported BigQuery field type {type_name!r}")


def _decode_record(value: object, field: BigQueryField) -> dict[str, object] | None:
    if value is None:
        return None
    raw_cells = value.get("f") if isinstance(value, Mapping) and "f" in value else value
    if not isinstance(raw_cells, Sequence) and not isinstance(value, Mapping):
        raw_cells = getattr(value, "f", raw_cells)
    if not isinstance(raw_cells, Sequence) or isinstance(raw_cells, (str, bytes, bytearray)):
        raise TypeError(f"BigQuery RECORD field {field.name!r} requires an f sequence")
    cells = list(raw_cells)
    if len(cells) != len(field.fields):
        raise ValueError(f"BigQuery RECORD field {field.name!r} has the wrong value count")
    return {
        nested.name: _decode_cell(cells[index], nested) for index, nested in enumerate(field.fields)
    }


def _decode_cell(cell: object, field: BigQueryField) -> object:
    value = _cell_value(cell)
    if field.mode == "REPEATED":
        if value is None:
            return []
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
            raise TypeError(f"repeated BigQuery field {field.name!r} requires a sequence")
        result: list[object] = []
        for element in value:
            element_value = _cell_value(element)
            if field.type_name == "RECORD":
                result.append(_decode_record(element_value, field))
            else:
                result.append(_decode_scalar(element_value, field.type_name))
        return result
    if value is None:
        return None
    if field.type_name == "RECORD":
        return _decode_record(value, field)
    return _decode_scalar(value, field.type_name)


def decode_bigquery_row(
    row: Mapping[str, object] | Sequence[object],
    schema: Mapping[str, object] | BigQuerySchema,
) -> dict[str, object]:
    """Decode one REST row's ``f``/``v`` cells against a schema."""

    parsed = parse_bigquery_schema(schema)
    raw_cells = row.get("f") if isinstance(row, Mapping) else getattr(row, "f", row)
    if not isinstance(raw_cells, Sequence) or isinstance(raw_cells, (str, bytes, bytearray)):
        raise TypeError("BigQuery REST row must contain an f sequence")
    cells = list(raw_cells)
    if len(cells) != len(parsed.fields):
        raise ValueError("BigQuery REST row cell count does not match the schema")
    return {
        field.name: _decode_cell(cells[index], field) for index, field in enumerate(parsed.fields)
    }


def page_schema(page: PageLike) -> Mapping[str, object]:
    """Extract and validate a page-like object's REST schema mapping."""

    raw_schema = _read_member(page, "schema")
    if not isinstance(raw_schema, Mapping):
        raise TypeError("BigQuery page must expose a schema mapping")
    return raw_schema


def decode_bigquery_page(
    page: PageLike,
    schema: Mapping[str, object] | BigQuerySchema | None = None,
) -> tuple[dict[str, object], ...]:
    """Decode every REST row in a page-like object."""

    raw_schema = schema if schema is not None else page_schema(page)
    parsed = parse_bigquery_schema(raw_schema)
    raw_rows = _read_member(page, "rows", ())
    if raw_rows is None:
        return ()
    if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes, bytearray)):
        raise TypeError("BigQuery page rows must be a sequence")
    return tuple(decode_bigquery_row(row, parsed) for row in raw_rows)


def decode_bigquery_pages(
    pages: Iterable[PageLike],
    schema: Mapping[str, object] | BigQuerySchema | None = None,
) -> Iterator[dict[str, object]]:
    """Stream decoded rows from pages, checking schema consistency."""

    expected = parse_bigquery_schema(schema) if schema is not None else None
    for page in pages:
        current = parse_bigquery_schema(page_schema(page))
        if expected is None:
            expected = current
        elif expected.schema_sha256 != current.schema_sha256:
            raise ValueError("BigQuery page schema changed during archival")
        yield from decode_bigquery_page(page, expected)


def arrow_schema(
    schema: Mapping[str, object] | BigQuerySchema,
) -> Any:
    """Build an explicit Arrow schema retaining BigQuery numeric types."""

    pa, _ = _load_pyarrow()
    parsed = parse_bigquery_schema(schema)

    def convert(field: BigQueryField) -> Any:
        if field.type_name == "STRING":
            value_type = pa.string()
        elif field.type_name == "BYTES":
            value_type = pa.binary()
        elif field.type_name == "INT64":
            value_type = pa.int64()
        elif field.type_name == "FLOAT64":
            value_type = pa.float64()
        elif field.type_name == "BOOL":
            value_type = pa.bool_()
        elif field.type_name == "TIMESTAMP":
            value_type = pa.timestamp("us", tz="UTC")
        elif field.type_name == "DATE":
            value_type = pa.date32()
        elif field.type_name == "TIME":
            value_type = pa.time64("us")
        elif field.type_name == "DATETIME":
            value_type = pa.timestamp("us")
        elif field.type_name == "NUMERIC":
            value_type = pa.decimal128(38, 9)
        elif field.type_name == "BIGNUMERIC":
            value_type = pa.decimal256(76, 38)
        elif field.type_name == "RECORD":
            value_type = pa.struct(
                [
                    pa.field(
                        child.name,
                        convert(child),
                        nullable=child.mode != "REQUIRED",
                    )
                    for child in field.fields
                ]
            )
        else:  # pragma: no cover - guarded by BigQueryField
            raise ValueError(f"unsupported BigQuery field type {field.type_name!r}")
        if field.mode == "REPEATED":
            return pa.list_(value_type)
        return value_type

    return pa.schema(
        [
            pa.field(field.name, convert(field), nullable=field.mode != "REQUIRED")
            for field in parsed.fields
        ]
    )


# A descriptive alias for callers that prefer a verb rather than a noun.
bigquery_arrow_schema = arrow_schema


@dataclass(frozen=True, slots=True)
class ShardMetadata:
    """Immutable metadata for one atomically published Parquet shard."""

    path: str
    row_count: int
    byte_count: int
    sha256: str
    schema_sha256: str
    min_block_slot: int | None = None
    max_block_slot: int | None = None
    min_block_timestamp: datetime | None = None
    max_block_timestamp: datetime | None = None

    def __post_init__(self) -> None:
        if not self.path or Path(self.path).is_absolute() or ".." in Path(self.path).parts:
            raise ValueError("shard path must be a safe relative path")
        if self.row_count < 0 or self.byte_count < 0:
            raise ValueError("shard row_count and byte_count cannot be negative")
        if not _HEX64.fullmatch(self.sha256) or not _HEX64.fullmatch(self.schema_sha256):
            raise ValueError("shard hashes must be lowercase SHA-256 hex")
        if self.min_block_slot is not None and self.min_block_slot < 0:
            raise ValueError("min_block_slot cannot be negative")
        if self.max_block_slot is not None and self.max_block_slot < 0:
            raise ValueError("max_block_slot cannot be negative")
        if (
            self.min_block_slot is not None
            and self.max_block_slot is not None
            and self.min_block_slot > self.max_block_slot
        ):
            raise ValueError("min_block_slot cannot exceed max_block_slot")
        if (
            self.min_block_timestamp is not None
            and self.max_block_timestamp is not None
            and self.min_block_timestamp > self.max_block_timestamp
        ):
            raise ValueError("min_block_timestamp cannot exceed max_block_timestamp")

    @property
    def bytes(self) -> int:
        """Short alias for the byte count in manifest-facing code."""

        return self.byte_count

    @property
    def schema_hash(self) -> str:
        return self.schema_sha256


@dataclass(frozen=True, slots=True)
class QueryProvenance:
    """Auditable evidence binding an archive to one guarded BigQuery job."""

    plan_name: str
    plan_sha256: str
    request_id: str
    job_id: str
    billing_project: str
    location: str
    source_tables: tuple[str, ...]
    source_contract_sha256: str
    start: str
    end: str
    dry_run_estimated_bytes: int
    maximum_bytes_billed: int
    actual_bytes_processed: int
    total_rows: int

    def __post_init__(self) -> None:
        for label in (
            "plan_name",
            "request_id",
            "job_id",
            "billing_project",
            "location",
            "start",
            "end",
        ):
            value = getattr(self, label)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{label} must be a non-empty string")
        if not _HEX64.fullmatch(self.plan_sha256):
            raise ValueError("plan_sha256 must be lowercase SHA-256 hex")
        if not _HEX64.fullmatch(self.source_contract_sha256):
            raise ValueError("source_contract_sha256 must be lowercase SHA-256 hex")
        if len(self.request_id) > 36 or not self.request_id.isascii():
            raise ValueError("request_id must be at most 36 ASCII characters")
        if not self.source_tables or any(
            not isinstance(table, str) or not table.strip() for table in self.source_tables
        ):
            raise ValueError("source_tables must contain non-empty strings")
        if len(self.source_tables) != len(set(self.source_tables)):
            raise ValueError("source_tables must be unique")
        numeric_fields = (
            self.dry_run_estimated_bytes,
            self.maximum_bytes_billed,
            self.actual_bytes_processed,
            self.total_rows,
        )
        if any(isinstance(value, bool) or not isinstance(value, int) for value in numeric_fields):
            raise TypeError("query provenance byte and row counts must be integers")
        if self.dry_run_estimated_bytes <= 0 or self.maximum_bytes_billed <= 0:
            raise ValueError("dry-run estimate and byte cap must be positive")
        if self.dry_run_estimated_bytes > self.maximum_bytes_billed:
            raise ValueError("dry-run estimate cannot exceed the byte cap")
        if not 0 <= self.actual_bytes_processed <= self.maximum_bytes_billed:
            raise ValueError("actual bytes processed must be within the byte cap")
        if self.total_rows < 0:
            raise ValueError("total_rows cannot be negative")


@dataclass(frozen=True, slots=True)
class ArchiveManifest:
    """Immutable deterministic archive manifest."""

    table: str
    schema_sha256: str
    shards: tuple[ShardMetadata, ...]
    manifest_sha256: str = ""
    manifest_path: str = "manifest.json"
    provenance: QueryProvenance | None = None

    def __post_init__(self) -> None:
        if not self.table.strip():
            raise ValueError("manifest table must be non-empty")
        if not _HEX64.fullmatch(self.schema_sha256):
            raise ValueError("manifest schema_sha256 must be lowercase SHA-256 hex")
        if self.manifest_sha256 and not _HEX64.fullmatch(self.manifest_sha256):
            raise ValueError("manifest_sha256 must be lowercase SHA-256 hex")
        if (
            not self.manifest_path
            or Path(self.manifest_path).is_absolute()
            or Path(self.manifest_path).name != self.manifest_path
        ):
            raise ValueError("manifest_path must be a relative basename")
        paths = [shard.path for shard in self.shards]
        if len(paths) != len(set(paths)):
            raise ValueError("manifest shard paths must be unique")
        if any(shard.schema_sha256 != self.schema_sha256 for shard in self.shards):
            raise ValueError("every shard schema hash must match the manifest schema")

    @property
    def table_name(self) -> str:
        return self.table

    @property
    def schema_hash(self) -> str:
        return self.schema_sha256

    @property
    def row_count(self) -> int:
        return sum(shard.row_count for shard in self.shards)

    @property
    def byte_count(self) -> int:
        return sum(shard.byte_count for shard in self.shards)

    @property
    def bytes(self) -> int:
        return self.byte_count


# Names used by earlier prototypes and by callers that spell out the type.
BigQueryShardMetadata = ShardMetadata
BigQueryArchiveManifest = ArchiveManifest


def _canonical_manifest_payload(manifest: ArchiveManifest) -> dict[str, object]:
    def iso(value: datetime | None) -> str | None:
        if value is None:
            return None
        normalized = value
        if normalized.tzinfo is not None:
            normalized = normalized.astimezone(UTC)
        return normalized.isoformat().replace("+00:00", "Z")

    provenance = manifest.provenance
    return {
        "version": 2,
        "table": manifest.table,
        "manifest_path": manifest.manifest_path,
        "schema_sha256": manifest.schema_sha256,
        "row_count": manifest.row_count,
        "byte_count": manifest.byte_count,
        "query_provenance": (
            None
            if provenance is None
            else {
                "plan_name": provenance.plan_name,
                "plan_sha256": provenance.plan_sha256,
                "request_id": provenance.request_id,
                "job_id": provenance.job_id,
                "billing_project": provenance.billing_project,
                "location": provenance.location,
                "source_tables": list(provenance.source_tables),
                "source_contract_sha256": provenance.source_contract_sha256,
                "start": provenance.start,
                "end": provenance.end,
                "dry_run_estimated_bytes": provenance.dry_run_estimated_bytes,
                "maximum_bytes_billed": provenance.maximum_bytes_billed,
                "actual_bytes_processed": provenance.actual_bytes_processed,
                "total_rows": provenance.total_rows,
            }
        ),
        "shards": [
            {
                "path": shard.path,
                "row_count": shard.row_count,
                "byte_count": shard.byte_count,
                "sha256": shard.sha256,
                "schema_sha256": shard.schema_sha256,
                "min_block_slot": shard.min_block_slot,
                "max_block_slot": shard.max_block_slot,
                "min_block_timestamp": iso(shard.min_block_timestamp),
                "max_block_timestamp": iso(shard.max_block_timestamp),
            }
            for shard in manifest.shards
        ],
    }


def manifest_bytes(manifest: ArchiveManifest) -> bytes:
    """Serialize a manifest without timestamps or host-specific paths."""

    payload = _canonical_manifest_payload(manifest)
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _manifest_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"manifest {label} must be an integer")
    return value


def _manifest_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"manifest {label} must be non-empty text")
    return value


def _manifest_timestamp(value: object, label: str) -> datetime | None:
    if value is None:
        return None
    text = _manifest_text(value, label)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"manifest {label} is not an ISO timestamp") from error
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else parsed


def load_archive_manifest(path: str | Path) -> ArchiveManifest:
    """Load a canonical v2 manifest and bind it to its on-disk hash."""

    manifest_file = Path(path)
    if manifest_file.is_symlink() or not manifest_file.is_file():
        raise FileNotFoundError(manifest_file)
    raw = manifest_file.read_bytes()
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("archive manifest is not valid UTF-8 JSON") from error
    payload = _require_mapping(value, "archive manifest")
    if payload.get("version") != 2:
        raise ValueError("unsupported archive manifest version")
    raw_shards = payload.get("shards")
    if not isinstance(raw_shards, Sequence) or isinstance(raw_shards, (str, bytes, bytearray)):
        raise TypeError("manifest shards must be a sequence")
    shards: list[ShardMetadata] = []
    for index, raw_shard in enumerate(raw_shards):
        shard = _require_mapping(raw_shard, f"manifest shard {index}")
        shards.append(
            ShardMetadata(
                path=_manifest_text(shard.get("path"), "shard path"),
                row_count=_manifest_int(shard.get("row_count"), "shard row_count"),
                byte_count=_manifest_int(shard.get("byte_count"), "shard byte_count"),
                sha256=_manifest_text(shard.get("sha256"), "shard sha256"),
                schema_sha256=_manifest_text(shard.get("schema_sha256"), "shard schema_sha256"),
                min_block_slot=(
                    None
                    if shard.get("min_block_slot") is None
                    else _manifest_int(shard.get("min_block_slot"), "min_block_slot")
                ),
                max_block_slot=(
                    None
                    if shard.get("max_block_slot") is None
                    else _manifest_int(shard.get("max_block_slot"), "max_block_slot")
                ),
                min_block_timestamp=_manifest_timestamp(
                    shard.get("min_block_timestamp"), "min_block_timestamp"
                ),
                max_block_timestamp=_manifest_timestamp(
                    shard.get("max_block_timestamp"), "max_block_timestamp"
                ),
            )
        )

    raw_provenance = payload.get("query_provenance")
    provenance: QueryProvenance | None = None
    if raw_provenance is not None:
        item = _require_mapping(raw_provenance, "manifest query_provenance")
        raw_tables = item.get("source_tables")
        if not isinstance(raw_tables, Sequence) or isinstance(raw_tables, (str, bytes, bytearray)):
            raise TypeError("manifest source_tables must be a sequence")
        provenance = QueryProvenance(
            plan_name=_manifest_text(item.get("plan_name"), "plan_name"),
            plan_sha256=_manifest_text(item.get("plan_sha256"), "plan_sha256"),
            request_id=_manifest_text(item.get("request_id"), "request_id"),
            job_id=_manifest_text(item.get("job_id"), "job_id"),
            billing_project=_manifest_text(item.get("billing_project"), "billing_project"),
            location=_manifest_text(item.get("location"), "location"),
            source_tables=tuple(_manifest_text(table, "source table") for table in raw_tables),
            source_contract_sha256=_manifest_text(
                item.get("source_contract_sha256"), "source_contract_sha256"
            ),
            start=_manifest_text(item.get("start"), "start"),
            end=_manifest_text(item.get("end"), "end"),
            dry_run_estimated_bytes=_manifest_int(
                item.get("dry_run_estimated_bytes"), "dry_run_estimated_bytes"
            ),
            maximum_bytes_billed=_manifest_int(
                item.get("maximum_bytes_billed"), "maximum_bytes_billed"
            ),
            actual_bytes_processed=_manifest_int(
                item.get("actual_bytes_processed"), "actual_bytes_processed"
            ),
            total_rows=_manifest_int(item.get("total_rows"), "total_rows"),
        )

    manifest_path = _manifest_text(payload.get("manifest_path"), "manifest_path")
    if manifest_file.name != manifest_path:
        raise ValueError("manifest_path does not match the loaded file")
    manifest = ArchiveManifest(
        table=_manifest_text(payload.get("table"), "table"),
        schema_sha256=_manifest_text(payload.get("schema_sha256"), "schema_sha256"),
        shards=tuple(shards),
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
        manifest_path=manifest_path,
        provenance=provenance,
    )
    if _manifest_int(payload.get("row_count"), "row_count") != manifest.row_count:
        raise ValueError("manifest row_count does not match its shards")
    if _manifest_int(payload.get("byte_count"), "byte_count") != manifest.byte_count:
        raise ValueError("manifest byte_count does not match its shards")
    if raw != manifest_bytes(manifest):
        raise ValueError("archive manifest is not in canonical form")
    return manifest


def verify_archive_manifest(
    manifest: ArchiveManifest,
    archive_root: str | Path,
) -> None:
    """Rehash every local shard and the canonical manifest before upload."""

    root = Path(archive_root)
    manifest_file = root / manifest.manifest_path
    if manifest_file.is_symlink() or not manifest_file.is_file():
        raise FileNotFoundError(manifest_file)
    raw = manifest_file.read_bytes()
    canonical = manifest_bytes(manifest)
    manifest_hash = hashlib.sha256(raw).hexdigest()
    if raw != canonical or manifest_hash != manifest.manifest_sha256:
        raise ArchiveConflictError("local archive manifest differs from its recorded hash")
    for shard in manifest.shards:
        path = root / shard.path
        if path.is_symlink() or not path.is_file():
            raise FileNotFoundError(path)
        size, digest = sha256_file_with_size(path)
        if size != shard.byte_count or digest != shard.sha256:
            raise ArchiveConflictError(f"local shard {shard.path!r} failed integrity verification")


def _safe_table_name(table: str) -> str:
    name = table.strip()
    if not name:
        raise ValueError("table name must be non-empty")
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)
    return safe.strip("._") or "table"


def _estimate_value_bytes(value: object) -> int:
    """Cheap deterministic bound used to avoid accumulating an unbounded shard."""

    if value is None:
        return 4
    if isinstance(value, bytes):
        return len(value) + 16
    if isinstance(value, (str, int, float, Decimal, date, time, datetime)):
        return len(str(value).encode("utf-8")) + 16
    if isinstance(value, Mapping):
        return 16 + sum(len(str(key)) + _estimate_value_bytes(item) for key, item in value.items())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return 16 + sum(_estimate_value_bytes(item) for item in value)
    return len(repr(value).encode("utf-8")) + 16


def _parquet_candidate_size(rows: Sequence[dict[str, object]], schema: Any) -> int:
    pa, pq = _load_pyarrow()
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(list(rows), schema=schema), sink, compression="zstd")
    return int(sink.getvalue().size)


def _block_stats(
    rows: Sequence[dict[str, object]],
) -> tuple[int | None, int | None, datetime | None, datetime | None]:
    slots = [value for row in rows if (value := row.get("block_slot")) is not None]
    timestamps = [value for row in rows if (value := row.get("block_timestamp")) is not None]
    if any(isinstance(value, bool) or not isinstance(value, int) for value in slots):
        raise TypeError("block_slot must decode to integers")
    if any(not isinstance(value, datetime) for value in timestamps):
        raise TypeError("block_timestamp must decode to datetimes")
    return (
        min(cast(Sequence[int], slots), default=None),
        max(cast(Sequence[int], slots), default=None),
        min(cast(Sequence[datetime], timestamps), default=None),
        max(cast(Sequence[datetime], timestamps), default=None),
    )


def _write_shard(
    rows: Sequence[dict[str, object]],
    schema: Any,
    output_dir: Path,
    relative_path: str,
    schema_hash: str,
) -> ShardMetadata:
    _, pq = _load_pyarrow()
    target = output_dir / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = Path(f"{target}.partial")
    if partial.exists():
        raise FileExistsError(f"refusing to overwrite stale partial shard {partial}")
    if target.exists():
        raise FileExistsError(f"refusing to overwrite existing shard {target}")
    table = schema  # keep strict-mypy from treating the optional import as a module
    pa, _ = _load_pyarrow()
    arrow_table = pa.Table.from_pylist(list(rows), schema=table)
    pq.write_table(arrow_table, partial, compression="zstd")
    with partial.open("rb") as handle:
        os.fsync(handle.fileno())
    os.rename(partial, target)
    _fsync_directory(target.parent)
    size, digest = sha256_file_with_size(target)
    min_slot, max_slot, min_timestamp, max_timestamp = _block_stats(rows)
    return ShardMetadata(
        path=relative_path,
        row_count=len(rows),
        byte_count=size,
        sha256=digest,
        schema_sha256=schema_hash,
        min_block_slot=min_slot,
        max_block_slot=max_slot,
        min_block_timestamp=min_timestamp,
        max_block_timestamp=max_timestamp,
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validated_page_stream(
    pages: Iterable[PageLike],
    provenance: QueryProvenance | None,
) -> Iterator[PageLike]:
    """Reject mixed, duplicated, or out-of-order query result pages."""

    missing = object()
    seen_job_id: str | None = None
    seen_total_rows: int | None = None
    seen_total_bytes: int | None = None
    for expected_number, page in enumerate(pages):
        page_number = _read_member(page, "page_number", missing)
        job_id = _read_member(page, "job_id", missing)
        total_rows = _read_member(page, "total_rows", missing)
        total_bytes = _read_member(page, "total_bytes_processed", missing)
        if provenance is not None and any(
            value is missing for value in (page_number, job_id, total_rows, total_bytes)
        ):
            raise ValueError("provenance-bound pages must expose query identity metadata")
        if page_number is not missing and (
            isinstance(page_number, bool)
            or not isinstance(page_number, int)
            or page_number != expected_number
        ):
            raise ValueError("BigQuery pages must be contiguous and start at zero")
        if job_id is not missing:
            if not isinstance(job_id, str) or not job_id:
                raise TypeError("BigQuery page job_id must be non-empty text")
            if seen_job_id is None:
                seen_job_id = job_id
            elif job_id != seen_job_id:
                raise ValueError("BigQuery page job_id changed during archival")
            if provenance is not None and job_id != provenance.job_id:
                raise ValueError("BigQuery page job_id does not match query provenance")
        if total_rows is not missing:
            if isinstance(total_rows, bool) or not isinstance(total_rows, int) or total_rows < 0:
                raise TypeError("BigQuery page total_rows must be a non-negative integer")
            if seen_total_rows is None:
                seen_total_rows = total_rows
            elif total_rows != seen_total_rows:
                raise ValueError("BigQuery page total_rows changed during archival")
            if provenance is not None and total_rows != provenance.total_rows:
                raise ValueError("BigQuery page total_rows does not match query provenance")
        if total_bytes is not missing:
            if isinstance(total_bytes, bool) or not isinstance(total_bytes, int) or total_bytes < 0:
                raise TypeError(
                    "BigQuery page total_bytes_processed must be a non-negative integer"
                )
            if seen_total_bytes is None:
                seen_total_bytes = total_bytes
            elif total_bytes != seen_total_bytes:
                raise ValueError("BigQuery page byte count changed during archival")
            if provenance is not None and total_bytes != provenance.actual_bytes_processed:
                raise ValueError("BigQuery page byte count does not match query provenance")
        yield page


def archive_bigquery_pages(
    pages: Iterable[PageLike],
    output_dir: str | Path,
    *,
    table: str = "table",
    schema: Mapping[str, object] | BigQuerySchema | None = None,
    max_rows_per_shard: int = 100_000,
    max_bytes_per_shard: int | None = 256 * 1024 * 1024,
    manifest_name: str = "manifest.json",
    provenance: QueryProvenance | None = None,
) -> ArchiveManifest:
    """Decode pages and atomically publish bounded zstd Parquet shards.

    ``max_rows_per_shard`` is a hard row bound.  ``max_bytes_per_shard`` is a
    target bound based on an in-memory Parquet candidate check near the limit;
    a single row whose encoded representation exceeds the target is published
    as one oversized shard because splitting a row would corrupt its value.
    """

    if max_rows_per_shard <= 0:
        raise ValueError("max_rows_per_shard must be positive")
    if max_bytes_per_shard is not None and max_bytes_per_shard <= 0:
        raise ValueError("max_bytes_per_shard must be positive when provided")
    if (
        not manifest_name
        or Path(manifest_name).is_absolute()
        or Path(manifest_name).name != manifest_name
    ):
        raise ValueError("manifest_name must be a safe relative basename")
    safe_table = _safe_table_name(table)
    root = Path(output_dir)
    root.parent.mkdir(parents=True, exist_ok=True)
    if root.exists() or root.is_symlink():
        raise FileExistsError(f"refusing to replace existing archive root {root}")
    staging = root.with_name(f".{root.name}.partial")
    if staging.exists() or staging.is_symlink():
        raise FileExistsError(f"refusing to replace stale staging archive {staging}")
    pages_iter = iter(_validated_page_stream(pages, provenance))
    first_page: PageLike | None = None
    if schema is None:
        try:
            first_page = next(pages_iter)
        except StopIteration as error:
            raise ValueError("at least one page or an explicit schema is required") from error
        parsed_schema = parse_bigquery_schema(page_schema(first_page))
    else:
        parsed_schema = parse_bigquery_schema(schema)
    arrow = arrow_schema(parsed_schema)

    def rows_stream() -> Iterator[dict[str, object]]:
        if first_page is not None:
            yield from decode_bigquery_page(first_page, parsed_schema)
        yield from decode_bigquery_pages(pages_iter, parsed_schema)

    staging.mkdir(mode=0o700)
    try:
        shards: list[ShardMetadata] = []
        pending: list[dict[str, object]] = []
        estimated = 0
        shard_index = 0

        def flush() -> None:
            nonlocal pending, estimated, shard_index
            if not pending:
                return
            relative = f"{safe_table}/part-{shard_index:06d}.parquet"
            shards.append(
                _write_shard(
                    pending,
                    arrow,
                    staging,
                    relative,
                    parsed_schema.schema_sha256,
                )
            )
            shard_index += 1
            pending = []
            estimated = 0

        for row in rows_stream():
            row_estimate = _estimate_value_bytes(row)
            if pending and (
                len(pending) >= max_rows_per_shard
                or (
                    max_bytes_per_shard is not None
                    and estimated + row_estimate > max_bytes_per_shard
                    and _parquet_candidate_size([*pending, row], arrow) > max_bytes_per_shard
                )
            ):
                flush()
            pending.append(row)
            estimated += row_estimate
            if len(pending) >= max_rows_per_shard:
                flush()
        flush()

        if provenance is not None and provenance.total_rows != sum(
            shard.row_count for shard in shards
        ):
            raise ValueError("decoded row count does not match BigQuery totalRows")
        manifest = ArchiveManifest(
            table=table,
            schema_sha256=parsed_schema.schema_sha256,
            shards=tuple(shards),
            manifest_path=manifest_name,
            provenance=provenance,
        )
        manifest_target = staging / manifest_name
        manifest_partial = Path(f"{manifest_target}.partial")
        payload = manifest_bytes(manifest)
        with manifest_partial.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.rename(manifest_partial, manifest_target)
        _fsync_directory(staging)
        _, manifest_hash = sha256_file_with_size(manifest_target)
        completed = ArchiveManifest(
            table=manifest.table,
            schema_sha256=manifest.schema_sha256,
            shards=manifest.shards,
            manifest_sha256=manifest_hash,
            manifest_path=manifest.manifest_path,
            provenance=manifest.provenance,
        )
        verify_archive_manifest(completed, staging)
        os.rename(staging, root)
        _fsync_directory(root.parent)
        return completed
    except BaseException:
        if staging.exists() and not staging.is_symlink():
            shutil.rmtree(staging)
        raise


# Concise aliases for callers that use ``archive_pages`` in application code.
archive_pages = archive_bigquery_pages


class ArchiveConflictError(RuntimeError):
    """Raised when an immutable object key already contains different bytes."""


@dataclass(frozen=True, slots=True)
class OVHS3Config:
    """Explicit OVH S3 connection inputs; no ambient credential lookup."""

    endpoint_url: str
    region: str
    bucket: str
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)

    def __post_init__(self) -> None:
        for name in ("endpoint_url", "region", "bucket", "access_key", "secret_key"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
            if value != value.strip() or "\n" in value or "\r" in value:
                raise ValueError(f"{name} cannot contain surrounding or line whitespace")
        parsed = urlparse(self.endpoint_url)
        expected_host = f"s3.{self.region.lower()}.io.cloud.ovh.net"
        if (
            parsed.scheme != "https"
            or parsed.hostname != expected_host
            or parsed.port is not None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.params
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("endpoint_url must be the HTTPS OVH S3 endpoint for region")
        if re.fullmatch(r"[A-Z0-9]+(?:-[A-Z0-9]+)*", self.region) is None:
            raise ValueError("region must be an uppercase OVH region identifier")
        if (
            re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", self.bucket) is None
            or ".." in self.bucket
        ):
            raise ValueError("bucket must be a valid DNS-style S3 bucket name")


@dataclass(frozen=True, slots=True)
class UploadResult:
    key: str
    byte_count: int
    sha256: str
    skipped: bool

    @property
    def bytes(self) -> int:
        return self.byte_count


def _not_found_error(error: BaseException) -> bool:
    code: object = None
    response = getattr(error, "response", None)
    if isinstance(response, Mapping):
        error_body = response.get("Error")
        if isinstance(error_body, Mapping):
            code = error_body.get("Code")
    if code is None:
        code = getattr(error, "code", None)
    return str(code) in {"404", "NoSuchKey", "NotFound", "NoSuchBucket"}


def _precondition_error(error: BaseException) -> bool:
    code: object = None
    response = getattr(error, "response", None)
    if isinstance(response, Mapping):
        error_body = response.get("Error")
        if isinstance(error_body, Mapping):
            code = error_body.get("Code")
        metadata = response.get("ResponseMetadata")
        if code is None and isinstance(metadata, Mapping):
            code = metadata.get("HTTPStatusCode")
    if code is None:
        code = getattr(error, "code", None)
    return str(code) in {"409", "412", "PreconditionFailed", "ConditionalRequestConflict"}


def _validate_s3_key(key: str) -> None:
    if (
        not key
        or key.startswith("/")
        or key.endswith("/")
        or "\\" in key
        or "//" in key
        or len(key.encode("utf-8")) > 1024
        or any(part in {"", ".", ".."} for part in key.split("/"))
    ):
        raise ValueError("S3 object key must be a canonical safe relative path")


class OVHS3Uploader:
    """Lazy boto3 uploader with immutable-key and full-stream verification."""

    def __init__(
        self,
        endpoint_url: str,
        region: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        *,
        client: Any | None = None,
    ) -> None:
        self.config = OVHS3Config(endpoint_url, region, bucket, access_key, secret_key)
        self._client = client
        self._provided_client = client is not None

    def _s3(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            import boto3
            from botocore.config import Config  # type: ignore[import-untyped]
        except ImportError as error:  # pragma: no cover - depends on optional package
            raise RuntimeError(
                "OVH S3 upload requires: pip install 'pumpquant[archive]'"
            ) from error
        self._client = boto3.session.Session().client(
            "s3",
            endpoint_url=self.config.endpoint_url,
            region_name=self.config.region,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            config=Config(
                connect_timeout=10,
                read_timeout=30,
                tcp_keepalive=True,
                max_pool_connections=32,
                retries={"max_attempts": 4, "mode": "adaptive"},
            ),
        )
        return self._client

    def _reset_transport(self) -> None:
        """Discard a failed botocore pool while preserving injected test clients."""

        if not self._provided_client:
            self._client = None

    def _range_head(self, key: str) -> Mapping[str, object] | None:
        """Synthesize HEAD metadata from a one-byte GET for OVH HEAD failures."""

        body = None
        try:
            response = self._s3().get_object(
                Bucket=self.config.bucket,
                Key=key,
                Range="bytes=0-0",
            )
        except Exception as error:
            if _not_found_error(error):
                return None
            raise
        try:
            if not isinstance(response, Mapping):
                raise RuntimeError("S3 range probe returned a non-object response")
            content_range = response.get("ContentRange")
            match = (
                re.fullmatch(r"bytes 0-0/([1-9][0-9]*)", content_range)
                if isinstance(content_range, str)
                else None
            )
            if match is None:
                raise RuntimeError("S3 range probe returned an invalid ContentRange")
            body = response.get("Body")
            if body is None or not hasattr(body, "read"):
                raise RuntimeError("S3 range probe returned no object byte")
            probe = body.read(2)
            if not isinstance(probe, bytes) or len(probe) != 1:
                raise RuntimeError("S3 range probe returned no object byte")
            result = dict(response)
            result["ContentLength"] = int(match.group(1))
            return result
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()

    def _head(self, key: str) -> Mapping[str, object] | None:
        try:
            response = self._s3().head_object(Bucket=self.config.bucket, Key=key)
        except Exception as error:
            if _not_found_error(error):
                return None
            # OVH can return a persistent generic 400 for HEAD while a ranged
            # GET of the same immutable version succeeds. The one-byte probe
            # carries ContentRange and x-amz-meta-sha256, so it preserves the
            # same size/hash precondition instead of weakening verification.
            return self._range_head(key)
        if not isinstance(response, Mapping):
            raise RuntimeError("S3 HEAD returned a non-object response")
        return cast(Mapping[str, object], response)

    def preflight(self, prefix: str) -> None:
        """Prove bucket region and prefix-list permission without writes."""

        normalized = prefix.strip("/")
        _validate_s3_key(f"{normalized}/probe" if normalized else "probe")
        client = self._s3()
        response = client.get_bucket_location(Bucket=self.config.bucket)
        if not isinstance(response, Mapping):
            raise RuntimeError("S3 bucket-location check returned a non-object response")
        location = response.get("LocationConstraint")
        if not isinstance(location, str) or location.upper() != self.config.region:
            raise RuntimeError("S3 bucket region does not match the configured endpoint")
        self._list_prefix_keys(normalized, max_keys=1)

    def _list_prefix_keys(self, prefix: str, *, max_keys: int = 1000) -> tuple[str, ...]:
        normalized = prefix.strip("/")
        request: dict[str, object] = {
            "Bucket": self.config.bucket,
            "Prefix": f"{normalized}/" if normalized else "",
            "MaxKeys": max_keys,
        }
        keys: list[str] = []
        seen_tokens: set[str] = set()
        while True:
            response = self._s3().list_objects_v2(**request)
            if not isinstance(response, Mapping):
                raise RuntimeError("S3 LIST returned a non-object response")
            contents = response.get("Contents", ())
            if not isinstance(contents, Sequence) or isinstance(contents, (str, bytes, bytearray)):
                raise RuntimeError("S3 LIST returned invalid contents")
            for item in contents:
                if not isinstance(item, Mapping) or not isinstance(item.get("Key"), str):
                    raise RuntimeError("S3 LIST returned an invalid object key")
                keys.append(cast(str, item["Key"]))
            if response.get("IsTruncated") is not True:
                return tuple(keys)
            token = response.get("NextContinuationToken")
            if not isinstance(token, str) or not token or token in seen_tokens:
                raise RuntimeError("S3 LIST returned an invalid continuation token")
            seen_tokens.add(token)
            request["ContinuationToken"] = token

    @staticmethod
    def _metadata_hash(response: Mapping[str, object]) -> str | None:
        metadata = response.get("Metadata")
        if not isinstance(metadata, Mapping):
            return None
        for key, value in metadata.items():
            if str(key).lower() == "sha256":
                return str(value).lower()
        return None

    def _verify_remote(self, key: str, expected_size: int, expected_hash: str) -> None:
        response = self._head(key)
        if response is None:
            raise RuntimeError(f"S3 object {key!r} disappeared after upload")
        content_length = response.get("ContentLength")
        try:
            actual_size = int(str(content_length))
        except (TypeError, ValueError) as error:
            raise RuntimeError("S3 HEAD returned invalid ContentLength") from error
        metadata_hash = self._metadata_hash(response)
        if actual_size != expected_size or metadata_hash != expected_hash:
            raise ArchiveConflictError(f"S3 object {key!r} differs from the local archive")
        # Verify bounded byte ranges so a transient StreamingBody interruption
        # retries only a small segment instead of restarting an 80+ MiB shard.
        # The final SHA-256 is still over the complete object in byte order.
        digest = hashlib.sha256()
        offset = 0
        while offset < expected_size:
            end = min(offset + _REMOTE_VERIFY_RANGE_BYTES, expected_size) - 1
            range_header = f"bytes={offset}-{end}"
            expected_range_size = end - offset + 1
            for attempt in range(_REMOTE_VERIFY_ATTEMPTS):
                body = None
                try:
                    body_response = self._s3().get_object(
                        Bucket=self.config.bucket,
                        Key=key,
                        Range=range_header,
                    )
                    if not isinstance(body_response, Mapping):
                        raise RuntimeError("S3 GET returned a non-object response")
                    content_range = body_response.get("ContentRange")
                    expected_content_range = f"bytes {offset}-{end}/{expected_size}"
                    if content_range != expected_content_range:
                        raise RuntimeError("S3 ranged GET returned an invalid ContentRange")
                    body = body_response.get("Body")
                    if body is None or not hasattr(body, "read"):
                        raise RuntimeError("S3 GET returned no readable body")
                    segment = bytearray()
                    while chunk := body.read(1024 * 1024):
                        if not isinstance(chunk, bytes):
                            raise RuntimeError("S3 GET returned a non-bytes chunk")
                        if len(segment) + len(chunk) > expected_range_size:
                            raise ArchiveConflictError(
                                f"S3 object {key!r} ranged body exceeds its requested bounds"
                            )
                        segment.extend(chunk)
                    if len(segment) != expected_range_size:
                        raise RuntimeError("S3 ranged GET returned an incomplete body")
                except ArchiveConflictError:
                    raise
                except Exception:
                    if attempt + 1 == _REMOTE_VERIFY_ATTEMPTS:
                        raise
                    time_module.sleep(min(2.0, 0.125 * (2**attempt)))
                    continue
                finally:
                    close = getattr(body, "close", None)
                    if callable(close):
                        close()
                digest.update(segment)
                offset = end + 1
                break
        if digest.hexdigest() != expected_hash:
            raise ArchiveConflictError(f"S3 object {key!r} body differs from the local archive")

    def upload_file(
        self,
        local_path: str | Path,
        key: str,
        *,
        expected_size: int | None = None,
        expected_sha256: str | None = None,
    ) -> UploadResult:
        """Upload one immutable object, or skip only an exact existing copy."""

        _validate_s3_key(key)
        path = Path(local_path)
        if path.is_symlink() or not path.is_file():
            raise FileNotFoundError(path)
        with path.open("rb") as source, tempfile.TemporaryFile() as snapshot:
            if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                raise FileNotFoundError(path)
            digest_builder = hashlib.sha256()
            size = 0
            while chunk := source.read(1024 * 1024):
                snapshot.write(chunk)
                digest_builder.update(chunk)
                size += len(chunk)
            snapshot.flush()
            os.fsync(snapshot.fileno())
            digest = digest_builder.hexdigest()
            if expected_size is not None and size != expected_size:
                raise ArchiveConflictError(f"local object {path.name!r} changed before upload")
            if expected_sha256 is not None and digest != expected_sha256:
                raise ArchiveConflictError(f"local object {path.name!r} changed before upload")
            for attempt in range(_REMOTE_UPLOAD_ATTEMPTS):
                snapshot.seek(0)
                try:
                    existing = self._head(key)
                    if existing is not None:
                        self._verify_remote(key, size, digest)
                        return UploadResult(key, size, digest, True)
                    self._s3().put_object(
                        Bucket=self.config.bucket,
                        Key=key,
                        Body=snapshot,
                        Metadata={"sha256": digest},
                        ContentLength=size,
                        IfNoneMatch="*",
                    )
                    self._verify_remote(key, size, digest)
                    return UploadResult(key, size, digest, False)
                except ArchiveConflictError:
                    raise
                except Exception as error:
                    if _precondition_error(error):
                        try:
                            self._verify_remote(key, size, digest)
                            return UploadResult(key, size, digest, True)
                        except ArchiveConflictError:
                            raise
                        except Exception:
                            pass
                    if attempt + 1 == _REMOTE_UPLOAD_ATTEMPTS:
                        raise
                    self._reset_transport()
                    time_module.sleep(min(2.0, 0.125 * (2**attempt)))
        raise RuntimeError("S3 upload retry loop terminated unexpectedly")

    def upload_manifest(
        self,
        manifest: ArchiveManifest,
        archive_root: str | Path,
        *,
        prefix: str = "",
        max_workers: int = 1,
    ) -> tuple[UploadResult, ...]:
        """Upload shards and then the manifest, always publishing it last."""

        if isinstance(max_workers, bool) or not 1 <= max_workers <= 32:
            raise ValueError("max_workers must be between 1 and 32")

        root = Path(archive_root)
        verify_archive_manifest(manifest, root)
        normalized_prefix = prefix.strip("/")
        if normalized_prefix:
            _validate_s3_key(f"{normalized_prefix}/probe")
        expected_keys = {
            *(
                f"{normalized_prefix}/{shard.path}" if normalized_prefix else shard.path
                for shard in manifest.shards
            ),
            (
                f"{normalized_prefix}/{manifest.manifest_path}"
                if normalized_prefix
                else manifest.manifest_path
            ),
        }
        remote_keys = set(self._list_prefix_keys(normalized_prefix))
        unexpected = remote_keys.difference(expected_keys)
        if unexpected:
            raise ArchiveConflictError("S3 archive prefix contains unmanifested objects")

        def upload_shard(shard: ShardMetadata) -> UploadResult:
            key = f"{normalized_prefix}/{shard.path}" if normalized_prefix else shard.path
            return thread_uploader().upload_file(
                root / shard.path,
                key,
                expected_size=shard.byte_count,
                expected_sha256=shard.sha256,
            )

        local = threading.local()

        def thread_uploader() -> OVHS3Uploader:
            worker = getattr(local, "uploader", None)
            if worker is None:
                worker = OVHS3Uploader(
                    self.config.endpoint_url,
                    self.config.region,
                    self.config.bucket,
                    self.config.access_key,
                    self.config.secret_key,
                    # Boto's low-level clients support concurrent requests;
                    # injected deterministic test clients are shared too.
                    client=self._client,
                )
                local.uploader = worker
            return cast(OVHS3Uploader, worker)

        if max_workers == 1:
            results = [upload_shard(shard) for shard in manifest.shards]
        else:
            with ThreadPoolExecutor(
                max_workers=min(max_workers, max(1, len(manifest.shards))),
                thread_name_prefix="pumpquant-s3",
            ) as executor:
                # Preserve manifest order, but cancel work that has not started
                # when one object fails. The manifest remains strictly last.
                futures: list[Future[UploadResult]] = [
                    executor.submit(upload_shard, shard) for shard in manifest.shards
                ]
                try:
                    results = [future.result() for future in futures]
                except BaseException:
                    for future in futures:
                        future.cancel()
                    raise
        manifest_key = (
            f"{normalized_prefix}/{manifest.manifest_path}"
            if normalized_prefix
            else manifest.manifest_path
        )
        results.append(
            self.upload_file(
                root / manifest.manifest_path,
                manifest_key,
                expected_size=len(manifest_bytes(manifest)),
                expected_sha256=manifest.manifest_sha256,
            )
        )
        if set(self._list_prefix_keys(normalized_prefix)) != expected_keys:
            raise ArchiveConflictError("S3 archive prefix changed during publication")
        return tuple(results)

    def verify_remote_manifest(
        self,
        manifest: ArchiveManifest,
        *,
        prefix: str = "",
    ) -> tuple[UploadResult, ...]:
        """Fully verify a published archive without requiring local shard files."""

        if not manifest.manifest_sha256:
            raise ValueError("manifest must carry its canonical SHA-256")
        normalized_prefix = prefix.strip("/")
        if normalized_prefix:
            _validate_s3_key(f"{normalized_prefix}/probe")
        expected: list[tuple[str, int, str]] = [
            (
                f"{normalized_prefix}/{shard.path}" if normalized_prefix else shard.path,
                shard.byte_count,
                shard.sha256,
            )
            for shard in manifest.shards
        ]
        manifest_payload = manifest_bytes(manifest)
        expected.append(
            (
                (
                    f"{normalized_prefix}/{manifest.manifest_path}"
                    if normalized_prefix
                    else manifest.manifest_path
                ),
                len(manifest_payload),
                manifest.manifest_sha256,
            )
        )
        if set(self._list_prefix_keys(normalized_prefix)) != {key for key, _, _ in expected}:
            raise ArchiveConflictError("S3 archive prefix does not match its manifest")
        results: list[UploadResult] = []
        for key, size, digest in expected:
            self._verify_remote(key, size, digest)
            results.append(UploadResult(key, size, digest, True))
        if set(self._list_prefix_keys(normalized_prefix)) != {key for key, _, _ in expected}:
            raise ArchiveConflictError("S3 archive prefix changed during verification")
        return tuple(results)

    def fetch_manifest(
        self,
        *,
        prefix: str = "",
        manifest_name: str = "manifest.json",
        maximum_bytes: int = 64 * 1024 * 1024,
    ) -> ArchiveManifest | None:
        """Fetch and validate the canonical manifest, or prove the prefix is empty."""

        if (
            not manifest_name
            or Path(manifest_name).is_absolute()
            or Path(manifest_name).name != manifest_name
        ):
            raise ValueError("manifest_name must be a safe relative basename")
        if maximum_bytes <= 0:
            raise ValueError("maximum_bytes must be positive")
        normalized_prefix = prefix.strip("/")
        if normalized_prefix:
            _validate_s3_key(f"{normalized_prefix}/probe")
        keys = set(self._list_prefix_keys(normalized_prefix))
        if not keys:
            return None
        key = f"{normalized_prefix}/{manifest_name}" if normalized_prefix else manifest_name
        if key not in keys:
            raise ArchiveConflictError("S3 archive prefix contains objects but no manifest")
        response = self._s3().get_object(Bucket=self.config.bucket, Key=key)
        if not isinstance(response, Mapping):
            raise RuntimeError("S3 GET returned a non-object response")
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise RuntimeError("S3 GET returned no readable body")
        raw = bytearray()
        try:
            while chunk := body.read(1024 * 1024):
                if not isinstance(chunk, bytes):
                    raise RuntimeError("S3 GET returned a non-bytes chunk")
                raw.extend(chunk)
                if len(raw) > maximum_bytes:
                    raise ArchiveConflictError("S3 archive manifest exceeds its size limit")
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()
        digest = hashlib.sha256(raw).hexdigest()
        metadata_digest = self._metadata_hash(response)
        content_length = response.get("ContentLength")
        try:
            expected_length = int(str(content_length))
        except (TypeError, ValueError) as error:
            raise RuntimeError("S3 GET returned invalid ContentLength") from error
        if expected_length != len(raw) or metadata_digest != digest:
            raise ArchiveConflictError("S3 archive manifest metadata differs from its body")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / manifest_name
            with path.open("xb") as handle:
                handle.write(raw)
            manifest = load_archive_manifest(path)
        if manifest.manifest_sha256 != digest:
            raise ArchiveConflictError("S3 archive manifest failed canonical hash verification")
        return manifest

    # A concise method name is useful for callers treating a manifest as the
    # unit of work; it still guarantees manifest-last ordering.
    upload = upload_manifest


S3Uploader = OVHS3Uploader


__all__ = [
    "ArchiveConflictError",
    "ArchiveManifest",
    "BigQueryArchiveManifest",
    "BigQueryField",
    "BigQuerySchema",
    "BigQueryShardMetadata",
    "OVHS3Config",
    "OVHS3Uploader",
    "QueryProvenance",
    "S3Uploader",
    "ShardMetadata",
    "UploadResult",
    "archive_bigquery_pages",
    "archive_pages",
    "arrow_schema",
    "bigquery_arrow_schema",
    "decode_bigquery_page",
    "decode_bigquery_pages",
    "decode_bigquery_row",
    "load_archive_manifest",
    "manifest_bytes",
    "page_schema",
    "parse_bigquery_schema",
    "schema_sha256",
    "verify_archive_manifest",
]
