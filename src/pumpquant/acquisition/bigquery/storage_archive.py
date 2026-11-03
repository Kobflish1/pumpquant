"""Read an already-completed BigQuery query job through the Storage Read API.

This module is intentionally separate from :mod:`pumpquant.bigquery_archive`'s
REST page reader.  It accepts an existing ``DONE`` query job, validates its
anonymous destination with an idempotent ``jobs.get`` call, creates exactly one
Storage Read API snapshot, and writes the returned Arrow batches to local,
atomic zstd Parquet shards.  It never submits a query and never uploads an
object.

The Google clients are optional and are loaded only when a caller does not
inject clients.  Tests and remote workers can therefore pass small fakes for
both clients without ADC or a network connection.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast

from pumpquant.acquisition.bigquery.archive import (
    ArchiveManifest,
    BigQuerySchema,
    QueryProvenance,
    ShardMetadata,
    _fsync_directory,
    _load_pyarrow,
    arrow_schema,
    manifest_bytes,
    parse_bigquery_schema,
    verify_archive_manifest,
)
from pumpquant.core.hashing import sha256_file_with_size

_HEX64 = re.compile(r"^[0-9a-f]{64}$")

_JOB_ID = re.compile(r"^[A-Za-z0-9_-]{1,1024}$")
_MAX_STREAMS = 128
_DEFAULT_MAX_STREAMS = 32
_DEFAULT_MAX_ROWS_PER_SHARD = 100_000
_DEFAULT_MAX_BYTES_PER_SHARD = 256 * 1024 * 1024
_MISSING = object()


class BigQueryStorageArchiveError(RuntimeError):
    """Raised when a completed job cannot be safely archived."""


class _Client(Protocol):
    def get_job(self, job_id: str, *, location: str) -> Any: ...

    def get_table(self, table: str) -> Any: ...


@dataclass(frozen=True, slots=True)
class AnonymousDestination:
    """Validated query-result destination returned by ``jobs.get``."""

    project: str
    dataset: str
    table: str

    @property
    def full_name(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"

    @property
    def storage_resource(self) -> str:
        return f"projects/{self.project}/datasets/{self.dataset}/tables/{self.table}"


@dataclass(frozen=True, slots=True)
class CompletedQueryJob:
    """Small, transport-independent snapshot of validated job metadata."""

    job_id: str
    billing_project: str
    location: str
    destination: AnonymousDestination
    schema: BigQuerySchema
    total_rows: int
    total_bytes_processed: int
    destination_etag: str | None = None


@dataclass(frozen=True, slots=True)
class _StreamArchive:
    stream_name: str
    shards: tuple[ShardMetadata, ...]
    row_count: int


def _member(value: object, name: str, default: object = _MISSING) -> object:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _mapping(value: object, description: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise BigQueryStorageArchiveError(f"{description} must be an object")
    return cast(Mapping[str, object], value)


def _nonempty_text(value: object, description: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BigQueryStorageArchiveError(f"{description} must be non-empty text")
    return value.strip()


def _nonnegative_int(value: object, description: str) -> int:
    if isinstance(value, bool):
        raise BigQueryStorageArchiveError(f"{description} must be a non-negative integer")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdigit():
        parsed = int(value)
    else:
        raise BigQueryStorageArchiveError(f"{description} must be a non-negative integer")
    if parsed < 0:
        raise BigQueryStorageArchiveError(f"{description} must be a non-negative integer")
    return parsed


def _field_mapping(field: object) -> dict[str, object]:
    """Convert REST fields and google-cloud SchemaField objects alike."""

    if isinstance(field, Mapping):
        name = field.get("name")
        type_name = field.get("type", field.get("field_type"))
        mode = field.get("mode", "NULLABLE")
        children = field.get("fields", ())
    else:
        name = getattr(field, "name", None)
        type_name = getattr(field, "field_type", getattr(field, "type", None))
        mode = getattr(field, "mode", "NULLABLE")
        children = getattr(field, "fields", ())
    if not isinstance(children, Sequence) or isinstance(children, (str, bytes, bytearray)):
        raise BigQueryStorageArchiveError("BigQuery schema nested fields must be a sequence")
    return {
        "name": name,
        "type": type_name,
        "mode": mode,
        "fields": [_field_mapping(child) for child in children],
    }


def _schema_mapping(raw: object) -> Mapping[str, object]:
    if isinstance(raw, BigQuerySchema):
        return {"fields": [_field_mapping(field) for field in raw.fields]}
    if isinstance(raw, Mapping):
        fields = raw.get("fields")
        if fields is None:
            raise BigQueryStorageArchiveError("job schema has no fields")
        if not isinstance(fields, Sequence) or isinstance(fields, (str, bytes, bytearray)):
            raise BigQueryStorageArchiveError("job schema fields must be a sequence")
        return {"fields": [_field_mapping(field) for field in fields]}
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        return {"fields": [_field_mapping(field) for field in raw]}
    raise BigQueryStorageArchiveError("completed query job has no usable schema")


def _nested(value: object, *names: str) -> object:
    current = value
    for name in names:
        current = _member(current, name)
        if current is _MISSING or current is None:
            return _MISSING
    return current


def _job_destination(job: object, billing_project: str) -> AnonymousDestination:
    raw = _member(job, "destination")
    if raw is _MISSING or raw is None:
        raw = _nested(job, "configuration", "query", "destination")
    if raw is _MISSING or raw is None:
        raise BigQueryStorageArchiveError("completed query job has no destination table")
    project: object
    dataset: object
    table: object
    if isinstance(raw, str):
        parts = raw.split(".")
        if len(parts) != 3:
            raise BigQueryStorageArchiveError("destination table has an invalid resource name")
        project, dataset, table = parts
    else:
        project = _member(raw, "project", _MISSING)
        if project is _MISSING:
            project = _member(raw, "project_id", _MISSING)
        if project is _MISSING:
            project = _member(raw, "projectId", _MISSING)
        dataset = _member(raw, "dataset_id", _MISSING)
        if dataset is _MISSING:
            dataset = _member(raw, "datasetId", _MISSING)
        table = _member(raw, "table_id", _MISSING)
        if table is _MISSING:
            table = _member(raw, "tableId", _MISSING)
    project_text = _nonempty_text(project, "destination project")
    dataset_text = _nonempty_text(dataset, "destination dataset")
    table_text = _nonempty_text(table, "destination table")
    if project_text != billing_project:
        raise BigQueryStorageArchiveError("job destination project differs from billing project")
    # BigQuery anonymous query-result datasets are hidden datasets whose IDs
    # begin with an underscore.  Requiring this marker prevents accidentally
    # reading a user-owned destination table.
    if not dataset_text.startswith("_"):
        raise BigQueryStorageArchiveError("job destination is not an anonymous table")
    for part, label in (
        (project_text, "destination project"),
        (dataset_text, "destination dataset"),
        (table_text, "destination table"),
    ):
        if any(char in part for char in ("/", "\\", "\n", "\r")):
            raise BigQueryStorageArchiveError(f"{label} contains unsafe characters")
    return AnonymousDestination(project_text, dataset_text, table_text)


def _job_status(job: object) -> tuple[str, object]:
    status = _member(job, "status")
    if status is _MISSING or status is None:
        state = _member(job, "state")
        error = _member(job, "error_result", _MISSING)
        if error is _MISSING:
            error = _member(job, "errorResult", None)
        return ("" if state is _MISSING else str(state).upper(), error)
    state = _member(status, "state", _MISSING)
    error = _member(status, "errorResult", _MISSING)
    if error is _MISSING:
        error = _member(status, "error_result", None)
    return ("" if state is _MISSING else str(state).upper(), error)


def _job_schema_optional(job: object) -> BigQuerySchema | None:
    raw = _member(job, "schema")
    if raw is _MISSING or raw is None:
        raw = _nested(job, "statistics", "query", "schema")
    if raw is _MISSING or raw is None:
        return None
    try:
        return parse_bigquery_schema(_schema_mapping(raw))
    except (TypeError, ValueError, BigQueryStorageArchiveError) as error:
        raise BigQueryStorageArchiveError(
            "completed query job has an invalid result schema"
        ) from error


def _job_schema(job: object) -> BigQuerySchema:
    schema = _job_schema_optional(job)
    if schema is None:
        raise BigQueryStorageArchiveError("completed query job has no result schema")
    return schema


def _table_reference(table: object) -> object:
    reference = _member(table, "table_reference", _MISSING)
    if reference is _MISSING:
        reference = _member(table, "tableReference", _MISSING)
    return table if reference is _MISSING else reference


def _validate_table_identity(table: object, destination: AnonymousDestination) -> None:
    reference = _table_reference(table)
    project = _member(reference, "project", _MISSING)
    if project is _MISSING:
        project = _member(reference, "project_id", _MISSING)
    if project is _MISSING:
        project = _member(reference, "projectId", _MISSING)
    dataset = _member(reference, "dataset_id", _MISSING)
    if dataset is _MISSING:
        dataset = _member(reference, "datasetId", _MISSING)
    table_id = _member(reference, "table_id", _MISSING)
    if table_id is _MISSING:
        table_id = _member(reference, "tableId", _MISSING)
    if project is not _MISSING and _nonempty_text(project, "table project") != destination.project:
        raise BigQueryStorageArchiveError("destination table metadata changed project")
    if dataset is not _MISSING and _nonempty_text(dataset, "table dataset") != destination.dataset:
        raise BigQueryStorageArchiveError("destination table metadata changed dataset")
    if table_id is not _MISSING and _nonempty_text(table_id, "table id") != destination.table:
        raise BigQueryStorageArchiveError("destination table metadata changed table id")


def _table_schema(table: object) -> BigQuerySchema | None:
    raw = _member(table, "schema", _MISSING)
    if raw is _MISSING or raw is None:
        return None
    try:
        return parse_bigquery_schema(_schema_mapping(raw))
    except (TypeError, ValueError, BigQueryStorageArchiveError) as error:
        raise BigQueryStorageArchiveError("destination table has an invalid schema") from error


def _table_rows(table: object) -> int | None:
    raw = _member(table, "num_rows", _MISSING)
    if raw is _MISSING:
        raw = _member(table, "numRows", _MISSING)
    if raw is _MISSING or raw is None:
        return None
    return _nonnegative_int(raw, "destination table num_rows")


def _table_etag(table: object) -> str | None:
    raw = _member(table, "etag", _MISSING)
    if raw is _MISSING or raw is None:
        return None
    return _nonempty_text(raw, "destination table etag")


def _job_stat(job: object, *paths: tuple[str, ...]) -> object:
    for path in paths:
        value = _nested(job, *path)
        if value is not _MISSING and value is not None:
            return value
    return _MISSING


def _job_identity(job: object, job_id: str, billing_project: str, location: str) -> None:
    raw_id = _member(job, "job_id", _MISSING)
    if raw_id is _MISSING:
        raw_id = _nested(job, "jobReference", "jobId")
    if raw_id is not _MISSING and _nonempty_text(raw_id, "job id") != job_id:
        raise BigQueryStorageArchiveError("jobs.get returned a different job")
    raw_project = _member(job, "project", _MISSING)
    if raw_project is _MISSING:
        raw_project = _nested(job, "jobReference", "projectId")
    if (
        raw_project is not _MISSING
        and _nonempty_text(raw_project, "job project") != billing_project
    ):
        raise BigQueryStorageArchiveError("jobs.get returned a different billing project")
    raw_location = _member(job, "location", _MISSING)
    if raw_location is _MISSING:
        raw_location = _nested(job, "jobReference", "location")
    if (
        raw_location is not _MISSING
        and raw_location is not None
        and (_nonempty_text(raw_location, "job location") != location)
    ):
        raise BigQueryStorageArchiveError("jobs.get returned a different location")


def resolve_completed_query_job(
    bigquery_client: _Client,
    *,
    billing_project: str,
    job_id: str,
    location: str,
) -> CompletedQueryJob:
    """Resolve and validate one existing ``DONE`` query job via ``jobs.get``.

    The official ``Client.get_job`` method is a GET-only wrapper around the
    BigQuery ``jobs.get`` endpoint.  No query endpoint is touched here.
    """

    if not isinstance(billing_project, str) or not billing_project.strip():
        raise ValueError("billing_project must be non-empty")
    if not isinstance(location, str) or not location.strip():
        raise ValueError("location must be non-empty")
    if not isinstance(job_id, str) or _JOB_ID.fullmatch(job_id) is None:
        raise ValueError("job_id has an invalid format")
    getter = getattr(bigquery_client, "get_job", None)
    if not callable(getter):
        raise TypeError("bigquery_client must expose get_job(job_id, location=...)")
    try:
        job = getter(job_id, location=location)
    except Exception as error:
        raise BigQueryStorageArchiveError("BigQuery jobs.get failed") from error
    if job is None:
        raise BigQueryStorageArchiveError("BigQuery jobs.get returned no job")
    _job_identity(job, job_id, billing_project, location)
    state, error_result = _job_status(job)
    if state != "DONE":
        raise BigQueryStorageArchiveError("BigQuery job is not DONE")
    if error_result not in (_MISSING, None, {}):
        raise BigQueryStorageArchiveError("BigQuery job failed")
    query_config = _nested(job, "configuration", "query")
    if query_config is _MISSING:
        job_type = _member(job, "job_type", _MISSING)
        if job_type is _MISSING or str(job_type).lower() != "query":
            raise BigQueryStorageArchiveError("BigQuery job is not a query job")
    elif query_config is None or not isinstance(query_config, Mapping):
        raise BigQueryStorageArchiveError("BigQuery job is not a query job")
    destination = _job_destination(job, billing_project)
    table_getter = getattr(bigquery_client, "get_table", None)
    table_metadata: object | None = None
    if callable(table_getter):
        try:
            table_metadata = table_getter(destination.full_name)
        except Exception as error:
            raise BigQueryStorageArchiveError("BigQuery destination tables.get failed") from error
        if table_metadata is None:
            raise BigQueryStorageArchiveError("BigQuery destination tables.get returned no table")
        _validate_table_identity(table_metadata, destination)
    table_schema = None if table_metadata is None else _table_schema(table_metadata)
    job_schema = _job_schema_optional(job)
    if (
        table_schema is not None
        and job_schema is not None
        and table_schema.schema_sha256 != job_schema.schema_sha256
    ):
        raise BigQueryStorageArchiveError(
            "destination table schema differs from jobs.get result schema"
        )
    schema = table_schema if table_schema is not None else job_schema
    if schema is None:  # pragma: no cover - guarded by _job_schema/table fallback
        raise BigQueryStorageArchiveError("completed query job has no result schema")
    total_rows_raw = _job_stat(
        job,
        ("total_rows",),
        ("statistics", "query", "totalRows"),
        ("statistics", "query", "total_rows"),
    )
    table_rows = None if table_metadata is None else _table_rows(table_metadata)
    if (
        table_rows is not None
        and total_rows_raw is not _MISSING
        and table_rows != _nonnegative_int(total_rows_raw, "job totalRows")
    ):
        raise BigQueryStorageArchiveError(
            "destination table num_rows differs from jobs.get totalRows"
        )
    if table_rows is not None:
        total_rows_raw = table_rows
    total_bytes_raw = _job_stat(
        job,
        ("total_bytes_processed",),
        ("statistics", "query", "totalBytesProcessed"),
        ("statistics", "totalBytesProcessed"),
    )
    if total_rows_raw is _MISSING:
        raise BigQueryStorageArchiveError("BigQuery job has no totalRows")
    if total_bytes_raw is _MISSING:
        raise BigQueryStorageArchiveError("BigQuery job has no totalBytesProcessed")
    return CompletedQueryJob(
        job_id=job_id,
        billing_project=billing_project,
        location=location,
        destination=destination,
        schema=schema,
        total_rows=_nonnegative_int(total_rows_raw, "job totalRows"),
        total_bytes_processed=_nonnegative_int(total_bytes_raw, "job totalBytesProcessed"),
        destination_etag=(None if table_metadata is None else _table_etag(table_metadata)),
    )


def _load_clients(billing_project: str) -> tuple[Any, Any]:
    try:
        import google.cloud.bigquery as bigquery
        import google.cloud.bigquery_storage_v1 as bigquery_storage_v1
    except ImportError as error:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "Storage Read archival requires the archive or bigquery optional extra"
        ) from error
    return bigquery.Client(project=billing_project), bigquery_storage_v1.BigQueryReadClient()


def _read_session_request(table_resource: str) -> Any:
    try:
        from google.cloud import bigquery_storage_v1

        types = bigquery_storage_v1.types
        options_type = types.ArrowSerializationOptions
        codec_type = getattr(options_type, "CompressionCodec", None)
        zstd = getattr(codec_type, "ZSTD", 2)
        arrow_options = options_type(buffer_compression=zstd)
        read_options = types.ReadSession.TableReadOptions(arrow_serialization_options=arrow_options)
        return types.ReadSession(
            table=table_resource,
            data_format=types.DataFormat.ARROW,
            read_options=read_options,
        )
    except ImportError:  # pragma: no cover - tests inject a fake client
        return {
            "table": table_resource,
            "data_format": "ARROW",
            "read_options": {
                "arrow_serialization_options": {"buffer_compression": 2, "codec": "ZSTD"}
            },
        }


def _session_streams(session: object) -> tuple[str, ...]:
    raw = _member(session, "streams", ())
    if raw is None:
        raw = ()
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raise BigQueryStorageArchiveError("Storage Read session returned invalid streams")
    names: list[str] = []
    for stream in raw:
        name = _member(stream, "name", _MISSING)
        if name is _MISSING and isinstance(stream, str):
            name = stream
        names.append(_nonempty_text(name, "Storage Read stream name"))
    if len(names) != len(set(names)):
        raise BigQueryStorageArchiveError("Storage Read session returned duplicate streams")
    return tuple(names)


def _as_arrow_table(value: object, pa: Any, session: object) -> Any:
    if isinstance(value, pa.Table):
        return value
    if isinstance(value, pa.RecordBatch):
        return pa.Table.from_batches([value])
    converter = getattr(value, "to_arrow", None)
    if callable(converter):
        converted = converter()
        if isinstance(converted, pa.Table):
            return converted
        if isinstance(converted, pa.RecordBatch):
            return pa.Table.from_batches([converted])
    converter = getattr(value, "to_table", None)
    if callable(converter):
        converted = converter()
        if isinstance(converted, pa.Table):
            return converted
        if isinstance(converted, pa.RecordBatch):
            return pa.Table.from_batches([converted])
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            reader = pa.ipc.open_stream(pa.py_buffer(value))
            return reader.read_all()
        except Exception as error:
            raise BigQueryStorageArchiveError(
                "Storage Read returned invalid Arrow bytes"
            ) from error
    # A few deterministic fakes expose a table under a mapping key.
    if isinstance(value, Mapping):
        nested = value.get("table", value.get("arrow_table", _MISSING))
        if nested is not _MISSING:
            return _as_arrow_table(nested, pa, session)
    del session
    raise BigQueryStorageArchiveError("Storage Read stream returned a non-Arrow batch")


def _iter_reader_tables(reader: object, session: object, pa: Any) -> Iterable[Any]:
    rows_method = getattr(reader, "rows", None)
    source: object
    if callable(rows_method):
        try:
            source = rows_method(session)
        except TypeError:
            source = rows_method()
    else:
        source = reader
    pages = getattr(source, "pages", _MISSING)
    if pages is not _MISSING:
        source = pages() if callable(pages) else pages
    try:
        iterator = iter(cast(Iterable[Any], source))
    except TypeError as error:
        raise BigQueryStorageArchiveError("Storage Read stream is not iterable") from error
    for item in iterator:
        yield _as_arrow_table(item, pa, session)


def _cast_table(table: Any, target_schema: Any, pa: Any) -> Any:
    if not isinstance(table, pa.Table):
        raise BigQueryStorageArchiveError("Storage Read returned an invalid Arrow table")
    if tuple(table.schema.names) != tuple(target_schema.names):
        raise BigQueryStorageArchiveError("Storage Read Arrow fields differ from the job schema")
    if table.schema != target_schema:
        try:
            table = table.cast(target_schema, safe=False)
        except (TypeError, ValueError, pa.ArrowException) as error:
            raise BigQueryStorageArchiveError(
                "Storage Read Arrow types differ from the job schema"
            ) from error
    return table


def _arrow_block_stats(
    table: Any, pa: Any
) -> tuple[int | None, int | None, datetime | None, datetime | None]:
    def values(name: str) -> list[Any]:
        if name not in table.schema.names:
            return []
        return [item for item in table.column(name).to_pylist() if item is not None]

    slots = values("block_slot")
    timestamps = values("block_timestamp")
    if any(isinstance(item, bool) or not isinstance(item, int) for item in slots):
        raise BigQueryStorageArchiveError("block_slot must be an integer Arrow field")
    if any(not isinstance(item, datetime) for item in timestamps):
        raise BigQueryStorageArchiveError("block_timestamp must be a timestamp Arrow field")
    if any(item < 0 for item in slots):
        raise BigQueryStorageArchiveError("block_slot cannot be negative")
    normalized_timestamps = [
        item.astimezone(UTC) if item.tzinfo is not None else item.replace(tzinfo=UTC)
        for item in timestamps
    ]
    del pa
    return (
        min(slots, default=None),
        max(slots, default=None),
        min(normalized_timestamps, default=None),
        max(normalized_timestamps, default=None),
    )


def _write_arrow_shard(
    table: Any,
    *,
    staging: Path,
    relative_path: str,
    schema_hash: str,
    pa: Any,
    pq: Any,
) -> ShardMetadata:
    if table.num_rows <= 0:
        raise ValueError("cannot write an empty Arrow shard")
    target = staging / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = Path(f"{target}.partial")
    if target.exists() or partial.exists():
        raise BigQueryStorageArchiveError(f"refusing to overwrite shard {relative_path!r}")
    pq.write_table(table, partial, compression="zstd")
    with partial.open("rb") as handle:
        os.fsync(handle.fileno())
    os.rename(partial, target)
    _fsync_directory(target.parent)
    size, digest = sha256_file_with_size(target)
    min_slot, max_slot, min_timestamp, max_timestamp = _arrow_block_stats(table, pa)
    return ShardMetadata(
        path=relative_path,
        row_count=int(table.num_rows),
        byte_count=size,
        sha256=digest,
        schema_sha256=schema_hash,
        min_block_slot=min_slot,
        max_block_slot=max_slot,
        min_block_timestamp=min_timestamp,
        max_block_timestamp=max_timestamp,
    )


def _piece_rows(table: Any, max_bytes: int | None, max_rows: int) -> int:
    rows = min(int(table.num_rows), max_rows)
    if max_bytes is None or rows <= 1:
        return rows
    if int(table.slice(0, rows).nbytes) <= max_bytes:
        return rows
    low, high = 1, rows
    while low < high:
        middle = (low + high + 1) // 2
        if int(table.slice(0, middle).nbytes) <= max_bytes:
            low = middle
        else:
            high = middle - 1
    # A single oversized row is retained as one shard; splitting a row is not
    # possible and silently dropping it would violate the totalRows proof.
    return max(1, low)


def _archive_stream(
    stream_name: str,
    *,
    storage_client: Any,
    session: object,
    target_schema: Any,
    schema_hash: str,
    staging: Path,
    stream_index: int,
    max_rows_per_shard: int,
    max_bytes_per_shard: int | None,
) -> _StreamArchive:
    pa, pq = _load_pyarrow()
    reader = storage_client.read_rows(stream_name)
    pending_tables: list[Any] = []
    pending_rows = 0
    pending_bytes = 0
    shards: list[ShardMetadata] = []
    shard_index = 0
    row_count = 0

    def flush_one(table: Any) -> Any:
        nonlocal shard_index, row_count
        take = _piece_rows(table, max_bytes_per_shard, max_rows_per_shard)
        piece = table.slice(0, take)
        relative = f"stream-{stream_index:05d}/part-{shard_index:06d}.parquet"
        shards.append(
            _write_arrow_shard(
                piece,
                staging=staging,
                relative_path=relative,
                schema_hash=schema_hash,
                pa=pa,
                pq=pq,
            )
        )
        shard_index += 1
        row_count += int(piece.num_rows)
        return table.slice(take)

    def concat_pending() -> Any:
        if not pending_tables:
            raise AssertionError("cannot concatenate an empty pending table list")
        if len(pending_tables) == 1:
            return pending_tables[0]
        try:
            return pa.concat_tables(pending_tables, promote_options="none")
        except TypeError:  # pyarrow < 14 compatibility for injected envs
            return pa.concat_tables(pending_tables)

    for raw_table in _iter_reader_tables(reader, session, pa):
        table = _cast_table(raw_table, target_schema, pa)
        if table.num_rows == 0:
            continue
        pending_tables.append(table)
        pending_rows += int(table.num_rows)
        pending_bytes += int(table.nbytes)
        while pending_tables and (
            pending_rows >= max_rows_per_shard
            or (max_bytes_per_shard is not None and pending_bytes >= max_bytes_per_shard)
        ):
            pending = flush_one(concat_pending())
            pending_tables = [] if pending.num_rows == 0 else [pending]
            pending_rows = int(pending.num_rows)
            pending_bytes = int(pending.nbytes)
    while pending_tables:
        pending = flush_one(concat_pending())
        pending_tables = [] if pending.num_rows == 0 else [pending]
        pending_rows = int(pending.num_rows)
        pending_bytes = int(pending.nbytes)
    # Reaching this return means the reader iterator was exhausted.  A stream
    # whose reader raised before here is not considered completed.
    return _StreamArchive(stream_name, tuple(shards), row_count)


def _validate_provenance(
    provenance: QueryProvenance,
    completed: CompletedQueryJob,
) -> None:
    if not isinstance(provenance, QueryProvenance):
        raise TypeError("provenance must be a QueryProvenance")
    if (
        provenance.job_id != completed.job_id
        or provenance.billing_project != completed.billing_project
        or provenance.location != completed.location
    ):
        raise BigQueryStorageArchiveError("query provenance does not match jobs.get metadata")
    if provenance.total_rows != completed.total_rows:
        raise BigQueryStorageArchiveError("query provenance total_rows differs from jobs.get")
    if provenance.actual_bytes_processed != completed.total_bytes_processed:
        raise BigQueryStorageArchiveError(
            "query provenance actual_bytes_processed differs from jobs.get"
        )


def archive_completed_query_job(
    *,
    billing_project: str,
    job_id: str,
    location: str,
    output_dir: str | Path,
    provenance: QueryProvenance,
    max_streams: int = _DEFAULT_MAX_STREAMS,
    max_rows_per_shard: int = _DEFAULT_MAX_ROWS_PER_SHARD,
    max_bytes_per_shard: int | None = _DEFAULT_MAX_BYTES_PER_SHARD,
    table: str | None = None,
    expected_schema_sha256: str | None = None,
    bigquery_client: _Client | None = None,
    storage_client: Any | None = None,
) -> ArchiveManifest:
    """Archive one existing completed query job via one Storage Read snapshot.

    The function is deliberately fail-closed: an existing output root or stale
    staging directory is never resumed or overwritten.  Any failure after
    staging begins removes the incomplete staging tree, leaving no manifest
    that could be mistaken for a complete archive.
    """

    if isinstance(max_streams, bool) or not 1 <= max_streams <= _MAX_STREAMS:
        raise ValueError(f"max_streams must be between 1 and {_MAX_STREAMS}")
    if isinstance(max_rows_per_shard, bool) or max_rows_per_shard <= 0:
        raise ValueError("max_rows_per_shard must be positive")
    if max_bytes_per_shard is not None and (
        isinstance(max_bytes_per_shard, bool) or max_bytes_per_shard <= 0
    ):
        raise ValueError("max_bytes_per_shard must be positive when provided")
    if bigquery_client is None or storage_client is None:
        loaded_bq, loaded_storage = _load_clients(billing_project)
        if bigquery_client is None:
            bigquery_client = loaded_bq
        if storage_client is None:
            storage_client = loaded_storage
    if bigquery_client is None or storage_client is None:  # pragma: no cover - defensive
        raise AssertionError("clients were not initialized")
    completed = resolve_completed_query_job(
        bigquery_client,
        billing_project=billing_project,
        job_id=job_id,
        location=location,
    )
    if expected_schema_sha256 is not None:
        if not _HEX64.fullmatch(expected_schema_sha256):
            raise ValueError("expected_schema_sha256 must be lowercase SHA-256 hex")
        if completed.schema.schema_sha256 != expected_schema_sha256:
            raise BigQueryStorageArchiveError(
                "jobs.get result schema differs from expected schema hash"
            )
    _validate_provenance(provenance, completed)
    safe_table = (
        completed.destination.full_name if table is None else _nonempty_text(table, "table")
    )
    root = Path(output_dir)
    root.parent.mkdir(parents=True, exist_ok=True)
    if root.exists() or root.is_symlink():
        raise FileExistsError(f"refusing to replace existing archive root {root}")
    staging = root.with_name(f".{root.name}.partial")
    if staging.exists() or staging.is_symlink():
        raise FileExistsError(f"refusing to resume stale staging archive {staging}")
    pa, _ = _load_pyarrow()
    target_schema = arrow_schema(completed.schema)
    request = _read_session_request(completed.destination.storage_resource)
    try:
        session = storage_client.create_read_session(
            parent=f"projects/{billing_project}",
            read_session=request,
            max_stream_count=max_streams,
        )
    except Exception as error:
        raise BigQueryStorageArchiveError("Storage Read create_read_session failed") from error
    streams = _session_streams(session)
    if len(streams) > max_streams:
        raise BigQueryStorageArchiveError("Storage Read returned more streams than requested")
    staging.mkdir(mode=0o700)
    futures: dict[str, Future[_StreamArchive]] = {}
    results: dict[str, _StreamArchive] = {}
    try:
        worker_count = max(1, min(len(streams), max_streams))
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="pumpquant-bqread",
        ) as executor:
            for stream_index, stream_name in enumerate(streams):
                futures[stream_name] = executor.submit(
                    _archive_stream,
                    stream_name,
                    storage_client=storage_client,
                    session=session,
                    target_schema=target_schema,
                    schema_hash=completed.schema.schema_sha256,
                    staging=staging,
                    stream_index=stream_index,
                    max_rows_per_shard=max_rows_per_shard,
                    max_bytes_per_shard=max_bytes_per_shard,
                )
            for stream_name in streams:
                try:
                    result = futures[stream_name].result()
                except BigQueryStorageArchiveError:
                    raise
                except Exception as error:
                    raise BigQueryStorageArchiveError(
                        "Storage Read stream failed before completion"
                    ) from error
                if result.stream_name in results:
                    raise BigQueryStorageArchiveError(
                        "Storage Read stream completed more than once"
                    )
                results[result.stream_name] = result
        if set(results) != set(streams) or len(results) != len(streams):
            raise BigQueryStorageArchiveError(
                "not every Storage Read stream completed exactly once"
            )
        row_count = sum(result.row_count for result in results.values())
        if row_count != completed.total_rows:
            raise BigQueryStorageArchiveError(
                f"Storage Read row count {row_count} does not match job totalRows "
                f"{completed.total_rows}"
            )
        table_getter = getattr(bigquery_client, "get_table", None)
        if callable(table_getter):
            try:
                final_table = table_getter(completed.destination.full_name)
            except Exception as error:
                raise BigQueryStorageArchiveError(
                    "BigQuery destination tables.get verification failed"
                ) from error
            if final_table is None:
                raise BigQueryStorageArchiveError(
                    "BigQuery destination tables.get verification returned no table"
                )
            _validate_table_identity(final_table, completed.destination)
            final_schema = _table_schema(final_table)
            if (
                final_schema is not None
                and final_schema.schema_sha256 != completed.schema.schema_sha256
            ):
                raise BigQueryStorageArchiveError(
                    "destination table schema changed during Storage Read archival"
                )
            final_rows = _table_rows(final_table)
            if final_rows is not None and final_rows != completed.total_rows:
                raise BigQueryStorageArchiveError(
                    "destination table row count changed during Storage Read archival"
                )
            final_etag = _table_etag(final_table)
            if completed.destination_etag is not None and final_etag != completed.destination_etag:
                raise BigQueryStorageArchiveError(
                    "destination table ETag changed during Storage Read archival"
                )
        shards = tuple(shard for stream_name in streams for shard in results[stream_name].shards)
        manifest_unsigned = ArchiveManifest(
            table=safe_table,
            schema_sha256=completed.schema.schema_sha256,
            shards=shards,
            manifest_path="manifest.json",
            provenance=provenance,
        )
        manifest_target = staging / manifest_unsigned.manifest_path
        manifest_partial = Path(f"{manifest_target}.partial")
        with manifest_partial.open("xb") as handle:
            handle.write(manifest_bytes(manifest_unsigned))
            handle.flush()
            os.fsync(handle.fileno())
        os.rename(manifest_partial, manifest_target)
        _fsync_directory(staging)
        digest = hashlib.sha256(manifest_target.read_bytes()).hexdigest()
        manifest = ArchiveManifest(
            table=manifest_unsigned.table,
            schema_sha256=manifest_unsigned.schema_sha256,
            shards=manifest_unsigned.shards,
            manifest_sha256=digest,
            manifest_path=manifest_unsigned.manifest_path,
            provenance=manifest_unsigned.provenance,
        )
        verify_archive_manifest(manifest, staging)
        os.rename(staging, root)
        _fsync_directory(root.parent)
        return manifest
    except BaseException:
        if staging.exists() and not staging.is_symlink():
            shutil.rmtree(staging)
        raise


def archive_bigquery_storage_job(**kwargs: Any) -> ArchiveManifest:
    """Compatibility alias with a descriptive name for remote workers."""

    return archive_completed_query_job(**kwargs)


__all__ = [
    "AnonymousDestination",
    "BigQueryStorageArchiveError",
    "CompletedQueryJob",
    "archive_bigquery_storage_job",
    "archive_completed_query_job",
    "resolve_completed_query_job",
]
