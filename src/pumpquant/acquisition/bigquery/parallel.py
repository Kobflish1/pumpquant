"""Read-only parallel transport for an existing BigQuery result table.

This module intentionally does not import the BigQuery client and never calls
``jobs.query`` (or any other POST endpoint).  A caller must supply an already
authorized, read-only HTTP session factory and an existing job or table
reference.  The transport is useful for a completed query whose anonymous
destination table is still within BigQuery's retention window.

``tabledata.list`` is a cursor API, but it also accepts a zero-based
``startIndex``.  We split the immutable result table into disjoint half-open
row ranges, use ``startIndex`` only for the first request in each range, then
follow that range's opaque ``pageToken``.  Every range must return exactly its
planned number of rows.  Consequently a successful run has a mechanical
row-count and gap/overlap proof independent of SQL ordering: the row position
is the source identity, not an inferred ordering key.

The optional probe reads two small ranges twice and compares canonical row
hashes.  It is a diagnostic for servers or transports that ignore
``startIndex``; it is deliberately separate from the reader because equal
rows are legal query output and therefore content hashes alone cannot prove
identity.  Storage Read API streams are the preferred high-throughput backend
when they are explicitly enabled and IAM-tested; this module is the bounded
REST fallback.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from time import sleep
from typing import Any, TypeAlias
from urllib.parse import quote

_MAX_TABLEDATA_ROWS = 100_000
_DEFAULT_RANGE_ROWS = 100_000
_DEFAULT_PAGE_ROWS = 10_000
_DEFAULT_WORKERS = 8
_MAX_WORKERS = 32
_HTTP_TIMEOUT_SECONDS = 60
_GET_ATTEMPTS = 8
_TRANSIENT_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
_ID = re.compile(r"^[A-Za-z0-9_]{1,1024}$")
_PROJECT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,1023}$")
_JOB_ID = re.compile(r"^[A-Za-z0-9_-]{1,1024}$")

JSONMapping: TypeAlias = Mapping[str, object]


class ParallelReadError(RuntimeError):
    """Raised when a range cannot be proven complete and non-overlapping."""


@dataclass(frozen=True, slots=True)
class TableReference:
    """A fully-qualified BigQuery table reference.

    Anonymous result datasets normally begin with ``_``.  We do not require
    that prefix because the job API is the authority and named destination
    tables are also safe to read through this read-only transport.
    """

    project_id: str
    dataset_id: str
    table_id: str

    def __post_init__(self) -> None:
        for label, value in (
            ("project_id", self.project_id),
            ("dataset_id", self.dataset_id),
            ("table_id", self.table_id),
        ):
            pattern = _PROJECT_ID if label == "project_id" else _ID
            if not isinstance(value, str) or pattern.fullmatch(value) is None:
                raise ValueError(f"{label} has an invalid BigQuery identifier")

    @property
    def path(self) -> str:
        return f"projects/{self.project_id}/datasets/{self.dataset_id}/tables/{self.table_id}"


@dataclass(frozen=True, slots=True)
class RowRange:
    """A disjoint half-open source row range ``[start, stop)``."""

    start: int
    stop: int

    def __post_init__(self) -> None:
        for label, value in (("start", self.start), ("stop", self.stop)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"row range {label} must be a non-negative integer")
        if self.stop <= self.start:
            raise ValueError("row range stop must be greater than start")

    @property
    def row_count(self) -> int:
        return self.stop - self.start


@dataclass(frozen=True, slots=True)
class TableDataChunk:
    """Rows for one source range, retaining the source range as identity."""

    row_range: RowRange
    rows: tuple[JSONMapping, ...]
    page_count: int
    rows_sha256: str

    def __post_init__(self) -> None:
        if len(self.rows) != self.row_range.row_count:
            raise ValueError("chunk row count does not match its source range")
        if isinstance(self.page_count, bool) or not isinstance(self.page_count, int):
            raise TypeError("chunk page_count must be an integer")
        if self.page_count <= 0:
            raise ValueError("chunk page_count must be positive")
        if re.fullmatch(r"[0-9a-f]{64}", self.rows_sha256) is None:
            raise ValueError("chunk rows_sha256 must be a lowercase SHA-256")


@dataclass(frozen=True, slots=True)
class StartIndexProbe:
    """Evidence from a bounded duplicate-read start-index probe.

    ``start_index_semantics_proven`` is true only when two distinct ranges
    were read twice with identical hashes and no cross-range row hashes
    collided.  A query may legitimately contain duplicate rows; in that case
    the probe returns false rather than claiming a proof from ambiguous data.
    The range reader itself still relies on the documented BigQuery offset
    contract and exact row counts.
    """

    total_rows: int
    ranges: tuple[RowRange, ...]
    repeated_hash_matches: bool
    cross_range_duplicate_hashes: int
    start_index_semantics_proven: bool


PageFetcher: TypeAlias = Callable[[int | None, str | None, int], Mapping[str, object]]
SessionFactory: TypeAlias = Callable[[], Any]


def plan_row_ranges(
    total_rows: int,
    *,
    range_rows: int = _DEFAULT_RANGE_ROWS,
) -> tuple[RowRange, ...]:
    """Partition ``[0, total_rows)`` into contiguous disjoint ranges."""

    _validate_total_rows(total_rows)
    _validate_positive("range_rows", range_rows)
    if total_rows == 0:
        return ()
    ranges: list[RowRange] = []
    start = 0
    while start < total_rows:
        stop = min(total_rows, start + range_rows)
        ranges.append(RowRange(start, stop))
        start = stop
    _validate_partition(ranges, total_rows)
    return tuple(ranges)


def _validate_total_rows(total_rows: int) -> None:
    if isinstance(total_rows, bool) or not isinstance(total_rows, int) or total_rows < 0:
        raise ValueError("total_rows must be a non-negative integer")


def _validate_positive(label: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")


def _validate_partition(ranges: tuple[RowRange, ...] | list[RowRange], total_rows: int) -> None:
    cursor = 0
    for item in ranges:
        if item.start != cursor:
            raise ParallelReadError("row ranges are not contiguous")
        cursor = item.stop
    if cursor != total_rows:
        raise ParallelReadError("row ranges do not cover totalRows")


def _canonical_row(row: Mapping[str, object]) -> bytes:
    try:
        encoded = json.dumps(
            row,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ParallelReadError("BigQuery row was not canonical JSON") from error
    return encoded


def row_hash(row: Mapping[str, object]) -> str:
    """Return a deterministic hash for one REST row mapping."""

    return hashlib.sha256(_canonical_row(row)).hexdigest()


def _rows_hash(rows: list[JSONMapping] | tuple[JSONMapping, ...]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        encoded = _canonical_row(row)
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _parse_optional_total_rows(payload: Mapping[str, object], expected: int) -> None:
    if "totalRows" not in payload:
        raise ParallelReadError("tabledata.list response omitted totalRows")
    raw = payload["totalRows"]
    if isinstance(raw, bool):
        raise ParallelReadError("tabledata.list returned invalid totalRows")
    if isinstance(raw, int):
        parsed = raw
    elif isinstance(raw, str) and raw.isdigit():
        parsed = int(raw)
    else:
        raise ParallelReadError("tabledata.list returned invalid totalRows")
    if parsed < 0 or parsed != expected:
        raise ParallelReadError("tabledata.list totalRows changed during the read")


def _parse_page_rows(payload: Mapping[str, object]) -> tuple[JSONMapping, ...]:
    raw = payload.get("rows", ())
    if raw is None:
        raw = ()
    if not isinstance(raw, (list, tuple)):
        raise ParallelReadError("tabledata.list returned invalid rows")
    rows: list[JSONMapping] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise ParallelReadError("tabledata.list returned invalid rows")
        rows.append(dict(item))
    return tuple(rows)


def _next_page_token(payload: Mapping[str, object]) -> str | None:
    token = payload.get("pageToken")
    if token in (None, ""):
        return None
    if not isinstance(token, str):
        raise ParallelReadError("tabledata.list returned an invalid pageToken")
    return token


def _read_range(
    row_range: RowRange,
    *,
    total_rows: int,
    fetch_page: PageFetcher,
    page_rows: int,
) -> TableDataChunk:
    rows: list[JSONMapping] = []
    cursor = row_range.start
    page_token: str | None = None
    first_request = True
    seen_tokens: set[str] = set()
    page_count = 0

    while cursor < row_range.stop:
        remaining = row_range.stop - cursor
        start_index = cursor if first_request else None
        try:
            payload = fetch_page(start_index, page_token, min(page_rows, remaining))
        except ParallelReadError:
            raise
        except Exception as error:
            raise ParallelReadError("tabledata.list page request failed") from error
        if not isinstance(payload, Mapping):
            raise ParallelReadError("tabledata.list returned a non-object response")
        _parse_optional_total_rows(payload, total_rows)
        page = _parse_page_rows(payload)
        if not page:
            raise ParallelReadError("tabledata.list returned no rows before a range ended")
        if len(page) > remaining:
            raise ParallelReadError("tabledata.list crossed the requested row range")
        rows.extend(page)
        cursor += len(page)
        page_count += 1
        next_token = _next_page_token(payload)
        if next_token is not None and next_token in seen_tokens:
            raise ParallelReadError("tabledata.list returned a repeated pageToken")
        if next_token is not None:
            seen_tokens.add(next_token)
        if cursor < row_range.stop and next_token is None:
            raise ParallelReadError("tabledata.list ended before the row range")
        page_token = next_token
        first_request = False

    if cursor != row_range.stop or len(rows) != row_range.row_count:
        raise ParallelReadError("tabledata.list range row count has a gap")
    frozen_rows = tuple(rows)
    return TableDataChunk(row_range, frozen_rows, page_count, _rows_hash(frozen_rows))


def read_rows_in_parallel(
    total_rows: int,
    fetch_page: PageFetcher,
    *,
    range_rows: int = _DEFAULT_RANGE_ROWS,
    page_rows: int = _DEFAULT_PAGE_ROWS,
    max_workers: int = _DEFAULT_WORKERS,
) -> Iterator[TableDataChunk]:
    """Read an existing table's rows over disjoint parallel REST ranges.

    ``fetch_page`` receives ``(start_index, page_token, max_results)``.  The
    first call in a range has a non-``None`` ``start_index`` and no token;
    continuation calls pass ``start_index=None`` and the prior opaque token.
    Results are yielded in source-range order while at most ``max_workers``
    requests are active.  No function in this module can submit a query.
    """

    _validate_total_rows(total_rows)
    _validate_positive("range_rows", range_rows)
    _validate_positive("page_rows", page_rows)
    if page_rows > _MAX_TABLEDATA_ROWS:
        raise ValueError(f"page_rows cannot exceed {_MAX_TABLEDATA_ROWS}")
    if isinstance(max_workers, bool) or not isinstance(max_workers, int):
        raise ValueError("max_workers must be an integer")
    if not 1 <= max_workers <= _MAX_WORKERS:
        raise ValueError(f"max_workers must be between 1 and {_MAX_WORKERS}")
    if not callable(fetch_page):
        raise TypeError("fetch_page must be callable")

    ranges = plan_row_ranges(total_rows, range_rows=range_rows)
    if not ranges:
        return
    with ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="pumpquant-bq-range",
    ) as pool:
        futures = [
            pool.submit(
                _read_range,
                item,
                total_rows=total_rows,
                fetch_page=fetch_page,
                page_rows=page_rows,
            )
            for item in ranges
        ]
        # Reading futures in range order preserves deterministic source
        # identity while requests for later ranges continue in parallel.
        for future in futures:
            yield future.result()


def iter_indexed_rows(chunks: Iterator[TableDataChunk]) -> Iterator[tuple[int, JSONMapping]]:
    """Yield ``(source_row_index, row)`` pairs with a contiguous proof.

    A sink that needs to retain result identity should persist this offset (or
    a range manifest that implies it) alongside the REST row.  SQL output
    order remains intentionally opaque; the offset is the only identity this
    transport can establish without changing the original query.
    """

    cursor = 0
    for chunk in chunks:
        if chunk.row_range.start != cursor:
            raise ParallelReadError("parallel chunks have a gap or overlap")
        if _rows_hash(chunk.rows) != chunk.rows_sha256:
            raise ParallelReadError("parallel chunk row hash does not match its rows")
        yield from enumerate(chunk.rows, start=chunk.row_range.start)
        cursor = chunk.row_range.stop


def probe_start_index_semantics(
    total_rows: int,
    fetch_page: PageFetcher,
    *,
    probe_rows: int = 32,
) -> StartIndexProbe:
    """Repeat two adjacent ranges and compare canonical row hashes.

    The probe is intentionally bounded (at most ``2 * probe_rows`` rows per
    pass) and should be run before a large parallel read.  Repeated reads of
    each range must have identical hashes.  Distinct ranges must not share row
    hashes for the probe to report a proof; duplicate-valued SQL rows make
    this condition ambiguous and therefore produce a non-proof report.
    """

    _validate_total_rows(total_rows)
    _validate_positive("probe_rows", probe_rows)
    if not callable(fetch_page):
        raise TypeError("fetch_page must be callable")
    if total_rows < 2:
        return StartIndexProbe(total_rows, (), True, 0, False)

    width = min(probe_rows, max(1, total_rows // 2))
    first = RowRange(0, width)
    second = RowRange(width, min(total_rows, width * 2))
    ranges = (first, second)
    first_pass = tuple(
        _read_range(item, total_rows=total_rows, fetch_page=fetch_page, page_rows=width)
        for item in ranges
    )
    second_pass = tuple(
        _read_range(item, total_rows=total_rows, fetch_page=fetch_page, page_rows=width)
        for item in ranges
    )
    repeated_matches = all(
        left.rows_sha256 == right.rows_sha256
        for left, right in zip(first_pass, second_pass, strict=True)
    )
    first_hashes = {row_hash(row) for row in first_pass[0].rows}
    second_hashes = {row_hash(row) for row in first_pass[1].rows}
    overlap = len(first_hashes.intersection(second_hashes))
    proven = repeated_matches and overlap == 0
    return StartIndexProbe(total_rows, ranges, repeated_matches, overlap, proven)


class ReadOnlyBigQueryTabledataReader:
    """Resolve an existing job's destination and read it using GET-only calls.

    ``session_factory`` must create an already-authorized read-only HTTP
    session.  One session is used for metadata, while worker threads obtain a
    session from the same factory through thread-local storage.  The class has
    no ``post`` path; passing a fake session with a failing ``post`` method is a
    useful unit-test guard against accidental query submission.
    """

    def __init__(
        self,
        billing_project: str,
        *,
        location: str,
        session_factory: SessionFactory,
        allow_named_destination: bool = False,
    ) -> None:
        if not isinstance(billing_project, str) or not billing_project.strip():
            raise ValueError("billing_project must be non-empty")
        if not isinstance(location, str) or not location.strip():
            raise ValueError("location must be non-empty")
        if not callable(session_factory):
            raise TypeError("session_factory must be callable")
        if not isinstance(allow_named_destination, bool):
            raise TypeError("allow_named_destination must be a boolean")
        self._billing_project = billing_project
        self._location = location
        self._session_factory = session_factory
        self._allow_named_destination = allow_named_destination

    def _validate_destination(self, table: TableReference) -> None:
        if not self._allow_named_destination and not table.dataset_id.startswith("_"):
            raise ParallelReadError(
                "parallel tabledata reads require an anonymous destination dataset"
            )

    def _session(self) -> Any:
        try:
            session = self._session_factory()
        except Exception as error:
            raise ParallelReadError("could not create a read-only HTTP session") from error
        if not hasattr(session, "get") or not callable(session.get):
            raise TypeError("session factory must return an object with get()")
        return session

    @staticmethod
    def _status(response: Any) -> int:
        status = getattr(response, "status_code", None)
        if isinstance(status, bool) or not isinstance(status, int):
            raise ParallelReadError("BigQuery response had no valid HTTP status")
        return status

    @classmethod
    def _json(cls, response: Any, operation: str) -> Mapping[str, object]:
        status = cls._status(response)
        if not 200 <= status < 300:
            raise ParallelReadError(f"BigQuery {operation} failed with HTTP {status}")
        try:
            payload = response.json()
        except Exception as error:
            raise ParallelReadError(f"BigQuery {operation} returned invalid JSON") from error
        if not isinstance(payload, Mapping):
            raise ParallelReadError(f"BigQuery {operation} returned a non-object response")
        if payload.get("error") or payload.get("errorResult"):
            raise ParallelReadError(f"BigQuery {operation} returned query errors")
        return payload

    @classmethod
    def _get(
        cls,
        session: Any,
        url: str,
        *,
        params: Mapping[str, object] | None,
        operation: str,
    ) -> Mapping[str, object]:
        for attempt in range(_GET_ATTEMPTS):
            try:
                response = session.get(
                    url,
                    params=dict(params or {}),
                    timeout=_HTTP_TIMEOUT_SECONDS,
                )
            except Exception as error:
                if attempt + 1 == _GET_ATTEMPTS:
                    raise ParallelReadError(f"BigQuery {operation} failed") from error
            else:
                status = cls._status(response)
                if status not in _TRANSIENT_HTTP_STATUSES:
                    return cls._json(response, operation)
                if attempt + 1 == _GET_ATTEMPTS:
                    raise ParallelReadError(f"BigQuery {operation} failed with HTTP {status}")
            sleep(min(0.5 * (2**attempt), 30.0))
        raise AssertionError("unreachable BigQuery GET retry state")

    def resolve_job_destination(self, job_id: str) -> TableReference:
        """Resolve a completed job's destination table using ``jobs.get``."""

        if not isinstance(job_id, str) or _JOB_ID.fullmatch(job_id) is None:
            raise ValueError("job_id has an invalid format")
        project = quote(self._billing_project, safe="")
        url = (
            "https://bigquery.googleapis.com/bigquery/v2/projects/"
            f"{project}/jobs/{quote(job_id, safe='')}"
        )
        payload = self._get(
            self._session(),
            url,
            params={"location": self._location},
            operation="existing-job metadata",
        )
        reference = payload.get("jobReference")
        if not isinstance(reference, Mapping):
            raise ParallelReadError("existing job omitted jobReference")
        if (
            reference.get("projectId") != self._billing_project
            or reference.get("jobId") != job_id
            or reference.get("location") not in (None, self._location)
        ):
            raise ParallelReadError("existing job reference does not match the request")
        status = payload.get("status")
        if not isinstance(status, Mapping) or status.get("state") != "DONE":
            raise ParallelReadError("existing job is not complete")
        if status.get("errorResult"):
            raise ParallelReadError("existing job failed")
        query_config = payload.get("configuration")
        if not isinstance(query_config, Mapping):
            raise ParallelReadError("existing job omitted query configuration")
        query = query_config.get("query")
        if not isinstance(query, Mapping) or query_config.get("dryRun", False) is not False:
            raise ParallelReadError("existing job is not a completed query")
        destination = query.get("destinationTable")
        if not isinstance(destination, Mapping):
            raise ParallelReadError("existing query has no destinationTable")
        try:
            return TableReference(
                str(destination["projectId"]),
                str(destination["datasetId"]),
                str(destination["tableId"]),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise ParallelReadError("existing query destinationTable is invalid") from error

    def _table_metadata(self, table: TableReference) -> Mapping[str, object]:
        project = quote(table.project_id, safe="")
        dataset = quote(table.dataset_id, safe="")
        table_id = quote(table.table_id, safe="")
        url = (
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}"
            f"/tables/{table_id}"
        )
        return self._get(
            self._session(),
            url,
            params={},
            operation="destination-table metadata",
        )

    def table_metadata(self, table: TableReference) -> Mapping[str, object]:
        """Return owner-authorized table metadata (schema, ETag, ``numRows``)."""

        self._validate_destination(table)
        return self._table_metadata(table)

    @staticmethod
    def _metadata_rows(metadata: Mapping[str, object]) -> int:
        value = metadata.get("numRows")
        if isinstance(value, bool):
            raise ParallelReadError("destination table returned invalid numRows")
        if isinstance(value, int):
            parsed = value
        elif isinstance(value, str) and value.isdigit():
            parsed = int(value)
        else:
            raise ParallelReadError("destination table omitted numRows")
        if parsed < 0:
            raise ParallelReadError("destination table returned invalid numRows")
        return parsed

    def _fetcher(self, table: TableReference) -> PageFetcher:
        project = quote(table.project_id, safe="")
        dataset = quote(table.dataset_id, safe="")
        table_id = quote(table.table_id, safe="")
        url = (
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}"
            f"/tables/{table_id}/data"
        )

        # requests.Session is not promised to be thread-safe.  Keep one
        # read-only session per range worker without asking the factory to
        # understand thread identity.
        import threading

        local = threading.local()

        def fetch(
            start_index: int | None,
            page_token: str | None,
            max_results: int,
        ) -> Mapping[str, object]:
            session = getattr(local, "session", None)
            if session is None:
                session = self._session()
                local.session = session
            params: dict[str, object] = {"maxResults": max_results}
            if start_index is not None:
                params["startIndex"] = str(start_index)
            if page_token is not None:
                params["pageToken"] = page_token
            return self._get(session, url, params=params, operation="tabledata.list")

        return fetch

    def read_table(
        self,
        table: TableReference,
        *,
        expected_total_rows: int | None = None,
        range_rows: int = _DEFAULT_RANGE_ROWS,
        page_rows: int = _DEFAULT_PAGE_ROWS,
        max_workers: int = _DEFAULT_WORKERS,
    ) -> Iterator[TableDataChunk]:
        """Read a known destination table after a GET-only metadata check."""

        self._validate_destination(table)
        metadata = self._table_metadata(table)
        total_rows = self._metadata_rows(metadata)
        initial_etag = metadata.get("etag")
        if not isinstance(initial_etag, str) or not initial_etag:
            raise ParallelReadError("destination table omitted an ETag")
        if expected_total_rows is not None:
            _validate_total_rows(expected_total_rows)
            if expected_total_rows != total_rows:
                raise ParallelReadError("destination table numRows differs from expected totalRows")
        chunks = read_rows_in_parallel(
            total_rows,
            self._fetcher(table),
            range_rows=range_rows,
            page_rows=page_rows,
            max_workers=max_workers,
        )

        # Anonymous cached results are expected to be immutable, but an ETag
        # and row-count check at the end prevents silently sealing a mutable
        # named table (or a table replaced during a read).
        def checked_chunks() -> Iterator[TableDataChunk]:
            yield from chunks
            final_metadata = self._table_metadata(table)
            if final_metadata.get("etag") != initial_etag:
                raise ParallelReadError("destination table changed during the read")
            if self._metadata_rows(final_metadata) != total_rows:
                raise ParallelReadError("destination table row count changed during the read")

        return checked_chunks()

    def read_existing_job(
        self,
        job_id: str,
        *,
        destination: TableReference | None = None,
        expected_total_rows: int | None = None,
        range_rows: int = _DEFAULT_RANGE_ROWS,
        page_rows: int = _DEFAULT_PAGE_ROWS,
        max_workers: int = _DEFAULT_WORKERS,
    ) -> Iterator[TableDataChunk]:
        """Read an existing job's destination without submitting a query."""

        resolved = self.resolve_job_destination(job_id)
        self._validate_destination(resolved)
        if destination is not None and destination != resolved:
            raise ParallelReadError("provided destination differs from job destinationTable")
        return self.read_table(
            resolved,
            expected_total_rows=expected_total_rows,
            range_rows=range_rows,
            page_rows=page_rows,
            max_workers=max_workers,
        )

    def probe_existing_job(
        self,
        job_id: str,
        *,
        destination: TableReference | None = None,
        probe_rows: int = 32,
    ) -> StartIndexProbe:
        """Run the bounded duplicate-hash offset probe for an existing job."""

        resolved = self.resolve_job_destination(job_id)
        self._validate_destination(resolved)
        if destination is not None and destination != resolved:
            raise ParallelReadError("provided destination differs from job destinationTable")
        metadata = self._table_metadata(resolved)
        total_rows = self._metadata_rows(metadata)
        return probe_start_index_semantics(
            total_rows,
            self._fetcher(resolved),
            probe_rows=probe_rows,
        )


__all__ = [
    "PageFetcher",
    "ParallelReadError",
    "ReadOnlyBigQueryTabledataReader",
    "RowRange",
    "StartIndexProbe",
    "TableDataChunk",
    "TableReference",
    "plan_row_ranges",
    "probe_start_index_semantics",
    "read_rows_in_parallel",
    "iter_indexed_rows",
    "row_hash",
]
