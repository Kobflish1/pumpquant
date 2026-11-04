"""Optional official Google Cloud BigQuery adapter.

The adapter supports metadata reads, non-billable dry runs, and execution of an
exact plan carrying its successful dry-run evidence. It never exposes table
writes, destination-table mutations, GCS exports, or the Storage Read API.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic, sleep
from typing import Any, cast
from urllib.parse import quote
from uuid import NAMESPACE_URL, uuid5

from pumpquant.acquisition.bigquery.source import (
    BackendDryRun,
    DatasetRef,
    DatasetSnapshot,
    DryRunResult,
    DryRunValidationError,
    FieldInfo,
    QueryParameter,
    QueryPlan,
    TableInfo,
    query_sha256,
    validate_plan_sql_provenance,
)

_QUERY_TIMEOUT_MS = 30_000
_HTTP_TIMEOUT_SECONDS = 60
_DEFAULT_MAX_RESULTS = 10_000
_MAX_RESULT_ROWS = 100_000
_MAX_RESULT_REQUESTS = 10_000
_RESULT_GET_ATTEMPTS = 8
_TRANSIENT_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
_BIGQUERY_READONLY_SCOPE = "https://www.googleapis.com/auth/bigquery.readonly"
_DRY_RUN_EVIDENCE_TTL_SECONDS = 300.0


@dataclass(frozen=True, slots=True)
class QueryResultPage:
    """One bounded page returned by a BigQuery ``jobs.query`` request.

    ``rows`` retains BigQuery's REST row mappings.  Decoding
    values against a schema belongs to a downstream ingestion layer; this
    adapter's responsibility is to transport the exact, bounded response.
    """

    job_id: str
    page_number: int
    schema: Mapping[str, Any]
    rows: tuple[Mapping[str, Any], ...]
    total_rows: int | None = None
    total_bytes_processed: int | None = None

    def __post_init__(self) -> None:
        if not self.job_id.strip():
            raise ValueError("job_id must be non-empty")
        if self.page_number < 0:
            raise ValueError("page_number cannot be negative")
        if not isinstance(self.schema, Mapping):
            raise TypeError("schema must be a mapping")
        if not isinstance(self.rows, tuple) or any(
            not isinstance(row, Mapping) for row in self.rows
        ):
            raise TypeError("rows must be a tuple of mappings")
        for label, value in (
            ("total_rows", self.total_rows),
            ("total_bytes_processed", self.total_bytes_processed),
        ):
            if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
                raise TypeError(f"{label} must be an integer or None")
            if value is not None and value < 0:
                raise ValueError(f"{label} cannot be negative")


@dataclass(frozen=True, slots=True)
class _DryRunEvidence:
    estimated_bytes: int
    referenced_tables: tuple[str, ...]
    provenance_source: str
    captured_monotonic: float


def _load_bigquery() -> Any:
    try:
        import google.cloud.bigquery as bigquery
    except ImportError as error:  # pragma: no cover - depends on optional package
        raise RuntimeError("BigQuery access requires: pip install 'pumpquant[bigquery]'") from error
    return bigquery


def _iso_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError("Google metadata timestamp was not a datetime")
    normalized = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return normalized.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _partition_granularity(partitioning: Any) -> str | None:
    if partitioning is None:
        return None
    value = getattr(partitioning, "type_", None)
    if value is None:
        return None
    enum_value = getattr(value, "value", value)
    return str(enum_value).upper()


def _flatten_schema(schema: Any, prefix: str = "") -> tuple[FieldInfo, ...]:
    fields: list[FieldInfo] = []
    for field in schema:
        path = f"{prefix}.{field.name}" if prefix else str(field.name)
        fields.append(FieldInfo(path, str(field.field_type), str(field.mode)))
        nested = getattr(field, "fields", ())
        if nested:
            fields.extend(_flatten_schema(nested, path))
    return tuple(fields)


def _to_rest_parameter(parameter: QueryParameter) -> dict[str, Any]:
    type_name = parameter.type_name.upper()
    if type_name == "TIMESTAMP":
        if not isinstance(parameter.value, str):
            raise TypeError(f"{parameter.name} TIMESTAMP value must be a string")
        datetime.fromisoformat(parameter.value.replace("Z", "+00:00"))
        return {
            "name": parameter.name,
            "parameterType": {"type": "TIMESTAMP"},
            "parameterValue": {"value": parameter.value},
        }
    if type_name == "ARRAY<STRING>":
        if not isinstance(parameter.value, tuple):
            raise TypeError(f"{parameter.name} ARRAY<STRING> value must be a tuple")
        return {
            "name": parameter.name,
            "parameterType": {"type": "ARRAY", "arrayType": {"type": "STRING"}},
            "parameterValue": {"arrayValues": [{"value": value} for value in parameter.value]},
        }
    raise ValueError(f"unsupported BigQuery parameter type {parameter.type_name!r}")


def _load_authorized_session(credentials: Any) -> Any:
    try:
        from google.auth.transport.requests import (
            AuthorizedSession,
        )
    except ImportError as error:  # pragma: no cover - installed with BigQuery extra
        raise RuntimeError("BigQuery access requires: pip install 'pumpquant[bigquery]'") from error
    return AuthorizedSession(credentials)


def _validate_readonly_credentials(credentials: Any) -> None:
    scopes = getattr(credentials, "scopes", None)
    if not isinstance(scopes, (tuple, list, set, frozenset)) or set(scopes) != {
        _BIGQUERY_READONLY_SCOPE
    }:
        raise RuntimeError(
            "BigQuery credentials must carry exactly the bigquery.readonly OAuth scope"
        )


def _execution_error(message: str) -> DryRunValidationError:
    """Build an execution error whose text contains no response payload data."""
    return DryRunValidationError(message)


def query_request_id(plan: QueryPlan) -> str:
    """Return a stable UUID for safe retries of the exact approved plan.

    BigQuery retains ``jobs.query`` request IDs for a limited idempotency
    window.  Deriving the UUID from the full plan hash means a transport retry
    cannot silently create a second scan with a different identifier, while a
    changed query, parameter, or byte cap necessarily receives a new ID.
    """

    return str(uuid5(NAMESPACE_URL, f"pumpquant:{query_sha256(plan)}"))


def _response_status(response: Any) -> int:
    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, bool) or not isinstance(status_code, int):
        raise _execution_error("BigQuery query response had no valid HTTP status")
    return status_code


def _response_json(response: Any, *, operation: str) -> Mapping[str, Any]:
    status_code = _response_status(response)
    if not 200 <= status_code < 300:
        raise _execution_error(f"BigQuery {operation} failed with HTTP {status_code}")
    try:
        payload = response.json()
    except Exception:
        raise _execution_error(f"BigQuery {operation} returned invalid JSON") from None
    if not isinstance(payload, Mapping):
        raise _execution_error(f"BigQuery {operation} returned a non-object response")
    return payload


def _validate_response_errors(payload: Mapping[str, Any], *, operation: str) -> None:
    # Do not include any error payload in the exception text.  BigQuery error
    # details can contain user SQL, parameter values, or other sensitive data.
    # ``errors[]`` can contain warnings or intermediate issues even when the
    # job ultimately succeeds. Fatal REST/query failures are represented by a
    # non-2xx response, top-level ``error``, or ``errorResult``. A completed
    # response without result schema is rejected separately.
    if payload.get("error") or payload.get("errorResult"):
        raise _execution_error(f"BigQuery {operation} returned query errors")


def _required_bool(payload: Mapping[str, Any], key: str, *, operation: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise _execution_error(f"BigQuery {operation} returned invalid {key}")
    return value


def _optional_nonnegative_int(
    payload: Mapping[str, Any], key: str, *, operation: str
) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, bool):
        raise _execution_error(f"BigQuery {operation} returned invalid {key}")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdigit():
        parsed = int(value)
    else:
        raise _execution_error(f"BigQuery {operation} returned invalid {key}")
    if parsed < 0:
        raise _execution_error(f"BigQuery {operation} returned invalid {key}")
    return parsed


def _validate_total_bytes_only(
    payload: Mapping[str, Any], *, maximum_bytes_billed: int, operation: str
) -> int | None:
    total_bytes = _optional_nonnegative_int(payload, "totalBytesProcessed", operation=operation)
    if total_bytes is not None and total_bytes > maximum_bytes_billed:
        raise _execution_error("BigQuery query exceeded maximum_bytes_billed")
    return total_bytes


def _validate_job_reference(
    payload: Mapping[str, Any],
    *,
    billing_project: str,
    location: str,
    require: bool,
    expected_job_id: str | None = None,
) -> str | None:
    top_level_location = payload.get("location")
    if top_level_location is not None and (
        not isinstance(top_level_location, str) or top_level_location != location
    ):
        raise _execution_error("BigQuery query response had an invalid location")
    raw_reference = payload.get("jobReference")
    if raw_reference is None:
        if require:
            raise _execution_error("BigQuery query response missing jobReference")
        return None
    if not isinstance(raw_reference, Mapping):
        raise _execution_error("BigQuery query response had invalid jobReference")
    project_id = raw_reference.get("projectId")
    job_id = raw_reference.get("jobId")
    reference_location = raw_reference.get("location")
    if (
        not isinstance(project_id, str)
        or not project_id
        or project_id != billing_project
        or not isinstance(job_id, str)
        or not job_id
        or (
            reference_location is not None
            and (
                not isinstance(reference_location, str)
                or not reference_location
                or reference_location != location
            )
        )
    ):
        raise _execution_error("BigQuery query response had an invalid jobReference")
    if expected_job_id is not None and job_id != expected_job_id:
        raise _execution_error("BigQuery query response jobReference changed")
    return job_id


def _validate_page_token(payload: Mapping[str, Any], *, operation: str) -> str | None:
    value = payload.get("pageToken")
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise _execution_error(f"BigQuery {operation} returned invalid pageToken")
    return value


def _rows_from_payload(
    payload: Mapping[str, Any], *, operation: str
) -> tuple[Mapping[str, Any], ...]:
    value = payload.get("rows")
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise _execution_error(f"BigQuery {operation} returned invalid rows")
    rows: list[Mapping[str, Any]] = []
    for row in value:
        if not isinstance(row, Mapping):
            raise _execution_error(f"BigQuery {operation} returned invalid rows")
        rows.append(dict(row))
    return tuple(rows)


def _page_from_payload(
    payload: Mapping[str, Any],
    *,
    job_id: str,
    page_number: int,
    maximum_bytes_billed: int,
    operation: str,
) -> tuple[QueryResultPage, str | None]:
    schema = payload.get("schema")
    if not isinstance(schema, Mapping):
        raise _execution_error(f"BigQuery {operation} completed without a schema")
    total_rows = _optional_nonnegative_int(payload, "totalRows", operation=operation)
    total_bytes = _validate_total_bytes_only(
        payload,
        maximum_bytes_billed=maximum_bytes_billed,
        operation=operation,
    )
    if total_rows is None:
        raise _execution_error(f"BigQuery {operation} completed without totalRows")
    if total_bytes is None:
        raise _execution_error(f"BigQuery {operation} completed without totalBytesProcessed")
    page = QueryResultPage(
        job_id=job_id,
        page_number=page_number,
        schema=dict(schema),
        rows=_rows_from_payload(payload, operation=operation),
        total_rows=total_rows,
        total_bytes_processed=total_bytes,
    )
    return page, _validate_page_token(payload, operation=operation)


def _validate_execution_approval(plan: QueryPlan, approval: DryRunResult) -> None:
    if not isinstance(approval, DryRunResult):
        raise _execution_error("BigQuery execution requires a DryRunResult approval")
    try:
        approval_plan_sha256 = approval.plan_sha256
        approval_under_cap = approval.under_cap
        approval_cap = approval.maximum_bytes_billed
        approval_tables = approval.referenced_tables
        approval_source = approval.provenance_source
        estimated_bytes = approval.estimated_bytes
    except Exception:
        raise _execution_error("BigQuery execution approval is malformed") from None

    if approval_plan_sha256 != query_sha256(plan):
        raise _execution_error("BigQuery execution approval does not match the query plan")
    if approval_under_cap is not True:
        raise _execution_error("BigQuery execution requires an under-cap dry-run approval")
    if approval_cap != plan.maximum_bytes_billed:
        raise _execution_error("BigQuery execution approval has a different byte cap")
    if not isinstance(approval_tables, (tuple, list)) or not all(
        isinstance(table, str) and table for table in approval_tables
    ):
        raise _execution_error("BigQuery execution approval has invalid table provenance")
    expected_tables = tuple(sorted(set(plan.referenced_tables)))
    actual_tables = tuple(sorted(set(approval_tables)))
    if actual_tables != expected_tables:
        raise _execution_error("BigQuery execution approval has different table provenance")
    if approval_source != "static_sql_allowlist":
        raise _execution_error("BigQuery execution approval has invalid provenance source")
    if (
        isinstance(estimated_bytes, bool)
        or not isinstance(estimated_bytes, int)
        or estimated_bytes <= 0
        or estimated_bytes > plan.maximum_bytes_billed
    ):
        raise _execution_error("BigQuery execution approval has invalid byte evidence")


def _validate_execution_bounds(
    max_results: int,
    max_polls: int,
    *,
    prefetch_one_page: bool = False,
) -> None:
    if (
        isinstance(max_results, bool)
        or not isinstance(max_results, int)
        or not 1 <= max_results <= _MAX_RESULT_ROWS
    ):
        raise ValueError(f"max_results must be between 1 and {_MAX_RESULT_ROWS}")
    if (
        isinstance(max_polls, bool)
        or not isinstance(max_polls, int)
        or not 0 <= max_polls <= _MAX_RESULT_REQUESTS
    ):
        raise ValueError(f"max_polls must be between 0 and {_MAX_RESULT_REQUESTS}")
    if not isinstance(prefetch_one_page, bool):
        raise ValueError("prefetch_one_page must be a boolean")


class GoogleBigQueryBackend:
    """Official-client implementation of the restricted backend protocol."""

    def __init__(
        self,
        billing_project: str,
        *,
        client: Any | None = None,
        session: Any | None = None,
    ) -> None:
        if not billing_project.strip():
            raise ValueError("billing_project must be non-empty")
        self._billing_project = billing_project
        self._module = _load_bigquery()
        self._client = (
            client if client is not None else self._module.Client(project=billing_project)
        )
        credentials = getattr(self._client, "_credentials", None)
        if session is None and credentials is None:
            raise RuntimeError("BigQuery client did not expose credentials for read-only dry runs")
        if session is None:
            _validate_readonly_credentials(credentials)
            self._session = _load_authorized_session(credentials)
        else:
            # Transport injection is retained for deterministic tests.
            # Production construction goes through the exact-scope gate above.
            self._session = session
        self._dry_run_evidence: dict[str, _DryRunEvidence] = {}

    def inspect_dataset(self, ref: DatasetRef) -> DatasetSnapshot:
        """Read dataset/table metadata without scanning table contents."""
        dataset = self._client.get_dataset(ref.full_name)
        dataset_location = str(dataset.location)
        table_items = sorted(self._client.list_tables(dataset), key=lambda item: item.table_id)
        tables: list[TableInfo] = []
        for item in table_items:
            table = self._client.get_table(f"{ref.full_name}.{item.table_id}")
            partitioning = getattr(table, "time_partitioning", None)
            tables.append(
                TableInfo(
                    name=str(table.table_id),
                    location=str(getattr(table, "location", None) or dataset_location),
                    fields=_flatten_schema(table.schema),
                    table_type=str(getattr(table, "table_type", None) or "UNKNOWN"),
                    partition_field=(
                        None if partitioning is None else getattr(partitioning, "field", None)
                    ),
                    partition_granularity=_partition_granularity(partitioning),
                    require_partition_filter=bool(
                        getattr(table, "require_partition_filter", False)
                    ),
                    clustering_fields=tuple(getattr(table, "clustering_fields", None) or ()),
                    num_rows=(None if table.num_rows is None else int(table.num_rows)),
                    logical_bytes=(None if table.num_bytes is None else int(table.num_bytes)),
                    created_at=_iso_timestamp(getattr(table, "created", None)),
                    modified_at=_iso_timestamp(getattr(table, "modified", None)),
                )
            )
        return DatasetSnapshot(
            ref=ref,
            location=dataset_location,
            tables=tuple(tables),
            captured_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        )

    def estimate_query(self, plan: QueryPlan) -> BackendDryRun:
        """Call ``jobs.query`` with ``dryRun=true`` and read-only OAuth.

        ``Client.query`` uses ``jobs.insert``, whose OAuth surface rejects the
        narrower ``bigquery.readonly`` scope.  The synchronous REST method is
        documented to accept that scope and, with ``dryRun=true``,
        validates and estimates without running or billing the query.
        """
        referenced = validate_plan_sql_provenance(plan)
        request_body = {
            "query": plan.sql,
            "dryRun": True,
            "useQueryCache": False,
            "useLegacySql": False,
            "parameterMode": "NAMED",
            "queryParameters": [_to_rest_parameter(item) for item in plan.parameters],
            "location": plan.location,
        }
        project = quote(self._billing_project, safe="")
        try:
            response = self._session.post(
                f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries",
                json=request_body,
                timeout=60,
            )
        except Exception as error:
            raise DryRunValidationError("BigQuery read-only dry-run request failed") from error
        status_code = getattr(response, "status_code", None)
        try:
            payload = response.json()
        except Exception:
            raise DryRunValidationError("BigQuery dry run returned invalid JSON") from None
        if not isinstance(payload, Mapping):
            raise DryRunValidationError("BigQuery dry run returned a non-object response")
        if status_code is None or not 200 <= int(status_code) < 300:
            error_payload = payload.get("error")
            reason = "unknown error"
            if isinstance(error_payload, Mapping):
                status = error_payload.get("status")
                if isinstance(status, str) and status:
                    reason = status
            raise DryRunValidationError(
                f"BigQuery read-only dry run failed with HTTP {status_code}: {reason}"
            )
        if payload.get("jobComplete") is not True:
            raise DryRunValidationError("BigQuery dry run did not complete")
        if payload.get("error") or payload.get("errorResult"):
            raise DryRunValidationError("BigQuery dry run returned query errors")
        total_bytes_processed = payload.get("totalBytesProcessed")
        if total_bytes_processed is None:
            raise DryRunValidationError("BigQuery dry run returned no byte statistics")
        try:
            estimated_bytes = int(total_bytes_processed)
        except (TypeError, ValueError) as error:
            raise DryRunValidationError(
                "BigQuery dry run returned invalid byte statistics"
            ) from error
        result = BackendDryRun(
            estimated_bytes=estimated_bytes,
            referenced_tables=referenced,
            provenance_source="static_sql_allowlist",
        )
        if 0 < estimated_bytes <= plan.maximum_bytes_billed:
            self._dry_run_evidence[query_sha256(plan)] = _DryRunEvidence(
                estimated_bytes=estimated_bytes,
                referenced_tables=referenced,
                provenance_source=result.provenance_source,
                captured_monotonic=monotonic(),
            )
        return result

    def execute_query(
        self,
        plan: QueryPlan,
        approval: DryRunResult,
        max_results: int = _DEFAULT_MAX_RESULTS,
        max_polls: int = _MAX_RESULT_REQUESTS,
        on_job_created: Callable[[str], None] | None = None,
        *,
        prefetch_one_page: bool = False,
    ) -> Iterator[QueryResultPage]:
        """Execute one approved query and stream bounded REST result pages.

        The approval is checked before this method returns an
        iterator, so callers cannot accidentally make a request by merely
        constructing the iterator.  Every HTTP request uses the read-only
        AuthorizedSession already held by this backend and carries the plan's
        byte cap.
        """
        _validate_execution_bounds(max_results, max_polls, prefetch_one_page=prefetch_one_page)
        _validate_execution_approval(plan, approval)
        # Query execution must use the exact same static source check as the
        # non-billable dry run.  This happens before constructing the iterator
        # and therefore before the first network request.
        validate_plan_sql_provenance(plan)
        evidence = self._dry_run_evidence.pop(query_sha256(plan), None)
        if evidence is None:
            raise _execution_error("BigQuery execution has no backend-issued dry-run evidence")
        if (
            evidence.estimated_bytes != approval.estimated_bytes
            or tuple(sorted(evidence.referenced_tables))
            != tuple(sorted(approval.referenced_tables))
            or evidence.provenance_source != approval.provenance_source
            or monotonic() - evidence.captured_monotonic > _DRY_RUN_EVIDENCE_TTL_SECONDS
        ):
            raise _execution_error("BigQuery backend dry-run evidence is stale or mismatched")
        return self._execute_query_pages(
            plan,
            max_results=max_results,
            max_polls=max_polls,
            on_job_created=on_job_created,
            prefetch_one_page=prefetch_one_page,
        )

    def submit_query(self, plan: QueryPlan, approval: DryRunResult) -> str:
        """Submit one approved query without retrieving any result rows.

        This is the coordinator transport.  It performs exactly one
        read-only ``jobs.query`` POST, using the same plan/provenance/dry-run
        gates as :meth:`execute_query`, and returns only the server job ID.
        The POST is not retried: callers persist an atomic
        ``submission_attempt_started`` marker before invoking this method, so
        an ambiguous transport outcome is reconciled through ``jobs.get``
        rather than submitted a second time.
        """

        _validate_execution_approval(plan, approval)
        validate_plan_sql_provenance(plan)
        evidence = self._dry_run_evidence.pop(query_sha256(plan), None)
        if evidence is None:
            raise _execution_error("BigQuery execution has no backend-issued dry-run evidence")
        if (
            evidence.estimated_bytes != approval.estimated_bytes
            or tuple(sorted(evidence.referenced_tables))
            != tuple(sorted(approval.referenced_tables))
            or evidence.provenance_source != approval.provenance_source
            or monotonic() - evidence.captured_monotonic > _DRY_RUN_EVIDENCE_TTL_SECONDS
        ):
            raise _execution_error("BigQuery backend dry-run evidence is stale or mismatched")

        project = quote(self._billing_project, safe="")
        query_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries"
        request_body = {
            "query": plan.sql,
            "dryRun": False,
            "useQueryCache": False,
            "useLegacySql": False,
            "parameterMode": "NAMED",
            "queryParameters": [_to_rest_parameter(item) for item in plan.parameters],
            "location": plan.location,
            # A zero timeout asks BigQuery to hand back a job reference as
            # quickly as possible.  No maxResults is sent because this phase
            # must not request or consume result rows.
            "timeoutMs": 0,
            "maximumBytesBilled": str(plan.maximum_bytes_billed),
            "requestId": query_request_id(plan),
            "labels": {"app": "pumpquant", "workflow": "query_coordinator"},
        }
        try:
            response = self._session.post(
                query_url,
                json=request_body,
                timeout=_HTTP_TIMEOUT_SECONDS,
            )
        except Exception:
            # Never include response/SQL payloads in this error: the caller's
            # durable started marker is the only safe recovery evidence.
            raise _execution_error("BigQuery query submission failed") from None
        payload = _response_json(response, operation="query submission")
        _validate_response_errors(payload, operation="query submission")
        job_id = _validate_job_reference(
            payload,
            billing_project=self._billing_project,
            location=plan.location,
            require=True,
        )
        if job_id is None:  # pragma: no cover - require=True invariant
            raise _execution_error("BigQuery query submission omitted jobReference")
        return job_id

    def get_job_metadata(self, plan: QueryPlan, job_id: str) -> Mapping[str, Any]:
        """GET job/table metadata without retrieving query result rows.

        The returned object contains the job's destination table and byte
        statistics plus a bounded ``tables.get`` schema/row-count/ETag probe
        when the job is complete.  It never calls ``queries/{job}/getQueryResults``
        or ``tabledata.list``.
        """

        validate_plan_sql_provenance(plan)
        if not isinstance(job_id, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,1024}", job_id) is None:
            raise ValueError("job_id has an invalid format")
        project = quote(self._billing_project, safe="")
        url = (
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/jobs/"
            f"{quote(job_id, safe='')}"
        )
        payload = self._get_json_with_retries(
            url,
            params={"location": plan.location},
            operation="job metadata",
        )
        _validate_response_errors(payload, operation="job metadata")
        _validate_job_reference(
            payload,
            billing_project=self._billing_project,
            location=plan.location,
            require=True,
            expected_job_id=job_id,
        )
        self._validate_existing_job_payload(payload, plan)

        status = payload.get("status")
        state = "UNKNOWN"
        if isinstance(status, Mapping) and isinstance(status.get("state"), str):
            state = status["state"]
        if isinstance(status, Mapping) and status.get("errorResult"):
            raise _execution_error("BigQuery existing job failed")

        configuration = payload.get("configuration")
        query_config: Mapping[str, Any] = {}
        if isinstance(configuration, Mapping) and isinstance(configuration.get("query"), Mapping):
            query_config = configuration["query"]
        destination = query_config.get("destinationTable")
        if destination is not None and not isinstance(destination, Mapping):
            raise _execution_error("BigQuery job destinationTable is invalid")
        statistics = payload.get("statistics")
        query_statistics: Mapping[str, Any] = {}
        if isinstance(statistics, Mapping):
            candidate = statistics.get("query")
            if isinstance(candidate, Mapping):
                query_statistics = candidate

        def stat_value(*keys: str) -> object:
            for key in keys:
                if key in query_statistics:
                    return query_statistics[key]
                if isinstance(statistics, Mapping) and key in statistics:
                    return statistics[key]
                if key in payload:
                    return payload[key]
            return None

        actual = _optional_nonnegative_int(
            {"totalBytesProcessed": stat_value("totalBytesProcessed")},
            "totalBytesProcessed",
            operation="job metadata",
        )
        billed = _optional_nonnegative_int(
            {"totalBytesBilled": stat_value("totalBytesBilled")},
            "totalBytesBilled",
            operation="job metadata",
        )
        metadata: dict[str, Any] = {
            "job_id": job_id,
            "state": state,
            "destination_table": None if destination is None else dict(destination),
            "schema": None,
            "num_rows": None,
            "etag": None,
            "actual_bytes_processed": actual,
            "total_bytes_billed": billed,
        }

        # Some test doubles and older REST representations include these
        # fields directly in jobs.get/statistics.query. Preserve them before
        # falling back to the authoritative destination-table GET.
        direct_schema = stat_value("schema")
        if isinstance(direct_schema, Mapping):
            metadata["schema"] = dict(direct_schema)
        direct_rows = stat_value("numRows", "totalRows")
        if direct_rows is not None:
            metadata["num_rows"] = _optional_nonnegative_int(
                {"numRows": direct_rows}, "numRows", operation="job metadata"
            )
        direct_etag = payload.get("etag")
        if isinstance(direct_etag, str) and direct_etag:
            metadata["etag"] = direct_etag

        if state == "DONE" and isinstance(destination, Mapping):
            table_project = destination.get("projectId")
            table_dataset = destination.get("datasetId")
            table_id = destination.get("tableId")
            if not all(
                isinstance(item, str) and item for item in (table_project, table_dataset, table_id)
            ):
                raise _execution_error("BigQuery job destinationTable is invalid")
            table_project_text = cast(str, table_project)
            table_dataset_text = cast(str, table_dataset)
            table_id_text = cast(str, table_id)
            table_url = (
                "https://bigquery.googleapis.com/bigquery/v2/projects/"
                f"{quote(table_project_text, safe='')}"
                f"/datasets/{quote(table_dataset_text, safe='')}"
                f"/tables/{quote(table_id_text, safe='')}"
            )
            table_payload = self._get_json_with_retries(
                table_url,
                params={},
                operation="destination table metadata",
            )
            table_schema = table_payload.get("schema")
            if isinstance(table_schema, Mapping):
                metadata["schema"] = dict(table_schema)
            table_rows = _optional_nonnegative_int(
                table_payload,
                "numRows",
                operation="destination table metadata",
            )
            metadata["num_rows"] = table_rows
            table_etag = table_payload.get("etag")
            if not isinstance(table_etag, str) or not table_etag:
                raise _execution_error("destination table metadata omitted ETag")
            metadata["etag"] = table_etag
        return metadata

    def _validate_existing_job_payload(
        self,
        payload: Mapping[str, Any],
        plan: QueryPlan,
    ) -> None:
        """Validate a jobs.get query configuration against an exact plan."""

        configuration = payload.get("configuration")
        if not isinstance(configuration, Mapping):
            raise _execution_error("BigQuery existing job has invalid configuration")
        query = configuration.get("query")
        if not isinstance(query, Mapping):
            raise _execution_error("BigQuery existing job is not a query job")
        expected_parameters = [_to_rest_parameter(item) for item in plan.parameters]
        labels = query.get("labels", configuration.get("labels"))
        invalid_plan = (
            configuration.get("dryRun", False) is not False
            or query.get("query") != plan.sql
            or query.get("useLegacySql") is not False
            or query.get("useQueryCache") is not False
            or query.get("parameterMode") != "NAMED"
            or query.get("queryParameters") != expected_parameters
            or str(query.get("maximumBytesBilled")) != str(plan.maximum_bytes_billed)
        )
        # Existing archive jobs use the older archive label.  Metadata
        # reconciliation accepts it as a read-only historical job while
        # coordinator submissions always use the coordinator label.
        valid_labels = (
            {
                "app": "pumpquant",
                "workflow": "query_coordinator",
            },
            {"app": "pumpquant", "workflow": "query_archive"},
        )
        if invalid_plan or labels not in valid_labels:
            raise _execution_error("BigQuery existing job does not match the approved plan")

    def _execute_query_pages(
        self,
        plan: QueryPlan,
        *,
        max_results: int,
        max_polls: int,
        on_job_created: Callable[[str], None] | None,
        prefetch_one_page: bool,
    ) -> Iterator[QueryResultPage]:
        project = quote(self._billing_project, safe="")
        query_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries"
        request_body = {
            "query": plan.sql,
            "dryRun": False,
            "useQueryCache": False,
            "useLegacySql": False,
            "parameterMode": "NAMED",
            "queryParameters": [_to_rest_parameter(item) for item in plan.parameters],
            "location": plan.location,
            "timeoutMs": _QUERY_TIMEOUT_MS,
            "maxResults": max_results,
            "maximumBytesBilled": str(plan.maximum_bytes_billed),
            "requestId": query_request_id(plan),
            "labels": {"app": "pumpquant", "workflow": "query_archive"},
        }
        try:
            response = self._session.post(
                query_url,
                json=request_body,
                timeout=_HTTP_TIMEOUT_SECONDS,
            )
        except Exception:
            raise _execution_error("BigQuery query request failed") from None
        payload = _response_json(response, operation="query request")
        _validate_response_errors(payload, operation="query request")
        job_id = _validate_job_reference(
            payload,
            billing_project=self._billing_project,
            location=plan.location,
            require=True,
        )
        # ``require=True`` above guarantees a string, but retaining this guard
        # keeps the invariant obvious to both type checkers and future edits.
        if job_id is None:  # pragma: no cover - defensive
            raise _execution_error("BigQuery query response missing jobReference")
        if on_job_created is not None:
            on_job_created(job_id)

        yield from self._stream_query_results(
            plan,
            project=project,
            job_id=job_id,
            response_payload=payload,
            max_results=max_results,
            max_polls=max_polls,
            prefetch_one_page=prefetch_one_page,
        )

    def resume_query(
        self,
        plan: QueryPlan,
        job_id: str,
        max_results: int = _DEFAULT_MAX_RESULTS,
        max_polls: int = _MAX_RESULT_REQUESTS,
        *,
        prefetch_one_page: bool = False,
    ) -> Iterator[QueryResultPage]:
        """Resume an already-created, exactly validated read-only query job."""

        _validate_execution_bounds(max_results, max_polls, prefetch_one_page=prefetch_one_page)
        validate_plan_sql_provenance(plan)
        if not isinstance(job_id, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,1024}", job_id) is None:
            raise ValueError("job_id has an invalid format")
        return self._resume_query_pages(
            plan,
            job_id=job_id,
            max_results=max_results,
            max_polls=max_polls,
            prefetch_one_page=prefetch_one_page,
        )

    def _resume_query_pages(
        self,
        plan: QueryPlan,
        *,
        job_id: str,
        max_results: int,
        max_polls: int,
        prefetch_one_page: bool,
    ) -> Iterator[QueryResultPage]:
        project = quote(self._billing_project, safe="")
        self._validate_existing_job(project=project, job_id=job_id, plan=plan)
        payload = self._get_query_results(
            project=project,
            job_id=job_id,
            location=plan.location,
            max_results=max_results,
            page_token=None,
        )
        _validate_job_reference(
            payload,
            billing_project=self._billing_project,
            location=plan.location,
            require=False,
            expected_job_id=job_id,
        )
        _validate_response_errors(payload, operation="query resume")
        yield from self._stream_query_results(
            plan,
            project=project,
            job_id=job_id,
            response_payload=payload,
            max_results=max_results,
            max_polls=max_polls,
            prefetch_one_page=prefetch_one_page,
        )

    def _validate_existing_job(
        self,
        *,
        project: str,
        job_id: str,
        plan: QueryPlan,
    ) -> None:
        url = (
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/jobs/"
            f"{quote(job_id, safe='')}"
        )
        payload = self._get_json_with_retries(
            url,
            params={"location": plan.location},
            operation="existing-job validation",
        )
        _validate_response_errors(payload, operation="existing-job validation")
        _validate_job_reference(
            payload,
            billing_project=self._billing_project,
            location=plan.location,
            require=True,
            expected_job_id=job_id,
        )
        configuration = payload.get("configuration")
        if not isinstance(configuration, Mapping):
            raise _execution_error("BigQuery existing job has invalid configuration")
        query = configuration.get("query")
        if not isinstance(query, Mapping):
            raise _execution_error("BigQuery existing job is not a query job")
        expected_parameters = [_to_rest_parameter(item) for item in plan.parameters]
        if (
            configuration.get("dryRun", False) is not False
            or query.get("query") != plan.sql
            or query.get("useLegacySql") is not False
            or query.get("useQueryCache") is not False
            or query.get("parameterMode") != "NAMED"
            or query.get("queryParameters") != expected_parameters
            or str(query.get("maximumBytesBilled")) != str(plan.maximum_bytes_billed)
            or configuration.get("labels") != {"app": "pumpquant", "workflow": "query_archive"}
        ):
            raise _execution_error("BigQuery existing job does not match the approved plan")
        status = payload.get("status")
        if isinstance(status, Mapping) and status.get("errorResult"):
            raise _execution_error("BigQuery existing job failed")

    def _stream_query_results(
        self,
        plan: QueryPlan,
        *,
        project: str,
        job_id: str,
        response_payload: Mapping[str, Any],
        max_results: int,
        max_polls: int,
        prefetch_one_page: bool,
    ) -> Iterator[QueryResultPage]:
        page_number = 0
        poll_count = 0
        seen_page_tokens: set[str] = set()
        # Prefetch is a one-worker optimisation. A single future
        # is kept at all times, so opaque page tokens can never be reordered and
        # the AuthorizedSession never has more than one GET in flight from this
        # stream. The executor is created only for an opted-in
        # future run.
        executor: ThreadPoolExecutor | None = None
        prefetched: Future[Mapping[str, Any]] | None = None
        if prefetch_one_page:
            executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pumpquant-bq-prefetch")
        try:
            while True:
                _validate_total_bytes_only(
                    response_payload,
                    maximum_bytes_billed=plan.maximum_bytes_billed,
                    operation="query response",
                )
                job_complete = _required_bool(
                    response_payload, "jobComplete", operation="query response"
                )
                if not job_complete:
                    if poll_count >= max_polls:
                        raise _execution_error("BigQuery query exceeded maximum poll count")
                    poll_count += 1
                    response_payload = self._get_query_results(
                        project=project,
                        job_id=job_id,
                        location=plan.location,
                        max_results=max_results,
                        page_token=None,
                    )
                    _validate_job_reference(
                        response_payload,
                        billing_project=self._billing_project,
                        location=plan.location,
                        require=False,
                        expected_job_id=job_id,
                    )
                    _validate_response_errors(response_payload, operation="query poll")
                    continue

                page, next_page_token = _page_from_payload(
                    response_payload,
                    job_id=job_id,
                    page_number=page_number,
                    maximum_bytes_billed=plan.maximum_bytes_billed,
                    operation="query response",
                )
                if next_page_token is not None and next_page_token in seen_page_tokens:
                    raise _execution_error("BigQuery query returned a repeated page token")
                if next_page_token is not None:
                    seen_page_tokens.add(next_page_token)

                # Start the single bounded prefetch before yielding this page,
                # allowing the consumer to process rows while the next GET is
                # in flight. Do not consume poll budget early when the caller
                # has set it to zero; that preserves the existing
                # first-page-then-budget-error behaviour.
                if executor is not None and next_page_token is not None and poll_count < max_polls:
                    poll_count += 1
                    prefetched = executor.submit(
                        self._get_query_results,
                        project=project,
                        job_id=job_id,
                        location=plan.location,
                        max_results=max_results,
                        page_token=next_page_token,
                    )

                yield page
                if next_page_token is None:
                    return

                if prefetched is not None:
                    # result is the only hand-off point from the worker; all
                    # response validation remains on the generator thread and
                    # therefore follows the non-prefetch path exactly.
                    response_payload = prefetched.result()
                    prefetched = None
                else:
                    if poll_count >= max_polls:
                        raise _execution_error("BigQuery query exceeded maximum poll count")
                    poll_count += 1
                    response_payload = self._get_query_results(
                        project=project,
                        job_id=job_id,
                        location=plan.location,
                        max_results=max_results,
                        page_token=next_page_token,
                    )
                _validate_job_reference(
                    response_payload,
                    billing_project=self._billing_project,
                    location=plan.location,
                    require=False,
                    expected_job_id=job_id,
                )
                _validate_response_errors(response_payload, operation="query page")
                page_number += 1
        finally:
            # A caller is allowed to stop a generator after any page. Cancel
            # queued work and wait for a running request before releasing the
            # executor, avoiding leaked worker threads or late session use.
            if prefetched is not None:
                prefetched.cancel()
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)

    def _get_query_results(
        self,
        *,
        project: str,
        job_id: str,
        location: str,
        max_results: int,
        page_token: str | None,
    ) -> Mapping[str, Any]:
        query_url = (
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries/"
            f"{quote(job_id, safe='')}"
        )
        params: dict[str, Any] = {
            "location": location,
            "maxResults": max_results,
            "timeoutMs": _QUERY_TIMEOUT_MS,
        }
        if page_token is not None:
            params["pageToken"] = page_token
        return self._get_json_with_retries(
            query_url,
            params=params,
            operation="query results request",
        )

    def _get_json_with_retries(
        self,
        url: str,
        *,
        params: Mapping[str, object],
        operation: str,
    ) -> Mapping[str, Any]:
        """Retry only idempotent BigQuery GETs, never query submission POSTs."""

        for attempt in range(_RESULT_GET_ATTEMPTS):
            try:
                response = self._session.get(
                    url,
                    params=dict(params),
                    timeout=_HTTP_TIMEOUT_SECONDS,
                )
            except Exception:
                if attempt + 1 == _RESULT_GET_ATTEMPTS:
                    raise _execution_error(f"BigQuery {operation} failed") from None
            else:
                status = _response_status(response)
                if status not in _TRANSIENT_HTTP_STATUSES:
                    try:
                        return _response_json(response, operation=operation)
                    except DryRunValidationError:
                        if not 200 <= status < 300 or attempt + 1 == _RESULT_GET_ATTEMPTS:
                            raise
                elif attempt + 1 == _RESULT_GET_ATTEMPTS:
                    raise _execution_error(f"BigQuery {operation} failed with HTTP {status}")
            sleep(min(0.5 * 2**attempt, 30.0))
        raise AssertionError("unreachable BigQuery GET retry state")
