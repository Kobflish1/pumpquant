"""Cost-guarded planning for the community Solana BigQuery dataset.

The module is dependency-free.  It can build and audit plans without Google
credentials; :mod:`pumpquant.google_bigquery` contains the optional adapter.
No function here executes a billable query or exports data.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Protocol, TypeAlias

SOURCE_PROJECT = "bigquery-public-data"
SOURCE_DATASET = "crypto_solana_mainnet_us"
SOURCE_LOCATION = "us-central1"

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMPSWAP_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
PUMP_FEE_PROGRAM_ID = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ"
DEFAULT_MARKET_PROGRAM_IDS = (PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID)
MAX_DAILY_BACKFILL_DAYS = 31
MAX_DAILY_BACKFILL_BYTES = 64 * 1024**3
MAX_SANDBOX_MONTHLY_BYTES = 1024**4

ParameterValue: TypeAlias = str | int | tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DatasetRef:
    """A fixed BigQuery project/dataset reference."""

    project: str = SOURCE_PROJECT
    dataset: str = SOURCE_DATASET

    def __post_init__(self) -> None:
        if not self.project.strip() or not self.dataset.strip():
            raise ValueError("project and dataset must be non-empty")

    @property
    def full_name(self) -> str:
        return f"{self.project}.{self.dataset}"


@dataclass(frozen=True, slots=True)
class FieldInfo:
    """A flattened BigQuery field, including nested paths."""

    path: str
    field_type: str
    mode: str

    def __post_init__(self) -> None:
        if not self.path.strip():
            raise ValueError("field path must be non-empty")
        if not self.field_type.strip() or not self.mode.strip():
            raise ValueError("field type and mode must be non-empty")


@dataclass(frozen=True, slots=True)
class TableInfo:
    """Zero-scan table metadata used to gate extraction plans."""

    name: str
    location: str
    fields: tuple[FieldInfo, ...]
    table_type: str = "UNKNOWN"
    partition_field: str | None = None
    partition_granularity: str | None = None
    require_partition_filter: bool = False
    clustering_fields: tuple[str, ...] = ()
    num_rows: int | None = None
    logical_bytes: int | None = None
    created_at: str | None = None
    modified_at: str | None = None

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.location.strip() or not self.table_type.strip():
            raise ValueError("table name, location, and type must be non-empty")
        for label, value in (("num_rows", self.num_rows), ("logical_bytes", self.logical_bytes)):
            if value is not None and value < 0:
                raise ValueError(f"{label} cannot be negative")

    @property
    def field_map(self) -> dict[str, FieldInfo]:
        return {field.path: field for field in self.fields}


@dataclass(frozen=True, slots=True)
class DatasetSnapshot:
    """A point-in-time metadata snapshot, not a data-completeness claim."""

    ref: DatasetRef
    location: str
    tables: tuple[TableInfo, ...]
    captured_at: str

    def __post_init__(self) -> None:
        if not self.location.strip() or not self.captured_at.strip():
            raise ValueError("location and captured_at must be non-empty")

    @property
    def table_map(self) -> dict[str, TableInfo]:
        return {table.name: table for table in self.tables}


@dataclass(frozen=True, slots=True)
class DatasetContractIssue:
    """A machine-readable metadata contract finding."""

    severity: str
    code: str
    message: str
    table: str | None = None

    def __post_init__(self) -> None:
        if self.severity not in {"error", "warning"}:
            raise ValueError("severity must be 'error' or 'warning'")


@dataclass(frozen=True, slots=True)
class QueryParameter:
    """A named Standard SQL parameter; values never enter SQL text."""

    name: str
    type_name: str
    value: ParameterValue

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.type_name.strip():
            raise ValueError("parameter name and type must be non-empty")


@dataclass(frozen=True, slots=True)
class QueryPlan:
    """An immutable, bounded query plan."""

    name: str
    sql: str
    parameters: tuple[QueryParameter, ...]
    maximum_bytes_billed: int
    location: str
    referenced_tables: tuple[str, ...]
    limitations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.sql.strip() or not self.location.strip():
            raise ValueError("plan name, SQL, and location must be non-empty")
        if self.maximum_bytes_billed <= 0:
            raise ValueError("maximum_bytes_billed must be positive")
        if "SELECT *" in self.sql.upper():
            raise ValueError("SELECT * is forbidden in BigQuery plans")
        names = [parameter.name for parameter in self.parameters]
        if len(names) != len(set(names)):
            raise ValueError("query parameter names must be unique")


@dataclass(frozen=True, slots=True)
class BackendDryRun:
    """Raw non-billable estimate returned by a backend adapter."""

    estimated_bytes: int
    referenced_tables: tuple[str, ...] = ()
    provenance_source: str = "backend_reported"

    def __post_init__(self) -> None:
        if self.estimated_bytes < 0:
            raise ValueError("estimated_bytes cannot be negative")
        if not self.provenance_source.strip():
            raise ValueError("provenance_source must be non-empty")


@dataclass(frozen=True, slots=True)
class DryRunResult:
    """Auditable result of comparing a dry-run estimate to its hard cap."""

    plan_sha256: str
    estimated_bytes: int
    maximum_bytes_billed: int
    under_cap: bool
    referenced_tables: tuple[str, ...]
    provenance_source: str


class BigQueryBackend(Protocol):
    """Small adapter surface, allowing network-free tests."""

    def inspect_dataset(self, ref: DatasetRef) -> DatasetSnapshot: ...

    def estimate_query(self, plan: QueryPlan) -> BackendDryRun: ...


class CostCapExceeded(RuntimeError):
    """Raised after a dry run proves the registered cap would be exceeded."""

    def __init__(self, result: DryRunResult) -> None:
        self.result = result
        super().__init__(
            "dry-run estimate "
            f"{result.estimated_bytes} exceeds maximum_bytes_billed "
            f"{result.maximum_bytes_billed}"
        )


class DryRunValidationError(RuntimeError):
    """Raised when a backend dry run lacks credible cost/provenance evidence."""


@dataclass(frozen=True, slots=True)
class _RequiredField:
    field_type: str
    mode: str | None = None


_REQUIRED_FIELDS: dict[str, dict[str, _RequiredField]] = {
    "Transactions": {
        "block_slot": _RequiredField("INTEGER"),
        "block_hash": _RequiredField("STRING"),
        "block_timestamp": _RequiredField("TIMESTAMP"),
        "recent_block_hash": _RequiredField("STRING"),
        "signature": _RequiredField("STRING"),
        "index": _RequiredField("INTEGER"),
        "fee": _RequiredField("NUMERIC"),
        "status": _RequiredField("STRING"),
        "err": _RequiredField("STRING"),
        "compute_units_consumed": _RequiredField("NUMERIC"),
        "accounts": _RequiredField("RECORD", "REPEATED"),
        "accounts.pubkey": _RequiredField("STRING"),
        "accounts.signer": _RequiredField("BOOLEAN"),
        "accounts.writable": _RequiredField("BOOLEAN"),
        "log_messages": _RequiredField("STRING", "REPEATED"),
        "balance_changes": _RequiredField("RECORD", "REPEATED"),
        "pre_token_balances": _RequiredField("RECORD", "REPEATED"),
        "pre_token_balances.amount": _RequiredField("BIGNUMERIC"),
        "post_token_balances": _RequiredField("RECORD", "REPEATED"),
        "post_token_balances.amount": _RequiredField("BIGNUMERIC"),
    },
    "Instructions": {
        "block_slot": _RequiredField("INTEGER"),
        "block_hash": _RequiredField("STRING"),
        "block_timestamp": _RequiredField("TIMESTAMP"),
        "tx_signature": _RequiredField("STRING"),
        "index": _RequiredField("INTEGER"),
        "parent_index": _RequiredField("INTEGER"),
        "accounts": _RequiredField("STRING", "REPEATED"),
        "data": _RequiredField("STRING"),
        "parsed": _RequiredField("STRING"),
        "program": _RequiredField("STRING"),
        "program_id": _RequiredField("STRING"),
        "instruction_type": _RequiredField("STRING"),
        "params": _RequiredField("RECORD", "REPEATED"),
        "params.key": _RequiredField("STRING"),
        "params.value": _RequiredField("STRING"),
    },
    "Token Transfers": {
        "block_slot": _RequiredField("INTEGER"),
        "block_hash": _RequiredField("STRING"),
        "block_timestamp": _RequiredField("TIMESTAMP"),
        "tx_signature": _RequiredField("STRING"),
        "source": _RequiredField("STRING"),
        "destination": _RequiredField("STRING"),
        "value": _RequiredField("NUMERIC"),
        "mint": _RequiredField("STRING"),
    },
    "Blocks": {
        "slot": _RequiredField("INTEGER"),
        "block_hash": _RequiredField("STRING"),
        "block_timestamp": _RequiredField("TIMESTAMP"),
        "previous_block_hash": _RequiredField("STRING"),
        "transaction_count": _RequiredField("INTEGER"),
    },
    "Accounts": {
        "block_slot": _RequiredField("INTEGER"),
        "block_hash": _RequiredField("STRING"),
        "block_timestamp": _RequiredField("TIMESTAMP"),
        "pubkey": _RequiredField("STRING"),
        "tx_signature": _RequiredField("STRING"),
        "retrieval_timestamp": _RequiredField("TIMESTAMP"),
        "program_data": _RequiredField("STRING"),
    },
    "Tokens": {},
    "Block Rewards": {},
}

_QUERY_CRITICAL_PARTITIONS = {
    "Transactions": ("block_timestamp", "DAY", True),
    "Instructions": ("block_timestamp", "DAY", True),
    "Token Transfers": ("block_timestamp", "DAY", True),
}


def dataset_schema_sha256(snapshot: DatasetSnapshot) -> str:
    """Hash table/field/layout metadata deterministically."""
    payload = {
        "ref": snapshot.ref.full_name,
        "location": snapshot.location,
        "tables": [
            {
                "name": table.name,
                "location": table.location,
                "table_type": table.table_type,
                "partition_field": table.partition_field,
                "partition_granularity": table.partition_granularity,
                "require_partition_filter": table.require_partition_filter,
                "clustering_fields": list(table.clustering_fields),
                "fields": [
                    {
                        "path": field.path,
                        "type": field.field_type.upper(),
                        "mode": field.mode.upper(),
                    }
                    for field in sorted(table.fields, key=lambda item: item.path)
                ],
            }
            for table in sorted(snapshot.tables, key=lambda item: item.name)
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def audit_dataset(
    snapshot: DatasetSnapshot,
    *,
    required_tables: tuple[str, ...] | None = None,
) -> tuple[DatasetContractIssue, ...]:
    """Compare live metadata with the full or plan-scoped source contract."""
    issues: list[DatasetContractIssue] = []
    if required_tables is None:
        selected_tables = set(_REQUIRED_FIELDS)
    else:
        selected_tables = set(required_tables)
        if not selected_tables:
            raise ValueError("required_tables cannot be empty")
        unknown_tables = selected_tables.difference(_REQUIRED_FIELDS)
        if unknown_tables:
            unknown = ", ".join(sorted(unknown_tables))
            raise ValueError(f"no source contract is defined for: {unknown}")
    if snapshot.ref != DatasetRef():
        issues.append(
            DatasetContractIssue(
                "error",
                "unexpected_dataset",
                f"expected {DatasetRef().full_name}, got {snapshot.ref.full_name}",
            )
        )
    if snapshot.location.lower() != SOURCE_LOCATION:
        issues.append(
            DatasetContractIssue(
                "error",
                "location_mismatch",
                f"expected dataset location {SOURCE_LOCATION}, got {snapshot.location}",
            )
        )

    tables = snapshot.table_map
    for table_name, required_fields in _REQUIRED_FIELDS.items():
        if table_name not in selected_tables:
            continue
        table = tables.get(table_name)
        if table is None:
            issues.append(
                DatasetContractIssue(
                    "error", "missing_table", f"required table {table_name!r} is absent", table_name
                )
            )
            continue
        if table.location.lower() != SOURCE_LOCATION:
            issues.append(
                DatasetContractIssue(
                    "error",
                    "table_location_mismatch",
                    f"expected {SOURCE_LOCATION}, got {table.location}",
                    table_name,
                )
            )
        if table.table_type.upper() != "TABLE":
            issues.append(
                DatasetContractIssue(
                    "error",
                    "source_not_base_table",
                    f"expected a native TABLE, got {table.table_type}",
                    table_name,
                )
            )
        field_map = table.field_map
        for path, requirement in required_fields.items():
            field = field_map.get(path)
            if field is None:
                issues.append(
                    DatasetContractIssue(
                        "error", "missing_field", f"required field {path!r} is absent", table_name
                    )
                )
                continue
            if field.field_type.upper() != requirement.field_type:
                issues.append(
                    DatasetContractIssue(
                        "error",
                        "field_type_mismatch",
                        f"{path} expected {requirement.field_type}, got {field.field_type}",
                        table_name,
                    )
                )
            if requirement.mode is not None and field.mode.upper() != requirement.mode:
                issues.append(
                    DatasetContractIssue(
                        "error",
                        "field_mode_mismatch",
                        f"{path} expected {requirement.mode}, got {field.mode}",
                        table_name,
                    )
                )

    for table_name, expected in _QUERY_CRITICAL_PARTITIONS.items():
        if table_name not in selected_tables:
            continue
        table = tables.get(table_name)
        if table is None:
            continue
        partition_field, granularity, filter_required = expected
        actual_granularity = (table.partition_granularity or "").upper()
        if table.partition_field != partition_field or actual_granularity != granularity:
            issues.append(
                DatasetContractIssue(
                    "error",
                    "unsafe_partitioning",
                    f"expected {granularity} partitioning on {partition_field}",
                    table_name,
                )
            )
        if filter_required and not table.require_partition_filter:
            issues.append(
                DatasetContractIssue(
                    "error",
                    "partition_filter_not_required",
                    "source no longer enforces its required timestamp partition filter",
                    table_name,
                )
            )

    issues.extend(
        (
            DatasetContractIssue(
                "warning",
                "community_source_no_sla",
                "the Solana dataset is community-maintained and has no completeness SLA",
            ),
            DatasetContractIssue(
                "warning",
                "lossless_raw_unavailable",
                "serialized transactions, all signatures, ALT definitions, return data, CPI stack "
                "height, exact account writes, source commitment, and local receipt time "
                "are absent",
            ),
        )
    )
    if "Accounts" in selected_tables:
        issues.append(
            DatasetContractIssue(
                "warning",
                "account_snapshots_not_writes",
                "Accounts rows are retrieval snapshots, not exact account writes with "
                "write versions",
                "Accounts",
            )
        )
    return tuple(issues)


def _as_utc_timestamp(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min, tzinfo=UTC)
    else:
        candidate = value.strip()
        if len(candidate) == 10:
            parsed = datetime.combine(date.fromisoformat(candidate), time.min, tzinfo=UTC)
        else:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamps must include a UTC offset (or use YYYY-MM-DD)")
    normalized = parsed.astimezone(UTC).isoformat()
    return normalized.replace("+00:00", "Z")


def _validated_window_and_programs(
    start: str | date | datetime,
    end: str | date | datetime,
    maximum_bytes_billed: int,
    program_ids: tuple[str, ...],
) -> tuple[str, str, tuple[str, ...]]:
    start_ts = _as_utc_timestamp(start)
    end_ts = _as_utc_timestamp(end)
    start_value = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
    end_value = datetime.fromisoformat(end_ts.replace("Z", "+00:00"))
    if end_value <= start_value:
        raise ValueError("end must be later than start")
    if maximum_bytes_billed <= 0:
        raise ValueError("maximum_bytes_billed must be positive")
    normalized_program_ids = tuple(dict.fromkeys(value.strip() for value in program_ids))
    if not normalized_program_ids or any(not value for value in normalized_program_ids):
        raise ValueError("program_ids must contain non-empty values")
    return start_ts, end_ts, normalized_program_ids


def build_pump_instruction_plan(
    start: str | date | datetime,
    end: str | date | datetime,
    *,
    maximum_bytes_billed: int,
    program_ids: tuple[str, ...] = DEFAULT_MARKET_PROGRAM_IDS,
) -> QueryPlan:
    """Plan the clustered, low-cost discovery extraction for market instructions."""
    start_ts, end_ts, normalized_program_ids = _validated_window_and_programs(
        start, end, maximum_bytes_billed, program_ids
    )
    instructions = f"`{SOURCE_PROJECT}.{SOURCE_DATASET}.Instructions`"
    sql = f"""SELECT
  block_slot,
  block_hash,
  block_timestamp,
  tx_signature,
  index AS source_index,
  parent_index AS source_parent_index,
  accounts,
  data,
  parsed,
  program,
  program_id,
  instruction_type,
  params,
  (block_slot IS NOT NULL AND block_hash IS NOT NULL AND tx_signature IS NOT NULL)
    AS chain_identity_complete
FROM {instructions}
WHERE block_timestamp >= @start_ts
  AND block_timestamp < @end_ts
  AND program_id IN UNNEST(@program_ids)
"""
    return QueryPlan(
        name="pump_instruction_discovery_v1",
        sql=sql,
        parameters=(
            QueryParameter("start_ts", "TIMESTAMP", start_ts),
            QueryParameter("end_ts", "TIMESTAMP", end_ts),
            QueryParameter("program_ids", "ARRAY<STRING>", normalized_program_ids),
        ),
        maximum_bytes_billed=maximum_bytes_billed,
        location=SOURCE_LOCATION,
        referenced_tables=(f"{SOURCE_PROJECT}.{SOURCE_DATASET}.Instructions",),
        limitations=(
            "discovery rows include only Pump/PumpSwap instructions, not sibling instructions",
            "transaction status, errors, logs, fees, and balance changes require later hydration",
            "historical seed only; this is not a lossless raw Solana transaction archive",
            "block_timestamp is chain time and must not populate observed_at_ns",
            "source instruction index and parent_index do not prove total CPI execution order",
        ),
    )


def build_daily_pump_instruction_plans(
    start: str | date | datetime,
    end: str | date | datetime,
    *,
    maximum_bytes_billed: int,
    program_ids: tuple[str, ...] = DEFAULT_MARKET_PROGRAM_IDS,
) -> tuple[QueryPlan, ...]:
    """Split a bounded UTC window into independently capped daily plans.

    Daily jobs keep REST query results below BigQuery's per-job result limits,
    make retries idempotent, and ensure a single failure cannot consume the
    remaining monthly Sandbox allowance.  Only complete UTC days are accepted.
    """
    if maximum_bytes_billed > MAX_DAILY_BACKFILL_BYTES:
        raise ValueError(f"daily backfill cap cannot exceed {MAX_DAILY_BACKFILL_BYTES} bytes")
    start_ts, end_ts, normalized_program_ids = _validated_window_and_programs(
        start, end, maximum_bytes_billed, program_ids
    )
    start_value = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
    end_value = datetime.fromisoformat(end_ts.replace("Z", "+00:00"))
    if start_value.time() != time.min or end_value.time() != time.min:
        raise ValueError("daily backfill boundaries must be midnight UTC")
    duration = end_value - start_value
    if duration.seconds or duration.microseconds:
        raise ValueError("daily backfill window must contain complete UTC days")
    if duration.days > MAX_DAILY_BACKFILL_DAYS:
        raise ValueError(f"daily backfill cannot exceed {MAX_DAILY_BACKFILL_DAYS} UTC days")
    return tuple(
        build_pump_instruction_plan(
            start_value + timedelta(days=offset),
            start_value + timedelta(days=offset + 1),
            maximum_bytes_billed=maximum_bytes_billed,
            program_ids=normalized_program_ids,
        )
        for offset in range(duration.days)
    )


def build_pump_transaction_plan(
    start: str | date | datetime,
    end: str | date | datetime,
    *,
    maximum_bytes_billed: int,
    program_ids: tuple[str, ...] = DEFAULT_MARKET_PROGRAM_IDS,
) -> QueryPlan:
    """Plan one row per Pump-related transaction while retaining all instructions.

    The first CTE benefits from ``Instructions`` clustering by ``program_id``.
    The second pass intentionally retains every source instruction for matched
    transactions; ``index``/``parent_index`` are not treated as a complete CPI
    execution order.
    """
    start_ts, end_ts, normalized_program_ids = _validated_window_and_programs(
        start, end, maximum_bytes_billed, program_ids
    )

    instructions = f"`{SOURCE_PROJECT}.{SOURCE_DATASET}.Instructions`"
    transactions = f"`{SOURCE_PROJECT}.{SOURCE_DATASET}.Transactions`"
    sql = f"""WITH matching_signatures AS (
  SELECT
    block_slot,
    block_hash,
    tx_signature,
    ARRAY_AGG(DISTINCT program_id IGNORE NULLS) AS matched_program_ids
  FROM {instructions}
  WHERE block_timestamp >= @start_ts
    AND block_timestamp < @end_ts
    AND program_id IN UNNEST(@program_ids)
  GROUP BY block_slot, block_hash, tx_signature
),
instruction_bundles AS (
  SELECT
    i.block_slot,
    i.block_hash,
    i.tx_signature,
    ANY_VALUE(m.matched_program_ids) AS matched_program_ids,
    ARRAY_AGG(
      STRUCT(
        i.index AS source_index,
        i.parent_index AS source_parent_index,
        i.accounts AS accounts,
        i.data AS data,
        i.parsed AS parsed,
        i.program AS program,
        i.program_id AS program_id,
        i.instruction_type AS instruction_type,
        i.params AS params
      )
      ORDER BY i.index, IFNULL(i.parent_index, -1), i.program_id, i.data
    ) AS source_instructions
  FROM {instructions} AS i
  INNER JOIN matching_signatures AS m
    ON i.block_slot = m.block_slot
    AND i.block_hash IS NOT DISTINCT FROM m.block_hash
    AND i.tx_signature = m.tx_signature
  WHERE i.block_timestamp >= @start_ts
    AND i.block_timestamp < @end_ts
  GROUP BY i.block_slot, i.block_hash, i.tx_signature
)
SELECT
  t.block_slot,
  t.block_hash,
  t.block_timestamp,
  t.recent_block_hash,
  t.signature,
  t.index AS tx_index,
  t.fee,
  t.status,
  t.err,
  t.compute_units_consumed,
  t.accounts,
  t.log_messages,
  t.balance_changes,
  t.pre_token_balances,
  t.post_token_balances,
  (t.block_slot IS NOT NULL AND t.block_hash IS NOT NULL AND t.signature IS NOT NULL)
    AS chain_identity_complete,
  b.matched_program_ids,
  b.source_instructions
FROM {transactions} AS t
INNER JOIN instruction_bundles AS b
  ON t.block_slot = b.block_slot
  AND t.block_hash IS NOT DISTINCT FROM b.block_hash
  AND t.signature = b.tx_signature
WHERE t.block_timestamp >= @start_ts
  AND t.block_timestamp < @end_ts
"""
    return QueryPlan(
        name="pump_transaction_bundle_v1",
        sql=sql,
        parameters=(
            QueryParameter("start_ts", "TIMESTAMP", start_ts),
            QueryParameter("end_ts", "TIMESTAMP", end_ts),
            QueryParameter("program_ids", "ARRAY<STRING>", normalized_program_ids),
        ),
        maximum_bytes_billed=maximum_bytes_billed,
        location=SOURCE_LOCATION,
        referenced_tables=(
            f"{SOURCE_PROJECT}.{SOURCE_DATASET}.Instructions",
            f"{SOURCE_PROJECT}.{SOURCE_DATASET}.Transactions",
        ),
        limitations=(
            "historical seed only; this is not a lossless raw Solana transaction archive",
            "block_timestamp is chain time and must not populate observed_at_ns",
            "source instruction index and parent_index do not prove total CPI execution order",
            "instruction data can be null when the upstream parser supplied only parsed JSON",
        ),
    )


def query_sha256(plan: QueryPlan) -> str:
    """Hash SQL, typed parameters, location, and the registered cost cap."""
    payload = {
        "name": plan.name,
        "sql": plan.sql,
        "parameters": [
            {"name": item.name, "type": item.type_name, "value": item.value}
            for item in plan.parameters
        ],
        "maximum_bytes_billed": plan.maximum_bytes_billed,
        "location": plan.location,
        "referenced_tables": plan.referenced_tables,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def referenced_source_table_names(plan: QueryPlan) -> tuple[str, ...]:
    """Return contracted source table IDs and reject foreign identifiers."""
    prefix = f"{SOURCE_PROJECT}.{SOURCE_DATASET}."
    names: list[str] = []
    for table in plan.referenced_tables:
        if not table.startswith(prefix):
            raise ValueError(f"plan references a non-source table: {table}")
        name = table.removeprefix(prefix)
        if name not in _REQUIRED_FIELDS:
            raise ValueError(f"no source contract is defined for: {name}")
        names.append(name)
    if not names:
        raise ValueError("plan must reference at least one source table")
    return tuple(dict.fromkeys(names))


_CTE_NAME = re.compile(r"(?:\bWITH|,)\s*([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(", re.IGNORECASE)
_SQL_SOURCE = re.compile(
    r"(?<!DISTINCT )\b(?:FROM|JOIN)\s+(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)
_QUOTED_IDENTIFIER = re.compile(r"`([^`]+)`")
_FROM_CLAUSE = re.compile(
    r"(?<!DISTINCT )\bFROM\b(?P<body>.*?)(?="
    r"\bWHERE\b|\bGROUP\s+BY\b|\bHAVING\b|\bQUALIFY\b|"
    r"\bORDER\s+BY\b|\bLIMIT\b|\bUNION\b|$)",
    re.IGNORECASE,
)
_READ_ONLY_START = re.compile(r"^\s*(?:SELECT|WITH)\b", re.IGNORECASE)
_MUTATING_SQL_TOKEN = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|EXPORT|LOAD|CALL|"
    r"GRANT|REVOKE|EXECUTE|DECLARE|SET|BEGIN|COMMIT|ROLLBACK)\b",
    re.IGNORECASE,
)


def validate_plan_sql_provenance(plan: QueryPlan) -> tuple[str, ...]:
    """Extract SQL sources and require an exact match with the plan allowlist.

    BigQuery ``jobs.query`` supports read-only OAuth and ``dryRun=true``, but
    its response does not include ``referencedTables``.  The live CLI therefore
    combines this strict SQL-source check with its metadata contract, which also
    requires every referenced source to remain a native base table.
    """
    # Reject any plan whose declared provenance escapes the one registered
    # public Solana dataset before attempting SQL token extraction.
    try:
        referenced_source_table_names(plan)
    except ValueError as error:
        raise DryRunValidationError(str(error)) from None

    # Plans are generated with parameters, so they need no literal strings,
    # comments, or multiple statements. Reject those constructs instead of
    # trying to implement a partial GoogleSQL lexer that could be confused by
    # source-looking text or comment-as-whitespace tricks.
    unsupported_fragments = ("'", '"', "--", "/*", "*/", ";")
    if any(fragment in plan.sql for fragment in unsupported_fragments):
        raise DryRunValidationError(
            "SQL literals, comments, and multiple statements are not supported "
            "by the static provenance verifier"
        )
    if _READ_ONLY_START.match(plan.sql) is None or _MUTATING_SQL_TOKEN.search(plan.sql):
        raise DryRunValidationError("SQL must be one read-only SELECT/WITH query")

    normalized_sql = re.sub(r"\s+", " ", plan.sql)
    cte_names = {match.group(1).lower() for match in _CTE_NAME.finditer(normalized_sql)}
    quoted_identifiers = [match.group(1) for match in _QUOTED_IDENTIFIER.finditer(plan.sql)]
    if plan.sql.count("`") != 2 * len(quoted_identifiers):
        raise DryRunValidationError("SQL contains an unterminated quoted identifier")

    # The generated plans never use comma joins. Rejecting that syntax keeps
    # this small static verifier fail-closed: otherwise a second source after a
    # comma would not have its own FROM/JOIN keyword and could evade extraction.
    for match in _FROM_CLAUSE.finditer(normalized_sql):
        if "," in match.group("body"):
            raise DryRunValidationError("SQL comma-join sources are not supported")

    # Generated SQL uses backticks only for physical table identifiers. Scan
    # every quoted identifier as well as FROM/JOIN tokens so comments or other
    # whitespace tricks cannot hide an additional physical source.
    sources: list[str] = list(quoted_identifiers)
    for match in _SQL_SOURCE.finditer(normalized_sql):
        token = match.group(1)
        if token.startswith("`"):
            continue
        if token.lower() not in cte_names:
            raise DryRunValidationError(
                f"SQL contains an unqualified or unregistered source: {token!r}"
            )
    actual = tuple(sorted(set(sources)))
    expected = tuple(sorted(set(plan.referenced_tables)))
    if not actual:
        raise DryRunValidationError("SQL provenance check found no source tables")
    if actual != expected:
        raise DryRunValidationError(
            f"SQL table provenance mismatch: expected {expected!r}, got {actual!r}"
        )
    return actual


def dry_run(backend: BigQueryBackend, plan: QueryPlan) -> DryRunResult:
    """Perform a non-billable estimate and enforce the registered cap locally."""
    estimate = backend.estimate_query(plan)
    if estimate.estimated_bytes <= 0:
        raise DryRunValidationError(
            "dry run returned no positive byte estimate for a native-table query"
        )
    expected_tables = tuple(sorted(set(plan.referenced_tables)))
    actual_tables = tuple(sorted(set(estimate.referenced_tables)))
    if not actual_tables:
        raise DryRunValidationError("dry run did not report referenced tables")
    if actual_tables != expected_tables:
        raise DryRunValidationError(
            "dry-run table provenance mismatch: "
            f"expected {expected_tables!r}, got {actual_tables!r}"
        )
    result = DryRunResult(
        plan_sha256=query_sha256(plan),
        estimated_bytes=estimate.estimated_bytes,
        maximum_bytes_billed=plan.maximum_bytes_billed,
        under_cap=estimate.estimated_bytes <= plan.maximum_bytes_billed,
        referenced_tables=actual_tables,
        provenance_source=estimate.provenance_source,
    )
    if not result.under_cap:
        raise CostCapExceeded(result)
    return result
