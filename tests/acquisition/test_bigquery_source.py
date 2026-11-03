from __future__ import annotations

import json
import unittest
from collections.abc import Mapping, Sequence
from contextlib import redirect_stdout
from dataclasses import replace
from io import StringIO
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from pumpquant.acquisition.bigquery.client import (
    GoogleBigQueryBackend,
    _flatten_schema,
    _load_bigquery,
)
from pumpquant.acquisition.bigquery.source import (
    DEFAULT_MARKET_PROGRAM_IDS,
    BackendDryRun,
    CostCapExceeded,
    DatasetRef,
    DatasetSnapshot,
    DryRunValidationError,
    FieldInfo,
    QueryPlan,
    TableInfo,
    audit_dataset,
    build_daily_pump_instruction_plans,
    build_pump_instruction_plan,
    build_pump_transaction_plan,
    dataset_schema_sha256,
    dry_run,
    query_sha256,
    referenced_source_table_names,
    validate_plan_sql_provenance,
)
from pumpquant.cli import main

GIB = 1024**3


def _field(path: str, field_type: str, mode: str = "NULLABLE") -> FieldInfo:
    return FieldInfo(path, field_type, mode)


def _valid_snapshot() -> DatasetSnapshot:
    transactions = TableInfo(
        name="Transactions",
        location="us-central1",
        table_type="TABLE",
        partition_field="block_timestamp",
        partition_granularity="DAY",
        require_partition_filter=True,
        clustering_fields=("signature",),
        fields=(
            _field("block_slot", "INTEGER"),
            _field("block_hash", "STRING"),
            _field("block_timestamp", "TIMESTAMP"),
            _field("recent_block_hash", "STRING"),
            _field("signature", "STRING"),
            _field("index", "INTEGER"),
            _field("fee", "NUMERIC"),
            _field("status", "STRING"),
            _field("err", "STRING"),
            _field("compute_units_consumed", "NUMERIC"),
            _field("accounts", "RECORD", "REPEATED"),
            _field("accounts.pubkey", "STRING"),
            _field("accounts.signer", "BOOLEAN"),
            _field("accounts.writable", "BOOLEAN"),
            _field("log_messages", "STRING", "REPEATED"),
            _field("balance_changes", "RECORD", "REPEATED"),
            _field("pre_token_balances", "RECORD", "REPEATED"),
            _field("pre_token_balances.amount", "BIGNUMERIC"),
            _field("post_token_balances", "RECORD", "REPEATED"),
            _field("post_token_balances.amount", "BIGNUMERIC"),
        ),
        num_rows=554_117_480_627,
        logical_bytes=830_940_000_000_000,
    )
    instructions = TableInfo(
        name="Instructions",
        location="us-central1",
        table_type="TABLE",
        partition_field="block_timestamp",
        partition_granularity="DAY",
        require_partition_filter=True,
        clustering_fields=("program_id",),
        fields=(
            _field("block_slot", "INTEGER"),
            _field("block_hash", "STRING"),
            _field("block_timestamp", "TIMESTAMP"),
            _field("tx_signature", "STRING"),
            _field("index", "INTEGER"),
            _field("parent_index", "INTEGER"),
            _field("accounts", "STRING", "REPEATED"),
            _field("data", "STRING"),
            _field("parsed", "STRING"),
            _field("program", "STRING"),
            _field("program_id", "STRING"),
            _field("instruction_type", "STRING"),
            _field("params", "RECORD", "REPEATED"),
            _field("params.key", "STRING"),
            _field("params.value", "STRING"),
        ),
    )
    transfers = TableInfo(
        name="Token Transfers",
        location="us-central1",
        table_type="TABLE",
        partition_field="block_timestamp",
        partition_granularity="DAY",
        require_partition_filter=True,
        fields=(
            _field("block_slot", "INTEGER"),
            _field("block_hash", "STRING"),
            _field("block_timestamp", "TIMESTAMP"),
            _field("tx_signature", "STRING"),
            _field("source", "STRING"),
            _field("destination", "STRING"),
            _field("value", "NUMERIC"),
            _field("mint", "STRING"),
        ),
    )
    blocks = TableInfo(
        name="Blocks",
        location="us-central1",
        table_type="TABLE",
        partition_field="block_timestamp",
        partition_granularity="MONTH",
        fields=(
            _field("slot", "INTEGER"),
            _field("block_hash", "STRING"),
            _field("block_timestamp", "TIMESTAMP"),
            _field("previous_block_hash", "STRING"),
            _field("transaction_count", "INTEGER"),
        ),
    )
    accounts = TableInfo(
        name="Accounts",
        location="us-central1",
        table_type="TABLE",
        partition_field="block_timestamp",
        partition_granularity="MONTH",
        fields=(
            _field("block_slot", "INTEGER"),
            _field("block_hash", "STRING"),
            _field("block_timestamp", "TIMESTAMP"),
            _field("pubkey", "STRING"),
            _field("tx_signature", "STRING"),
            _field("retrieval_timestamp", "TIMESTAMP"),
            _field("program_data", "STRING"),
        ),
    )
    return DatasetSnapshot(
        ref=DatasetRef(),
        location="us-central1",
        tables=(
            transactions,
            instructions,
            transfers,
            blocks,
            accounts,
            TableInfo("Tokens", "us-central1", (), table_type="TABLE"),
            TableInfo("Block Rewards", "us-central1", (), table_type="TABLE"),
        ),
        captured_at="2026-07-13T16:00:00Z",
    )


class _FakeBackend:
    def __init__(self, estimated_bytes: int, snapshot: DatasetSnapshot | None = None) -> None:
        self.estimated_bytes = estimated_bytes
        self.snapshot = snapshot or _valid_snapshot()
        self.estimated_plans = 0

    def inspect_dataset(self, ref: DatasetRef) -> DatasetSnapshot:
        if ref != DatasetRef():
            raise AssertionError("unexpected dataset")
        return self.snapshot

    def estimate_query(self, plan: QueryPlan) -> BackendDryRun:
        self.estimated_plans += 1
        return BackendDryRun(self.estimated_bytes, plan.referenced_tables)


class _FakeResponse:
    def __init__(self, payload: object, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code

    def json(self) -> object:
        return self.payload


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self.response = response
        self.url: str | None = None
        self.body: dict[str, Any] | None = None
        self.timeout: int | None = None

    def post(self, url: str, *, json: dict[str, Any], timeout: int) -> _FakeResponse:
        self.url = url
        self.body = json
        self.timeout = timeout
        return self.response


class BigQuerySourceTests(unittest.TestCase):
    def test_missing_optional_dependency_has_actionable_error(self) -> None:
        original_import = __import__

        def blocked_import(
            name: str,
            globals: Mapping[str, object] | None = None,
            locals: Mapping[str, object] | None = None,
            fromlist: Sequence[str] = (),
            level: int = 0,
        ) -> object:
            if name == "google.cloud":
                raise ImportError("blocked for test")
            return original_import(name, globals, locals, fromlist, level)

        with (
            patch("builtins.__import__", side_effect=blocked_import),
            self.assertRaisesRegex(RuntimeError, r"pumpquant\[bigquery\]"),
        ):
            _load_bigquery()

    def test_google_adapter_builds_typed_dry_run_and_requires_statistics(self) -> None:
        session = _FakeSession(
            _FakeResponse(
                {
                    "jobComplete": True,
                    "totalBytesProcessed": str(8 * GIB),
                }
            )
        )
        plan = build_pump_transaction_plan(
            "2026-07-01", "2026-07-02", maximum_bytes_billed=10 * GIB
        )
        with patch(
            "pumpquant.acquisition.bigquery.client._load_bigquery", return_value=SimpleNamespace()
        ):
            backend = GoogleBigQueryBackend(
                "test-project", client=SimpleNamespace(), session=session
            )
            result = backend.estimate_query(plan)
        self.assertEqual(result.estimated_bytes, 8 * GIB)
        self.assertEqual(result.provenance_source, "static_sql_allowlist")
        self.assertEqual(result.referenced_tables, tuple(sorted(plan.referenced_tables)))
        self.assertEqual(
            session.url,
            "https://bigquery.googleapis.com/bigquery/v2/projects/test-project/queries",
        )
        self.assertIsNotNone(session.body)
        assert session.body is not None
        self.assertIs(session.body["dryRun"], True)
        self.assertIs(session.body["useQueryCache"], False)
        self.assertIs(session.body["useLegacySql"], False)
        self.assertEqual(session.body["location"], "us-central1")
        self.assertEqual(len(session.body["queryParameters"]), 3)
        self.assertNotIn("maximumBytesBilled", session.body)
        self.assertEqual(session.timeout, 60)

        session.response = _FakeResponse({"jobComplete": True})
        with self.assertRaisesRegex(DryRunValidationError, "no byte statistics"):
            backend.estimate_query(plan)

        session.response = _FakeResponse(
            {"error": {"status": "PERMISSION_DENIED"}}, status_code=403
        )
        with self.assertRaisesRegex(DryRunValidationError, "PERMISSION_DENIED"):
            backend.estimate_query(plan)

    def test_google_schema_flattener_retains_nested_exact_numeric_type(self) -> None:
        amount = SimpleNamespace(name="amount", field_type="BIGNUMERIC", mode="NULLABLE", fields=())
        balances = SimpleNamespace(
            name="pre_token_balances",
            field_type="RECORD",
            mode="REPEATED",
            fields=(amount,),
        )
        flattened = _flatten_schema((balances,))
        self.assertEqual(
            [(field.path, field.field_type, field.mode) for field in flattened],
            [
                ("pre_token_balances", "RECORD", "REPEATED"),
                ("pre_token_balances.amount", "BIGNUMERIC", "NULLABLE"),
            ],
        )

    def test_plan_uses_fixed_tables_and_typed_parameters(self) -> None:
        plan = build_pump_transaction_plan(
            "2026-07-01", "2026-07-02", maximum_bytes_billed=512 * GIB
        )
        self.assertNotIn("2026-07-01", plan.sql)
        self.assertNotIn(DEFAULT_MARKET_PROGRAM_IDS[0], plan.sql)
        self.assertIn("@start_ts", plan.sql)
        self.assertIn("@end_ts", plan.sql)
        self.assertIn("@program_ids", plan.sql)
        self.assertIn("bigquery-public-data.crypto_solana_mainnet_us.Instructions", plan.sql)
        self.assertIn("bigquery-public-data.crypto_solana_mainnet_us.Transactions", plan.sql)
        self.assertNotIn("SELECT *", plan.sql.upper())
        parameters = {item.name: item for item in plan.parameters}
        self.assertEqual(parameters["program_ids"].type_name, "ARRAY<STRING>")
        self.assertEqual(parameters["program_ids"].value, DEFAULT_MARKET_PROGRAM_IDS)

    def test_plan_preserves_failures_and_joins_on_chain_identity(self) -> None:
        plan = build_pump_transaction_plan(
            "2026-07-01", "2026-07-02", maximum_bytes_billed=512 * GIB
        )
        self.assertIn("t.status", plan.sql)
        self.assertIn("t.err", plan.sql)
        self.assertNotIn("status =", plan.sql.lower())
        self.assertIn("t.block_slot = b.block_slot", plan.sql)
        self.assertIn("t.block_hash IS NOT DISTINCT FROM b.block_hash", plan.sql)
        self.assertIn("t.signature = b.tx_signature", plan.sql)
        self.assertIn("chain_identity_complete", plan.sql)
        self.assertIn("source_instructions", plan.sql)

    def test_instruction_discovery_plan_uses_cluster_key_and_one_source(self) -> None:
        plan = build_pump_instruction_plan(
            "2026-07-01", "2026-07-02", maximum_bytes_billed=512 * GIB
        )
        self.assertEqual(plan.name, "pump_instruction_discovery_v1")
        self.assertEqual(
            plan.referenced_tables,
            ("bigquery-public-data.crypto_solana_mainnet_us.Instructions",),
        )
        self.assertIn("program_id IN UNNEST(@program_ids)", plan.sql)
        self.assertIn("instruction_type", plan.sql)
        self.assertIn("chain_identity_complete", plan.sql)
        self.assertNotIn("Transactions", plan.sql)
        self.assertTrue(any("hydration" in item for item in plan.limitations))
        self.assertEqual(validate_plan_sql_provenance(plan), plan.referenced_tables)

    def test_daily_instruction_plans_require_complete_utc_days_and_are_independent(self) -> None:
        plans = build_daily_pump_instruction_plans(
            "2026-06-07", "2026-07-07", maximum_bytes_billed=64 * GIB
        )
        self.assertEqual(len(plans), 30)
        self.assertEqual(plans[0].parameters[0].value, "2026-06-07T00:00:00Z")
        self.assertEqual(plans[0].parameters[1].value, "2026-06-08T00:00:00Z")
        self.assertEqual(plans[-1].parameters[1].value, "2026-07-07T00:00:00Z")
        self.assertEqual(len({query_sha256(plan) for plan in plans}), 30)
        with self.assertRaisesRegex(ValueError, "midnight UTC"):
            build_daily_pump_instruction_plans(
                "2026-06-07T01:00:00Z",
                "2026-06-08T01:00:00Z",
                maximum_bytes_billed=64 * GIB,
            )
        with self.assertRaisesRegex(ValueError, "cannot exceed 31"):
            build_daily_pump_instruction_plans(
                "2026-06-01", "2026-07-03", maximum_bytes_billed=64 * GIB
            )

    def test_static_sql_provenance_matches_allowlist_and_rejects_extra_source(self) -> None:
        plan = build_pump_transaction_plan(
            "2026-07-01", "2026-07-02", maximum_bytes_billed=512 * GIB
        )
        self.assertEqual(validate_plan_sql_provenance(plan), tuple(sorted(plan.referenced_tables)))
        injected = replace(plan, sql=plan.sql + "\nJOIN unexpected_table AS x ON TRUE")
        with self.assertRaisesRegex(DryRunValidationError, "unqualified"):
            validate_plan_sql_provenance(injected)

        comma_injected = replace(
            plan,
            sql=plan.sql.replace(
                "FROM `bigquery-public-data.crypto_solana_mainnet_us.Instructions` AS i",
                "FROM `bigquery-public-data.crypto_solana_mainnet_us.Instructions` AS i, "
                "`bigquery-public-data.crypto_solana_mainnet_us.Accounts` AS hidden",
            ),
        )
        with self.assertRaisesRegex(DryRunValidationError, "comma-join"):
            validate_plan_sql_provenance(comma_injected)

        comment_injected = replace(
            plan,
            sql="SELECT '`bigquery-public-data.crypto_solana_mainnet_us.Instructions`' "
            "FROM/**/other_dataset.other_table",
            referenced_tables=("bigquery-public-data.crypto_solana_mainnet_us.Instructions",),
        )
        with self.assertRaisesRegex(DryRunValidationError, "comments"):
            validate_plan_sql_provenance(comment_injected)

        missing_allowlist = replace(plan, referenced_tables=(plan.referenced_tables[0],))
        with self.assertRaisesRegex(DryRunValidationError, "provenance mismatch"):
            validate_plan_sql_provenance(missing_allowlist)

    def test_plan_rejects_unbounded_or_ambiguous_windows(self) -> None:
        with self.assertRaises(ValueError):
            build_pump_transaction_plan("2026-07-01", "2026-07-02", maximum_bytes_billed=0)
        with self.assertRaises(ValueError):
            build_pump_transaction_plan("2026-07-02", "2026-07-01", maximum_bytes_billed=GIB)
        with self.assertRaises(ValueError):
            build_pump_transaction_plan(
                "2026-07-01T00:00:00",
                "2026-07-02T00:00:00Z",
                maximum_bytes_billed=GIB,
            )

    def test_query_hash_is_deterministic_and_includes_cap(self) -> None:
        first = build_pump_transaction_plan("2026-07-01", "2026-07-02", maximum_bytes_billed=GIB)
        second = build_pump_transaction_plan("2026-07-01", "2026-07-02", maximum_bytes_billed=GIB)
        larger_cap = build_pump_transaction_plan(
            "2026-07-01", "2026-07-02", maximum_bytes_billed=2 * GIB
        )
        self.assertEqual(query_sha256(first), query_sha256(second))
        self.assertNotEqual(query_sha256(first), query_sha256(larger_cap))

    def test_dry_run_passes_under_cap_and_blocks_over_cap(self) -> None:
        plan = build_pump_transaction_plan(
            "2026-07-01", "2026-07-02", maximum_bytes_billed=10 * GIB
        )
        backend = _FakeBackend(9 * GIB)
        result = dry_run(backend, plan)
        self.assertTrue(result.under_cap)
        self.assertEqual(backend.estimated_plans, 1)

        over_backend = _FakeBackend(11 * GIB)
        with self.assertRaises(CostCapExceeded) as raised:
            dry_run(over_backend, plan)
        self.assertFalse(raised.exception.result.under_cap)
        self.assertEqual(over_backend.estimated_plans, 1)

    def test_dry_run_rejects_missing_cost_or_table_provenance(self) -> None:
        plan = build_pump_transaction_plan(
            "2026-07-01", "2026-07-02", maximum_bytes_billed=10 * GIB
        )
        with self.assertRaisesRegex(DryRunValidationError, "positive byte estimate"):
            dry_run(_FakeBackend(0), plan)

        class MissingTablesBackend(_FakeBackend):
            def estimate_query(self, plan: QueryPlan) -> BackendDryRun:
                return BackendDryRun(GIB)

        with self.assertRaisesRegex(DryRunValidationError, "referenced tables"):
            dry_run(MissingTablesBackend(GIB), plan)

        class WrongTableBackend(_FakeBackend):
            def estimate_query(self, plan: QueryPlan) -> BackendDryRun:
                return BackendDryRun(GIB, ("other.project.table",))

        with self.assertRaisesRegex(DryRunValidationError, "provenance mismatch"):
            dry_run(WrongTableBackend(GIB), plan)

    def test_sql_provenance_rejects_mutating_statements(self) -> None:
        base = build_pump_instruction_plan("2026-07-01", "2026-07-02", maximum_bytes_billed=GIB)
        table = "`bigquery-public-data.crypto_solana_mainnet_us.Instructions`"
        statements = (
            f"DELETE FROM {table} WHERE TRUE",
            f"UPDATE {table} SET program = NULL WHERE TRUE",
            (
                f"MERGE {table} AS target USING {table} AS source ON FALSE "
                "WHEN NOT MATCHED THEN INSERT ROW"
            ),
            f"CREATE TABLE scratch.copy AS SELECT block_slot FROM {table}",
            f"EXPORT DATA OPTIONS(uri=@start_ts) AS SELECT block_slot FROM {table}",
            f"CALL procedure_name((SELECT COUNT(*) FROM {table}))",
        )
        for sql in statements:
            with (
                self.subTest(sql=sql.split()[0]),
                self.assertRaisesRegex(DryRunValidationError, "read-only"),
            ):
                validate_plan_sql_provenance(replace(base, sql=sql))

    def test_sql_provenance_cannot_expand_the_declared_dataset_allowlist(self) -> None:
        base = build_pump_instruction_plan("2026-07-01", "2026-07-02", maximum_bytes_billed=GIB)
        foreign = replace(
            base,
            sql="SELECT value FROM `attacker-project.secret.dataset`",
            referenced_tables=("attacker-project.secret.dataset",),
        )
        with self.assertRaisesRegex(DryRunValidationError, "non-source table"):
            validate_plan_sql_provenance(foreign)

    def test_valid_metadata_has_warnings_but_no_contract_errors(self) -> None:
        snapshot = _valid_snapshot()
        issues = audit_dataset(snapshot)
        self.assertFalse(any(issue.severity == "error" for issue in issues))
        self.assertIn("lossless_raw_unavailable", {issue.code for issue in issues})
        self.assertEqual(dataset_schema_sha256(snapshot), dataset_schema_sha256(snapshot))

    def test_plan_scoped_audit_ignores_unreferenced_source_tables(self) -> None:
        snapshot = _valid_snapshot()
        two_table_snapshot = replace(
            snapshot,
            tables=tuple(
                table for table in snapshot.tables if table.name in {"Instructions", "Transactions"}
            ),
        )
        plan = build_pump_transaction_plan("2026-07-01", "2026-07-02", maximum_bytes_billed=GIB)
        issues = audit_dataset(
            two_table_snapshot, required_tables=referenced_source_table_names(plan)
        )
        self.assertFalse(any(issue.severity == "error" for issue in issues))
        self.assertTrue(
            any(issue.severity == "error" for issue in audit_dataset(two_table_snapshot))
        )

    def test_sql_selected_instruction_fields_are_in_contract(self) -> None:
        snapshot = _valid_snapshot()
        instructions = snapshot.table_map["Instructions"]
        broken = replace(
            snapshot,
            tables=tuple(
                replace(
                    table,
                    fields=tuple(field for field in instructions.fields if field.path != "program"),
                )
                if table.name == "Instructions"
                else table
                for table in snapshot.tables
            ),
        )
        issues = audit_dataset(broken, required_tables=("Instructions", "Transactions"))
        self.assertTrue(
            any(
                issue.code == "missing_field"
                and issue.table == "Instructions"
                and "program" in issue.message
                for issue in issues
            )
        )

    def test_missing_field_location_and_partitioning_are_hard_errors(self) -> None:
        snapshot = _valid_snapshot()
        transactions = snapshot.table_map["Transactions"]
        broken_transactions = replace(
            transactions,
            fields=tuple(field for field in transactions.fields if field.path != "index"),
            partition_field=None,
        )
        broken = replace(
            snapshot,
            location="US",
            tables=tuple(
                broken_transactions if table.name == "Transactions" else table
                for table in snapshot.tables
            ),
        )
        error_codes = {issue.code for issue in audit_dataset(broken) if issue.severity == "error"}
        self.assertIn("location_mismatch", error_codes)
        self.assertIn("missing_field", error_codes)
        self.assertIn("unsafe_partitioning", error_codes)

    def test_query_source_must_remain_a_native_base_table(self) -> None:
        snapshot = _valid_snapshot()
        broken = replace(
            snapshot,
            tables=tuple(
                replace(table, table_type="VIEW") if table.name == "Instructions" else table
                for table in snapshot.tables
            ),
        )
        issues = audit_dataset(broken, required_tables=("Instructions",))
        self.assertTrue(any(issue.code == "source_not_base_table" for issue in issues))

        omitted_type = replace(
            snapshot,
            tables=tuple(
                TableInfo(table.name, table.location, table.fields)
                if table.name == "Instructions"
                else table
                for table in snapshot.tables
            ),
        )
        omitted_issues = audit_dataset(omitted_type, required_tables=("Instructions",))
        self.assertTrue(any(issue.code == "source_not_base_table" for issue in omitted_issues))

    def test_big_numeric_amount_type_must_not_regress_to_float_like_type(self) -> None:
        snapshot = _valid_snapshot()
        transactions = snapshot.table_map["Transactions"]
        changed_fields = tuple(
            replace(field, field_type="FLOAT")
            if field.path == "pre_token_balances.amount"
            else field
            for field in transactions.fields
        )
        broken = replace(
            snapshot,
            tables=tuple(
                replace(table, fields=changed_fields) if table.name == "Transactions" else table
                for table in snapshot.tables
            ),
        )
        self.assertTrue(any(issue.code == "field_type_mismatch" for issue in audit_dataset(broken)))

    def test_bq_plan_cli_is_offline_and_parses_iec_limit(self) -> None:
        output = StringIO()
        with redirect_stdout(output):
            status = main(
                [
                    "bq-plan",
                    "--start",
                    "2026-07-01",
                    "--end",
                    "2026-07-02",
                    "--max-bytes",
                    "512GiB",
                ]
            )
        payload = json.loads(output.getvalue())
        self.assertEqual(status, 0)
        self.assertEqual(payload["maximum_bytes_billed"], 512 * GIB)
        self.assertFalse(payload["execution_enabled"])
        self.assertNotIn("sql", payload)

    def test_bq_plan_cli_can_select_instruction_discovery(self) -> None:
        output = StringIO()
        with redirect_stdout(output):
            status = main(
                [
                    "bq-plan",
                    "--plan",
                    "instruction-discovery",
                    "--start",
                    "2026-07-01",
                    "--end",
                    "2026-07-02",
                    "--max-bytes",
                    "512GiB",
                ]
            )
        payload = json.loads(output.getvalue())
        self.assertEqual(status, 0)
        self.assertEqual(payload["plan_name"], "pump_instruction_discovery_v1")
        self.assertEqual(len(payload["referenced_tables"]), 1)

    def test_bq_dry_run_cli_checks_metadata_then_reports_zero_billed_bytes(self) -> None:
        backend = _FakeBackend(8 * GIB)
        output = StringIO()
        with (
            patch("pumpquant.cli.bigquery._google_backend", return_value=backend),
            redirect_stdout(output),
        ):
            status = main(
                [
                    "bq-dry-run",
                    "--billing-project",
                    "test-project",
                    "--start",
                    "2026-07-01",
                    "--end",
                    "2026-07-02",
                    "--max-bytes",
                    "10GiB",
                ]
            )
        payload = json.loads(output.getvalue())
        self.assertEqual(status, 0)
        self.assertEqual(payload["status"], "dry_run_passed")
        self.assertEqual(payload["bytes_billed"], 0)
        self.assertFalse(payload["query_executed"])
        self.assertEqual(payload["dry_run"]["provenance_source"], "backend_reported")
        self.assertEqual(backend.estimated_plans, 1)

    def test_bq_batch_dry_run_enforces_cumulative_cap(self) -> None:
        backend = _FakeBackend(2 * GIB)
        output = StringIO()
        with (
            patch("pumpquant.cli.bigquery._google_backend", return_value=backend),
            redirect_stdout(output),
        ):
            status = main(
                [
                    "bq-batch-dry-run",
                    "--billing-project",
                    "test-project",
                    "--start",
                    "2026-06-01",
                    "--end",
                    "2026-06-04",
                    "--daily-max-bytes",
                    "3GiB",
                    "--total-max-bytes",
                    "5GiB",
                ]
            )
        payload = json.loads(output.getvalue())
        self.assertEqual(status, 5)
        self.assertEqual(payload["status"], "blocked_cumulative_cost_cap")
        self.assertEqual(payload["cumulative_estimated_bytes"], 6 * GIB)
        self.assertEqual(backend.estimated_plans, 3)
        self.assertFalse(payload["query_executed"])
        self.assertEqual(payload["bytes_billed"], 0)

        backend = _FakeBackend(GIB)
        output = StringIO()
        with (
            patch("pumpquant.cli.bigquery._google_backend", return_value=backend),
            redirect_stdout(output),
        ):
            status = main(
                [
                    "bq-batch-dry-run",
                    "--billing-project",
                    "test-project",
                    "--start",
                    "2026-06-01",
                    "--end",
                    "2026-06-04",
                    "--daily-max-bytes",
                    "2GiB",
                    "--total-max-bytes",
                    "4GiB",
                ]
            )
        payload = json.loads(output.getvalue())
        self.assertEqual(status, 0)
        self.assertEqual(payload["status"], "batch_dry_run_passed")
        self.assertEqual(payload["remaining_cumulative_bytes"], GIB)
        self.assertEqual(len(payload["days"]), 3)


if __name__ == "__main__":
    unittest.main()
