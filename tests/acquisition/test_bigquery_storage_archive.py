from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, skipUnless

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - exercised in minimal installs
    pa = None
    pq = None

from pumpquant.acquisition.bigquery.archive import QueryProvenance, load_archive_manifest
from pumpquant.acquisition.bigquery.storage_archive import (
    BigQueryStorageArchiveError,
    archive_completed_query_job,
    resolve_completed_query_job,
)

SCHEMA = {
    "fields": [
        {"name": "block_slot", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "block_timestamp", "type": "TIMESTAMP"},
        {"name": "amount", "type": "NUMERIC"},
        {
            "name": "accounts",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "pubkey", "type": "STRING"},
                {"name": "signer", "type": "BOOL"},
            ],
        },
    ]
}


def _job(total_rows: int = 4) -> dict[str, object]:
    return {
        "jobReference": {
            "projectId": "billing-project",
            "jobId": "job-1",
            "location": "us-central1",
        },
        "status": {"state": "DONE"},
        "configuration": {
            "query": {
                "query": "SELECT 1",
                "destination": {
                    "projectId": "billing-project",
                    "datasetId": "_anon",
                    "tableId": "results",
                },
            }
        },
        "schema": SCHEMA,
        "statistics": {
            "query": {
                "totalRows": str(total_rows),
                "totalBytesProcessed": "123",
            }
        },
    }


class _BigQuery:
    def __init__(self, job: object) -> None:
        self.job = job
        self.calls: list[tuple[str, str]] = []

    def get_job(self, job_id: str, *, location: str) -> object:
        self.calls.append((job_id, location))
        return self.job


class _BigQueryWithTable(_BigQuery):
    def __init__(self, job: object, table: object) -> None:
        super().__init__(job)
        self.table = table
        self.table_calls: list[str] = []

    def get_table(self, table: str) -> object:
        self.table_calls.append(table)
        return self.table


class _Rows:
    def __init__(self, pages: list[pa.Table]) -> None:
        self.pages = pages

    def rows(self, session: object) -> _Rows:
        del session
        return self


class _Storage:
    def __init__(self, pages: dict[str, list[pa.Table]]) -> None:
        self.pages = pages
        self.read_calls: list[str] = []
        self.session_calls: list[dict[str, object]] = []

    def create_read_session(
        self,
        *,
        parent: str,
        read_session: object,
        max_stream_count: int,
    ) -> object:
        self.session_calls.append(
            {
                "parent": parent,
                "read_session": read_session,
                "max_stream_count": max_stream_count,
            }
        )
        return {"streams": [{"name": name} for name in self.pages]}

    def read_rows(self, stream_name: str) -> _Rows:
        self.read_calls.append(stream_name)
        return _Rows(self.pages[stream_name])


def _table(slots: list[int]) -> pa.Table:
    return pa.table(
        {
            "block_slot": pa.array(slots, type=pa.int64()),
            "block_timestamp": pa.array(
                ["2026-06-07T00:00:00Z"] * len(slots), type=pa.string()
            ).cast(pa.timestamp("us", tz="UTC")),
            "amount": pa.array(["1.000000000"] * len(slots), type=pa.string()).cast(
                pa.decimal128(38, 9)
            ),
            "accounts": pa.array(
                [[{"pubkey": "wallet", "signer": True}]] * len(slots),
                type=pa.list_(
                    pa.struct(
                        [
                            pa.field("pubkey", pa.string()),
                            pa.field("signer", pa.bool_()),
                        ]
                    )
                ),
            ),
        }
    )


def _provenance(total_rows: int = 4) -> QueryProvenance:
    return QueryProvenance(
        plan_name="storage-test",
        plan_sha256="a" * 64,
        request_id="0f0eeaca-7f6d-52ec-a733-22a2d86c436a",
        job_id="job-1",
        billing_project="billing-project",
        location="us-central1",
        source_tables=("project.dataset.source",),
        source_contract_sha256="b" * 64,
        start="2026-06-07T00:00:00Z",
        end="2026-06-08T00:00:00Z",
        dry_run_estimated_bytes=123,
        maximum_bytes_billed=500,
        actual_bytes_processed=123,
        total_rows=total_rows,
    )


@skipUnless(pa is not None and pq is not None, "archive extra is not installed")
class BigQueryStorageArchiveTests(TestCase):
    def test_jobs_get_requires_done_anonymous_query(self) -> None:
        client = _BigQuery(_job())
        resolved = resolve_completed_query_job(
            client,
            billing_project="billing-project",
            job_id="job-1",
            location="us-central1",
        )
        self.assertEqual(client.calls, [("job-1", "us-central1")])
        self.assertEqual(resolved.destination.dataset, "_anon")
        self.assertEqual(resolved.total_rows, 4)

        bad = _job()
        assert isinstance(bad["configuration"], dict)
        assert isinstance(bad["configuration"]["query"], dict)
        bad["configuration"]["query"]["destination"]["datasetId"] = "user_dataset"
        with self.assertRaisesRegex(BigQueryStorageArchiveError, "anonymous"):
            resolve_completed_query_job(
                _BigQuery(bad),
                billing_project="billing-project",
                job_id="job-1",
                location="us-central1",
            )

    def test_destination_table_metadata_supplies_schema_and_rows(self) -> None:
        job = _job()
        del job["schema"]
        assert isinstance(job["statistics"], dict)
        del job["statistics"]["query"]["totalRows"]
        table = {
            "tableReference": {
                "projectId": "billing-project",
                "datasetId": "_anon",
                "tableId": "results",
            },
            "schema": SCHEMA,
            "numRows": "4",
            "etag": "etag-1",
        }
        client = _BigQueryWithTable(job, table)
        resolved = resolve_completed_query_job(
            client,
            billing_project="billing-project",
            job_id="job-1",
            location="us-central1",
        )
        self.assertEqual(client.table_calls, ["billing-project._anon.results"])
        self.assertEqual(resolved.total_rows, 4)
        self.assertEqual(resolved.destination_etag, "etag-1")

    def test_all_streams_are_read_and_manifest_is_last(self) -> None:
        bq = _BigQuery(_job())
        storage = _Storage({"stream-a": [_table([1, 2])], "stream-b": [_table([3, 4])]})
        with TemporaryDirectory() as directory:
            root = Path(directory) / "archive"
            manifest = archive_completed_query_job(
                billing_project="billing-project",
                job_id="job-1",
                location="us-central1",
                output_dir=root,
                provenance=_provenance(),
                max_streams=2,
                max_rows_per_shard=1,
                bigquery_client=bq,
                storage_client=storage,
            )
            self.assertEqual(set(storage.read_calls), {"stream-a", "stream-b"})
            self.assertEqual(storage.session_calls[0]["max_stream_count"], 2)
            request = storage.session_calls[0]["read_session"]
            if isinstance(request, dict):
                assert isinstance(request["read_options"], dict)
                options = request["read_options"]["arrow_serialization_options"]
                assert isinstance(options, dict)
                self.assertEqual(
                    options["buffer_compression"],
                    2,
                )
            self.assertEqual(manifest.row_count, 4)
            self.assertEqual(load_archive_manifest(root / "manifest.json"), manifest)
            self.assertFalse(list(root.rglob("*.partial")))
            table = pq.read_table(root / manifest.shards[0].path)
            self.assertEqual(table.schema.field("amount").type, pa.decimal128(38, 9))
            self.assertEqual(manifest.shards[0].min_block_slot, 1)
            self.assertTrue((root / "manifest.json").read_text(encoding="utf-8").endswith("\n"))

    def test_count_mismatch_cleans_staging(self) -> None:
        bq = _BigQuery(_job(total_rows=5))
        storage = _Storage({"stream-a": [_table([1, 2])], "stream-b": [_table([3, 4])]})
        with TemporaryDirectory() as directory:
            root = Path(directory) / "archive"
            with self.assertRaisesRegex(BigQueryStorageArchiveError, "totalRows"):
                archive_completed_query_job(
                    billing_project="billing-project",
                    job_id="job-1",
                    location="us-central1",
                    output_dir=root,
                    provenance=_provenance(total_rows=5),
                    max_streams=2,
                    bigquery_client=bq,
                    storage_client=storage,
                )
            self.assertFalse(root.exists())
            self.assertFalse((Path(directory) / ".archive.partial").exists())

    def test_no_resume_of_stale_staging(self) -> None:
        bq = _BigQuery(_job())
        storage = _Storage({"stream-a": [_table([1, 2])], "stream-b": [_table([3, 4])]})
        with TemporaryDirectory() as directory:
            root = Path(directory) / "archive"
            (Path(directory) / ".archive.partial").mkdir()
            with self.assertRaises(FileExistsError):
                archive_completed_query_job(
                    billing_project="billing-project",
                    job_id="job-1",
                    location="us-central1",
                    output_dir=root,
                    provenance=_provenance(),
                    bigquery_client=bq,
                    storage_client=storage,
                )
