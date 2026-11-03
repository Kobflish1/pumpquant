from __future__ import annotations

import threading
import unittest
from urllib.parse import urlparse

from pumpquant.acquisition.bigquery.parallel import (
    ParallelReadError,
    ReadOnlyBigQueryTabledataReader,
    RowRange,
    TableReference,
    iter_indexed_rows,
    plan_row_ranges,
    probe_start_index_semantics,
    read_rows_in_parallel,
)


class _Response:
    def __init__(self, payload: object, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code

    def json(self) -> object:
        return self.payload


def _page_fetcher(values: list[dict[str, object]], *, ignore_start: bool = False):
    """Return a deterministic variable-size tabledata.list fake."""

    def fetch(start_index: int | None, page_token: str | None, max_results: int):
        if page_token is None:
            cursor = 0 if ignore_start else (0 if start_index is None else start_index)
        else:
            if not page_token.startswith("next:"):
                raise AssertionError("unexpected token")
            cursor = int(page_token.removeprefix("next:"))
        # Deliberately return fewer rows than requested to exercise pageToken
        # continuation and the range-boundary check.
        end = min(cursor + min(max_results, 2), len(values))
        payload: dict[str, object] = {
            "totalRows": str(len(values)),
            "rows": values[cursor:end],
        }
        if end < len(values):
            payload["pageToken"] = f"next:{end}"
        return payload

    return fetch


class BigQueryParallelTests(unittest.TestCase):
    def test_plan_ranges_are_contiguous_and_exact(self) -> None:
        self.assertEqual(
            plan_row_ranges(250, range_rows=100),
            (RowRange(0, 100), RowRange(100, 200), RowRange(200, 250)),
        )
        self.assertEqual(plan_row_ranges(0), ())
        with self.assertRaises(ValueError):
            plan_row_ranges(-1)

    def test_parallel_reader_proves_count_and_preserves_source_offsets(self) -> None:
        # Duplicate values are legal.  They must remain separate because the
        # half-open source range, rather than a content hash, is row identity.
        values = [{"value": index % 3} for index in range(11)]
        starts: list[int | None] = []
        lock = threading.Lock()
        base = _page_fetcher(values)

        def fetch(start_index: int | None, page_token: str | None, max_results: int):
            with lock:
                starts.append(start_index)
            return base(start_index, page_token, max_results)

        chunks = list(
            read_rows_in_parallel(
                len(values),
                fetch,
                range_rows=4,
                page_rows=3,
                max_workers=3,
            )
        )
        self.assertEqual(
            [chunk.row_range for chunk in chunks],
            [RowRange(0, 4), RowRange(4, 8), RowRange(8, 11)],
        )
        flattened = [row["value"] for chunk in chunks for row in chunk.rows]
        self.assertEqual(flattened, [value["value"] for value in values])
        indexed = list(iter_indexed_rows(iter(chunks)))
        self.assertEqual([offset for offset, _ in indexed], list(range(len(values))))
        self.assertEqual(
            sorted(value for value in starts if value is not None),
            [0, 4, 8],
        )

    def test_parallel_reader_rejects_gap_before_range_end(self) -> None:
        def no_token(start_index: int | None, page_token: str | None, max_results: int):
            del page_token, max_results
            return {"totalRows": "4", "rows": [{"value": start_index}]}

        with self.assertRaisesRegex(ParallelReadError, "ended before"):
            list(read_rows_in_parallel(4, no_token, range_rows=4, page_rows=2))

    def test_parallel_reader_rejects_repeated_page_token(self) -> None:
        def repeated(start_index: int | None, page_token: str | None, max_results: int):
            del start_index, max_results
            if page_token is None:
                return {"totalRows": "2", "rows": [{"value": 1}], "pageToken": "same"}
            return {"totalRows": "2", "rows": [{"value": 1}], "pageToken": "same"}

        with self.assertRaisesRegex(ParallelReadError, "repeated pageToken"):
            list(read_rows_in_parallel(2, repeated, range_rows=2, page_rows=1))

    def test_indexed_rows_reject_chunk_gap_or_overlap(self) -> None:
        chunks = list(
            read_rows_in_parallel(
                4,
                _page_fetcher([{"value": 0}, {"value": 1}, {"value": 2}, {"value": 3}]),
                range_rows=2,
                page_rows=2,
            )
        )
        with self.assertRaisesRegex(ParallelReadError, "gap or overlap"):
            list(iter_indexed_rows(iter([chunks[1]])))

    def test_probe_requires_stable_hashes_and_distinct_adjacent_rows(self) -> None:
        values = [{"value": index} for index in range(8)]
        report = probe_start_index_semantics(8, _page_fetcher(values), probe_rows=2)
        self.assertTrue(report.repeated_hash_matches)
        self.assertEqual(report.cross_range_duplicate_hashes, 0)
        self.assertTrue(report.start_index_semantics_proven)

        ignored = probe_start_index_semantics(
            8,
            _page_fetcher(values, ignore_start=True),
            probe_rows=2,
        )
        self.assertTrue(ignored.repeated_hash_matches)
        self.assertGreater(ignored.cross_range_duplicate_hashes, 0)
        self.assertFalse(ignored.start_index_semantics_proven)

        duplicate_values = [{"value": 1} for _ in range(8)]
        ambiguous = probe_start_index_semantics(8, _page_fetcher(duplicate_values), probe_rows=2)
        self.assertFalse(ambiguous.start_index_semantics_proven)

    def test_probe_rejects_unstable_repeated_range(self) -> None:
        values = [{"value": index} for index in range(8)]
        calls = 0
        base = _page_fetcher(values)

        def unstable(start_index: int | None, page_token: str | None, max_results: int):
            nonlocal calls
            calls += 1
            payload = dict(base(start_index, page_token, max_results))
            if calls > 2 and payload.get("rows"):
                payload["rows"] = [{"value": "changed"}]
            return payload

        report = probe_start_index_semantics(8, unstable, probe_rows=2)
        self.assertFalse(report.repeated_hash_matches)
        self.assertFalse(report.start_index_semantics_proven)


class _ReadOnlySession:
    def __init__(self, values: list[dict[str, object]]) -> None:
        self.values = values
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.post_calls = 0
        self.lock = threading.Lock()

    def post(self, *args: object, **kwargs: object) -> None:
        self.post_calls += 1
        raise AssertionError("parallel reader must never submit a query")

    def get(self, url: str, *, params: dict[str, object], timeout: int) -> _Response:
        del timeout
        with self.lock:
            self.calls.append((url, dict(params)))
        if "/jobs/job-1" in url:
            return _Response(
                {
                    "jobReference": {
                        "projectId": "billing-project",
                        "jobId": "job-1",
                        "location": "us-central1",
                    },
                    "configuration": {
                        "dryRun": False,
                        "query": {
                            "destinationTable": {
                                "projectId": "billing-project",
                                "datasetId": "_anonymous",
                                "tableId": "results_1",
                            }
                        },
                    },
                    "status": {"state": "DONE"},
                }
            )
        if "/datasets/_anonymous/tables/results_1/data" in url:
            if "pageToken" in params:
                cursor = int(str(params["pageToken"]).removeprefix("next:"))
            else:
                cursor = int(str(params["startIndex"]))
            max_results = int(str(params["maxResults"]))
            end = min(cursor + min(max_results, 2), len(self.values))
            payload: dict[str, object] = {
                "totalRows": str(len(self.values)),
                "rows": self.values[cursor:end],
            }
            if end < len(self.values):
                payload["pageToken"] = f"next:{end}"
            return _Response(payload)
        if "/datasets/_anonymous/tables/results_1" in url:
            return _Response({"numRows": str(len(self.values)), "etag": "table-etag"})
        raise AssertionError(f"unexpected GET URL: {url} {params}")


class ReadOnlyReaderTests(unittest.TestCase):
    def test_existing_job_reader_is_get_only_and_binds_destination(self) -> None:
        session = _ReadOnlySession([{"value": index} for index in range(7)])
        reader = ReadOnlyBigQueryTabledataReader(
            "billing-project",
            location="us-central1",
            session_factory=lambda: session,
        )
        chunks = list(reader.read_existing_job("job-1", range_rows=3, page_rows=2, max_workers=2))
        self.assertEqual([row["value"] for chunk in chunks for row in chunk.rows], list(range(7)))
        self.assertEqual(session.post_calls, 0)
        self.assertTrue(session.calls)
        self.assertTrue(all(urlparse(url).scheme == "https" for url, _ in session.calls))
        starts = [
            params["startIndex"]
            for url, params in session.calls
            if "/data" in url and "startIndex" in params
        ]
        self.assertEqual(sorted(str(value) for value in starts), ["0", "3", "6"])

    def test_existing_job_reader_rejects_destination_mismatch(self) -> None:
        session = _ReadOnlySession([])
        reader = ReadOnlyBigQueryTabledataReader(
            "billing-project",
            location="us-central1",
            session_factory=lambda: session,
        )
        wrong = TableReference("billing-project", "_anonymous", "different")
        with self.assertRaisesRegex(ParallelReadError, "differs"):
            reader.read_existing_job("job-1", destination=wrong)
        self.assertEqual(session.post_calls, 0)

    def test_named_destination_requires_explicit_opt_in(self) -> None:
        session = _ReadOnlySession([])
        reader = ReadOnlyBigQueryTabledataReader(
            "billing-project",
            location="us-central1",
            session_factory=lambda: session,
        )
        with self.assertRaisesRegex(ParallelReadError, "anonymous destination"):
            reader.read_table(TableReference("billing-project", "named_dataset", "table"))
        self.assertEqual(session.calls, [])


if __name__ == "__main__":
    unittest.main()
