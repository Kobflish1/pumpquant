from __future__ import annotations

import unittest
from dataclasses import replace
from threading import Event, Lock
from time import monotonic
from types import SimpleNamespace
from unittest.mock import patch

from pumpquant.acquisition.bigquery.client import (
    GoogleBigQueryBackend,
    QueryResultPage,
    _DryRunEvidence,
    _to_rest_parameter,
)
from pumpquant.acquisition.bigquery.source import (
    DryRunResult,
    DryRunValidationError,
    build_pump_instruction_plan,
    query_sha256,
)


class _FakeResponse:
    def __init__(self, payload: object, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code

    def json(self) -> object:
        if isinstance(self.payload, BaseException):
            raise self.payload
        return self.payload


class _FakeSession:
    def __init__(self, post_response: _FakeResponse, get_responses: list[_FakeResponse] = None):
        self.post_response = post_response
        self.get_responses = list(get_responses or [])
        self.post_calls: list[tuple[str, dict[str, object], int]] = []
        self.get_calls: list[tuple[str, dict[str, object], int]] = []

    def post(self, url: str, *, json: dict[str, object], timeout: int) -> _FakeResponse:
        self.post_calls.append((url, json, timeout))
        return self.post_response

    def get(self, url: str, *, params: dict[str, object], timeout: int) -> _FakeResponse:
        self.get_calls.append((url, params, timeout))
        if not self.get_responses:
            raise AssertionError("unexpected GET request")
        return self.get_responses.pop(0)


def _plan(cap: int = 100) -> object:
    return build_pump_instruction_plan("2026-07-01", "2026-07-02", maximum_bytes_billed=cap)


def _approval(plan: object, *, under_cap: bool = True, cap: int | None = None) -> DryRunResult:
    assert hasattr(plan, "referenced_tables")
    return DryRunResult(
        plan_sha256=query_sha256(plan),  # type: ignore[arg-type]
        estimated_bytes=10,
        maximum_bytes_billed=plan.maximum_bytes_billed if cap is None else cap,  # type: ignore[attr-defined]
        under_cap=under_cap,
        referenced_tables=plan.referenced_tables,  # type: ignore[attr-defined]
        provenance_source="static_sql_allowlist",
    )


def _job_reference(job_id: str = "job-1") -> dict[str, str]:
    return {"projectId": "billing-project", "jobId": job_id, "location": "us-central1"}


def _job_resource(plan: object, *, query: str | None = None) -> dict[str, object]:
    return {
        "jobReference": _job_reference(),
        "configuration": {
            "labels": {"app": "pumpquant", "workflow": "query_archive"},
            "query": {
                "query": plan.sql if query is None else query,  # type: ignore[attr-defined]
                "useLegacySql": False,
                "useQueryCache": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    _to_rest_parameter(item)
                    for item in plan.parameters  # type: ignore[attr-defined]
                ],
                "maximumBytesBilled": str(plan.maximum_bytes_billed),  # type: ignore[attr-defined]
            },
        },
        "status": {"state": "DONE"},
    }


def _completed(
    *, rows: list[dict[str, object]] | None = None, token: str | None = None
) -> dict[str, object]:
    payload: dict[str, object] = {
        "jobComplete": True,
        "schema": {"fields": [{"name": "value", "type": "STRING"}]},
        "totalRows": "1" if rows else "0",
        "totalBytesProcessed": "10",
    }
    if rows is not None:
        payload["rows"] = rows
    if token is not None:
        payload["pageToken"] = token
    return payload


class GoogleBigQueryExecutionTests(unittest.TestCase):
    def _backend(
        self, session: _FakeSession, *, seed_evidence: bool = True
    ) -> GoogleBigQueryBackend:
        with patch(
            "pumpquant.acquisition.bigquery.client._load_bigquery", return_value=SimpleNamespace()
        ):
            backend = GoogleBigQueryBackend(
                "billing-project", client=SimpleNamespace(), session=session
            )
        if seed_evidence:
            plan = _plan()
            backend._dry_run_evidence[query_sha256(plan)] = _DryRunEvidence(
                estimated_bytes=10,
                referenced_tables=plan.referenced_tables,
                provenance_source="static_sql_allowlist",
                captured_monotonic=monotonic(),
            )
        return backend

    def test_posts_guarded_named_query_and_streams_pages(self) -> None:
        plan = _plan()
        session = _FakeSession(
            _FakeResponse(
                {
                    "jobReference": _job_reference(),
                    **_completed(rows=[{"f": [{"v": "first"}]}], token="next-1"),
                }
            ),
            [
                _FakeResponse(
                    {
                        **_completed(rows=[{"f": [{"v": "second"}]}]),
                    }
                )
            ],
        )
        pages = list(self._backend(session).execute_query(plan, _approval(plan), max_results=7))
        self.assertEqual([page.page_number for page in pages], [0, 1])
        self.assertEqual(pages[0].rows[0]["f"][0]["v"], "first")
        self.assertIsInstance(pages[0], QueryResultPage)
        self.assertEqual(len(session.post_calls), 1)
        post_url, body, timeout = session.post_calls[0]
        self.assertEqual(
            post_url,
            "https://bigquery.googleapis.com/bigquery/v2/projects/billing-project/queries",
        )
        self.assertEqual(timeout, 60)
        self.assertIs(body["dryRun"], False)
        self.assertIs(body["useQueryCache"], False)
        self.assertIs(body["useLegacySql"], False)
        self.assertEqual(body["parameterMode"], "NAMED")
        self.assertEqual(body["location"], "us-central1")
        self.assertEqual(body["timeoutMs"], 30_000)
        self.assertEqual(body["maxResults"], 7)
        self.assertEqual(body["maximumBytesBilled"], "100")
        self.assertRegex(str(body["requestId"]), r"^[0-9a-f-]{36}$")
        self.assertEqual(
            body["labels"],
            {"app": "pumpquant", "workflow": "query_archive"},
        )
        self.assertEqual(len(body["queryParameters"]), 3)
        self.assertEqual(len(session.get_calls), 1)
        get_url, params, get_timeout = session.get_calls[0]
        self.assertTrue(get_url.endswith("/queries/job-1"))
        self.assertEqual(
            params,
            {
                "location": "us-central1",
                "maxResults": 7,
                "timeoutMs": 30_000,
                "pageToken": "next-1",
            },
        )
        self.assertEqual(get_timeout, 60)

    def test_incomplete_initial_response_is_polled(self) -> None:
        plan = _plan()
        session = _FakeSession(
            _FakeResponse({"jobReference": _job_reference(), "jobComplete": False}),
            [_FakeResponse(_completed(rows=[]))],
        )
        pages = list(self._backend(session).execute_query(plan, _approval(plan)))
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0].rows, ())
        self.assertEqual(
            session.get_calls[0][1],
            {"location": "us-central1", "maxResults": 10_000, "timeoutMs": 30_000},
        )

    def test_result_get_retries_transient_failure_without_resubmitting(self) -> None:
        plan = _plan()
        session = _FakeSession(
            _FakeResponse(
                {
                    "jobReference": _job_reference(),
                    **_completed(rows=[], token="next-1"),
                }
            ),
            [
                _FakeResponse({}, status_code=503),
                _FakeResponse(_completed(rows=[])),
            ],
        )
        with patch("pumpquant.acquisition.bigquery.client.sleep") as mocked_sleep:
            pages = list(self._backend(session).execute_query(plan, _approval(plan)))
        self.assertEqual(len(pages), 2)
        self.assertEqual(len(session.post_calls), 1)
        self.assertEqual(len(session.get_calls), 2)
        mocked_sleep.assert_called_once_with(0.5)

    def test_empty_result_requires_schema_but_allows_missing_rows(self) -> None:
        plan = _plan()
        session = _FakeSession(
            _FakeResponse({"jobReference": _job_reference(), **_completed(rows=None)})
        )
        page = next(self._backend(session).execute_query(plan, _approval(plan)))
        self.assertEqual(page.rows, ())
        self.assertEqual(page.total_rows, 0)

    def test_approval_and_provenance_are_checked_before_post(self) -> None:
        plan = _plan()
        session = _FakeSession(_FakeResponse({}))
        backend = self._backend(session)
        bad_approvals = [
            replace(_approval(plan), plan_sha256="wrong"),
            replace(_approval(plan), under_cap=False),
            replace(_approval(plan), maximum_bytes_billed=101),
            replace(_approval(plan), referenced_tables=()),
            replace(_approval(plan), estimated_bytes=0),
        ]
        for approval in bad_approvals:
            with self.subTest(approval=approval):
                with self.assertRaises(DryRunValidationError):
                    backend.execute_query(plan, approval)
                self.assertEqual(session.post_calls, [])
        duck_typed = SimpleNamespace(
            plan_sha256=query_sha256(plan),
            under_cap=True,
            maximum_bytes_billed=plan.maximum_bytes_billed,
            referenced_tables=plan.referenced_tables,
            estimated_bytes=10,
            provenance_source="static_sql_allowlist",
        )
        with self.assertRaises(DryRunValidationError):
            backend.execute_query(plan, duck_typed)  # type: ignore[arg-type]
        bad_plan = replace(plan, sql=plan.sql + "\nJOIN unregistered.table AS x ON TRUE")
        with self.assertRaises(DryRunValidationError):
            backend.execute_query(bad_plan, _approval(bad_plan))
        self.assertEqual(session.post_calls, [])

    def test_forged_approval_without_backend_evidence_cannot_execute(self) -> None:
        plan = _plan()
        session = _FakeSession(_FakeResponse({}))
        backend = self._backend(session, seed_evidence=False)
        with self.assertRaisesRegex(DryRunValidationError, "backend-issued"):
            backend.execute_query(plan, _approval(plan))
        self.assertEqual(session.post_calls, [])

    def test_job_reference_is_required_and_bound_to_request(self) -> None:
        plan = _plan()
        for reference in [
            None,
            {"projectId": "other", "jobId": "job-1", "location": "us-central1"},
            {"projectId": "billing-project", "jobId": "job-1", "location": "EU"},
        ]:
            payload: dict[str, object] = {"jobComplete": False}
            if reference is not None:
                payload["jobReference"] = reference
            with self.subTest(reference=reference):
                session = _FakeSession(_FakeResponse(payload))
                with self.assertRaises(DryRunValidationError):
                    next(self._backend(session).execute_query(plan, _approval(plan)))
                self.assertEqual(session.get_calls, [])

        without_location = _FakeSession(
            _FakeResponse(
                {
                    "jobReference": {
                        "projectId": "billing-project",
                        "jobId": "job-1",
                    },
                    **_completed(rows=[]),
                }
            )
        )
        self.assertEqual(
            next(self._backend(without_location).execute_query(plan, _approval(plan))).job_id,
            "job-1",
        )

    def test_warning_array_does_not_abort_successful_result(self) -> None:
        plan = _plan()
        session = _FakeSession(
            _FakeResponse(
                {
                    "jobReference": _job_reference(),
                    "errors": [{"reason": "warning", "message": "not surfaced"}],
                    **_completed(rows=[]),
                }
            )
        )
        self.assertEqual(len(list(self._backend(session).execute_query(plan, _approval(plan)))), 1)

    def test_resume_validates_existing_job_without_submitting_query(self) -> None:
        plan = _plan()
        session = _FakeSession(
            _FakeResponse({}),
            [
                _FakeResponse(_job_resource(plan)),
                _FakeResponse({"jobReference": _job_reference(), **_completed(rows=[])}),
            ],
        )
        pages = list(self._backend(session).resume_query(plan, "job-1"))
        self.assertEqual(len(pages), 1)
        self.assertEqual(session.post_calls, [])
        self.assertIn("/jobs/job-1", session.get_calls[0][0])
        self.assertIn("/queries/job-1", session.get_calls[1][0])

    def test_resume_rejects_job_from_different_query(self) -> None:
        plan = _plan()
        session = _FakeSession(
            _FakeResponse({}),
            [_FakeResponse(_job_resource(plan, query="SELECT 1"))],
        )
        with self.assertRaisesRegex(DryRunValidationError, "does not match"):
            list(self._backend(session).resume_query(plan, "job-1"))
        self.assertEqual(len(session.get_calls), 1)

    def test_fail_closed_errors_do_not_include_payload_or_row_values(self) -> None:
        plan = _plan()
        secret = "secret-row-value-and-header"
        cases = [
            _FakeResponse({"error": {"message": secret}}, status_code=403),
            _FakeResponse(RuntimeError(secret)),
            _FakeResponse(
                {
                    "jobReference": _job_reference(),
                    **_completed(rows=[]),
                    "totalBytesProcessed": "101",
                }
            ),
            _FakeResponse(
                {"jobReference": _job_reference(), "jobComplete": True, "rows": [{"v": secret}]}
            ),
        ]
        for response in cases:
            with self.subTest(response=response):
                session = _FakeSession(response)
                with self.assertRaises(DryRunValidationError) as raised:
                    next(self._backend(session).execute_query(plan, _approval(plan)))
                self.assertNotIn(secret, str(raised.exception))

    def test_repeated_page_tokens_and_poll_budget_fail_closed(self) -> None:
        plan = _plan()
        repeated = _FakeSession(
            _FakeResponse(
                {
                    "jobReference": _job_reference(),
                    **_completed(rows=[], token="same"),
                }
            ),
            [_FakeResponse({**_completed(rows=[], token="same")})],
        )
        with self.assertRaisesRegex(DryRunValidationError, "repeated page token"):
            list(self._backend(repeated).execute_query(plan, _approval(plan)))

        incomplete = _FakeSession(
            _FakeResponse({"jobReference": _job_reference(), "jobComplete": False})
        )
        with self.assertRaisesRegex(DryRunValidationError, "maximum poll"):
            list(self._backend(incomplete).execute_query(plan, _approval(plan), max_polls=0))

    def test_one_page_prefetch_preserves_opaque_token_order_and_output(self) -> None:
        plan = _plan()
        first = {
            "jobReference": _job_reference(),
            **_completed(rows=[{"f": [{"v": "first"}]}], token="opaque-a"),
        }
        second = {
            "jobReference": _job_reference(),
            **_completed(rows=[{"f": [{"v": "second"}]}], token="opaque-b"),
        }
        third = {"jobReference": _job_reference(), **_completed(rows=[{"f": [{"v": "third"}]}])}
        session = _FakeSession(_FakeResponse(first), [_FakeResponse(second), _FakeResponse(third)])
        pages = list(
            self._backend(session).execute_query(
                plan,
                _approval(plan),
                prefetch_one_page=True,
            )
        )
        self.assertEqual([page.page_number for page in pages], [0, 1, 2])
        self.assertEqual(
            [page.rows[0]["f"][0]["v"] for page in pages],
            ["first", "second", "third"],
        )
        self.assertEqual(
            [call[1]["pageToken"] for call in session.get_calls],
            ["opaque-a", "opaque-b"],
        )

    def test_one_page_prefetch_has_only_one_get_in_flight_and_cleans_up(self) -> None:
        plan = _plan()
        started = Event()
        release = Event()
        lock = Lock()
        active = 0
        peak = 0

        class TrackingSession(_FakeSession):
            def get(self, url: str, *, params: dict[str, object], timeout: int) -> _FakeResponse:
                nonlocal active, peak
                with lock:
                    active += 1
                    peak = max(peak, active)
                started.set()
                release.wait(timeout=5)
                try:
                    return super().get(url, params=params, timeout=timeout)
                finally:
                    with lock:
                        active -= 1

        session = TrackingSession(
            _FakeResponse(
                {
                    "jobReference": _job_reference(),
                    **_completed(rows=[], token="opaque-a"),
                }
            ),
            [_FakeResponse(_completed(rows=[]))],
        )
        iterator = self._backend(session).execute_query(
            plan,
            _approval(plan),
            prefetch_one_page=True,
        )
        self.assertEqual(next(iterator).page_number, 0)
        self.assertTrue(started.wait(timeout=5))
        self.assertEqual(peak, 1)
        release.set()
        self.assertEqual([page.page_number for page in iterator], [1])
        iterator.close()
        self.assertEqual(active, 0)

    def test_one_page_prefetch_supports_resume(self) -> None:
        plan = _plan()
        session = _FakeSession(
            _FakeResponse({}),
            [
                _FakeResponse(_job_resource(plan)),
                _FakeResponse(
                    {
                        "jobReference": _job_reference(),
                        **_completed(rows=[{"f": [{"v": "first"}]}], token="opaque-a"),
                    }
                ),
                _FakeResponse(
                    {
                        "jobReference": _job_reference(),
                        **_completed(rows=[{"f": [{"v": "second"}]}]),
                    }
                ),
            ],
        )
        pages = list(
            self._backend(session).resume_query(
                plan,
                "job-1",
                prefetch_one_page=True,
            )
        )
        self.assertEqual([page.page_number for page in pages], [0, 1])
        self.assertEqual(
            [call[1].get("pageToken") for call in session.get_calls],
            [None, None, "opaque-a"],
        )


if __name__ == "__main__":
    unittest.main()
