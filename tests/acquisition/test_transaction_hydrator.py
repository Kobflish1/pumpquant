from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from pumpquant.acquisition.transaction_hydrator import (
    HttpResponse,
    SolanaRpcConfig,
    hydrate_transactions,
    verify_hydration_segment,
)

SIG_A = "3" * 88
SIG_B = "4" * 88
SIG_C = "5" * 88


def _identifier(signature: str) -> str:
    return hashlib.sha256(signature.encode("ascii")).hexdigest()[:24]


def _item(signature: str, result: object) -> dict[str, object]:
    return {"jsonrpc": "2.0", "id": _identifier(signature), "result": result}


def _body(*items: dict[str, object]) -> bytes:
    return json.dumps(list(items), separators=(",", ":")).encode()


class _Clock:
    def __init__(self) -> None:
        self.current = datetime(2026, 7, 15, 12, tzinfo=UTC)

    def __call__(self) -> datetime:
        value = self.current
        self.current += timedelta(microseconds=1)
        return value


class _Transport:
    def __init__(self, *responses: HttpResponse | BaseException | Callable[[bytes], HttpResponse]):
        self.responses = list(responses)
        self.payloads: list[bytes] = []

    def post(self, payload: bytes, timeout_seconds: float) -> HttpResponse:
        self.payloads.append(payload)
        if timeout_seconds <= 0:
            raise AssertionError("timeout was not positive")
        if not self.responses:
            raise AssertionError("unexpected transport call")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        if callable(response):
            return response(payload)
        return response


def _transaction(slot: int = 123) -> dict[str, object]:
    return {
        "slot": slot,
        "blockTime": 1_700_000_000,
        "transaction": ["AQID", "base64"],
        "transactionIndex": 7,
        "version": 0,
        "meta": {
            "err": None,
            "fee": 5000,
            "computeUnitsConsumed": 99_001,
            "costUnits": 101_002,
            "innerInstructions": [{"index": 2, "instructions": []}],
            "loadedAddresses": {"readonly": ["r"], "writable": ["w"]},
            "logMessages": ["Program log: preserved"],
            "preBalances": [10],
            "postBalances": [5],
            "preTokenBalances": [],
            "postTokenBalances": [],
            "rewards": [],
            "status": {"Ok": None},
        },
    }


def _records(path: Path, name: str = "coverage.ndjson") -> list[dict[str, object]]:
    return [json.loads(line) for line in (path / name).read_text().splitlines()]


class TransactionHydratorTests(TestCase):
    def test_preserves_full_result_wire_body_and_redacts_secrets(self) -> None:
        config = SolanaRpcConfig(
            "https://rpc.example.test/private-api-key?token=query-secret",
            source_label="test-archive",
            headers=(("Authorization", "Bearer header-secret"),),
            batch_size=1,
            segment_size=1,
        )
        wire = _body(_item(SIG_A, _transaction()))
        transport = _Transport(HttpResponse(200, wire))
        with TemporaryDirectory() as directory:
            result = hydrate_transactions(
                [SIG_A],
                directory,
                config,
                transport=transport,
                clock=_Clock(),
                sleeper=lambda _: None,
            )
            self.assertEqual((result.found, result.unresolved), (1, 0))
            segment = result.segment_paths[0]
            verify_hydration_segment(segment)
            record = _records(segment)[0]
            self.assertEqual(record["rpc_response"], _item(SIG_A, _transaction()))
            wire_path = segment / record["wire_files"][0]  # type: ignore[operator]
            self.assertEqual(wire_path.read_bytes(), wire)
            archive_bytes = b"".join(
                path.read_bytes() for path in segment.rglob("*") if path.is_file()
            )
            self.assertNotIn(b"private-api-key", archive_bytes)
            self.assertNotIn(b"query-secret", archive_bytes)
            self.assertNotIn(b"header-secret", archive_bytes)
            self.assertNotIn("private-api-key", repr(config))
            self.assertNotIn("header-secret", repr(config))
            self.assertEqual(record["endpoint_origin"], "https://rpc.example.test")

    def test_http_retry_then_null_is_an_explicit_unresolved_record(self) -> None:
        sleeps: list[float] = []
        transport = _Transport(
            HttpResponse(429, b'{"rate":"limited"}', {"Retry-After": "0.25"}),
            HttpResponse(200, _body(_item(SIG_A, None))),
        )
        config = SolanaRpcConfig(
            "https://rpc.example.test/key",
            batch_size=1,
            segment_size=1,
            initial_backoff_seconds=0.1,
        )
        with TemporaryDirectory() as directory:
            result = hydrate_transactions(
                [SIG_A],
                directory,
                config,
                transport=transport,
                clock=_Clock(),
                sleeper=sleeps.append,
            )
            self.assertEqual((result.found, result.missing), (0, 1))
            self.assertEqual(sleeps, [0.25])
            record = _records(result.segment_paths[0])[0]
            self.assertEqual(record["attempt_count"], 2)
            self.assertEqual(record["status"], "missing")
            self.assertEqual(len(record["wire_files"]), 2)  # type: ignore[arg-type]
            self.assertEqual(_records(result.segment_paths[0], "exceptions.ndjson"), [record])

    def test_transient_rpc_error_retries_only_the_failed_signature(self) -> None:
        first = _body(
            _item(SIG_A, _transaction(1)),
            {
                "jsonrpc": "2.0",
                "id": _identifier(SIG_B),
                "error": {"code": -32005, "message": "busy"},
            },
        )
        second = _body(_item(SIG_B, _transaction(2)))
        transport = _Transport(HttpResponse(200, first), HttpResponse(200, second))
        config = SolanaRpcConfig(
            "https://rpc.example.test",
            batch_size=2,
            segment_size=2,
            initial_backoff_seconds=0,
        )
        with TemporaryDirectory() as directory:
            result = hydrate_transactions(
                [SIG_B, SIG_A],
                directory,
                config,
                transport=transport,
                clock=_Clock(),
                sleeper=lambda _: None,
            )
            self.assertEqual(result.found, 2)
            requests = [json.loads(payload) for payload in transport.payloads]
            self.assertEqual(len(requests[0]), 2)
            self.assertEqual([item["params"][0] for item in requests[1]], [SIG_B])
            records = _records(result.segment_paths[0])
            self.assertEqual([record["signature"] for record in records], [SIG_A, SIG_B])
            self.assertEqual([record["attempt_count"] for record in records], [1, 2])

    def test_invalid_protocol_exhaustion_and_transport_text_is_not_leaked(self) -> None:
        transport = _Transport(
            TimeoutError("https://rpc.example.test/secret-key"),
            HttpResponse(200, b"not-json"),
        )
        config = SolanaRpcConfig(
            "https://rpc.example.test/secret-key",
            batch_size=1,
            segment_size=1,
            max_attempts=2,
            initial_backoff_seconds=0,
        )
        with TemporaryDirectory() as directory:
            result = hydrate_transactions(
                [SIG_A],
                directory,
                config,
                transport=transport,
                clock=_Clock(),
                sleeper=lambda _: None,
            )
            self.assertEqual(result.protocol_error, 1)
            record = _records(result.segment_paths[0])[0]
            self.assertEqual(record["error_kind"], "invalid_json_rpc_response")
            self.assertNotIn(
                "secret-key",
                (result.segment_paths[0] / "coverage.ndjson").read_text(),
            )

    def test_resume_skips_only_verified_found_records(self) -> None:
        config = SolanaRpcConfig(
            "https://rpc.example.test",
            batch_size=2,
            segment_size=2,
        )
        clock = _Clock()
        with TemporaryDirectory() as directory:
            first_transport = _Transport(
                HttpResponse(200, _body(_item(SIG_A, _transaction()), _item(SIG_B, None)))
            )
            first = hydrate_transactions(
                [SIG_A, SIG_B],
                directory,
                config,
                transport=first_transport,
                clock=clock,
                sleeper=lambda _: None,
            )
            self.assertEqual((first.found, first.missing), (1, 1))
            second_transport = _Transport(
                HttpResponse(200, _body(_item(SIG_B, _transaction()), _item(SIG_C, _transaction())))
            )
            second = hydrate_transactions(
                [SIG_A, SIG_B, SIG_C, SIG_A],
                directory,
                config,
                transport=second_transport,
                clock=clock,
                sleeper=lambda _: None,
            )
            self.assertEqual((second.requested, second.skipped_resolved, second.found), (3, 1, 2))
            requested = json.loads(second_transport.payloads[0])
            self.assertEqual([item["params"][0] for item in requested], [SIG_B, SIG_C])

    def test_verifier_detects_tampering(self) -> None:
        config = SolanaRpcConfig(
            "https://rpc.example.test",
            batch_size=1,
            segment_size=1,
        )
        with TemporaryDirectory() as directory:
            result = hydrate_transactions(
                [SIG_A],
                directory,
                config,
                transport=_Transport(HttpResponse(200, _body(_item(SIG_A, _transaction())))),
                clock=_Clock(),
                sleeper=lambda _: None,
            )
            segment = result.segment_paths[0]
            verify_hydration_segment(segment)
            (segment / "coverage.ndjson").write_bytes(b"tampered\n")
            with self.assertRaisesRegex(ValueError, "integrity failure"):
                verify_hydration_segment(segment)

    def test_rejects_malformed_signature_before_transport(self) -> None:
        with (
            TemporaryDirectory() as directory,
            self.assertRaisesRegex(ValueError, "base58"),
        ):
            hydrate_transactions(
                ["not a signature"],
                directory,
                SolanaRpcConfig("https://rpc.example.test"),
                transport=_Transport(),
            )
