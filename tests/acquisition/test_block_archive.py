from __future__ import annotations

import gzip
import json
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from pumpquant.acquisition.block_archive import (
    BlockArchiveConfig,
    archive_full_blocks,
    verify_block_segment,
)
from pumpquant.acquisition.transaction_hydrator import HttpResponse, SolanaRpcConfig


class _Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 7, 15, 15, tzinfo=UTC)
        self.lock = threading.Lock()

    def __call__(self) -> datetime:
        with self.lock:
            value = self.value
            self.value += timedelta(microseconds=1)
            return value


class _Transport:
    def __init__(self, *responses: HttpResponse | Callable[[bytes], HttpResponse]) -> None:
        self.responses = list(responses)
        self.payloads: list[bytes] = []
        self.lock = threading.Lock()

    def post(self, payload: bytes, timeout_seconds: float) -> HttpResponse:
        if timeout_seconds <= 0:
            raise AssertionError("timeout must be positive")
        with self.lock:
            self.payloads.append(payload)
            if not self.responses:
                raise AssertionError("unexpected request")
            response = self.responses.pop(0)
        return response(payload) if callable(response) else response


def _block(slot: int) -> dict[str, object]:
    return {
        "jsonrpc": "2.0",
        "id": f"block-{slot}",
        "result": {
            "blockHeight": slot - 10,
            "blockTime": 1_700_000_000 + slot,
            "blockhash": f"hash-{slot}",
            "parentSlot": slot - 1,
            "previousBlockhash": f"hash-{slot - 1}",
            "rewards": [{"lamports": 1, "pubkey": "validator"}],
            "transactions": [
                {
                    "meta": {
                        "computeUnitsConsumed": 10,
                        "innerInstructions": [],
                        "logMessages": ["Program log: raw"],
                        "postBalances": [1],
                        "postTokenBalances": [],
                        "preBalances": [2],
                        "preTokenBalances": [],
                    },
                    "transaction": ["AQID", "base64"],
                    "version": 0,
                }
            ],
        },
    }


def _response(payload: dict[str, object]) -> HttpResponse:
    return HttpResponse(200, json.dumps(payload, separators=(",", ":")).encode())


def _coverage(segment: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in (segment / "coverage.ndjson").read_text().splitlines()]


class BlockArchiveTests(TestCase):
    def test_archives_exact_full_block_and_redacts_provider_secrets(self) -> None:
        rpc = SolanaRpcConfig(
            "https://rpc.example.test/provider-secret?key=query-secret",
            source_label="test-block-source",
            headers=(("Authorization", "Bearer header-secret"),),
        )
        decoded = _response(_block(100))
        compressed = gzip.compress(decoded.body, mtime=0)
        wire = HttpResponse(200, decoded.body, {"Content-Encoding": "gzip"}, compressed)
        transport = _Transport(wire)
        with TemporaryDirectory() as directory:
            result = archive_full_blocks(
                [100],
                directory,
                rpc,
                BlockArchiveConfig(slots_per_segment=1, max_concurrency=1),
                transport=transport,
                clock=_Clock(),
                sleeper=lambda _: None,
            )
            self.assertEqual((result.found, result.unresolved), (1, 0))
            segment = result.segment_paths[0]
            verify_block_segment(segment)
            record = _coverage(segment)[0]
            self.assertEqual(record["transaction_count"], 1)
            self.assertEqual(record["reward_count"], 1)
            wire_entry = record["wire_files"][0]  # type: ignore[index]
            self.assertEqual(  # type: ignore[index]
                (segment / wire_entry["path"]).read_bytes(),
                compressed,
            )
            request = json.loads(transport.payloads[0])
            self.assertEqual(request["method"], "getBlock")
            self.assertEqual(request["params"][0], 100)
            self.assertEqual(
                request["params"][1],
                {
                    "commitment": "finalized",
                    "encoding": "base64",
                    "maxSupportedTransactionVersion": 0,
                    "rewards": True,
                    "transactionDetails": "full",
                },
            )
            archive_bytes = b"".join(
                path.read_bytes() for path in segment.rglob("*") if path.is_file()
            )
            self.assertNotIn(b"provider-secret", archive_bytes)
            self.assertNotIn(b"query-secret", archive_bytes)
            self.assertNotIn(b"header-secret", archive_bytes)

    def test_transient_rpc_limit_retries_then_records_skipped_slot(self) -> None:
        slot = 101
        limited = {
            "jsonrpc": "2.0",
            "id": f"block-{slot}",
            "error": {"code": 429, "message": "limited"},
        }
        skipped = {
            "jsonrpc": "2.0",
            "id": f"block-{slot}",
            "error": {"code": -32007, "message": "slot skipped"},
        }
        transport = _Transport(_response(limited), _response(skipped))
        sleeps: list[float] = []
        rpc = SolanaRpcConfig(
            "https://rpc.example.test",
            max_attempts=2,
            initial_backoff_seconds=0.25,
        )
        with TemporaryDirectory() as directory:
            result = archive_full_blocks(
                [slot],
                directory,
                rpc,
                BlockArchiveConfig(slots_per_segment=1, max_concurrency=1),
                transport=transport,
                clock=_Clock(),
                sleeper=sleeps.append,
            )
            self.assertEqual((result.skipped, result.unresolved), (1, 0))
            record = _coverage(result.segment_paths[0])[0]
            self.assertEqual(record["attempt_count"], 2)
            self.assertEqual(record["error_kind"], "slot_skipped")
            self.assertEqual(len(record["wire_files"]), 2)  # type: ignore[arg-type]
            self.assertEqual(sleeps, [0.25])

    def test_resume_skips_verified_found_but_retries_missing(self) -> None:
        rpc = SolanaRpcConfig("https://rpc.example.test")
        config = BlockArchiveConfig(slots_per_segment=2, max_concurrency=1)
        clock = _Clock()
        with TemporaryDirectory() as directory:
            first = archive_full_blocks(
                [200, 201],
                directory,
                rpc,
                config,
                transport=_Transport(
                    _response(_block(200)),
                    _response({"jsonrpc": "2.0", "id": "block-201", "result": None}),
                ),
                clock=clock,
                sleeper=lambda _: None,
            )
            self.assertEqual((first.found, first.missing), (1, 1))
            transport = _Transport(_response(_block(201)), _response(_block(202)))
            second = archive_full_blocks(
                [200, 201, 202, 200],
                directory,
                rpc,
                config,
                transport=transport,
                clock=clock,
                sleeper=lambda _: None,
            )
            self.assertEqual(
                (second.requested, second.skipped_resolved, second.found),
                (3, 1, 2),
            )
            requested_slots = sorted(
                json.loads(payload)["params"][0] for payload in transport.payloads
            )
            self.assertEqual(requested_slots, [201, 202])

    def test_verifier_detects_wire_tampering(self) -> None:
        rpc = SolanaRpcConfig("https://rpc.example.test")
        with TemporaryDirectory() as directory:
            result = archive_full_blocks(
                [300],
                directory,
                rpc,
                BlockArchiveConfig(slots_per_segment=1, max_concurrency=1),
                transport=_Transport(_response(_block(300))),
                clock=_Clock(),
                sleeper=lambda _: None,
            )
            segment = result.segment_paths[0]
            verify_block_segment(segment)
            wire = next((segment / "wire").iterdir())
            wire.write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "integrity failure"):
                verify_block_segment(segment)
