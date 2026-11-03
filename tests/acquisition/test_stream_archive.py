from __future__ import annotations

import base64
import gzip
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from pumpquant.acquisition.stream_archive import (
    RawStreamConfig,
    RawStreamSegmentWriter,
    verify_raw_stream_segment,
)


def _config(**overrides: object) -> RawStreamConfig:
    values: dict[str, object] = {
        "source_label": "solana-public-mainnet",
        "endpoint_origin": "wss://api.mainnet-beta.solana.com",
        "transport": "solana-websocket",
        "stream_name": "pump-processed",
        "commitment": "processed",
        "subscription_contract_sha256": "a" * 64,
        "max_records_per_segment": 2,
        "max_raw_payload_bytes": 1024,
    }
    values.update(overrides)
    return RawStreamConfig(**values)  # type: ignore[arg-type]


class RawStreamArchiveTests(TestCase):
    def test_exact_frames_receipt_clocks_and_gap_metadata_round_trip(self) -> None:
        first = b'{"jsonrpc":"2.0","result":7,"id":"pump"}'
        second = b'{"params":{"result":{"context":{"slot":9}}}}'
        with TemporaryDirectory() as directory:
            writer = RawStreamSegmentWriter(
                directory,
                _config(),
                connection_id="connection-1",
                segment_sequence=0,
                opened_at_ns=100,
            )
            writer.append(
                first,
                local_sequence=10,
                received_at_ns=101,
                monotonic_ns=201,
                metadata={"frame_type": "subscription_ack"},
            )
            self.assertFalse(writer.should_rotate)
            writer.append(
                second,
                local_sequence=11,
                received_at_ns=102,
                monotonic_ns=202,
                metadata={
                    "frame_type": "log_notification",
                    "slot": 9,
                    "signature": "signature",
                },
            )
            self.assertTrue(writer.should_rotate)
            result = writer.close(
                close_reason="disconnect",
                closed_at_ns=103,
                error_kind="connection_closed",
            )
            verify_raw_stream_segment(result.path)
            self.assertEqual(result.record_count, 2)
            self.assertEqual((result.first_sequence, result.last_sequence), (10, 11))
            self.assertEqual((result.min_slot, result.max_slot), (9, 9))
            with gzip.open(result.path / "records.ndjson.gz", "rt") as source:
                records = [json.loads(line) for line in source]
            self.assertEqual(
                [base64.b64decode(record["payload"]) for record in records],
                [first, second],
            )
            self.assertEqual(
                [(record["received_at_ns"], record["monotonic_ns"]) for record in records],
                [(101, 201), (102, 202)],
            )
            manifest = json.loads((result.path / "manifest.json").read_text())
            self.assertEqual(manifest["close_reason"], "disconnect")
            self.assertEqual(manifest["error_kind"], "connection_closed")

    def test_rejects_sequence_gap_and_abort_removes_staging(self) -> None:
        with TemporaryDirectory() as directory:
            writer = RawStreamSegmentWriter(
                directory,
                _config(),
                connection_id="connection-1",
                segment_sequence=0,
                opened_at_ns=1,
            )
            writer.append(
                b"one",
                local_sequence=1,
                received_at_ns=2,
                monotonic_ns=3,
            )
            with self.assertRaisesRegex(ValueError, "contiguous"):
                writer.append(
                    b"three",
                    local_sequence=3,
                    received_at_ns=4,
                    monotonic_ns=5,
                )
            writer.abort()
            self.assertFalse(list(Path(directory).rglob("*.partial")))

    def test_empty_connection_segment_is_verifiable(self) -> None:
        with TemporaryDirectory() as directory:
            writer = RawStreamSegmentWriter(
                directory,
                _config(),
                connection_id="connection-1",
                segment_sequence=0,
                opened_at_ns=10,
            )
            result = writer.close(
                close_reason="connect_error",
                closed_at_ns=11,
                error_kind="timeout",
            )
            verify_raw_stream_segment(result.path)
            self.assertEqual(result.record_count, 0)
            self.assertIsNone(result.first_sequence)

    def test_verifier_detects_compressed_record_tampering(self) -> None:
        with TemporaryDirectory() as directory:
            writer = RawStreamSegmentWriter(
                directory,
                _config(),
                connection_id="connection-1",
                segment_sequence=0,
                opened_at_ns=1,
            )
            writer.append(
                b"payload",
                local_sequence=0,
                received_at_ns=2,
                monotonic_ns=3,
            )
            result = writer.close(close_reason="normal", closed_at_ns=4)
            records = result.path / "records.ndjson.gz"
            records.write_bytes(records.read_bytes() + b"tampered")
            with self.assertRaisesRegex(ValueError, "integrity"):
                verify_raw_stream_segment(result.path)
