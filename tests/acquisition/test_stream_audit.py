from __future__ import annotations

from pathlib import Path

import pytest

from pumpquant.acquisition.stream_archive import RawStreamConfig, RawStreamSegmentWriter
from pumpquant.acquisition.stream_audit import RawStreamAuditError, audit_raw_stream_capture


def _config() -> RawStreamConfig:
    return RawStreamConfig(
        source_label="test-source",
        endpoint_origin="wss://rpc.example.test",
        transport="test-websocket",
        stream_name="pump-processed",
        commitment="processed",
        subscription_contract_sha256="a" * 64,
        max_records_per_segment=2,
    )


def _segment(
    root: Path,
    *,
    segment_sequence: int,
    first_sequence: int,
    records: int = 2,
    close_reason: str = "normal_rotation",
) -> Path:
    writer = RawStreamSegmentWriter(
        root,
        _config(),
        connection_id="connection-a",
        segment_sequence=segment_sequence,
        opened_at_ns=100 + segment_sequence * 100,
    )
    for offset in range(records):
        writer.append(
            f"frame-{first_sequence + offset}".encode(),
            local_sequence=first_sequence + offset,
            received_at_ns=110 + segment_sequence * 100 + offset,
            monotonic_ns=1_000 + offset,
            metadata={"slot": 500 + first_sequence + offset},
        )
    return writer.close(
        close_reason=close_reason,
        closed_at_ns=150 + segment_sequence * 100,
    ).path


def test_audit_recomputes_accounting_and_cross_segment_continuity(tmp_path: Path) -> None:
    _segment(tmp_path, segment_sequence=0, first_sequence=0)
    _segment(
        tmp_path,
        segment_sequence=1,
        first_sequence=2,
        close_reason="duration_complete",
    )
    first = audit_raw_stream_capture(tmp_path)
    second = audit_raw_stream_capture(tmp_path)
    assert first.to_json() == second.to_json()
    payload = first.to_dict()
    assert payload["segment_count"] == 2
    assert payload["record_count"] == 4
    assert payload["sequence_gap_count"] == 0
    stream = payload["streams"]["pump-processed"]  # type: ignore[index]
    assert stream["min_slot"] == 500  # type: ignore[index]
    assert stream["max_slot"] == 503  # type: ignore[index]
    assert stream["close_reasons"] == {  # type: ignore[index]
        "duration_complete": 1,
        "normal_rotation": 1,
    }


def test_audit_reports_sequence_gaps_without_hiding_valid_frames(tmp_path: Path) -> None:
    _segment(tmp_path, segment_sequence=0, first_sequence=0)
    _segment(tmp_path, segment_sequence=2, first_sequence=5)
    payload = audit_raw_stream_capture(tmp_path).to_dict()
    assert payload["sequence_gap_count"] == 4
    stream = payload["streams"]["pump-processed"]  # type: ignore[index]
    assert stream["segment_sequence_gap_count"] == 1  # type: ignore[index]
    assert stream["record_sequence_gap_count"] == 3  # type: ignore[index]


def test_audit_fails_on_tampered_records(tmp_path: Path) -> None:
    path = _segment(tmp_path, segment_sequence=0, first_sequence=0)
    (path / "records.ndjson.gz").write_bytes(b"tampered")
    with pytest.raises(RawStreamAuditError, match="verification failed"):
        audit_raw_stream_capture(tmp_path)
