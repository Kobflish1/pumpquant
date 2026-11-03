"""Deterministic integrity and continuity audit for raw live-stream captures."""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from pumpquant.acquisition.stream_archive import verify_raw_stream_segment

_KIND = "pumpquant.raw_stream_segment"
_SCHEMA_VERSION = 1


class RawStreamAuditError(RuntimeError):
    """Raised when a live-stream archive cannot be verified consistently."""


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _nonnegative(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RawStreamAuditError(f"{label} must be a non-negative integer")
    return value


def _nullable_nonnegative(value: object, label: str) -> int | None:
    if value is None:
        return None
    return _nonnegative(value, label)


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise RawStreamAuditError(f"{label} must be non-empty text")
    return value


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class RawStreamAuditResult:
    """Stable mapping and canonical JSON returned by a capture audit."""

    payload: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(json.dumps(self.payload, sort_keys=True)))

    def to_json(self, *, indent: int | None = None) -> str:
        if indent is None:
            return _canonical(self.payload).decode("utf-8")
        return json.dumps(self.payload, allow_nan=False, indent=indent, sort_keys=True) + "\n"


@dataclass(frozen=True, slots=True)
class _Segment:
    path: Path
    manifest_sha256: str
    segment_id: str
    source_id: str
    stream_name: str
    commitment: str
    sequence: int
    connection_id: str
    close_reason: str
    error_kind: str | None
    record_count: int
    raw_payload_bytes: int
    compressed_bytes: int
    parse_errors: int
    rpc_errors: int
    first_sequence: int | None
    last_sequence: int | None
    first_received_at_ns: int | None
    last_received_at_ns: int | None
    min_slot: int | None
    max_slot: int | None


def _load_segment(manifest_path: Path) -> _Segment:
    try:
        encoded = manifest_path.read_bytes()
        raw = json.loads(encoded)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RawStreamAuditError(f"unreadable stream manifest: {manifest_path}") from error
    if (
        not isinstance(raw, dict)
        or raw.get("schema_version") != _SCHEMA_VERSION
        or raw.get("kind") != _KIND
    ):
        raise RawStreamAuditError(f"unsupported stream manifest: {manifest_path}")
    try:
        verify_raw_stream_segment(manifest_path.parent)
    except (OSError, ValueError) as error:
        raise RawStreamAuditError(
            f"stream segment verification failed: {manifest_path.parent}: {error}"
        ) from error
    files = raw.get("files")
    records_meta = files.get("records.ndjson.gz") if isinstance(files, dict) else None
    if not isinstance(records_meta, dict):
        raise RawStreamAuditError(f"stream manifest has no records metadata: {manifest_path}")
    error_kind = raw.get("error_kind")
    if error_kind is not None and not isinstance(error_kind, str):
        raise RawStreamAuditError(f"invalid stream error_kind: {manifest_path}")
    return _Segment(
        path=manifest_path.parent,
        manifest_sha256=_sha256(encoded),
        segment_id=_text(raw.get("segment_id"), "segment_id"),
        source_id=_text(raw.get("source_id"), "source_id"),
        stream_name=_text(raw.get("stream_name"), "stream_name"),
        commitment=_text(raw.get("commitment"), "commitment"),
        sequence=_nonnegative(raw.get("segment_sequence"), "segment_sequence"),
        connection_id=_text(raw.get("connection_id"), "connection_id"),
        close_reason=_text(raw.get("close_reason"), "close_reason"),
        error_kind=error_kind,
        record_count=_nonnegative(raw.get("record_count"), "record_count"),
        raw_payload_bytes=_nonnegative(raw.get("raw_payload_bytes"), "raw_payload_bytes"),
        compressed_bytes=_nonnegative(records_meta.get("byte_count"), "compressed byte_count"),
        parse_errors=_nonnegative(raw.get("parse_error_count"), "parse_error_count"),
        rpc_errors=_nonnegative(raw.get("rpc_error_count"), "rpc_error_count"),
        first_sequence=_nullable_nonnegative(raw.get("first_sequence"), "first_sequence"),
        last_sequence=_nullable_nonnegative(raw.get("last_sequence"), "last_sequence"),
        first_received_at_ns=_nullable_nonnegative(
            raw.get("first_received_at_ns"), "first_received_at_ns"
        ),
        last_received_at_ns=_nullable_nonnegative(
            raw.get("last_received_at_ns"), "last_received_at_ns"
        ),
        min_slot=_nullable_nonnegative(raw.get("min_slot"), "min_slot"),
        max_slot=_nullable_nonnegative(raw.get("max_slot"), "max_slot"),
    )


def _minimum(values: list[int]) -> int | None:
    return min(values) if values else None


def _maximum(values: list[int]) -> int | None:
    return max(values) if values else None


def _present(values: list[int | None]) -> list[int]:
    return [value for value in values if value is not None]


def audit_raw_stream_capture(root_value: str | os.PathLike[str]) -> RawStreamAuditResult:
    """Verify every segment and recompute stream continuity/accounting."""

    root = Path(root_value).expanduser()
    if root.is_symlink() or not root.is_dir():
        raise RawStreamAuditError("capture root must be a non-symlink directory")
    manifests = sorted((root / "segments").glob("*/*/manifest.json"))
    if not manifests:
        raise RawStreamAuditError("capture contains no published stream segments")
    segments = [_load_segment(path) for path in manifests]
    identities: dict[str, str] = {}
    for segment in segments:
        previous = identities.get(segment.segment_id)
        if previous is not None and previous != segment.manifest_sha256:
            raise RawStreamAuditError(
                f"conflicting manifests share segment_id {segment.segment_id}"
            )
        identities[segment.segment_id] = segment.manifest_sha256
    if len(identities) != len(segments):
        raise RawStreamAuditError("capture contains duplicate segment manifests")

    grouped: defaultdict[str, list[_Segment]] = defaultdict(list)
    for segment in segments:
        grouped[segment.stream_name].append(segment)

    stream_payloads: dict[str, object] = {}
    total_sequence_gaps = 0
    total_sequence_overlaps = 0
    for stream_name in sorted(grouped):
        values = sorted(
            grouped[stream_name],
            key=lambda value: (value.sequence, value.segment_id),
        )
        if len({value.source_id for value in values}) != 1:
            raise RawStreamAuditError(f"stream {stream_name} mixes source contracts")
        if len({value.commitment for value in values}) != 1:
            raise RawStreamAuditError(f"stream {stream_name} mixes commitments")
        if len({value.sequence for value in values}) != len(values):
            raise RawStreamAuditError(f"stream {stream_name} repeats segment_sequence")

        segment_sequence_gaps = 0
        record_sequence_gaps = 0
        record_sequence_overlaps = 0
        prior_segment: int | None = None
        prior_record: int | None = None
        for value in values:
            if prior_segment is not None and value.sequence != prior_segment + 1:
                segment_sequence_gaps += value.sequence - prior_segment - 1
            prior_segment = value.sequence
            if value.record_count == 0:
                if value.first_sequence is not None or value.last_sequence is not None:
                    raise RawStreamAuditError("empty stream segment has sequence bounds")
                continue
            if value.first_sequence is None or value.last_sequence is None:
                raise RawStreamAuditError("non-empty stream segment has no sequence bounds")
            if value.last_sequence - value.first_sequence + 1 != value.record_count:
                raise RawStreamAuditError(
                    "stream segment sequence span disagrees with record_count"
                )
            if prior_record is not None:
                difference = value.first_sequence - prior_record - 1
                if difference > 0:
                    record_sequence_gaps += difference
                elif difference < 0:
                    record_sequence_overlaps += -difference
            prior_record = value.last_sequence

        total_sequence_gaps += segment_sequence_gaps + record_sequence_gaps
        total_sequence_overlaps += record_sequence_overlaps
        closes = Counter(value.close_reason for value in values)
        stream_payloads[stream_name] = {
            "source_id": values[0].source_id,
            "commitment": values[0].commitment,
            "segment_count": len(values),
            "record_count": sum(value.record_count for value in values),
            "raw_payload_bytes": sum(value.raw_payload_bytes for value in values),
            "compressed_bytes": sum(value.compressed_bytes for value in values),
            "parse_error_count": sum(value.parse_errors for value in values),
            "rpc_error_count": sum(value.rpc_errors for value in values),
            "disconnect_segment_count": closes.get("disconnect", 0),
            "connection_count": len({value.connection_id for value in values}),
            "close_reasons": dict(sorted(closes.items())),
            "error_kinds": dict(
                sorted(Counter(value.error_kind for value in values if value.error_kind).items())
            ),
            "segment_sequence_gap_count": segment_sequence_gaps,
            "record_sequence_gap_count": record_sequence_gaps,
            "record_sequence_overlap_count": record_sequence_overlaps,
            "first_received_at_ns": _minimum(
                _present([item.first_received_at_ns for item in values])
            ),
            "last_received_at_ns": _maximum(
                _present([item.last_received_at_ns for item in values])
            ),
            "min_slot": _minimum(_present([item.min_slot for item in values])),
            "max_slot": _maximum(_present([item.max_slot for item in values])),
            "manifest_set_sha256": _sha256(
                "\n".join(sorted(value.manifest_sha256 for value in values)).encode("ascii")
            ),
        }

    payload: dict[str, object] = {
        "version": 1,
        "kind": "pumpquant.raw_stream_capture_audit",
        "capture_root": str(root.resolve()),
        "segment_count": len(segments),
        "stream_count": len(grouped),
        "record_count": sum(value.record_count for value in segments),
        "raw_payload_bytes": sum(value.raw_payload_bytes for value in segments),
        "compressed_bytes": sum(value.compressed_bytes for value in segments),
        "parse_error_count": sum(value.parse_errors for value in segments),
        "rpc_error_count": sum(value.rpc_errors for value in segments),
        "disconnect_segment_count": sum(value.close_reason == "disconnect" for value in segments),
        "sequence_gap_count": total_sequence_gaps,
        "sequence_overlap_count": total_sequence_overlaps,
        "manifest_set_sha256": _sha256(
            "\n".join(sorted(value.manifest_sha256 for value in segments)).encode("ascii")
        ),
        "streams": stream_payloads,
    }
    return RawStreamAuditResult(payload)


__all__ = ["RawStreamAuditError", "RawStreamAuditResult", "audit_raw_stream_capture"]
