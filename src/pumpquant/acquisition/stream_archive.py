"""Atomic exact-frame archive for live RPC and Yellowstone streams.

WebSocket text frames and protobuf messages are stored as base64 inside a gzip
NDJSON segment.  Base64 is deliberate: decoding recovers the exact application
payload bytes, while the envelope adds local wall/monotonic receipt clocks,
connection identity, local sequence, slot/signature hints, and a payload hash.

The writer never interprets a frame as market truth.  Commitment, filter hash,
transport, connection close reason, and parse/error counts remain explicit so
disconnects and weaker fallback feeds cannot masquerade as lossless coverage.
"""

from __future__ import annotations

import base64
import gzip
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, TextIO, cast

from pumpquant.core.hashing import canonical_json, sha256_bytes, sha256_file

_SCHEMA_VERSION = 1
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _write_bytes(path: Path, payload: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise OSError("raw stream archive write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _nonnegative(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _safe_name(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SAFE_NAME.fullmatch(value):
        raise ValueError(f"{label} contains unsafe characters")
    return value


def _safe_origin(value: object) -> str:
    if not isinstance(value, str) or not value or "?" in value or "#" in value or "@" in value:
        raise ValueError("endpoint_origin must exclude credentials, path query, and fragment")
    return value


@dataclass(frozen=True, slots=True)
class RawStreamConfig:
    source_label: str
    endpoint_origin: str
    transport: str
    stream_name: str
    commitment: str
    subscription_contract_sha256: str
    max_records_per_segment: int = 5_000
    max_raw_payload_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        _safe_name(self.source_label, "source_label")
        _safe_origin(self.endpoint_origin)
        _safe_name(self.transport, "transport")
        _safe_name(self.stream_name, "stream_name")
        _safe_name(self.commitment, "commitment")
        if not _HEX64.fullmatch(self.subscription_contract_sha256):
            raise ValueError("subscription_contract_sha256 must be lowercase SHA-256 hex")
        for name in ("max_records_per_segment", "max_raw_payload_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")

    @property
    def source_id(self) -> str:
        identity = (
            f"{self.source_label}\0{self.endpoint_origin}\0{self.transport}\0"
            f"{self.stream_name}\0{self.commitment}\0{self.subscription_contract_sha256}"
        )
        return sha256_bytes(identity.encode())


@dataclass(frozen=True, slots=True)
class RawStreamSegmentManifest:
    segment_id: str
    path: Path
    record_count: int
    raw_payload_bytes: int
    first_sequence: int | None
    last_sequence: int | None
    first_received_at_ns: int | None
    last_received_at_ns: int | None
    min_slot: int | None
    max_slot: int | None
    parse_error_count: int
    rpc_error_count: int
    close_reason: str
    records_sha256: str
    records_byte_count: int
    manifest_sha256: str


class RawStreamSegmentWriter:
    """One crash-isolated live-stream segment published by atomic rename."""

    def __init__(
        self,
        output_root: str | os.PathLike[str],
        config: RawStreamConfig,
        *,
        connection_id: str,
        segment_sequence: int,
        opened_at_ns: int,
    ) -> None:
        if not isinstance(config, RawStreamConfig):
            raise TypeError("config must be RawStreamConfig")
        self._config = config
        self._connection_id = _safe_name(connection_id, "connection_id")
        self._segment_sequence = _nonnegative(segment_sequence, "segment_sequence")
        self._opened_at_ns = _nonnegative(opened_at_ns, "opened_at_ns")
        root = Path(output_root)
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._segment_root = root / "segments" / config.stream_name
        self._segment_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._staging = Path(
            tempfile.mkdtemp(
                prefix=f".{self._connection_id}-{self._segment_sequence:08d}.",
                suffix=".partial",
                dir=self._segment_root,
            )
        )
        os.chmod(self._staging, 0o700)
        self._records_path = self._staging / "records.ndjson.gz"
        descriptor = os.open(
            self._records_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        self._binary: BinaryIO = os.fdopen(descriptor, "wb")
        self._gzip = gzip.GzipFile(
            filename="records.ndjson",
            mode="wb",
            fileobj=self._binary,
            mtime=0,
        )
        self._closed = False
        self._record_count = 0
        self._raw_payload_bytes = 0
        self._first_sequence: int | None = None
        self._last_sequence: int | None = None
        self._first_received_at_ns: int | None = None
        self._last_received_at_ns: int | None = None
        self._min_slot: int | None = None
        self._max_slot: int | None = None
        self._parse_error_count = 0
        self._rpc_error_count = 0

    @property
    def record_count(self) -> int:
        return self._record_count

    @property
    def should_rotate(self) -> bool:
        return (
            self._record_count >= self._config.max_records_per_segment
            or self._raw_payload_bytes >= self._config.max_raw_payload_bytes
        )

    def append(
        self,
        payload: bytes,
        *,
        local_sequence: int,
        received_at_ns: int,
        monotonic_ns: int,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        if self._closed:
            raise RuntimeError("raw stream segment is already closed")
        if not isinstance(payload, bytes):
            raise TypeError("payload must be exact bytes")
        sequence = _nonnegative(local_sequence, "local_sequence")
        wall = _nonnegative(received_at_ns, "received_at_ns")
        monotonic = _nonnegative(monotonic_ns, "monotonic_ns")
        if self._last_sequence is not None and sequence != self._last_sequence + 1:
            raise ValueError("local_sequence must be contiguous within a segment")
        meta = dict(metadata or {})
        try:
            json.dumps(meta, allow_nan=False)
        except (TypeError, ValueError, OverflowError) as error:
            raise TypeError("metadata must be a JSON-serializable object") from error
        slot_value = meta.get("slot")
        slot = None if slot_value is None else _nonnegative(slot_value, "metadata.slot")
        parse_error = bool(meta.get("parse_error", False))
        rpc_error = bool(meta.get("rpc_error", False))
        envelope = {
            "schema_version": _SCHEMA_VERSION,
            "connection_id": self._connection_id,
            "local_sequence": sequence,
            "received_at_ns": wall,
            "monotonic_ns": monotonic,
            "payload_encoding": "base64",
            "payload_byte_count": len(payload),
            "payload_sha256": sha256_bytes(payload),
            "payload": base64.b64encode(payload).decode("ascii"),
            "metadata": meta,
        }
        self._gzip.write(canonical_json(envelope))
        self._record_count += 1
        self._raw_payload_bytes += len(payload)
        self._first_sequence = sequence if self._first_sequence is None else self._first_sequence
        self._last_sequence = sequence
        self._first_received_at_ns = (
            wall if self._first_received_at_ns is None else self._first_received_at_ns
        )
        self._last_received_at_ns = wall
        if slot is not None:
            self._min_slot = slot if self._min_slot is None else min(self._min_slot, slot)
            self._max_slot = slot if self._max_slot is None else max(self._max_slot, slot)
        self._parse_error_count += int(parse_error)
        self._rpc_error_count += int(rpc_error)

    def close(
        self,
        *,
        close_reason: str,
        closed_at_ns: int,
        error_kind: str | None = None,
    ) -> RawStreamSegmentManifest:
        if self._closed:
            raise RuntimeError("raw stream segment is already closed")
        reason = _safe_name(close_reason, "close_reason")
        closed = _nonnegative(closed_at_ns, "closed_at_ns")
        if closed < self._opened_at_ns:
            raise ValueError("closed_at_ns cannot precede opened_at_ns")
        safe_error = None if error_kind is None else _safe_name(error_kind, "error_kind")
        self._closed = True
        try:
            self._gzip.close()
            self._binary.flush()
            os.fsync(self._binary.fileno())
            self._binary.close()
            records_size = self._records_path.stat().st_size
            records_hash = sha256_file(self._records_path)
            identity = (
                f"{self._config.source_id}\0{self._connection_id}\0"
                f"{self._segment_sequence}\0{records_hash}"
            )
            segment_id = sha256_bytes(identity.encode())
            payload: dict[str, object] = {
                "schema_version": _SCHEMA_VERSION,
                "kind": "pumpquant.raw_stream_segment",
                "segment_id": segment_id,
                "source_id": self._config.source_id,
                "source_label": self._config.source_label,
                "endpoint_origin": self._config.endpoint_origin,
                "transport": self._config.transport,
                "stream_name": self._config.stream_name,
                "commitment": self._config.commitment,
                "subscription_contract_sha256": (self._config.subscription_contract_sha256),
                "connection_id": self._connection_id,
                "segment_sequence": self._segment_sequence,
                "opened_at_ns": self._opened_at_ns,
                "closed_at_ns": closed,
                "close_reason": reason,
                "error_kind": safe_error,
                "record_count": self._record_count,
                "raw_payload_bytes": self._raw_payload_bytes,
                "first_sequence": self._first_sequence,
                "last_sequence": self._last_sequence,
                "first_received_at_ns": self._first_received_at_ns,
                "last_received_at_ns": self._last_received_at_ns,
                "min_slot": self._min_slot,
                "max_slot": self._max_slot,
                "parse_error_count": self._parse_error_count,
                "rpc_error_count": self._rpc_error_count,
                "files": {
                    "records.ndjson.gz": {
                        "byte_count": records_size,
                        "sha256": records_hash,
                    }
                },
            }
            manifest_bytes = canonical_json(payload)
            _write_bytes(self._staging / "manifest.json", manifest_bytes)
            _fsync_directory(self._staging)
            destination = self._segment_root / segment_id
            if destination.exists():
                raise FileExistsError("raw stream segment id already exists")
            os.rename(self._staging, destination)
            _fsync_directory(self._segment_root)
            return RawStreamSegmentManifest(
                segment_id=segment_id,
                path=destination,
                record_count=self._record_count,
                raw_payload_bytes=self._raw_payload_bytes,
                first_sequence=self._first_sequence,
                last_sequence=self._last_sequence,
                first_received_at_ns=self._first_received_at_ns,
                last_received_at_ns=self._last_received_at_ns,
                min_slot=self._min_slot,
                max_slot=self._max_slot,
                parse_error_count=self._parse_error_count,
                rpc_error_count=self._rpc_error_count,
                close_reason=reason,
                records_sha256=records_hash,
                records_byte_count=records_size,
                manifest_sha256=sha256_bytes(manifest_bytes),
            )
        except BaseException:
            if not self._binary.closed:
                self._binary.close()
            shutil.rmtree(self._staging, ignore_errors=True)
            raise

    def abort(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._gzip.close()
        finally:
            if not self._binary.closed:
                self._binary.close()
            shutil.rmtree(self._staging, ignore_errors=True)


def verify_raw_stream_segment(path: str | os.PathLike[str]) -> None:
    """Verify compressed bytes and every exact frame envelope in a segment."""

    root = Path(path)
    try:
        manifest = json.loads((root / "manifest.json").read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("raw stream manifest is unreadable") from error
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != _SCHEMA_VERSION
        or manifest.get("kind") != "pumpquant.raw_stream_segment"
    ):
        raise ValueError("raw stream manifest has an unsupported schema")
    files = manifest.get("files")
    file_meta = files.get("records.ndjson.gz") if isinstance(files, dict) else None
    if not isinstance(file_meta, dict):
        raise ValueError("raw stream manifest has no records metadata")
    records = root / "records.ndjson.gz"
    expected_size = file_meta.get("byte_count")
    expected_hash = file_meta.get("sha256")
    if (
        isinstance(expected_size, bool)
        or not isinstance(expected_size, int)
        or expected_size < 0
        or not isinstance(expected_hash, str)
        or not _HEX64.fullmatch(expected_hash)
    ):
        raise ValueError("raw stream records metadata is invalid")
    if records.stat().st_size != expected_size or sha256_file(records) != expected_hash:
        raise ValueError("raw stream records failed integrity verification")
    count = 0
    payload_bytes = 0
    first_sequence: int | None = None
    last_sequence: int | None = None
    try:
        with gzip.open(records, "rt", encoding="utf-8") as source:
            text_source = cast(TextIO, source)
            for line in text_source:
                envelope = json.loads(line)
                if not isinstance(envelope, dict):
                    raise ValueError("raw stream record is not an object")
                sequence = _nonnegative(envelope.get("local_sequence"), "local_sequence")
                encoded = envelope.get("payload")
                if not isinstance(encoded, str):
                    raise ValueError("raw stream record has no base64 payload")
                try:
                    payload = base64.b64decode(encoded, validate=True)
                except ValueError as error:
                    raise ValueError("raw stream payload is invalid base64") from error
                if envelope.get("payload_byte_count") != len(payload):
                    raise ValueError("raw stream payload length mismatch")
                if envelope.get("payload_sha256") != sha256_bytes(payload):
                    raise ValueError("raw stream payload checksum mismatch")
                if last_sequence is not None and sequence != last_sequence + 1:
                    raise ValueError("raw stream sequence is not contiguous")
                first_sequence = sequence if first_sequence is None else first_sequence
                last_sequence = sequence
                count += 1
                payload_bytes += len(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("raw stream records are unreadable") from error
    if (
        manifest.get("record_count") != count
        or manifest.get("raw_payload_bytes") != payload_bytes
        or manifest.get("first_sequence") != first_sequence
        or manifest.get("last_sequence") != last_sequence
    ):
        raise ValueError("raw stream manifest accounting mismatch")


__all__ = [
    "RawStreamConfig",
    "RawStreamSegmentManifest",
    "RawStreamSegmentWriter",
    "verify_raw_stream_segment",
]
