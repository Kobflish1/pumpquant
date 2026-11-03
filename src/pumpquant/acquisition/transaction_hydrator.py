"""Lossless, restartable Solana ``getTransaction`` archival.

The BigQuery Instructions archive identifies transactions but does not contain
their serialized bytes or execution metadata.  This module hydrates those
signatures through Solana JSON-RPC and publishes small immutable segments.  A
segment contains the exact HTTP response bodies, a canonical per-signature
coverage ledger, a separate exception ledger, and hashes for every file.

Endpoint paths, query strings, credentials, and custom header values are never
written to disk or included in representations.  This matters for providers
whose API key is embedded in the URL.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import shutil
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, TypeAlias, cast
from urllib.parse import urlsplit

from pumpquant.core.hashing import sha256_file

JSONValue: TypeAlias = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]
JSONObject: TypeAlias = dict[str, JSONValue]

_BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,128}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_TRANSIENT_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
_TRANSIENT_RPC_CODES = frozenset({429, -32005, -32004, -32002, -32603})
_SCHEMA_VERSION = 1


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _endpoint_origin(endpoint: str) -> str:
    """Return only scheme and authority, excluding secret-bearing URL parts."""

    parsed = urlsplit(endpoint)
    host = parsed.hostname
    if host is None:
        raise ValueError("RPC endpoint must contain a hostname")
    authority = host
    if ":" in host and not host.startswith("["):
        authority = f"[{host}]"
    if parsed.port is not None:
        authority = f"{authority}:{parsed.port}"
    return f"{parsed.scheme.lower()}://{authority}"


def _signature(value: object) -> str:
    if not isinstance(value, str) or not _BASE58_RE.fullmatch(value):
        raise ValueError("Solana signature must be 32-128 base58 characters")
    return value


@dataclass(frozen=True, slots=True, repr=False)
class SolanaRpcConfig:
    """Configuration whose representation intentionally redacts all secrets."""

    endpoint: str
    source_label: str = "solana-rpc"
    headers: tuple[tuple[str, str], ...] = field(default=(), repr=False)
    commitment: str = "finalized"
    max_supported_transaction_version: int = 0
    batch_size: int = 20
    segment_size: int = 1_000
    timeout_seconds: float = 30.0
    max_attempts: int = 5
    initial_backoff_seconds: float = 0.5
    max_backoff_seconds: float = 10.0
    retry_null_results: bool = False
    user_agent: str = "pumpquant-transaction-hydrator/0.1"

    def __post_init__(self) -> None:
        parsed = urlsplit(self.endpoint)
        if parsed.scheme.lower() not in {"http", "https"} or parsed.hostname is None:
            raise ValueError("RPC endpoint must be an absolute HTTP(S) URL")
        if parsed.scheme.lower() != "https" and parsed.hostname not in {
            "127.0.0.1",
            "localhost",
            "::1",
        }:
            raise ValueError("non-local RPC endpoints must use HTTPS")
        if not self.source_label.strip():
            raise ValueError("source_label must be non-empty")
        if self.commitment not in {"confirmed", "finalized"}:
            raise ValueError("transaction hydration commitment must be confirmed or finalized")
        for name in (
            "max_supported_transaction_version",
            "batch_size",
            "segment_size",
            "max_attempts",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise TypeError(f"{name} must be a non-negative integer")
        if self.batch_size < 1 or self.segment_size < 1 or self.max_attempts < 1:
            raise ValueError("batch_size, segment_size, and max_attempts must be positive")
        if self.batch_size > self.segment_size:
            raise ValueError("batch_size cannot exceed segment_size")
        for name in ("timeout_seconds", "initial_backoff_seconds", "max_backoff_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
                raise TypeError(f"{name} must be a non-negative number")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.initial_backoff_seconds > self.max_backoff_seconds:
            raise ValueError("initial_backoff_seconds cannot exceed max_backoff_seconds")
        forbidden = {"content-type", "user-agent", "content-length", "host"}
        seen: set[str] = set()
        for name, value in self.headers:
            if not name.strip() or "\n" in name or "\r" in name:
                raise ValueError("custom header name is invalid")
            if not value or "\n" in value or "\r" in value:
                raise ValueError("custom header value is invalid")
            normalized = name.casefold()
            if normalized in forbidden or normalized in seen:
                raise ValueError("custom headers contain a forbidden or duplicate name")
            seen.add(normalized)

    @property
    def endpoint_origin(self) -> str:
        return _endpoint_origin(self.endpoint)

    @property
    def source_id(self) -> str:
        payload = f"{self.source_label}\0{self.endpoint_origin}".encode()
        return hashlib.sha256(payload).hexdigest()

    def __repr__(self) -> str:
        return (
            "SolanaRpcConfig(endpoint='<redacted>', "
            f"source_label={self.source_label!r}, commitment={self.commitment!r}, "
            f"batch_size={self.batch_size}, segment_size={self.segment_size})"
        )


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    body: bytes
    headers: Mapping[str, str] = field(default_factory=dict)
    wire_body: bytes | None = None


class JsonRpcTransport(Protocol):
    def post(self, payload: bytes, timeout_seconds: float) -> HttpResponse: ...


class UrllibJsonRpcTransport:
    """Small stdlib transport; its representation never contains the endpoint."""

    def __init__(self, config: SolanaRpcConfig) -> None:
        self._endpoint = config.endpoint
        self._headers = {
            "Content-Type": "application/json",
            "User-Agent": config.user_agent,
            **dict(config.headers),
        }

    def __repr__(self) -> str:
        return "UrllibJsonRpcTransport(endpoint='<redacted>')"

    def post(self, payload: bytes, timeout_seconds: float) -> HttpResponse:
        request = urllib.request.Request(
            self._endpoint,
            data=payload,
            headers=self._headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                headers = {str(key): str(value) for key, value in response.headers.items()}
                raw_body = response.read()
                return _http_response(int(response.status), raw_body, headers)
        except urllib.error.HTTPError as error:
            # HTTP error bodies are evidence and must be archived too.  Do not
            # propagate the exception because its text can contain the URL.
            body = error.read()
            headers = {str(key): str(value) for key, value in error.headers.items()}
            return _http_response(int(error.code), body, headers)


def _http_response(
    status: int,
    raw_body: bytes,
    headers: Mapping[str, str],
) -> HttpResponse:
    encoding = next(
        (value for key, value in headers.items() if key.casefold() == "content-encoding"),
        "",
    ).casefold()
    if encoding == "gzip":
        try:
            return HttpResponse(status, gzip.decompress(raw_body), headers, raw_body)
        except (OSError, EOFError):
            # Keep the undecodable wire bytes so the normal protocol-error path
            # archives evidence rather than hiding a corrupt response.
            return HttpResponse(status, raw_body, headers)
    return HttpResponse(status, raw_body, headers)


@dataclass(frozen=True, slots=True)
class HydrationResult:
    requested: int
    skipped_resolved: int
    found: int
    missing: int
    rpc_error: int
    transport_error: int
    protocol_error: int
    segment_paths: tuple[Path, ...]

    @property
    def unresolved(self) -> int:
        return self.missing + self.rpc_error + self.transport_error + self.protocol_error


def _request_id(signature: str) -> str:
    return hashlib.sha256(signature.encode("ascii")).hexdigest()[:24]


def _request_payload(signatures: Sequence[str], config: SolanaRpcConfig) -> bytes:
    requests = [
        {
            "jsonrpc": "2.0",
            "id": _request_id(signature),
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "commitment": config.commitment,
                    "encoding": "base64",
                    "maxSupportedTransactionVersion": config.max_supported_transaction_version,
                },
            ],
        }
        for signature in signatures
    ]
    return json.dumps(requests, allow_nan=False, separators=(",", ":")).encode("utf-8")


def _json_object(value: object) -> JSONObject:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("JSON-RPC item must be an object")
    return cast(JSONObject, value)


def _response_items(body: bytes) -> dict[str, JSONObject]:
    try:
        decoded = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("response was not valid UTF-8 JSON") from error
    raw_items: object = [decoded] if isinstance(decoded, dict) else decoded
    if not isinstance(raw_items, list):
        raise ValueError("JSON-RPC batch response was not an array")
    items: dict[str, JSONObject] = {}
    for raw_item in raw_items:
        item = _json_object(raw_item)
        identifier = item.get("id")
        if not isinstance(identifier, str) or identifier in items:
            raise ValueError("JSON-RPC response contained a missing or duplicate id")
        if item.get("jsonrpc") != "2.0":
            raise ValueError("JSON-RPC response had an invalid version")
        if ("result" in item) == ("error" in item):
            raise ValueError("JSON-RPC response must contain exactly one of result or error")
        items[identifier] = item
    return items


def _retry_after(headers: Mapping[str, str]) -> float | None:
    value = next((item for key, item in headers.items() if key.casefold() == "retry-after"), None)
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return max(0.0, seconds)


def _safe_transport_error(error: BaseException) -> str:
    # Exception text can carry a provider URL.  A type-only classification is
    # enough for the exception ledger; operators use logs outside this archive.
    if isinstance(error, TimeoutError):
        return "timeout"
    if isinstance(error, urllib.error.URLError):
        return "url_transport_error"
    return f"transport_{type(error).__name__.casefold()}"


def _iter_chunks(values: Sequence[str], size: int) -> Iterable[tuple[str, ...]]:
    for start in range(0, len(values), size):
        yield tuple(values[start : start + size])


def _load_resolved(root: Path, source_id: str) -> set[str]:
    resolved: set[str] = set()
    segment_root = root / "segments"
    if not segment_root.exists():
        return resolved
    for manifest_path in sorted(segment_root.glob("*/manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(manifest, dict) or manifest.get("source_id") != source_id:
            continue
        coverage_path = manifest_path.parent / "coverage.ndjson"
        expected = manifest.get("files")
        if not isinstance(expected, dict):
            continue
        coverage_meta = expected.get("coverage.ndjson")
        if not isinstance(coverage_meta, dict):
            continue
        digest = coverage_meta.get("sha256")
        if not isinstance(digest, str) or not _HEX64_RE.fullmatch(digest):
            continue
        if not coverage_path.is_file() or sha256_file(coverage_path) != digest:
            continue
        with coverage_path.open("rb") as rows:
            for line in rows:
                try:
                    record = json.loads(line)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    break
                if (
                    isinstance(record, dict)
                    and record.get("status") == "found"
                    and isinstance(record.get("signature"), str)
                ):
                    resolved.add(cast(str, record["signature"]))
    return resolved


def _write_bytes(path: Path, payload: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise OSError("segment write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rpc_error_code(item: JSONObject) -> int | None:
    error = item.get("error")
    if not isinstance(error, dict):
        return None
    code = error.get("code")
    if isinstance(code, int) and not isinstance(code, bool):
        return code
    return None


def _status_record(
    *,
    signature: str,
    status: str,
    attempt_count: int,
    observed_at: str,
    source_id: str,
    source_label: str,
    endpoint_origin: str,
    wire_files: Sequence[str],
    rpc_response: JSONObject | None = None,
    error_kind: str | None = None,
) -> JSONObject:
    record: JSONObject = {
        "schema_version": _SCHEMA_VERSION,
        "signature": signature,
        "status": status,
        "request_id": _request_id(signature),
        "attempt_count": attempt_count,
        "observed_at": observed_at,
        "source_id": source_id,
        "source_label": source_label,
        "endpoint_origin": endpoint_origin,
        "wire_files": list(wire_files),
    }
    if rpc_response is not None:
        record["rpc_response"] = rpc_response
        record["rpc_response_sha256"] = _sha256_bytes(_canonical_json(rpc_response))
    if error_kind is not None:
        record["error_kind"] = error_kind
    return record


def _hydrate_segment(
    signatures: tuple[str, ...],
    root: Path,
    config: SolanaRpcConfig,
    transport: JsonRpcTransport,
    *,
    clock: Callable[[], datetime],
    sleeper: Callable[[float], None],
) -> tuple[Path, dict[str, int]]:
    created_at = _iso_utc(clock())
    seed = "\0".join((*signatures, config.source_id, created_at)).encode("utf-8")
    segment_id = hashlib.sha256(seed).hexdigest()
    segment_root = root / "segments"
    segment_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    staging = Path(tempfile.mkdtemp(prefix=f".{segment_id}.", suffix=".partial", dir=segment_root))
    os.chmod(staging, 0o700)
    (staging / "wire").mkdir(mode=0o700)

    records: dict[str, JSONObject] = {}
    attempt_counts = {signature: 0 for signature in signatures}
    wire_files: dict[str, list[str]] = {signature: [] for signature in signatures}
    wire_sequence = 0
    try:
        for initial_batch in _iter_chunks(signatures, config.batch_size):
            pending = list(initial_batch)
            for attempt in range(1, config.max_attempts + 1):
                if not pending:
                    break
                for signature in pending:
                    attempt_counts[signature] += 1
                request_payload = _request_payload(pending, config)
                response: HttpResponse | None = None
                transport_kind: str | None = None
                try:
                    response = transport.post(request_payload, config.timeout_seconds)
                except Exception as error:  # transport implementations are external
                    transport_kind = _safe_transport_error(error)

                if response is not None:
                    wire_name = f"wire/response-{wire_sequence:08d}-attempt-{attempt:02d}.json"
                    wire_sequence += 1
                    wire_body = (
                        response.wire_body if response.wire_body is not None else response.body
                    )
                    _write_bytes(staging / wire_name, wire_body)
                    for signature in pending:
                        wire_files[signature].append(wire_name)

                if transport_kind is not None:
                    if attempt < config.max_attempts:
                        sleeper(
                            min(
                                config.initial_backoff_seconds * 2 ** (attempt - 1),
                                config.max_backoff_seconds,
                            )
                        )
                        continue
                    observed = _iso_utc(clock())
                    for signature in pending:
                        records[signature] = _status_record(
                            signature=signature,
                            status="transport_error",
                            attempt_count=attempt_counts[signature],
                            observed_at=observed,
                            source_id=config.source_id,
                            source_label=config.source_label,
                            endpoint_origin=config.endpoint_origin,
                            wire_files=wire_files[signature],
                            error_kind=transport_kind,
                        )
                    break

                assert response is not None
                if not 200 <= response.status < 300:
                    transient = response.status in _TRANSIENT_HTTP_STATUSES
                    if transient and attempt < config.max_attempts:
                        delay = _retry_after(response.headers)
                        if delay is None:
                            delay = min(
                                config.initial_backoff_seconds * 2 ** (attempt - 1),
                                config.max_backoff_seconds,
                            )
                        sleeper(min(delay, config.max_backoff_seconds))
                        continue
                    observed = _iso_utc(clock())
                    for signature in pending:
                        records[signature] = _status_record(
                            signature=signature,
                            status="transport_error",
                            attempt_count=attempt_counts[signature],
                            observed_at=observed,
                            source_id=config.source_id,
                            source_label=config.source_label,
                            endpoint_origin=config.endpoint_origin,
                            wire_files=wire_files[signature],
                            error_kind=f"http_status_{response.status}",
                        )
                    break

                try:
                    items = _response_items(response.body)
                except ValueError:
                    if attempt < config.max_attempts:
                        sleeper(
                            min(
                                config.initial_backoff_seconds * 2 ** (attempt - 1),
                                config.max_backoff_seconds,
                            )
                        )
                        continue
                    observed = _iso_utc(clock())
                    for signature in pending:
                        records[signature] = _status_record(
                            signature=signature,
                            status="protocol_error",
                            attempt_count=attempt_counts[signature],
                            observed_at=observed,
                            source_id=config.source_id,
                            source_label=config.source_label,
                            endpoint_origin=config.endpoint_origin,
                            wire_files=wire_files[signature],
                            error_kind="invalid_json_rpc_response",
                        )
                    break

                next_pending: list[str] = []
                observed = _iso_utc(clock())
                for signature in pending:
                    item = items.get(_request_id(signature))
                    if item is None:
                        if attempt < config.max_attempts:
                            next_pending.append(signature)
                        else:
                            records[signature] = _status_record(
                                signature=signature,
                                status="protocol_error",
                                attempt_count=attempt_counts[signature],
                                observed_at=observed,
                                source_id=config.source_id,
                                source_label=config.source_label,
                                endpoint_origin=config.endpoint_origin,
                                wire_files=wire_files[signature],
                                error_kind="missing_response_id",
                            )
                        continue
                    if "error" in item:
                        code = _rpc_error_code(item)
                        if code in _TRANSIENT_RPC_CODES and attempt < config.max_attempts:
                            next_pending.append(signature)
                            continue
                        records[signature] = _status_record(
                            signature=signature,
                            status="rpc_error",
                            attempt_count=attempt_counts[signature],
                            observed_at=observed,
                            source_id=config.source_id,
                            source_label=config.source_label,
                            endpoint_origin=config.endpoint_origin,
                            wire_files=wire_files[signature],
                            rpc_response=item,
                            error_kind="json_rpc_error",
                        )
                        continue
                    if item.get("result") is None:
                        if config.retry_null_results and attempt < config.max_attempts:
                            next_pending.append(signature)
                            continue
                        records[signature] = _status_record(
                            signature=signature,
                            status="missing",
                            attempt_count=attempt_counts[signature],
                            observed_at=observed,
                            source_id=config.source_id,
                            source_label=config.source_label,
                            endpoint_origin=config.endpoint_origin,
                            wire_files=wire_files[signature],
                            rpc_response=item,
                            error_kind="provider_returned_null",
                        )
                        continue
                    if not isinstance(item.get("result"), dict):
                        if attempt < config.max_attempts:
                            next_pending.append(signature)
                            continue
                        records[signature] = _status_record(
                            signature=signature,
                            status="protocol_error",
                            attempt_count=attempt_counts[signature],
                            observed_at=observed,
                            source_id=config.source_id,
                            source_label=config.source_label,
                            endpoint_origin=config.endpoint_origin,
                            wire_files=wire_files[signature],
                            error_kind="non_object_transaction_result",
                        )
                        continue
                    records[signature] = _status_record(
                        signature=signature,
                        status="found",
                        attempt_count=attempt_counts[signature],
                        observed_at=observed,
                        source_id=config.source_id,
                        source_label=config.source_label,
                        endpoint_origin=config.endpoint_origin,
                        wire_files=wire_files[signature],
                        rpc_response=item,
                    )
                pending = next_pending
                if pending and attempt < config.max_attempts:
                    sleeper(
                        min(
                            config.initial_backoff_seconds * 2 ** (attempt - 1),
                            config.max_backoff_seconds,
                        )
                    )

        if set(records) != set(signatures):
            raise RuntimeError(
                "hydration did not produce exactly one coverage record per signature"
            )

        coverage = b"".join(_canonical_json(records[signature]) for signature in signatures)
        exceptions = b"".join(
            _canonical_json(records[signature])
            for signature in signatures
            if records[signature]["status"] != "found"
        )
        _write_bytes(staging / "coverage.ndjson", coverage)
        _write_bytes(staging / "exceptions.ndjson", exceptions)

        files: dict[str, JSONObject] = {}
        for path in sorted(item for item in staging.rglob("*") if item.is_file()):
            relative = path.relative_to(staging).as_posix()
            files[relative] = {"byte_count": path.stat().st_size, "sha256": sha256_file(path)}
        statuses = ("found", "missing", "rpc_error", "transport_error", "protocol_error")
        counts = {status: 0 for status in statuses}
        for record in records.values():
            status = cast(str, record["status"])
            counts[status] += 1
        manifest: dict[str, object] = {
            "schema_version": _SCHEMA_VERSION,
            "segment_id": segment_id,
            "created_at": created_at,
            "source_id": config.source_id,
            "source_label": config.source_label,
            "endpoint_origin": config.endpoint_origin,
            "commitment": config.commitment,
            "max_supported_transaction_version": config.max_supported_transaction_version,
            "input_signature_count": len(signatures),
            "input_signature_sha256": _sha256_bytes("\n".join(signatures).encode("ascii")),
            "counts": counts,
            "files": files,
        }
        manifest_payload = _canonical_json(manifest)
        _write_bytes(staging / "manifest.json", manifest_payload)
        _fsync_directory(staging / "wire")
        _fsync_directory(staging)
        destination = segment_root / segment_id
        os.rename(staging, destination)
        _fsync_directory(segment_root)
        return destination, counts
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def hydrate_transactions(
    signatures: Iterable[str],
    output_root: str | os.PathLike[str],
    config: SolanaRpcConfig,
    *,
    transport: JsonRpcTransport | None = None,
    clock: Callable[[], datetime] = _utc_now,
    sleeper: Callable[[float], None] = time.sleep,
) -> HydrationResult:
    """Hydrate unique signatures into immutable, restartable archive segments.

    Only verified ``found`` records from prior segments for the same source are
    skipped.  A null result or error remains eligible for a future run or an
    alternate provider, which prevents provider retention gaps from becoming
    false claims of chain absence.
    """

    if not isinstance(config, SolanaRpcConfig):
        raise TypeError("config must be a SolanaRpcConfig")
    normalized = sorted({_signature(value) for value in signatures})
    requested = len(normalized)
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, root.stat().st_mode & 0o700)
    resolved = _load_resolved(root, config.source_id)
    remaining = [signature for signature in normalized if signature not in resolved]
    selected_transport = transport if transport is not None else UrllibJsonRpcTransport(config)

    statuses = ("found", "missing", "rpc_error", "transport_error", "protocol_error")
    totals = {status: 0 for status in statuses}
    paths: list[Path] = []
    for segment in _iter_chunks(remaining, config.segment_size):
        path, counts = _hydrate_segment(
            segment,
            root,
            config,
            selected_transport,
            clock=clock,
            sleeper=sleeper,
        )
        paths.append(path)
        for status, count in counts.items():
            totals[status] += count
    return HydrationResult(
        requested=requested,
        skipped_resolved=requested - len(remaining),
        found=totals["found"],
        missing=totals["missing"],
        rpc_error=totals["rpc_error"],
        transport_error=totals["transport_error"],
        protocol_error=totals["protocol_error"],
        segment_paths=tuple(paths),
    )


def verify_hydration_segment(path: str | os.PathLike[str]) -> None:
    """Verify an immutable hydration segment and every file it binds."""

    root = Path(path)
    manifest_path = root / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("hydration segment manifest is unreadable") from error
    if not isinstance(manifest, dict) or manifest.get("schema_version") != _SCHEMA_VERSION:
        raise ValueError("hydration segment manifest has an unsupported schema")
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise ValueError("hydration segment manifest has no file inventory")
    actual_files = {
        item.relative_to(root).as_posix()
        for item in root.rglob("*")
        if item.is_file() and item != manifest_path
    }
    if actual_files != set(files):
        raise ValueError("hydration segment file inventory differs from disk")
    for relative, raw_meta in files.items():
        if not isinstance(relative, str) or not isinstance(raw_meta, dict):
            raise ValueError("hydration segment file metadata is invalid")
        file_path = root / relative
        try:
            file_path.relative_to(root)
        except ValueError as error:
            raise ValueError("hydration segment contains an unsafe file path") from error
        expected_size = raw_meta.get("byte_count")
        expected_hash = raw_meta.get("sha256")
        if (
            isinstance(expected_size, bool)
            or not isinstance(expected_size, int)
            or expected_size < 0
            or not isinstance(expected_hash, str)
            or not _HEX64_RE.fullmatch(expected_hash)
        ):
            raise ValueError("hydration segment file metadata is invalid")
        if file_path.stat().st_size != expected_size or sha256_file(file_path) != expected_hash:
            raise ValueError(f"hydration segment integrity failure for {relative}")


__all__ = [
    "HttpResponse",
    "HydrationResult",
    "JsonRpcTransport",
    "SolanaRpcConfig",
    "UrllibJsonRpcTransport",
    "hydrate_transactions",
    "verify_hydration_segment",
]
