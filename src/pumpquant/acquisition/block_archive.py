"""Immutable full-block archive for efficient Solana transaction hydration.

One ``getBlock`` call preserves transaction order and hydrates every serialized
transaction and execution metadata record in a slot.  For Pump/PumpSwap-heavy
periods this is orders of magnitude fewer requests than one ``getTransaction``
call per discovered signature, while also retaining useful non-Pump context.

Each published segment contains exact JSON-RPC wire bodies, a small canonical
coverage/exception ledger, and SHA-256 metadata for every file.  Segments are
created beside their final destination and atomically renamed; verified found
or skipped slots are restart-safe.  Provider URL paths, query strings, and
header values are never persisted.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

from pumpquant.acquisition.transaction_hydrator import (
    JsonRpcTransport,
    SolanaRpcConfig,
    UrllibJsonRpcTransport,
)
from pumpquant.core.hashing import sha256_file

_SCHEMA_VERSION = 1
_TRANSIENT_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
_TRANSIENT_RPC_CODES = frozenset({429, -32004, -32005, -32603})
_SKIPPED_SLOT_CODES = frozenset({-32007})
_MISSING_HISTORY_CODES = frozenset({-32009})
_STATUSES = (
    "found",
    "skipped",
    "missing",
    "rpc_error",
    "transport_error",
    "protocol_error",
)


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode()


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_bytes(path: Path, payload: bytes) -> dict[str, object]:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise OSError("block archive write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return {"byte_count": len(payload), "sha256": _sha256(payload)}


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


def _slot(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Solana slots must be non-negative integers")
    return value


@dataclass(frozen=True, slots=True)
class BlockArchiveConfig:
    """Full-block request, checkpointing, and public-endpoint pacing policy."""

    slots_per_segment: int = 25
    max_concurrency: int = 4
    requests_per_second: float = 0.0
    include_rewards: bool = True

    def __post_init__(self) -> None:
        for name in ("slots_per_segment", "max_concurrency"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if (
            isinstance(self.requests_per_second, bool)
            or not isinstance(self.requests_per_second, (int, float))
            or self.requests_per_second < 0
        ):
            raise ValueError("requests_per_second must be a non-negative number")
        if not isinstance(self.include_rewards, bool):
            raise TypeError("include_rewards must be boolean")


@dataclass(frozen=True, slots=True)
class BlockArchiveProgress:
    completed_slots: int
    total_slots: int
    segment_path: Path
    counts: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class BlockArchiveResult:
    requested: int
    skipped_resolved: int
    found: int
    skipped: int
    missing: int
    rpc_error: int
    transport_error: int
    protocol_error: int
    segment_paths: tuple[Path, ...]

    @property
    def unresolved(self) -> int:
        return self.missing + self.rpc_error + self.transport_error + self.protocol_error


@dataclass(frozen=True, slots=True)
class _BlockFetch:
    slot: int
    status: str
    attempt_count: int
    observed_at: str
    wire_bodies: tuple[bytes, ...]
    http_statuses: tuple[int | None, ...]
    summary: Mapping[str, object]
    error_kind: str | None = None


class _RateLimiter:
    def __init__(
        self,
        requests_per_second: float,
        *,
        monotonic: Callable[[], float],
        sleeper: Callable[[float], None],
    ) -> None:
        self._interval = 0.0 if requests_per_second == 0 else 1.0 / requests_per_second
        self._monotonic = monotonic
        self._sleeper = sleeper
        self._lock = threading.Lock()
        self._next_start = 0.0

    def wait(self) -> None:
        if self._interval == 0:
            return
        with self._lock:
            now = self._monotonic()
            reserved = max(now, self._next_start)
            self._next_start = reserved + self._interval
        delay = reserved - now
        if delay > 0:
            self._sleeper(delay)


class _TransportFactory(Protocol):
    def __call__(self) -> JsonRpcTransport: ...


def _request(slot: int, rpc: SolanaRpcConfig, config: BlockArchiveConfig) -> bytes:
    payload = {
        "jsonrpc": "2.0",
        "id": f"block-{slot}",
        "method": "getBlock",
        "params": [
            slot,
            {
                "commitment": rpc.commitment,
                "encoding": "base64",
                "maxSupportedTransactionVersion": rpc.max_supported_transaction_version,
                "rewards": config.include_rewards,
                "transactionDetails": "full",
            },
        ],
    }
    return json.dumps(payload, allow_nan=False, separators=(",", ":")).encode()


def _parse_response(body: bytes, slot: int) -> dict[str, object]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("getBlock response was not valid UTF-8 JSON") from error
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("getBlock response was not a JSON object")
    response = cast(dict[str, object], value)
    if response.get("jsonrpc") != "2.0" or response.get("id") != f"block-{slot}":
        raise ValueError("getBlock response had an invalid version or id")
    if ("result" in response) == ("error" in response):
        raise ValueError("getBlock response must contain exactly one of result or error")
    return response


def _rpc_code(response: Mapping[str, object]) -> int | None:
    error = response.get("error")
    if not isinstance(error, Mapping):
        return None
    code = error.get("code")
    if isinstance(code, int) and not isinstance(code, bool):
        return code
    return None


def _summary(result: Mapping[str, object]) -> dict[str, object]:
    blockhash = result.get("blockhash")
    previous = result.get("previousBlockhash")
    parent = result.get("parentSlot")
    block_time = result.get("blockTime")
    block_height = result.get("blockHeight")
    transactions = result.get("transactions")
    rewards = result.get("rewards")
    if not isinstance(blockhash, str) or not isinstance(previous, str):
        raise ValueError("getBlock result is missing block hashes")
    if isinstance(parent, bool) or not isinstance(parent, int) or parent < 0:
        raise ValueError("getBlock result has an invalid parentSlot")
    if block_time is not None and (isinstance(block_time, bool) or not isinstance(block_time, int)):
        raise ValueError("getBlock result has an invalid blockTime")
    if block_height is not None and (
        isinstance(block_height, bool) or not isinstance(block_height, int)
    ):
        raise ValueError("getBlock result has an invalid blockHeight")
    if not isinstance(transactions, list):
        raise ValueError("getBlock result has no full transactions array")
    if rewards is not None and not isinstance(rewards, list):
        raise ValueError("getBlock result has an invalid rewards array")
    return {
        "blockhash": blockhash,
        "previous_blockhash": previous,
        "parent_slot": parent,
        "block_time": block_time,
        "block_height": block_height,
        "transaction_count": len(transactions),
        "reward_count": len(rewards) if isinstance(rewards, list) else None,
    }


def _safe_transport_error(error: BaseException) -> str:
    if isinstance(error, TimeoutError):
        return "timeout"
    return f"transport_{type(error).__name__.casefold()}"


def _backoff(rpc: SolanaRpcConfig, attempt: int) -> float:
    return float(min(rpc.initial_backoff_seconds * 2 ** (attempt - 1), rpc.max_backoff_seconds))


def _fetch_block(
    slot: int,
    rpc: SolanaRpcConfig,
    config: BlockArchiveConfig,
    transport: JsonRpcTransport,
    limiter: _RateLimiter,
    *,
    clock: Callable[[], datetime],
    sleeper: Callable[[float], None],
) -> _BlockFetch:
    wire_bodies: list[bytes] = []
    http_statuses: list[int | None] = []
    payload = _request(slot, rpc, config)
    for attempt in range(1, rpc.max_attempts + 1):
        limiter.wait()
        try:
            http = transport.post(payload, rpc.timeout_seconds)
        except Exception as error:  # external transport boundary
            http_statuses.append(None)
            if attempt < rpc.max_attempts:
                sleeper(_backoff(rpc, attempt))
                continue
            return _BlockFetch(
                slot,
                "transport_error",
                attempt,
                _iso_utc(clock()),
                tuple(wire_bodies),
                tuple(http_statuses),
                {},
                _safe_transport_error(error),
            )

        wire_bodies.append(http.wire_body if http.wire_body is not None else http.body)
        http_statuses.append(http.status)
        if not 200 <= http.status < 300:
            if http.status in _TRANSIENT_HTTP_STATUSES and attempt < rpc.max_attempts:
                sleeper(_backoff(rpc, attempt))
                continue
            return _BlockFetch(
                slot,
                "transport_error",
                attempt,
                _iso_utc(clock()),
                tuple(wire_bodies),
                tuple(http_statuses),
                {},
                f"http_status_{http.status}",
            )

        try:
            response = _parse_response(http.body, slot)
        except ValueError:
            if attempt < rpc.max_attempts:
                sleeper(_backoff(rpc, attempt))
                continue
            return _BlockFetch(
                slot,
                "protocol_error",
                attempt,
                _iso_utc(clock()),
                tuple(wire_bodies),
                tuple(http_statuses),
                {},
                "invalid_json_rpc_response",
            )

        if "error" in response:
            code = _rpc_code(response)
            if code in _TRANSIENT_RPC_CODES and attempt < rpc.max_attempts:
                sleeper(_backoff(rpc, attempt))
                continue
            if code in _SKIPPED_SLOT_CODES:
                status, kind = "skipped", "slot_skipped"
            elif code in _MISSING_HISTORY_CODES:
                status, kind = "missing", "provider_history_missing"
            else:
                status, kind = "rpc_error", "json_rpc_error"
            return _BlockFetch(
                slot,
                status,
                attempt,
                _iso_utc(clock()),
                tuple(wire_bodies),
                tuple(http_statuses),
                {"rpc_error_code": code},
                kind,
            )

        result = response.get("result")
        if result is None:
            return _BlockFetch(
                slot,
                "missing",
                attempt,
                _iso_utc(clock()),
                tuple(wire_bodies),
                tuple(http_statuses),
                {},
                "provider_returned_null",
            )
        if not isinstance(result, Mapping):
            if attempt < rpc.max_attempts:
                sleeper(_backoff(rpc, attempt))
                continue
            return _BlockFetch(
                slot,
                "protocol_error",
                attempt,
                _iso_utc(clock()),
                tuple(wire_bodies),
                tuple(http_statuses),
                {},
                "non_object_block_result",
            )
        try:
            summary = _summary(result)
        except ValueError:
            if attempt < rpc.max_attempts:
                sleeper(_backoff(rpc, attempt))
                continue
            return _BlockFetch(
                slot,
                "protocol_error",
                attempt,
                _iso_utc(clock()),
                tuple(wire_bodies),
                tuple(http_statuses),
                {},
                "incomplete_block_result",
            )
        return _BlockFetch(
            slot,
            "found",
            attempt,
            _iso_utc(clock()),
            tuple(wire_bodies),
            tuple(http_statuses),
            summary,
        )
    raise AssertionError("unreachable getBlock retry state")


def _record(
    fetch: _BlockFetch,
    rpc: SolanaRpcConfig,
    wire_files: list[dict[str, object]],
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": _SCHEMA_VERSION,
        "slot": fetch.slot,
        "status": fetch.status,
        "attempt_count": fetch.attempt_count,
        "observed_at": fetch.observed_at,
        "source_id": rpc.source_id,
        "source_label": rpc.source_label,
        "endpoint_origin": rpc.endpoint_origin,
        "http_statuses": list(fetch.http_statuses),
        "wire_files": wire_files,
        **fetch.summary,
    }
    if fetch.error_kind is not None:
        value["error_kind"] = fetch.error_kind
    return value


def _chunks(values: list[int], size: int) -> Iterable[tuple[int, ...]]:
    for start in range(0, len(values), size):
        yield tuple(values[start : start + size])


def verify_block_segment(path: str | os.PathLike[str]) -> None:
    """Verify a published full-block segment and every exact wire body."""

    root = Path(path)
    manifest_path = root / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("block segment manifest is unreadable") from error
    if not isinstance(manifest, dict) or manifest.get("schema_version") != _SCHEMA_VERSION:
        raise ValueError("block segment manifest has an unsupported schema")
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise ValueError("block segment manifest has no file inventory")
    actual = {
        item.relative_to(root).as_posix()
        for item in root.rglob("*")
        if item.is_file() and item != manifest_path
    }
    if actual != set(files):
        raise ValueError("block segment file inventory differs from disk")
    for relative, raw_meta in files.items():
        if not isinstance(relative, str) or not isinstance(raw_meta, dict):
            raise ValueError("block segment file metadata is invalid")
        candidate = root / relative
        try:
            candidate.relative_to(root)
        except ValueError as error:
            raise ValueError("block segment file path is unsafe") from error
        size = raw_meta.get("byte_count")
        digest = raw_meta.get("sha256")
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or not isinstance(digest, str)
            or len(digest) != 64
        ):
            raise ValueError("block segment file metadata is invalid")
        if candidate.stat().st_size != size or sha256_file(candidate) != digest:
            raise ValueError(f"block segment integrity failure for {relative}")


def _resolved_slots(
    root: Path,
    rpc: SolanaRpcConfig,
    config: BlockArchiveConfig,
) -> set[int]:
    resolved: set[int] = set()
    for manifest_path in sorted((root / "segments").glob("*/manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("existing block segment manifest is unreadable") from error
        expected_contract = {
            "source_id": rpc.source_id,
            "commitment": rpc.commitment,
            "encoding": "base64",
            "transaction_details": "full",
            "include_rewards": config.include_rewards,
            "max_supported_transaction_version": rpc.max_supported_transaction_version,
        }
        if not isinstance(manifest, dict) or any(
            manifest.get(key) != value for key, value in expected_contract.items()
        ):
            continue
        verify_block_segment(manifest_path.parent)
        with (manifest_path.parent / "coverage.ndjson").open("rb") as rows:
            for line in rows:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("existing block coverage record is invalid")
                slot = record.get("slot")
                if record.get("status") in {"found", "skipped"}:
                    resolved.add(_slot(slot))
    return resolved


def _archive_segment(
    slots: tuple[int, ...],
    root: Path,
    rpc: SolanaRpcConfig,
    config: BlockArchiveConfig,
    transport: JsonRpcTransport,
    limiter: _RateLimiter,
    *,
    clock: Callable[[], datetime],
    sleeper: Callable[[float], None],
) -> tuple[Path, dict[str, int]]:
    created_at = _iso_utc(clock())
    segment_seed = f"{rpc.source_id}\0{created_at}\0" + ",".join(map(str, slots))
    segment_id = _sha256(segment_seed.encode())
    segment_root = root / "segments"
    segment_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    staging = Path(tempfile.mkdtemp(prefix=f".{segment_id}.", suffix=".partial", dir=segment_root))
    os.chmod(staging, 0o700)
    wire_root = staging / "wire"
    wire_root.mkdir(mode=0o700)
    files: dict[str, dict[str, object]] = {}
    records: dict[int, dict[str, object]] = {}
    try:
        with ThreadPoolExecutor(
            max_workers=min(config.max_concurrency, len(slots)),
            thread_name_prefix="pumpquant-getblock",
        ) as pool:
            futures = {
                pool.submit(
                    _fetch_block,
                    slot,
                    rpc,
                    config,
                    transport,
                    limiter,
                    clock=clock,
                    sleeper=sleeper,
                ): slot
                for slot in slots
            }
            for future in as_completed(futures):
                fetch = future.result()
                wire_entries: list[dict[str, object]] = []
                for attempt, body in enumerate(fetch.wire_bodies, start=1):
                    relative = f"wire/slot-{fetch.slot:012d}-attempt-{attempt:02d}.json"
                    metadata = _write_bytes(staging / relative, body)
                    files[relative] = metadata
                    wire_entries.append({"path": relative, **metadata})
                records[fetch.slot] = _record(fetch, rpc, wire_entries)

        if set(records) != set(slots):
            raise RuntimeError("block archive did not produce one coverage record per slot")
        coverage = b"".join(_canonical_json(records[slot]) for slot in slots)
        exceptions = b"".join(
            _canonical_json(records[slot])
            for slot in slots
            if records[slot]["status"] not in {"found", "skipped"}
        )
        files["coverage.ndjson"] = _write_bytes(staging / "coverage.ndjson", coverage)
        files["exceptions.ndjson"] = _write_bytes(staging / "exceptions.ndjson", exceptions)
        counts = {status: 0 for status in _STATUSES}
        for record in records.values():
            counts[cast(str, record["status"])] += 1
        manifest: dict[str, object] = {
            "schema_version": _SCHEMA_VERSION,
            "kind": "pumpquant.full_block_segment",
            "segment_id": segment_id,
            "created_at": created_at,
            "source_id": rpc.source_id,
            "source_label": rpc.source_label,
            "endpoint_origin": rpc.endpoint_origin,
            "commitment": rpc.commitment,
            "encoding": "base64",
            "transaction_details": "full",
            "include_rewards": config.include_rewards,
            "max_supported_transaction_version": rpc.max_supported_transaction_version,
            "slot_count": len(slots),
            "min_slot": min(slots),
            "max_slot": max(slots),
            "slots_sha256": _sha256("\n".join(map(str, slots)).encode()),
            "counts": counts,
            "files": files,
        }
        _write_bytes(staging / "manifest.json", _canonical_json(manifest))
        _fsync_directory(wire_root)
        _fsync_directory(staging)
        destination = segment_root / segment_id
        os.rename(staging, destination)
        _fsync_directory(segment_root)
        return destination, counts
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def archive_full_blocks(
    slots: Iterable[int],
    output_root: str | os.PathLike[str],
    rpc: SolanaRpcConfig,
    config: BlockArchiveConfig | None = None,
    *,
    transport: JsonRpcTransport | None = None,
    clock: Callable[[], datetime] = _utc_now,
    monotonic: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
    progress: Callable[[BlockArchiveProgress], None] | None = None,
) -> BlockArchiveResult:
    """Fetch full finalized blocks and publish restartable immutable segments."""

    if not isinstance(rpc, SolanaRpcConfig):
        raise TypeError("rpc must be a SolanaRpcConfig")
    if config is None:
        config = BlockArchiveConfig()
    if not isinstance(config, BlockArchiveConfig):
        raise TypeError("config must be a BlockArchiveConfig")
    requested_slots = sorted({_slot(value) for value in slots})
    requested = len(requested_slots)
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, root.stat().st_mode & 0o700)
    resolved = _resolved_slots(root, rpc, config)
    remaining = [slot for slot in requested_slots if slot not in resolved]
    selected_transport = transport if transport is not None else UrllibJsonRpcTransport(rpc)
    limiter = _RateLimiter(
        config.requests_per_second,
        monotonic=monotonic,
        sleeper=sleeper,
    )
    totals = {status: 0 for status in _STATUSES}
    paths: list[Path] = []
    completed = 0
    for segment_slots in _chunks(remaining, config.slots_per_segment):
        path, counts = _archive_segment(
            segment_slots,
            root,
            rpc,
            config,
            selected_transport,
            limiter,
            clock=clock,
            sleeper=sleeper,
        )
        paths.append(path)
        completed += len(segment_slots)
        for status, count in counts.items():
            totals[status] += count
        if progress is not None:
            progress(BlockArchiveProgress(completed, len(remaining), path, counts))
    return BlockArchiveResult(
        requested=requested,
        skipped_resolved=requested - len(remaining),
        found=totals["found"],
        skipped=totals["skipped"],
        missing=totals["missing"],
        rpc_error=totals["rpc_error"],
        transport_error=totals["transport_error"],
        protocol_error=totals["protocol_error"],
        segment_paths=tuple(paths),
    )


__all__ = [
    "BlockArchiveConfig",
    "BlockArchiveProgress",
    "BlockArchiveResult",
    "archive_full_blocks",
    "verify_block_segment",
]
