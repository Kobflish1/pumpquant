"""Verified k-way streaming merge of normalised swap chunks into bars.

Normalised swap datasets are commonly published as several independently
sealed directories.  This module verifies every directory before opening its
rows, performs a deterministic merge on the complete
``SwapEvent.ordering_key``, and feeds that one global stream to
:func:`pumpquant.features.bars.iter_bars_chain_ordered`.

The merge keeps one bounded decoded batch per input chunk in a heap.  Duplicate
event IDs use a temporary SQLite index and source-order regressions are checked
as each row advances, so Python memory is bounded by
``O(chunks * batch_size)`` rather than the number of swaps.  Output writers are
also streaming and publish through a temporary sibling followed by
``os.replace``; an error therefore cannot leave a partially written bars file
in place.
"""

from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import math
import os
import sqlite3
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, cast

from pumpquant.core.hashing import sha256_file
from pumpquant.core.io import iter_swap_event_batches
from pumpquant.core.schema import Bar, SwapEvent
from pumpquant.features.bars import iter_bars_chain_ordered

NORMALIZED_KIND = "pumpquant.normalized_swap_dataset"
SCHEMA_VERSION = 1
PHYSICAL_ORDER = ("slot", "tx_index", "instruction_index", "event_id")
DEFAULT_BATCH_SIZE = 32_768


class ChunkBarsError(RuntimeError):
    """A sealed chunk or the global merge violated an invariant."""


@dataclass(frozen=True, slots=True)
class VerifiedSwapChunk:
    """Metadata for one normalised swap chunk after manifest verification."""

    root: Path
    manifest_path: Path
    swaps_path: Path
    manifest_sha256: str
    swaps_sha256: str
    row_count: int
    inventoried_paths: tuple[Path, ...] = ()


@dataclass(frozen=True, slots=True)
class ChunkBarsReport:
    """Small publication report returned by :func:`write_chunk_bars`."""

    status: str
    output: str
    chunks: int
    swaps_read: int
    bars_written: int
    swaps_skipped_unanchored: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "output": self.output,
            "chunks": self.chunks,
            "swaps_read": self.swaps_read,
            "bars_written": self.bars_written,
            "swaps_skipped_unanchored": self.swaps_skipped_unanchored,
        }


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ChunkBarsError(f"{label} must be an object with text keys")
    return cast(Mapping[str, object], value)


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ChunkBarsError(f"{label} must be non-empty text")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ChunkBarsError(f"{label} must be a non-negative integer")
    return value


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ChunkBarsError(f"{label} must be a positive integer")
    return value


def _sha256_file(path: Path) -> str:
    try:
        return sha256_file(path)
    except OSError as error:
        raise ChunkBarsError(f"cannot read manifested file: {path}") from error


def _safe_relative(root: Path, value: object, label: str) -> Path:
    relative = Path(_text(value, label))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ChunkBarsError(f"{label} escapes its dataset")
    current = root
    for component in relative.parts:
        current /= component
        if current.is_symlink():
            raise ChunkBarsError(f"{label} traverses a symlink")
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as error:
        raise ChunkBarsError(f"{label} escapes its dataset") from error
    return candidate


def _file_entries(manifest: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    """Normalise the mapping/list file-inventory forms used by manifests."""

    raw = manifest.get("files")
    entries: dict[str, Mapping[str, object]] = {}
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            if not isinstance(key, str) or not key.strip():
                raise ChunkBarsError("manifest file paths must be non-empty text")
            entries[key] = _mapping(value, f"manifest file {key}")
        return entries
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        for index, value in enumerate(raw):
            item = _mapping(value, f"manifest file {index}")
            relative = _text(item.get("path"), f"manifest file {index} path")
            if relative in entries:
                raise ChunkBarsError(f"manifest inventories duplicate file: {relative}")
            entries[relative] = item
        return entries
    raise ChunkBarsError("normalized manifest has no file inventory")


def _resolve_chunk(value: str | Path) -> VerifiedSwapChunk:
    raw = Path(value).expanduser()
    if raw.is_symlink():
        raise ChunkBarsError(f"chunk input must not be a symlink: {raw}")
    if raw.is_dir():
        root = raw.resolve()
        manifest_path = root / "manifest.json"
    elif raw.is_file() and raw.name == "manifest.json":
        root = raw.parent.resolve()
        manifest_path = raw.resolve()
    else:
        raise ChunkBarsError("chunk input must be a sealed directory or manifest.json: " + str(raw))
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ChunkBarsError(f"sealed chunk manifest is missing: {manifest_path}")
    try:
        body = manifest_path.read_bytes()
        manifest = _mapping(json.loads(body), "chunk manifest")
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ChunkBarsError(f"chunk manifest is invalid JSON: {manifest_path}") from error
    if manifest.get("kind") != NORMALIZED_KIND:
        raise ChunkBarsError(f"unsupported chunk manifest kind: {manifest.get('kind')!r}")
    if manifest.get("version") != SCHEMA_VERSION:
        raise ChunkBarsError(f"unsupported chunk manifest version: {manifest.get('version')!r}")
    declared_order = manifest.get("physical_order")
    if not isinstance(declared_order, Sequence) or isinstance(
        declared_order, (str, bytes, bytearray)
    ):
        raise ChunkBarsError("chunk physical_order is malformed or missing")
    if tuple(declared_order) != PHYSICAL_ORDER:
        raise ChunkBarsError("chunk physical_order is not canonical")

    row_count = _nonnegative_int(manifest.get("row_count"), "chunk row_count")
    entries = _file_entries(manifest)
    candidates = [relative for relative in entries if Path(relative).name == "swaps.parquet"]
    if len(candidates) != 1:
        raise ChunkBarsError("chunk manifest must inventory exactly one swaps.parquet file")
    swaps_relative = candidates[0]
    swaps_path = _safe_relative(root, swaps_relative, "swaps.parquet path")

    # Verify every declared byte/hash, not just swaps.parquet.  This catches a
    # removed or modified audit/source sidecar before any row is consumed.
    swaps_digest = ""
    inventoried_paths: list[Path] = []
    for relative, metadata in entries.items():
        path = _safe_relative(root, relative, f"manifest file {relative} path")
        if path.is_symlink() or not path.is_file():
            raise ChunkBarsError(f"manifest file is missing or not regular: {path}")
        byte_count = _nonnegative_int(
            metadata.get("byte_count"), f"manifest file {relative} byte_count"
        )
        expected = _text(metadata.get("sha256"), f"manifest file {relative} sha256").lower()
        if len(expected) != 64 or any(
            character not in "0123456789abcdef" for character in expected
        ):
            raise ChunkBarsError(f"manifest file {relative} sha256 must be SHA-256 hex")
        actual = _sha256_file(path)
        if path.stat().st_size != byte_count or actual != expected:
            raise ChunkBarsError(f"manifest file {relative} failed byte/hash verification")
        inventoried_paths.append(path)
        if path == swaps_path:
            swaps_digest = actual
    if not swaps_digest:  # pragma: no cover - candidates guarantees this
        raise ChunkBarsError("swaps.parquet file metadata is missing")

    try:
        import pyarrow.parquet as pq
    except ImportError as error:  # pragma: no cover - optional dependency
        raise ChunkBarsError("Parquet support requires: pip install 'pumpquant[data]'") from error
    parquet = pq.ParquetFile(swaps_path)
    try:
        metadata = parquet.metadata
        actual_rows = 0 if metadata is None else int(metadata.num_rows)
        if actual_rows != row_count:
            raise ChunkBarsError(
                f"swaps.parquet row count differs from manifest ({actual_rows} != {row_count})"
            )
        schema_digest = manifest.get("swap_schema_sha256")
        if schema_digest is not None:
            expected_schema = _text(schema_digest, "chunk swap_schema_sha256").lower()
            actual_schema = hashlib.sha256(
                parquet.schema_arrow.serialize().to_pybytes()
            ).hexdigest()
            if expected_schema != actual_schema:
                raise ChunkBarsError("swaps.parquet schema hash differs from manifest")
    finally:
        close = getattr(parquet, "close", None)
        if callable(close):
            close()
    return VerifiedSwapChunk(
        root=root,
        manifest_path=manifest_path,
        swaps_path=swaps_path,
        manifest_sha256=hashlib.sha256(body).hexdigest(),
        swaps_sha256=swaps_digest,
        row_count=row_count,
        inventoried_paths=tuple(inventoried_paths),
    )


def verify_normalized_chunks(
    chunks: Sequence[str | Path] | str | Path,
) -> tuple[VerifiedSwapChunk, ...]:
    """Verify and return unique sealed normalised swap chunks.

    Verification includes manifest kind/version, every inventoried file's byte
    count and SHA-256, Parquet row count, and (when present) schema hash.  No
    rows are decoded by this function.
    """

    if isinstance(chunks, (str, Path)):
        values: tuple[str | Path, ...] = (chunks,)
    else:
        values = tuple(chunks)
    if not values:
        raise ChunkBarsError("at least one normalized swap chunk is required")
    result: list[VerifiedSwapChunk] = []
    seen: set[Path] = set()
    for value in values:
        info = _resolve_chunk(value)
        if info.swaps_path in seen:
            raise ChunkBarsError(f"duplicate normalized swap chunk: {info.swaps_path}")
        seen.add(info.swaps_path)
        result.append(info)
    return tuple(result)


def _iter_checked_chunk(info: VerifiedSwapChunk, *, batch_size: int) -> Iterator[SwapEvent]:
    previous: tuple[int, int, int, str] | None = None
    count = 0
    for batch in iter_swap_event_batches(info.swaps_path, batch_size=batch_size):
        for event in batch:
            count += 1
            key = event.ordering_key
            if previous is not None and key < previous:
                raise ChunkBarsError(
                    f"chunk ordering regression in {info.swaps_path}: {key!r} follows {previous!r}"
                )
            previous = key
            yield event
    if count != info.row_count:
        raise ChunkBarsError(
            f"chunk row count changed while streaming {info.swaps_path}: "
            f"{count} != {info.row_count}"
        )


def iter_merged_swap_events(
    chunks: Sequence[str | Path] | str | Path,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Iterator[SwapEvent]:
    """Yield one globally chain-ordered stream from sealed Parquet chunks.

    At most one row per chunk is retained by the heap.  A disk-backed event ID
    index rejects duplicate/overlapping rows across chunks and each source is
    checked for regressions before its next row enters the heap.
    """

    size = _positive_int(batch_size, "batch_size")

    def _lazy() -> Iterator[SwapEvent]:
        # Match the existing streaming bar API: constructing the iterator does
        # not open or consume input, while the first iteration still verifies
        # every manifest and file before yielding any row.
        verified = verify_normalized_chunks(chunks)
        yield from _iter_merged_verified_swap_events(verified, batch_size=size)

    return _lazy()


def _iter_merged_verified_swap_events(
    verified: Sequence[VerifiedSwapChunk],
    *,
    batch_size: int,
) -> Iterator[SwapEvent]:
    """Merge metadata that was already verified by the caller."""

    def _iter() -> Iterator[SwapEvent]:
        iterators = [_iter_checked_chunk(info, batch_size=batch_size) for info in verified]
        heap: list[tuple[tuple[int, int, int, str], int, SwapEvent]] = []
        with tempfile.TemporaryDirectory(prefix="pumpquant-chunk-merge-") as directory:
            connection = sqlite3.connect(Path(directory) / "event_ids.sqlite3")
            # The index is disk-backed.  SQLite's page cache is
            # bounded below while event IDs do not accumulate in Python sets.
            connection.execute("PRAGMA journal_mode=OFF")
            connection.execute("PRAGMA synchronous=OFF")
            connection.execute("PRAGMA temp_store=FILE")
            connection.execute("PRAGMA cache_size=-8192")
            connection.execute("CREATE TABLE event_ids(event_id TEXT PRIMARY KEY) WITHOUT ROWID")
            previous: tuple[int, int, int, str] | None = None
            try:
                for index, iterator in enumerate(iterators):
                    try:
                        event = next(iterator)
                    except StopIteration:
                        continue
                    heapq.heappush(heap, (event.ordering_key, index, event))
                while heap:
                    key, index, event = heapq.heappop(heap)
                    if previous is not None and key < previous:
                        raise ChunkBarsError(
                            f"global ordering regression: {key!r} follows {previous!r}"
                        )
                    previous = key
                    try:
                        connection.execute(
                            "INSERT INTO event_ids(event_id) VALUES (?)",
                            (event.event_id,),
                        )
                    except sqlite3.IntegrityError as error:
                        raise ChunkBarsError(
                            f"duplicate/overlapping event_id across chunks: {event.event_id}"
                        ) from error
                    yield event
                    try:
                        successor = next(iterators[index])
                    except StopIteration:
                        continue
                    heapq.heappush(heap, (successor.ordering_key, index, successor))
            finally:
                connection.close()
                for iterator in iterators:
                    close = getattr(iterator, "close", None)
                    if callable(close):
                        close()

    return _iter()


def _validate_launch_times(
    launch_times: Mapping[str, object] | None,
) -> Mapping[str, int]:
    if launch_times is None:
        return {}
    validated: dict[str, int] = {}
    for mint, timestamp in launch_times.items():
        if not isinstance(mint, str) or not mint.strip():
            raise ChunkBarsError("launch_times keys must be non-empty mint strings")
        if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp < 0:
            raise ChunkBarsError("launch timestamps must be nonnegative integers")
        validated[mint] = timestamp
    return validated


def load_launch_times(path: str | Path) -> dict[str, int]:
    """Load a JSON ``{mint: launch_block_time_ns}`` mapping without inference."""

    source = Path(path).expanduser()
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ChunkBarsError(f"launch-times JSON is invalid: {source}") from error
    mapping = _mapping(value, "launch-times mapping")
    return dict(_validate_launch_times(mapping))


def _coerce_launch_times(
    launch_times: Mapping[str, int] | str | Path | None,
) -> Mapping[str, int]:
    if isinstance(launch_times, (str, Path)):
        return load_launch_times(launch_times)
    return _validate_launch_times(launch_times)


def _bar_payload(bar: Bar) -> dict[str, object]:
    payload: dict[str, object] = {field.name: getattr(bar, field.name) for field in fields(Bar)}
    payload["phase"] = bar.phase.value
    return {"schema_version": SCHEMA_VERSION, "record_type": "bar", **payload}


def _bar_schema() -> Any:
    import pyarrow as pa

    # Keep this explicit so an empty output still has the same schema as
    # io.write_parquet/read_bars, and so batches cannot alter inferred types.
    return pa.schema(
        [
            pa.field("schema_version", pa.int64(), nullable=False),
            pa.field("record_type", pa.string(), nullable=False),
            pa.field("market_id", pa.string(), nullable=False),
            pa.field("mint", pa.string(), nullable=False),
            pa.field("quote_mint", pa.string(), nullable=False),
            pa.field("start_time_ns", pa.int64(), nullable=False),
            pa.field("end_time_ns", pa.int64(), nullable=False),
            pa.field("phase", pa.string(), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("high", pa.float64(), nullable=False),
            pa.field("low", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("base_volume", pa.float64(), nullable=False),
            pa.field("quote_volume", pa.float64(), nullable=False),
            pa.field("buy_quote_volume", pa.float64(), nullable=False),
            pa.field("sell_quote_volume", pa.float64(), nullable=False),
            pa.field("trade_count", pa.int64(), nullable=False),
            pa.field("unique_traders", pa.int64(), nullable=False),
            pa.field("virtual_base_reserve", pa.float64(), nullable=False),
            pa.field("virtual_quote_reserve", pa.float64(), nullable=False),
            pa.field("token_age_seconds", pa.float64()),
            pa.field("real_base_reserve", pa.float64()),
            pa.field("real_quote_reserve", pa.float64()),
            pa.field("venue_fee_bps", pa.float64()),
            pa.field("available_at_ns", pa.int64()),
        ]
    )


def _fsync_parent(path: Path) -> None:
    try:
        descriptor = os.open(path.parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_jsonl_atomic(
    target: Path,
    bars: Iterator[Bar],
) -> int:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".partial", dir=target.parent
    )
    temporary = Path(temporary_name)
    published = False
    count = 0
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            for bar in bars:
                handle.write(json.dumps(_bar_payload(bar), separators=(",", ":"), allow_nan=False))
                handle.write("\n")
                count += 1
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        published = True
        _fsync_parent(target)
        return count
    finally:
        if not published:
            temporary.unlink(missing_ok=True)


def _write_parquet_atomic(
    target: Path,
    bars: Iterator[Bar],
    *,
    batch_size: int,
) -> int:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:  # pragma: no cover - optional dependency
        raise ChunkBarsError("Parquet support requires: pip install 'pumpquant[data]'") from error
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".partial", dir=target.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    published = False
    count = 0
    writer: Any | None = None
    try:
        schema = _bar_schema()
        writer = pq.ParquetWriter(temporary, schema, compression="zstd")
        pending: list[dict[str, object]] = []
        for bar in bars:
            pending.append(_bar_payload(bar))
            count += 1
            if len(pending) >= batch_size:
                writer.write_table(pa.Table.from_pylist(pending, schema=schema))
                pending.clear()
        if pending:
            writer.write_table(pa.Table.from_pylist(pending, schema=schema))
        writer.close()
        writer = None
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        published = True
        _fsync_parent(target)
        return count
    finally:
        if writer is not None:
            writer.close()
        if not published:
            temporary.unlink(missing_ok=True)


def iter_chunk_bars(
    chunks: Sequence[str | Path] | str | Path,
    interval_ns: int,
    launch_times: Mapping[str, int] | str | Path | None = None,
    *,
    require_all_mints: bool = False,
    anchored_only: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Iterator[Bar]:
    """Stream bars built from the one global event merge.

    ``launch_times`` is used exactly as supplied; no first-observed or other
    inferred timestamp is ever substituted.  With ``require_all_mints=True``,
    every mint observed in the chunks must have a mapping entry.  With
    ``anchored_only=True``, unanchored events are skipped instead; the two
    flags are mutually exclusive.
    """

    if not isinstance(require_all_mints, bool) or not isinstance(anchored_only, bool):
        raise ChunkBarsError("require_all_mints and anchored_only must be boolean")
    if require_all_mints and anchored_only:
        raise ChunkBarsError("require_all_mints and anchored_only are mutually exclusive")
    launches = _coerce_launch_times(launch_times)
    merged = iter_merged_swap_events(chunks, batch_size=batch_size)

    def _checked() -> Iterator[SwapEvent]:
        for event in merged:
            if require_all_mints and event.mint not in launches:
                raise ChunkBarsError(f"mint {event.mint} has no true launch-time anchor")
            if anchored_only and event.mint not in launches:
                continue
            yield event

    return iter_bars_chain_ordered(_checked(), interval_ns, launch_times=launches)


def write_chunk_bars(
    chunks: Sequence[str | Path] | str | Path,
    output: str | Path,
    interval_ns: int,
    launch_times: Mapping[str, int] | str | Path | None = None,
    *,
    require_all_mints: bool = False,
    anchored_only: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, object]:
    """Build and atomically publish JSONL (or Parquet) bars from chunks."""

    interval = _positive_int(interval_ns, "interval_ns")
    size = _positive_int(batch_size, "batch_size")
    if not isinstance(require_all_mints, bool) or not isinstance(anchored_only, bool):
        raise ChunkBarsError("require_all_mints and anchored_only must be boolean")
    if require_all_mints and anchored_only:
        raise ChunkBarsError("require_all_mints and anchored_only are mutually exclusive")
    launches = _coerce_launch_times(launch_times)
    verified = verify_normalized_chunks(chunks)
    raw_target = Path(output).expanduser()
    if raw_target.is_symlink():
        raise ChunkBarsError(f"output must not be a symlink: {raw_target}")
    target = raw_target.resolve()
    protected = {info.manifest_path.resolve() for info in verified}
    for info in verified:
        protected.update(path.resolve() for path in info.inventoried_paths)
    if target in protected:
        raise ChunkBarsError("output must be distinct from every verified chunk file")
    target.parent.mkdir(parents=True, exist_ok=True)

    merged = _iter_merged_verified_swap_events(verified, batch_size=size)

    skipped_unanchored = 0

    def _checked() -> Iterator[SwapEvent]:
        nonlocal skipped_unanchored
        for event in merged:
            if require_all_mints and event.mint not in launches:
                raise ChunkBarsError(f"mint {event.mint} has no true launch-time anchor")
            if anchored_only and event.mint not in launches:
                skipped_unanchored += 1
                continue
            yield event

    bars = iter_bars_chain_ordered(_checked(), interval, launch_times=launches)
    if target.suffix.lower() == ".parquet":
        bars_written = _write_parquet_atomic(target, bars, batch_size=size)
    else:
        bars_written = _write_jsonl_atomic(target, bars)
    swaps_read = sum(info.row_count for info in verified)
    return ChunkBarsReport(
        status="published",
        output=str(target),
        chunks=len(verified),
        swaps_read=swaps_read,
        bars_written=bars_written,
        swaps_skipped_unanchored=skipped_unanchored,
    ).as_dict()


def _load_launch_times_argument(path: str | None) -> Mapping[str, int] | None:
    return None if path is None else load_launch_times(path)


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point for ``scripts/build_chunk_bars.py``."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", help="sealed chunk directories or manifest.json paths")
    parser.add_argument(
        "--chunk",
        dest="chunk_paths",
        action="append",
        default=[],
        help="chunk path (repeatable)",
    )
    parser.add_argument("--output", "-o", required=True, help="output JSONL or .parquet path")
    parser.add_argument("--interval-ns", type=int, default=None)
    parser.add_argument("--interval-ms", type=float, default=None)
    parser.add_argument("--launch-times", help="JSON true launch-time mapping")
    anchor_group = parser.add_mutually_exclusive_group()
    anchor_group.add_argument(
        "--require-all-mints",
        "--require-complete",
        action="store_true",
        help="fail if any observed mint lacks a true launch anchor",
    )
    anchor_group.add_argument(
        "--anchored-only",
        action="store_true",
        help="skip swaps whose mint lacks a true launch anchor",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args(argv)
    chunks = tuple(args.chunk_paths) + tuple(args.paths)
    if not chunks:
        parser.error("at least one --chunk or positional chunk path is required")
    if args.interval_ns is None and args.interval_ms is None:
        parser.error("one of --interval-ns or --interval-ms is required")
    if args.interval_ns is not None and args.interval_ms is not None:
        parser.error("--interval-ns and --interval-ms are mutually exclusive")
    if args.interval_ns is not None:
        interval_ns = args.interval_ns
    else:
        if (
            not isinstance(args.interval_ms, (int, float))
            or not math.isfinite(args.interval_ms)
            or args.interval_ms <= 0
        ):
            parser.error("--interval-ms must be a finite positive number")
        interval_ns = int(round(args.interval_ms * 1_000_000))
    try:
        report = write_chunk_bars(
            chunks,
            args.output,
            interval_ns,
            _load_launch_times_argument(args.launch_times),
            require_all_mints=args.require_all_mints,
            anchored_only=args.anchored_only,
            batch_size=args.batch_size,
        )
    except (ChunkBarsError, TypeError, ValueError) as error:
        parser.exit(2, f"error: {error}\n")
    print(json.dumps(report, sort_keys=True))
    return 0


__all__ = [
    "ChunkBarsError",
    "ChunkBarsReport",
    "DEFAULT_BATCH_SIZE",
    "NORMALIZED_KIND",
    "PHYSICAL_ORDER",
    "VerifiedSwapChunk",
    "iter_chunk_bars",
    "iter_merged_swap_events",
    "load_launch_times",
    "main",
    "verify_normalized_chunks",
    "write_chunk_bars",
]


if __name__ == "__main__":  # pragma: no cover - exercised by the script wrapper
    raise SystemExit(main())
