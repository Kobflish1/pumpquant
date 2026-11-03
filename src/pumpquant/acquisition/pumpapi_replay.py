"""Concurrent, resumable PumpAPI historical replay ingestion.

Hourly PumpAPI archives contain every supported venue and transfer.  This
module retains only Pump.fun and official PumpSwap venue events, preserves the
provider's millisecond event clock and archive order, and writes compact
Parquet without retaining the large source archive.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import ssl
import sys
import tempfile
import time
import urllib.request
from collections import Counter
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypedDict

import certifi
import pyarrow as pa
import pyarrow.parquet as pq

from pumpquant.core.hashing import sha256_file

if sys.version_info >= (3, 14):
    from compression import zstd
else:  # pragma: no cover - exercised on Python < 3.14
    import zstandard as zstd

KIND = "pumpquant.pumpapi_replay"
VERSION = 3
DEFAULT_BASE_URL = "https://replay.pumpapi.io"
PUMP_POOLS = frozenset(("pump", "pump-amm"))
NUMERIC_TEXT_FIELDS = (
    "tokenAmount",
    "quoteAmount",
    "tokensInPool",
    "quoteInPool",
    "vTokensInBondingCurve",
    "vQuoteInBondingCurve",
    "price",
    "marketCapQuote",
    "poolFeeRate",
    "priorityFee",
    "supply",
    "decimals",
)
SCHEMA = pa.schema(
    [
        ("source_hour_utc", pa.timestamp("ms", tz="UTC")),
        ("archive_sequence", pa.int64()),
        ("signature", pa.string()),
        ("action", pa.string()),
        ("pool", pa.string()),
        ("block", pa.int64()),
        ("timestamp_ms", pa.int64()),
        ("local_timestamp_ms", pa.int64()),
        ("pool_id", pa.string()),
        ("mint", pa.string()),
        ("quote_mint", pa.string()),
        ("tx_signer", pa.string()),
        *[(field, pa.string()) for field in NUMERIC_TEXT_FIELDS],
        ("creator_fee_address", pa.string()),
        ("mayhem_mode", pa.bool_()),
        ("cashback_enabled", pa.bool_()),
        ("pool_created_by", pa.string()),
        ("burned_liquidity", pa.string()),
        ("token_program", pa.string()),
        ("name", pa.string()),
        ("symbol", pa.string()),
        ("uri", pa.string()),
        ("traders_involved_json", pa.string()),
        ("programs_used_json", pa.string()),
        ("participant_post_balances_json", pa.large_string()),
        ("token_extensions_json", pa.string()),
        ("address_lookup_table_count", pa.int32()),
        ("mint_authority", pa.string()),
        ("freeze_authority", pa.string()),
        ("structural_raw_json", pa.large_string()),
    ]
)


class ArchiveMetrics(TypedDict):
    archive_lines: int
    rows: int
    action_counts: dict[str, int]
    pool_counts: dict[str, int]
    minimum_timestamp_ms: int | None
    maximum_timestamp_ms: int | None
    parquet_bytes: int
    parquet_sha256: str


@dataclass(frozen=True)
class HourResult:
    hour: datetime
    rows: int
    source_bytes: int
    source_sha256: str
    parquet_bytes: int
    parquet_sha256: str
    action_counts: dict[str, int]
    pool_counts: dict[str, int]
    minimum_timestamp_ms: int | None
    maximum_timestamp_ms: int | None
    resumed: bool


def _atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".partial", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, allow_nan=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _hour_stem(hour: datetime) -> str:
    return hour.astimezone(UTC).strftime("%Y%m%dT%H0000Z")


def _hour_url(hour: datetime, base_url: str) -> str:
    # PumpAPI's actual archive listing labels files by the starting UTC hour.
    return f"{base_url.rstrip('/')}/{hour.astimezone(UTC):%Y/%m/%d/%H}.jsonl.zst"


def iter_hours(start: datetime, end: datetime) -> Iterator[datetime]:
    start = start.astimezone(UTC)
    end = end.astimezone(UTC)
    if start.minute or start.second or start.microsecond:
        raise ValueError("start must be aligned to a UTC hour")
    if end.minute or end.second or end.microsecond:
        raise ValueError("end must be aligned to a UTC hour")
    if end <= start:
        raise ValueError("end must be later than start")
    cursor = start
    while cursor < end:
        yield cursor
        cursor += timedelta(hours=1)


def _numeric_text(value: object) -> str | None:
    return None if value is None else str(value)


def _participant_post_balances(event: dict[str, object]) -> str:
    participants = event.get("tradersInvolved")
    participant_keys = set(participants) if isinstance(participants, dict) else set()
    signer = event.get("txSigner")
    if isinstance(signer, str):
        participant_keys.add(signer)
    raw_balances = event.get("postBalances")
    if not isinstance(raw_balances, dict):
        return "{}"
    mint = event.get("mint")
    quote_mint = event.get("quoteMint")
    compact: dict[str, dict[str, object]] = {}
    for wallet in sorted(participant_keys):
        balances = raw_balances.get(wallet)
        if not isinstance(balances, dict):
            continue
        item: dict[str, object] = {}
        for source, target in (
            ("sol", "sol"),
            (mint, "token"),
            (quote_mint, "quote"),
        ):
            if isinstance(source, str) and source in balances:
                item[target] = balances[source]
        if item:
            compact[wallet] = item
    return json.dumps(compact, separators=(",", ":"), sort_keys=True)


def _row(event: dict[str, object], hour: datetime, sequence: int, raw: bytes) -> dict[str, object]:
    action = event.get("action")
    result: dict[str, object] = {
        "source_hour_utc": hour,
        "archive_sequence": sequence,
        "signature": event.get("signature"),
        "action": action,
        "pool": event.get("pool"),
        "block": event.get("block"),
        "timestamp_ms": event.get("timestamp"),
        "local_timestamp_ms": event.get("localTimestamp"),
        "pool_id": event.get("poolId"),
        "mint": event.get("mint"),
        "quote_mint": event.get("quoteMint"),
        "tx_signer": event.get("txSigner"),
        "creator_fee_address": event.get("creatorFeeAddress"),
        "mayhem_mode": event.get("mayhemMode"),
        "cashback_enabled": event.get("cashbackEnabled"),
        "pool_created_by": event.get("poolCreatedBy"),
        "burned_liquidity": event.get("burnedLiquidity"),
        "token_program": event.get("tokenProgram"),
        "name": event.get("name"),
        "symbol": event.get("symbol"),
        "uri": event.get("uri"),
        "traders_involved_json": json.dumps(
            event.get("tradersInvolved", {}),
            separators=(",", ":"),
            sort_keys=True,
        ),
        "programs_used_json": json.dumps(
            event.get("programsUsed", []),
            separators=(",", ":"),
        ),
        "participant_post_balances_json": _participant_post_balances(event),
        "token_extensions_json": json.dumps(
            event.get("tokenExtensions", {}),
            separators=(",", ":"),
            sort_keys=True,
        ),
        "address_lookup_table_count": (
            len(tables) if isinstance(tables := event.get("addressLookupTables"), list) else 0
        ),
        "mint_authority": event.get("mintAuthority"),
        "freeze_authority": event.get("freezeAuthority"),
        # Buys and sells repeat large metadata/post-balance objects hundreds of
        # thousands of times per hour.  Their complete economic state is
        # normalized above.  Preserve the raw provider payload for the rare
        # structural events where future schema additions may matter.
        "structural_raw_json": (
            None if action in {"buy", "sell"} else raw.decode("utf-8").rstrip("\n")
        ),
    }
    for field in NUMERIC_TEXT_FIELDS:
        result[field] = _numeric_text(event.get(field))
    return result


def materialize_archive(
    archive: Path,
    parquet_path: Path,
    *,
    hour: datetime,
    batch_rows: int = 25_000,
) -> ArchiveMetrics:
    """Filter one downloaded archive into one atomic Pump/PumpSwap Parquet part."""

    if batch_rows <= 0:
        raise ValueError("batch_rows must be positive")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(
        prefix=f".{parquet_path.name}.", suffix=".partial", dir=parquet_path.parent
    )
    os.close(descriptor)
    temporary = Path(name)
    actions: Counter[str] = Counter()
    pools: Counter[str] = Counter()
    rows: list[dict[str, object]] = []
    retained = 0
    scanned = 0
    minimum_timestamp: int | None = None
    maximum_timestamp: int | None = None
    writer: pq.ParquetWriter | None = None

    def flush() -> None:
        nonlocal writer
        if not rows:
            return
        table = pa.Table.from_pylist(rows, schema=SCHEMA)
        if writer is None:
            writer = pq.ParquetWriter(
                temporary,
                SCHEMA,
                compression="zstd",
                use_dictionary=True,
                write_statistics=True,
            )
        writer.write_table(table)
        rows.clear()

    try:
        # zstandard's reader cannot iterate lines; buffering gives both backends readline.
        with io.BufferedReader(zstd.open(archive, "rb")) as stream:
            for scanned, raw in enumerate(stream, start=1):
                event = json.loads(raw, parse_float=str)
                pool = event.get("pool")
                if pool not in PUMP_POOLS:
                    continue
                timestamp = event.get("timestamp")
                if not isinstance(timestamp, int):
                    raise ValueError(f"missing integer timestamp at archive line {scanned}")
                rows.append(_row(event, hour, scanned, raw))
                retained += 1
                actions[str(event.get("action"))] += 1
                pools[str(pool)] += 1
                minimum_timestamp = (
                    timestamp if minimum_timestamp is None else min(minimum_timestamp, timestamp)
                )
                maximum_timestamp = (
                    timestamp if maximum_timestamp is None else max(maximum_timestamp, timestamp)
                )
                if len(rows) >= batch_rows:
                    flush()
        flush()
        if writer is None:
            pq.write_table(pa.Table.from_pylist([], schema=SCHEMA), temporary, compression="zstd")
        else:
            writer.close()
            writer = None
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, parquet_path)
    finally:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)
    return {
        "archive_lines": scanned,
        "rows": retained,
        "action_counts": dict(sorted(actions.items())),
        "pool_counts": dict(sorted(pools.items())),
        "minimum_timestamp_ms": minimum_timestamp,
        "maximum_timestamp_ms": maximum_timestamp,
        "parquet_bytes": parquet_path.stat().st_size,
        "parquet_sha256": sha256_file(parquet_path),
    }


def _download(
    url: str,
    destination: Path,
    *,
    timeout_seconds: float,
) -> tuple[int, str]:
    request = urllib.request.Request(
        url,
        headers={"Accept": "*/*", "User-Agent": "curl/8.7.1"},
    )
    context = ssl.create_default_context(cafile=certifi.where())
    digest = hashlib.sha256()
    received = 0
    with (
        urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response,
        destination.open("wb") as output,
    ):
        while block := response.read(8 * 1024 * 1024):
            output.write(block)
            digest.update(block)
            received += len(block)
        output.flush()
        os.fsync(output.fileno())
    if received == 0:
        raise ValueError(f"empty PumpAPI archive: {url}")
    return received, digest.hexdigest()


def _load_complete(manifest: Path, parquet: Path, hour: datetime) -> HourResult | None:
    if not manifest.is_file() or not parquet.is_file():
        return None
    value = json.loads(manifest.read_text())
    if (
        value.get("kind") != f"{KIND}.hour"
        or value.get("version") != VERSION
        or value.get("status") != "complete"
        or value.get("hour_start_utc") != hour.isoformat()
        or value.get("parquet_bytes") != parquet.stat().st_size
        or value.get("parquet_sha256") != sha256_file(parquet)
    ):
        return None
    return HourResult(
        hour=hour,
        rows=int(value["rows"]),
        source_bytes=int(value["source_bytes"]),
        source_sha256=str(value["source_sha256"]),
        parquet_bytes=int(value["parquet_bytes"]),
        parquet_sha256=str(value["parquet_sha256"]),
        action_counts=dict(value["action_counts"]),
        pool_counts=dict(value["pool_counts"]),
        minimum_timestamp_ms=value.get("minimum_timestamp_ms"),
        maximum_timestamp_ms=value.get("maximum_timestamp_ms"),
        resumed=True,
    )


def ingest_hour(
    hour: datetime,
    output: Path,
    *,
    base_url: str = DEFAULT_BASE_URL,
    timeout_seconds: float = 180.0,
    attempts: int = 4,
) -> HourResult:
    hour = hour.astimezone(UTC)
    stem = _hour_stem(hour)
    part = output / "parts" / f"{stem}.parquet"
    manifest = output / "parts" / f"{stem}.json"
    resumed = _load_complete(manifest, part, hour)
    if resumed is not None:
        return resumed
    output.joinpath("downloads").mkdir(parents=True, exist_ok=True)
    archive = output / "downloads" / f".{stem}.jsonl.zst.partial"
    url = _hour_url(hour, base_url)
    failure: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            source_bytes, source_sha256 = _download(url, archive, timeout_seconds=timeout_seconds)
            metrics = materialize_archive(archive, part, hour=hour)
            value = {
                "kind": f"{KIND}.hour",
                "version": VERSION,
                "status": "complete",
                "hour_start_utc": hour.isoformat(),
                "hour_end_utc": (hour + timedelta(hours=1)).isoformat(),
                "source_url": url,
                "source_bytes": source_bytes,
                "source_sha256": source_sha256,
                **metrics,
            }
            _atomic_json(manifest, value)
            return HourResult(
                hour=hour,
                rows=metrics["rows"],
                source_bytes=source_bytes,
                source_sha256=source_sha256,
                parquet_bytes=metrics["parquet_bytes"],
                parquet_sha256=metrics["parquet_sha256"],
                action_counts=metrics["action_counts"],
                pool_counts=metrics["pool_counts"],
                minimum_timestamp_ms=metrics["minimum_timestamp_ms"],
                maximum_timestamp_ms=metrics["maximum_timestamp_ms"],
                resumed=False,
            )
        except Exception as error:
            failure = error
            archive.unlink(missing_ok=True)
            if attempt < attempts:
                time.sleep(min(10.0, float(2 ** (attempt - 1))))
        finally:
            archive.unlink(missing_ok=True)
    raise RuntimeError(f"PumpAPI hour failed after {attempts} attempts: {url}") from failure


def ingest_range(
    start: datetime,
    end: datetime,
    output: Path,
    *,
    workers: int,
    base_url: str = DEFAULT_BASE_URL,
    timeout_seconds: float = 180.0,
    attempts: int = 4,
) -> dict[str, object]:
    hours = list(iter_hours(start, end))
    if workers <= 0:
        raise ValueError("workers must be positive")
    output.mkdir(parents=True, exist_ok=True)
    results: list[HourResult] = []
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=min(workers, len(hours))) as executor:
        futures = {
            executor.submit(
                ingest_hour,
                hour,
                output,
                base_url=base_url,
                timeout_seconds=timeout_seconds,
                attempts=attempts,
            ): hour
            for hour in hours
        }
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(
                json.dumps(
                    {
                        "status": "hour_complete",
                        "hour": result.hour.isoformat(),
                        "completed_hours": len(results),
                        "hours": len(hours),
                        "rows": result.rows,
                        "resumed": result.resumed,
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                    }
                ),
                flush=True,
            )
    results.sort(key=lambda item: item.hour)
    actions: Counter[str] = Counter()
    pools: Counter[str] = Counter()
    for result in results:
        actions.update(result.action_counts)
        pools.update(result.pool_counts)
    manifest = {
        "kind": KIND,
        "version": VERSION,
        "status": "complete",
        "source": base_url,
        "start_utc": start.astimezone(UTC).isoformat(),
        "end_utc": end.astimezone(UTC).isoformat(),
        "hour_count": len(results),
        "row_count": sum(result.rows for result in results),
        "source_bytes_streamed": sum(result.source_bytes for result in results),
        "parquet_bytes": sum(result.parquet_bytes for result in results),
        "action_counts": dict(sorted(actions.items())),
        "pool_counts": dict(sorted(pools.items())),
        "minimum_timestamp_ms": min(
            (
                result.minimum_timestamp_ms
                for result in results
                if result.minimum_timestamp_ms is not None
            ),
            default=None,
        ),
        "maximum_timestamp_ms": max(
            (
                result.maximum_timestamp_ms
                for result in results
                if result.maximum_timestamp_ms is not None
            ),
            default=None,
        ),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "parts": [
            {
                "hour_start_utc": result.hour.isoformat(),
                "rows": result.rows,
                "source_bytes": result.source_bytes,
                "source_sha256": result.source_sha256,
                "parquet_bytes": result.parquet_bytes,
                "parquet_sha256": result.parquet_sha256,
                "path": f"parts/{_hour_stem(result.hour)}.parquet",
            }
            for result in results
        ],
    }
    _atomic_json(output / "manifest.json", manifest)
    return manifest
