"""Versioned interchange for normalized swaps and derived bars.

JSONL is the dependency-free interchange format.  Parquet helpers are enabled
by the optional ``pumpquant[data]`` dependencies and use the same records.
Raw Solana transactions should always be archived separately before decoding.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import MISSING, fields
from pathlib import Path
from typing import Any, TypeVar, cast

from pumpquant.core.schema import Bar, Side, SwapEvent, VenuePhase

SCHEMA_VERSION = 1
Record = SwapEvent | Bar
T = TypeVar("T", SwapEvent, Bar)


def _record_payload(record: Record) -> dict[str, Any]:
    payload = {field.name: getattr(record, field.name) for field in fields(record)}
    payload["phase"] = record.phase.value
    if isinstance(record, SwapEvent):
        payload["side"] = record.side.value
        record_type = "swap_event"
    else:
        record_type = "bar"
    return {"schema_version": SCHEMA_VERSION, "record_type": record_type, **payload}


def _require_version(payload: Mapping[str, Any]) -> None:
    version = payload.get("schema_version")
    if version != SCHEMA_VERSION:
        raise ValueError(f"unsupported schema_version {version!r}; expected {SCHEMA_VERSION}")


def _values_for(record_type: type[T], payload: Mapping[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for field in fields(record_type):
        if field.name in payload:
            values[field.name] = payload[field.name]
        elif field.default is not MISSING:
            values[field.name] = field.default
        else:
            raise ValueError(f"missing required field {field.name!r}")
    return values


def swap_event_from_dict(payload: Mapping[str, Any]) -> SwapEvent:
    _require_version(payload)
    if payload.get("record_type") != "swap_event":
        raise ValueError("record_type must be 'swap_event'")
    values = _values_for(SwapEvent, payload)
    values["phase"] = VenuePhase(values["phase"])
    values["side"] = Side(values["side"])
    return SwapEvent(**values)


def bar_from_dict(payload: Mapping[str, Any]) -> Bar:
    _require_version(payload)
    if payload.get("record_type") != "bar":
        raise ValueError("record_type must be 'bar'")
    values = _values_for(Bar, payload)
    values["phase"] = VenuePhase(values["phase"])
    return Bar(**values)


def write_jsonl(path: str | Path, records: Iterable[Record]) -> int:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with target.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(_record_payload(record), separators=(",", ":")))
            handle.write("\n")
            count += 1
    return count


def _read_payloads(path: str | Path) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    with Path(path).open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid JSON on line {line_number}: {error}") from error
            if not isinstance(value, dict):
                raise ValueError(f"line {line_number} must contain a JSON object")
            payloads.append(value)
    return payloads


def read_swap_events(path: str | Path) -> list[SwapEvent]:
    if Path(path).suffix.lower() == ".parquet":
        return read_parquet(path, SwapEvent)
    return [swap_event_from_dict(payload) for payload in _read_payloads(path)]


def read_bars(path: str | Path) -> list[Bar]:
    if Path(path).suffix.lower() == ".parquet":
        return read_parquet(path, Bar)
    return [bar_from_dict(payload) for payload in _read_payloads(path)]


def write_parquet(path: str | Path, records: Iterable[Record]) -> int:
    """Write normalized records to Parquet when the data extra is installed."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:  # pragma: no cover - depends on optional package
        raise RuntimeError("Parquet support requires: pip install 'pumpquant[data]'") from error
    rows = [_record_payload(record) for record in records]
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), target, compression="zstd")
    return len(rows)


def _validate_parquet_batch_size(batch_size: int) -> None:
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")


def _record_from_payload(record_type: type[T], payload: Mapping[str, Any]) -> T:
    if record_type is SwapEvent:
        record: Record = swap_event_from_dict(payload)
    elif record_type is Bar:
        record = bar_from_dict(payload)
    else:
        raise TypeError("record_type must be SwapEvent or Bar")
    return cast(T, record)


def iter_parquet_batches(
    path: str | Path,
    record_type: type[T],
    *,
    batch_size: int = 65_536,
) -> Iterator[list[T]]:
    """Yield validated homogeneous Parquet records in physical row order.

    ``ParquetFile.iter_batches`` bounds conversion to Python objects by
    ``batch_size``.  Arrow is read without worker threads so batches and rows
    retain the deterministic order in which they were written.  Every row is
    passed through the same schema-version and record-type validation used by
    the JSONL reader.
    """

    if record_type not in (SwapEvent, Bar):
        raise TypeError("record_type must be SwapEvent or Bar")
    _validate_parquet_batch_size(batch_size)
    try:
        import pyarrow.parquet as pq
    except ImportError as error:  # pragma: no cover - depends on optional package
        raise RuntimeError("Parquet support requires: pip install 'pumpquant[data]'") from error

    def _iter() -> Iterator[list[T]]:
        parquet = pq.ParquetFile(Path(path))
        try:
            for batch in parquet.iter_batches(batch_size=batch_size, use_threads=False):
                records: list[T] = []
                for payload in batch.to_pylist():
                    if not isinstance(payload, Mapping):
                        raise ValueError("Parquet rows must contain JSON-like objects")
                    records.append(_record_from_payload(record_type, payload))
                if records:
                    yield records
        finally:
            close = getattr(parquet, "close", None)
            if callable(close):
                close()

    return _iter()


def iter_swap_event_batches(
    path: str | Path,
    *,
    batch_size: int = 65_536,
) -> Iterator[list[SwapEvent]]:
    """Yield validated ``SwapEvent`` batches from a local Parquet file."""

    return iter_parquet_batches(path, SwapEvent, batch_size=batch_size)


def read_parquet(path: str | Path, record_type: type[T]) -> list[T]:
    """Read a homogeneous SwapEvent or Bar Parquet file."""
    return [record for batch in iter_parquet_batches(path, record_type) for record in batch]
