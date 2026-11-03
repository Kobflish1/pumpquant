"""Export true launch times directly from sealed lifecycle-event sidecars.

The full lifecycle feature dataset repeats anchor columns once per swap and is
therefore much larger than the source sidecar.  This module takes the shorter
path needed by bar construction: it verifies each lifecycle manifest/shard,
checks global chain order and event identity through the same disk-backed index
as the feature materializer, and atomically writes only true Pump launch
anchors.  It never substitutes a first-observed swap timestamp.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

from pumpquant.features.lifecycle import (
    DEFAULT_BATCH_SIZE,
    LifecycleFeatureError,
    _LifecycleIndex,
    _resolve_lifecycle_input,
)


def _atomic_json(path: Path, payload: Mapping[str, int]) -> None:
    if path.is_symlink():
        raise LifecycleFeatureError(f"output must not be a symlink: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".partial", dir=path.parent
    )
    temporary = Path(temporary_name)
    published = False
    try:
        body = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        published = True
        parent = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        if not published:
            temporary.unlink(missing_ok=True)


def export_lifecycle_launch_times(
    inputs: Sequence[str | Path] | str | Path,
    output: str | Path,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, object]:
    """Verify lifecycle sidecars and emit ``{mint: launch_block_time_ns}``.

    Inputs must be supplied in global chain order.  They may be direct
    ``pumpquant.trade_event_dataset`` roots or materialized chunk wrappers.
    Duplicate lifecycle identities, order overlap, multiple launch events for
    one mint, or a launch without a block timestamp fail before publication.
    """

    if isinstance(inputs, (str, Path)):
        values: tuple[str | Path, ...] = (inputs,)
    else:
        values = tuple(inputs)
    if not values:
        raise LifecycleFeatureError("at least one lifecycle input is required")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise LifecycleFeatureError("batch_size must be a positive integer")

    target = Path(output).expanduser().resolve()
    resolved = tuple(_resolve_lifecycle_input(value) for value in values)
    protected = {
        path.resolve()
        for item in resolved
        for path in (item.manifest_path, *(shard.path for shard in item.shards))
    }
    if target in protected:
        raise LifecycleFeatureError("output must be distinct from every verified lifecycle file")

    row_count = 0
    launch_anchor_count = 0
    with tempfile.TemporaryDirectory(prefix="pumpquant-lifecycle-launches-") as directory:
        index = _LifecycleIndex(Path(directory) / "identity.sqlite")
        try:
            import pyarrow.parquet as pq

            for item in resolved:
                streamed = 0
                for shard in item.shards:
                    parquet = pq.ParquetFile(shard.path)
                    try:
                        for batch in parquet.iter_batches(batch_size=batch_size):
                            for raw in batch.to_pylist():
                                if not isinstance(raw, Mapping):
                                    raise LifecycleFeatureError(
                                        "lifecycle Parquet row must be an object"
                                    )
                                anchor = index.add_row(raw)
                                if anchor is not None and anchor.kind == "launch":
                                    launch_anchor_count += 1
                                row_count += 1
                                streamed += 1
                    finally:
                        parquet.close()
                if streamed != item.row_count:
                    raise LifecycleFeatureError(
                        "streamed lifecycle row count differs from manifest"
                    )
            index.commit()
            launch_times: dict[str, int] = {}
            launch_ids: dict[str, str] = {}
            cursor = index.connection.execute(
                "SELECT mint,event_id,block_time_ns FROM anchors WHERE kind='launch' "
                "ORDER BY mint,slot,tx_index,instruction_index,event_index,event_id"
            )
            for mint_value, event_id_value, timestamp_value in cursor:
                mint = str(mint_value)
                event_id = str(event_id_value)
                if timestamp_value is None:
                    raise LifecycleFeatureError(f"launch anchor has no block timestamp: {event_id}")
                timestamp = int(timestamp_value)
                if timestamp < 0:
                    raise LifecycleFeatureError(
                        f"launch anchor has a negative block timestamp: {event_id}"
                    )
                if mint in launch_times:
                    raise LifecycleFeatureError(
                        "multiple true launch anchors for mint "
                        f"{mint}: {launch_ids[mint]} and {event_id}"
                    )
                launch_times[mint] = timestamp
                launch_ids[mint] = event_id
        finally:
            index.close()

    _atomic_json(target, launch_times)
    return {
        "status": "published",
        "output": str(target),
        "input_count": len(resolved),
        "lifecycle_rows_read": row_count,
        "launch_anchor_rows": launch_anchor_count,
        "launch_times_written": len(launch_times),
        "first_observed_fallback": False,
    }


__all__ = ["export_lifecycle_launch_times"]
