"""Export verified lifecycle launch anchors for :mod:`pumpquant.features.bars`.

The lifecycle feature materialiser writes one feature row per swap.  Launch
anchor columns are therefore repeated many times, and rows without a launch
anchor are rather than an invitation to infer one from the first
observed swap.  This module verifies the feature dataset manifest and emits
the small ``{mint: launch_block_time_ns}`` object consumed by ``pumpquant
bars``.

The exporter is fail-closed: a malformed manifest/row, a
conflicting anchor, or an ambiguous duplicate aborts before an existing output
is replaced.  Mints whose rows say that the launch anchor is
missing are reported and omitted unless another verified partition supplies
one consistent true anchor.  ``strict`` (also exposed as
``--require-complete`` by the CLI) fails when any mint remains unresolved
across all inputs; it never turns a first-observed timestamp into a launch
time.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from pumpquant.core.hashing import sha256_file
from pumpquant.features.lifecycle import FEATURES_KIND

try:  # Optional data dependency; loaded only when this exporter is used.
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - exercised without pumpquant[data]
    pq = None


DEFAULT_BATCH_SIZE = 32_768
SCHEMA_VERSION = 1
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_STATUS_RE = re.compile(
    r"^missing_(?:launch|pool_creation|graduation)"
    r"(?:_and_(?:launch|pool_creation|graduation))*$"
)
_STATUS_NAMES = ("launch", "pool_creation", "graduation")


class LaunchTimesError(RuntimeError):
    """A verified feature dataset cannot produce an unambiguous mapping."""


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise LaunchTimesError(f"{label} must be an object with text keys")
    return cast(Mapping[str, object], value)


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LaunchTimesError(f"{label} must be a non-negative integer")
    return value


def _sha256_file(path: Path) -> str:
    try:
        return sha256_file(path)
    except OSError as error:
        raise LaunchTimesError(f"cannot read verified file: {path}") from error


def _json_object(path: Path, label: str) -> Mapping[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LaunchTimesError(f"{label} is not valid JSON: {path}") from error
    return _mapping(value, label)


def _safe_relative(root: Path, raw: object, label: str) -> Path:
    if not isinstance(raw, str) or not raw.strip():
        raise LaunchTimesError(f"{label} must be non-empty text")
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise LaunchTimesError(f"{label} escapes its dataset")
    current = root
    for component in relative.parts:
        current /= component
        if current.is_symlink():
            raise LaunchTimesError(f"{label} traverses a symlink")
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as error:
        raise LaunchTimesError(f"{label} escapes its dataset") from error
    return candidate


def _file_entries(manifest: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    raw = manifest.get("files")
    if isinstance(raw, Mapping):
        entries: dict[str, Mapping[str, object]] = {}
        for key, value in raw.items():
            if not isinstance(key, str) or not key:
                raise LaunchTimesError("feature manifest file paths must be text")
            entries[key] = _mapping(value, f"feature manifest file {key}")
        return entries
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        entries = {}
        for index, value in enumerate(raw):
            item = _mapping(value, f"feature manifest file {index}")
            relative = item.get("path")
            if not isinstance(relative, str) or not relative:
                raise LaunchTimesError(f"feature manifest file {index} path is invalid")
            if relative in entries:
                raise LaunchTimesError(f"duplicate feature manifest file: {relative}")
            entries[relative] = item
        return entries
    raise LaunchTimesError("feature manifest has no file inventory")


def _verify_file(root: Path, relative: str, metadata: Mapping[str, object], label: str) -> Path:
    path = _safe_relative(root, relative, f"{label} path")
    if path.is_symlink() or not path.is_file():
        raise LaunchTimesError(f"{label} is missing or not a regular file: {path}")
    byte_count = _nonnegative_int(metadata.get("byte_count"), f"{label} byte_count")
    digest = metadata.get("sha256")
    if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
        raise LaunchTimesError(f"{label} sha256 must be SHA-256 hex")
    if path.stat().st_size != byte_count or _sha256_file(path) != digest.lower():
        raise LaunchTimesError(f"{label} failed byte/hash verification")
    return path


def _require_arrow() -> Any:
    if pq is None:  # pragma: no cover - optional dependency
        raise LaunchTimesError("Parquet support requires: pip install 'pumpquant[data]'")
    return pq


def _resolve_feature_input(value: str | Path) -> tuple[Path, Path, Mapping[str, object]]:
    raw = Path(value).expanduser()
    if raw.is_symlink():
        raise LaunchTimesError(f"feature input must not be a symlink: {raw}")
    if raw.is_dir():
        root = raw.resolve()
        manifest_path = root / "manifest.json"
        feature_hint = root / "features.parquet"
    elif raw.is_file() and raw.name == "manifest.json":
        root = raw.parent.resolve()
        manifest_path = raw.resolve()
        feature_hint = root / "features.parquet"
    elif raw.is_file() and raw.suffix.lower() == ".parquet":
        root = raw.parent.resolve()
        manifest_path = root / "manifest.json"
        feature_hint = raw.resolve()
    else:
        raise LaunchTimesError(
            f"feature input must be a feature directory, manifest.json, or features.parquet: {raw}"
        )
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise LaunchTimesError(f"verified feature manifest is missing: {manifest_path}")
    manifest = _json_object(manifest_path, "feature manifest")
    if manifest.get("kind") != FEATURES_KIND:
        raise LaunchTimesError(f"unsupported feature manifest kind: {manifest.get('kind')!r}")
    if manifest.get("version") != SCHEMA_VERSION:
        raise LaunchTimesError(f"unsupported feature manifest version: {manifest.get('version')!r}")
    row_count = _nonnegative_int(manifest.get("row_count"), "feature row_count")
    entries = _file_entries(manifest)
    # A manifest-last feature materialisation always inventories this file.
    feature_candidates = [
        relative for relative in entries if Path(relative).name == "features.parquet"
    ]
    if not feature_candidates:
        raise LaunchTimesError("feature manifest does not inventory features.parquet")
    if len(feature_candidates) != 1:
        raise LaunchTimesError("feature manifest inventories multiple features.parquet files")
    feature_relative = feature_candidates[0]
    feature_path = _verify_file(
        root, feature_relative, entries[feature_relative], "features.parquet"
    )
    if feature_path != feature_hint:
        raise LaunchTimesError(
            f"input feature path disagrees with manifest: {feature_hint} != {feature_path}"
        )
    # Verify every declared sidecar too (normally audit.json).  This prevents
    # consuming a manifest whose audit/source seal was removed after publish.
    for relative, metadata in entries.items():
        if relative == feature_relative:
            continue
        _verify_file(root, relative, metadata, f"feature manifest file {relative}")
    audit_path_raw = manifest.get("audit_path")
    if audit_path_raw is not None:
        if not isinstance(audit_path_raw, str) or audit_path_raw not in entries:
            raise LaunchTimesError("feature manifest audit_path is not inventoried")
        audit_digest = manifest.get("audit_sha256")
        declared_digest = entries[audit_path_raw].get("sha256")
        if audit_digest != declared_digest:
            raise LaunchTimesError("feature manifest audit hash differs from file inventory")
    arrow_schema_hash = manifest.get("feature_schema_sha256")
    parquet = _require_arrow().ParquetFile(feature_path)
    try:
        metadata = parquet.metadata
        actual_rows = 0 if metadata is None else int(metadata.num_rows)
        if actual_rows != row_count:
            raise LaunchTimesError(
                f"features.parquet row count differs from manifest ({actual_rows} != {row_count})"
            )
        if arrow_schema_hash is not None:
            if (
                not isinstance(arrow_schema_hash, str)
                or _SHA256_RE.fullmatch(arrow_schema_hash) is None
            ):
                raise LaunchTimesError("feature_schema_sha256 must be SHA-256 hex")
            schema_digest = hashlib.sha256(
                parquet.schema_arrow.serialize().to_pybytes()
            ).hexdigest()
            if schema_digest != arrow_schema_hash.lower():
                raise LaunchTimesError("features.parquet schema hash differs from manifest")
    finally:
        close = getattr(parquet, "close", None)
        if callable(close):
            close()
    return feature_path, manifest_path, manifest


def _text(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or value != value.strip()
        or any(character.isspace() for character in value)
    ):
        raise LaunchTimesError(f"{label} must be non-empty text without whitespace")
    return value


def _optional_bool(row: Mapping[str, object], name: str, label: str) -> bool | None:
    if name not in row or row[name] is None:
        return None
    value = row[name]
    if not isinstance(value, bool):
        raise LaunchTimesError(f"{label} must be boolean")
    return value


def _anchor_statuses(row: Mapping[str, object], row_number: int) -> tuple[str, str]:
    label = f"feature row {row_number}"
    launch_status = row.get("launch_anchor_status")
    if launch_status not in {"present", "missing"}:
        raise LaunchTimesError(f"{label} has invalid launch_anchor_status")
    lifecycle_status = row.get("lifecycle_anchor_status")
    if not isinstance(lifecycle_status, str) or (
        lifecycle_status != "complete" and _STATUS_RE.fullmatch(lifecycle_status) is None
    ):
        raise LaunchTimesError(f"{label} has invalid lifecycle_anchor_status")
    missing_launch = _optional_bool(row, "missing_launch_anchor", label)
    if missing_launch is not None and missing_launch != (launch_status == "missing"):
        raise LaunchTimesError(f"{label} launch status/flag mismatch")
    if lifecycle_status == "complete" and launch_status != "present":
        raise LaunchTimesError(f"{label} complete lifecycle is missing its launch anchor")
    if lifecycle_status != "complete":
        missing_parts = lifecycle_status.removeprefix("missing_").split("_and_")
        if "launch" in missing_parts and launch_status != "missing":
            raise LaunchTimesError(f"{label} lifecycle status/launch status mismatch")
        if "launch" not in missing_parts and launch_status != "present":
            raise LaunchTimesError(f"{label} lifecycle status/launch status mismatch")
        if len(set(missing_parts)) != len(missing_parts) or any(
            part not in _STATUS_NAMES for part in missing_parts
        ):
            raise LaunchTimesError(f"{label} has invalid lifecycle anchor status")
    return launch_status, lifecycle_status


def _launch_time(row: Mapping[str, object], row_number: int) -> int:
    for name in ("launch_block_time_ns", "launch_time_ns"):
        if name in row:
            return _nonnegative_int(row[name], f"feature row {row_number} {name}")
    raise LaunchTimesError(f"feature row {row_number} is missing launch_block_time_ns")


def _validate_time_and_anchor_fields(
    row: Mapping[str, object], row_number: int, launch_status: str
) -> None:
    label = f"feature row {row_number}"
    for name in ("block_time_ns", "launch_block_time_ns", "launch_time_ns"):
        if name in row and row[name] is not None:
            _nonnegative_int(row[name], f"{label} {name}")
    for name in ("launch_slot", "launch_tx_index", "launch_instruction_index"):
        if name in row and row[name] is not None:
            _nonnegative_int(row[name], f"{label} {name}")
    if launch_status == "missing":
        for name in (
            "launch_slot",
            "launch_tx_index",
            "launch_instruction_index",
            "launch_block_time_ns",
            "launch_time_ns",
            "launch_transition_id",
            "launch_event_id",
        ):
            if row.get(name) is not None:
                raise LaunchTimesError(f"{label} missing launch status has {name}")
        return

    event_position = tuple(
        _nonnegative_int(row.get(name), f"{label} {name}")
        for name in ("slot", "tx_index", "instruction_index")
    )
    launch_position = tuple(
        _nonnegative_int(row.get(name), f"{label} {name}")
        for name in ("launch_slot", "launch_tx_index", "launch_instruction_index")
    )
    if launch_position > event_position:
        raise LaunchTimesError(f"{label} launch anchor occurs after its swap")
    block_time = _nonnegative_int(row.get("block_time_ns"), f"{label} block_time_ns")
    launch_time = _launch_time(row, row_number)
    if launch_time > block_time:
        raise LaunchTimesError(f"{label} launch time occurs after its swap")


def _event_id(row: Mapping[str, object], row_number: int) -> str:
    value = row.get("event_id")
    if value is None:
        # A lifecycle feature dataset is expected to retain event identity;
        # silently deduplicating rows without it would make overlap ambiguous.
        raise LaunchTimesError(f"feature row {row_number} is missing event_id")
    return _text(value, f"feature row {row_number} event_id")


def _write_json_atomic(path: Path, payload: Mapping[str, int]) -> None:
    if path.is_symlink():
        raise LaunchTimesError(f"output must not be a symlink: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".partial", dir=path.parent
    )
    temporary = Path(temporary_name)
    published = False
    try:
        body = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        published = True
        parent_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        if not published:
            temporary.unlink(missing_ok=True)


def export_launch_times(
    inputs: Sequence[str | Path] | str | Path,
    output: str | Path,
    *,
    strict: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, object]:
    """Verify feature datasets and atomically write a bars-compatible mapping.

    Missing launch anchors are represented by valid lifecycle statuses and are
    omitted unless another verified input supplies a consistent true anchor.
    The returned report lists unresolved and cross-partition-resolved mints.
    No first-observed timestamp is ever used as a substitute.  ``strict``
    fails before publication when any mint remains unresolved.
    """

    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise LaunchTimesError("batch_size must be a positive integer")
    if isinstance(inputs, (str, Path)):
        input_values: tuple[str | Path, ...] = (inputs,)
    else:
        input_values = tuple(inputs)
    if not input_values:
        raise LaunchTimesError("at least one feature input is required")
    verified: list[tuple[Path, Path, Mapping[str, object]]] = []
    identities: set[Path] = set()
    protected_paths: set[Path] = set()
    for value in input_values:
        feature_path, manifest_path, manifest = _resolve_feature_input(value)
        if feature_path in identities:
            raise LaunchTimesError(f"duplicate feature input: {feature_path}")
        identities.add(feature_path)
        protected_paths.update((feature_path, manifest_path))
        protected_paths.update(
            _safe_relative(feature_path.parent, relative, "feature manifest file path")
            for relative in _file_entries(manifest)
        )
        verified.append((feature_path, manifest_path, manifest))
    parquet_api = _require_arrow()
    launch_times: dict[str, int] = {}
    event_ids: set[str] = set()
    missing: set[str] = set()
    row_count = 0
    for feature_path, _manifest_path, _manifest in verified:
        # Enrichment partitions are self-contained.  A mint can
        # therefore be missing its older launch event in one partition and
        # carry the true anchor in another.  Mixed status inside one sealed
        # feature dataset is corrupt; cross-dataset resolution is legitimate.
        status_by_mint: dict[str, str] = {}
        parquet = parquet_api.ParquetFile(feature_path)
        try:
            names = set(parquet.schema_arrow.names)
            required = {
                "event_id",
                "mint",
                "slot",
                "tx_index",
                "instruction_index",
                "block_time_ns",
                "launch_slot",
                "launch_tx_index",
                "launch_instruction_index",
                "launch_block_time_ns",
                "launch_anchor_status",
                "lifecycle_anchor_status",
            }
            if not required.issubset(names):
                absent = ", ".join(sorted(required - names))
                raise LaunchTimesError(f"{feature_path} lacks required feature columns: {absent}")
            for batch in parquet.iter_batches(batch_size=batch_size, use_threads=False):
                for raw in batch.to_pylist():
                    row_count += 1
                    row = _mapping(raw, f"feature row {row_count}")
                    event_id = _event_id(row, row_count)
                    if event_id in event_ids:
                        raise LaunchTimesError(f"duplicate feature event_id: {event_id}")
                    event_ids.add(event_id)
                    mint = _text(row.get("mint"), f"feature row {row_count} mint")
                    launch_status, _lifecycle_status = _anchor_statuses(row, row_count)
                    _validate_time_and_anchor_fields(row, row_count, launch_status)
                    previous_status = status_by_mint.get(mint)
                    if previous_status is not None and previous_status != launch_status:
                        raise LaunchTimesError(
                            f"mint {mint} has ambiguous duplicate launch anchor status"
                        )
                    status_by_mint[mint] = launch_status
                    if launch_status == "missing":
                        missing.add(mint)
                        continue
                    timestamp = _launch_time(row, row_count)
                    prior = launch_times.get(mint)
                    if prior is not None and prior != timestamp:
                        raise LaunchTimesError(
                            f"mint {mint} has conflicting launch times: {prior} != {timestamp}"
                        )
                    launch_times[mint] = timestamp
        finally:
            close = getattr(parquet, "close", None)
            if callable(close):
                close()
    resolved_across_inputs = sorted(missing.intersection(launch_times))
    excluded = sorted(missing.difference(launch_times))
    if strict and excluded:
        raise LaunchTimesError(
            "strict launch export requires a true anchor for every mint; "
            "missing launch anchors: " + ", ".join(excluded)
        )
    raw_target = Path(output).expanduser()
    if raw_target.is_symlink():
        raise LaunchTimesError(f"output must not be a symlink: {raw_target}")
    target = raw_target.resolve()
    if target in protected_paths:
        raise LaunchTimesError("output must be distinct from every verified feature file")
    _write_json_atomic(target, launch_times)
    return {
        "status": "published",
        "strict": bool(strict),
        "inputs": len(verified),
        "rows_read": row_count,
        "mints_with_true_anchors": len(launch_times),
        "mints_excluded_missing_anchor": excluded,
        "missing_anchor_mints": excluded,
        "mints_resolved_from_other_inputs": resolved_across_inputs,
        "excluded_count": len(excluded),
        "launch_times_written": len(launch_times),
        "output": str(target),
    }


def build_launch_times(
    inputs: Sequence[str | Path] | str | Path,
    *,
    strict: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, int]:
    """Return an in-memory launch mapping after verifying feature inputs.

    This helper performs the same validation as :func:`export_launch_times`
    but does not publish a file.  It is useful to library callers that already
    own their output writer.
    """

    if isinstance(inputs, (str, Path)):
        input_values: tuple[str | Path, ...] = (inputs,)
    else:
        input_values = tuple(inputs)
    temporary = Path(tempfile.mkdtemp(prefix="pumpquant-launch-times."))
    try:
        target = temporary / "launch_times.json"
        export_launch_times(input_values, target, strict=strict, batch_size=batch_size)
        value = json.loads(target.read_text(encoding="utf-8"))
        if not isinstance(value, dict):  # pragma: no cover - writer invariant
            raise LaunchTimesError("exported launch mapping is not an object")
        return {
            str(key): _nonnegative_int(item, f"launch time {key}") for key, item in value.items()
        }
    finally:
        try:
            target.unlink(missing_ok=True)
            temporary.rmdir()
        except OSError:
            pass


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "LaunchTimesError",
    "build_launch_times",
    "export_launch_times",
]
