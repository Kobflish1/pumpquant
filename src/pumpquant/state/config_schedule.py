"""Build an auditable fee/config schedule from a sealed state-event sidecar.

The event adapter deliberately keeps state events in a sidecar instead of
silently joining them to swaps.  This module is the small, fail-closed bridge
between that sidecar and :class:`pumpquant.amm_state_materializer.AMMFeeSchedule`.
It verifies the adapter manifest and every listed shard before yielding a row,
keeps chain order (including log order for events in one slot), and writes a
slot-bounded JSON schedule only after all input has been consumed.

``SetParamsEvent`` and PumpSwap ``CreateConfigEvent``/``UpdateFeeConfigEvent``
carry useful fee fields, but they are not account snapshots.  A candidate is
therefore marked ``partial`` unless every executable fee component is present
in the event (or an explicit caller supplied resolution).  Missing values are
represented as ``config: null`` in the materializer schedule; no value is
filled with a guessed zero or a fee observed on a trade.  All transitions,
including unresolved and non-fee state events, remain in ``transitions.jsonl``
and in the audit metadata.

The accepted input is either a directory containing ``manifest.json`` and the
state shards advertised by ``state_shards`` (the direct adapter form), or a
wrapper whose manifest contains a ``state`` object with the same metadata.  A
state shard may be JSONL or Parquet.  Iteration is bounded by ``batch_size``;
Parquet is imported lazily so the JSONL path remains usable without the data
extra.
"""

# Several provenance/error messages intentionally keep the field names and
# source terms together; only line-length lint is disabled for this module.
# The functional Ruff checks remain enabled.
# ruff: noqa: E501

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import shutil
import tempfile
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, TypeAlias, cast

from pumpquant.core.hashing import sha256_file
from pumpquant.core.schema import VenuePhase
from pumpquant.decoding.instruction_intent import PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID
from pumpquant.decoding.program_events import (
    PINNED_EVENT_IDL_REVISION,
    PUMP_EVENT_IDL_SHA256,
    PUMPSWAP_EVENT_IDL_SHA256,
    decode_state_event,
)
from pumpquant.state.materializer import load_fee_schedule

STATE_CONFIG_SCHEDULE_KIND = "pumpquant.amm_config_schedule"
STATE_EVENT_DATASET_KIND = "pumpquant.state_event_dataset"
AUDIT_KIND = "pumpquant.amm_config_schedule_audit"
SCHEMA_VERSION = 1
DEFAULT_BATCH_SIZE = 16_384

_HEX64 = frozenset("0123456789abcdef")
_KNOWN_PROGRAMS = frozenset((PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID))
_KNOWN_IDL_SHA256 = {
    PUMP_PROGRAM_ID: PUMP_EVENT_IDL_SHA256,
    PUMPSWAP_PROGRAM_ID: PUMPSWAP_EVENT_IDL_SHA256,
}
_PUMP_CONFIG_EVENTS = frozenset(("SetParamsEvent", "PumpSetParamsEvent"))
_PUMPSWAP_CONFIG_EVENTS = frozenset(
    (
        "CreateConfigEvent",
        "UpdateFeeConfigEvent",
        "PumpSwapCreateConfigEvent",
        "PumpSwapUpdateFeeConfigEvent",
    )
)
_PUMP_SWAP_POOL_EVENTS = frozenset(
    (
        "DepositEvent",
        "WithdrawEvent",
        "DisableEvent",
        "UpdateAdminEvent",
        "MigratePoolCoinCreatorEvent",
        "SetBondingCurveCoinCreatorEvent",
        "SetMetaplexCoinCreatorEvent",
        "CollectCoinCreatorFeeEvent",
        "ReservedFeeRecipientsEvent",
        "AdminSetCoinCreatorEvent",
    )
)
_PUMP_NONCONFIG_EVENTS = frozenset(
    (
        "UpdateMayhemVirtualParamsEvent",
        "SetCreatorEvent",
        "MigrateBondingCurveCreatorEvent",
        "DistributeCreatorFeesEvent",
        "CollectCreatorFeeEvent",
        "ReservedFeeRecipientsEvent",
        "UpdateGlobalAuthorityEvent",
    )
)

RequiredSnapshot: TypeAlias = str
ConfigStatus: TypeAlias = Literal[
    "resolved", "partial", "unknown", "unresolved", "malformed", "no_config_change"
]


class StateConfigScheduleError(ValueError):
    """A sidecar, manifest, ordering, or schedule invariant failed."""


def _canonical_json(value: object) -> bytes:
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as error:
        raise StateConfigScheduleError("value is not canonical JSON") from error
    return (encoded + "\n").encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    try:
        return sha256_file(path)
    except OSError as error:
        raise StateConfigScheduleError(f"cannot read shard {path}") from error


def _text(value: object, label: str, *, required: bool = True) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    if not required and value is None:
        return None
    raise StateConfigScheduleError(f"{label} must be non-empty text")


def _nonnegative_int(value: object, label: str, *, required: bool = True) -> int | None:
    if value is None and not required:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise StateConfigScheduleError(f"{label} must be a non-negative integer")
    return value


def _hex64(value: object, label: str, *, required: bool = True) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or len(value) != 64 or any(char not in _HEX64 for char in value):
        raise StateConfigScheduleError(f"{label} must be lowercase SHA-256 hex")
    return value


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise StateConfigScheduleError(f"{label} must be an object with text keys")
    return cast(Mapping[str, object], value)


def _safe_relative(root: Path, value: object, label: str) -> Path:
    relative = Path(cast(str, _text(value, label)))
    if relative.is_absolute() or ".." in relative.parts:
        raise StateConfigScheduleError(f"{label} escapes its dataset")
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as error:
        raise StateConfigScheduleError(f"{label} escapes its dataset") from error
    return candidate


def _jsonable(value: object) -> object:
    """Convert Arrow/decoder values to deterministic JSON primitives."""

    if isinstance(value, Mapping):
        return {
            str(key): _jsonable(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, bytes):
        return value.hex()
    # ``pyarrow`` scalar values expose ``as_py``; keeping this duck-typed
    # avoids importing pyarrow for JSONL users.
    as_py = getattr(value, "as_py", None)
    if callable(as_py):
        return _jsonable(as_py())
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise StateConfigScheduleError(f"unsupported values_json type: {type(value).__name__}")


def _parse_values(value: object, label: str) -> Mapping[str, object]:
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as error:
            raise StateConfigScheduleError(f"{label} is not valid JSON") from error
    else:
        decoded = value
    return _mapping(_jsonable(decoded), label)


def _first(values: Mapping[str, object], *names: str) -> object:
    for name in names:
        if name in values:
            return values[name]
    return None


def _phase_for_program(program_id: str) -> str:
    return (
        VenuePhase.BONDING_CURVE.value
        if program_id == PUMP_PROGRAM_ID
        else VenuePhase.PUMPSWAP.value
    )


def _normalized_phase(value: object) -> str | None:
    """Normalize venue labels emitted by the sealed adapter to domain phases."""

    phase = _text(value, "phase", required=False)
    if phase in {"pump", "pump_curve"}:
        return VenuePhase.BONDING_CURVE.value
    return phase


def _scope_market(values: Mapping[str, object], row_market: object, program_id: str) -> str | None:
    if isinstance(row_market, str) and row_market.strip() and row_market != "*":
        return row_market
    candidate = (
        _first(values, "market_id", "market", "pool", "bonding_curve")
        if program_id == PUMPSWAP_PROGRAM_ID
        else _first(values, "market_id", "market", "bonding_curve")
    )
    return candidate if isinstance(candidate, str) and candidate.strip() else None


def _validate_values_json(values: Mapping[str, object]) -> None:
    # Values are exact decoder primitives.  In particular, booleans must not
    # pass as integers in fee fields (Python's bool is an int subclass).
    for name, value in values.items():
        if isinstance(value, (dict, list, tuple)):
            _jsonable(value)
        elif not isinstance(value, (str, int, float, bool)) and value is not None:
            raise StateConfigScheduleError(f"state event value {name} has unsupported type")


@dataclass(frozen=True, slots=True)
class StateEventRecord:
    """One normalized state-event sidecar row with source provenance."""

    slot: int
    tx_index: int
    instruction_index: int
    event_id: str
    state_event_id: str
    event_name: str
    program_id: str
    values: Mapping[str, object]
    log_index: int | None = None
    invoke_depth: int | None = None
    market_id: str | None = None
    phase: str | None = None
    signature: str | None = None
    raw_sha256: str | None = None
    idl_sha256: str | None = None
    source_manifest_sha256: str | None = None
    source_shard_path: str | None = None
    source_shard_index: int | None = None
    source_row_offset: int | None = None

    def __post_init__(self) -> None:
        for name in ("slot", "tx_index", "instruction_index"):
            _nonnegative_int(getattr(self, name), name)
        if self.log_index is not None:
            _nonnegative_int(self.log_index, "log_index")
        if self.invoke_depth is not None:
            _nonnegative_int(self.invoke_depth, "invoke_depth")
        if self.program_id not in _KNOWN_PROGRAMS:
            raise StateConfigScheduleError("state event program_id is not Pump or PumpSwap")
        _text(self.event_id, "event_id")
        _text(self.state_event_id, "state_event_id")
        _text(self.event_name, "event_name")
        _validate_values_json(self.values)
        if self.phase is not None and self.phase not in {
            "*",
            *(phase.value for phase in VenuePhase),
        }:
            raise StateConfigScheduleError("state event phase is unsupported")
        _hex64(self.raw_sha256, "raw_sha256", required=False)
        _hex64(self.idl_sha256, "idl_sha256", required=False)
        _hex64(self.source_manifest_sha256, "source_manifest_sha256", required=False)
        object.__setattr__(self, "values", dict(self.values))

    @property
    def ordering_key(self) -> tuple[int, int, int, int, int, str]:
        """Chain order; missing log indexes use a deterministic final tie."""

        return (
            self.slot,
            self.tx_index,
            self.instruction_index,
            self.log_index if self.log_index is not None else 2**63 - 1,
            self.invoke_depth if self.invoke_depth is not None else 2**31 - 1,
            self.state_event_id,
        )


@dataclass(frozen=True, slots=True)
class ConfigTransition:
    """Auditable interpretation of one event (resolved or explicitly unknown)."""

    event_id: str
    state_event_id: str
    slot: int
    ordering_key: tuple[int, int, int, int, int, str]
    program_id: str
    event_name: str
    market_id: str | None
    phase: str | None
    status: ConfigStatus
    reason: str
    config: Mapping[str, object] | None
    partial_config: Mapping[str, object] | None
    config_sha256: str
    required_account_snapshots: tuple[RequiredSnapshot, ...] = field(default_factory=tuple)
    source_manifest_sha256: str | None = None

    def payload(self) -> dict[str, object]:
        return {
            "record_type": "config_transition",
            "event_id": self.event_id,
            "state_event_id": self.state_event_id,
            "slot": self.slot,
            "ordering_key": list(self.ordering_key),
            "program_id": self.program_id,
            "event_name": self.event_name,
            "market_id": self.market_id,
            "phase": self.phase,
            "status": self.status,
            "reason": self.reason,
            "config": None if self.config is None else dict(self.config),
            "partial_config": None if self.partial_config is None else dict(self.partial_config),
            "config_sha256": self.config_sha256,
            "required_account_snapshots": list(self.required_account_snapshots),
            "source_manifest_sha256": self.source_manifest_sha256,
        }


@dataclass(frozen=True, slots=True)
class StateConfigScheduleResult:
    """Result returned by :func:`build_state_config_schedule`."""

    manifest: Mapping[str, object]
    transitions: tuple[ConfigTransition, ...]
    entries: tuple[Mapping[str, object], ...]

    @property
    def schedule(self) -> Mapping[str, object]:
        return {"version": SCHEMA_VERSION, "entries": list(self.entries)}


@dataclass(frozen=True, slots=True)
class _Shard:
    path: Path
    relative_path: str
    row_count: int
    sha256: str


@dataclass(frozen=True, slots=True)
class _Source:
    root: Path
    manifest_path: Path
    manifest_sha256: str
    manifest: Mapping[str, object]
    state_schema_sha256: str | None
    state_idl_sha256: object | None
    shards: tuple[_Shard, ...]
    row_count: int | None


def _state_metadata(manifest: Mapping[str, object]) -> Mapping[str, object] | None:
    nested = manifest.get("state")
    if isinstance(nested, Mapping):
        return cast(Mapping[str, object], nested)
    if any(key in manifest for key in ("state_shards", "state_row_count", "state_schema_sha256")):
        return manifest
    return None


def _nested_state_manifest(
    root: Path, manifest: Mapping[str, object]
) -> tuple[Mapping[str, object] | None, Path | None]:
    """Resolve a materialized wrapper's ``trade-events/state`` sidecar."""

    for key in ("state_event_dataset", "state_event_dataset_path", "state_path"):
        value = manifest.get(key)
        if value is None:
            continue
        if isinstance(value, Mapping):
            return cast(Mapping[str, object], value), None
        nested_root = _safe_relative(root, value, f"{key} path")
        nested_manifest = nested_root / "manifest.json"
        if nested_manifest.is_symlink() or not nested_manifest.is_file():
            continue
        try:
            raw_nested = nested_manifest.read_bytes()
            nested = _mapping(json.loads(raw_nested), "nested state-event manifest")
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise StateConfigScheduleError("nested state-event manifest is invalid JSON") from error
        return nested, nested_root
    return None, None


def _idl_metadata(value: object) -> object | None:
    """Validate a string IDL hash or the adapter's per-program hash map."""

    if value is None:
        return None
    if isinstance(value, str):
        return _hex64(value, "state_idl_sha256")
    mapping = _mapping(value, "state_idl_sha256")
    result: dict[str, str] = {}
    for key, item in sorted(mapping.items()):
        result[key] = cast(str, _hex64(item, f"state_idl_sha256[{key}]"))
    return result


def _resolve_shard_path(root: Path, state_root: Path, relative: Path) -> Path:
    """Resolve adapter paths whether they are state-relative or dataset-relative."""

    candidates: list[Path] = []
    state_prefix = state_root.relative_to(root)
    if tuple(relative.parts[: len(state_prefix.parts)]) == state_prefix.parts:
        candidates.append(root / relative)
    else:
        # Some manifests list a dataset-relative path even when a state path
        # prefix is present in the wrapper metadata.
        candidates.append(root / relative)
    candidates.append(state_root / relative)
    if relative.parts and relative.parts[0] == state_root.name:
        candidates.append(state_root.parent / relative)
    for candidate in candidates:
        resolved = candidate.resolve()
        try:
            resolved.relative_to(root.resolve())
        except ValueError:
            continue
        if not resolved.is_symlink() and resolved.is_file():
            return resolved
    # Return the most canonical state-relative candidate for a useful missing
    # file error; the caller still checks existence and symlink status.
    return (state_root / relative).resolve()


def _load_source(source: str | Path) -> _Source:
    path = Path(source).expanduser().resolve()
    if path.is_file() and path.name == "manifest.json":
        root, manifest_path = path.parent, path
    elif path.is_dir():
        root, manifest_path = path, path / "manifest.json"
    else:
        raise StateConfigScheduleError("state_event_dataset must be a directory or manifest.json")
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise StateConfigScheduleError(f"state-event manifest is missing: {manifest_path}")
    try:
        raw = manifest_path.read_bytes()
        manifest = _mapping(json.loads(raw), "state-event manifest")
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise StateConfigScheduleError("state-event manifest is invalid JSON") from error
    metadata = _state_metadata(manifest)
    nested_root: Path | None = None
    if metadata is None:
        metadata, nested_root = _nested_state_manifest(root, manifest)
    if metadata is None:
        # A wrapper may point to an independently sealed ``state/manifest.json``.
        nested_path_value = manifest.get("state_manifest_path", "state/manifest.json")
        nested_path = _safe_relative(root, nested_path_value, "state manifest path")
        if not nested_path.is_file():
            raise StateConfigScheduleError("manifest has no state shard metadata")
        try:
            nested_raw = nested_path.read_bytes()
            metadata = _mapping(json.loads(nested_raw), "state-event manifest")
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise StateConfigScheduleError("nested state-event manifest is invalid JSON") from error
        nested_root = nested_path.parent
    if nested_root is not None:
        state_path_value = metadata.get("path", metadata.get("state_path", "."))
        state_root = _safe_relative(nested_root, state_path_value, "state path")
    else:
        state_path_value = metadata.get("path", metadata.get("state_path", "state"))
        state_root = _safe_relative(root, state_path_value, "state path")
    raw_shards = metadata.get("shards", metadata.get("state_shards"))
    if isinstance(raw_shards, Mapping):
        raw_shards = raw_shards.get("entries", raw_shards.get("shards"))
    if not isinstance(raw_shards, Sequence) or isinstance(raw_shards, (str, bytes, bytearray)):
        raise StateConfigScheduleError("state shards must be an array")
    shards: list[_Shard] = []
    for index, raw_shard in enumerate(raw_shards):
        if isinstance(raw_shard, str):
            shard_meta: Mapping[str, object] = {"path": raw_shard}
        else:
            shard_meta = _mapping(raw_shard, f"state shard {index}")
        raw_path = shard_meta.get("path", shard_meta.get("file"))
        if raw_path is None:
            raise StateConfigScheduleError(f"state shard {index} has no path")
        relative = Path(cast(str, _text(raw_path, f"state shard {index} path")))
        if not relative.parts or relative.is_absolute() or ".." in relative.parts:
            raise StateConfigScheduleError(f"state shard {index} path is unsafe")
        # Adapter manifests commonly list ``part-00000.parquet`` under the
        # declared state/ prefix; paths already beginning with that prefix are
        # rooted at the dataset.  Materialized wrappers may use a longer
        # prefix such as ``trade-events/state``.
        shard_path = _resolve_shard_path(root, state_root, relative)
        if shard_path.is_symlink() or not shard_path.is_file():
            raise StateConfigScheduleError(f"state shard is missing: {shard_path}")
        byte_count = _nonnegative_int(
            shard_meta.get("byte_count"), f"state shard {index} byte_count"
        )
        expected_hash = _hex64(shard_meta.get("sha256"), f"state shard {index} sha256")
        if shard_path.stat().st_size != cast(int, byte_count):
            raise StateConfigScheduleError(
                f"state shard {index} byte count does not match manifest"
            )
        actual_hash = _sha256_file(shard_path)
        if actual_hash != expected_hash:
            raise StateConfigScheduleError(f"state shard {index} hash does not match manifest")
        rows = _nonnegative_int(shard_meta.get("row_count"), f"state shard {index} row_count")
        shards.append(
            _Shard(shard_path, str(shard_path.relative_to(root)), cast(int, rows), actual_hash)
        )
    if not shards:
        raise StateConfigScheduleError("state-event dataset has no shards")
    declared_rows_raw = metadata.get("row_count", metadata.get("state_row_count"))
    declared_rows = (
        None
        if declared_rows_raw is None
        else cast(int, _nonnegative_int(declared_rows_raw, "state_row_count"))
    )
    if declared_rows is not None and declared_rows != sum(item.row_count for item in shards):
        raise StateConfigScheduleError("state row count does not match shard metadata")
    schema_hash = _hex64(
        metadata.get("schema_sha256", metadata.get("state_schema_sha256")),
        "state_schema_sha256",
        required=False,
    )
    idl_hash = _idl_metadata(metadata.get("idl_sha256", metadata.get("state_idl_sha256")))
    return _Source(
        root=root,
        manifest_path=manifest_path,
        manifest_sha256=_sha256_bytes(raw),
        manifest=manifest,
        state_schema_sha256=schema_hash,
        state_idl_sha256=idl_hash,
        shards=tuple(shards),
        row_count=declared_rows,
    )


def _row_from_mapping(
    raw: Mapping[str, object],
    *,
    source: _Source | None,
    shard: _Shard | None,
    shard_index: int | None,
    row_offset: int | None,
) -> StateEventRecord:
    def required_alias(*names: str) -> object:
        value = _first(raw, *names)
        if value is None:
            raise StateConfigScheduleError(f"state row is missing {names[0]}")
        return value

    slot = cast(int, _nonnegative_int(required_alias("slot", "block_slot"), "slot"))
    tx_index = cast(
        int,
        _nonnegative_int(
            _first(raw, "tx_index", "transaction_index", "tx_position") or 0, "tx_index"
        ),
    )
    ix = cast(
        int,
        _nonnegative_int(
            _first(raw, "instruction_index", "ix_index", "source_index") or 0, "instruction_index"
        ),
    )
    event_id_value = required_alias("state_event_id", "event_id")
    event_id = cast(str, _text(event_id_value, "event_id"))
    state_id = cast(str, _text(_first(raw, "state_event_id") or event_id, "state_event_id"))
    event_name = cast(str, _text(required_alias("event_name", "kind"), "event_name"))
    program_id = cast(str, _text(required_alias("program_id", "program"), "program_id"))
    values = _parse_values(required_alias("values_json", "values", "event_values"), "values_json")
    log_index = _nonnegative_int(
        _first(raw, "log_index", "event_index", "state_event_index"), "log_index", required=False
    )
    invoke_depth = _nonnegative_int(
        _first(raw, "invoke_depth", "cpi_depth"), "invoke_depth", required=False
    )
    market = _text(_first(raw, "market_id", "market"), "market_id", required=False)
    phase = _normalized_phase(_first(raw, "phase", "venue"))
    signature = _text(_first(raw, "signature", "tx_signature"), "signature", required=False)
    raw_sha = _hex64(
        _first(raw, "raw_sha256", "raw_event_sha256", "event_raw_sha256"),
        "raw_sha256",
        required=False,
    )
    idl_sha = _hex64(_first(raw, "idl_sha256", "event_idl_sha256"), "idl_sha256", required=False)
    raw_base64 = _first(raw, "raw_event_base64", "event_raw_base64")
    if raw_base64 is not None:
        encoded = _text(raw_base64, "raw_event_base64")
        try:
            raw_bytes = base64.b64decode(cast(str, encoded), validate=True)
        except (ValueError, binascii.Error) as error:
            raise StateConfigScheduleError("raw_event_base64 is not valid base64") from error
        if raw_sha is not None and _sha256_bytes(raw_bytes) != raw_sha:
            raise StateConfigScheduleError("raw event hash does not match raw_event_base64")
        try:
            decoded = decode_state_event(program_id, raw_bytes)
        except Exception as error:
            raise StateConfigScheduleError(
                "raw_event_base64 is not a pinned state event"
            ) from error
        if decoded.event_name != event_name:
            raise StateConfigScheduleError("state row event_name does not match raw event")
    if source is not None:
        expected_idl = source.state_idl_sha256
        if (
            idl_sha is not None
            and isinstance(expected_idl, str)
            and expected_idl in _KNOWN_IDL_SHA256.values()
            and idl_sha != expected_idl
        ):
            raise StateConfigScheduleError("state row IDL hash differs from source manifest")
        if (
            idl_sha is not None
            and isinstance(expected_idl, Mapping)
            and idl_sha not in set(cast(Mapping[str, str], expected_idl).values())
        ):
            raise StateConfigScheduleError("state row IDL hash differs from source manifest")
        source_manifest_sha = source.manifest_sha256
    else:
        source_manifest_sha = None
    return StateEventRecord(
        slot=slot,
        tx_index=tx_index,
        instruction_index=ix,
        event_id=event_id,
        state_event_id=state_id,
        event_name=event_name,
        program_id=program_id,
        values=values,
        log_index=log_index,
        invoke_depth=invoke_depth,
        market_id=market,
        phase=phase,
        signature=signature,
        raw_sha256=raw_sha,
        idl_sha256=idl_sha,
        source_manifest_sha256=source_manifest_sha,
        source_shard_path=None if shard is None else shard.relative_path,
        source_shard_index=shard_index,
        source_row_offset=row_offset,
    )


def _iter_jsonl(path: Path, *, batch_size: int) -> Iterator[Mapping[str, object]]:
    try:
        handle = path.open("r", encoding="utf-8")
    except OSError as error:
        raise StateConfigScheduleError(f"cannot open state shard {path}") from error
    with handle:
        batch: list[Mapping[str, object]] = []
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                raise StateConfigScheduleError(
                    f"state JSONL line {line_number} is invalid"
                ) from error
            batch.append(_mapping(value, f"state JSONL line {line_number}"))
            if len(batch) >= batch_size:
                yield from batch
                batch.clear()
        yield from batch


def _iter_parquet(path: Path, *, batch_size: int) -> Iterator[Mapping[str, object]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as error:  # pragma: no cover - optional dependency
        raise StateConfigScheduleError(
            "Parquet state shards require: pip install 'pumpquant[data]'"
        ) from error
    try:
        parquet = pq.ParquetFile(path)
    except Exception as error:  # pragma: no cover - pyarrow-specific failures
        raise StateConfigScheduleError(f"cannot open state Parquet shard {path}") from error
    try:
        for batch in parquet.iter_batches(batch_size=batch_size, use_threads=False):
            rows = batch.to_pylist()
            for raw in rows:
                yield _mapping(raw, "state Parquet row")
    finally:
        close = getattr(parquet, "close", None)
        if callable(close):
            close()


def iter_state_event_records(
    source: str | Path,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    start_slot: int | None = None,
    end_slot: int | None = None,
) -> Iterator[StateEventRecord]:
    """Yield verified state rows in source order, bounded by ``batch_size``.

    The source is required to be sealed.  The stream checks canonical chain
    order and duplicate event IDs before yielding each record.  ``start_slot``
    is inclusive and ``end_slot`` is exclusive; filtering still consumes and
    validates the complete source, so a partial result cannot hide a corrupt
    later shard.
    """

    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise StateConfigScheduleError("batch_size must be positive")
    if start_slot is not None:
        _nonnegative_int(start_slot, "start_slot")
    if end_slot is not None:
        _nonnegative_int(end_slot, "end_slot")
        if start_slot is not None and end_slot <= start_slot:
            raise StateConfigScheduleError("end_slot must exceed start_slot")
    loaded = _load_source(source)
    seen_ids: set[str] = set()
    previous_key: tuple[int, int, int, int, int, str] | None = None
    yielded = 0
    for shard_index, shard in enumerate(loaded.shards):
        suffix = shard.path.suffix.lower()
        raw_rows = (
            _iter_parquet(shard.path, batch_size=batch_size)
            if suffix == ".parquet"
            else _iter_jsonl(shard.path, batch_size=batch_size)
        )
        row_offset = 0
        for raw in raw_rows:
            row = _row_from_mapping(
                raw, source=loaded, shard=shard, shard_index=shard_index, row_offset=row_offset
            )
            row_offset += 1
            if row.event_id in seen_ids:
                raise StateConfigScheduleError(f"duplicate state event_id: {row.event_id}")
            seen_ids.add(row.event_id)
            key = row.ordering_key
            if previous_key is not None and key < previous_key:
                raise StateConfigScheduleError("state events are not in deterministic chain order")
            previous_key = key
            if (start_slot is None or row.slot >= start_slot) and (
                end_slot is None or row.slot < end_slot
            ):
                yielded += 1
                yield row
        if row_offset != shard.row_count:
            raise StateConfigScheduleError(
                f"state shard {shard.relative_path} yielded {row_offset} rows, "
                f"manifest declares {shard.row_count}"
            )
    if loaded.row_count is not None and sum(1 for _ in seen_ids) != loaded.row_count:
        raise StateConfigScheduleError("state row count does not match streamed rows")


def _bps(value: object, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 10_000:
        raise StateConfigScheduleError(f"{name} must be an integer basis-point value in [0, 10000]")
    return value


def _config_from_event(
    row: StateEventRecord,
    override: Mapping[str, object] | None,
) -> tuple[
    ConfigStatus, str, Mapping[str, object] | None, Mapping[str, object] | None, tuple[str, ...]
]:
    values = dict(row.values)
    if override is not None:
        supplied = _mapping(override, "config resolution")
        nested = supplied.get("config")
        if nested is not None:
            values.update(_mapping(nested, "config resolution config"))
        values.update({key: value for key, value in supplied.items() if key != "config"})
    nested_event_config = values.get("config")
    if isinstance(nested_event_config, Mapping):
        values.update(cast(Mapping[str, object], nested_event_config))
    if row.program_id == PUMP_PROGRAM_ID and row.event_name in _PUMP_CONFIG_EVENTS:
        try:
            protocol = _bps(
                _first(values, "protocol_fee_bps", "fee_basis_points", "protocol_fee_basis_points"),
                "protocol_fee_bps",
            )
            creator = _bps(
                _first(values, "creator_fee_bps", "creator_fee_basis_points"), "creator_fee_bps"
            )
        except StateConfigScheduleError as error:
            return (
                "malformed",
                str(error),
                None,
                {
                    key: value
                    for key, value in values.items()
                    if key
                    in {
                        "protocol_fee_bps",
                        "fee_basis_points",
                        "creator_fee_bps",
                        "creator_fee_basis_points",
                    }
                }
                or None,
                ("Global account snapshot", "FeeConfig tier vector"),
            )
        enabled_value = _first(values, "creator_fee_enabled", "enable_creator_fee")
        enabled = enabled_value if isinstance(enabled_value, bool) else None
        partial: dict[str, object] = {}
        if protocol is not None:
            partial["protocol_fee_bps"] = protocol
        if creator is not None:
            partial["creator_fee_bps"] = creator
        if enabled is not None:
            partial["creator_fee_enabled"] = enabled
        required = (
            "Global account snapshot",
            "FeeConfig tier vector",
            "BondingCurve account snapshot",
            "SharingConfig account snapshot",
        )
        if protocol is None or creator is None or enabled is None:
            return (
                "partial",
                "Pump SetParamsEvent lacks a complete executable fee config",
                None,
                partial or None,
                required,
            )
        config = {
            "kind": "pump",
            "protocol_fee_bps": protocol,
            "creator_fee_bps": creator,
            "creator_fee_enabled": enabled,
            "atomic": True,
        }
        return (
            "resolved",
            "complete Pump fee configuration carried by event/resolution",
            config,
            None,
            (),
        )
    if row.program_id == PUMPSWAP_PROGRAM_ID and row.event_name in _PUMPSWAP_CONFIG_EVENTS:
        fields = {
            "lp_fee_bps": _first(values, "lp_fee_bps", "lp_fee_basis_points"),
            "protocol_fee_bps": _first(values, "protocol_fee_bps", "protocol_fee_basis_points"),
            "creator_fee_bps": _first(
                values,
                "creator_fee_bps",
                "coin_creator_fee_basis_points",
                "creator_fee_basis_points",
            ),
            "cashback_fee_bps": _first(values, "cashback_fee_bps", "cashback_fee_basis_points"),
            "buyback_fee_bps": _first(values, "buyback_fee_bps", "buyback_fee_basis_points"),
        }
        partial = {name: value for name, value in fields.items() if value is not None}
        normalized: dict[str, int] = {}
        try:
            for name, value in fields.items():
                parsed = _bps(value, name)
                if parsed is None:
                    continue
                normalized[name] = parsed
        except StateConfigScheduleError as error:
            return "malformed", str(error), None, partial or None, ("FeeConfig account snapshot",)
        required = (
            "Pool account snapshot",
            "GlobalConfig account snapshot",
            "FeeConfig tier vector",
            "SharingConfig account snapshot",
        )
        if len(normalized) != len(fields):
            return (
                "partial",
                "PumpSwap config event omits one or more executable fee components",
                None,
                normalized or None,
                required,
            )
        if sum(normalized.values()) > 10_000:
            return (
                "malformed",
                "PumpSwap fee components exceed 10000 bps",
                None,
                normalized,
                required,
            )
        config = {"kind": "pumpswap", **normalized, "atomic": True}
        return (
            "resolved",
            "complete PumpSwap fee configuration carried by event/resolution",
            config,
            None,
            (),
        )
    if row.program_id == PUMP_PROGRAM_ID and row.event_name in _PUMP_NONCONFIG_EVENTS:
        return (
            "no_config_change",
            "state event changes creator/reserve metadata, not fee configuration",
            None,
            None,
            (),
        )
    if row.program_id == PUMPSWAP_PROGRAM_ID and row.event_name in _PUMP_SWAP_POOL_EVENTS:
        return (
            "no_config_change",
            "state event changes pool/admin/liquidity metadata, not fee rates",
            None,
            None,
            (),
        )
    return (
        "unresolved",
        "event is not a pinned fee/config transition",
        None,
        None,
        ("account snapshot and pinned event mapping",),
    )


def _override_for(
    row: StateEventRecord,
    overrides: Mapping[str, Mapping[str, object]]
    | Callable[[StateEventRecord], Mapping[str, object] | None]
    | None,
) -> Mapping[str, object] | None:
    if overrides is None:
        return None
    if callable(overrides):
        return overrides(row)
    value = overrides.get(row.state_event_id, overrides.get(row.event_id))
    return None if value is None else _mapping(value, "config resolution")


def _transition(
    row: StateEventRecord,
    overrides: Mapping[str, Mapping[str, object]]
    | Callable[[StateEventRecord], Mapping[str, object] | None]
    | None,
) -> ConfigTransition:
    status, reason, config, partial, required = _config_from_event(
        row, _override_for(row, overrides)
    )
    market = _scope_market(row.values, row.market_id, row.program_id)
    phase = row.phase or _phase_for_program(row.program_id)
    # A resolved hash intentionally matches the materializer's canonical
    # config hash.  Partial/unresolved records still receive a stable hash of
    # their exact candidate so they can be audited without pretending to be
    # executable configuration.
    hash_payload: object = (
        config if config is not None else {"status": status, "partial_config": partial}
    )
    config_hash = _sha256_bytes(_canonical_json(hash_payload))
    return ConfigTransition(
        event_id=row.event_id,
        state_event_id=row.state_event_id,
        slot=row.slot,
        ordering_key=row.ordering_key,
        program_id=row.program_id,
        event_name=row.event_name,
        market_id=market,
        phase=phase,
        status=status,
        reason=reason,
        config=config,
        partial_config=partial,
        config_sha256=config_hash,
        required_account_snapshots=tuple(required),
        source_manifest_sha256=row.source_manifest_sha256,
    )


def _schedule_entries(transitions: Sequence[ConfigTransition]) -> tuple[Mapping[str, object], ...]:
    """Collapse same-slot transitions to their final chain-ordered state.

    The materializer intentionally resolves by slot (not by log index), so two
    entries with one slot would be ambiguous.  The complete transition audit
    retains every same-slot event; the schedule promotes only the last state in
    each ``(market_id, phase, slot)`` scope.
    """

    candidates = [item for item in transitions if item.status != "no_config_change"]
    grouped: dict[tuple[str | None, str | None], list[ConfigTransition]] = {}
    for item in candidates:
        grouped.setdefault((item.market_id, item.phase), []).append(item)
    entries: list[Mapping[str, object]] = []
    for scope, items in grouped.items():
        ordered = sorted(items, key=lambda item: item.ordering_key)
        by_slot: dict[int, ConfigTransition] = {}
        for item in ordered:
            by_slot[item.slot] = item
        slots = sorted(by_slot)
        for index, slot in enumerate(slots):
            item = by_slot[slot]
            end = None if index + 1 == len(slots) else slots[index + 1]
            entries.append(
                {
                    "start_slot": slot,
                    "end_slot": end,
                    "market_id": scope[0],
                    "phase": scope[1],
                    "config": None if item.config is None else dict(item.config),
                }
            )
    entries.sort(
        key=lambda item: (
            cast(int, item["start_slot"]),
            0 if item["market_id"] is None else 1,
            "" if item["market_id"] is None else cast(str, item["market_id"]),
            "" if item["phase"] is None else cast(str, item["phase"]),
        )
    )
    return tuple(entries)


def _file_meta(path: Path, root: Path) -> dict[str, object]:
    data = path.read_bytes()
    return {
        "path": str(path.relative_to(root)),
        "byte_count": len(data),
        "sha256": _sha256_bytes(data),
    }


def _write_bytes(path: Path, value: bytes) -> None:
    path.write_bytes(value)
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def build_state_config_schedule(
    source: str | Path,
    output: str | Path,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    start_slot: int | None = None,
    end_slot: int | None = None,
    config_resolutions: Mapping[str, Mapping[str, object]]
    | Callable[[StateEventRecord], Mapping[str, object] | None]
    | None = None,
) -> StateConfigScheduleResult:
    """Verify ``source`` and atomically publish a materializer fee schedule.

    ``config_resolutions`` is intentionally explicit: callers may provide
    account-snapshot fields keyed by event ID, but this bridge never fetches or
    guesses them.  The returned ``schedule.json`` is accepted directly by
    :func:`pumpquant.amm_state_materializer.load_fee_schedule`.
    """

    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise StateConfigScheduleError("batch_size must be positive")
    loaded = _load_source(source)
    rows = iter_state_event_records(
        source, batch_size=batch_size, start_slot=start_slot, end_slot=end_slot
    )
    transitions = tuple(_transition(row, config_resolutions) for row in rows)
    entries = _schedule_entries(transitions)
    schedule_payload = {"version": SCHEMA_VERSION, "entries": list(entries)}
    schedule_body = _canonical_json(schedule_payload)
    schedule_jsonl_body = b"".join(_canonical_json(entry) for entry in entries)
    transition_body = b"".join(_canonical_json(item.payload()) for item in transitions)
    status_counts = Counter(item.status for item in transitions)
    required = sorted({name for item in transitions for name in item.required_account_snapshots})
    audit_payload: dict[str, object] = {
        "kind": AUDIT_KIND,
        "version": SCHEMA_VERSION,
        "source_kind": loaded.manifest.get("kind", STATE_EVENT_DATASET_KIND),
        "source_manifest_sha256": loaded.manifest_sha256,
        "source_state_schema_sha256": loaded.state_schema_sha256,
        "source_state_idl_sha256": loaded.state_idl_sha256,
        "source_row_count": loaded.row_count,
        "source_shard_count": len(loaded.shards),
        "transition_count": len(transitions),
        "schedule_entry_count": len(entries),
        "status_counts": dict(sorted(status_counts.items())),
        "required_account_snapshots": required,
        "source_order": "slot,tx_index,instruction_index,log_index,invoke_depth,state_event_id",
        "same_slot_policy": "last_chain_ordered_transition_promoted; all transitions retained",
        "start_slot": start_slot,
        "end_slot": end_slot,
        "schedule_sha256": _sha256_bytes(schedule_body),
        "schedule_jsonl_sha256": _sha256_bytes(schedule_jsonl_body),
        "transitions_sha256": _sha256_bytes(transition_body),
        "pinned_event_idl_revision": PINNED_EVENT_IDL_REVISION,
        "pinned_event_idl_sha256": dict(_KNOWN_IDL_SHA256),
    }
    audit_body = _canonical_json(audit_payload)
    target = Path(output).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{target.name}.", suffix=".partial", dir=target.parent)
    )
    published = False
    try:
        schedule_path = staging / "schedule.json"
        schedule_jsonl_path = staging / "schedule.jsonl"
        transitions_path = staging / "transitions.jsonl"
        audit_path = staging / "audit.json"
        _write_bytes(schedule_path, schedule_body)
        _write_bytes(schedule_jsonl_path, schedule_jsonl_body)
        _write_bytes(transitions_path, transition_body)
        _write_bytes(audit_path, audit_body)
        files = {
            path.name: _file_meta(path, staging)
            for path in (schedule_path, schedule_jsonl_path, transitions_path, audit_path)
        }
        manifest_payload: dict[str, object] = {
            "kind": STATE_CONFIG_SCHEDULE_KIND,
            "version": SCHEMA_VERSION,
            "source_manifest_sha256": loaded.manifest_sha256,
            "source_manifest_path": str(loaded.manifest_path),
            "source_state_schema_sha256": loaded.state_schema_sha256,
            "source_state_idl_sha256": loaded.state_idl_sha256,
            "source_row_count": loaded.row_count,
            "source_shard_count": len(loaded.shards),
            "transition_count": len(transitions),
            "schedule_entry_count": len(entries),
            "schedule_sha256": _sha256_bytes(schedule_body),
            "schedule_jsonl_sha256": _sha256_bytes(schedule_jsonl_body),
            "transitions_sha256": _sha256_bytes(transition_body),
            "audit_sha256": _sha256_bytes(audit_body),
            "files": files,
        }
        manifest_body = _canonical_json(manifest_payload)
        _write_bytes(staging / "manifest.json", manifest_body)
        old: Path | None = None
        if target.exists():
            old = Path(
                tempfile.mkdtemp(
                    prefix=f".{target.name}.previous.", suffix=".partial", dir=target.parent
                )
            )
            old.rmdir()
            os.replace(target, old)
        try:
            os.replace(staging, target)
        except BaseException:
            if old is not None and not target.exists():
                os.replace(old, target)
            raise
        published = True
        if old is not None:
            shutil.rmtree(old, ignore_errors=True)
        manifest_payload = dict(manifest_payload)
        manifest_payload["manifest_sha256"] = _sha256_bytes(manifest_body)
        return StateConfigScheduleResult(manifest_payload, transitions, entries)
    finally:
        if not published and staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def load_state_config_schedule(path: str | Path) -> Any:
    """Load and validate the published ``schedule.json`` through materializer code."""

    target = Path(path)
    schedule_path = target / "schedule.json" if target.is_dir() else target
    return load_fee_schedule(schedule_path)


# Naming aliases make the bridge easy to discover from scripts and callers
# that use "config schedule" or "fee schedule" terminology interchangeably.
build_fee_schedule_from_state_events = build_state_config_schedule
build_config_schedule = build_state_config_schedule
materialize_state_config_schedule = build_state_config_schedule
iter_state_events = iter_state_event_records
iter_state_event_rows = iter_state_event_records
load_state_event_records = iter_state_event_records


__all__ = [
    "AUDIT_KIND",
    "ConfigTransition",
    "DEFAULT_BATCH_SIZE",
    "SCHEMA_VERSION",
    "STATE_CONFIG_SCHEDULE_KIND",
    "STATE_EVENT_DATASET_KIND",
    "StateConfigScheduleError",
    "StateConfigScheduleResult",
    "StateEventRecord",
    "build_fee_schedule_from_state_events",
    "build_config_schedule",
    "build_state_config_schedule",
    "iter_state_event_records",
    "iter_state_event_rows",
    "iter_state_events",
    "load_state_event_records",
    "load_state_config_schedule",
    "materialize_state_config_schedule",
]
