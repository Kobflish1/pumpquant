"""Bounded, manifest-last materialization of reconstructed AMM state.

The normalizer publishes one ``swaps.parquet`` file and a manifest describing
its identity.  This module consumes that file in Arrow batches, delegates all
state and quote semantics to :mod:`pumpquant.amm_state_reconstruction`, and
publishes two schema-explicit Parquet files (post-event snapshots and quote
levels) in one atomic directory replacement.

Fee configuration is intentionally a separate, slot-bounded input.  A missing
schedule entry produces an ``unknown_config`` snapshot; the event's observed
``venue_fee_bps`` is never used as a protocol fee fallback.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any, TypeAlias, cast

from pumpquant.core.hashing import sha256_file
from pumpquant.core.io import iter_swap_event_batches
from pumpquant.core.schema import Number, SwapEvent, VenuePhase
from pumpquant.pricing.pumpswap import PumpSwapFees
from pumpquant.pricing.quote_surface import SURFACE_LABELS
from pumpquant.state.reconstruction import (
    AMMQuoteConfig,
    AMMStateReconstructor,
    FeeConfig,
    FeeConfigResolver,
    PumpFeeConfig,
    PumpSwapAMMConfig,
)

MATERIALIZED_KIND = "pumpquant.amm_state_dataset"
AUDIT_KIND = "pumpquant.amm_state_audit"
SCHEMA_VERSION = 1
NORMALIZED_KIND = "pumpquant.normalized_swap_dataset"
PHYSICAL_ORDER = ("slot", "tx_index", "instruction_index", "event_id")
DEFAULT_BATCH_SIZE = 32_768


class MaterializationError(RuntimeError):
    """A source, schedule, output, or publication invariant failed."""


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MaterializationError(f"{label} must be a non-negative integer")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MaterializationError(f"{label} must be non-empty text")
    return value


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise MaterializationError(f"{label} must be an object with text keys")
    return cast(Mapping[str, object], value)


def _optional_slot(value: object, label: str) -> int | None:
    if value is None:
        return None
    return _nonnegative_int(value, label)


def _phase(value: object, label: str = "phase") -> VenuePhase | str | None:
    if value is None:
        return None
    if value == "*":
        return "*"
    try:
        return value if isinstance(value, VenuePhase) else VenuePhase(_text(value, label))
    except ValueError as error:
        raise MaterializationError(f"unsupported {label}") from error


def _fee_payload(value: object) -> object:
    """Convert one fee value to deterministic JSON-safe configuration fields."""

    if value is None:
        return None
    if isinstance(value, PumpSwapFees):
        return {
            "kind": "pumpswap",
            "lp_fee_bps": value.lp_fee_bps,
            "protocol_fee_bps": value.protocol_fee_bps,
            "creator_fee_bps": value.creator_fee_bps,
            "cashback_fee_bps": value.cashback_fee_bps,
            "buyback_fee_bps": value.buyback_fee_bps,
        }
    if isinstance(value, PumpFeeConfig):
        return {
            "kind": "pump",
            "protocol_fee_bps": value.protocol_fee_bps,
            "creator_fee_bps": value.creator_fee_bps,
            "creator_fee_enabled": value.creator_fee_enabled,
            "atomic": value.atomic,
        }
    if isinstance(value, PumpSwapAMMConfig):
        return {
            "kind": "pumpswap",
            "fees": _fee_payload(value.fees),
            "atomic": value.atomic,
        }
    if isinstance(value, AMMQuoteConfig):
        return {
            "kind": "quote_config",
            **{
                name: _fee_payload(item) if name in {"pumpswap_fees", "pump_fee_config"} else item
                for name, item in asdict(value).items()
            },
        }
    if is_dataclass(value):
        fields = asdict(cast(Any, value))
        return {str(key): _fee_payload(item) for key, item in fields.items()}
    if isinstance(value, Mapping):
        return {str(key): _fee_payload(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_fee_payload(item) for item in value]
    if isinstance(value, (str, int, float, bool)):
        return value
    raise MaterializationError(f"unsupported fee configuration value: {type(value).__name__}")


def _config_hash(value: object) -> str:
    return _sha256_bytes(_canonical_json(_fee_payload(value)))


def _pumpswap_from_mapping(values: Mapping[str, object]) -> PumpSwapFees:
    source: Mapping[str, object] = values
    nested = values.get("fees")
    if nested is not None:
        source = _mapping(nested, "PumpSwap fees")
    names = (
        "lp_fee_bps",
        "protocol_fee_bps",
        "creator_fee_bps",
        "cashback_fee_bps",
        "buyback_fee_bps",
    )
    kwargs: dict[str, int] = {}
    for name in names:
        raw = source.get(name, 0)
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            raise MaterializationError(f"{name} must be a non-negative integer")
        kwargs[name] = raw
    try:
        return PumpSwapFees(**kwargs)
    except ValueError as error:
        raise MaterializationError(str(error)) from error


def _coerce_fee_config(value: object) -> FeeConfig | None:
    """Parse a strict config value used by Python callers and JSON schedules."""

    if value is None:
        return None
    if isinstance(value, (AMMQuoteConfig, PumpFeeConfig, PumpSwapAMMConfig, PumpSwapFees)):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return AMMQuoteConfig.normalized(value)
    if not isinstance(value, Mapping):
        raise MaterializationError(f"unsupported fee configuration type: {type(value).__name__}")
    values = dict(value)
    nested = values.get("config")
    if nested is not None:
        return _coerce_fee_config(nested)
    kind = str(values.get("kind", values.get("type", ""))).lower()
    if "fees" in values:
        fees_value = values["fees"]
        fees = (
            fees_value
            if isinstance(fees_value, PumpSwapFees)
            else _pumpswap_from_mapping(_mapping(fees_value, "PumpSwap fees"))
        )
        atomic = values.get("atomic", True)
        if not isinstance(atomic, bool):
            raise MaterializationError("atomic must be boolean")
        return PumpSwapAMMConfig(fees=fees, atomic=atomic)
    if kind in {"quote_config", "amm_quote_config", "normalized", "approximate"}:
        values.pop("kind", None)
        values.pop("type", None)
    pumpswap_fields = values.get("pumpswap_fees")
    if pumpswap_fields is not None:
        if isinstance(pumpswap_fields, PumpSwapFees):
            values["pumpswap_fees"] = pumpswap_fields
        else:
            values["pumpswap_fees"] = _pumpswap_from_mapping(
                _mapping(pumpswap_fields, "pumpswap_fees")
            )
        try:
            return AMMQuoteConfig(**values)
        except (TypeError, ValueError) as error:
            raise MaterializationError("PumpSwap quote configuration fields are invalid") from error
    pump_fields = values.get("pump_fee_config")
    if pump_fields is not None:
        values["pump_fee_config"] = _coerce_fee_config(pump_fields)
        try:
            return AMMQuoteConfig(**values)
        except (TypeError, ValueError) as error:
            raise MaterializationError("Pump quote configuration fields are invalid") from error
    if kind in {"pumpswap", "pump_swap", "pump-swap"} or (
        "lp_fee_bps" in values and not {"protocol_fee_bps", "creator_fee_enabled"}.issubset(values)
    ):
        fees = _pumpswap_from_mapping(values)
        atomic = values.get("atomic", True)
        if not isinstance(atomic, bool):
            raise MaterializationError("atomic must be boolean")
        return PumpSwapAMMConfig(fees=fees, atomic=atomic)
    if kind in {"pump", "bonding_curve", "bonding-curve"} or {
        "protocol_fee_bps",
        "creator_fee_bps",
        "creator_fee_enabled",
    }.issubset(values):
        protocol = values.get("protocol_fee_bps")
        creator = values.get("creator_fee_bps")
        enabled = values.get("creator_fee_enabled", True)
        atomic = values.get("atomic", True)
        if (
            isinstance(protocol, bool)
            or not isinstance(protocol, int)
            or isinstance(creator, bool)
            or not isinstance(creator, int)
            or not isinstance(enabled, bool)
            or not isinstance(atomic, bool)
        ):
            raise MaterializationError("Pump fee configuration fields are invalid")
        try:
            return PumpFeeConfig(protocol, creator, enabled, atomic)
        except ValueError as error:
            raise MaterializationError(str(error)) from error
    if "fee_bps" in values:
        fee_bps = values.get("fee_bps")
        atomic = values.get("atomic", False)
        if isinstance(fee_bps, bool) or not isinstance(fee_bps, (int, float)):
            raise MaterializationError("fee_bps must be a finite number")
        if not isinstance(atomic, bool):
            raise MaterializationError("atomic must be boolean")
        try:
            return AMMQuoteConfig(fee_bps=fee_bps, atomic=atomic)
        except ValueError as error:
            raise MaterializationError(str(error)) from error
    try:
        return AMMQuoteConfig(**values)
    except (TypeError, ValueError) as error:
        raise MaterializationError("fee configuration fields are invalid") from error


FeeScheduleValue: TypeAlias = FeeConfig | Mapping[str, object] | int | float | None


@dataclass(frozen=True, slots=True)
class FeeScheduleEntry:
    """One explicit fee/config validity window.

    ``start_slot`` is inclusive and ``end_slot`` is exclusive.  ``None`` is a
    deliberate unknown-config interval, not permission to infer a fee from an
    event.  An open-ended end is allowed for a current config, but no event
    before ``start_slot`` can use it.
    """

    start_slot: int
    end_slot: int | None = None
    config: FeeScheduleValue = None
    market_id: str | None = None
    phase: VenuePhase | str | None = None

    def __post_init__(self) -> None:
        _nonnegative_int(self.start_slot, "start_slot")
        if self.end_slot is not None:
            _nonnegative_int(self.end_slot, "end_slot")
            if self.end_slot <= self.start_slot:
                raise MaterializationError("fee schedule end_slot must exceed start_slot")
        if self.market_id is not None:
            _text(self.market_id, "market_id")
        normalized_phase = _phase(self.phase)
        if normalized_phase is not self.phase:
            object.__setattr__(self, "phase", normalized_phase)
        object.__setattr__(self, "config", _coerce_fee_config(self.config))

    @property
    def start_slot_inclusive(self) -> int:
        return self.start_slot

    @property
    def end_slot_exclusive(self) -> int | None:
        return self.end_slot

    def matches(self, event: SwapEvent) -> bool:
        return (
            event.slot >= self.start_slot
            and (self.end_slot is None or event.slot < self.end_slot)
            and (
                self.market_id is None or self.market_id == "*" or self.market_id == event.market_id
            )
            and (self.phase is None or self.phase == "*" or self.phase == event.phase)
        )

    def specificity(self) -> int:
        return int(self.market_id not in (None, "*")) + int(self.phase not in (None, "*"))

    def payload(self) -> dict[str, object]:
        return {
            "start_slot": self.start_slot,
            "end_slot": self.end_slot,
            "market_id": self.market_id,
            "phase": (
                None
                if self.phase is None
                else self.phase
                if self.phase == "*"
                else VenuePhase(self.phase).value
            ),
            "config": _fee_payload(_coerce_fee_config(self.config)),
        }


def _coerce_schedule_entry(value: object, index: int = 0) -> FeeScheduleEntry:
    if isinstance(value, FeeScheduleEntry):
        return value
    values = dict(_mapping(value, f"fee schedule entry {index}"))
    start = values.pop("start_slot", values.pop("start_slot_inclusive", None))
    end = values.pop("end_slot", values.pop("end_slot_exclusive", None))
    if start is None:
        start = values.pop("from_slot", values.pop("available_from_slot", None))
    if end is None:
        end = values.pop("to_slot", values.pop("available_to_slot", None))
    if start is None:
        raise MaterializationError(f"fee schedule entry {index} has no start slot")
    config = values.pop("config", values.pop("fee_config", None))
    if "config_path" in values:
        raise MaterializationError("config_path is not allowed in a schedule")
    market_id = values.pop("market_id", values.pop("market", None))
    phase = values.pop("phase", None)
    if values:
        raise MaterializationError(
            f"fee schedule entry {index} has unsupported fields: {sorted(values)}"
        )
    return FeeScheduleEntry(
        start_slot=_nonnegative_int(start, f"entry {index} start_slot"),
        end_slot=_optional_slot(end, f"entry {index} end_slot"),
        config=cast(FeeScheduleValue, config),
        market_id=cast(str | None, market_id),
        phase=cast(VenuePhase | str | None, phase),
    )


class AMMFeeSchedule:
    """Validated slot schedule used as a ``SwapEvent`` config resolver."""

    def __init__(self, entries: Iterable[FeeScheduleEntry]) -> None:
        normalized = tuple(
            _coerce_schedule_entry(entry, index) for index, entry in enumerate(entries)
        )
        if not normalized:
            raise MaterializationError("fee schedule must contain at least one entry")
        # Overlapping entries are valid only when they have different matching
        # specificity (an exact market may override a phase wildcard).  Ties
        # would make the historical config ambiguous, so reject them.
        for index, left in enumerate(normalized):
            for right in normalized[index + 1 :]:
                if left.specificity() != right.specificity():
                    continue
                if left.market_id not in (right.market_id, None, "*") and right.market_id not in (
                    None,
                    "*",
                ):
                    continue
                if left.phase not in (right.phase, None, "*") and right.phase not in (
                    None,
                    "*",
                ):
                    continue
                left_end = float("inf") if left.end_slot is None else left.end_slot
                right_end = float("inf") if right.end_slot is None else right.end_slot
                if left.start_slot < right_end and right.start_slot < left_end:
                    raise MaterializationError("fee schedule has overlapping windows")
        self.entries = tuple(
            sorted(
                normalized,
                key=lambda item: (
                    item.start_slot,
                    item.specificity(),
                    "" if item.market_id is None else item.market_id,
                    "" if item.phase is None else str(item.phase),
                ),
            )
        )

    def resolve(self, event: SwapEvent) -> FeeConfig | None:
        matches = [entry for entry in self.entries if entry.matches(event)]
        if not matches:
            return None
        selected = max(matches, key=lambda entry: entry.specificity())
        return _coerce_fee_config(selected.config)

    def __call__(self, event: SwapEvent) -> FeeConfig | None:
        return self.resolve(event)

    def payload(self) -> dict[str, object]:
        return {"version": 1, "entries": [entry.payload() for entry in self.entries]}

    def sha256(self) -> str:
        return _sha256_bytes(_canonical_json(self.payload()))


def load_fee_schedule(path: str | Path) -> AMMFeeSchedule:
    source = Path(path)
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MaterializationError(f"fee schedule is not valid JSON: {source}") from error
    if isinstance(payload, list):
        raw_entries: object = payload
    else:
        document = _mapping(payload, "fee schedule")
        if document.get("version", 1) != 1:
            raise MaterializationError("unsupported fee schedule version")
        raw_entries = document.get("entries", document.get("schedule"))
    if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, (str, bytes, bytearray)):
        raise MaterializationError("fee schedule entries must be an array")
    return AMMFeeSchedule(cast(Iterable[FeeScheduleEntry], raw_entries))


@dataclass(frozen=True, slots=True)
class _InputIdentity:
    parquet_path: Path
    manifest_path: Path | None
    manifest_sha256: str | None
    swaps_sha256: str
    row_count: int
    manifest: Mapping[str, object] | None


class _DiskIdentityIndex:
    """SQLite-backed duplicate and market-identity index for bounded RAM."""

    def __init__(self, path: Path) -> None:
        self._connection = sqlite3.connect(path)
        self._connection.execute("PRAGMA journal_mode=DELETE")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA cache_size=-4096")
        self._connection.execute("CREATE TABLE event_ids (event_id TEXT PRIMARY KEY NOT NULL)")
        self._connection.execute(
            "CREATE TABLE market_identities ("
            "market_id TEXT PRIMARY KEY NOT NULL, mint TEXT NOT NULL, quote_mint TEXT NOT NULL)"
        )
        self._connection.commit()

    def check(self, event: SwapEvent) -> None:
        try:
            self._connection.execute(
                "INSERT INTO event_ids(event_id) VALUES (?)", (event.event_id,)
            )
        except sqlite3.IntegrityError as error:
            raise ValueError(f"duplicate event_id: {event.event_id}") from error
        prior = self._connection.execute(
            "SELECT mint, quote_mint FROM market_identities WHERE market_id = ?",
            (event.market_id,),
        ).fetchone()
        identity = (event.mint, event.quote_mint)
        if prior is not None and tuple(prior) != identity:
            raise ValueError(f"inconsistent mint/quote identity for market {event.market_id}")
        if prior is None:
            self._connection.execute(
                "INSERT INTO market_identities(market_id, mint, quote_mint) VALUES (?, ?, ?)",
                (event.market_id, event.mint, event.quote_mint),
            )

    def commit(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()


def _safe_relative(root: Path, value: object, label: str) -> Path:
    relative = Path(_text(value, label))
    if relative.is_absolute() or ".." in relative.parts:
        raise MaterializationError(f"{label} escapes its dataset")
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as error:
        raise MaterializationError(f"{label} escapes its dataset") from error
    return candidate


def _resolve_input(path: str | Path) -> _InputIdentity:
    source = Path(path).expanduser().resolve()
    if source.is_dir():
        root = source
        manifest_path = root / "manifest.json"
        parquet_path = root / "swaps.parquet"
    elif source.is_file() and source.suffix.lower() == ".parquet":
        root = source.parent
        manifest_path = root / "manifest.json"
        parquet_path = source
    else:
        raise MaterializationError(f"normalized input is not a directory or Parquet file: {source}")
    manifest: Mapping[str, object] | None = None
    manifest_sha: str | None = None
    if manifest_path.exists():
        try:
            raw_manifest = manifest_path.read_bytes()
            manifest = _mapping(json.loads(raw_manifest), "normalized manifest")
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise MaterializationError("normalized manifest is invalid JSON") from error
        if manifest.get("kind") != NORMALIZED_KIND or manifest.get("version") != 1:
            raise MaterializationError("input is not a version-1 normalized swap dataset")
        declared_order = manifest.get("physical_order")
        if declared_order is not None:
            if not isinstance(declared_order, Sequence) or isinstance(
                declared_order, (str, bytes, bytearray)
            ):
                raise MaterializationError("normalized input physical order is malformed")
            if tuple(declared_order) != PHYSICAL_ORDER:
                raise MaterializationError("normalized input physical order is not canonical")
        files = _mapping(manifest.get("files"), "normalized manifest files")
        raw_file = files.get(parquet_path.name)
        if raw_file is None:
            raw_file = files.get("swaps.parquet")
        file_meta = _mapping(raw_file, "normalized swaps file metadata")
        declared_path = file_meta.get("path")
        if declared_path is not None:
            parquet_path = _safe_relative(root, declared_path, "normalized swaps path")
        expected_size = _nonnegative_int(file_meta.get("byte_count"), "normalized swaps byte_count")
        expected_hash = _text(file_meta.get("sha256"), "normalized swaps sha256")
        if len(expected_hash) != 64:
            raise MaterializationError("normalized swaps sha256 must be SHA-256 hex")
        if (
            parquet_path.is_symlink()
            or not parquet_path.is_file()
            or parquet_path.stat().st_size != expected_size
        ):
            raise MaterializationError("normalized swaps byte count does not match manifest")
        actual_hash = sha256_file(parquet_path)
        if actual_hash != expected_hash:
            raise MaterializationError("normalized swaps hash does not match manifest")
        manifest_sha = _sha256_bytes(raw_manifest)
    if parquet_path.is_symlink() or not parquet_path.is_file():
        raise MaterializationError(f"normalized swaps Parquet is missing: {parquet_path}")
    try:
        import pyarrow.parquet as pq
    except ImportError as error:  # pragma: no cover - optional dependency
        raise MaterializationError(
            "Parquet support requires: pip install 'pumpquant[data]'"
        ) from error
    parquet = pq.ParquetFile(parquet_path)
    try:
        metadata = parquet.metadata
        row_count = 0 if metadata is None else int(metadata.num_rows)
        declared_schema_hash = None if manifest is None else manifest.get("swap_schema_sha256")
        if declared_schema_hash is not None:
            if not isinstance(declared_schema_hash, str) or len(declared_schema_hash) != 64:
                raise MaterializationError("normalized swap_schema_sha256 is invalid")
            actual_schema_hash = _sha256_bytes(parquet.schema_arrow.serialize().to_pybytes())
            if actual_schema_hash != declared_schema_hash:
                raise MaterializationError("normalized swap schema hash does not match manifest")
    finally:
        parquet.close()
    if manifest is not None:
        declared_rows = manifest.get("row_count")
        if declared_rows != row_count:
            raise MaterializationError("normalized swaps row count does not match manifest")
    return _InputIdentity(
        parquet_path=parquet_path,
        manifest_path=manifest_path if manifest_path.exists() else None,
        manifest_sha256=manifest_sha,
        swaps_sha256=sha256_file(parquet_path),
        row_count=row_count,
        manifest=manifest,
    )


def _arrow_schema() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
    except ImportError as error:  # pragma: no cover - optional dependency
        raise MaterializationError(
            "Parquet support requires: pip install 'pumpquant[data]'"
        ) from error
    snapshot = pa.schema(
        [
            pa.field("schema_version", pa.int16(), nullable=False),
            pa.field("record_type", pa.string(), nullable=False),
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("market_id", pa.string(), nullable=False),
            pa.field("mint", pa.string(), nullable=False),
            pa.field("quote_mint", pa.string(), nullable=False),
            pa.field("slot", pa.int64(), nullable=False),
            pa.field("tx_index", pa.int32(), nullable=False),
            pa.field("instruction_index", pa.int32(), nullable=False),
            pa.field("ordering_key", pa.string(), nullable=False),
            pa.field("phase", pa.string(), nullable=False),
            pa.field("block_time_ns", pa.int64(), nullable=False),
            pa.field("observed_at_ns", pa.int64(), nullable=False),
            pa.field("virtual_base_reserve", pa.float64(), nullable=False),
            pa.field("virtual_quote_reserve", pa.float64(), nullable=False),
            pa.field("real_base_reserve", pa.float64(), nullable=False),
            pa.field("real_quote_reserve", pa.float64(), nullable=False),
            pa.field("quote_status", pa.string(), nullable=False),
            pa.field("quote_reason", pa.string()),
            pa.field("unquoted_requests_json", pa.string()),
            pa.field("quote_count", pa.int32(), nullable=False),
            pa.field("spread_bps", pa.float64()),
            pa.field("config_sha256", pa.string()),
            pa.field("surface_label", pa.string(), nullable=False),
            pa.field("orderbook_label", pa.string(), nullable=False),
            pa.field("l2_label", pa.string(), nullable=False),
            pa.field("signature", pa.string()),
            pa.field("program_version", pa.string()),
            pa.field("base_decimals", pa.int16()),
            pa.field("quote_decimals", pa.int16()),
        ]
    )
    quote = pa.schema(
        [
            pa.field("schema_version", pa.int16(), nullable=False),
            pa.field("record_type", pa.string(), nullable=False),
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("market_id", pa.string(), nullable=False),
            pa.field("mint", pa.string(), nullable=False),
            pa.field("quote_mint", pa.string(), nullable=False),
            pa.field("slot", pa.int64(), nullable=False),
            pa.field("tx_index", pa.int32(), nullable=False),
            pa.field("instruction_index", pa.int32(), nullable=False),
            pa.field("ordering_key", pa.string(), nullable=False),
            pa.field("phase", pa.string(), nullable=False),
            pa.field("quote_status", pa.string(), nullable=False),
            pa.field("side", pa.string(), nullable=False),
            pa.field("requested_quote_amount", pa.float64(), nullable=False),
            pa.field("input_amount", pa.float64(), nullable=False),
            pa.field("output_amount", pa.float64(), nullable=False),
            pa.field("base_amount", pa.float64(), nullable=False),
            pa.field("quote_amount", pa.float64(), nullable=False),
            pa.field("curve_quote_amount", pa.float64(), nullable=False),
            pa.field("fee_quote_amount", pa.float64(), nullable=False),
            pa.field("fee_components_json", pa.string(), nullable=False),
            pa.field("average_price_numerator", pa.float64(), nullable=False),
            pa.field("average_price_denominator", pa.float64(), nullable=False),
            pa.field("marginal_price_before_numerator", pa.float64(), nullable=False),
            pa.field("marginal_price_before_denominator", pa.float64(), nullable=False),
            pa.field("marginal_price_after_numerator", pa.float64(), nullable=False),
            pa.field("marginal_price_after_denominator", pa.float64(), nullable=False),
            pa.field("impact_bps", pa.float64(), nullable=False),
            pa.field("math_mode", pa.string(), nullable=False),
            pa.field("target_slippage_bps", pa.float64()),
            pa.field("surface_label", pa.string(), nullable=False),
            pa.field("orderbook_label", pa.string(), nullable=False),
            pa.field("l2_label", pa.string(), nullable=False),
        ]
    )
    return snapshot, quote


def _number(value: Number) -> float:
    result = float(value)
    if not (result == result) or result in (float("inf"), float("-inf")):
        raise MaterializationError("output amount is not finite")
    return result


def _snapshot_row(snapshot: Any, config_sha256: str | None) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "record_type": "amm_state_snapshot",
        "event_id": snapshot.event_id,
        "market_id": snapshot.market_id,
        "mint": snapshot.mint,
        "quote_mint": snapshot.quote_mint,
        "slot": snapshot.slot,
        "tx_index": snapshot.tx_index,
        "instruction_index": snapshot.instruction_index,
        "ordering_key": json.dumps(list(snapshot.ordering_key), separators=(",", ":")),
        "phase": snapshot.phase.value,
        "block_time_ns": snapshot.block_time_ns,
        "observed_at_ns": snapshot.observed_at_ns,
        "virtual_base_reserve": _number(snapshot.virtual_base_reserve),
        "virtual_quote_reserve": _number(snapshot.virtual_quote_reserve),
        "real_base_reserve": _number(snapshot.real_base_reserve),
        "real_quote_reserve": _number(snapshot.real_quote_reserve),
        "quote_status": snapshot.quote_status.value,
        "quote_reason": snapshot.quote_reason,
        "unquoted_requests_json": (
            None
            if not snapshot.unquoted_requests
            else json.dumps(list(snapshot.unquoted_requests), separators=(",", ":"))
        ),
        "quote_count": len(snapshot.quotes),
        "spread_bps": snapshot.spread_bps,
        "config_sha256": config_sha256,
        "surface_label": snapshot.labels[0],
        "orderbook_label": snapshot.labels[1],
        "l2_label": snapshot.labels[2],
        "signature": snapshot.signature,
        "program_version": snapshot.program_version,
        "base_decimals": snapshot.base_decimals,
        "quote_decimals": snapshot.quote_decimals,
    }


def _quote_row(snapshot: Any, level: Any) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "record_type": "amm_quote_level",
        "event_id": snapshot.event_id,
        "market_id": snapshot.market_id,
        "mint": snapshot.mint,
        "quote_mint": snapshot.quote_mint,
        "slot": snapshot.slot,
        "tx_index": snapshot.tx_index,
        "instruction_index": snapshot.instruction_index,
        "ordering_key": json.dumps(list(snapshot.ordering_key), separators=(",", ":")),
        "phase": snapshot.phase.value,
        "quote_status": snapshot.quote_status.value,
        "side": level.side.value,
        "requested_quote_amount": _number(level.requested_quote_amount),
        "input_amount": _number(level.input_amount),
        "output_amount": _number(level.output_amount),
        "base_amount": _number(level.base_amount),
        "quote_amount": _number(level.quote_amount),
        "curve_quote_amount": _number(level.curve_quote_amount),
        "fee_quote_amount": _number(level.fee_quote_amount),
        "fee_components_json": json.dumps(
            {name: _number(amount) for name, amount in level.fee_components},
            sort_keys=True,
            separators=(",", ":"),
        ),
        "average_price_numerator": _number(level.average_price_numerator),
        "average_price_denominator": _number(level.average_price_denominator),
        "marginal_price_before_numerator": _number(level.marginal_price_before_numerator),
        "marginal_price_before_denominator": _number(level.marginal_price_before_denominator),
        "marginal_price_after_numerator": _number(level.marginal_price_after_numerator),
        "marginal_price_after_denominator": _number(level.marginal_price_after_denominator),
        "impact_bps": level.impact_bps,
        "math_mode": level.math_mode,
        "target_slippage_bps": level.target_slippage_bps,
        "surface_label": snapshot.labels[0],
        "orderbook_label": snapshot.labels[1],
        "l2_label": snapshot.labels[2],
    }


def _write_bytes(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _validate_batch_size(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise MaterializationError("batch_size must be a positive integer")


def _schedule_and_config(
    fee_config: FeeConfig | None,
    fee_schedule: (
        AMMFeeSchedule | Iterable[FeeScheduleEntry] | Mapping[str, object] | str | Path | None
    ),
) -> tuple[FeeConfig | None, AMMFeeSchedule | None, dict[str, object]]:
    if fee_config is not None and fee_schedule is not None:
        raise MaterializationError("pass fee_config or fee_schedule, not both")
    if fee_schedule is None:
        direct = _coerce_fee_config(fee_config)
        return direct, None, {"mode": "single_config", "config": _fee_payload(direct)}
    if isinstance(fee_schedule, (str, Path)):
        schedule = load_fee_schedule(fee_schedule)
    elif isinstance(fee_schedule, AMMFeeSchedule):
        schedule = fee_schedule
    elif isinstance(fee_schedule, Mapping):
        document = _mapping(fee_schedule, "fee schedule")
        if document.get("version", 1) != 1:
            raise MaterializationError("unsupported fee schedule version")
        raw_entries = document.get("entries", document.get("schedule"))
        if not isinstance(raw_entries, Sequence) or isinstance(
            raw_entries, (str, bytes, bytearray)
        ):
            raise MaterializationError("fee schedule entries must be an array")
        schedule = AMMFeeSchedule(cast(Iterable[FeeScheduleEntry], raw_entries))
    else:
        schedule = AMMFeeSchedule(fee_schedule)
    return None, schedule, {"mode": "slot_schedule", "schedule": schedule.payload()}


def materialize_amm_states(
    input_path: str | Path,
    output: str | Path,
    *,
    quote_notionals: Iterable[Number] | None = None,
    slippage_buckets_bps: Iterable[Number] | None = None,
    fee_config: FeeConfig | None = None,
    fee_schedule: (
        AMMFeeSchedule | Iterable[FeeScheduleEntry] | Mapping[str, object] | str | Path | None
    ) = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    start_slot: int | None = None,
    end_slot: int | None = None,
) -> dict[str, object]:
    """Materialize reconstructed states with bounded input/output batches.

    The target is replaced only after every input row has been validated and
    both Parquet writers, audit, and manifest have been sealed in staging.
    """

    _validate_batch_size(batch_size)
    if start_slot is not None:
        _nonnegative_int(start_slot, "start_slot")
    if end_slot is not None:
        _nonnegative_int(end_slot, "end_slot")
    if start_slot is not None and end_slot is not None and end_slot <= start_slot:
        raise MaterializationError("end_slot must exceed start_slot")
    source = _resolve_input(input_path)
    raw_target = Path(output).expanduser()
    if raw_target.is_symlink():
        raise MaterializationError("output must not be a symlink")
    target = raw_target.resolve()
    if target == source.parquet_path or target.is_file():
        raise MaterializationError("output must be a distinct directory")
    target.parent.mkdir(parents=True, exist_ok=True)
    direct_config, schedule, config_payload = _schedule_and_config(fee_config, fee_schedule)
    config_sha = _config_hash(config_payload)
    snapshot_schema, quote_schema = _arrow_schema()
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:  # pragma: no cover - optional dependency
        raise MaterializationError(
            "Parquet support requires: pip install 'pumpquant[data]'"
        ) from error

    staging = Path(
        tempfile.mkdtemp(prefix=f".{target.name}.", suffix=".partial", dir=target.parent)
    )
    published = False
    snapshot_path = staging / "snapshots.parquet"
    quote_path = staging / "quotes.parquet"
    identity_path = staging / "identity.sqlite"
    identity_index: _DiskIdentityIndex | None = None
    snapshots_written = 0
    quotes_written = 0
    status_counts: Counter[str] = Counter()
    slot_min: int | None = None
    slot_max: int | None = None
    schedule_resolver: FeeConfigResolver | None = None if schedule is None else schedule.resolve
    reconstructor = AMMStateReconstructor(
        quote_notionals=quote_notionals,
        slippage_buckets_bps=slippage_buckets_bps,
        fee_config=direct_config,
        fee_config_resolver=schedule_resolver,
        track_event_ids=False,
        track_market_identities=False,
    )
    snapshot_writer: Any = None
    quote_writer: Any = None
    try:
        identity_index = _DiskIdentityIndex(identity_path)
        snapshot_writer = pq.ParquetWriter(snapshot_path, snapshot_schema, compression="zstd")
        quote_writer = pq.ParquetWriter(quote_path, quote_schema, compression="zstd")
        for batch in iter_swap_event_batches(source.parquet_path, batch_size=batch_size):
            snapshot_rows: list[dict[str, object]] = []
            quote_rows: list[dict[str, object]] = []
            for event in batch:
                if start_slot is not None and event.slot < start_slot:
                    raise MaterializationError(
                        f"input event precedes start_slot: {event.slot} < {start_slot}"
                    )
                if end_slot is not None and event.slot >= end_slot:
                    raise MaterializationError(
                        f"input event reaches end_slot: {event.slot} >= {end_slot}"
                    )
                identity_index.check(event)
                resolved_config = direct_config if schedule is None else schedule.resolve(event)
                config_hash = None if resolved_config is None else _config_hash(resolved_config)
                snapshot = next(reconstructor.consume((event,)))
                snapshot_rows.append(_snapshot_row(snapshot, config_hash))
                quote_rows.extend(_quote_row(snapshot, level) for level in snapshot.quotes)
                snapshots_written += 1
                quotes_written += len(snapshot.quotes)
                status_counts[snapshot.quote_status.value] += 1
                slot_min = event.slot if slot_min is None else min(slot_min, event.slot)
                slot_max = event.slot if slot_max is None else max(slot_max, event.slot)
            if snapshot_rows:
                snapshot_writer.write_table(
                    pa.Table.from_pylist(snapshot_rows, schema=snapshot_schema)
                )
            if quote_rows:
                quote_writer.write_table(pa.Table.from_pylist(quote_rows, schema=quote_schema))
            identity_index.commit()
        if snapshots_written != source.row_count:
            raise MaterializationError(
                "streamed normalized row count differs from input manifest metadata"
            )
        snapshot_writer.close()
        quote_writer.close()
        snapshot_writer = None
        quote_writer = None
        identity_index.close()
        identity_index = None
        identity_path.unlink(missing_ok=True)
        files: dict[str, dict[str, object]] = {}
        for path in (snapshot_path, quote_path):
            files[path.name] = {
                "byte_count": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        audit_payload: dict[str, object] = {
            "kind": AUDIT_KIND,
            "version": SCHEMA_VERSION,
            "input_row_count": source.row_count,
            "snapshot_row_count": snapshots_written,
            "quote_row_count": quotes_written,
            "status_counts": dict(sorted(status_counts.items())),
            "slot_min": slot_min,
            "slot_max": slot_max,
            "physical_order": list(PHYSICAL_ORDER),
            "input_read_mode": "streaming_parquet_batches",
            "reconstruction": "post_event_amm_state",
            "config_sha256": config_sha,
            "config": config_payload,
            "fee_source": "explicit_schedule_or_none",
            "config_availability": "slot_bounded",
            "labels": list(SURFACE_LABELS),
            "not_true_l2": True,
        }
        audit_body = _canonical_json(audit_payload)
        _write_bytes(staging / "audit.json", audit_body)
        files["audit.json"] = {
            "byte_count": len(audit_body),
            "sha256": _sha256_bytes(audit_body),
        }
        manifest_payload: dict[str, object] = {
            "kind": MATERIALIZED_KIND,
            "version": SCHEMA_VERSION,
            "input": {
                "path": str(source.parquet_path),
                "manifest_path": (
                    None if source.manifest_path is None else str(source.manifest_path)
                ),
                "manifest_sha256": source.manifest_sha256,
                "swaps_sha256": source.swaps_sha256,
                "row_count": source.row_count,
            },
            "row_count": snapshots_written,
            "quote_row_count": quotes_written,
            "snapshot_schema_sha256": _sha256_bytes(snapshot_schema.serialize().to_pybytes()),
            "quote_schema_sha256": _sha256_bytes(quote_schema.serialize().to_pybytes()),
            "physical_order": list(PHYSICAL_ORDER),
            "config_sha256": config_sha,
            "config": config_payload,
            "labels": list(SURFACE_LABELS),
            "reconstruction": "post_event_amm_state",
            "orderbook_observed": False,
            "true_l2": False,
            "audit_path": "audit.json",
            "audit_sha256": files["audit.json"]["sha256"],
            "files": files,
        }
        _write_bytes(staging / "manifest.json", _canonical_json(manifest_payload))
        directory_fd = os.open(staging, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        previous: Path | None = None
        if target.exists():
            previous = Path(
                tempfile.mkdtemp(
                    prefix=f".{target.name}.previous.",
                    suffix=".partial",
                    dir=target.parent,
                )
            )
            previous.rmdir()
            try:
                os.replace(target, previous)
            except BaseException:
                previous.rmdir()
                raise
        try:
            os.replace(staging, target)
        except BaseException:
            if previous is not None and not target.exists():
                os.replace(previous, target)
            raise
        published = True
        if previous is not None:
            shutil.rmtree(previous, ignore_errors=True)
        parent_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        return manifest_payload
    finally:
        if snapshot_writer is not None:
            snapshot_writer.close()
        if quote_writer is not None:
            quote_writer.close()
        if identity_index is not None:
            identity_index.close()
        if not published and staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


# Friendly aliases used by scripts and callers that prefer a short verb.
materialize = materialize_amm_states
materialize_amm_state = materialize_amm_states
materialize_amm_state_dataset = materialize_amm_states
run = materialize_amm_states


__all__ = [
    "AMMFeeSchedule",
    "AUDIT_KIND",
    "FeeScheduleEntry",
    "MATERIALIZED_KIND",
    "MaterializationError",
    "SCHEMA_VERSION",
    "load_fee_schedule",
    "materialize",
    "materialize_amm_state",
    "materialize_amm_state_dataset",
    "materialize_amm_states",
    "run",
]
