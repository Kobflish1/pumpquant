"""Read-only audit of a sealed signature inventory against full-block archives.

The full-block archive deliberately keeps the exact JSON-RPC response body for
every request attempt.  This module consumes those bytes without changing the
archive: segment manifests and all files are verified with
``pumpquant.block_archive.verify_block_segment`` before a wire body is read.
The successful (last) response for a slot is decompressed when it is gzip
encoded, decoded as JSON, and its ``result.transactions`` array is walked in
the order returned by Solana.  A serialized transaction starts with a Solana
shortvec signature count followed by 64-byte signatures; the first signature
is converted to canonical base58 and compared with a sealed Parquet inventory.

Only one wire body and one Arrow record batch are live at a time.  The
inventory and the set of archived signatures are retained (roughly one string
per transaction), while raw block payloads are never accumulated in memory.
The report contains sorted values and canonical JSON serialization so rerunning
an audit over the same immutable inputs is byte deterministic.
"""

from __future__ import annotations

import base64
import binascii
import gzip
import hashlib
import json
import zlib
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from pumpquant.acquisition.block_archive import verify_block_segment
from pumpquant.acquisition.signature_inventory import (
    inventory_schema_sha256,
    is_valid_signature,
    load_signature_inventory_manifest,
    verify_signature_inventory,
)

try:  # pyarrow is optional for the rest of the package.
    import pyarrow.parquet as _pq
except ImportError:  # pragma: no cover - exercised in minimal installs
    _pq = None


_BASE58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BLOCK_KIND = "pumpquant.full_block_segment"
_AUDIT_KIND = "pumpquant.block_inventory_audit"
_SCHEMA_VERSION = 1


class BlockInventoryAuditError(RuntimeError):
    """Raised when a sealed inventory or block archive cannot be audited."""


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _base58_encode(value: bytes) -> str:
    """Encode bytes using Solana's canonical Bitcoin-base58 alphabet."""

    if not value:
        return ""
    number = int.from_bytes(value, "big")
    chars: list[str] = []
    while number:
        number, remainder = divmod(number, 58)
        chars.append(chr(_BASE58_ALPHABET[remainder]))
    leading = len(value) - len(value.lstrip(b"\x00"))
    return "1" * leading + "".join(reversed(chars))


def _slot(value: object, label: str = "slot") -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BlockInventoryAuditError(f"{label} must be a non-negative integer")
    return value


def _safe_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BlockInventoryAuditError(f"{label} must be a non-negative integer")
    return value


def _shortvec_count(payload: bytes) -> tuple[int, int]:
    """Return ``(signature_count, bytes_consumed)`` from a Solana shortvec.

    Solana's compact-u16 is a little-endian base-128 integer.  Transaction
    signatures are bounded by a u16 and therefore accepting more than three
    bytes is malformed (the decoder still guards against an overlong varint so
    an arbitrary payload cannot cause unbounded work).
    """

    value = 0
    shift = 0
    for index, byte in enumerate(payload[:5]):
        value |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            if index > 0 and value < (1 << (7 * index)):
                raise ValueError("noncanonical Solana shortvec signature count")
            if value > 0xFFFF:
                raise ValueError("shortvec signature count exceeds u16")
            return value, index + 1
        shift += 7
    raise ValueError("unterminated Solana shortvec signature count")


def _first_signature(serialized: bytes) -> str:
    count, offset = _shortvec_count(serialized)
    if count <= 0:
        raise ValueError("serialized transaction has no signatures")
    end = offset + count * 64
    if len(serialized) < end:
        raise ValueError("serialized transaction is shorter than its signatures")
    return _base58_encode(serialized[offset : offset + 64])


def _decompress_wire(payload: bytes) -> bytes:
    # ``block_archive`` stores the exact HTTP body and intentionally does not
    # persist provider headers.  gzip magic is therefore the only reliable
    # local indicator that decompression is required.
    if payload.startswith(b"\x1f\x8b"):
        return gzip.decompress(payload)
    return payload


def _json_object(payload: bytes, description: str) -> dict[str, object]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BlockInventoryAuditError(f"{description} is not valid UTF-8 JSON") from error
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise BlockInventoryAuditError(f"{description} must be a JSON object")
    return cast(dict[str, object], value)


def _manifest_candidates(root_value: str | Path) -> list[Path]:
    root = Path(root_value).expanduser()
    if root.is_symlink():
        raise BlockInventoryAuditError(f"archive root must not be a symlink: {root}")
    if not root.exists():
        raise FileNotFoundError(root)
    if root.is_file():
        if root.name != "manifest.json":
            raise BlockInventoryAuditError(f"archive file is not manifest.json: {root}")
        return [root]
    if not root.is_dir():
        raise BlockInventoryAuditError(f"archive root is not a directory: {root}")
    # ``rglob`` allows callers to provide a day root, a parent containing many
    # day roots, or individual segment directories.  Non-block manifests (for
    # example a signature inventory manifest) are filtered after reading JSON.
    return sorted(path for path in root.rglob("manifest.json") if path.is_file())


@dataclass(frozen=True, slots=True)
class _Segment:
    path: Path
    payload: Mapping[str, object]
    manifest_sha256: str

    @property
    def segment_id(self) -> str:
        value = self.payload.get("segment_id")
        return value if isinstance(value, str) else self.manifest_sha256


def _load_segments(roots: Sequence[str | Path] | str | Path) -> tuple[_Segment, ...]:
    if isinstance(roots, (str, Path)):
        roots = (roots,)
    if not roots:
        raise ValueError("at least one block archive root is required")
    candidates: set[Path] = set()
    for root in roots:
        candidates.update(path.resolve() for path in _manifest_candidates(root))

    segments: list[_Segment] = []
    by_id: dict[str, str] = {}
    by_hash: set[str] = set()
    for manifest_path in sorted(candidates):
        try:
            payload_bytes = manifest_path.read_bytes()
            payload = json.loads(payload_bytes)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BlockInventoryAuditError(
                f"block segment manifest is unreadable: {manifest_path}"
            ) from error
        if not isinstance(payload, Mapping) or payload.get("kind") != _BLOCK_KIND:
            continue
        if payload.get("schema_version") != _SCHEMA_VERSION:
            raise BlockInventoryAuditError(
                f"unsupported block segment manifest schema: {manifest_path}"
            )
        # Existing block_archive verification checks the manifest's complete
        # file inventory and every checksum before any body is consumed.
        try:
            verify_block_segment(manifest_path.parent)
        except (OSError, ValueError) as error:
            raise BlockInventoryAuditError(
                f"block segment verification failed: {manifest_path.parent}: {error}"
            ) from error
        manifest_hash = _sha256_bytes(payload_bytes)
        segment_id = payload.get("segment_id")
        if not isinstance(segment_id, str) or not segment_id:
            raise BlockInventoryAuditError(f"block segment has no segment_id: {manifest_path}")
        prior_hash = by_id.get(segment_id)
        if prior_hash is not None and prior_hash != manifest_hash:
            raise BlockInventoryAuditError(f"conflicting manifests share segment_id {segment_id}")
        by_id[segment_id] = manifest_hash
        if manifest_hash in by_hash:
            continue
        by_hash.add(manifest_hash)
        segments.append(
            _Segment(manifest_path.parent, cast(Mapping[str, object], payload), manifest_hash)
        )

    segments.sort(
        key=lambda item: (
            _safe_int(item.payload.get("min_slot"), "segment min_slot"),
            _safe_int(item.payload.get("max_slot"), "segment max_slot"),
            item.segment_id,
            item.manifest_sha256,
        )
    )
    return tuple(segments)


def _inventory_location(value: str | Path) -> tuple[Path, Path]:
    path = Path(value).expanduser()
    if path.is_symlink():
        raise BlockInventoryAuditError("signature inventory path must not be a symlink")
    if path.is_dir():
        manifest_path = path / "manifest.json"
    elif path.is_file() and path.name == "manifest.json":
        manifest_path = path
    elif path.is_file() and path.suffix == ".parquet":
        # A sealed inventory is normally addressed by its directory, but
        # accepting the Parquet path is convenient when the sibling manifest
        # is still present and lets verification bind the exact file bytes.
        manifest_path = path.parent / "manifest.json"
    else:
        raise BlockInventoryAuditError(
            "signature inventory must be a directory or its manifest.json path"
        )
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)
    return manifest_path, manifest_path.parent


def _load_inventory(value: str | Path, *, batch_size: int) -> tuple[set[str], Any]:
    if _pq is None:  # pragma: no cover - optional archive dependency
        raise RuntimeError("block inventory audit requires: pip install 'pumpquant[archive]'")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    manifest_path, root = _inventory_location(value)
    try:
        manifest = load_signature_inventory_manifest(manifest_path)
        verify_signature_inventory(manifest_path, root)
        if manifest.inventory_schema_sha256 != inventory_schema_sha256():
            raise BlockInventoryAuditError("signature inventory schema hash is unsupported")
    except Exception as error:  # archive and pyarrow errors vary by optional version
        raise BlockInventoryAuditError(
            f"signature inventory verification failed: {error}"
        ) from error
    inventory_path = root / manifest.inventory_path
    signatures: set[str] = set()
    try:
        parquet = _pq.ParquetFile(inventory_path)
    except Exception as error:  # pyarrow exposes version-specific exception types
        raise BlockInventoryAuditError(
            f"signature inventory Parquet is unreadable: {inventory_path}"
        ) from error
    try:
        try:
            for batch in parquet.iter_batches(
                batch_size=batch_size, columns=["signature"], use_threads=False
            ):
                for value in batch.column(0).to_pylist():
                    if not isinstance(value, str) or not is_valid_signature(value):
                        raise BlockInventoryAuditError(
                            "signature inventory contains a malformed signature"
                        )
                    signatures.add(value)
        except BlockInventoryAuditError:
            raise
        except Exception as error:  # pyarrow exposes version-specific exception types
            raise BlockInventoryAuditError(
                f"signature inventory Parquet scan failed: {inventory_path}"
            ) from error
    finally:
        close = getattr(parquet, "close", None)
        if callable(close):
            close()
    return signatures, manifest


@dataclass(slots=True)
class _SlotStats:
    slot: int
    segment_id: str
    segment_path: str
    status: str
    error_kind: str | None = None
    wire_attempt_count: int = 0
    transaction_count: int = 0
    meta_present_count: int = 0
    meta_null_count: int = 0
    null_result_count: int = 0
    rpc_error_count: int = 0
    wire_decode_error_count: int = 0
    transaction_decode_error_count: int = 0
    final_body_sha256: str | None = None
    blockhash: str | None = None
    signatures: set[str] | None = None
    duplicate_signatures: set[str] | None = None
    transaction_signature_order_sha256: str | None = None
    transaction_body_order_sha256: str | None = None
    record_count: int = 1
    duplicate_record_count: int = 0
    material_conflict: bool = False
    valid_found_record: bool = False

    def __post_init__(self) -> None:
        self.signatures = set()
        self.duplicate_signatures = set()

    def payload(self) -> dict[str, object]:
        return {
            "slot": self.slot,
            "segment_id": self.segment_id,
            "segment_path": self.segment_path,
            "status": self.status,
            "error_kind": self.error_kind,
            "wire_attempt_count": self.wire_attempt_count,
            "transaction_count": self.transaction_count,
            "tx_count": self.transaction_count,
            "meta_present_count": self.meta_present_count,
            "meta_count": self.meta_present_count,
            "meta_null_count": self.meta_null_count,
            "null_result_count": self.null_result_count,
            "rpc_error_count": self.rpc_error_count,
            "wire_decode_error_count": self.wire_decode_error_count,
            "transaction_decode_error_count": self.transaction_decode_error_count,
            "error_count": int(
                self.status in {"rpc_error", "transport_error", "protocol_error"}
                or self.wire_decode_error_count > 0
                or self.transaction_decode_error_count > 0
            ),
            "blockhash": self.blockhash,
            "unique_transaction_signature_count": len(self.signatures or set()),
            "duplicate_transaction_signature_count": len(self.duplicate_signatures or set()),
            "transaction_signature_order_sha256": (self.transaction_signature_order_sha256),
            "transaction_body_order_sha256": self.transaction_body_order_sha256,
            "record_count": self.record_count,
            "duplicate_record_count": self.duplicate_record_count,
            "material_conflict": self.material_conflict,
            "valid_found": self.valid_found_record,
        }


def _wire_path(segment: Path, value: object) -> Path:
    if not isinstance(value, str) or not value:
        raise BlockInventoryAuditError("wire file entry has no path")
    relative = Path(value)
    if (
        relative.is_absolute()
        or not relative.parts
        or relative.parts[0] != "wire"
        or ".." in relative.parts
    ):
        raise BlockInventoryAuditError("wire file path is outside the wire directory")
    unresolved = segment / value
    if unresolved.is_symlink():
        raise BlockInventoryAuditError("wire file path must not be a symlink")
    candidate = unresolved.resolve()
    try:
        candidate.relative_to(segment.resolve())
    except ValueError as error:
        raise BlockInventoryAuditError("wire file path escapes its segment") from error
    if candidate.is_symlink() or not candidate.is_file():
        raise FileNotFoundError(candidate)
    return candidate


def _last_wire_body(segment: Path, raw: Mapping[str, object]) -> tuple[bytes | None, int]:
    entries = raw.get("wire_files", [])
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes, bytearray)):
        raise BlockInventoryAuditError("coverage wire_files must be a sequence")
    if not entries:
        return None, 0
    body: bytes | None = None
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise BlockInventoryAuditError("coverage wire file entry must be an object")
        body = _wire_path(segment, entry.get("path")).read_bytes()
    return body, len(entries)


def _transaction_values(result: Mapping[str, object]) -> list[Mapping[str, object]]:
    transactions = result.get("transactions")
    if not isinstance(transactions, list):
        raise BlockInventoryAuditError("getBlock result has no transactions array")
    values: list[Mapping[str, object]] = []
    for value in transactions:
        if not isinstance(value, Mapping):
            raise BlockInventoryAuditError("getBlock transaction entry is not an object")
        values.append(value)
    return values


def _decode_transaction(value: Mapping[str, object]) -> tuple[str, str]:
    encoded = value.get("transaction")
    if not isinstance(encoded, Sequence) or isinstance(encoded, (str, bytes, bytearray)):
        raise ValueError("transaction is not a base64 tuple")
    if len(encoded) != 2 or encoded[1] != "base64" or not isinstance(encoded[0], str):
        raise ValueError("transaction is not a base64 tuple")
    try:
        serialized = base64.b64decode(encoded[0], validate=True)
    except (ValueError, TypeError, binascii.Error) as error:
        raise ValueError("transaction base64 is invalid") from error
    signature = _first_signature(serialized)
    return signature, _sha256_bytes(serialized)


def _read_coverage(segment: _Segment) -> Iterable[Mapping[str, object]]:
    path = segment.path / "coverage.ndjson"
    try:
        handle = path.open("r", encoding="utf-8")
    except OSError as error:
        raise BlockInventoryAuditError(f"coverage file is unreadable: {path}") from error
    with handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                value = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise BlockInventoryAuditError(
                    f"coverage record {path}:{line_number} is invalid JSON"
                ) from error
            if not isinstance(value, Mapping):
                raise BlockInventoryAuditError(
                    f"coverage record {path}:{line_number} is not an object"
                )
            yield value


@dataclass(frozen=True, slots=True)
class BlockInventoryAuditResult(Mapping[str, object]):
    """Deterministic audit result with mapping and JSON convenience methods."""

    payload: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(json.dumps(self.payload, sort_keys=True)))

    as_dict = to_dict

    def to_json(self, *, indent: int | None = None) -> str:
        return json.dumps(self.payload, sort_keys=True, separators=(",", ":"), indent=indent) + "\n"

    def __getitem__(self, key: str) -> object:
        return self.payload[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.payload)

    def __len__(self) -> int:
        return len(self.payload)

    def get(self, key: str, default: object = None) -> object:
        return self.payload.get(key, default)


def audit_block_inventory(
    inventory: str | Path,
    archive_roots: Sequence[str | Path] | str | Path,
    *,
    batch_size: int = 4_096,
    include_signature_details: bool = False,
    signature_sample_size: int = 16,
) -> BlockInventoryAuditResult:
    """Audit a sealed signature inventory against recursively discovered blocks."""

    if not isinstance(include_signature_details, bool):
        raise TypeError("include_signature_details must be boolean")
    if (
        isinstance(signature_sample_size, bool)
        or not isinstance(signature_sample_size, int)
        or signature_sample_size < 0
    ):
        raise ValueError("signature_sample_size must be a non-negative integer")
    inventory_signatures, inventory_manifest = _load_inventory(inventory, batch_size=batch_size)
    segments = _load_segments(archive_roots)

    slot_records: list[_SlotStats] = []
    segment_payloads: list[dict[str, object]] = []

    for segment in segments:
        raw_counts = segment.payload.get("counts")
        manifest_counts = (
            {
                key: value
                for key, value in raw_counts.items()
                if isinstance(key, str) and isinstance(value, int)
            }
            if isinstance(raw_counts, Mapping)
            else {}
        )
        segment_payloads.append(
            {
                "segment_id": segment.segment_id,
                "segment_path": str(segment.path),
                "manifest_sha256": segment.manifest_sha256,
                "min_slot": _safe_int(segment.payload.get("min_slot"), "segment min_slot"),
                "max_slot": _safe_int(segment.payload.get("max_slot"), "segment max_slot"),
                "slot_count": _safe_int(segment.payload.get("slot_count"), "segment slot_count"),
                "counts": dict(sorted(manifest_counts.items())),
            }
        )
        segment_slots: list[int] = []
        for raw in _read_coverage(segment):
            slot = _slot(raw.get("slot"))
            segment_slots.append(slot)
            status = raw.get("status")
            if not isinstance(status, str) or not status:
                raise BlockInventoryAuditError(f"coverage record for slot {slot} has no status")
            raw_error_kind = raw.get("error_kind")
            error_kind = raw_error_kind if isinstance(raw_error_kind, str) else None
            stats = _SlotStats(
                slot,
                segment.segment_id,
                str(segment.path),
                status,
                error_kind,
            )
            body, attempts = _last_wire_body(segment.path, raw)
            stats.wire_attempt_count = attempts
            if body is None:
                if status == "missing":
                    stats.null_result_count = 1
                elif status in {"rpc_error", "transport_error", "protocol_error"}:
                    stats.rpc_error_count = 1
                slot_records.append(stats)
                continue
            stats.final_body_sha256 = _sha256_bytes(body)
            try:
                decoded_body = _decompress_wire(body)
                response = _json_object(decoded_body, f"wire body for slot {slot}")
            except (OSError, EOFError, ValueError, zlib.error, BlockInventoryAuditError) as error:
                stats.wire_decode_error_count = 1
                stats.error_kind = stats.error_kind or type(error).__name__.casefold()
                slot_records.append(stats)
                continue

            if response.get("jsonrpc") != "2.0" or response.get("id") != f"block-{slot}":
                stats.wire_decode_error_count = 1
                stats.error_kind = stats.error_kind or "invalid_json_rpc_identity"
                slot_records.append(stats)
                continue
            if ("result" in response) == ("error" in response):
                stats.wire_decode_error_count = 1
                stats.error_kind = stats.error_kind or "invalid_json_rpc_shape"
                slot_records.append(stats)
                continue
            if "error" in response:
                stats.rpc_error_count = 1
                slot_records.append(stats)
                continue
            result = response.get("result")
            if result is None:
                stats.null_result_count = 1
                slot_records.append(stats)
                continue
            if not isinstance(result, Mapping):
                stats.wire_decode_error_count = 1
                stats.error_kind = stats.error_kind or "non_object_result"
                slot_records.append(stats)
                continue
            blockhash = result.get("blockhash")
            if not isinstance(blockhash, str) or not blockhash:
                stats.wire_decode_error_count = 1
                stats.error_kind = stats.error_kind or "missing_blockhash"
                slot_records.append(stats)
                continue
            stats.blockhash = blockhash
            try:
                transactions = _transaction_values(result)
            except BlockInventoryAuditError as error:
                stats.wire_decode_error_count = 1
                stats.error_kind = stats.error_kind or str(error)
                slot_records.append(stats)
                continue

            stats.transaction_count = len(transactions)
            signature_order = hashlib.sha256()
            transaction_body_order = hashlib.sha256()
            for transaction in transactions:
                if transaction.get("meta") is None:
                    stats.meta_null_count += 1
                else:
                    stats.meta_present_count += 1
                try:
                    signature, body_hash = _decode_transaction(transaction)
                except ValueError:
                    stats.transaction_decode_error_count += 1
                    continue
                assert stats.signatures is not None
                assert stats.duplicate_signatures is not None
                if signature in stats.signatures:
                    stats.duplicate_signatures.add(signature)
                stats.signatures.add(signature)
                signature_order.update(signature.encode("ascii"))
                signature_order.update(b"\n")
                transaction_body_order.update(bytes.fromhex(body_hash))
            stats.transaction_signature_order_sha256 = signature_order.hexdigest()
            stats.transaction_body_order_sha256 = transaction_body_order.hexdigest()
            slot_records.append(stats)
        expected_slot_count = _safe_int(segment.payload.get("slot_count"), "segment slot_count")
        if len(segment_slots) != expected_slot_count:
            raise BlockInventoryAuditError(
                f"coverage slot count differs from segment manifest: {segment.path}"
            )
        if segment_slots and (
            min(segment_slots) != _safe_int(segment.payload.get("min_slot"), "segment min_slot")
            or max(segment_slots) != _safe_int(segment.payload.get("max_slot"), "segment max_slot")
            or len(set(segment_slots)) != len(segment_slots)
        ):
            raise BlockInventoryAuditError(
                f"coverage slot range or uniqueness differs from segment manifest: {segment.path}"
            )

    slot_records.sort(key=lambda value: (value.slot, value.segment_id, value.segment_path))
    by_slot: defaultdict[int, list[_SlotStats]] = defaultdict(list)
    for value in slot_records:
        by_slot[value.slot].append(value)

    def valid_found(value: _SlotStats) -> bool:
        return (
            value.status == "found"
            and value.wire_decode_error_count == 0
            and value.rpc_error_count == 0
            and value.null_result_count == 0
            and value.transaction_decode_error_count == 0
            and value.final_body_sha256 is not None
            and value.blockhash is not None
            and value.transaction_signature_order_sha256 is not None
            and value.transaction_body_order_sha256 is not None
        )

    def found_fingerprint(value: _SlotStats) -> tuple[object, ...]:
        return (
            value.blockhash,
            value.transaction_count,
            value.transaction_signature_order_sha256,
            value.transaction_body_order_sha256,
        )

    # A retry can leave a missing/error/skipped record in one segment and a
    # found record in another.  Reconcile those records before computing the
    # canonical archive set.  Among multiple valid found records, deterministic
    # segment ordering chooses the first one; a conflict is only material when
    # the blockhash or transaction-signature set differs.  Identical repeated
    # blocks are therefore harmless duplicates, not conflicts.
    canonical_records: list[_SlotStats] = []
    slot_conflict_count = 0
    slot_duplicate_count = 0
    physical_found_occurrences = 0
    repeated_signatures: set[str] = set()
    conflicting_signatures: set[str] = set()
    for slot in sorted(by_slot):
        values = by_slot[slot]
        found = [value for value in values if valid_found(value)]
        canonical = found[0] if found else values[0]
        physical_found_occurrences += sum(value.transaction_count for value in found)
        for value in found:
            repeated_signatures.update(value.duplicate_signatures or set())
            conflicting_signatures.update(value.duplicate_signatures or set())
        if len(values) > 1:
            slot_duplicate_count += len(values) - 1
        if len(found) > 1:
            fingerprints = {found_fingerprint(item) for item in found}
            prior_signatures: set[str] = set()
            for item in found:
                item_signatures = item.signatures or set()
                overlap = prior_signatures & item_signatures
                repeated_signatures.update(overlap)
                if found_fingerprint(item) != found_fingerprint(found[0]):
                    conflicting_signatures.update(overlap)
                prior_signatures.update(item_signatures)
            if len(fingerprints) > 1:
                slot_conflict_count += 1
                canonical.error_kind = canonical.error_kind or "conflicting_found_records"
        canonical.record_count = len(values)
        canonical.duplicate_record_count = max(0, len(values) - 1)
        canonical.material_conflict = bool(
            len(found) > 1 and len({found_fingerprint(item) for item in found}) > 1
        )
        canonical.valid_found_record = valid_found(canonical)
        # Keep the canonical record object and attach reconciliation metadata
        # for aggregation below.  ``_SlotStats.payload`` remains the compact
        # stable shape used by callers that only need per-slot counts.
        canonical_records.append(canonical)

    # Keep one canonical signature set. Physical retry duplicates are counted
    # per slot above without retaining global per-signature dictionaries of
    # slots and serialized-body sets, which matters for million-tx hours.
    archived_signatures: set[str] = set()
    for record in canonical_records:
        if not valid_found(record):
            continue
        for signature in record.signatures or set():
            if signature in archived_signatures:
                repeated_signatures.add(signature)
                conflicting_signatures.add(signature)
            archived_signatures.add(signature)

    archived_occurrences = physical_found_occurrences
    duplicate_occurrences = max(0, archived_occurrences - len(archived_signatures))
    duplicate_signature_count = len(repeated_signatures)
    conflict_signature_count = len(conflicting_signatures)

    status_counts = Counter(value.status for value in canonical_records)
    totals: dict[str, object] = {
        "slot_count": len(canonical_records),
        "found_slot_count": status_counts.get("found", 0),
        "valid_found_slot_count": sum(1 for value in canonical_records if value.valid_found_record),
        "skipped_slot_count": status_counts.get("skipped", 0),
        "missing_slot_count": status_counts.get("missing", 0),
        "rpc_error_slot_count": status_counts.get("rpc_error", 0),
        "transport_error_slot_count": status_counts.get("transport_error", 0),
        "protocol_error_slot_count": status_counts.get("protocol_error", 0),
        "transaction_count": sum(value.transaction_count for value in canonical_records),
        "meta_present_count": sum(value.meta_present_count for value in canonical_records),
        "meta_null_count": sum(value.meta_null_count for value in canonical_records),
        "null_result_slot_count": sum(value.null_result_count for value in canonical_records),
        "rpc_error_count": sum(value.rpc_error_count for value in canonical_records),
        "wire_decode_error_count": sum(
            value.wire_decode_error_count for value in canonical_records
        ),
        "transaction_decode_error_count": sum(
            value.transaction_decode_error_count for value in canonical_records
        ),
        "error_count": sum(
            int(
                value.status in {"rpc_error", "transport_error", "protocol_error"}
                or value.wire_decode_error_count > 0
                or value.transaction_decode_error_count > 0
            )
            for value in canonical_records
        ),
    }
    totals["tx_count"] = totals["transaction_count"]
    totals["meta_count"] = totals["meta_present_count"]
    intersection = sorted(inventory_signatures & archived_signatures)
    missing = sorted(inventory_signatures - archived_signatures)
    extra = sorted(archived_signatures - inventory_signatures)

    def value_hash(values: Sequence[str]) -> str:
        return _sha256_bytes("\n".join(values).encode("ascii"))

    def value_sample(values: Sequence[str]) -> list[str]:
        if signature_sample_size == 0:
            return []
        if len(values) <= signature_sample_size * 2:
            return list(values)
        return list(values[:signature_sample_size]) + list(values[-signature_sample_size:])

    intersection_sample = value_sample(intersection)
    missing_sample = value_sample(missing)
    extra_sample = value_sample(extra)
    archived_sorted = sorted(archived_signatures)
    slot_payloads = [value.payload() for value in canonical_records]
    slot_payload_by_slot = {str(value.slot): value.payload() for value in canonical_records}
    found_slots = sorted(value.slot for value in canonical_records if valid_found(value))
    found_slots_sha256 = _sha256_bytes("\n".join(map(str, found_slots)).encode("ascii"))
    found_slot_payloads = [
        {
            "slot": value.slot,
            "blockhash": value.blockhash,
            "transaction_signature_sha256": _sha256_bytes(
                "\n".join(sorted(value.signatures or set())).encode("ascii")
            ),
            "transaction_signature_order_sha256": (value.transaction_signature_order_sha256),
            "transaction_body_order_sha256": value.transaction_body_order_sha256,
        }
        for value in canonical_records
        if valid_found(value)
    ]
    found_slot_blockhashes_sha256 = _sha256_bytes(
        "\n".join(
            f"{value.slot}\t{value.blockhash or ''}"
            for value in canonical_records
            if valid_found(value)
        ).encode("utf-8")
    )
    duplicate_payload = {
        "archived_occurrence_count": duplicate_occurrences,
        "archived_signature_count": duplicate_signature_count,
        "archived_signature_duplicate_count": duplicate_signature_count,
        "slot_count": slot_duplicate_count,
        "slot_duplicate_count": slot_duplicate_count,
        "inventory_row_count": inventory_manifest.inventory_row_count - len(inventory_signatures),
        "inventory_duplicate_count": inventory_manifest.inventory_row_count
        - len(inventory_signatures),
    }
    conflict_payload = {
        "archived_signature_count": conflict_signature_count,
        "signature_count": conflict_signature_count,
        "slot_count": slot_conflict_count,
        "slot_conflict_count": slot_conflict_count,
    }
    payload: dict[str, object] = {
        "version": _SCHEMA_VERSION,
        "kind": _AUDIT_KIND,
        "inventory": {
            "manifest_path": str(_inventory_location(inventory)[0].resolve()),
            "manifest_sha256": inventory_manifest.manifest_sha256,
            "inventory_path": inventory_manifest.inventory_path,
            "row_count": inventory_manifest.inventory_row_count,
            "unique_signature_count": len(inventory_signatures),
        },
        "archive_roots": sorted(
            {str(Path(root).expanduser().resolve()) for root in (archive_roots,)}
        )
        if isinstance(archive_roots, (str, Path))
        else sorted({str(Path(root).expanduser().resolve()) for root in archive_roots}),
        "segments": sorted(
            segment_payloads, key=lambda value: (value["min_slot"], value["segment_id"])
        ),
        "segment_count": len(segment_payloads),
        "found_slots": found_slots,
        "found_slots_sha256": found_slots_sha256,
        "found_slot_blockhashes_sha256": found_slot_blockhashes_sha256,
        "found_slot_hashes": found_slot_payloads,
        "signatures": {
            "inventory_count": len(inventory_signatures),
            "inventory_sha256": value_hash(sorted(inventory_signatures)),
            "archived_unique_count": len(archived_signatures),
            "archived_unique_sha256": value_hash(archived_sorted),
            "archived_occurrence_count": archived_occurrences,
            "intersection_count": len(intersection),
            "missing_count": len(missing),
            "extra_count": len(extra),
            "intersection_sha256": value_hash(intersection),
            "missing_sha256": value_hash(missing),
            "extra_sha256": value_hash(extra),
            "intersection": intersection_sample,
            "missing": missing_sample,
            "extra": extra_sample,
            "intersection_sample": intersection_sample,
            "missing_sample": missing_sample,
            "extra_sample": extra_sample,
            "signature_details_complete": include_signature_details,
        },
        "inventory_signature_count": len(inventory_signatures),
        "inventory_unique_signature_count": len(inventory_signatures),
        "inventory_signatures_sha256": value_hash(sorted(inventory_signatures)),
        "archived_signature_count": len(archived_signatures),
        "archived_unique_signature_count": len(archived_signatures),
        "archived_signatures_sha256": value_hash(archived_sorted),
        "intersection_count": len(intersection),
        "inventory_intersection_count": len(intersection),
        "missing_count": len(missing),
        "inventory_missing_count": len(missing),
        "extra_count": len(extra),
        "inventory_extra_count": len(extra),
        "intersection_sha256": value_hash(intersection),
        "missing_sha256": value_hash(missing),
        "extra_sha256": value_hash(extra),
        "intersection_signatures": (
            intersection if include_signature_details else intersection_sample
        ),
        "missing_signatures": missing if include_signature_details else missing_sample,
        "extra_signatures": extra if include_signature_details else extra_sample,
        "duplicates": duplicate_payload,
        "conflicts": conflict_payload,
        "duplicate_count": duplicate_occurrences,
        "conflict_count": conflict_signature_count + slot_conflict_count,
        "totals": totals,
        "per_slot": slot_payloads,
        "slot_statistics": slot_payloads,
        "per_slot_by_slot": slot_payload_by_slot,
        "slot_statistics_by_slot": slot_payload_by_slot,
    }
    if include_signature_details:
        signatures_payload = cast(dict[str, object], payload["signatures"])
        signatures_payload["intersection"] = intersection
        signatures_payload["missing"] = missing
        signatures_payload["extra"] = extra
    return BlockInventoryAuditResult(payload)


# Verb aliases keep the API discoverable for callers that naturally use
# ``audit_block_archive`` or ``run_block_inventory_audit``.
audit_block_archive = audit_block_inventory
audit_archived_inventory = audit_block_inventory
run_block_inventory_audit = audit_block_inventory
audit_inventory_against_blocks = audit_block_inventory
compare_inventory_to_blocks = audit_block_inventory


def audit_block_inventory_json(
    inventory: str | Path,
    archive_roots: Sequence[str | Path] | str | Path,
    *,
    batch_size: int = 4_096,
    include_signature_details: bool = False,
    signature_sample_size: int = 16,
    indent: int | None = None,
) -> str:
    """Return the canonical JSON audit directly."""

    return audit_block_inventory(
        inventory,
        archive_roots,
        batch_size=batch_size,
        include_signature_details=include_signature_details,
        signature_sample_size=signature_sample_size,
    ).to_json(indent=indent)


__all__ = [
    "BlockInventoryAuditError",
    "BlockInventoryAuditResult",
    "audit_archived_inventory",
    "audit_block_archive",
    "audit_block_inventory",
    "audit_block_inventory_json",
    "audit_inventory_against_blocks",
    "compare_inventory_to_blocks",
    "run_block_inventory_audit",
]
