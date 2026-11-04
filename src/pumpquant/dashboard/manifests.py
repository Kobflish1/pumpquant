"""Verify sealed artifact manifests.

Every listed file must exist inside its root, with the size and SHA-256 the manifest
records, before the dashboard reads it.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pumpquant.core.hashing import sha256_file_with_size
from pumpquant.dashboard.config import (
    DEFAULT_MANIFEST,
    DashboardError,
    contains_holdout,
)

LIFECYCLE_STATES = frozenset({"pending", "sealed", "error", "stale"})


class ManifestError(DashboardError, ValueError):
    """Raised for an invalid, incomplete, or tampered manifest."""


def normalize_lifecycle(value: object) -> str:
    """Normalise producer-specific lifecycle labels.

    Unknown labels are treated as ``pending``.  A dashboard
    must never turn an unfamiliar state into an apparently sealed result.
    """

    if isinstance(value, str):
        normalized = value.strip().lower().replace("_", "-")
        aliases = {
            "complete": "sealed",
            "completed": "sealed",
            "success": "sealed",
            "ok": "sealed",
            "published": "sealed",
            "sealed": "sealed",
            "stale": "stale",
            "error": "error",
            "failed": "error",
            "failure": "error",
            "pending": "pending",
            "running": "pending",
            "in-progress": "pending",
            "partial": "pending",
            "waiting": "pending",
        }
        return aliases.get(normalized, "pending")
    return "pending"


def safe_relative_path(raw: object) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ManifestError("manifest artifact path must be a non-empty string")
    # POSIX artifact manifests are portable; reject Windows separators too so a
    # path authored on another host cannot become traversal on this one.
    if "\\" in raw:
        raise ManifestError(f"unsafe artifact path: {raw!r}")
    relative = Path(raw)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise ManifestError(f"unsafe artifact path: {raw!r}")
    normalized = relative.as_posix()
    if contains_holdout(normalized):
        raise ManifestError("manifest references a holdout artifact")
    return normalized


def path_has_symlink(path: Path, root: Path) -> bool:
    """Return true when any component below ``root`` is a symlink."""

    cursor = root
    if cursor.is_symlink():
        return True
    try:
        relative = path.relative_to(root)
    except ValueError:
        return True
    for component in relative.parts:
        cursor = cursor / component
        if cursor.is_symlink():
            return True
    return False


def _manifest_entries(manifest: Mapping[str, object]) -> list[Mapping[str, object]]:
    raw = manifest.get("artifacts", manifest.get("files", ()))
    if isinstance(raw, Mapping):
        entries: list[Mapping[str, object]] = []
        for path, metadata in raw.items():
            if not isinstance(metadata, Mapping):
                raise ManifestError("manifest files mapping values must be objects")
            entries.append({**dict(metadata), "path": path})
        return entries
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ManifestError("complete manifest must list artifacts")
    entries = []
    for item in raw:
        if isinstance(item, str):
            raise ManifestError("artifact entries need byte_count and sha256")
        if not isinstance(item, Mapping):
            raise ManifestError("manifest artifact entries must be objects")
        entries.append(item)
    return entries


def _entry_size(entry: Mapping[str, object]) -> int:
    value = entry.get("byte_count", entry.get("bytes", entry.get("size")))
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ManifestError("manifest artifact byte_count must be a non-negative integer")
    return value


def _entry_hash(entry: Mapping[str, object]) -> str:
    value = entry.get("sha256", entry.get("hash"))
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ManifestError("manifest artifact sha256 must be a 64-character hex digest")
    return value.lower()


@dataclass(frozen=True)
class VerifiedArtifact:
    relative_path: str
    path: Path
    byte_count: int
    sha256: str
    metadata: Mapping[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.relative_path,
            "byte_count": self.byte_count,
            "sha256": self.sha256,
            **jsonable(dict(self.metadata)),
        }


@dataclass(frozen=True)
class ManifestReport:
    root: Path
    manifest_path: Path
    lifecycle: str
    manifest_sha256: str | None = None
    manifest_byte_count: int | None = None
    manifest: Mapping[str, object] = field(default_factory=dict)
    artifacts: tuple[VerifiedArtifact, ...] = ()
    error: str | None = None
    updated_at: str | None = None

    @property
    def status(self) -> str:
        return self.lifecycle

    def as_dict(self, *, include_root: bool = True) -> dict[str, object]:
        result: dict[str, object] = {
            "lifecycle": self.lifecycle,
            "status": self.lifecycle,
            "manifest": {
                "path": self.manifest_path.name,
                "sha256": self.manifest_sha256,
                "byte_count": self.manifest_byte_count,
                "status": (
                    self.manifest.get(
                        "status", self.manifest.get("lifecycle", self.manifest.get("state"))
                    )
                    if self.manifest
                    else None
                ),
                "updated_at": self.updated_at,
            },
            "artifacts": [item.as_dict() for item in self.artifacts],
            "error": self.error,
        }
        if include_root:
            result["root"] = str(self.root)
        return result

    def __getitem__(self, key: str) -> object:
        """Provide a small mapping-compatible view for callers/tests."""

        if key == "lifecycle" or key == "status":
            return self.lifecycle
        return self.as_dict()[key]

    def get(self, key: str, default: object = None) -> object:
        try:
            return self[key]
        except KeyError:
            return default


def _complete_status(manifest: Mapping[str, object]) -> bool:
    status = manifest.get("status", manifest.get("lifecycle", manifest.get("state")))
    if normalize_lifecycle(status) == "sealed":
        return True
    return manifest.get("sealed") is True or manifest.get("complete") is True


def verify_sealed_manifest(
    root: str | Path,
    *,
    manifest_name: str = DEFAULT_MANIFEST,
    stale_after_seconds: float = 24 * 60 * 60,
    now: float | None = None,
) -> ManifestReport:
    """Inspect one exact root and verify its manifest and listed artifacts.

    Missing roots/manifests and non-complete manifests are pending.
    Malformed manifests, traversal, symlinks, missing files, size mismatches,
    and digest mismatches are errors.  A complete manifest whose mtime exceeds
    ``stale_after_seconds`` remains fully verified but is labelled stale.
    """

    original_root_path = Path(root)
    if original_root_path.exists() and original_root_path.is_symlink():
        return ManifestReport(
            root=original_root_path.resolve(strict=False),
            manifest_path=original_root_path / manifest_name,
            lifecycle="error",
            error="artifact root must not be a symlink",
        )
    root_path = original_root_path.resolve(strict=False)
    manifest_path = root_path / manifest_name
    if contains_holdout(root_path) or contains_holdout(manifest_name):
        return ManifestReport(
            root=root_path,
            manifest_path=manifest_path,
            lifecycle="error",
            error="holdout roots are forbidden",
        )
    try:
        if not root_path.exists() or not root_path.is_dir():
            return ManifestReport(root=root_path, manifest_path=manifest_path, lifecycle="pending")
        if root_path.is_symlink() or not manifest_path.exists() or not manifest_path.is_file():
            return ManifestReport(root=root_path, manifest_path=manifest_path, lifecycle="pending")
        if manifest_path.is_symlink():
            raise ManifestError("manifest must not be a symlink")
        raw = manifest_path.read_bytes()
        manifest_sha256 = hashlib.sha256(raw).hexdigest()
        if not raw:
            raise ManifestError("manifest is empty")
        parsed = json.loads(raw.decode("utf-8"))
        if not isinstance(parsed, Mapping):
            raise ManifestError("manifest must be a JSON object")
        if not _complete_status(parsed):
            return ManifestReport(
                root=root_path,
                manifest_path=manifest_path,
                lifecycle="pending",
                manifest_sha256=manifest_sha256,
                manifest_byte_count=len(raw),
                manifest=dict(parsed),
                updated_at=_file_updated_at(manifest_path),
            )
        entries = _manifest_entries(parsed)
        verified: list[VerifiedArtifact] = []
        seen: set[str] = set()
        for entry in entries:
            entry_status = entry.get("status", entry.get("lifecycle"))
            if entry_status is not None and normalize_lifecycle(entry_status) != "sealed":
                raise ManifestError("manifest lists an artifact that is not sealed")
            relative = safe_relative_path(
                entry.get("path", entry.get("relative_path", entry.get("name")))
            )
            if relative in seen:
                raise ManifestError(f"manifest repeats artifact path: {relative}")
            seen.add(relative)
            path = root_path / relative
            # ``resolve`` catches a symlink nested below an otherwise valid
            # root.  The final path must remain below the exact root.
            resolved = path.resolve(strict=False)
            try:
                resolved.relative_to(root_path)
            except ValueError as exc:
                raise ManifestError(f"artifact path escapes root: {relative}") from exc
            if path_has_symlink(path, root_path) or not path.exists() or not path.is_file():
                raise ManifestError(f"manifested artifact is missing or symlinked: {relative}")
            expected_size = _entry_size(entry)
            expected_hash = _entry_hash(entry)
            actual_size, actual_hash = sha256_file_with_size(path)
            if actual_size != expected_size:
                raise ManifestError(
                    f"artifact byte count mismatch for {relative}: {actual_size} != {expected_size}"
                )
            if actual_hash != expected_hash:
                raise ManifestError(f"artifact sha256 mismatch for {relative}")
            verified.append(
                VerifiedArtifact(
                    relative_path=relative,
                    path=path,
                    byte_count=actual_size,
                    sha256=actual_hash,
                    metadata=dict(entry),
                )
            )
        age = max(0.0, (now if now is not None else time.time()) - manifest_path.stat().st_mtime)
        lifecycle = "stale" if age > stale_after_seconds else "sealed"
        return ManifestReport(
            root=root_path,
            manifest_path=manifest_path,
            lifecycle=lifecycle,
            manifest_sha256=manifest_sha256,
            manifest_byte_count=len(raw),
            manifest=dict(parsed),
            artifacts=tuple(verified),
            updated_at=_file_updated_at(manifest_path),
        )
    except (OSError, UnicodeError, json.JSONDecodeError, ManifestError) as exc:
        return ManifestReport(
            root=root_path,
            manifest_path=manifest_path,
            lifecycle="error",
            error=str(exc),
        )


_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def jsonable(value: Any) -> Any:
    """Return a deterministic JSON-safe copy of a payload.

    Artifact payloads are expected to be JSON, but recursively filtering them
    protects the HTTP encoder from accidental non-JSON values in test doubles
    or callers constructing a snapshot by hand.
    """

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return str(value)


def _file_updated_at(path: Path) -> str | None:
    try:
        return iso_timestamp(datetime.fromtimestamp(path.stat().st_mtime, UTC))
    except OSError:
        return None


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_timestamp(value: datetime | None = None) -> str:
    value = value or utc_now()
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
