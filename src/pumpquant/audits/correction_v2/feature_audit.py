"""Independent second-pass audit and attestation for corrected feature v2.

This module is deliberately a *reader* of a draft correction root.  It never
opens a strategy result, report, outcome, FX, or holdout payload.  The draft
manifest and its sidecars are checked first, then the corrected Parquet is
streamed in canonical chain order.  Reserve algebra, predecessor links,
coverage semantics, and the four preregistered family source eligibility
views are recomputed without invoking a grid runner.

The public sealer copies the verified draft payload byte-for-byte into a new
root and writes a new manifest last.  Existing roots are never overwritten.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import importlib
import json
import math
import os
import shutil
import sys
import time
from collections import Counter, defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, NoReturn, cast

import pyarrow.parquet as pq

from pumpquant.audits.correction_v2.contract import (
    CONFIG_PATH,
    DEVELOPMENT_END_SLOT,
    DEVELOPMENT_START_SLOT,
    HOLDOUT_END_SLOT,
    HOLDOUT_START_SLOT,
    CorrectionV2Contract,
    CorrectionV2ContractError,
    canonical_json,
    load_contract,
)
from pumpquant.audits.correction_v2.retry_contract import (
    CONFIG_PATH as RETRY_CONFIG_PATH,
)
from pumpquant.audits.correction_v2.retry_contract import (
    CorrectionV2RetryContract,
    CorrectionV2RetryContractError,
    load_retry_contract,
)
from pumpquant.core.hashing import sha256_file
from pumpquant.core.schema import VenuePhase
from pumpquant.research.families.amm_impact_ofi import (
    create_amm_impact_ofi_source_sql,
    default_amm_impact_ofi_candidates,
)
from pumpquant.research.families.persistent_pressure import default_persistent_pressure_candidates
from pumpquant.research.families.progress_speed import (
    create_progress_crossing_source_sql,
    default_progress_speed_candidates,
)
from pumpquant.research.families.shock_recovery import (
    create_shock_recovery_source_sql,
    default_shock_recovery_candidates,
)

KIND = "pumpquant.independent_feature_audit_v2"
VERSION = 2
FEATURE_KIND = "pumpquant.shock_recovery_feature_dataset"
FEATURE_VERSION = 2
PHYSICAL_ORDER = "slot,tx_index,log_index,event_index,event_id"
FINAL_HOLDOUT_START_SLOT = HOLDOUT_START_SLOT

FAMILY_IDS: tuple[str, ...] = (
    "shock-recovery-fresh-v2",
    "progress-speed-fresh-v2",
    "persistent-pressure-fresh-v2",
    "amm-impact-ofi-fresh-v2",
)

_FORBIDDEN_PARTS = frozenset({"holdout", "outcome", "outcomes", "report", "reports", "s3"})
_DOWNSTREAM_FLAGS = frozenset(
    {
        "holdout_accessed",
        "holdout_payload_reads_allowed",
        "holdout_feature_generation_allowed",
        "outcomes_read",
        "reports_read",
        "s3_read",
        "strategy_outcomes_read",
        "strategy_outcome_read",
    }
)
_KNOWN_PHASES = frozenset(item.value for item in VenuePhase)
_RESERVE_SOURCES = frozenset(
    {"unavailable", "pump_event_local_candidate", "pumpswap_persisted_raw"}
)
_REASONS = frozenset(
    {
        "segment_start",
        "continuous",
        "coverage_gap",
        "phase_transition",
        "missing_predecessor",
        "missing_post_state",
        "reserve_mismatch",
        "real_reserve_mismatch",
    }
)
_SHA256_HEX = frozenset("0123456789abcdef")


class IndependentFeatureAuditError(RuntimeError):
    """A draft, stream, family, or publication invariant failed."""


# Compatibility name used by callers that group correction-v2 errors.
IndependentFeatureAuditV2Error = IndependentFeatureAuditError


def _fail(message: str) -> NoReturn:
    raise IndependentFeatureAuditError(message)


def _sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _is_sha(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in _SHA256_HEX for c in value)


def _forbidden_path(path: Path) -> bool:
    return any(part.lower() in _FORBIDDEN_PARTS for part in path.parts)


def _regular(path: Path, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        _fail(f"{label} must be a regular non-symlink file")


def _safe_root(path: Path, label: str) -> Path:
    if _forbidden_path(path):
        _fail(f"{label} points at a forbidden downstream path")
    if path.is_symlink() or not path.is_dir():
        _fail(f"{label} must be a regular directory")
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        _fail(f"{label} cannot be resolved safely: {error}")
    absolute = path.absolute()
    # macOS exposes the temporary directory through the conventional /var
    # alias while realpath returns /private/var.  That system alias is safe;
    # any other changed component denotes a user-created symlink traversal.
    harmless_var_alias = (
        absolute.parts[:2] == (os.sep, "var")
        and resolved.parts[:3] == (os.sep, "private", "var")
        and resolved.parts[3:] == absolute.parts[2:]
    )
    if (resolved != absolute and not harmless_var_alias) or _forbidden_path(resolved):
        _fail(f"{label} may not traverse a symlink or forbidden path")
    return resolved


def _safe_output_parent(path: Path) -> None:
    """Reject output-parent symlink traversal, including missing descendants."""

    if _forbidden_path(path):
        _fail("independent output parent is unsafe")
    absolute = path.absolute()
    try:
        resolved = absolute.resolve(strict=False)
    except OSError as error:
        _fail(f"independent output parent cannot be resolved safely: {error}")
    harmless_var_alias = (
        absolute.parts[:2] == (os.sep, "var")
        and resolved.parts[:3] == (os.sep, "private", "var")
        and resolved.parts[3:] == absolute.parts[2:]
    )
    if resolved != absolute and not harmless_var_alias:
        _fail("independent output parent may not traverse a symlink")


def _safe_relative(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or os.path.isabs(value):
        _fail(f"{label} must be a relative path")
    candidate = Path(value)
    if candidate.name in {"", ".", ".."} or any(
        part in {"", ".", ".."} for part in candidate.parts
    ):
        _fail(f"{label} is unsafe")
    if _forbidden_path(candidate):
        _fail(f"{label} points at a forbidden path")
    return candidate.as_posix()


def _parse_canonical(path: Path, label: str) -> tuple[dict[str, object], bytes]:
    _regular(path, label)
    body = path.read_bytes()
    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_unique_object_pairs,
            parse_constant=lambda token: (_ for _ in ()).throw(
                IndependentFeatureAuditError(f"{label} contains non-finite JSON {token}")
            ),
        )
    except IndependentFeatureAuditError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        _fail(f"{label} is invalid UTF-8 JSON: {error}")
    if not isinstance(value, dict):
        _fail(f"{label} must be a JSON object")
    payload = cast(dict[str, object], value)
    if body != canonical_json(payload):
        _fail(f"{label} is not canonical JSON")
    return payload, body


def _unique_object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            _fail(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _json_no_downstream(value: object, label: str) -> None:
    """Reject a positive downstream-read declaration anywhere in JSON."""

    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key).lower()
            if key_text in _DOWNSTREAM_FLAGS and item is not False:
                _fail(f"{label} declares forbidden downstream access in {key}")
            _json_no_downstream(item, label)
    elif isinstance(value, list):
        for item in value:
            _json_no_downstream(item, label)


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        _fail(f"{label} must be an integer >= {minimum}")
    return value


def _text(value: object, label: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        _fail(f"{label} must be non-empty text")
    return value


def _bool(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        _fail(f"{label} must be boolean")
    return value


def _optional_int(value: object, label: str, *, minimum: int = 0) -> int | None:
    if value is None:
        return None
    return _integer(value, label, minimum=minimum)


def _optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _text(value, label)


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail(f"{label} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        _fail(f"{label} must be finite")
    return number


def _entry(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        _fail(f"{label} must be an object")
    return cast(dict[str, object], value)


def _inventory(value: object, label: str) -> dict[str, dict[str, object]]:
    """Normalize both historical dict and list file inventories."""

    result: dict[str, dict[str, object]] = {}
    iterator: list[tuple[object, object]]
    if isinstance(value, Mapping):
        iterator = list(value.items())
    elif isinstance(value, list):
        pairs: list[tuple[object, object]] = []
        for item in value:
            obj = _entry(item, label)
            name = obj.get("path", obj.get("name"))
            pairs.append((_text(name, f"{label}.path"), obj))
        iterator = pairs
    else:
        _fail(f"{label} must be an object or array")
    for name_value, item in iterator:
        name = _safe_relative(str(name_value), f"{label} path")
        if name in result:
            _fail(f"{label} contains duplicate {name}")
        obj = _entry(item, f"{label}.{name}")
        byte_count = _integer(obj.get("byte_count"), f"{label}.{name}.byte_count")
        digest = obj.get("sha256")
        if not _is_sha(digest):
            _fail(f"{label}.{name}.sha256 is invalid")
        result[name] = {**obj, "byte_count": byte_count, "sha256": cast(str, digest)}
    return result


def _verify_inventory(
    root: Path, inventory: Mapping[str, Mapping[str, object]], label: str
) -> None:
    for name, entry in inventory.items():
        path = root / name
        _regular(path, f"{label} {name}")
        actual_size = path.stat().st_size
        actual_sha = sha256_file(path)
        if actual_size != entry.get("byte_count") or actual_sha != entry.get("sha256"):
            _fail(f"{label} {name} differs from its manifest seal")
    # A sealed root may contain only the manifest and listed payloads.  This
    # catches an unlisted symlink before any downstream consumer can discover it.
    actual: set[str] = set()
    for path in root.rglob("*"):
        relative = path.relative_to(root).as_posix()
        if path.is_dir() and not path.is_symlink():
            continue
        actual.add(relative)
    expected = set(inventory) | {"manifest.json"}
    if actual != expected:
        _fail(f"{label} inventory does not cover the complete root")
    for path in root.rglob("*"):
        if path.is_symlink():
            _fail(f"{label} contains a symlink: {path.relative_to(root)}")


def _path_ref(value: object, label: str) -> tuple[str, str | None, int | None]:
    obj = _entry(value, label)
    raw = _text(obj.get("value"), f"{label}.value")
    digest = obj.get("sha256")
    if digest is not None and not _is_sha(digest):
        _fail(f"{label}.sha256 is invalid")
    byte_count = obj.get("byte_count")
    if byte_count is not None:
        _integer(byte_count, f"{label}.byte_count")
    return raw, cast(str | None, digest), cast(int | None, byte_count)


def _verify_reference_file(
    value: object,
    label: str,
    *,
    expected_path: str | None = None,
    expected_sha: str | None = None,
    require_file: bool = False,
) -> dict[str, object]:
    raw, declared_sha, declared_bytes = _path_ref(value, label)
    path = Path(raw)
    if expected_path is not None and raw != expected_path:
        _fail(f"{label} path differs from the frozen binding")
    actual_sha: str | None
    actual_bytes: int | None
    if path.exists():
        if path.is_symlink() or not path.is_file() or _forbidden_path(path):
            _fail(f"{label} points at an unsafe file")
        actual_sha = sha256_file(path)
        actual_bytes = path.stat().st_size
        if declared_sha is not None and declared_sha != actual_sha:
            _fail(f"{label} SHA differs from its file")
        if declared_bytes is not None and declared_bytes != actual_bytes:
            _fail(f"{label} byte count differs from its file")
    elif require_file:
        _fail(f"{label} file is absent")
    else:
        actual_sha = declared_sha
        actual_bytes = declared_bytes
    if expected_sha is not None:
        if declared_sha != expected_sha:
            _fail(f"{label} SHA differs from the frozen binding")
        actual_sha = expected_sha
    result: dict[str, object] = {"value": raw}
    if declared_sha is not None:
        result["sha256"] = declared_sha
    if actual_bytes is not None:
        result["byte_count"] = actual_bytes
    return result


@dataclass(frozen=True, slots=True)
class _Draft:
    root: Path
    manifest_path: Path
    manifest_body: bytes
    manifest: dict[str, object]
    files: dict[str, dict[str, object]]
    features_path: Path
    audit_path: Path
    audit_body: bytes
    audit: dict[str, object]
    predecessor_path: Path
    predecessor_body: bytes
    predecessor: dict[str, object]
    v1_manifest_sha256: str
    v1_audit_sha256: str
    mode_manifest_sha256: str
    mode_audit_sha256: str
    mode_modes_sha256: str
    config_sha256: str
    amendment_sha256: str
    contract: CorrectionV2Contract


@dataclass(frozen=True, slots=True)
class _RetryContext:
    """Validated Retry-1 contract plus the immutable parent attestation."""

    contract: CorrectionV2RetryContract
    config_path: Path
    parent_root: Path
    parent_manifest: dict[str, object]
    parent_manifest_body: bytes
    parent_source_code_inventory_sha256: str
    parent_build_identity_sha256: str


def _require_sha(value: object, label: str) -> str:
    if not _is_sha(value):
        _fail(f"{label} must be a lowercase SHA-256 digest")
    return cast(str, value)


def _require_development_flags(value: Mapping[str, object], label: str) -> None:
    if value.get("development_only") is not True:
        _fail(f"{label} is not development-only")
    for name in (
        "holdout_payload_reads_allowed",
        "holdout_feature_generation_allowed",
        "holdout_accessed",
        "outcomes_read",
        "reports_read",
        "s3_read",
    ):
        if value.get(name) is not False:
            _fail(f"{label} {name} must be false")


def _parent_sidecar(
    root: Path,
    manifest: Mapping[str, object],
    name: str,
    label: str,
) -> tuple[dict[str, object], bytes, str]:
    value = manifest.get(name)
    default = {
        "independent_audit_path": "independent-audit.json",
        "lineage_audit": "lineage-audit.json",
    }.get(name)
    if name == "lineage_audit":
        reference = _entry(value, label) if value is not None else {"path": default}
        relative = _safe_relative(reference.get("path"), f"{label}.path")
    else:
        relative = _safe_relative(value or default, label)
    path = root / relative
    payload, body = _parse_canonical(path, label)
    return payload, body, relative


def _parent_stable_identity(
    key: str,
    expected: str,
    sources: Sequence[Mapping[str, object]],
    files: Mapping[str, Mapping[str, object]],
) -> None:
    aliases: dict[str, tuple[str, ...]] = {
        "draft_manifest_sha256": (
            "draft_manifest_sha256",
            "parent_draft_manifest_sha256",
        ),
        "draft_audit_sha256": ("draft_audit_sha256", "parent_draft_audit_sha256"),
        "predecessor_sha256": (
            "predecessor_sha256",
            "predecessor_audit_sha256",
            "draft_predecessor_audit_sha256",
        ),
        "features_sha256": ("features_sha256", "draft_features_sha256"),
        "row_identity_sha256": ("row_identity_sha256",),
        "decision_sha256": ("decision_sha256",),
        "lineage_sha256": ("lineage_sha256", "lineage_audit_sha256"),
    }
    found: set[str] = set()
    for source in sources:
        for alias in aliases[key]:
            value = source.get(alias)
            if value is not None:
                found.add(_require_sha(value, f"parent {key}"))
        nested_values: list[Mapping[str, object]] = []
        stable_value = source.get("stable_data")
        if isinstance(stable_value, Mapping):
            nested_values.append(stable_value)
        technical_value = source.get("technical_retry")
        if isinstance(technical_value, Mapping):
            nested_values.append(technical_value)
            technical_stable = technical_value.get("stable_data")
            if isinstance(technical_stable, Mapping):
                nested_values.append(technical_stable)
        for nested in nested_values:
            value = nested.get(key)
            if value is None and key == "predecessor_sha256":
                value = nested.get("predecessor_audit_sha256")
            if value is None and key == "lineage_sha256":
                value = nested.get("lineage_audit_sha256")
            if value is not None:
                found.add(_require_sha(value, f"parent stable_data.{key}"))
    file_aliases = {
        "draft_manifest_sha256": ("draft-manifest.json",),
        "draft_audit_sha256": ("draft-audit.json",),
        "predecessor_sha256": ("predecessor-audit.json",),
        "features_sha256": ("features.parquet",),
        "lineage_sha256": ("lineage-audit.json",),
    }
    for file_name in file_aliases.get(key, ()):
        entry = files.get(file_name)
        if entry is not None:
            found.add(_require_sha(entry.get("sha256"), f"parent files.{file_name}"))
    if not found:
        _fail(f"parent attested root is missing stable identity {key}")
    if found != {expected}:
        _fail(f"parent stable identity {key} differs from Retry-1 preregistration")


def _load_retry_context(
    technical_retry_config: str | Path | None,
    parent_attested_root: str | Path | None,
) -> _RetryContext | None:
    if technical_retry_config is None and parent_attested_root is None:
        return None
    if technical_retry_config is None or parent_attested_root is None:
        _fail("Retry-1 publication requires both technical retry config and parent attested root")
    config_path = Path(technical_retry_config)
    _safe_output_parent(config_path.parent)
    if config_path.is_symlink() or not config_path.is_file() or _forbidden_path(config_path):
        _fail("Retry-1 config file is absent or unsafe")
    try:
        retry = load_retry_contract(config_path)
    except (CorrectionV2RetryContractError, OSError) as error:
        _fail(f"Retry-1 config is not the frozen preregistration: {error}")
    parent_root = _safe_root(Path(parent_attested_root), "parent attested root")
    manifest, manifest_body = _parse_canonical(
        parent_root / "manifest.json", "parent attested manifest"
    )
    if _sha256_bytes(manifest_body) != retry.parent.attested_manifest_sha256:
        _fail("parent attested manifest SHA differs from Retry-1 preregistration")
    if (
        manifest.get("kind") != FEATURE_KIND
        or manifest.get("version") != FEATURE_VERSION
        or manifest.get("status") != "complete"
        or manifest.get("attestation_kind") != KIND
        or manifest.get("attestation_version") != VERSION
    ):
        _fail("parent root is not a complete independent corrected-v2 attestation")
    if manifest.get("config_sha256") != retry.parent_config_sha256:
        _fail("parent correction config identity differs from Retry-1 preregistration")
    if manifest.get("amendment_sha256") != retry.parent_amendment_sha256:
        _fail("parent correction amendment identity differs from Retry-1 preregistration")
    _json_no_downstream(manifest, "parent attested manifest")
    _require_development_flags(manifest, "parent attested manifest")
    # The parent is a sealed provenance input, not a retry payload source.
    # Read only its canonical manifest and the development-only attestation /
    # lineage sidecars needed for stable identities and access flags; do not
    # rehash or consume the parent's feature/family payloads here.
    parent_files = _inventory(manifest.get("files"), "parent attested files")
    parent_actual: set[str] = set()
    for path in parent_root.rglob("*"):
        relative = path.relative_to(parent_root).as_posix()
        if path.is_symlink():
            _fail(f"parent attested root contains a symlink: {relative}")
        if path.is_dir():
            continue
        parent_actual.add(relative)
    if parent_actual != set(parent_files) | {"manifest.json"}:
        _fail("parent attested inventory does not cover its complete root")
    independent, independent_body, independent_name = _parent_sidecar(
        parent_root,
        manifest,
        "independent_audit_path",
        "parent independent audit",
    )
    independent_entry = parent_files.get(independent_name)
    if independent_entry is None or independent_entry.get("sha256") != _sha256_bytes(
        independent_body
    ):
        _fail("parent independent audit is not sealed in its manifest inventory")
    if independent_entry.get("byte_count") != len(independent_body):
        _fail("parent independent audit byte count differs from its inventory")
    _json_no_downstream(independent, "parent independent audit")
    _require_development_flags(independent, "parent independent audit")
    lineage, lineage_body, lineage_name = _parent_sidecar(
        parent_root, manifest, "lineage_audit", "parent lineage audit"
    )
    lineage_entry = parent_files.get(lineage_name)
    if lineage_entry is None or lineage_entry.get("sha256") != _sha256_bytes(lineage_body):
        _fail("parent lineage audit is not sealed in its manifest inventory")
    if lineage_entry.get("byte_count") != len(lineage_body):
        _fail("parent lineage audit byte count differs from its inventory")
    _json_no_downstream(lineage, "parent lineage audit")
    _require_development_flags(lineage, "parent lineage audit")
    payload_value = manifest.get("attestation_payload")
    payload = (
        _entry(payload_value, "parent attestation payload") if payload_value is not None else {}
    )
    _json_no_downstream(payload, "parent attestation payload")
    stable_sources: tuple[Mapping[str, object], ...] = (
        manifest,
        payload,
        independent,
        lineage,
    )
    for key, expected in retry.stable_data.items():
        _parent_stable_identity(key, expected, stable_sources, parent_files)
    parent_technical = manifest.get("technical_retry")
    parent_technical_obj = (
        _entry(parent_technical, "parent technical_retry") if parent_technical is not None else {}
    )
    parent_evaluation = manifest.get("evaluation_code_audit")
    parent_evaluation_obj = (
        _entry(parent_evaluation, "parent evaluation_code_audit")
        if parent_evaluation is not None
        else {}
    )
    parent_inventory = _require_sha(
        manifest.get(
            "source_code_inventory_sha256",
            parent_technical_obj.get(
                "source_code_inventory_sha256",
                parent_evaluation_obj.get("source_code_inventory_sha256"),
            ),
        ),
        "parent source_code_inventory_sha256",
    )
    parent_build = _require_sha(
        manifest.get(
            "build_identity_sha256",
            parent_technical_obj.get(
                "build_identity_sha256", parent_evaluation_obj.get("build_identity_sha256")
            ),
        ),
        "parent build_identity_sha256",
    )
    for label, value in (
        (
            "parent independent source_code_inventory_sha256",
            independent.get("source_code_inventory_sha256"),
        ),
        (
            "parent independent build_identity_sha256",
            independent.get("build_identity_sha256"),
        ),
    ):
        if value is not None and _require_sha(value, label) != (
            parent_inventory if "inventory" in label else parent_build
        ):
            _fail(f"{label} differs from parent manifest")
    for label, source in (
        ("parent manifest", manifest),
        ("parent independent audit", independent),
    ):
        evaluation = source.get("evaluation_code_audit")
        if isinstance(evaluation, Mapping):
            inventory_value = evaluation.get("source_code_inventory_sha256")
            build_value = evaluation.get("build_identity_sha256")
            if (
                inventory_value is not None
                and _require_sha(inventory_value, f"{label} evaluation source inventory")
                != parent_inventory
            ):
                _fail(f"{label} evaluation source inventory differs from parent")
            if (
                build_value is not None
                and _require_sha(build_value, f"{label} evaluation build identity") != parent_build
            ):
                _fail(f"{label} evaluation build identity differs from parent")
    return _RetryContext(
        contract=retry,
        config_path=config_path,
        parent_root=parent_root,
        parent_manifest=manifest,
        parent_manifest_body=manifest_body,
        parent_source_code_inventory_sha256=parent_inventory,
        parent_build_identity_sha256=parent_build,
    )


def _retry_stable_data(
    context: _RetryContext,
    draft: _Draft,
    independent: Mapping[str, object],
    lineage_sha256: str | None,
) -> dict[str, str]:
    if lineage_sha256 is None:
        _fail("Retry-1 publication requires a sealed lineage audit identity")
    actual = {
        "draft_manifest_sha256": _sha256_bytes(draft.manifest_body),
        "draft_audit_sha256": _sha256_bytes(draft.audit_body),
        "predecessor_sha256": _sha256_bytes(draft.predecessor_body),
        "features_sha256": _require_sha(
            draft.files.get("features.parquet", {}).get("sha256"),
            "draft features_sha256",
        ),
        "row_identity_sha256": _require_sha(
            independent.get("row_identity_sha256"), "independent row_identity_sha256"
        ),
        "decision_sha256": _require_sha(
            independent.get("decision_sha256"), "independent decision_sha256"
        ),
        "lineage_sha256": _require_sha(lineage_sha256, "lineage_sha256"),
    }
    if actual != dict(context.contract.stable_data):
        _fail("Retry-1 stable data identities differ from preregistration")
    return actual


def _technical_retry_payload(
    context: _RetryContext,
    evaluation_code: Mapping[str, object],
    stable_data: Mapping[str, str],
) -> dict[str, object]:
    source_files_value = evaluation_code.get("source_files")
    if not isinstance(source_files_value, list):
        _fail("Retry-1 evaluation code inventory is malformed")
    source_files = {
        _text(_entry(item, "Retry-1 source file").get("path"), "Retry-1 source path"): _entry(
            item, "Retry-1 source file"
        )
        for item in source_files_value
    }
    retry_config_entry = source_files.get(RETRY_CONFIG_PATH.as_posix())
    if (
        retry_config_entry is None
        or retry_config_entry.get("sha256") != context.contract.config_sha256
    ):
        _fail("Retry-1 config changed after contract verification")
    retry_document_entry = source_files.get(context.contract.retry_document.path)
    if (
        retry_document_entry is None
        or retry_document_entry.get("sha256") != context.contract.retry_document.sha256
    ):
        _fail("Retry-1 document changed after contract verification")
    inventory = _require_sha(
        evaluation_code.get("source_code_inventory_sha256"),
        "Retry-1 source_code_inventory_sha256",
    )
    build = _require_sha(
        evaluation_code.get("build_identity_sha256"),
        "Retry-1 build_identity_sha256",
    )
    if inventory == context.parent_source_code_inventory_sha256:
        _fail("Retry-1 source code inventory was not changed from parent")
    if build == context.parent_build_identity_sha256:
        _fail("Retry-1 build identity was not changed from parent")
    return {
        "retry_id": context.contract.retry_id,
        "retry_config_sha256": context.contract.config_sha256,
        "retry_document_sha256": context.contract.retry_document.sha256,
        "parent_config_sha256": context.contract.parent_config_sha256,
        "parent_amendment_sha256": context.contract.parent_amendment_sha256,
        "parent_attested_manifest_sha256": context.contract.parent.attested_manifest_sha256,
        "parent_shock_manifest_sha256": context.contract.parent.shock_manifest_sha256,
        "stable_data": dict(stable_data),
        "parent_source_code_inventory_sha256": context.parent_source_code_inventory_sha256,
        "parent_build_identity_sha256": context.parent_build_identity_sha256,
        "source_code_inventory_sha256": inventory,
        "build_identity_sha256": build,
        "retry_requirements": dict(context.contract.requirements),
        "development_only": True,
        "holdout_payload_reads_allowed": False,
        "holdout_feature_generation_allowed": False,
        "holdout_accessed": False,
        "outcomes_read": False,
        "reports_read": False,
        "s3_read": False,
    }


def _load_draft(root_value: Path, config_path: Path) -> _Draft:
    root = _safe_root(Path(root_value), "draft correction root")
    manifest_path = root / "manifest.json"
    manifest, manifest_body = _parse_canonical(manifest_path, "draft manifest")
    _json_no_downstream(manifest, "draft manifest")
    if (
        manifest.get("kind") != FEATURE_KIND
        or manifest.get("version") != FEATURE_VERSION
        or manifest.get("status") not in {"complete", "draft"}
        or manifest.get("development_only") is not True
    ):
        _fail("draft is not a corrected feature-v2 dataset")
    if (
        manifest.get("start_slot_inclusive") != DEVELOPMENT_START_SLOT
        or manifest.get("end_slot_exclusive") != DEVELOPMENT_END_SLOT
        or manifest.get("final_holdout_start_slot_inclusive") != HOLDOUT_START_SLOT
        or manifest.get("final_holdout_end_slot_exclusive") != HOLDOUT_END_SLOT
    ):
        _fail("draft slot bounds differ from the frozen development/holdout intervals")
    for name in (
        "holdout_payload_reads_allowed",
        "holdout_feature_generation_allowed",
        "holdout_accessed",
        "outcomes_read",
        "reports_read",
        "s3_read",
    ):
        if manifest.get(name) is not False:
            _fail(f"draft manifest {name} must be false")
    files = _inventory(manifest.get("files"), "draft files")
    _verify_inventory(root, files, "draft")
    feature_entry = files.get("features.parquet")
    if feature_entry is None:
        _fail("draft inventory does not seal features.parquet")
    features_path = root / "features.parquet"
    row_count = _integer(feature_entry.get("row_count"), "draft features row_count")
    if manifest.get("row_count") != row_count:
        _fail("draft feature row counts disagree")
    if manifest.get("physical_order") != PHYSICAL_ORDER:
        _fail("draft physical order differs from the canonical order")
    audit_name = _safe_relative(manifest.get("audit_path", "audit.json"), "draft audit_path")
    predecessor_name = _safe_relative(
        manifest.get("predecessor_audit_path", "predecessor-audit.json"),
        "draft predecessor_audit_path",
    )
    audit_path = root / audit_name
    predecessor_path = root / predecessor_name
    audit, audit_body = _parse_canonical(audit_path, "draft audit")
    predecessor, predecessor_body = _parse_canonical(predecessor_path, "draft predecessor audit")
    _json_no_downstream(audit, "draft audit")
    _json_no_downstream(predecessor, "draft predecessor audit")
    if manifest.get("audit_sha256") != _sha256_bytes(audit_body):
        _fail("draft audit differs from its manifest binding")
    if manifest.get("predecessor_audit_sha256") != _sha256_bytes(predecessor_body):
        _fail("draft predecessor audit differs from its manifest binding")
    if audit.get("version") != FEATURE_VERSION or predecessor.get("version") != FEATURE_VERSION:
        _fail("draft audits are not correction-v2 audits")
    for payload, label in ((audit, "draft audit"), (predecessor, "draft predecessor audit")):
        if payload.get("development_only") is not True:
            _fail(f"{label} is not development-only")
        if payload.get("start_slot_inclusive") != DEVELOPMENT_START_SLOT:
            _fail(f"{label} start bound differs")
        if payload.get("end_slot_exclusive") != DEVELOPMENT_END_SLOT:
            _fail(f"{label} end bound differs")
        if payload.get("holdout_accessed") is not False:
            _fail(f"{label} holdout_accessed must be false")
        if payload.get("holdout_payload_reads_allowed") is not False:
            _fail(f"{label} holdout_payload_reads_allowed must be false")

    # The canonical contract is read and validated before checking references.
    config_path = Path(config_path)
    if config_path.is_symlink() or not config_path.is_file() or _forbidden_path(config_path):
        _fail("correction config file is absent or unsafe")
    try:
        contract = load_contract(config_path)
    except (CorrectionV2ContractError, OSError) as error:
        _fail(f"correction config is not the frozen v2 contract: {error}")
    config_sha = sha256_file(config_path)
    references = _entry(manifest.get("references"), "draft references")
    _verify_reference_file(
        references.get("config"),
        "references.config",
        expected_path=config_path.as_posix(),
        expected_sha=config_sha,
        require_file=True,
    )
    amendment_sha = contract.amendment.sha256
    if amendment_sha is None:
        _fail("correction amendment deployment binding is unresolved")
    _verify_reference_file(
        references.get("amendment"),
        "references.amendment",
        expected_path=contract.amendment.path,
        expected_sha=amendment_sha,
        require_file=True,
    )
    _verify_reference_file(references.get("code"), "references.code")

    # Re-check the independently sealed v1 source binding.  Importing this
    # private loader only validates source lifecycle; it does not access any
    # outcomes or strategy result.
    input_v1 = _entry(manifest.get("input_v1"), "draft input_v1")
    input_root_value = _text(input_v1.get("root"), "draft input_v1.root")
    try:
        from scripts.correct_shock_recovery_features_v2 import _load_input

        source_seal = _load_input(Path(input_root_value))
    except Exception as error:
        if isinstance(error, IndependentFeatureAuditError):
            raise
        _fail(f"draft v1 input cannot be independently sealed: {error}")
    v1_manifest_sha = source_seal.manifest_sha256
    v1_audit_sha = source_seal.audit_sha256
    if input_v1.get("manifest_sha256") != v1_manifest_sha:
        _fail("draft input_v1 manifest SHA binding differs")
    if input_v1.get("audit_sha256") != v1_audit_sha:
        _fail("draft input_v1 audit SHA binding differs")
    for field_name in ("v1_manifest_sha256", "source_manifest_sha256"):
        if manifest.get(field_name) not in {None, v1_manifest_sha}:
            _fail(f"draft v1 {field_name} binding differs")
    for field_name in ("v1_audit_sha256", "source_audit_sha256"):
        if manifest.get(field_name) not in {None, v1_audit_sha}:
            _fail(f"draft v1 {field_name} binding differs")
    declared_source_files = _inventory(input_v1.get("files"), "draft input_v1.files")
    if set(declared_source_files) != set(source_seal.files):
        _fail("draft input_v1 file inventory differs from the sealed v1 root")
    for name, entry in declared_source_files.items():
        source_entry = source_seal.files.get(name)
        if (
            source_entry is None
            or entry.get("byte_count") != source_entry.get("byte_count")
            or entry.get("sha256") != source_entry.get("sha256")
        ):
            _fail(f"draft input_v1 file binding differs for {name}")

    mode_ref = _entry(manifest.get("market_mode_index"), "draft market_mode_index")
    mode_root_value = _text(mode_ref.get("root"), "market_mode_index.root")
    try:
        from scripts.correct_shock_recovery_features_v2 import _load_mode_index

        mode_seal = _load_mode_index(Path(mode_root_value))
    except Exception as error:
        _fail(f"draft market-mode index cannot be independently sealed: {error}")
    mode_hashes = {
        "manifest": mode_seal.manifest_sha256,
        "audit": mode_seal.audit_sha256,
        "modes": mode_seal.modes_sha256,
    }
    for field_name, expected_digest in (
        ("manifest_sha256", mode_seal.manifest_sha256),
        ("audit_sha256", mode_seal.audit_sha256),
        ("modes_sha256", mode_seal.modes_sha256),
    ):
        if mode_ref.get(field_name) != expected_digest or manifest.get(
            f"mode_index_{field_name}"
        ) not in {
            None,
            expected_digest,
        }:
            _fail(f"draft market-mode {field_name} binding differs")
    mode_files = _inventory(mode_ref.get("files"), "market_mode_index.files")
    if set(mode_files) != set(mode_seal.files):
        _fail("draft market-mode file inventory differs")
    for name, entry in mode_files.items():
        expected_entry = mode_seal.files.get(name)
        if (
            expected_entry is None
            or entry.get("byte_count") != expected_entry.get("byte_count")
            or entry.get("sha256") != expected_entry.get("sha256")
        ):
            _fail(f"draft market-mode file binding differs for {name}")

    return _Draft(
        root=root,
        manifest_path=manifest_path,
        manifest_body=manifest_body,
        manifest=manifest,
        files=files,
        features_path=features_path,
        audit_path=audit_path,
        audit_body=audit_body,
        audit=audit,
        predecessor_path=predecessor_path,
        predecessor_body=predecessor_body,
        predecessor=predecessor,
        v1_manifest_sha256=v1_manifest_sha,
        v1_audit_sha256=v1_audit_sha,
        mode_manifest_sha256=mode_hashes["manifest"],
        mode_audit_sha256=mode_hashes["audit"],
        mode_modes_sha256=mode_hashes["modes"],
        config_sha256=config_sha,
        amendment_sha256=amendment_sha,
        contract=contract,
    )


def _ordering(row: Mapping[str, object]) -> tuple[int, int, int, int, str]:
    return (
        _integer(row.get("slot"), "row.slot"),
        _integer(row.get("tx_index"), "row.tx_index"),
        _integer(row.get("log_index"), "row.log_index"),
        _integer(row.get("event_index"), "row.event_index"),
        _text(row.get("event_id"), "row.event_id"),
    )


def _ordering_text(key: tuple[int, int, int, int, str]) -> str:
    return ",".join(str(item) for item in key)


def _parse_ordering_text(value: object, label: str) -> tuple[int, int, int, int, str]:
    text = _text(value, label)
    pieces = text.split(",")
    if len(pieces) != 5:
        _fail(f"{label} is not canonical ordering text")
    try:
        numbers = tuple(int(item) for item in pieces[:4])
    except ValueError:
        _fail(f"{label} contains a non-integer ordering component")
    if any(item < 0 for item in numbers):
        _fail(f"{label} contains a negative ordering component")
    return cast(tuple[int, int, int, int, str], (*numbers, pieces[4]))


def _pair(row: Mapping[str, object], base: str, quote: str) -> tuple[int | None, int | None]:
    return _optional_int(row.get(base), f"row.{base}"), _optional_int(
        row.get(quote), f"row.{quote}"
    )


def _event_local_candidate(
    post_base: int | None,
    post_quote: int | None,
    base_amount: int,
    quote_amount: int,
    side: str,
    *,
    allow_zero: bool = False,
) -> tuple[int, int] | None:
    if post_base is None or post_quote is None:
        return None
    candidate = (
        (post_base + base_amount, post_quote - quote_amount)
        if side == "buy"
        else (post_base - base_amount, post_quote + quote_amount)
    )
    if (candidate[0] < 0 or candidate[1] < 0) or (
        not allow_zero and (candidate[0] == 0 or candidate[1] == 0)
    ):
        _fail("event-local reserve algebra produced a non-positive Pump predecessor")
    return candidate


def _post_hash(row: Mapping[str, object]) -> str:
    base, quote = _pair(row, "post_base_reserve_atomic", "post_quote_reserve_atomic")
    real_base, real_quote = _pair(
        row, "post_real_base_reserve_atomic", "post_real_quote_reserve_atomic"
    )
    payload = {
        "event_id": row.get("event_id"),
        "market_id": row.get("market_id"),
        "mint": row.get("mint"),
        "quote_mint": row.get("quote_mint", ""),
        "phase": row.get("phase"),
        "ordering_key": _ordering_text(_ordering(row)),
        "post_base_atomic": base,
        "post_quote_atomic": quote,
        "post_real_base_atomic": real_base,
        "post_real_quote_atomic": real_quote,
    }
    return _sha256_bytes(canonical_json(payload))


def _post_hash_legacy_default_quote(row: Mapping[str, object]) -> str:
    """Hash form emitted by the correction for v1 rows lacking quote_mint."""

    base, quote = _pair(row, "post_base_reserve_atomic", "post_quote_reserve_atomic")
    real_base, real_quote = _pair(
        row, "post_real_base_reserve_atomic", "post_real_quote_reserve_atomic"
    )
    return _sha256_bytes(
        canonical_json(
            {
                "event_id": row.get("event_id"),
                "market_id": row.get("market_id"),
                "mint": row.get("mint"),
                "quote_mint": None,
                "phase": row.get("phase"),
                "ordering_key": _ordering_text(_ordering(row)),
                "post_base_atomic": base,
                "post_quote_atomic": quote,
                "post_real_base_atomic": real_base,
                "post_real_quote_atomic": real_quote,
            }
        )
    )


@dataclass(slots=True)
class _MaskBucket:
    """Rows sharing one family-level eligibility bit mask.

    The stream never updates one bucket per candidate.  It records a single
    mask for each family/row and projects candidate counters from these
    compact buckets once, when the final payload is assembled.
    """

    rows: int = 0
    mints: set[str] = field(default_factory=set)
    markets: set[str] = field(default_factory=set)
    segments: set[tuple[str, int]] = field(default_factory=set)

    def add(self, row: Mapping[str, object]) -> None:
        self.rows += 1
        mint = _text(row.get("mint"), "row.mint")
        market = _text(row.get("market_id"), "row.market_id")
        segment = _integer(row.get("continuity_segment"), "row.continuity_segment")
        self.mints.add(mint)
        self.markets.add(market)
        self.segments.add((market, segment))


@dataclass(slots=True)
class _FamilyMaskAccumulator:
    """Aggregate source eligibility without a per-candidate hot loop."""

    family_id: str
    signatures: tuple[tuple[str, ...], ...]
    feature_masks: dict[str, int]
    feature_names: tuple[str, ...]
    all_bits: int
    checkpoint_masks: dict[str, int] = field(default_factory=dict)
    buckets: dict[int, _MaskBucket] = field(default_factory=dict)

    def mask_for(
        self,
        valid_features: Mapping[str, bool],
        *,
        base_source_ok: bool = True,
        restrict_mask: int | None = None,
    ) -> int:
        """Return one eligibility mask using only feature-mask operations."""

        if not base_source_ok:
            return 0
        mask = self.all_bits if restrict_mask is None else restrict_mask
        for feature, signature_bits in self.feature_masks.items():
            if not valid_features.get(feature, False):
                mask &= ~signature_bits
        return mask

    def record(self, mask: int, row: Mapping[str, object]) -> None:
        if mask == 0:
            return
        bucket = self.buckets.get(mask)
        if bucket is None:
            bucket = _MaskBucket()
            self.buckets[mask] = bucket
        bucket.add(row)

    def project(self, signature: tuple[str, ...]) -> dict[str, int]:
        """Project one candidate's counters from compact mask buckets."""

        try:
            index = self.signatures.index(signature)
        except ValueError:
            return {
                "eligible_rows": 0,
                "eligible_mints": 0,
                "eligible_markets": 0,
                "eligible_segments": 0,
            }
        bit = 1 << index
        rows = 0
        mints: set[str] = set()
        markets: set[str] = set()
        segments: set[tuple[str, int]] = set()
        for mask, bucket in self.buckets.items():
            if mask & bit:
                rows += bucket.rows
                mints.update(bucket.mints)
                markets.update(bucket.markets)
                segments.update(bucket.segments)
        return {
            "eligible_rows": rows,
            "eligible_mints": len(mints),
            "eligible_markets": len(markets),
            "eligible_segments": len(segments),
        }


# Candidate registries are immutable source definitions.  Construct each
# exactly once; the streaming hot path only indexes these tuples.
_FROZEN_FAMILY_CANDIDATES: dict[str, tuple[Any, ...]] = {
    FAMILY_IDS[0]: tuple(default_shock_recovery_candidates()),
    FAMILY_IDS[1]: tuple(default_progress_speed_candidates()),
    FAMILY_IDS[2]: tuple(default_persistent_pressure_candidates()),
    FAMILY_IDS[3]: tuple(default_amm_impact_ofi_candidates()),
}


def _candidate_signature(family_id: str, candidate: Any) -> tuple[str, ...]:
    if family_id == FAMILY_IDS[0]:
        return (
            candidate.shock_feature,
            candidate.recovery_feature,
            candidate.gate_feature or "",
        )
    if family_id == FAMILY_IDS[1]:
        return (str(candidate.quote_checkpoint), candidate.speed_measure.value)
    if family_id == FAMILY_IDS[2]:
        return (candidate.pressure_feature, candidate.confirmation_feature)
    return (candidate.signal_feature,)


_FROZEN_SIGNATURE_SPECS: dict[str, tuple[tuple[tuple[str, ...], Any], ...]] = {}
for _family_id, _candidates in _FROZEN_FAMILY_CANDIDATES.items():
    _seen_signatures: dict[tuple[str, ...], Any] = {}
    for _candidate in _candidates:
        _seen_signatures.setdefault(_candidate_signature(_family_id, _candidate), _candidate)
    _FROZEN_SIGNATURE_SPECS[_family_id] = tuple(_seen_signatures.items())


def _signature_requirements(family_id: str, signature: tuple[str, ...]) -> tuple[str, ...]:
    if family_id == FAMILY_IDS[0]:
        return tuple(item for item in signature if item)
    if family_id == FAMILY_IDS[1]:
        return ("events_to_cross",) if signature[1] == "event_count" else ("seconds_to_cross",)
    if family_id == FAMILY_IDS[2]:
        return signature
    return signature


def _new_family_masks() -> dict[str, _FamilyMaskAccumulator]:
    """Freeze feature-to-signature masks once before any Parquet rows stream."""

    result: dict[str, _FamilyMaskAccumulator] = {}
    for family_id in FAMILY_IDS:
        signatures = tuple(
            signature for signature, _candidate in _FROZEN_SIGNATURE_SPECS[family_id]
        )
        feature_masks: dict[str, int] = defaultdict(int)
        for index, signature in enumerate(signatures):
            bit = 1 << index
            for feature in _signature_requirements(family_id, signature):
                feature_masks[feature] |= bit
        checkpoint_masks: dict[str, int] = {}
        if family_id == FAMILY_IDS[1]:
            for index, signature in enumerate(signatures):
                checkpoint_masks.setdefault(signature[0], 0)
                checkpoint_masks[signature[0]] |= 1 << index
        result[family_id] = _FamilyMaskAccumulator(
            family_id=family_id,
            signatures=signatures,
            feature_masks=dict(feature_masks),
            feature_names=tuple(sorted(feature_masks)),
            all_bits=(1 << len(signatures)) - 1,
            checkpoint_masks=checkpoint_masks,
        )
    return result


def _family_candidates(family_id: str) -> tuple[Any, ...]:
    try:
        return _FROZEN_FAMILY_CANDIDATES[family_id]
    except KeyError as error:
        raise IndependentFeatureAuditError(f"unknown family {family_id}") from error


def _family_sql_hashes() -> dict[str, str]:
    shock_sql = create_shock_recovery_source_sql(input_table="features", output_view="source")
    progress_sql = create_progress_crossing_source_sql(input_table="features", output_view="source")
    amm_sql = create_amm_impact_ofi_source_sql(input_table="features", output_view="source")
    return {
        FAMILY_IDS[0]: _sha256_bytes(shock_sql.encode()),
        FAMILY_IDS[1]: _sha256_bytes(progress_sql.encode()),
        # Persistent pressure intentionally reuses the exact shock source and
        # applies its frozen sign-reversed mapping in the v1 runner.
        FAMILY_IDS[2]: _sha256_bytes(shock_sql.encode()),
        FAMILY_IDS[3]: _sha256_bytes(amm_sql.encode()),
    }


def _canonical_eval(contract: CorrectionV2Contract) -> dict[str, object]:
    evaluation = asdict(contract.evaluation)
    # Decimal is not JSON-native; the contract's canonical spelling is fixed.
    wallet = cast(dict[str, object], evaluation["wallet"])
    wallet["starting_usd"] = str(wallet["starting_usd"])
    return evaluation


_RUNNER_SPECS: dict[str, tuple[str, str]] = {
    FAMILY_IDS[0]: (
        "scripts.run_shock_recovery_production_grid",
        "scripts/run_shock_recovery_production_grid.py",
    ),
    FAMILY_IDS[1]: (
        "scripts.run_progress_speed_production_grid",
        "scripts/run_progress_speed_production_grid.py",
    ),
    FAMILY_IDS[2]: (
        "scripts.run_persistent_pressure_production_grid",
        "scripts/run_persistent_pressure_production_grid.py",
    ),
    FAMILY_IDS[3]: (
        "scripts.run_amm_impact_ofi_production_grid",
        "scripts/run_amm_impact_ofi_production_grid.py",
    ),
}


def _evaluation_code_audit(
    draft: _Draft,
    *,
    retry_contract: CorrectionV2RetryContract | None = None,
    retry_config_path: Path | None = None,
    parent_source_code_inventory_sha256: str | None = None,
    parent_build_identity_sha256: str | None = None,
) -> dict[str, object]:
    """Bind frozen runner constants, evaluation policy, and source hashes."""

    contract = draft.contract
    runner_constants: dict[str, dict[str, object]] = {}
    source_files: list[dict[str, object]] = []
    for family in contract.families:
        module_name, relative = _RUNNER_SPECS[family.id]
        try:
            module = importlib.import_module(module_name)
        except Exception as error:
            _fail(f"cannot import frozen runner {module_name}: {error}")
        expected_v1_denominator = (
            _integer(
                getattr(module, "PRIOR_ATTEMPTED_CANDIDATES", -1),
                f"{family.id}.PRIOR_ATTEMPTED_CANDIDATES",
            )
            + family.candidate_count
        )
        checks: dict[str, object] = {
            "EXPERIMENT_ID": getattr(module, "EXPERIMENT_ID", None),
            "START_SLOT": getattr(module, "START_SLOT", None),
            "END_SLOT": getattr(module, "END_SLOT", None),
            "FINAL_HOLDOUT_START_SLOT": getattr(module, "FINAL_HOLDOUT_START_SLOT", None),
            "CANDIDATE_COUNT": getattr(module, "CANDIDATE_COUNT", None),
            "TRIAL_DENOMINATOR": getattr(module, "TRIAL_DENOMINATOR", None),
            "EXPECTED_TRIAL_DENOMINATOR": getattr(module, "EXPECTED_TRIAL_DENOMINATOR", None),
            "BOOTSTRAP_SEED": getattr(module, "BOOTSTRAP_SEED", None),
            "REGISTRY_PRIOR_ATTEMPTED_CANDIDATES": getattr(
                module, "REGISTRY_PRIOR_ATTEMPTED_CANDIDATES", None
            ),
            "PRIOR_ATTEMPTED_CANDIDATES": getattr(module, "PRIOR_ATTEMPTED_CANDIDATES", None),
            "v2_trial_denominator": family.cumulative_denominator,
            "v2_z_threshold": family.z_threshold,
            "expected_v1_trial_denominator": expected_v1_denominator,
        }
        if checks["EXPERIMENT_ID"] != family.v1_experiment_id:
            _fail(f"{family.id} runner experiment identity drifted")
        if (
            checks["START_SLOT"] != DEVELOPMENT_START_SLOT
            or checks["END_SLOT"] != DEVELOPMENT_END_SLOT
        ):
            _fail(f"{family.id} runner development bounds drifted")
        if checks["FINAL_HOLDOUT_START_SLOT"] != HOLDOUT_START_SLOT:
            _fail(f"{family.id} runner holdout boundary drifted")
        if checks["CANDIDATE_COUNT"] != family.candidate_count:
            _fail(f"{family.id} runner candidate count drifted")
        if checks["TRIAL_DENOMINATOR"] != expected_v1_denominator:
            _fail(f"{family.id} runner v1 trial denominator drifted")
        if checks["BOOTSTRAP_SEED"] != family.v1_experiment_id:
            _fail(f"{family.id} runner bootstrap seed drifted")
        runner_constants[family.id] = checks
        source_path = Path(relative)
        _regular(source_path, f"runner source {relative}")
        source_files.append(
            {
                "path": relative,
                "byte_count": source_path.stat().st_size,
                "sha256": sha256_file(source_path),
            }
        )
    # Include all registry/SQL modules that determine eligibility and the
    # bridge that supplies corrected-v2 runtime overrides.
    additional = [
        "src/pumpquant/audits/correction_v2/contract.py",
        "src/pumpquant/audits/correction_v2/grid.py",
        "src/pumpquant/audits/correction_v2/feature_audit.py",
        "src/pumpquant/audits/correction_v2/lineage.py",
        "scripts/seal_corrected_features_v2.py",
        "scripts/correct_shock_recovery_features_v2.py",
        "src/pumpquant/research/families/shock_recovery.py",
        "src/pumpquant/research/families/progress_speed.py",
        "src/pumpquant/research/families/persistent_pressure.py",
        "src/pumpquant/research/families/amm_impact_ofi.py",
    ]
    if retry_contract is not None:
        if retry_config_path is None:
            _fail("Retry-1 code inventory requires its config path")
        additional.extend(
            [
                "src/pumpquant/audits/correction_v2/retry_contract.py",
                retry_contract.retry_document.path,
                "scripts/run_corrected_family_grid_v2.py",
            ]
        )
    for relative in additional:
        path = Path(relative)
        _regular(path, f"source code {relative}")
        source_files.append(
            {"path": relative, "byte_count": path.stat().st_size, "sha256": sha256_file(path)}
        )
    if retry_contract is not None:
        # Keep the inventory label canonical even when the CLI received an
        # absolute path.  This makes publication and grid-time recomputation
        # produce the same identity while still hashing the verified file.
        retry_config_label = RETRY_CONFIG_PATH.as_posix()
        retry_config_file = retry_config_path
        if retry_config_file is None:
            _fail("Retry-1 code inventory requires its config path")
        source_files.append(
            {
                "path": retry_config_label,
                "byte_count": retry_config_file.stat().st_size,
                "sha256": sha256_file(retry_config_file),
            }
        )
    source_files.sort(key=lambda item: cast(str, item["path"]))
    inventory_sha = _sha256_bytes(canonical_json(source_files))
    identity_payload: dict[str, object] = {
        "config_sha256": draft.config_sha256,
        "amendment_sha256": draft.amendment_sha256,
        "source_inventory_sha256": inventory_sha,
        "contract_kind": contract.kind,
        "contract_version": contract.version,
    }
    if retry_contract is not None:
        if parent_source_code_inventory_sha256 is None or parent_build_identity_sha256 is None:
            _fail("Retry-1 build identity requires parent code identities")
        identity_payload.update(
            {
                "technical_retry_id": retry_contract.retry_id,
                "retry_config_sha256": retry_contract.config_sha256,
                "retry_document_sha256": retry_contract.retry_document.sha256,
                "parent_source_code_inventory_sha256": parent_source_code_inventory_sha256,
                "parent_build_identity_sha256": parent_build_identity_sha256,
            }
        )
    build_identity = _sha256_bytes(canonical_json(identity_payload))
    # If the draft carries an explicit build/inventory identity, it is a hard
    # binding.  Older draft roots omit these fields and are still auditable.
    for field_name, expected in (
        ("source_code_inventory_sha256", inventory_sha),
        ("build_identity_sha256", build_identity),
    ):
        declared = draft.manifest.get(field_name)
        if declared is not None and declared != expected:
            _fail(f"draft {field_name} differs from the current source build")
    result: dict[str, object] = {
        "evaluation_policy": _canonical_eval(contract),
        "contract_sha256": draft.config_sha256,
        "amendment_sha256": draft.amendment_sha256,
        "runner_constants": runner_constants,
        "source_files": source_files,
        "source_code_inventory_sha256": inventory_sha,
        "build_identity_sha256": build_identity,
        "runtime_v2_overrides": {
            family.id: {
                "TRIAL_DENOMINATOR": family.cumulative_denominator,
                "Z_THRESHOLD": family.z_threshold,
                "BOOTSTRAP_SEED": family.bootstrap_seed,
                "candidate_order_sha256": family.candidate_order_sha256,
                "registry_sha256": family.registry_sha256,
            }
            for family in contract.families
        },
    }
    if retry_contract is not None:
        result["technical_retry"] = {
            "retry_id": retry_contract.retry_id,
            "retry_config_sha256": retry_contract.config_sha256,
            "retry_document_sha256": retry_contract.retry_document.sha256,
            "parent_source_code_inventory_sha256": parent_source_code_inventory_sha256,
            "parent_build_identity_sha256": parent_build_identity_sha256,
        }
    return result


@dataclass(slots=True)
class _StreamState:
    row_count: int = 0
    event_ids: set[str] = field(default_factory=set)
    first_global: tuple[int, int, int, int, str] | None = None
    previous_global: tuple[int, int, int, int, str] | None = None
    previous_available_at_ns: int | None = None
    availability_regressions: int = 0
    previous_by_market: dict[str, dict[str, object]] = field(default_factory=dict)
    phase_counts: Counter[str] = field(default_factory=Counter)
    phase_details: dict[str, dict[str, object]] = field(default_factory=dict)
    reserve_source_counts: Counter[str] = field(default_factory=Counter)
    reason_counts: Counter[str] = field(default_factory=Counter)
    quote_mode_counts: Counter[str] = field(default_factory=Counter)
    segment_counts: Counter[str] = field(default_factory=Counter)
    coverage_counts: Counter[str] = field(default_factory=Counter)
    mints: set[str] = field(default_factory=set)
    markets: set[str] = field(default_factory=set)
    family_digests: dict[str, Any] = field(
        default_factory=lambda: {family_id: hashlib.sha256() for family_id in FAMILY_IDS}
    )
    family_masks: dict[str, _FamilyMaskAccumulator] = field(default_factory=_new_family_masks)
    family_candidates: dict[str, tuple[Any, ...]] = field(default_factory=dict)
    family_mask_updates: int = 0
    candidate_registry_row_iterations: int = 0
    candidate_row_digest_updates: int = 0
    row_identity_digest: Any = field(default_factory=hashlib.sha256)
    decision_digest: Any = field(default_factory=hashlib.sha256)
    progress_seen: dict[tuple[str, float], bool] = field(default_factory=dict)
    progress_first_pre: dict[tuple[str, int], int | None] = field(default_factory=dict)
    progress_event_counts: dict[tuple[str, int], int] = field(
        default_factory=lambda: defaultdict(int)
    )
    progress_crossings: dict[tuple[str, float], dict[str, object]] = field(default_factory=dict)
    rolling_depth: dict[tuple[str, str, int], deque[float | None]] = field(default_factory=dict)
    rolling_impact: dict[tuple[str, str, int], deque[float | None]] = field(default_factory=dict)
    family_sql_hashes: dict[str, str] = field(default_factory=_family_sql_hashes)


def _new_family_audits() -> dict[str, tuple[Any, ...]]:
    return {family_id: _family_candidates(family_id) for family_id in FAMILY_IDS}


def _digest_family_row(
    state: _StreamState,
    family_id: str,
    row: Mapping[str, object],
    mask: int,
    values: Mapping[str, object],
) -> None:
    """Add one canonical row/mask record to a family digest.

    This is deliberately one update per family/row.  Candidate-level
    observation hashes are projected from the family digest at finalization.
    """

    if mask == 0:
        return
    state.family_digests[family_id].update(
        canonical_json(
            {
                "event_id": row.get("event_id"),
                "ordering_key": _ordering_text(_ordering(row)),
                "eligible_mask": mask,
                "values": dict(sorted(values.items())),
            }
        )
    )


def _record_family_mask(
    state: _StreamState,
    family_id: str,
    row: Mapping[str, object],
    *,
    base_source_ok: bool = True,
    valid_features: Mapping[str, bool] | None = None,
    values: Mapping[str, object] | None = None,
    restrict_mask: int | None = None,
) -> int:
    """Record one family mask and one family digest update at most."""

    accumulator = state.family_masks[family_id]
    if valid_features is None:
        observed: dict[str, bool] = {}
        observed_values: dict[str, object] = {}
        for feature in accumulator.feature_names:
            value = row.get(feature)
            valid = _valid_feature(value)
            observed[feature] = valid
            if valid:
                observed_values[feature] = value
        valid_features = observed
        values = observed_values
    mask = accumulator.mask_for(
        valid_features,
        base_source_ok=base_source_ok,
        restrict_mask=restrict_mask,
    )
    state.family_mask_updates += 1
    accumulator.record(mask, row)
    _digest_family_row(state, family_id, row, mask, values or {})
    return mask


def _required_schema(path: Path, manifest: Mapping[str, object]) -> pq.ParquetFile:
    parquet = pq.ParquetFile(path)
    expected_schema_sha = manifest.get("schema_sha256")
    actual_schema_sha = _sha256_bytes(parquet.schema_arrow.serialize().to_pybytes())
    if expected_schema_sha is not None and expected_schema_sha != actual_schema_sha:
        _fail("corrected feature schema differs from the draft manifest")
    required = {
        "event_id",
        "market_id",
        "mint",
        "quote_mint",
        "slot",
        "tx_index",
        "log_index",
        "event_index",
        "block_time_ns",
        "available_at_ns",
        "launch_time_ns",
        "phase",
        "side",
        "base_amount_atomic",
        "quote_amount_atomic",
        "base_decimals",
        "quote_decimals",
        "effective_pre_base_atomic",
        "effective_pre_quote_atomic",
        "effective_pre_real_base_atomic",
        "effective_pre_real_quote_atomic",
        "post_base_reserve_atomic",
        "post_quote_reserve_atomic",
        "post_virtual_base_reserve_atomic",
        "post_virtual_quote_reserve_atomic",
        "post_real_base_reserve_atomic",
        "post_real_quote_reserve_atomic",
        "coverage_gap",
        "reserve_continuity",
        "real_reserve_continuity",
        "continuity_segment",
        "predecessor_event_id",
        "predecessor_ordering_key",
        "predecessor_market_id",
        "predecessor_mint",
        "predecessor_quote_mint",
        "predecessor_phase",
        "predecessor_post_state_sha256",
        "predecessor_post_base_atomic",
        "predecessor_post_quote_atomic",
        "reserve_source",
        "predecessor_reason",
        "predecessor_identity_bound",
        "predecessor_reserve_equality_checked",
    }
    missing = sorted(required - set(parquet.schema_arrow.names))
    if missing:
        _fail("corrected feature schema is missing: " + ", ".join(missing))
    return parquet


def _valid_feature(value: object) -> bool:
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return math.isfinite(float(value))


def _audit_row(row: dict[str, object], state: _StreamState) -> None:
    key = _ordering(row)
    if state.previous_global is not None and key <= state.previous_global:
        _fail("corrected feature stream is not strictly canonical ordered")
    state.previous_global = key
    if state.first_global is None:
        state.first_global = key
    event_id = key[4]
    if event_id in state.event_ids:
        _fail("corrected feature stream contains duplicate event_id")
    state.event_ids.add(event_id)
    state.row_count += 1
    if not DEVELOPMENT_START_SLOT <= key[0] < DEVELOPMENT_END_SLOT:
        _fail("corrected feature row escapes the development interval")
    market_id = _text(row.get("market_id"), "row.market_id")
    mint = _text(row.get("mint"), "row.mint")
    quote_mint = _text(row.get("quote_mint"), "row.quote_mint")
    phase = _text(row.get("phase"), "row.phase")
    side = _text(row.get("side"), "row.side")
    if phase not in _KNOWN_PHASES:
        _fail(f"row.phase is unknown: {phase}")
    if side not in {"buy", "sell"}:
        _fail(f"row.side is unknown: {side}")
    for name in (
        "tx_index",
        "log_index",
        "event_index",
        "block_time_ns",
        "available_at_ns",
        "launch_time_ns",
    ):
        _integer(row.get(name), f"row.{name}")
    available_at_ns = _integer(row.get("available_at_ns"), "row.available_at_ns")
    if (
        state.previous_available_at_ns is not None
        and available_at_ns < state.previous_available_at_ns
    ):
        state.availability_regressions += 1
        _fail("corrected feature availability regressed in canonical order")
    state.previous_available_at_ns = available_at_ns
    if _integer(row["launch_time_ns"], "row.launch_time_ns") > _integer(
        row["block_time_ns"], "row.block_time_ns"
    ):
        _fail("row launch_time_ns is after block_time_ns")
    for name in ("base_amount_atomic", "quote_amount_atomic"):
        _integer(row.get(name), f"row.{name}", minimum=1)
    for name in ("base_decimals", "quote_decimals"):
        _integer(row.get(name), f"row.{name}")
    segment = _integer(row.get("continuity_segment"), "row.continuity_segment")
    coverage_gap = _bool(row.get("coverage_gap"), "row.coverage_gap")
    continuity = _bool(row.get("reserve_continuity"), "row.reserve_continuity")
    real_continuity = _bool(row.get("real_reserve_continuity"), "row.real_reserve_continuity")
    predecessor_bound = _bool(
        row.get("predecessor_identity_bound"), "row.predecessor_identity_bound"
    )
    equality_checked = _bool(
        row.get("predecessor_reserve_equality_checked"),
        "row.predecessor_reserve_equality_checked",
    )
    reserve_source = _text(row.get("reserve_source"), "row.reserve_source")
    reason = _text(row.get("predecessor_reason"), "row.predecessor_reason")
    if reserve_source not in _RESERVE_SOURCES or reason not in _REASONS:
        _fail("row reserve_source or predecessor_reason is outside the frozen vocabulary")
    previous = state.previous_by_market.get(market_id)
    same_identity = bool(
        previous is not None
        and previous.get("phase") == phase
        and previous.get("mint") == mint
        and previous.get("quote_mint") == quote_mint
    )
    if previous is None:
        if segment != 0:
            _fail("first row for a market must start at continuity segment zero")
    else:
        previous_segment = _integer(previous.get("continuity_segment"), "previous segment")
        if segment < previous_segment:
            _fail("continuity segment regressed")
        if continuity and (not same_identity or segment != previous_segment):
            _fail("continuous row changed identity or continuity segment")
        if not continuity and same_identity and segment <= previous_segment:
            _fail("non-continuous row did not advance its continuity segment")
        if not same_identity and segment <= previous_segment:
            _fail("phase/mint transition did not advance its continuity segment")
    pred_fields = {
        "predecessor_event_id": row.get("predecessor_event_id"),
        "predecessor_ordering_key": row.get("predecessor_ordering_key"),
        "predecessor_market_id": row.get("predecessor_market_id"),
        "predecessor_mint": row.get("predecessor_mint"),
        "predecessor_quote_mint": row.get("predecessor_quote_mint"),
        "predecessor_phase": row.get("predecessor_phase"),
        "predecessor_post_state_sha256": row.get("predecessor_post_state_sha256"),
        "predecessor_post_base_atomic": row.get("predecessor_post_base_atomic"),
        "predecessor_post_quote_atomic": row.get("predecessor_post_quote_atomic"),
    }
    if predecessor_bound:
        if previous is None or not same_identity or not continuity:
            _fail("predecessor identity is bound without the immediate same-identity row")
        previous_key = _ordering(previous)
        if pred_fields["predecessor_event_id"] != previous_key[4]:
            _fail("predecessor event identity differs from the immediate prior row")
        if (
            _parse_ordering_text(
                pred_fields["predecessor_ordering_key"], "predecessor_ordering_key"
            )
            != previous_key
        ):
            _fail("predecessor ordering key differs from the immediate prior row")
        for field_name, expected in (
            ("predecessor_market_id", market_id),
            ("predecessor_mint", mint),
            ("predecessor_quote_mint", quote_mint),
            ("predecessor_phase", phase),
            ("predecessor_post_base_atomic", previous.get("post_base_reserve_atomic")),
            ("predecessor_post_quote_atomic", previous.get("post_quote_reserve_atomic")),
        ):
            if pred_fields[field_name] != expected:
                _fail(f"{field_name} differs from the immediate prior row")
        previous_hash = _post_hash(previous)
        accepted_hashes = {previous_hash}
        if previous.get("quote_mint") == "SOL":
            accepted_hashes.add(_post_hash_legacy_default_quote(previous))
        if pred_fields["predecessor_post_state_sha256"] not in accepted_hashes:
            _fail("predecessor post-state hash differs from the immediate prior row")
        if not equality_checked:
            _fail("predecessor identity is bound without a reserve equality check")
    else:
        if any(value is not None for value in pred_fields.values()):
            _fail("unbound predecessor carries identity or reserve metadata")
    effective_pre_base, effective_pre_quote = _pair(
        row, "effective_pre_base_atomic", "effective_pre_quote_atomic"
    )
    if (effective_pre_base is None) != (effective_pre_quote is None):
        _fail("effective pre reserve pair is incomplete")
    post_virtual_base, post_virtual_quote = _pair(
        row, "post_virtual_base_reserve_atomic", "post_virtual_quote_reserve_atomic"
    )
    post_base, post_quote = _pair(row, "post_base_reserve_atomic", "post_quote_reserve_atomic")
    if (post_base is None) != (post_quote is None):
        _fail("published post reserve pair is incomplete")
    post_real_base, post_real_quote = _pair(
        row, "post_real_base_reserve_atomic", "post_real_quote_reserve_atomic"
    )
    if (post_real_base is None) != (post_real_quote is None):
        _fail("published real post reserve pair is incomplete")
    amount_base = _integer(row.get("base_amount_atomic"), "row.base_amount_atomic", minimum=1)
    amount_quote = _integer(row.get("quote_amount_atomic"), "row.quote_amount_atomic", minimum=1)
    effective_pre_real_base, effective_pre_real_quote = _pair(
        row, "effective_pre_real_base_atomic", "effective_pre_real_quote_atomic"
    )
    if (effective_pre_real_base is None) != (effective_pre_real_quote is None):
        _fail("effective real pre reserve pair is incomplete")
    virtual_candidate: tuple[int, int] | None = None
    real_candidate: tuple[int, int] | None = None
    if phase == VenuePhase.BONDING_CURVE.value:
        if (post_virtual_base is None) != (post_virtual_quote is None):
            _fail("Pump virtual post reserve pair is incomplete")
        if (
            post_virtual_base is None
            and post_virtual_quote is None
            and reason != "missing_post_state"
        ):
            _fail("Pump row is missing its executable virtual post reserves")
        if (
            post_virtual_base is None
            and post_virtual_quote is None
            and (post_base is not None or post_quote is not None)
        ):
            _fail("Pump generic post reserves are present without a virtual post state")
        if post_virtual_base is not None and (
            post_base != post_virtual_base or post_quote != post_virtual_quote
        ):
            _fail("Pump generic post reserves are not bound to virtual post reserves")
        virtual_candidate = _event_local_candidate(
            post_virtual_base, post_virtual_quote, amount_base, amount_quote, side
        )
        real_candidate = _event_local_candidate(
            post_real_base,
            post_real_quote,
            amount_base,
            amount_quote,
            side,
            allow_zero=True,
        )
        if (
            real_candidate is not None
            and (
                effective_pre_real_base,
                effective_pre_real_quote,
            )
            != real_candidate
        ):
            _fail("Pump real pre reserve differs from event-local real algebra")
        prior_virtual = (
            _pair(previous, "post_virtual_base_reserve_atomic", "post_virtual_quote_reserve_atomic")
            if previous is not None and same_identity
            else (None, None)
        )
        prior_real = (
            _pair(previous, "post_real_base_reserve_atomic", "post_real_quote_reserve_atomic")
            if previous is not None and same_identity
            else (None, None)
        )
        if reason == "continuous":
            if (
                not equality_checked
                or virtual_candidate is None
                or prior_virtual != virtual_candidate
            ):
                _fail("Pump continuous reason is not backed by exact virtual reserve equality")
            if (
                effective_pre_base is None
                or (effective_pre_base, effective_pre_quote) != virtual_candidate
            ):
                _fail("Pump continuous row has the wrong event-local virtual predecessor")
            if reserve_source != "pump_event_local_candidate":
                _fail("Pump continuous row has an invalid reserve source")
        elif reason == "reserve_mismatch":
            if not equality_checked or virtual_candidate is None or prior_virtual == (None, None):
                _fail("Pump reserve_mismatch lacks a comparable prior virtual state")
            if virtual_candidate == prior_virtual:
                _fail("Pump reserve_mismatch row actually matches prior virtual reserves")
            if effective_pre_base is not None or reserve_source != "unavailable":
                _fail("Pump reserve_mismatch row exposes a virtual predecessor")
        elif reason == "real_reserve_mismatch":
            if (
                not equality_checked
                or virtual_candidate is None
                or virtual_candidate != prior_virtual
            ):
                _fail("Pump real_reserve_mismatch lacks exact virtual equality")
            if real_candidate is None or real_candidate == prior_real:
                _fail("Pump real_reserve_mismatch lacks an exact real reserve mismatch")
            if effective_pre_base is not None or reserve_source != "unavailable":
                _fail("Pump real_reserve_mismatch row exposes a virtual predecessor")
        elif reason in {
            "segment_start",
            "coverage_gap",
            "phase_transition",
            "missing_predecessor",
            "missing_post_state",
        }:
            if equality_checked:
                _fail("Pump non-comparison reason unexpectedly performed equality checking")
            if effective_pre_base is not None or reserve_source != "unavailable":
                _fail("Pump non-comparison row exposes a virtual predecessor")
        else:
            _fail("unsupported Pump predecessor reason")
        if real_continuity:
            if real_candidate is None or prior_real == (None, None) or real_candidate != prior_real:
                _fail("Pump real reserve continuity does not match prior real post state")
            if (
                effective_pre_real_base is None
                or (
                    effective_pre_real_base,
                    effective_pre_real_quote,
                )
                != real_candidate
            ):
                _fail("Pump real pre reserve differs from event-local real algebra")
        elif reason == "continuous" and real_candidate is not None and real_candidate != prior_real:
            _fail("Pump continuous reason hides a real reserve mismatch")
    elif phase == VenuePhase.PUMPSWAP.value:
        virtual_candidate = (
            None
            if effective_pre_base is None or effective_pre_quote is None
            else (effective_pre_base, effective_pre_quote)
        )
        if continuity and (effective_pre_base is None or effective_pre_quote is None):
            _fail("PumpSwap continuous row has no persisted pre reserves")
        if effective_pre_base is None:
            if reserve_source != "unavailable":
                _fail("PumpSwap missing pre reserves have an available source")
        elif reserve_source != "pumpswap_persisted_raw":
            _fail("PumpSwap pre reserves are not bound to persisted raw state")
        raw_pre = (effective_pre_base, effective_pre_quote)
        prior_persisted = (
            _pair(previous, "post_base_reserve_atomic", "post_quote_reserve_atomic")
            if previous is not None and same_identity
            else (None, None)
        )
        if reason == "continuous":
            if not equality_checked or raw_pre != prior_persisted:
                _fail("PumpSwap continuous reason lacks exact persisted-pre equality")
        elif reason == "reserve_mismatch":
            if not equality_checked or raw_pre == (None, None) or prior_persisted == raw_pre:
                _fail("PumpSwap reserve_mismatch lacks exact persisted-pre mismatch")
        elif equality_checked:
            _fail("PumpSwap non-comparison reason unexpectedly checked equality")
    if real_continuity and not continuity:
        _fail("real reserve continuity cannot hold without virtual continuity")
    if continuity and coverage_gap:
        _fail("reserve continuity row is marked as a coverage gap")
    expected_coverage = (
        reason != "continuous"
        if phase == VenuePhase.BONDING_CURVE.value
        else reason
        not in {
            "segment_start",
            "continuous",
        }
    )
    if coverage_gap != expected_coverage:
        _fail("coverage_gap differs from frozen reason semantics")
    if continuity != predecessor_bound:
        _fail("reserve_continuity and predecessor identity are inconsistent")
    state.row_identity_digest.update(
        canonical_json(
            {
                "event_id": event_id,
                "ordering_key": _ordering_text(key),
                "market_id": market_id,
                "mint": mint,
                "quote_mint": quote_mint,
                "phase": phase,
            }
        )
    )
    state.decision_digest.update(
        canonical_json(
            {
                "event_id": event_id,
                "ordering_key": _ordering_text(key),
                "market_id": market_id,
                "mint": mint,
                "phase": phase,
                "continuity_segment": segment,
                "reason": reason,
                "reserve_continuity": continuity,
                "real_reserve_continuity": real_continuity,
                "equality_checked": equality_checked,
                "candidate_virtual_pre": virtual_candidate,
                "candidate_real_pre": real_candidate,
                "published_pre": (effective_pre_base, effective_pre_quote),
                "published_real_pre": (effective_pre_real_base, effective_pre_real_quote),
                "predecessor_event_id": pred_fields["predecessor_event_id"],
                "predecessor_ordering_key": pred_fields["predecessor_ordering_key"],
                "predecessor_post_base_atomic": pred_fields["predecessor_post_base_atomic"],
                "predecessor_post_quote_atomic": pred_fields["predecessor_post_quote_atomic"],
            }
        )
    )
    state.phase_counts[phase] += 1
    state.reserve_source_counts[reserve_source] += 1
    state.reason_counts[reason] += 1
    state.quote_mode_counts[quote_mint] += 1
    state.segment_counts[str(segment)] += 1
    state.coverage_counts["coverage_gap" if coverage_gap else "continuous_or_segment"] += 1
    state.mints.add(mint)
    state.markets.add(market_id)
    detail = state.phase_details.setdefault(
        phase,
        {
            "rows": 0,
            "coverage_gap_rows": 0,
            "reserve_continuity_rows": 0,
            "real_reserve_continuity_rows": 0,
            "continuity_segments": set(),
            "mints": set(),
            "markets": set(),
            "quote_mints": set(),
            "usable_pre_post_rows": 0,
            "predecessor_identity_rows": 0,
            "reserve_equality_checks": 0,
            "reserve_source_counts": Counter(),
            "reason_counts": Counter(),
        },
    )
    detail["rows"] = int(cast(int, detail["rows"])) + 1
    detail["coverage_gap_rows"] = int(cast(int, detail["coverage_gap_rows"])) + int(coverage_gap)
    detail["reserve_continuity_rows"] = int(cast(int, detail["reserve_continuity_rows"])) + int(
        continuity
    )
    detail["real_reserve_continuity_rows"] = int(
        cast(int, detail["real_reserve_continuity_rows"])
    ) + int(real_continuity)
    cast(set[tuple[str, int]], detail["continuity_segments"]).add((market_id, segment))
    cast(set[str], detail["mints"]).add(mint)
    cast(set[str], detail["markets"]).add(market_id)
    cast(set[str], detail["quote_mints"]).add(quote_mint)
    if (
        continuity
        and effective_pre_base is not None
        and effective_pre_quote is not None
        and post_base is not None
        and post_quote is not None
    ):
        detail["usable_pre_post_rows"] = int(cast(int, detail["usable_pre_post_rows"])) + 1
    detail["predecessor_identity_rows"] = int(cast(int, detail["predecessor_identity_rows"])) + int(
        predecessor_bound
    )
    detail["reserve_equality_checks"] = int(cast(int, detail["reserve_equality_checks"])) + int(
        equality_checked
    )
    cast(Counter[str], detail["reserve_source_counts"])[reserve_source] += 1
    cast(Counter[str], detail["reason_counts"])[reason] += 1
    previous_by_market = state.previous_by_market
    previous_by_market[market_id] = row
    # Shock-recovery and persistent-pressure eligibility are entirely past-only
    # source predicates; no labels or strategy outcomes are read.
    base_source_ok = not coverage_gap and continuity
    for family_id in (FAMILY_IDS[0], FAMILY_IDS[2]):
        accumulator = state.family_masks[family_id]
        valid_features: dict[str, bool] = {}
        values: dict[str, object] = {}
        for feature in accumulator.feature_names:
            value = row.get(feature)
            valid = _valid_feature(value)
            valid_features[feature] = valid
            if valid:
                values[feature] = value
        _record_family_mask(
            state,
            family_id,
            row,
            base_source_ok=base_source_ok,
            valid_features=valid_features,
            values=values,
        )
    _audit_progress(row, state)
    _audit_amm(row, state)


def _audit_progress(row: Mapping[str, object], state: _StreamState) -> None:
    if row.get("phase") != VenuePhase.BONDING_CURVE.value:
        return
    if _integer(row.get("continuity_segment"), "row.continuity_segment") != 0:
        return
    if bool(row.get("coverage_gap")) or not bool(row.get("reserve_continuity")):
        return
    market = _text(row.get("market_id"), "row.market_id")
    segment_key = (market, 0)
    if segment_key not in state.progress_first_pre:
        state.progress_first_pre[segment_key] = _optional_int(
            row.get("effective_pre_quote_atomic"), "row.effective_pre_quote_atomic"
        )
        state.progress_event_counts[segment_key] = 0
    state.progress_event_counts[segment_key] += 1
    launch_pre = state.progress_first_pre[segment_key]
    post_quote = _optional_int(
        row.get("post_quote_reserve_atomic"), "row.post_quote_reserve_atomic"
    )
    quote_decimals = _integer(row.get("quote_decimals"), "row.quote_decimals")
    block_time = _integer(row.get("block_time_ns"), "row.block_time_ns")
    launch_time = _integer(row.get("launch_time_ns"), "row.launch_time_ns")
    if launch_pre is None or post_quote is None or block_time < launch_time:
        return
    net_quote = (post_quote - launch_pre) / (10.0**quote_decimals)
    seconds = (block_time - launch_time) / 1_000_000_000.0
    if not math.isfinite(net_quote) or not math.isfinite(seconds) or seconds < 0.0:
        return
    for checkpoint in (1.0, 5.0, 15.0, 30.0):
        marker = (market, checkpoint)
        if marker in state.progress_crossings or net_quote < checkpoint:
            continue
        state.progress_crossings[marker] = dict(row)
        event_value = float(state.progress_event_counts[segment_key])
        progress_values = {
            "events_to_cross": event_value,
            "seconds_to_cross": seconds,
        }
        progress_valid = {name: _valid_feature(value) for name, value in progress_values.items()}
        _record_family_mask(
            state,
            FAMILY_IDS[1],
            row,
            valid_features=progress_valid,
            values=progress_values,
            restrict_mask=state.family_masks[FAMILY_IDS[1]].checkpoint_masks[str(checkpoint)],
        )


def _audit_amm(row: Mapping[str, object], state: _StreamState) -> None:
    market = _text(row.get("market_id"), "row.market_id")
    phase = _text(row.get("phase"), "row.phase")
    segment = _integer(row.get("continuity_segment"), "row.continuity_segment")
    partition = (market, phase, segment)
    depth_value: float | None = None
    impact_value: float | None = None
    if (
        not bool(row.get("coverage_gap"))
        and bool(row.get("reserve_continuity"))
        and row.get("effective_pre_quote_atomic") is not None
        and _integer(
            row.get("effective_pre_quote_atomic"), "row.effective_pre_quote_atomic", minimum=1
        )
        > 0
        and _integer(row.get("quote_amount_atomic"), "row.quote_amount_atomic", minimum=1) > 0
        and row.get("side") in {"buy", "sell"}
    ):
        depth_value = (
            (1.0 if row.get("side") == "buy" else -1.0)
            * _integer(row.get("quote_amount_atomic"), "row.quote_amount_atomic", minimum=1)
            / _integer(
                row.get("effective_pre_quote_atomic"), "row.effective_pre_quote_atomic", minimum=1
            )
        )
    gross = row.get("gross_directional_impact_bps")
    if (
        not bool(row.get("coverage_gap"))
        and bool(row.get("reserve_continuity"))
        and _valid_feature(gross)
        and row.get("side") in {"buy", "sell"}
    ):
        impact_value = (
            (1.0 if row.get("side") == "buy" else -1.0) * abs(float(cast(float, gross))) / 10_000.0
        )
    for values, value in ((state.rolling_depth, depth_value), (state.rolling_impact, impact_value)):
        history = values.setdefault(partition, deque())
        history.append(value)
        while len(history) > 20:
            history.popleft()
    depth5 = list(state.rolling_depth[partition])[-5:]
    depth20 = list(state.rolling_depth[partition])[-20:]
    impact5 = list(state.rolling_impact[partition])[-5:]
    impact20 = list(state.rolling_impact[partition])[-20:]
    features: dict[str, float | None] = {}
    for stem, window5, window20 in (
        ("amm_depth", depth5, depth20),
        ("amm_impact", impact5, impact20),
    ):
        for length, window in ((5, window5), (20, window20)):
            if len(window) == length and all(item is not None for item in window):
                numeric = [float(cast(float, item)) for item in window]
                net = sum(numeric)
                gross_value = sum(abs(item) for item in numeric)
                features[f"{stem}_pressure_{length}e"] = net
                features[f"{stem}_imbalance_{length}e"] = (
                    net / gross_value if gross_value > 0.0 else None
                )
    amm_valid = {name: _valid_feature(value) for name, value in features.items()}
    _record_family_mask(
        state,
        FAMILY_IDS[3],
        row,
        valid_features=amm_valid,
        values={name: value for name, value in features.items() if _valid_feature(value)},
    )


def _family_payload(state: _StreamState) -> dict[str, dict[str, object]]:
    sql_hashes = state.family_sql_hashes
    result: dict[str, dict[str, object]] = {}
    for family_id in FAMILY_IDS:
        candidates = state.family_candidates[family_id]
        accumulator = state.family_masks[family_id]
        family_digest = state.family_digests[family_id].hexdigest()
        entries: list[dict[str, object]] = []
        for candidate in candidates:
            signature = _candidate_signature(family_id, candidate)
            projected = accumulator.project(signature)
            candidate_payload: dict[str, object] = {
                "candidate_sha256": candidate.sha256,
                **projected,
            }
            candidate_payload["eligibility_signature"] = list(signature)
            candidate_payload["eligible_observation_sha256"] = _sha256_bytes(
                canonical_json(
                    {
                        "family_digest": family_digest,
                        "eligibility_signature": list(signature),
                    }
                )
            )
            entries.append(candidate_payload)
        payload: dict[str, object] = {
            "family_id": family_id,
            "family_version": 2,
            "candidate_count": len(candidates),
            "candidate_order": [candidate.sha256 for candidate in candidates],
            "candidate_order_sha256": _sha256_bytes(
                "\n".join(candidate.sha256 for candidate in candidates).encode()
            ),
            "eligibility_sql_sha256": sql_hashes[family_id],
            "eligibility_row_sha256": family_digest,
            "candidates": entries,
            "total_eligible_rows": sum(
                _integer(item["eligible_rows"], "candidate eligible_rows") for item in entries
            ),
            "total_eligible_mints": sum(
                _integer(item["eligible_mints"], "candidate eligible_mints") for item in entries
            ),
            "total_eligible_markets": sum(
                _integer(item["eligible_markets"], "candidate eligible_markets") for item in entries
            ),
            "viability_audit_sha256": _sha256_bytes(
                canonical_json(
                    {
                        "family_id": family_id,
                        "row_digest": family_digest,
                        "candidates": entries,
                    }
                )
            ),
        }
        payload["audit_sha256"] = _sha256_bytes(canonical_json(payload))
        result[family_id] = payload
    return result


def _stream_features(
    draft: _Draft, *, batch_size: int
) -> tuple[dict[str, object], dict[str, dict[str, object]], dict[str, object]]:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    parquet = _required_schema(draft.features_path, draft.manifest)
    state = _StreamState(family_candidates=_new_family_audits())
    expected_rows = _integer(draft.manifest.get("row_count"), "draft row_count")
    stream_columns = tuple(
        sorted(
            {
                "event_id",
                "market_id",
                "mint",
                "quote_mint",
                "slot",
                "tx_index",
                "log_index",
                "event_index",
                "block_time_ns",
                "available_at_ns",
                "launch_time_ns",
                "phase",
                "side",
                "base_amount_atomic",
                "quote_amount_atomic",
                "base_decimals",
                "quote_decimals",
                "effective_pre_base_atomic",
                "effective_pre_quote_atomic",
                "effective_pre_real_base_atomic",
                "effective_pre_real_quote_atomic",
                "post_base_reserve_atomic",
                "post_quote_reserve_atomic",
                "post_virtual_base_reserve_atomic",
                "post_virtual_quote_reserve_atomic",
                "post_real_base_reserve_atomic",
                "post_real_quote_reserve_atomic",
                "coverage_gap",
                "reserve_continuity",
                "real_reserve_continuity",
                "continuity_segment",
                "predecessor_event_id",
                "predecessor_ordering_key",
                "predecessor_market_id",
                "predecessor_mint",
                "predecessor_quote_mint",
                "predecessor_phase",
                "predecessor_post_state_sha256",
                "predecessor_post_base_atomic",
                "predecessor_post_quote_atomic",
                "reserve_source",
                "predecessor_reason",
                "predecessor_identity_bound",
                "predecessor_reserve_equality_checked",
                "gross_directional_impact_bps",
                "shock_price_5e",
                "shock_price_20e",
                "shock_flow_5e",
                "shock_flow_20e",
                "recovery_price_2e",
                "recovery_flow_2e",
                "sell_excitation_relaxation_5e",
                "sell_excitation_relaxation_20e",
            }
        )
    )
    for batch in parquet.iter_batches(batch_size=batch_size, columns=stream_columns):
        for row_value in batch.to_pylist():
            row = cast(dict[str, object], row_value)
            _audit_row(row, state)
    if state.row_count != expected_rows:
        _fail("streamed feature row count differs from the draft manifest")
    # Phase rows are populated directly from counters updated during the
    # independent stream.
    phase_rows: dict[str, dict[str, object]] = {}
    # Keep exact per-phase counters without retaining rows by counting the
    # canonical digest stream in a tiny third set of counters during the scan.
    # The row-level audit above already validated every field; derive reliable
    # phase totals from the draft audit only after checking its row count below.
    for phase, detail_value in sorted(state.phase_details.items()):
        detail = detail_value
        phase_rows[phase] = {
            "rows": detail["rows"],
            "coverage_gap_rows": detail["coverage_gap_rows"],
            "reserve_continuity_rows": detail["reserve_continuity_rows"],
            "real_reserve_continuity_rows": detail["real_reserve_continuity_rows"],
            "distinct_mints": len(cast(set[str], detail["mints"])),
            "distinct_markets": len(cast(set[str], detail["markets"])),
            "quote_mints": sorted(cast(set[str], detail["quote_mints"])),
            "usable_pre_post_rows": detail["usable_pre_post_rows"],
            "predecessor_identity_rows": detail["predecessor_identity_rows"],
            "reserve_equality_checks": detail["reserve_equality_checks"],
            "continuity_segment_count": len(
                cast(set[tuple[str, int]], detail["continuity_segments"])
            ),
            "reserve_source_counts": dict(
                sorted(cast(Counter[str], detail["reserve_source_counts"]).items())
            ),
            "reason_counts": dict(sorted(cast(Counter[str], detail["reason_counts"]).items())),
        }
        if (
            int(cast(int, detail["rows"])) > 0
            and int(cast(int, detail["reserve_continuity_rows"])) == 0
        ):
            _fail(f"phase {phase} has no causally continuous reserve rows")
    independent: dict[str, object] = {
        "kind": KIND,
        "version": VERSION,
        "status": "verified",
        "development_only": True,
        "start_slot_inclusive": DEVELOPMENT_START_SLOT,
        "end_slot_exclusive": DEVELOPMENT_END_SLOT,
        "final_holdout_start_slot_inclusive": HOLDOUT_START_SLOT,
        "final_holdout_end_slot_exclusive": HOLDOUT_END_SLOT,
        "holdout_payload_reads_allowed": False,
        "holdout_feature_generation_allowed": False,
        "holdout_accessed": False,
        "outcomes_read": False,
        "reports_read": False,
        "s3_read": False,
        "physical_order": PHYSICAL_ORDER,
        "row_count": state.row_count,
        "distinct_event_id_count": len(state.event_ids),
        "distinct_mint_count": len(state.mints),
        "distinct_market_count": len(state.markets),
        "ordering": {
            "strict": True,
            "key": PHYSICAL_ORDER,
            "unique_event_ids": True,
            "availability_regressions": state.availability_regressions,
            "start": _ordering_text(state.first_global) if state.first_global else None,
            "end": _ordering_text(state.previous_global) if state.previous_global else None,
        },
        "development_bounds": {
            "start_slot_inclusive": DEVELOPMENT_START_SLOT,
            "end_slot_exclusive": DEVELOPMENT_END_SLOT,
            "holdout_start_slot_inclusive": HOLDOUT_START_SLOT,
            "holdout_end_slot_exclusive": HOLDOUT_END_SLOT,
            "holdout_accessed": False,
        },
        "phase_counts": phase_rows,
        "reserve_source_counts": dict(sorted(state.reserve_source_counts.items())),
        "reason_counts": dict(sorted(state.reason_counts.items())),
        "quote_mode_counts": dict(sorted(state.quote_mode_counts.items())),
        "segment_counts": dict(sorted(state.segment_counts.items())),
        "coverage_counts": dict(sorted(state.coverage_counts.items())),
        "row_identity_sha256": state.row_identity_digest.hexdigest(),
        "decision_sha256": state.decision_digest.hexdigest(),
        "family_aggregation": {
            "algorithm": "family_feature_mask_v2",
            "candidate_registry_row_iterations": state.candidate_registry_row_iterations,
            "candidate_row_digest_updates": state.candidate_row_digest_updates,
            "family_mask_updates": state.family_mask_updates,
            "unique_signature_counts": {
                family_id: len(state.family_masks[family_id].signatures) for family_id in FAMILY_IDS
            },
        },
        "family_eligibility": _family_payload(state),
    }
    # Avoid carrying the fragile min() expression into a non-empty assertion.
    if state.previous_global is not None:
        independent["ordering"]["end"] = _ordering_text(state.previous_global)  # type: ignore[index]
    return (
        independent,
        cast(dict[str, dict[str, object]], independent["family_eligibility"]),
        {
            "state": state,
            "parquet_schema_sha256": _sha256_bytes(parquet.schema_arrow.serialize().to_pybytes()),
        },
    )


def _compare_draft_counts(draft: _Draft, independent: Mapping[str, object]) -> None:
    row_count = _integer(independent.get("row_count"), "independent row_count")
    source_rows = draft.predecessor.get("source_rows", draft.audit.get("source_rows"))
    mode_accepted = draft.predecessor.get(
        "mode_exclusion_accepted_rows", draft.audit.get("mode_exclusion_accepted_rows", 0)
    )
    rejected_rows = draft.predecessor.get("rejected_rows", draft.audit.get("rejected_rows"))
    if source_rows is None or mode_accepted is None or rejected_rows is None:
        _fail("draft audit is missing source/rejected/mode row accounting")
    source_rows_i = _integer(source_rows, "draft source_rows")
    mode_i = _integer(mode_accepted, "draft mode_exclusion_accepted_rows")
    rejected_rows_i = _integer(rejected_rows, "draft rejected_rows")
    if source_rows_i - mode_i != row_count:
        _fail("corrected feature rows silently differ from source rows minus mode exclusions")
    rejected_names = [
        name
        for name in ("rejected-amounts.parquet", "rejected-events.parquet")
        if name in draft.files
    ]
    if not rejected_names:
        _fail("draft inventory has no rejected amount stream")
    rejected_path = draft.root / rejected_names[0]
    metadata = pq.ParquetFile(rejected_path).metadata
    if metadata is None or metadata.num_rows != rejected_rows_i:
        _fail("draft rejected stream row count differs from its audit")
    # Draft phase/sidecar counts are bindings, not trusted evidence.  Compare
    # the fields that both versions promise and fail closed on disagreement.
    draft_phase = draft.predecessor.get("phase_counts", draft.predecessor.get("phase_viability"))
    independent_phase = independent.get("phase_counts")
    if isinstance(draft_phase, Mapping) and isinstance(independent_phase, Mapping):
        for phase, payload in independent_phase.items():
            declared = draft_phase.get(phase)
            if isinstance(declared, Mapping) and declared.get(
                "rows", declared.get("total_rows")
            ) != payload.get("rows"):
                _fail(f"draft phase count differs for {phase}")


def _lineage_payload(
    lineage_audit: Mapping[str, object] | bytes | str | Path | None,
    lineage_audit_path: Path | None,
) -> tuple[bytes, dict[str, object], dict[str, object]] | None:
    if lineage_audit is not None and lineage_audit_path is not None:
        _fail("provide lineage_audit or lineage_audit_path, not both")
    if lineage_audit_path is not None:
        path = Path(lineage_audit_path)
        if _forbidden_path(path):
            _fail("lineage audit path points at a forbidden downstream path")
        _safe_output_parent(path.parent)
        payload, body = _parse_canonical(path, "external lineage audit")
        _json_no_downstream(payload, "external lineage audit")
        return body, payload, {"source": path.as_posix(), "sha256": _sha256_bytes(body)}
    if lineage_audit is None:
        return None
    if isinstance(lineage_audit, Mapping):
        payload = cast(dict[str, object], dict(lineage_audit))
        body = canonical_json(payload)
    elif isinstance(lineage_audit, bytes):
        try:
            payload = cast(dict[str, object], json.loads(lineage_audit.decode("utf-8")))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            _fail(f"lineage audit bytes are invalid JSON: {error}")
        if not isinstance(payload, dict) or lineage_audit != canonical_json(payload):
            _fail("lineage audit bytes are not canonical JSON")
        body = lineage_audit
    else:
        path = Path(os.fspath(lineage_audit))
        if _forbidden_path(path):
            _fail("lineage audit path points at a forbidden downstream path")
        _safe_output_parent(path.parent)
        payload, body = _parse_canonical(path, "external lineage audit")
    _json_no_downstream(payload, "external lineage audit")
    return body, payload, {"source": "payload", "sha256": _sha256_bytes(body)}


def _lineage_value(payload: Mapping[str, object], names: Sequence[str], label: str) -> object:
    for name in names:
        if name in payload:
            return payload[name]
    _fail(f"external lineage audit is missing {label}")


def _validate_lineage_binding(payload: Mapping[str, object], draft: _Draft) -> None:
    """Validate declarations supplied by the external lifecycle auditor.

    This intentionally checks declarations and cross-bindings only.  The
    lifecycle proof itself remains owned by the external auditor and is not
    reimplemented here.
    """

    kind = _text(_lineage_value(payload, ("kind", "audit_kind"), "kind"), "lineage.kind")
    if "lineage" not in kind.lower():
        _fail("external lineage audit kind is not a lineage audit")
    version = _integer(_lineage_value(payload, ("version",), "version"), "lineage.version")
    if version < 1:
        _fail("external lineage audit version is invalid")
    status = _text(_lineage_value(payload, ("status",), "status"), "lineage.status")
    if status not in {"verified", "complete", "attested", "sealed"}:
        _fail("external lineage audit is not externally verified")
    if payload.get("development_only") is not True:
        _fail("external lineage audit must be development-only")
    for name in (
        "holdout_accessed",
        "holdout_payload_reads_allowed",
        "holdout_feature_generation_allowed",
        "outcomes_read",
        "reports_read",
        "s3_read",
    ):
        if payload.get(name) is not False:
            _fail(f"external lineage audit {name} must be false")
    expected_hashes = {
        "v1_manifest": draft.v1_manifest_sha256,
        "v1_audit": draft.v1_audit_sha256,
        "mode_manifest": draft.mode_manifest_sha256,
        "mode_audit": draft.mode_audit_sha256,
        "mode_modes": draft.mode_modes_sha256,
    }
    aliases = {
        "v1_manifest": ("v1_manifest_sha256", "source_manifest_sha256", "input_v1_manifest_sha256"),
        "v1_audit": ("v1_audit_sha256", "source_audit_sha256", "input_v1_audit_sha256"),
        "mode_manifest": ("mode_index_manifest_sha256", "market_mode_manifest_sha256"),
        "mode_audit": ("mode_index_audit_sha256", "market_mode_audit_sha256"),
        "mode_modes": ("mode_index_modes_sha256", "market_mode_modes_sha256"),
    }
    for name, expected in expected_hashes.items():
        declared = _lineage_value(payload, aliases[name], name)
        if declared != expected:
            _fail(f"external lineage audit {name} binding differs")
    source_rows = _integer(
        _lineage_value(payload, ("source_rows", "v1_source_rows"), "source_rows"),
        "lineage.source_rows",
    )
    rejected_rows = _integer(
        _lineage_value(payload, ("rejected_rows", "v1_rejected_rows"), "rejected_rows"),
        "lineage.rejected_rows",
    )
    excluded_rows = _integer(
        _lineage_value(
            payload,
            (
                "mode_exclusion_accepted_rows",
                "accepted_mode_exclusion_rows",
                "excluded_accepted_rows",
            ),
            "mode_exclusion_accepted_rows",
        ),
        "lineage.mode_exclusion_accepted_rows",
    )
    excluded_rejected_rows = _integer(
        _lineage_value(
            payload,
            ("mode_exclusion_rejected_rows", "excluded_rejected_rows"),
            "mode_exclusion_rejected_rows",
        ),
        "lineage.mode_exclusion_rejected_rows",
    )
    draft_source = _integer(
        draft.predecessor.get("source_rows", draft.audit.get("source_rows")),
        "draft source_rows",
    )
    draft_rejected = _integer(
        draft.predecessor.get("rejected_rows", draft.audit.get("rejected_rows")),
        "draft rejected_rows",
    )
    draft_excluded = _integer(
        draft.predecessor.get(
            "mode_exclusion_accepted_rows", draft.audit.get("mode_exclusion_accepted_rows")
        ),
        "draft mode_exclusion_accepted_rows",
    )
    draft_excluded_rejected = _integer(
        draft.predecessor.get(
            "mode_exclusion_rejected_rows", draft.audit.get("mode_exclusion_rejected_rows")
        ),
        "draft mode_exclusion_rejected_rows",
    )
    draft_rows = _integer(draft.manifest.get("row_count"), "draft row_count")
    if (source_rows, rejected_rows, excluded_rows, excluded_rejected_rows) != (
        draft_source,
        draft_rejected,
        draft_excluded,
        draft_excluded_rejected,
    ):
        _fail("external lineage audit row accounting differs from the draft")
    if source_rows - excluded_rows != draft_rows:
        _fail("external lineage audit source/mode accounting is inconsistent")


def audit_corrected_features_v2_independent(
    draft_root: Path,
    *,
    config_path: str | Path = CONFIG_PATH,
    batch_size: int = 32_768,
    lineage_audit: Mapping[str, object] | bytes | str | Path | None = None,
    lineage_audit_path: str | Path | None = None,
    technical_retry_config: str | Path | None = None,
    parent_attested_root: str | Path | None = None,
) -> dict[str, object]:
    """Audit a draft corrected-v2 root without publishing a new root."""

    started = time.perf_counter()
    retry_context = _load_retry_context(technical_retry_config, parent_attested_root)
    draft = _load_draft(Path(draft_root), Path(config_path))
    independent, families, _stream_meta = _stream_features(draft, batch_size=batch_size)
    _compare_draft_counts(draft, independent)
    evaluation_code = _evaluation_code_audit(
        draft,
        retry_contract=retry_context.contract if retry_context is not None else None,
        retry_config_path=retry_context.config_path if retry_context is not None else None,
        parent_source_code_inventory_sha256=(
            retry_context.parent_source_code_inventory_sha256 if retry_context is not None else None
        ),
        parent_build_identity_sha256=(
            retry_context.parent_build_identity_sha256 if retry_context is not None else None
        ),
    )
    lineage = _lineage_payload(
        lineage_audit, Path(lineage_audit_path) if lineage_audit_path else None
    )
    if lineage is not None:
        _validate_lineage_binding(lineage[1], draft)
    if retry_context is not None and lineage is None:
        _fail("Retry-1 auditing requires an external lineage audit")
    audit: dict[str, object] = {
        **independent,
        "kind": KIND,
        "version": VERSION,
        "status": "verified",
        "draft_manifest_sha256": _sha256_bytes(draft.manifest_body),
        "draft_audit_sha256": _sha256_bytes(draft.audit_body),
        "draft_predecessor_audit_sha256": _sha256_bytes(draft.predecessor_body),
        "draft_features_sha256": draft.files["features.parquet"]["sha256"],
        "v1_manifest_sha256": draft.v1_manifest_sha256,
        "v1_audit_sha256": draft.v1_audit_sha256,
        "mode_index_manifest_sha256": draft.mode_manifest_sha256,
        "mode_index_audit_sha256": draft.mode_audit_sha256,
        "mode_index_modes_sha256": draft.mode_modes_sha256,
        "config_sha256": draft.config_sha256,
        "amendment_sha256": draft.amendment_sha256,
        "materializer_code_reference": dict(
            _entry(
                _entry(draft.manifest.get("references"), "draft references").get("code"),
                "draft references.code",
            )
        ),
        "evaluation_code_audit": evaluation_code,
        "lineage_audit_attached": lineage is not None,
        "holdout_accessed": False,
        "outcomes_read": False,
        "reports_read": False,
        "s3_read": False,
        "elapsed_seconds": time.perf_counter() - started,
    }
    if lineage is not None:
        body, payload, ref = lineage
        audit["external_lineage_audit"] = {
            **ref,
            "kind": payload.get("kind"),
            "version": payload.get("version"),
            "byte_count": len(body),
        }
    if retry_context is not None:
        lineage_sha = _sha256_bytes(lineage[0]) if lineage is not None else None
        stable_data = _retry_stable_data(retry_context, draft, independent, lineage_sha)
        retry_payload = _technical_retry_payload(retry_context, evaluation_code, stable_data)
        audit["source_code_inventory_sha256"] = evaluation_code["source_code_inventory_sha256"]
        audit["build_identity_sha256"] = evaluation_code["build_identity_sha256"]
        audit["technical_retry"] = retry_payload
    audit["audit_sha256"] = _sha256_bytes(canonical_json(audit))
    return audit


def _copy_fsync(source: Path, target: Path) -> tuple[int, str]:
    """Copy one payload, fsync it, and return metrics for the copied bytes.

    The digest is updated from each streamed block, so callers can seal the
    copy without opening the target for a second read.
    """

    _regular(source, f"source payload {source}")
    digest = hashlib.sha256()
    byte_count = 0
    with source.open("rb") as src, target.open("xb") as dst:
        for block in iter(lambda: src.read(8 * 1024 * 1024), b""):
            dst.write(block)
            digest.update(block)
            byte_count += len(block)
        dst.flush()
        os.fsync(dst.fileno())
    return byte_count, digest.hexdigest()


def _write_fsync(path: Path, body: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())


def _publish_noreplace(staging: Path, output: Path) -> None:
    if os.name == "posix" and sys.platform.startswith("linux"):
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = getattr(libc, "renameat2", None)
        if renameat2 is not None:
            renameat2.argtypes = [
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_uint,
            ]
            renameat2.restype = ctypes.c_int
            result = renameat2(-100, os.fsencode(staging), -100, os.fsencode(output), 1)
            if result == 0:
                return
            error = ctypes.get_errno()
            if error == errno.EEXIST:
                raise FileExistsError(f"refusing to overwrite independent output root {output}")
            raise OSError(error, os.strerror(error), output)
    if os.path.lexists(output):
        raise FileExistsError(f"refusing to overwrite independent output root {output}")
    os.rename(staging, output)


def seal_corrected_features_v2_independent(
    draft_root: Path,
    output: Path,
    *,
    config_path: str | Path = CONFIG_PATH,
    batch_size: int = 32_768,
    lineage_audit: Mapping[str, object] | bytes | str | Path | None = None,
    lineage_audit_path: str | Path | None = None,
    technical_retry_config: str | Path | None = None,
    parent_attested_root: str | Path | None = None,
) -> dict[str, object]:
    """Verify and publish a new attested root, manifest last, no overwrite."""

    output = Path(output)
    if os.path.lexists(output):
        raise FileExistsError(f"refusing to overwrite independent output root {output}")
    if _forbidden_path(output):
        _fail("independent output path is unsafe")
    _safe_output_parent(output.parent)
    if output.resolve() == Path(draft_root).resolve():
        _fail("independent output must be distinct from the draft root")
    retry_context = _load_retry_context(technical_retry_config, parent_attested_root)
    draft = _load_draft(Path(draft_root), Path(config_path))
    independent, families, stream_meta = _stream_features(draft, batch_size=batch_size)
    _compare_draft_counts(draft, independent)
    evaluation_code = _evaluation_code_audit(
        draft,
        retry_contract=retry_context.contract if retry_context is not None else None,
        retry_config_path=retry_context.config_path if retry_context is not None else None,
        parent_source_code_inventory_sha256=(
            retry_context.parent_source_code_inventory_sha256 if retry_context is not None else None
        ),
        parent_build_identity_sha256=(
            retry_context.parent_build_identity_sha256 if retry_context is not None else None
        ),
    )
    lineage = _lineage_payload(
        lineage_audit, Path(lineage_audit_path) if lineage_audit_path else None
    )
    if lineage is None:
        _fail("publishing an independent grid-ready root requires an external lineage audit")
    _validate_lineage_binding(lineage[1], draft)
    stable_data: dict[str, str] | None = None
    retry_payload: dict[str, object] | None = None
    if retry_context is not None:
        stable_data = _retry_stable_data(
            retry_context,
            draft,
            independent,
            _sha256_bytes(lineage[0]),
        )
        retry_payload = _technical_retry_payload(retry_context, evaluation_code, stable_data)
    output.parent.mkdir(parents=True, exist_ok=True)
    _safe_output_parent(output.parent)
    staging = output.with_name(
        f".{output.name}.independent-v2-{os.getpid()}-{time.time_ns()}.partial"
    )
    if os.path.lexists(staging):
        raise FileExistsError(f"staging path already exists: {staging}")
    staging.mkdir()
    started = time.perf_counter()
    try:
        # Payload copies remain byte-identical.  Draft sidecars receive names
        # that make their provenance explicit and cannot be mistaken for the
        # new manifest.
        copied: dict[str, dict[str, object]] = {}
        copy_map = {"features.parquet": "features.parquet"}
        for name in draft.files:
            if name == draft.audit_path.relative_to(draft.root).as_posix():
                copy_map[name] = "draft-audit.json"
            elif name == draft.predecessor_path.relative_to(draft.root).as_posix():
                copy_map[name] = "predecessor-audit.json"
            elif name != "features.parquet":
                copy_map[name] = name
        copy_map[draft.manifest_path.name] = "draft-manifest.json"
        for source_name, target_name in copy_map.items():
            source = draft.root / source_name
            target = staging / target_name
            byte_count, digest = _copy_fsync(source, target)
            copied[target_name] = {
                "byte_count": byte_count,
                "sha256": digest,
            }
            if source_name in draft.files:
                entry = draft.files[source_name]
                if source_name == "features.parquet":
                    for metadata_name in ("row_count", "schema_sha256"):
                        if metadata_name in entry:
                            copied[target_name][metadata_name] = entry[metadata_name]
                if (
                    copied[target_name]["byte_count"] != entry["byte_count"]
                    or copied[target_name]["sha256"] != entry["sha256"]
                ):
                    _fail(f"byte-identical draft copy failed for {source_name}")
            elif source_name == draft.manifest_path.name and (
                copied[target_name]["byte_count"] != len(draft.manifest_body)
                or copied[target_name]["sha256"] != _sha256_bytes(draft.manifest_body)
            ):
                _fail("byte-identical draft manifest copy failed")

        independent_payload: dict[str, object] = {
            **independent,
            "kind": KIND,
            "version": VERSION,
            "status": "verified",
            "draft_manifest_sha256": _sha256_bytes(draft.manifest_body),
            "draft_audit_sha256": _sha256_bytes(draft.audit_body),
            "draft_predecessor_audit_sha256": _sha256_bytes(draft.predecessor_body),
            "materializer_code_reference": dict(
                _entry(
                    _entry(draft.manifest.get("references"), "draft references").get("code"),
                    "draft references.code",
                )
            ),
            "evaluation_code_audit": evaluation_code,
            "holdout_accessed": False,
            "outcomes_read": False,
            "reports_read": False,
            "s3_read": False,
            "elapsed_seconds": time.perf_counter() - started,
        }
        if retry_payload is not None:
            independent_payload["source_code_inventory_sha256"] = evaluation_code[
                "source_code_inventory_sha256"
            ]
            independent_payload["build_identity_sha256"] = evaluation_code["build_identity_sha256"]
            independent_payload["technical_retry"] = retry_payload
        if lineage is not None:
            independent_payload["lineage_audit_sha256"] = _sha256_bytes(lineage[0])
            independent_payload["external_lineage_audit"] = {
                "kind": lineage[1].get("kind"),
                "version": lineage[1].get("version"),
                "status": lineage[1].get("status"),
                "lineage_audit_sha256": _sha256_bytes(lineage[0]),
            }
        independent_body = canonical_json(independent_payload)
        independent_path = staging / "independent-audit.json"
        _write_fsync(independent_path, independent_body)
        copied["independent-audit.json"] = {
            "byte_count": len(independent_body),
            "sha256": _sha256_bytes(independent_body),
        }
        family_paths: dict[str, str] = {}
        for family_id in FAMILY_IDS:
            safe = family_id.replace("-", "_")
            name = f"{safe}-viability-audit.json"
            body = canonical_json(families[family_id])
            _write_fsync(staging / name, body)
            copied[name] = {"byte_count": len(body), "sha256": _sha256_bytes(body)}
            family_paths[family_id] = name
        lineage_ref: dict[str, object] | None = None
        if lineage is not None:
            lineage_body, lineage_obj, lineage_meta = lineage
            _write_fsync(staging / "lineage-audit.json", lineage_body)
            copied["lineage-audit.json"] = {
                "byte_count": len(lineage_body),
                "sha256": _sha256_bytes(lineage_body),
            }
            lineage_ref = {
                **lineage_meta,
                "path": "lineage-audit.json",
                "kind": lineage_obj.get("kind"),
                "version": lineage_obj.get("version"),
            }
        attestation_payload = {
            "draft_manifest_sha256": _sha256_bytes(draft.manifest_body),
            "draft_audit_sha256": _sha256_bytes(draft.audit_body),
            "draft_predecessor_audit_sha256": _sha256_bytes(draft.predecessor_body),
            "features_sha256": copied["features.parquet"]["sha256"],
            "independent_audit_sha256": copied["independent-audit.json"]["sha256"],
            "family_audit_sha256": {
                family: copied[path]["sha256"] for family, path in family_paths.items()
            },
            "source_code_inventory_sha256": evaluation_code["source_code_inventory_sha256"],
            "build_identity_sha256": evaluation_code["build_identity_sha256"],
            "config_sha256": draft.config_sha256,
            "amendment_sha256": draft.amendment_sha256,
            "schema_sha256": draft.manifest.get("schema_sha256"),
            "row_identity_sha256": independent["row_identity_sha256"],
            "decision_sha256": independent["decision_sha256"],
        }
        if retry_payload is not None:
            attestation_payload["technical_retry"] = retry_payload
        if lineage is not None:
            attestation_payload["lineage_audit_sha256"] = copied.get("lineage-audit.json", {}).get(
                "sha256"
            )
        manifest: dict[str, object] = {
            # Keep the published root consumable by feature/grid readers; the
            # independent attestation is a separate kind field.
            "kind": FEATURE_KIND,
            "version": VERSION,
            "attestation_kind": KIND,
            "attestation_version": VERSION,
            "status": "complete",
            "development_only": True,
            "start_slot_inclusive": DEVELOPMENT_START_SLOT,
            "end_slot_exclusive": DEVELOPMENT_END_SLOT,
            "final_holdout_start_slot_inclusive": HOLDOUT_START_SLOT,
            "final_holdout_end_slot_exclusive": HOLDOUT_END_SLOT,
            "holdout_payload_reads_allowed": False,
            "holdout_feature_generation_allowed": False,
            "holdout_accessed": False,
            "outcomes_read": False,
            "reports_read": False,
            "s3_read": False,
            "physical_order": PHYSICAL_ORDER,
            "draft_root": str(draft.root),
            "draft_manifest_sha256": _sha256_bytes(draft.manifest_body),
            "draft_audit_sha256": _sha256_bytes(draft.audit_body),
            "draft_predecessor_audit_sha256": _sha256_bytes(draft.predecessor_body),
            "v1_manifest_sha256": draft.v1_manifest_sha256,
            "v1_audit_sha256": draft.v1_audit_sha256,
            "mode_index_manifest_sha256": draft.mode_manifest_sha256,
            "mode_index_audit_sha256": draft.mode_audit_sha256,
            "mode_index_modes_sha256": draft.mode_modes_sha256,
            "config_sha256": draft.config_sha256,
            "amendment_sha256": draft.amendment_sha256,
            "schema_sha256": draft.manifest.get("schema_sha256"),
            "references": draft.manifest.get("references"),
            "input_v1": draft.manifest.get("input_v1"),
            "market_mode_index": draft.manifest.get("market_mode_index"),
            "mode_index_columns": draft.manifest.get("mode_index_columns"),
            "allowed_source_columns": draft.manifest.get("allowed_source_columns"),
            "allowed_column_contract": draft.manifest.get("allowed_column_contract"),
            "correction": draft.manifest.get("correction"),
            "predecessor_audit_path": "predecessor-audit.json",
            "predecessor_audit_sha256": copied.get("predecessor-audit.json", {}).get("sha256"),
            "independent_audit_path": "independent-audit.json",
            "independent_audit_sha256": copied["independent-audit.json"]["sha256"],
            "family_audit_paths": family_paths,
            "family_audit_sha256": {
                family: copied[path]["sha256"] for family, path in family_paths.items()
            },
            "row_count": independent["row_count"],
            "phase_counts": independent["phase_counts"],
            "reserve_source_counts": independent["reserve_source_counts"],
            "quote_mode_counts": independent["quote_mode_counts"],
            "family_aggregation": independent["family_aggregation"],
            "family_eligibility": families,
            "evaluation_code_audit": evaluation_code,
            "source_code_inventory_sha256": evaluation_code["source_code_inventory_sha256"],
            "build_identity_sha256": evaluation_code["build_identity_sha256"],
            "attestation_payload": attestation_payload,
            "attestation_sha256": _sha256_bytes(canonical_json(attestation_payload)),
            "lineage_audit": lineage_ref,
            "files": copied,
            "audit_path": "independent-audit.json",
            "audit_sha256": copied["independent-audit.json"]["sha256"],
            "truth_boundaries": [
                (
                    "independent second-pass stream; no strategy outcomes, reports, S3, "
                    "or holdout payloads"
                ),
                (
                    "Pump reserve algebra excludes fees and is checked against prior "
                    "virtual post state"
                ),
                "family eligibility is recomputed from frozen source SQL semantics",
                "all copied draft payloads are byte-identical and independently hashed",
            ],
        }
        if retry_payload is not None:
            manifest["technical_retry"] = retry_payload
        # Manifest is the publication marker and is written only after every
        # payload has been fsynced and hashed.
        manifest_body = canonical_json(manifest)
        _write_fsync(staging / "manifest.json", manifest_body)
        directory_fd = os.open(staging, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        _publish_noreplace(staging, output)
        parent_fd = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        return manifest
    except BaseException:
        if staging.is_dir() and not staging.is_symlink():
            shutil.rmtree(staging, ignore_errors=True)
        raise


# Concise aliases for CLI/integration callers.
audit_independent_feature_v2 = audit_corrected_features_v2_independent
seal_independent_feature_v2 = seal_corrected_features_v2_independent
audit_and_seal_independent_feature_v2 = seal_corrected_features_v2_independent


__all__ = [
    "FEATURE_KIND",
    "FEATURE_VERSION",
    "FAMILY_IDS",
    "IndependentFeatureAuditError",
    "IndependentFeatureAuditV2Error",
    "KIND",
    "PHYSICAL_ORDER",
    "VERSION",
    "audit_and_seal_independent_feature_v2",
    "audit_corrected_features_v2_independent",
    "audit_independent_feature_v2",
    "seal_corrected_features_v2_independent",
    "seal_independent_feature_v2",
    "sha256_file",
]
