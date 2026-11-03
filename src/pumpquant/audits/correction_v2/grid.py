"""Strict runtime bridge for the four fresh correction-v2 family grids.

The family implementations in ``scripts/run_*_production_grid.py`` are the
single source of the v1 fold, execution, cost, selection, and commit-before-
test logic.  This module supplies a small, explicit profile that scopes only
the corrected identities and multiplicity values, verifies the sealed
corrected feature root, and binds the resulting report to the correction
contract.  It intentionally does not implement a second grid.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import importlib
import json
import os
import shutil
import stat
import sys
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import Final, NoReturn, cast

import pyarrow.parquet as pq

from pumpquant.audits.correction_v2.contract import (
    CONFIG_PATH,
    CORRECTION_ID,
    DEVELOPMENT_END_SLOT,
    DEVELOPMENT_START_SLOT,
    HOLDOUT_START_SLOT,
    CorrectionV2Contract,
    FamilyContract,
    _registry_payload_for_family,
    _source_registry_digest,
    canonical_json,
    load_correction_v2_contract,
)
from pumpquant.audits.correction_v2.retry_contract import (
    CONFIG_PATH as RETRY_CONFIG_PATH,
)
from pumpquant.audits.correction_v2.retry_contract import (
    CorrectionV2RetryContract,
    load_correction_v2_retry_contract,
)
from pumpquant.core.hashing import sha256_file

FEATURE_KIND: Final[str] = "pumpquant.shock_recovery_feature_dataset"
FEATURE_VERSION: Final[int] = 2
FEATURE_MANIFEST_KIND: Final[str] = FEATURE_KIND
FEATURE_MANIFEST_VERSION: Final[int] = FEATURE_VERSION
CORRECTED_FEATURE_KIND: Final[str] = FEATURE_KIND
CORRECTED_FEATURE_VERSION: Final[int] = FEATURE_VERSION

# A correction materializer deliberately emits a *draft* root.  Grid readers
# consume only the fresh root published by the independent second-pass
# sealer.  Keep these values literal here so the gate cannot accidentally
# accept a v1/draft sidecar merely because it happens to share the feature
# kind/version.
INDEPENDENT_ATTESTATION_KIND: Final[str] = "pumpquant.independent_feature_audit_v2"
INDEPENDENT_ATTESTATION_VERSION: Final[int] = 2
INDEPENDENT_ATTESTATION_STATUS: Final[str] = "complete"
INDEPENDENT_AUDIT_STATUS: Final[str] = "verified"
_INDEPENDENT_FAMILY_IDS: Final[tuple[str, ...]] = (
    "shock-recovery-fresh-v2",
    "progress-speed-fresh-v2",
    "persistent-pressure-fresh-v2",
    "amm-impact-ofi-fresh-v2",
)
# The production grid runners live in the private research repository, not this snapshot.
_PRIVATE_RUNNERS: Final[dict[str, str]] = {
    "shock-recovery-fresh-v2": "scripts.run_shock_recovery_production_grid",
    "progress-speed-fresh-v2": "scripts.run_progress_speed_production_grid",
    "persistent-pressure-fresh-v2": "scripts.run_persistent_pressure_production_grid",
    "amm-impact-ofi-fresh-v2": "scripts.run_amm_impact_ofi_production_grid",
}

_SHA256_LENGTH = 64
_HEX = frozenset("0123456789abcdef")
_FAMILY_ALIASES: Final[dict[str, str]] = {
    "shock_recovery": "shock-recovery-fresh-v2",
    "shock-recovery": "shock-recovery-fresh-v2",
    "shock-recovery-fresh-v2": "shock-recovery-fresh-v2",
    "progress_speed": "progress-speed-fresh-v2",
    "progress-speed": "progress-speed-fresh-v2",
    "progress-speed-fresh-v2": "progress-speed-fresh-v2",
    "persistent_pressure": "persistent-pressure-fresh-v2",
    "persistent-pressure": "persistent-pressure-fresh-v2",
    "persistent-pressure-fresh-v2": "persistent-pressure-fresh-v2",
    "amm_impact_ofi": "amm-impact-ofi-fresh-v2",
    "amm-impact-ofi": "amm-impact-ofi-fresh-v2",
    "amm-impact-ofi-fresh-v2": "amm-impact-ofi-fresh-v2",
}


class CorrectedGridV2Error(RuntimeError):
    """A corrected-v2 input, profile, or publication invariant failed."""


class CorrectedFeatureVerificationError(CorrectedGridV2Error):
    """The corrected feature root is absent, tampered, or not sealed."""


_RootSnapshot = tuple[tuple[str, int, int, int, int, int, int], ...]


@dataclass(frozen=True, slots=True)
class GridRuntimeProfile:
    """Immutable values scoped into one existing family runner invocation."""

    family: FamilyContract
    report_kind: str
    manifest_kind: str
    registry_kind: str
    experiment_id: str
    version: int
    trial_denominator: int
    z_threshold: float
    bootstrap_seed: str
    corrected_feature_manifest_sha256: str
    contract_sha256: str
    amendment_sha256: str
    mode_index_manifest_sha256: str
    mode_index_audit_sha256: str
    mode_index_modes_sha256: str
    predecessor_audit_sha256: str
    corrected_code_sha256: str
    v1_manifest_sha256: str
    v1_audit_sha256: str
    feature_verifier: Callable[[Path], tuple[dict[str, object], Path, str]]
    retry_contract: CorrectionV2RetryContract | None = None
    retry_id: str | None = None
    retry_config_sha256: str | None = None
    retry_document_sha256: str | None = None
    parent_config_sha256: str | None = None
    parent_amendment_sha256: str | None = None
    parent_attested_manifest_sha256: str | None = None
    parent_shock_manifest_sha256: str | None = None
    technical_retry_sha256: str | None = None
    parent_source_code_inventory_sha256: str | None = None
    parent_build_identity_sha256: str | None = None
    retry_source_code_inventory_sha256: str | None = None
    retry_build_identity_sha256: str | None = None

    @property
    def family_id(self) -> str:
        return self.family.id

    @property
    def candidate_count(self) -> int:
        return self.family.candidate_count

    @property
    def candidate_order_sha256(self) -> str:
        return self.family.candidate_order_sha256

    @property
    def registry_sha256(self) -> str:
        return self.family.registry_sha256

    @property
    def overrides(self) -> Mapping[str, object]:
        """Names understood by the tiny compatibility scope in each runner."""

        return MappingProxyType(
            {
                "EXPERIMENT_ID": self.experiment_id,
                "REPORT_KIND": self.report_kind,
                "MANIFEST_KIND": self.manifest_kind,
                "REGISTRY_KIND": self.registry_kind,
                "REGISTRY_PRIOR_ATTEMPTED_CANDIDATES": (
                    self.trial_denominator - self.family.candidate_count
                ),
                "VERSION": self.version,
                "TRIAL_DENOMINATOR": self.trial_denominator,
                "EXPECTED_TRIAL_DENOMINATOR": self.trial_denominator,
                "Z_THRESHOLD": self.z_threshold,
                "BOOTSTRAP_SEED": self.bootstrap_seed,
            }
        )

    @property
    def technical_retry(self) -> bool:
        return self.retry_contract is not None


# Public alias used by callers that prefer the shorter name.
RuntimeProfile = GridRuntimeProfile


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _fail(message: str) -> NoReturn:
    raise CorrectedFeatureVerificationError(message)


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        _fail(f"{label} must be an object")
    return cast(dict[str, object], value)


def _sha(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_LENGTH
        or any(char not in _HEX for char in value)
    ):
        _fail(f"{label} is not a lowercase SHA-256 digest")
    return value


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        _fail(f"{label} is not an integer >= {minimum}")
    return value


def _safe_relative(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or os.path.isabs(value):
        _fail(f"{label} is not a safe relative path")
    path = Path(value)
    if any(part in {"", ".", ".."} for part in path.parts):
        _fail(f"{label} contains an unsafe path component")
    if any(_holdout_component(part) for part in path.parts):
        _fail(f"{label} may not reference a holdout path")
    return path.as_posix()


def _holdout_component(value: str) -> bool:
    text = value.casefold()
    return (
        text == "holdout"
        or text.startswith(("holdout-", "holdout_", ".holdout"))
        or text.endswith(("-holdout", "_holdout"))
    )


def _verify_provenance_path_declaration(value: object, label: str) -> str:
    """Validate a provenance-only path without requiring its old root mounted."""

    if not isinstance(value, str) or not value.strip():
        _fail(f"{label} is missing")
    forbidden = {"outcome", "outcomes", "report", "reports", "s3"}
    if any(part.casefold() in forbidden or _holdout_component(part) for part in Path(value).parts):
        _fail(f"{label} points at a forbidden downstream path")
    return value


def _regular_file(path: Path, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        _fail(f"{label} must be a regular non-symlink file")


def _sealed_root_snapshot(root: Path) -> _RootSnapshot:
    """Capture cheap mutation evidence for one already hash-verified root."""

    try:
        root_stat = os.stat(root, follow_symlinks=False)
        if not stat.S_ISDIR(root_stat.st_mode):
            _fail("corrected feature input must remain a regular directory")
        values = [
            (
                ".",
                root_stat.st_mode,
                root_stat.st_dev,
                root_stat.st_ino,
                root_stat.st_size,
                root_stat.st_mtime_ns,
                root_stat.st_ctime_ns,
            )
        ]
        with os.scandir(root) as entries:
            for entry in entries:
                if entry.is_symlink():
                    _fail("corrected feature input gained a symlink")
                item = entry.stat(follow_symlinks=False)
                if not stat.S_ISREG(item.st_mode):
                    _fail("corrected feature input gained a non-regular payload")
                values.append(
                    (
                        entry.name,
                        item.st_mode,
                        item.st_dev,
                        item.st_ino,
                        item.st_size,
                        item.st_mtime_ns,
                        item.st_ctime_ns,
                    )
                )
    except OSError as error:
        _fail(f"corrected feature input cannot be snapshotted safely: {error}")
    return tuple(sorted(values))


def _cached_feature_verifier(
    root: Path,
    manifest: Mapping[str, object],
    feature_path: Path,
    manifest_sha256: str,
    snapshot: _RootSnapshot,
) -> Callable[[Path], tuple[dict[str, object], Path, str]]:
    """Reuse one full verification inside the immediately selected runner.

    The runner historically verifies its feature input itself.  The corrected
    bridge has already completed the stronger attestation gate immediately
    before dispatch, so a second 9.6 GB hash adds no boundary.  Device/inode,
    size, mode, mtime, and ctime for the root and every payload make this cache
    fail closed on ordinary concurrent mutation without rereading feature
    bytes.
    """

    expected_root = root.resolve(strict=True)
    expected_feature = feature_path.resolve(strict=True)
    frozen_manifest = dict(manifest)

    def verify(requested_root: Path) -> tuple[dict[str, object], Path, str]:
        requested = Path(requested_root)
        try:
            resolved = requested.resolve(strict=True)
        except OSError as error:
            _fail(f"cached corrected feature input cannot be resolved safely: {error}")
        if requested.absolute() != expected_root or resolved != expected_root:
            _fail("runner requested a different corrected feature root")
        if _sealed_root_snapshot(expected_root) != snapshot:
            _fail("corrected feature root changed after full verification")
        if sha256_file(expected_root / "manifest.json") != manifest_sha256:
            _fail("corrected feature manifest changed after full verification")
        return dict(frozen_manifest), expected_feature, manifest_sha256

    return verify


def _assert_development_path(
    path: Path,
    label: str,
    *,
    require_directory: bool,
) -> None:
    """Reject holdout-labelled paths and every symlink traversal before use."""

    path = Path(path)
    if any(_holdout_component(part) for part in path.parts):
        raise CorrectedGridV2Error(f"{label} may not reference a holdout path")
    if path.is_symlink():
        raise CorrectedGridV2Error(f"{label} may not be a symlink")
    if require_directory and not path.is_dir():
        raise CorrectedGridV2Error(f"{label} must be a regular directory")
    anchor = path if path.exists() else path.parent
    while not anchor.exists():
        if anchor.is_symlink():
            raise CorrectedGridV2Error(f"{label} may not traverse a symlink")
        parent = anchor.parent
        if parent == anchor:
            raise CorrectedGridV2Error(f"{label} has no resolvable directory ancestor")
        anchor = parent
    if anchor.is_symlink() or anchor.resolve(strict=True) != anchor.absolute():
        raise CorrectedGridV2Error(f"{label} may not traverse a symlink")
    if any(_holdout_component(part) for part in anchor.resolve(strict=True).parts):
        raise CorrectedGridV2Error(f"{label} may not resolve through a holdout path")


def _assert_pairwise_disjoint_roots(
    values: tuple[tuple[str, Path], ...],
) -> None:
    """Reject equal or nested input/output/temp roots before any directory write."""

    resolved = [(label, Path(path).absolute()) for label, path in values]
    for index, (left_label, left) in enumerate(resolved):
        for right_label, right in resolved[index + 1 :]:
            if left == right or left in right.parents or right in left.parents:
                raise CorrectedGridV2Error(f"{left_label} and {right_label} must be disjoint roots")


def _publish_directory_noreplace(staging: Path, output: Path) -> None:
    if sys.platform.startswith("linux"):
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = getattr(libc, "renameat2", None)
        if renameat2 is not None:
            renameat2.argtypes = (
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_uint,
            )
            renameat2.restype = ctypes.c_int
            result = renameat2(
                -100,
                os.fsencode(staging),
                -100,
                os.fsencode(output),
                1,
            )
            if result == 0:
                return
            error = ctypes.get_errno()
            if error == errno.EEXIST:
                raise FileExistsError(f"refusing to overwrite corrected output root {output}")
            raise OSError(error, os.strerror(error), output)
    if os.path.lexists(output):
        raise FileExistsError(f"refusing to overwrite corrected output root {output}")
    os.rename(staging, output)


def _verify_entry(path: Path, entry: Mapping[str, object], label: str) -> dict[str, object]:
    expected_bytes = _integer(entry.get("byte_count"), f"{label} byte_count")
    expected_sha = _sha(entry.get("sha256"), f"{label} sha256")
    _regular_file(path, label)
    actual_bytes = path.stat().st_size
    actual_sha = sha256_file(path)
    if actual_bytes != expected_bytes or actual_sha != expected_sha:
        _fail(f"{label} differs from its sealed inventory")
    result: dict[str, object] = {"byte_count": actual_bytes, "sha256": actual_sha}
    if "row_count" in entry:
        result["row_count"] = _integer(entry["row_count"], f"{label} row_count")
    return result


def _file_inventory(value: object, label: str) -> dict[str, dict[str, object]]:
    if isinstance(value, dict):
        result: dict[str, dict[str, object]] = {}
        for name, entry in value.items():
            relative = _safe_relative(name, f"{label} path")
            result[relative] = _mapping(entry, f"{label} {relative}")
        return result
    if isinstance(value, list):
        result = {}
        for index, raw in enumerate(value):
            item = _mapping(raw, f"{label} entry {index}")
            relative = _safe_relative(item.get("path"), f"{label} entry {index} path")
            if relative in result:
                _fail(f"{label} repeats {relative}")
            result[relative] = item
        return result
    _fail(f"{label} inventory is missing")


def _verify_inventory(root: Path, files_value: object, label: str) -> dict[str, dict[str, object]]:
    files = _file_inventory(files_value, label)
    verified: dict[str, dict[str, object]] = {}
    for relative, entry in files.items():
        verified[relative] = _verify_entry(root / relative, entry, f"{label} {relative}")
    actual: set[str] = set()
    actual_directories: set[str] = set()
    for path in root.rglob("*"):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            _fail(f"{label} contains a symlink: {relative}")
        if path.is_dir():
            actual_directories.add(relative)
        elif path.is_file():
            if relative != "manifest.json":
                actual.add(relative)
        else:
            _fail(f"{label} contains a non-file payload: {relative}")
    if actual != set(files):
        missing = sorted(set(files) - actual)
        extra = sorted(actual - set(files))
        _fail(f"{label} does not exactly seal payload files (missing={missing}, extra={extra})")
    expected_directories = {
        parent.as_posix()
        for relative in files
        for parent in Path(relative).parents
        if parent != Path(".")
    }
    if actual_directories != expected_directories:
        missing = sorted(expected_directories - actual_directories)
        extra = sorted(actual_directories - expected_directories)
        _fail(
            f"{label} does not exactly seal payload directories (missing={missing}, extra={extra})"
        )
    return verified


def _load_json(path: Path, label: str, *, require_canonical: bool = True) -> dict[str, object]:
    _regular_file(path, label)
    body = path.read_bytes()
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        _fail(f"{label} is not valid UTF-8 JSON")
        raise AssertionError from error
    result = _mapping(value, label)
    if require_canonical and body != canonical_json(result):
        _fail(f"{label} is not canonical JSON")
    return result


def _verify_reference(
    item: Mapping[str, object],
    *,
    label: str,
    expected_path: str,
    expected_sha256: str,
    expected_file: Path | None = None,
) -> None:
    value = item.get("value")
    if value != expected_path:
        _fail(f"{label} value is not bound to {expected_path}")
    if item.get("value_sha256") != _sha256_bytes(expected_path.encode()):
        _fail(f"{label} value digest is invalid")
    declared_sha = _sha(item.get("sha256"), f"{label} sha256")
    if declared_sha != expected_sha256:
        _fail(f"{label} digest differs from the correction contract")
    if expected_file is not None:
        _regular_file(expected_file, label)
        if item.get("byte_count") != expected_file.stat().st_size:
            _fail(f"{label} byte count differs from the referenced file")
        if sha256_file(expected_file) != declared_sha:
            _fail(f"{label} file differs from its sealed digest")


def _verify_code_reference(item: Mapping[str, object]) -> str:
    value = item.get("value")
    if not isinstance(value, str) or not value.strip():
        _fail("references.code value is missing")
    forbidden = {"outcome", "outcomes", "report", "reports", "holdout", "s3"}
    path = Path(value)
    if any(part.casefold() in forbidden for part in path.parts) or any(
        _holdout_component(part) for part in path.parts
    ):
        _fail("references.code points at a forbidden downstream path")
    if item.get("value_sha256") != _sha256_bytes(value.encode()):
        _fail("references.code value digest is invalid")
    _regular_file(path, "references.code")
    digest = _sha(item.get("sha256"), "references.code sha256")
    if item.get("byte_count") != path.stat().st_size or sha256_file(path) != digest:
        _fail("references.code file differs from its sealed identity")
    return digest


def _verify_attested_code_declaration(item: Mapping[str, object]) -> str:
    """Validate the correction-code provenance without reopening its old bundle.

    The independent publisher already hashed the referenced correction bundle
    before copying the draft manifest into the immutable attested root.  Grid
    execution is governed by the separately recomputed current-code inventory,
    so requiring the historical bundle to remain mounted would add a redundant
    large-file hash and make an otherwise self-contained attestation fragile.
    """

    value = item.get("value")
    if not isinstance(value, str) or not value.strip():
        _fail("references.code value is missing")
    forbidden = {"outcome", "outcomes", "report", "reports", "holdout", "s3"}
    if any(part.casefold() in forbidden for part in Path(value).parts):
        _fail("references.code points at a forbidden downstream path")
    if item.get("value_sha256") != _sha256_bytes(value.encode()):
        _fail("references.code value digest is invalid")
    _integer(item.get("byte_count"), "references.code byte_count", minimum=1)
    return _sha(item.get("sha256"), "references.code sha256")


def _reject_downstream_flags(value: Mapping[str, object], label: str) -> None:
    for name in (
        "holdout_payload_reads_allowed",
        "holdout_feature_generation_allowed",
        "holdout_accessed",
        "outcomes_read",
        "reports_read",
        "s3_read",
    ):
        if name in value and value.get(name) is not False:
            _fail(f"{label} {name} must be false")


def _verify_feature_parquet(
    path: Path,
    manifest: Mapping[str, object],
    *,
    expected_rows: int,
    start_slot: int,
    end_slot: int,
) -> None:
    try:
        from scripts.correct_shock_recovery_features_v2 import CORRECTED_FEATURE_SCHEMA

        parquet = pq.ParquetFile(path)
    except Exception as error:
        _fail(f"corrected feature Parquet cannot be opened safely: {error}")
    if parquet.metadata is None or parquet.metadata.num_rows != expected_rows:
        _fail("corrected feature Parquet row count differs from its manifest")
    schema = parquet.schema_arrow
    if schema != CORRECTED_FEATURE_SCHEMA:
        _fail("corrected feature Parquet schema differs from the correction schema")
    schema_sha = _sha256_bytes(schema.serialize().to_pybytes())
    if manifest.get("schema_sha256") != schema_sha:
        _fail("corrected feature Parquet schema digest differs from its manifest")
    try:
        slot_index = schema.names.index("slot")
    except ValueError:
        _fail("corrected feature Parquet has no slot column")
    for group_index in range(parquet.metadata.num_row_groups):
        column = parquet.metadata.row_group(group_index).column(slot_index)
        statistics = column.statistics
        if statistics is None or not statistics.has_min_max:
            _fail("corrected feature Parquet slot statistics are missing")
        minimum = _integer(statistics.min, f"row group {group_index} minimum slot")
        maximum = _integer(statistics.max, f"row group {group_index} maximum slot")
        if minimum < start_slot or maximum >= end_slot or maximum < minimum:
            _fail("corrected feature Parquet contains a row outside development slots")


def _resolve_component_root(value: object, label: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        _fail(f"{label} root is missing")
    root = Path(value)
    if any(_holdout_component(part) for part in root.parts):
        _fail(f"{label} may not reference a holdout path")
    if root.is_symlink() or not root.is_dir():
        _fail(f"{label} root must be a regular directory")
    return root


def _verify_v1_input_binding(manifest: Mapping[str, object]) -> dict[str, str]:
    """Recheck the immutable v1 source seal bound by the correction manifest."""

    value = manifest.get("input_v1")
    if value is None:
        _fail("corrected feature manifest is missing input_v1 binding")
    bound = _mapping(value, "input_v1")
    root = _resolve_component_root(bound.get("root"), "input_v1")
    try:
        # Import lazily so importing this bridge does not force pyarrow for
        # profile-only callers.
        from scripts.correct_shock_recovery_features_v2 import _load_input

        seal = _load_input(root)
    except Exception as error:
        if isinstance(error, CorrectedFeatureVerificationError):
            raise
        _fail(f"bound v1 input cannot be independently sealed: {error}")
    if bound.get("manifest_sha256") != seal.manifest_sha256:
        _fail("input_v1 manifest hash differs from its sealed source")
    if bound.get("audit_sha256") != seal.audit_sha256:
        _fail("input_v1 audit hash differs from its sealed source")
    files = _mapping(bound.get("files"), "input_v1.files")
    if set(files) != set(seal.files):
        _fail("input_v1 file inventory differs from its independently sealed source")
    for name, entry in files.items():
        if name not in seal.files:
            _fail(f"input_v1 file is not independently sealed: {name}")
        source = _mapping(entry, f"input_v1.files[{name}]")
        actual = seal.files[name]
        if source.get("byte_count") != actual.get("byte_count") or source.get(
            "sha256"
        ) != actual.get("sha256"):
            _fail(f"input_v1 file binding differs for {name}")
    for field, digest in (
        ("v1_manifest_sha256", seal.manifest_sha256),
        ("source_manifest_sha256", seal.manifest_sha256),
        ("v1_audit_sha256", seal.audit_sha256),
        ("source_audit_sha256", seal.audit_sha256),
    ):
        # The independent sealer carries the canonical v1 identities under
        # the explicit v1 fields; source_* aliases are optional on that new
        # publication even though draft materializers emit both spellings.
        if manifest.get(field) is not None and manifest.get(field) != digest:
            _fail(f"corrected feature manifest {field} differs from input_v1")
    return {"manifest": seal.manifest_sha256, "audit": seal.audit_sha256}


def _verify_mode_binding(
    manifest: Mapping[str, object], contract: CorrectionV2Contract
) -> dict[str, str]:
    mode_ref = _mapping(manifest.get("market_mode_index"), "market_mode_index")
    if mode_ref.get("kind") != "pumpquant.pump_market_mode_index" or mode_ref.get("version") != 2:
        _fail("market-mode index kind/version is not v2")
    if (
        mode_ref.get("start_slot_inclusive") != contract.dev.start_slot_inclusive
        or mode_ref.get("end_slot_exclusive") != contract.dev.end_slot_exclusive
    ):
        _fail("market-mode index escapes the corrected development interval")
    root = _resolve_component_root(mode_ref.get("root"), "market_mode_index")
    try:
        from scripts.correct_shock_recovery_features_v2 import _load_mode_index

        seal = _load_mode_index(root)
    except Exception as error:
        if isinstance(error, CorrectedFeatureVerificationError):
            raise
        _fail(f"bound market-mode index cannot be independently sealed: {error}")
    expected = {
        "manifest": seal.manifest_sha256,
        "audit": seal.audit_sha256,
        "modes": seal.modes_sha256,
    }
    for field, digest in (
        ("manifest_sha256", seal.manifest_sha256),
        ("audit_sha256", seal.audit_sha256),
        ("modes_sha256", seal.modes_sha256),
    ):
        if mode_ref.get(field) != digest:
            _fail(f"market-mode index {field} differs from its sealed root")
    declared_files = _file_inventory(mode_ref.get("files"), "market_mode_index.files")
    if set(declared_files) != set(seal.files):
        _fail("market-mode index inventory differs from its independently sealed root")
    for name, entry in declared_files.items():
        if name not in seal.files:
            _fail(f"market-mode index file is not independently sealed: {name}")
        if entry.get("byte_count") != seal.files[name].get("byte_count") or entry.get(
            "sha256"
        ) != seal.files[name].get("sha256"):
            _fail(f"market-mode index file binding differs for {name}")
    return expected


def _inventory_sha(files: Mapping[str, Mapping[str, object]], name: str, label: str) -> str:
    entry = files.get(name)
    if entry is None:
        _fail(f"{label} is not present in the sealed file inventory")
    return _sha(entry.get("sha256"), f"{label} sha256")


def _assert_development_audit_flags(
    value: Mapping[str, object], label: str, *, require_all: bool = False
) -> None:
    """Require the exact development-only/no-downstream audit declaration."""

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
        if require_all and name not in value:
            _fail(f"{label} is missing {name}")
        if name in value and value.get(name) is not False:
            _fail(f"{label} {name} must be false")


def _verify_attested_root(
    root: Path,
    manifest: Mapping[str, object],
    files: Mapping[str, Mapping[str, object]],
    *,
    manifest_body: bytes,
    feature_path: Path,
    contract: CorrectionV2Contract,
    config_path: Path,
    expected_config_sha256: str | None = None,
    expected_amendment_sha256: str | None = None,
    retry_contract: CorrectionV2RetryContract | None = None,
) -> None:
    """Verify the independent v2 publication before returning a feature root.

    The correction materializer's ``status=complete`` draft is intentionally
    not sufficient for grid execution.  Publication performs the independent
    semantic row stream exactly once.  This runtime gate rehashes every sealed
    payload, cross-checks its copied evidence and lineage, and recomputes the
    current-source inventory without repeating that ordered Python row scan.
    """

    if (
        manifest.get("attestation_kind") != INDEPENDENT_ATTESTATION_KIND
        or manifest.get("attestation_version") != INDEPENDENT_ATTESTATION_VERSION
        or manifest.get("status") != INDEPENDENT_ATTESTATION_STATUS
    ):
        _fail("feature input is not an independently attested corrected v2 root")

    payload = _mapping(manifest.get("attestation_payload"), "attestation_payload")
    expected_payload_keys = {
        "draft_manifest_sha256",
        "draft_audit_sha256",
        "draft_predecessor_audit_sha256",
        "features_sha256",
        "independent_audit_sha256",
        "family_audit_sha256",
        "source_code_inventory_sha256",
        "build_identity_sha256",
        "config_sha256",
        "amendment_sha256",
        "schema_sha256",
        "row_identity_sha256",
        "decision_sha256",
        "lineage_audit_sha256",
    }
    if retry_contract is not None:
        expected_payload_keys.add("technical_retry")
    if set(payload) != expected_payload_keys:
        _fail("attestation payload fields are not the frozen v2 identity set")
    if manifest.get("attestation_sha256") != _sha256_bytes(canonical_json(payload)):
        _fail("attestation payload hash is invalid")
    for name in expected_payload_keys - {"family_audit_sha256", "technical_retry"}:
        _sha(payload.get(name), f"attestation payload {name}")
    family_payload_hashes = _mapping(
        payload.get("family_audit_sha256"), "attestation payload family_audit_sha256"
    )
    if set(family_payload_hashes) != set(_INDEPENDENT_FAMILY_IDS):
        _fail("attestation family audit hashes do not contain exactly the four v2 families")
    for family_id in _INDEPENDENT_FAMILY_IDS:
        _sha(
            family_payload_hashes.get(family_id),
            f"attestation payload family_audit_sha256.{family_id}",
        )

    required_payloads = {
        "features.parquet",
        "draft-manifest.json",
        "draft-audit.json",
        "predecessor-audit.json",
        "independent-audit.json",
        "lineage-audit.json",
    }
    if not required_payloads.issubset(files):
        missing = sorted(required_payloads - set(files))
        _fail(f"attested root is missing required sidecars: {missing}")

    independent_name = _safe_relative(
        manifest.get("independent_audit_path"), "independent_audit_path"
    )
    predecessor_name = _safe_relative(
        manifest.get("predecessor_audit_path"), "predecessor_audit_path"
    )
    if independent_name != "independent-audit.json":
        _fail("independent_audit_path is not the frozen v2 sidecar")
    if predecessor_name != "predecessor-audit.json":
        _fail("predecessor_audit_path is not the frozen v2 sidecar")
    if manifest.get("audit_path") != independent_name:
        _fail("audit_path is not bound to the independent audit sidecar")

    family_paths_value = _mapping(manifest.get("family_audit_paths"), "family_audit_paths")
    if set(family_paths_value) != set(_INDEPENDENT_FAMILY_IDS):
        _fail("family_audit_paths do not contain exactly the four v2 families")
    family_paths: dict[str, str] = {}
    for family_id in _INDEPENDENT_FAMILY_IDS:
        family_path = _safe_relative(
            family_paths_value.get(family_id), f"family_audit_paths.{family_id}"
        )
        expected_family_path = f"{family_id.replace('-', '_')}-viability-audit.json"
        if family_path != expected_family_path:
            _fail(f"family_audit_paths.{family_id} is not the frozen v2 sidecar")
        if family_path in {independent_name, predecessor_name, "lineage-audit.json"}:
            _fail(f"family_audit_paths.{family_id} collides with a required sidecar")
        family_paths[family_id] = family_path
        if family_path not in files:
            _fail(f"family sidecar {family_id} is not sealed in the file inventory")

    lineage_ref = _mapping(manifest.get("lineage_audit"), "lineage_audit")
    lineage_name = _safe_relative(lineage_ref.get("path"), "lineage_audit.path")
    if lineage_name != "lineage-audit.json":
        _fail("lineage_audit.path is not the frozen v2 sidecar")

    independent_sha = _inventory_sha(files, independent_name, "independent audit")
    predecessor_sha = _inventory_sha(files, predecessor_name, "predecessor audit")
    lineage_sha = _inventory_sha(files, lineage_name, "lineage audit")
    if manifest.get("independent_audit_sha256") != independent_sha:
        _fail("independent audit hash differs from its inventory")
    if manifest.get("audit_sha256") != independent_sha:
        _fail("audit hash is not bound to the independent audit")
    if manifest.get("predecessor_audit_sha256") != predecessor_sha:
        _fail("predecessor audit hash differs from its inventory")
    if lineage_ref.get("sha256") != lineage_sha:
        _fail("lineage audit hash differs from its inventory")
    if lineage_ref.get("kind") is None or lineage_ref.get("version") != 2:
        _fail("lineage audit reference is not v2")

    family_hashes = _mapping(manifest.get("family_audit_sha256"), "family_audit_sha256")
    if set(family_hashes) != set(_INDEPENDENT_FAMILY_IDS):
        _fail("family_audit_sha256 does not contain exactly the four v2 families")
    for family_id, family_path in family_paths.items():
        digest = _inventory_sha(files, family_path, f"family audit {family_id}")
        if family_hashes.get(family_id) != digest:
            _fail(f"family audit hash differs for {family_id}")
        if family_payload_hashes.get(family_id) != digest:
            _fail(f"attestation family audit hash differs for {family_id}")

    for field, digest in (
        ("draft_manifest_sha256", _inventory_sha(files, "draft-manifest.json", "draft manifest")),
        ("draft_audit_sha256", _inventory_sha(files, "draft-audit.json", "draft audit")),
        ("draft_predecessor_audit_sha256", predecessor_sha),
        ("features_sha256", _inventory_sha(files, "features.parquet", "features")),
        ("independent_audit_sha256", independent_sha),
        ("lineage_audit_sha256", lineage_sha),
    ):
        if payload.get(field) != digest or (field in manifest and manifest.get(field) != digest):
            _fail(f"attestation {field} is not cross-bound to the sealed inventory")
    for field, digest in (
        ("draft_manifest_sha256", _inventory_sha(files, "draft-manifest.json", "draft manifest")),
        ("draft_audit_sha256", _inventory_sha(files, "draft-audit.json", "draft audit")),
        ("draft_predecessor_audit_sha256", predecessor_sha),
    ):
        if manifest.get(field) != digest:
            _fail(f"attested manifest {field} is missing or differs from its inventory")

    if payload.get("schema_sha256") != manifest.get("schema_sha256"):
        _fail("attestation schema identity differs from the manifest")
    if payload.get("config_sha256") != manifest.get("config_sha256"):
        _fail("attestation config identity differs from the manifest")
    if payload.get("amendment_sha256") != manifest.get("amendment_sha256"):
        _fail("attestation amendment identity differs from the manifest")

    # Read all sidecars only after the inventory has proved that every byte is
    # present and sealed.  Canonical parsing also rejects duplicate keys.
    independent = _load_json(root / independent_name, "independent audit")
    predecessor = _load_json(root / predecessor_name, "predecessor audit")
    lineage = _load_json(root / lineage_name, "external lineage audit")
    draft_manifest = _load_json(root / "draft-manifest.json", "draft manifest copy")
    draft_audit = _load_json(root / "draft-audit.json", "draft audit copy")
    family_payloads = {
        family_id: _load_json(root / family_path, f"{family_id} family audit")
        for family_id, family_path in family_paths.items()
    }
    retry_binding: dict[str, object] | None = None
    if retry_contract is not None:
        retry_binding = _verify_retry_feature_binding(manifest, retry_contract)
        if independent.get("technical_retry") != retry_binding:
            _fail("Retry-1 independent technical binding differs from manifest")
        if payload.get("technical_retry") != retry_binding:
            _fail("Retry-1 attestation technical binding differs from manifest")

    if (
        draft_manifest.get("kind") != FEATURE_KIND
        or draft_manifest.get("version") != FEATURE_VERSION
        or draft_manifest.get("status") not in {"complete", "draft"}
    ):
        _fail("draft-manifest sidecar is not a corrected v2 source root")
    _assert_development_audit_flags(draft_manifest, "draft-manifest sidecar")
    if draft_audit.get("version") != FEATURE_VERSION:
        _fail("draft-audit sidecar is not corrected v2 evidence")
    _assert_development_audit_flags(draft_audit, "draft-audit sidecar")
    if predecessor.get("version") != FEATURE_VERSION:
        _fail("predecessor audit sidecar is not corrected v2 evidence")
    _assert_development_audit_flags(predecessor, "predecessor audit sidecar")
    if draft_manifest.get("audit_sha256") != _inventory_sha(
        files, "draft-audit.json", "draft audit"
    ):
        _fail("draft-manifest audit identity differs from its copied audit sidecar")
    if draft_manifest.get("predecessor_audit_sha256") != predecessor_sha:
        _fail("draft-manifest predecessor identity differs from its copied sidecar")
    draft_files = _file_inventory(draft_manifest.get("files"), "draft-manifest files")
    draft_to_attested = {"audit.json": "draft-audit.json"}
    for name, entry in draft_files.items():
        copied_name = draft_to_attested.get(name, name)
        copied = files.get(copied_name)
        if copied is None or (
            entry.get("byte_count") != copied.get("byte_count")
            or entry.get("sha256") != copied.get("sha256")
        ):
            _fail(f"draft-manifest file identity differs for {name}")
    for name in (
        "development_only",
        "start_slot_inclusive",
        "end_slot_exclusive",
        "final_holdout_start_slot_inclusive",
        "final_holdout_end_slot_exclusive",
        "physical_order",
        "row_count",
        "v1_manifest_sha256",
        "v1_audit_sha256",
        "mode_index_manifest_sha256",
        "mode_index_audit_sha256",
        "mode_index_modes_sha256",
        "schema_sha256",
        "predecessor_audit_sha256",
        "references",
        "input_v1",
        "market_mode_index",
        "mode_index_columns",
        "allowed_source_columns",
        "allowed_column_contract",
        "correction",
    ):
        if manifest.get(name) != draft_manifest.get(name):
            _fail(f"attested manifest {name} differs from the copied draft manifest")
    _verify_provenance_path_declaration(manifest.get("draft_root"), "draft_root")
    _verify_provenance_path_declaration(
        _mapping(manifest.get("input_v1"), "input_v1").get("root"),
        "input_v1.root",
    )
    _verify_provenance_path_declaration(
        _mapping(manifest.get("market_mode_index"), "market_mode_index").get("root"),
        "market_mode_index.root",
    )

    if (
        independent.get("kind") != INDEPENDENT_ATTESTATION_KIND
        or independent.get("version") != INDEPENDENT_ATTESTATION_VERSION
        or independent.get("status") != INDEPENDENT_AUDIT_STATUS
    ):
        _fail("independent audit sidecar is not verified v2 evidence")
    _assert_development_audit_flags(independent, "independent audit sidecar", require_all=True)
    if (
        independent.get("start_slot_inclusive") != contract.dev.start_slot_inclusive
        or independent.get("end_slot_exclusive") != contract.dev.end_slot_exclusive
        or independent.get("final_holdout_start_slot_inclusive")
        != contract.holdout.interval.start_slot_inclusive
        or independent.get("final_holdout_end_slot_exclusive")
        != contract.holdout.interval.end_slot_exclusive
        or independent.get("physical_order") != "slot,tx_index,log_index,event_index,event_id"
    ):
        _fail("independent audit sidecar violates the frozen slot/order contract")
    if (
        independent.get("draft_manifest_sha256") != manifest.get("draft_manifest_sha256")
        or independent.get("draft_audit_sha256") != manifest.get("draft_audit_sha256")
        or independent.get("draft_predecessor_audit_sha256")
        != manifest.get("draft_predecessor_audit_sha256")
    ):
        _fail("independent audit draft identities differ from the attested root")

    # Validate the external lineage declarations using the independent
    # verifier's binding rules.  Build the already-verified draft view from
    # the attested bytes in this root; reopening and rehashing the original v1
    # and mode roots here would make every one of the four grids repeat the
    # publication-time source audit and would make the attested root needlessly
    # non-portable.
    try:
        from pumpquant.audits.correction_v2 import feature_audit as independent_audit

        draft = independent_audit._Draft(
            root=root,
            manifest_path=root / "manifest.json",
            manifest_body=manifest_body,
            manifest=dict(manifest),
            files={name: dict(entry) for name, entry in files.items()},
            features_path=feature_path,
            audit_path=root / independent_name,
            audit_body=(root / independent_name).read_bytes(),
            audit=dict(independent),
            predecessor_path=root / predecessor_name,
            predecessor_body=(root / predecessor_name).read_bytes(),
            predecessor=dict(predecessor),
            v1_manifest_sha256=_sha(manifest.get("v1_manifest_sha256"), "v1_manifest_sha256"),
            v1_audit_sha256=_sha(manifest.get("v1_audit_sha256"), "v1_audit_sha256"),
            mode_manifest_sha256=_sha(
                manifest.get("mode_index_manifest_sha256"),
                "mode_index_manifest_sha256",
            ),
            mode_audit_sha256=_sha(
                manifest.get("mode_index_audit_sha256"), "mode_index_audit_sha256"
            ),
            mode_modes_sha256=_sha(
                manifest.get("mode_index_modes_sha256"), "mode_index_modes_sha256"
            ),
            config_sha256=_sha(manifest.get("config_sha256"), "config_sha256"),
            amendment_sha256=_sha(manifest.get("amendment_sha256"), "amendment_sha256"),
            contract=contract,
        )
        independent_audit._validate_lineage_binding(lineage, draft)
    except Exception as error:
        if isinstance(error, CorrectedFeatureVerificationError):
            raise
        _fail(f"external lineage audit is invalid: {error}")
    if lineage.get("kind") != lineage_ref.get("kind") or lineage.get("version") != lineage_ref.get(
        "version"
    ):
        _fail("lineage audit identity differs from its manifest reference")
    if (
        independent.get("lineage_audit_sha256") != lineage_sha
        or payload.get("lineage_audit_sha256") != lineage_sha
    ):
        _fail("independent lineage identity differs from its sealed sidecar")
    lineage_sidecar = _mapping(independent.get("external_lineage_audit"), "independent lineage")
    if (
        lineage_sidecar.get("kind") != lineage.get("kind")
        or lineage_sidecar.get("version") != lineage.get("version")
        or lineage_sidecar.get("lineage_audit_sha256") != lineage_sha
    ):
        _fail("independent lineage metadata is not cross-bound")

    # The row stream was independently recomputed exactly once before this
    # immutable root was published.  At grid time the feature SHA, attestation
    # envelope, canonical independent sidecar, lineage sidecar, and four family
    # sidecars are all cross-bound.  Re-running `_stream_features` here would
    # add four redundant ordered Python scans without adding a new trust
    # boundary.  Recompute only the small current-code identity.
    try:
        if retry_contract is None:
            evaluation_code = independent_audit._evaluation_code_audit(draft)
        else:
            assert retry_binding is not None
            evaluation_code = independent_audit._evaluation_code_audit(
                draft,
                retry_contract=retry_contract,
                retry_config_path=Path(RETRY_CONFIG_PATH),
                parent_source_code_inventory_sha256=_sha(
                    retry_binding.get("parent_source_code_inventory_sha256"),
                    "Retry-1 parent source inventory",
                ),
                parent_build_identity_sha256=_sha(
                    retry_binding.get("parent_build_identity_sha256"),
                    "Retry-1 parent build identity",
                ),
            )
        # ``_evaluation_code_audit`` is assembled from dataclasses and may
        # contain tuples; the published JSON necessarily carries arrays.
        # Canonical round-tripping makes the comparison byte/shape exact to
        # what the attestation sealer wrote.
        evaluation_code = cast(
            dict[str, object], json.loads(canonical_json(evaluation_code).decode("utf-8"))
        )
    except Exception as error:
        if isinstance(error, CorrectedFeatureVerificationError):
            raise
        _fail(f"independent v2 audit cannot be recomputed: {error}")

    # Both legacy v2 and Retry-1 must match the files in the running immutable
    # bundle.  Retry-1 changes the inventory relative to its parent, not
    # relative to the current runtime; skipping this comparison would let
    # unbound evaluation code pass whenever the preregistration uses a
    # publication-time inventory binding.
    if independent.get("evaluation_code_audit") != evaluation_code:
        _fail("independent evaluation-code audit differs from current files")
    if manifest.get("evaluation_code_audit") != evaluation_code:
        _fail("manifest evaluation-code audit differs from current files")
    if retry_contract is not None and (
        independent.get("source_code_inventory_sha256")
        != manifest.get("source_code_inventory_sha256")
        or independent.get("build_identity_sha256") != manifest.get("build_identity_sha256")
    ):
        _fail("Retry-1 independent and manifest code identities differ")
    code_reference = _mapping(
        _mapping(manifest.get("references"), "attested references").get("code"),
        "attested references.code",
    )
    materializer_code = _mapping(
        independent.get("materializer_code_reference"),
        "independent materializer-code reference",
    )
    if materializer_code != code_reference:
        _fail("materializer code reference differs from publication-time evidence")
    if manifest.get("family_aggregation") != independent.get("family_aggregation"):
        _fail("manifest family aggregation differs from independent evidence")
    families_value = _mapping(independent.get("family_eligibility"), "independent families")
    if set(families_value) != set(_INDEPENDENT_FAMILY_IDS):
        _fail("independent audit does not contain exactly the four v2 families")
    families = {
        family_id: _mapping(families_value[family_id], f"independent family {family_id}")
        for family_id in _INDEPENDENT_FAMILY_IDS
    }
    if manifest.get("family_eligibility") != families:
        _fail("manifest family eligibility differs from independent evidence")
    if manifest.get("row_count") != independent.get("row_count"):
        _fail("manifest row count differs from independent evidence")
    if manifest.get("phase_counts") != independent.get("phase_counts"):
        _fail("manifest phase counts differ from independent evidence")
    aggregation = _mapping(independent.get("family_aggregation"), "independent family aggregation")
    if (
        aggregation.get("algorithm") != "family_feature_mask_v2"
        or aggregation.get("candidate_registry_row_iterations") != 0
        or aggregation.get("candidate_row_digest_updates") != 0
    ):
        _fail("independent family aggregation used an unsupported hot-path algorithm")

    for family_id, expected_family in families.items():
        actual_family = family_payloads[family_id]
        if actual_family != expected_family:
            _fail(f"family audit payload differs for {family_id}")
        family_contract = contract.family_map[family_id]
        if (
            expected_family.get("family_id") != family_id
            or expected_family.get("family_version") != INDEPENDENT_ATTESTATION_VERSION
            or expected_family.get("candidate_count") != family_contract.candidate_count
            or expected_family.get("candidate_order") != list(family_contract.candidate_hashes)
            or expected_family.get("candidate_order_sha256")
            != family_contract.candidate_order_sha256
        ):
            _fail(f"family audit candidate identity differs for {family_id}")
        family_without_hash = {
            key: value for key, value in actual_family.items() if key != "audit_sha256"
        }
        if actual_family.get("audit_sha256") != _sha256_bytes(canonical_json(family_without_hash)):
            _fail(f"family audit self-hash is invalid for {family_id}")

    current_inventory = evaluation_code.get("source_code_inventory_sha256")
    current_build = evaluation_code.get("build_identity_sha256")
    if (
        manifest.get("source_code_inventory_sha256") != current_inventory
        or manifest.get("build_identity_sha256") != current_build
        or payload.get("source_code_inventory_sha256") != current_inventory
        or payload.get("build_identity_sha256") != current_build
    ):
        _fail("source code inventory/build identity differs from current files")
    if payload.get("row_identity_sha256") != independent.get("row_identity_sha256") or payload.get(
        "decision_sha256"
    ) != independent.get("decision_sha256"):
        _fail("attestation row/decision identity differs from independent evidence")
    # Keep a final direct check here even though the normal reference checks
    # below also validate these values.  The attestation is a single identity
    # envelope and every identity must agree before a grid can open a row.
    feature_sha = _inventory_sha(files, "features.parquet", "features")
    if payload.get("features_sha256") != feature_sha:
        _fail("attestation feature identity differs from the feature payload")
    if manifest.get("row_count") != independent.get("row_count"):
        _fail("attestation row count differs from independent evidence")
    if retry_contract is not None:
        assert retry_binding is not None
        actual_stable = {
            "draft_manifest_sha256": _inventory_sha(files, "draft-manifest.json", "draft manifest"),
            "draft_audit_sha256": _inventory_sha(files, "draft-audit.json", "draft audit"),
            "predecessor_sha256": predecessor_sha,
            "features_sha256": feature_sha,
            "row_identity_sha256": _sha(independent.get("row_identity_sha256"), "row identity"),
            "decision_sha256": _sha(independent.get("decision_sha256"), "decision identity"),
            "lineage_sha256": lineage_sha,
        }
        if _mapping(retry_binding.get("stable_data"), "Retry-1 stable_data") != actual_stable:
            _fail("Retry-1 stable data identities differ from sealed payloads")
    config_sha = sha256_file(config_path)
    expected_config = expected_config_sha256 or config_sha
    if config_sha != expected_config or manifest.get("config_sha256") != expected_config:
        _fail("attestation config identity differs from the current config")
    expected_amendment = expected_amendment_sha256 or contract.amendment.sha256
    if manifest.get("amendment_sha256") != expected_amendment:
        _fail("attestation amendment identity differs from the correction contract")
    if manifest_body != canonical_json(dict(manifest)):
        _fail("attested manifest is not canonical JSON")


def _verify_retry_feature_binding(
    manifest: Mapping[str, object],
    retry: CorrectionV2RetryContract,
) -> dict[str, object]:
    """Require a fresh, internally consistent Retry-1 feature publication."""

    binding = _mapping(manifest.get("technical_retry"), "technical_retry")
    expected_fields = {
        "retry_id",
        "retry_config_sha256",
        "retry_document_sha256",
        "parent_config_sha256",
        "parent_amendment_sha256",
        "parent_attested_manifest_sha256",
        "parent_shock_manifest_sha256",
        "stable_data",
        "parent_source_code_inventory_sha256",
        "parent_build_identity_sha256",
        "source_code_inventory_sha256",
        "build_identity_sha256",
        "retry_requirements",
        "development_only",
        "holdout_payload_reads_allowed",
        "holdout_feature_generation_allowed",
        "holdout_accessed",
        "outcomes_read",
        "reports_read",
        "s3_read",
    }
    if set(binding) != expected_fields:
        _fail("Retry-1 technical_retry fields differ from the frozen schema")
    if binding.get("retry_id") != retry.retry_id:
        _fail("feature root retry_id differs from Retry-1 preregistration")
    if binding.get("retry_config_sha256") != retry.config_sha256:
        _fail("Retry-1 feature config identity differs from preregistration")
    if binding.get("retry_document_sha256") != retry.retry_document.sha256:
        _fail("Retry-1 feature document identity differs from preregistration")
    parent = retry.parent
    parent_values = {
        "parent_config_sha256": retry.parent_config_sha256,
        "parent_amendment_sha256": retry.parent_amendment_sha256,
        "parent_attested_manifest_sha256": parent.attested_manifest_sha256,
        "parent_shock_manifest_sha256": parent.shock_manifest_sha256,
    }
    for field, expected in parent_values.items():
        if binding.get(field) != expected:
            _fail(f"Retry-1 feature {field} differs from parent binding")
    stable = _mapping(binding.get("stable_data"), "Retry-1 feature stable_data")
    if stable != dict(retry.stable_data):
        _fail("Retry-1 feature stable data identities differ from parent")
    inventory = _sha(
        binding.get("source_code_inventory_sha256"),
        "Retry-1 feature code inventory",
    )
    build = _sha(binding.get("build_identity_sha256"), "Retry-1 feature build identity")
    if manifest.get("source_code_inventory_sha256") != inventory:
        _fail("Retry-1 nested and manifest code inventories differ")
    if manifest.get("build_identity_sha256") != build:
        _fail("Retry-1 nested and manifest build identities differ")
    parent_inventory = _sha(
        binding.get("parent_source_code_inventory_sha256"),
        "Retry-1 parent code inventory",
    )
    parent_build = _sha(
        binding.get("parent_build_identity_sha256"),
        "Retry-1 parent build identity",
    )
    if parent_inventory == inventory:
        _fail("Retry-1 feature code inventory was not changed from parent")
    if parent_build == build:
        _fail("Retry-1 feature build identity was not changed from parent")
    requirements = _mapping(binding.get("retry_requirements"), "Retry-1 feature retry_requirements")
    if requirements != dict(retry.requirements):
        _fail("Retry-1 feature requirements differ from preregistration")
    _assert_development_audit_flags(binding, "Retry-1 technical_retry", require_all=True)
    return binding


def verify_corrected_feature_dataset(
    root: Path,
    *,
    contract: CorrectionV2Contract | None = None,
    config_path: str | Path = CONFIG_PATH,
    technical_retry_config: str | Path | None = None,
    retry_contract: CorrectionV2RetryContract | None = None,
) -> tuple[dict[str, object], Path, str]:
    """Verify one sealed v2 corrected feature root before any grid query.

    Every payload listed by the manifest is hashed, the root cannot contain
    unlisted files, and the publication-time source/mode lineage plus current
    predecessor, amendment, configuration, and evaluation-code bindings are
    cross-checked.  A v1 feature root fails immediately on its kind/version.
    """

    contract_value = load_correction_v2_contract() if contract is None else contract
    retry_value = retry_contract
    if technical_retry_config is not None:
        if retry_value is not None:
            raise CorrectedGridV2Error("Retry-1 config was supplied twice")
        try:
            retry_value = load_correction_v2_retry_contract(technical_retry_config)
        except Exception as error:
            raise CorrectedGridV2Error(f"cannot load Retry-1 config: {error}") from error
    if retry_value is not None and retry_value.parent_contract.kind != contract_value.kind:
        raise CorrectedGridV2Error("Retry-1 parent contract kind differs from v2 contract")
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        _fail("corrected feature input must be a regular directory")
    if any(_holdout_component(part) for part in root.parts):
        _fail("corrected feature input may not reference a holdout path")
    try:
        resolved_root = root.resolve(strict=True)
    except OSError as error:
        _fail(f"corrected feature input cannot be resolved safely: {error}")
    if resolved_root != root.absolute() or any(
        _holdout_component(part) for part in resolved_root.parts
    ):
        _fail("corrected feature input may not traverse a symlink or holdout path")
    manifest_path = root / "manifest.json"
    _regular_file(manifest_path, "corrected feature manifest")
    body = manifest_path.read_bytes()
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        _fail("corrected feature manifest is invalid JSON")
        raise AssertionError from error
    manifest = _mapping(parsed, "corrected feature manifest")
    if body != canonical_json(manifest):
        _fail("corrected feature manifest is not canonical JSON")
    if retry_value is not None:
        _verify_retry_feature_binding(manifest, retry_value)
    if (
        manifest.get("kind") != FEATURE_KIND
        or manifest.get("version") != FEATURE_VERSION
        or manifest.get("status") != "complete"
        or manifest.get("development_only") is not True
    ):
        _fail("feature input is not a sealed corrected v2 dataset")
    if (
        manifest.get("start_slot_inclusive") != contract_value.dev.start_slot_inclusive
        or manifest.get("end_slot_exclusive") != contract_value.dev.end_slot_exclusive
        or manifest.get("final_holdout_start_slot_inclusive")
        != contract_value.holdout.interval.start_slot_inclusive
        or manifest.get("final_holdout_end_slot_exclusive")
        != contract_value.holdout.interval.end_slot_exclusive
    ):
        _fail("corrected feature bounds differ from the correction contract")
    _reject_downstream_flags(manifest, "corrected feature manifest")
    for name in (
        "holdout_payload_reads_allowed",
        "holdout_feature_generation_allowed",
        "holdout_accessed",
        "outcomes_read",
        "reports_read",
        "s3_read",
    ):
        if name not in manifest:
            _fail(f"corrected feature manifest is missing {name}")
    files = _verify_inventory(root, manifest.get("files"), "corrected feature files")
    feature_meta = files.get("features.parquet")
    if feature_meta is None:
        _fail("corrected feature files do not inventory features.parquet")
    feature_path = root / "features.parquet"
    declared_rows = _integer(feature_meta.get("row_count"), "corrected feature row_count")
    if manifest.get("row_count") != declared_rows:
        _fail("corrected feature row counts disagree")
    _verify_feature_parquet(
        feature_path,
        manifest,
        expected_rows=declared_rows,
        start_slot=contract_value.dev.start_slot_inclusive,
        end_slot=contract_value.dev.end_slot_exclusive,
    )
    references = _mapping(manifest.get("references"), "corrected feature references")
    config_path_value = Path(config_path)
    if not config_path_value.is_file() or config_path_value.is_symlink():
        _fail("correction config file is absent or unsafe")
    config_sha = sha256_file(config_path_value)
    expected_config_path = Path(CONFIG_PATH).as_posix()
    expected_config_sha = sha256_file(Path(CONFIG_PATH))
    if config_sha != expected_config_sha:
        _fail("runtime config path differs from the canonical correction config")
    if retry_value is not None and config_sha != retry_value.parent_config_sha256:
        _fail("Retry-1 parent correction config differs from preregistration")
    _verify_reference(
        _mapping(references.get("config"), "references.config"),
        label="references.config",
        expected_path=expected_config_path,
        expected_sha256=expected_config_sha,
        expected_file=config_path_value,
    )
    amendment = contract_value.amendment
    amendment_path_text = amendment.path
    amendment_sha = amendment.sha256
    if amendment_sha is None:
        _fail("correction amendment uses an unresolved deployment binding")
    amendment_path = Path(amendment_path_text)
    _verify_reference(
        _mapping(references.get("amendment"), "references.amendment"),
        label="references.amendment",
        expected_path=amendment_path_text,
        expected_sha256=amendment_sha,
        expected_file=amendment_path,
    )
    _verify_attested_code_declaration(_mapping(references.get("code"), "references.code"))
    _verify_attested_root(
        root,
        manifest,
        files,
        manifest_body=body,
        feature_path=feature_path,
        contract=contract_value,
        config_path=config_path_value,
        expected_config_sha256=expected_config_sha,
        expected_amendment_sha256=amendment_sha,
        retry_contract=retry_value,
    )
    manifest_sha = _sha256_bytes(body)
    # Bind the resolved canonical values for profile/report publication.
    return manifest, feature_path, manifest_sha


def _registry_call(family_id: str) -> tuple[tuple[object, ...], Mapping[str, object]]:
    if family_id == "shock-recovery-fresh-v2":
        from pumpquant.research.families.shock_recovery import default_shock_recovery_candidates

        return tuple(default_shock_recovery_candidates()), _registry_payload_for_family(family_id)
    if family_id == "progress-speed-fresh-v2":
        from pumpquant.research.families.progress_speed import (
            default_progress_speed_candidates,
        )

        return tuple(default_progress_speed_candidates()), _registry_payload_for_family(family_id)
    if family_id == "persistent-pressure-fresh-v2":
        from pumpquant.research.families.persistent_pressure import (
            default_persistent_pressure_candidates,
        )

        return tuple(default_persistent_pressure_candidates()), _registry_payload_for_family(
            family_id
        )
    from pumpquant.research.families.amm_impact_ofi import (
        default_amm_impact_ofi_candidates,
    )

    return tuple(default_amm_impact_ofi_candidates()), _registry_payload_for_family(family_id)


def _assert_candidate_registry(family: FamilyContract) -> None:
    candidates, payload = _registry_call(family.id)
    hashes = tuple(getattr(candidate, "sha256", None) for candidate in candidates)
    if hashes != family.candidate_hashes:
        raise CorrectedGridV2Error(f"{family.id} candidate order differs from correction contract")
    candidate_order_hash = _sha256_bytes("\n".join(cast(tuple[str, ...], hashes)).encode())
    if candidate_order_hash != family.candidate_order_sha256:
        raise CorrectedGridV2Error(f"{family.id} candidate-order hash is not recomputable")
    # Use the contract's sealed digest path so the bridge inherits its
    # cross-runtime normalization for the derived NormalDist z-threshold.
    # Candidate identity/order and every non-derived registry field remain
    # byte-sensitive.
    if _source_registry_digest(payload) != family.registry_sha256:
        raise CorrectedGridV2Error(f"{family.id} registry hash differs from correction contract")


def _profile_report_kind(family: FamilyContract) -> tuple[str, str, str]:
    stem = family.id.removesuffix("-fresh-v2").replace("-", "_")
    return (
        f"pumpquant.{stem}_correction_v2_report",
        f"pumpquant.{stem}_correction_v2_manifest",
        f"pumpquant.{stem}_correction_v2_registry",
    )


def build_runtime_profile(
    family: str,
    *,
    contract: CorrectionV2Contract | None = None,
    feature_manifest: Mapping[str, object] | None = None,
    feature_manifest_sha256: str | None = None,
    config_path: str | Path = CONFIG_PATH,
    technical_retry_config: str | Path | None = None,
    retry_contract: CorrectionV2RetryContract | None = None,
) -> GridRuntimeProfile:
    """Build and verify one family profile from the sealed correction config."""

    contract_value = load_correction_v2_contract() if contract is None else contract
    retry_value = retry_contract
    if technical_retry_config is not None:
        if retry_value is not None:
            raise CorrectedGridV2Error("Retry-1 config was supplied twice")
        try:
            retry_value = load_correction_v2_retry_contract(technical_retry_config)
        except Exception as error:
            raise CorrectedGridV2Error(f"cannot load Retry-1 config: {error}") from error
    if retry_value is not None and retry_value.parent_contract.kind != contract_value.kind:
        raise CorrectedGridV2Error("Retry-1 parent contract differs from supplied v2 contract")
    try:
        family_id = _FAMILY_ALIASES[family.strip().lower()]
    except (AttributeError, KeyError) as error:
        raise CorrectedGridV2Error(
            "family must be one of shock_recovery, progress_speed, "
            "persistent_pressure, amm_impact_ofi"
        ) from error
    family_contract = contract_value.family_map[family_id]
    _assert_candidate_registry(family_contract)
    report_kind, manifest_kind, registry_kind = _profile_report_kind(family_contract)
    config_file = Path(config_path)
    if not config_file.is_file() or config_file.is_symlink():
        raise CorrectedGridV2Error("correction config file is absent or unsafe")
    config_sha = sha256_file(config_file)
    if config_sha != sha256_file(Path(CONFIG_PATH)):
        raise CorrectedGridV2Error("runtime config differs from canonical correction config")
    if retry_value is not None:
        if config_sha != retry_value.parent_config_sha256:
            raise CorrectedGridV2Error(
                "Retry-1 parent correction config differs from preregistration"
            )
        retry_config_file = Path(technical_retry_config or RETRY_CONFIG_PATH)
        if retry_config_file.is_symlink() or not retry_config_file.is_file():
            raise CorrectedGridV2Error("Retry-1 config file is absent or unsafe")
        if sha256_file(retry_config_file) != retry_value.config_sha256:
            raise CorrectedGridV2Error("runtime Retry-1 config differs from its preregistration")
    if feature_manifest is None or feature_manifest_sha256 is None:
        raise CorrectedGridV2Error("a verified corrected feature manifest is required")
    retry_binding = (
        _verify_retry_feature_binding(feature_manifest, retry_value)
        if retry_value is not None
        else None
    )
    predecessor_sha = _sha(feature_manifest.get("predecessor_audit_sha256"), "predecessor audit")
    references = _mapping(feature_manifest.get("references"), "feature references")
    code_reference = _mapping(references.get("code"), "feature references.code")
    code_sha = _sha(code_reference.get("sha256"), "feature code sha256")
    v1_manifest_sha = _sha(feature_manifest.get("v1_manifest_sha256"), "v1 manifest")
    v1_audit_sha = _sha(feature_manifest.get("v1_audit_sha256"), "v1 audit")
    mode_ref = _mapping(feature_manifest.get("market_mode_index"), "market mode")
    mode_manifest_sha = _sha(mode_ref.get("manifest_sha256"), "mode manifest")
    mode_audit_sha = _sha(mode_ref.get("audit_sha256"), "mode audit")
    mode_modes_sha = _sha(mode_ref.get("modes_sha256"), "mode modes")
    amendment_sha = contract_value.amendment.sha256
    if amendment_sha is None:
        raise CorrectedGridV2Error("correction amendment digest is unresolved")

    def verifier(root: Path) -> tuple[dict[str, object], Path, str]:
        return verify_corrected_feature_dataset(
            root,
            contract=contract_value,
            config_path=config_file,
            retry_contract=retry_value,
        )

    retry_family = retry_value.family_map[family_id] if retry_value is not None else None
    return GridRuntimeProfile(
        family=family_contract,
        report_kind=report_kind,
        manifest_kind=manifest_kind,
        registry_kind=registry_kind,
        experiment_id=family_contract.id,
        version=2,
        trial_denominator=(
            retry_family.cumulative_denominator
            if retry_family is not None
            else family_contract.cumulative_denominator
        ),
        z_threshold=(
            retry_family.z_threshold if retry_family is not None else family_contract.z_threshold
        ),
        bootstrap_seed=family_contract.bootstrap_seed,
        corrected_feature_manifest_sha256=_sha(feature_manifest_sha256, "feature manifest"),
        contract_sha256=config_sha,
        amendment_sha256=amendment_sha,
        mode_index_manifest_sha256=mode_manifest_sha,
        mode_index_audit_sha256=mode_audit_sha,
        mode_index_modes_sha256=mode_modes_sha,
        predecessor_audit_sha256=predecessor_sha,
        corrected_code_sha256=code_sha,
        v1_manifest_sha256=v1_manifest_sha,
        v1_audit_sha256=v1_audit_sha,
        feature_verifier=verifier,
        retry_contract=retry_value,
        retry_id=None if retry_value is None else retry_value.retry_id,
        retry_config_sha256=None if retry_value is None else retry_value.config_sha256,
        retry_document_sha256=None if retry_value is None else retry_value.retry_document.sha256,
        parent_config_sha256=None if retry_value is None else retry_value.parent_config_sha256,
        parent_amendment_sha256=None
        if retry_value is None
        else retry_value.parent_amendment_sha256,
        parent_attested_manifest_sha256=(
            None if retry_value is None else retry_value.parent.attested_manifest_sha256
        ),
        parent_shock_manifest_sha256=(
            None if retry_value is None else retry_value.parent.shock_manifest_sha256
        ),
        technical_retry_sha256=(
            None if retry_binding is None else _sha256_bytes(canonical_json(retry_binding))
        ),
        parent_source_code_inventory_sha256=(
            None
            if retry_binding is None
            else _sha(
                retry_binding.get("parent_source_code_inventory_sha256"),
                "Retry-1 parent source code inventory",
            )
        ),
        parent_build_identity_sha256=(
            None
            if retry_binding is None
            else _sha(
                retry_binding.get("parent_build_identity_sha256"),
                "Retry-1 parent build identity",
            )
        ),
        retry_source_code_inventory_sha256=(
            None
            if retry_value is None
            else _sha(
                feature_manifest.get("source_code_inventory_sha256"),
                "Retry-1 source code inventory",
            )
        ),
        retry_build_identity_sha256=(
            None
            if retry_value is None
            else _sha(feature_manifest.get("build_identity_sha256"), "Retry-1 build identity")
        ),
    )


def _binding_payload(profile: GridRuntimeProfile) -> dict[str, object]:
    binding: dict[str, object] = {
        "contract_kind": "pumpquant.fresh_bonding_prestate_correction_v2_contract",
        "contract_version": 2,
        "contract_sha256": profile.contract_sha256,
        "correction_contract_sha256": profile.contract_sha256,
        "config_sha256": profile.contract_sha256,
        "correction_id": CORRECTION_ID,
        "family_id": profile.family.id,
        "family_kind": profile.family.kind,
        "family_version": profile.family.version,
        "experiment_id": profile.experiment_id,
        "report_kind": profile.report_kind,
        "manifest_kind": profile.manifest_kind,
        "candidate_count": profile.family.candidate_count,
        "candidate_order_sha256": profile.family.candidate_order_sha256,
        "registry_sha256": profile.family.registry_sha256,
        "bootstrap_seed": profile.bootstrap_seed,
        "trial_denominator": profile.trial_denominator,
        "z_threshold": profile.z_threshold,
        "feature_manifest_sha256": profile.corrected_feature_manifest_sha256,
        "corrected_feature_manifest_sha256": profile.corrected_feature_manifest_sha256,
        "amendment_sha256": profile.amendment_sha256,
        "mode_index_manifest_sha256": profile.mode_index_manifest_sha256,
        "mode_index_audit_sha256": profile.mode_index_audit_sha256,
        "mode_index_modes_sha256": profile.mode_index_modes_sha256,
        "predecessor_audit_sha256": profile.predecessor_audit_sha256,
        "corrected_code_sha256": profile.corrected_code_sha256,
        "v1_manifest_sha256": profile.v1_manifest_sha256,
        "source_manifest_sha256": profile.v1_manifest_sha256,
        "v1_audit_sha256": profile.v1_audit_sha256,
        "source_audit_sha256": profile.v1_audit_sha256,
    }
    if profile.technical_retry:
        binding.update(
            {
                "retry_id": profile.retry_id,
                "retry_config_sha256": profile.retry_config_sha256,
                "retry_document_sha256": profile.retry_document_sha256,
                "parent_config_sha256": profile.parent_config_sha256,
                "parent_amendment_sha256": profile.parent_amendment_sha256,
                "parent_attested_manifest_sha256": profile.parent_attested_manifest_sha256,
                "parent_shock_manifest_sha256": profile.parent_shock_manifest_sha256,
                "technical_retry_sha256": profile.technical_retry_sha256,
                "parent_source_code_inventory_sha256": (
                    profile.parent_source_code_inventory_sha256
                ),
                "parent_build_identity_sha256": profile.parent_build_identity_sha256,
                "retry_source_code_inventory_sha256": profile.retry_source_code_inventory_sha256,
                "retry_build_identity_sha256": profile.retry_build_identity_sha256,
            }
        )
    return binding


def _verify_grid_registry(output: Path, profile: GridRuntimeProfile) -> None:
    registry = _load_json(output / "registry.json", "grid registry")
    if (
        registry.get("kind") != profile.registry_kind
        or registry.get("version") != profile.version
        or registry.get("experiment_id") != profile.experiment_id
        or registry.get("candidate_count") != profile.candidate_count
        or registry.get("prior_attempted_candidates")
        != profile.trial_denominator - profile.candidate_count
        or registry.get("trial_denominator") != profile.trial_denominator
        or registry.get("z_threshold") != profile.z_threshold
    ):
        raise CorrectedGridV2Error("grid registry differs from the corrected profile")
    candidates = registry.get("candidates")
    if not isinstance(candidates, list):
        raise CorrectedGridV2Error("grid registry candidate inventory is invalid")
    hashes = tuple(item.get("sha256") if isinstance(item, dict) else None for item in candidates)
    if hashes != profile.family.candidate_hashes:
        raise CorrectedGridV2Error("grid registry candidate order differs from the contract")


def _verify_grid_report_multiplicity(
    report: Mapping[str, object], profile: GridRuntimeProfile
) -> None:
    """Prove the report was screened with the profile's frozen denominator/z."""

    if (
        report.get("candidate_count") != profile.candidate_count
        or report.get("trial_denominator") != profile.trial_denominator
    ):
        raise CorrectedGridV2Error(
            "grid report candidate count or trial denominator differs from the profile"
        )
    promotion = report.get("promotion_audit")
    survivors = report.get("statistical_screen_survivors")
    if not isinstance(promotion, list) or not isinstance(survivors, list):
        raise CorrectedGridV2Error("grid report multiplicity audit is missing")
    for label, rows in (("promotion", promotion), ("survivor", survivors)):
        for index, row in enumerate(rows, 1):
            if not isinstance(row, Mapping):
                raise CorrectedGridV2Error(
                    f"grid report {label} multiplicity row {index} is invalid"
                )
            if row.get("bonferroni_z_threshold") != profile.z_threshold:
                raise CorrectedGridV2Error(
                    f"grid report {label} z threshold differs from the profile"
                )


def _augment_output(output: Path, profile: GridRuntimeProfile) -> dict[str, object]:
    manifest_path = output / "manifest.json"
    report_path = output / "report.json"
    _regular_file(manifest_path, "grid manifest")
    _regular_file(report_path, "grid report")
    manifest = _load_json(manifest_path, "grid manifest")
    report = _load_json(report_path, "grid report")
    _verify_inventory(output, manifest.get("files"), "grid files")
    _verify_grid_registry(output, profile)
    _verify_grid_report_multiplicity(report, profile)
    binding = _binding_payload(profile)
    if (
        manifest.get("kind") != profile.manifest_kind
        or manifest.get("version") != profile.version
        or manifest.get("status") != "complete"
        or manifest.get("experiment_id") != profile.experiment_id
        or manifest.get("development_start_slot_inclusive") != DEVELOPMENT_START_SLOT
        or manifest.get("development_end_slot_exclusive") != DEVELOPMENT_END_SLOT
        or manifest.get("holdout_accessed") is not False
        or ("holdout_unlocked" in manifest and manifest.get("holdout_unlocked") is not False)
    ):
        raise CorrectedGridV2Error("grid manifest is not a sealed corrected-v2 artifact")
    if (
        report.get("kind") != profile.report_kind
        or report.get("version") != profile.version
        or report.get("experiment_id") != profile.experiment_id
    ):
        raise CorrectedGridV2Error("grid report identity differs from the corrected profile")
    if report.get("feature_manifest_sha256") != profile.corrected_feature_manifest_sha256:
        raise CorrectedGridV2Error("grid report feature hash differs from corrected input")
    if manifest.get("feature_manifest_sha256") != profile.corrected_feature_manifest_sha256:
        raise CorrectedGridV2Error("grid manifest feature hash differs from corrected input")
    if profile.technical_retry:
        for value, label in ((manifest, "grid manifest"), (report, "grid report")):
            for field, expected in (
                ("retry_id", profile.retry_id),
                ("retry_config_sha256", profile.retry_config_sha256),
                ("retry_document_sha256", profile.retry_document_sha256),
                ("parent_config_sha256", profile.parent_config_sha256),
                ("parent_amendment_sha256", profile.parent_amendment_sha256),
                (
                    "parent_attested_manifest_sha256",
                    profile.parent_attested_manifest_sha256,
                ),
                ("parent_shock_manifest_sha256", profile.parent_shock_manifest_sha256),
                ("technical_retry_sha256", profile.technical_retry_sha256),
                (
                    "parent_source_code_inventory_sha256",
                    profile.parent_source_code_inventory_sha256,
                ),
                ("parent_build_identity_sha256", profile.parent_build_identity_sha256),
            ):
                # The frozen v1 family runners do not know about the outer
                # technical-retry envelope.  Accept an absent field here and
                # bind it below; still reject any conflicting value emitted by
                # a runner or stale bridge.
                actual = value.get(field)
                if actual is not None and actual != expected:
                    raise CorrectedGridV2Error(f"{label} {field} differs from Retry-1")
    holdout = report.get("holdout")
    data_audit = report.get("data_audit")
    if (
        not isinstance(holdout, dict)
        or holdout.get("accessed") is not False
        or holdout.get("unlocked") is not False
        or holdout.get("start_slot_inclusive") != HOLDOUT_START_SLOT
        or not isinstance(data_audit, dict)
        or data_audit.get("out_of_bounds_rows") != 0
        or _integer(data_audit.get("min_slot"), "grid report minimum slot") < DEVELOPMENT_START_SLOT
        or _integer(data_audit.get("max_slot"), "grid report maximum slot") >= DEVELOPMENT_END_SLOT
    ):
        raise CorrectedGridV2Error("grid report violates development/holdout bounds")
    _reject_downstream_flags(manifest, "grid manifest")
    _reject_downstream_flags(report, "grid report")
    report["correction_v2"] = binding
    report["holdout_accessed"] = False
    report["holdout_unlocked"] = False
    report["feature_manifest_sha256"] = profile.corrected_feature_manifest_sha256
    report["candidate_count"] = profile.candidate_count
    report["trial_denominator"] = profile.trial_denominator
    report["z_threshold"] = profile.z_threshold
    report["correction_contract_sha256"] = profile.contract_sha256
    report["amendment_sha256"] = profile.amendment_sha256
    report["mode_index_manifest_sha256"] = profile.mode_index_manifest_sha256
    report["mode_index_audit_sha256"] = profile.mode_index_audit_sha256
    report["mode_index_modes_sha256"] = profile.mode_index_modes_sha256
    report["predecessor_audit_sha256"] = profile.predecessor_audit_sha256
    report["corrected_code_sha256"] = profile.corrected_code_sha256
    report["v1_manifest_sha256"] = profile.v1_manifest_sha256
    report["v1_audit_sha256"] = profile.v1_audit_sha256
    if profile.technical_retry:
        report["retry_id"] = profile.retry_id
        report["retry_config_sha256"] = profile.retry_config_sha256
        report["retry_document_sha256"] = profile.retry_document_sha256
        report["parent_config_sha256"] = profile.parent_config_sha256
        report["parent_amendment_sha256"] = profile.parent_amendment_sha256
        report["parent_attested_manifest_sha256"] = profile.parent_attested_manifest_sha256
        report["parent_shock_manifest_sha256"] = profile.parent_shock_manifest_sha256
        report["technical_retry_sha256"] = profile.technical_retry_sha256
        report["parent_source_code_inventory_sha256"] = profile.parent_source_code_inventory_sha256
        report["parent_build_identity_sha256"] = profile.parent_build_identity_sha256
        report["retry_source_code_inventory_sha256"] = profile.retry_source_code_inventory_sha256
        report["retry_build_identity_sha256"] = profile.retry_build_identity_sha256
    report["experiment_id"] = profile.experiment_id
    report["kind"] = profile.report_kind
    report["version"] = profile.version
    report_body = canonical_json(report)
    report_path.write_bytes(report_body)
    files = _file_inventory(manifest.get("files"), "grid manifest files")
    files["report.json"] = {
        "byte_count": report_path.stat().st_size,
        "sha256": sha256_file(report_path),
    }
    manifest["correction_v2"] = binding
    manifest["holdout_accessed"] = False
    manifest["holdout_unlocked"] = False
    manifest["feature_manifest_sha256"] = profile.corrected_feature_manifest_sha256
    manifest["candidate_count"] = profile.candidate_count
    manifest["trial_denominator"] = profile.trial_denominator
    manifest["z_threshold"] = profile.z_threshold
    manifest["correction_contract_sha256"] = profile.contract_sha256
    manifest["amendment_sha256"] = profile.amendment_sha256
    manifest["mode_index_manifest_sha256"] = profile.mode_index_manifest_sha256
    manifest["mode_index_audit_sha256"] = profile.mode_index_audit_sha256
    manifest["mode_index_modes_sha256"] = profile.mode_index_modes_sha256
    manifest["predecessor_audit_sha256"] = profile.predecessor_audit_sha256
    manifest["corrected_code_sha256"] = profile.corrected_code_sha256
    manifest["v1_manifest_sha256"] = profile.v1_manifest_sha256
    manifest["v1_audit_sha256"] = profile.v1_audit_sha256
    if profile.technical_retry:
        manifest["retry_id"] = profile.retry_id
        manifest["retry_config_sha256"] = profile.retry_config_sha256
        manifest["retry_document_sha256"] = profile.retry_document_sha256
        manifest["parent_config_sha256"] = profile.parent_config_sha256
        manifest["parent_amendment_sha256"] = profile.parent_amendment_sha256
        manifest["parent_attested_manifest_sha256"] = profile.parent_attested_manifest_sha256
        manifest["parent_shock_manifest_sha256"] = profile.parent_shock_manifest_sha256
        manifest["technical_retry_sha256"] = profile.technical_retry_sha256
        manifest["parent_source_code_inventory_sha256"] = (
            profile.parent_source_code_inventory_sha256
        )
        manifest["parent_build_identity_sha256"] = profile.parent_build_identity_sha256
        manifest["retry_source_code_inventory_sha256"] = profile.retry_source_code_inventory_sha256
        manifest["retry_build_identity_sha256"] = profile.retry_build_identity_sha256
    manifest["experiment_id"] = profile.experiment_id
    manifest["kind"] = profile.manifest_kind
    manifest["version"] = profile.version
    manifest["files"] = files
    manifest_body = canonical_json(manifest)
    manifest_path.write_bytes(manifest_body)
    return manifest


def run_corrected_family_grid(
    family: str,
    feature_input: Path,
    output: Path,
    *,
    causal_fx: Path,
    temp_directory: Path,
    memory_limit: str = "24GB",
    # Zero delegates to the selected DuckDB runner, which resolves to all
    # host logical CPUs.  Keep an explicit positive value available for
    # controlled replay/benchmark runs.
    threads: int = 0,
    config_path: str | Path = CONFIG_PATH,
    technical_retry_config: str | Path | None = None,
) -> dict[str, object]:
    """Run exactly one corrected family through its existing v1 implementation."""

    causal_fx = Path(causal_fx)
    temp_directory = Path(temp_directory)
    feature_root = Path(feature_input)
    output = Path(output)
    _assert_development_path(feature_root, "corrected feature input", require_directory=True)
    _assert_development_path(causal_fx, "causal FX input", require_directory=True)
    for name in ("manifest.json", "candles.csv"):
        if (causal_fx / name).is_symlink() or not (causal_fx / name).is_file():
            raise CorrectedGridV2Error(f"causal FX input {name} must be a regular file")
    _assert_development_path(
        temp_directory,
        "corrected grid temporary directory",
        require_directory=False,
    )
    _assert_development_path(output, "corrected output", require_directory=False)
    _assert_pairwise_disjoint_roots(
        (
            ("corrected feature input", feature_root),
            ("causal FX input", causal_fx),
            ("corrected grid temporary directory", temp_directory),
            ("corrected output", output),
        )
    )
    if temp_directory.exists() and (not temp_directory.is_dir() or any(temp_directory.iterdir())):
        raise CorrectedGridV2Error("corrected grid temporary directory must be absent or empty")
    contract = load_correction_v2_contract(config_path)
    retry_value = (
        load_correction_v2_retry_contract(technical_retry_config)
        if technical_retry_config is not None
        else None
    )
    # Verify before opening DuckDB or evaluating a single feature row.  Capture
    # metadata on both sides of the full hash so the selected legacy runner can
    # reuse this result without a second multi-gigabyte read.
    snapshot_before = _sealed_root_snapshot(feature_root)
    feature_manifest, feature_path, feature_hash = verify_corrected_feature_dataset(
        feature_root,
        contract=contract,
        config_path=config_path,
        retry_contract=retry_value,
    )
    snapshot_after = _sealed_root_snapshot(feature_root)
    if snapshot_before != snapshot_after:
        raise CorrectedFeatureVerificationError(
            "corrected feature root changed during full verification"
        )
    profile = build_runtime_profile(
        family,
        contract=contract,
        feature_manifest=feature_manifest,
        feature_manifest_sha256=feature_hash,
        config_path=config_path,
        technical_retry_config=technical_retry_config,
    )
    profile = replace(
        profile,
        feature_verifier=_cached_feature_verifier(
            feature_root,
            feature_manifest,
            feature_path,
            feature_hash,
            snapshot_after,
        ),
    )
    if any(_holdout_component(part) for part in output.parts):
        raise CorrectedGridV2Error("corrected output may not reference a holdout path")
    output.parent.mkdir(parents=True, exist_ok=True)
    resolved_parent = output.parent.resolve(strict=True)
    if resolved_parent != output.parent.absolute() or any(
        _holdout_component(part) for part in resolved_parent.parts
    ):
        raise CorrectedGridV2Error("corrected output path contains a symlink ancestor")
    if os.path.lexists(output):
        raise FileExistsError(f"refusing to overwrite corrected output root {output}")
    runner_module = importlib.import_module(_PRIVATE_RUNNERS[profile.family_id])
    selected_runner: Callable[..., object] = runner_module.run_production_grid
    staging = output.with_name(f".{output.name}.corrected-v2-{uuid.uuid4().hex}.partial")
    if os.path.lexists(staging):
        raise CorrectedGridV2Error("generated corrected output staging path already exists")
    try:
        selected_runner(
            Path(feature_input),
            staging,
            causal_fx=causal_fx,
            temp_directory=temp_directory,
            memory_limit=memory_limit,
            threads=threads,
            runtime_profile=profile,
        )
        if _sealed_root_snapshot(feature_root) != snapshot_after:
            raise CorrectedFeatureVerificationError(
                "corrected feature root changed during grid execution"
            )
        result = _augment_output(staging, profile)
        for name in ("report.json", "manifest.json"):
            descriptor = os.open(staging / name, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        directory = os.open(staging, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        _publish_directory_noreplace(staging, output)
        parent = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
        return result
    except BaseException:
        if staging.is_dir() and not staging.is_symlink():
            shutil.rmtree(staging, ignore_errors=True)
        raise


# Compatibility aliases keep the bridge discoverable without exposing the
# private target runner modules as part of the public API.
profile_for_family = build_runtime_profile
verify_feature_dataset = verify_corrected_feature_dataset
run_family_grid = run_corrected_family_grid


__all__ = [
    "CORRECTED_FEATURE_KIND",
    "CORRECTED_FEATURE_VERSION",
    "CorrectedFeatureVerificationError",
    "CorrectedGridV2Error",
    "FEATURE_KIND",
    "FEATURE_MANIFEST_KIND",
    "FEATURE_MANIFEST_VERSION",
    "FEATURE_VERSION",
    "GridRuntimeProfile",
    "RuntimeProfile",
    "build_runtime_profile",
    "profile_for_family",
    "run_corrected_family_grid",
    "run_family_grid",
    "verify_feature_dataset",
    "verify_corrected_feature_dataset",
]
