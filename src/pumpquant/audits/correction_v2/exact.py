"""Corrected-v2 handoff boundary for exact and capacity replays.

The corrected feature/grid bridge is deliberately kept separate from the
integer execution kernel.  It verifies the sealed correction-v2 identity and
returns only the development payloads needed by the exact survivor runner.
No holdout reader (or result/outcome reader) belongs in this module.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Final, cast

from pumpquant.audits.correction_v2 import grid as corrected_grid_v2
from pumpquant.audits.correction_v2.contract import (
    CONFIG_PATH,
    DEVELOPMENT_END_SLOT,
    DEVELOPMENT_START_SLOT,
    HOLDOUT_START_SLOT,
    CorrectionV2Contract,
    load_correction_v2_contract,
)
from pumpquant.audits.correction_v2.grid import GridRuntimeProfile
from pumpquant.audits.correction_v2.retry_contract import (
    CorrectionV2RetryContract,
    load_correction_v2_retry_contract,
)
from pumpquant.core.hashing import sha256_file

CORRECTED_VERSION: Final[int] = 2
CORRECTED_FEATURE_KIND: Final[str] = corrected_grid_v2.CORRECTED_FEATURE_KIND
CORRECTED_GRID_VERSION: Final[int] = 2
FAMILY_IDS: Final[tuple[str, ...]] = (
    "shock-recovery-fresh-v2",
    "progress-speed-fresh-v2",
    "persistent-pressure-fresh-v2",
    "amm-impact-ofi-fresh-v2",
)
FAMILY_ALIASES: Final[dict[str, str]] = {
    "shock_recovery": FAMILY_IDS[0],
    "shock-recovery": FAMILY_IDS[0],
    FAMILY_IDS[0]: FAMILY_IDS[0],
    "progress_speed": FAMILY_IDS[1],
    "progress-speed": FAMILY_IDS[1],
    FAMILY_IDS[1]: FAMILY_IDS[1],
    "persistent_pressure": FAMILY_IDS[2],
    "persistent-pressure": FAMILY_IDS[2],
    FAMILY_IDS[2]: FAMILY_IDS[2],
    "amm_impact_ofi": FAMILY_IDS[3],
    "amm-impact-ofi": FAMILY_IDS[3],
    FAMILY_IDS[3]: FAMILY_IDS[3],
}


class CorrectedExactV2Error(RuntimeError):
    """A corrected-v2 feature/grid/exact handoff is not sealed."""


@dataclass(frozen=True, slots=True)
class CorrectedFeatureInput:
    manifest: dict[str, object]
    feature_path: Path
    manifest_sha256: str


@dataclass(frozen=True, slots=True)
class CorrectedGridInput:
    family: str
    root: Path
    profile: GridRuntimeProfile
    manifest: dict[str, object]
    report: dict[str, object]
    manifest_sha256: str


def canonical_family(value: object) -> str:
    if not isinstance(value, str):
        raise CorrectedExactV2Error("family is invalid")
    try:
        return FAMILY_ALIASES[value.strip().lower()]
    except KeyError as error:
        raise CorrectedExactV2Error("family is not a corrected-v2 family") from error


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    try:
        return sha256_file(path)
    except OSError as error:
        raise CorrectedExactV2Error(f"cannot hash {path}") from error


def _canonical_json(value: object) -> bytes:
    try:
        return (
            json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise CorrectedExactV2Error("artifact is not canonical JSON") from error


def _load_object(path: Path, label: str) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise CorrectedExactV2Error(f"{label} is missing or unsafe")
    body = path.read_bytes()
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CorrectedExactV2Error(f"{label} is invalid JSON") from error
    if not isinstance(value, dict):
        raise CorrectedExactV2Error(f"{label} is not an object")
    result = cast(dict[str, object], value)
    if body != _canonical_json(result):
        raise CorrectedExactV2Error(f"{label} is not canonical JSON")
    return result


def _sha(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise CorrectedExactV2Error(f"{label} is not a lowercase SHA-256 digest")
    return value


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CorrectedExactV2Error(f"{label} is invalid")
    return value


def _holdout_component(value: str) -> bool:
    text = value.casefold()
    return (
        text == "holdout"
        or text.startswith(("holdout-", "holdout_", ".holdout"))
        or text.endswith(("-holdout", "_holdout"))
    )


def _regular_root(root: Path, label: str) -> None:
    if root.is_symlink() or not root.is_dir():
        raise CorrectedExactV2Error(f"{label} is missing or unsafe")
    # A corrected artifact may not traverse a holdout-labelled path.  This is
    # a path guard only; no holdout payload is opened by this module.
    try:
        resolved = root.resolve(strict=True)
    except OSError as error:
        raise CorrectedExactV2Error(f"{label} cannot be resolved safely") from error
    if resolved != root.absolute():
        raise CorrectedExactV2Error(f"{label} traverses a symlink ancestor")
    if any(_holdout_component(part) for part in resolved.parts):
        raise CorrectedExactV2Error(f"{label} resolves to a holdout path")


def assert_development_path(path: Path, label: str) -> None:
    """Reject textual, resolved, and symlink-routed holdout paths."""

    candidate = Path(path)
    if ".." in candidate.parts or any(_holdout_component(part) for part in candidate.parts):
        raise CorrectedExactV2Error(f"{label} may not reference a holdout path")
    absolute = candidate if candidate.is_absolute() else Path.cwd() / candidate
    cursor = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        cursor /= part
        try:
            if cursor.is_symlink():
                raise CorrectedExactV2Error(f"{label} may not traverse a symlink")
            if not os.path.lexists(cursor):
                break
        except OSError as error:
            raise CorrectedExactV2Error(f"{label} cannot be inspected safely") from error
    try:
        resolved = absolute.resolve(strict=False)
    except OSError as error:
        raise CorrectedExactV2Error(f"{label} cannot be resolved safely") from error
    if any(_holdout_component(part) for part in resolved.parts):
        raise CorrectedExactV2Error(f"{label} resolves to a holdout path")


def _safe_relative(value: object, label: str) -> Path:
    text = value if isinstance(value, str) else ""
    pure = PurePosixPath(text)
    if (
        not text
        or pure.as_posix() != text
        or pure.is_absolute()
        or ".." in pure.parts
        or any(part in {"", "."} for part in pure.parts)
        or any(_holdout_component(part) for part in pure.parts)
    ):
        raise CorrectedExactV2Error(f"{label} is unsafe")
    return Path(*pure.parts)


def _binding(profile: GridRuntimeProfile) -> dict[str, object]:
    """Return the public correction identity expected on grid artifacts."""

    binding: dict[str, object] = {
        "contract_kind": "pumpquant.fresh_bonding_prestate_correction_v2_contract",
        "contract_version": 2,
        "contract_sha256": profile.contract_sha256,
        "correction_contract_sha256": profile.contract_sha256,
        "config_sha256": profile.contract_sha256,
        "correction_id": "fresh-bonding-prestate-correction-v2",
        "family_id": profile.family_id,
        "family_kind": profile.family.kind,
        "family_version": profile.family.version,
        "experiment_id": profile.experiment_id,
        "report_kind": profile.report_kind,
        "manifest_kind": profile.manifest_kind,
        "candidate_count": profile.candidate_count,
        "candidate_order_sha256": profile.candidate_order_sha256,
        "registry_sha256": profile.registry_sha256,
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


def _require_binding(value: object, profile: GridRuntimeProfile, label: str) -> None:
    if not isinstance(value, Mapping):
        raise CorrectedExactV2Error(f"{label} correction_v2 binding is missing")
    expected = _binding(profile)
    actual = dict(cast(Mapping[str, object], value))
    for key, expected_value in expected.items():
        if actual.get(key) != expected_value:
            raise CorrectedExactV2Error(f"{label} correction_v2 binding differs at {key}")


def validate_correction_binding_set(
    value: object,
    *,
    feature_manifest_sha256: str | None = None,
    technical_retry_config: str | Path | None = None,
    retry_contract: CorrectionV2RetryContract | None = None,
) -> dict[str, object]:
    """Validate the static contract and shared provenance of four bindings."""

    if not isinstance(value, Mapping):
        raise CorrectedExactV2Error("correction_v2 binding is missing")
    correction = dict(cast(Mapping[str, object], value))
    contract = load_correction_v2_contract()
    retry_value = retry_contract
    if technical_retry_config is not None:
        if retry_value is not None:
            raise CorrectedExactV2Error("Retry-1 config was supplied twice")
        try:
            retry_value = load_correction_v2_retry_contract(technical_retry_config)
        except Exception as error:
            raise CorrectedExactV2Error(f"cannot load Retry-1 config: {error}") from error
    # Retry-1 layers a technical rerun identity over the unchanged correction
    # contract.  Core correction fields therefore remain the original config
    # and amendment; retry config/document identities live in explicit fields.
    config_sha = _sha256_file(Path(CONFIG_PATH))
    amendment_sha = contract.amendment_sha256
    expected_top: dict[str, object] = {
        "contract_kind": "pumpquant.fresh_bonding_prestate_correction_v2_contract",
        "contract_version": 2,
        "correction_id": "fresh-bonding-prestate-correction-v2",
        "contract_sha256": config_sha,
        "correction_contract_sha256": config_sha,
        "config_sha256": config_sha,
        "correction_config_sha256": config_sha,
        "amendment_sha256": amendment_sha,
    }
    if retry_value is not None:
        expected_top.update(
            {
                "retry_id": retry_value.retry_id,
                "retry_config_sha256": retry_value.config_sha256,
                "retry_document_sha256": retry_value.retry_document.sha256,
                "parent_config_sha256": retry_value.parent_config_sha256,
                "parent_amendment_sha256": retry_value.parent_amendment_sha256,
                "parent_attested_manifest_sha256": (retry_value.parent.attested_manifest_sha256),
                "parent_shock_manifest_sha256": retry_value.parent.shock_manifest_sha256,
            }
        )
    if feature_manifest_sha256 is not None:
        expected_top["feature_manifest_sha256"] = feature_manifest_sha256
        expected_top["corrected_feature_manifest_sha256"] = feature_manifest_sha256
    for field, expected in expected_top.items():
        if correction.get(field) != expected:
            raise CorrectedExactV2Error(f"correction_v2 binding differs at {field}")
    families = correction.get("families")
    if not isinstance(families, Mapping) or set(families) != set(FAMILY_IDS):
        raise CorrectedExactV2Error("correction_v2 family bindings are incomplete")
    shared_fields: tuple[str, ...] = (
        "feature_manifest_sha256",
        "corrected_feature_manifest_sha256",
        "amendment_sha256",
        "mode_index_manifest_sha256",
        "mode_index_audit_sha256",
        "mode_index_modes_sha256",
        "predecessor_audit_sha256",
        "corrected_code_sha256",
        "v1_manifest_sha256",
        "source_manifest_sha256",
        "v1_audit_sha256",
        "source_audit_sha256",
    )
    digest_fields = shared_fields
    if retry_value is not None:
        shared_fields = shared_fields + (
            "retry_id",
            "retry_config_sha256",
            "retry_document_sha256",
            "parent_config_sha256",
            "parent_amendment_sha256",
            "parent_attested_manifest_sha256",
            "parent_shock_manifest_sha256",
            "technical_retry_sha256",
            "parent_source_code_inventory_sha256",
            "parent_build_identity_sha256",
            "retry_source_code_inventory_sha256",
            "retry_build_identity_sha256",
        )
        digest_fields = digest_fields + (
            "retry_config_sha256",
            "retry_document_sha256",
            "parent_config_sha256",
            "parent_amendment_sha256",
            "parent_attested_manifest_sha256",
            "parent_shock_manifest_sha256",
            "technical_retry_sha256",
            "parent_source_code_inventory_sha256",
            "parent_build_identity_sha256",
            "retry_source_code_inventory_sha256",
            "retry_build_identity_sha256",
        )
    for field in digest_fields:
        expected = correction.get(field)
        _sha(expected, f"correction_v2 {field}")
        if field == "amendment_sha256" and expected != contract.amendment_sha256:
            raise CorrectedExactV2Error("correction_v2 amendment binding differs")
        if field in {"source_manifest_sha256", "v1_manifest_sha256"} and expected != correction.get(
            "v1_manifest_sha256"
        ):
            raise CorrectedExactV2Error("correction_v2 v1 manifest aliases differ")
        if field in {"source_audit_sha256", "v1_audit_sha256"} and expected != correction.get(
            "v1_audit_sha256"
        ):
            raise CorrectedExactV2Error("correction_v2 v1 audit aliases differ")
    for family in contract.families:
        raw = families.get(family.id)
        if not isinstance(raw, Mapping):
            raise CorrectedExactV2Error(f"correction_v2 {family.id} binding is invalid")
        binding = cast(Mapping[str, object], raw)
        stem = family.id.removesuffix("-fresh-v2").replace("-", "_")
        retry_family = retry_value.family_map[family.id] if retry_value is not None else None
        expected_family: dict[str, object] = {
            "contract_kind": expected_top["contract_kind"],
            "contract_version": 2,
            "contract_sha256": config_sha,
            "correction_contract_sha256": config_sha,
            "config_sha256": config_sha,
            "correction_id": expected_top["correction_id"],
            "family_id": family.id,
            "family_kind": family.kind,
            "family_version": family.version,
            "experiment_id": family.id,
            "report_kind": f"pumpquant.{stem}_correction_v2_report",
            "manifest_kind": f"pumpquant.{stem}_correction_v2_manifest",
            "candidate_count": family.candidate_count,
            "candidate_order_sha256": family.candidate_order_sha256,
            "registry_sha256": family.registry_sha256,
            "bootstrap_seed": family.v1_experiment_id,
            "trial_denominator": (
                retry_family.cumulative_denominator
                if retry_family is not None
                else family.cumulative_denominator
            ),
            "z_threshold": (
                retry_family.z_threshold if retry_family is not None else family.z_threshold
            ),
        }
        if retry_value is not None:
            expected_family.update(
                {
                    "retry_id": retry_value.retry_id,
                    "retry_config_sha256": retry_value.config_sha256,
                    "retry_document_sha256": retry_value.retry_document.sha256,
                    "parent_config_sha256": retry_value.parent_config_sha256,
                    "parent_amendment_sha256": retry_value.parent_amendment_sha256,
                    "parent_attested_manifest_sha256": (
                        retry_value.parent.attested_manifest_sha256
                    ),
                    "parent_shock_manifest_sha256": (retry_value.parent.shock_manifest_sha256),
                }
            )
        for field, expected in expected_family.items():
            if binding.get(field) != expected:
                raise CorrectedExactV2Error(f"correction_v2 {family.id} binding differs at {field}")
        for field in shared_fields:
            if binding.get(field) != correction.get(field):
                raise CorrectedExactV2Error(
                    f"correction_v2 {family.id} shared binding differs at {field}"
                )
    return correction


def verify_corrected_feature(
    root: Path,
    *,
    contract: CorrectionV2Contract | None = None,
    config_path: str | Path | None = None,
    technical_retry_config: str | Path | None = None,
) -> CorrectedFeatureInput:
    """Verify a corrected feature root through the canonical v2 verifier."""

    _regular_root(Path(root), "corrected feature root")
    try:
        if contract is None and config_path is None and technical_retry_config is None:
            manifest, feature_path, digest = corrected_grid_v2.verify_corrected_feature_dataset(
                Path(root)
            )
        elif technical_retry_config is not None:
            manifest, feature_path, digest = corrected_grid_v2.verify_corrected_feature_dataset(
                Path(root),
                contract=contract,
                config_path=config_path or CONFIG_PATH,
                technical_retry_config=technical_retry_config,
            )
        elif contract is None:
            assert config_path is not None
            manifest, feature_path, digest = corrected_grid_v2.verify_corrected_feature_dataset(
                Path(root), config_path=config_path
            )
        elif config_path is None:
            manifest, feature_path, digest = corrected_grid_v2.verify_corrected_feature_dataset(
                Path(root), contract=contract
            )
        else:
            manifest, feature_path, digest = corrected_grid_v2.verify_corrected_feature_dataset(
                Path(root), contract=contract, config_path=config_path
            )
    except Exception as error:
        if isinstance(error, CorrectedExactV2Error):
            raise
        raise CorrectedExactV2Error(f"corrected feature verification failed: {error}") from error
    if manifest.get("kind") != CORRECTED_FEATURE_KIND or manifest.get("version") != 2:
        raise CorrectedExactV2Error("feature input is not corrected feature v2")
    return CorrectedFeatureInput(manifest, Path(feature_path), digest)


def build_profile(
    family: str,
    feature: CorrectedFeatureInput,
    *,
    contract: CorrectionV2Contract | None = None,
    config_path: str | Path | None = None,
    technical_retry_config: str | Path | None = None,
) -> GridRuntimeProfile:
    canonical = canonical_family(family)
    try:
        if technical_retry_config is not None:
            profile = corrected_grid_v2.build_runtime_profile(
                canonical,
                contract=contract,
                feature_manifest=feature.manifest,
                feature_manifest_sha256=feature.manifest_sha256,
                config_path=config_path or CONFIG_PATH,
                technical_retry_config=technical_retry_config,
            )
        elif contract is None and config_path is None:
            profile = corrected_grid_v2.build_runtime_profile(
                canonical,
                feature_manifest=feature.manifest,
                feature_manifest_sha256=feature.manifest_sha256,
            )
        elif contract is None:
            assert config_path is not None
            profile = corrected_grid_v2.build_runtime_profile(
                canonical,
                feature_manifest=feature.manifest,
                feature_manifest_sha256=feature.manifest_sha256,
                config_path=config_path,
            )
        elif config_path is None:
            profile = corrected_grid_v2.build_runtime_profile(
                canonical,
                contract=contract,
                feature_manifest=feature.manifest,
                feature_manifest_sha256=feature.manifest_sha256,
            )
        else:
            profile = corrected_grid_v2.build_runtime_profile(
                canonical,
                contract=contract,
                feature_manifest=feature.manifest,
                feature_manifest_sha256=feature.manifest_sha256,
                config_path=config_path,
            )
    except Exception as error:
        raise CorrectedExactV2Error(
            f"cannot build corrected profile for {canonical}: {error}"
        ) from error
    if profile.family_id != canonical or profile.version != CORRECTED_GRID_VERSION:
        raise CorrectedExactV2Error("corrected profile identity differs from family")
    return profile


def verify_corrected_grid(
    family: str,
    root: Path,
    *,
    feature: CorrectedFeatureInput,
    contract: CorrectionV2Contract | None = None,
    config_path: str | Path | None = None,
    technical_retry_config: str | Path | None = None,
) -> CorrectedGridInput:
    """Verify one corrected-v2 family grid and all correction bindings."""

    canonical = canonical_family(family)
    root = Path(root)
    _regular_root(root, f"{canonical} grid root")
    profile = build_profile(
        canonical,
        feature,
        contract=contract,
        config_path=config_path,
        technical_retry_config=technical_retry_config,
    )
    manifest_path = root / "manifest.json"
    report_path = root / "report.json"
    try:
        manifest = corrected_grid_v2._load_json(manifest_path, "corrected grid manifest")
        report = corrected_grid_v2._load_json(report_path, "corrected grid report")
        corrected_grid_v2._verify_inventory(root, manifest.get("files"), "corrected grid files")
        corrected_grid_v2._verify_grid_registry(root, profile)
        corrected_grid_v2._verify_grid_report_multiplicity(report, profile)
    except Exception as error:
        raise CorrectedExactV2Error(f"{canonical} grid seal is invalid: {error}") from error

    expected = {
        "kind": profile.manifest_kind,
        "version": 2,
        "status": "complete",
        "experiment_id": canonical,
        "development_start_slot_inclusive": DEVELOPMENT_START_SLOT,
        "development_end_slot_exclusive": DEVELOPMENT_END_SLOT,
        "holdout_accessed": False,
        "holdout_unlocked": False,
        "feature_manifest_sha256": feature.manifest_sha256,
        "candidate_count": profile.candidate_count,
        "trial_denominator": profile.trial_denominator,
        "z_threshold": profile.z_threshold,
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise CorrectedExactV2Error(f"{canonical} grid manifest violates {key}")
    for key, value in {
        "kind": profile.report_kind,
        "version": 2,
        "experiment_id": canonical,
        "feature_manifest_sha256": feature.manifest_sha256,
        "holdout_accessed": False,
        "holdout_unlocked": False,
        "candidate_count": profile.candidate_count,
        "trial_denominator": profile.trial_denominator,
        "z_threshold": profile.z_threshold,
    }.items():
        if report.get(key) != value:
            raise CorrectedExactV2Error(f"{canonical} grid report violates {key}")
    if profile.technical_retry:
        for value, label in ((manifest, "grid manifest"), (report, "grid report")):
            for key, expected_value in (
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
                (
                    "retry_source_code_inventory_sha256",
                    profile.retry_source_code_inventory_sha256,
                ),
                ("retry_build_identity_sha256", profile.retry_build_identity_sha256),
            ):
                if value.get(key) != expected_value:
                    raise CorrectedExactV2Error(f"{label} violates Retry-1 {key}")
    holdout = report.get("holdout")
    if not isinstance(holdout, Mapping):
        raise CorrectedExactV2Error(f"{canonical} grid report holdout lock is missing")
    if holdout.get("accessed") is not False or holdout.get("unlocked") is not False:
        raise CorrectedExactV2Error(f"{canonical} grid report opens or unlocks holdout")
    if holdout.get("start_slot_inclusive") != HOLDOUT_START_SLOT:
        raise CorrectedExactV2Error(f"{canonical} grid report holdout boundary differs")
    if report.get("holdout_accessed") is not None and report.get("holdout_accessed") is not False:
        raise CorrectedExactV2Error(f"{canonical} grid report opens holdout")
    survivors = report.get("statistical_screen_survivors")
    if not isinstance(survivors, list):
        raise CorrectedExactV2Error(f"{canonical} survivor inventory is missing")
    allowed_candidates = set(profile.family.candidate_hashes)
    seen_candidates: set[str] = set()
    for index, raw_survivor in enumerate(survivors, 1):
        if not isinstance(raw_survivor, Mapping):
            raise CorrectedExactV2Error(f"{canonical} survivor {index} is invalid")
        candidate = raw_survivor.get("candidate_sha256")
        if not isinstance(candidate, str) or candidate not in allowed_candidates:
            raise CorrectedExactV2Error(f"{canonical} survivor {index} is outside its registry")
        if candidate in seen_candidates:
            raise CorrectedExactV2Error(f"{canonical} survivor inventory is duplicated")
        seen_candidates.add(candidate)
        if raw_survivor.get("statistical_screen_survivor") is not True:
            raise CorrectedExactV2Error(f"{canonical} survivor {index} is not promoted")
        _integer(raw_survivor.get("outcomes"), f"{canonical} survivor {index} outcomes", minimum=1)
    _require_binding(manifest.get("correction_v2"), profile, f"{canonical} manifest")
    _require_binding(report.get("correction_v2"), profile, f"{canonical} report")
    for value, label in (
        (manifest, f"{canonical} manifest"),
        (report, f"{canonical} report"),
    ):
        for field, expected_hash in (
            ("correction_contract_sha256", profile.contract_sha256),
            ("amendment_sha256", profile.amendment_sha256),
            ("mode_index_manifest_sha256", profile.mode_index_manifest_sha256),
            ("mode_index_audit_sha256", profile.mode_index_audit_sha256),
            ("mode_index_modes_sha256", profile.mode_index_modes_sha256),
            ("predecessor_audit_sha256", profile.predecessor_audit_sha256),
            ("corrected_code_sha256", profile.corrected_code_sha256),
            ("v1_manifest_sha256", profile.v1_manifest_sha256),
            ("v1_audit_sha256", profile.v1_audit_sha256),
        ):
            if value.get(field) != expected_hash:
                raise CorrectedExactV2Error(f"{label} {field} differs from profile")
    return CorrectedGridInput(
        family=canonical,
        root=root,
        profile=profile,
        manifest=manifest,
        report=report,
        manifest_sha256=_sha256_file(manifest_path),
    )


def verify_corrected_grid_set(
    feature_root: Path,
    grids: Mapping[str, Path],
    *,
    contract: CorrectionV2Contract | None = None,
    config_path: str | Path | None = None,
    technical_retry_config: str | Path | None = None,
) -> tuple[CorrectedFeatureInput, dict[str, CorrectedGridInput]]:
    """Verify the corrected feature plus exactly the four corrected grids."""

    feature = verify_corrected_feature(
        feature_root,
        contract=contract,
        config_path=config_path,
        technical_retry_config=technical_retry_config,
    )
    canonical_grids: dict[str, Path] = {}
    for raw_family, path in grids.items():
        family = canonical_family(raw_family)
        if family in canonical_grids:
            raise CorrectedExactV2Error(f"duplicate corrected family {family}")
        canonical_grids[family] = Path(path)
    if set(canonical_grids) != set(FAMILY_IDS):
        raise CorrectedExactV2Error("all four corrected-v2 family grids are required")
    result = {
        family: verify_corrected_grid(
            family,
            canonical_grids[family],
            feature=feature,
            contract=contract,
            config_path=config_path,
            technical_retry_config=technical_retry_config,
        )
        for family in FAMILY_IDS
    }
    return feature, result


def verify_exact_output(
    root: Path,
    *,
    technical_retry_config: str | Path | None = None,
) -> dict[str, object]:
    """Verify a corrected exact-v2 output without reopening feature payloads.

    This is intentionally a seal/identity check only.  Capacity supplies the
    independently verified feature and grid roots before it attempts replay.
    """

    root = Path(root)
    _regular_root(root, "corrected exact output root")
    manifest_path = root / "manifest.json"
    report_path = root / "report.json"
    try:
        manifest = _load_object(manifest_path, "corrected exact manifest")
        report = _load_object(report_path, "corrected exact report")
        verified_files = corrected_grid_v2._verify_inventory(
            root, manifest.get("files"), "corrected exact files"
        )
    except Exception as error:
        raise CorrectedExactV2Error(f"corrected exact seal is invalid: {error}") from error
    if (
        manifest.get("kind") != "pumpquant.exact_survivor_gate_v2_manifest"
        or manifest.get("version") != 2
        or manifest.get("status") != "complete"
        or manifest.get("development_start_slot_inclusive") != DEVELOPMENT_START_SLOT
        or manifest.get("development_end_slot_exclusive") != DEVELOPMENT_END_SLOT
        or manifest.get("holdout_accessed") is not False
        or manifest.get("holdout_unlocked") is not False
    ):
        raise CorrectedExactV2Error("corrected exact manifest is not development-only")
    if (
        report.get("kind") != "pumpquant.exact_survivor_gate_v2_report"
        or report.get("version") != 2
        or report.get("development_start_slot_inclusive") != DEVELOPMENT_START_SLOT
        or report.get("development_end_slot_exclusive") != DEVELOPMENT_END_SLOT
        or report.get("holdout_accessed") is not False
        or report.get("holdout_unlocked") is not False
    ):
        raise CorrectedExactV2Error("corrected exact report is not development-only")
    holdout = report.get("holdout")
    if (
        not isinstance(holdout, Mapping)
        or holdout.get("accessed") is not False
        or holdout.get("unlocked") is not False
    ):
        raise CorrectedExactV2Error("corrected exact report opens or unlocks holdout")
    if holdout.get("start_slot_inclusive") != HOLDOUT_START_SLOT:
        raise CorrectedExactV2Error("corrected exact report holdout boundary differs")
    retry_value: CorrectionV2RetryContract | None = None
    if technical_retry_config is not None:
        try:
            retry_value = load_correction_v2_retry_contract(technical_retry_config)
        except Exception as error:
            raise CorrectedExactV2Error(f"cannot load Retry-1 config: {error}") from error
    correction = report.get("correction_v2")
    if not isinstance(correction, Mapping):
        raise CorrectedExactV2Error("corrected exact correction_v2 binding is missing")
    if manifest.get("correction_v2") != correction:
        raise CorrectedExactV2Error("corrected exact correction binding differs")
    expected_feature = _sha(report.get("feature_manifest_sha256"), "exact feature hash")
    validate_correction_binding_set(
        correction,
        feature_manifest_sha256=expected_feature,
        retry_contract=retry_value,
    )
    if retry_value is not None:
        for value, label in ((manifest, "exact manifest"), (report, "exact report")):
            for key, expected_value in (
                ("retry_id", retry_value.retry_id),
                ("retry_config_sha256", retry_value.config_sha256),
                ("retry_document_sha256", retry_value.retry_document.sha256),
                ("parent_config_sha256", retry_value.parent_config_sha256),
                ("parent_amendment_sha256", retry_value.parent_amendment_sha256),
                (
                    "parent_attested_manifest_sha256",
                    retry_value.parent.attested_manifest_sha256,
                ),
                ("parent_shock_manifest_sha256", retry_value.parent.shock_manifest_sha256),
                ("technical_retry_sha256", correction.get("technical_retry_sha256")),
                (
                    "parent_source_code_inventory_sha256",
                    correction.get("parent_source_code_inventory_sha256"),
                ),
                ("parent_build_identity_sha256", correction.get("parent_build_identity_sha256")),
                (
                    "retry_source_code_inventory_sha256",
                    correction.get("retry_source_code_inventory_sha256"),
                ),
                ("retry_build_identity_sha256", correction.get("retry_build_identity_sha256")),
            ):
                if value.get(key) != expected_value:
                    raise CorrectedExactV2Error(f"{label} violates Retry-1 {key}")
    expected_fx = _sha(report.get("causal_fx_manifest_sha256"), "exact FX hash")
    if (
        manifest.get("feature_manifest_sha256") != expected_feature
        or manifest.get("causal_fx_manifest_sha256") != expected_fx
    ):
        raise CorrectedExactV2Error("corrected exact feature/FX hashes are not bound")
    selected = report.get("selected_holdout_candidate")
    selected_identity: dict[str, str] | None = None
    selected_result: dict[str, object] | None = None
    if selected is not None:
        if not isinstance(selected, Mapping):
            raise CorrectedExactV2Error("corrected exact selected candidate is invalid")
        family = canonical_family(selected.get("family"))
        candidate = selected.get("candidate_sha256")
        _sha(candidate, "selected candidate")
        if candidate not in load_correction_v2_contract().family_map[family].candidate_hashes:
            raise CorrectedExactV2Error(
                "corrected exact selected candidate is outside the frozen registry"
            )
        selected_identity = {"family": family, "candidate_sha256": cast(str, candidate)}
        if report.get("status") != "development_gate_passed_holdout_candidate_selected":
            raise CorrectedExactV2Error("corrected exact selection status is invalid")
        nested = holdout.get("selected_candidate")
        if not isinstance(nested, Mapping) or (
            nested.get("family") != family or nested.get("candidate_sha256") != candidate
        ):
            raise CorrectedExactV2Error("corrected exact selected candidate identity differs")
        survivors = report.get("development_survivors")
        if not isinstance(survivors, list):
            raise CorrectedExactV2Error("corrected exact survivor inventory is missing")
        matches = 0
        for item in survivors:
            if not isinstance(item, Mapping):
                raise CorrectedExactV2Error("corrected exact survivor row is invalid")
            if item.get("family") == family and item.get("candidate_sha256") == candidate:
                if (
                    item.get("development_survivor") is not True
                    or item.get("exact_integer_replay_passed") is not True
                    or item.get("shared_10usd_capital_replay_passed") is not True
                ):
                    raise CorrectedExactV2Error("selected corrected survivor did not pass")
                matches += 1
                selected_result = dict(cast(Mapping[str, object], item))
        if matches != 1:
            raise CorrectedExactV2Error("corrected exact selected candidate is not unique")
        assert selected_result is not None
        ledger_relative = _safe_relative(
            selected_result.get("shared_wallet_ledger_relative_path"),
            "corrected exact shared wallet ledger path",
        )
        expected_relative = Path("shared-wallet-ledgers") / family / f"{candidate}.json"
        if ledger_relative != expected_relative:
            raise CorrectedExactV2Error("corrected exact shared wallet ledger path differs")
        ledger_entry = verified_files.get(ledger_relative.as_posix())
        if ledger_entry is None:
            raise CorrectedExactV2Error("corrected exact shared wallet ledger is unsealed")
        ledger_bytes = _integer(
            selected_result.get("shared_wallet_ledger_byte_count"),
            "corrected exact shared wallet ledger byte_count",
            minimum=1,
        )
        ledger_sha = _sha(
            selected_result.get("shared_wallet_ledger_sha256"),
            "corrected exact shared wallet ledger sha256",
        )
        if (
            ledger_entry.get("byte_count") != ledger_bytes
            or ledger_entry.get("sha256") != ledger_sha
        ):
            raise CorrectedExactV2Error("corrected exact shared wallet ledger binding differs")
        ledger = _load_object(root / ledger_relative, "corrected exact shared wallet ledger")
        if (
            ledger.get("kind") != "pumpquant.exact_survivor_gate_v2_shared_wallet_ledger"
            or ledger.get("version") != 2
            or ledger.get("family") != family
            or ledger.get("candidate_sha256") != candidate
        ):
            raise CorrectedExactV2Error("corrected exact shared wallet ledger identity differs")
        summary = ledger.get("summary")
        accepted_trades = ledger.get("accepted_trades")
        if not isinstance(summary, Mapping) or not isinstance(accepted_trades, list):
            raise CorrectedExactV2Error("corrected exact shared wallet ledger is incomplete")
        for ledger_key, result_key in (
            ("starting_usd", "shared_start_usd"),
            ("starting_quote_atomic", "shared_start_quote_atomic"),
            ("final_usd", "shared_final_usd"),
            ("final_quote_atomic", "shared_final_quote_atomic"),
            ("minimum_equity_quote_atomic", "shared_minimum_equity_quote_atomic"),
            ("maximum_concurrent_positions", "shared_maximum_concurrent_positions"),
        ):
            if summary.get(ledger_key) != selected_result.get(result_key):
                raise CorrectedExactV2Error(
                    f"corrected exact shared wallet ledger differs at {ledger_key}"
                )
        if len(accepted_trades) != selected_result.get("shared_accepted_trade_count"):
            raise CorrectedExactV2Error(
                "corrected exact shared wallet ledger accepted count differs"
            )
    elif report.get("status") not in {
        "no_statistical_survivor",
        "no_exact_integer_survivor",
        "no_shared_capital_survivor",
    }:
        raise CorrectedExactV2Error("corrected exact report has an invalid empty status")
    elif holdout.get("selected_candidate") is not None:
        raise CorrectedExactV2Error("corrected exact empty result names a selected candidate")
    return {
        "root": root,
        "manifest": manifest,
        "report": report,
        "manifest_sha256": _sha256_file(manifest_path),
        "report_sha256": _sha256_file(report_path),
        "feature_manifest_sha256": expected_feature,
        "causal_fx_manifest_sha256": expected_fx,
        "selected_candidate": selected_identity,
        "selected_result": selected_result,
        "corrected_v2": True,
    }


__all__ = [
    "CORRECTED_FEATURE_KIND",
    "CORRECTED_GRID_VERSION",
    "CORRECTED_VERSION",
    "CorrectedExactV2Error",
    "CorrectedFeatureInput",
    "CorrectedGridInput",
    "FAMILY_ALIASES",
    "FAMILY_IDS",
    "build_profile",
    "assert_development_path",
    "canonical_family",
    "verify_corrected_feature",
    "verify_corrected_grid",
    "verify_corrected_grid_set",
    "validate_correction_binding_set",
]
