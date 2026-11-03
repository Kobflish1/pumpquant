"""Fail-closed contract for the technical Retry-1 correction pass.

Retry-1 is an optional context layered on top of the frozen correction-v2
contract.  It does not create a new candidate universe: the four family
registries, candidate order, bootstrap seeds, data identities, and every
execution policy remain those of the parent contract.  Only the pre-registered
multiplicity ledger and the independently attested evaluation-code identity
change.

This module intentionally contains no feature/report readers.  It validates
the small canonical JSON preregistration and exposes immutable identities for
the feature/grid/exact bridges to bind later.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from statistics import NormalDist
from types import MappingProxyType
from typing import Final, cast

from pumpquant.audits.correction_v2.contract import (
    AMENDMENT_PATH,
    CorrectionV2Contract,
    canonical_json,
    load_correction_v2_contract,
)
from pumpquant.audits.correction_v2.contract import (
    CONFIG_PATH as PARENT_CONFIG_PATH,
)

CONFIG_PATH: Final[Path] = Path("configs/fresh_bonding_prestate_correction_v2_retry1.json")
RETRY_CONFIG_PATH: Final[Path] = CONFIG_PATH
RETRY_DOCUMENT_PATH: Final[str] = "docs/FRESH_V1_BONDING_PRESTATE_CORRECTION_V2_RETRY1.md"
RETRY_DOC_PATH: Final[str] = RETRY_DOCUMENT_PATH
RETRY_ID: Final[str] = "fresh-bonding-prestate-correction-v2-retry1"
CONTRACT_KIND: Final[str] = "pumpquant.fresh_bonding_prestate_correction_v2_retry_preregistration"
CONTRACT_VERSION: Final[int] = 1
STATUS: Final[str] = "preregistered_pre_outcome"
FROZEN_CONFIG_SHA256: Final[str] = (
    "cd710f4605cfaaebbd3f7ef740521ba6a40a33f030c2180738b62ab77215108a"
)
BASE_DENOMINATOR: Final[int] = 1463
RETRY_DENOMINATORS: Final[tuple[int, ...]] = (1655, 1687, 1719, 1751)
RETRY_CANDIDATE_COUNTS: Final[tuple[int, ...]] = (192, 32, 32, 32)
FAMILY_IDS: Final[tuple[str, ...]] = (
    "shock-recovery-fresh-v2",
    "progress-speed-fresh-v2",
    "persistent-pressure-fresh-v2",
    "amm-impact-ofi-fresh-v2",
)
STABLE_DATA_KEYS: Final[frozenset[str]] = frozenset(
    {
        "draft_manifest_sha256",
        "draft_audit_sha256",
        "predecessor_sha256",
        "features_sha256",
        "row_identity_sha256",
        "decision_sha256",
        "lineage_sha256",
    }
)

_HEX = frozenset("0123456789abcdef")
_ROOT = Path(__file__).resolve().parents[4]


class CorrectionV2RetryContractError(ValueError):
    """The Retry-1 preregistration is absent, malformed, or has drifted."""


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    try:
        return _sha256_bytes(path.read_bytes())
    except OSError as error:
        raise CorrectionV2RetryContractError(f"cannot hash {path}") from error


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise CorrectionV2RetryContractError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise CorrectionV2RetryContractError(f"{label} must be an object")
    return cast(dict[str, object], value)


def _keys(value: Mapping[str, object], expected: frozenset[str], label: str) -> None:
    unknown = set(value) - expected
    missing = expected - set(value)
    if unknown:
        raise CorrectionV2RetryContractError(
            f"{label} has unknown key(s): {', '.join(sorted(unknown))}"
        )
    if missing:
        raise CorrectionV2RetryContractError(
            f"{label} is missing key(s): {', '.join(sorted(missing))}"
        )


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CorrectionV2RetryContractError(f"{label} must be a non-empty string")
    return value


def _bool(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise CorrectionV2RetryContractError(f"{label} must be a boolean")
    return value


def _int(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CorrectionV2RetryContractError(f"{label} must be an integer >= {minimum}")
    return value


def _sha(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _HEX for character in value)
    ):
        raise CorrectionV2RetryContractError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _relative(value: object, label: str) -> str:
    text = _text(value, label)
    path = Path(text)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise CorrectionV2RetryContractError(f"{label} must be a safe relative path")
    return path.as_posix()


@dataclass(frozen=True, slots=True)
class RetryDocument:
    path: str
    sha256: str


@dataclass(frozen=True, slots=True)
class RetryParentBinding:
    attested_manifest_sha256: str
    shock_manifest_sha256: str
    failed_progress_invocation: str
    failed_progress_error_stage: str
    old_runtime_bundle_sha256: str
    patched_progress_runner_sha256: str


@dataclass(frozen=True, slots=True)
class RetryFamily:
    id: str
    candidate_count: int
    candidate_order_sha256: str
    registry_sha256: str
    bootstrap_seed: str
    retry_attempt_count: int
    cumulative_denominator: int
    z_threshold: float


@dataclass(frozen=True, slots=True)
class CorrectionV2RetryContract:
    """Validated immutable Retry-1 context and all sealed parent identities."""

    kind: str
    version: int
    status: str
    retry_id: str
    retry_document: RetryDocument
    original: Mapping[str, str]
    parent: RetryParentBinding
    stable_data: Mapping[str, str]
    families: tuple[RetryFamily, ...]
    ledger: Mapping[str, object]
    development: Mapping[str, object]
    holdout: Mapping[str, object]
    evaluation_policy: Mapping[str, object]
    outcome_access: Mapping[str, object]
    requirements: Mapping[str, object]
    code_inventory: Mapping[str, object]
    candidate_total: int
    config_sha256: str
    parent_contract: CorrectionV2Contract

    @property
    def amendment_sha256(self) -> str:
        return self.retry_document.sha256

    @property
    def retry_document_sha256(self) -> str:
        return self.retry_document.sha256

    @property
    def parent_config_sha256(self) -> str:
        return self.original["config_sha256"]

    @property
    def parent_amendment_sha256(self) -> str:
        return self.original["amendment_sha256"]

    @property
    def source_code_inventory_sha256(self) -> str | None:
        # The exact new inventory is sealed in each fresh feature root.  The
        # preregistration requires a new inventory/build identity rather than
        # freezing the source bytes before the patched runner is published.
        value = self.code_inventory.get("source_code_inventory_sha256")
        return None if value is None else _sha(value, "code_inventory.source_code_inventory_sha256")

    @property
    def build_identity_sha256(self) -> str | None:
        value = self.code_inventory.get("build_identity_sha256")
        return None if value is None else _sha(value, "code_inventory.build_identity_sha256")

    @property
    def family_map(self) -> Mapping[str, RetryFamily]:
        return MappingProxyType({family.id: family for family in self.families})

    @property
    def denominators(self) -> tuple[int, ...]:
        return tuple(family.cumulative_denominator for family in self.families)

    @property
    def stable_data_map(self) -> Mapping[str, str]:
        return self.stable_data


def _load_document(value: object) -> RetryDocument:
    obj = _mapping(value, "retry_document")
    _keys(obj, frozenset({"path", "sha256"}), "retry_document")
    path = _relative(obj["path"], "retry_document.path")
    if path != RETRY_DOCUMENT_PATH:
        raise CorrectionV2RetryContractError(
            "retry_document.path differs from canonical Retry-1 document"
        )
    digest = _sha(obj["sha256"], "retry_document.sha256")
    document = _ROOT / path
    if not document.is_file() or document.is_symlink() or _sha256_file(document) != digest:
        raise CorrectionV2RetryContractError("retry document hash differs from the current file")
    return RetryDocument(path, digest)


def _load_parent(value: object, parent_contract: CorrectionV2Contract) -> RetryParentBinding:
    obj = _mapping(value, "parent")
    expected = frozenset(
        {
            "attested_manifest_sha256",
            "failed_progress_error_stage",
            "failed_progress_invocation",
            "old_runtime_bundle_sha256",
            "patched_progress_runner_sha256",
            "shock_manifest_sha256",
        }
    )
    _keys(obj, expected, "parent")
    return RetryParentBinding(
        _sha(obj["attested_manifest_sha256"], "parent.attested_manifest_sha256"),
        _sha(obj["shock_manifest_sha256"], "parent.shock_manifest_sha256"),
        _text(obj["failed_progress_invocation"], "parent.failed_progress_invocation"),
        _text(obj["failed_progress_error_stage"], "parent.failed_progress_error_stage"),
        _sha(obj["old_runtime_bundle_sha256"], "parent.old_runtime_bundle_sha256"),
        _sha(obj["patched_progress_runner_sha256"], "parent.patched_progress_runner_sha256"),
    )


def _load_stable_data(value: object) -> Mapping[str, str]:
    obj = _mapping(value, "stable_data")
    _keys(obj, STABLE_DATA_KEYS, "stable_data")
    return MappingProxyType({key: _sha(obj[key], f"stable_data.{key}") for key in STABLE_DATA_KEYS})


def _load_family(value: object, index: int, parent: CorrectionV2Contract) -> RetryFamily:
    obj = _mapping(value, f"families[{index}]")
    expected = frozenset(
        {
            "id",
            "candidate_count",
            "candidate_order_sha256",
            "registry_sha256",
            "registry_source",
            "registry_format",
            "bootstrap_seed",
            "v1_experiment_id",
            "retry_attempt_count",
            "cumulative_denominator",
            "z_threshold",
        }
    )
    _keys(obj, expected, f"families[{index}]")
    family_id = _text(obj["id"], f"families[{index}].id")
    if family_id != parent.families[index].id:
        raise CorrectionV2RetryContractError("Retry-1 family order differs from parent")
    source = parent.family_map[family_id]
    count = _int(obj["candidate_count"], f"families[{index}].candidate_count", minimum=1)
    order = _sha(obj["candidate_order_sha256"], f"families[{index}].candidate_order_sha256")
    registry = _sha(obj["registry_sha256"], f"families[{index}].registry_sha256")
    seed = _text(obj["bootstrap_seed"], f"families[{index}].bootstrap_seed")
    v1_id = _text(obj["v1_experiment_id"], f"families[{index}].v1_experiment_id")
    if (count, order, registry, seed) != (
        source.candidate_count,
        source.candidate_order_sha256,
        source.registry_sha256,
        source.bootstrap_seed,
    ):
        raise CorrectionV2RetryContractError(
            f"{family_id} candidate/registry/seed identity drifted"
        )
    if v1_id != source.v1_experiment_id or obj["registry_source"] != source.registry_source:
        raise CorrectionV2RetryContractError(f"{family_id} parent source identity drifted")
    if obj["registry_format"] != source.registry_format:
        raise CorrectionV2RetryContractError(f"{family_id} registry format drifted")
    denominator = _int(
        obj["cumulative_denominator"],
        f"families[{index}].cumulative_denominator",
        minimum=1,
    )
    if denominator != RETRY_DENOMINATORS[index]:
        raise CorrectionV2RetryContractError(f"{family_id} retry denominator differs from ledger")
    z = obj["z_threshold"]
    if isinstance(z, bool) or not isinstance(z, (int, float)) or not math.isfinite(float(z)):
        raise CorrectionV2RetryContractError(f"families[{index}].z_threshold is invalid")
    expected_z = NormalDist().inv_cdf(1.0 - 0.05 / denominator)
    # ``statistics.NormalDist.inv_cdf`` can differ by two binary64 ULPs
    # across supported CPython/libm builds.  Keep the frozen decimal binding
    # exact while allowing only that platform-level recomputation drift.
    if not math.isclose(float(z), expected_z, rel_tol=0.0, abs_tol=4.0 * math.ulp(float(z))):
        raise CorrectionV2RetryContractError(f"{family_id} retry z_threshold does not recompute")
    retry_count = _int(
        obj["retry_attempt_count"],
        f"families[{index}].retry_attempt_count",
        minimum=1,
    )
    if retry_count != RETRY_CANDIDATE_COUNTS[index]:
        raise CorrectionV2RetryContractError(f"{family_id} retry candidate count differs")
    return RetryFamily(
        family_id,
        count,
        order,
        registry,
        seed,
        retry_count,
        denominator,
        float(z),
    )


def _load_policy(value: object, parent: CorrectionV2Contract) -> Mapping[str, object]:
    obj = _mapping(value, "evaluation_policy")
    # Preserve a strict top-level object while comparing the canonical value
    # recursively to the unchanged v2 policy.  The retry preregistration uses
    # the same JSON shape as ``evaluation`` in the parent contract.
    try:
        parent_raw = json.loads((_ROOT / PARENT_CONFIG_PATH).read_bytes().decode("utf-8"))[
            "evaluation"
        ]
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, KeyError) as error:
        raise CorrectionV2RetryContractError("cannot read parent evaluation policy") from error
    if obj != parent_raw:
        raise CorrectionV2RetryContractError("Retry-1 evaluation policy differs from parent")
    # Touch the object so an accidentally unused parent does not hide a future
    # schema change; the dataclass itself carries the fully validated parent.
    if parent.evaluation.folds.commit_before_test_read is not True:
        raise CorrectionV2RetryContractError("parent evaluation policy is not fail-closed")
    return MappingProxyType(dict(obj))


def _load_flags(value: object, label: str) -> Mapping[str, object]:
    obj = _mapping(value, label)
    if not obj:
        raise CorrectionV2RetryContractError(f"{label} cannot be empty")
    for key, flag in obj.items():
        if not isinstance(flag, bool):
            raise CorrectionV2RetryContractError(f"{label}.{key} must be boolean")
        if flag and (key.casefold().find("holdout") >= 0 or key.casefold().find("outcome") >= 0):
            raise CorrectionV2RetryContractError(f"{label}.{key} must be false")
    return MappingProxyType(dict(obj))


def _load_requirements(value: object) -> Mapping[str, object]:
    obj = _mapping(value, "requirements")
    expected = frozenset(
        {
            "fresh_all_four_roots",
            "manifest_last",
            "new_attestation",
            "new_code_inventory",
            "new_empty_family_roots",
            "no_mixed_roots",
            "no_parent_outputs_reused",
            "same_data_identities",
            "same_evaluation_policies",
        }
    )
    _keys(obj, expected, "requirements")
    for key in expected:
        if key == "new_empty_family_roots":
            if _int(obj[key], f"requirements.{key}", minimum=1) != 4:
                raise CorrectionV2RetryContractError(f"requirements.{key} must be four")
        elif _bool(obj[key], f"requirements.{key}") is not True:
            raise CorrectionV2RetryContractError(f"requirements.{key} must be true")
    return MappingProxyType(dict(obj))


def _load_code_inventory(value: object) -> Mapping[str, object]:
    obj = _mapping(value, "code_inventory")
    expected = frozenset(
        {
            "all_four_family_runners_required",
            "bind_before_outcomes",
            "build_identity_required",
            "file_inventory_required",
            "new",
            "resolved_build_identity",
        }
    )
    _keys(obj, expected, "code_inventory")
    for key in expected - {"resolved_build_identity"}:
        if _bool(obj[key], f"code_inventory.{key}") is not True:
            raise CorrectionV2RetryContractError(f"code_inventory.{key} must be true")
    if obj["resolved_build_identity"] != "required-at-seal":
        raise CorrectionV2RetryContractError("code_inventory.resolved_build_identity is invalid")
    return MappingProxyType(dict(obj))


def _load_ledger(value: object, families: tuple[RetryFamily, ...]) -> Mapping[str, object]:
    obj = _mapping(value, "ledger")
    # The writer keeps the complete denominator arithmetic in this object.
    # Require the explicit base and retry counts, while allowing descriptive
    # audit fields to evolve only if they remain part of the canonical file.
    base = _int(obj.get("base"), "ledger.base", minimum=1)
    if base != BASE_DENOMINATOR:
        raise CorrectionV2RetryContractError("ledger base denominator is not 1463")
    denoms = obj.get("cumulative_denominators")
    if denoms != list(RETRY_DENOMINATORS):
        raise CorrectionV2RetryContractError("ledger denominators differ from Retry-1")
    retry_counts = obj.get("retry_candidate_counts")
    if not isinstance(retry_counts, list) or len(retry_counts) != 4:
        raise CorrectionV2RetryContractError("ledger retry_attempt_counts is invalid")
    for index, value_item in enumerate(retry_counts):
        count = _int(value_item, f"ledger.retry_attempt_counts[{index}]", minimum=1)
        if count != families[index].retry_attempt_count:
            raise CorrectionV2RetryContractError("ledger retry count differs from family")
    z_values = obj.get("z_thresholds")
    if not isinstance(z_values, list) or len(z_values) != 4:
        raise CorrectionV2RetryContractError("ledger z_thresholds is invalid")
    for index, z in enumerate(z_values):
        if (
            not isinstance(z, (int, float))
            or isinstance(z, bool)
            or not math.isclose(float(z), families[index].z_threshold, rel_tol=0.0, abs_tol=1e-15)
        ):
            raise CorrectionV2RetryContractError("ledger z threshold differs from family")
    return MappingProxyType(dict(obj))


def load_retry_contract(path: str | Path = CONFIG_PATH) -> CorrectionV2RetryContract:
    """Load and validate the canonical Retry-1 preregistration."""

    config_path = Path(path)
    try:
        raw = config_path.read_bytes()
        parsed = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs)
    except CorrectionV2RetryContractError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CorrectionV2RetryContractError("Retry-1 config is not valid UTF-8 JSON") from error
    if not isinstance(parsed, dict) or raw != canonical_json(parsed):
        raise CorrectionV2RetryContractError("Retry-1 config is not canonical JSON")
    root = cast(dict[str, object], parsed)
    expected_keys = frozenset(
        {
            "candidate_total",
            "code_inventory",
            "development",
            "evaluation_policy",
            "families",
            "holdout",
            "kind",
            "ledger",
            "original",
            "outcome_access",
            "parent",
            "requirements",
            "retry_document",
            "retry_id",
            "stable_data",
            "status",
            "version",
        }
    )
    _keys(root, expected_keys, "retry contract")
    kind = _text(root["kind"], "kind")
    if kind != CONTRACT_KIND or _int(root["version"], "version") != CONTRACT_VERSION:
        raise CorrectionV2RetryContractError("Retry-1 kind/version is invalid")
    status = _text(root["status"], "status")
    if status != STATUS:
        raise CorrectionV2RetryContractError("Retry-1 status is not preregistered_pre_outcome")
    retry_id = _text(root["retry_id"], "retry_id")
    if retry_id != RETRY_ID:
        raise CorrectionV2RetryContractError("Retry-1 identity is invalid")
    parent = load_correction_v2_contract()
    retry_document = _load_document(root["retry_document"])
    original_obj = _mapping(root["original"], "original")
    _keys(
        original_obj,
        frozenset({"config_path", "config_sha256", "amendment_path", "amendment_sha256"}),
        "original",
    )
    original_config_path = _relative(original_obj["config_path"], "original.config_path")
    if original_config_path != Path(PARENT_CONFIG_PATH).as_posix():
        raise CorrectionV2RetryContractError(
            "original.config_path differs from canonical v2 config"
        )
    original_config_sha = _sha(original_obj["config_sha256"], "original.config_sha256")
    if original_config_sha != _sha256_file(_ROOT / PARENT_CONFIG_PATH):
        raise CorrectionV2RetryContractError("original.config_sha256 differs from v2 config")
    original_amendment_path = _relative(original_obj["amendment_path"], "original.amendment_path")
    if original_amendment_path != AMENDMENT_PATH or parent.amendment.sha256 is None:
        raise CorrectionV2RetryContractError("original.amendment_path differs from v2 amendment")
    original_amendment_sha = _sha(original_obj["amendment_sha256"], "original.amendment_sha256")
    if original_amendment_sha != parent.amendment.sha256:
        raise CorrectionV2RetryContractError("original.amendment_sha256 differs from v2 amendment")
    original = MappingProxyType(
        {
            "config_path": original_config_path,
            "config_sha256": original_config_sha,
            "amendment_path": original_amendment_path,
            "amendment_sha256": original_amendment_sha,
        }
    )
    parent_binding = _load_parent(root["parent"], parent)
    stable_data = _load_stable_data(root["stable_data"])
    raw_families = root["families"]
    if not isinstance(raw_families, list) or len(raw_families) != 4:
        raise CorrectionV2RetryContractError("Retry-1 must contain exactly four families")
    families = tuple(_load_family(item, index, parent) for index, item in enumerate(raw_families))
    ledger = _load_ledger(root["ledger"], families)
    development = _mapping(root["development"], "development")
    _keys(
        development,
        frozenset({"start_slot_inclusive", "end_slot_exclusive"}),
        "development",
    )
    if (
        development.get("start_slot_inclusive") != parent.dev.start_slot_inclusive
        or development.get("end_slot_exclusive") != parent.dev.end_slot_exclusive
    ):
        raise CorrectionV2RetryContractError("Retry-1 development bounds differ from parent")
    holdout = _mapping(root["holdout"], "holdout")
    _keys(
        holdout,
        frozenset(
            {
                "start_slot_inclusive",
                "end_slot_exclusive",
                "feature_generation_allowed",
                "label_generation_allowed",
                "locked",
                "metadata_queries_allowed",
                "outcome_access_allowed",
                "payload_queries_allowed",
                "physically_forbidden",
                "pnl_access_allowed",
                "viability_tuning_allowed",
            }
        ),
        "holdout",
    )
    for key, value in holdout.items():
        if key not in {"start_slot_inclusive", "end_slot_exclusive"} and not isinstance(
            value, bool
        ):
            raise CorrectionV2RetryContractError(f"holdout.{key} must be boolean")
    if holdout.get("locked") is not True or holdout.get("physically_forbidden") is not True:
        raise CorrectionV2RetryContractError("Retry-1 holdout is not physically locked")
    if (
        holdout.get("start_slot_inclusive") != parent.holdout.interval.start_slot_inclusive
        or holdout.get("end_slot_exclusive") != parent.holdout.interval.end_slot_exclusive
    ):
        raise CorrectionV2RetryContractError("Retry-1 holdout bounds differ from parent")
    if any(
        value is not False
        for key, value in holdout.items()
        if "access" in key or "outcome" in key or "payload" in key or key.endswith("_allowed")
    ):
        raise CorrectionV2RetryContractError("Retry-1 holdout/outcome access is not false")
    evaluation_policy = _load_policy(root["evaluation_policy"], parent)
    outcome_obj = _mapping(root["outcome_access"], "outcome_access")
    _keys(
        outcome_obj,
        frozenset(
            {
                "corrected_pnl_read",
                "corrected_report_read",
                "corrected_survivor_read",
                "holdout_touched",
                "old_shock_result_quarantined",
                "old_shock_result_read",
                "progress_output_published",
            }
        ),
        "outcome_access",
    )
    outcome_access = _load_flags(outcome_obj, "outcome_access")
    expected_outcome_access = {
        "corrected_pnl_read": False,
        "corrected_report_read": False,
        "corrected_survivor_read": False,
        "holdout_touched": False,
        "old_shock_result_quarantined": True,
        "old_shock_result_read": False,
        "progress_output_published": False,
    }
    if dict(outcome_access) != expected_outcome_access:
        raise CorrectionV2RetryContractError(
            "outcome_access differs from the frozen pre-outcome incident state"
        )
    requirements = _load_requirements(root["requirements"])
    code_inventory = _load_code_inventory(root["code_inventory"])
    candidate_total = _int(root["candidate_total"], "candidate_total", minimum=1)
    if candidate_total != parent.candidate_total:
        raise CorrectionV2RetryContractError("Retry-1 candidate_total differs from parent")
    if sum(item.candidate_count for item in families) != candidate_total:
        raise CorrectionV2RetryContractError("Retry-1 family candidate total is inconsistent")
    config_sha = _sha256_bytes(raw)
    if config_sha != FROZEN_CONFIG_SHA256:
        raise CorrectionV2RetryContractError(
            "Retry-1 config differs from its frozen preregistration digest"
        )
    return CorrectionV2RetryContract(
        kind,
        CONTRACT_VERSION,
        status,
        retry_id,
        retry_document,
        original,
        parent_binding,
        stable_data,
        families,
        ledger,
        MappingProxyType(dict(development)),
        MappingProxyType(dict(holdout)),
        evaluation_policy,
        outcome_access,
        requirements,
        code_inventory,
        candidate_total,
        config_sha,
        parent,
    )


load_correction_v2_retry_contract = load_retry_contract
RetryContract = CorrectionV2RetryContract
Retry1Contract = CorrectionV2RetryContract
RetryContractError = CorrectionV2RetryContractError
Retry1ContractError = CorrectionV2RetryContractError


__all__ = [
    "BASE_DENOMINATOR",
    "CONFIG_PATH",
    "RETRY_CONFIG_PATH",
    "CONTRACT_KIND",
    "CONTRACT_VERSION",
    "CorrectionV2RetryContract",
    "CorrectionV2RetryContractError",
    "FAMILY_IDS",
    "FROZEN_CONFIG_SHA256",
    "RETRY_DENOMINATORS",
    "RETRY_CANDIDATE_COUNTS",
    "RETRY_DOCUMENT_PATH",
    "RETRY_DOC_PATH",
    "RETRY_ID",
    "RetryDocument",
    "RetryContract",
    "RetryContractError",
    "Retry1Contract",
    "Retry1ContractError",
    "RetryFamily",
    "RetryParentBinding",
    "load_correction_v2_retry_contract",
    "load_retry_contract",
]
