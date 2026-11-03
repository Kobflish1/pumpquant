"""Canonical, fail-closed contract for the fresh bonding-prestate correction.

The correction contract is intentionally a small, read-only boundary.  It
loads only the canonical JSON configuration and the source candidate
registries; it never opens a feature, report, remote, object-store, or
holdout payload.  A deployment may resolve the amendment digest through the
required binding in the JSON document.  Keeping that binding explicit avoids
silently freezing a documentation hash while the amendment is under review.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import NormalDist
from types import MappingProxyType
from typing import Final, Protocol, cast

from pumpquant.research.families.amm_impact_ofi import (
    default_amm_impact_ofi_candidates,
)
from pumpquant.research.families.amm_impact_ofi import (
    registry_payload as amm_registry_payload,
)
from pumpquant.research.families.persistent_pressure import (
    default_persistent_pressure_candidates,
)
from pumpquant.research.families.persistent_pressure import (
    registry_payload as persistent_registry_payload,
)
from pumpquant.research.families.progress_speed import (
    default_progress_speed_candidates,
)
from pumpquant.research.families.progress_speed import (
    registry_payload as progress_registry_payload,
)
from pumpquant.research.families.shock_recovery import default_shock_recovery_candidates
from pumpquant.research.families.shock_recovery_grid import (
    registry_payload as shock_registry_payload,
)

CONFIG_PATH: Final[Path] = Path("configs/fresh_bonding_prestate_correction_v2.json")
CONTRACT_KIND: Final[str] = "pumpquant.fresh_bonding_prestate_correction_v2_contract"
CONTRACT_VERSION: Final[int] = 2
CORRECTION_ID: Final[str] = "fresh-bonding-prestate-correction-v2"
CORRECTION_KIND: Final[str] = "pumpquant.bonding_prestate_correction"
AMENDMENT_PATH: Final[str] = "docs/FRESH_V1_BONDING_PRESTATE_CORRECTION_V2.md"
AMM_REGISTRY_PATH: Final[str] = "artifacts/amm-impact-ofi-fresh-v1/registry.json"

DEVELOPMENT_START_SLOT: Final[int] = 431_265_461
DEVELOPMENT_END_SLOT: Final[int] = 432_432_000
HOLDOUT_START_SLOT: Final[int] = 432_432_000
HOLDOUT_END_SLOT: Final[int] = 432_864_000

_ALPHA: Final[float] = 0.05
_HEX64_CHARS: Final[frozenset[str]] = frozenset("0123456789abcdef")
_SEALED_V1_REGISTRY_Z: Final[Mapping[str, float]] = MappingProxyType(
    {
        "pumpquant.shock_recovery_fresh_registry": 3.922901121347733,
        "pumpquant.progress_speed_registry": 3.929546034498202,
        "pumpquant.persistent_pressure_registry": 3.9360026984861083,
        "pumpquant.amm_impact_ofi_registry": 3.9422812559637803,
    }
)


class _CandidateLike(Protocol):
    @property
    def sha256(self) -> str: ...


class CorrectionV2ContractError(ValueError):
    """The correction contract is absent, malformed, or has drifted."""


def canonical_json(value: object) -> bytes:
    """Encode a JSON value using the contract's canonical UTF-8 form."""

    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as error:
        raise CorrectionV2ContractError("value cannot be encoded as canonical JSON") from error
    return (encoded + "\n").encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise CorrectionV2ContractError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class SlotInterval:
    """A half-open slot interval."""

    start_slot_inclusive: int
    end_slot_exclusive: int

    def __post_init__(self) -> None:
        if isinstance(self.start_slot_inclusive, bool) or not isinstance(
            self.start_slot_inclusive, int
        ):
            raise CorrectionV2ContractError("interval start must be an integer")
        if isinstance(self.end_slot_exclusive, bool) or not isinstance(
            self.end_slot_exclusive, int
        ):
            raise CorrectionV2ContractError("interval end must be an integer")
        if self.start_slot_inclusive < 0 or self.end_slot_exclusive <= self.start_slot_inclusive:
            raise CorrectionV2ContractError("slot interval must be non-empty and ordered")


@dataclass(frozen=True, slots=True)
class HoldoutLock:
    """The physical no-read boundary for the chronological holdout."""

    interval: SlotInterval
    locked: bool
    physically_forbidden: bool
    feature_generation_allowed: bool
    label_generation_allowed: bool
    outcome_access_allowed: bool
    pnl_access_allowed: bool
    metadata_queries_allowed: bool
    payload_queries_allowed: bool
    viability_tuning_allowed: bool

    def __post_init__(self) -> None:
        if not isinstance(self.interval, SlotInterval):
            raise CorrectionV2ContractError("holdout interval has an invalid type")
        flags = (
            self.locked,
            self.physically_forbidden,
            self.feature_generation_allowed,
            self.label_generation_allowed,
            self.outcome_access_allowed,
            self.pnl_access_allowed,
            self.metadata_queries_allowed,
            self.payload_queries_allowed,
            self.viability_tuning_allowed,
        )
        if any(not isinstance(flag, bool) for flag in flags):
            raise CorrectionV2ContractError("holdout lock flags must be booleans")
        if not self.locked or not self.physically_forbidden or any(flags[2:]):
            raise CorrectionV2ContractError("holdout is not physically fail-closed")


@dataclass(frozen=True, slots=True)
class CandidateRef:
    """One immutable candidate identity in frozen registry order."""

    sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.sha256, str)
            or len(self.sha256) != 64
            or any(character not in _HEX64_CHARS for character in self.sha256)
        ):
            raise CorrectionV2ContractError("candidate SHA-256 is invalid")


@dataclass(frozen=True, slots=True)
class FamilyContract:
    """A corrected v2 family and its frozen v1 candidate order."""

    id: str
    kind: str
    version: int
    v1_experiment_id: str
    v1_kind: str
    v1_version: int
    candidate_count: int
    candidate_order_sha256: str
    registry_sha256: str
    registry_source: str
    registry_format: str
    candidates: tuple[CandidateRef, ...]
    cumulative_denominator: int
    z_threshold: float
    bootstrap_seed: str

    def __post_init__(self) -> None:
        text_fields = (
            self.id,
            self.kind,
            self.v1_experiment_id,
            self.v1_kind,
            self.registry_source,
            self.registry_format,
            self.bootstrap_seed,
        )
        if any(not isinstance(value, str) or not value.strip() for value in text_fields):
            raise CorrectionV2ContractError("family text field is missing or invalid")
        if self.version != CONTRACT_VERSION or self.v1_version != 1:
            raise CorrectionV2ContractError("family version is not correction v2 over v1")
        if isinstance(self.candidate_count, bool) or not isinstance(self.candidate_count, int):
            raise CorrectionV2ContractError("candidate count must be an integer")
        if self.candidate_count != len(self.candidates) or self.candidate_count <= 0:
            raise CorrectionV2ContractError("candidate count does not match frozen order")
        if any(not isinstance(candidate, CandidateRef) for candidate in self.candidates):
            raise CorrectionV2ContractError("candidate order contains an invalid type")
        if len({candidate.sha256 for candidate in self.candidates}) != self.candidate_count:
            raise CorrectionV2ContractError("candidate order contains duplicate identities")
        for label, value in (
            ("candidate_order_sha256", self.candidate_order_sha256),
            ("registry_sha256", self.registry_sha256),
        ):
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in _HEX64_CHARS for character in value)
            ):
                raise CorrectionV2ContractError(f"{label} is invalid")
        if (
            isinstance(self.cumulative_denominator, bool)
            or not isinstance(self.cumulative_denominator, int)
            or self.cumulative_denominator < self.candidate_count
        ):
            raise CorrectionV2ContractError("family denominator is invalid")
        if not isinstance(self.z_threshold, (int, float)) or isinstance(self.z_threshold, bool):
            raise CorrectionV2ContractError("family z threshold is invalid")
        if not math.isfinite(float(self.z_threshold)):
            raise CorrectionV2ContractError("family z threshold is not finite")
        if self.bootstrap_seed != self.v1_experiment_id:
            raise CorrectionV2ContractError("bootstrap seed must remain the exact v1 experiment ID")

    @property
    def candidate_hashes(self) -> tuple[str, ...]:
        return tuple(candidate.sha256 for candidate in self.candidates)

    @property
    def candidate_order(self) -> tuple[str, ...]:
        return self.candidate_hashes

    @property
    def registry_hash(self) -> str:
        return self.registry_sha256

    @property
    def denominator(self) -> int:
        return self.cumulative_denominator

    @property
    def threshold(self) -> float:
        return self.z_threshold


@dataclass(frozen=True, slots=True)
class FoldPolicy:
    initial_train_fraction: float
    contiguous_block_count: int
    validation_test_fold_count: int
    launch_time_embargo_seconds: int
    chronological: bool
    whole_token: bool
    threshold_fit_scope: str
    selection_scope: str
    commit_before_test_read: bool


@dataclass(frozen=True, slots=True)
class LatencyPolicy:
    decision_to_entry_events: int
    exit_horizons_events: tuple[int, ...]
    same_phase_required: bool
    same_continuity_segment_required: bool

    @property
    def entry_event_offset(self) -> int:
        return self.decision_to_entry_events


@dataclass(frozen=True, slots=True)
class CostPolicy:
    screening_notional_quote: float
    max_reserve_fraction: float
    fixed_cost_quote_per_leg: float
    adverse_selection_bps: float
    observed_fee_required: bool
    fallback_fee_allowed: bool

    @property
    def reserve_cap_fraction(self) -> float:
        return self.max_reserve_fraction

    @property
    def adverse_selection_haircut_bps(self) -> float:
        return self.adverse_selection_bps


@dataclass(frozen=True, slots=True)
class WalletPolicy:
    starting_usd: Decimal
    allocation_percent: int
    max_concurrent_positions: int
    one_position_per_mint: bool
    insufficient_cash_rejected: bool
    no_negative_equity: bool
    causal_fx_series_required: bool
    final_sol_usd_requirements_unchanged: bool

    @property
    def starting_capital_usd(self) -> Decimal:
        return self.starting_usd


@dataclass(frozen=True, slots=True)
class EvaluationPolicy:
    folds: FoldPolicy
    latency: LatencyPolicy
    costs: CostPolicy
    wallet: WalletPolicy


@dataclass(frozen=True, slots=True)
class AmendmentBinding:
    path: str
    sha256: str | None
    deployment_binding_kind: str | None
    deployment_binding_field: str | None
    deployment_binding_required: bool

    def __post_init__(self) -> None:
        if self.path != AMENDMENT_PATH:
            raise CorrectionV2ContractError("amendment path differs from the sealed document")
        if self.sha256 is not None and (
            len(self.sha256) != 64
            or any(character not in _HEX64_CHARS for character in self.sha256)
        ):
            raise CorrectionV2ContractError("amendment SHA-256 is invalid")
        has_hash = self.sha256 is not None
        has_binding = (
            self.deployment_binding_kind is not None
            and self.deployment_binding_field is not None
            and self.deployment_binding_required
        )
        if has_hash == has_binding:
            raise CorrectionV2ContractError(
                "amendment must use exactly one digest or required deployment binding"
            )


@dataclass(frozen=True, slots=True)
class CorrectionV2Contract:
    """Fully validated immutable correction-v2 experiment contract."""

    kind: str
    version: int
    experiment_id: str
    correction_id: str
    correction_kind: str
    correction_version: int
    amendment: AmendmentBinding
    development: SlotInterval
    holdout: HoldoutLock
    families: tuple[FamilyContract, ...]
    evaluation: EvaluationPolicy
    candidate_total: int
    corrected_outcomes_read_before_sealing: bool

    @property
    def dev(self) -> SlotInterval:
        """Alias used by runners that call the development interval ``dev``."""

        return self.development

    @property
    def family_map(self) -> Mapping[str, FamilyContract]:
        return MappingProxyType({family.id: family for family in self.families})

    @property
    def amendment_sha256(self) -> str | None:
        return self.amendment.sha256

    def __post_init__(self) -> None:
        if self.kind != CONTRACT_KIND or self.version != CONTRACT_VERSION:
            raise CorrectionV2ContractError("contract kind/version is not correction v2")
        if (
            self.experiment_id != CORRECTION_ID
            or self.correction_id != CORRECTION_ID
            or self.correction_kind != CORRECTION_KIND
            or self.correction_version != CONTRACT_VERSION
        ):
            raise CorrectionV2ContractError("correction identity is invalid")
        if not isinstance(self.amendment, AmendmentBinding):
            raise CorrectionV2ContractError("amendment binding has an invalid type")
        if self.development != SlotInterval(DEVELOPMENT_START_SLOT, DEVELOPMENT_END_SLOT):
            raise CorrectionV2ContractError("development interval differs from the sealed boundary")
        if self.holdout.interval != SlotInterval(HOLDOUT_START_SLOT, HOLDOUT_END_SLOT):
            raise CorrectionV2ContractError("holdout interval differs from the sealed boundary")
        if len(self.families) != 4 or any(
            not isinstance(family, FamilyContract) for family in self.families
        ):
            raise CorrectionV2ContractError("exactly four family contracts are required")
        if len({family.id for family in self.families}) != 4:
            raise CorrectionV2ContractError("family IDs must be unique")
        if self.candidate_total != sum(family.candidate_count for family in self.families):
            raise CorrectionV2ContractError("candidate total does not match family counts")
        if self.candidate_total != 288:
            raise CorrectionV2ContractError("correction-v2 candidate total must be 288")
        if self.corrected_outcomes_read_before_sealing:
            raise CorrectionV2ContractError("corrected outcomes cannot be read before sealing")


def _mapping(value: object, path: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise CorrectionV2ContractError(f"{path} must be an object")
    return value


def _keys(value: Mapping[str, object], allowed: frozenset[str], path: str) -> None:
    unknown = set(value) - allowed
    if unknown:
        names = ", ".join(sorted(unknown))
        raise CorrectionV2ContractError(f"{path} has unknown key(s): {names}")


def _required(value: Mapping[str, object], key: str, path: str) -> object:
    if key not in value:
        raise CorrectionV2ContractError(f"{path} is missing required key {key!r}")
    return value[key]


def _text(value: object, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CorrectionV2ContractError(f"{path} must be a non-empty string")
    return value


def _bool(value: object, path: str) -> bool:
    if not isinstance(value, bool):
        raise CorrectionV2ContractError(f"{path} must be a boolean")
    return value


def _int(value: object, path: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CorrectionV2ContractError(f"{path} must be an integer")
    if minimum is not None and value < minimum:
        raise CorrectionV2ContractError(f"{path} must be >= {minimum}")
    return value


def _number(value: object, path: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CorrectionV2ContractError(f"{path} must be a number")
    result = float(value)
    if not math.isfinite(result) or (minimum is not None and result < minimum):
        raise CorrectionV2ContractError(f"{path} must be finite and valid")
    return result


def _decimal(value: object, path: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise CorrectionV2ContractError(f"{path} must be a decimal string")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise CorrectionV2ContractError(f"{path} is not a decimal") from error
    if not result.is_finite() or result <= 0:
        raise CorrectionV2ContractError(f"{path} must be positive and finite")
    return result


def _interval(value: object, path: str) -> SlotInterval:
    obj = _mapping(value, path)
    _keys(obj, frozenset({"start_slot_inclusive", "end_slot_exclusive"}), path)
    return SlotInterval(
        _int(
            _required(obj, "start_slot_inclusive", path), f"{path}.start_slot_inclusive", minimum=0
        ),
        _int(_required(obj, "end_slot_exclusive", path), f"{path}.end_slot_exclusive", minimum=1),
    )


def _load_amendment(value: object) -> AmendmentBinding:
    obj = _mapping(value, "amendment")
    _keys(obj, frozenset({"path", "sha256", "deployment_binding"}), "amendment")
    path = _text(_required(obj, "path", "amendment"), "amendment.path")
    digest = obj.get("sha256")
    if digest is not None:
        digest = _text(digest, "amendment.sha256")
    binding_kind: str | None = None
    binding_field: str | None = None
    binding_required = False
    binding = obj.get("deployment_binding")
    if binding is not None:
        binding_obj = _mapping(binding, "amendment.deployment_binding")
        _keys(
            binding_obj,
            frozenset({"kind", "field", "required"}),
            "amendment.deployment_binding",
        )
        binding_kind = _text(
            _required(binding_obj, "kind", "amendment.deployment_binding"),
            "amendment.deployment_binding.kind",
        )
        binding_field = _text(
            _required(binding_obj, "field", "amendment.deployment_binding"),
            "amendment.deployment_binding.field",
        )
        binding_required = _bool(
            _required(binding_obj, "required", "amendment.deployment_binding"),
            "amendment.deployment_binding.required",
        )
    result = AmendmentBinding(path, digest, binding_kind, binding_field, binding_required)
    if result.sha256 is not None:
        # A literal digest is permitted only when it is demonstrably the
        # current amendment file.  The default contract deliberately uses the
        # required deployment binding branch above.
        amendment_file = Path(__file__).resolve().parents[4] / result.path
        if (
            not amendment_file.is_file()
            or _sha256_bytes(amendment_file.read_bytes()) != result.sha256
        ):
            raise CorrectionV2ContractError("amendment SHA-256 does not match the current file")
    elif result.deployment_binding_kind != "pumpquant.required_deployment_binding":
        raise CorrectionV2ContractError("amendment deployment binding kind is invalid")
    elif result.deployment_binding_field != "amendment_sha256":
        raise CorrectionV2ContractError("amendment deployment binding field is invalid")
    return result


def _load_holdout(value: object) -> HoldoutLock:
    obj = _mapping(value, "holdout")
    _keys(
        obj,
        frozenset(
            {
                "start_slot_inclusive",
                "end_slot_exclusive",
                "locked",
                "physically_forbidden",
                "feature_generation_allowed",
                "label_generation_allowed",
                "outcome_access_allowed",
                "pnl_access_allowed",
                "metadata_queries_allowed",
                "payload_queries_allowed",
                "viability_tuning_allowed",
            }
        ),
        "holdout",
    )
    interval = _interval(
        {
            "start_slot_inclusive": _required(obj, "start_slot_inclusive", "holdout"),
            "end_slot_exclusive": _required(obj, "end_slot_exclusive", "holdout"),
        },
        "holdout.interval",
    )
    return HoldoutLock(
        interval,
        *(
            _bool(_required(obj, name, "holdout"), f"holdout.{name}")
            for name in (
                "locked",
                "physically_forbidden",
                "feature_generation_allowed",
                "label_generation_allowed",
                "outcome_access_allowed",
                "pnl_access_allowed",
                "metadata_queries_allowed",
                "payload_queries_allowed",
                "viability_tuning_allowed",
            )
        ),
    )


def _registry_payload_for_family(family_id: str) -> Mapping[str, object]:
    if family_id == "shock-recovery-fresh-v2":
        return cast(Mapping[str, object], shock_registry_payload())
    if family_id == "progress-speed-fresh-v2":
        return cast(Mapping[str, object], progress_registry_payload())
    if family_id == "persistent-pressure-fresh-v2":
        return cast(Mapping[str, object], persistent_registry_payload())
    if family_id != "amm-impact-ofi-fresh-v2":
        raise CorrectionV2ContractError(f"unknown corrected family {family_id!r}")
    path = Path(__file__).resolve().parents[4] / AMM_REGISTRY_PATH
    if path.is_symlink() or not path.is_file():
        raise CorrectionV2ContractError("AMM registry artifact is absent or unsafe")
    body = path.read_bytes()
    try:
        value = json.loads(body.decode("utf-8"), object_pairs_hook=_canonical_object_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CorrectionV2ContractError("AMM registry artifact is invalid JSON") from error
    if not isinstance(value, dict) or body != canonical_json(value):
        raise CorrectionV2ContractError("AMM registry artifact is not canonical JSON")
    artifact = cast(Mapping[str, object], value)
    module = cast(Mapping[str, object], amm_registry_payload())
    if _source_registry_digest(artifact) != _source_registry_digest(module):
        raise CorrectionV2ContractError("AMM registry artifact differs from its module")
    return artifact


def _family_specs() -> tuple[tuple[str, tuple[_CandidateLike, ...], Mapping[str, object]], ...]:
    """Read source registries only (no outcome-bearing data)."""

    shock = tuple(default_shock_recovery_candidates())
    progress = tuple(default_progress_speed_candidates())
    persistent = tuple(default_persistent_pressure_candidates())
    amm = tuple(default_amm_impact_ofi_candidates())
    return (
        ("shock-recovery-fresh-v2", shock, _registry_payload_for_family("shock-recovery-fresh-v2")),
        (
            "progress-speed-fresh-v2",
            progress,
            _registry_payload_for_family("progress-speed-fresh-v2"),
        ),
        (
            "persistent-pressure-fresh-v2",
            persistent,
            _registry_payload_for_family("persistent-pressure-fresh-v2"),
        ),
        (
            "amm-impact-ofi-fresh-v2",
            amm,
            _registry_payload_for_family("amm-impact-ofi-fresh-v2"),
        ),
    )


def _family_kind(family_id: str) -> str:
    return f"pumpquant.{family_id.removesuffix('-fresh-v2').replace('-', '_')}_correction_v2"


_FAMILIES_DIR = "src/pumpquant/research/families"


def _registry_source(family_id: str) -> str:
    return {
        "shock-recovery-fresh-v2": f"{_FAMILIES_DIR}/shock_recovery_grid.py:registry_payload",
        "progress-speed-fresh-v2": f"{_FAMILIES_DIR}/progress_speed.py:registry_payload",
        "persistent-pressure-fresh-v2": f"{_FAMILIES_DIR}/persistent_pressure.py:registry_payload",
        "amm-impact-ofi-fresh-v2": AMM_REGISTRY_PATH,
    }[family_id]


def _v1_experiment_id(family_id: str) -> str:
    return {
        "shock-recovery-fresh-v2": "shock-recovery-fresh-v1",
        "progress-speed-fresh-v2": "progress-speed-fresh-v1",
        "persistent-pressure-fresh-v2": "persistent-pressure-fresh-v1",
        "amm-impact-ofi-fresh-v2": "amm-impact-ofi-fresh-v1",
    }[family_id]


def _cumulative_denominator(family_id: str) -> int:
    return {
        "shock-recovery-fresh-v2": 1431,
        "progress-speed-fresh-v2": 1463,
        "persistent-pressure-fresh-v2": 1495,
        "amm-impact-ofi-fresh-v2": 1527,
    }[family_id]


def _v1_kind(family_id: str) -> str:
    return {
        "shock-recovery-fresh-v2": "pumpquant.shock_recovery_fresh_registry",
        "progress-speed-fresh-v2": "pumpquant.progress_speed_registry",
        "persistent-pressure-fresh-v2": "pumpquant.persistent_pressure_registry",
        "amm-impact-ofi-fresh-v2": "pumpquant.amm_impact_ofi_registry",
    }[family_id]


def _source_registry_digest(payload: Mapping[str, object]) -> str:
    # NormalDist.inv_cdf can differ by one final binary ULP across supported
    # Python/libm builds.  The v1 registry hash was sealed with exact decimal
    # values, so normalize only that derived, non-candidate field after proving
    # the runtime value is equivalent.  Candidate order, geometry, denominator,
    # and every other registry field remain byte-sensitive.
    normalized = dict(payload)
    kind = normalized.get("kind")
    sealed_z = _SEALED_V1_REGISTRY_Z.get(kind) if isinstance(kind, str) else None
    if sealed_z is not None:
        runtime_z = normalized.get("z_threshold")
        if (
            isinstance(runtime_z, bool)
            or not isinstance(runtime_z, (int, float))
            or not math.isclose(float(runtime_z), sealed_z, rel_tol=0.0, abs_tol=1e-15)
        ):
            raise CorrectionV2ContractError(
                f"{kind} runtime z_threshold differs from its sealed v1 value"
            )
        normalized["z_threshold"] = sealed_z
    return _sha256_bytes(canonical_json(normalized))


def _load_family(
    value: object,
    index: int,
    source: tuple[str, tuple[_CandidateLike, ...], Mapping[str, object]],
) -> FamilyContract:
    path = f"families[{index}]"
    obj = _mapping(value, path)
    _keys(
        obj,
        frozenset(
            {
                "id",
                "kind",
                "version",
                "v1_experiment_id",
                "v1_kind",
                "v1_version",
                "candidate_count",
                "candidate_order_sha256",
                "registry_sha256",
                "registry_source",
                "registry_format",
                "candidates",
                "cumulative_denominator",
                "z_threshold",
                "bootstrap_seed",
            }
        ),
        path,
    )
    family_id = _text(_required(obj, "id", path), f"{path}.id")
    source_id, source_candidates, source_payload = source
    if family_id != source_id:
        raise CorrectionV2ContractError(f"{path}.id is not the frozen family order")
    candidates_value = _required(obj, "candidates", path)
    if not isinstance(candidates_value, list):
        raise CorrectionV2ContractError(f"{path}.candidates must be an array")
    candidates: list[CandidateRef] = []
    for candidate_index, candidate in enumerate(candidates_value):
        # The array intentionally contains only SHA-256 strings.  Names remain
        # derivable from the source registry and this minimizes duplicated
        # mutable metadata in the sealed contract.
        candidates.append(CandidateRef(_text(candidate, f"{path}.candidates[{candidate_index}]")))
    source_hashes = tuple(candidate.sha256 for candidate in source_candidates)
    if tuple(candidate.sha256 for candidate in candidates) != source_hashes:
        raise CorrectionV2ContractError(
            f"{path}.candidates differ from the current frozen module order"
        )
    candidate_order_hash = _text(
        _required(obj, "candidate_order_sha256", path), f"{path}.candidate_order_sha256"
    )
    expected_candidate_order_hash = _sha256_bytes("\n".join(source_hashes).encode())
    if candidate_order_hash != expected_candidate_order_hash:
        raise CorrectionV2ContractError(f"{path}.candidate_order_sha256 is not recomputable")
    registry_hash = _text(_required(obj, "registry_sha256", path), f"{path}.registry_sha256")
    expected_registry_hash = _source_registry_digest(source_payload)
    if registry_hash != expected_registry_hash:
        raise CorrectionV2ContractError(f"{path}.registry_sha256 differs from the current registry")
    registry_source = _text(_required(obj, "registry_source", path), f"{path}.registry_source")
    if registry_source != _registry_source(family_id):
        raise CorrectionV2ContractError(f"{path}.registry_source differs from the current registry")
    registry_format = _text(_required(obj, "registry_format", path), f"{path}.registry_format")
    if registry_format != "canonical_json_utf8_newline":
        raise CorrectionV2ContractError(f"{path}.registry_format is unsupported")
    expected_count = len(source_hashes)
    candidate_count = _int(
        _required(obj, "candidate_count", path), f"{path}.candidate_count", minimum=1
    )
    if candidate_count != expected_count:
        raise CorrectionV2ContractError(f"{path}.candidate_count differs from the source registry")
    denominator = _int(
        _required(obj, "cumulative_denominator", path), f"{path}.cumulative_denominator", minimum=1
    )
    if denominator != _cumulative_denominator(family_id):
        raise CorrectionV2ContractError(
            f"{path}.cumulative_denominator differs from the sealed ledger"
        )
    expected_z = NormalDist().inv_cdf(1.0 - _ALPHA / denominator)
    z_threshold = _number(_required(obj, "z_threshold", path), f"{path}.z_threshold", minimum=0.0)
    if not math.isclose(z_threshold, expected_z, rel_tol=0.0, abs_tol=1e-15):
        raise CorrectionV2ContractError(
            f"{path}.z_threshold does not recompute from its denominator"
        )
    v1_id = _text(_required(obj, "v1_experiment_id", path), f"{path}.v1_experiment_id")
    if v1_id != _v1_experiment_id(family_id):
        raise CorrectionV2ContractError(
            f"{path}.v1_experiment_id differs from the frozen v1 family"
        )
    bootstrap_seed = _text(_required(obj, "bootstrap_seed", path), f"{path}.bootstrap_seed")
    if bootstrap_seed != v1_id:
        raise CorrectionV2ContractError(
            f"{path}.bootstrap_seed differs from the v1 family experiment ID"
        )
    return FamilyContract(
        id=family_id,
        kind=_text(_required(obj, "kind", path), f"{path}.kind"),
        version=_int(_required(obj, "version", path), f"{path}.version"),
        v1_experiment_id=v1_id,
        v1_kind=_text(_required(obj, "v1_kind", path), f"{path}.v1_kind"),
        v1_version=_int(_required(obj, "v1_version", path), f"{path}.v1_version"),
        candidate_count=candidate_count,
        candidate_order_sha256=candidate_order_hash,
        registry_sha256=registry_hash,
        registry_source=registry_source,
        registry_format=registry_format,
        candidates=tuple(candidates),
        cumulative_denominator=denominator,
        z_threshold=z_threshold,
        bootstrap_seed=bootstrap_seed,
    )


def _load_evaluation(value: object) -> EvaluationPolicy:
    obj = _mapping(value, "evaluation")
    _keys(obj, frozenset({"folds", "latency", "costs", "wallet"}), "evaluation")
    folds_obj = _mapping(_required(obj, "folds", "evaluation"), "evaluation.folds")
    _keys(
        folds_obj,
        frozenset(
            {
                "initial_train_fraction",
                "contiguous_block_count",
                "validation_test_fold_count",
                "launch_time_embargo_seconds",
                "chronological",
                "whole_token",
                "threshold_fit_scope",
                "selection_scope",
                "commit_before_test_read",
            }
        ),
        "evaluation.folds",
    )
    folds = FoldPolicy(
        _number(
            _required(folds_obj, "initial_train_fraction", "evaluation.folds"),
            "evaluation.folds.initial_train_fraction",
        ),
        _int(
            _required(folds_obj, "contiguous_block_count", "evaluation.folds"),
            "evaluation.folds.contiguous_block_count",
            minimum=1,
        ),
        _int(
            _required(folds_obj, "validation_test_fold_count", "evaluation.folds"),
            "evaluation.folds.validation_test_fold_count",
            minimum=1,
        ),
        _int(
            _required(folds_obj, "launch_time_embargo_seconds", "evaluation.folds"),
            "evaluation.folds.launch_time_embargo_seconds",
            minimum=0,
        ),
        _bool(
            _required(folds_obj, "chronological", "evaluation.folds"),
            "evaluation.folds.chronological",
        ),
        _bool(
            _required(folds_obj, "whole_token", "evaluation.folds"), "evaluation.folds.whole_token"
        ),
        _text(
            _required(folds_obj, "threshold_fit_scope", "evaluation.folds"),
            "evaluation.folds.threshold_fit_scope",
        ),
        _text(
            _required(folds_obj, "selection_scope", "evaluation.folds"),
            "evaluation.folds.selection_scope",
        ),
        _bool(
            _required(folds_obj, "commit_before_test_read", "evaluation.folds"),
            "evaluation.folds.commit_before_test_read",
        ),
    )
    if (
        folds.initial_train_fraction != 0.30
        or folds.contiguous_block_count != 10
        or folds.validation_test_fold_count != 5
        or folds.launch_time_embargo_seconds != 660
        or not folds.chronological
        or not folds.whole_token
        or folds.threshold_fit_scope != "purged_training_mints_only"
        or folds.selection_scope != "validation_only"
        or not folds.commit_before_test_read
    ):
        raise CorrectionV2ContractError("fold policy differs from the unchanged v1 contract")
    latency_obj = _mapping(_required(obj, "latency", "evaluation"), "evaluation.latency")
    _keys(
        latency_obj,
        frozenset(
            {
                "decision_to_entry_events",
                "exit_horizons_events",
                "same_phase_required",
                "same_continuity_segment_required",
            }
        ),
        "evaluation.latency",
    )
    exits = _required(latency_obj, "exit_horizons_events", "evaluation.latency")
    if not isinstance(exits, list):
        raise CorrectionV2ContractError("evaluation.latency.exit_horizons_events must be an array")
    exit_horizons = tuple(
        _int(item, f"evaluation.latency.exit_horizons_events[{i}]", minimum=1)
        for i, item in enumerate(exits)
    )
    latency = LatencyPolicy(
        _int(
            _required(latency_obj, "decision_to_entry_events", "evaluation.latency"),
            "evaluation.latency.decision_to_entry_events",
            minimum=0,
        ),
        exit_horizons,
        _bool(
            _required(latency_obj, "same_phase_required", "evaluation.latency"),
            "evaluation.latency.same_phase_required",
        ),
        _bool(
            _required(latency_obj, "same_continuity_segment_required", "evaluation.latency"),
            "evaluation.latency.same_continuity_segment_required",
        ),
    )
    if latency != LatencyPolicy(2, (20, 50), True, True):
        raise CorrectionV2ContractError("latency policy differs from the unchanged v1 contract")
    costs_obj = _mapping(_required(obj, "costs", "evaluation"), "evaluation.costs")
    _keys(
        costs_obj,
        frozenset(
            {
                "screening_notional_quote",
                "max_reserve_fraction",
                "fixed_cost_quote_per_leg",
                "adverse_selection_bps",
                "observed_fee_required",
                "fallback_fee_allowed",
            }
        ),
        "evaluation.costs",
    )
    costs = CostPolicy(
        _number(
            _required(costs_obj, "screening_notional_quote", "evaluation.costs"),
            "evaluation.costs.screening_notional_quote",
            minimum=0.0,
        ),
        _number(
            _required(costs_obj, "max_reserve_fraction", "evaluation.costs"),
            "evaluation.costs.max_reserve_fraction",
            minimum=0.0,
        ),
        _number(
            _required(costs_obj, "fixed_cost_quote_per_leg", "evaluation.costs"),
            "evaluation.costs.fixed_cost_quote_per_leg",
            minimum=0.0,
        ),
        _number(
            _required(costs_obj, "adverse_selection_bps", "evaluation.costs"),
            "evaluation.costs.adverse_selection_bps",
            minimum=0.0,
        ),
        _bool(
            _required(costs_obj, "observed_fee_required", "evaluation.costs"),
            "evaluation.costs.observed_fee_required",
        ),
        _bool(
            _required(costs_obj, "fallback_fee_allowed", "evaluation.costs"),
            "evaluation.costs.fallback_fee_allowed",
        ),
    )
    if costs != CostPolicy(0.1, 0.0025, 0.00016, 30.0, True, False):
        raise CorrectionV2ContractError(
            "execution cost policy differs from the unchanged v1 contract"
        )
    wallet_obj = _mapping(_required(obj, "wallet", "evaluation"), "evaluation.wallet")
    _keys(
        wallet_obj,
        frozenset(
            {
                "starting_usd",
                "allocation_percent",
                "max_concurrent_positions",
                "one_position_per_mint",
                "insufficient_cash_rejected",
                "no_negative_equity",
                "causal_fx_series_required",
                "final_sol_usd_requirements_unchanged",
            }
        ),
        "evaluation.wallet",
    )
    wallet = WalletPolicy(
        _decimal(
            _required(wallet_obj, "starting_usd", "evaluation.wallet"),
            "evaluation.wallet.starting_usd",
        ),
        _int(
            _required(wallet_obj, "allocation_percent", "evaluation.wallet"),
            "evaluation.wallet.allocation_percent",
            minimum=1,
        ),
        _int(
            _required(wallet_obj, "max_concurrent_positions", "evaluation.wallet"),
            "evaluation.wallet.max_concurrent_positions",
            minimum=1,
        ),
        _bool(
            _required(wallet_obj, "one_position_per_mint", "evaluation.wallet"),
            "evaluation.wallet.one_position_per_mint",
        ),
        _bool(
            _required(wallet_obj, "insufficient_cash_rejected", "evaluation.wallet"),
            "evaluation.wallet.insufficient_cash_rejected",
        ),
        _bool(
            _required(wallet_obj, "no_negative_equity", "evaluation.wallet"),
            "evaluation.wallet.no_negative_equity",
        ),
        _bool(
            _required(wallet_obj, "causal_fx_series_required", "evaluation.wallet"),
            "evaluation.wallet.causal_fx_series_required",
        ),
        _bool(
            _required(wallet_obj, "final_sol_usd_requirements_unchanged", "evaluation.wallet"),
            "evaluation.wallet.final_sol_usd_requirements_unchanged",
        ),
    )
    if wallet != WalletPolicy(Decimal("10"), 20, 5, True, True, True, True, True):
        raise CorrectionV2ContractError("wallet policy differs from the unchanged v1 contract")
    return EvaluationPolicy(folds, latency, costs, wallet)


def load_contract(path: str | Path = CONFIG_PATH) -> CorrectionV2Contract:
    """Load and validate the canonical correction-v2 contract.

    Validation is deliberately performed before returning any contract
    object.  Duplicate keys, noncanonical JSON, unknown fields, registry
    drift, candidate-order drift, z-threshold drift, and policy changes all
    raise :class:`CorrectionV2ContractError`.
    """

    config_path = Path(path)
    try:
        raw = config_path.read_bytes()
    except OSError as error:
        raise CorrectionV2ContractError(f"cannot read contract {config_path}") from error
    try:
        parsed = json.loads(
            raw.decode("utf-8"),
            parse_constant=lambda value: (_ for _ in ()).throw(
                CorrectionV2ContractError(f"non-finite JSON constant {value}")
            ),
            object_pairs_hook=_canonical_object_pairs,
        )
    except CorrectionV2ContractError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CorrectionV2ContractError("contract is not valid UTF-8 JSON") from error
    if raw != canonical_json(parsed):
        raise CorrectionV2ContractError("contract JSON is not canonical")
    root = _mapping(parsed, "contract")
    _keys(
        root,
        frozenset(
            {
                "kind",
                "version",
                "experiment_id",
                "correction",
                "amendment",
                "development",
                "holdout",
                "families",
                "evaluation",
                "candidate_total",
                "corrected_outcomes_read_before_sealing",
            }
        ),
        "contract",
    )
    correction_obj = _mapping(_required(root, "correction", "contract"), "correction")
    _keys(correction_obj, frozenset({"id", "kind", "version"}), "correction")
    amendment = _load_amendment(_required(root, "amendment", "contract"))
    development = _interval(_required(root, "development", "contract"), "development")
    holdout = _load_holdout(_required(root, "holdout", "contract"))
    family_values = _required(root, "families", "contract")
    if not isinstance(family_values, list) or len(family_values) != 4:
        raise CorrectionV2ContractError("contract.families must contain exactly four entries")
    source_specs = _family_specs()
    families = tuple(
        _load_family(value, i, source_specs[i]) for i, value in enumerate(family_values)
    )
    evaluation = _load_evaluation(_required(root, "evaluation", "contract"))
    result = CorrectionV2Contract(
        kind=_text(_required(root, "kind", "contract"), "contract.kind"),
        version=_int(_required(root, "version", "contract"), "contract.version"),
        experiment_id=_text(_required(root, "experiment_id", "contract"), "contract.experiment_id"),
        correction_id=_text(_required(correction_obj, "id", "correction"), "correction.id"),
        correction_kind=_text(_required(correction_obj, "kind", "correction"), "correction.kind"),
        correction_version=_int(
            _required(correction_obj, "version", "correction"), "correction.version"
        ),
        amendment=amendment,
        development=development,
        holdout=holdout,
        families=families,
        evaluation=evaluation,
        candidate_total=_int(
            _required(root, "candidate_total", "contract"), "contract.candidate_total", minimum=1
        ),
        corrected_outcomes_read_before_sealing=_bool(
            _required(root, "corrected_outcomes_read_before_sealing", "contract"),
            "contract.corrected_outcomes_read_before_sealing",
        ),
    )
    if result.kind != CONTRACT_KIND or result.version != CONTRACT_VERSION:
        raise CorrectionV2ContractError(
            "contract kind/version differs from the canonical constants"
        )
    if (
        result.correction_id != CORRECTION_ID
        or result.correction_kind != CORRECTION_KIND
        or result.correction_version != CONTRACT_VERSION
    ):
        raise CorrectionV2ContractError("correction identity differs from the canonical constants")
    family_ids = tuple(family.id for family in result.families)
    expected_ids = tuple(source_id for source_id, _candidates, _payload in source_specs)
    if family_ids != expected_ids:
        raise CorrectionV2ContractError("family order differs from the canonical order")
    for family in result.families:
        if family.kind != _family_kind(family.id):
            raise CorrectionV2ContractError(f"{family.id} has an invalid correction kind")
        if family.v1_kind != _v1_kind(family.id):
            raise CorrectionV2ContractError(f"{family.id} has an invalid v1 registry kind")
    return result


load_correction_v2_contract = load_contract


__all__ = [
    "AMENDMENT_PATH",
    "CONFIG_PATH",
    "CONTRACT_KIND",
    "CONTRACT_VERSION",
    "CORRECTION_ID",
    "CORRECTION_KIND",
    "AmendmentBinding",
    "CandidateRef",
    "CorrectionV2Contract",
    "CorrectionV2ContractError",
    "CostPolicy",
    "EvaluationPolicy",
    "FamilyContract",
    "FoldPolicy",
    "HoldoutLock",
    "LatencyPolicy",
    "SlotInterval",
    "WalletPolicy",
    "canonical_json",
    "load_contract",
    "load_correction_v2_contract",
]
