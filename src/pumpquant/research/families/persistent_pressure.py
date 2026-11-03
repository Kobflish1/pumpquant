"""Preregistered persistent-pressure candidate registry for fresh Pump data.

This family transfers one narrow market-microstructure hypothesis: persistent
signed flow can predict short-horizon price pressure, but the signal should be
expressed in a state-aware way for an AMM.  It therefore compares normalized
signed quote flow with reserve-price impact space.  The registry contains no
outcomes, fitted thresholds, winner selection, or holdout access.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from enum import StrEnum
from statistics import NormalDist

CONT_OFI_URL = "https://arxiv.org/abs/1011.6402"
LILLO_FARMER_URL = "https://arxiv.org/abs/cond-mat/0311053"
AMM_IMPACT_SPACE_URL = "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6145286"
PUMP_PROGRAM_DOCS_URL = (
    "https://github.com/pump-fun/pump-public-docs/blob/main/docs/PUMP_PROGRAM_README.md"
)

LOOKBACK_EVENTS = (5, 20)
PRESSURE_QUANTILES = (0.75, 0.90)
CONFIRMATION_QUANTILES = (0.60, 0.75)
HOLDING_EVENTS = (20, 50)
PRIOR_ATTEMPTED_CANDIDATES = 1_175


class PressureMeasure(StrEnum):
    PRICE = "price"
    FLOW = "flow"


@dataclass(frozen=True, slots=True)
class PersistentPressureCandidate:
    """One stable member of the frozen 32-candidate family."""

    measure: PressureMeasure
    lookback_events: int
    pressure_quantile: float
    confirmation_quantile: float
    holding_events: int

    def __post_init__(self) -> None:
        if not isinstance(self.measure, PressureMeasure):
            raise ValueError("unregistered pressure measure")
        if self.lookback_events not in LOOKBACK_EVENTS:
            raise ValueError("unregistered lookback")
        if self.pressure_quantile not in PRESSURE_QUANTILES:
            raise ValueError("unregistered pressure quantile")
        if self.confirmation_quantile not in CONFIRMATION_QUANTILES:
            raise ValueError("unregistered confirmation quantile")
        if self.holding_events not in HOLDING_EVENTS:
            raise ValueError("unregistered holding horizon")

    @property
    def pressure_feature(self) -> str:
        return f"shock_{self.measure.value}_{self.lookback_events}e"

    @property
    def confirmation_feature(self) -> str:
        return f"recovery_{self.measure.value}_2e"

    @property
    def name(self) -> str:
        return (
            "persistent-pressure:"
            f"{self.measure.value}-{self.lookback_events}e:"
            f"upper-q{self.pressure_quantile:g}:"
            f"confirm-q{self.confirmation_quantile:g}:"
            f"hold-{self.holding_events}"
        )

    @property
    def sha256(self) -> str:
        payload = {
            "confirmation_quantile": self.confirmation_quantile,
            "holding_events": self.holding_events,
            "lookback_events": self.lookback_events,
            "measure": self.measure.value,
            "pressure_quantile": self.pressure_quantile,
            "version": 1,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


def default_persistent_pressure_candidates() -> tuple[PersistentPressureCandidate, ...]:
    """Return the exact deterministic 32-candidate preregistered grid."""

    candidates = tuple(
        PersistentPressureCandidate(
            measure=measure,
            lookback_events=lookback,
            pressure_quantile=pressure_quantile,
            confirmation_quantile=confirmation_quantile,
            holding_events=holding,
        )
        for holding in HOLDING_EVENTS
        for lookback in LOOKBACK_EVENTS
        for measure in PressureMeasure
        for pressure_quantile in PRESSURE_QUANTILES
        for confirmation_quantile in CONFIRMATION_QUANTILES
    )
    if len(candidates) != 32 or len({item.sha256 for item in candidates}) != 32:
        raise AssertionError("persistent-pressure registry geometry is not exact")
    return candidates


def cumulative_trial_count() -> int:
    return PRIOR_ATTEMPTED_CANDIDATES + len(default_persistent_pressure_candidates())


def bonferroni_z_threshold(alpha: float = 0.05) -> float:
    if not math.isfinite(alpha) or not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be finite and inside (0, 1)")
    return NormalDist().inv_cdf(1.0 - alpha / cumulative_trial_count())


def registry_payload() -> dict[str, object]:
    """Return the frozen registry without fitted cutoffs or results."""

    candidates = default_persistent_pressure_candidates()
    return {
        "candidate_count": len(candidates),
        "candidate_order_sha256": hashlib.sha256(
            "\n".join(item.sha256 for item in candidates).encode()
        ).hexdigest(),
        "candidates": [
            {
                **asdict(item),
                "measure": item.measure.value,
                "pressure_feature": item.pressure_feature,
                "confirmation_feature": item.confirmation_feature,
                "name": item.name,
                "sha256": item.sha256,
            }
            for item in candidates
        ],
        "cumulative_trial_count": cumulative_trial_count(),
        "kind": "pumpquant.persistent_pressure_registry",
        "prior_attempted_candidate_count": PRIOR_ATTEMPTED_CANDIDATES,
        "sources": [
            CONT_OFI_URL,
            LILLO_FARMER_URL,
            AMM_IMPACT_SPACE_URL,
            PUMP_PROGRAM_DOCS_URL,
        ],
        "version": 1,
        "z_threshold": bonferroni_z_threshold(),
    }


__all__ = [
    "CONFIRMATION_QUANTILES",
    "HOLDING_EVENTS",
    "LOOKBACK_EVENTS",
    "PRESSURE_QUANTILES",
    "PRIOR_ATTEMPTED_CANDIDATES",
    "PersistentPressureCandidate",
    "PressureMeasure",
    "bonferroni_z_threshold",
    "cumulative_trial_count",
    "default_persistent_pressure_candidates",
    "registry_payload",
]
