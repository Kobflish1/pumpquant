"""Causal sell-shock then recovery registry for fresh-data research.

This module defines measurements and stable candidate identities only.  It
does not evaluate outcomes, select parameters, or authorise holdout access.
Shock and recovery windows are disjoint by construction.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from enum import StrEnum
from statistics import NormalDist

BACRY_MUZY_URL = "https://arxiv.org/abs/1301.1135"
FARMER_IMPACT_URL = "https://arxiv.org/abs/physics/0602015"
PUMP_PREDICTION_URL = "https://arxiv.org/abs/2602.14860"

SHOCK_LENGTHS = (5, 20)
SHOCK_QUANTILES = (0.10, 0.20)
RECOVERY_QUANTILES = (0.60, 0.75)
HOLDING_EVENTS = (20, 50)
PRIOR_ATTEMPTED_CANDIDATES = 951


class ShockMeasure(StrEnum):
    PRICE = "price"
    FLOW = "flow"


class RecoveryMeasure(StrEnum):
    PRICE = "price"
    FLOW = "flow"


class StateProfile(StrEnum):
    NONE = "none"
    LOW_CURRENT_IMPACT = "low_current_impact"
    SELL_EXCITATION_RELAXED = "sell_excitation_relaxed"


@dataclass(frozen=True, slots=True)
class ShockRecoveryCandidate:
    """One stable member of the preregistered 192-candidate family."""

    shock_measure: ShockMeasure
    shock_length: int
    recovery_measure: RecoveryMeasure
    shock_quantile: float
    recovery_quantile: float
    state_profile: StateProfile
    holding_events: int

    def __post_init__(self) -> None:
        if self.shock_length not in SHOCK_LENGTHS:
            raise ValueError("unregistered shock length")
        if self.shock_quantile not in SHOCK_QUANTILES:
            raise ValueError("unregistered shock quantile")
        if self.recovery_quantile not in RECOVERY_QUANTILES:
            raise ValueError("unregistered recovery quantile")
        if self.holding_events not in HOLDING_EVENTS:
            raise ValueError("unregistered holding horizon")

    @property
    def shock_feature(self) -> str:
        return f"shock_{self.shock_measure.value}_{self.shock_length}e"

    @property
    def recovery_feature(self) -> str:
        return f"recovery_{self.recovery_measure.value}_2e"

    @property
    def gate_feature(self) -> str | None:
        if self.state_profile is StateProfile.NONE:
            return None
        if self.state_profile is StateProfile.LOW_CURRENT_IMPACT:
            return "gross_directional_impact_bps"
        return f"sell_excitation_relaxation_{self.shock_length}e"

    @property
    def name(self) -> str:
        return (
            "shock-recovery:"
            f"{self.shock_measure.value}-{self.shock_length}e:"
            f"shock-q{self.shock_quantile:g}:"
            f"recover-{self.recovery_measure.value}-q{self.recovery_quantile:g}:"
            f"gate-{self.state_profile.value}:hold-{self.holding_events}"
        )

    @property
    def sha256(self) -> str:
        payload = {
            "holding_events": self.holding_events,
            "recovery_measure": self.recovery_measure.value,
            "recovery_quantile": self.recovery_quantile,
            "shock_length": self.shock_length,
            "shock_measure": self.shock_measure.value,
            "shock_quantile": self.shock_quantile,
            "state_profile": self.state_profile.value,
            "version": 1,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


def default_shock_recovery_candidates() -> tuple[ShockRecoveryCandidate, ...]:
    """Return the exact deterministic 192-candidate preregistered Cartesian grid."""

    result = tuple(
        ShockRecoveryCandidate(
            shock_measure=shock_measure,
            shock_length=shock_length,
            recovery_measure=recovery_measure,
            shock_quantile=shock_quantile,
            recovery_quantile=recovery_quantile,
            state_profile=state_profile,
            holding_events=holding_events,
        )
        for holding_events in HOLDING_EVENTS
        for shock_measure in ShockMeasure
        for shock_length in SHOCK_LENGTHS
        for recovery_measure in RecoveryMeasure
        for shock_quantile in SHOCK_QUANTILES
        for recovery_quantile in RECOVERY_QUANTILES
        for state_profile in StateProfile
    )
    if len(result) != 192 or len({item.sha256 for item in result}) != len(result):
        raise AssertionError("shock-recovery registry geometry is not exact")
    return result


def cumulative_trial_count() -> int:
    return PRIOR_ATTEMPTED_CANDIDATES + len(default_shock_recovery_candidates())


def bonferroni_z_threshold(alpha: float = 0.05) -> float:
    """Compute the frozen one-sided family-wise normal threshold."""

    if not math.isfinite(alpha) or not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be finite and inside (0, 1)")
    return NormalDist().inv_cdf(1.0 - alpha / cumulative_trial_count())


_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier {value!r}")
    return value


def create_shock_recovery_source_sql(
    *,
    input_table: str = "raw_source_events",
    output_view: str = "shock_recovery_source",
) -> str:
    """Create phase/gap-safe shock and recovery measurements in DuckDB.

    All windows end at the decision event or earlier.  In particular, the
    shock windows end two events before the decision while recovery uses only
    the last two landed events.
    """

    source = _identifier(input_table)
    output = _identifier(output_view)
    order = "slot, tx_index, log_index, event_index, event_id"
    segment = f"market_id, shock_continuity_segment ORDER BY {order}"
    return f"""
CREATE VIEW {output} AS
WITH marked AS (
    SELECT
        raw.*,
        lag(phase) OVER (PARTITION BY market_id ORDER BY {order}) AS shock_prior_phase,
        CASE WHEN side = 'buy' THEN CAST(quote_amount_atomic AS DOUBLE)
             WHEN side = 'sell' THEN -CAST(quote_amount_atomic AS DOUBLE)
             ELSE NULL END AS shock_signed_quote_atomic,
        CASE WHEN coverage_gap OR NOT reserve_continuity
                  OR lag(phase) OVER (PARTITION BY market_id ORDER BY {order}) IS NULL
                  OR phase <> lag(phase) OVER (PARTITION BY market_id ORDER BY {order})
             THEN 1 ELSE 0 END AS shock_segment_start
    FROM {source} AS raw
), segmented AS (
    SELECT
        *,
        sum(shock_segment_start) OVER (
            PARTITION BY market_id ORDER BY {order}
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS shock_continuity_segment
    FROM marked
), windowed AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY {segment}) AS shock_segment_event_number,
        lag(post_spot_price, 2) OVER segment_window AS shock_end_price,
        lag(post_spot_price, 7) OVER segment_window AS shock_start_price_5e,
        lag(post_spot_price, 22) OVER segment_window AS shock_start_price_20e,
        count(*) OVER shock5_window AS shock_count_5e,
        count(*) OVER shock20_window AS shock_count_20e,
        sum(shock_signed_quote_atomic) OVER shock5_window AS shock_signed_quote_5e,
        sum(shock_signed_quote_atomic) OVER shock20_window AS shock_signed_quote_20e,
        sum(CAST(quote_amount_atomic AS DOUBLE)) OVER shock5_window
            AS shock_total_quote_5e,
        sum(CAST(quote_amount_atomic AS DOUBLE)) OVER shock20_window
            AS shock_total_quote_20e,
        max(hawkes_sell_pre) OVER shock5_window AS shock_sell_hawkes_peak_5e,
        max(hawkes_sell_pre) OVER shock20_window AS shock_sell_hawkes_peak_20e,
        count(*) OVER recovery_window AS recovery_count_2e,
        sum(shock_signed_quote_atomic) OVER recovery_window AS recovery_signed_quote_2e,
        sum(CAST(quote_amount_atomic AS DOUBLE)) OVER recovery_window
            AS recovery_total_quote_2e
    FROM segmented
    WINDOW
        segment_window AS (PARTITION BY {segment}),
        shock5_window AS (
            PARTITION BY {segment} ROWS BETWEEN 6 PRECEDING AND 2 PRECEDING
        ),
        shock20_window AS (
            PARTITION BY {segment} ROWS BETWEEN 21 PRECEDING AND 2 PRECEDING
        ),
        recovery_window AS (
            PARTITION BY {segment} ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        )
)
SELECT
    windowed.*,
    CASE WHEN shock_count_5e = 5 AND shock_end_price > 0.0
              AND shock_start_price_5e > 0.0
         THEN ln(shock_end_price / shock_start_price_5e) END AS shock_price_5e,
    CASE WHEN shock_count_20e = 20 AND shock_end_price > 0.0
              AND shock_start_price_20e > 0.0
         THEN ln(shock_end_price / shock_start_price_20e) END AS shock_price_20e,
    CASE WHEN shock_count_5e = 5 AND shock_total_quote_5e > 0.0
         THEN shock_signed_quote_5e / shock_total_quote_5e END AS shock_flow_5e,
    CASE WHEN shock_count_20e = 20 AND shock_total_quote_20e > 0.0
         THEN shock_signed_quote_20e / shock_total_quote_20e END AS shock_flow_20e,
    CASE WHEN recovery_count_2e = 2 AND post_spot_price > 0.0
              AND shock_end_price > 0.0
         THEN ln(post_spot_price / shock_end_price) END AS recovery_price_2e,
    CASE WHEN recovery_count_2e = 2 AND recovery_total_quote_2e > 0.0
         THEN recovery_signed_quote_2e / recovery_total_quote_2e END
         AS recovery_flow_2e,
    CASE WHEN shock_count_5e = 5 AND shock_sell_hawkes_peak_5e > 0.0
         THEN hawkes_sell_pre / shock_sell_hawkes_peak_5e END
         AS sell_excitation_relaxation_5e,
    CASE WHEN shock_count_20e = 20 AND shock_sell_hawkes_peak_20e > 0.0
         THEN hawkes_sell_pre / shock_sell_hawkes_peak_20e END
         AS sell_excitation_relaxation_20e
FROM windowed
""".strip()


def registry_payload() -> dict[str, object]:
    """Return a JSON-serializable registry without fitted thresholds or results."""

    candidates = default_shock_recovery_candidates()
    return {
        "candidate_count": len(candidates),
        "candidate_order_sha256": hashlib.sha256(
            "\n".join(item.sha256 for item in candidates).encode()
        ).hexdigest(),
        "candidates": [
            {
                **asdict(item),
                "shock_measure": item.shock_measure.value,
                "recovery_measure": item.recovery_measure.value,
                "state_profile": item.state_profile.value,
                "name": item.name,
                "sha256": item.sha256,
            }
            for item in candidates
        ],
        "cumulative_trial_count": cumulative_trial_count(),
        "kind": "pumpquant.shock_recovery_registry",
        "prior_attempted_candidate_count": PRIOR_ATTEMPTED_CANDIDATES,
        "sources": [BACRY_MUZY_URL, FARMER_IMPACT_URL, PUMP_PREDICTION_URL],
        "version": 1,
        "z_threshold": bonferroni_z_threshold(),
    }


__all__ = [
    "HOLDING_EVENTS",
    "PRIOR_ATTEMPTED_CANDIDATES",
    "RECOVERY_QUANTILES",
    "SHOCK_LENGTHS",
    "SHOCK_QUANTILES",
    "RecoveryMeasure",
    "ShockMeasure",
    "ShockRecoveryCandidate",
    "StateProfile",
    "bonferroni_z_threshold",
    "create_shock_recovery_source_sql",
    "cumulative_trial_count",
    "default_shock_recovery_candidates",
    "registry_payload",
]
