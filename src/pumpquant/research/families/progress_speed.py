"""Causal bonding-curve progress-speed registry for fresh-data research.

The Pump.fun literature reports that, conditional on reaching the same
bonding-curve state, tokens that get there in fewer swaps are more likely to
graduate.  This module turns that structural observation into a small,
falsifiable *trading* family.  It defines stable candidate identities and a
past-only first-crossing view; it does not inspect outcomes, select a winner,
or authorize chronological-holdout access.

The checkpoint state is net quote reserve accumulated since the first
continuous bonding-curve event.  It is deliberately measured from executable
reserve states rather than from a guessed protocol constant.  Only continuity
segment zero is eligible, so a mid-history gap cannot masquerade as launch.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from enum import StrEnum
from statistics import NormalDist

PUMP_PREDICTION_URL = "https://arxiv.org/abs/2602.14860"
PUMP_PROGRAM_DOCS_URL = (
    "https://github.com/pump-fun/pump-public-docs/blob/main/docs/PUMP_PROGRAM_README.md"
)
CONT_OFI_URL = "https://arxiv.org/abs/1011.6402"

QUOTE_CHECKPOINTS = (1.0, 5.0, 15.0, 30.0)
SPEED_QUANTILES = (0.25, 0.10)
HOLDING_EVENTS = (20, 50)
PRIOR_ATTEMPTED_CANDIDATES = 1_143


class SpeedMeasure(StrEnum):
    EVENT_COUNT = "event_count"
    ELAPSED_SECONDS = "elapsed_seconds"


@dataclass(frozen=True, slots=True)
class ProgressSpeedCandidate:
    """One stable member of the preregistered 32-candidate family."""

    quote_checkpoint: float
    speed_measure: SpeedMeasure
    speed_quantile: float
    holding_events: int

    def __post_init__(self) -> None:
        if self.quote_checkpoint not in QUOTE_CHECKPOINTS:
            raise ValueError("unregistered quote checkpoint")
        if self.speed_quantile not in SPEED_QUANTILES:
            raise ValueError("unregistered speed quantile")
        if self.holding_events not in HOLDING_EVENTS:
            raise ValueError("unregistered holding horizon")
        if not isinstance(self.speed_measure, SpeedMeasure):
            raise ValueError("unregistered speed measure")

    @property
    def speed_feature(self) -> str:
        if self.speed_measure is SpeedMeasure.EVENT_COUNT:
            return "events_to_cross"
        return "seconds_to_cross"

    @property
    def name(self) -> str:
        return (
            "progress-speed:"
            f"checkpoint-{self.quote_checkpoint:g}-quote:"
            f"{self.speed_measure.value}:lower-q{self.speed_quantile:g}:"
            f"hold-{self.holding_events}"
        )

    @property
    def sha256(self) -> str:
        payload = {
            "holding_events": self.holding_events,
            "quote_checkpoint": self.quote_checkpoint,
            "speed_measure": self.speed_measure.value,
            "speed_quantile": self.speed_quantile,
            "version": 1,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


def default_progress_speed_candidates() -> tuple[ProgressSpeedCandidate, ...]:
    """Return the exact deterministic 32-candidate preregistered grid."""

    result = tuple(
        ProgressSpeedCandidate(
            quote_checkpoint=checkpoint,
            speed_measure=measure,
            speed_quantile=quantile,
            holding_events=holding,
        )
        for holding in HOLDING_EVENTS
        for checkpoint in QUOTE_CHECKPOINTS
        for measure in SpeedMeasure
        for quantile in SPEED_QUANTILES
    )
    if len(result) != 32 or len({item.sha256 for item in result}) != len(result):
        raise AssertionError("progress-speed registry geometry is not exact")
    return result


def cumulative_trial_count() -> int:
    return PRIOR_ATTEMPTED_CANDIDATES + len(default_progress_speed_candidates())


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


def create_progress_crossing_source_sql(
    *,
    input_table: str = "raw_source_events",
    output_view: str = "progress_crossings",
) -> str:
    """Create one causal first-crossing row per market and quote checkpoint.

    The input is the sealed shock-recovery feature projection.  The view uses
    only bonding-curve rows in the original, gap-free segment.  It derives net
    quote accumulation from reserve states and never reads a later graduation
    label.  The first-crossing decision row itself is observable; any backtest
    must still impose its declared entry latency.
    """

    source = _identifier(input_table)
    output = _identifier(output_view)
    order = "slot, tx_index, log_index, event_index, event_id"
    checkpoint_sql = ", ".join(f"({value!r})" for value in QUOTE_CHECKPOINTS)
    return f"""
CREATE VIEW {output} AS
WITH eligible AS (
    SELECT
        raw.*,
        row_number() OVER (
            PARTITION BY market_id, continuity_segment ORDER BY {order}
        ) AS events_to_cross,
        first_value(effective_pre_quote_atomic) OVER (
            PARTITION BY market_id, continuity_segment ORDER BY {order}
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS launch_pre_quote_atomic
    FROM {source} AS raw
    WHERE phase = 'bonding_curve'
      AND continuity_segment = 0
      AND NOT coverage_gap
      AND reserve_continuity
), path AS (
    SELECT
        eligible.*,
        (CAST(post_quote_reserve_atomic AS DOUBLE)
         - CAST(launch_pre_quote_atomic AS DOUBLE))
        / pow(10.0, CAST(quote_decimals AS DOUBLE)) AS net_quote_added,
        CAST(block_time_ns - launch_time_ns AS DOUBLE) / 1000000000.0
            AS seconds_to_cross
    FROM eligible
    WHERE launch_pre_quote_atomic IS NOT NULL
      AND post_quote_reserve_atomic IS NOT NULL
      AND block_time_ns >= launch_time_ns
), checkpoints(quote_checkpoint) AS (
    VALUES {checkpoint_sql}
), crossed AS (
    SELECT path.*, checkpoints.quote_checkpoint
    FROM path
    CROSS JOIN checkpoints
    WHERE path.net_quote_added >= checkpoints.quote_checkpoint
      AND path.seconds_to_cross >= 0.0
)
SELECT *
FROM crossed
QUALIFY row_number() OVER (
    PARTITION BY market_id, quote_checkpoint ORDER BY {order}
) = 1
""".strip()


def registry_payload() -> dict[str, object]:
    """Return the frozen registry without fitted cutoffs or results."""

    candidates = default_progress_speed_candidates()
    return {
        "candidate_count": len(candidates),
        "candidate_order_sha256": hashlib.sha256(
            "\n".join(item.sha256 for item in candidates).encode()
        ).hexdigest(),
        "candidates": [
            {
                **asdict(item),
                "speed_measure": item.speed_measure.value,
                "speed_feature": item.speed_feature,
                "name": item.name,
                "sha256": item.sha256,
            }
            for item in candidates
        ],
        "cumulative_trial_count": cumulative_trial_count(),
        "kind": "pumpquant.progress_speed_registry",
        "prior_attempted_candidate_count": PRIOR_ATTEMPTED_CANDIDATES,
        "sources": [PUMP_PREDICTION_URL, PUMP_PROGRAM_DOCS_URL, CONT_OFI_URL],
        "version": 1,
        "z_threshold": bonferroni_z_threshold(),
    }


__all__ = [
    "HOLDING_EVENTS",
    "PRIOR_ATTEMPTED_CANDIDATES",
    "QUOTE_CHECKPOINTS",
    "SPEED_QUANTILES",
    "ProgressSpeedCandidate",
    "SpeedMeasure",
    "bonferroni_z_threshold",
    "create_progress_crossing_source_sql",
    "cumulative_trial_count",
    "default_progress_speed_candidates",
    "registry_payload",
]
