"""Preregistered AMM impact-space order-flow imbalance family.

Pump and PumpSwap expose deterministic reserve curves, not resting limit-order
queues.  This module therefore translates order-flow imbalance into two
causal AMM quantities:

* user quote flow divided by the quote reserve visible before each event; and
* the event's realised directional execution impact, expressed in return units.

Each quantity is retained both as signed pressure and as a signed/gross
balance.  The registry and SQL contain no outcomes, fitted cutoffs, winners,
or chronological-holdout access.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from enum import StrEnum
from statistics import NormalDist

CONT_OFI_URL = "https://arxiv.org/abs/1011.6402"
INTEGRATED_OFI_URL = "https://arxiv.org/abs/2112.13213"
AMM_IMPACT_SPACE_URL = "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6145286"
AMM_ASYMMETRY_URL = "https://arxiv.org/abs/2603.08603"
PUMP_PROGRAM_DOCS_URL = (
    "https://github.com/pump-fun/pump-public-docs/blob/main/docs/PUMP_PROGRAM_README.md"
)

LOOKBACK_EVENTS = (5, 20)
SIGNAL_QUANTILES = (0.75, 0.90)
HOLDING_EVENTS = (20, 50)
PRIOR_ATTEMPTED_CANDIDATES = 1_207


class AmmOfiMeasure(StrEnum):
    """Frozen, interpretable AMM analogues of order-flow imbalance."""

    DEPTH_PRESSURE = "amm_depth_pressure"
    DEPTH_IMBALANCE = "amm_depth_imbalance"
    IMPACT_PRESSURE = "amm_impact_pressure"
    IMPACT_IMBALANCE = "amm_impact_imbalance"


@dataclass(frozen=True, slots=True)
class AmmImpactOfiCandidate:
    """One stable member of the 32-candidate atomic signal family."""

    measure: AmmOfiMeasure
    lookback_events: int
    signal_quantile: float
    holding_events: int

    def __post_init__(self) -> None:
        if not isinstance(self.measure, AmmOfiMeasure):
            raise ValueError("unregistered AMM OFI measure")
        if self.lookback_events not in LOOKBACK_EVENTS:
            raise ValueError("unregistered AMM OFI lookback")
        if self.signal_quantile not in SIGNAL_QUANTILES:
            raise ValueError("unregistered AMM OFI signal quantile")
        if self.holding_events not in HOLDING_EVENTS:
            raise ValueError("unregistered AMM OFI holding horizon")

    @property
    def signal_feature(self) -> str:
        return f"{self.measure.value}_{self.lookback_events}e"

    @property
    def name(self) -> str:
        return (
            "amm-impact-ofi:"
            f"{self.measure.value}-{self.lookback_events}e:"
            f"upper-q{self.signal_quantile:g}:hold-{self.holding_events}"
        )

    @property
    def sha256(self) -> str:
        payload = {
            "holding_events": self.holding_events,
            "lookback_events": self.lookback_events,
            "measure": self.measure.value,
            "signal_quantile": self.signal_quantile,
            "version": 1,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


def default_amm_impact_ofi_candidates() -> tuple[AmmImpactOfiCandidate, ...]:
    """Return the exact deterministic 32-candidate preregistered grid."""

    candidates = tuple(
        AmmImpactOfiCandidate(
            measure=measure,
            lookback_events=lookback,
            signal_quantile=quantile,
            holding_events=holding,
        )
        for holding in HOLDING_EVENTS
        for lookback in LOOKBACK_EVENTS
        for measure in AmmOfiMeasure
        for quantile in SIGNAL_QUANTILES
    )
    if len(candidates) != 32 or len({item.sha256 for item in candidates}) != 32:
        raise AssertionError("AMM impact OFI registry geometry is not exact")
    return candidates


def cumulative_trial_count() -> int:
    return PRIOR_ATTEMPTED_CANDIDATES + len(default_amm_impact_ofi_candidates())


def bonferroni_z_threshold(alpha: float = 0.05) -> float:
    """Frozen one-sided family-wise normal threshold."""

    if not math.isfinite(alpha) or not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be finite and inside (0, 1)")
    return NormalDist().inv_cdf(1.0 - alpha / cumulative_trial_count())


_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier {value!r}")
    return value


def create_amm_impact_ofi_source_sql(
    *,
    input_table: str = "raw_source_events",
    output_view: str = "amm_impact_ofi_source",
) -> str:
    """Create phase- and gap-safe AMM OFI measurements in DuckDB.

    The quote-depth unit is ``user_quote / causal_pre_quote_reserve``.  The
    impact unit is the event's absolute directional execution impact divided
    by 10,000.  Both are signed by canonical trade direction.  Windows include
    the decision event and contain no following rows; production entry remains
    delayed by two landed events.
    """

    source = _identifier(input_table)
    output = _identifier(output_view)
    order = "slot, tx_index, log_index, event_index, event_id"
    segment = f"market_id, phase, continuity_segment ORDER BY {order}"
    return f"""
CREATE VIEW {output} AS
WITH atomic_units AS (
    SELECT
        raw.*,
        CASE
            WHEN NOT coverage_gap AND reserve_continuity
              AND effective_pre_quote_atomic IS NOT NULL
              AND effective_pre_quote_atomic > 0
              AND quote_amount_atomic > 0
              AND side IN ('buy', 'sell')
            THEN (CASE WHEN side = 'buy' THEN 1.0 ELSE -1.0 END)
                 * CAST(quote_amount_atomic AS DOUBLE)
                 / CAST(effective_pre_quote_atomic AS DOUBLE)
        END AS amm_signed_depth_unit,
        CASE
            WHEN NOT coverage_gap AND reserve_continuity
              AND gross_directional_impact_bps IS NOT NULL
              AND isfinite(gross_directional_impact_bps)
              AND side IN ('buy', 'sell')
            THEN (CASE WHEN side = 'buy' THEN 1.0 ELSE -1.0 END)
                 * abs(gross_directional_impact_bps) / 10000.0
        END AS amm_signed_impact_unit
    FROM {source} AS raw
), windowed AS (
    SELECT
        *,
        count(amm_signed_depth_unit) OVER depth5_window AS amm_depth_count_5e,
        count(amm_signed_depth_unit) OVER depth20_window AS amm_depth_count_20e,
        sum(amm_signed_depth_unit) OVER depth5_window AS amm_depth_net_5e,
        sum(amm_signed_depth_unit) OVER depth20_window AS amm_depth_net_20e,
        sum(abs(amm_signed_depth_unit)) OVER depth5_window AS amm_depth_gross_5e,
        sum(abs(amm_signed_depth_unit)) OVER depth20_window AS amm_depth_gross_20e,
        count(amm_signed_impact_unit) OVER depth5_window AS amm_impact_count_5e,
        count(amm_signed_impact_unit) OVER depth20_window AS amm_impact_count_20e,
        sum(amm_signed_impact_unit) OVER depth5_window AS amm_impact_net_5e,
        sum(amm_signed_impact_unit) OVER depth20_window AS amm_impact_net_20e,
        sum(abs(amm_signed_impact_unit)) OVER depth5_window AS amm_impact_gross_5e,
        sum(abs(amm_signed_impact_unit)) OVER depth20_window AS amm_impact_gross_20e
    FROM atomic_units
    WINDOW
        depth5_window AS (
            PARTITION BY {segment} ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ),
        depth20_window AS (
            PARTITION BY {segment} ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )
)
SELECT
    windowed.*,
    CASE WHEN amm_depth_count_5e = 5
         THEN amm_depth_net_5e END AS amm_depth_pressure_5e,
    CASE WHEN amm_depth_count_20e = 20
         THEN amm_depth_net_20e END AS amm_depth_pressure_20e,
    CASE WHEN amm_depth_count_5e = 5 AND amm_depth_gross_5e > 0.0
         THEN amm_depth_net_5e / amm_depth_gross_5e
         END AS amm_depth_imbalance_5e,
    CASE WHEN amm_depth_count_20e = 20 AND amm_depth_gross_20e > 0.0
         THEN amm_depth_net_20e / amm_depth_gross_20e
         END AS amm_depth_imbalance_20e,
    CASE WHEN amm_impact_count_5e = 5
         THEN amm_impact_net_5e END AS amm_impact_pressure_5e,
    CASE WHEN amm_impact_count_20e = 20
         THEN amm_impact_net_20e END AS amm_impact_pressure_20e,
    CASE WHEN amm_impact_count_5e = 5 AND amm_impact_gross_5e > 0.0
         THEN amm_impact_net_5e / amm_impact_gross_5e
         END AS amm_impact_imbalance_5e,
    CASE WHEN amm_impact_count_20e = 20 AND amm_impact_gross_20e > 0.0
         THEN amm_impact_net_20e / amm_impact_gross_20e
         END AS amm_impact_imbalance_20e
FROM windowed
""".strip()


def registry_payload() -> dict[str, object]:
    """Return the frozen registry without fitted cutoffs or results."""

    candidates = default_amm_impact_ofi_candidates()
    return {
        "candidate_count": len(candidates),
        "candidate_order_sha256": hashlib.sha256(
            "\n".join(item.sha256 for item in candidates).encode()
        ).hexdigest(),
        "candidates": [
            {
                **asdict(item),
                "measure": item.measure.value,
                "signal_feature": item.signal_feature,
                "name": item.name,
                "sha256": item.sha256,
            }
            for item in candidates
        ],
        "cumulative_trial_count": cumulative_trial_count(),
        "kind": "pumpquant.amm_impact_ofi_registry",
        "prior_attempted_candidate_count": PRIOR_ATTEMPTED_CANDIDATES,
        "sources": [
            CONT_OFI_URL,
            INTEGRATED_OFI_URL,
            AMM_IMPACT_SPACE_URL,
            AMM_ASYMMETRY_URL,
            PUMP_PROGRAM_DOCS_URL,
        ],
        "version": 1,
        "z_threshold": bonferroni_z_threshold(),
    }


__all__ = [
    "HOLDING_EVENTS",
    "LOOKBACK_EVENTS",
    "PRIOR_ATTEMPTED_CANDIDATES",
    "SIGNAL_QUANTILES",
    "AmmImpactOfiCandidate",
    "AmmOfiMeasure",
    "bonferroni_z_threshold",
    "create_amm_impact_ofi_source_sql",
    "cumulative_trial_count",
    "default_amm_impact_ofi_candidates",
    "registry_payload",
]
