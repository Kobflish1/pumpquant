"""Fresh-development individual event research registry.

This is the first expanded *individual* search after the four compact v1
families.  It reuses the source-backed axes in :mod:`pumpquant.research.event_research`
but limits them to measurements that can be reconstructed causally from the
sealed fresh feature root.  No combinations are present here: every candidate
is one scalar signal, one polarity, one train-fitted quantile, and one holding
horizon.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict

from pumpquant.features.alpha import HAWKES_URL, INTRADAY_MOMENTUM_URL
from pumpquant.research.event_research import (
    CONCRETUM_MOMENTUM_URL,
    DEFAULT_EVENT_RESEARCH_AXES,
    HIGH_QUANTILES,
    LOW_QUANTILES,
    EventResearchAxis,
    EventResearchCandidate,
    default_event_research_candidates,
)
from pumpquant.research.event_screen import EventRuleOperator

EXPERIMENT_ID = "fresh-expanded-individual-v1"
PRIOR_ATTEMPTED_CANDIDATES = 1_239
HOLDING_EVENTS = (20, 50)

# These axes are restricted to causal event/reserve quantities
# present in the completed fresh projection or derived by the runner.  Wallet,
# social, Mayhem, and global-concurrency fields are not silently imputed into
# this run; they require a separate enriched projection and contract.
SUPPORTED_DEFAULT_FEATURES = frozenset(
    {
        "event_signed_quote_imbalance_5e",
        "event_signed_quote_imbalance_20e",
        "event_signed_flow_acceleration_5e_20e",
        "event_depth_normalized_signed_flow_20e",
        "event_signed_intensity_pressure_5e_20e",
        "event_reserve_momentum_5e",
        "event_reserve_momentum_20e",
        "event_reserve_momentum_acceleration_5e_20e",
        "event_signed_trend_efficiency_20e",
        "event_distance_from_high_50e",
        "event_rolling_vwap_deviation_20e",
        "event_signed_volume_pressure_5e_50e",
    }
)


def _custom_pair(
    rank: int,
    family: str,
    feature: str,
    source: str,
    rationale: str,
) -> tuple[EventResearchAxis, EventResearchAxis]:
    return (
        EventResearchAxis(
            rank=rank,
            family=f"{family}_continuation",
            feature=feature,
            operator=EventRuleOperator.GREATER_EQUAL,
            quantiles=HIGH_QUANTILES,
            primary_source=source,
            rationale=rationale,
            evidence_scope="transfer_methodology",
        ),
        EventResearchAxis(
            rank=rank,
            family=f"{family}_reversal",
            feature=feature,
            operator=EventRuleOperator.LESS_EQUAL,
            quantiles=LOW_QUANTILES,
            primary_source=source,
            rationale=rationale,
            evidence_scope="transfer_methodology",
        ),
    )


CUSTOM_AXES = (
    *_custom_pair(
        2,
        "arrival_duration_ratio",
        "event_duration_ratio_20e_5e",
        HAWKES_URL,
        (
            "Recent inter-arrival compression or relaxation is a causal intensity "
            "state; it is not a clock-session assumption."
        ),
    ),
    *_custom_pair(
        2,
        "quote_volume_ratio",
        "event_quote_volume_ratio_5e_50e",
        CONCRETUM_MOMENTUM_URL,
        (
            "Short-versus-long quote activity is a launch-relative volume-surprise "
            "analogue, tested after exact costs."
        ),
    ),
    *_custom_pair(
        2,
        "volatility_ratio",
        "event_volatility_ratio_5e_50e",
        INTRADAY_MOMENTUM_URL,
        (
            "Short-versus-long realized reserve-mark volatility is a state filter, "
            "not a claim that volatility itself predicts direction."
        ),
    ),
)


FRESH_AXES: tuple[EventResearchAxis, ...] = tuple(
    sorted(
        (
            *(
                axis
                for axis in DEFAULT_EVENT_RESEARCH_AXES
                if axis.feature in SUPPORTED_DEFAULT_FEATURES
            ),
            *CUSTOM_AXES,
        ),
        key=lambda axis: (axis.rank, axis.family),
    )
)


def candidates() -> tuple[EventResearchCandidate, ...]:
    result = default_event_research_candidates(HOLDING_EVENTS, FRESH_AXES)
    if len(result) != 240 or len({item.sha256 for item in result}) != len(result):
        raise AssertionError("fresh expanded candidate registry geometry is not frozen")
    return result


def candidate_order_sha256() -> str:
    return hashlib.sha256("\n".join(item.sha256 for item in candidates()).encode()).hexdigest()


def trial_denominator() -> int:
    return PRIOR_ATTEMPTED_CANDIDATES + len(candidates())


def registry_payload() -> dict[str, object]:
    values = candidates()
    return {
        "kind": "pumpquant.fresh_expanded_individual_registry",
        "version": 1,
        "experiment_id": EXPERIMENT_ID,
        "prior_attempted_candidate_count": PRIOR_ATTEMPTED_CANDIDATES,
        "candidate_count": len(values),
        "candidate_order_sha256": candidate_order_sha256(),
        "cumulative_trial_denominator": trial_denominator(),
        "holding_events": list(HOLDING_EVENTS),
        "axes": [asdict(axis) for axis in FRESH_AXES],
        "candidates": [
            {
                "sha256": item.sha256,
                "name": item.name,
                "axis": asdict(item.axis),
                "quantile": item.quantile,
                "holding_events": item.holding_events,
            }
            for item in values
        ],
        "outcome_policy": {
            "thresholds": "train-only DuckDB quantile_cont per chronological fold",
            "screen": (
                "normalized float AMM labels with observed fees, 30 bps adverse "
                "selection, and 0.1 SOL notional"
            ),
            "promotion": (
                "repeatable positive OOS only; exact integer/shared-$10 gates remain separate"
            ),
            "combinations": "locked until genuine individual survivors exist",
            "holdout_accessed": False,
        },
    }


_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def create_fresh_raw_adapter_sql(
    *, input_table: str = "raw_features", output_view: str = "raw_source_events"
) -> str:
    """Adapt the sealed minimal feature projection to the research view.

    The adapter only derives prefix information from the current row and its
    causal predecessor.  Fields that are not present in the minimal fresh
    projection (wallet/global/social state) are NULLs, while
    ``first_seen_censored`` is true so no candidate can silently treat missing
    participant history as clean history.
    """

    if not _IDENTIFIER.fullmatch(input_table) or not _IDENTIFIER.fullmatch(output_view):
        raise ValueError("unsafe fresh event research SQL identifier")
    null_fields = (
        "base_size_fraction",
        "bonding_progress",
        "creator_dump_pressure",
        "creator_net_sell_share",
        "fee_excluded_directional_impact_bps",
        "global_activity_ratio_1s_30s",
        "global_signed_quote_imbalance_30s",
        "market_quote_share_global_5s",
        "new_wallet_flow_share",
        "repeat_wallet_flow_share",
        "quote_size_fraction",
        "real_to_virtual_base_ratio",
        "real_to_virtual_quote_ratio",
        "reserve_k_change",
        "unknown_trader_flow_share",
        "vpin",
        "wallet_entropy",
        "wallet_hhi",
        "wallet_top1_share",
    )
    null_sql = ",\n        ".join(f"CAST(NULL AS DOUBLE) AS {name}" for name in null_fields)
    order = "slot, tx_index, log_index, event_index, event_id"
    return f"""
CREATE VIEW {output_view} AS
WITH ordered AS (
    SELECT
        raw.*,
        lag(available_at_ns) OVER market_window AS prior_available_at_ns,
        lag(post_spot_price) OVER market_window AS prior_post_spot_price,
        lag(phase) OVER market_window AS prior_phase
    FROM {input_table} AS raw
    WINDOW market_window AS (PARTITION BY market_id ORDER BY {order})
)
SELECT
    ordered.*,
    effective_pre_base_atomic AS pre_base_reserve_atomic,
    effective_pre_quote_atomic AS pre_quote_reserve_atomic,
    CASE
        WHEN prior_available_at_ns IS NOT NULL
          AND phase = prior_phase
          AND NOT coverage_gap
          AND reserve_continuity
          AND available_at_ns >= prior_available_at_ns
        THEN CAST(available_at_ns - prior_available_at_ns AS DOUBLE) / 1000000000.0
    END AS seconds_since_previous_event,
    CASE
        WHEN prior_post_spot_price IS NOT NULL
          AND prior_post_spot_price > 0.0
          AND post_spot_price IS NOT NULL
          AND post_spot_price > 0.0
          AND phase = prior_phase
          AND NOT coverage_gap
          AND reserve_continuity
        THEN post_spot_price / prior_post_spot_price - 1.0
    END AS reserve_mark_return,
    TRUE AS first_seen_censored,
    {null_sql},
    CAST(NULL AS DOUBLE) AS global_signed_quote_imbalance_5s,
    CAST(NULL AS DOUBLE) AS hawkes_signed_quote_pre
FROM ordered
""".strip()


__all__ = [
    "CUSTOM_AXES",
    "EXPERIMENT_ID",
    "FRESH_AXES",
    "HOLDING_EVENTS",
    "PRIOR_ATTEMPTED_CANDIDATES",
    "SUPPORTED_DEFAULT_FEATURES",
    "candidate_order_sha256",
    "candidates",
    "create_fresh_raw_adapter_sql",
    "registry_payload",
    "trial_denominator",
]
