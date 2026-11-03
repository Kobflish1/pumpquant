"""Fold-calibrated causal event research for Pump/PumpSwap.

The first event screen deliberately used fixed thresholds on already-materialized
features.  This module defines the broader second-stage *atomic* search.  It
adds gap-safe event-window measurements, fits every scalar threshold inside the
training side of each chronological fold, and keeps candidate identity separate
from the fitted threshold value.

None of the cited futures, equity, LOB, or Pump.fun papers is treated as proof
of executable Pump.fun profit.  They motivate falsifiable measurements only.
Literal queue imbalance, spread, microprice, cancellations, and other L2-only
fields are intentionally absent because a constant-product AMM has no order
book.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from numbers import Real

from pumpquant.features.alpha import (
    CFMM_URL,
    CONT_OFI_URL,
    HAWKES_URL,
    INTRADAY_MOMENTUM_URL,
    PUMP_PREDICTION_URL,
    TSMOM_URL,
    VPIN_URL,
)
from pumpquant.research.event_screen import EventCandidateMetrics, EventRuleOperator

OFI_FORECAST_URL = "https://arxiv.org/abs/2408.03594"
COMMON_FLOW_URL = "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6535019"
CONCRETUM_MOMENTUM_URL = "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4824172"

HIGH_QUANTILES = (0.50, 0.70, 0.85, 0.95)
LOW_QUANTILES = (0.50, 0.30, 0.15, 0.05)


@dataclass(frozen=True, slots=True)
class EventResearchAxis:
    """One research-backed scalar measurement and one declared polarity."""

    rank: int
    family: str
    feature: str
    operator: EventRuleOperator
    quantiles: tuple[float, ...]
    primary_source: str
    rationale: str
    evidence_scope: str
    requires_uncensored_wallet_history: bool = False

    def __post_init__(self) -> None:
        if self.rank < 1 or not self.family.strip() or not self.feature.strip():
            raise ValueError("invalid event research axis")
        if not self.quantiles or len(set(self.quantiles)) != len(self.quantiles):
            raise ValueError("research quantiles must be non-empty and unique")
        if any(not math.isfinite(value) or not 0.0 < value < 1.0 for value in self.quantiles):
            raise ValueError("research quantiles must be finite and inside (0, 1)")
        expected = tuple(
            sorted(
                self.quantiles,
                reverse=self.operator is EventRuleOperator.LESS_EQUAL,
            )
        )
        if self.quantiles != expected:
            raise ValueError("research quantiles must become more selective in registry order")
        if not self.primary_source.strip() or not self.rationale.strip():
            raise ValueError("research evidence metadata is required")
        if self.evidence_scope not in {
            "transfer_methodology",
            "pump_structure_not_pnl",
            "amm_mechanics_not_alpha",
        }:
            raise ValueError("invalid research evidence scope")


def _axis_pair(
    rank: int,
    family: str,
    feature: str,
    primary_source: str,
    rationale: str,
    evidence_scope: str = "transfer_methodology",
    *,
    requires_uncensored_wallet_history: bool = False,
) -> tuple[EventResearchAxis, EventResearchAxis]:
    return (
        EventResearchAxis(
            rank=rank,
            family=f"{family}_continuation",
            feature=feature,
            operator=EventRuleOperator.GREATER_EQUAL,
            quantiles=HIGH_QUANTILES,
            primary_source=primary_source,
            rationale=rationale,
            evidence_scope=evidence_scope,
            requires_uncensored_wallet_history=requires_uncensored_wallet_history,
        ),
        EventResearchAxis(
            rank=rank,
            family=f"{family}_reversal",
            feature=feature,
            operator=EventRuleOperator.LESS_EQUAL,
            quantiles=LOW_QUANTILES,
            primary_source=primary_source,
            rationale=rationale,
            evidence_scope=evidence_scope,
            requires_uncensored_wallet_history=requires_uncensored_wallet_history,
        ),
    )


# Nineteen measurements, each tested in both long-only polarities.  State and
# execution fields such as unsigned VPIN, impact, concentration, progress, and
# creator dumping are deliberately reserved for later one-gate-at-a-time tests.
DEFAULT_EVENT_RESEARCH_AXES: tuple[EventResearchAxis, ...] = sum(
    (
        _axis_pair(
            1,
            "signed_flow_5e",
            "event_signed_quote_imbalance_5e",
            CONT_OFI_URL,
            "Exact swap-side SOL imbalance over five causal events.",
        ),
        _axis_pair(
            1,
            "signed_flow_20e",
            "event_signed_quote_imbalance_20e",
            CONT_OFI_URL,
            "Exact swap-side SOL imbalance over twenty causal events.",
        ),
        _axis_pair(
            1,
            "signed_flow_acceleration",
            "event_signed_flow_acceleration_5e_20e",
            CONT_OFI_URL,
            "Five-event signed flow minus the twenty-event state.",
        ),
        _axis_pair(
            1,
            "flow_per_amm_depth",
            "event_depth_normalized_signed_flow_20e",
            CFMM_URL,
            "Signed SOL flow divided by observed AMM quote reserves; never native L2 OFI.",
            "amm_mechanics_not_alpha",
        ),
        _axis_pair(
            1,
            "marked_hawkes",
            "hawkes_signed_quote_pre",
            HAWKES_URL,
            "Pre-event exponentially decayed signed SOL excitation.",
        ),
        _axis_pair(
            1,
            "signed_intensity_pressure",
            "event_signed_intensity_pressure_5e_20e",
            OFI_FORECAST_URL,
            "Signed short flow scaled by recent-versus-slow arrival intensity.",
        ),
        _axis_pair(
            1,
            "signed_vpin",
            "signed_vpin",
            VPIN_URL,
            "Exact equal-SOL-volume signed buckets; interpretation remains contested.",
        ),
        _axis_pair(
            2,
            "reserve_momentum_5e",
            "event_reserve_momentum_5e",
            INTRADAY_MOMENTUM_URL,
            "Five-event reserve-mark momentum translated from intraday momentum research.",
        ),
        _axis_pair(
            2,
            "reserve_momentum_20e",
            "event_reserve_momentum_20e",
            TSMOM_URL,
            "Twenty-event reserve-mark momentum; the source horizon is not copied literally.",
        ),
        _axis_pair(
            2,
            "reserve_momentum_acceleration",
            "event_reserve_momentum_acceleration_5e_20e",
            INTRADAY_MOMENTUM_URL,
            "Short per-event reserve momentum minus medium per-event momentum.",
        ),
        _axis_pair(
            2,
            "signed_trend_efficiency",
            "event_signed_trend_efficiency_20e",
            TSMOM_URL,
            "Net reserve displacement divided by absolute path length.",
        ),
        _axis_pair(
            2,
            "price_path_location",
            "event_distance_from_high_50e",
            INTRADAY_MOMENTUM_URL,
            "Near-high continuation versus deep-drawdown long-only reversal.",
        ),
        _axis_pair(
            2,
            "rolling_vwap_deviation",
            "event_rolling_vwap_deviation_20e",
            CONCRETUM_MOMENTUM_URL,
            "Reserve mark relative to exact rolling swap quote/base VWAP.",
        ),
        _axis_pair(
            2,
            "anchored_vwap_deviation",
            "event_anchored_vwap_deviation",
            PUMP_PREDICTION_URL,
            "Reserve mark relative to exact VWAP since the last coverage/state reset.",
            "pump_structure_not_pnl",
        ),
        _axis_pair(
            3,
            "idiosyncratic_signed_flow",
            "event_idiosyncratic_signed_flow_20e",
            COMMON_FLOW_URL,
            "Token signed flow residual after contemporaneous global Pump flow.",
        ),
        _axis_pair(
            3,
            "attention_weighted_flow",
            "event_attention_weighted_signed_flow_20e",
            HAWKES_URL,
            "Token signed flow scaled by its causal share of global young-token SOL flow.",
        ),
        _axis_pair(
            3,
            "new_wallet_signed_flow",
            "event_new_wallet_signed_flow_20e",
            PUMP_PREDICTION_URL,
            "Signed flow scaled by new-wallet participation; graduation evidence is not PnL.",
            "pump_structure_not_pnl",
            requires_uncensored_wallet_history=True,
        ),
        _axis_pair(
            3,
            "repeat_wallet_signed_flow",
            "event_repeat_wallet_signed_flow_20e",
            PUMP_PREDICTION_URL,
            "Signed flow scaled by repeat-wallet participation.",
            "pump_structure_not_pnl",
            requires_uncensored_wallet_history=True,
        ),
        _axis_pair(
            3,
            "abnormal_signed_volume",
            "event_signed_volume_pressure_5e_50e",
            CONCRETUM_MOMENTUM_URL,
            "Signed five-event flow scaled by short-versus-long SOL volume.",
        ),
    ),
    (),
)


EVENT_RESEARCH_STATE_FEATURES: tuple[str, ...] = (
    "base_size_fraction",
    "bonding_progress",
    "creator_dump_pressure",
    "creator_net_sell_share",
    "event_duration_ratio_20e_5e",
    "event_quote_volume_ratio_5e_50e",
    "event_volatility_ratio_5e_50e",
    "fee_excluded_directional_impact_bps",
    "first_seen_censored",
    "global_activity_ratio_1s_30s",
    "gross_directional_impact_bps",
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


@dataclass(frozen=True, slots=True)
class EventResearchCandidate:
    """Stable candidate identity; its numeric threshold is fold-specific."""

    axis: EventResearchAxis
    quantile: float
    holding_events: int

    def __post_init__(self) -> None:
        if self.quantile not in self.axis.quantiles:
            raise ValueError("candidate quantile is not registered on its axis")
        if self.holding_events < 1:
            raise ValueError("candidate holding horizon must be positive")

    @property
    def name(self) -> str:
        return (
            f"event-research:{self.axis.family}:{self.axis.operator.value}:"
            f"q-{self.quantile:g}:hold-{self.holding_events}"
        )

    @property
    def sha256(self) -> str:
        payload = {
            "axis": asdict(self.axis),
            "quantile": self.quantile,
            "holding_events": self.holding_events,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class EventResearchThreshold:
    candidate_sha256: str
    fold: int
    value: float | None


def default_event_research_candidates(
    holding_events: Sequence[int] = (5, 20, 50),
    axes: Sequence[EventResearchAxis] = DEFAULT_EVENT_RESEARCH_AXES,
) -> tuple[EventResearchCandidate, ...]:
    """Return the frozen axis × train quantile × holding-horizon registry."""

    horizons = tuple(holding_events)
    if (
        not horizons
        or tuple(sorted(set(horizons))) != horizons
        or any(value < 1 for value in horizons)
    ):
        raise ValueError("holding horizons must be positive, unique, and sorted")
    result = tuple(
        EventResearchCandidate(axis, quantile, horizon)
        for horizon in horizons
        for axis in sorted(axes, key=lambda item: (item.rank, item.family))
        for quantile in axis.quantiles
    )
    hashes = [candidate.sha256 for candidate in result]
    if len(set(hashes)) != len(hashes):
        raise AssertionError("event research candidate registry contains duplicates")
    return result


def event_research_signal_features(
    axes: Sequence[EventResearchAxis] = DEFAULT_EVENT_RESEARCH_AXES,
) -> tuple[str, ...]:
    return tuple(
        sorted(
            {axis.feature for axis in axes}
            | set(EVENT_RESEARCH_STATE_FEATURES)
            | {"first_seen_censored"}
        )
    )


_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier {value!r}")
    return value


def _number(value: float) -> str:
    if not math.isfinite(value):
        raise ValueError("SQL numeric literal must be finite")
    return format(value, ".17g")


def create_event_research_source_sql(
    *,
    input_table: str = "raw_source_events",
    output_view: str = "source_events",
) -> str:
    """Create gap- and phase-safe causal event-window features in DuckDB."""

    source = _identifier(input_table)
    output = _identifier(output_view)
    order = "slot, tx_index, log_index, event_index, event_id"
    segment = f"market_id, research_continuity_segment ORDER BY {order}"
    return f"""
CREATE VIEW {output} AS
WITH lagged AS (
    SELECT
        raw.*,
        lag(phase) OVER (PARTITION BY market_id ORDER BY {order}) AS research_prior_phase,
        CASE WHEN side = 'buy' THEN CAST(quote_amount_atomic AS DOUBLE)
             WHEN side = 'sell' THEN -CAST(quote_amount_atomic AS DOUBLE)
             ELSE NULL END AS research_signed_quote_atomic,
        CAST(quote_amount_atomic AS DOUBLE) /
            pow(10.0, CAST(quote_decimals AS DOUBLE)) AS research_quote_amount,
        CAST(base_amount_atomic AS DOUBLE) /
            pow(10.0, CAST(base_decimals AS DOUBLE)) AS research_base_amount,
        CASE WHEN reserve_mark_return IS NOT NULL AND reserve_mark_return > -1.0
             THEN ln(1.0 + reserve_mark_return) ELSE NULL END AS research_log_return
    FROM {source} AS raw
), marked AS (
    SELECT
        *,
        CASE WHEN coverage_gap OR NOT reserve_continuity
                  OR research_prior_phase IS NULL OR phase <> research_prior_phase
             THEN 1 ELSE 0 END AS research_segment_start
    FROM lagged
), segmented AS (
    SELECT
        *,
        sum(research_segment_start) OVER (
            PARTITION BY market_id ORDER BY {order}
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS research_continuity_segment
    FROM marked
), windowed AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY {segment}) AS research_segment_event_number,
        count(*) OVER w5 AS research_count_5e,
        count(*) OVER w20 AS research_count_20e,
        count(*) OVER w50 AS research_count_50e,
        sum(research_signed_quote_atomic) OVER w5 AS research_signed_quote_5e,
        sum(research_signed_quote_atomic) OVER w20 AS research_signed_quote_20e,
        sum(CAST(quote_amount_atomic AS DOUBLE)) OVER w5 AS research_total_quote_5e,
        sum(CAST(quote_amount_atomic AS DOUBLE)) OVER w20 AS research_total_quote_20e,
        avg(research_quote_amount) OVER w5 AS research_mean_quote_5e,
        avg(research_quote_amount) OVER w50 AS research_mean_quote_50e,
        avg(seconds_since_previous_event) OVER w5 AS research_mean_duration_5e,
        avg(seconds_since_previous_event) OVER w20 AS research_mean_duration_20e,
        count(seconds_since_previous_event) OVER w5 AS research_duration_count_5e,
        count(seconds_since_previous_event) OVER w20 AS research_duration_count_20e,
        lag(post_spot_price, 5) OVER wsegment AS research_lag_price_5e,
        lag(post_spot_price, 20) OVER wsegment AS research_lag_price_20e,
        max(post_spot_price) OVER w50 AS research_high_price_50e,
        sum(abs(research_log_return)) OVER w20 AS research_abs_path_20e,
        sqrt(avg(research_log_return * research_log_return) OVER w5)
            AS research_volatility_5e,
        sqrt(avg(research_log_return * research_log_return) OVER w50)
            AS research_volatility_50e,
        sum(research_quote_amount) OVER w20 AS research_rolling_quote_20e,
        sum(research_base_amount) OVER w20 AS research_rolling_base_20e,
        sum(research_quote_amount) OVER wall AS research_anchored_quote,
        sum(research_base_amount) OVER wall AS research_anchored_base
    FROM segmented
    WINDOW
        wsegment AS (PARTITION BY {segment}),
        w5 AS (PARTITION BY {segment} ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
        w20 AS (PARTITION BY {segment} ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
        w50 AS (PARTITION BY {segment} ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
        wall AS (PARTITION BY {segment} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
), core AS (
    SELECT
        *,
        CASE WHEN research_count_5e = 5 AND research_total_quote_5e > 0.0
             THEN research_signed_quote_5e / research_total_quote_5e END
            AS event_signed_quote_imbalance_5e,
        CASE WHEN research_count_20e = 20 AND research_total_quote_20e > 0.0
             THEN research_signed_quote_20e / research_total_quote_20e END
            AS event_signed_quote_imbalance_20e,
        CASE WHEN research_count_20e = 20 AND post_quote_reserve_atomic > 0
             THEN research_signed_quote_20e /
                  CAST(post_quote_reserve_atomic AS DOUBLE) END
            AS event_depth_normalized_signed_flow_20e,
        CASE WHEN post_spot_price > 0.0 AND research_lag_price_5e > 0.0
             THEN ln(post_spot_price / research_lag_price_5e) END
            AS event_reserve_momentum_5e,
        CASE WHEN post_spot_price > 0.0 AND research_lag_price_20e > 0.0
             THEN ln(post_spot_price / research_lag_price_20e) END
            AS event_reserve_momentum_20e,
        CASE WHEN research_count_50e = 50 AND post_spot_price > 0.0
                  AND research_high_price_50e > 0.0
             THEN ln(post_spot_price / research_high_price_50e) END
            AS event_distance_from_high_50e,
        CASE WHEN research_count_20e = 20 AND post_spot_price > 0.0
                  AND research_rolling_base_20e > 0.0
                  AND research_rolling_quote_20e > 0.0
             THEN ln(post_spot_price /
                     (research_rolling_quote_20e / research_rolling_base_20e)) END
            AS event_rolling_vwap_deviation_20e,
        CASE WHEN post_spot_price > 0.0 AND research_anchored_base > 0.0
                  AND research_anchored_quote > 0.0
             THEN ln(post_spot_price /
                     (research_anchored_quote / research_anchored_base)) END
            AS event_anchored_vwap_deviation,
        CASE WHEN research_count_50e = 50 AND research_mean_quote_50e > 0.0
             THEN research_mean_quote_5e / research_mean_quote_50e END
            AS event_quote_volume_ratio_5e_50e,
        CASE WHEN research_duration_count_5e >= 4
                  AND research_duration_count_20e >= 19
                  AND research_mean_duration_5e > 0.0
             THEN research_mean_duration_20e / research_mean_duration_5e END
            AS event_duration_ratio_20e_5e,
        CASE WHEN research_count_50e = 50 AND research_volatility_50e > 0.0
             THEN research_volatility_5e / research_volatility_50e END
            AS event_volatility_ratio_5e_50e
    FROM windowed
), derived AS (
    SELECT
        *,
        event_signed_quote_imbalance_5e - event_signed_quote_imbalance_20e
            AS event_signed_flow_acceleration_5e_20e,
        event_signed_quote_imbalance_5e * event_duration_ratio_20e_5e
            AS event_signed_intensity_pressure_5e_20e,
        event_reserve_momentum_5e / 5.0 - event_reserve_momentum_20e / 20.0
            AS event_reserve_momentum_acceleration_5e_20e,
        CASE WHEN research_abs_path_20e > 0.0
             THEN event_reserve_momentum_20e / research_abs_path_20e END
            AS event_signed_trend_efficiency_20e,
        event_signed_quote_imbalance_20e - global_signed_quote_imbalance_30s
            AS event_idiosyncratic_signed_flow_20e,
        event_signed_quote_imbalance_20e * market_quote_share_global_5s
            AS event_attention_weighted_signed_flow_20e,
        event_signed_quote_imbalance_20e * new_wallet_flow_share
            AS event_new_wallet_signed_flow_20e,
        event_signed_quote_imbalance_20e * repeat_wallet_flow_share
            AS event_repeat_wallet_signed_flow_20e,
        event_signed_quote_imbalance_5e * event_quote_volume_ratio_5e_50e
            AS event_signed_volume_pressure_5e_50e
    FROM core
)
SELECT * FROM derived
""".strip()


def _selected_candidates(
    candidates: Sequence[EventResearchCandidate], holding_events: int
) -> tuple[EventResearchCandidate, ...]:
    selected = tuple(
        candidate for candidate in candidates if candidate.holding_events == holding_events
    )
    if not selected:
        raise ValueError("no research candidates exist for requested holding horizon")
    return selected


def create_event_research_thresholds_sql(
    candidates: Sequence[EventResearchCandidate],
    holding_events: int,
    *,
    output_table: str | None = None,
    labels_table: str = "event_labels",
) -> str:
    """Fit pooled non-overlapping decision thresholds on train memberships only."""

    selected = _selected_candidates(candidates, holding_events)
    labels = _identifier(labels_table)
    output = _identifier(
        f"event_research_thresholds_{holding_events}" if output_table is None else output_table
    )
    columns: list[str] = []
    for index, candidate in enumerate(selected):
        prefix = f"c{index:03d}"
        feature = _identifier(candidate.axis.feature)
        condition = (
            f"labels.eligible_{holding_events} "
            f"AND labels.label_end_{holding_events}_available_at_ns < memberships.cutoff_ns "
            f"AND labels.{feature} IS NOT NULL"
        )
        if candidate.axis.requires_uncensored_wallet_history:
            condition += " AND NOT labels.first_seen_censored"
        columns.append(
            f"quantile_cont(labels.{feature}, {_number(candidate.quantile)}) "
            f"FILTER (WHERE {condition}) AS {prefix}_threshold"
        )
    column_sql = ",\n        ".join(columns)
    return f"""
CREATE TABLE {output} AS
SELECT
        memberships.fold,
        {column_sql}
FROM {labels} AS labels
INNER JOIN memberships AS memberships
    ON labels.mint = memberships.mint
WHERE memberships.split = 'train'
GROUP BY memberships.fold
ORDER BY memberships.fold
""".strip()


def event_research_aggregate_sql(
    candidates: Sequence[EventResearchCandidate],
    holding_events: int,
    *,
    thresholds_table: str | None = None,
    labels_table: str = "event_labels",
) -> str:
    """Apply fold-fitted thresholds and aggregate PnL by whole token first."""

    selected = _selected_candidates(candidates, holding_events)
    labels = _identifier(labels_table)
    thresholds = _identifier(
        f"event_research_thresholds_{holding_events}"
        if thresholds_table is None
        else thresholds_table
    )
    inner: list[str] = []
    outer: list[str] = []
    for index, candidate in enumerate(selected):
        prefix = f"c{index:03d}"
        feature = _identifier(candidate.axis.feature)
        operator = ">=" if candidate.axis.operator is EventRuleOperator.GREATER_EQUAL else "<="
        condition = (
            f"labels.eligible_{holding_events} "
            f"AND labels.label_end_{holding_events}_available_at_ns < memberships.cutoff_ns "
            f"AND thresholds.{prefix}_threshold IS NOT NULL "
            f"AND labels.{feature} {operator} thresholds.{prefix}_threshold"
        )
        if candidate.axis.requires_uncensored_wallet_history:
            condition += " AND NOT labels.first_seen_censored"
        inner.extend(
            (
                f"count(*) FILTER (WHERE {condition}) AS {prefix}_n",
                f"sum(labels.pnl_{holding_events}) FILTER (WHERE {condition}) AS {prefix}_sum",
                f"count(*) FILTER (WHERE {condition} AND labels.pnl_{holding_events} > 0.0) "
                f"AS {prefix}_wins",
                f"count(*) FILTER (WHERE {condition} AND labels.pnl_{holding_events} < 0.0) "
                f"AS {prefix}_losses",
                f"sum(labels.pnl_{holding_events}) FILTER "
                f"(WHERE {condition} AND labels.pnl_{holding_events} > 0.0) "
                f"AS {prefix}_gross_profit",
                f"sum(-labels.pnl_{holding_events}) FILTER "
                f"(WHERE {condition} AND labels.pnl_{holding_events} < 0.0) "
                f"AS {prefix}_gross_loss",
                f"sum(labels.pnl_{holding_events} * labels.pnl_{holding_events}) FILTER "
                f"(WHERE {condition}) AS {prefix}_sum_sq",
                f"sum(labels.pnl_{holding_events} * labels.pnl_{holding_events}) FILTER "
                f"(WHERE {condition} AND labels.pnl_{holding_events} < 0.0) "
                f"AS {prefix}_downside_sum_sq",
            )
        )
        outer.extend(
            (
                f"coalesce(sum({prefix}_n), 0)::BIGINT AS {prefix}_outcomes",
                f"count(*) FILTER (WHERE {prefix}_n > 0)::BIGINT AS {prefix}_mints",
                f"coalesce(sum({prefix}_sum), 0.0) AS {prefix}_total_pnl",
                f"coalesce(sum({prefix}_wins), 0)::BIGINT AS {prefix}_wins",
                f"coalesce(sum({prefix}_losses), 0)::BIGINT AS {prefix}_losses",
                f"coalesce(sum({prefix}_gross_profit), 0.0) AS {prefix}_gross_profit",
                f"coalesce(sum({prefix}_gross_loss), 0.0) AS {prefix}_gross_loss",
                f"coalesce(sum({prefix}_sum_sq), 0.0) AS {prefix}_sum_sq",
                f"coalesce(sum({prefix}_downside_sum_sq), 0.0) AS {prefix}_downside_sum_sq",
                f"sum({prefix}_sum) / NULLIF(sum({prefix}_n), 0) AS {prefix}_mean_event",
                f"avg({prefix}_sum / NULLIF({prefix}_n, 0)) "
                f"FILTER (WHERE {prefix}_n > 0) AS {prefix}_mean_token",
                f"stddev_samp({prefix}_sum / NULLIF({prefix}_n, 0)) "
                f"FILTER (WHERE {prefix}_n > 0) AS {prefix}_std_token",
                f"avg(CASE WHEN {prefix}_sum / NULLIF({prefix}_n, 0) > 0.0 "
                f"THEN 1.0 ELSE 0.0 END) FILTER (WHERE {prefix}_n > 0) "
                f"AS {prefix}_positive_tokens",
            )
        )
    inner_sql = ",\n        ".join(inner)
    outer_sql = ",\n    ".join(outer)
    return f"""
WITH per_token AS (
    SELECT
        memberships.fold,
        memberships.split,
        labels.mint,
        {inner_sql}
    FROM {labels} AS labels
    INNER JOIN memberships AS memberships
        ON labels.mint = memberships.mint
    INNER JOIN {thresholds} AS thresholds
        ON memberships.fold = thresholds.fold
    GROUP BY memberships.fold, memberships.split, memberships.cutoff_ns, labels.mint
)
SELECT
    fold,
    split,
    {outer_sql}
FROM per_token
GROUP BY fold, split
ORDER BY fold, split
""".strip()


def parse_event_research_threshold_rows(
    rows: Sequence[Mapping[str, object]],
    candidates: Sequence[EventResearchCandidate],
    holding_events: int,
) -> tuple[EventResearchThreshold, ...]:
    selected = _selected_candidates(candidates, holding_events)
    result: list[EventResearchThreshold] = []
    for row in rows:
        fold_value = row.get("fold")
        if isinstance(fold_value, bool) or not isinstance(fold_value, Real):
            raise ValueError("fold is not numeric")
        fold_float = float(fold_value)
        if not math.isfinite(fold_float) or not fold_float.is_integer():
            raise ValueError("fold is not a finite integer")
        for index, candidate in enumerate(selected):
            value = row.get(f"c{index:03d}_threshold")
            threshold: float | None
            if value is None:
                threshold = None
            elif isinstance(value, bool) or not isinstance(value, Real):
                raise ValueError("research threshold is not numeric")
            else:
                threshold = float(value)
                if not math.isfinite(threshold):
                    raise ValueError("research threshold is not finite")
            result.append(EventResearchThreshold(candidate.sha256, int(fold_float), threshold))
    return tuple(result)


def parse_event_research_aggregate_rows(
    rows: Sequence[Mapping[str, object]],
    candidates: Sequence[EventResearchCandidate],
    holding_events: int,
) -> tuple[EventCandidateMetrics, ...]:
    """Convert the wide aggregate into the existing auditable metric shape."""

    selected = _selected_candidates(candidates, holding_events)
    result: list[EventCandidateMetrics] = []

    def required_float(value: object, name: str) -> float:
        if isinstance(value, bool) or not isinstance(value, Real):
            raise ValueError(f"{name} is not numeric")
        number = float(value)
        if not math.isfinite(number):
            raise ValueError(f"{name} is not finite")
        return number

    def required_int(value: object, name: str) -> int:
        number = required_float(value, name)
        if not number.is_integer():
            raise ValueError(f"{name} is not an integer")
        return int(number)

    def optional_float(value: object, name: str) -> float | None:
        return None if value is None else required_float(value, name)

    def optional_int(value: object, name: str) -> int | None:
        return None if value is None else required_int(value, name)

    for row in rows:
        fold = required_int(row["fold"], "fold")
        split_value = row["split"]
        if not isinstance(split_value, str) or split_value not in {"train", "test"}:
            raise ValueError("split must be train or test")
        for index, candidate in enumerate(selected):
            prefix = f"c{index:03d}"
            outcomes = required_int(row[f"{prefix}_outcomes"], "outcomes")
            mints = required_int(row[f"{prefix}_mints"], "mints")
            mean_event = optional_float(row[f"{prefix}_mean_event"], "mean_event")
            mean_token = optional_float(row[f"{prefix}_mean_token"], "mean_token")
            token_std = optional_float(row[f"{prefix}_std_token"], "token_std")
            wins = optional_int(row.get(f"{prefix}_wins"), "wins")
            losses = optional_int(row.get(f"{prefix}_losses"), "losses")
            gross_profit = optional_float(row.get(f"{prefix}_gross_profit"), "gross_profit")
            gross_loss = optional_float(row.get(f"{prefix}_gross_loss"), "gross_loss")
            sum_sq = optional_float(row.get(f"{prefix}_sum_sq"), "sum_sq")
            downside_sum_sq = optional_float(
                row.get(f"{prefix}_downside_sum_sq"), "downside_sum_sq"
            )
            breakevens = (
                None
                if outcomes < 0 or wins is None or losses is None
                else max(0, outcomes - wins - losses)
            )
            decisive_outcomes = None if wins is None or losses is None else wins + losses
            win_rate = (
                None if wins is None or decisive_outcomes in (None, 0) else wins / decisive_outcomes
            )
            loss_rate = (
                None
                if losses is None or decisive_outcomes in (None, 0)
                else losses / decisive_outcomes
            )
            average_win = None if gross_profit is None or wins in (None, 0) else gross_profit / wins
            average_loss = (
                None if gross_loss is None or losses in (None, 0) else gross_loss / losses
            )
            payoff_ratio = (
                None
                if average_win is None or average_loss is None or average_loss <= 0.0
                else average_win / average_loss
            )
            profit_factor = (
                None
                if gross_profit is None or gross_loss is None or gross_loss <= 0.0
                else gross_profit / gross_loss
            )
            pnl_std = None
            if sum_sq is not None and outcomes >= 2:
                mean = mean_event or 0.0
                pnl_std = math.sqrt(max(0.0, (sum_sq - outcomes * mean * mean) / (outcomes - 1)))
            downside_deviation = (
                None
                if downside_sum_sq is None or outcomes <= 0
                else math.sqrt(max(0.0, downside_sum_sq / outcomes))
            )
            sharpe = (
                None
                if mean_event is None or pnl_std is None or pnl_std <= 0.0
                else mean_event / pnl_std
            )
            sortino = (
                None
                if mean_event is None or downside_deviation is None or downside_deviation <= 0.0
                else mean_event / downside_deviation
            )
            token_t_stat = (
                None
                if mean_token is None or token_std is None or token_std <= 0.0 or mints < 2
                else mean_token / (token_std / math.sqrt(mints))
            )
            result.append(
                EventCandidateMetrics(
                    candidate_sha256=candidate.sha256,
                    fold=fold,
                    split=split_value,
                    outcomes=outcomes,
                    mints=mints,
                    total_quote_pnl=required_float(row[f"{prefix}_total_pnl"], "total_quote_pnl"),
                    mean_event_pnl_quote=mean_event,
                    mean_token_pnl_quote=mean_token,
                    token_pnl_std_quote=token_std,
                    token_t_stat=token_t_stat,
                    positive_token_share=optional_float(
                        row[f"{prefix}_positive_tokens"], "positive_token_share"
                    ),
                    wins=wins,
                    losses=losses,
                    breakevens=breakevens,
                    win_rate=win_rate,
                    loss_rate=loss_rate,
                    average_win_quote=average_win,
                    average_loss_quote=average_loss,
                    payoff_ratio=payoff_ratio,
                    gross_profit_quote=gross_profit,
                    gross_loss_quote=gross_loss,
                    profit_factor=profit_factor,
                    expectancy_quote=mean_event,
                    pnl_std_quote=pnl_std,
                    downside_deviation_quote=downside_deviation,
                    sharpe_per_trade=sharpe,
                    sortino_per_trade=sortino,
                )
            )
    return tuple(result)


__all__ = [
    "COMMON_FLOW_URL",
    "CONCRETUM_MOMENTUM_URL",
    "DEFAULT_EVENT_RESEARCH_AXES",
    "EVENT_RESEARCH_STATE_FEATURES",
    "EventResearchAxis",
    "EventResearchCandidate",
    "EventResearchThreshold",
    "HIGH_QUANTILES",
    "LOW_QUANTILES",
    "OFI_FORECAST_URL",
    "create_event_research_source_sql",
    "create_event_research_thresholds_sql",
    "default_event_research_candidates",
    "event_research_aggregate_sql",
    "event_research_signal_features",
    "parse_event_research_aggregate_rows",
    "parse_event_research_threshold_rows",
]
