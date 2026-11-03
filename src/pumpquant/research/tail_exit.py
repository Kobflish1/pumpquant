"""Pre-registered extreme-tail event signals with causal dynamic exits.

This module is intentionally narrower than ``fresh_event_research``.  It
tests whether the negative fixed-horizon result was caused by exit design and
tail dilution, rather than opening a new unrestricted feature search.  Signal
thresholds are still fit only inside each chronological training fold.  Exit
triggers are observed on a later event and filled after the same declared
event latency against the constant-product AMM state.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Sequence
from dataclasses import asdict, dataclass

from pumpquant.features.alpha import (
    CFMM_URL,
    CONT_OFI_URL,
    HAWKES_URL,
    INTRADAY_MOMENTUM_URL,
)
from pumpquant.research.event_research import EventResearchAxis, EventResearchCandidate
from pumpquant.research.event_screen import EventRuleOperator, EventScreenConfig

EXPERIMENT_ID = "fresh-tail-exit-v1"
VERSION = 1
PRIOR_ATTEMPTED_CANDIDATES = 1_479
TAIL_QUANTILES_HIGH = (0.99, 0.995)
TAIL_QUANTILES_LOW = (0.01, 0.005)


@dataclass(frozen=True, slots=True)
class ExitProfile:
    """One complete, frozen take-profit/stop/trailing/time exit rule."""

    profile_id: str
    time_exit_events: int
    take_profit: float
    stop_loss: float
    trailing_stop: float
    rationale: str

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", self.profile_id):
            raise ValueError("exit profile id must be a safe lowercase identifier")
        if self.time_exit_events < 1:
            raise ValueError("time_exit_events must be positive")
        if self.take_profit <= 0.0:
            raise ValueError("take_profit must be positive")
        if not 0.0 < self.stop_loss < 1.0:
            raise ValueError("stop_loss must be inside (0, 1)")
        if not 0.0 < self.trailing_stop < 1.0:
            raise ValueError("trailing_stop must be inside (0, 1)")
        if not self.rationale.strip():
            raise ValueError("exit profile rationale is required")


@dataclass(frozen=True, slots=True)
class TailExitCandidate:
    """Stable identity for one signal-tail and exit interaction."""

    signal: EventResearchCandidate
    exit_profile: ExitProfile

    @property
    def name(self) -> str:
        return (
            f"tail-exit:{self.signal.axis.family}:{self.signal.axis.operator.value}:"
            f"q-{self.signal.quantile:g}:"
            f"tp-{self.exit_profile.take_profit:g}:"
            f"sl-{self.exit_profile.stop_loss:g}:"
            f"trail-{self.exit_profile.trailing_stop:g}:"
            f"time-{self.exit_profile.time_exit_events}"
        )

    @property
    def sha256(self) -> str:
        value = {
            "signal_sha256": self.signal.sha256,
            "exit_profile": asdict(self.exit_profile),
        }
        return hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


def _tail_pair(
    rank: int,
    family: str,
    feature: str,
    source: str,
    rationale: str,
    *,
    evidence_scope: str = "transfer_methodology",
) -> tuple[EventResearchAxis, EventResearchAxis]:
    return (
        EventResearchAxis(
            rank=rank,
            family=f"{family}_continuation",
            feature=feature,
            operator=EventRuleOperator.GREATER_EQUAL,
            quantiles=TAIL_QUANTILES_HIGH,
            primary_source=source,
            rationale=rationale,
            evidence_scope=evidence_scope,
        ),
        EventResearchAxis(
            rank=rank,
            family=f"{family}_reversal",
            feature=feature,
            operator=EventRuleOperator.LESS_EQUAL,
            quantiles=TAIL_QUANTILES_LOW,
            primary_source=source,
            rationale=rationale,
            evidence_scope=evidence_scope,
        ),
    )


TAIL_AXES: tuple[EventResearchAxis, ...] = (
    *_tail_pair(
        1,
        "signed_intensity_pressure",
        "event_signed_intensity_pressure_5e_20e",
        "https://arxiv.org/abs/2408.03594",
        (
            "Extreme signed short-flow pressure scaled by recent-versus-slow arrival "
            "intensity; the test asks whether only the most unusual pressure states "
            "survive costs."
        ),
    ),
    *_tail_pair(
        1,
        "arrival_duration_ratio",
        "event_duration_ratio_20e_5e",
        HAWKES_URL,
        (
            "Extreme compression or relaxation in causal inter-arrival duration is "
            "used as a transparent Hawkes-arrival state, not as a clock-session rule."
        ),
    ),
    *_tail_pair(
        1,
        "signed_flow_acceleration",
        "event_signed_flow_acceleration_5e_20e",
        CONT_OFI_URL,
        (
            "Recent signed SOL flow minus its medium event-window state; the Pump "
            "translation is swap-side flow, never native order-book OFI."
        ),
    ),
    *_tail_pair(
        1,
        "flow_per_amm_depth",
        "event_depth_normalized_signed_flow_20e",
        CFMM_URL,
        (
            "Signed SOL flow divided by observed quote reserves, translating the "
            "depth-normalized idea to executable constant-product AMM depth."
        ),
        evidence_scope="amm_mechanics_not_alpha",
    ),
    *_tail_pair(
        2,
        "reserve_momentum_acceleration",
        "event_reserve_momentum_acceleration_5e_20e",
        INTRADAY_MOMENTUM_URL,
        (
            "Short per-event reserve-mark momentum minus medium per-event momentum; "
            "the futures horizon is not copied literally to Pump/PumpSwap."
        ),
    ),
)


EXIT_PROFILES: tuple[ExitProfile, ...] = (
    ExitProfile(
        profile_id="protective_5e",
        time_exit_events=5,
        take_profit=0.10,
        stop_loss=0.06,
        trailing_stop=0.04,
        rationale=(
            "Short protective exit: test whether a five-event decision horizon "
            "captures bursts while limiting adverse AMM drift."
        ),
    ),
    ExitProfile(
        profile_id="convex_10e",
        time_exit_events=10,
        take_profit=0.30,
        stop_loss=0.12,
        trailing_stop=0.10,
        rationale=(
            "Short convex exit: allow a larger move while declaring the stop, trail, "
            "and ten-event time limit before seeing outcomes."
        ),
    ),
)


def _signal_candidates(holding_events: int) -> tuple[EventResearchCandidate, ...]:
    return tuple(
        EventResearchCandidate(axis, quantile, holding_events)
        for axis in sorted(TAIL_AXES, key=lambda item: (item.rank, item.family))
        for quantile in axis.quantiles
    )


def candidates() -> tuple[TailExitCandidate, ...]:
    result = tuple(
        TailExitCandidate(signal, profile)
        for profile in EXIT_PROFILES
        for signal in _signal_candidates(profile.time_exit_events)
    )
    hashes = [item.sha256 for item in result]
    if len(result) != 40 or len(set(hashes)) != len(hashes):
        raise AssertionError("tail-exit candidate registry geometry is not frozen")
    return result


def candidate_payload(candidate: TailExitCandidate) -> dict[str, object]:
    return {
        "sha256": candidate.sha256,
        "name": candidate.name,
        "axis": asdict(candidate.signal.axis),
        "quantile": candidate.signal.quantile,
        "holding_events": candidate.signal.holding_events,
        "source": candidate.signal.axis.primary_source,
        "mechanism": candidate.signal.axis.rationale,
        "parameters": {
            "signal_quantile": candidate.signal.quantile,
            "holding_events": candidate.signal.holding_events,
            "exit_profile": candidate.exit_profile.profile_id,
            "take_profit": candidate.exit_profile.take_profit,
            "stop_loss": candidate.exit_profile.stop_loss,
            "trailing_stop": candidate.exit_profile.trailing_stop,
            "time_exit_events": candidate.exit_profile.time_exit_events,
        },
        "signal": {
            "sha256": candidate.signal.sha256,
            "name": candidate.signal.name,
            "axis": asdict(candidate.signal.axis),
            "quantile": candidate.signal.quantile,
            "holding_events": candidate.signal.holding_events,
        },
        "exit_profile": asdict(candidate.exit_profile),
        "threshold_policy": "fit_inside_each_training_fold",
        "interaction_policy": (
            "signal tail × exit profile was preregistered as one intentional "
            "interaction; no post-outcome exit selection is permitted"
        ),
    }


def candidate_order_sha256() -> str:
    return hashlib.sha256("\n".join(item.sha256 for item in candidates()).encode()).hexdigest()


def trial_denominator() -> int:
    return PRIOR_ATTEMPTED_CANDIDATES + len(candidates())


def research_announcement() -> dict[str, object]:
    """Return the complete pre-outcome brief shown by the live workstation."""

    sources = [
        {
            "theory": "High-frequency order-flow imbalance forecasting",
            "source": "https://arxiv.org/abs/2408.03594",
            "mechanism": (
                "Extreme signed intensity pressure may forecast the next short flow state."
            ),
            "pump_translation": (
                "Use signed swap quote flow × recent/slow event-arrival intensity on a "
                "constant-product bonding curve or PumpSwap pool."
            ),
        },
        {
            "theory": "Hawkes price-and-trade dynamics",
            "source": HAWKES_URL,
            "mechanism": (
                "Arrival self-excitation and duration compression identify short-lived "
                "activity states."
            ),
            "pump_translation": (
                "Use causal inter-arrival duration ratio between swap events; no "
                "resting orders or queue position are assumed."
            ),
        },
        {
            "theory": "Price impact of order-flow events",
            "source": CONT_OFI_URL,
            "mechanism": (
                "A recent signed-flow acceleration can precede continued directional pressure."
            ),
            "pump_translation": (
                "Translate order-flow imbalance to exact buy-minus-sell SOL swap flow "
                "and preserve AMM reserve/fee semantics."
            ),
        },
        {
            "theory": "Constant-function AMM mechanics",
            "source": CFMM_URL,
            "mechanism": (
                "Flow relative to available depth should matter for executable price movement."
            ),
            "pump_translation": (
                "Normalize signed SOL flow by observed quote reserves on the bonding "
                "curve or PumpSwap pool; this is not native L2 OFI."
            ),
        },
        {
            "theory": "Intraday momentum and volatility",
            "source": INTRADAY_MOMENTUM_URL,
            "mechanism": (
                "Acceleration in short-window reserve-mark momentum may distinguish "
                "bursts from drift."
            ),
            "pump_translation": (
                "Use reserve-mark momentum acceleration from causal event windows; "
                "source horizons are not copied literally."
            ),
        },
    ]
    return {
        "kind": "pumpquant.research_announcement",
        "version": VERSION,
        "experiment_id": EXPERIMENT_ID,
        "status": "preregistered",
        "objective": (
            "Distinguish tail dilution from exit-design failure after the prior "
            "20/50-event screen was uniformly negative."
        ),
        "sources": sources,
        "parameter_grid": {
            "signal_features": [axis.feature for axis in TAIL_AXES[::2]],
            "polarities": ["greater_equal continuation", "less_equal reversal"],
            "train_tail_quantiles": {
                "continuation": list(TAIL_QUANTILES_HIGH),
                "reversal": list(TAIL_QUANTILES_LOW),
            },
            "latency_events": 2,
            "exit_profiles": [asdict(profile) for profile in EXIT_PROFILES],
            "screen_notional_sol": 0.10,
            "fixed_cost_sol_per_leg": 0.00016,
            "adverse_selection_bps_per_leg": 30.0,
            "max_reserve_fraction": 0.0025,
            "observed_fee_required": True,
            "candidate_count": len(candidates()),
        },
        "candidate_count": len(candidates()),
        "prior_attempted_candidate_count": PRIOR_ATTEMPTED_CANDIDATES,
        "cumulative_trial_denominator": trial_denominator(),
        "causal_translation": (
            "Every entry and exit is evaluated on chronological Pump/PumpSwap reserve "
            "state with observed fees, constant-product price impact, two-event entry "
            "and exit latency, and no CLOB assumptions."
        ),
        "holdout_accessed": False,
    }


def registry_payload() -> dict[str, object]:
    values = candidates()
    return {
        "kind": "pumpquant.fresh_tail_exit_registry",
        "version": VERSION,
        "experiment_id": EXPERIMENT_ID,
        "prior_attempted_candidate_count": PRIOR_ATTEMPTED_CANDIDATES,
        "candidate_count": len(values),
        "candidate_order_sha256": candidate_order_sha256(),
        "cumulative_trial_denominator": trial_denominator(),
        "axes": [asdict(axis) for axis in TAIL_AXES],
        "exit_profiles": [asdict(profile) for profile in EXIT_PROFILES],
        "candidates": [candidate_payload(item) for item in values],
        "announcement": research_announcement(),
        "outcome_policy": {
            "thresholds": "train-only DuckDB quantile_cont per chronological fold",
            "screen": (
                "normalized float constant-product AMM outcomes with observed fees, "
                "30 bps adverse selection, 0.1 SOL notional, and causal dynamic exits"
            ),
            "promotion": (
                "repeatable positive OOS only; exact integer/shared-$10/capacity/"
                "holdout/forward gates remain separate"
            ),
            "holdout_accessed": False,
        },
    }


_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier {value!r}")
    return value


def _number(value: float) -> str:
    if not math.isfinite(value):
        raise ValueError("SQL numeric literal must be finite")
    return format(value, ".17g")


def _fee_rate_sql(prefix: str, fallback_rate: float) -> str:
    ratio = (
        f"CAST({prefix}_fee_quote_atomic AS DOUBLE) / "
        f"NULLIF(CAST({prefix}_quote_amount_atomic AS DOUBLE), 0.0)"
    )
    return (
        "CASE WHEN "
        f"{prefix}_fee_quote_atomic IS NOT NULL AND {prefix}_quote_amount_atomic > 0 "
        f"AND {ratio} BETWEEN 0.0 AND 0.05 THEN {ratio} "
        f"ELSE {_number(fallback_rate)} END"
    )


def _observed_fee_sql(prefix: str) -> str:
    ratio = (
        f"CAST({prefix}_fee_quote_atomic AS DOUBLE) / "
        f"NULLIF(CAST({prefix}_quote_amount_atomic AS DOUBLE), 0.0)"
    )
    return (
        f"{prefix}_fee_quote_atomic IS NOT NULL AND "
        f"{prefix}_quote_amount_atomic > 0 AND {ratio} BETWEEN 0.0 AND 0.05"
    )


def tail_exit_source_columns(signal_features: Sequence[str]) -> tuple[str, ...]:
    """Return the compact source projection required by the v2 engine."""

    selected_features = tuple(sorted({_identifier(feature) for feature in signal_features}))
    feature_columns = tuple(
        feature for feature in selected_features if feature != "first_seen_censored"
    )
    if not feature_columns:
        raise ValueError("tail-exit signal features cannot be empty")
    return tuple(
        dict.fromkeys(
            (
                "event_id",
                "market_id",
                "mint",
                "slot",
                "tx_index",
                "log_index",
                "event_index",
                "block_time_ns",
                "available_at_ns",
                "launch_time_ns",
                "phase",
                "coverage_gap",
                "reserve_continuity",
                "effective_pre_base_atomic",
                "effective_pre_quote_atomic",
                "base_decimals",
                "quote_decimals",
                "fee_asset",
                "fee_quote_atomic",
                "quote_amount_atomic",
                "post_spot_price",
                "first_seen_censored",
                *feature_columns,
            )
        )
    )


def create_tail_exit_labels_sql(
    config: EventScreenConfig,
    profile: ExitProfile,
    *,
    output_table: str = "event_labels",
    source_table: str = "source_events",
    signal_features: Sequence[str],
) -> str:
    """Create causal after-cost labels for one preregistered exit profile.

    A signal is sampled at non-overlapping ``time_exit_events`` decisions.  The
    entry fills after ``latency_events`` events.  Each later monitoring event
    can trigger a take-profit, stop-loss, or trailing stop; the exit fill is
    then taken after the same latency.  If no trigger occurs, the declared
    time exit is used.  Any coverage/reserve/phase break before the fill makes
    the outcome ineligible.
    """

    table = _identifier(output_table)
    source = _identifier(source_table)
    selected_features = tuple(sorted({_identifier(feature) for feature in signal_features}))
    feature_columns = tuple(
        feature for feature in selected_features if feature != "first_seen_censored"
    )
    if not feature_columns:
        raise ValueError("tail-exit signal features cannot be empty")
    if profile.time_exit_events not in config.holding_events:
        raise ValueError("screen config must include the exit profile horizon")
    order = "slot, tx_index, log_index, event_index, event_id"
    latency = config.latency_events
    horizon = profile.time_exit_events
    signal_select_columns = ",\n        ".join(f"signal.{feature}" for feature in feature_columns)
    output_signal_columns = ",\n    ".join(feature_columns)
    source_columns = tail_exit_source_columns(selected_features)
    source_select_columns = ",\n        ".join(f"source.{column}" for column in source_columns)
    fallback_rate = config.fallback_fee_bps / 10_000.0
    adverse_multiplier = max(0.0, 1.0 - config.adverse_selection_bps / 10_000.0)
    entry_fee_rate = _fee_rate_sql("entry", fallback_rate)
    exit_fee_rate = _fee_rate_sql("exit", fallback_rate)
    entry_fee_observed = _observed_fee_sql("entry")
    exit_fee_observed = _observed_fee_sql("exit")

    raw_quote = (
        "(exit_quote_reserve - "
        "(exit_base_reserve * exit_quote_reserve) / "
        "(exit_base_reserve + CASE WHEN exit_phase = 'bonding_curve' "
        "THEN entry_base_out ELSE entry_base_out * (1.0 - exit_fee_rate) END))"
    )
    fee_adjusted = (
        "CASE WHEN exit_phase = 'bonding_curve' "
        f"THEN ({raw_quote}) * (1.0 - exit_fee_rate) ELSE ({raw_quote}) END"
    )
    quote_out = f"({fee_adjusted}) * {_number(adverse_multiplier)}"
    valid = (
        "entry_event_id IS NOT NULL "
        "AND entry_phase IS NOT NULL "
        "AND entry_coverage_gap = FALSE "
        "AND entry_reserve_continuity "
        "AND path_bad_to_exit = 0 "
        "AND entry_base_reserve > 0.0 AND entry_quote_reserve > 0.0 "
        "AND entry_base_out > 0.0 "
        "AND exit_event_id IS NOT NULL "
        "AND exit_base_reserve > 0.0 AND exit_quote_reserve > 0.0 "
        "AND exit_phase = entry_phase "
        "AND exit_available_at_ns IS NOT NULL "
        f"AND {entry_fee_observed} AND {exit_fee_observed} "
        f"AND {_number(config.quote_notional)} <= "
        f"entry_quote_reserve * {_number(config.max_reserve_fraction)} "
        f"AND entry_base_out <= exit_base_reserve * {_number(config.max_reserve_fraction)} "
        f"AND token_age_seconds BETWEEN {_number(config.min_age_seconds)} "
        f"AND {_number(config.max_age_seconds)} "
        f"AND ({quote_out}) > {_number(config.fixed_cost_quote)}"
    )
    return f"""
CREATE TABLE {table} AS
WITH sequenced AS (
    SELECT
        {source_select_columns},
        row_number() OVER (PARTITION BY market_id ORDER BY {order}) AS market_event_number
    FROM {source} AS source
), signals AS (
    SELECT
        signal.event_id,
        signal.event_id AS signal_event_id,
        signal.market_id,
        signal.mint,
        signal.slot,
        signal.tx_index,
        signal.log_index,
        signal.event_index,
        signal.block_time_ns,
        signal.available_at_ns,
        signal.post_spot_price AS signal_mark_price,
        signal.phase,
        signal.market_event_number,
        signal.first_seen_censored,
        launches.launch_time_ns,
        entry.event_id AS entry_event_id,
        entry.block_time_ns AS entry_block_time_ns,
        entry.phase AS entry_phase,
        entry.coverage_gap AS entry_coverage_gap,
        entry.reserve_continuity AS entry_reserve_continuity,
        entry.effective_pre_base_atomic AS entry_effective_pre_base_atomic,
        entry.effective_pre_quote_atomic AS entry_effective_pre_quote_atomic,
        entry.base_decimals AS entry_base_decimals,
        entry.quote_decimals AS entry_quote_decimals,
        entry.fee_quote_atomic AS entry_fee_quote_atomic,
        entry.quote_amount_atomic AS entry_quote_amount_atomic,
        entry.available_at_ns AS entry_available_at_ns,
        entry.post_spot_price AS entry_mark_price,
        {signal_select_columns}
    FROM sequenced AS signal
    INNER JOIN launches AS launches
        ON signal.mint = launches.mint
    LEFT JOIN sequenced AS entry
        ON entry.market_id = signal.market_id
       AND entry.market_event_number = signal.market_event_number + {latency}
    WHERE signal.market_event_number % {horizon} = 1
), entry_values AS (
    SELECT
        signals.*,
        (CAST(entry_effective_pre_base_atomic AS DOUBLE) /
            pow(10.0, CAST(entry_base_decimals AS DOUBLE))) AS entry_base_reserve,
        (CAST(entry_effective_pre_quote_atomic AS DOUBLE) /
            pow(10.0, CAST(entry_quote_decimals AS DOUBLE))) AS entry_quote_reserve,
        {entry_fee_rate} AS entry_fee_rate
    FROM signals
), entry_sized AS (
    SELECT
        entry_values.*,
        CASE WHEN entry_phase = 'bonding_curve'
             THEN {_number(config.quote_notional)} / (1.0 + entry_fee_rate)
             ELSE {_number(config.quote_notional)} * (1.0 - entry_fee_rate)
        END AS entry_net_quote
    FROM entry_values
), entry_priced AS (
    SELECT
        entry_sized.*,
        (entry_base_reserve -
            (entry_base_reserve * entry_quote_reserve) /
            (entry_quote_reserve + entry_net_quote)) * {_number(adverse_multiplier)}
            AS entry_base_out,
        {_number(config.quote_notional)} / NULLIF(
            (entry_base_reserve -
                (entry_base_reserve * entry_quote_reserve) /
                (entry_quote_reserve + entry_net_quote)) * {_number(adverse_multiplier)},
            0.0) AS entry_average_price
    FROM entry_sized
), future_path AS (
    SELECT
        entry_priced.*,
        future.market_event_number - entry_priced.market_event_number - {latency}
            AS future_offset,
        future.post_spot_price AS future_mark_price,
        future.event_id AS trigger_event_id,
        future.block_time_ns AS trigger_block_time_ns,
        future.coverage_gap AS future_coverage_gap,
        future.reserve_continuity AS future_reserve_continuity,
        future.phase AS future_phase,
        exit_row.event_id AS exit_event_id,
        exit_row.block_time_ns AS exit_block_time_ns,
        exit_row.available_at_ns AS exit_available_at_ns,
        exit_row.phase AS exit_phase,
        exit_row.coverage_gap AS exit_coverage_gap,
        exit_row.reserve_continuity AS exit_reserve_continuity,
        exit_row.effective_pre_base_atomic AS exit_effective_pre_base_atomic,
        exit_row.effective_pre_quote_atomic AS exit_effective_pre_quote_atomic,
        exit_row.base_decimals AS exit_base_decimals,
        exit_row.quote_decimals AS exit_quote_decimals,
        exit_row.fee_quote_atomic AS exit_fee_quote_atomic,
        exit_row.quote_amount_atomic AS exit_quote_amount_atomic,
        exit_row.post_spot_price AS exit_mark_price,
        CASE WHEN future.coverage_gap
                  OR NOT future.reserve_continuity
                  OR future.phase <> entry_priced.entry_phase
             THEN 1 ELSE 0 END AS future_bad,
        CASE WHEN exit_row.event_id IS NULL
                  OR exit_row.coverage_gap
                  OR NOT exit_row.reserve_continuity
                  OR exit_row.phase <> entry_priced.entry_phase
             THEN 1 ELSE 0 END AS exit_bad
    FROM entry_priced AS entry_priced
    INNER JOIN sequenced AS future
        ON future.market_id = entry_priced.market_id
       AND future.market_event_number > entry_priced.market_event_number + {latency}
       AND future.market_event_number <= entry_priced.market_event_number
            + {latency + horizon + latency}
    LEFT JOIN sequenced AS exit_row
        ON exit_row.market_id = future.market_id
       AND exit_row.market_event_number = future.market_event_number + {latency}
), path_flags AS (
    SELECT
        future_path.*,
        max(future_bad) OVER (
            PARTITION BY signal_event_id ORDER BY future_offset
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS bad_to_trigger,
        max(future_bad) OVER (
            PARTITION BY signal_event_id ORDER BY future_offset
            ROWS BETWEEN UNBOUNDED PRECEDING AND {latency} FOLLOWING
        ) AS future_bad_to_exit,
        max(future_mark_price) OVER (
            PARTITION BY signal_event_id ORDER BY future_offset
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_high
    FROM future_path
), classified AS (
    SELECT
        path_flags.*,
        CASE WHEN future_bad_to_exit = 1 OR exit_bad = 1 THEN 1 ELSE 0 END
            AS path_bad_to_exit,
        CASE
            WHEN future_mark_price >= entry_average_price * (1.0 + {_number(profile.take_profit)})
                THEN 'take_profit'
            WHEN future_mark_price <= entry_average_price * (1.0 - {_number(profile.stop_loss)})
                THEN 'stop_loss'
            WHEN running_high > 0.0
                AND future_mark_price / running_high - 1.0 <= -{_number(profile.trailing_stop)}
                THEN 'trailing_stop'
            ELSE NULL
        END AS trigger_reason
    FROM path_flags
), trigger_candidates AS (
    SELECT *
    FROM classified
    WHERE future_offset BETWEEN 1 AND {horizon}
      AND path_bad_to_exit = 0
      AND trigger_reason IS NOT NULL
), triggered AS (
    SELECT *
    FROM trigger_candidates
    QUALIFY row_number() OVER (
        PARTITION BY signal_event_id ORDER BY future_offset
    ) = 1
), time_exits AS (
    SELECT * REPLACE ('time_exit' AS trigger_reason)
    FROM classified
    WHERE future_offset = {horizon}
      AND path_bad_to_exit = 0
      AND NOT EXISTS (
          SELECT 1 FROM triggered
          WHERE triggered.signal_event_id = classified.signal_event_id
      )
), selected_exits AS (
    SELECT * FROM triggered
    UNION ALL
    SELECT * FROM time_exits
), exit_values AS (
    SELECT
        selected_exits.*,
        (CAST(exit_effective_pre_base_atomic AS DOUBLE) /
            pow(10.0, CAST(exit_base_decimals AS DOUBLE))) AS exit_base_reserve,
        (CAST(exit_effective_pre_quote_atomic AS DOUBLE) /
            pow(10.0, CAST(exit_quote_decimals AS DOUBLE))) AS exit_quote_reserve,
        {exit_fee_rate} AS exit_fee_rate
    FROM selected_exits
), quoted AS (
    SELECT
        exit_values.*,
        ({quote_out}) AS quote_out,
        (block_time_ns - launch_time_ns) / 1000000000.0 AS token_age_seconds
    FROM exit_values
), scored AS (
    SELECT
        quoted.*,
        CASE WHEN {valid} THEN TRUE ELSE FALSE END AS eligible_{horizon},
        exit_block_time_ns AS label_end_{horizon}_block_time_ns,
        exit_available_at_ns AS label_end_{horizon}_available_at_ns,
        CASE WHEN {valid}
             THEN quote_out - {_number(config.quote_notional)}
                  - {_number(2.0 * config.fixed_cost_quote)}
             ELSE NULL END AS pnl_{horizon}
    FROM quoted
)
SELECT
    signal_event_id AS event_id,
    market_id,
    mint,
    slot,
    tx_index,
    log_index,
    event_index,
    block_time_ns,
    launch_time_ns,
    token_age_seconds,
    market_event_number,
    first_seen_censored,
    signal_mark_price,
    entry_event_id,
    entry_block_time_ns,
    entry_available_at_ns,
    entry_mark_price,
    entry_average_price,
    entry_base_out,
    trigger_event_id,
    trigger_block_time_ns,
    future_mark_price AS trigger_mark_price,
    exit_event_id,
    exit_block_time_ns,
    exit_available_at_ns,
    exit_mark_price,
    quote_out / NULLIF(entry_base_out, 0.0) AS exit_average_price,
    quote_out,
    entry_fee_rate,
    exit_fee_rate,
    future_offset AS holding_events,
    {output_signal_columns},
    eligible_{horizon},
    label_end_{horizon}_block_time_ns,
    label_end_{horizon}_available_at_ns,
    pnl_{horizon},
    trigger_reason AS exit_reason
FROM scored
""".strip()


__all__ = [
    "EXPERIMENT_ID",
    "EXIT_PROFILES",
    "ExitProfile",
    "PRIOR_ATTEMPTED_CANDIDATES",
    "TAIL_AXES",
    "TAIL_QUANTILES_HIGH",
    "TAIL_QUANTILES_LOW",
    "TailExitCandidate",
    "candidate_payload",
    "candidates",
    "candidate_order_sha256",
    "create_tail_exit_labels_sql",
    "registry_payload",
    "research_announcement",
    "tail_exit_source_columns",
    "trial_denominator",
]
