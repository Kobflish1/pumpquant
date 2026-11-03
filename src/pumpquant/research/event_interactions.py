"""A frozen, one-gate-at-a-time interaction registry for event research.

This module is intentionally small and deliberately *not* a strategy runner.
The event labels and their delayed round-trip outcomes are produced by the
existing event-screen pipeline.  Here we only declare the bounded signal ×
gate hypotheses, fit their two scalar cut-points inside each training fold,
and provide one-pass, token-first aggregate SQL plus row parsers.

The default registry is fixed at 360 candidates:

* six directional event measurements;
* one continuation and one long-only reversal quantile for each measurement;
* three holding horizons (5, 20, and 50 events);
* exactly one of five state/risk gates, each at two registered quantiles.

No interaction contains a second gate, and no outcome/label construction is
implemented here.  ``event_labels`` is an input to the SQL builders.
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
    CONT_OFI_URL,
    INTRADAY_MOMENTUM_URL,
    PUMP_PREDICTION_URL,
    TSMOM_URL,
)
from pumpquant.research.event_screen import EventCandidateMetrics, EventRuleOperator

INTERACTION_HOLDING_EVENTS: tuple[int, ...] = (5, 20, 50)


@dataclass(frozen=True, slots=True)
class EventInteractionSignalAxis:
    """One directional measurement and its registered train quantile."""

    rank: int
    family: str
    feature: str
    operator: EventRuleOperator
    quantiles: tuple[float, ...]
    primary_source: str
    rationale: str

    def __post_init__(self) -> None:
        if self.rank < 1 or not self.family.strip() or not self.feature.strip():
            raise ValueError("invalid event interaction signal axis")
        if not self.quantiles or len(set(self.quantiles)) != len(self.quantiles):
            raise ValueError("interaction signal quantiles must be non-empty and unique")
        if any(not math.isfinite(value) or not 0.0 < value < 1.0 for value in self.quantiles):
            raise ValueError("interaction signal quantiles must be inside (0, 1)")
        if not self.primary_source.strip() or not self.rationale.strip():
            raise ValueError("interaction signal evidence metadata is required")

    @property
    def axis(self) -> EventInteractionSignalAxis:
        """Self alias for code that treats a signal as a nested candidate axis."""

        return self

    @property
    def quantile(self) -> float:
        """The sole registered signal quantile."""

        if len(self.quantiles) != 1:
            raise ValueError("interaction signal axis has multiple quantiles")
        return self.quantiles[0]

    @property
    def thresholds(self) -> tuple[float, ...]:
        return self.quantiles


@dataclass(frozen=True, slots=True)
class EventInteractionGateAxis:
    """One non-directional state gate; it is never combined with another gate."""

    family: str
    feature: str
    operator: EventRuleOperator
    quantiles: tuple[float, ...]
    primary_source: str
    rationale: str
    requires_uncensored_wallet_history: bool = False

    def __post_init__(self) -> None:
        if not self.family.strip() or not self.feature.strip():
            raise ValueError("invalid event interaction gate axis")
        if not self.quantiles or len(set(self.quantiles)) != len(self.quantiles):
            raise ValueError("interaction gate quantiles must be non-empty and unique")
        if any(not math.isfinite(value) or not 0.0 < value < 1.0 for value in self.quantiles):
            raise ValueError("interaction gate quantiles must be inside (0, 1)")
        if not self.primary_source.strip() or not self.rationale.strip():
            raise ValueError("interaction gate evidence metadata is required")

    @property
    def axis(self) -> EventInteractionGateAxis:
        return self

    @property
    def thresholds(self) -> tuple[float, ...]:
        return self.quantiles


def _signal_pair(
    rank: int,
    family: str,
    feature: str,
    primary_source: str,
    rationale: str,
) -> tuple[EventInteractionSignalAxis, EventInteractionSignalAxis]:
    """Build the only two allowed directional polarities for one feature."""

    return (
        EventInteractionSignalAxis(
            rank=rank,
            family=f"{family}_continuation",
            feature=feature,
            operator=EventRuleOperator.GREATER_EQUAL,
            quantiles=(0.85,),
            primary_source=primary_source,
            rationale=rationale,
        ),
        EventInteractionSignalAxis(
            rank=rank,
            family=f"{family}_reversal",
            feature=feature,
            operator=EventRuleOperator.LESS_EQUAL,
            quantiles=(0.15,),
            primary_source=primary_source,
            rationale=rationale,
        ),
    )


# Keep this tuple explicit and ordered.  The order is part of the stable wide
# SQL column contract (families are sorted again by the registry generator so
# that caller-provided axis sequences cannot silently perturb identities).
DEFAULT_EVENT_INTERACTION_SIGNAL_AXES: tuple[EventInteractionSignalAxis, ...] = sum(
    (
        _signal_pair(
            1,
            "signed_quote_imbalance_5e",
            "event_signed_quote_imbalance_5e",
            CONT_OFI_URL,
            "Exact buy-minus-sell quote imbalance over the five prior causal events.",
        ),
        _signal_pair(
            2,
            "signed_quote_imbalance_20e",
            "event_signed_quote_imbalance_20e",
            CONT_OFI_URL,
            "Exact buy-minus-sell quote imbalance over the twenty prior causal events.",
        ),
        _signal_pair(
            3,
            "signed_flow_acceleration_5e_20e",
            "event_signed_flow_acceleration_5e_20e",
            CONT_OFI_URL,
            "Five-event signed quote flow minus its twenty-event state.",
        ),
        _signal_pair(
            4,
            "reserve_momentum_5e",
            "event_reserve_momentum_5e",
            INTRADAY_MOMENTUM_URL,
            "Five-event reserve-mark log momentum measured causally at the event.",
        ),
        _signal_pair(
            5,
            "reserve_momentum_20e",
            "event_reserve_momentum_20e",
            TSMOM_URL,
            "Twenty-event reserve-mark log momentum measured causally at the event.",
        ),
        _signal_pair(
            6,
            "rolling_vwap_deviation_20e",
            "event_rolling_vwap_deviation_20e",
            INTRADAY_MOMENTUM_URL,
            "Reserve mark relative to the exact causal twenty-event quote/base VWAP.",
        ),
    ),
    (),
)


DEFAULT_EVENT_INTERACTION_GATE_AXES: tuple[EventInteractionGateAxis, ...] = (
    EventInteractionGateAxis(
        family="bonding_progress",
        feature="bonding_progress",
        operator=EventRuleOperator.GREATER_EQUAL,
        quantiles=(0.50, 0.75),
        primary_source=PUMP_PREDICTION_URL,
        rationale="Require a sufficiently advanced bonding-curve state.",
    ),
    EventInteractionGateAxis(
        family="gross_directional_impact",
        feature="gross_directional_impact_bps",
        operator=EventRuleOperator.LESS_EQUAL,
        quantiles=(0.50, 0.25),
        primary_source=CONT_OFI_URL,
        rationale="Bound the gross directional price impact of the current event.",
    ),
    EventInteractionGateAxis(
        family="event_volatility_ratio",
        feature="event_volatility_ratio_5e_50e",
        operator=EventRuleOperator.LESS_EQUAL,
        quantiles=(0.50, 0.25),
        primary_source=INTRADAY_MOMENTUM_URL,
        rationale="Reject states whose short-event volatility is unusually elevated.",
    ),
    EventInteractionGateAxis(
        family="wallet_hhi",
        feature="wallet_hhi",
        operator=EventRuleOperator.LESS_EQUAL,
        quantiles=(0.50, 0.25),
        primary_source=PUMP_PREDICTION_URL,
        rationale="Bound quote-flow concentration and require an uncensored wallet history.",
        requires_uncensored_wallet_history=True,
    ),
    EventInteractionGateAxis(
        family="creator_dump_pressure",
        feature="creator_dump_pressure",
        operator=EventRuleOperator.LESS_EQUAL,
        quantiles=(0.50, 0.25),
        primary_source=PUMP_PREDICTION_URL,
        rationale="Avoid states in which creator selling dominates recent flow.",
    ),
)


# Short aliases make the registry easy to discover while preserving the
# explicit ``*_AXES`` names used by the event-research module.
DEFAULT_EVENT_INTERACTION_SIGNALS = DEFAULT_EVENT_INTERACTION_SIGNAL_AXES
DEFAULT_EVENT_INTERACTION_GATES = DEFAULT_EVENT_INTERACTION_GATE_AXES
# The shorter constants are useful to callers that do not use the event-
# research module's ``*_AXES`` naming.  They are aliases to the same immutable
# tuples, not separately configurable registries.
DEFAULT_INTERACTION_SIGNAL_AXES = DEFAULT_EVENT_INTERACTION_SIGNAL_AXES
DEFAULT_INTERACTION_GATE_AXES = DEFAULT_EVENT_INTERACTION_GATE_AXES
DEFAULT_INTERACTION_SIGNALS = DEFAULT_EVENT_INTERACTION_SIGNAL_AXES
DEFAULT_INTERACTION_GATES = DEFAULT_EVENT_INTERACTION_GATE_AXES
DEFAULT_EVENT_INTERACTION_AXES = DEFAULT_EVENT_INTERACTION_SIGNAL_AXES


@dataclass(frozen=True, slots=True)
class EventInteractionCandidate:
    """Stable signal × exactly-one-gate identity; thresholds are fold-specific."""

    signal_axis: EventInteractionSignalAxis
    signal_quantile: float
    gate_axis: EventInteractionGateAxis
    gate_quantile: float
    holding_events: int

    def __post_init__(self) -> None:
        if self.signal_quantile not in self.signal_axis.quantiles:
            raise ValueError("candidate signal quantile is not registered")
        if self.gate_quantile not in self.gate_axis.quantiles:
            raise ValueError("candidate gate quantile is not registered")
        if self.holding_events < 1:
            raise ValueError("candidate holding horizon must be positive")

    @property
    def signal(self) -> EventInteractionSignalAxis:
        """Alias matching the terminology used by the gated-screen API."""

        return self.signal_axis

    @property
    def axis(self) -> EventInteractionSignalAxis:
        """Alias matching :class:`EventResearchCandidate`."""

        return self.signal_axis

    @property
    def gate(self) -> EventInteractionGateAxis:
        return self.gate_axis

    @property
    def quantile(self) -> float:
        """Signal quantile alias for code shared with event-research candidates."""

        return self.signal_quantile

    @property
    def signal_threshold(self) -> float:
        """The registered signal quantile (the fitted value is fold-specific)."""

        return self.signal_quantile

    @property
    def gate_threshold(self) -> float:
        """The registered gate quantile (the fitted value is fold-specific)."""

        return self.gate_quantile

    @property
    def name(self) -> str:
        return (
            f"event-interaction:{self.signal_axis.family}:"
            f"{self.signal_axis.operator.value}:q-{self.signal_quantile:g}:"
            f"hold-{self.holding_events}:gate:{self.gate_axis.family}:"
            f"{self.gate_axis.operator.value}:q-{self.gate_quantile:g}"
        )

    @property
    def sha256(self) -> str:
        payload = {
            "signal_axis": asdict(self.signal_axis),
            "signal_quantile": self.signal_quantile,
            "gate_axis": asdict(self.gate_axis),
            "gate_quantile": self.gate_quantile,
            "holding_events": self.holding_events,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class EventInteractionThreshold:
    """Two fold-fitted cut-points for one registered interaction."""

    candidate_sha256: str
    fold: int
    signal_threshold: float | None
    gate_threshold: float | None

    @property
    def signal_value(self) -> float | None:
        return self.signal_threshold

    @property
    def gate_value(self) -> float | None:
        return self.gate_threshold


# Additional names are intentionally aliases, not duplicate classes.  They
# let callers use either the descriptive interaction terminology or the
# shorter registry terminology without changing identity/serialization.
EventInteractionAxis = EventInteractionSignalAxis
EventInteractionSignal = EventInteractionSignalAxis
EventInteractionGate = EventInteractionGateAxis
InteractionSignalAxis = EventInteractionSignalAxis
InteractionGateAxis = EventInteractionGateAxis
InteractionCandidate = EventInteractionCandidate
InteractionThreshold = EventInteractionThreshold
EventInteractionMetrics = EventCandidateMetrics


def default_event_interaction_candidates(
    holding_events: Sequence[int] = INTERACTION_HOLDING_EVENTS,
    signal_axes: Sequence[EventInteractionSignalAxis] = DEFAULT_EVENT_INTERACTION_SIGNAL_AXES,
    gate_axes: Sequence[EventInteractionGateAxis] = DEFAULT_EVENT_INTERACTION_GATE_AXES,
) -> tuple[EventInteractionCandidate, ...]:
    """Return the frozen signal × one-gate × quantile × horizon registry."""

    horizons = tuple(holding_events)
    if (
        not horizons
        or tuple(sorted(set(horizons))) != horizons
        or any(isinstance(value, bool) or value < 1 for value in horizons)
    ):
        raise ValueError("holding horizons must be positive, unique, and sorted")
    if not signal_axes or not gate_axes:
        raise ValueError("interaction signal and gate axes cannot be empty")

    sorted_signals = tuple(sorted(signal_axes, key=lambda item: (item.rank, item.family)))
    sorted_gates = tuple(sorted(gate_axes, key=lambda item: item.family))
    result = tuple(
        EventInteractionCandidate(
            signal_axis=signal,
            signal_quantile=signal_quantile,
            gate_axis=gate,
            gate_quantile=gate_quantile,
            holding_events=horizon,
        )
        for horizon in horizons
        for signal in sorted_signals
        for signal_quantile in signal.quantiles
        for gate in sorted_gates
        for gate_quantile in gate.quantiles
    )
    hashes = [candidate.sha256 for candidate in result]
    if len(set(hashes)) != len(hashes):
        raise AssertionError("event interaction candidate registry contains duplicates")
    return result


def event_interaction_signal_features(
    signal_axes: Sequence[EventInteractionSignalAxis] = DEFAULT_EVENT_INTERACTION_SIGNAL_AXES,
    gate_axes: Sequence[EventInteractionGateAxis] = DEFAULT_EVENT_INTERACTION_GATE_AXES,
) -> tuple[str, ...]:
    """Return all source features needed by ``create_event_labels_sql``."""

    return tuple(
        sorted(
            {axis.feature for axis in signal_axes}
            | {axis.feature for axis in gate_axes}
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


def _selected_candidates(
    candidates: Sequence[EventInteractionCandidate], holding_events: int
) -> tuple[EventInteractionCandidate, ...]:
    selected = tuple(
        candidate for candidate in candidates if candidate.holding_events == holding_events
    )
    if not selected:
        raise ValueError("no interaction candidates exist for requested holding horizon")
    return selected


def _wallet_condition(candidate: EventInteractionCandidate, prefix: str = "labels") -> str:
    if candidate.gate_axis.requires_uncensored_wallet_history:
        return f" AND NOT {prefix}.first_seen_censored"
    return ""


def _causal_membership_condition(candidate: EventInteractionCandidate, holding_events: int) -> str:
    """Condition shared by both threshold fitting and aggregate application."""

    signal = _identifier(candidate.signal_axis.feature)
    gate = _identifier(candidate.gate_axis.feature)
    return (
        f"labels.eligible_{holding_events} "
        f"AND labels.label_end_{holding_events}_available_at_ns < memberships.cutoff_ns "
        f"AND labels.{signal} IS NOT NULL "
        f"AND labels.{gate} IS NOT NULL"
        f"{_wallet_condition(candidate)}"
    )


def create_event_interaction_thresholds_sql(
    candidates: Sequence[EventInteractionCandidate],
    holding_events: int,
    *,
    output_table: str | None = None,
    labels_table: str = "event_labels",
) -> str:
    """Fit signal and gate quantiles on eligible training labels only.

    A separate threshold column pair is emitted for every candidate.  The
    strict label-end/cutoff condition is repeated in each ``FILTER`` so a
    candidate can never borrow a value from a future fold or from an ineligible
    row.  Test rows are excluded by the outer ``WHERE`` as well.
    """

    selected = _selected_candidates(candidates, holding_events)
    labels = _identifier(labels_table)
    output = _identifier(
        f"event_interaction_thresholds_{holding_events}" if output_table is None else output_table
    )
    columns: list[str] = []
    for index, candidate in enumerate(selected):
        prefix = f"c{index:03d}"
        signal = _identifier(candidate.signal_axis.feature)
        gate = _identifier(candidate.gate_axis.feature)
        condition = _causal_membership_condition(candidate, holding_events)
        columns.extend(
            (
                f"quantile_cont(labels.{signal}, {_number(candidate.signal_quantile)}) "
                f"FILTER (WHERE {condition}) AS {prefix}_signal_threshold",
                f"quantile_cont(labels.{gate}, {_number(candidate.gate_quantile)}) "
                f"FILTER (WHERE {condition}) AS {prefix}_gate_threshold",
            )
        )
    column_sql = ",\n        ".join(columns)
    return f"""
CREATE TABLE {output} AS
SELECT
        memberships.fold,
        {column_sql}
FROM {labels} AS labels
INNER JOIN memberships USING (mint)
WHERE memberships.split = 'train'
GROUP BY memberships.fold
ORDER BY memberships.fold
""".strip()


def event_interaction_aggregate_sql(
    candidates: Sequence[EventInteractionCandidate],
    holding_events: int,
    *,
    thresholds_table: str | None = None,
    labels_table: str = "event_labels",
) -> str:
    """Apply fold thresholds and aggregate outcomes by whole token first."""

    selected = _selected_candidates(candidates, holding_events)
    labels = _identifier(labels_table)
    thresholds = _identifier(
        f"event_interaction_thresholds_{holding_events}"
        if thresholds_table is None
        else thresholds_table
    )
    inner: list[str] = []
    outer: list[str] = []
    for index, candidate in enumerate(selected):
        prefix = f"c{index:03d}"
        signal = _identifier(candidate.signal_axis.feature)
        gate = _identifier(candidate.gate_axis.feature)
        signal_operator = (
            ">=" if candidate.signal_axis.operator is EventRuleOperator.GREATER_EQUAL else "<="
        )
        gate_operator = (
            ">=" if candidate.gate_axis.operator is EventRuleOperator.GREATER_EQUAL else "<="
        )
        # A test row is an unseen outcome and is therefore allowed to finish
        # after the fold cutoff.  Training rows retain the strict causal bound.
        cutoff = (
            "(memberships.split = 'test' OR "
            f"labels.label_end_{holding_events}_available_at_ns < memberships.cutoff_ns)"
        )
        condition = (
            f"labels.eligible_{holding_events} AND {cutoff} "
            f"AND labels.{signal} IS NOT NULL AND labels.{gate} IS NOT NULL "
            f"AND thresholds.{prefix}_signal_threshold IS NOT NULL "
            f"AND thresholds.{prefix}_gate_threshold IS NOT NULL "
            f"AND labels.{signal} {signal_operator} thresholds.{prefix}_signal_threshold "
            f"AND labels.{gate} {gate_operator} thresholds.{prefix}_gate_threshold"
            f"{_wallet_condition(candidate)}"
        )
        inner.extend(
            (
                f"count(*) FILTER (WHERE {condition}) AS {prefix}_n",
                f"sum(labels.pnl_{holding_events}) FILTER (WHERE {condition}) AS {prefix}_sum",
            )
        )
        outer.extend(
            (
                f"coalesce(sum({prefix}_n), 0)::BIGINT AS {prefix}_outcomes",
                f"count(*) FILTER (WHERE {prefix}_n > 0)::BIGINT AS {prefix}_mints",
                f"coalesce(sum({prefix}_sum), 0.0) AS {prefix}_total_pnl",
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
    INNER JOIN memberships USING (mint)
    INNER JOIN {thresholds} AS thresholds USING (fold)
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


def parse_event_interaction_threshold_rows(
    rows: Sequence[Mapping[str, object]],
    candidates: Sequence[EventInteractionCandidate],
    holding_events: int,
) -> tuple[EventInteractionThreshold, ...]:
    """Parse wide fold threshold rows into stable candidate identities."""

    selected = _selected_candidates(candidates, holding_events)
    result: list[EventInteractionThreshold] = []

    def optional_threshold(value: object, name: str) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, Real):
            raise ValueError(f"{name} is not numeric")
        number = float(value)
        if not math.isfinite(number):
            raise ValueError(f"{name} is not finite")
        return number

    for row in rows:
        fold_value = row.get("fold")
        if isinstance(fold_value, bool) or not isinstance(fold_value, Real):
            raise ValueError("fold is not numeric")
        fold_float = float(fold_value)
        if not math.isfinite(fold_float) or not fold_float.is_integer():
            raise ValueError("fold is not a finite integer")
        for index, candidate in enumerate(selected):
            prefix = f"c{index:03d}"
            result.append(
                EventInteractionThreshold(
                    candidate_sha256=candidate.sha256,
                    fold=int(fold_float),
                    signal_threshold=optional_threshold(
                        row.get(f"{prefix}_signal_threshold"), "signal threshold"
                    ),
                    gate_threshold=optional_threshold(
                        row.get(f"{prefix}_gate_threshold"), "gate threshold"
                    ),
                )
            )
    return tuple(result)


def parse_event_interaction_aggregate_rows(
    rows: Sequence[Mapping[str, object]],
    candidates: Sequence[EventInteractionCandidate],
    holding_events: int,
) -> tuple[EventCandidateMetrics, ...]:
    """Convert the wide interaction aggregate into auditable metric rows."""

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
                )
            )
    return tuple(result)


# Function aliases for clients that use the shorter interaction prefix.
create_interaction_thresholds_sql = create_event_interaction_thresholds_sql
interaction_aggregate_sql = event_interaction_aggregate_sql
parse_interaction_threshold_rows = parse_event_interaction_threshold_rows
parse_interaction_aggregate_rows = parse_event_interaction_aggregate_rows
default_interaction_candidates = default_event_interaction_candidates
interaction_signal_features = event_interaction_signal_features


__all__ = [
    "DEFAULT_EVENT_INTERACTION_GATES",
    "DEFAULT_EVENT_INTERACTION_AXES",
    "DEFAULT_EVENT_INTERACTION_SIGNAL_AXES",
    "DEFAULT_EVENT_INTERACTION_SIGNALS",
    "DEFAULT_EVENT_INTERACTION_GATE_AXES",
    "DEFAULT_INTERACTION_GATE_AXES",
    "DEFAULT_INTERACTION_GATES",
    "DEFAULT_INTERACTION_SIGNAL_AXES",
    "DEFAULT_INTERACTION_SIGNALS",
    "EventInteractionAxis",
    "EventInteractionCandidate",
    "EventInteractionGate",
    "EventInteractionGateAxis",
    "EventInteractionSignal",
    "EventInteractionSignalAxis",
    "EventInteractionThreshold",
    "EventInteractionMetrics",
    "INTERACTION_HOLDING_EVENTS",
    "InteractionCandidate",
    "InteractionGateAxis",
    "InteractionSignalAxis",
    "InteractionThreshold",
    "create_event_interaction_thresholds_sql",
    "create_interaction_thresholds_sql",
    "default_event_interaction_candidates",
    "default_interaction_candidates",
    "event_interaction_aggregate_sql",
    "event_interaction_signal_features",
    "interaction_aggregate_sql",
    "interaction_signal_features",
    "parse_event_interaction_aggregate_rows",
    "parse_event_interaction_threshold_rows",
    "parse_interaction_aggregate_rows",
    "parse_interaction_threshold_rows",
]
