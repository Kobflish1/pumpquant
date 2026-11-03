"""Pre-registered event-level alpha screening over causal microstructure rows.

This is the information-screen tier between event feature materialization and
the slower strategy replay.  It evaluates one directional measurement at a
time, uses delayed constant-product round-trip labels, samples non-overlapping
event horizons, and aggregates outcomes by whole token before selection.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from numbers import Real
from statistics import NormalDist

from pumpquant.features.alpha import (
    CONT_OFI_URL,
    HAWKES_URL,
    PUMP_PREDICTION_URL,
    VPIN_URL,
)


class EventRuleOperator(StrEnum):
    """Supported transparent event-feature comparisons."""

    GREATER_EQUAL = "ge"
    LESS_EQUAL = "le"


@dataclass(frozen=True, slots=True)
class EventSignalAxis:
    """One bounded directional hypothesis family and its fixed thresholds."""

    rank: int
    family: str
    feature: str
    operator: EventRuleOperator
    thresholds: tuple[float, ...]
    primary_source: str
    rationale: str
    requires_uncensored_wallet_history: bool = False

    def __post_init__(self) -> None:
        if self.rank < 1 or not self.family.strip() or not self.feature.strip():
            raise ValueError("invalid event signal axis")
        if not self.thresholds or not all(math.isfinite(value) for value in self.thresholds):
            raise ValueError("event signal thresholds must be finite and non-empty")
        if len(set(self.thresholds)) != len(self.thresholds):
            raise ValueError("event signal thresholds must be unique")
        if not self.primary_source.strip() or not self.rationale.strip():
            raise ValueError("event signal evidence metadata is required")


@dataclass(frozen=True, slots=True)
class EventRiskGateAxis:
    """A non-directional event state reserved for survivor-only testing."""

    family: str
    feature: str
    operator: EventRuleOperator
    thresholds: tuple[float, ...]
    primary_source: str
    rationale: str

    def __post_init__(self) -> None:
        EventSignalAxis(
            rank=1,
            family=self.family,
            feature=self.feature,
            operator=self.operator,
            thresholds=self.thresholds,
            primary_source=self.primary_source,
            rationale=self.rationale,
        )


DEFAULT_EVENT_SIGNAL_AXES: tuple[EventSignalAxis, ...] = (
    EventSignalAxis(
        1,
        "arrival_flow_continuation",
        "arrival_flow_imbalance",
        EventRuleOperator.GREATER_EQUAL,
        (0.25, 0.50, 0.75),
        CONT_OFI_URL,
        "Recent buy arrivals dominate sell arrivals after the current event.",
    ),
    EventSignalAxis(
        1,
        "arrival_flow_reversal",
        "arrival_flow_imbalance",
        EventRuleOperator.LESS_EQUAL,
        (-0.25, -0.50, -0.75),
        CONT_OFI_URL,
        "Long-only rebound test after sell-arrival dominance.",
    ),
    EventSignalAxis(
        1,
        "marked_hawkes_continuation",
        "hawkes_signed_quote_pre",
        EventRuleOperator.GREATER_EQUAL,
        (0.5, 2.0, 8.0),
        HAWKES_URL,
        "Pre-event exponentially decayed signed quote excitation is positive.",
    ),
    EventSignalAxis(
        1,
        "marked_hawkes_reversal",
        "hawkes_signed_quote_pre",
        EventRuleOperator.LESS_EQUAL,
        (-0.5, -2.0, -8.0),
        HAWKES_URL,
        "Long-only rebound test after negative pre-event signed excitation.",
    ),
    EventSignalAxis(
        1,
        "exact_vpin_continuation",
        "signed_vpin",
        EventRuleOperator.GREATER_EQUAL,
        (0.25, 0.50, 0.80),
        VPIN_URL,
        "Exact equal-quote-volume buckets show persistent net buying.",
    ),
    EventSignalAxis(
        1,
        "exact_vpin_reversal",
        "signed_vpin",
        EventRuleOperator.LESS_EQUAL,
        (-0.25, -0.50, -0.80),
        VPIN_URL,
        "Long-only rebound test after exact-bucket net selling.",
    ),
    EventSignalAxis(
        2,
        "reserve_mark_continuation",
        "reserve_mark_return",
        EventRuleOperator.GREATER_EQUAL,
        (0.001, 0.005, 0.020),
        PUMP_PREDICTION_URL,
        "The current trade advanced the causal reserve mark.",
    ),
    EventSignalAxis(
        2,
        "reserve_mark_reversal",
        "reserve_mark_return",
        EventRuleOperator.LESS_EQUAL,
        (-0.001, -0.005, -0.020),
        PUMP_PREDICTION_URL,
        "Long-only rebound test after a reserve-mark decline.",
    ),
    EventSignalAxis(
        2,
        "reserve_flow_acceleration",
        "reserve_quote_acceleration",
        EventRuleOperator.GREATER_EQUAL,
        (0.001, 0.005, 0.015),
        CONT_OFI_URL,
        "Reserve quote-flow change accelerated relative to the prior event.",
    ),
    EventSignalAxis(
        2,
        "reserve_flow_exhaustion",
        "reserve_quote_acceleration",
        EventRuleOperator.LESS_EQUAL,
        (-0.001, -0.005, -0.015),
        CONT_OFI_URL,
        "Long-only rebound test after reserve quote-flow deceleration.",
    ),
    EventSignalAxis(
        2,
        "global_quote_flow_continuation",
        "global_signed_quote_imbalance_5s",
        EventRuleOperator.GREATER_EQUAL,
        (0.10, 0.30, 0.60),
        CONT_OFI_URL,
        "SOL buying dominates selling across all concurrently young launch markets.",
    ),
    EventSignalAxis(
        2,
        "global_quote_flow_reversal",
        "global_signed_quote_imbalance_5s",
        EventRuleOperator.LESS_EQUAL,
        (-0.10, -0.30, -0.60),
        CONT_OFI_URL,
        "Long-only rebound test during broad selling across young launch markets.",
    ),
    EventSignalAxis(
        2,
        "cross_token_attention_share",
        "market_quote_share_global_5s",
        EventRuleOperator.GREATER_EQUAL,
        (0.01, 0.05, 0.20),
        PUMP_PREDICTION_URL,
        "The current market captures a large causal share of young-token SOL flow.",
    ),
    EventSignalAxis(
        3,
        "new_wallet_expansion",
        "new_wallet_flow_share",
        EventRuleOperator.GREATER_EQUAL,
        (0.25, 0.50, 0.80),
        PUMP_PREDICTION_URL,
        "A broadening cohort of wallets supplies recent quote flow.",
        requires_uncensored_wallet_history=True,
    ),
    EventSignalAxis(
        3,
        "repeat_wallet_conviction",
        "repeat_wallet_flow_share",
        EventRuleOperator.GREATER_EQUAL,
        (0.50, 0.75, 0.90),
        PUMP_PREDICTION_URL,
        "Previously observed wallets supply persistent recent quote flow.",
        requires_uncensored_wallet_history=True,
    ),
)


DEFAULT_EVENT_RISK_GATES: tuple[EventRiskGateAxis, ...] = (
    EventRiskGateAxis(
        "toxicity",
        "vpin",
        EventRuleOperator.LESS_EQUAL,
        (0.60, 0.80),
        VPIN_URL,
        "Exact VPIN is a survivor-only toxicity gate, not standalone direction.",
    ),
    EventRiskGateAxis(
        "wallet_concentration",
        "wallet_hhi",
        EventRuleOperator.LESS_EQUAL,
        (0.10, 0.20, 0.40),
        PUMP_PREDICTION_URL,
        "Reject recent quote flow dominated by too few wallets.",
    ),
    EventRiskGateAxis(
        "top_wallet_share",
        "wallet_top1_share",
        EventRuleOperator.LESS_EQUAL,
        (0.20, 0.40, 0.60),
        PUMP_PREDICTION_URL,
        "Bound the largest recent wallet's share of quote flow.",
    ),
    EventRiskGateAxis(
        "unknown_wallet_flow",
        "unknown_trader_flow_share",
        EventRuleOperator.LESS_EQUAL,
        (0.10, 0.30),
        PUMP_PREDICTION_URL,
        "Require sufficient wallet attribution for participant features.",
    ),
    EventRiskGateAxis(
        "creator_dump",
        "creator_dump_pressure",
        EventRuleOperator.LESS_EQUAL,
        (0.05, 0.20),
        PUMP_PREDICTION_URL,
        "Avoid states in which creator selling dominates recent flow.",
    ),
    EventRiskGateAxis(
        "global_activity_burst",
        "global_activity_ratio_1s_30s",
        EventRuleOperator.LESS_EQUAL,
        (2.0, 5.0, 10.0),
        HAWKES_URL,
        "Bound execution risk when young-token event activity abruptly crowds the chain.",
    ),
)


@dataclass(frozen=True, slots=True)
class EventScreenConfig:
    """Execution label, cohort, and activity controls for the first screen."""

    latency_events: int = 2
    holding_events: tuple[int, ...] = (5, 20, 50)
    quote_notional: float = 0.10
    fallback_fee_bps: float = 150.0
    adverse_selection_bps: float = 30.0
    fixed_cost_quote: float = 0.00016
    max_reserve_fraction: float = 0.0025
    min_age_seconds: float = 5.0
    max_age_seconds: float = 600.0
    min_train_tokens: int = 2_000
    max_train_tokens: int = 3_000
    test_tokens: int = 3_000
    embargo_ns: int = 660_000_000_000
    minimum_train_outcomes: int = 500
    minimum_train_mints: int = 100
    require_observed_fee: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.require_observed_fee, bool):
            raise ValueError("require_observed_fee must be boolean")
        integer_values = (
            self.latency_events,
            *self.holding_events,
            self.min_train_tokens,
            self.max_train_tokens,
            self.test_tokens,
            self.embargo_ns,
            self.minimum_train_outcomes,
            self.minimum_train_mints,
        )
        if any(isinstance(value, bool) or not isinstance(value, int) for value in integer_values):
            raise ValueError("event screen count and timing fields must be integers")
        if self.latency_events < 1:
            raise ValueError("latency_events must be positive")
        if (
            not self.holding_events
            or any(value < 1 for value in self.holding_events)
            or tuple(sorted(set(self.holding_events))) != self.holding_events
        ):
            raise ValueError("holding_events must be positive, unique, and sorted")
        numeric = (
            self.quote_notional,
            self.fallback_fee_bps,
            self.adverse_selection_bps,
            self.fixed_cost_quote,
            self.max_reserve_fraction,
            self.min_age_seconds,
            self.max_age_seconds,
        )
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in numeric):
            raise ValueError("event screen numeric values must be real numbers")
        if not all(math.isfinite(value) for value in numeric):
            raise ValueError("event screen numeric values must be finite")
        if self.quote_notional <= 0.0 or not 0.0 <= self.fallback_fee_bps < 10_000.0:
            raise ValueError("invalid event screen notional or fee")
        if self.adverse_selection_bps < 0.0 or self.fixed_cost_quote < 0.0:
            raise ValueError("event screen costs cannot be negative")
        if not 0.0 < self.max_reserve_fraction <= 1.0:
            raise ValueError("max_reserve_fraction must be in (0, 1]")
        if self.min_age_seconds < 0.0 or self.max_age_seconds <= self.min_age_seconds:
            raise ValueError("invalid event screen age range")
        integers = (
            self.min_train_tokens,
            self.max_train_tokens,
            self.test_tokens,
            self.minimum_train_outcomes,
            self.minimum_train_mints,
        )
        if any(value < 1 for value in integers):
            raise ValueError("event screen cohort and activity counts must be positive")
        if self.max_train_tokens < self.min_train_tokens:
            raise ValueError("max_train_tokens cannot be smaller than min_train_tokens")
        if self.embargo_ns < 0:
            raise ValueError("embargo_ns cannot be negative")


@dataclass(frozen=True, slots=True)
class EventCandidate:
    axis: EventSignalAxis
    threshold: float
    holding_events: int

    def __post_init__(self) -> None:
        if self.threshold not in self.axis.thresholds:
            raise ValueError("candidate threshold is not pre-registered on its axis")
        if self.holding_events < 1:
            raise ValueError("candidate holding_events must be positive")

    @property
    def name(self) -> str:
        return (
            f"event:{self.axis.family}:{self.axis.operator.value}:"
            f"{self.threshold:g}:hold-{self.holding_events}"
        )

    @property
    def sha256(self) -> str:
        payload = {
            "axis": asdict(self.axis),
            "threshold": self.threshold,
            "holding_events": self.holding_events,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class EventGatedCandidate:
    """One promoted single signal plus exactly one pre-registered risk gate."""

    signal: EventCandidate
    gate: EventRiskGateAxis
    gate_threshold: float

    def __post_init__(self) -> None:
        if self.gate_threshold not in self.gate.thresholds:
            raise ValueError("gate threshold is not pre-registered on its axis")

    @property
    def name(self) -> str:
        return (
            f"{self.signal.name}:gate:{self.gate.family}:"
            f"{self.gate.operator.value}:{self.gate_threshold:g}"
        )

    @property
    def sha256(self) -> str:
        payload = {
            "signal_sha256": self.signal.sha256,
            "gate": asdict(self.gate),
            "gate_threshold": self.gate_threshold,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


def default_event_candidates(
    config: EventScreenConfig | None = None,
    axes: Sequence[EventSignalAxis] = DEFAULT_EVENT_SIGNAL_AXES,
) -> tuple[EventCandidate, ...]:
    """Build the bounded single-signal × threshold × horizon registry."""

    selected = EventScreenConfig() if config is None else config
    result = [
        EventCandidate(axis, threshold, horizon)
        for horizon in selected.holding_events
        for axis in sorted(axes, key=lambda item: (item.rank, item.family))
        for threshold in axis.thresholds
    ]
    hashes = [candidate.sha256 for candidate in result]
    if len(set(hashes)) != len(hashes):
        raise AssertionError("event candidate registry contains duplicates")
    return tuple(result)


def default_event_gate_candidates(
    signal: EventCandidate,
    gates: Sequence[EventRiskGateAxis] = DEFAULT_EVENT_RISK_GATES,
) -> tuple[EventGatedCandidate, ...]:
    """Build the bounded one-gate-at-a-time registry for a genuine survivor."""

    result = tuple(
        EventGatedCandidate(signal, gate, threshold)
        for gate in sorted(gates, key=lambda item: item.family)
        for threshold in gate.thresholds
    )
    hashes = [candidate.sha256 for candidate in result]
    if len(set(hashes)) != len(hashes):
        raise AssertionError("event gate registry contains duplicates")
    return result


@dataclass(frozen=True, slots=True)
class LaunchCohort:
    mint: str
    launch_time_ns: int

    def __post_init__(self) -> None:
        if not self.mint.strip() or self.launch_time_ns < 0:
            raise ValueError("invalid launch cohort")


@dataclass(frozen=True, slots=True)
class EventCohortFold:
    number: int
    train: tuple[LaunchCohort, ...]
    test: tuple[LaunchCohort, ...]
    test_start_ns: int
    train_cutoff_ns: int


def event_cohort_folds(
    launches: Mapping[str, int],
    selected_mints: Iterable[str],
    config: EventScreenConfig | None = None,
) -> tuple[EventCohortFold, ...]:
    """Create rolling whole-token folds with a launch-time embargo."""

    selected = EventScreenConfig() if config is None else config
    unique_mints = set(selected_mints)
    cohorts = sorted(
        (LaunchCohort(mint, launch) for mint, launch in launches.items() if mint in unique_mints),
        key=lambda item: (item.launch_time_ns, item.mint),
    )
    folds: list[EventCohortFold] = []
    test_start = selected.min_train_tokens
    while test_start + selected.test_tokens <= len(cohorts):
        test = tuple(cohorts[test_start : test_start + selected.test_tokens])
        test_start_ns = test[0].launch_time_ns
        cutoff = max(0, test_start_ns - selected.embargo_ns)
        eligible_train = [
            cohort for cohort in cohorts[:test_start] if cohort.launch_time_ns <= cutoff
        ]
        train = tuple(eligible_train[-selected.max_train_tokens :])
        if len(train) >= selected.min_train_tokens:
            folds.append(
                EventCohortFold(
                    number=len(folds) + 1,
                    train=train,
                    test=test,
                    test_start_ns=test_start_ns,
                    train_cutoff_ns=cutoff,
                )
            )
        test_start += selected.test_tokens
    return tuple(folds)


_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier {value!r}")
    return value


def _number(value: float) -> str:
    if not math.isfinite(value):
        raise ValueError("SQL numeric literal must be finite")
    return format(value, ".17g")


def _beta_continued_fraction(a: float, b: float, x: float) -> float:
    """Evaluate the incomplete-beta continued fraction without SciPy."""

    max_iterations = 200
    epsilon = 3.0e-14
    tiny = 1.0e-300
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    d = tiny if abs(d) < tiny else d
    d = 1.0 / d
    result = d
    for index in range(1, max_iterations + 1):
        m = float(index)
        m2 = 2.0 * m
        coefficient = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + coefficient * d
        d = tiny if abs(d) < tiny else d
        c = 1.0 + coefficient / c
        c = tiny if abs(c) < tiny else c
        d = 1.0 / d
        result *= d * c
        coefficient = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + coefficient * d
        d = tiny if abs(d) < tiny else d
        c = 1.0 + coefficient / c
        c = tiny if abs(c) < tiny else c
        d = 1.0 / d
        delta = d * c
        result *= delta
        if abs(delta - 1.0) <= epsilon:
            break
    return result


def _regularized_beta(x: float, a: float, b: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    front = math.exp(
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log1p(-x)
    )
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _beta_continued_fraction(a, b, x) / a
    return 1.0 - front * _beta_continued_fraction(b, a, 1.0 - x) / b


def student_t_to_normal_z(t_stat: float, observations: int) -> float:
    """Convert a Student-t statistic to a signed normal-equivalent z-score."""

    if observations < 2 or not math.isfinite(t_stat):
        return float("nan")
    if t_stat == 0.0:
        return 0.0
    degrees = float(observations - 1)
    x = degrees / (degrees + t_stat * t_stat)
    tail = 0.5 * _regularized_beta(x, degrees / 2.0, 0.5)
    probability = 1.0 - tail if t_stat > 0.0 else tail
    probability = min(1.0 - 1.0e-15, max(1.0e-15, probability))
    return NormalDist().inv_cdf(probability)


def event_candidate_condition_sql(candidate: EventCandidate) -> str:
    """Render a controlled predicate for one registered candidate."""

    feature = _identifier(candidate.axis.feature)
    operator = ">=" if candidate.axis.operator is EventRuleOperator.GREATER_EQUAL else "<="
    condition = f"{feature} {operator} {_number(candidate.threshold)}"
    if candidate.axis.requires_uncensored_wallet_history:
        condition = f"({condition} AND NOT first_seen_censored)"
    return condition


def event_gated_candidate_condition_sql(candidate: EventGatedCandidate) -> str:
    """Render one transparent signal-and-gate predicate."""

    signal = event_candidate_condition_sql(candidate.signal)
    gate_feature = _identifier(candidate.gate.feature)
    gate_operator = ">=" if candidate.gate.operator is EventRuleOperator.GREATER_EQUAL else "<="
    gate = f"{gate_feature} {gate_operator} {_number(candidate.gate_threshold)}"
    return f"({signal} AND {gate})"


_SIGNAL_FEATURES = tuple(
    sorted(
        {axis.feature for axis in DEFAULT_EVENT_SIGNAL_AXES}
        | {axis.feature for axis in DEFAULT_EVENT_RISK_GATES}
        | {"first_seen_censored"}
    )
)


def _lead_columns(alias: str, offset: int) -> list[str]:
    fields = (
        "block_time_ns",
        "available_at_ns",
        "phase",
        "effective_pre_base_atomic",
        "effective_pre_quote_atomic",
        "base_decimals",
        "quote_decimals",
        "fee_quote_atomic",
        "quote_amount_atomic",
        "reserve_continuity",
    )
    return [f"lead({name}, {offset}) OVER market_window AS {alias}_{name}" for name in fields]


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


def create_event_labels_sql(
    config: EventScreenConfig | None = None,
    *,
    output_table: str = "event_labels",
    signal_features: Sequence[str] | None = None,
) -> str:
    """Create delayed, capacity-limited normalized CP labels in DuckDB SQL.

    The caller must register ``source_events`` and ``launches``.  Atomic source
    reserves are converted to normalized doubles only inside this screening
    label.  Protocol-atomic replay remains a later survivor gate.
    """

    selected = EventScreenConfig() if config is None else config
    table = _identifier(output_table)
    selected_features = (
        _SIGNAL_FEATURES
        if signal_features is None
        else tuple(sorted({_identifier(feature) for feature in signal_features}))
    )
    if not selected_features:
        raise ValueError("event label signal features cannot be empty")
    order = "slot, tx_index, log_index, event_index, event_id"
    signal_columns = ",\n            ".join(selected_features)
    lead_columns = _lead_columns("entry", selected.latency_events)
    for horizon in selected.holding_events:
        exit_offset = selected.latency_events + horizon
        lead_columns.extend(_lead_columns(f"exit_{horizon}", exit_offset))
        lead_columns.append(
            "sum(CASE WHEN coverage_gap THEN 1 ELSE 0 END) OVER "
            f"(market_window ROWS BETWEEN 1 FOLLOWING AND {exit_offset} FOLLOWING) "
            f"AS future_gap_count_{horizon}"
        )
    lead_sql = ",\n            ".join(lead_columns)
    fallback_rate = selected.fallback_fee_bps / 10_000.0
    adverse_multiplier = max(0.0, 1.0 - selected.adverse_selection_bps / 10_000.0)
    entry_fee_rate = _fee_rate_sql("entry", fallback_rate)
    entry_fee_observed = _observed_fee_sql("entry")

    normalized_exits: list[str] = []
    exit_outputs: list[str] = []
    for horizon in selected.holding_events:
        prefix = f"exit_{horizon}"
        fee_rate = _fee_rate_sql(prefix, fallback_rate)
        observed_fee_condition = (
            f"AND {entry_fee_observed} AND {_observed_fee_sql(prefix)} "
            if selected.require_observed_fee
            else ""
        )
        normalized_exits.extend(
            (
                f"CAST({prefix}_effective_pre_base_atomic AS DOUBLE) / "
                f"pow(10.0, CAST({prefix}_base_decimals AS DOUBLE)) AS {prefix}_base_reserve",
                f"CAST({prefix}_effective_pre_quote_atomic AS DOUBLE) / "
                f"pow(10.0, CAST({prefix}_quote_decimals AS DOUBLE)) AS {prefix}_quote_reserve",
                f"{fee_rate} AS {prefix}_fee_rate",
            )
        )
        raw_quote = (
            f"({prefix}_quote_reserve - "
            f"({prefix}_base_reserve * {prefix}_quote_reserve) / "
            f"({prefix}_base_reserve + CASE WHEN {prefix}_phase = 'bonding_curve' "
            f"THEN entry_base_out ELSE entry_base_out * (1.0 - {prefix}_fee_rate) END))"
        )
        fee_adjusted = (
            f"CASE WHEN {prefix}_phase = 'bonding_curve' "
            f"THEN ({raw_quote}) * (1.0 - {prefix}_fee_rate) ELSE ({raw_quote}) END"
        )
        quote_out = f"({fee_adjusted}) * {_number(adverse_multiplier)}"
        valid = (
            f"market_event_number % {horizon} = 1 "
            f"AND NOT coverage_gap "
            f"AND future_gap_count_{horizon} = 0 "
            f"AND entry_reserve_continuity AND {prefix}_reserve_continuity "
            f"AND entry_phase = {prefix}_phase "
            f"{observed_fee_condition}"
            f"AND entry_base_reserve > 0.0 AND entry_quote_reserve > 0.0 "
            f"AND {prefix}_base_reserve > 0.0 AND {prefix}_quote_reserve > 0.0 "
            f"AND entry_base_out > 0.0 "
            f"AND {_number(selected.quote_notional)} <= "
            f"entry_quote_reserve * {_number(selected.max_reserve_fraction)} "
            f"AND entry_base_out <= "
            f"{prefix}_base_reserve * {_number(selected.max_reserve_fraction)} "
            f"AND token_age_seconds BETWEEN {_number(selected.min_age_seconds)} "
            f"AND {_number(selected.max_age_seconds)} "
            f"AND ({quote_out}) > {_number(selected.fixed_cost_quote)}"
        )
        exit_outputs.extend(
            (
                f"({valid}) AS eligible_{horizon}",
                f"{prefix}_block_time_ns AS label_end_{horizon}_block_time_ns",
                f"{prefix}_available_at_ns AS label_end_{horizon}_available_at_ns",
                f"CASE WHEN {valid} THEN ({quote_out}) - {_number(selected.quote_notional)} "
                f"- {_number(2.0 * selected.fixed_cost_quote)} ELSE NULL END AS pnl_{horizon}",
            )
        )

    normalized_exit_sql = ",\n            ".join(normalized_exits)
    exit_output_sql = ",\n        ".join(exit_outputs)
    return f"""
CREATE TABLE {table} AS
WITH reserve_path AS (
    SELECT
        event_id,
        market_id,
        mint,
        slot,
        tx_index,
        log_index,
        event_index,
        block_time_ns,
        available_at_ns,
        phase,
        base_decimals,
        quote_decimals,
        fee_quote_atomic,
        quote_amount_atomic,
        post_base_reserve_atomic,
        post_quote_reserve_atomic,
        reserve_continuity,
        coverage_gap,
        {signal_columns},
        CASE WHEN pre_base_reserve_atomic IS NOT NULL
             THEN pre_base_reserve_atomic
             WHEN reserve_continuity
             THEN lag(post_base_reserve_atomic) OVER reserve_window
             ELSE NULL END AS effective_pre_base_atomic,
        CASE WHEN pre_quote_reserve_atomic IS NOT NULL
             THEN pre_quote_reserve_atomic
             WHEN reserve_continuity
             THEN lag(post_quote_reserve_atomic) OVER reserve_window
             ELSE NULL END AS effective_pre_quote_atomic
    FROM source_events
    WINDOW reserve_window AS (PARTITION BY market_id ORDER BY {order})
), ordered AS (
    SELECT
        *,
        row_number() OVER market_window AS market_event_number,
        {lead_sql}
    FROM reserve_path
    WINDOW market_window AS (PARTITION BY market_id ORDER BY {order})
), normalized AS (
    SELECT
        ordered.*,
        CAST(entry_effective_pre_base_atomic AS DOUBLE) /
            pow(10.0, CAST(entry_base_decimals AS DOUBLE)) AS entry_base_reserve,
        CAST(entry_effective_pre_quote_atomic AS DOUBLE) /
            pow(10.0, CAST(entry_quote_decimals AS DOUBLE)) AS entry_quote_reserve,
        {entry_fee_rate} AS entry_fee_rate,
        {normalized_exit_sql}
    FROM ordered
), entries AS (
    SELECT
        *,
        CASE WHEN entry_phase = 'bonding_curve'
             THEN {_number(selected.quote_notional)} / (1.0 + entry_fee_rate)
             ELSE {_number(selected.quote_notional)} * (1.0 - entry_fee_rate)
        END AS entry_net_quote
    FROM normalized
), sized AS (
    SELECT
        *,
        (entry_base_reserve -
            (entry_base_reserve * entry_quote_reserve) /
            (entry_quote_reserve + entry_net_quote)) *
            {_number(adverse_multiplier)} AS entry_base_out
    FROM entries
), aged AS (
    SELECT
        sized.*,
        launches.launch_time_ns,
        (block_time_ns - launches.launch_time_ns) / 1000000000.0 AS token_age_seconds
    FROM sized
    INNER JOIN launches USING (mint)
)
SELECT
        event_id,
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
        {signal_columns},
        {exit_output_sql}
FROM aged
""".strip()


@dataclass(frozen=True, slots=True)
class EventCandidateMetrics:
    candidate_sha256: str
    fold: int
    split: str
    outcomes: int
    mints: int
    total_quote_pnl: float
    mean_event_pnl_quote: float | None
    mean_token_pnl_quote: float | None
    token_pnl_std_quote: float | None
    token_t_stat: float | None
    positive_token_share: float | None
    # Trade-level diagnostics are optional because the legacy screen and
    # interaction ledgers predate this richer audit surface.  New event
    # research aggregates populate them from the same after-cost labels;
    # older artifacts remain explicitly ``None`` rather than being inferred.
    wins: int | None = None
    losses: int | None = None
    breakevens: int | None = None
    win_rate: float | None = None
    loss_rate: float | None = None
    average_win_quote: float | None = None
    average_loss_quote: float | None = None
    payoff_ratio: float | None = None
    gross_profit_quote: float | None = None
    gross_loss_quote: float | None = None
    profit_factor: float | None = None
    expectancy_quote: float | None = None
    pnl_std_quote: float | None = None
    downside_deviation_quote: float | None = None
    sharpe_per_trade: float | None = None
    sortino_per_trade: float | None = None
    max_drawdown_quote: float | None = None


@dataclass(frozen=True, slots=True)
class EventGateMetrics:
    """Token-paired metrics for one survivor risk gate.

    ``parent`` is the promoted single signal with no risk gate and ``gated``
    is the same signal after applying exactly one gate.  The delta fields are
    computed per mint (gated PnL minus parent PnL) before they are aggregated,
    so a prolific token cannot dominate the paired comparison.
    """

    candidate_sha256: str
    fold: int
    split: str
    parent_outcomes: int
    parent_mints: int
    gated_outcomes: int
    gated_mints: int
    gate_feature_observed_outcomes: int
    parent_total_quote_pnl: float
    gated_total_quote_pnl: float
    raw_total_quote_pnl_delta: float
    paired_token_pnl_delta_sum_quote: float
    mean_token_pnl_delta_quote: float | None
    token_pnl_delta_std_quote: float | None
    token_delta_t_stat: float | None
    positive_token_delta_share: float | None
    outcome_retention: float | None
    mint_retention: float | None
    gate_feature_observation_retention: float | None


def event_gate_baseline_sha256(signal: EventCandidate) -> str:
    """Return a stable identity for the no-change baseline of ``signal``."""

    payload = {"signal_sha256": signal.sha256, "gate": "none"}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def event_aggregate_sql(
    candidates: Sequence[EventCandidate],
    holding_events: int,
    *,
    labels_table: str = "event_labels",
) -> str:
    """Aggregate one horizon in a single scan, clustering first by token."""

    table = _identifier(labels_table)
    selected = [item for item in candidates if item.holding_events == holding_events]
    if not selected:
        raise ValueError("no candidates exist for requested holding horizon")
    inner: list[str] = []
    outer: list[str] = []
    for index, candidate in enumerate(selected):
        prefix = f"c{index:03d}"
        cutoff = (
            "(memberships.split = 'test' OR "
            f"labels.label_end_{holding_events}_available_at_ns < memberships.cutoff_ns)"
        )
        condition = (
            f"eligible_{holding_events} AND {cutoff} AND "
            f"({event_candidate_condition_sql(candidate)})"
        )
        inner.extend(
            (
                f"count(*) FILTER (WHERE {condition}) AS {prefix}_n",
                f"sum(pnl_{holding_events}) FILTER (WHERE {condition}) AS {prefix}_sum",
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
    FROM {table} AS labels
    INNER JOIN memberships USING (mint)
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


def parse_event_aggregate_rows(
    rows: Sequence[Mapping[str, object]],
    candidates: Sequence[EventCandidate],
    holding_events: int,
) -> tuple[EventCandidateMetrics, ...]:
    """Convert the wide one-pass aggregate into an auditable long ledger."""

    selected = [item for item in candidates if item.holding_events == holding_events]
    result: list[EventCandidateMetrics] = []

    def required_float(value: object, name: str) -> float:
        if isinstance(value, bool) or not isinstance(value, Real):
            raise ValueError(f"{name} is not numeric")
        result = float(value)
        if not math.isfinite(result):
            raise ValueError(f"{name} is not finite")
        return result

    def required_int(value: object, name: str) -> int:
        numeric = required_float(value, name)
        if not numeric.is_integer():
            raise ValueError(f"{name} is not an integer")
        return int(numeric)

    def optional_float(value: object, name: str) -> float | None:
        return None if value is None else required_float(value, name)

    for row in rows:
        fold = required_int(row["fold"], "fold")
        split_value = row["split"]
        if not isinstance(split_value, str) or split_value not in {"train", "test"}:
            raise ValueError("split must be train or test")
        split = split_value
        for index, candidate in enumerate(selected):
            prefix = f"c{index:03d}"
            outcomes = required_int(row[f"{prefix}_outcomes"], "outcomes")
            mints = required_int(row[f"{prefix}_mints"], "mints")
            mean_event = optional_float(row[f"{prefix}_mean_event"], "mean_event_pnl_quote")
            mean_token = optional_float(row[f"{prefix}_mean_token"], "mean_token_pnl_quote")
            token_std = optional_float(row[f"{prefix}_std_token"], "token_pnl_std_quote")
            token_t_stat = (
                None
                if mean_token is None or token_std is None or token_std <= 0.0 or mints < 2
                else mean_token / (token_std / math.sqrt(mints))
            )
            result.append(
                EventCandidateMetrics(
                    candidate_sha256=candidate.sha256,
                    fold=fold,
                    split=split,
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


def select_event_fold_winner(
    metrics: Sequence[EventCandidateMetrics],
    config: EventScreenConfig | None = None,
) -> EventCandidateMetrics | None:
    """Select a positive, sufficiently active training candidate or no trade."""

    selected = EventScreenConfig() if config is None else config
    eligible = [
        item
        for item in metrics
        if item.split == "train"
        and item.outcomes >= selected.minimum_train_outcomes
        and item.mints >= selected.minimum_train_mints
        and item.total_quote_pnl > 0.0
        and item.mean_event_pnl_quote is not None
        and item.mean_event_pnl_quote > 0.0
        and item.mean_token_pnl_quote is not None
        and item.mean_token_pnl_quote > 0.0
        and item.token_t_stat is not None
        and item.token_t_stat > 0.0
    ]
    if not eligible:
        return None

    def selection_key(
        item: EventCandidateMetrics,
    ) -> tuple[float, float, float, float, int, str]:
        assert item.mean_token_pnl_quote is not None
        assert item.mean_event_pnl_quote is not None
        assert item.token_t_stat is not None
        return (
            item.token_t_stat,
            item.mean_token_pnl_quote,
            item.mean_event_pnl_quote,
            -math.inf if item.positive_token_share is None else item.positive_token_share,
            item.mints,
            item.candidate_sha256,
        )

    return max(
        eligible,
        key=selection_key,
    )


def event_gate_aggregate_sql(
    candidates: Sequence[EventGatedCandidate],
    holding_events: int,
    *,
    labels_table: str = "event_labels",
) -> str:
    """Aggregate one survivor's parent/gate pairs in one DuckDB scan.

    Every row is admitted with the same delayed-label and train-cutoff
    predicate used by :func:`event_aggregate_sql`.  The inner relation first
    collapses rows by whole mint; the outer relation then computes paired
    token deltas for each registered gate.  Candidates must all belong to one
    signal and one fixed holding horizon, which keeps the stage explicitly
    one-gate-at-a-time and prevents accidental combinations.
    """

    table = _identifier(labels_table)
    if not candidates:
        raise ValueError("no gate candidates exist")
    selected = tuple(candidates)
    signal = selected[0].signal
    if holding_events != signal.holding_events:
        raise ValueError("gate holding horizon differs from survivor horizon")
    if any(item.signal != signal for item in selected):
        raise ValueError("gate candidates must share one survivor signal")

    # Training and test labels are both strictly causal relative to the
    # membership cutoff.  Test memberships carry the sealed development-end
    # cutoff, so a label ending at that boundary is not admitted.
    cutoff = f"labels.label_end_{holding_events}_available_at_ns < memberships.cutoff_ns"
    parent = (
        f"labels.eligible_{holding_events} AND {cutoff} "
        f"AND ({event_candidate_condition_sql(signal)})"
    )
    inner: list[str] = [
        f"count(*) FILTER (WHERE {parent}) AS parent_n",
        f"coalesce(sum(labels.pnl_{holding_events}) FILTER (WHERE {parent}), 0.0) AS parent_sum",
    ]
    outer: list[str] = [
        "coalesce(sum(parent_n), 0)::BIGINT AS parent_outcomes",
        "count(*) FILTER (WHERE parent_n > 0)::BIGINT AS parent_mints",
        "coalesce(sum(parent_sum), 0.0) AS parent_total_pnl",
    ]
    for index, candidate in enumerate(selected):
        prefix = f"g{index:03d}"
        gate_feature = _identifier(candidate.gate.feature)
        gated = (
            f"labels.eligible_{holding_events} AND {cutoff} "
            f"AND ({event_gated_candidate_condition_sql(candidate)})"
        )
        inner.extend(
            (
                f"count(*) FILTER (WHERE {gated}) AS {prefix}_n",
                f"count(*) FILTER (WHERE {parent} AND labels.{gate_feature} IS NOT NULL) "
                f"AS {prefix}_observed_n",
                f"coalesce(sum(labels.pnl_{holding_events}) FILTER (WHERE {gated}), 0.0) "
                f"AS {prefix}_sum",
            )
        )
        # Pair on the fixed parent opportunity set.  A gate opportunity that
        # is suppressed contributes zero gated PnL, and each token therefore
        # contributes (gated_sum - parent_sum) / parent_n exactly once.
        delta = f"(({prefix}_sum - parent_sum) / NULLIF(parent_n, 0))"
        outer.extend(
            (
                f"coalesce(sum({prefix}_n), 0)::BIGINT AS {prefix}_gated_outcomes",
                f"count(*) FILTER (WHERE {prefix}_n > 0)::BIGINT AS {prefix}_gated_mints",
                f"coalesce(sum({prefix}_observed_n), 0)::BIGINT "
                f"AS {prefix}_feature_observed_outcomes",
                f"coalesce(sum({prefix}_sum), 0.0) AS {prefix}_gated_total_pnl",
                f"coalesce(sum({prefix}_sum - parent_sum), 0.0) AS {prefix}_raw_total_delta",
                f"coalesce(sum({delta}) FILTER (WHERE parent_n > 0), 0.0) "
                f"AS {prefix}_paired_token_delta_sum",
                f"avg({delta}) FILTER (WHERE parent_n > 0) AS {prefix}_mean_delta",
                f"stddev_samp({delta}) FILTER (WHERE parent_n > 0) AS {prefix}_std_delta",
                f"avg(CASE WHEN {delta} > 0.0 THEN 1.0 ELSE 0.0 END) "
                f"FILTER (WHERE parent_n > 0) AS {prefix}_positive_delta",
                f"avg(CAST({prefix}_n AS DOUBLE) / NULLIF(parent_n, 0)) "
                f"FILTER (WHERE parent_n > 0) AS {prefix}_outcome_retention",
                f"count(*) FILTER (WHERE {prefix}_n > 0)::DOUBLE / "
                f"NULLIF(count(*) FILTER (WHERE parent_n > 0), 0) "
                f"AS {prefix}_mint_retention",
                f"sum({prefix}_observed_n)::DOUBLE / NULLIF(sum(parent_n), 0) "
                f"AS {prefix}_feature_observation_retention",
            )
        )
    return f"""
WITH per_token AS (
    SELECT
        memberships.fold,
        memberships.split,
        memberships.cutoff_ns,
        labels.mint,
        {", ".join(inner)}
    FROM {table} AS labels
    INNER JOIN memberships USING (mint)
    GROUP BY memberships.fold, memberships.split, memberships.cutoff_ns, labels.mint
)
SELECT
    fold,
    split,
    {", ".join(outer)}
FROM per_token
GROUP BY fold, split
ORDER BY fold, split
""".strip()


def parse_event_gate_aggregate_rows(
    rows: Sequence[Mapping[str, object]],
    candidates: Sequence[EventGatedCandidate],
    holding_events: int,
) -> tuple[EventGateMetrics, ...]:
    """Convert :func:`event_gate_aggregate_sql` rows to long gate metrics."""

    selected = tuple(candidates)
    if not selected:
        raise ValueError("no gate candidates exist")
    if any(item.signal.holding_events != holding_events for item in selected):
        raise ValueError("gate holding horizon differs from candidate")

    def required_float(value: object, name: str) -> float:
        if isinstance(value, bool) or not isinstance(value, Real):
            raise ValueError(f"{name} is not numeric")
        result = float(value)
        if not math.isfinite(result):
            raise ValueError(f"{name} is not finite")
        return result

    def required_int(value: object, name: str) -> int:
        result = required_float(value, name)
        if not result.is_integer():
            raise ValueError(f"{name} is not an integer")
        return int(result)

    def optional_float(value: object, name: str) -> float | None:
        return None if value is None else required_float(value, name)

    result: list[EventGateMetrics] = []
    for row in rows:
        fold = required_int(row["fold"], "fold")
        split_value = row["split"]
        if not isinstance(split_value, str) or split_value not in {"train", "test"}:
            raise ValueError("split must be train or test")
        parent_outcomes = required_int(row["parent_outcomes"], "parent_outcomes")
        parent_mints = required_int(row["parent_mints"], "parent_mints")
        parent_total = required_float(row["parent_total_pnl"], "parent_total_pnl")
        for index, candidate in enumerate(selected):
            prefix = f"g{index:03d}"
            gated_outcomes = required_int(row[f"{prefix}_gated_outcomes"], "gated_outcomes")
            gated_mints = required_int(row[f"{prefix}_gated_mints"], "gated_mints")
            observed_outcomes = required_int(
                row[f"{prefix}_feature_observed_outcomes"],
                "gate_feature_observed_outcomes",
            )
            gated_total = required_float(row[f"{prefix}_gated_total_pnl"], "gated_total_pnl")
            mean_delta = optional_float(row[f"{prefix}_mean_delta"], "mean_delta")
            std_delta = optional_float(row[f"{prefix}_std_delta"], "std_delta")
            t_stat = (
                None
                if mean_delta is None or std_delta is None or std_delta <= 0.0 or parent_mints < 2
                else mean_delta / (std_delta / math.sqrt(parent_mints))
            )
            result.append(
                EventGateMetrics(
                    candidate_sha256=candidate.sha256,
                    fold=fold,
                    split=split_value,
                    parent_outcomes=parent_outcomes,
                    parent_mints=parent_mints,
                    gated_outcomes=gated_outcomes,
                    gated_mints=gated_mints,
                    gate_feature_observed_outcomes=observed_outcomes,
                    parent_total_quote_pnl=parent_total,
                    gated_total_quote_pnl=gated_total,
                    raw_total_quote_pnl_delta=required_float(
                        row[f"{prefix}_raw_total_delta"], "raw_total_delta"
                    ),
                    paired_token_pnl_delta_sum_quote=required_float(
                        row[f"{prefix}_paired_token_delta_sum"],
                        "paired_token_delta_sum",
                    ),
                    mean_token_pnl_delta_quote=mean_delta,
                    token_pnl_delta_std_quote=std_delta,
                    token_delta_t_stat=t_stat,
                    positive_token_delta_share=optional_float(
                        row[f"{prefix}_positive_delta"], "positive_delta"
                    ),
                    outcome_retention=optional_float(
                        row[f"{prefix}_outcome_retention"], "outcome_retention"
                    ),
                    mint_retention=optional_float(
                        row[f"{prefix}_mint_retention"], "mint_retention"
                    ),
                    gate_feature_observation_retention=optional_float(
                        row[f"{prefix}_feature_observation_retention"],
                        "gate_feature_observation_retention",
                    ),
                )
            )
    return tuple(result)


def select_event_gate_fold_winner(
    metrics: Sequence[EventGateMetrics],
    config: EventScreenConfig | None = None,
    *,
    baseline_sha256: str | None = None,
    minimum_retention: float = 0.25,
    minimum_feature_observation_retention: float = 0.95,
    minimum_gate_outcomes: int | None = None,
    minimum_gate_mints: int | None = None,
) -> EventGateMetrics | None:
    """Select a positive gate on training data, retaining a no-change baseline.

    The baseline is represented by the metric whose hash is
    ``baseline_sha256`` (or the first metric with a zero paired delta).  A
    gate must clear parent/gated activity and retention floors and improve the
    paired token delta; otherwise the baseline wins when it has enough parent
    activity.  Returning ``None`` means even the parent failed its activity
    floor, so the fold has no admissible trade.
    """

    selected = EventScreenConfig() if config is None else config
    if not 0.0 <= minimum_retention <= 1.0:
        raise ValueError("minimum_retention must be in [0, 1]")
    if not 0.0 <= minimum_feature_observation_retention <= 1.0:
        raise ValueError("minimum_feature_observation_retention must be in [0, 1]")
    gate_outcomes = (
        selected.minimum_train_outcomes if minimum_gate_outcomes is None else minimum_gate_outcomes
    )
    gate_mints = selected.minimum_train_mints if minimum_gate_mints is None else minimum_gate_mints
    if gate_outcomes < 1 or gate_mints < 1:
        raise ValueError("gate activity floors must be positive")
    train = [item for item in metrics if item.split == "train"]
    if not train:
        return None
    baseline = next(
        (
            item
            for item in train
            if baseline_sha256 is not None and item.candidate_sha256 == baseline_sha256
        ),
        None,
    )
    if baseline is None:
        baseline = next(
            (
                item
                for item in train
                if item.raw_total_quote_pnl_delta == 0.0
                and item.paired_token_pnl_delta_sum_quote == 0.0
            ),
            None,
        )
    if baseline is None:
        # Callers may provide only the gate metrics; still retain the
        # registered no-change alternative so a negative/underpowered gate
        # cannot become the fold winner by default.
        parent = train[0]
        baseline = EventGateMetrics(
            candidate_sha256="baseline" if baseline_sha256 is None else baseline_sha256,
            fold=parent.fold,
            split="train",
            parent_outcomes=parent.parent_outcomes,
            parent_mints=parent.parent_mints,
            gated_outcomes=parent.parent_outcomes,
            gated_mints=parent.parent_mints,
            gate_feature_observed_outcomes=parent.parent_outcomes,
            parent_total_quote_pnl=parent.parent_total_quote_pnl,
            gated_total_quote_pnl=parent.parent_total_quote_pnl,
            raw_total_quote_pnl_delta=0.0,
            paired_token_pnl_delta_sum_quote=0.0,
            mean_token_pnl_delta_quote=0.0 if parent.parent_mints else None,
            token_pnl_delta_std_quote=None,
            token_delta_t_stat=0.0 if parent.parent_mints else None,
            positive_token_delta_share=0.0 if parent.parent_mints else None,
            outcome_retention=1.0 if parent.parent_outcomes else None,
            mint_retention=1.0 if parent.parent_mints else None,
            gate_feature_observation_retention=1.0 if parent.parent_outcomes else None,
        )

    def parent_ok(item: EventGateMetrics) -> bool:
        return item.parent_outcomes >= gate_outcomes and item.parent_mints >= gate_mints

    baseline_ok = baseline is not None and parent_ok(baseline)
    eligible: list[EventGateMetrics] = []
    for item in train:
        if baseline is not None and item.candidate_sha256 == baseline.candidate_sha256:
            continue
        if not parent_ok(item):
            continue
        if item.gated_outcomes < gate_outcomes or item.gated_mints < gate_mints:
            continue
        if (
            item.outcome_retention is None
            or item.mint_retention is None
            or item.outcome_retention < minimum_retention
            or item.mint_retention < minimum_retention
        ):
            continue
        if (
            item.gate_feature_observation_retention is None
            or item.gate_feature_observation_retention < minimum_feature_observation_retention
        ):
            continue
        if item.raw_total_quote_pnl_delta <= 0.0:
            continue
        if item.paired_token_pnl_delta_sum_quote <= 0.0:
            continue
        if item.mean_token_pnl_delta_quote is None or item.mean_token_pnl_delta_quote <= 0.0:
            continue
        if item.token_delta_t_stat is None or item.token_delta_t_stat <= 0.0:
            continue
        eligible.append(item)
    if not eligible:
        return baseline if baseline_ok else None

    def selection_key(item: EventGateMetrics) -> tuple[float, float, float, float, int, str]:
        return (
            -math.inf if item.token_delta_t_stat is None else item.token_delta_t_stat,
            -math.inf
            if item.mean_token_pnl_delta_quote is None
            else item.mean_token_pnl_delta_quote,
            item.paired_token_pnl_delta_sum_quote,
            -math.inf
            if item.positive_token_delta_share is None
            else item.positive_token_delta_share,
            item.gated_mints,
            item.candidate_sha256,
        )

    winner = max(eligible, key=selection_key)
    # A no-change baseline remains the deterministic tie-break for a gate that
    # does not improve the paired metric; positive gates always outrank it.
    return (
        winner
        if winner.raw_total_quote_pnl_delta > 0.0 and winner.paired_token_pnl_delta_sum_quote > 0.0
        else (baseline if baseline_ok else None)
    )


__all__ = [
    "DEFAULT_EVENT_RISK_GATES",
    "DEFAULT_EVENT_SIGNAL_AXES",
    "EventCandidate",
    "EventGatedCandidate",
    "EventCandidateMetrics",
    "EventGateMetrics",
    "EventCohortFold",
    "EventRiskGateAxis",
    "EventRuleOperator",
    "EventScreenConfig",
    "EventSignalAxis",
    "LaunchCohort",
    "create_event_labels_sql",
    "default_event_candidates",
    "default_event_gate_candidates",
    "event_aggregate_sql",
    "event_gate_aggregate_sql",
    "event_gate_baseline_sha256",
    "event_candidate_condition_sql",
    "event_gated_candidate_condition_sql",
    "event_cohort_folds",
    "parse_event_aggregate_rows",
    "parse_event_gate_aggregate_rows",
    "select_event_gate_fold_winner",
    "select_event_fold_winner",
    "student_t_to_normal_z",
]
