"""Development-only execution and walk-forward grid for shock/recovery v1.

The registry and source-window SQL live in :mod:`pumpquant.shock_recovery`.
This module deliberately contains no RPC, HTTP, or holdout code.  It accepts
already-materialized development rows, calibrates every threshold on the
current fold's training mints, and evaluates delayed constant-product labels.

The public functions are intentionally small enough to use in tests and in a
local runner.  Rows are mappings so that the evaluator can consume DuckDB,
Arrow, or test fixtures without introducing a data dependency here.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from numbers import Real
from pathlib import Path
from typing import cast

from pumpquant.core.hashing import sha256_file
from pumpquant.research.families.shock_recovery import (
    PRIOR_ATTEMPTED_CANDIDATES,
    ShockRecoveryCandidate,
    StateProfile,
    default_shock_recovery_candidates,
)

EXPERIMENT_ID = "shock-recovery-fresh-v1"
VERSION = 1
DEVELOPMENT_START_SLOT_INCLUSIVE = 431_265_461
DEVELOPMENT_END_SLOT_EXCLUSIVE = 432_432_000
DEVELOPMENT_BOUNDARY_SLOT = DEVELOPMENT_END_SLOT_EXCLUSIVE
HOLDOUT_BOUNDARY_SLOT = DEVELOPMENT_END_SLOT_EXCLUSIVE
FINAL_HOLDOUT_START_SLOT_INCLUSIVE = DEVELOPMENT_END_SLOT_EXCLUSIVE
FINAL_HOLDOUT_END_SLOT_EXCLUSIVE = 432_864_000
EMBARGO_NS = 660 * 1_000_000_000
INITIAL_TRAIN_FRACTION = 0.30
NESTED_BLOCK_COUNT = 10
NESTED_FOLD_COUNT = 5
SCREENING_NOTIONAL_QUOTE = 0.1
MAX_RESERVE_FRACTION = 0.0025
FIXED_COST_QUOTE_PER_LEG = 0.00016
ADVERSE_SELECTION_BPS = 30.0
OBSERVED_FEE_MAX_RATE = 0.05
CUMULATIVE_TRIAL_COUNT = PRIOR_ATTEMPTED_CANDIDATES + 192
Z_THRESHOLD = 3.922901121347733

REPORT_KIND = "pumpquant.shock_recovery_fresh_grid"
REGISTRY_KIND = "pumpquant.shock_recovery_fresh_registry"
THRESHOLD_KIND = "pumpquant.shock_recovery_threshold"
METRIC_KIND = "pumpquant.shock_recovery_metric"
MANIFEST_KIND = "pumpquant.shock_recovery_fresh_manifest"


class ShockRecoveryGridError(RuntimeError):
    """Raised when a development-only integrity or causal invariant fails."""


def _finite(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ShockRecoveryGridError(f"{name} is not numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ShockRecoveryGridError(f"{name} is not finite")
    return result


def _integer(value: object, name: str, *, nonnegative: bool = True) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ShockRecoveryGridError(f"{name} is not an integer")
    if nonnegative and value < 0:
        raise ShockRecoveryGridError(f"{name} is negative")
    return value


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ShockRecoveryGridError(f"{name} is missing or invalid")
    return value


def _canonical_json(value: object) -> bytes:
    try:
        encoded = json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise ShockRecoveryGridError("value cannot be encoded as canonical JSON") from error
    return (encoded + "\n").encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def validate_development_upper_bound(upper_bound_slot_exclusive: int) -> int:
    """Reject a development interval that can touch the fresh holdout.

    Equality is intentionally rejected.  A caller must provide a strict
    development upper bound below epoch-1001; this avoids an accidental
    ``<=`` predicate admitting the first holdout slot.
    """

    upper = _integer(upper_bound_slot_exclusive, "development upper slot")
    if upper >= DEVELOPMENT_END_SLOT_EXCLUSIVE:
        raise ShockRecoveryGridError(
            "development upper bound must be strictly below epoch-1001 "
            f"({DEVELOPMENT_END_SLOT_EXCLUSIVE})"
        )
    if upper <= DEVELOPMENT_START_SLOT_INCLUSIVE:
        raise ShockRecoveryGridError("development interval is empty")
    return upper


def validate_development_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    upper_bound_slot_exclusive: int = DEVELOPMENT_END_SLOT_EXCLUSIVE - 1,
) -> tuple[dict[str, object], ...]:
    """Validate and deterministically order materialized rows.

    Holdout rows are rejected rather than silently filtered.  This is a
    deliberate fail-closed boundary: an accidental archival/holdout mix must
    not become a seemingly valid development report.
    """

    upper = validate_development_upper_bound(upper_bound_slot_exclusive)
    materialized: list[dict[str, object]] = []
    seen_ids: set[tuple[str, str]] = set()
    for index, source in enumerate(rows):
        if not isinstance(source, Mapping):
            raise ShockRecoveryGridError(f"row {index} is not a mapping")
        row = {str(key): value for key, value in source.items()}
        mint = _text(row.get("mint"), f"row {index} mint")
        slot = _integer(row.get("slot"), f"row {index} slot")
        if slot < DEVELOPMENT_START_SLOT_INCLUSIVE:
            raise ShockRecoveryGridError(f"row {index} is before development start")
        if slot >= DEVELOPMENT_END_SLOT_EXCLUSIVE:
            raise ShockRecoveryGridError(
                f"row {index} reaches holdout slot {slot} >= {DEVELOPMENT_END_SLOT_EXCLUSIVE}"
            )
        # The externally supplied value is the last permitted development
        # slot (the frozen contract's exclusive boundary minus one).  Keep
        # the name ``upper_bound_slot_exclusive`` for compatibility with the
        # interval manifests while accepting that terminal development slot.
        if slot > upper:
            raise ShockRecoveryGridError(
                f"row {index} reaches configured exclusive upper bound {upper}"
            )
        event_id = _text(row.get("event_id", f"row-{index}"), f"row {index} event_id")
        key = (mint, event_id)
        if key in seen_ids:
            raise ShockRecoveryGridError(f"duplicate event identity {key!r}")
        seen_ids.add(key)
        availability = row.get("available_at_ns", row.get("block_time_ns"))
        if availability is not None:
            _integer(availability, f"row {index} available_at_ns")
        # A row may have either atomic or normalized reserves.  We defer the
        # detailed fill checks until a signal actually attempts an outcome.
        materialized.append(row)
    materialized.sort(key=_row_order_key)
    return tuple(materialized)


def _row_order_key(row: Mapping[str, object]) -> tuple[object, ...]:
    """Canonical chain order used by all event windows and fills."""

    def number(name: str, fallback: int = 0) -> int:
        value = row.get(name, fallback)
        if isinstance(value, bool) or not isinstance(value, int):
            return fallback
        return value

    return (
        _text(row.get("mint"), "row mint"),
        number("slot"),
        number("tx_index"),
        number("log_index", number("instruction_index")),
        number("event_index", number("instruction_index")),
        _text(row.get("event_id", ""), "row event_id"),
        _text(row.get("market_id", row.get("mint")), "row market_id"),
    )


def _sequence_sha256(values: Sequence[str]) -> str:
    digest = hashlib.sha256()
    for value in values:
        encoded = value.encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class ShockRecoveryGridConfig:
    """Frozen economic and fold controls from the v1 preregistration."""

    development_end_slot_exclusive: int = DEVELOPMENT_END_SLOT_EXCLUSIVE - 1
    embargo_ns: int = EMBARGO_NS
    quote_notional: float = SCREENING_NOTIONAL_QUOTE
    max_reserve_fraction: float = MAX_RESERVE_FRACTION
    fixed_cost_quote_per_leg: float = FIXED_COST_QUOTE_PER_LEG
    adverse_selection_bps: float = ADVERSE_SELECTION_BPS
    observed_fee_required: bool = True
    minimum_train_outcomes: int = 500
    minimum_train_mints: int = 300
    initial_train_fraction: float = INITIAL_TRAIN_FRACTION
    block_count: int = NESTED_BLOCK_COUNT
    fold_count: int = NESTED_FOLD_COUNT
    bootstrap_replicates: int = 10_000
    bootstrap_block_length_mints: int = 500
    bootstrap_seed: str = EXPERIMENT_ID
    causal_fx_available: bool = False

    def __post_init__(self) -> None:
        validate_development_upper_bound(self.development_end_slot_exclusive)
        if self.embargo_ns < 0:
            raise ShockRecoveryGridError("embargo_ns cannot be negative")
        for name in (
            "quote_notional",
            "max_reserve_fraction",
            "fixed_cost_quote_per_leg",
            "adverse_selection_bps",
            "initial_train_fraction",
        ):
            value = _finite(getattr(self, name), name)
            if value < 0.0:
                raise ShockRecoveryGridError(f"{name} cannot be negative")
        if self.quote_notional <= 0.0 or not 0.0 < self.max_reserve_fraction <= 1.0:
            raise ShockRecoveryGridError("invalid notional or reserve cap")
        if self.adverse_selection_bps >= 10_000.0:
            raise ShockRecoveryGridError("adverse haircut must be below 10000 bps")
        if not 0.0 < self.initial_train_fraction < 1.0:
            raise ShockRecoveryGridError("initial train fraction must lie in (0, 1)")
        if self.minimum_train_outcomes < 1 or self.minimum_train_mints < 1:
            raise ShockRecoveryGridError("minimum train counts must be positive")
        if self.block_count != 10 or self.fold_count != 5:
            raise ShockRecoveryGridError("v1 requires exactly ten blocks and five folds")
        if self.bootstrap_replicates < 1 or self.bootstrap_block_length_mints < 1:
            raise ShockRecoveryGridError("bootstrap controls must be positive")
        if not isinstance(self.causal_fx_available, bool):
            raise ShockRecoveryGridError("causal_fx_available must be boolean")


@dataclass(frozen=True, slots=True)
class TokenCohort:
    mint: str
    launch_time_ns: int


@dataclass(frozen=True, slots=True)
class NestedFold:
    number: int
    train: tuple[TokenCohort, ...]
    validation: tuple[TokenCohort, ...]
    test: tuple[TokenCohort, ...]
    train_cutoff_ns: int
    validation_start_ns: int

    @property
    def train_mints(self) -> tuple[str, ...]:
        return tuple(item.mint for item in self.train)

    @property
    def validation_mints(self) -> tuple[str, ...]:
        return tuple(item.mint for item in self.validation)

    @property
    def test_mints(self) -> tuple[str, ...]:
        return tuple(item.mint for item in self.test)


def nested_walk_forward_folds(
    launches: Mapping[str, int],
    selected_mints: Iterable[str] | None = None,
    *,
    config: ShockRecoveryGridConfig | None = None,
) -> tuple[NestedFold, ...]:
    """Create five alternating validation/test folds over whole mints.

    The first ``floor(.30*N)`` mints are the initial prefix.  The remainder
    is split into exactly ten contiguous blocks using quotient/remainder
    allocation; block ``2k`` is validation and block ``2k+1`` is test.
    """

    selected = ShockRecoveryGridConfig() if config is None else config
    names = set(launches) if selected_mints is None else set(selected_mints)
    cohorts: list[TokenCohort] = []
    for mint in names:
        if mint not in launches:
            raise ShockRecoveryGridError(f"launch map is missing mint {mint!r}")
        launch = _integer(launches[mint], f"launch time for {mint}")
        cohorts.append(TokenCohort(_text(mint, "mint"), launch))
    cohorts.sort(key=lambda item: (item.launch_time_ns, item.mint))
    if len({item.mint for item in cohorts}) != len(cohorts):
        raise ShockRecoveryGridError("duplicate launch mint")
    initial = math.floor(selected.initial_train_fraction * len(cohorts))
    remainder = len(cohorts) - initial
    if initial < 1 or remainder < selected.block_count:
        return ()
    quotient, extra = divmod(remainder, selected.block_count)
    blocks: list[tuple[TokenCohort, ...]] = []
    cursor = initial
    for index in range(selected.block_count):
        size = quotient + (1 if index < extra else 0)
        blocks.append(tuple(cohorts[cursor : cursor + size]))
        cursor += size
    result: list[NestedFold] = []
    for fold_number in range(selected.fold_count):
        validation = blocks[2 * fold_number]
        test = blocks[2 * fold_number + 1]
        if not validation or not test:
            return ()
        validation_start = validation[0].launch_time_ns
        cutoff = validation_start - selected.embargo_ns
        prefix_end = initial + sum(len(blocks[index]) for index in range(2 * fold_number))
        # ``strictly before validation`` and the launch-time embargo are both
        # explicit.  No later validation/test mint can enter the train set.
        train = tuple(cohort for cohort in cohorts[:prefix_end] if cohort.launch_time_ns < cutoff)
        result.append(
            NestedFold(
                number=fold_number + 1,
                train=train,
                validation=validation,
                test=test,
                train_cutoff_ns=cutoff,
                validation_start_ns=validation_start,
            )
        )
    return tuple(result)


def _cohorts_from_rows(
    rows: Sequence[Mapping[str, object]],
    launches: Mapping[str, int] | None,
) -> dict[str, int]:
    result: dict[str, int] = (
        {}
        if launches is None
        else {
            _text(mint, "launch mint"): _integer(value, "launch time")
            for mint, value in launches.items()
        }
    )
    for row in rows:
        mint = _text(row.get("mint"), "row mint")
        value = row.get("launch_time_ns")
        if value is None:
            if mint not in result:
                raise ShockRecoveryGridError(f"launch map is missing mint {mint!r}")
            continue
        launch = _integer(value, f"launch time for {mint}")
        prior = result.setdefault(mint, launch)
        if prior != launch:
            raise ShockRecoveryGridError(f"inconsistent launch time for {mint!r}")
    if not result:
        raise ShockRecoveryGridError("no launch cohorts")
    return result


def _quantile_cont(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] + weight * (ordered[upper] - ordered[lower])


def _value(row: Mapping[str, object], key: str) -> float | None:
    value = row.get(key)
    if value is None or isinstance(value, bool) or not isinstance(value, Real):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _shock_feature(candidate: ShockRecoveryCandidate) -> str:
    return f"shock_{candidate.shock_measure.value}_{candidate.shock_length}e"


def _recovery_feature(candidate: ShockRecoveryCandidate) -> str:
    return f"recovery_{candidate.recovery_measure.value}_2e"


def _state_feature(candidate: ShockRecoveryCandidate) -> str | None:
    if candidate.state_profile is StateProfile.NONE:
        return None
    if candidate.state_profile is StateProfile.LOW_CURRENT_IMPACT:
        return "gross_directional_impact_bps"
    return f"sell_excitation_relaxation_{candidate.shock_length}e"


@dataclass(frozen=True, slots=True)
class FittedThresholds:
    candidate_sha256: str
    fold: int
    shock_threshold: float | None
    recovery_threshold: float | None
    state_threshold: float | None
    training_rows: int


def fit_candidate_thresholds(
    candidate: ShockRecoveryCandidate,
    training_rows: Iterable[Mapping[str, object]],
    *,
    fold: int = 0,
) -> FittedThresholds:
    """Fit all thresholds using training rows only (exact continuous quantiles)."""

    rows = tuple(training_rows)
    shock = [value for row in rows if (value := _value(row, _shock_feature(candidate))) is not None]
    recovery = [
        value for row in rows if (value := _value(row, _recovery_feature(candidate))) is not None
    ]
    state_feature = _state_feature(candidate)
    state = (
        []
        if state_feature is None
        else [value for row in rows if (value := _value(row, state_feature)) is not None]
    )
    state_quantile = 0.5
    return FittedThresholds(
        candidate_sha256=candidate.sha256,
        fold=fold,
        shock_threshold=_quantile_cont(shock, candidate.shock_quantile),
        recovery_threshold=_quantile_cont(recovery, candidate.recovery_quantile),
        state_threshold=_quantile_cont(state, state_quantile),
        training_rows=len(rows),
    )


def _signal_matches(
    row: Mapping[str, object],
    candidate: ShockRecoveryCandidate,
    thresholds: FittedThresholds,
) -> bool:
    shock = _value(row, _shock_feature(candidate))
    recovery = _value(row, _recovery_feature(candidate))
    if shock is None or recovery is None:
        return False
    if thresholds.shock_threshold is None or thresholds.recovery_threshold is None:
        return False
    if shock > thresholds.shock_threshold or recovery < thresholds.recovery_threshold:
        return False
    state_feature = _state_feature(candidate)
    if state_feature is None:
        return True
    state = _value(row, state_feature)
    return (
        state is not None
        and thresholds.state_threshold is not None
        and state <= thresholds.state_threshold
    )


def _phase(row: Mapping[str, object]) -> str | None:
    value = row.get("phase")
    return None if value is None else str(value)


def _available_ns(row: Mapping[str, object]) -> int:
    value = row.get("available_at_ns", row.get("block_time_ns", row.get("slot", 0)))
    return _integer(value, "available_at_ns")


def _bool(row: Mapping[str, object], name: str, default: bool) -> bool:
    value = row.get(name)
    if value is None:
        return default
    if not isinstance(value, bool):
        # Arrow/JSON fixtures occasionally use 0/1; only those are safe.
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        return default
    return value


def _decimals(row: Mapping[str, object], name: str) -> int:
    value = row.get(name, 0)
    return _integer(value, name)


def _reserve(
    row: Mapping[str, object], side: str, kind: str, prior: Mapping[str, object] | None
) -> float | None:
    """Read a reserve in normalized units, falling back to a prior post state."""

    names = (
        f"{kind}_{side}_reserve",
        f"{kind}_{side}_reserve_normalized",
        f"{kind}_{side}_reserve_atomic",
        f"effective_{kind}_{side}_reserve_atomic",
    )
    for name in names:
        value = row.get(name)
        if value is None:
            continue
        numeric = _value(row, name)
        if numeric is None or numeric <= 0.0:
            return None
        if name.endswith("_atomic"):
            numeric /= 10.0 ** _decimals(row, f"{side}_decimals")
        return numeric
    if kind == "pre" and prior is not None and _bool(row, "reserve_continuity", True):
        return _reserve(prior, side, "post", None)
    return None


def _fee_rate(row: Mapping[str, object]) -> float | None:
    fee_bps_key = next(
        (
            key
            for key in ("venue_fee_bps", "observed_fee_bps", "fee_bps")
            if row.get(key) is not None
        ),
        None,
    )
    if fee_bps_key is not None:
        value = _value(row, fee_bps_key)
        if value is None:
            return None
        rate = value / 10_000.0
    elif row.get("observed_fee_rate") is not None or row.get("fee_rate") is not None:
        value = _value(row, "observed_fee_rate")
        if value is None:
            value = _value(row, "fee_rate")
        if value is None:
            return None
        rate = value
    else:
        fee = _value(row, "fee_quote_atomic")
        quote = _value(row, "quote_amount_atomic")
        if fee is None or quote is None or quote <= 0.0:
            fee = _value(row, "fee_quote")
            quote = _value(row, "quote_amount")
        if fee is None or quote is None or quote <= 0.0:
            return None
        rate = fee / quote
    return rate if math.isfinite(rate) and 0.0 <= rate <= OBSERVED_FEE_MAX_RATE else None


def _fee_asset(row: Mapping[str, object]) -> str | None:
    value = row.get("fee_asset")
    if value is None:
        # Legacy projections only retained quote-denominated aggregate fees.
        return "quote"
    normalized = str(value).lower()
    return normalized if normalized in {"base", "quote"} else None


def _side(row: Mapping[str, object]) -> str:
    value = row.get("side", "buy")
    return str(value).lower()


def _fill_buy(
    row: Mapping[str, object],
    prior: Mapping[str, object] | None,
    quote_in: float,
    haircut: float,
    reserve_fraction: float,
) -> tuple[float, float] | None:
    base_reserve = _reserve(row, "base", "pre", prior)
    quote_reserve = _reserve(row, "quote", "pre", prior)
    fee = _fee_rate(row)
    if base_reserve is None or quote_reserve is None or fee is None:
        return None
    if quote_in > quote_reserve * reserve_fraction:
        return None
    fee_asset = _fee_asset(row)
    if fee_asset is None:
        return None
    # A quote-denominated fee is added to the curve input.  A flipped
    # PumpSwap pool instead charges in canonical base, so it reduces the base
    # received after applying the invariant.
    net_quote = quote_in / (1.0 + fee) if fee_asset == "quote" else quote_in
    if net_quote <= 0.0:
        return None
    base_out = base_reserve - (base_reserve * quote_reserve) / (quote_reserve + net_quote)
    if fee_asset == "base":
        base_out *= 1.0 - fee
    base_out *= haircut
    if base_out <= 0.0:
        return None
    return base_out, base_reserve


def _fill_sell(
    row: Mapping[str, object],
    prior: Mapping[str, object] | None,
    base_in: float,
    haircut: float,
    reserve_fraction: float,
) -> float | None:
    base_reserve = _reserve(row, "base", "pre", prior)
    quote_reserve = _reserve(row, "quote", "pre", prior)
    fee = _fee_rate(row)
    if base_reserve is None or quote_reserve is None or fee is None:
        return None
    if base_in <= 0.0 or base_in > base_reserve * reserve_fraction:
        return None
    fee_asset = _fee_asset(row)
    if fee_asset is None:
        return None
    # In normal orientation the fee is deducted from quote output.  In a
    # flipped PumpSwap pool it is added to the canonical-base input instead.
    effective_base = base_in * haircut
    if fee_asset == "base":
        effective_base /= 1.0 + fee
    raw_quote = quote_reserve - (base_reserve * quote_reserve) / (base_reserve + effective_base)
    if raw_quote <= 0.0:
        return None
    quote_out = raw_quote * (1.0 - fee) if fee_asset == "quote" else raw_quote
    return quote_out if quote_out > 0.0 else None


@dataclass(frozen=True, slots=True)
class PositionOutcome:
    candidate_sha256: str
    mint: str
    fold: int
    split: str
    decision_index: int
    entry_index: int
    exit_index: int
    entry_event_id: str
    exit_event_id: str
    pnl_quote: float

    @property
    def pnl(self) -> float:
        """Short alias used by generic outcome consumers."""

        return self.pnl_quote


def _path_valid(rows: Sequence[Mapping[str, object]], start: int, end: int) -> bool:
    if start < 0 or end >= len(rows) or end <= start:
        return False
    phase = _phase(rows[start])
    market = rows[start].get("market_id", rows[start].get("mint"))
    prior_available = _available_ns(rows[start])
    segment = rows[start].get("continuity_segment")
    for row in rows[start : end + 1]:
        if _phase(row) != phase or row.get("market_id", row.get("mint")) != market:
            return False
        if segment is not None and row.get("continuity_segment") != segment:
            return False
        if _bool(row, "coverage_gap", False) or not _bool(row, "reserve_continuity", True):
            return False
        available = _available_ns(row)
        if available < prior_available:
            return False
        prior_available = available
    return True


def evaluate_candidate(
    candidate: ShockRecoveryCandidate,
    rows: Iterable[Mapping[str, object]],
    thresholds: FittedThresholds,
    *,
    fold: int = 0,
    split: str = "train",
    mint_filter: set[str] | frozenset[str] | None = None,
    cutoff_ns: int | None = None,
    config: ShockRecoveryGridConfig | None = None,
) -> tuple[PositionOutcome, ...]:
    """Evaluate one candidate with a one-position-per-mint policy."""

    selected = ShockRecoveryGridConfig() if config is None else config
    if split not in {"train", "validation", "test"}:
        raise ShockRecoveryGridError("split must be train, validation, or test")
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for source in rows:
        row = dict(source)
        mint = _text(row.get("mint"), "row mint")
        if mint_filter is not None and mint not in mint_filter:
            continue
        grouped[mint].append(row)
    outcomes: list[PositionOutcome] = []
    haircut = 1.0 - selected.adverse_selection_bps / 10_000.0
    for mint, values in sorted(grouped.items()):
        values.sort(key=_row_order_key)
        next_decision = 0
        for decision_index, decision in enumerate(values):
            if decision_index < next_decision or not _signal_matches(
                decision, candidate, thresholds
            ):
                continue
            entry_index = decision_index + 2
            exit_index = entry_index + candidate.holding_events
            if not _path_valid(values, decision_index, exit_index):
                continue
            entry = values[entry_index]
            exit_row = values[exit_index]
            if cutoff_ns is not None and split == "train" and _available_ns(exit_row) >= cutoff_ns:
                continue
            if cutoff_ns is not None and split != "train" and _available_ns(decision) >= cutoff_ns:
                # The development boundary is checked by the loader; this
                # condition prevents accidental use of a later committed row.
                continue
            prior_entry = values[entry_index - 1] if entry_index else None
            prior_exit = values[exit_index - 1] if exit_index else None
            buy = _fill_buy(
                entry,
                prior_entry,
                selected.quote_notional,
                haircut,
                selected.max_reserve_fraction,
            )
            if buy is None:
                continue
            base_out, _ = buy
            quote_out = _fill_sell(
                exit_row,
                prior_exit,
                base_out,
                haircut,
                selected.max_reserve_fraction,
            )
            if quote_out is None:
                continue
            pnl = quote_out - selected.quote_notional - 2.0 * selected.fixed_cost_quote_per_leg
            if not math.isfinite(pnl):
                continue
            outcomes.append(
                PositionOutcome(
                    candidate_sha256=candidate.sha256,
                    mint=mint,
                    fold=fold,
                    split=split,
                    decision_index=decision_index,
                    entry_index=entry_index,
                    exit_index=exit_index,
                    entry_event_id=_text(
                        entry.get("event_id", f"{mint}:{entry_index}"), "entry event_id"
                    ),
                    exit_event_id=_text(
                        exit_row.get("event_id", f"{mint}:{exit_index}"), "exit event_id"
                    ),
                    pnl_quote=pnl,
                )
            )
            next_decision = exit_index + 1
    return tuple(outcomes)


@dataclass(frozen=True, slots=True)
class CandidateMetrics:
    candidate_sha256: str
    fold: int
    split: str
    outcomes: int
    mints: int
    total_pnl_quote: float
    mean_event_pnl_quote: float | None
    mean_token_pnl_quote: float | None
    token_pnl_std_quote: float | None
    token_cluster_t_stat: float | None
    positive_token_share: float | None
    outcome_rows: tuple[PositionOutcome, ...] = field(default_factory=tuple, repr=False)

    @property
    def total_quote_pnl(self) -> float:
        return self.total_pnl_quote

    @property
    def token_t_stat(self) -> float | None:
        return self.token_cluster_t_stat


def candidate_metrics(
    candidate_sha256: str,
    outcomes: Iterable[PositionOutcome],
    *,
    fold: int = 0,
    split: str = "train",
) -> CandidateMetrics:
    values = tuple(outcomes)
    by_mint: dict[str, list[float]] = defaultdict(list)
    for item in values:
        by_mint[item.mint].append(item.pnl_quote)
    token_means = tuple(sum(pnl) / len(pnl) for pnl in by_mint.values())
    total = sum(item.pnl_quote for item in values)
    mean_event = None if not values else total / len(values)
    mean_token = None if not token_means else sum(token_means) / len(token_means)
    std = None
    t_stat = None
    if len(token_means) >= 2:
        assert mean_token is not None
        mean = mean_token
        variance = sum((value - mean) ** 2 for value in token_means) / (len(token_means) - 1)
        std = math.sqrt(variance)
        if std > 0.0:
            t_stat = mean / (std / math.sqrt(len(token_means)))
    positive_share = (
        None if not token_means else sum(value > 0 for value in token_means) / len(token_means)
    )
    return CandidateMetrics(
        candidate_sha256=candidate_sha256,
        fold=fold,
        split=split,
        outcomes=len(values),
        mints=len(by_mint),
        total_pnl_quote=total,
        mean_event_pnl_quote=mean_event,
        mean_token_pnl_quote=mean_token,
        token_pnl_std_quote=std,
        token_cluster_t_stat=t_stat,
        positive_token_share=positive_share,
        outcome_rows=values,
    )


def _training_eligible(metric: CandidateMetrics, config: ShockRecoveryGridConfig) -> bool:
    return (
        metric.outcomes >= config.minimum_train_outcomes
        and metric.mints >= config.minimum_train_mints
        and metric.total_pnl_quote > 0.0
        and metric.mean_event_pnl_quote is not None
        and metric.mean_event_pnl_quote > 0.0
        and metric.mean_token_pnl_quote is not None
        and metric.mean_token_pnl_quote > 0.0
        and metric.token_cluster_t_stat is not None
        and metric.token_cluster_t_stat > 0.0
    )


def select_training_winner(
    metrics: Sequence[CandidateMetrics],
    *,
    config: ShockRecoveryGridConfig | None = None,
) -> CandidateMetrics | None:
    """Select by token-cluster t-statistic, then total PnL, then hash."""

    selected = ShockRecoveryGridConfig() if config is None else config
    selection_split = (
        "validation" if any(item.split == "validation" for item in metrics) else "train"
    )
    eligible = [
        item
        for item in metrics
        if item.split == selection_split and _training_eligible(item, selected)
    ]
    if not eligible:
        return None
    # Canonical hash is a final ascending tie-break, unlike ``max``'s usual
    # descending string comparison.
    return sorted(
        eligible,
        key=lambda item: (
            -(item.token_cluster_t_stat or -math.inf),
            -item.total_pnl_quote,
            item.candidate_sha256,
        ),
    )[0]


@dataclass(frozen=True, slots=True)
class FoldResult:
    number: int
    train_mints: tuple[str, ...]
    validation_mints: tuple[str, ...]
    test_mints: tuple[str, ...]
    train_cutoff_ns: int
    thresholds: tuple[FittedThresholds, ...]
    validation_metrics: tuple[CandidateMetrics, ...]
    selected_candidate_sha256: str | None
    selected_test_metrics: CandidateMetrics | None


@dataclass(frozen=True, slots=True)
class DevelopmentResult:
    experiment_id: str
    status: str
    candidate_count: int
    trial_denominator: int
    folds: tuple[FoldResult, ...]
    promoted: tuple[dict[str, object], ...]
    holdout_accessed: bool = False
    network_accessed: bool = False


def evaluate_registry(
    rows: Iterable[Mapping[str, object]],
    launches: Mapping[str, int] | None = None,
    *,
    config: ShockRecoveryGridConfig | None = None,
    candidates: Sequence[ShockRecoveryCandidate] | None = None,
    experiment_id: str = EXPERIMENT_ID,
) -> DevelopmentResult:
    """Run the complete nested development grid without holdout/network access."""

    selected = ShockRecoveryGridConfig() if config is None else config
    materialized = validate_development_rows(
        rows,
        upper_bound_slot_exclusive=selected.development_end_slot_exclusive,
    )
    launch_map = _cohorts_from_rows(materialized, launches)
    mints = tuple(sorted({str(row["mint"]) for row in materialized}))
    folds = nested_walk_forward_folds(launch_map, mints, config=selected)
    registry = tuple(default_shock_recovery_candidates() if candidates is None else candidates)
    if len(registry) != 192 or tuple(item.sha256 for item in registry) != tuple(
        item.sha256 for item in default_shock_recovery_candidates()
    ):
        raise ShockRecoveryGridError("candidate registry is not the exact preregistered 192 order")
    grouped = {
        mint: tuple(row for row in materialized if row.get("mint") == mint) for mint in mints
    }
    fold_results: list[FoldResult] = []
    for fold in folds:
        train_rows = tuple(
            row
            for mint in fold.train_mints
            for row in grouped[mint]
            if _available_ns(row) < fold.train_cutoff_ns
        )
        validation_metrics: list[CandidateMetrics] = []
        thresholds: list[FittedThresholds] = []
        for candidate in registry:
            fitted = fit_candidate_thresholds(candidate, train_rows, fold=fold.number)
            thresholds.append(fitted)
            validation_outcomes = evaluate_candidate(
                candidate,
                (row for mint in fold.validation_mints for row in grouped[mint]),
                fitted,
                fold=fold.number,
                split="validation",
                mint_filter=set(fold.validation_mints),
                config=selected,
            )
            validation_metrics.append(
                candidate_metrics(
                    candidate.sha256, validation_outcomes, fold=fold.number, split="validation"
                )
            )
        winner = select_training_winner(validation_metrics, config=selected)
        test_metrics = None
        if winner is not None:
            candidate = next(item for item in registry if item.sha256 == winner.candidate_sha256)
            fitted = next(item for item in thresholds if item.candidate_sha256 == candidate.sha256)
            test_rows = (row for mint in fold.test_mints for row in grouped[mint])
            test_outcomes = evaluate_candidate(
                candidate,
                test_rows,
                fitted,
                fold=fold.number,
                split="test",
                mint_filter=set(fold.test_mints),
                config=selected,
            )
            test_metrics = candidate_metrics(
                candidate.sha256, test_outcomes, fold=fold.number, split="test"
            )
        fold_results.append(
            FoldResult(
                number=fold.number,
                train_mints=fold.train_mints,
                validation_mints=fold.validation_mints,
                test_mints=fold.test_mints,
                train_cutoff_ns=fold.train_cutoff_ns,
                thresholds=tuple(thresholds),
                validation_metrics=tuple(validation_metrics),
                selected_candidate_sha256=None if winner is None else winner.candidate_sha256,
                selected_test_metrics=test_metrics,
            )
        )
    promoted = promote_survivors(tuple(fold_results), registry=registry, config=selected)
    status = "development_survivor" if promoted else "no_survivor"
    return DevelopmentResult(
        experiment_id=experiment_id,
        status=status,
        candidate_count=len(registry),
        trial_denominator=PRIOR_ATTEMPTED_CANDIDATES + len(registry),
        folds=tuple(fold_results),
        promoted=promoted,
    )


def _student_z(metric: CandidateMetrics) -> float | None:
    if metric.token_cluster_t_stat is None or metric.mints < 2:
        return None
    # Normal approximation is frozen in the preregistration's multiplicity
    # gate.  For large token clusters this is effectively exact; retaining the
    # t statistic in the ledger keeps the calculation auditable.
    return metric.token_cluster_t_stat


def _stationary_bootstrap_lower_95(
    token_values: Sequence[float],
    *,
    replicates: int,
    mean_block_length: int,
    seed_material: str,
) -> float | None:
    """Return a deterministic stationary-bootstrap lower 95% bound."""

    if not token_values or any(not math.isfinite(value) for value in token_values):
        return None
    if len(token_values) == 1:
        return token_values[0]
    generator = random.Random(int(hashlib.sha256(seed_material.encode()).hexdigest(), 16))
    count = len(token_values)
    restart_probability = 1.0 / float(max(1, mean_block_length))
    samples: list[float] = []
    for _ in range(replicates):
        index = generator.randrange(count)
        total = 0.0
        for position in range(count):
            total += token_values[index]
            if position + 1 < count and generator.random() < restart_probability:
                index = generator.randrange(count)
            else:
                index = (index + 1) % count
        samples.append(total / count)
    samples.sort()
    return samples[max(0, math.floor(0.025 * replicates) - 1)]


def promote_survivors(
    folds: Sequence[FoldResult],
    *,
    registry: Sequence[ShockRecoveryCandidate] | None = None,
    config: ShockRecoveryGridConfig | None = None,
) -> tuple[dict[str, object], ...]:
    """Apply the preregistered development gates to selected test folds."""

    by_hash: dict[str, list[CandidateMetrics]] = defaultdict(list)
    for fold in folds:
        metric = fold.selected_test_metrics
        if metric is not None:
            by_hash[metric.candidate_sha256].append(metric)
    selected = ShockRecoveryGridConfig() if config is None else config
    z_threshold = Z_THRESHOLD
    results: list[dict[str, object]] = []
    for candidate_hash, metrics in sorted(by_hash.items()):
        total = sum(item.total_pnl_quote for item in metrics)
        outcomes = sum(item.outcomes for item in metrics)
        mints = len({outcome.mint for item in metrics for outcome in item.outcome_rows})
        mean_event = sum(item.total_pnl_quote for item in metrics) / outcomes if outcomes else None
        aggregate_by_mint: dict[str, float] = defaultdict(float)
        for metric in metrics:
            for outcome in metric.outcome_rows:
                aggregate_by_mint[outcome.mint] += outcome.pnl_quote
        token_values = tuple(aggregate_by_mint.values())
        mean_token = sum(token_values) / len(token_values) if token_values else None
        z_values = [value for item in metrics if (value := _student_z(item)) is not None]
        stouffer = (
            sum(z_values) / math.sqrt(len(z_values))
            if len(z_values) == len(metrics) and metrics
            else None
        )
        fold_positive = all((item.total_pnl_quote > 0.0) for item in metrics)
        positive_total = sum(max(0.0, item.total_pnl_quote) for item in metrics)
        no_fold_dominates = bool(positive_total) and all(
            item.total_pnl_quote <= positive_total * 0.5 for item in metrics
        )
        bootstrap_lower = _stationary_bootstrap_lower_95(
            token_values,
            replicates=selected.bootstrap_replicates,
            mean_block_length=selected.bootstrap_block_length_mints,
            seed_material=selected.bootstrap_seed,
        )
        exact_replay_positive = bool(
            metrics
            and total > 0.0
            and all(
                math.isfinite(outcome.pnl_quote)
                for item in metrics
                for outcome in item.outcome_rows
            )
        )
        # FX is an explicit promotion input.  It is absent from this
        # development-only runner, so shared-capital replay remains locked.
        shared_capital_positive = False
        gate = (
            len(metrics) >= 3
            and outcomes >= 1_000
            and mints >= 500
            and total > 0.0
            and mean_event is not None
            and mean_event > 0.0
            and mean_token is not None
            and mean_token > 0.0
            and fold_positive
            and no_fold_dominates
            and stouffer is not None
            and stouffer >= z_threshold
            and bootstrap_lower is not None
            and bootstrap_lower > 0.0
            and exact_replay_positive
            and shared_capital_positive
        )
        results.append(
            {
                "candidate_sha256": candidate_hash,
                "selected_fold_count": len(metrics),
                "outcomes": outcomes,
                "mints": mints,
                "total_pnl_quote": total,
                "mean_event_pnl_quote": mean_event,
                "mean_token_pnl_quote": mean_token,
                "stouffer_z": stouffer,
                "z_threshold": z_threshold,
                "bootstrap_lower_95": bootstrap_lower,
                "exact_replay_positive": exact_replay_positive,
                "shared_capital_positive": shared_capital_positive,
                "no_fold_dominates": no_fold_dominates,
                "eligible": gate,
            }
        )
    return tuple(item for item in results if bool(item["eligible"]))


def registry_payload(
    *,
    experiment_id: str = EXPERIMENT_ID,
    config: ShockRecoveryGridConfig | None = None,
) -> dict[str, object]:
    selected = ShockRecoveryGridConfig() if config is None else config
    candidates = default_shock_recovery_candidates()
    return {
        "kind": REGISTRY_KIND,
        "version": VERSION,
        "experiment_id": experiment_id,
        "candidate_count": len(candidates),
        "candidate_order_sha256": _sha256_bytes(
            "\n".join(item.sha256 for item in candidates).encode()
        ),
        "cumulative_trial_count": PRIOR_ATTEMPTED_CANDIDATES + len(candidates),
        "z_threshold": Z_THRESHOLD,
        "config": asdict(selected),
        "holdout_policy": {"accessed": False, "network_accessed": False},
        "candidates": [
            {
                "sha256": item.sha256,
                "name": item.name,
                "shock_measure": item.shock_measure.value,
                "shock_length": item.shock_length,
                "recovery_measure": item.recovery_measure.value,
                "shock_quantile": item.shock_quantile,
                "recovery_quantile": item.recovery_quantile,
                "state_profile": item.state_profile.value,
                "holding_events": item.holding_events,
                "threshold_policy": "train_only_quantile_cont",
            }
            for item in candidates
        ],
    }


def _record_for_threshold(threshold: FittedThresholds, *, experiment_id: str) -> dict[str, object]:
    return {
        "kind": THRESHOLD_KIND,
        "version": VERSION,
        "experiment_id": experiment_id,
        **asdict(threshold),
    }


def _record_for_metric(metric: CandidateMetrics, *, experiment_id: str) -> dict[str, object]:
    payload = asdict(metric)
    payload.pop("outcome_rows", None)
    return {
        "kind": METRIC_KIND,
        "version": VERSION,
        "experiment_id": experiment_id,
        "metrics": payload,
    }


def _atomic_write(path: Path, body: bytes, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"refusing to overwrite {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with temporary.open("wb") as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def write_local_report(
    result: DevelopmentResult,
    output_dir: Path,
    *,
    config: ShockRecoveryGridConfig | None = None,
    overwrite: bool = False,
) -> dict[str, object]:
    """Write payloads first and the integrity manifest last.

    A zero-survivor result is a valid completed experiment; it receives the
    same manifest and hash checks as a positive result.
    """

    selected = ShockRecoveryGridConfig() if config is None else config
    output_dir.mkdir(parents=True, exist_ok=True)
    registry_path = output_dir / "registry.json"
    thresholds_path = output_dir / "thresholds.jsonl"
    metrics_path = output_dir / "metrics.jsonl"
    report_path = output_dir / "report.json"
    manifest_path = output_dir / "manifest.json"
    registry_body = _canonical_json(
        registry_payload(experiment_id=result.experiment_id, config=selected)
    )
    threshold_lines = [
        _canonical_json(_record_for_threshold(item, experiment_id=result.experiment_id))
        for fold in result.folds
        for item in fold.thresholds
    ]
    metric_lines = [
        _canonical_json(_record_for_metric(metric, experiment_id=result.experiment_id))
        for fold in result.folds
        for metric in (
            *fold.validation_metrics,
            *(tuple() if fold.selected_test_metrics is None else (fold.selected_test_metrics,)),
        )
    ]
    report_payload: dict[str, object] = {
        "kind": REPORT_KIND,
        "version": VERSION,
        "experiment_id": result.experiment_id,
        "status": result.status,
        "valid": True,
        "zero_survivor": not bool(result.promoted),
        "candidate_count": result.candidate_count,
        "trial_denominator": result.trial_denominator,
        "z_threshold": Z_THRESHOLD,
        "fold_count": len(result.folds),
        "promoted": list(result.promoted),
        "holdout_accessed": False,
        "network_accessed": False,
        "config": asdict(selected),
        "folds": [
            {
                "number": fold.number,
                "train_mints": list(fold.train_mints),
                "validation_mints": list(fold.validation_mints),
                "test_mints": list(fold.test_mints),
                "train_mints_sha256": _sequence_sha256(fold.train_mints),
                "validation_mints_sha256": _sequence_sha256(fold.validation_mints),
                "test_mints_sha256": _sequence_sha256(fold.test_mints),
                "train_cutoff_ns": fold.train_cutoff_ns,
                "selected_candidate_sha256": fold.selected_candidate_sha256,
                "selected_test_metrics": None
                if fold.selected_test_metrics is None
                else _record_for_metric(
                    fold.selected_test_metrics, experiment_id=result.experiment_id
                ),
            }
            for fold in result.folds
        ],
    }
    report_body = _canonical_json(report_payload)
    # Refuse overwrite consistently for all payloads, including an existing
    # manifest.  This protects the append-only/final-manifest contract.
    for path in (registry_path, thresholds_path, metrics_path, report_path, manifest_path):
        if path.exists() and not overwrite:
            raise FileExistsError(f"refusing to overwrite {path}")
    _atomic_write(registry_path, registry_body, overwrite=overwrite)
    _atomic_write(thresholds_path, b"".join(threshold_lines), overwrite=overwrite)
    _atomic_write(metrics_path, b"".join(metric_lines), overwrite=overwrite)
    _atomic_write(report_path, report_body, overwrite=overwrite)
    files = {}
    for path in (registry_path, thresholds_path, metrics_path, report_path):
        files[path.name] = {"byte_count": path.stat().st_size, "sha256": sha256_file(path)}
    manifest_payload: dict[str, object] = {
        "kind": MANIFEST_KIND,
        "version": VERSION,
        "experiment_id": result.experiment_id,
        "status": "complete",
        "valid": True,
        "zero_survivor": not bool(result.promoted),
        "files": files,
        "report_sha256": files["report.json"]["sha256"],
        "holdout_accessed": False,
        "network_accessed": False,
    }
    _atomic_write(manifest_path, _canonical_json(manifest_payload), overwrite=overwrite)
    return manifest_payload


def verify_local_report(output_dir: Path) -> dict[str, object]:
    """Verify every manifest-listed payload and reject tampering."""

    manifest_path = output_dir / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ShockRecoveryGridError("manifest is missing or not a regular file")
    try:
        value = json.loads(manifest_path.read_bytes())
    except (OSError, json.JSONDecodeError) as error:
        raise ShockRecoveryGridError("manifest is invalid JSON") from error
    if (
        not isinstance(value, dict)
        or value.get("kind") != MANIFEST_KIND
        or value.get("version") != VERSION
    ):
        raise ShockRecoveryGridError("manifest kind/version is unsupported")
    manifest = cast(dict[str, object], value)
    if (
        manifest.get("holdout_accessed") is not False
        or manifest.get("network_accessed") is not False
    ):
        raise ShockRecoveryGridError("manifest reports forbidden holdout/network access")
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise ShockRecoveryGridError("manifest files is not an object")
    for name, metadata in files.items():
        if not isinstance(name, str) or not isinstance(metadata, dict):
            raise ShockRecoveryGridError("manifest file entry is invalid")
        path = output_dir / name
        if path.is_symlink() or not path.is_file():
            raise ShockRecoveryGridError(f"manifested payload is missing: {name}")
        expected_size = metadata.get("byte_count")
        expected_hash = metadata.get("sha256")
        if (
            not isinstance(expected_size, int)
            or expected_size < 0
            or not isinstance(expected_hash, str)
        ):
            raise ShockRecoveryGridError(f"manifest metadata is invalid: {name}")
        if path.stat().st_size != expected_size or sha256_file(path) != expected_hash:
            raise ShockRecoveryGridError(f"manifested payload was tampered: {name}")
    if manifest.get("report_sha256") != sha256_file(output_dir / "report.json"):
        raise ShockRecoveryGridError("report hash binding is invalid")
    return manifest


# Descriptive aliases mirror the preregistration terminology.
nested_whole_token_walk_forward = nested_walk_forward_folds
evaluate_shock_recovery_registry = evaluate_registry
write_manifest_last = write_local_report
verify_manifest = verify_local_report


__all__ = [
    "ADVERSE_SELECTION_BPS",
    "CUMULATIVE_TRIAL_COUNT",
    "DEVELOPMENT_BOUNDARY_SLOT",
    "DEVELOPMENT_END_SLOT_EXCLUSIVE",
    "DEVELOPMENT_START_SLOT_INCLUSIVE",
    "DevelopmentResult",
    "FittedThresholds",
    "FoldResult",
    "MANIFEST_KIND",
    "METRIC_KIND",
    "NestedFold",
    "PositionOutcome",
    "REGISTRY_KIND",
    "REPORT_KIND",
    "ShockRecoveryGridConfig",
    "ShockRecoveryGridError",
    "TokenCohort",
    "THRESHOLD_KIND",
    "HOLDOUT_BOUNDARY_SLOT",
    "candidate_metrics",
    "default_shock_recovery_candidates",
    "evaluate_candidate",
    "evaluate_registry",
    "evaluate_shock_recovery_registry",
    "fit_candidate_thresholds",
    "nested_walk_forward_folds",
    "nested_whole_token_walk_forward",
    "promote_survivors",
    "registry_payload",
    "select_training_winner",
    "validate_development_rows",
    "validate_development_upper_bound",
    "verify_local_report",
    "verify_manifest",
    "write_manifest_last",
    "write_local_report",
]
