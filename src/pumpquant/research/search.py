"""Scalable, execution-aware walk-forward strategy search.

The first version of the search layer accepted a tuple of candidates.  That is
convenient for a smoke test, but it quietly turns a large Cartesian product
into an unbounded allocation.  This module keeps the old API while adding
re-iterable candidate streams, content addressed specifications, optional
successive-halving/racing, and a durable trial ledger.

The search remains deliberately conservative: all splits are whole-token,
chronological cohorts, a candidate is selected from training bars only, and
the test bars are replayed exactly once for the selected specification.
"""

from __future__ import annotations

import fcntl
import hashlib
import itertools
import json
import math
import os
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Protocol, TypeVar

from pumpquant.core.schema import Bar, Side
from pumpquant.execution.bar_replay import (
    BacktestResult,
    ExecutionConfig,
    Fill,
    MomentumSpec,
    OpenPosition,
    OrderFailure,
    PreparedBacktestBars,
    RoundTrip,
    prepare_backtest_bars,
    run_backtest,
    run_prepared_backtest,
)
from pumpquant.validation.metrics import (
    Performance,
    deflated_sharpe_probability,
    performance,
    probabilistic_sharpe_ratio,
)
from pumpquant.validation.splits import WalkForwardFold, embargoed_walk_forward


@dataclass(frozen=True, slots=True)
class TokenCohort:
    mint: str
    launch_time_ns: int


@dataclass(frozen=True, slots=True)
class CandidateEvaluation:
    spec: MomentumSpec
    performance: Performance
    probabilistic_sharpe: float
    total_quote_pnl: float
    attempted_capital_quote: float
    return_on_attempted_capital: float
    rejected_orders: int
    position_outcomes: int
    eligible: bool

    @property
    def candidate_sha256(self) -> str:
        """Content hash used by the experiment ledger and tie breaking."""
        return momentum_spec_sha256(self.spec)


@dataclass(frozen=True, slots=True)
class FoldSearchResult:
    number: int
    train_mints: tuple[str, ...]
    test_mints: tuple[str, ...]
    test_start_ns: int
    train_cutoff_ns: int
    candidate_evaluations: tuple[CandidateEvaluation, ...]
    selected: CandidateEvaluation | None
    test_backtest: BacktestResult | None
    test_outcome_diagnostics: Performance
    attempted_candidates: int = 0
    failed_candidates: int = 0
    pruned_candidates: int = 0


@dataclass(frozen=True, slots=True)
class SearchReport:
    candidate_count: int
    trial_count: int
    folds: tuple[FoldSearchResult, ...]
    outcome_diagnostics: Performance
    diagnostic_probabilistic_sharpe: float
    diagnostic_deflated_sharpe: float
    failed_trial_count: int = 0
    pruned_trial_count: int = 0
    ledger_rows_appended: int = 0
    evaluation_count: int = 0


def _json_safe(value: Any) -> Any:
    """Convert dataclass payloads into strict JSON values.

    ``json.dumps`` otherwise emits non-standard ``NaN``/``Infinity`` tokens
    for a few valid small-sample diagnostics.  JSONL records use ``null`` for
    those values so independent readers agree on the representation.
    """
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _resume_json_safe(value: Any) -> Any:
    """JSON-safe payload retaining non-finite diagnostics for exact resume."""
    if isinstance(value, Mapping):
        return {str(key): _resume_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_resume_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return {"__pumpquant_float__": repr(value)}
    if isinstance(value, Enum):
        return value.value
    return value


def _spec_payload(spec: MomentumSpec) -> dict[str, Any]:
    # ``asdict`` is stable for this frozen dataclass, and sorting the resulting
    # JSON keys makes hashes independent of Python dictionary insertion order.
    payload = asdict(spec)
    # Python considers ``-0.0 == 0.0``; normalize signed zero so equal specs
    # cannot acquire distinct content hashes.
    float_fields = {
        "entry_momentum",
        "entry_buy_imbalance",
        "min_liquidity_quote",
        "min_age_seconds",
        "max_age_seconds",
        "exit_momentum",
        "take_profit",
        "stop_loss",
        "trailing_stop",
    }
    return {key: (float(value) if key in float_fields else value) for key, value in payload.items()}


def momentum_spec_sha256(spec: MomentumSpec) -> str:
    """Return the canonical SHA-256 content hash for one strategy spec."""
    encoded = json.dumps(
        _spec_payload(spec), sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


FOLD_MINT_HASH_SCHEME = "sha256-length-prefixed-utf8-v1"


def mint_sequence_sha256(mints: Iterable[str]) -> str:
    """Hash an ordered mint sequence without JSON or delimiter ambiguity."""

    digest = hashlib.sha256()
    for mint in mints:
        if not isinstance(mint, str):
            raise TypeError("mint sequence values must be strings")
        encoded = mint.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


# Short aliases are useful to callers that use a generic candidate protocol.
candidate_sha256 = momentum_spec_sha256
spec_sha256 = momentum_spec_sha256
hash_momentum_spec = momentum_spec_sha256
spec_hash = momentum_spec_sha256


T = TypeVar("T")


class CandidateStream(Protocol):
    """Minimal re-iterable protocol accepted by :func:`walk_forward_search`."""

    def __iter__(self) -> Iterator[MomentumSpec]: ...


def _dedupe_values(values: Iterable[T]) -> tuple[T, ...]:
    """Stable, small-axis deduplication without relying on value ordering."""
    result: list[T] = []
    for value in values:
        if value not in result:
            result.append(value)
    return tuple(result)


@dataclass(frozen=True, slots=True)
class MomentumGridStream(Iterable[MomentumSpec]):
    """A lazy Cartesian product of momentum parameter axes.

    Axes are the only materialized state.  A product of billions of points
    therefore remains cheap to construct and can be consumed in batches by a
    search caller.  Each axis is stably deduplicated at construction time.
    """

    momentum_lookbacks: tuple[int, ...]
    entry_momenta: tuple[float, ...]
    entry_imbalances: tuple[float, ...]
    take_profits: tuple[float, ...]
    stop_losses: tuple[float, ...]
    template: MomentumSpec
    deduplicated: ClassVar[bool] = True

    def __iter__(self) -> Iterator[MomentumSpec]:
        for lookback, momentum, imbalance, take_profit, stop_loss in itertools.product(
            self.momentum_lookbacks,
            self.entry_momenta,
            self.entry_imbalances,
            self.take_profits,
            self.stop_losses,
        ):
            yield MomentumSpec(
                momentum_lookback=lookback,
                volume_lookback=self.template.volume_lookback,
                entry_momentum=momentum,
                entry_buy_imbalance=imbalance,
                min_liquidity_quote=self.template.min_liquidity_quote,
                min_trade_count=self.template.min_trade_count,
                min_age_seconds=self.template.min_age_seconds,
                max_age_seconds=self.template.max_age_seconds,
                exit_momentum=self.template.exit_momentum,
                take_profit=take_profit,
                stop_loss=stop_loss,
                trailing_stop=self.template.trailing_stop,
                max_holding_bars=self.template.max_holding_bars,
            )

    def __len__(self) -> int:
        return math.prod(
            (
                len(self.momentum_lookbacks),
                len(self.entry_momenta),
                len(self.entry_imbalances),
                len(self.take_profits),
                len(self.stop_losses),
            )
        )


def momentum_grid_stream(
    *,
    momentum_lookbacks: Sequence[int] = (2, 3, 5),
    entry_momenta: Sequence[float] = (0.03, 0.06),
    entry_imbalances: Sequence[float] = (0.1, 0.3),
    take_profits: Sequence[float] = (0.2, 0.35),
    stop_losses: Sequence[float] = (0.1, 0.2),
    template: MomentumSpec | None = None,
) -> MomentumGridStream:
    """Construct a deterministic lazy Cartesian candidate stream."""
    base = template or MomentumSpec()
    axes = (
        _dedupe_values(momentum_lookbacks),
        _dedupe_values(entry_momenta),
        _dedupe_values(entry_imbalances),
        _dedupe_values(take_profits),
        _dedupe_values(stop_losses),
    )
    if any(not axis for axis in axes):
        raise ValueError("candidate axes cannot be empty")
    return MomentumGridStream(*axes, template=base)


def momentum_grid(
    *,
    momentum_lookbacks: Sequence[int] = (2, 3, 5),
    entry_momenta: Sequence[float] = (0.03, 0.06),
    entry_imbalances: Sequence[float] = (0.1, 0.3),
    take_profits: Sequence[float] = (0.2, 0.35),
    stop_losses: Sequence[float] = (0.1, 0.2),
    template: MomentumSpec | None = None,
) -> tuple[MomentumSpec, ...]:
    """Create the original explicitly enumerable baseline grid.

    New code should prefer :func:`momentum_grid_stream`; this wrapper remains
    for compatibility and is intentionally the only API that materializes the
    Cartesian product.
    """
    return tuple(
        momentum_grid_stream(
            momentum_lookbacks=momentum_lookbacks,
            entry_momenta=entry_momenta,
            entry_imbalances=entry_imbalances,
            take_profits=take_profits,
            stop_losses=stop_losses,
            template=template,
        )
    )


# Explicit lazy aliases for callers that prefer an iterator-oriented name.
iter_momentum_grid = momentum_grid_stream
lazy_momentum_grid = momentum_grid_stream
lazy_momentum_grid_stream = momentum_grid_stream


def _halton(index: int, base: int) -> float:
    result = 0.0
    factor = 1.0 / base
    while index:
        index, remainder = divmod(index, base)
        result += remainder * factor
        factor /= base
    return result


def _seed_shift(seed: int, dimension: int) -> float:
    digest = hashlib.sha256(f"pumpquant-halton:{seed}:{dimension}".encode("ascii")).digest()
    return int.from_bytes(digest, "big") / 2**256


@dataclass(frozen=True, slots=True)
class MomentumLowDiscrepancyStream(Iterable[MomentumSpec]):
    """Deterministic seeded Halton candidates with stable hash deduplication."""

    count: int
    seed: int
    template: MomentumSpec
    momentum_lookback_range: tuple[int, int]
    entry_momentum_range: tuple[float, float]
    entry_imbalance_range: tuple[float, float]
    take_profit_range: tuple[float, float]
    stop_loss_range: tuple[float, float]
    deduplicated: ClassVar[bool] = True

    def __post_init__(self) -> None:
        if self.count < 0:
            raise ValueError("count cannot be negative")
        low, high = self.momentum_lookback_range
        if low < 1 or high < low:
            raise ValueError("invalid momentum_lookback_range")
        for name, bounds in (
            ("entry_momentum_range", self.entry_momentum_range),
            ("entry_imbalance_range", self.entry_imbalance_range),
            ("take_profit_range", self.take_profit_range),
            ("stop_loss_range", self.stop_loss_range),
        ):
            if not all(math.isfinite(float(value)) for value in bounds) or bounds[1] < bounds[0]:
                raise ValueError(f"invalid {name}")

    @staticmethod
    def _sample(bounds: tuple[float, float], unit: float) -> float:
        low, high = bounds
        return low + (high - low) * unit

    def __iter__(self) -> Iterator[MomentumSpec]:
        seen: set[str] = set()
        emitted = 0
        attempt = 0
        # A duplicate can only arise from quantizing an integer lookback or
        # from a deliberately degenerate range.  Stop rather than spin forever
        # when a caller asks for more unique points than the space contains.
        max_attempts = max(self.count * 100 + 100, 100)
        primes = (2, 3, 5, 7, 11)
        while emitted < self.count and attempt < max_attempts:
            index = attempt + 1
            units = [
                (_halton(index, prime) + _seed_shift(self.seed, dimension)) % 1.0
                for dimension, prime in enumerate(primes)
            ]
            lookback_low, lookback_high = self.momentum_lookback_range
            lookback = lookback_low + int(units[0] * (lookback_high - lookback_low + 1))
            spec = MomentumSpec(
                momentum_lookback=lookback,
                volume_lookback=self.template.volume_lookback,
                entry_momentum=self._sample(self.entry_momentum_range, units[1]),
                entry_buy_imbalance=self._sample(self.entry_imbalance_range, units[2]),
                min_liquidity_quote=self.template.min_liquidity_quote,
                min_trade_count=self.template.min_trade_count,
                min_age_seconds=self.template.min_age_seconds,
                max_age_seconds=self.template.max_age_seconds,
                exit_momentum=self.template.exit_momentum,
                take_profit=self._sample(self.take_profit_range, units[3]),
                stop_loss=self._sample(self.stop_loss_range, units[4]),
                trailing_stop=self.template.trailing_stop,
                max_holding_bars=self.template.max_holding_bars,
            )
            digest = momentum_spec_sha256(spec)
            if digest not in seen:
                seen.add(digest)
                emitted += 1
                yield spec
            attempt += 1
        if emitted < self.count:
            raise ValueError(
                "low-discrepancy ranges contain fewer unique MomentumSpec values than requested"
            )

    def __len__(self) -> int:
        return self.count


def momentum_low_discrepancy_stream(
    count: int,
    *,
    seed: int = 0,
    template: MomentumSpec | None = None,
    momentum_lookback_range: tuple[int, int] = (1, 32),
    entry_momentum_range: tuple[float, float] = (-0.1, 0.3),
    entry_imbalance_range: tuple[float, float] = (-1.0, 1.0),
    take_profit_range: tuple[float, float] = (0.01, 1.0),
    stop_loss_range: tuple[float, float] = (0.01, 0.95),
) -> MomentumLowDiscrepancyStream:
    """Construct a reproducible low-discrepancy MomentumSpec stream."""
    return MomentumLowDiscrepancyStream(
        count=count,
        seed=seed,
        template=template or MomentumSpec(),
        momentum_lookback_range=momentum_lookback_range,
        entry_momentum_range=entry_momentum_range,
        entry_imbalance_range=entry_imbalance_range,
        take_profit_range=take_profit_range,
        stop_loss_range=stop_loss_range,
    )


# Naming variants make the generic idea discoverable without duplicating logic.
seeded_low_discrepancy_momentum_stream = momentum_low_discrepancy_stream
low_discrepancy_momentum_stream = momentum_low_discrepancy_stream
halton_momentum_stream = momentum_low_discrepancy_stream


def seeded_low_discrepancy_candidates(
    count: int,
    *,
    seed: int = 0,
    template: MomentumSpec | None = None,
    bounds: Mapping[str, tuple[float, float] | tuple[int, int]] | None = None,
) -> MomentumLowDiscrepancyStream:
    """Generic convenience wrapper accepting parameter-name bounds."""
    values: dict[str, Any] = {
        "momentum_lookback_range": (1, 32),
        "entry_momentum_range": (-0.1, 0.3),
        "entry_imbalance_range": (-1.0, 1.0),
        "take_profit_range": (0.01, 1.0),
        "stop_loss_range": (0.01, 0.95),
    }
    if bounds:
        for key, pair in bounds.items():
            normalized = key if key.endswith("_range") else f"{key}_range"
            if normalized not in values:
                raise ValueError(f"unsupported MomentumSpec bound {key!r}")
            values[normalized] = pair
    return momentum_low_discrepancy_stream(
        count,
        seed=seed,
        template=template,
        **values,
    )


class ExperimentLedger:
    """Append-only fsync-safe JSONL ledger for every candidate/fold trial.

    A process-local object may be shared by several searches.  Each append
    takes an advisory file lock, writes one complete JSON line, flushes, and
    fsyncs before releasing the lock.  A row is emitted exactly once for every
    candidate/fold terminal state (completed, failed, or pruned).
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.rows = 0

    def append(self, record: Mapping[str, Any]) -> None:
        payload = _json_safe(dict(record))
        line = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        fd = os.open(self.path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            with os.fdopen(fd, "a", encoding="utf-8") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    handle.write(line)
                    handle.flush()
                    os.fsync(handle.fileno())
                    self.rows += 1
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except BaseException:
            # fdopen owns the descriptor after entering the context.  If an
            # exception occurs before that point, close the raw descriptor.
            with suppress(OSError):
                os.close(fd)
            raise

    def append_trial(self, record: Mapping[str, Any] | None = None, **fields: Any) -> None:
        """Append a trial payload (convenience alias for :meth:`append`)."""
        payload: dict[str, Any] = {} if record is None else dict(record)
        payload.update(fields)
        self.append(payload)

    def read_records(self) -> tuple[dict[str, Any], ...]:
        """Read committed JSONL rows for deterministic search resumption."""
        if not self.path.exists():
            return ()
        records: list[dict[str, Any]] = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(
                        f"invalid experiment ledger JSON at line {line_number}"
                    ) from error
                if not isinstance(payload, dict):
                    raise ValueError(f"experiment ledger row {line_number} is not an object")
                records.append(payload)
        return tuple(records)

    def __enter__(self) -> ExperimentLedger:
        return self

    def __exit__(self, *_: object) -> None:
        return None


def _launch_time(bar: Bar) -> int:
    if bar.token_age_seconds is None:
        raise ValueError(f"token_age_seconds is required for search ({bar.mint})")
    launch = int(bar.end_time_ns - round(float(bar.token_age_seconds) * 1_000_000_000))
    if launch < 0:
        raise ValueError(f"derived launch time is negative for {bar.mint}")
    return launch


def _cohorts(bars: Sequence[Bar]) -> tuple[TokenCohort, ...]:
    launch_by_mint: dict[str, int] = {}
    for bar in bars:
        launch = _launch_time(bar)
        previous = launch_by_mint.setdefault(bar.mint, launch)
        if abs(previous - launch) > 1_000_000:
            raise ValueError(f"inconsistent derived launch time for {bar.mint}")
    return tuple(TokenCohort(mint, launch) for mint, launch in launch_by_mint.items())


def _bars_by_mint(bars: Iterable[Bar]) -> dict[str, tuple[Bar, ...]]:
    indexed: dict[str, list[Bar]] = {}
    for bar in bars:
        indexed.setdefault(bar.mint, []).append(bar)
    return {mint: tuple(mint_bars) for mint, mint_bars in indexed.items()}


def _candidate_evaluation(
    backtest: BacktestResult,
    spec: MomentumSpec,
    minimum_positions: int,
) -> CandidateEvaluation:
    returns = backtest.returns
    stats = performance(returns)
    evaluation = CandidateEvaluation(
        spec=spec,
        performance=stats,
        probabilistic_sharpe=probabilistic_sharpe_ratio(returns),
        total_quote_pnl=backtest.total_quote_pnl,
        attempted_capital_quote=backtest.attempted_capital_quote,
        return_on_attempted_capital=backtest.return_on_attempted_capital,
        rejected_orders=backtest.rejected_orders,
        position_outcomes=backtest.position_outcomes,
        eligible=backtest.position_outcomes >= minimum_positions,
    )
    return evaluation


def _evaluate(
    bars: list[Bar],
    spec: MomentumSpec,
    config: ExecutionConfig,
    minimum_positions: int,
) -> CandidateEvaluation:
    return _candidate_evaluation(
        run_backtest(bars, spec, config),
        spec,
        minimum_positions,
    )


def _evaluate_prepared(
    prepared: PreparedBacktestBars,
    spec: MomentumSpec,
    config: ExecutionConfig,
    minimum_positions: int,
) -> CandidateEvaluation:
    return _candidate_evaluation(
        run_prepared_backtest(prepared, spec, config),
        spec,
        minimum_positions,
    )


def _selection_key(evaluation: CandidateEvaluation) -> tuple[float, float, float, float]:
    if not evaluation.eligible:
        return (-math.inf, -math.inf, -math.inf, -math.inf)
    # PSR/DSR remain audit diagnostics until a shared-capital calendar-time
    # return series exists.  They must not drive candidate selection here.
    return (
        evaluation.return_on_attempted_capital,
        evaluation.total_quote_pnl,
        evaluation.performance.mean_return,
        -evaluation.rejected_orders,
    )


def _deterministic_selection_key(
    evaluation: CandidateEvaluation,
) -> tuple[float, float, float, float, str]:
    """Make exact training ties independent of worker partitioning."""

    return (*_selection_key(evaluation), evaluation.candidate_sha256)


@dataclass(slots=True)
class _TrialState:
    spec: MomentumSpec
    digest: str
    attempted_rounds: list[int] = field(default_factory=list)
    evaluation: CandidateEvaluation | None = None
    test_backtest: BacktestResult | None = None
    error: str | None = None
    resumed: bool = False


def _iter_unique(candidates: Iterable[MomentumSpec]) -> Iterator[MomentumSpec]:
    known_unique = bool(getattr(candidates, "deduplicated", False))
    seen: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, MomentumSpec):
            raise TypeError("candidate streams must yield MomentumSpec values")
        digest = momentum_spec_sha256(candidate)
        if known_unique or digest not in seen:
            if not known_unique:
                seen.add(digest)
            yield candidate


def _record_float(payload: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    value = payload.get(key, default)
    if isinstance(value, Mapping) and value.get("__pumpquant_float__") in {
        "inf",
        "-inf",
        "nan",
    }:
        return float(value["__pumpquant_float__"])
    return default if value is None else float(value)


def _evaluation_from_record(
    record: Mapping[str, Any], spec: MomentumSpec
) -> CandidateEvaluation | None:
    payload = record.get("resume_evaluation", record.get("evaluation"))
    if not isinstance(payload, Mapping):
        return None
    performance_payload = payload.get("performance")
    if not isinstance(performance_payload, Mapping):
        return None
    stats = Performance(
        observations=int(performance_payload.get("observations", 0)),
        compounded_return=_record_float(performance_payload, "compounded_return"),
        mean_return=_record_float(performance_payload, "mean_return"),
        volatility=_record_float(performance_payload, "volatility"),
        sharpe=_record_float(performance_payload, "sharpe"),
        sortino=_record_float(performance_payload, "sortino"),
        sequence_max_drawdown=_record_float(performance_payload, "sequence_max_drawdown"),
        hit_rate=_record_float(performance_payload, "hit_rate"),
        profit_factor=_record_float(performance_payload, "profit_factor"),
        skewness=_record_float(performance_payload, "skewness"),
        excess_kurtosis=_record_float(performance_payload, "excess_kurtosis"),
    )
    return CandidateEvaluation(
        spec=spec,
        performance=stats,
        probabilistic_sharpe=_record_float(payload, "probabilistic_sharpe"),
        total_quote_pnl=_record_float(payload, "total_quote_pnl"),
        attempted_capital_quote=_record_float(payload, "attempted_capital_quote"),
        return_on_attempted_capital=_record_float(payload, "return_on_attempted_capital"),
        rejected_orders=int(payload.get("rejected_orders", 0)),
        position_outcomes=int(payload.get("position_outcomes", 0)),
        eligible=bool(payload.get("eligible", False)),
    )


def _fill_from_record(payload: Mapping[str, Any]) -> Fill:
    return Fill(
        market_id=str(payload["market_id"]),
        mint=str(payload["mint"]),
        quote_mint=str(payload["quote_mint"]),
        time_ns=int(payload["time_ns"]),
        available_at_ns=int(payload["available_at_ns"]),
        side=Side(str(payload["side"])),
        base_amount=float(payload["base_amount"]),
        quote_amount=float(payload["quote_amount"]),
        mark_price=float(payload["mark_price"]),
        average_price=float(payload["average_price"]),
        curve_impact_bps=float(payload["curve_impact_bps"]),
        venue_fee_quote=float(payload["venue_fee_quote"]),
        fixed_cost_quote=float(payload["fixed_cost_quote"]),
        latency_bars=int(payload["latency_bars"]),
        model=str(payload["model"]),
    )


def _backtest_from_record(payload: Mapping[str, Any]) -> BacktestResult:
    trades: list[RoundTrip] = []
    for item in payload.get("trades", []):
        if not isinstance(item, Mapping):
            continue
        trades.append(
            RoundTrip(
                market_id=str(item["market_id"]),
                mint=str(item["mint"]),
                quote_mint=str(item["quote_mint"]),
                entry=_fill_from_record(item["entry"]),
                exit=_fill_from_record(item["exit"]),
                entry_signal_time_ns=int(item["entry_signal_time_ns"]),
                exit_signal_time_ns=int(item["exit_signal_time_ns"]),
                exit_reason=str(item["exit_reason"]),
                quote_pnl=float(item["quote_pnl"]),
                return_pct=float(item["return_pct"]),
                holding_bars=int(item["holding_bars"]),
                prior_failure_cost_quote=float(item["prior_failure_cost_quote"]),
            )
        )
    open_positions: list[OpenPosition] = []
    for item in payload.get("open_positions", []):
        if not isinstance(item, Mapping):
            continue
        open_positions.append(
            OpenPosition(
                market_id=str(item["market_id"]),
                mint=str(item["mint"]),
                quote_mint=str(item["quote_mint"]),
                entry=_fill_from_record(item["entry"]),
                entry_signal_time_ns=int(item["entry_signal_time_ns"]),
                as_of_time_ns=int(item["as_of_time_ns"]),
                liquidation_value_quote=float(item["liquidation_value_quote"]),
                prior_failure_cost_quote=float(item["prior_failure_cost_quote"]),
                quote_pnl=float(item["quote_pnl"]),
                return_pct=float(item["return_pct"]),
                holding_bars=int(item["holding_bars"]),
                valuation_reason=str(item["valuation_reason"]),
            )
        )
    failures: list[OrderFailure] = []
    for item in payload.get("failures", []):
        if not isinstance(item, Mapping):
            continue
        failures.append(
            OrderFailure(
                market_id=str(item["market_id"]),
                mint=str(item["mint"]),
                quote_mint=str(item["quote_mint"]),
                time_ns=int(item["time_ns"]),
                side=Side(str(item["side"])),
                fixed_cost_quote=float(item["fixed_cost_quote"]),
                reason=str(item["reason"]),
                position_related=bool(item["position_related"]),
            )
        )
    return BacktestResult(
        spec=MomentumSpec(**payload["spec"]),
        config=ExecutionConfig(**payload["config"]),
        trades=tuple(trades),
        open_positions=tuple(open_positions),
        failures=tuple(failures),
        missed_entry_orders=int(payload["missed_entry_orders"]),
        missed_exit_orders=int(payload["missed_exit_orders"]),
    )


def _resource_sizes(train_size: int, minimum_size: int, rounds: int) -> tuple[int, ...]:
    if rounds < 1:
        raise ValueError("halving_rounds must be positive")
    if train_size < 1:
        return ()
    sizes: list[int] = []
    for round_number in range(1, rounds + 1):
        size = max(minimum_size, math.ceil(train_size * round_number / rounds))
        size = min(train_size, size)
        if not sizes or size != sizes[-1]:
            sizes.append(size)
    return tuple(sizes)


def _ranked(states: Iterable[_TrialState], *, final: bool) -> list[_TrialState]:
    def key(state: _TrialState) -> tuple[float, float, float, float, str]:
        if state.evaluation is None:
            score = (-math.inf, -math.inf, -math.inf, -math.inf)
        elif final:
            score = _selection_key(state.evaluation)
        else:
            # Early resources may not yet have enough positions for the final
            # eligibility threshold.  Rank their provisional execution score
            # rather than pruning every candidate as an apparent tie.
            score = (
                state.evaluation.return_on_attempted_capital,
                state.evaluation.total_quote_pnl,
                state.evaluation.performance.mean_return,
                -state.evaluation.rejected_orders,
            )
        return (*score, state.digest)

    return sorted(
        states,
        key=key,
        reverse=True,
    )


@dataclass(frozen=True, slots=True)
class _FoldLedgerIdentity:
    train_mint_count: int
    train_mints_sha256: str
    test_mint_count: int
    test_mints_sha256: str


def _fold_ledger_identity(fold: WalkForwardFold[TokenCohort]) -> _FoldLedgerIdentity:
    """Compute the compact fold identity once, rather than once per trial."""

    return _FoldLedgerIdentity(
        train_mint_count=len(fold.train),
        train_mints_sha256=mint_sequence_sha256(cohort.mint for cohort in fold.train),
        test_mint_count=len(fold.test),
        test_mints_sha256=mint_sequence_sha256(cohort.mint for cohort in fold.test),
    )


def _append_trial(
    ledger: ExperimentLedger | None,
    *,
    experiment_id: str,
    data_sha256: str | None,
    config: ExecutionConfig,
    fold: WalkForwardFold[TokenCohort],
    fold_identity: _FoldLedgerIdentity,
    train_cutoff_ns: int,
    state: _TrialState,
    status: str,
    selected: bool,
    final_round: int,
    resource_sizes: tuple[int, ...],
    test_backtest: BacktestResult | None = None,
) -> None:
    if ledger is None:
        return
    evaluation = None if state.evaluation is None else asdict(state.evaluation)
    resume_evaluation = None if state.evaluation is None else asdict(state.evaluation)
    if evaluation is not None:
        # These sequence-level diagnostics are intentionally not treated as
        # portfolio evidence.  Keep the ledger aligned with the CLI's fixed
        # notional diagnostics while retaining all execution/trial fields.
        performance_payload = evaluation.get("performance")
        if isinstance(performance_payload, dict):
            performance_payload.pop("compounded_return", None)
            performance_payload.pop("sequence_max_drawdown", None)
    ledger.append(
        {
            "ledger_schema_version": 3,
            "experiment_id": experiment_id,
            "data_sha256": data_sha256,
            "fold": fold.number,
            "fold_mint_hash_scheme": FOLD_MINT_HASH_SCHEME,
            "train_mint_count": fold_identity.train_mint_count,
            "train_mints_sha256": fold_identity.train_mints_sha256,
            "test_mint_count": fold_identity.test_mint_count,
            "test_mints_sha256": fold_identity.test_mints_sha256,
            "test_start_ns": fold.test_start_ns,
            "train_cutoff_ns": train_cutoff_ns,
            "candidate_sha256": state.digest,
            "candidate_id": state.digest,
            "trial_id": f"{experiment_id}:fold-{fold.number}:candidate-{state.digest}",
            "spec": _spec_payload(state.spec),
            "status": status,
            "attempted": True,
            "selected": selected,
            "attempted_rounds": list(state.attempted_rounds),
            "final_round": final_round,
            "resource_sizes": list(resource_sizes),
            "execution_config": asdict(config),
            "evaluation": evaluation,
            # Keep the complete immutable training result for exact resume;
            # ``evaluation`` remains the fixed-notional reporting view.
            "resume_evaluation": _resume_json_safe(resume_evaluation),
            "test_backtest": None if test_backtest is None else asdict(test_backtest),
            "error": state.error,
        }
    )


def _resume_index(
    ledger: ExperimentLedger | None,
    *,
    experiment_id: str,
    data_sha256: str | None,
    config: ExecutionConfig,
) -> dict[tuple[int, str], dict[str, Any]]:
    if ledger is None:
        return {}
    index: dict[tuple[int, str], dict[str, Any]] = {}
    for record in ledger.read_records():
        if record.get("experiment_id") != experiment_id:
            continue
        if data_sha256 is not None and record.get("data_sha256") != data_sha256:
            continue
        if record.get("execution_config") != asdict(config):
            continue
        status = record.get("status")
        digest = record.get("candidate_sha256")
        fold_number = record.get("fold")
        if status not in {"completed", "failed", "pruned"}:
            continue
        if not isinstance(digest, str) or not isinstance(fold_number, int):
            continue
        # The last committed terminal row wins if a user manually retried a
        # trial.  Search itself only emits one row per terminal candidate/fold.
        index[(fold_number, digest)] = record
    return index


def _run_fold(
    bars_by_mint: Mapping[str, Sequence[Bar]],
    fold: WalkForwardFold[TokenCohort],
    candidates: Iterable[MomentumSpec],
    config: ExecutionConfig,
    *,
    minimum_train_positions: int,
    min_train_tokens: int,
    embargo_ns: int,
    candidate_batch_size: int,
    survivor_count: int | None,
    halving_rounds: int,
    halving_eta: float,
    candidate_count_hint: int | None,
    ledger: ExperimentLedger | None,
    experiment_id: str,
    data_sha256: str | None,
    first_fold: bool,
    resume_index: Mapping[tuple[int, str], dict[str, Any]] | None,
    defer_test_replay: bool,
) -> tuple[FoldSearchResult, int, int, int, list[float], int]:
    train_mints_ordered = tuple(cohort.mint for cohort in fold.train)
    fold_identity = _fold_ledger_identity(fold)
    train_mints = set(train_mints_ordered)
    test_mints = {cohort.mint for cohort in fold.test}
    train_cutoff = fold.test_start_ns - embargo_ns
    train_bars_by_mint = {
        mint: tuple(
            bar
            for bar in bars_by_mint.get(mint, ())
            if (bar.end_time_ns if bar.available_at_ns is None else bar.available_at_ns)
            < train_cutoff
        )
        for mint in train_mints_ordered
    }
    test_bars = [bar for cohort in fold.test for bar in bars_by_mint.get(cohort.mint, ())]
    resource_sizes = _resource_sizes(len(train_mints_ordered), min_train_tokens, halving_rounds)
    if not resource_sizes:
        raise ValueError("training fold contains no cohorts")

    if candidate_batch_size < 1:
        raise ValueError("candidate_batch_size must be positive")
    if survivor_count is not None and survivor_count < 1:
        raise ValueError("survivor_count must be positive")
    if halving_eta <= 1.0 or not math.isfinite(halving_eta):
        raise ValueError("halving_eta must be finite and greater than one")

    # With racing enabled, keep a bounded default even when the caller did not
    # provide an explicit survivor count.  The first round remains streaming;
    # no candidate tuple is constructed.
    default_race_cap = survivor_count
    if halving_rounds > 1 and default_race_cap is None:
        default_race_cap = candidate_batch_size

    active: list[_TrialState] = []
    attempted = failed = pruned = 0
    trial_sharpes: list[float] = []
    observed_candidates = 0

    for round_number, resource_size in enumerate(resource_sizes, start=1):
        source: Iterable[MomentumSpec | _TrialState]
        source = _iter_unique(candidates) if round_number == 1 else active
        round_bars = [
            bar for mint in train_mints_ordered[:resource_size] for bar in train_bars_by_mint[mint]
        ]
        final_round = round_number == len(resource_sizes)
        # Compute the frontier before consuming the stream whenever possible.
        # This is what keeps a capped final round from accumulating every
        # evaluation just to sort it at the end.
        if final_round:
            target = survivor_count
        else:
            total_hint = candidate_count_hint or len(active)
            target = max(1, math.ceil(total_hint / halving_eta)) if total_hint else None
            if default_race_cap is not None:
                target = default_race_cap if target is None else min(target, default_race_cap)
        round_states: list[_TrialState] = []
        pending: list[_TrialState] = []

        def append_failed(
            state: _TrialState,
            error: Exception,
            *,
            _round_number: int = round_number,
        ) -> None:
            nonlocal failed
            state.error = f"{type(error).__name__}: {error}"
            failed += 1
            # A failed trial has no finite Sharpe.  Record a neutral zero
            # observation so DSR's candidate denominator still covers it.
            trial_sharpes.append(0.0)
            _append_trial(
                ledger,
                experiment_id=experiment_id,
                data_sha256=data_sha256,
                config=config,
                fold=fold,
                fold_identity=fold_identity,
                train_cutoff_ns=train_cutoff,
                state=state,
                status="failed",
                selected=False,
                final_round=_round_number,
                resource_sizes=resource_sizes,
            )

        def append_evaluated(
            state: _TrialState,
            evaluation: CandidateEvaluation,
            *,
            _round_number: int = round_number,
            _final_round: bool = final_round,
            _target: int | None = target,
        ) -> None:
            nonlocal pruned, round_states
            state.attempted_rounds.append(_round_number)
            state.evaluation = evaluation
            if _final_round:
                trial_sharpes.append(evaluation.performance.sharpe)
            round_states.append(state)
            if _target is None or len(round_states) <= _target:
                return
            ranked = _ranked(round_states, final=_final_round)
            round_states = ranked[:_target]
            dropped_state = ranked[_target]
            pruned += 1
            _append_trial(
                ledger,
                experiment_id=experiment_id,
                data_sha256=data_sha256,
                config=config,
                fold=fold,
                fold_identity=fold_identity,
                train_cutoff_ns=train_cutoff,
                state=dropped_state,
                status="pruned",
                selected=False,
                final_round=_round_number,
                resource_sizes=resource_sizes,
            )
            # One terminal row and one DSR observation per candidate/fold.
            if not _final_round and dropped_state.evaluation is not None:
                trial_sharpes.append(dropped_state.evaluation.performance.sharpe)

        def flush_pending(
            *,
            _pending: list[_TrialState] = pending,
            _round_bars: list[Bar] = round_bars,
        ) -> None:
            if not _pending:
                return
            grouped: dict[tuple[int, int], list[_TrialState]] = {}
            for state in _pending:
                key = (state.spec.momentum_lookback, state.spec.volume_lookback)
                grouped.setdefault(key, []).append(state)
            outcomes: dict[str, CandidateEvaluation | Exception] = {}
            for (momentum_lookback, volume_lookback), states in grouped.items():
                try:
                    prepared = prepare_backtest_bars(
                        _round_bars,
                        momentum_lookback=momentum_lookback,
                        volume_lookback=volume_lookback,
                    )
                except Exception as error:
                    for state in states:
                        outcomes[state.digest] = error
                    continue
                for state in states:
                    try:
                        outcomes[state.digest] = _evaluate_prepared(
                            prepared,
                            state.spec,
                            config,
                            minimum_train_positions,
                        )
                    except Exception as error:
                        outcomes[state.digest] = error
            # Feature work is grouped, but ranking and ledger order remain the
            # original candidate-stream order.
            for state in _pending:
                outcome = outcomes[state.digest]
                if isinstance(outcome, Exception):
                    append_failed(state, outcome)
                else:
                    append_evaluated(state, outcome)
            _pending.clear()

        for item in source:
            resumed_state = item if isinstance(item, _TrialState) else None
            if resumed_state is not None:
                spec = resumed_state.spec
            else:
                if not isinstance(item, MomentumSpec):
                    raise TypeError("candidate streams must yield MomentumSpec values")
                spec = item
            digest = momentum_spec_sha256(spec)
            if resumed_state is not None and resumed_state.resumed:
                flush_pending()
                # A committed terminal row already contains the full training
                # result.  Carry the frontier forward without replaying it.
                round_states.append(resumed_state)
                if target is not None and len(round_states) > target:
                    ranked = _ranked(round_states, final=final_round)
                    round_states = ranked[:target]
                    if ranked[target] is not resumed_state:
                        pruned += 1
                continue
            if round_number == 1:
                state = _TrialState(spec=spec, digest=digest)
                observed_candidates += 1
                attempted += 1
            else:
                # ``active`` is already deduplicated and bounded from the prior
                # round.  Looking up by hash preserves its deterministic order.
                state = (
                    resumed_state
                    if resumed_state is not None
                    else _TrialState(spec=spec, digest=digest)
                )
                attempted += 1
            record = (
                resume_index.get((fold.number, digest))
                if resume_index is not None and round_number == 1
                else None
            )
            if record is not None and record.get("resource_sizes") == list(resource_sizes):
                flush_pending()
                attempted_rounds = record.get("attempted_rounds")
                committed_attempts = (
                    len(attempted_rounds)
                    if isinstance(attempted_rounds, list) and attempted_rounds
                    else 1
                )
                # The first-round candidate was already counted above; only
                # add prior resource replays recovered from the ledger.
                attempted += max(0, committed_attempts - 1)
                status = record.get("status")
                if status == "failed":
                    failed += 1
                    trial_sharpes.append(0.0)
                    continue
                if status == "pruned":
                    pruned += 1
                    resumed_evaluation = _evaluation_from_record(record, spec)
                    if resumed_evaluation is not None:
                        trial_sharpes.append(resumed_evaluation.performance.sharpe)
                    continue
                resumed_evaluation = _evaluation_from_record(record, spec)
                if resumed_evaluation is not None:
                    state.evaluation = resumed_evaluation
                    state.resumed = True
                    cached_test = record.get("test_backtest")
                    if isinstance(cached_test, Mapping):
                        try:
                            state.test_backtest = _backtest_from_record(cached_test)
                        except (KeyError, TypeError, ValueError):
                            state.test_backtest = None
                    round_states.append(state)
                    trial_sharpes.append(resumed_evaluation.performance.sharpe)
                    continue
            pending.append(state)
            if len(pending) >= candidate_batch_size:
                flush_pending()
        flush_pending()
        active = round_states

        if not active:
            break

    evaluations = [state.evaluation for state in active if state.evaluation is not None]
    # If a final cap is not used, ``active`` preserves candidate stream order;
    # otherwise ranking order gives deterministic bounded output.
    selected = max(evaluations, key=_deterministic_selection_key) if evaluations else None
    selected_result = selected if selected is not None and selected.eligible else None

    if selected_result is None or defer_test_replay:
        test_backtest = None
        test_stats = performance([])
    else:
        selected_state = next(
            state
            for state in active
            if state.evaluation is not None and state.spec == selected_result.spec
        )
        test_backtest = selected_state.test_backtest
        if test_backtest is None:
            test_backtest = run_backtest(test_bars, selected_result.spec, config)
        test_stats = performance(test_backtest.returns)

    # Successful survivors are emitted only after selection, so the selected
    # bit is accurate without ever consulting fold-test observations.  The
    # selected test replay is cached in its row for exact resume.
    for state in active:
        if state.evaluation is None or state.resumed:
            continue
        _append_trial(
            ledger,
            experiment_id=experiment_id,
            data_sha256=data_sha256,
            config=config,
            fold=fold,
            fold_identity=fold_identity,
            train_cutoff_ns=train_cutoff,
            state=state,
            status="completed",
            selected=selected_result is not None and state.spec == selected_result.spec,
            final_round=len(resource_sizes),
            resource_sizes=resource_sizes,
            test_backtest=(
                test_backtest
                if selected_result is not None and state.spec == selected_result.spec
                else None
            ),
        )

    result = FoldSearchResult(
        number=fold.number,
        train_mints=tuple(sorted(train_mints)),
        test_mints=tuple(sorted(test_mints)),
        test_start_ns=fold.test_start_ns,
        train_cutoff_ns=train_cutoff,
        candidate_evaluations=tuple(
            evaluation for evaluation in evaluations if evaluation is not None
        ),
        selected=selected_result,
        test_backtest=test_backtest,
        test_outcome_diagnostics=test_stats,
        attempted_candidates=attempted,
        failed_candidates=failed,
        pruned_candidates=pruned,
    )
    return (
        result,
        attempted,
        failed,
        pruned,
        trial_sharpes,
        observed_candidates,
    )


def walk_forward_search(
    bars: Iterable[Bar],
    candidates: Iterable[MomentumSpec],
    config: ExecutionConfig,
    *,
    min_train_tokens: int,
    test_tokens: int,
    embargo_ns: int = 0,
    minimum_train_positions: int = 5,
    candidate_batch_size: int = 256,
    survivor_count: int | None = None,
    halving_rounds: int = 1,
    halving_eta: float = 3.0,
    ledger: ExperimentLedger | str | os.PathLike[str] | None = None,
    experiment_id: str = "search",
    data_sha256: str | None = None,
    resume: bool = False,
    defer_test_replay: bool = False,
) -> SearchReport:
    """Run chronological search over a lazy or ordinary candidate iterable.

    ``halving_rounds > 1`` evaluates all candidates on progressively larger
    *past* training-cohort prefixes and retains only the best eta fraction at
    each stage.  ``survivor_count`` bounds final candidate/evaluation state;
    ``candidate_batch_size`` bounds the default racing frontier.  The fold test
    cohort is replayed only for the selected training candidate.

    With ``resume=True`` and an ``ExperimentLedger`` (or ledger path), matching
    committed terminal rows are carried forward by fold/spec hash without
    re-running their training backtests.  The candidate stream is still
    consumed to prove the multiplicity denominator is unchanged.

    Candidate iterables must be re-iterable when more than one fold or racing
    round is requested.  ``MomentumGridStream`` and
    ``MomentumLowDiscrepancyStream`` satisfy this without materializing the
    Cartesian product.  A one-shot iterator is accepted for a single fold.
    """
    materialized = bars if isinstance(bars, (list, tuple)) else list(bars)
    if not materialized:
        raise ValueError("bars cannot be empty")
    if minimum_train_positions < 1:
        raise ValueError("minimum_train_positions must be positive")
    if candidate_batch_size < 1:
        raise ValueError("candidate_batch_size must be positive")
    if halving_rounds < 1:
        raise ValueError("halving_rounds must be positive")
    if halving_eta <= 1.0 or not math.isfinite(halving_eta):
        raise ValueError("halving_eta must be finite and greater than one")

    folds = embargoed_walk_forward(
        _cohorts(materialized),
        min_train_size=min_train_tokens,
        test_size=test_tokens,
        embargo_ns=embargo_ns,
    )
    if not folds:
        raise ValueError("not enough chronological token cohorts for one fold")
    indexed_bars = _bars_by_mint(materialized)

    first_iterator = iter(candidates)
    reiterable = iter(candidates) is not first_iterator
    if (len(folds) > 1 or halving_rounds > 1) and not reiterable:
        raise ValueError(
            "candidate iterable must be re-iterable for multiple folds or halving rounds"
        )

    try:
        candidate_count_hint = len(candidates)  # type: ignore[arg-type]
    except TypeError:
        candidate_count_hint = None

    owned_ledger: ExperimentLedger | None = None
    if isinstance(ledger, (str, os.PathLike)):
        owned_ledger = ExperimentLedger(ledger)
        ledger_sink: ExperimentLedger | None = owned_ledger
    else:
        ledger_sink = ledger
    ledger_rows_before = ledger_sink.rows if ledger_sink is not None else 0
    resume_records = (
        _resume_index(
            ledger_sink,
            experiment_id=experiment_id,
            data_sha256=data_sha256,
            config=config,
        )
        if resume
        else None
    )

    fold_results: list[FoldSearchResult] = []
    trial_sharpes: list[float] = []
    candidate_count: int | None = None
    trial_count = failed_count = pruned_count = evaluation_count = 0
    for fold_number, fold in enumerate(folds):
        source: Iterable[MomentumSpec]
        source = first_iterator if fold_number == 0 and not reiterable else candidates
        result, attempted, failed, pruned, sharpes, observed = _run_fold(
            indexed_bars,
            fold,
            source,
            config,
            minimum_train_positions=minimum_train_positions,
            min_train_tokens=min_train_tokens,
            embargo_ns=embargo_ns,
            candidate_batch_size=candidate_batch_size,
            survivor_count=survivor_count,
            halving_rounds=halving_rounds,
            halving_eta=halving_eta,
            candidate_count_hint=candidate_count_hint,
            ledger=ledger_sink,
            experiment_id=experiment_id,
            data_sha256=data_sha256,
            first_fold=fold_number == 0,
            resume_index=resume_records,
            defer_test_replay=defer_test_replay,
        )
        if observed == 0:
            raise ValueError("at least one candidate is required")
        if candidate_count is None:
            candidate_count = observed
        elif observed != candidate_count:
            raise ValueError("candidate stream changed between folds")
        fold_results.append(result)
        # ``attempted`` includes racing re-evaluations; trial_count is the
        # multiplicity denominator of terminal candidate/fold trials.
        evaluation_count += attempted
        trial_count += observed
        failed_count += failed
        pruned_count += pruned
        trial_sharpes.extend(sharpes)

    oos_returns: list[float] = []
    for result in fold_results:
        if result.test_backtest is not None:
            oos_returns.extend(result.test_backtest.returns)
    oos_stats = performance(oos_returns)
    return SearchReport(
        candidate_count=candidate_count or 0,
        trial_count=trial_count,
        folds=tuple(fold_results),
        outcome_diagnostics=oos_stats,
        diagnostic_probabilistic_sharpe=probabilistic_sharpe_ratio(oos_returns),
        diagnostic_deflated_sharpe=deflated_sharpe_probability(oos_returns, trial_sharpes),
        failed_trial_count=failed_count,
        pruned_trial_count=pruned_count,
        ledger_rows_appended=(ledger_sink.rows - ledger_rows_before if ledger_sink else 0),
        evaluation_count=evaluation_count,
    )
