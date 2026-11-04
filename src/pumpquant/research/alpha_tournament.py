"""Quality-first, multi-family alpha tournament orchestration.

Stage one compares one directional feature at a time under common execution
profiles.  Risk measurements are excluded as standalone alpha
and may only be added to survivors as gates.  Pairwise combinations are built
from caller-supplied survivors, which prevents an immediate combinatorial
explosion and keeps the complete trial denominator auditable.
"""

from __future__ import annotations

import math
import multiprocessing as mp
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace

from pumpquant.core.schema import Bar
from pumpquant.execution.bar_replay import ExecutionConfig
from pumpquant.features.alpha import (
    CONT_OFI_URL,
    HAWKES_URL,
    INTRADAY_MOMENTUM_URL,
    PUMP_PREDICTION_URL,
    VPIN_URL,
    AlphaFeatureConfig,
)
from pumpquant.research.alpha_strategy import (
    AlphaBacktestResult,
    AlphaStrategySpec,
    PreparedAlphaBars,
    RuleOperator,
    SignalRule,
    alpha_spec_sha256,
    prepare_alpha_bars,
    run_prepared_alpha_backtest,
)
from pumpquant.validation.metrics import (
    Performance,
    deflated_sharpe_probability,
    performance,
    probabilistic_sharpe_ratio,
)
from pumpquant.validation.splits import WalkForwardFold, embargoed_walk_forward


@dataclass(frozen=True, slots=True)
class AlphaSignalAxis:
    """Pre-registered thresholds for one hypothesis family."""

    rank: int
    family: str
    feature: str
    operator: RuleOperator
    thresholds: tuple[float, ...]
    evidence: str
    pump_transfer: str
    primary_source: str
    rationale: str

    def __post_init__(self) -> None:
        if self.rank < 1 or not self.family.strip() or not self.thresholds:
            raise ValueError("invalid alpha signal axis")
        if not all(math.isfinite(value) for value in self.thresholds):
            raise ValueError("signal thresholds must be finite")
        for threshold in self.thresholds:
            SignalRule(self.feature, self.operator, threshold)


@dataclass(frozen=True, slots=True)
class RiskGateAxis:
    """A non-directional state or execution filter for survivor testing."""

    family: str
    feature: str
    operator: RuleOperator
    thresholds: tuple[float, ...]
    primary_source: str
    rationale: str

    def __post_init__(self) -> None:
        if not self.thresholds:
            raise ValueError("risk-gate thresholds cannot be empty")
        for threshold in self.thresholds:
            SignalRule(self.feature, self.operator, threshold)


@dataclass(frozen=True, slots=True)
class ExitProfile:
    name: str
    take_profit: float
    stop_loss: float
    trailing_stop: float
    max_holding_bars: int

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("exit profile name cannot be empty")
        if self.take_profit <= 0.0 or not 0.0 < self.stop_loss < 1.0:
            raise ValueError("invalid exit profile")
        if not 0.0 < self.trailing_stop < 1.0 or self.max_holding_bars < 1:
            raise ValueError("invalid exit profile")


DEFAULT_FEATURE_CONFIGS: tuple[AlphaFeatureConfig, ...] = (
    AlphaFeatureConfig(
        short_window=1,
        medium_window=3,
        long_window=8,
        opening_range_bars=2,
        vpin_window=4,
    ),
    AlphaFeatureConfig(
        short_window=2,
        medium_window=5,
        long_window=13,
        opening_range_bars=3,
        vpin_window=8,
    ),
    AlphaFeatureConfig(
        short_window=3,
        medium_window=8,
        long_window=21,
        opening_range_bars=5,
        vpin_window=8,
    ),
)


DEFAULT_EXIT_PROFILES: tuple[ExitProfile, ...] = (
    ExitProfile("fast", take_profit=0.15, stop_loss=0.08, trailing_stop=0.06, max_holding_bars=5),
    ExitProfile(
        "balanced",
        take_profit=0.30,
        stop_loss=0.15,
        trailing_stop=0.12,
        max_holding_bars=10,
    ),
    ExitProfile(
        "convex",
        take_profit=0.60,
        stop_loss=0.20,
        trailing_stop=0.15,
        max_holding_bars=20,
    ),
)


DEFAULT_SIGNAL_AXES: tuple[AlphaSignalAxis, ...] = (
    AlphaSignalAxis(
        1,
        "signed_flow_persistence",
        "signed_flow_short",
        RuleOperator.GREATER_EQUAL,
        (0.05, 0.15, 0.30, 0.50),
        "high",
        "high",
        CONT_OFI_URL,
        "Exact on-chain buy-minus-sell quote flow, tested before any interaction.",
    ),
    AlphaSignalAxis(
        1,
        "flow_per_amm_depth",
        "ofi_depth_proxy",
        RuleOperator.GREATER_EQUAL,
        (0.001, 0.005, 0.02, 0.05),
        "high_lob_analogue",
        "high_as_amm_adaptation",
        CONT_OFI_URL,
        "Signed quote flow normalized by executable AMM depth; never labeled native OFI.",
    ),
    AlphaSignalAxis(
        1,
        "signed_flow_acceleration",
        "flow_acceleration",
        RuleOperator.GREATER_EQUAL,
        (0.03, 0.10, 0.20, 0.35),
        "high",
        "high",
        CONT_OFI_URL,
        "Recent flow improvement relative to the longer causal window.",
    ),
    AlphaSignalAxis(
        2,
        "event_intensity_acceleration",
        "intensity_acceleration",
        RuleOperator.GREATER_EQUAL,
        (0.25, 0.50, 1.0, 2.0),
        "medium_high",
        "medium_high",
        HAWKES_URL,
        "A transparent precursor to pooled marked-Hawkes estimation.",
    ),
    AlphaSignalAxis(
        2,
        "reserve_progress_velocity",
        "reserve_progress_velocity",
        RuleOperator.GREATER_EQUAL,
        (0.0001, 0.0005, 0.002, 0.01),
        "pump_native",
        "high",
        PUMP_PREDICTION_URL,
        "How rapidly real reserve depletion advances, per second.",
    ),
    AlphaSignalAxis(
        3,
        "launch_age_momentum",
        "momentum_short",
        RuleOperator.GREATER_EQUAL,
        (0.02, 0.05, 0.10, 0.20),
        "medium",
        "medium",
        INTRADAY_MOMENTUM_URL,
        "Futures intraday momentum translated to pre-registered token-age windows.",
    ),
    AlphaSignalAxis(
        3,
        "launch_range_breakout",
        "launch_range_breakout",
        RuleOperator.GREATER_EQUAL,
        (0.0, 0.02, 0.05, 0.10),
        "medium_heavy_tuning_risk",
        "low_medium",
        INTRADAY_MOMENTUM_URL,
        "Breakout from a fixed first-N-bar range; the opening window is not optimized in-fold.",
    ),
    AlphaSignalAxis(
        3,
        "anchored_vwap_continuation",
        "anchored_vwap_deviation",
        RuleOperator.GREATER_EQUAL,
        (0.01, 0.03, 0.07, 0.15),
        "descriptive",
        "medium_high",
        PUMP_PREDICTION_URL,
        "Price strength relative to exact cumulative quote/base VWAP.",
    ),
    AlphaSignalAxis(
        4,
        "flow_exhaustion_reversal",
        "flow_acceleration",
        RuleOperator.LESS_EQUAL,
        (-0.03, -0.10, -0.20, -0.35),
        "medium",
        "low_medium",
        HAWKES_URL,
        "Long-only fade candidate; must later survive a stable-depth risk gate.",
    ),
    AlphaSignalAxis(
        4,
        "drawdown_reversal",
        "drawdown_from_high",
        RuleOperator.LESS_EQUAL,
        (-0.03, -0.08, -0.15, -0.30),
        "descriptive",
        "low_medium",
        HAWKES_URL,
        "Long-only reversal after a trailing drawdown; exploratory until flow confirms it.",
    ),
)


DEFAULT_RISK_GATES: tuple[RiskGateAxis, ...] = (
    RiskGateAxis(
        "toxicity",
        "vpin_proxy",
        RuleOperator.LESS_EQUAL,
        (0.40, 0.60, 0.80),
        VPIN_URL,
        "Contested VPIN adaptation is used only as a toxicity gate.",
    ),
    RiskGateAxis(
        "impact",
        "price_impact_1_quote_bps",
        RuleOperator.LESS_EQUAL,
        (100.0, 250.0, 500.0, 1_000.0),
        CONT_OFI_URL,
        "Reject states whose executable curvature overwhelms the signal.",
    ),
    RiskGateAxis(
        "volatility_regime",
        "volatility_ratio",
        RuleOperator.LESS_EQUAL,
        (1.0, 2.0, 4.0),
        INTRADAY_MOMENTUM_URL,
        "Avoid short-window volatility explosions relative to the long regime.",
    ),
    RiskGateAxis(
        "liquidity_stability",
        "liquidity_change",
        RuleOperator.GREATER_EQUAL,
        (-0.50, -0.20, 0.0),
        CONT_OFI_URL,
        "Require executable liquidity not to be collapsing too rapidly.",
    ),
)


def default_single_signal_specs(
    *,
    feature_configs: Sequence[AlphaFeatureConfig] = DEFAULT_FEATURE_CONFIGS,
    exit_profiles: Sequence[ExitProfile] = DEFAULT_EXIT_PROFILES,
    signal_axes: Sequence[AlphaSignalAxis] = DEFAULT_SIGNAL_AXES,
    min_liquidity_quote: float = 5.0,
    capacity_multiples: Sequence[float] = (0.0,),
    min_trade_count: int = 3,
    min_age_seconds: float = 5.0,
    max_age_seconds: float = 600.0,
) -> tuple[AlphaStrategySpec, ...]:
    """Materialise the bounded stage-one quality grid."""

    specs: list[AlphaStrategySpec] = []
    seen: set[str] = set()
    if not capacity_multiples or any(
        not math.isfinite(value) or value < 0.0 for value in capacity_multiples
    ):
        raise ValueError("capacity_multiples must be finite, nonnegative, and non-empty")
    for feature_config in feature_configs:
        window_name = (
            f"w{feature_config.short_window}-{feature_config.medium_window}-"
            f"{feature_config.long_window}"
        )
        for axis in sorted(signal_axes, key=lambda item: (item.rank, item.family)):
            for threshold in axis.thresholds:
                for exit_profile in exit_profiles:
                    for capacity_multiple in capacity_multiples:
                        base_name = (
                            f"single:{axis.family}:{axis.operator.value}:"
                            f"{threshold:g}:{window_name}:{exit_profile.name}"
                        )
                        spec = AlphaStrategySpec(
                            name=(
                                base_name
                                if capacity_multiple == 0.0
                                else f"{base_name}:capacity-{capacity_multiple:g}x"
                            ),
                            feature_config=feature_config,
                            entry_rules=(SignalRule(axis.feature, axis.operator, threshold),),
                            min_liquidity_quote=min_liquidity_quote,
                            min_signal_capacity_multiple=capacity_multiple,
                            min_trade_count=min_trade_count,
                            min_age_seconds=min_age_seconds,
                            max_age_seconds=max_age_seconds,
                            take_profit=exit_profile.take_profit,
                            stop_loss=exit_profile.stop_loss,
                            trailing_stop=exit_profile.trailing_stop,
                            max_holding_bars=exit_profile.max_holding_bars,
                        )
                        digest = alpha_spec_sha256(spec)
                        if digest not in seen:
                            seen.add(digest)
                            specs.append(spec)
    return tuple(specs)


def risk_gate_variants(
    survivor: AlphaStrategySpec,
    gates: Sequence[RiskGateAxis] = DEFAULT_RISK_GATES,
) -> tuple[AlphaStrategySpec, ...]:
    """Add exactly one non-directional gate to an already selected survivor."""

    variants: list[AlphaStrategySpec] = []
    existing = {rule.feature for rule in survivor.entry_rules}
    for gate in gates:
        if gate.feature in existing:
            continue
        for threshold in gate.thresholds:
            rule = SignalRule(gate.feature, gate.operator, threshold)
            variants.append(
                replace(
                    survivor,
                    name=(
                        f"gate:{survivor.name}:{gate.family}:{gate.operator.value}:{threshold:g}"
                    ),
                    entry_rules=survivor.entry_rules + (rule,),
                )
            )
    return tuple(variants)


def capacity_gate_variants(
    survivor: AlphaStrategySpec,
    multiples: Sequence[float] = (0.5, 1.0, 2.0, 5.0),
) -> tuple[AlphaStrategySpec, ...]:
    """Require signal-time capacity headroom before a delayed order is sent."""

    variants: list[AlphaStrategySpec] = []
    for multiple in multiples:
        if not math.isfinite(multiple) or multiple < 0.0:
            raise ValueError("capacity multiples must be finite and nonnegative")
        variants.append(
            replace(
                survivor,
                name=f"capacity:{survivor.name}:{multiple:g}x",
                min_signal_capacity_multiple=multiple,
            )
        )
    return tuple(variants)


def pairwise_survivor_combinations(
    survivors: Sequence[AlphaStrategySpec],
) -> tuple[AlphaStrategySpec, ...]:
    """Create unique pairwise AND rules only for compatible survivor specs."""

    combinations: list[AlphaStrategySpec] = []
    seen: set[str] = set()
    for left_index, left in enumerate(survivors):
        for right in survivors[left_index + 1 :]:
            compatible = (
                left.feature_config == right.feature_config
                and left.allowed_phases == right.allowed_phases
                and left.min_liquidity_quote == right.min_liquidity_quote
                and left.min_signal_capacity_multiple == right.min_signal_capacity_multiple
                and left.min_trade_count == right.min_trade_count
                and left.min_age_seconds == right.min_age_seconds
                and left.max_age_seconds == right.max_age_seconds
                and left.take_profit == right.take_profit
                and left.stop_loss == right.stop_loss
                and left.trailing_stop == right.trailing_stop
                and left.max_holding_bars == right.max_holding_bars
            )
            if not compatible:
                continue
            rules = tuple(sorted(set(left.entry_rules + right.entry_rules), key=repr))
            if len(rules) != 2 or rules[0].feature == rules[1].feature:
                continue
            spec = replace(
                left,
                name=f"pair:{left.name}+{right.name}",
                entry_rules=rules,
                exit_rule=None,
            )
            digest = alpha_spec_sha256(spec)
            if digest not in seen:
                seen.add(digest)
                combinations.append(spec)
    return tuple(combinations)


@dataclass(frozen=True, slots=True)
class TokenCohort:
    mint: str
    launch_time_ns: int


@dataclass(frozen=True, slots=True)
class AlphaCandidateEvaluation:
    spec: AlphaStrategySpec
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
        return alpha_spec_sha256(self.spec)


@dataclass(frozen=True, slots=True)
class AlphaFoldResult:
    number: int
    train_mints: tuple[str, ...]
    test_mints: tuple[str, ...]
    test_start_ns: int
    train_cutoff_ns: int
    candidate_evaluations: tuple[AlphaCandidateEvaluation, ...]
    selected: AlphaCandidateEvaluation | None
    test_backtest: AlphaBacktestResult | None


@dataclass(frozen=True, slots=True)
class AlphaTournamentReport:
    candidate_count: int
    trial_count: int
    folds: tuple[AlphaFoldResult, ...]
    outcome_diagnostics: Performance
    diagnostic_probabilistic_sharpe: float
    diagnostic_deflated_sharpe: float


_WORKER_PREPARED: PreparedAlphaBars | None = None
_WORKER_EXECUTION: ExecutionConfig | None = None
_WORKER_MINIMUM_POSITIONS: int | None = None
_WORKER_MINIMUM_RETURN: float | None = None


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
    return {mint: tuple(values) for mint, values in indexed.items()}


def _candidate_evaluation(
    backtest: AlphaBacktestResult,
    spec: AlphaStrategySpec,
    minimum_positions: int,
    minimum_return_on_attempted_capital: float,
) -> AlphaCandidateEvaluation:
    returns = backtest.returns
    return AlphaCandidateEvaluation(
        spec=spec,
        performance=performance(returns),
        probabilistic_sharpe=probabilistic_sharpe_ratio(returns),
        total_quote_pnl=backtest.total_quote_pnl,
        attempted_capital_quote=backtest.attempted_capital_quote,
        return_on_attempted_capital=backtest.return_on_attempted_capital,
        rejected_orders=backtest.rejected_orders,
        position_outcomes=backtest.position_outcomes,
        eligible=(
            backtest.position_outcomes >= minimum_positions
            and backtest.return_on_attempted_capital > minimum_return_on_attempted_capital
        ),
    )


def _evaluate_worker(spec: AlphaStrategySpec) -> AlphaCandidateEvaluation:
    """Fork-worker entry point; prepared rows are inherited copy-on-write."""

    if (
        _WORKER_PREPARED is None
        or _WORKER_EXECUTION is None
        or _WORKER_MINIMUM_POSITIONS is None
        or _WORKER_MINIMUM_RETURN is None
    ):
        raise RuntimeError("alpha evaluation worker was not initialized")
    return _candidate_evaluation(
        run_prepared_alpha_backtest(_WORKER_PREPARED, spec, _WORKER_EXECUTION),
        spec,
        _WORKER_MINIMUM_POSITIONS,
        _WORKER_MINIMUM_RETURN,
    )


def _selection_key(
    evaluation: AlphaCandidateEvaluation,
) -> tuple[float, float, float, float, str]:
    if not evaluation.eligible:
        return (-math.inf, -math.inf, -math.inf, -math.inf, evaluation.candidate_sha256)
    # Every candidate in a fold uses the same quote notional and execution
    # model. Prefer realised quote profit after all modelled costs over a sparse,
    # high-percentage anomaly. RoAC and PSR remain deterministic tie-breakers.
    return (
        evaluation.total_quote_pnl,
        evaluation.return_on_attempted_capital,
        evaluation.probabilistic_sharpe,
        -evaluation.rejected_orders,
        evaluation.candidate_sha256,
    )


def _evaluate_candidates(
    bars: list[Bar],
    candidates: tuple[AlphaStrategySpec, ...],
    config: ExecutionConfig,
    minimum_positions: int,
    minimum_return_on_attempted_capital: float,
    max_workers: int,
) -> tuple[AlphaCandidateEvaluation, ...]:
    grouped: dict[AlphaFeatureConfig, list[AlphaStrategySpec]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.feature_config, []).append(candidate)
    evaluations: list[AlphaCandidateEvaluation] = []
    for feature_config, specs in grouped.items():
        prepared = prepare_alpha_bars(bars, feature_config)
        worker_count = min(max_workers, len(specs))
        if worker_count > 1 and "fork" in mp.get_all_start_methods():
            global _WORKER_PREPARED, _WORKER_EXECUTION, _WORKER_MINIMUM_POSITIONS
            global _WORKER_MINIMUM_RETURN
            _WORKER_PREPARED = prepared
            _WORKER_EXECUTION = config
            _WORKER_MINIMUM_POSITIONS = minimum_positions
            _WORKER_MINIMUM_RETURN = minimum_return_on_attempted_capital
            try:
                context = mp.get_context("fork")
                with context.Pool(processes=worker_count) as pool:
                    evaluations.extend(pool.map(_evaluate_worker, specs, chunksize=1))
            finally:
                _WORKER_PREPARED = None
                _WORKER_EXECUTION = None
                _WORKER_MINIMUM_POSITIONS = None
                _WORKER_MINIMUM_RETURN = None
        else:
            for spec in specs:
                evaluations.append(
                    _candidate_evaluation(
                        run_prepared_alpha_backtest(prepared, spec, config),
                        spec,
                        minimum_positions,
                        minimum_return_on_attempted_capital,
                    )
                )
    evaluations.sort(key=lambda item: item.candidate_sha256)
    return tuple(evaluations)


def _run_fold(
    bars_by_mint: Mapping[str, Sequence[Bar]],
    fold: WalkForwardFold[TokenCohort],
    candidates: tuple[AlphaStrategySpec, ...],
    config: ExecutionConfig,
    minimum_train_positions: int,
    minimum_train_return_on_attempted_capital: float,
    max_train_tokens: int | None,
    embargo_ns: int,
    max_workers: int,
) -> AlphaFoldResult:
    effective_train = fold.train if max_train_tokens is None else fold.train[-max_train_tokens:]
    train_mints = tuple(cohort.mint for cohort in effective_train)
    test_mints = tuple(cohort.mint for cohort in fold.test)
    train_cutoff = fold.test_start_ns - embargo_ns
    train_bars = [
        bar
        for mint in train_mints
        for bar in bars_by_mint.get(mint, ())
        if (bar.end_time_ns if bar.available_at_ns is None else bar.available_at_ns) < train_cutoff
    ]
    test_bars = [bar for mint in test_mints for bar in bars_by_mint.get(mint, ())]
    evaluations = _evaluate_candidates(
        train_bars,
        candidates,
        config,
        minimum_train_positions,
        minimum_train_return_on_attempted_capital,
        max_workers,
    )
    eligible = [evaluation for evaluation in evaluations if evaluation.eligible]
    selected = max(eligible, key=_selection_key) if eligible else None
    test_backtest = (
        None
        if selected is None
        else run_prepared_alpha_backtest(
            prepare_alpha_bars(test_bars, selected.spec.feature_config),
            selected.spec,
            config,
        )
    )
    return AlphaFoldResult(
        number=fold.number,
        train_mints=train_mints,
        test_mints=test_mints,
        test_start_ns=fold.test_start_ns,
        train_cutoff_ns=train_cutoff,
        candidate_evaluations=evaluations,
        selected=selected,
        test_backtest=test_backtest,
    )


def alpha_walk_forward_tournament(
    bars: Iterable[Bar],
    candidates: Iterable[AlphaStrategySpec],
    config: ExecutionConfig,
    *,
    min_train_tokens: int,
    test_tokens: int,
    embargo_ns: int = 0,
    minimum_train_positions: int = 5,
    minimum_train_return_on_attempted_capital: float = 0.0,
    max_train_tokens: int | None = None,
    max_workers: int = 1,
    on_fold: Callable[[AlphaFoldResult], None] | None = None,
) -> AlphaTournamentReport:
    """Select on past token cohorts and replay each test cohort once.

    All candidate/fold evaluations remain in the report and therefore count in
    the DSR denominator.  CSCV/PBO, block-bootstrap White Reality Check, and an
    untouched final holdout remain promotion gates outside this bounded first
    screen.
    """

    materialized = bars if isinstance(bars, (list, tuple)) else list(bars)
    if max_workers < 1:
        raise ValueError("max_workers must be positive")
    if not math.isfinite(minimum_train_return_on_attempted_capital):
        raise ValueError("minimum training return must be finite")
    if max_train_tokens is not None and max_train_tokens < min_train_tokens:
        raise ValueError("max_train_tokens cannot be smaller than min_train_tokens")
    candidate_values: list[AlphaStrategySpec] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, AlphaStrategySpec):
            raise TypeError("candidates must contain AlphaStrategySpec values")
        digest = alpha_spec_sha256(candidate)
        if digest not in seen:
            seen.add(digest)
            candidate_values.append(candidate)
    if not candidate_values:
        raise ValueError("at least one candidate is required")
    candidate_tuple = tuple(candidate_values)
    folds = embargoed_walk_forward(
        _cohorts(materialized),
        min_train_size=min_train_tokens,
        test_size=test_tokens,
        embargo_ns=embargo_ns,
    )
    if not folds:
        raise ValueError("not enough chronological token cohorts for one fold")
    indexed = _bars_by_mint(materialized)
    fold_values: list[AlphaFoldResult] = []
    for source_fold in folds:
        result = _run_fold(
            indexed,
            source_fold,
            candidate_tuple,
            config,
            minimum_train_positions,
            minimum_train_return_on_attempted_capital,
            max_train_tokens,
            embargo_ns,
            max_workers,
        )
        fold_values.append(result)
        if on_fold is not None:
            on_fold(result)
    fold_results = tuple(fold_values)
    oos_returns: list[float] = []
    trial_sharpes: list[float] = []
    for result_fold in fold_results:
        trial_sharpes.extend(
            evaluation.performance.sharpe for evaluation in result_fold.candidate_evaluations
        )
        if result_fold.test_backtest is not None:
            oos_returns.extend(result_fold.test_backtest.returns)
    return AlphaTournamentReport(
        candidate_count=len(candidate_tuple),
        trial_count=len(candidate_tuple) * len(fold_results),
        folds=fold_results,
        outcome_diagnostics=performance(oos_returns),
        diagnostic_probabilistic_sharpe=probabilistic_sharpe_ratio(oos_returns),
        diagnostic_deflated_sharpe=deflated_sharpe_probability(oos_returns, trial_sharpes),
    )
