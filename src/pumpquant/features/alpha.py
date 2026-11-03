"""Research-backed causal features for Pump bonding curves and PumpSwap.

The registry deliberately distinguishes AMM-native measurements from concepts
adapted from central-limit-order-book or futures research.  In particular,
``ofi_depth_proxy`` and ``vpin_proxy`` are bar-level AMM analogues; they are not
observed order-flow imbalance, queue imbalance, microprice, or native L2 data.

Every output row uses only bars whose snapshots are available no later than
that row.  A strategy still has to wait for its configured execution latency,
which is enforced by :mod:`pumpquant.alpha_strategy`.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from pumpquant.core.schema import Bar, VenuePhase

CONT_OFI_URL = "https://arxiv.org/abs/1011.6402"
VPIN_URL = "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1695596"
HAWKES_URL = "https://arxiv.org/abs/1301.1135"
INTRADAY_MOMENTUM_URL = "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3760365"
TSMOM_URL = "https://w4.stern.nyu.edu/facdir/lpederse/papers/TimeSeriesMomentum.pdf"
AMIHUD_URL = "https://doi.org/10.1016/S1386-4181(01)00024-6"
PUMP_PREDICTION_URL = "https://arxiv.org/abs/2602.14860"
CFMM_URL = "https://arxiv.org/abs/1911.03380"


@dataclass(frozen=True, slots=True)
class AlphaFeatureDefinition:
    """Auditable meaning and provenance for one scalar feature."""

    name: str
    family: str
    role: str
    evidence: str
    pump_transfer: str
    required_fields: tuple[str, ...]
    primary_source: str
    description: str


def _feature(
    name: str,
    family: str,
    role: str,
    evidence: str,
    pump_transfer: str,
    required_fields: tuple[str, ...],
    primary_source: str,
    description: str,
) -> AlphaFeatureDefinition:
    return AlphaFeatureDefinition(
        name=name,
        family=family,
        role=role,
        evidence=evidence,
        pump_transfer=pump_transfer,
        required_fields=required_fields,
        primary_source=primary_source,
        description=description,
    )


ALPHA_FEATURES: tuple[AlphaFeatureDefinition, ...] = (
    _feature(
        "return_1",
        "price_path",
        "directional",
        "measurement",
        "high",
        ("virtual_base_reserve", "virtual_quote_reserve"),
        CFMM_URL,
        "One-bar return of the reserve-implied marginal price.",
    ),
    _feature(
        "momentum_short",
        "momentum",
        "directional",
        "medium",
        "medium",
        ("virtual_base_reserve", "virtual_quote_reserve"),
        INTRADAY_MOMENTUM_URL,
        "Reserve-mark return over the configured short launch-relative window.",
    ),
    _feature(
        "momentum_medium",
        "momentum",
        "directional",
        "medium",
        "medium",
        ("virtual_base_reserve", "virtual_quote_reserve"),
        INTRADAY_MOMENTUM_URL,
        "Reserve-mark return over the configured medium window.",
    ),
    _feature(
        "momentum_long",
        "momentum",
        "directional",
        "high_futures_low_horizon_transfer",
        "low_medium",
        ("virtual_base_reserve", "virtual_quote_reserve"),
        TSMOM_URL,
        "Reserve-mark return over the configured long window; not a literal futures horizon.",
    ),
    _feature(
        "realized_vol_short",
        "volatility",
        "risk",
        "high",
        "high",
        ("virtual_base_reserve", "virtual_quote_reserve"),
        INTRADAY_MOMENTUM_URL,
        "RMS log return over the short window.",
    ),
    _feature(
        "realized_vol_long",
        "volatility",
        "risk",
        "high",
        "high",
        ("virtual_base_reserve", "virtual_quote_reserve"),
        INTRADAY_MOMENTUM_URL,
        "RMS log return over the long window.",
    ),
    _feature(
        "volatility_ratio",
        "volatility",
        "regime",
        "high",
        "high",
        ("virtual_base_reserve", "virtual_quote_reserve"),
        INTRADAY_MOMENTUM_URL,
        "Short realized volatility divided by long realized volatility.",
    ),
    _feature(
        "trend_efficiency",
        "price_path",
        "regime",
        "descriptive",
        "medium",
        ("virtual_base_reserve", "virtual_quote_reserve"),
        TSMOM_URL,
        "Absolute medium-window displacement divided by total path length.",
    ),
    _feature(
        "drawdown_from_high",
        "price_path",
        "risk",
        "descriptive",
        "high",
        ("virtual_base_reserve", "virtual_quote_reserve"),
        CFMM_URL,
        "Current reserve mark divided by the trailing long-window high minus one.",
    ),
    _feature(
        "close_location_value",
        "price_path",
        "directional",
        "descriptive",
        "medium",
        ("high", "low", "close"),
        INTRADAY_MOMENTUM_URL,
        "Close location in the current high-low range, scaled to [-1, 1].",
    ),
    _feature(
        "launch_range_breakout",
        "launch_breakout",
        "directional",
        "medium_heavy_tuning_risk",
        "low_medium",
        ("high", "low", "close", "token_age_seconds"),
        INTRADAY_MOMENTUM_URL,
        "Signed distance outside a pre-registered first-N-bar launch range.",
    ),
    _feature(
        "anchored_vwap_deviation",
        "vwap",
        "directional",
        "descriptive",
        "high",
        ("base_volume", "quote_volume", "close"),
        PUMP_PREDICTION_URL,
        "Close divided by exact cumulative quote/base VWAP since first observation minus one.",
    ),
    _feature(
        "rolling_vwap_deviation",
        "vwap",
        "directional",
        "descriptive",
        "high",
        ("base_volume", "quote_volume", "close"),
        PUMP_PREDICTION_URL,
        "Close divided by medium-window quote/base VWAP minus one.",
    ),
    _feature(
        "true_range_pct",
        "volatility",
        "risk",
        "high",
        "high",
        ("high", "low", "close"),
        INTRADAY_MOMENTUM_URL,
        "Gap-aware bar true range divided by the prior close.",
    ),
    _feature(
        "quote_volume_zscore",
        "volume",
        "regime",
        "high",
        "high",
        ("quote_volume",),
        INTRADAY_MOMENTUM_URL,
        "Current quote volume standardized inside the trailing long window.",
    ),
    _feature(
        "volume_ratio",
        "volume",
        "regime",
        "high",
        "high",
        ("quote_volume",),
        INTRADAY_MOMENTUM_URL,
        "Mean short-window quote volume divided by mean long-window volume.",
    ),
    _feature(
        "signed_flow_current",
        "signed_flow",
        "directional",
        "pump_native",
        "high",
        ("buy_quote_volume", "sell_quote_volume", "quote_volume"),
        PUMP_PREDICTION_URL,
        "Current exact-side signed quote volume divided by quote volume.",
    ),
    _feature(
        "signed_flow_short",
        "signed_flow",
        "directional",
        "high",
        "high",
        ("buy_quote_volume", "sell_quote_volume", "quote_volume"),
        CONT_OFI_URL,
        "Short-window exact-side signed quote flow divided by total flow.",
    ),
    _feature(
        "signed_flow_long",
        "signed_flow",
        "regime",
        "high",
        "high",
        ("buy_quote_volume", "sell_quote_volume", "quote_volume"),
        CONT_OFI_URL,
        "Long-window exact-side signed quote flow divided by total flow.",
    ),
    _feature(
        "flow_acceleration",
        "signed_flow",
        "directional",
        "high",
        "high",
        ("buy_quote_volume", "sell_quote_volume", "quote_volume"),
        CONT_OFI_URL,
        "Short signed-flow ratio minus long signed-flow ratio.",
    ),
    _feature(
        "signed_flow_run_length",
        "signed_flow",
        "directional",
        "descriptive",
        "high",
        ("buy_quote_volume", "sell_quote_volume"),
        HAWKES_URL,
        "Signed count of consecutive buy- or sell-dominant closed bars.",
    ),
    _feature(
        "ofi_depth_proxy",
        "signed_flow",
        "directional",
        "high_lob_analogue",
        "high_as_amm_adaptation",
        (
            "buy_quote_volume",
            "sell_quote_volume",
            "virtual_base_reserve",
            "virtual_quote_reserve",
        ),
        CONT_OFI_URL,
        "Short signed quote flow divided by current executable AMM liquidity; not L2 OFI.",
    ),
    _feature(
        "vpin_proxy",
        "toxicity",
        "risk",
        "contested",
        "medium_as_risk_only",
        ("buy_quote_volume", "sell_quote_volume", "quote_volume"),
        VPIN_URL,
        "Rolling absolute exact-side bar imbalance; not equal-volume-bucket VPIN.",
    ),
    _feature(
        "trade_intensity",
        "event_intensity",
        "regime",
        "pump_native",
        "high",
        ("trade_count", "start_time_ns", "end_time_ns"),
        PUMP_PREDICTION_URL,
        "Closed-bar trades per second.",
    ),
    _feature(
        "intensity_ratio",
        "event_intensity",
        "regime",
        "medium_high",
        "medium_high",
        ("trade_count", "start_time_ns", "end_time_ns"),
        HAWKES_URL,
        "Mean short-window trade intensity divided by its long-window mean.",
    ),
    _feature(
        "intensity_acceleration",
        "event_intensity",
        "directional",
        "medium_high",
        "medium_high",
        ("trade_count", "start_time_ns", "end_time_ns"),
        HAWKES_URL,
        "Recent short-window intensity divided by the immediately prior short window minus one.",
    ),
    _feature(
        "participant_ratio",
        "participation",
        "regime",
        "pump_native",
        "high",
        ("unique_traders", "trade_count"),
        PUMP_PREDICTION_URL,
        "Unique traders divided by trades in the closed bar.",
    ),
    _feature(
        "participant_acceleration",
        "participation",
        "regime",
        "pump_native",
        "high",
        ("unique_traders", "trade_count"),
        PUMP_PREDICTION_URL,
        "Current participant ratio divided by its long-window mean minus one.",
    ),
    _feature(
        "average_trade_size_quote",
        "participation",
        "regime",
        "measurement",
        "high",
        ("quote_volume", "trade_count"),
        PUMP_PREDICTION_URL,
        "Quote volume divided by trade count.",
    ),
    _feature(
        "liquidity_quote",
        "liquidity_impact",
        "execution",
        "pump_native",
        "high",
        (
            "virtual_base_reserve",
            "virtual_quote_reserve",
            "real_base_reserve",
            "real_quote_reserve",
        ),
        CFMM_URL,
        "Twice the smaller executable reserve side expressed in quote units.",
    ),
    _feature(
        "liquidity_change",
        "liquidity_impact",
        "execution",
        "pump_native",
        "high",
        ("virtual_base_reserve", "virtual_quote_reserve"),
        CFMM_URL,
        "One-bar proportional change in executable quote liquidity.",
    ),
    _feature(
        "price_impact_1_quote_bps",
        "liquidity_impact",
        "execution",
        "pump_native",
        "high",
        ("virtual_base_reserve", "virtual_quote_reserve", "venue_fee_bps"),
        CFMM_URL,
        "Normalized one-quote-unit constant-product buy impact including the observed fee.",
    ),
    _feature(
        "price_impact_change",
        "liquidity_impact",
        "execution",
        "pump_native",
        "high",
        ("virtual_base_reserve", "virtual_quote_reserve", "venue_fee_bps"),
        CFMM_URL,
        "One-bar proportional change in the normalized impact estimate.",
    ),
    _feature(
        "amihud_illiquidity",
        "liquidity_impact",
        "risk",
        "high",
        "high_as_execution_gate",
        ("quote_volume", "virtual_base_reserve", "virtual_quote_reserve"),
        AMIHUD_URL,
        "Absolute one-bar reserve-mark return per quote-volume unit.",
    ),
    _feature(
        "kyle_lambda_proxy",
        "liquidity_impact",
        "diagnostic",
        "descriptive_proxy",
        "medium",
        ("buy_quote_volume", "sell_quote_volume"),
        CONT_OFI_URL,
        "One-bar return per signed quote-volume unit; undefined at zero signed flow.",
    ),
    _feature(
        "reserve_progress",
        "lifecycle",
        "state",
        "pump_native_proxy",
        "high",
        ("real_base_reserve", "virtual_base_reserve"),
        PUMP_PREDICTION_URL,
        "Bounded reserve-depletion proxy; not authoritative protocol graduation percent.",
    ),
    _feature(
        "reserve_progress_velocity",
        "lifecycle",
        "directional",
        "pump_native",
        "high",
        ("real_base_reserve", "virtual_base_reserve", "end_time_ns"),
        PUMP_PREDICTION_URL,
        "One-step change in reserve progress per second.",
    ),
    _feature(
        "reserve_progress_acceleration",
        "lifecycle",
        "directional",
        "pump_native",
        "high",
        ("real_base_reserve", "virtual_base_reserve", "end_time_ns"),
        PUMP_PREDICTION_URL,
        "One-step change in reserve-progress velocity per second.",
    ),
    _feature(
        "real_quote_reserve_change",
        "lifecycle",
        "state",
        "pump_native",
        "high",
        ("real_quote_reserve",),
        PUMP_PREDICTION_URL,
        "One-bar proportional change in real quote reserves.",
    ),
    _feature(
        "venue_fee_bps",
        "cost",
        "execution",
        "pump_native",
        "high_when_observed",
        ("venue_fee_bps",),
        CFMM_URL,
        "Observed venue fee in basis points; missing remains missing.",
    ),
    _feature(
        "log_token_age",
        "lifecycle",
        "state",
        "pump_native",
        "high",
        ("token_age_seconds",),
        PUMP_PREDICTION_URL,
        "Natural logarithm of one plus token age in seconds.",
    ),
    _feature(
        "price_volume_confirmation",
        "interaction",
        "directional",
        "medium",
        "medium",
        ("quote_volume", "virtual_base_reserve", "virtual_quote_reserve"),
        INTRADAY_MOMENTUM_URL,
        "One-bar return multiplied by trailing quote-volume z-score.",
    ),
)


_FEATURE_INDEX_MUTABLE = {definition.name: index for index, definition in enumerate(ALPHA_FEATURES)}
if len(_FEATURE_INDEX_MUTABLE) != len(ALPHA_FEATURES):  # pragma: no cover - import invariant
    raise RuntimeError("duplicate alpha feature name")
ALPHA_FEATURE_INDEX: Mapping[str, int] = MappingProxyType(_FEATURE_INDEX_MUTABLE)


@dataclass(frozen=True, slots=True)
class AlphaFeatureConfig:
    """Pre-registered windows shared by one feature matrix."""

    short_window: int = 3
    medium_window: int = 8
    long_window: int = 21
    opening_range_bars: int = 5
    vpin_window: int = 8

    def __post_init__(self) -> None:
        values = (
            self.short_window,
            self.medium_window,
            self.long_window,
            self.opening_range_bars,
            self.vpin_window,
        )
        if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
            raise ValueError("alpha feature windows must be integers")
        if not 1 <= self.short_window < self.medium_window < self.long_window:
            raise ValueError("require 1 <= short_window < medium_window < long_window")
        if self.opening_range_bars < 2 or self.vpin_window < 2:
            raise ValueError("opening_range_bars and vpin_window must be at least two")


@dataclass(frozen=True, slots=True)
class AlphaFeatureRow:
    """One closed bar and its fixed-order causal feature vector."""

    bar: Bar
    values: tuple[float | None, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.bar, Bar):
            raise ValueError("bar must be a Bar")
        if len(self.values) != len(ALPHA_FEATURES):
            raise ValueError("alpha feature vector length does not match registry")
        if any(value is not None and not math.isfinite(value) for value in self.values):
            raise ValueError("alpha features must be finite or None")

    def value(self, name: str) -> float | None:
        """Return one feature by its registered name."""

        try:
            index = ALPHA_FEATURE_INDEX[name]
        except KeyError as error:
            raise KeyError(f"unknown alpha feature {name!r}") from error
        return self.values[index]

    @property
    def ready_count(self) -> int:
        return sum(value is not None for value in self.values)

    @property
    def momentum(self) -> float | None:
        """Compatibility accessor used by the generic exit rule."""

        return self.value("momentum_short")


_PHASE_ORDER = {
    VenuePhase.BONDING_CURVE: 0,
    VenuePhase.PUMPSWAP: 1,
    VenuePhase.CLOB: 2,
}


def _available_at(bar: Bar) -> int:
    return bar.end_time_ns if bar.available_at_ns is None else bar.available_at_ns


def _prefix(values: list[float]) -> list[float]:
    result = [0.0]
    total = 0.0
    for value in values:
        total += value
        result.append(total)
    return result


def _window_sum(prefix: list[float], end: int, length: int) -> float:
    return prefix[end + 1] - prefix[end + 1 - length]


def _ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0.0:
        return None
    value = numerator / denominator
    return value if math.isfinite(value) else None


def _liquidity_quote(bar: Bar) -> float:
    virtual_base = float(bar.virtual_base_reserve)
    virtual_quote = float(bar.virtual_quote_reserve)
    mark = virtual_quote / virtual_base
    executable_base = (
        virtual_base if bar.real_base_reserve is None else float(bar.real_base_reserve)
    )
    executable_quote = (
        virtual_quote if bar.real_quote_reserve is None else float(bar.real_quote_reserve)
    )
    return 2.0 * min(executable_base * mark, executable_quote)


def _impact_1_quote_bps(bar: Bar) -> float | None:
    base_reserve = float(bar.virtual_base_reserve)
    quote_reserve = float(bar.virtual_quote_reserve)
    fee_rate = 0.0 if bar.venue_fee_bps is None else float(bar.venue_fee_bps) / 10_000.0
    quote_in = 1.0
    net_quote = quote_in * (1.0 - fee_rate)
    invariant = base_reserve * quote_reserve
    base_out = base_reserve - invariant / (quote_reserve + net_quote)
    if bar.real_base_reserve is not None and base_out > float(bar.real_base_reserve):
        return None
    if base_out <= 0.0:
        return None
    spot = quote_reserve / base_reserve
    execution = quote_in / base_out
    value = max(0.0, execution / spot - 1.0) * 10_000.0
    return value if math.isfinite(value) else None


def _progress(bar: Bar) -> float | None:
    if bar.real_base_reserve is None:
        return None
    raw = 1.0 - float(bar.real_base_reserve) / float(bar.virtual_base_reserve)
    return min(1.0, max(0.0, raw))


def _zscore_current(
    prefix: list[float], prefix_squares: list[float], index: int, length: int
) -> float:
    total = _window_sum(prefix, index, length)
    total_squares = _window_sum(prefix_squares, index, length)
    mean = total / length
    variance = max(0.0, total_squares / length - mean * mean)
    if variance <= 0.0:
        return 0.0
    return (prefix[index + 1] - prefix[index] - mean) / math.sqrt(variance)


def _momentum(marks: list[float], index: int, window: int) -> float | None:
    return None if index < window else marks[index] / marks[index - window] - 1.0


def _realized_vol(log_square_prefix: list[float], index: int, window: int) -> float | None:
    if index < window:
        return None
    start = index - window + 1
    total = log_square_prefix[index + 1] - log_square_prefix[start]
    return math.sqrt(max(0.0, total / window))


def _ordered_markets(bars: Iterable[Bar]) -> dict[str, list[Bar]]:
    grouped: dict[str, list[Bar]] = defaultdict(list)
    identities: dict[str, tuple[str, str]] = {}
    seen: set[tuple[str, int, VenuePhase]] = set()
    for bar in bars:
        if not isinstance(bar, Bar):
            raise TypeError("bars must contain only Bar instances")
        identity = (bar.mint, bar.quote_mint)
        if identities.setdefault(bar.market_id, identity) != identity:
            raise ValueError(f"inconsistent identity for market {bar.market_id}")
        position = (bar.market_id, bar.start_time_ns, bar.phase)
        if position in seen:
            raise ValueError(
                "duplicate bar for market, start_time_ns, and phase: "
                f"{bar.market_id}, {bar.start_time_ns}, {bar.phase.value}"
            )
        seen.add(position)
        grouped[bar.market_id].append(bar)

    for market_id, history in grouped.items():
        history.sort(key=lambda item: (item.start_time_ns, _PHASE_ORDER[item.phase]))
        availability = [_available_at(item) for item in history]
        if any(
            current < previous
            for previous, current in zip(availability, availability[1:], strict=False)
        ):
            raise ValueError(
                f"market {market_id} has non-monotonic available_at_ns; causal order is ambiguous"
            )
    return grouped


def _market_features(history: list[Bar], config: AlphaFeatureConfig) -> list[AlphaFeatureRow]:
    marks = [float(bar.virtual_quote_reserve) / float(bar.virtual_base_reserve) for bar in history]
    quote_volumes = [float(bar.quote_volume) for bar in history]
    base_volumes = [float(bar.base_volume) for bar in history]
    signed_volumes = [float(bar.buy_quote_volume) - float(bar.sell_quote_volume) for bar in history]
    absolute_signed_volumes = [abs(value) for value in signed_volumes]
    intensities = [
        bar.trade_count / ((bar.end_time_ns - bar.start_time_ns) / 1_000_000_000) for bar in history
    ]
    participant_ratios = [bar.unique_traders / bar.trade_count for bar in history]
    liquidities = [_liquidity_quote(bar) for bar in history]
    impacts = [_impact_1_quote_bps(bar) for bar in history]
    progresses = [_progress(bar) for bar in history]

    log_returns = [0.0]
    absolute_mark_changes = [0.0]
    for index in range(1, len(history)):
        log_returns.append(math.log(marks[index] / marks[index - 1]))
        absolute_mark_changes.append(abs(marks[index] - marks[index - 1]))

    quote_prefix = _prefix(quote_volumes)
    quote_square_prefix = _prefix([value * value for value in quote_volumes])
    base_prefix = _prefix(base_volumes)
    signed_prefix = _prefix(signed_volumes)
    absolute_signed_prefix = _prefix(absolute_signed_volumes)
    intensity_prefix = _prefix(intensities)
    participant_prefix = _prefix(participant_ratios)
    log_square_prefix = _prefix([value * value for value in log_returns])
    path_prefix = _prefix(absolute_mark_changes)

    opening_count = min(config.opening_range_bars, len(history))
    opening_high = max(float(bar.high) for bar in history[:opening_count])
    opening_low = min(float(bar.low) for bar in history[:opening_count])

    rows: list[AlphaFeatureRow] = []
    run_sign = 0
    run_length = 0
    prior_velocity: float | None = None

    for index, bar in enumerate(history):
        return_1 = None if index == 0 else marks[index] / marks[index - 1] - 1.0

        momentum_short = _momentum(marks, index, config.short_window)
        momentum_medium = _momentum(marks, index, config.medium_window)
        momentum_long = _momentum(marks, index, config.long_window)
        vol_short = _realized_vol(log_square_prefix, index, config.short_window)
        vol_long = _realized_vol(log_square_prefix, index, config.long_window)
        volatility_ratio = (
            None if vol_short is None or vol_long is None else _ratio(vol_short, vol_long)
        )

        if index < config.medium_window:
            trend_efficiency = None
        else:
            path = path_prefix[index + 1] - path_prefix[index - config.medium_window + 1]
            trend_efficiency = _ratio(abs(marks[index] - marks[index - config.medium_window]), path)

        if index + 1 < config.long_window:
            drawdown = None
        else:
            trailing_high = max(marks[index - config.long_window + 1 : index + 1])
            drawdown = marks[index] / trailing_high - 1.0

        bar_range = float(bar.high) - float(bar.low)
        close_location = (
            0.0 if bar_range <= 0.0 else 2.0 * (float(bar.close) - float(bar.low)) / bar_range - 1.0
        )

        if index + 1 <= config.opening_range_bars:
            launch_breakout = None
        elif float(bar.close) > opening_high:
            launch_breakout = float(bar.close) / opening_high - 1.0
        elif float(bar.close) < opening_low:
            launch_breakout = -(opening_low / float(bar.close) - 1.0)
        else:
            launch_breakout = 0.0

        anchored_vwap = quote_prefix[index + 1] / base_prefix[index + 1]
        anchored_vwap_deviation = float(bar.close) / anchored_vwap - 1.0
        if index + 1 < config.medium_window:
            rolling_vwap_deviation = None
        else:
            rolling_quote = _window_sum(quote_prefix, index, config.medium_window)
            rolling_base = _window_sum(base_prefix, index, config.medium_window)
            rolling_vwap_deviation = float(bar.close) / (rolling_quote / rolling_base) - 1.0

        if index == 0:
            true_range_pct = None
        else:
            prior_close = float(history[index - 1].close)
            true_range = max(
                float(bar.high) - float(bar.low),
                abs(float(bar.high) - prior_close),
                abs(float(bar.low) - prior_close),
            )
            true_range_pct = true_range / prior_close

        if index + 1 < config.long_window:
            volume_zscore = None
            volume_ratio = None
        else:
            volume_zscore = _zscore_current(
                quote_prefix, quote_square_prefix, index, config.long_window
            )
            short_volume_mean = (
                _window_sum(quote_prefix, index, config.short_window) / config.short_window
            )
            long_volume_mean = (
                _window_sum(quote_prefix, index, config.long_window) / config.long_window
            )
            volume_ratio = _ratio(short_volume_mean, long_volume_mean)

        signed_current = signed_volumes[index] / quote_volumes[index]
        current_sign = (signed_volumes[index] > 0.0) - (signed_volumes[index] < 0.0)
        if current_sign == 0:
            run_sign = 0
            run_length = 0
        elif current_sign == run_sign:
            run_length += 1
        else:
            run_sign = current_sign
            run_length = 1
        signed_run_length = float(run_sign * run_length)

        if index + 1 < config.short_window:
            signed_short = None
            ofi_depth_proxy = None
        else:
            short_signed = _window_sum(signed_prefix, index, config.short_window)
            short_total = _window_sum(quote_prefix, index, config.short_window)
            signed_short = short_signed / short_total
            ofi_depth_proxy = _ratio(short_signed, liquidities[index])

        if index + 1 < config.long_window:
            signed_long = None
            flow_acceleration = None
        else:
            long_signed = _window_sum(signed_prefix, index, config.long_window)
            long_total = _window_sum(quote_prefix, index, config.long_window)
            signed_long = long_signed / long_total
            flow_acceleration = None if signed_short is None else signed_short - signed_long

        if index + 1 < config.vpin_window:
            vpin_proxy = None
        else:
            absolute_imbalance = _window_sum(absolute_signed_prefix, index, config.vpin_window)
            vpin_volume = _window_sum(quote_prefix, index, config.vpin_window)
            vpin_proxy = absolute_imbalance / vpin_volume

        if index + 1 < config.long_window:
            intensity_ratio = None
            participant_acceleration = None
        else:
            short_intensity = (
                _window_sum(intensity_prefix, index, config.short_window) / config.short_window
            )
            long_intensity = (
                _window_sum(intensity_prefix, index, config.long_window) / config.long_window
            )
            intensity_ratio = _ratio(short_intensity, long_intensity)
            participant_mean = (
                _window_sum(participant_prefix, index, config.long_window) / config.long_window
            )
            ratio = _ratio(participant_ratios[index], participant_mean)
            participant_acceleration = None if ratio is None else ratio - 1.0

        if index + 1 < 2 * config.short_window:
            intensity_acceleration = None
        else:
            current_intensity = (
                _window_sum(intensity_prefix, index, config.short_window) / config.short_window
            )
            prior_end = index - config.short_window
            prior_intensity = (
                _window_sum(intensity_prefix, prior_end, config.short_window) / config.short_window
            )
            ratio = _ratio(current_intensity, prior_intensity)
            intensity_acceleration = None if ratio is None else ratio - 1.0

        if index == 0:
            liquidity_change = None
        else:
            liquidity_ratio = _ratio(liquidities[index], liquidities[index - 1])
            liquidity_change = None if liquidity_ratio is None else liquidity_ratio - 1.0
        impact = impacts[index]
        prior_impact = None if index == 0 else impacts[index - 1]
        impact_change = (
            None
            if impact is None or prior_impact is None or prior_impact <= 0.0
            else impact / prior_impact - 1.0
        )
        amihud = None if return_1 is None else abs(return_1) / quote_volumes[index]
        kyle_lambda = (
            None
            if return_1 is None or abs(signed_volumes[index]) <= 1e-15
            else return_1 / signed_volumes[index]
        )

        progress = progresses[index]
        prior_progress = None if index == 0 else progresses[index - 1]
        if index == 0 or progress is None or prior_progress is None:
            progress_velocity = None
        else:
            elapsed = (bar.end_time_ns - history[index - 1].end_time_ns) / 1_000_000_000
            progress_velocity = _ratio(progress - prior_progress, elapsed)
        if index == 0 or progress_velocity is None or prior_velocity is None:
            progress_acceleration = None
        else:
            elapsed = (bar.end_time_ns - history[index - 1].end_time_ns) / 1_000_000_000
            progress_acceleration = _ratio(progress_velocity - prior_velocity, elapsed)
        prior_velocity = progress_velocity

        previous_real_quote = None if index == 0 else history[index - 1].real_quote_reserve
        if (
            bar.real_quote_reserve is None
            or previous_real_quote is None
            or float(previous_real_quote) <= 0.0
        ):
            real_quote_change = None
        else:
            real_quote_change = float(bar.real_quote_reserve) / float(previous_real_quote) - 1.0

        fee_bps = None if bar.venue_fee_bps is None else float(bar.venue_fee_bps)
        log_age = (
            None if bar.token_age_seconds is None else math.log1p(float(bar.token_age_seconds))
        )
        price_volume_confirmation = (
            None if return_1 is None or volume_zscore is None else return_1 * volume_zscore
        )

        values = (
            return_1,
            momentum_short,
            momentum_medium,
            momentum_long,
            vol_short,
            vol_long,
            volatility_ratio,
            trend_efficiency,
            drawdown,
            close_location,
            launch_breakout,
            anchored_vwap_deviation,
            rolling_vwap_deviation,
            true_range_pct,
            volume_zscore,
            volume_ratio,
            signed_current,
            signed_short,
            signed_long,
            flow_acceleration,
            signed_run_length,
            ofi_depth_proxy,
            vpin_proxy,
            intensities[index],
            intensity_ratio,
            intensity_acceleration,
            participant_ratios[index],
            participant_acceleration,
            quote_volumes[index] / bar.trade_count,
            liquidities[index],
            liquidity_change,
            impact,
            impact_change,
            amihud,
            kyle_lambda,
            progress,
            progress_velocity,
            progress_acceleration,
            real_quote_change,
            fee_bps,
            log_age,
            price_volume_confirmation,
        )
        rows.append(AlphaFeatureRow(bar=bar, values=values))
    return rows


def causal_alpha_features(
    bars: Iterable[Bar], config: AlphaFeatureConfig | None = None
) -> list[AlphaFeatureRow]:
    """Build deterministic, prefix-invariant features for every market.

    The current closed bar is included because the simulator cannot execute
    until a strictly later latency-eligible snapshot.  Non-monotonic
    ``available_at_ns`` is rejected instead of silently incorporating data
    that had not arrived at an earlier decision time.
    """

    selected = config or AlphaFeatureConfig()
    grouped = _ordered_markets(bars)
    rows: list[AlphaFeatureRow] = []
    for market_id in sorted(grouped):
        rows.extend(_market_features(grouped[market_id], selected))
    return rows


def alpha_feature_definition(name: str) -> AlphaFeatureDefinition:
    """Return provenance for one registered feature."""

    try:
        return ALPHA_FEATURES[ALPHA_FEATURE_INDEX[name]]
    except KeyError as error:
        raise KeyError(f"unknown alpha feature {name!r}") from error
