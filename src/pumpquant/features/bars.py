"""Deterministic bar aggregation and causal first-pass market features.

These features describe observed swaps and a constant-product reserve snapshot;
they are not an execution model.  In particular, OHLCV and reserve-derived
liquidity do not account for validator latency, transaction ordering, failed
transactions, fees, or adverse selection.  Backtests must model those effects
before interpreting a signal as tradable edge.
"""

from __future__ import annotations

import math
import sqlite3
import struct
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from tempfile import TemporaryDirectory
from typing import Any

from pumpquant.core.schema import Bar, FeatureRow, Side, SwapEvent, VenuePhase

_PHASE_ORDER = {phase: index for index, phase in enumerate(VenuePhase)}


class _FsumAccumulator:
    """Streaming equivalent of ``math.fsum`` for finite float inputs.

    ``build_bars`` historically feeds every amount in a bucket to
    :func:`math.fsum`.  Keeping only a normal running ``float`` would change
    the result for adversarial (but valid) amount sequences, so use the same
    partial-sums expansion used by the well-known Python ``msum`` recipe.  The
    expansion is tiny for normal data and, importantly, does not retain the
    source events themselves.
    """

    __slots__ = ("_partials",)

    def __init__(self) -> None:
        self._partials: list[float] = []

    def add(self, value: float) -> None:
        partials = self._partials
        index = 0
        for other in partials:
            if abs(value) < abs(other):
                value, other = other, value
            high = value + other
            low = other - (high - value)
            if low:
                partials[index] = low
                index += 1
            value = high
        partials[index:] = [value]

    def total(self) -> float:
        return math.fsum(self._partials)


class _BarAccumulator:
    """Scalar state for one ``(market, bucket, phase)`` bar."""

    __slots__ = (
        "market_id",
        "start_time_ns",
        "phase",
        "mint",
        "quote_mint",
        "open",
        "high",
        "low",
        "close",
        "base_volume",
        "quote_volume",
        "buy_quote_volume",
        "sell_quote_volume",
        "trade_count",
        "traders",
        "virtual_base_reserve",
        "virtual_quote_reserve",
        "real_base_reserve",
        "real_quote_reserve",
        "venue_fee_bps",
        "max_observed_at_ns",
    )

    def __init__(self, event: SwapEvent, start_time_ns: int) -> None:
        self.market_id = event.market_id
        self.start_time_ns = start_time_ns
        self.phase = event.phase
        self.mint = event.mint
        self.quote_mint = event.quote_mint
        price = float(event.price)
        self.open = price
        self.high = price
        self.low = price
        self.close = price
        self.base_volume = _FsumAccumulator()
        self.quote_volume = _FsumAccumulator()
        self.buy_quote_volume = _FsumAccumulator()
        self.sell_quote_volume = _FsumAccumulator()
        self.trade_count = 0
        self.traders: set[str] = set()
        self.virtual_base_reserve = float(event.virtual_base_reserve)
        self.virtual_quote_reserve = float(event.virtual_quote_reserve)
        self.real_base_reserve = float(event.real_base_reserve)
        self.real_quote_reserve = float(event.real_quote_reserve)
        self.venue_fee_bps = None if event.venue_fee_bps is None else float(event.venue_fee_bps)
        self.max_observed_at_ns = event.observed_at_ns
        self.add(event)

    def add(self, event: SwapEvent) -> None:
        price = float(event.price)
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.base_volume.add(float(event.base_amount))
        quote_amount = float(event.quote_amount)
        self.quote_volume.add(quote_amount)
        if event.side is Side.BUY:
            self.buy_quote_volume.add(quote_amount)
        else:
            self.sell_quote_volume.add(quote_amount)
        self.trade_count += 1
        if event.trader is not None:
            self.traders.add(event.trader)
        self.virtual_base_reserve = float(event.virtual_base_reserve)
        self.virtual_quote_reserve = float(event.virtual_quote_reserve)
        self.real_base_reserve = float(event.real_base_reserve)
        self.real_quote_reserve = float(event.real_quote_reserve)
        self.venue_fee_bps = None if event.venue_fee_bps is None else float(event.venue_fee_bps)
        self.max_observed_at_ns = max(self.max_observed_at_ns, event.observed_at_ns)


def _bar_from_accumulator(
    accumulator: _BarAccumulator,
    interval_ns: int,
    launch_times: Mapping[str, int],
    availability_floor_ns: int,
    unique_traders: int | None = None,
) -> Bar:
    """Finalize one streaming accumulator using ``build_bars`` semantics."""

    end_time_ns = accumulator.start_time_ns + interval_ns
    available_at_ns = max(
        end_time_ns,
        accumulator.max_observed_at_ns,
        availability_floor_ns,
    )
    launch = launch_times.get(accumulator.mint)
    age = None if launch is None else (end_time_ns - launch) / 1_000_000_000
    return Bar(
        market_id=accumulator.market_id,
        mint=accumulator.mint,
        quote_mint=accumulator.quote_mint,
        start_time_ns=accumulator.start_time_ns,
        end_time_ns=end_time_ns,
        phase=accumulator.phase,
        open=accumulator.open,
        high=accumulator.high,
        low=accumulator.low,
        close=accumulator.close,
        base_volume=accumulator.base_volume.total(),
        quote_volume=accumulator.quote_volume.total(),
        buy_quote_volume=accumulator.buy_quote_volume.total(),
        sell_quote_volume=accumulator.sell_quote_volume.total(),
        trade_count=accumulator.trade_count,
        unique_traders=(len(accumulator.traders) if unique_traders is None else unique_traders),
        virtual_base_reserve=accumulator.virtual_base_reserve,
        virtual_quote_reserve=accumulator.virtual_quote_reserve,
        token_age_seconds=age,
        real_base_reserve=accumulator.real_base_reserve,
        real_quote_reserve=accumulator.real_quote_reserve,
        venue_fee_bps=accumulator.venue_fee_bps,
        available_at_ns=available_at_ns,
    )


def _encode_fsum_partials(partials: list[float]) -> bytes:
    """Encode an fsum expansion for the disk-backed streaming accumulator."""

    return struct.pack(f"<{len(partials)}d", *partials)


def _decode_fsum_partials(value: bytes | memoryview) -> list[float]:
    """Decode an fsum expansion written by :func:`_encode_fsum_partials`."""

    payload = bytes(value)
    if len(payload) % 8:
        raise RuntimeError("corrupt streaming fsum expansion")
    return list(struct.unpack(f"<{len(payload) // 8}d", payload))


def _accumulator_row(accumulator: _BarAccumulator) -> tuple[object, ...]:
    """Return a SQLite row for one in-progress bucket."""

    return (
        accumulator.market_id,
        accumulator.start_time_ns,
        accumulator.phase.value,
        accumulator.mint,
        accumulator.quote_mint,
        accumulator.open,
        accumulator.high,
        accumulator.low,
        accumulator.close,
        _encode_fsum_partials(accumulator.base_volume._partials),
        _encode_fsum_partials(accumulator.quote_volume._partials),
        _encode_fsum_partials(accumulator.buy_quote_volume._partials),
        _encode_fsum_partials(accumulator.sell_quote_volume._partials),
        accumulator.trade_count,
        accumulator.virtual_base_reserve,
        accumulator.virtual_quote_reserve,
        accumulator.real_base_reserve,
        accumulator.real_quote_reserve,
        accumulator.venue_fee_bps,
        accumulator.max_observed_at_ns,
    )


def _accumulator_from_row(row: tuple[Any, ...]) -> _BarAccumulator:
    """Rehydrate one in-progress bucket from SQLite without retaining events."""

    accumulator = object.__new__(_BarAccumulator)
    (
        market_id,
        start_time_ns,
        phase,
        mint,
        quote_mint,
        open_price,
        high,
        low,
        close,
        base_partials,
        quote_partials,
        buy_partials,
        sell_partials,
        trade_count,
        virtual_base_reserve,
        virtual_quote_reserve,
        real_base_reserve,
        real_quote_reserve,
        venue_fee_bps,
        max_observed_at_ns,
    ) = row
    accumulator.market_id = str(market_id)
    accumulator.start_time_ns = int(start_time_ns)
    accumulator.phase = VenuePhase(str(phase))
    accumulator.mint = str(mint)
    accumulator.quote_mint = str(quote_mint)
    accumulator.open = float(open_price)
    accumulator.high = float(high)
    accumulator.low = float(low)
    accumulator.close = float(close)
    accumulator.base_volume = _FsumAccumulator()
    accumulator.base_volume._partials = _decode_fsum_partials(base_partials)
    accumulator.quote_volume = _FsumAccumulator()
    accumulator.quote_volume._partials = _decode_fsum_partials(quote_partials)
    accumulator.buy_quote_volume = _FsumAccumulator()
    accumulator.buy_quote_volume._partials = _decode_fsum_partials(buy_partials)
    accumulator.sell_quote_volume = _FsumAccumulator()
    accumulator.sell_quote_volume._partials = _decode_fsum_partials(sell_partials)
    accumulator.trade_count = int(trade_count)
    # Distinct traders are kept in SQLite.  The set is intentionally empty so
    # rehydrating a row never scales Python memory with bucket cardinality.
    accumulator.traders = set()
    accumulator.virtual_base_reserve = float(virtual_base_reserve)
    accumulator.virtual_quote_reserve = float(virtual_quote_reserve)
    accumulator.real_base_reserve = float(real_base_reserve)
    accumulator.real_quote_reserve = float(real_quote_reserve)
    accumulator.venue_fee_bps = None if venue_fee_bps is None else float(venue_fee_bps)
    accumulator.max_observed_at_ns = int(max_observed_at_ns)
    return accumulator


def _positive_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _validate_launch_times(launch_times: Mapping[str, int] | None) -> Mapping[str, int]:
    if launch_times is None:
        return {}
    for mint, timestamp in launch_times.items():
        if not isinstance(mint, str) or not mint.strip():
            raise ValueError("launch_times keys must be non-empty mint strings")
        if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp < 0:
            raise ValueError("launch timestamps must be nonnegative integers")
    return launch_times


def build_bars(
    events: Iterable[SwapEvent],
    interval_ns: int,
    launch_times: Mapping[str, int] | None = None,
) -> list[Bar]:
    """Aggregate swaps into non-empty fixed-width bars.

    Events are ordered by their chain location before open/close and terminal
    reserve values are selected.  A phase transition creates a separate bar,
    even when both phases occur in the same wall-clock interval, so two market
    mechanisms are never silently mixed.

    ``token_age_seconds`` is measured at the bar's exclusive end.  It is
    ``None`` when no launch timestamp is supplied for that mint.
    """

    interval_ns = _positive_int("interval_ns", interval_ns)
    launches = _validate_launch_times(launch_times)
    materialized = list(events)

    seen_event_ids: set[str] = set()
    identity_by_market: dict[str, tuple[str, str]] = {}
    buckets: dict[tuple[str, int, VenuePhase], list[SwapEvent]] = defaultdict(list)

    for event in materialized:
        if not isinstance(event, SwapEvent):
            raise TypeError("events must contain only SwapEvent instances")
        if event.event_id in seen_event_ids:
            raise ValueError(f"duplicate event_id: {event.event_id}")
        seen_event_ids.add(event.event_id)

        identity = (event.mint, event.quote_mint)
        prior_identity = identity_by_market.setdefault(event.market_id, identity)
        if identity != prior_identity:
            raise ValueError(f"inconsistent mint/quote identity for market {event.market_id}")
        launch = launches.get(event.mint)
        if launch is not None and launch > event.block_time_ns:
            raise ValueError(f"launch time for {event.mint} is after event {event.event_id}")

        start_time_ns = (event.block_time_ns // interval_ns) * interval_ns
        buckets[(event.market_id, start_time_ns, event.phase)].append(event)

    bars: list[Bar] = []
    availability_floor_by_market: dict[str, int] = {}
    sorted_keys = sorted(
        buckets,
        key=lambda key: (key[0], key[1], _PHASE_ORDER[key[2]]),
    )
    for market_id, start_time_ns, phase in sorted_keys:
        bucket = sorted(
            buckets[(market_id, start_time_ns, phase)],
            key=lambda event: event.ordering_key,
        )
        first = bucket[0]
        last = bucket[-1]
        mint = first.mint
        end_time_ns = start_time_ns + interval_ns
        available_at_ns = max(
            end_time_ns,
            max(event.observed_at_ns for event in bucket),
            availability_floor_by_market.get(market_id, 0),
        )
        availability_floor_by_market[market_id] = available_at_ns
        launch = launches.get(mint)
        age = None if launch is None else (end_time_ns - launch) / 1_000_000_000

        prices = [float(event.price) for event in bucket]
        base_volume = math.fsum(float(event.base_amount) for event in bucket)
        quote_volume = math.fsum(float(event.quote_amount) for event in bucket)
        buy_quote_volume = math.fsum(
            float(event.quote_amount) for event in bucket if event.side is Side.BUY
        )
        sell_quote_volume = math.fsum(
            float(event.quote_amount) for event in bucket if event.side is Side.SELL
        )

        bars.append(
            Bar(
                market_id=market_id,
                mint=mint,
                quote_mint=first.quote_mint,
                start_time_ns=start_time_ns,
                end_time_ns=end_time_ns,
                phase=phase,
                open=prices[0],
                high=max(prices),
                low=min(prices),
                close=prices[-1],
                base_volume=base_volume,
                quote_volume=quote_volume,
                buy_quote_volume=buy_quote_volume,
                sell_quote_volume=sell_quote_volume,
                trade_count=len(bucket),
                unique_traders=len({event.trader for event in bucket if event.trader is not None}),
                virtual_base_reserve=float(last.virtual_base_reserve),
                virtual_quote_reserve=float(last.virtual_quote_reserve),
                token_age_seconds=age,
                real_base_reserve=float(last.real_base_reserve),
                real_quote_reserve=float(last.real_quote_reserve),
                venue_fee_bps=(None if last.venue_fee_bps is None else float(last.venue_fee_bps)),
                available_at_ns=available_at_ns,
            )
        )
    return bars


def iter_bars_chain_ordered(
    events: Iterable[SwapEvent],
    interval_ns: int,
    launch_times: Mapping[str, int] | None = None,
) -> Iterator[Bar]:
    """Stream fixed-width bars from a chain-ordered event stream.

    ``events`` must be in nondecreasing :attr:`SwapEvent.ordering_key` order,
    and the corresponding time-bucket starts must also be nondecreasing.  A
    regression fails closed because a completed bucket cannot be reopened
    without retaining all of its events.  Scalar accumulators, distinct
    trader keys, and correctness indexes are kept in a bounded-cache
    temporary SQLite database; Python memory therefore does not grow with
    either the total stream size or the number of markets in one bucket.

    Bars are yielded in deterministic time-major order: for each completed
    bucket start, market IDs are ascending and phases follow
    ``VenuePhase`` order.  This differs from :func:`build_bars`'s historical
    market-major list order; callers that require that order can sort by
    ``(bar.market_id, bar.start_time_ns, _PHASE_ORDER[bar.phase])``.  Values
    and validation semantics are otherwise identical to ``build_bars``.
    """

    interval_ns = _positive_int("interval_ns", interval_ns)
    launches = _validate_launch_times(launch_times)

    def _iter() -> Iterator[Bar]:
        previous_ordering_key: tuple[int, int, int, str] | None = None
        previous_start_time_ns: int | None = None

        with TemporaryDirectory(prefix="pumpquant-bars-") as database_directory:
            database_path = f"{database_directory}/indexes.sqlite3"
            connection = sqlite3.connect(database_path)
            try:
                # The indexes are correctness state, not an output cache.  A
                # small bounded page cache keeps their growth on disk while
                # preserving exact duplicate and identity validation.
                connection.execute("PRAGMA journal_mode=OFF")
                connection.execute("PRAGMA synchronous=OFF")
                connection.execute("PRAGMA temp_store=FILE")
                connection.execute("PRAGMA cache_size=-8192")
                connection.execute(
                    "CREATE TABLE event_ids (event_id TEXT PRIMARY KEY) WITHOUT ROWID"
                )
                connection.execute(
                    "CREATE TABLE market_identity ("
                    "market_id TEXT PRIMARY KEY, mint TEXT NOT NULL, quote_mint TEXT NOT NULL"
                    ") WITHOUT ROWID"
                )
                connection.execute(
                    "CREATE TABLE availability_floor ("
                    "market_id TEXT PRIMARY KEY, available_at_ns INTEGER NOT NULL"
                    ") WITHOUT ROWID"
                )
                connection.execute(
                    "CREATE TABLE active_bars ("
                    "market_id TEXT NOT NULL, start_time_ns INTEGER NOT NULL, "
                    "phase TEXT NOT NULL, mint TEXT NOT NULL, quote_mint TEXT NOT NULL, "
                    "open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, "
                    "close REAL NOT NULL, base_partials BLOB NOT NULL, "
                    "quote_partials BLOB NOT NULL, buy_partials BLOB NOT NULL, "
                    "sell_partials BLOB NOT NULL, trade_count INTEGER NOT NULL, "
                    "virtual_base_reserve REAL NOT NULL, "
                    "virtual_quote_reserve REAL NOT NULL, "
                    "real_base_reserve REAL NOT NULL, real_quote_reserve REAL NOT NULL, "
                    "venue_fee_bps REAL, max_observed_at_ns INTEGER NOT NULL, "
                    "PRIMARY KEY (market_id, start_time_ns, phase)"
                    ") WITHOUT ROWID"
                )
                connection.execute(
                    "CREATE TABLE traders ("
                    "market_id TEXT NOT NULL, start_time_ns INTEGER NOT NULL, "
                    "phase TEXT NOT NULL, trader TEXT NOT NULL, "
                    "PRIMARY KEY (market_id, start_time_ns, phase, trader)"
                    ") WITHOUT ROWID"
                )

                def flush(start_time_ns: int) -> Iterator[Bar]:
                    cursor = connection.execute(
                        "SELECT market_id, start_time_ns, phase, mint, quote_mint, "
                        "open, high, low, close, base_partials, quote_partials, "
                        "buy_partials, sell_partials, trade_count, "
                        "virtual_base_reserve, virtual_quote_reserve, "
                        "real_base_reserve, real_quote_reserve, venue_fee_bps, "
                        "max_observed_at_ns FROM active_bars "
                        "WHERE start_time_ns = ? ORDER BY market_id, "
                        "CASE phase WHEN 'bonding_curve' THEN 0 "
                        "WHEN 'pumpswap' THEN 1 WHEN 'clob' THEN 2 ELSE 3 END",
                        (start_time_ns,),
                    )
                    for raw_row in cursor:
                        row = tuple(raw_row)
                        accumulator = _accumulator_from_row(row)
                        row = connection.execute(
                            "SELECT available_at_ns FROM availability_floor WHERE market_id = ?",
                            (accumulator.market_id,),
                        ).fetchone()
                        prior_floor = 0 if row is None else int(row[0])
                        trader_row = connection.execute(
                            "SELECT COUNT(*) FROM traders WHERE market_id = ? "
                            "AND start_time_ns = ? AND phase = ?",
                            (
                                accumulator.market_id,
                                accumulator.start_time_ns,
                                accumulator.phase.value,
                            ),
                        ).fetchone()
                        unique_traders = 0 if trader_row is None else int(trader_row[0])
                        bar = _bar_from_accumulator(
                            accumulator,
                            interval_ns,
                            launches,
                            prior_floor,
                            unique_traders=unique_traders,
                        )
                        connection.execute(
                            "INSERT INTO availability_floor(market_id, available_at_ns) "
                            "VALUES (?, ?) ON CONFLICT(market_id) DO UPDATE SET "
                            "available_at_ns=excluded.available_at_ns",
                            (accumulator.market_id, bar.available_at_ns),
                        )
                        connection.execute(
                            "DELETE FROM active_bars WHERE market_id = ? "
                            "AND start_time_ns = ? AND phase = ?",
                            (
                                accumulator.market_id,
                                accumulator.start_time_ns,
                                accumulator.phase.value,
                            ),
                        )
                        connection.execute(
                            "DELETE FROM traders WHERE market_id = ? "
                            "AND start_time_ns = ? AND phase = ?",
                            (
                                accumulator.market_id,
                                accumulator.start_time_ns,
                                accumulator.phase.value,
                            ),
                        )
                        yield bar

                for event in events:
                    if not isinstance(event, SwapEvent):
                        raise TypeError("events must contain only SwapEvent instances")
                    try:
                        connection.execute(
                            "INSERT INTO event_ids(event_id) VALUES (?)",
                            (event.event_id,),
                        )
                    except sqlite3.IntegrityError as error:
                        raise ValueError(f"duplicate event_id: {event.event_id}") from error

                    ordering_key = event.ordering_key
                    if previous_ordering_key is not None and ordering_key < previous_ordering_key:
                        raise ValueError(
                            "events must be in nondecreasing ordering_key order: "
                            f"{ordering_key!r} follows {previous_ordering_key!r}"
                        )
                    previous_ordering_key = ordering_key

                    identity = (event.mint, event.quote_mint)
                    row = connection.execute(
                        "SELECT mint, quote_mint FROM market_identity WHERE market_id = ?",
                        (event.market_id,),
                    ).fetchone()
                    if row is None:
                        connection.execute(
                            "INSERT INTO market_identity(market_id, mint, quote_mint) "
                            "VALUES (?, ?, ?)",
                            (event.market_id, *identity),
                        )
                    elif identity != (str(row[0]), str(row[1])):
                        raise ValueError(
                            f"inconsistent mint/quote identity for market {event.market_id}"
                        )
                    launch = launches.get(event.mint)
                    if launch is not None and launch > event.block_time_ns:
                        raise ValueError(
                            f"launch time for {event.mint} is after event {event.event_id}"
                        )

                    start_time_ns = (event.block_time_ns // interval_ns) * interval_ns
                    if (
                        previous_start_time_ns is not None
                        and start_time_ns < previous_start_time_ns
                    ):
                        raise ValueError(
                            "event time-bucket starts must be nondecreasing: "
                            f"{start_time_ns} follows {previous_start_time_ns}"
                        )
                    if previous_start_time_ns is None:
                        previous_start_time_ns = start_time_ns
                    elif start_time_ns > previous_start_time_ns:
                        yield from flush(previous_start_time_ns)
                        previous_start_time_ns = start_time_ns

                    row = connection.execute(
                        "SELECT market_id, start_time_ns, phase, mint, quote_mint, "
                        "open, high, low, close, base_partials, quote_partials, "
                        "buy_partials, sell_partials, trade_count, "
                        "virtual_base_reserve, virtual_quote_reserve, "
                        "real_base_reserve, real_quote_reserve, venue_fee_bps, "
                        "max_observed_at_ns FROM active_bars WHERE market_id = ? "
                        "AND start_time_ns = ? AND phase = ?",
                        (event.market_id, start_time_ns, event.phase.value),
                    ).fetchone()
                    if row is None:
                        accumulator = _BarAccumulator(event, start_time_ns)
                    else:
                        accumulator = _accumulator_from_row(tuple(row))
                        accumulator.add(event)
                    connection.execute(
                        "INSERT OR REPLACE INTO active_bars ("
                        "market_id, start_time_ns, phase, mint, quote_mint, open, high, low, "
                        "close, base_partials, quote_partials, buy_partials, sell_partials, "
                        "trade_count, virtual_base_reserve, virtual_quote_reserve, "
                        "real_base_reserve, real_quote_reserve, venue_fee_bps, "
                        "max_observed_at_ns) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        _accumulator_row(accumulator),
                    )
                    if event.trader is not None:
                        connection.execute(
                            "INSERT OR IGNORE INTO traders ("
                            "market_id, start_time_ns, phase, trader) VALUES (?, ?, ?, ?)",
                            (event.market_id, start_time_ns, event.phase.value, event.trader),
                        )

                if previous_start_time_ns is not None:
                    yield from flush(previous_start_time_ns)
            finally:
                connection.close()

    return _iter()


def build_bars_chain_ordered(
    events: Iterable[SwapEvent],
    interval_ns: int,
    launch_times: Mapping[str, int] | None = None,
) -> list[Bar]:
    """Materialize :func:`iter_bars_chain_ordered` for batch callers."""

    return list(iter_bars_chain_ordered(events, interval_ns, launch_times))


def _zscore(values: list[float]) -> float:
    mean = math.fsum(values) / len(values)
    variance = math.fsum((value - mean) ** 2 for value in values) / len(values)
    if variance <= 0.0:
        return 0.0
    return (values[-1] - mean) / math.sqrt(variance)


def _price_impact_1_quote_bps(
    base_reserve: float,
    quote_reserve: float,
    real_base_reserve: float | None,
    fee_bps: float,
) -> float | None:
    """Impact of one normalized quote unit; ``None`` when inventory cannot fill."""

    quote_in = 1.0
    net_quote = quote_in * (1.0 - fee_bps / 10_000.0)
    invariant = base_reserve * quote_reserve
    base_out = base_reserve - invariant / (quote_reserve + net_quote)
    if real_base_reserve is not None and base_out > real_base_reserve:
        return None
    spot_price = quote_reserve / base_reserve
    execution_price = quote_in / base_out
    return max(0.0, (execution_price / spot_price - 1.0) * 10_000.0)


def causal_features(
    bars: Iterable[Bar],
    momentum_lookback: int = 3,
    volume_lookback: int = 3,
) -> list[FeatureRow]:
    """Build per-market features without consulting a later chain bar.

    Momentum is the reserve-mark return over ``momentum_lookback`` bars.
    Realized volatility is the root-mean-square of the same window's one-bar
    log returns.  Volume z-score uses a trailing window that includes the
    current, already-closed bar.  Warm-up values are ``None``.

    ``reserve_progress`` is a bounded reserve-depletion proxy,
    ``1 - real_base / virtual_base``.  It is not the protocol's authoritative
    graduation percentage because that also requires launch-configuration
    constants not present in a swap record.
    """

    momentum_lookback = _positive_int("momentum_lookback", momentum_lookback)
    volume_lookback = _positive_int("volume_lookback", volume_lookback)

    grouped: dict[str, list[Bar]] = defaultdict(list)
    identity_by_market: dict[str, tuple[str, str]] = {}
    seen_positions: set[tuple[str, int, VenuePhase]] = set()
    for bar in bars:
        if not isinstance(bar, Bar):
            raise TypeError("bars must contain only Bar instances")
        identity = (bar.mint, bar.quote_mint)
        prior_identity = identity_by_market.setdefault(bar.market_id, identity)
        if identity != prior_identity:
            raise ValueError(f"inconsistent identity for market {bar.market_id}")
        position = (bar.market_id, bar.start_time_ns, bar.phase)
        if position in seen_positions:
            raise ValueError(
                "duplicate bar for market, start_time_ns, and phase: "
                f"{bar.market_id}, {bar.start_time_ns}, {bar.phase.value}"
            )
        seen_positions.add(position)
        grouped[bar.market_id].append(bar)

    rows: list[FeatureRow] = []
    for market_id in sorted(grouped):
        history = sorted(
            grouped[market_id],
            key=lambda bar: (bar.start_time_ns, _PHASE_ORDER[bar.phase]),
        )
        one_bar_log_returns: list[float] = []

        for index, bar in enumerate(history):
            current_close = float(bar.virtual_quote_reserve) / float(bar.virtual_base_reserve)
            if index == 0:
                return_1 = None
            else:
                previous = history[index - 1]
                previous_close = float(previous.virtual_quote_reserve) / float(
                    previous.virtual_base_reserve
                )
                return_1 = current_close / previous_close - 1.0
                one_bar_log_returns.append(math.log(current_close / previous_close))

            if index < momentum_lookback:
                momentum = None
                realized_vol = None
            else:
                anchor = history[index - momentum_lookback]
                anchor_close = float(anchor.virtual_quote_reserve) / float(
                    anchor.virtual_base_reserve
                )
                momentum = current_close / anchor_close - 1.0
                return_window = one_bar_log_returns[-momentum_lookback:]
                realized_vol = math.sqrt(
                    math.fsum(value * value for value in return_window) / momentum_lookback
                )

            if index + 1 < volume_lookback:
                quote_volume_zscore = None
            else:
                volume_window = [
                    float(item.quote_volume)
                    for item in history[index - volume_lookback + 1 : index + 1]
                ]
                quote_volume_zscore = _zscore(volume_window)

            quote_volume = float(bar.quote_volume)
            buy_imbalance = (
                float(bar.buy_quote_volume) - float(bar.sell_quote_volume)
            ) / quote_volume
            duration_seconds = (bar.end_time_ns - bar.start_time_ns) / 1_000_000_000
            trade_intensity = bar.trade_count / duration_seconds

            virtual_base = float(bar.virtual_base_reserve)
            virtual_quote = float(bar.virtual_quote_reserve)
            mark = virtual_quote / virtual_base
            executable_base = (
                virtual_base if bar.real_base_reserve is None else float(bar.real_base_reserve)
            )
            executable_quote = (
                virtual_quote if bar.real_quote_reserve is None else float(bar.real_quote_reserve)
            )
            liquidity_quote = 2.0 * min(executable_base * mark, executable_quote)
            price_impact = _price_impact_1_quote_bps(
                virtual_base,
                virtual_quote,
                None if bar.real_base_reserve is None else float(bar.real_base_reserve),
                0.0 if bar.venue_fee_bps is None else float(bar.venue_fee_bps),
            )

            if bar.real_base_reserve is None:
                reserve_progress = None
            else:
                raw_progress = 1.0 - float(bar.real_base_reserve) / virtual_base
                reserve_progress = min(1.0, max(0.0, raw_progress))

            rows.append(
                FeatureRow(
                    bar=bar,
                    return_1=return_1,
                    momentum=momentum,
                    realized_vol=realized_vol,
                    quote_volume_zscore=quote_volume_zscore,
                    buy_imbalance=buy_imbalance,
                    trade_intensity=trade_intensity,
                    liquidity_quote=liquidity_quote,
                    price_impact_1_quote_bps=price_impact,
                    reserve_progress=reserve_progress,
                )
            )
    return rows
