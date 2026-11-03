"""Deterministic synthetic data for smoke tests—not evidence of an edge."""

from __future__ import annotations

import math
import random

from pumpquant.core.schema import Bar, VenuePhase


def generate_synthetic_bars(
    *, tokens: int = 36, bars_per_token: int = 30, seed: int = 7
) -> list[Bar]:
    if tokens < 1 or bars_per_token < 6:
        raise ValueError("tokens must be positive and bars_per_token must be at least six")
    rng = random.Random(seed)
    result: list[Bar] = []
    second = 1_000_000_000
    for token_index in range(tokens):
        launch_ns = token_index * 45 * second
        price = 0.000_000_03
        prior_imbalance = 0.0
        quality = rng.gauss(0.0, 0.018)
        fast_curve = token_index % 7 == 0
        rug_bar = bars_per_token // 2 if token_index % 11 == 0 else None
        progress = 0.0
        for index in range(bars_per_token):
            shock = rng.gauss(0.0, 0.055)
            if rug_bar is not None and index >= rug_bar:
                shock -= 0.20
            return_rate = quality + 0.07 * prior_imbalance + shock
            return_rate = min(0.28, max(-0.24, return_rate))
            open_price = price
            close_price = max(1e-12, price * math.exp(return_rate))
            imbalance_mean = 0.55 if fast_curve else 0.12 + quality * 3
            current_imbalance = min(
                0.95, max(-0.95, rng.gauss(imbalance_mean, 0.35 if fast_curve else 0.45))
            )
            quote_volume = max(15.0, rng.lognormvariate(3.5 + abs(current_imbalance), 0.35))
            buy_quote = quote_volume * (current_imbalance + 1.0) / 2.0
            sell_quote = quote_volume - buy_quote
            range_scale = abs(rng.gauss(0.025, 0.012))
            high = max(open_price, close_price) * (1.0 + range_scale)
            low = min(open_price, close_price) / (1.0 + range_scale)
            progress_rate = 0.075 if fast_curve else 0.025
            progress = min(0.98, progress + max(0.0, current_imbalance) * progress_rate)
            quote_reserve = 30.0 + 55.0 * progress
            base_reserve = quote_reserve / close_price
            real_base = base_reserve * (1.0 - progress)
            start_ns = launch_ns + index * second
            phase = VenuePhase.PUMPSWAP if progress >= 0.90 else VenuePhase.BONDING_CURVE
            trades = max(1, round(quote_volume / rng.uniform(0.8, 2.5)))
            result.append(
                Bar(
                    market_id=(
                        f"SYNTH-POOL-{token_index:04d}"
                        if phase is VenuePhase.PUMPSWAP
                        else f"SYNTH-CURVE-{token_index:04d}"
                    ),
                    mint=f"SYNTH-{token_index:04d}",
                    quote_mint="SOL",
                    start_time_ns=start_ns,
                    end_time_ns=start_ns + second,
                    phase=phase,
                    open=open_price,
                    high=high,
                    low=low,
                    close=close_price,
                    base_volume=quote_volume / close_price,
                    quote_volume=quote_volume,
                    buy_quote_volume=buy_quote,
                    sell_quote_volume=sell_quote,
                    trade_count=trades,
                    unique_traders=max(1, round(trades * rng.uniform(0.45, 0.9))),
                    virtual_base_reserve=base_reserve,
                    virtual_quote_reserve=quote_reserve,
                    token_age_seconds=float(index + 1),
                    real_base_reserve=real_base,
                    real_quote_reserve=(
                        0.0 if rug_bar is not None and index >= rug_bar else quote_reserve - 30.0
                    ),
                    venue_fee_bps=30.0 if phase is VenuePhase.PUMPSWAP else 125.0,
                )
            )
            price = close_price
            prior_imbalance = current_imbalance
    return result
