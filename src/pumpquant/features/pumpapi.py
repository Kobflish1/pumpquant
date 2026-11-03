"""Causal Pump.fun entry features derived from a PumpAPI checkpoint state.

The registry is shared by discovery and exact threshold evaluation.  Every
expression must use only columns known at ``decision_time_ms``.  Algebraic
complements (for example sell share = 1 - buy share) are deliberately omitted:
they create no new behavioral partition and only inflate the trial family.
"""

from __future__ import annotations

from collections import OrderedDict


def _ordered(*groups: dict[str, str]) -> dict[str, str]:
    result: OrderedDict[str, str] = OrderedDict()
    for group in groups:
        overlap = result.keys() & group.keys()
        if overlap:
            raise ValueError(f"duplicate PumpAPI features: {sorted(overlap)}")
        result.update(group)
    return dict(result)


ACTIVITY_FEATURES = {
    "trade_count": "trade_count::DOUBLE",
    "buy_count": "buy_count::DOUBLE",
    "sell_count": "sell_count::DOUBLE",
    "unique_traders": "unique_traders::DOUBLE",
    "unique_buyers": "unique_buyers::DOUBLE",
    "unique_sellers": "unique_sellers::DOUBLE",
    "age_seconds": "(decision_time_ms - launch_time_ms)::DOUBLE / 1000.0",
    "trades_per_second": (
        "trade_count::DOUBLE / nullif((decision_time_ms - launch_time_ms)::DOUBLE / 1000.0, 0)"
    ),
    "buys_per_second": (
        "buy_count::DOUBLE / nullif((decision_time_ms - launch_time_ms)::DOUBLE / 1000.0, 0)"
    ),
    "sells_per_second": (
        "sell_count::DOUBLE / nullif((decision_time_ms - launch_time_ms)::DOUBLE / 1000.0, 0)"
    ),
    "unique_traders_per_second": (
        "unique_traders::DOUBLE / nullif((decision_time_ms - launch_time_ms)::DOUBLE / 1000.0, 0)"
    ),
    "seconds_per_trade": (
        "(decision_time_ms - launch_time_ms)::DOUBLE / nullif(trade_count::DOUBLE * 1000.0, 0)"
    ),
}

PARTICIPATION_FEATURES = {
    "buy_trade_share": "buy_count::DOUBLE / nullif(trade_count, 0)",
    "buyer_seller_count_ratio": "(buy_count + 1.0) / (sell_count + 1.0)",
    "buyer_seller_unique_ratio": "(unique_buyers + 1.0) / (unique_sellers + 1.0)",
    "unique_buyer_imbalance": (
        "(unique_buyers - unique_sellers)::DOUBLE / nullif(unique_buyers + unique_sellers, 0)"
    ),
    "unique_trader_share": "unique_traders::DOUBLE / nullif(trade_count, 0)",
    "buyer_breadth": "unique_buyers::DOUBLE / nullif(buy_count, 0)",
    "seller_breadth": "unique_sellers::DOUBLE / nullif(sell_count, 0)",
    "trades_per_unique_trader": ("trade_count::DOUBLE / nullif(unique_traders, 0)"),
    "buys_per_unique_buyer": "buy_count::DOUBLE / nullif(unique_buyers, 0)",
    "sells_per_unique_seller": "sell_count::DOUBLE / nullif(unique_sellers, 0)",
    "cross_side_trader_count": (
        "greatest(unique_buyers + unique_sellers - unique_traders, 0)::DOUBLE"
    ),
    "cross_side_trader_share": (
        "greatest(unique_buyers + unique_sellers - unique_traders, 0)::DOUBLE / "
        "nullif(unique_traders, 0)"
    ),
}

FLOW_FEATURES = {
    "buy_quote_amount": "buy_quote_amount",
    "sell_quote_amount": "sell_quote_amount",
    "signed_quote_flow": "signed_quote_flow",
    "total_quote_flow": "buy_quote_amount + sell_quote_amount",
    "buy_token_amount": "buy_token_amount",
    "sell_token_amount": "sell_token_amount",
    "signed_token_flow": "signed_token_flow",
    "total_token_flow": "buy_token_amount + sell_token_amount",
    "buy_quote_share": ("buy_quote_amount / nullif(buy_quote_amount + sell_quote_amount, 0)"),
    "quote_flow_imbalance": ("signed_quote_flow / nullif(buy_quote_amount + sell_quote_amount, 0)"),
    "buy_token_share": ("buy_token_amount / nullif(buy_token_amount + sell_token_amount, 0)"),
    "token_flow_imbalance": ("signed_token_flow / nullif(buy_token_amount + sell_token_amount, 0)"),
    "buy_sell_quote_ratio": "(buy_quote_amount + 1e-12) / (sell_quote_amount + 1e-12)",
    "buy_sell_token_ratio": "(buy_token_amount + 1e-12) / (sell_token_amount + 1e-12)",
    "average_buy_quote": "buy_quote_amount / nullif(buy_count, 0)",
    "average_sell_quote": "sell_quote_amount / nullif(sell_count, 0)",
    "average_buy_token": "buy_token_amount / nullif(buy_count, 0)",
    "average_sell_token": "sell_token_amount / nullif(sell_count, 0)",
    "average_buy_sell_quote_ratio": (
        "(buy_quote_amount / nullif(buy_count, 0)) / "
        "nullif(sell_quote_amount / nullif(sell_count, 0), 0)"
    ),
    "average_buy_sell_token_ratio": (
        "(buy_token_amount / nullif(buy_count, 0)) / "
        "nullif(sell_token_amount / nullif(sell_count, 0), 0)"
    ),
    "quote_flow_per_second": (
        "(buy_quote_amount + sell_quote_amount) / "
        "nullif((decision_time_ms - launch_time_ms)::DOUBLE / 1000.0, 0)"
    ),
    "signed_quote_flow_per_second": (
        "signed_quote_flow / nullif((decision_time_ms - launch_time_ms)::DOUBLE / 1000.0, 0)"
    ),
    "buy_quote_per_buyer": "buy_quote_amount / nullif(unique_buyers, 0)",
    "sell_quote_per_seller": "sell_quote_amount / nullif(unique_sellers, 0)",
}

RECENCY_FEATURES = {
    "buy_quote_last_1s": "buy_quote_last_1s",
    "sell_quote_last_1s": "sell_quote_last_1s",
    "buy_quote_last_5s": "buy_quote_last_5s",
    "sell_quote_last_5s": "sell_quote_last_5s",
    "quote_flow_imbalance_last_1s": (
        "(buy_quote_last_1s - sell_quote_last_1s) / "
        "nullif(buy_quote_last_1s + sell_quote_last_1s, 0)"
    ),
    "quote_flow_imbalance_last_5s": (
        "(buy_quote_last_5s - sell_quote_last_5s) / "
        "nullif(buy_quote_last_5s + sell_quote_last_5s, 0)"
    ),
    "buy_quote_share_last_1s": (
        "buy_quote_last_1s / nullif(buy_quote_last_1s + sell_quote_last_1s, 0)"
    ),
    "buy_quote_share_last_5s": (
        "buy_quote_last_5s / nullif(buy_quote_last_5s + sell_quote_last_5s, 0)"
    ),
    "recent_quote_share_1s": (
        "(buy_quote_last_1s + sell_quote_last_1s) / nullif(buy_quote_amount + sell_quote_amount, 0)"
    ),
    "recent_quote_share_5s": (
        "(buy_quote_last_5s + sell_quote_last_5s) / nullif(buy_quote_amount + sell_quote_amount, 0)"
    ),
    "buy_acceleration_1s_vs_5s": ("buy_quote_last_1s / nullif(buy_quote_last_5s / 5.0, 0)"),
    "sell_acceleration_1s_vs_5s": ("sell_quote_last_1s / nullif(sell_quote_last_5s / 5.0, 0)"),
    "net_acceleration_1s_vs_5s": (
        "(buy_quote_last_1s - sell_quote_last_1s) / "
        "nullif((buy_quote_last_5s - sell_quote_last_5s) / 5.0, 0)"
    ),
    "recent_buy_pressure_change": (
        "buy_quote_last_1s / nullif(buy_quote_last_1s + sell_quote_last_1s, 0) - "
        "buy_quote_last_5s / nullif(buy_quote_last_5s + sell_quote_last_5s, 0)"
    ),
}

CREATOR_FEATURES = {
    "creator_trade_count": "creator_trade_count::DOUBLE",
    "creator_buy_quote": "creator_buy_quote",
    "creator_sell_quote": "creator_sell_quote",
    "creator_net_quote": "creator_buy_quote - creator_sell_quote",
    "creator_trade_share": ("creator_trade_count::DOUBLE / nullif(trade_count, 0)"),
    "creator_buy_share": "creator_buy_quote / nullif(buy_quote_amount, 0)",
    "creator_sell_share": "creator_sell_quote / nullif(sell_quote_amount, 0)",
    "creator_quote_share": (
        "(creator_buy_quote + creator_sell_quote) / nullif(buy_quote_amount + sell_quote_amount, 0)"
    ),
    "creator_flow_imbalance": (
        "(creator_buy_quote - creator_sell_quote) / "
        "nullif(creator_buy_quote + creator_sell_quote, 0)"
    ),
}

PRICE_PATH_FEATURES = {
    "latest_price": "latest_price",
    "latest_market_cap_quote": "latest_market_cap_quote",
    "log_price_volatility": "log_price_volatility",
    "price_return": "latest_price / nullif(first_trade_price, 0) - 1.0",
    "price_range": "maximum_price / nullif(minimum_price, 0) - 1.0",
    "drawdown_from_peak": "latest_price / nullif(maximum_price, 0) - 1.0",
    "rebound_from_trough": "latest_price / nullif(minimum_price, 0) - 1.0",
    "price_path_position": (
        "(latest_price - minimum_price) / nullif(maximum_price - minimum_price, 0)"
    ),
    "return_per_second": (
        "(latest_price / nullif(first_trade_price, 0) - 1.0) / "
        "nullif((decision_time_ms - launch_time_ms)::DOUBLE / 1000.0, 0)"
    ),
    "range_per_second": (
        "(maximum_price / nullif(minimum_price, 0) - 1.0) / "
        "nullif((decision_time_ms - launch_time_ms)::DOUBLE / 1000.0, 0)"
    ),
}

CURVE_AND_COST_FEATURES = {
    "latest_real_base": "latest_real_base",
    "latest_real_quote": "latest_real_quote",
    "latest_virtual_base": "latest_virtual_base",
    "latest_virtual_quote": "latest_virtual_quote",
    "latest_pool_fee_rate": "latest_pool_fee_rate",
    "ofi_per_virtual_quote": "signed_quote_flow / nullif(latest_virtual_quote, 0)",
    "ofi_per_real_quote": "signed_quote_flow / nullif(latest_real_quote, 0)",
    "quote_turnover_per_virtual_quote": (
        "(buy_quote_amount + sell_quote_amount) / nullif(latest_virtual_quote, 0)"
    ),
    "quote_turnover_per_real_quote": (
        "(buy_quote_amount + sell_quote_amount) / nullif(latest_real_quote, 0)"
    ),
    "market_cap_per_virtual_quote": ("latest_market_cap_quote / nullif(latest_virtual_quote, 0)"),
    "market_cap_per_real_quote": ("latest_market_cap_quote / nullif(latest_real_quote, 0)"),
    "real_virtual_quote_ratio": ("latest_real_quote / nullif(latest_virtual_quote, 0)"),
    "real_virtual_base_ratio": "latest_real_base / nullif(latest_virtual_base, 0)",
    "virtual_reserve_price": ("latest_virtual_quote / nullif(latest_virtual_base, 0)"),
    "price_vs_virtual_reserve_price": (
        "latest_price / nullif(latest_virtual_quote / nullif(latest_virtual_base, 0), 0)"
    ),
    "quote_liquidity_per_trader": ("latest_virtual_quote / nullif(unique_traders, 0)"),
}

STRUCTURAL_FEATURES = {
    "mayhem_mode": "mayhem_mode::INTEGER::DOUBLE",
    "cashback_enabled": "cashback_enabled::INTEGER::DOUBLE",
    "has_creator_fee_address": "(creator_fee_address IS NOT NULL)::INTEGER::DOUBLE",
    "token_2022": "(token_program = 'spl-token-2022')::INTEGER::DOUBLE",
}


def _rolling_window_features() -> dict[str, str]:
    """Distinct pressure/participation measurements over causal time windows."""

    result: OrderedDict[str, str] = OrderedDict()
    for seconds in (1, 2, 5, 10, 20, 30, 60, 120):
        prefix = f"window_{seconds}s"
        buy_count = f"buy_count_last_{seconds}s"
        sell_count = f"sell_count_last_{seconds}s"
        buy_quote = f"buy_quote_last_{seconds}s"
        sell_quote = f"sell_quote_last_{seconds}s"
        buy_token = f"buy_token_last_{seconds}s"
        sell_token = f"sell_token_last_{seconds}s"
        result.update(
            {
                f"{prefix}_trade_count": f"({buy_count} + {sell_count})::DOUBLE",
                f"{prefix}_buy_trade_share": (
                    f"{buy_count}::DOUBLE / nullif({buy_count} + {sell_count}, 0)"
                ),
                f"{prefix}_trade_imbalance": (
                    f"({buy_count} - {sell_count})::DOUBLE / nullif({buy_count} + {sell_count}, 0)"
                ),
                f"{prefix}_quote_flow": f"{buy_quote} + {sell_quote}",
                f"{prefix}_buy_quote_share": (
                    f"{buy_quote} / nullif({buy_quote} + {sell_quote}, 0)"
                ),
                f"{prefix}_quote_imbalance": (
                    f"({buy_quote} - {sell_quote}) / nullif({buy_quote} + {sell_quote}, 0)"
                ),
                f"{prefix}_token_flow": f"{buy_token} + {sell_token}",
                f"{prefix}_buy_token_share": (
                    f"{buy_token} / nullif({buy_token} + {sell_token}, 0)"
                ),
                f"{prefix}_token_imbalance": (
                    f"({buy_token} - {sell_token}) / nullif({buy_token} + {sell_token}, 0)"
                ),
                f"{prefix}_average_buy_quote": (f"{buy_quote} / nullif({buy_count}, 0)"),
                f"{prefix}_average_sell_quote": (f"{sell_quote} / nullif({sell_count}, 0)"),
                f"{prefix}_average_buy_sell_quote_ratio": (
                    f"({buy_quote} / nullif({buy_count}, 0)) / "
                    f"nullif({sell_quote} / nullif({sell_count}, 0), 0)"
                ),
                f"{prefix}_quote_per_second": (f"({buy_quote} + {sell_quote}) / {seconds}.0"),
            }
        )
    return dict(result)


CONCENTRATION_FEATURES = {
    "maximum_buy_quote": "maximum_buy_quote",
    "maximum_sell_quote": "maximum_sell_quote",
    "maximum_buy_trade_share": "maximum_buy_quote / nullif(buy_quote_amount, 0)",
    "maximum_sell_trade_share": "maximum_sell_quote / nullif(sell_quote_amount, 0)",
    "quote_size_hhi": (
        "quote_amount_sum_squares / "
        "nullif((buy_quote_amount + sell_quote_amount) * "
        "(buy_quote_amount + sell_quote_amount), 0)"
    ),
    "buy_size_hhi": ("buy_quote_sum_squares / nullif(buy_quote_amount * buy_quote_amount, 0)"),
    "sell_size_hhi": ("sell_quote_sum_squares / nullif(sell_quote_amount * sell_quote_amount, 0)"),
    "signer_quote_hhi": (
        "signer_quote_sum_squares / "
        "nullif((buy_quote_amount + sell_quote_amount) * "
        "(buy_quote_amount + sell_quote_amount), 0)"
    ),
    "signer_buy_quote_hhi": (
        "signer_buy_quote_sum_squares / nullif(buy_quote_amount * buy_quote_amount, 0)"
    ),
    "signer_sell_quote_hhi": (
        "signer_sell_quote_sum_squares / nullif(sell_quote_amount * sell_quote_amount, 0)"
    ),
    "largest_signer_quote_share": (
        "maximum_signer_quote / nullif(buy_quote_amount + sell_quote_amount, 0)"
    ),
    "largest_signer_buy_share": ("maximum_signer_buy_quote / nullif(buy_quote_amount, 0)"),
    "largest_signer_sell_share": ("maximum_signer_sell_quote / nullif(sell_quote_amount, 0)"),
}

TIMING_AND_STREAK_FEATURES = {
    "milliseconds_since_trade": "milliseconds_since_trade::DOUBLE",
    "milliseconds_since_buy": "milliseconds_since_buy::DOUBLE",
    "milliseconds_since_sell": "milliseconds_since_sell::DOUBLE",
    "silence_share_of_age": (
        "milliseconds_since_trade::DOUBLE / nullif(decision_time_ms - launch_time_ms, 0)"
    ),
    "buy_silence_share_of_age": (
        "milliseconds_since_buy::DOUBLE / nullif(decision_time_ms - launch_time_ms, 0)"
    ),
    "sell_silence_share_of_age": (
        "milliseconds_since_sell::DOUBLE / nullif(decision_time_ms - launch_time_ms, 0)"
    ),
    "buy_sell_silence_ratio": ("(milliseconds_since_buy + 1.0) / (milliseconds_since_sell + 1.0)"),
    "current_buy_streak": "current_buy_streak::DOUBLE",
    "current_sell_streak": "current_sell_streak::DOUBLE",
    "current_buy_streak_share": ("current_buy_streak::DOUBLE / nullif(trade_count, 0)"),
    "current_sell_streak_share": ("current_sell_streak::DOUBLE / nullif(trade_count, 0)"),
    "milliseconds_to_peak": ("(maximum_price_timestamp_ms - launch_time_ms)::DOUBLE"),
    "milliseconds_since_peak": ("(decision_time_ms - maximum_price_timestamp_ms)::DOUBLE"),
    "milliseconds_to_trough": ("(minimum_price_timestamp_ms - launch_time_ms)::DOUBLE"),
    "milliseconds_since_trough": ("(decision_time_ms - minimum_price_timestamp_ms)::DOUBLE"),
}

EXECUTION_COMPETITION_FEATURES = {
    "initial_priority_fee": "initial_priority_fee",
    "latest_priority_fee": "latest_priority_fee",
    "average_priority_fee": "total_priority_fee / nullif(trade_count, 0)",
    "maximum_priority_fee": "maximum_priority_fee",
    "priority_fee_acceleration": (
        "latest_priority_fee / nullif(total_priority_fee / nullif(trade_count, 0), 0)"
    ),
    "multi_action_event_share": ("multi_action_event_count::DOUBLE / nullif(trade_count, 0)"),
    "programs_per_trade": ("cumulative_program_count::DOUBLE / nullif(trade_count, 0)"),
    "involved_wallets_per_trade": (
        "cumulative_involved_wallet_count::DOUBLE / nullif(trade_count, 0)"
    ),
}

LAUNCH_AND_CURVE_FEATURES = {
    "initial_buy_quote": "initial_buy_quote",
    "initial_supply": "initial_supply",
    "launch_name_length": "length(launch_name)::DOUBLE",
    "launch_symbol_length": "length(launch_symbol)::DOUBLE",
    "launch_uri_length": "length(launch_uri)::DOUBLE",
    "launch_uri_is_ipfs": ("(lower(launch_uri) LIKE '%ipfs%')::INTEGER::DOUBLE"),
    "launch_uri_is_pump": ("(lower(launch_uri) LIKE '%pump%')::INTEGER::DOUBLE"),
    "bonding_real_base_progress": ("1.0 - latest_real_base / nullif(initial_real_base, 0)"),
    "bonding_virtual_base_progress": (
        "1.0 - latest_virtual_base / nullif(initial_virtual_base, 0)"
    ),
    "bonding_real_quote_change": "latest_real_quote - initial_real_quote",
    "bonding_virtual_quote_change": ("latest_virtual_quote - initial_virtual_quote"),
    "market_cap_multiple_from_launch": (
        "latest_market_cap_quote / nullif(initial_market_cap_quote, 0)"
    ),
    "fee_rate_change_from_launch": ("latest_pool_fee_rate - initial_pool_fee_rate"),
    "initial_buy_per_initial_virtual_quote": (
        "initial_buy_quote / nullif(initial_virtual_quote, 0)"
    ),
}

ATOMIC_INTENSITY_FEATURES = {
    "latest_interarrival_ms": "latest_interarrival_ms::DOUBLE",
    "mean_interarrival_ms": ("total_interarrival_ms::DOUBLE / nullif(trade_count - 1, 0)"),
    "interarrival_cv": (
        "sqrt(greatest(0.0, "
        "interarrival_ms_sum_squares::DOUBLE / nullif(trade_count - 1, 0) "
        "- power(total_interarrival_ms::DOUBLE / nullif(trade_count - 1, 0), 2)"
        ")) / nullif(total_interarrival_ms::DOUBLE / nullif(trade_count - 1, 0), 0)"
    ),
    "current_atomic_event_count": "current_atomic_event_count::DOUBLE",
    "current_atomic_signature_count": "current_atomic_signature_count::DOUBLE",
    "current_atomic_signer_count": "current_atomic_signer_count::DOUBLE",
    "current_atomic_quote_flow": ("current_atomic_buy_quote + current_atomic_sell_quote"),
    "current_atomic_quote_imbalance": (
        "(current_atomic_buy_quote - current_atomic_sell_quote) / "
        "nullif(current_atomic_buy_quote + current_atomic_sell_quote, 0)"
    ),
    "current_atomic_flow_share": (
        "(current_atomic_buy_quote + current_atomic_sell_quote) / "
        "nullif(buy_quote_amount + sell_quote_amount, 0)"
    ),
}

ACTOR_HISTORY_FEATURES = {
    "creator_prior_launch_count": "creator_prior_launch_count::DOUBLE",
    "repeat_actor_trade_share": ("repeat_actor_trade_count::DOUBLE / nullif(trade_count, 0)"),
    "mean_signer_prior_distinct_mints": (
        "signer_prior_distinct_mints_sum::DOUBLE / nullif(trade_count, 0)"
    ),
    "estimated_signer_inventory_hhi": (
        "signer_inventory_sum_squares / "
        "nullif(power(greatest(buy_token_amount - sell_token_amount, 0), 2), 0)"
    ),
}

GLOBAL_REGIME_FEATURES = {
    "global_launch_count_last_60s": "global_launch_count_last_60s::DOUBLE",
    "global_trade_count_last_60s": "global_trade_count_last_60s::DOUBLE",
    "global_buy_trade_share_last_60s": (
        "global_buy_count_last_60s::DOUBLE / nullif(global_trade_count_last_60s, 0)"
    ),
    "global_quote_flow_last_60s": ("global_buy_quote_last_60s + global_sell_quote_last_60s"),
    "global_quote_imbalance_last_60s": (
        "(global_buy_quote_last_60s - global_sell_quote_last_60s) / "
        "nullif(global_buy_quote_last_60s + global_sell_quote_last_60s, 0)"
    ),
    "token_share_of_global_trades_60s": (
        "(buy_count_last_60s + sell_count_last_60s)::DOUBLE / "
        "nullif(global_trade_count_last_60s, 0)"
    ),
    "token_share_of_global_quote_60s": (
        "(buy_quote_last_60s + sell_quote_last_60s) / "
        "nullif(global_buy_quote_last_60s + global_sell_quote_last_60s, 0)"
    ),
    "global_priority_fee_per_trade_60s": (
        "global_priority_fee_last_60s / nullif(global_trade_count_last_60s, 0)"
    ),
}

STATE_TRANSITION_FEATURES = {
    **{
        f"price_return_last_{window}": f"price_return_last_{window}"
        for window in ("5s", "20s", "60s")
    },
    **{
        f"unique_traders_added_last_{window}": (f"unique_traders_added_last_{window}::DOUBLE")
        for window in ("5s", "20s", "60s")
    },
    **{
        f"buyer_breadth_change_last_{window}": (f"buyer_breadth_change_last_{window}")
        for window in ("5s", "20s", "60s")
    },
    **{
        f"virtual_quote_change_last_{window}": (f"virtual_quote_change_last_{window}")
        for window in ("5s", "20s", "60s")
    },
    **{
        f"signer_inventory_hhi_change_last_{window}": (f"signer_inventory_hhi_change_last_{window}")
        for window in ("5s", "20s", "60s")
    },
}

PROGRAM_IDENTITY_FEATURES = {
    f"program_{name}_trade_share": (f"program_{name}_count::DOUBLE / nullif(trade_count, 0)")
    for name in (
        "flashx",
        "jupiter",
        "mayhem",
        "df1",
        "6vo",
        "gmgn",
        "provf",
        "terminal",
    )
}

METADATA_REUSE_FEATURES = {
    "prior_same_uri_launch_count": "prior_same_uri_launch_count::DOUBLE",
    "prior_same_name_launch_count": "prior_same_name_launch_count::DOUBLE",
    "prior_same_symbol_launch_count": "prior_same_symbol_launch_count::DOUBLE",
    "name_symbol_exact_match": (
        "(lower(coalesce(launch_name, '')) = lower(coalesce(launch_symbol, '')))::INTEGER::DOUBLE"
    ),
    "name_contains_pump": ("contains(lower(coalesce(launch_name, '')), 'pump')::INTEGER::DOUBLE"),
    "symbol_contains_pump": (
        "contains(lower(coalesce(launch_symbol, '')), 'pump')::INTEGER::DOUBLE"
    ),
}

RECOVERED_BALANCE_FEATURES = {
    "recovered_signer_balance_hhi": (
        "recovered_signer_balance_sum_squares / nullif(power(recovered_signer_token_balance, 2), 0)"
    ),
    "recovered_observed_token_share": (
        "recovered_signer_token_balance / nullif(initial_supply, 0)"
    ),
    "latest_signer_post_token_share": (
        "latest_signer_post_token_balance / nullif(initial_supply, 0)"
    ),
    "latest_signer_post_sol_balance": "latest_signer_post_sol_balance",
    "latest_trade_quote_to_signer_sol": (
        "latest_trade_quote_amount / nullif(latest_signer_post_sol_balance, 0)"
    ),
}


def _action_impact_features() -> dict[str, str]:
    result: dict[str, str] = {}
    for label, quote_size in (("1m", 0.001), ("2p5m", 0.0025), ("5m", 0.005)):
        net = f"({quote_size} * (1.0 - coalesce(latest_pool_fee_rate, 0.0)))"
        result[f"entry_{label}_quote_depth_fraction"] = (
            f"{quote_size} / nullif(latest_virtual_quote, 0)"
        )
        result[f"entry_{label}_marginal_impact_proxy"] = (
            f"power((latest_virtual_quote + {net}) / nullif(latest_virtual_quote, 0), 2) - 1.0"
        )
        result[f"entry_{label}_curve_tokens_out_proxy"] = (
            "latest_virtual_base - "
            f"(latest_virtual_base * latest_virtual_quote) / "
            f"nullif(latest_virtual_quote + {net}, 0)"
        )
    return result


ACTION_IMPACT_FEATURES = _action_impact_features()

ROLLING_WINDOW_FEATURES = _rolling_window_features()

FEATURE_GROUPS = {
    "activity": ACTIVITY_FEATURES,
    "participation": PARTICIPATION_FEATURES,
    "flow": FLOW_FEATURES,
    "recency": RECENCY_FEATURES,
    "creator": CREATOR_FEATURES,
    "price_path": PRICE_PATH_FEATURES,
    "curve_and_cost": CURVE_AND_COST_FEATURES,
    "structural": STRUCTURAL_FEATURES,
}

FEATURES = _ordered(*FEATURE_GROUPS.values())

EXTENDED_FEATURE_GROUPS = {
    **FEATURE_GROUPS,
    "rolling_windows": ROLLING_WINDOW_FEATURES,
    "concentration": CONCENTRATION_FEATURES,
    "timing_and_streak": TIMING_AND_STREAK_FEATURES,
    "execution_competition": EXECUTION_COMPETITION_FEATURES,
    "launch_and_curve": LAUNCH_AND_CURVE_FEATURES,
}

V4_FEATURE_GROUPS = {
    **EXTENDED_FEATURE_GROUPS,
    "atomic_and_intensity": ATOMIC_INTENSITY_FEATURES,
    "actor_history": ACTOR_HISTORY_FEATURES,
    "global_regime": GLOBAL_REGIME_FEATURES,
    "state_transitions": STATE_TRANSITION_FEATURES,
    "program_identity": PROGRAM_IDENTITY_FEATURES,
    "metadata_reuse": METADATA_REUSE_FEATURES,
    "recovered_balances": RECOVERED_BALANCE_FEATURES,
    "action_impact": ACTION_IMPACT_FEATURES,
}

EXTENDED_FEATURES = _ordered(*EXTENDED_FEATURE_GROUPS.values())
V4_FEATURES = _ordered(*V4_FEATURE_GROUPS.values())

__all__ = [
    "EXTENDED_FEATURES",
    "EXTENDED_FEATURE_GROUPS",
    "FEATURES",
    "FEATURE_GROUPS",
    "V4_FEATURES",
    "V4_FEATURE_GROUPS",
]
