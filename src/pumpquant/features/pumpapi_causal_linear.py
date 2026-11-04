"""Linear-prefix causal state materialisation for PumpAPI launch research."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import duckdb

from pumpquant.features._sql import one_row

KIND = "pumpquant.pumpapi_causal_states"
VERSION = 4
ROLLING_WINDOWS_MS = (1_000, 2_000, 5_000, 10_000, 20_000, 30_000, 60_000, 120_000)


def _window_suffix(milliseconds: int) -> str:
    if milliseconds % 1_000:
        return f"{milliseconds}ms"
    return f"{milliseconds // 1_000}s"


_STATE_COLUMNS = """
mint,
launch_time_ms,
launch_archive_sequence,
launch_block,
launch_pool_id,
creator,
creator_prior_launch_count,
prior_same_uri_launch_count,
prior_same_name_launch_count,
prior_same_symbol_launch_count,
creator_fee_address,
mayhem_mode,
cashback_enabled,
token_program,
launch_name,
launch_symbol,
launch_uri,
initial_buy_quote,
initial_supply,
initial_priority_fee,
try_cast(initial_real_base AS DOUBLE) AS initial_real_base,
try_cast(initial_real_quote AS DOUBLE) AS initial_real_quote,
try_cast(initial_virtual_base AS DOUBLE) AS initial_virtual_base,
try_cast(initial_virtual_quote AS DOUBLE) AS initial_virtual_quote,
try_cast(initial_market_cap_quote AS DOUBLE) AS initial_market_cap_quote,
try_cast(initial_pool_fee_rate AS DOUBLE) AS initial_pool_fee_rate,
{checkpoint_kind} AS checkpoint_kind,
checkpoint_value,
decision_time_ms,
decision_archive_sequence,
({maximum_timestamp_ms} - decision_time_ms) < {horizon}
    AS right_censored_300s,
coalesce(trade_number, 0) AS trade_count,
coalesce(cum_buy_count, 0) AS buy_count,
coalesce(cum_sell_count, 0) AS sell_count,
coalesce(cum_unique_traders, 0) AS unique_traders,
coalesce(cum_unique_buyers, 0) AS unique_buyers,
coalesce(cum_unique_sellers, 0) AS unique_sellers,
coalesce(cum_buy_quote, 0.0) AS buy_quote_amount,
coalesce(cum_sell_quote, 0.0) AS sell_quote_amount,
coalesce(cum_buy_token, 0.0) AS buy_token_amount,
coalesce(cum_sell_token, 0.0) AS sell_token_amount,
coalesce(cum_buy_quote, 0.0) - coalesce(cum_sell_quote, 0.0)
    AS signed_quote_flow,
coalesce(cum_buy_token, 0.0) - coalesce(cum_sell_token, 0.0)
    AS signed_token_flow,
{rolling},
coalesce(cum_creator_trade_count, 0) AS creator_trade_count,
coalesce(cum_creator_buy_quote, 0.0) AS creator_buy_quote,
coalesce(cum_creator_sell_quote, 0.0) AS creator_sell_quote,
first_trade_price,
price AS latest_price,
CASE
  WHEN coalesce(cum_price_count, 0) = 0 THEN NULL
  ELSE sqrt(greatest(
    0.0,
    cum_log_price_sq / cum_price_count
      - (cum_log_price / cum_price_count) * (cum_log_price / cum_price_count)
  ))
END AS log_price_volatility,
minimum_price,
maximum_price,
maximum_price_timestamp_ms,
minimum_price_timestamp_ms,
timestamp_ms AS latest_trade_timestamp_ms,
last_buy_timestamp_ms,
last_sell_timestamp_ms,
CASE WHEN timestamp_ms IS NOT NULL
     THEN greatest(decision_time_ms - timestamp_ms, 0) END
    AS milliseconds_since_trade,
CASE WHEN last_buy_timestamp_ms IS NOT NULL
     THEN greatest(decision_time_ms - last_buy_timestamp_ms, 0) END
    AS milliseconds_since_buy,
CASE WHEN last_sell_timestamp_ms IS NOT NULL
     THEN greatest(decision_time_ms - last_sell_timestamp_ms, 0) END
    AS milliseconds_since_sell,
coalesce(current_buy_streak, 0) AS current_buy_streak,
coalesce(current_sell_streak, 0) AS current_sell_streak,
coalesce(cum_quote_amount_sq, 0.0) AS quote_amount_sum_squares,
coalesce(cum_buy_quote_sq, 0.0) AS buy_quote_sum_squares,
coalesce(cum_sell_quote_sq, 0.0) AS sell_quote_sum_squares,
coalesce(max_buy_quote, 0.0) AS maximum_buy_quote,
coalesce(max_sell_quote, 0.0) AS maximum_sell_quote,
coalesce(cum_signer_quote_sq, 0.0) AS signer_quote_sum_squares,
coalesce(cum_signer_buy_quote_sq, 0.0) AS signer_buy_quote_sum_squares,
coalesce(cum_signer_sell_quote_sq, 0.0) AS signer_sell_quote_sum_squares,
coalesce(cum_signer_inventory_sq, 0.0)
    AS signer_inventory_sum_squares,
coalesce(cum_recovered_signer_balance_sq, 0.0)
    AS recovered_signer_balance_sum_squares,
coalesce(cum_recovered_signer_token_balance, 0.0)
    AS recovered_signer_token_balance,
signer_post_sol_balance AS latest_signer_post_sol_balance,
signer_post_token_balance AS latest_signer_post_token_balance,
signer_post_quote_balance AS latest_signer_post_quote_balance,
quote_amount AS latest_trade_quote_amount,
coalesce(max_signer_quote, 0.0) AS maximum_signer_quote,
coalesce(max_signer_buy_quote, 0.0) AS maximum_signer_buy_quote,
coalesce(max_signer_sell_quote, 0.0) AS maximum_signer_sell_quote,
coalesce(cum_priority_fee, 0.0) AS total_priority_fee,
coalesce(max_priority_fee, 0.0) AS maximum_priority_fee,
latest_priority_fee,
coalesce(cum_multi_action_events, 0) AS multi_action_event_count,
coalesce(cum_program_count, 0) AS cumulative_program_count,
coalesce(cum_program_flashx, 0) AS program_flashx_count,
coalesce(cum_program_jupiter, 0) AS program_jupiter_count,
coalesce(cum_program_mayhem, 0) AS program_mayhem_count,
coalesce(cum_program_df1, 0) AS program_df1_count,
coalesce(cum_program_6vo, 0) AS program_6vo_count,
coalesce(cum_program_gmgn, 0) AS program_gmgn_count,
coalesce(cum_program_provf, 0) AS program_provf_count,
coalesce(cum_program_terminal, 0) AS program_terminal_count,
coalesce(cum_involved_wallet_count, 0) AS cumulative_involved_wallet_count,
interarrival_ms AS latest_interarrival_ms,
coalesce(cum_interarrival_ms, 0) AS total_interarrival_ms,
coalesce(cum_interarrival_ms_sq, 0) AS interarrival_ms_sum_squares,
coalesce(cum_repeat_actor_trades, 0) AS repeat_actor_trade_count,
coalesce(cum_signer_prior_distinct_mints, 0)
    AS signer_prior_distinct_mints_sum,
coalesce(current_atomic_event_count, 0) AS current_atomic_event_count,
coalesce(current_atomic_signature_count, 0)
    AS current_atomic_signature_count,
coalesce(current_atomic_signer_count, 0) AS current_atomic_signer_count,
coalesce(current_atomic_buy_quote, 0.0) AS current_atomic_buy_quote,
coalesce(current_atomic_sell_quote, 0.0) AS current_atomic_sell_quote,
coalesce(global_launch_count, 0)
  - coalesce(prior_60s_global_launch_count, 0)
    AS global_launch_count_last_60s,
coalesce(global_trade_count, 0)
  - coalesce(prior_60s_global_trade_count, 0)
    AS global_trade_count_last_60s,
coalesce(global_buy_count, 0)
  - coalesce(prior_60s_global_buy_count, 0)
    AS global_buy_count_last_60s,
coalesce(global_sell_count, 0)
  - coalesce(prior_60s_global_sell_count, 0)
    AS global_sell_count_last_60s,
coalesce(global_buy_quote, 0.0)
  - coalesce(prior_60s_global_buy_quote, 0.0)
    AS global_buy_quote_last_60s,
coalesce(global_sell_quote, 0.0)
  - coalesce(prior_60s_global_sell_quote, 0.0)
    AS global_sell_quote_last_60s,
coalesce(global_priority_fee, 0.0)
  - coalesce(prior_60s_global_priority_fee, 0.0)
    AS global_priority_fee_last_60s,
coalesce(pool, 'pump') AS latest_pool,
coalesce(pool_id, launch_pool_id) AS latest_pool_id,
coalesce(real_base, try_cast(initial_real_base AS DOUBLE)) AS latest_real_base,
coalesce(real_quote, try_cast(initial_real_quote AS DOUBLE)) AS latest_real_quote,
coalesce(virtual_base, try_cast(initial_virtual_base AS DOUBLE))
    AS latest_virtual_base,
coalesce(virtual_quote, try_cast(initial_virtual_quote AS DOUBLE))
    AS latest_virtual_quote,
coalesce(market_cap_quote, try_cast(initial_market_cap_quote AS DOUBLE))
    AS latest_market_cap_quote,
coalesce(pool_fee_rate, try_cast(initial_pool_fee_rate AS DOUBLE))
    AS latest_pool_fee_rate,
coalesce(real_base_text, initial_real_base) AS latest_real_base_text,
coalesce(real_quote_text, initial_real_quote) AS latest_real_quote_text,
coalesce(virtual_base_text, initial_virtual_base) AS latest_virtual_base_text,
coalesce(virtual_quote_text, initial_virtual_quote) AS latest_virtual_quote_text,
coalesce(pool_fee_rate_text, initial_pool_fee_rate) AS latest_pool_fee_rate_text,
coalesce(decimals_text, decimals) AS decimals_text
"""


def _current_rows_sql(kind: str, checkpoint: int) -> str:
    """The latest prefix row for each launch at one checkpoint."""
    if kind == "time_ms":
        return f"""
        SELECT
            l.*,
            {checkpoint}::BIGINT AS checkpoint_value,
            l.launch_time_ms + {checkpoint} AS decision_time_ms,
            NULL::BIGINT AS decision_archive_sequence,
            p.*
        FROM launches l
        ASOF LEFT JOIN prefix p
          ON l.mint = p.mint
         AND l.launch_time_ms + {checkpoint} > p.timestamp_ms
        """
    if kind == "trade_event":
        return f"""
        SELECT
            l.*,
            {checkpoint}::BIGINT AS checkpoint_value,
            p.timestamp_ms AS decision_time_ms,
            p.archive_sequence AS decision_archive_sequence,
            p.*
        FROM launches l
        INNER JOIN prefix p
          ON l.mint = p.mint
         AND p.trade_number = {checkpoint}
        """
    if kind == "batched":
        return "SELECT * FROM all_current"
    raise ValueError(f"unsupported checkpoint kind: {kind}")


def _window_ctes(current: str) -> tuple[list[str], str]:
    """Join each launch to the prefix row one rolling window earlier, then the global counts."""
    ctes = [f"current AS ({current})"]
    previous = "current"
    for milliseconds in ROLLING_WINDOWS_MS:
        alias = f"lag_{milliseconds}"
        ctes.append(
            f"""
            {alias} AS (
                SELECT
                    c.*,
                    q.cum_buy_count AS prior_{milliseconds}_buy_count,
                    q.cum_sell_count AS prior_{milliseconds}_sell_count,
                    q.cum_buy_quote AS prior_{milliseconds}_buy_quote,
                    q.cum_sell_quote AS prior_{milliseconds}_sell_quote,
                    q.cum_buy_token AS prior_{milliseconds}_buy_token,
                    q.cum_sell_token AS prior_{milliseconds}_sell_token,
                    q.cum_unique_traders AS prior_{milliseconds}_unique_traders,
                    q.cum_unique_buyers AS prior_{milliseconds}_unique_buyers,
                    q.cum_unique_sellers AS prior_{milliseconds}_unique_sellers,
                    q.cum_signer_inventory_sq
                      AS prior_{milliseconds}_signer_inventory_sq,
                    q.price AS prior_{milliseconds}_price,
                    q.virtual_quote AS prior_{milliseconds}_virtual_quote
                FROM {previous} c
                ASOF LEFT JOIN prefix q
                  ON c.mint = q.mint
                 AND c.decision_time_ms - {milliseconds} > q.timestamp_ms
            )
            """
        )
        previous = alias
    ctes.append(
        f"""
        global_current AS (
            SELECT
                c.*,
                g.global_launch_count,
                g.global_trade_count,
                g.global_buy_count,
                g.global_sell_count,
                g.global_buy_quote,
                g.global_sell_quote,
                g.global_priority_fee
            FROM {previous} c
            ASOF LEFT JOIN global_prefix g
              ON c.decision_time_ms > g.timestamp_ms
        )
        """
    )
    ctes.append(
        """
        global_60s AS (
            SELECT
                c.*,
                g.global_launch_count AS prior_60s_global_launch_count,
                g.global_trade_count AS prior_60s_global_trade_count,
                g.global_buy_count AS prior_60s_global_buy_count,
                g.global_sell_count AS prior_60s_global_sell_count,
                g.global_buy_quote AS prior_60s_global_buy_quote,
                g.global_sell_quote AS prior_60s_global_sell_quote,
                g.global_priority_fee AS prior_60s_global_priority_fee
            FROM global_current c
            ASOF LEFT JOIN global_prefix g
              ON c.decision_time_ms - 60000 > g.timestamp_ms
        )
        """
    )
    previous = "global_60s"
    return ctes, previous


def _rolling_columns() -> list[str]:
    """Flow, price and breadth changes over each rolling window."""
    rolling_select: list[str] = []
    for milliseconds in ROLLING_WINDOWS_MS:
        suffix = _window_suffix(milliseconds)
        for side in ("buy", "sell"):
            rolling_select.extend(
                (
                    f"""
                    coalesce(cum_{side}_count, 0)
                      - coalesce(prior_{milliseconds}_{side}_count, 0)
                        AS {side}_count_last_{suffix}
                    """,
                    f"""
                    coalesce(cum_{side}_quote, 0.0)
                      - coalesce(prior_{milliseconds}_{side}_quote, 0.0)
                        AS {side}_quote_last_{suffix}
                    """,
                    f"""
                    coalesce(cum_{side}_token, 0.0)
                      - coalesce(prior_{milliseconds}_{side}_token, 0.0)
                        AS {side}_token_last_{suffix}
                    """,
                )
            )
        if milliseconds in (5_000, 20_000, 60_000):
            rolling_select.extend(
                (
                    f"""
                    price / nullif(prior_{milliseconds}_price, 0) - 1.0
                      AS price_return_last_{suffix}
                    """,
                    f"""
                    coalesce(cum_unique_traders, 0)
                      - coalesce(prior_{milliseconds}_unique_traders, 0)
                        AS unique_traders_added_last_{suffix}
                    """,
                    f"""
                    (
                      cum_unique_buyers::DOUBLE / nullif(cum_buy_count, 0)
                    ) - (
                      prior_{milliseconds}_unique_buyers::DOUBLE
                        / nullif(prior_{milliseconds}_buy_count, 0)
                    ) AS buyer_breadth_change_last_{suffix}
                    """,
                    f"""
                    virtual_quote - prior_{milliseconds}_virtual_quote
                      AS virtual_quote_change_last_{suffix}
                    """,
                    f"""
                    cum_signer_inventory_sq
                      / nullif(power(greatest(cum_buy_token - cum_sell_token, 0), 2), 0)
                    - prior_{milliseconds}_signer_inventory_sq
                      / nullif(power(greatest(
                          prior_{milliseconds}_buy_token
                            - prior_{milliseconds}_sell_token,
                          0
                        ), 2), 0)
                      AS signer_inventory_hhi_change_last_{suffix}
                    """,
                )
            )
    return rolling_select


def _part_query(kind: str, checkpoint: int, maximum_timestamp_ms: int, horizon: int) -> str:
    ctes, source = _window_ctes(_current_rows_sql(kind, checkpoint))
    columns = _STATE_COLUMNS.format(
        checkpoint_kind="checkpoint_kind" if kind == "batched" else repr(kind) + "::VARCHAR",
        maximum_timestamp_ms=maximum_timestamp_ms,
        horizon=horizon,
        rolling=",".join(_rolling_columns()),
    )
    return f"WITH {','.join(ctes)}\nSELECT\n{columns}\nFROM {source}"


# The stages build the per-launch prefix tables in order; each reads only the tables before it.
_LAUNCHES = """
CREATE TABLE launches AS
WITH raw_launches AS (
SELECT
    mint,
    timestamp_ms AS launch_time_ms,
    archive_sequence AS launch_archive_sequence,
    block AS launch_block,
    pool_id AS launch_pool_id,
    tx_signer AS creator,
    creator_fee_address,
    mayhem_mode,
    cashback_enabled,
    token_program,
    name AS launch_name,
    symbol AS launch_symbol,
    uri AS launch_uri,
    try_cast(quoteAmount AS DOUBLE) AS initial_buy_quote,
    try_cast(supply AS DOUBLE) AS initial_supply,
    try_cast(priorityFee AS DOUBLE) AS initial_priority_fee,
    decimals,
    tokensInPool AS initial_real_base,
    quoteInPool AS initial_real_quote,
    vTokensInBondingCurve AS initial_virtual_base,
    vQuoteInBondingCurve AS initial_virtual_quote,
    marketCapQuote AS initial_market_cap_quote,
    poolFeeRate AS initial_pool_fee_rate
FROM read_parquet('{source}')
WHERE action = 'create' AND pool = 'pump'
QUALIFY row_number() OVER (
  PARTITION BY mint ORDER BY timestamp_ms, archive_sequence
) = 1
)
SELECT *,
  row_number() OVER (
    PARTITION BY creator
    ORDER BY launch_time_ms, launch_archive_sequence
  ) - 1 AS creator_prior_launch_count,
  row_number() OVER (
    PARTITION BY launch_uri
    ORDER BY launch_time_ms, launch_archive_sequence
  ) - 1 AS prior_same_uri_launch_count,
  row_number() OVER (
    PARTITION BY lower(launch_name)
    ORDER BY launch_time_ms, launch_archive_sequence
  ) - 1 AS prior_same_name_launch_count,
  row_number() OVER (
    PARTITION BY lower(launch_symbol)
    ORDER BY launch_time_ms, launch_archive_sequence
  ) - 1 AS prior_same_symbol_launch_count
FROM raw_launches
"""

_TRADE_FLAGS = """
CREATE TABLE trade_flags AS
WITH base AS (
  SELECT
    r.mint,
    r.timestamp_ms,
    r.archive_sequence,
    r.signature,
    r.action,
    r.pool,
    r.pool_id,
    r.tx_signer,
    l.creator,
    try_cast(json_extract_string(
      r.participant_post_balances_json,
      '$."' || r.tx_signer || '".sol'
    ) AS DOUBLE) AS signer_post_sol_balance,
    try_cast(json_extract_string(
      r.participant_post_balances_json,
      '$."' || r.tx_signer || '".token'
    ) AS DOUBLE) AS signer_post_token_balance,
    try_cast(json_extract_string(
      r.participant_post_balances_json,
      '$."' || r.tx_signer || '".quote'
    ) AS DOUBLE) AS signer_post_quote_balance,
    try_cast(r.tokenAmount AS DOUBLE) AS token_amount,
    try_cast(r.quoteAmount AS DOUBLE) AS quote_amount,
    try_cast(r.tokensInPool AS DOUBLE) AS real_base,
    try_cast(r.quoteInPool AS DOUBLE) AS real_quote,
    try_cast(r.vTokensInBondingCurve AS DOUBLE) AS virtual_base,
    try_cast(r.vQuoteInBondingCurve AS DOUBLE) AS virtual_quote,
    try_cast(r.price AS DOUBLE) AS price,
    try_cast(r.marketCapQuote AS DOUBLE) AS market_cap_quote,
    try_cast(r.poolFeeRate AS DOUBLE) AS pool_fee_rate,
    try_cast(r.priorityFee AS DOUBLE) AS priority_fee,
    coalesce(json_array_length(r.programs_used_json), 0)
        AS program_count,
    contains(r.programs_used_json, 'FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9')
        AS uses_program_flashx,
    contains(r.programs_used_json, 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4')
        AS uses_program_jupiter,
    contains(r.programs_used_json, 'MAyhSmzXzV1pTf7LsNkrNwkWKTo4ougAJ1PPg47MD4e')
        AS uses_program_mayhem,
    contains(r.programs_used_json, 'DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH')
        AS uses_program_df1,
    contains(r.programs_used_json, '6Vo3245eszAb5wuqEMw8mGdbfRUdKbHhDHP5LcaGuTAB')
        AS uses_program_6vo,
    contains(r.programs_used_json, 'GMgnVFR8Jb39LoXsEVzb3DvBy3ywCmdmJquHUy1Lrkqb')
        AS uses_program_gmgn,
    contains(r.programs_used_json, 'proVF4pMXVaYqmy4NjniPh4pqKNfMmsihgd4wdkCX3u')
        AS uses_program_provf,
    contains(r.programs_used_json, 'term9YPb9mzAsABaqN71A4xdbxHmpBNZavpBiQKZzN3')
        AS uses_program_terminal,
    coalesce(json_array_length(json_keys(r.traders_involved_json)), 0)
        AS involved_wallet_count,
    r.tokensInPool AS real_base_text,
    r.quoteInPool AS real_quote_text,
    r.vTokensInBondingCurve AS virtual_base_text,
    r.vQuoteInBondingCurve AS virtual_quote_text,
    r.poolFeeRate AS pool_fee_rate_text,
    r.decimals AS decimals_text
  FROM read_parquet('{source}') r
  INNER JOIN launches l USING (mint)
  WHERE r.action IN ('buy', 'sell')
    AND (
      r.timestamp_ms > l.launch_time_ms
      OR (
        r.timestamp_ms = l.launch_time_ms
        AND r.archive_sequence > l.launch_archive_sequence
      )
    )
)
SELECT
  *,
  row_number() OVER (
    PARTITION BY mint ORDER BY timestamp_ms, archive_sequence
  ) AS trade_number,
  row_number() OVER (
    PARTITION BY mint, tx_signer ORDER BY timestamp_ms, archive_sequence
  ) = 1 AS first_trader,
  row_number() OVER (
    PARTITION BY mint, action, tx_signer ORDER BY timestamp_ms, archive_sequence
  ) = 1 AS first_side_trader,
  count(*) OVER (PARTITION BY signature) AS signature_event_count
FROM base
"""

_TEMPORAL_FLAGS = """
CREATE TABLE temporal_flags AS
WITH lagged AS (
  SELECT *,
    lag(timestamp_ms) OVER (
      PARTITION BY mint ORDER BY timestamp_ms, archive_sequence
    ) AS previous_trade_timestamp_ms,
    count(*) OVER (
      PARTITION BY tx_signer
      ORDER BY timestamp_ms, archive_sequence
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS signer_prior_trade_count,
    count(DISTINCT mint) OVER (
      PARTITION BY tx_signer
      ORDER BY timestamp_ms, archive_sequence
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS signer_prior_distinct_mints,
    lag(coalesce(signer_post_token_balance, 0.0), 1, 0.0) OVER (
      PARTITION BY mint, tx_signer
      ORDER BY timestamp_ms, archive_sequence
    ) AS signer_previous_post_token_balance
  FROM trade_flags
),
clustered AS (
  SELECT *,
    CASE
      WHEN previous_trade_timestamp_ms IS NULL
        OR timestamp_ms - previous_trade_timestamp_ms > 3
      THEN 1 ELSE 0
    END AS atomic_cluster_start,
    CASE WHEN previous_trade_timestamp_ms IS NULL THEN NULL
         ELSE timestamp_ms - previous_trade_timestamp_ms END
      AS interarrival_ms
  FROM lagged
),
cluster_ids AS (
  SELECT *,
    sum(atomic_cluster_start) OVER (
      PARTITION BY mint ORDER BY timestamp_ms, archive_sequence
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS atomic_cluster_id
  FROM clustered
)
SELECT *,
  count(*) OVER cluster_w AS current_atomic_event_count,
  count(DISTINCT signature) OVER cluster_w
    AS current_atomic_signature_count,
  count(DISTINCT tx_signer) OVER cluster_w
    AS current_atomic_signer_count,
  sum(CASE WHEN action = 'buy' THEN quote_amount ELSE 0 END)
    OVER cluster_w AS current_atomic_buy_quote,
  sum(CASE WHEN action = 'sell' THEN quote_amount ELSE 0 END)
    OVER cluster_w AS current_atomic_sell_quote
FROM cluster_ids
WINDOW cluster_w AS (
  PARTITION BY mint, atomic_cluster_id
  ORDER BY timestamp_ms, archive_sequence
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
"""

_SIGNER_RUNNING = """
CREATE TABLE signer_running AS
SELECT
  *,
  sum(quote_amount) OVER signer_w AS signer_quote,
  sum(CASE WHEN action = 'buy' THEN quote_amount ELSE 0 END)
    OVER signer_w AS signer_buy_quote,
  sum(CASE WHEN action = 'sell' THEN quote_amount ELSE 0 END)
    OVER signer_w AS signer_sell_quote,
  sum(CASE WHEN action = 'buy' THEN token_amount ELSE -token_amount END)
    OVER signer_w AS signer_net_token
FROM temporal_flags
WINDOW signer_w AS (
  PARTITION BY mint, tx_signer
  ORDER BY timestamp_ms, archive_sequence
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
"""

_CONCENTRATION_FLAGS = """
CREATE TABLE concentration_flags AS
SELECT
  *,
  signer_quote * signer_quote
    - (signer_quote - quote_amount) * (signer_quote - quote_amount)
      AS signer_quote_sq_delta,
  signer_buy_quote * signer_buy_quote
    - (
        signer_buy_quote
        - CASE WHEN action = 'buy' THEN quote_amount ELSE 0 END
      ) * (
        signer_buy_quote
        - CASE WHEN action = 'buy' THEN quote_amount ELSE 0 END
      ) AS signer_buy_quote_sq_delta,
  signer_sell_quote * signer_sell_quote
    - (
        signer_sell_quote
        - CASE WHEN action = 'sell' THEN quote_amount ELSE 0 END
      ) * (
        signer_sell_quote
        - CASE WHEN action = 'sell' THEN quote_amount ELSE 0 END
      ) AS signer_sell_quote_sq_delta,
  greatest(signer_net_token, 0.0)
      * greatest(signer_net_token, 0.0)
    - greatest(
        signer_net_token
          - CASE WHEN action = 'buy' THEN token_amount ELSE -token_amount END,
        0.0
      ) * greatest(
        signer_net_token
          - CASE WHEN action = 'buy' THEN token_amount ELSE -token_amount END,
        0.0
      ) AS signer_inventory_sq_delta,
  coalesce(signer_post_token_balance, 0.0)
      * coalesce(signer_post_token_balance, 0.0)
    - signer_previous_post_token_balance
      * signer_previous_post_token_balance
        AS recovered_signer_balance_sq_delta,
  coalesce(signer_post_token_balance, 0.0)
    - signer_previous_post_token_balance
        AS recovered_signer_balance_delta,
  max(CASE WHEN action = 'sell' THEN trade_number END) OVER (
    PARTITION BY mint ORDER BY timestamp_ms, archive_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS last_sell_trade_number,
  max(CASE WHEN action = 'buy' THEN trade_number END) OVER (
    PARTITION BY mint ORDER BY timestamp_ms, archive_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS last_buy_trade_number
FROM signer_running
"""

_PREFIX = """
CREATE TABLE prefix AS
SELECT
  *,
  sum((action = 'buy')::INTEGER) OVER w AS cum_buy_count,
  sum((action = 'sell')::INTEGER) OVER w AS cum_sell_count,
  sum(first_trader::INTEGER) OVER w AS cum_unique_traders,
  sum((first_side_trader AND action = 'buy')::INTEGER) OVER w
    AS cum_unique_buyers,
  sum((first_side_trader AND action = 'sell')::INTEGER) OVER w
    AS cum_unique_sellers,
  sum(CASE WHEN action = 'buy' THEN quote_amount ELSE 0 END) OVER w
    AS cum_buy_quote,
  sum(CASE WHEN action = 'sell' THEN quote_amount ELSE 0 END) OVER w
    AS cum_sell_quote,
  sum(CASE WHEN action = 'buy' THEN token_amount ELSE 0 END) OVER w
    AS cum_buy_token,
  sum(CASE WHEN action = 'sell' THEN token_amount ELSE 0 END) OVER w
    AS cum_sell_token,
  sum(quote_amount * quote_amount) OVER w AS cum_quote_amount_sq,
  sum(CASE WHEN action = 'buy' THEN quote_amount * quote_amount ELSE 0 END)
    OVER w AS cum_buy_quote_sq,
  sum(CASE WHEN action = 'sell' THEN quote_amount * quote_amount ELSE 0 END)
    OVER w AS cum_sell_quote_sq,
  max(CASE WHEN action = 'buy' THEN quote_amount END) OVER w
    AS max_buy_quote,
  max(CASE WHEN action = 'sell' THEN quote_amount END) OVER w
    AS max_sell_quote,
  sum(signer_quote_sq_delta) OVER w AS cum_signer_quote_sq,
  sum(signer_buy_quote_sq_delta) OVER w AS cum_signer_buy_quote_sq,
  sum(signer_sell_quote_sq_delta) OVER w AS cum_signer_sell_quote_sq,
  sum(signer_inventory_sq_delta) OVER w AS cum_signer_inventory_sq,
  sum(recovered_signer_balance_sq_delta) OVER w
    AS cum_recovered_signer_balance_sq,
  sum(recovered_signer_balance_delta) OVER w
    AS cum_recovered_signer_token_balance,
  max(signer_quote) OVER w AS max_signer_quote,
  max(signer_buy_quote) OVER w AS max_signer_buy_quote,
  max(signer_sell_quote) OVER w AS max_signer_sell_quote,
  sum((tx_signer = creator)::INTEGER) OVER w AS cum_creator_trade_count,
  sum(CASE WHEN tx_signer = creator AND action = 'buy'
           THEN quote_amount ELSE 0 END) OVER w AS cum_creator_buy_quote,
  sum(CASE WHEN tx_signer = creator AND action = 'sell'
           THEN quote_amount ELSE 0 END) OVER w AS cum_creator_sell_quote,
  count(price) FILTER (WHERE price > 0) OVER w AS cum_price_count,
  sum(ln(price)) FILTER (WHERE price > 0) OVER w AS cum_log_price,
  sum(ln(price) * ln(price)) FILTER (WHERE price > 0) OVER w
    AS cum_log_price_sq,
  first_value(price IGNORE NULLS) OVER w AS first_trade_price,
  min(price) FILTER (WHERE price > 0) OVER w AS minimum_price,
  max(price) FILTER (WHERE price > 0) OVER w AS maximum_price,
  arg_max(timestamp_ms, price) FILTER (WHERE price > 0) OVER w
    AS maximum_price_timestamp_ms,
  arg_min(timestamp_ms, price) FILTER (WHERE price > 0) OVER w
    AS minimum_price_timestamp_ms,
  max(timestamp_ms) FILTER (WHERE action = 'buy') OVER w
    AS last_buy_timestamp_ms,
  max(timestamp_ms) FILTER (WHERE action = 'sell') OVER w
    AS last_sell_timestamp_ms,
  CASE WHEN action = 'buy'
       THEN trade_number - coalesce(last_sell_trade_number, 0)
       ELSE 0 END AS current_buy_streak,
  CASE WHEN action = 'sell'
       THEN trade_number - coalesce(last_buy_trade_number, 0)
       ELSE 0 END AS current_sell_streak,
  sum(coalesce(priority_fee, 0.0)) OVER w AS cum_priority_fee,
  max(priority_fee) OVER w AS max_priority_fee,
  priority_fee AS latest_priority_fee,
  sum((signature_event_count > 1)::INTEGER) OVER w
    AS cum_multi_action_events,
  sum(program_count) OVER w AS cum_program_count,
  sum(uses_program_flashx::INTEGER) OVER w AS cum_program_flashx,
  sum(uses_program_jupiter::INTEGER) OVER w AS cum_program_jupiter,
  sum(uses_program_mayhem::INTEGER) OVER w AS cum_program_mayhem,
  sum(uses_program_df1::INTEGER) OVER w AS cum_program_df1,
  sum(uses_program_6vo::INTEGER) OVER w AS cum_program_6vo,
  sum(uses_program_gmgn::INTEGER) OVER w AS cum_program_gmgn,
  sum(uses_program_provf::INTEGER) OVER w AS cum_program_provf,
  sum(uses_program_terminal::INTEGER) OVER w AS cum_program_terminal,
  sum(involved_wallet_count) OVER w AS cum_involved_wallet_count,
  sum(coalesce(interarrival_ms, 0)) OVER w AS cum_interarrival_ms,
  sum(coalesce(interarrival_ms, 0) * coalesce(interarrival_ms, 0))
    OVER w AS cum_interarrival_ms_sq,
  sum((signer_prior_trade_count > 0)::INTEGER) OVER w
    AS cum_repeat_actor_trades,
  sum(coalesce(signer_prior_distinct_mints, 0)) OVER w
    AS cum_signer_prior_distinct_mints
FROM concentration_flags
WINDOW w AS (
  PARTITION BY mint
  ORDER BY timestamp_ms, archive_sequence
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
"""

_GLOBAL_PREFIX = """
CREATE TABLE global_prefix AS
WITH events AS (
  SELECT
    timestamp_ms,
    archive_sequence,
    action,
    try_cast(quoteAmount AS DOUBLE) AS quote_amount,
    try_cast(priorityFee AS DOUBLE) AS priority_fee
  FROM read_parquet('{source}')
  WHERE action IN ('create', 'buy', 'sell')
)
SELECT
  timestamp_ms,
  archive_sequence,
  sum((action = 'create')::INTEGER) OVER w AS global_launch_count,
  sum((action IN ('buy', 'sell'))::INTEGER) OVER w AS global_trade_count,
  sum((action = 'buy')::INTEGER) OVER w AS global_buy_count,
  sum((action = 'sell')::INTEGER) OVER w AS global_sell_count,
  sum(CASE WHEN action = 'buy' THEN coalesce(quote_amount, 0.0) ELSE 0 END)
    OVER w AS global_buy_quote,
  sum(CASE WHEN action = 'sell' THEN coalesce(quote_amount, 0.0) ELSE 0 END)
    OVER w AS global_sell_quote,
  sum(coalesce(priority_fee, 0.0)) OVER w AS global_priority_fee
FROM events
WINDOW w AS (
  ORDER BY timestamp_ms, archive_sequence
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
"""

_STAGES = (
    _LAUNCHES,
    _TRADE_FLAGS,
    _TEMPORAL_FLAGS,
    _SIGNER_RUNNING,
    _CONCENTRATION_FLAGS,
    _PREFIX,
    _GLOBAL_PREFIX,
)


def materialize_causal_states_linear(
    source_glob: str,
    output: Path,
    *,
    time_checkpoints_ms: tuple[int, ...],
    event_checkpoints: tuple[int, ...],
    outcome_horizon_ms: int = 300_000,
    threads: int = 10,
) -> dict[str, Any]:
    if not time_checkpoints_ms or not event_checkpoints:
        raise ValueError("time and event checkpoints must be nonempty")
    if any(value <= 0 for value in (*time_checkpoints_ms, *event_checkpoints)):
        raise ValueError("checkpoints must be positive")
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"refusing to overwrite {output}")
    output.mkdir(parents=True, exist_ok=True)
    parts = output / "parts"
    parts.mkdir()
    database = output / ".work.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute(f"PRAGMA threads={threads}")
    connection.execute("PRAGMA preserve_insertion_order=false")
    escaped = source_glob.replace("'", "''")
    started = time.monotonic()

    for stage in _STAGES:
        connection.execute(stage.format(source=escaped))
    maximum_timestamp_ms = one_row(
        connection.execute(f"SELECT max(timestamp_ms) FROM read_parquet('{escaped}')")
    )[0]
    launch_count = one_row(connection.execute("SELECT count(*) FROM launches"))[0]
    event_counts = {
        value: one_row(
            connection.execute("SELECT count(*) FROM prefix WHERE trade_number = ?", [value])
        )[0]
        for value in event_checkpoints
    }
    total_rows = launch_count * len(time_checkpoints_ms) + sum(event_counts.values())
    schedule = [
        *(("time_ms", value, launch_count) for value in time_checkpoints_ms),
        *(("trade_event", value, event_counts[value]) for value in event_checkpoints),
    ]
    part_records: list[dict[str, Any]] = []
    for index, (kind, value, expected_rows) in enumerate(schedule, start=1):
        path = parts / f"{index:02d}-{kind}-{value}.parquet"
        query = _part_query(kind, value, maximum_timestamp_ms, outcome_horizon_ms)
        record = _write_part(connection, query, path, expected_rows)
        record = {
            "path": str(path.relative_to(output)),
            "checkpoint_kind": kind,
            "checkpoint_value": value,
            **record,
        }
        part_records.append(record)
        _report_progress(part_records, total_rows, started)

    elapsed = time.monotonic() - started
    manifest = {
        "kind": KIND,
        "version": VERSION,
        "status": "complete",
        "source_glob": source_glob,
        "launches": launch_count,
        "rows": total_rows,
        "time_rows": launch_count * len(time_checkpoints_ms),
        "event_rows": sum(event_counts.values()),
        "time_checkpoints_ms": list(time_checkpoints_ms),
        "event_checkpoints": list(event_checkpoints),
        "maximum_source_timestamp_ms": maximum_timestamp_ms,
        "outcome_horizon_ms": outcome_horizon_ms,
        "elapsed_seconds": elapsed,
        "parts": part_records,
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    connection.close()
    database.unlink(missing_ok=True)
    for suffix in (".wal",):
        Path(str(database) + suffix).unlink(missing_ok=True)
    return manifest


def _write_part(
    connection: duckdb.DuckDBPyConnection, query: str, path: Path, expected_rows: int
) -> dict[str, Any]:
    """Write one checkpoint's rows to Parquet and check the row count it was promised."""
    escaped_path = str(path).replace("'", "''")
    connection.execute(
        f"COPY ({query}) TO '{escaped_path}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
    )
    actual_rows = one_row(
        connection.execute(f"SELECT count(*) FROM read_parquet('{escaped_path}')")
    )[0]
    if actual_rows != expected_rows:
        raise RuntimeError(f"{path.name}: expected {expected_rows} rows, got {actual_rows}")
    return {"rows": actual_rows, "bytes": path.stat().st_size}


def _report_progress(parts: list[dict[str, Any]], total_rows: int, started: float) -> None:
    completed = sum(part["rows"] for part in parts)
    rate = completed / (time.monotonic() - started)
    progress = {
        "completed_rows": completed,
        "total_rows": total_rows,
        "percent": 100 * completed / total_rows,
        "rows_per_second": rate,
        "eta_seconds": (total_rows - completed) / rate if rate else None,
        "sealed_part": parts[-1],
    }
    print(json.dumps(progress, sort_keys=True), flush=True)
