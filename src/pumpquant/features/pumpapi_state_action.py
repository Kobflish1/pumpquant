"""Causal PumpAPI launch-state materialization for probability discovery.

The materializer keeps every earliest Pump launch in the denominator and emits
both elapsed-time and trade-event checkpoints.  Features use only events
strictly available at the checkpoint.  No outcome or eventual lifecycle field
is used.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from pumpquant.features._sql import one_row

KIND = "pumpquant.pumpapi_causal_states"
VERSION = 1


def _sql_list(values: tuple[int, ...]) -> str:
    if not values or any(isinstance(value, bool) or value <= 0 for value in values):
        raise ValueError("checkpoints must be positive integers")
    return ", ".join(f"({value})" for value in values)


def materialize_causal_states(
    source_glob: str,
    output: Path,
    *,
    time_checkpoints_ms: tuple[int, ...],
    event_checkpoints: tuple[int, ...],
    outcome_horizon_ms: int = 300_000,
    threads: int | None = None,
) -> dict[str, Any]:
    """Build a one-row-per-launch-checkpoint causal feature table."""

    if outcome_horizon_ms <= 0:
        raise ValueError("outcome_horizon_ms must be positive")
    output.mkdir(parents=True, exist_ok=True)
    table_path = output / "causal-states.parquet"
    manifest_path = output / "manifest.json"
    if table_path.exists() or manifest_path.exists():
        raise FileExistsError(f"refusing to overwrite {output}")

    connection = duckdb.connect()
    if threads is not None:
        if threads <= 0:
            raise ValueError("threads must be positive")
        connection.execute(f"PRAGMA threads={threads}")
    escaped_source = source_glob.replace("'", "''")

    connection.execute(
        f"""
        CREATE TEMP TABLE raw AS
        SELECT
            archive_sequence,
            signature,
            action,
            pool,
            block,
            timestamp_ms,
            pool_id,
            mint,
            tx_signer,
            tokenAmount,
            quoteAmount,
            tokensInPool,
            quoteInPool,
            vTokensInBondingCurve,
            vQuoteInBondingCurve,
            price,
            marketCapQuote,
            poolFeeRate,
            supply,
            decimals,
            creator_fee_address,
            mayhem_mode,
            cashback_enabled,
            token_program
        FROM read_parquet('{escaped_source}', union_by_name=true)
        WHERE mint IS NOT NULL
          AND pool IN ('pump', 'pump-amm')
        """
    )
    connection.execute(
        """
        CREATE TEMP TABLE launches AS
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
            decimals,
            supply,
            quoteAmount AS initial_quote_amount,
            tokensInPool AS initial_real_base,
            quoteInPool AS initial_real_quote,
            vTokensInBondingCurve AS initial_virtual_base,
            vQuoteInBondingCurve AS initial_virtual_quote,
            price AS initial_price,
            marketCapQuote AS initial_market_cap_quote,
            poolFeeRate AS initial_pool_fee_rate
        FROM raw
        WHERE action = 'create' AND pool = 'pump'
        QUALIFY row_number() OVER (
            PARTITION BY mint ORDER BY timestamp_ms, archive_sequence
        ) = 1
        """
    )
    connection.execute(
        """
        CREATE TEMP TABLE trades AS
        SELECT
            r.mint,
            r.timestamp_ms,
            r.archive_sequence,
            r.block,
            r.signature,
            r.action,
            r.pool,
            r.pool_id,
            r.tx_signer,
            TRY_CAST(r.tokenAmount AS DOUBLE) AS token_amount,
            TRY_CAST(r.quoteAmount AS DOUBLE) AS quote_amount,
            TRY_CAST(r.tokensInPool AS DOUBLE) AS real_base,
            TRY_CAST(r.quoteInPool AS DOUBLE) AS real_quote,
            TRY_CAST(r.vTokensInBondingCurve AS DOUBLE) AS virtual_base,
            TRY_CAST(r.vQuoteInBondingCurve AS DOUBLE) AS virtual_quote,
            TRY_CAST(r.price AS DOUBLE) AS price,
            TRY_CAST(r.marketCapQuote AS DOUBLE) AS market_cap_quote,
            TRY_CAST(r.poolFeeRate AS DOUBLE) AS pool_fee_rate,
            r.tokensInPool AS real_base_text,
            r.quoteInPool AS real_quote_text,
            r.vTokensInBondingCurve AS virtual_base_text,
            r.vQuoteInBondingCurve AS virtual_quote_text,
            r.poolFeeRate AS pool_fee_rate_text,
            r.decimals AS decimals_text,
            row_number() OVER (
                PARTITION BY r.mint ORDER BY r.timestamp_ms, r.archive_sequence
            ) AS trade_number
        FROM raw r
        INNER JOIN launches l USING (mint)
        WHERE r.action IN ('buy', 'sell')
          AND (
            r.timestamp_ms > l.launch_time_ms
            OR (
                r.timestamp_ms = l.launch_time_ms
                AND r.archive_sequence > l.launch_archive_sequence
            )
          )
        """
    )
    maximum_timestamp_ms = one_row(connection.execute("SELECT max(timestamp_ms) FROM raw"))[0]
    if maximum_timestamp_ms is None:
        raise ValueError("source contains no timestamps")

    connection.execute(
        f"""
        CREATE TEMP TABLE time_checkpoints AS
        SELECT
            l.*,
            'time_ms'::VARCHAR AS checkpoint_kind,
            v.checkpoint_value::BIGINT AS checkpoint_value,
            l.launch_time_ms + v.checkpoint_value AS decision_time_ms,
            NULL::BIGINT AS decision_archive_sequence
        FROM launches l
        CROSS JOIN (VALUES {_sql_list(time_checkpoints_ms)}) v(checkpoint_value)
        """
    )
    connection.execute(
        f"""
        CREATE TEMP TABLE event_checkpoints AS
        SELECT
            l.*,
            'trade_event'::VARCHAR AS checkpoint_kind,
            t.trade_number::BIGINT AS checkpoint_value,
            t.timestamp_ms AS decision_time_ms,
            t.archive_sequence AS decision_archive_sequence
        FROM launches l
        CROSS JOIN (VALUES {_sql_list(event_checkpoints)}) v(checkpoint_value)
        INNER JOIN trades t
          ON t.mint = l.mint
         AND t.trade_number = v.checkpoint_value
        """
    )
    connection.execute(
        """
        CREATE TEMP TABLE checkpoints AS
        SELECT * FROM time_checkpoints
        UNION ALL
        SELECT * FROM event_checkpoints
        """
    )

    query = f"""
    WITH causal AS (
            SELECT
                c.*,
                t.* EXCLUDE (mint),
                struct_pack(t := t.timestamp_ms, s := t.archive_sequence) AS event_order,
                t.timestamp_ms >= c.decision_time_ms - 1000 AS in_last_1s,
                t.timestamp_ms >= c.decision_time_ms - 5000 AS in_last_5s,
                t.tx_signer = c.creator AS is_creator_trade
        FROM checkpoints c
        LEFT JOIN trades t
          ON t.mint = c.mint
         AND (
            (
              c.checkpoint_kind = 'time_ms'
              AND t.timestamp_ms < c.decision_time_ms
            )
            OR
            (
              c.checkpoint_kind = 'trade_event'
              AND (
                t.timestamp_ms < c.decision_time_ms
                OR (
                  t.timestamp_ms = c.decision_time_ms
                  AND t.archive_sequence <= c.decision_archive_sequence
                )
              )
            )
         )
    )
    SELECT
        mint,
        launch_time_ms,
        launch_archive_sequence,
        launch_block,
        launch_pool_id,
        creator,
        creator_fee_address,
        mayhem_mode,
        cashback_enabled,
        token_program,
        checkpoint_kind,
        checkpoint_value,
        decision_time_ms,
        decision_archive_sequence,
        ({maximum_timestamp_ms} - launch_time_ms) < {outcome_horizon_ms}
            AS right_censored_300s,
        count(signature) AS trade_count,
        count(signature) FILTER (WHERE action = 'buy') AS buy_count,
        count(signature) FILTER (WHERE action = 'sell') AS sell_count,
        count(DISTINCT tx_signer) AS unique_traders,
        count(DISTINCT tx_signer) FILTER (WHERE action = 'buy') AS unique_buyers,
        count(DISTINCT tx_signer) FILTER (WHERE action = 'sell') AS unique_sellers,
        coalesce(sum(quote_amount) FILTER (WHERE action = 'buy'), 0.0)
            AS buy_quote_amount,
        coalesce(sum(quote_amount) FILTER (WHERE action = 'sell'), 0.0)
            AS sell_quote_amount,
        coalesce(sum(token_amount) FILTER (WHERE action = 'buy'), 0.0)
            AS buy_token_amount,
        coalesce(sum(token_amount) FILTER (WHERE action = 'sell'), 0.0)
            AS sell_token_amount,
        coalesce(
            sum(CASE WHEN action = 'buy' THEN quote_amount
                     WHEN action = 'sell' THEN -quote_amount ELSE 0 END),
            0.0
        ) AS signed_quote_flow,
        coalesce(
            sum(CASE WHEN action = 'buy' THEN token_amount
                     WHEN action = 'sell' THEN -token_amount ELSE 0 END),
            0.0
        ) AS signed_token_flow,
        coalesce(
                sum(quote_amount) FILTER (
                    WHERE action = 'buy'
                      AND in_last_1s
            ),
            0.0
        ) AS buy_quote_last_1s,
        coalesce(
                sum(quote_amount) FILTER (
                    WHERE action = 'sell'
                      AND in_last_1s
            ),
            0.0
        ) AS sell_quote_last_1s,
        coalesce(
                sum(quote_amount) FILTER (
                    WHERE action = 'buy'
                      AND in_last_5s
            ),
            0.0
        ) AS buy_quote_last_5s,
        coalesce(
                sum(quote_amount) FILTER (
                    WHERE action = 'sell'
                      AND in_last_5s
            ),
            0.0
        ) AS sell_quote_last_5s,
            count(signature) FILTER (
                WHERE is_creator_trade
            ) AS creator_trade_count,
            coalesce(sum(quote_amount) FILTER (
                WHERE is_creator_trade AND action = 'buy'
            ), 0.0) AS creator_buy_quote,
            coalesce(sum(quote_amount) FILTER (
                WHERE is_creator_trade AND action = 'sell'
        ), 0.0) AS creator_sell_quote,
        arg_min(price, event_order) AS first_trade_price,
        arg_max(price, event_order) AS latest_price,
        stddev_pop(ln(price)) FILTER (WHERE price > 0) AS log_price_volatility,
        min(price) FILTER (WHERE price > 0) AS minimum_price,
        max(price) FILTER (WHERE price > 0) AS maximum_price,
        coalesce(
            arg_max(pool, event_order),
            'pump'
        ) AS latest_pool,
        coalesce(
            arg_max(pool_id, event_order),
            launch_pool_id
        ) AS latest_pool_id,
            coalesce(
                arg_max(real_base, event_order),
                TRY_CAST(any_value(initial_real_base) AS DOUBLE)
        ) AS latest_real_base,
            coalesce(
                arg_max(real_quote, event_order),
                TRY_CAST(any_value(initial_real_quote) AS DOUBLE)
        ) AS latest_real_quote,
            coalesce(
                arg_max(virtual_base, event_order),
                TRY_CAST(any_value(initial_virtual_base) AS DOUBLE)
        ) AS latest_virtual_base,
            coalesce(
                arg_max(virtual_quote, event_order),
                TRY_CAST(any_value(initial_virtual_quote) AS DOUBLE)
        ) AS latest_virtual_quote,
            coalesce(
                arg_max(market_cap_quote, event_order),
                TRY_CAST(any_value(initial_market_cap_quote) AS DOUBLE)
        ) AS latest_market_cap_quote,
            coalesce(
                arg_max(pool_fee_rate, event_order),
                TRY_CAST(any_value(initial_pool_fee_rate) AS DOUBLE)
        ) AS latest_pool_fee_rate,
            coalesce(
                arg_max(real_base_text, event_order),
                any_value(initial_real_base)
        ) AS latest_real_base_text,
            coalesce(
                arg_max(real_quote_text, event_order),
                any_value(initial_real_quote)
        ) AS latest_real_quote_text,
            coalesce(
                arg_max(virtual_base_text, event_order),
                any_value(initial_virtual_base)
        ) AS latest_virtual_base_text,
            coalesce(
                arg_max(virtual_quote_text, event_order),
                any_value(initial_virtual_quote)
        ) AS latest_virtual_quote_text,
            coalesce(
                arg_max(pool_fee_rate_text, event_order),
                any_value(initial_pool_fee_rate)
        ) AS latest_pool_fee_rate_text,
            coalesce(
                arg_max(decimals_text, event_order),
                any_value(decimals)
        ) AS decimals_text
    FROM causal
    GROUP BY
        mint,
        launch_time_ms,
        launch_archive_sequence,
        launch_block,
        launch_pool_id,
        creator,
        creator_fee_address,
        mayhem_mode,
        cashback_enabled,
        token_program,
        checkpoint_kind,
        checkpoint_value,
        decision_time_ms,
        decision_archive_sequence
    """
    with tempfile.NamedTemporaryFile(
        dir=output, prefix=".causal-states.", suffix=".parquet", delete=False
    ) as handle:
        temporary = Path(handle.name)
    try:
        escaped_output = str(temporary).replace("'", "''")
        connection.execute(
            f"COPY ({query}) TO '{escaped_output}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
        )
        temporary.replace(table_path)
    finally:
        temporary.unlink(missing_ok=True)

    summary = one_row(
        connection.execute(
            f"""
        SELECT
            count(*) AS rows,
            count(DISTINCT mint) AS launches,
            count(*) FILTER (WHERE checkpoint_kind = 'time_ms') AS time_rows,
            count(*) FILTER (WHERE checkpoint_kind = 'trade_event') AS event_rows,
            count(*) FILTER (WHERE trade_count = 0) AS zero_activity_rows,
            count(*) FILTER (WHERE right_censored_300s) AS right_censored_rows
        FROM read_parquet('{str(table_path).replace("'", "''")}')
        """
        )
    )
    manifest = {
        "kind": KIND,
        "version": VERSION,
        "status": "complete",
        "source_glob": source_glob,
        "table": table_path.name,
        "maximum_source_timestamp_ms": maximum_timestamp_ms,
        "outcome_horizon_ms": outcome_horizon_ms,
        "time_checkpoints_ms": list(time_checkpoints_ms),
        "event_checkpoints": list(event_checkpoints),
        "rows": summary[0],
        "launches": summary[1],
        "time_rows": summary[2],
        "event_rows": summary[3],
        "zero_activity_rows": summary[4],
        "right_censored_rows": summary[5],
        "parquet_bytes": table_path.stat().st_size,
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest
