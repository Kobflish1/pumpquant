"""Vectorized fixed-horizon Pump action payoff materialization."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import duckdb

from pumpquant.features._sql import one_row

KIND = "pumpquant.pumpapi_action_payoffs"
VERSION = 1


def materialize_action_payoffs(
    replay_glob: str,
    causal_glob: str,
    output: Path,
    *,
    budgets_lamports: tuple[int, ...],
    horizons_ms: tuple[int, ...],
    entry_latency_ms: int,
    transaction_cost_lamports: int,
    threads: int = 10,
) -> dict[str, Any]:
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"refusing to overwrite {output}")
    if any(value <= 0 for value in (*budgets_lamports, *horizons_ms)):
        raise ValueError("budgets and horizons must be positive")
    output.mkdir(parents=True, exist_ok=True)
    parts = output / "parts"
    parts.mkdir()
    database = output / ".work.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute(f"PRAGMA threads={threads}")
    connection.execute("PRAGMA preserve_insertion_order=false")
    replay = replay_glob.replace("'", "''")
    causal = causal_glob.replace("'", "''")
    started = time.monotonic()

    connection.execute(
        f"""
        CREATE TABLE curve_states AS
        SELECT
          mint,
          timestamp_ms,
          archive_sequence,
          cast(round(cast(vTokensInBondingCurve AS DECIMAL(38, 9)) * 1000000)
               AS BIGINT) AS virtual_base,
          cast(round(cast(vQuoteInBondingCurve AS DECIMAL(38, 12)) * 1000000000)
               AS BIGINT) AS virtual_quote,
          cast(round(cast(tokensInPool AS DECIMAL(38, 9)) * 1000000)
               AS BIGINT) AS real_base,
          cast(round(cast(quoteInPool AS DECIMAL(38, 12)) * 1000000000)
               AS BIGINT) AS real_quote,
          cast(round(cast(poolFeeRate AS DOUBLE) * 10000) AS BIGINT) AS fee_bps
        FROM read_parquet('{replay}')
        WHERE pool = 'pump'
          AND action IN ('create', 'buy', 'sell')
          AND vTokensInBondingCurve IS NOT NULL
          AND vQuoteInBondingCurve IS NOT NULL
        """
    )
    connection.execute(
        f"""
        CREATE TABLE decisions AS
        SELECT
          mint,
          checkpoint_kind,
          checkpoint_value,
          decision_time_ms,
          decision_archive_sequence,
          decision_time_ms + {entry_latency_ms} AS entry_time_ms
        FROM read_parquet('{causal}')
        WHERE latest_pool = 'pump'
          AND NOT right_censored_300s
        """
    )
    connection.execute(
        """
        CREATE TABLE entries AS
        SELECT
          d.*,
          e.virtual_base AS entry_virtual_base,
          e.virtual_quote AS entry_virtual_quote,
          e.real_base AS entry_real_base,
          e.real_quote AS entry_real_quote,
          e.fee_bps
        FROM decisions d
        ASOF LEFT JOIN curve_states e
          ON d.mint = e.mint
         AND d.entry_time_ms > e.timestamp_ms
        """
    )
    eligible = one_row(
        connection.execute(
            """
        SELECT count(*) FROM entries
        WHERE entry_virtual_base > 0
          AND entry_virtual_quote > 0
          AND entry_real_base > 0
          AND fee_bps >= 0
        """
        )
    )[0]
    total_rows = eligible * len(budgets_lamports) * len(horizons_ms)
    completed = 0
    records: list[dict[str, Any]] = []
    budget_values = ", ".join(f"({value})" for value in budgets_lamports)

    for index, horizon in enumerate(horizons_ms, start=1):
        path = parts / f"{index:02d}-horizon-{horizon}ms.parquet"
        query = f"""
        WITH paired AS (
          SELECT
            e.*,
            x.virtual_base AS exit_virtual_base,
            x.virtual_quote AS exit_virtual_quote,
            x.real_quote AS exit_real_quote
          FROM entries e
          ASOF LEFT JOIN curve_states x
            ON e.mint = x.mint
           AND e.entry_time_ms + {horizon} > x.timestamp_ms
          WHERE e.entry_virtual_base > 0
            AND e.entry_virtual_quote > 0
            AND e.entry_real_base > 0
            AND e.fee_bps >= 0
        ),
        actions AS (
          SELECT p.*, b.budget::BIGINT AS budget_lamports
          FROM paired p
          CROSS JOIN (VALUES {budget_values}) b(budget)
        ),
        raw_buy AS (
          SELECT
            *,
            ((budget_lamports - 1)::HUGEINT * 10000)
              // (10000 + fee_bps) AS budget_curve_quote
          FROM actions
        ),
        sized AS (
          SELECT
            *,
            least(
              entry_real_base::HUGEINT,
              (budget_curve_quote * entry_virtual_base)
                // (entry_virtual_quote + budget_curve_quote)
            ) AS base_amount
          FROM raw_buy
        ),
        bought AS (
          SELECT
            *,
            CASE
              WHEN base_amount < entry_real_base
                THEN budget_curve_quote
              ELSE (base_amount * entry_virtual_quote)
                // (entry_virtual_base - base_amount) + 1
            END AS entry_curve_quote
          FROM sized
        ),
        entry_costed AS (
          SELECT
            *,
            entry_curve_quote
              + (entry_curve_quote * fee_bps + 9999) // 10000
              + 1 AS entry_spend_lamports
          FROM bought
        ),
        sold AS (
          SELECT
            *,
            CASE
              WHEN base_amount <= 0 OR exit_virtual_base IS NULL THEN NULL
              ELSE
                (base_amount * (exit_virtual_quote + entry_curve_quote))
                  // exit_virtual_base
            END AS exit_curve_quote,
            CASE
              WHEN exit_real_quote IS NULL THEN NULL
              ELSE exit_real_quote + entry_curve_quote
            END AS counterfactual_real_quote
          FROM entry_costed
        ),
        credited AS (
          SELECT
            *,
            CASE
              WHEN exit_curve_quote IS NULL
                OR exit_curve_quote > counterfactual_real_quote
                THEN NULL
              ELSE greatest(
                0,
                exit_curve_quote
                  - (exit_curve_quote * fee_bps + 9999) // 10000
                  - 1
              )
            END AS exit_credit_lamports
          FROM sold
        )
        SELECT
          mint,
          checkpoint_kind,
          checkpoint_value,
          decision_time_ms,
          entry_time_ms,
          {horizon}::BIGINT AS horizon_ms,
          budget_lamports,
          base_amount::BIGINT AS base_amount_atomic,
          entry_spend_lamports::BIGINT AS entry_spend_lamports,
          exit_credit_lamports::BIGINT AS exit_credit_lamports,
          {transaction_cost_lamports}::BIGINT AS transaction_cost_lamports,
          CASE WHEN exit_credit_lamports IS NULL THEN NULL
               ELSE exit_credit_lamports - entry_spend_lamports
                    - {transaction_cost_lamports} END AS net_pnl_lamports,
          CASE WHEN exit_credit_lamports IS NULL THEN NULL
               ELSE (
                 exit_credit_lamports - entry_spend_lamports
                   - {transaction_cost_lamports}
               )::DOUBLE / budget_lamports END AS net_return,
          exit_credit_lamports IS NOT NULL AS executable
        FROM credited
        """
        escaped_path = str(path).replace("'", "''")
        connection.execute(
            f"COPY ({query}) TO '{escaped_path}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
        )
        rows = one_row(connection.execute(f"SELECT count(*) FROM read_parquet('{escaped_path}')"))[
            0
        ]
        expected = eligible * len(budgets_lamports)
        if rows != expected:
            raise RuntimeError(f"horizon {horizon}: expected {expected}, got {rows}")
        completed += rows
        elapsed = time.monotonic() - started
        rate = completed / elapsed
        eta = (total_rows - completed) / rate if rate else None
        record = {"path": str(path.relative_to(output)), "horizon_ms": horizon, "rows": rows}
        records.append(record)
        print(
            json.dumps(
                {
                    "completed_rows": completed,
                    "total_rows": total_rows,
                    "percent": 100 * completed / total_rows,
                    "rows_per_second": rate,
                    "eta_seconds": eta,
                },
                sort_keys=True,
            ),
            flush=True,
        )
    elapsed = time.monotonic() - started
    manifest = {
        "kind": KIND,
        "version": VERSION,
        "status": "complete",
        "replay_glob": replay_glob,
        "causal_glob": causal_glob,
        "eligible_state_rows": eligible,
        "budgets_lamports": list(budgets_lamports),
        "horizons_ms": list(horizons_ms),
        "entry_latency_ms": entry_latency_ms,
        "transaction_cost_lamports": transaction_cost_lamports,
        "rows": total_rows,
        "elapsed_seconds": elapsed,
        "parts": records,
        "limitations": [
            "Development baseline uses one observed median entry-latency value.",
            "Own trade reserve impact is persisted additively while external landed "
            "reserve deltas are held fixed.",
            "Total Pump fee rate is conservatively rounded as two fee components.",
            "This fixed-horizon tensor is discovery evidence, not live authorization.",
        ],
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    connection.close()
    database.unlink(missing_ok=True)
    Path(str(database) + ".wal").unlink(missing_ok=True)
    return manifest
