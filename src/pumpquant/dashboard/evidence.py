"""Read-only token/trade evidence store used by the research dashboard.

The aggregate research ledger is deliberately small.  This companion SQLite
artifact keeps the much larger drill-down surface out of progress heartbeats
while preserving an immutable, indexed path from a candidate to every OOS
trade and from a trade to the token's real decoded Pump/PumpSwap price bars.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import quote

from pumpquant.research.event_screen import EventRuleOperator
from pumpquant.research.tail_exit import TailExitCandidate

TOKEN_EVIDENCE_KIND = "pumpquant.tail_exit_token_evidence"
TOKEN_EVIDENCE_VERSION = 2
TOKEN_EVIDENCE_DATABASE = "token-evidence.sqlite3"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class TokenEvidenceError(RuntimeError):
    """The token evidence artifact or query contract is invalid."""


def _candidate_id(value: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise TokenEvidenceError("candidate_sha256 must be 64 lowercase hex characters")
    return value


def _positive_int(value: int, name: str, *, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > maximum:
        raise TokenEvidenceError(f"{name} must be between 0 and {maximum}")
    return value


def initialize_token_evidence_database(path: Path) -> sqlite3.Connection:
    """Create a new evidence database with the frozen v2 schema."""

    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to overwrite token evidence database {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        PRAGMA journal_mode = DELETE;
        PRAGMA synchronous = FULL;
        PRAGMA foreign_keys = ON;
        CREATE TABLE metadata (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE candidates (
            candidate_sha256 TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            family TEXT NOT NULL,
            profile_id TEXT NOT NULL,
            feature TEXT NOT NULL,
            operator TEXT NOT NULL,
            quantile REAL NOT NULL,
            source_url TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE trades (
            trade_id INTEGER PRIMARY KEY,
            candidate_sha256 TEXT NOT NULL REFERENCES candidates(candidate_sha256),
            fold INTEGER NOT NULL,
            mint TEXT NOT NULL,
            market_id TEXT NOT NULL,
            signal_event_id TEXT NOT NULL,
            signal_time_ns INTEGER NOT NULL,
            signal_price REAL,
            entry_event_id TEXT NOT NULL,
            entry_time_ns INTEGER NOT NULL,
            entry_mark_price REAL,
            entry_price REAL NOT NULL,
            exit_event_id TEXT NOT NULL,
            exit_time_ns INTEGER NOT NULL,
            exit_mark_price REAL,
            exit_price REAL NOT NULL,
            exit_reason TEXT NOT NULL,
            holding_events INTEGER NOT NULL,
            net_pnl_quote REAL NOT NULL,
            quote_in REAL NOT NULL,
            quote_out REAL NOT NULL,
            fixed_cost_quote REAL NOT NULL,
            entry_fee_rate REAL,
            exit_fee_rate REAL
        );
        CREATE INDEX trades_candidate_time
            ON trades(candidate_sha256, signal_time_ns, trade_id);
        CREATE INDEX trades_candidate_mint
            ON trades(candidate_sha256, mint, signal_time_ns, trade_id);
        CREATE TABLE bars (
            mint TEXT NOT NULL,
            time_seconds INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            quote_volume REAL NOT NULL,
            event_count INTEGER NOT NULL,
            PRIMARY KEY (mint, time_seconds)
        ) WITHOUT ROWID;
        """
    )
    return connection


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def candidate_trade_rows_sql(
    candidates: tuple[TailExitCandidate, ...],
    holding_events: int,
    *,
    labels_table: str = "event_labels",
    thresholds_table: str = "evidence_thresholds",
) -> str:
    """Select every causally eligible OOS trade for registered candidates."""

    if not candidates:
        raise ValueError("candidate trade query requires at least one candidate")
    if any(item.signal.holding_events != holding_events for item in candidates):
        raise ValueError("candidate holding horizons do not match the label table")
    parts: list[str] = []
    for candidate in candidates:
        feature = candidate.signal.axis.feature
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", feature):
            raise ValueError("candidate feature is not a safe SQL identifier")
        operator = (
            ">=" if candidate.signal.axis.operator is EventRuleOperator.GREATER_EQUAL else "<="
        )
        uncensored = (
            "AND NOT labels.first_seen_censored"
            if candidate.signal.axis.requires_uncensored_wallet_history
            else ""
        )
        parts.append(
            f"""
SELECT
    {_sql_string(candidate.sha256)} AS candidate_sha256,
    memberships.fold,
    labels.mint,
    labels.market_id,
    CAST(labels.event_id AS VARCHAR) AS signal_event_id,
    labels.block_time_ns AS signal_time_ns,
    labels.signal_mark_price AS signal_price,
    CAST(labels.entry_event_id AS VARCHAR) AS entry_event_id,
    labels.entry_block_time_ns AS entry_time_ns,
    labels.entry_mark_price,
    labels.entry_average_price AS entry_price,
    CAST(labels.exit_event_id AS VARCHAR) AS exit_event_id,
    labels.exit_block_time_ns AS exit_time_ns,
    labels.exit_mark_price,
    labels.exit_average_price AS exit_price,
    labels.exit_reason,
    labels.holding_events,
    labels.pnl_{holding_events} AS net_pnl_quote,
    labels.quote_out,
    labels.entry_fee_rate,
    labels.exit_fee_rate
FROM {labels_table} AS labels
INNER JOIN memberships AS memberships
    ON labels.mint = memberships.mint AND memberships.split = 'test'
INNER JOIN {thresholds_table} AS thresholds
    ON thresholds.candidate_sha256 = {_sql_string(candidate.sha256)}
   AND thresholds.fold = memberships.fold
WHERE labels.eligible_{holding_events}
  AND labels.label_end_{holding_events}_available_at_ns < memberships.cutoff_ns
  AND thresholds.fitted_threshold IS NOT NULL
  AND labels.{feature} {operator} thresholds.fitted_threshold
  {uncensored}
""".strip()
        )
    return "\nUNION ALL\n".join(parts)


class TokenEvidenceStore:
    """Bounded parameterized queries over one immutable SQLite artifact."""

    def __init__(self, path: Path) -> None:
        resolved = path.resolve(strict=True)
        if path.is_symlink() or not resolved.is_file():
            raise TokenEvidenceError("token evidence database is missing or unsafe")
        self.path = resolved

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{quote(str(self.path))}?mode=ro&immutable=1"
        connection = sqlite3.connect(uri, uri=True, timeout=3.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        return connection

    def summary(self) -> dict[str, Any]:
        with self._connect() as connection:
            candidate_count = int(
                connection.execute("SELECT count(*) FROM candidates").fetchone()[0]
            )
            trade_count = int(connection.execute("SELECT count(*) FROM trades").fetchone()[0])
            token_count = int(
                connection.execute("SELECT count(DISTINCT mint) FROM trades").fetchone()[0]
            )
            bar_count = int(connection.execute("SELECT count(*) FROM bars").fetchone()[0])
        return {
            "kind": TOKEN_EVIDENCE_KIND,
            "version": TOKEN_EVIDENCE_VERSION,
            "candidate_count": candidate_count,
            "trade_count": trade_count,
            "token_count": token_count,
            "bar_count": bar_count,
        }

    def list_trades(
        self,
        candidate_sha256: str,
        *,
        limit: int = 5_000,
        offset: int = 0,
    ) -> dict[str, Any]:
        candidate_id = _candidate_id(candidate_sha256)
        selected_limit = _positive_int(limit, "limit", maximum=10_000)
        selected_offset = _positive_int(offset, "offset", maximum=10_000_000)
        with self._connect() as connection:
            candidate = connection.execute(
                "SELECT * FROM candidates WHERE candidate_sha256 = ?",
                (candidate_id,),
            ).fetchone()
            if candidate is None:
                raise TokenEvidenceError("candidate is not present in token evidence")
            total = int(
                connection.execute(
                    "SELECT count(*) FROM trades WHERE candidate_sha256 = ?",
                    (candidate_id,),
                ).fetchone()[0]
            )
            rows = connection.execute(
                """
                SELECT trade_id, fold, mint, market_id, signal_time_ns,
                       entry_time_ns, entry_price, exit_time_ns, exit_price,
                       exit_reason, holding_events, net_pnl_quote, quote_in,
                       quote_out, fixed_cost_quote
                FROM trades
                WHERE candidate_sha256 = ?
                ORDER BY signal_time_ns, trade_id
                LIMIT ? OFFSET ?
                """,
                (candidate_id, selected_limit, selected_offset),
            ).fetchall()
        return {
            "candidate": dict(candidate),
            "total": total,
            "limit": selected_limit,
            "offset": selected_offset,
            "trades": [dict(row) for row in rows],
        }

    def trade_chart(self, candidate_sha256: str, trade_id: int) -> dict[str, Any]:
        candidate_id = _candidate_id(candidate_sha256)
        selected_trade_id = _positive_int(trade_id, "trade_id", maximum=2**63 - 1)
        with self._connect() as connection:
            selected = connection.execute(
                "SELECT * FROM trades WHERE candidate_sha256 = ? AND trade_id = ?",
                (candidate_id, selected_trade_id),
            ).fetchone()
            if selected is None:
                raise TokenEvidenceError("trade is not present for this candidate")
            mint = str(selected["mint"])
            trades = connection.execute(
                """
                SELECT * FROM trades
                WHERE candidate_sha256 = ? AND mint = ?
                ORDER BY signal_time_ns, trade_id
                """,
                (candidate_id, mint),
            ).fetchall()
            bar_columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(bars)").fetchall()
            }
            volume = "quote_volume" if "quote_volume" in bar_columns else "NULL AS quote_volume"
            bars = connection.execute(
                f"""
                SELECT time_seconds AS time, open, high, low, close, {volume}, event_count
                FROM bars WHERE mint = ? ORDER BY time_seconds
                """,
                (mint,),
            ).fetchall()
        return {
            "candidate_sha256": candidate_id,
            "selected_trade_id": selected_trade_id,
            "mint": mint,
            "pump_fun_url": f"https://pump.fun/coin/{quote(mint, safe='')}",
            "bars": [dict(row) for row in bars],
            "trades": [dict(row) for row in trades],
        }


__all__ = [
    "TOKEN_EVIDENCE_DATABASE",
    "TOKEN_EVIDENCE_KIND",
    "TOKEN_EVIDENCE_VERSION",
    "TokenEvidenceError",
    "TokenEvidenceStore",
    "candidate_trade_rows_sql",
    "initialize_token_evidence_database",
]
