"""Post-stair execution audit helpers for the frozen live M4 strategy."""

from __future__ import annotations

import math
import statistics
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from pumpquant.pricing.pumpswap import (
    PumpSwapFees,
    PumpSwapQuote,
    pumpswap_buy_exact_base,
    pumpswap_buy_exact_quote_in,
    pumpswap_sell_exact_base,
)

# Telemetry is split into attempts that bid exactly this priority-fee floor and those above it.
FLOOR_BID_MICRO_LAMPORTS = 50_000


def higher_quantile(values: Sequence[float | int], probability: float) -> float:
    if not values:
        raise ValueError("quantile requires at least one value")
    if not 0 < probability <= 1:
        raise ValueError("probability must be in (0, 1]")
    ordered = sorted(float(value) for value in values)
    return ordered[math.ceil(probability * len(ordered)) - 1]


def distribution(values: Iterable[float | int]) -> dict[str, float | int | None]:
    observed = [float(value) for value in values]
    if not observed:
        return {
            "count": 0,
            "minimum": None,
            "median": None,
            "mean": None,
            "p90": None,
            "p95": None,
            "p99": None,
            "maximum": None,
        }
    return {
        "count": len(observed),
        "minimum": min(observed),
        "median": statistics.median(observed),
        "mean": statistics.fmean(observed),
        "p90": higher_quantile(observed, 0.90),
        "p95": higher_quantile(observed, 0.95),
        "p99": higher_quantile(observed, 0.99),
        "maximum": max(observed),
    }


def _events(rows: Iterable[Mapping[str, Any]], event: str) -> list[Mapping[str, Any]]:
    return [row for row in rows if row.get("event") == event]


def extract_fee_skip_cases(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Bind each fee-context skip to its frozen sizing inputs."""

    cases: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if (
            row.get("event") != "entry_skipped"
            or row.get("reason") != "fee_context_outside_calibration"
        ):
            continue
        decision = next(
            (
                candidate
                for candidate in reversed(rows[:index])
                if candidate.get("event") == "decision"
                and candidate.get("pool") == row.get("pool")
                and candidate.get("action") == "enter"
            ),
            None,
        )
        prior_close = next(
            (
                candidate
                for candidate in reversed(rows[:index])
                if candidate.get("event") == "position_closed"
                and isinstance(candidate.get("ending_balance_lamports"), int)
            ),
            None,
        )
        prior_open = next(
            (
                candidate
                for candidate in reversed(rows[:index])
                if candidate.get("event") == "position_opened"
                and isinstance(candidate.get("sol_usd_price"), (int, float))
            ),
            None,
        )
        if decision is None or prior_close is None or prior_open is None:
            raise ValueError("fee skip cannot be bound to decision and sizing state")
        balance = int(prior_close["ending_balance_lamports"])
        sol_usd = float(prior_open["sol_usd_price"])
        quote_lamports = min(
            math.floor(balance * 0.11),
            math.floor((0.38 / sol_usd) * 1_000_000_000),
        )
        localized = int(row["localized_p75_micro_lamports"])
        cases.append(
            {
                "pool": str(row["pool"]),
                "mint": str(row["mint"]),
                "decision_time_ns": int(decision["decision_time_ns"]),
                "decision_wall_time_ns": int(decision["wall_time_ns"]),
                "source_slot": int(decision["source_slot"]),
                "balance_lamports": balance,
                "sol_usd_price": sol_usd,
                "quote_lamports": quote_lamports,
                "localized_p75_micro_lamports": localized,
                "maximum_supported_micro_lamports": int(row["maximum_supported_micro_lamports"]),
                "selected_if_submitted_micro_lamports": max(50_000, localized),
                "estimated_entry_fee_if_submitted_lamports": 5_000
                + math.ceil(max(50_000, localized) * 150_000 / 1_000_000),
            }
        )
    return cases


def entry_retry_schedules(
    rows: Sequence[Mapping[str, Any]],
    chain_transactions: Mapping[str, Mapping[str, Any]],
    *,
    maximum_attempts: int = 8,
) -> list[list[int]]:
    """Return empirical entry landing-slot schedules relative to decision state."""

    decisions = {
        str(row["pool"]): row
        for row in rows
        if row.get("event") == "decision" and row.get("action") == "enter"
    }
    attempts: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("event") == "transaction_ready" and row.get("side") == "entry":
            attempts[str(row["pool"])].append(row)

    raw_schedules: list[list[int]] = []
    retry_gaps: list[int] = []
    for pool, pool_attempts in attempts.items():
        decision = decisions.get(pool)
        if decision is None:
            continue
        offsets = []
        for attempt in sorted(pool_attempts, key=lambda item: int(item["wall_time_ns"])):
            transaction = chain_transactions.get(str(attempt["signature"]))
            if not transaction or not isinstance(transaction.get("slot"), int):
                continue
            offsets.append(int(transaction["slot"]) - int(decision["source_slot"]))
        offsets = [offset for offset in offsets if offset >= 0]
        if not offsets:
            continue
        raw_schedules.append(offsets)
        retry_gaps.extend(
            right - left for left, right in zip(offsets, offsets[1:], strict=False) if right > left
        )

    if not raw_schedules:
        raise ValueError("no signature-linked entry schedules are available")
    extension_gap = max(1, round(statistics.median(retry_gaps or [3])))
    schedules: list[list[int]] = []
    for raw in raw_schedules:
        schedule = list(raw)
        while len(schedule) < maximum_attempts:
            schedule.append(schedule[-1] + extension_gap)
        schedules.append(schedule[:maximum_attempts])
    return schedules


def _fees(event: Mapping[str, Any]) -> PumpSwapFees:
    return PumpSwapFees(
        lp_fee_bps=int(event["lp_fee_bps"]),
        protocol_fee_bps=int(event["protocol_fee_bps"]),
        creator_fee_bps=int(event["creator_fee_bps"]),
    )


def _buy_quote(
    *, gross_quote: int, base_reserve: int, quote_reserve: int, event: Mapping[str, Any]
) -> PumpSwapQuote:
    return pumpswap_buy_exact_quote_in(
        gross_quote_amount=gross_quote,
        base_reserve=base_reserve,
        quote_reserve=quote_reserve + int(event["virtual_quote_reserves"]),
        fees=_fees(event),
    )


def _buy_base(
    *, base_amount: int, base_reserve: int, quote_reserve: int, event: Mapping[str, Any]
) -> PumpSwapQuote:
    return pumpswap_buy_exact_base(
        base_amount=base_amount,
        base_reserve=base_reserve,
        quote_reserve=quote_reserve + int(event["virtual_quote_reserves"]),
        fees=_fees(event),
    )


def _sell_base(
    *, base_amount: int, base_reserve: int, quote_reserve: int, event: Mapping[str, Any]
) -> PumpSwapQuote:
    quote = pumpswap_sell_exact_base(
        base_amount=base_amount,
        base_reserve=base_reserve,
        quote_reserve=quote_reserve + int(event["virtual_quote_reserves"]),
        fees=_fees(event),
    )
    if quote.curve_quote_amount - quote.lp_fee_quote > quote_reserve:
        raise ValueError("counterfactual sell exceeds real quote reserves")
    return quote


def _state_at(path: Sequence[Mapping[str, Any]], slot: int) -> Mapping[str, Any] | None:
    candidates = [event for event in path if int(event["slot"]) <= slot]
    return candidates[-1] if candidates else None


def _apply_external_order(
    *,
    base_reserve: int,
    quote_reserve: int,
    event: Mapping[str, Any],
) -> tuple[int, int, bool]:
    """Replay one observed order against counterfactual reserves."""

    if event["event_name"] == "BuyEvent":
        base_amount = int(event["base_amount"])
        if base_amount >= base_reserve:
            return base_reserve, quote_reserve, False
        quote = _buy_base(
            base_amount=base_amount,
            base_reserve=base_reserve,
            quote_reserve=quote_reserve,
            event=event,
        )
        if quote.user_quote_amount > int(event["slippage_limit_quote"]):
            return base_reserve, quote_reserve, False
        return (
            base_reserve - base_amount,
            quote_reserve + quote.curve_quote_amount + quote.lp_fee_quote,
            True,
        )

    if event["event_name"] == "SellEvent":
        base_amount = int(event["base_amount"])
        try:
            quote = _sell_base(
                base_amount=base_amount,
                base_reserve=base_reserve,
                quote_reserve=quote_reserve,
                event=event,
            )
        except ValueError:
            return base_reserve, quote_reserve, False
        if quote.user_quote_amount < int(event["slippage_limit_quote"]):
            return base_reserve, quote_reserve, False
        return (
            base_reserve + base_amount,
            quote_reserve - (quote.curve_quote_amount - quote.lp_fee_quote),
            True,
        )
    raise ValueError("unsupported external order")


def replay_fee_skip_schedule(
    case: Mapping[str, Any],
    path: Sequence[Mapping[str, Any]],
    landing_offsets: Sequence[int],
    *,
    quote_to_landing_slots: int = 2,
) -> dict[str, Any]:
    """Causally replay one skipped entry under one empirical retry schedule.

    Subsequent users retain their observed order sizes and slippage limits, but
    each order is re-quoted against the counterfactual reserves. This avoids
    assuming that inserting our trade leaves the historical reserve path intact.
    """

    if not path:
        raise ValueError("fee-skip path is empty")
    source_slot = int(case["source_slot"])
    quote_lamports = int(case["quote_lamports"])
    maximum_quote = math.floor(quote_lamports * 1.05)
    entry: dict[str, Any] | None = None
    attempts: list[dict[str, Any]] = []

    for attempt_number, landing_offset in enumerate(landing_offsets, 1):
        landing_slot = source_slot + int(landing_offset)
        build_slot = max(source_slot, landing_slot - quote_to_landing_slots)
        build = _state_at(path, build_slot)
        landing = _state_at(path, landing_slot)
        if build is None or landing is None:
            attempts.append(
                {
                    "attempt": attempt_number,
                    "build_slot": build_slot,
                    "landing_slot": landing_slot,
                    "status": "state_unavailable",
                }
            )
            continue
        built = _buy_quote(
            gross_quote=quote_lamports,
            base_reserve=int(build["post_base_reserves"]),
            quote_reserve=int(build["post_quote_reserves"]),
            event=build,
        )
        required = _buy_base(
            base_amount=built.base_amount,
            base_reserve=int(landing["post_base_reserves"]),
            quote_reserve=int(landing["post_quote_reserves"]),
            event=landing,
        )
        status = "filled" if required.user_quote_amount <= maximum_quote else "exceeded_slippage"
        attempts.append(
            {
                "attempt": attempt_number,
                "build_slot": int(build["slot"]),
                "landing_slot": int(landing["slot"]),
                "base_amount": built.base_amount,
                "required_quote_lamports": required.user_quote_amount,
                "maximum_quote_lamports": maximum_quote,
                "status": status,
            }
        )
        if status == "filled":
            entry = {
                "event": landing,
                "base_amount": built.base_amount,
                "entry_quote_lamports": required.user_quote_amount,
                "base_reserves": int(landing["post_base_reserves"]) - built.base_amount,
                "quote_reserves": int(landing["post_quote_reserves"])
                + required.curve_quote_amount
                + required.lp_fee_quote,
            }
            break

    entry_fee = int(case["estimated_entry_fee_if_submitted_lamports"])
    if entry is None:
        return {
            "status": "no_fill_after_maximum_attempts",
            "attempts": attempts,
            "submitted_attempts": len(attempts),
            "failed_entry_fees_lamports": len(attempts) * entry_fee,
        }

    base_reserve = int(entry["base_reserves"])
    quote_reserve = int(entry["quote_reserves"])
    base_amount = int(entry["base_amount"])
    landing_event = entry["event"]
    external_executed = 0
    external_rejected = 0
    last_quote: int | None = None
    last_event: Mapping[str, Any] = landing_event
    exit_reason = "timeout"
    decision_seconds = int(case["decision_time_ns"]) // 1_000_000_000

    for event in path:
        if (int(event["slot"]), int(event["order"])) <= (
            int(landing_event["slot"]),
            int(landing_event["order"]),
        ):
            continue
        if int(event["block_time"]) > decision_seconds + 300:
            break
        base_reserve, quote_reserve, executed = _apply_external_order(
            base_reserve=base_reserve,
            quote_reserve=quote_reserve,
            event=event,
        )
        if executed:
            external_executed += 1
        else:
            external_rejected += 1
        try:
            last_quote = _sell_base(
                base_amount=base_amount,
                base_reserve=base_reserve,
                quote_reserve=quote_reserve,
                event=event,
            ).user_quote_amount
        except ValueError:
            last_quote = 0
        last_event = event
        multiple = last_quote / quote_lamports
        if multiple >= 1.75:
            exit_reason = "take_profit"
            break
        if multiple <= 0.20:
            exit_reason = "stop_loss"
            break

    if last_quote is None:
        last_quote = _sell_base(
            base_amount=base_amount,
            base_reserve=base_reserve,
            quote_reserve=quote_reserve,
            event=landing_event,
        ).user_quote_amount
    return {
        "status": "filled",
        "attempts": attempts,
        "entry_attempt": len(attempts),
        "entry_quote_lamports": int(entry["entry_quote_lamports"]),
        "base_amount": base_amount,
        "exit_reason": exit_reason,
        "trigger_quote_lamports": last_quote,
        "return_multiple_on_frozen_budget": last_quote / quote_lamports,
        "trigger_slot": int(last_event["slot"]),
        "trigger_seconds_after_decision": int(last_event["block_time"]) - decision_seconds,
        "external_orders_executed": external_executed,
        "external_orders_rejected_by_counterfactual": external_rejected,
        "estimated_round_trip_pnl_lamports": last_quote
        - int(entry["entry_quote_lamports"])
        - len(attempts) * entry_fee
        - 10_500,
    }


def summarize_fee_skip_replays(
    case: Mapping[str, Any],
    path: Sequence[Mapping[str, Any]],
    schedules: Sequence[Sequence[int]],
) -> dict[str, Any]:
    replays = [replay_fee_skip_schedule(case, path, schedule) for schedule in schedules]
    filled = [replay for replay in replays if replay["status"] == "filled"]
    return {
        "pool": case["pool"],
        "mint": case["mint"],
        "case": dict(case),
        "schedule_count": len(replays),
        "fill_count": len(filled),
        "fill_probability_over_empirical_schedules": (
            len(filled) / len(replays) if replays else None
        ),
        "outcomes": dict(
            Counter(replay.get("exit_reason", replay["status"]) for replay in replays)
        ),
        "estimated_pnl_lamports": distribution(
            replay["estimated_round_trip_pnl_lamports"] for replay in filled
        ),
        "return_multiple": distribution(
            replay["return_multiple_on_frozen_budget"] for replay in filled
        ),
        "entry_attempt": distribution(replay["entry_attempt"] for replay in filled),
        "replays": replays,
    }


def summarize_live_execution(
    rows: Sequence[Mapping[str, Any]],
    chain_transactions: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    ready = _events(rows, "transaction_ready")
    ledger_landed = {str(row["signature"]): row for row in _events(rows, "transaction_landed")}
    by_side: dict[str, Any] = {}
    for side in ("entry", "exit"):
        attempts = [row for row in ready if row.get("side") == side]
        chain_found = [
            (row, transaction)
            for row in attempts
            if (transaction := chain_transactions.get(str(row["signature"]))) is not None
        ]
        success = [
            (row, transaction) for row, transaction in chain_found if transaction.get("err") is None
        ]
        failed = [
            (row, transaction)
            for row, transaction in chain_found
            if transaction.get("err") is not None
        ]
        by_side[side] = {
            "submitted": len(attempts),
            "chain_found": len(chain_found),
            "successful": len(success),
            "failed": len(failed),
            "missing": len(attempts) - len(chain_found),
            "successful_fill_probability": len(success) / len(attempts) if attempts else None,
            "retry_events": len([row for row in rows if row.get("event") == f"{side}_retry"]),
            "retry_rate_per_submitted_attempt": len(
                [row for row in rows if row.get("event") == f"{side}_retry"]
            )
            / len(attempts)
            if attempts
            else None,
            "localized_p75_micro_lamports": distribution(
                int(row["localized_p75_micro_lamports"]) for row in attempts
            ),
            "selected_micro_lamports": distribution(
                int(row["selected_micro_lamports"]) for row in attempts
            ),
            "estimated_total_fee_lamports": distribution(
                int(row["estimated_total_fee_lamports"]) for row in attempts
            ),
            "actual_fee_lamports": distribution(
                int(transaction["fee_lamports"]) for _, transaction in chain_found
            ),
            "failed_attempt_fee_lamports": {
                **distribution(int(transaction["fee_lamports"]) for _, transaction in failed),
                "total": sum(int(transaction["fee_lamports"]) for _, transaction in failed),
            },
            "successful_attempt_fee_lamports": {
                **distribution(int(transaction["fee_lamports"]) for _, transaction in success),
                "total": sum(int(transaction["fee_lamports"]) for _, transaction in success),
            },
            "send_to_confirmed_ms": distribution(
                float(ledger_landed[str(row["signature"])]["send_to_confirmed_ms"])
                for row, _ in success
                if str(row["signature"]) in ledger_landed
            ),
            "compute_units_consumed": distribution(
                int(transaction["compute_units_consumed"])
                for _, transaction in chain_found
                if isinstance(transaction.get("compute_units_consumed"), int)
            ),
        }

    closes = _events(rows, "position_closed")
    opens = {str(row["pool"]): row for row in _events(rows, "position_opened")}
    close_by_pool = {str(row["pool"]): row for row in closes}
    attempts_by_pool: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in ready:
        attempts_by_pool[str(row["pool"])].append(row)

    trade_economics = []
    for pool, opened in opens.items():
        close = close_by_pool.get(pool)
        entry_transaction = chain_transactions.get(str(opened.get("entry_signature")))
        exit_transaction = (
            chain_transactions.get(str(close.get("exit_signature"))) if close is not None else None
        )
        if close is None or entry_transaction is None or exit_transaction is None:
            continue
        buys = [
            event
            for event in entry_transaction.get("trade_events", [])
            if event.get("event_name") == "BuyEvent" and event.get("pool") == pool
        ]
        sells = [
            event
            for event in exit_transaction.get("trade_events", [])
            if event.get("event_name") == "SellEvent" and event.get("pool") == pool
        ]
        if not buys or not sells:
            continue
        pool_attempts = attempts_by_pool[pool]
        total_fees = sum(
            int(chain_transactions[str(row["signature"])]["fee_lamports"])
            for row in pool_attempts
            if str(row["signature"]) in chain_transactions
        )
        failed_fees = sum(
            int(chain_transactions[str(row["signature"])]["fee_lamports"])
            for row in pool_attempts
            if str(row["signature"]) in chain_transactions
            and chain_transactions[str(row["signature"])].get("err") is not None
        )
        entry_quote = int(buys[-1]["user_quote_amount"])
        exit_quote = int(sells[-1]["user_quote_amount"])
        balance_deltas = [
            int(chain_transactions[str(row["signature"])]["fee_payer_balance_delta_lamports"])
            for row in pool_attempts
            if str(row["signature"]) in chain_transactions
            and isinstance(
                chain_transactions[str(row["signature"])].get("fee_payer_balance_delta_lamports"),
                int,
            )
        ]
        exact_pnl = sum(balance_deltas) if balance_deltas else None
        trade_economics.append(
            {
                "pool": pool,
                "reason": close["reason"],
                "entry_attempt": int(opened.get("entry_attempt", 1)),
                "exit_attempt": int(close.get("exit_attempt", 1)),
                "entry_quote_lamports": entry_quote,
                "exit_quote_lamports": exit_quote,
                "total_transaction_fees_lamports": total_fees,
                "failed_attempt_fees_lamports": failed_fees,
                "event_quote_pnl_after_transaction_fees_lamports": exit_quote
                - entry_quote
                - total_fees,
                "economic_pnl_lamports": exact_pnl,
            }
        )

    def economics_group(field: str, value: int) -> dict[str, Any]:
        records = [
            row
            for row in trade_economics
            if row[field] == value and isinstance(row["economic_pnl_lamports"], int)
        ]
        return {
            "count": len(records),
            "win_count": sum(row["economic_pnl_lamports"] > 0 for row in records),
            "take_profit_count": sum(row["reason"] == "take_profit" for row in records),
            "economic_pnl_lamports": {
                **distribution(row["economic_pnl_lamports"] for row in records),
                "total": sum(row["economic_pnl_lamports"] for row in records),
            },
            "failed_attempt_fees_lamports": {
                **distribution(row["failed_attempt_fees_lamports"] for row in records),
                "total": sum(row["failed_attempt_fees_lamports"] for row in records),
            },
        }

    economics_by_entry_attempt = {
        str(attempt): economics_group("entry_attempt", attempt)
        for attempt in sorted({row["entry_attempt"] for row in trade_economics})
    }
    economics_by_exit_attempt = {
        str(attempt): economics_group("exit_attempt", attempt)
        for attempt in sorted({row["exit_attempt"] for row in trade_economics})
    }
    exact_trade_economics = [
        row for row in trade_economics if isinstance(row["economic_pnl_lamports"], int)
    ]
    all_fee_payer_deltas = [
        int(transaction["fee_payer_balance_delta_lamports"])
        for transaction in chain_transactions.values()
        if isinstance(transaction.get("fee_payer_balance_delta_lamports"), int)
    ]
    assigned_signatures = {
        str(row["signature"])
        for pool in opens
        for row in attempts_by_pool[pool]
        if str(row["signature"]) in chain_transactions
    }
    unassigned_fee_payer_deltas = [
        int(transaction["fee_payer_balance_delta_lamports"])
        for signature, transaction in chain_transactions.items()
        if signature not in assigned_signatures
        and isinstance(transaction.get("fee_payer_balance_delta_lamports"), int)
    ]

    exit_drift = []
    for close in closes:
        transaction = chain_transactions.get(str(close.get("exit_signature")))
        if not transaction:
            continue
        sells = [
            event
            for event in transaction.get("trade_events", [])
            if event.get("event_name") == "SellEvent" and event.get("pool") == close.get("pool")
        ]
        triggers = [
            row
            for row in rows
            if row.get("event") == "exit_triggered"
            and row.get("pool") == close.get("pool")
            and int(row["wall_time_ns"]) <= int(close["wall_time_ns"])
            and row.get("executable_quote_lamports") is not None
        ]
        if not sells or not triggers:
            continue
        trigger = triggers[-1]
        actual_quote = int(sells[-1]["user_quote_amount"])
        trigger_quote = int(trigger["executable_quote_lamports"])
        exit_drift.append(
            {
                "pool": close["pool"],
                "reason": close["reason"],
                "exit_attempt": int(close.get("exit_attempt", 1)),
                "trigger_quote_lamports": trigger_quote,
                "realized_quote_lamports": actual_quote,
                "drift_lamports": actual_quote - trigger_quote,
                "realized_to_trigger_ratio": actual_quote / trigger_quote,
            }
        )
    retry_drift = [row for row in exit_drift if row["exit_attempt"] > 1]
    first_attempt_drift = [row for row in exit_drift if row["exit_attempt"] == 1]
    ordinary_retry_drift = [
        row for row in retry_drift if row["reason"] != "recovery_after_failed_exit"
    ]
    drift_by_reason = {}
    for reason in sorted({row["reason"] for row in exit_drift}):
        records = [row for row in exit_drift if row["reason"] == reason]
        retries = [row for row in records if row["exit_attempt"] > 1]
        drift_by_reason[reason] = {
            "count": len(records),
            "retry_count": len(retries),
            "retry_realized_to_trigger_ratio": distribution(
                row["realized_to_trigger_ratio"] for row in retries
            ),
            "retry_improved_count": sum(row["realized_to_trigger_ratio"] >= 1 for row in retries),
            "retry_worsened_count": sum(row["realized_to_trigger_ratio"] < 1 for row in retries),
        }

    fee_latency_strata: dict[str, dict[str, Any]] = {}
    for side in ("entry", "exit"):
        attempts = [
            row
            for row in ready
            if row.get("side") == side and str(row["signature"]) in ledger_landed
        ]
        fee_latency_strata[side] = {}
        bids = [(row, int(row["selected_micro_lamports"])) for row in attempts]
        floor = [row for row, bid in bids if bid == FLOOR_BID_MICRO_LAMPORTS]
        above = [row for row, bid in bids if bid > FLOOR_BID_MICRO_LAMPORTS]
        for label, group in (("floor_bid", floor), ("above_floor_bid", above)):
            fee_latency_strata[side][label] = {
                "count": len(group),
                "selected_micro_lamports": distribution(
                    int(row["selected_micro_lamports"]) for row in group
                ),
                "estimated_total_fee_lamports": distribution(
                    int(row["estimated_total_fee_lamports"]) for row in group
                ),
                "send_to_confirmed_ms": distribution(
                    float(ledger_landed[str(row["signature"])]["send_to_confirmed_ms"])
                    for row in group
                ),
            }
    return {
        "submitted_attempts": len(ready),
        "position_opened_count": len(_events(rows, "position_opened")),
        "position_closed_count": len(closes),
        "close_reasons": dict(Counter(str(row["reason"]) for row in closes)),
        "retry_causes": {
            event: dict(
                Counter(str(row["retry_cause"]) for row in rows if row.get("event") == event)
            )
            for event in ("entry_retry", "exit_retry")
        },
        "fee_context_rejection_count": len(extract_fee_skip_cases(rows)),
        "by_side": by_side,
        "fee_latency_strata": fee_latency_strata,
        "trade_economics": {
            "count": len(trade_economics),
            "exact_balance_delta_count": len(exact_trade_economics),
            "economic_pnl_lamports": {
                **distribution(row["economic_pnl_lamports"] for row in exact_trade_economics),
                "total": sum(row["economic_pnl_lamports"] for row in exact_trade_economics),
            },
            "all_submitted_transaction_balance_delta_lamports": {
                "count": len(all_fee_payer_deltas),
                "total": sum(all_fee_payer_deltas),
            },
            "unassigned_transaction_balance_delta_lamports": {
                "count": len(unassigned_fee_payer_deltas),
                "total": sum(unassigned_fee_payer_deltas),
            },
            "transaction_fees_lamports": {
                **distribution(row["total_transaction_fees_lamports"] for row in trade_economics),
                "total": sum(row["total_transaction_fees_lamports"] for row in trade_economics),
            },
            "failed_attempt_fees_lamports": {
                **distribution(row["failed_attempt_fees_lamports"] for row in trade_economics),
                "total": sum(row["failed_attempt_fees_lamports"] for row in trade_economics),
            },
            "by_entry_attempt": economics_by_entry_attempt,
            "by_exit_attempt": economics_by_exit_attempt,
            "records": trade_economics,
        },
        "exit_quote_drift": {
            "all": {
                "count": len(exit_drift),
                "realized_to_trigger_ratio": distribution(
                    row["realized_to_trigger_ratio"] for row in exit_drift
                ),
                "drift_lamports": distribution(row["drift_lamports"] for row in exit_drift),
            },
            "first_attempt": {
                "count": len(first_attempt_drift),
                "realized_to_trigger_ratio": distribution(
                    row["realized_to_trigger_ratio"] for row in first_attempt_drift
                ),
            },
            "retry": {
                "count": len(retry_drift),
                "realized_to_trigger_ratio": distribution(
                    row["realized_to_trigger_ratio"] for row in retry_drift
                ),
                "improved_count": sum(row["realized_to_trigger_ratio"] >= 1 for row in retry_drift),
                "worsened_count": sum(row["realized_to_trigger_ratio"] < 1 for row in retry_drift),
            },
            "ordinary_retry": {
                "count": len(ordinary_retry_drift),
                "realized_to_trigger_ratio": distribution(
                    row["realized_to_trigger_ratio"] for row in ordinary_retry_drift
                ),
                "improved_count": sum(
                    row["realized_to_trigger_ratio"] >= 1 for row in ordinary_retry_drift
                ),
                "worsened_count": sum(
                    row["realized_to_trigger_ratio"] < 1 for row in ordinary_retry_drift
                ),
            },
            "by_reason": drift_by_reason,
            "records": exit_drift,
        },
    }


__all__ = [
    "distribution",
    "entry_retry_schedules",
    "extract_fee_skip_cases",
    "higher_quantile",
    "replay_fee_skip_schedule",
    "summarize_fee_skip_replays",
    "summarize_live_execution",
]
