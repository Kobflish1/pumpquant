"""Empirical, small-sample-safe execution models for PumpQuant replay.

The live sample is too small for a high-capacity latency model.  This module
therefore keeps complete observed round-trip templates together, exposes
support gates, and supplies deterministic block resampling.  A
template is an execution path, not an independent collection of marginal
latency draws.
"""

from __future__ import annotations

import hashlib
import math
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ExecutionLegTemplate:
    side: str
    full_latency_ms: float
    landed_attempt: int
    attempted_transactions: int
    estimated_attempt_fees_lamports: tuple[int, ...]
    selected_micro_lamports: int | None
    localized_p75_micro_lamports: int | None

    def __post_init__(self) -> None:
        if self.side not in {"entry", "exit"}:
            raise ValueError("side must be entry or exit")
        if not math.isfinite(self.full_latency_ms) or self.full_latency_ms < 0:
            raise ValueError("full latency must be finite and non-negative")
        if self.landed_attempt < 1 or self.attempted_transactions < 1:
            raise ValueError("attempt counts must be positive")
        if self.landed_attempt > self.attempted_transactions:
            raise ValueError("landed attempt exceeds attempted transactions")
        if any(value < 0 for value in self.estimated_attempt_fees_lamports):
            raise ValueError("fees must be non-negative")

    @property
    def conservative_paid_fee_lamports(self) -> int:
        """Charge every ready/broadcast attempt through the landed attempt."""

        return sum(self.estimated_attempt_fees_lamports)


@dataclass(frozen=True, slots=True)
class RoundTripExecutionTemplate:
    source_id: str
    ordinal: int
    entry: ExecutionLegTemplate
    exit: ExecutionLegTemplate

    def __post_init__(self) -> None:
        if not self.source_id or self.ordinal < 0:
            raise ValueError("invalid round-trip template identity")
        if self.entry.side != "entry" or self.exit.side != "exit":
            raise ValueError("round trip requires entry then exit")


@dataclass(frozen=True, slots=True)
class ExecutionSupport:
    eligible_entries: int
    landed_entries: int
    paired_round_trips: int
    entry_landing_probability: float
    entry_landing_wilson_lower_95: float
    entry_latency_p50_ms: float | None
    entry_latency_p90_ms: float | None
    entry_latency_p99_ms: float | None
    exit_latency_p50_ms: float | None
    exit_latency_p90_ms: float | None
    exit_latency_p99_ms: float | None
    maximum_exit_latency_ms: float | None
    fee_regimes: int
    ready_for_empirical_replay: bool
    limitations: tuple[str, ...]


def _higher_quantile(values: Sequence[float], probability: float) -> float | None:
    if not values:
        return None
    if not 0 < probability <= 1:
        raise ValueError("probability must be in (0, 1]")
    ordered = sorted(float(value) for value in values)
    return ordered[max(0, math.ceil(probability * len(ordered)) - 1)]


def wilson_lower(successes: int, trials: int, z: float = 1.959963984540054) -> float:
    """Wilson lower bound for an observed Bernoulli proportion."""

    if trials < 1 or successes < 0 or successes > trials:
        raise ValueError("invalid binomial counts")
    proportion = successes / trials
    denominator = 1.0 + z * z / trials
    center = proportion + z * z / (2.0 * trials)
    radius = z * math.sqrt(
        proportion * (1.0 - proportion) / trials + z * z / (4.0 * trials * trials)
    )
    return max(0.0, (center - radius) / denominator)


def _latest_before(
    rows: Sequence[dict[str, Any]], event: str, time_ns: int
) -> dict[str, Any] | None:
    eligible = [
        row
        for row in rows
        if row.get("event") == event
        and isinstance(row.get("wall_time_ns"), int)
        and int(row["wall_time_ns"]) <= time_ns
    ]
    return max(eligible, key=lambda row: int(row["wall_time_ns"])) if eligible else None


def _leg_template(
    rows: Sequence[dict[str, Any]],
    *,
    side: str,
    start_ns: int,
    landed: dict[str, Any],
) -> ExecutionLegTemplate:
    landed_ns = int(landed["wall_time_ns"])
    landed_attempt = max(1, int(landed.get("attempt", 1)))
    ready = sorted(
        (
            row
            for row in rows
            if row.get("event") == "transaction_ready"
            and row.get("side") == side
            and isinstance(row.get("wall_time_ns"), int)
            and start_ns <= int(row["wall_time_ns"]) <= landed_ns
        ),
        key=lambda row: (int(row.get("attempt", 1)), int(row["wall_time_ns"])),
    )
    fees = tuple(
        int(row["estimated_total_fee_lamports"])
        for row in ready
        if isinstance(row.get("estimated_total_fee_lamports"), int)
    )
    if not fees:
        # This is conservative relative to the current finalist's
        # fixed 23k convention and makes missing fee telemetry visible.
        fees = (23_000,) * landed_attempt
    last_ready = ready[-1] if ready else {}
    return ExecutionLegTemplate(
        side=side,
        full_latency_ms=(landed_ns - start_ns) / 1_000_000.0,
        landed_attempt=landed_attempt,
        attempted_transactions=max(landed_attempt, len(ready)),
        estimated_attempt_fees_lamports=fees,
        selected_micro_lamports=(
            int(last_ready["selected_micro_lamports"])
            if isinstance(last_ready.get("selected_micro_lamports"), int)
            else None
        ),
        localized_p75_micro_lamports=(
            int(last_ready["localized_p75_micro_lamports"])
            if isinstance(last_ready.get("localized_p75_micro_lamports"), int)
            else None
        ),
    )


def build_round_trip_templates(
    events: Iterable[dict[str, Any]],
    *,
    minimum_pairs: int = 30,
) -> tuple[tuple[RoundTripExecutionTemplate, ...], ExecutionSupport]:
    """Build signature-linked, joint entry/exit templates from a live ledger."""

    rows = [dict(row) for row in events]
    by_mint: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        mint = row.get("mint")
        if isinstance(mint, str):
            by_mint[mint].append(row)
    eligible = {
        str(row["mint"])
        for row in rows
        if row.get("event") == "decision"
        and row.get("action") == "enter"
        and isinstance(row.get("mint"), str)
    }
    landed_entries = {
        str(row["mint"])
        for row in rows
        if row.get("event") == "transaction_landed"
        and row.get("side") == "entry"
        and isinstance(row.get("mint"), str)
    }
    templates: list[RoundTripExecutionTemplate] = []
    for mint, mint_rows in by_mint.items():
        mint_rows.sort(key=lambda row: int(row.get("wall_time_ns", 0)))
        decision = next(
            (
                row
                for row in mint_rows
                if row.get("event") == "decision"
                and row.get("action") == "enter"
                and isinstance(row.get("decision_time_ns"), int)
            ),
            None,
        )
        entry_landed = next(
            (
                row
                for row in mint_rows
                if row.get("event") == "transaction_landed"
                and row.get("side") == "entry"
                and isinstance(row.get("wall_time_ns"), int)
            ),
            None,
        )
        exit_landed_rows = [
            row
            for row in mint_rows
            if row.get("event") == "transaction_landed"
            and row.get("side") == "exit"
            and isinstance(row.get("wall_time_ns"), int)
        ]
        if decision is None or entry_landed is None or not exit_landed_rows:
            continue
        exit_landed = exit_landed_rows[-1]
        trigger = _latest_before(mint_rows, "exit_triggered", int(exit_landed["wall_time_ns"]))
        if trigger is None:
            continue
        entry_start_ns = int(decision["decision_time_ns"])
        exit_start_ns = int(trigger["wall_time_ns"])
        if (
            int(entry_landed["wall_time_ns"]) < entry_start_ns
            or int(exit_landed["wall_time_ns"]) < exit_start_ns
        ):
            continue
        templates.append(
            RoundTripExecutionTemplate(
                source_id=mint,
                ordinal=len(templates),
                entry=_leg_template(
                    mint_rows,
                    side="entry",
                    start_ns=entry_start_ns,
                    landed=entry_landed,
                ),
                exit=_leg_template(
                    mint_rows,
                    side="exit",
                    start_ns=exit_start_ns,
                    landed=exit_landed,
                ),
            )
        )

    entry_latencies = [template.entry.full_latency_ms for template in templates]
    exit_latencies = [template.exit.full_latency_ms for template in templates]
    regimes = {
        max(
            template.entry.localized_p75_micro_lamports or 0,
            template.exit.localized_p75_micro_lamports or 0,
        )
        // 10_000
        for template in templates
    }
    landing_probability = len(landed_entries & eligible) / len(eligible) if eligible else 0.0
    limitations: list[str] = []
    if len(templates) < minimum_pairs:
        limitations.append(f"paired_round_trips={len(templates)}<{minimum_pairs}")
    if len(regimes) < 2:
        limitations.append("fewer_than_two_observed_fee_regimes")
    limitations.extend(
        [
            "historical_live_sample_is_not_a_stationary_future_law",
            "estimated_ready_fees_are_conservatively_charged_until_chain_fee_reconciliation_is_joined",
        ]
    )
    support = ExecutionSupport(
        eligible_entries=len(eligible),
        landed_entries=len(landed_entries & eligible),
        paired_round_trips=len(templates),
        entry_landing_probability=landing_probability,
        entry_landing_wilson_lower_95=(
            wilson_lower(len(landed_entries & eligible), len(eligible)) if eligible else 0.0
        ),
        entry_latency_p50_ms=_higher_quantile(entry_latencies, 0.50),
        entry_latency_p90_ms=_higher_quantile(entry_latencies, 0.90),
        entry_latency_p99_ms=_higher_quantile(entry_latencies, 0.99),
        exit_latency_p50_ms=_higher_quantile(exit_latencies, 0.50),
        exit_latency_p90_ms=_higher_quantile(exit_latencies, 0.90),
        exit_latency_p99_ms=_higher_quantile(exit_latencies, 0.99),
        maximum_exit_latency_ms=max(exit_latencies) if exit_latencies else None,
        fee_regimes=len(regimes),
        ready_for_empirical_replay=len(templates) >= minimum_pairs,
        limitations=tuple(limitations),
    )
    return tuple(templates), support


def deterministic_template_index(key: str, seed: int, template_count: int) -> int:
    if template_count < 1:
        raise ValueError("template_count must be positive")
    digest = hashlib.sha256(f"{seed}:{key}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % template_count


def stationary_block_indices(
    sample_count: int,
    output_count: int,
    *,
    mean_block_length: float,
    seed: int,
) -> tuple[int, ...]:
    """Deterministic stationary bootstrap indices without an RNG dependency."""

    if sample_count < 1 or output_count < 0 or mean_block_length < 1:
        raise ValueError("invalid block-resampling controls")
    result: list[int] = []
    counter = 0
    current = deterministic_template_index("start", seed, sample_count)
    restart_probability = 1.0 / mean_block_length
    while len(result) < output_count:
        result.append(current)
        counter += 1
        digest = hashlib.sha256(f"{seed}:restart:{counter}".encode()).digest()
        uniform = int.from_bytes(digest[:8], "big") / 2**64
        if uniform < restart_probability:
            current = deterministic_template_index(f"block:{counter}", seed, sample_count)
        else:
            current = (current + 1) % sample_count
    return tuple(result)


def template_summary(templates: Sequence[RoundTripExecutionTemplate]) -> dict[str, Any]:
    return {
        "pairs": len(templates),
        "entry_landed_attempts": dict(Counter(item.entry.landed_attempt for item in templates)),
        "exit_landed_attempts": dict(Counter(item.exit.landed_attempt for item in templates)),
        "entry_conservative_fee_lamports": [
            item.entry.conservative_paid_fee_lamports for item in templates
        ],
        "exit_conservative_fee_lamports": [
            item.exit.conservative_paid_fee_lamports for item in templates
        ],
    }


def serialize_library(
    templates: Sequence[RoundTripExecutionTemplate], support: ExecutionSupport
) -> dict[str, Any]:
    return {
        "kind": "pumpquant.execution_realism_library",
        "version": 1,
        "support": asdict(support),
        "summary": template_summary(templates),
        "templates": [asdict(template) for template in templates],
    }


__all__ = [
    "ExecutionLegTemplate",
    "ExecutionSupport",
    "RoundTripExecutionTemplate",
    "build_round_trip_templates",
    "deterministic_template_index",
    "serialize_library",
    "stationary_block_indices",
    "template_summary",
    "wilson_lower",
]
