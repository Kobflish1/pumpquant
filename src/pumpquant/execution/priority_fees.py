"""Small empirical priority-fee controller for Solana transactions.

The controller selects the cheapest observed bid arm whose conservative
latency quantile meets a caller-supplied service-level target. It does not
pretend that a single fee is optimal in every localized fee market.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from statistics import median
from typing import Any


@dataclass(frozen=True, slots=True)
class FeeArm:
    floor_micro_lamports: int
    samples: int
    landed_samples: int
    landing_probability: float
    latency_quantile_ms: float
    latency_median_ms: float
    calibration_localized_p75_median: float
    selected_bid_micro_lamports: int
    estimated_total_fee_lamports: int
    meets_latency_sla: bool
    context_supported: bool


@dataclass(frozen=True, slots=True)
class FeeDecision:
    action: str
    reason: str
    side: str
    latency_sla_ms: float
    latency_quantile: float
    current_localized_p75_micro_lamports: int
    selected_bid_micro_lamports: int | None
    estimated_total_fee_lamports: int | None
    arms: tuple[FeeArm, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _quantile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    # "higher" is deliberately conservative and deterministic for small arms.
    return ordered[math.ceil(quantile * len(ordered)) - 1]


def select_priority_fee(
    samples: Iterable[Mapping[str, Any]],
    *,
    side: str,
    current_localized_p75_micro_lamports: int,
    compute_unit_limit: int,
    latency_sla_ms: float = 1_000.0,
    latency_quantile: float = 0.99,
    minimum_samples_per_arm: int = 10,
    maximum_context_ratio: float = 2.0,
    base_fee_lamports: int = 5_000,
    maximum_total_fee_lamports: int = 105_000,
    allow_best_effort: bool = False,
) -> FeeDecision:
    """Select the lowest-fee empirically adequate arm.

    ``current_localized_p75_micro_lamports`` must be computed from the exact
    writable accounts of the transaction. An arm is considered out of context
    when current localized demand exceeds twice that arm's calibration median.
    Lower demand remains supported because the learned floor is retained.
    """

    if side not in {"entry", "exit"}:
        raise ValueError("side must be entry or exit")
    if current_localized_p75_micro_lamports < 0:
        raise ValueError("localized fee cannot be negative")
    if compute_unit_limit <= 0:
        raise ValueError("compute unit limit must be positive")
    if latency_sla_ms <= 0 or not 0 < latency_quantile <= 1:
        raise ValueError("latency contract is invalid")
    if minimum_samples_per_arm < 1 or maximum_context_ratio < 1:
        raise ValueError("calibration contract is invalid")

    grouped: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for raw in samples:
        if raw.get("side") != side:
            continue
        floor = raw.get("bid_floor_micro_lamports")
        localized = raw.get("localized_p75_micro_lamports")
        if (
            isinstance(floor, (int, float))
            and int(floor) >= 0
            and isinstance(localized, (int, float))
        ):
            # Floors below the sampled localized fee did not bind and are the
            # same empirical arm. This coalesces the preregistered 0 and 1,000
            # control floors without inventing a numerical clustering rule.
            effective_floor = 0 if int(floor) <= int(localized) else int(floor)
            grouped[effective_floor].append(raw)

    arms: list[FeeArm] = []
    for floor, rows in sorted(grouped.items()):
        latencies = [
            float(row["attempt_to_processed_ms"])
            for row in rows
            if isinstance(row.get("attempt_to_processed_ms"), (int, float))
        ]
        localized = [
            float(row["localized_p75_micro_lamports"])
            for row in rows
            if isinstance(row.get("localized_p75_micro_lamports"), (int, float))
        ]
        if not latencies or not localized:
            continue
        calibration_context = median(localized)
        context_supported = current_localized_p75_micro_lamports <= max(
            1.0, calibration_context * maximum_context_ratio
        )
        selected_bid = max(floor, current_localized_p75_micro_lamports)
        total_fee = base_fee_lamports + math.ceil(selected_bid * compute_unit_limit / 1_000_000)
        quantile_latency = _quantile(latencies, latency_quantile)
        arms.append(
            FeeArm(
                floor_micro_lamports=floor,
                samples=len(rows),
                landed_samples=len(latencies),
                landing_probability=len(latencies) / len(rows),
                latency_quantile_ms=quantile_latency,
                latency_median_ms=median(latencies),
                calibration_localized_p75_median=calibration_context,
                selected_bid_micro_lamports=selected_bid,
                estimated_total_fee_lamports=total_fee,
                meets_latency_sla=quantile_latency <= latency_sla_ms,
                context_supported=context_supported,
            )
        )

    eligible = [
        arm
        for arm in arms
        if arm.samples >= minimum_samples_per_arm
        and arm.landing_probability == 1.0
        and arm.meets_latency_sla
        and arm.context_supported
        and arm.estimated_total_fee_lamports <= maximum_total_fee_lamports
    ]
    if eligible:
        selected = min(
            eligible,
            key=lambda arm: (
                arm.estimated_total_fee_lamports,
                arm.latency_quantile_ms,
            ),
        )
        return FeeDecision(
            action="submit",
            reason="lowest_fee_arm_meeting_empirical_latency_sla",
            side=side,
            latency_sla_ms=latency_sla_ms,
            latency_quantile=latency_quantile,
            current_localized_p75_micro_lamports=current_localized_p75_micro_lamports,
            selected_bid_micro_lamports=selected.selected_bid_micro_lamports,
            estimated_total_fee_lamports=selected.estimated_total_fee_lamports,
            arms=tuple(arms),
        )

    if allow_best_effort:
        fallback = [
            arm
            for arm in arms
            if arm.samples >= minimum_samples_per_arm
            and arm.context_supported
            and arm.estimated_total_fee_lamports <= maximum_total_fee_lamports
        ]
        if fallback:
            selected = min(
                fallback,
                key=lambda arm: (
                    arm.latency_quantile_ms,
                    arm.estimated_total_fee_lamports,
                ),
            )
            return FeeDecision(
                action="submit",
                reason="best_effort_lowest_empirical_tail_latency",
                side=side,
                latency_sla_ms=latency_sla_ms,
                latency_quantile=latency_quantile,
                current_localized_p75_micro_lamports=current_localized_p75_micro_lamports,
                selected_bid_micro_lamports=selected.selected_bid_micro_lamports,
                estimated_total_fee_lamports=selected.estimated_total_fee_lamports,
                arms=tuple(arms),
            )

    return FeeDecision(
        action="skip",
        reason="no_supported_arm_meets_latency_fee_and_sample_gates",
        side=side,
        latency_sla_ms=latency_sla_ms,
        latency_quantile=latency_quantile,
        current_localized_p75_micro_lamports=current_localized_p75_micro_lamports,
        selected_bid_micro_lamports=None,
        estimated_total_fee_lamports=None,
        arms=tuple(arms),
    )


__all__ = ["FeeArm", "FeeDecision", "select_priority_fee"]
