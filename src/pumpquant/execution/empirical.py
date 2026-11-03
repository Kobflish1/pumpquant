"""Convert signature-linked PumpSwap telemetry into replay-ready leg samples."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from typing import Any

from pumpquant.execution.telemetry import summarize_attempts


@dataclass(frozen=True, slots=True)
class EmpiricalLegSample:
    side: str
    route: str | None
    signature: str
    bid_floor_micro_lamports: int
    compute_unit_price_micro_lamports: int
    global_p75_micro_lamports: int
    localized_p75_micro_lamports: int
    congestion_regime: str
    pre_submit_ms: float
    send_to_processed_ms: float
    attempt_to_processed_ms: float
    send_to_land_ms: float
    actual_fee_lamports: int
    actual_compute_units: int


@dataclass(frozen=True, slots=True)
class EmpiricalExecutionGate:
    ready: bool
    reasons: tuple[str, ...]
    entry_submit_decisions: int
    exit_submit_decisions: int
    entry_pre_send_failures: int
    exit_pre_send_failures: int
    entry_submission_probability: float | None
    exit_submission_probability: float | None
    entry_landed_samples: int
    exit_landed_samples: int
    entry_bid_count: int
    exit_bid_count: int
    entry_regime_count: int
    exit_regime_count: int


@dataclass(frozen=True, slots=True)
class EmpiricalExecutionProfile:
    kind: str
    version: int
    route_filter: str | None
    economic_latency: str
    samples: tuple[EmpiricalLegSample, ...]
    gate: EmpiricalExecutionGate

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _integer(row: Mapping[str, Any], key: str) -> int:
    value = row.get(key)
    return int(value) if isinstance(value, (int, float)) else 0


def _number(row: Mapping[str, Any], key: str) -> float | None:
    value = row.get(key)
    return float(value) if isinstance(value, (int, float)) else None


def _regime(global_p75: int, localized_p75: int) -> str:
    # The writable-account market is the relevant fee market for this
    # transaction even when the global sample is zero.
    effective_p75 = max(global_p75, localized_p75)
    if effective_p75 <= 1_000:
        return "quiet"
    if effective_p75 <= 10_000:
        return "active"
    return "congested"


def build_empirical_execution_profile(
    events: Iterable[Mapping[str, Any]],
    *,
    minimum_landed_per_leg: int = 30,
    minimum_bids_per_leg: int = 3,
    minimum_regimes_per_leg: int = 2,
    minimum_submission_probability: float = 0.99,
    route: str | None = None,
) -> EmpiricalExecutionProfile:
    rows = tuple(dict(event) for event in events)
    attempts = {attempt.attempt_id: attempt for attempt in summarize_attempts(rows)}
    grouped: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for row in rows:
        grouped[str(row["attempt_id"])][str(row["event"])] = row

    samples: list[EmpiricalLegSample] = []
    decisions = {"entry": 0, "exit": 0}
    pre_send_failures = {"entry": 0, "exit": 0}
    sent = {"entry": 0, "exit": 0}
    for attempt_id, attempt in attempts.items():
        if attempt.purpose == "pumpswap_calibration_buy":
            side = "entry"
        elif attempt.purpose == "pumpswap_calibration_sell":
            side = "exit"
        else:
            continue
        rows_by_event = grouped[attempt_id]
        created = rows_by_event.get("attempt_created", {})
        attempt_route = str(created["route"]) if isinstance(created.get("route"), str) else None
        if route is not None and attempt_route != route:
            continue
        decision = rows_by_event.get("decision", {})
        if decision.get("action") != "submit":
            continue
        decisions[side] += 1
        pre_send_failures[side] += int(attempt.pre_send_failure)
        sent[side] += int(attempt.sent)
        landed = rows_by_event.get("landed", {})
        built_event = rows_by_event.get("built_signed", {})
        send_to_processed_ms = _number(landed, "websocket_processed_ms")
        # The broadcast requests are concurrent and the `sent` event is emitted
        # only after their HTTP promises settle. WebSocket processed time starts
        # at broadcast initiation, so adding it to `sent.elapsed_ms` double
        # counts time. `built_signed` is the last event immediately before send.
        pre_submit_ms = _number(built_event, "elapsed_ms")
        if (
            not attempt.landed
            or attempt.landed_error
            or not attempt.reconciled
            or attempt.signature is None
            or attempt.send_to_land_ms is None
            or attempt.compute_units is None
            or send_to_processed_ms is None
            or pre_submit_ms is None
        ):
            continue
        selected_bid = _integer(decision, "selected_micro_lamports")
        global_p75 = _integer(decision, "global_p75_micro_lamports")
        localized_p75 = _integer(decision, "localized_p75_micro_lamports")
        samples.append(
            EmpiricalLegSample(
                side=side,
                route=attempt_route,
                signature=attempt.signature,
                bid_floor_micro_lamports=_integer(decision, "bid_floor_micro_lamports"),
                compute_unit_price_micro_lamports=selected_bid,
                global_p75_micro_lamports=global_p75,
                localized_p75_micro_lamports=localized_p75,
                congestion_regime=_regime(global_p75, localized_p75),
                pre_submit_ms=pre_submit_ms,
                send_to_processed_ms=send_to_processed_ms,
                attempt_to_processed_ms=pre_submit_ms + send_to_processed_ms,
                send_to_land_ms=attempt.send_to_land_ms,
                actual_fee_lamports=attempt.fee_lamports,
                actual_compute_units=attempt.compute_units,
            )
        )

    side_samples = {
        side: [sample for sample in samples if sample.side == side] for side in ("entry", "exit")
    }
    reasons: list[str] = []
    for side in ("entry", "exit"):
        landed_count = len(side_samples[side])
        bid_count = len({sample.compute_unit_price_micro_lamports for sample in side_samples[side]})
        regime_count = len({sample.congestion_regime for sample in side_samples[side]})
        availability = sent[side] / decisions[side] if decisions[side] else None
        if landed_count < minimum_landed_per_leg:
            reasons.append(f"{side}_landed_samples={landed_count}<{minimum_landed_per_leg}")
        if bid_count < minimum_bids_per_leg:
            reasons.append(f"{side}_bid_count={bid_count}<{minimum_bids_per_leg}")
        if regime_count < minimum_regimes_per_leg:
            reasons.append(f"{side}_regime_count={regime_count}<{minimum_regimes_per_leg}")
        if availability is None or availability < minimum_submission_probability:
            reasons.append(
                f"{side}_submission_probability={availability}<{minimum_submission_probability}"
            )

    gate = EmpiricalExecutionGate(
        ready=not reasons,
        reasons=tuple(reasons),
        entry_submit_decisions=decisions["entry"],
        exit_submit_decisions=decisions["exit"],
        entry_pre_send_failures=pre_send_failures["entry"],
        exit_pre_send_failures=pre_send_failures["exit"],
        entry_submission_probability=(
            sent["entry"] / decisions["entry"] if decisions["entry"] else None
        ),
        exit_submission_probability=(
            sent["exit"] / decisions["exit"] if decisions["exit"] else None
        ),
        entry_landed_samples=len(side_samples["entry"]),
        exit_landed_samples=len(side_samples["exit"]),
        entry_bid_count=len(
            {sample.compute_unit_price_micro_lamports for sample in side_samples["entry"]}
        ),
        exit_bid_count=len(
            {sample.compute_unit_price_micro_lamports for sample in side_samples["exit"]}
        ),
        entry_regime_count=len({sample.congestion_regime for sample in side_samples["entry"]}),
        exit_regime_count=len({sample.congestion_regime for sample in side_samples["exit"]}),
    )
    return EmpiricalExecutionProfile(
        kind="pumpquant.empirical_execution_profile",
        version=3,
        route_filter=route,
        economic_latency="attempt_created_to_websocket_processed",
        samples=tuple(samples),
        gate=gate,
    )


__all__ = [
    "EmpiricalExecutionGate",
    "EmpiricalExecutionProfile",
    "EmpiricalLegSample",
    "build_empirical_execution_profile",
]
