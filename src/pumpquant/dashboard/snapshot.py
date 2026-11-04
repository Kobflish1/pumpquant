"""Assemble the dashboard snapshot from verified artifacts, telemetry and progress files."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from pumpquant.dashboard.manifests import (
    ManifestReport,
    VerifiedArtifact,
    iso_timestamp,
    jsonable,
)

_FAMILY_ALIASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "shock_recovery",
        (
            "shock_recovery",
            "shock-recovery",
            "shockrecovery",
        ),
    ),
    (
        "progress_speed",
        (
            "progress_speed",
            "progress-speed",
            "progressspeed",
        ),
    ),
    (
        "persistent_pressure",
        (
            "persistent_pressure",
            "persistent-pressure",
            "persistentpressure",
        ),
    ),
    (
        "amm_impact_ofi",
        (
            "amm_impact_ofi",
            "amm-impact-ofi",
            "ammimpactofi",
            "impact_ofi",
        ),
    ),
)


def _parse_payload(artifact: VerifiedArtifact) -> tuple[str, object] | None:
    suffix = artifact.path.suffix.lower()
    if suffix not in {".json", ".jsonl", ".ndjson"}:
        return None
    try:
        raw = artifact.path.read_text(encoding="utf-8")
        if suffix == ".json":
            value = json.loads(raw)
        else:
            value = [json.loads(line) for line in raw.splitlines() if line.strip()]
    except (OSError, UnicodeError, json.JSONDecodeError):
        # A verified non-JSON payload is still a sealed artifact.  It simply
        # cannot contribute structured dashboard fields.
        return None
    return artifact.relative_path, value


def _payload_key(path: str) -> str:
    return Path(path).stem.lower().replace("-", "_")


def _family_for(value: str) -> str | None:
    lowered = value.lower().replace("/", "_")
    for family, aliases in _FAMILY_ALIASES:
        if any(alias in lowered for alias in aliases):
            return family
    return None


def _as_list(value: object) -> list[object]:
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [jsonable(item) for item in value]
    if value is None:
        return []
    return [jsonable(value)]


def _metric_rows(value: object) -> list[object]:
    """Extract metric records from a JSON object or a report wrapper."""

    if isinstance(value, list):
        return _as_list(value)
    if not isinstance(value, Mapping):
        return []
    if any(key in value for key in ("candidate", "metrics", "evaluation")):
        return [jsonable(value)]
    for key in ("rows", "ledger", "trials", "metrics", "candidates"):
        nested = value.get(key)
        if isinstance(nested, (list, Mapping)):
            rows = _metric_rows(nested)
            if rows:
                return rows
    return []


def _flatten_metric_candidate(item: Mapping[str, object]) -> dict[str, object] | None:
    """Flatten a metric-ledger row into the dashboard's candidate shape.

    The expanded event grid stores provenance under ``candidate``
    (and the statistics under ``metrics``) so the ledger is self-describing.
    The GUI needs one row with both pieces, while preserving every original
    field for audit/detail views.
    """

    candidate = item.get("candidate")
    metrics = item.get("metrics")
    # The full walk-forward/alpha runner predates the event grid and calls
    # this object ``evaluation``.  Its Performance dataclass is nested one
    # level deeper, so normalise both producer contracts before the browser
    # sees them.
    evaluation = item.get("evaluation")
    backtest = item.get("backtest")
    candidate_map = candidate if isinstance(candidate, Mapping) else {}
    evaluation_map = evaluation if isinstance(evaluation, Mapping) else {}
    metrics_map = metrics if isinstance(metrics, Mapping) else {}
    performance = evaluation_map.get("performance")
    performance_map = performance if isinstance(performance, Mapping) else {}
    backtest_map = backtest if isinstance(backtest, Mapping) else {}
    if not candidate_map and not metrics_map and not evaluation_map:
        return None
    axis = candidate_map.get("axis")
    axis_map = axis if isinstance(axis, Mapping) else {}
    flattened: dict[str, object] = {
        **jsonable(dict(candidate_map)),
        **jsonable(dict(axis_map)),
        **jsonable(dict(evaluation_map)),
        **jsonable(dict(performance_map)),
        **jsonable(dict(metrics_map)),
        **jsonable(dict(backtest_map)),
        **jsonable(dict(item)),
    }
    # Keep the candidate's stable display identity and provenance explicit;
    # ``item`` itself may otherwise only contain a hash and a nested object.
    if "candidate" not in flattened:
        flattened["candidate"] = candidate_map.get(
            "name", candidate_map.get("sha256", candidate_map.get("candidate_sha256"))
        )
    if "candidate_sha256" not in flattened:
        flattened["candidate_sha256"] = candidate_map.get(
            "sha256",
            evaluation_map.get("candidate_sha256", candidate_map.get("candidate_sha256")),
        )
    if "family" not in flattened:
        flattened["family"] = axis_map.get("family")
    if (
        "candidate" not in flattened
        or flattened.get("candidate") in (None, "")
        or isinstance(flattened.get("candidate"), Mapping)
    ):
        flattened["candidate"] = (
            candidate_map.get("name")
            or candidate_map.get("sha256")
            or evaluation_map.get("candidate_sha256")
            or "Unnamed research"
        )
    return flattened


def _tail_exit_candidate_summaries(
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]] | None:
    """Return one exact additive OOS summary per registered tail/exit idea.

    The live progress snapshot and final metric ledger contain
    the same fold records. Deduplicate them first, then combine only test-fold
    rows. Additive statistics and pooled event variance remain exact; fields
    without sufficient components are left untouched or null.
    """

    if not any(row.get("kind") == "pumpquant.fresh_tail_exit_metric" for row in rows):
        return None
    # The same scientific row can arrive through the live snapshot and the
    # sealed ledger with different publication metadata. Its identity is the
    # registered candidate and chronological fold/split, not wrapper bytes.
    unique_by_fold: dict[tuple[str, str, str], Mapping[str, object]] = {}
    for row in rows:
        if row.get("kind") != "pumpquant.fresh_tail_exit_metric":
            continue
        candidate_id = row.get("candidate_sha256")
        fold = row.get("fold")
        split = row.get("split")
        if not isinstance(candidate_id, str) or not candidate_id:
            continue
        identity = (candidate_id, str(fold), str(split))
        unique_by_fold[identity] = row
    grouped: dict[str, list[Mapping[str, object]]] = {}
    for row in unique_by_fold.values():
        candidate_id = row.get("candidate_sha256")
        assert isinstance(candidate_id, str)
        grouped.setdefault(candidate_id, []).append(row)

    def number(row: Mapping[str, object], key: str) -> float | None:
        value = row.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None

    result: list[dict[str, object]] = []
    for candidate_id, candidate_rows in grouped.items():
        test_rows = [row for row in candidate_rows if row.get("split") == "test"]
        selected = test_rows or candidate_rows
        first = dict(selected[0])
        outcomes = sum(int(number(row, "outcomes") or 0) for row in selected)
        mints = sum(int(number(row, "mints") or 0) for row in selected)
        wins = sum(int(number(row, "wins") or 0) for row in selected)
        losses = sum(int(number(row, "losses") or 0) for row in selected)
        breakevens = sum(int(number(row, "breakevens") or 0) for row in selected)
        total_pnl = sum(number(row, "total_quote_pnl") or 0.0 for row in selected)
        gross_profit = sum(number(row, "gross_profit_quote") or 0.0 for row in selected)
        gross_loss = sum(number(row, "gross_loss_quote") or 0.0 for row in selected)
        mean_event = total_pnl / outcomes if outcomes else None
        sum_squares = 0.0
        downside_squares = 0.0
        variance_complete = outcomes >= 2
        downside_complete = outcomes > 0
        for row in selected:
            count = int(number(row, "outcomes") or 0)
            mean = number(row, "mean_event_pnl_quote")
            std = number(row, "pnl_std_quote")
            downside = number(row, "downside_deviation_quote")
            if count >= 2 and mean is not None and std is not None:
                sum_squares += (count - 1) * std * std + count * mean * mean
            else:
                variance_complete = False
            if count > 0 and downside is not None:
                downside_squares += count * downside * downside
            else:
                downside_complete = False
        pnl_std = (
            math.sqrt(max(0.0, (sum_squares - outcomes * mean_event * mean_event) / (outcomes - 1)))
            if variance_complete and mean_event is not None
            else None
        )
        downside_deviation = (
            math.sqrt(max(0.0, downside_squares / outcomes)) if downside_complete else None
        )
        average_win = gross_profit / wins if wins else None
        average_loss = gross_loss / losses if losses else None
        first.update(
            {
                "candidate": first.get("candidate", candidate_id),
                "candidate_sha256": candidate_id,
                "currency": "SOL",
                "quote_currency": "SOL",
                "split": "test",
                "fold": f"{len(selected)} OOS folds",
                "fold_count": len(selected),
                "outcomes": outcomes,
                "trades": outcomes,
                "total_trades": outcomes,
                "mints": mints,
                "total_quote_pnl": total_pnl,
                "mean_event_pnl_quote": mean_event,
                "expectancy_quote": mean_event,
                "wins": wins,
                "losses": losses,
                "breakevens": breakevens,
                "win_rate": wins / (wins + losses) if wins + losses else None,
                "loss_rate": losses / (wins + losses) if wins + losses else None,
                "average_win_quote": average_win,
                "average_loss_quote": average_loss,
                "payoff_ratio": (
                    average_win / average_loss
                    if average_win is not None and average_loss is not None and average_loss > 0.0
                    else None
                ),
                "gross_profit_quote": gross_profit,
                "gross_loss_quote": gross_loss,
                "profit_factor": gross_profit / gross_loss if gross_loss > 0.0 else None,
                "pnl_std_quote": pnl_std,
                "downside_deviation_quote": downside_deviation,
                "sharpe_per_trade": (
                    mean_event / pnl_std
                    if mean_event is not None and pnl_std is not None and pnl_std > 0.0
                    else None
                ),
                "sortino_per_trade": (
                    mean_event / downside_deviation
                    if mean_event is not None
                    and downside_deviation is not None
                    and downside_deviation > 0.0
                    else None
                ),
                "source_metric_rows": len(selected),
                "status": "oos_aggregate_complete",
            }
        )
        result.append(first)
    return sorted(result, key=lambda row: str(row.get("candidate", "")))


def _section_status(available: bool, reports: Sequence[ManifestReport]) -> str:
    if not available:
        return "pending"
    if any(report.lifecycle == "error" for report in reports):
        return "error"
    if any(report.lifecycle == "stale" for report in reports):
        return "stale"
    return "sealed"


@dataclass(slots=True)
class _Collected:
    """Everything the snapshot shows, gathered from the verified payloads."""

    input_summary: object = None
    interval: object = None
    families: list[dict[str, object]] = field(default_factory=list)
    candidates: list[object] = field(default_factory=list)
    metric_candidates: list[dict[str, object]] = field(default_factory=list)
    folds: list[object] = field(default_factory=list)
    grids: list[object] = field(default_factory=list)
    trades: object = None
    wallet: object = None
    capacity: object = None
    gates: object = None
    forward: object = None
    log_events: list[object] = field(default_factory=list)
    pipeline: object = None
    progress: Mapping[str, object] | None = None
    progress_resources: dict[str, object] = field(default_factory=dict)

    def add_metric_rows(self, rows: list[object]) -> None:
        for item in rows:
            if isinstance(item, Mapping):
                flattened = _flatten_metric_candidate(item)
                if flattened is not None:
                    self.metric_candidates.append(flattened)


def _collect_progress(collected: _Collected, mapping: Mapping[str, object]) -> None:
    collected.progress = mapping
    live_rows = mapping.get("live_candidates")
    if isinstance(live_rows, list):
        collected.add_metric_rows(live_rows)
    reported_pipeline = mapping.get("pipeline")
    collected.pipeline = reported_pipeline if isinstance(reported_pipeline, Mapping) else mapping
    reported_interval = mapping.get("interval", mapping.get("development_interval"))
    if isinstance(reported_interval, Mapping):
        collected.interval = reported_interval
    reported_resources = mapping.get("resources")
    if isinstance(reported_resources, Mapping):
        collected.progress_resources.update(jsonable(dict(reported_resources)))


def _collect_family(
    collected: _Collected, family: str, path: str, mapping: Mapping[str, object]
) -> None:
    family_payload: dict[str, object] = {"family": family, "artifact": path}
    family_payload.update(jsonable(dict(mapping)))
    if isinstance(mapping.get("candidates"), list):
        collected.candidates.extend(_as_list(mapping["candidates"]))
    if isinstance(mapping.get("folds"), list):
        collected.folds.extend(_as_list(mapping["folds"]))
    if isinstance(mapping.get("grid"), list):
        collected.grids.extend(_as_list(mapping["grid"]))
    collected.families.append(family_payload)


def _collect_strategy(
    collected: _Collected, key: str, family: str | None, mapping: Mapping[str, object]
) -> None:
    if isinstance(mapping.get("families"), list):
        collected.families.extend(_as_list(mapping["families"]))  # type: ignore[arg-type]
    family_grids = mapping.get("family_grids")
    if isinstance(family_grids, Mapping):
        for family_name, family_grid in family_grids.items():
            family_id = _family_for(str(family_name)) or str(family_name)
            family_entry: dict[str, object] = {"family": family_id, "grid": jsonable(family_grid)}
            if isinstance(family_grid, Mapping):
                if isinstance(family_grid.get("candidates"), list):
                    collected.candidates.extend(_as_list(family_grid["candidates"]))
                if isinstance(family_grid.get("folds"), list):
                    collected.folds.extend(_as_list(family_grid["folds"]))
            collected.families.append(family_entry)
    if isinstance(mapping.get("candidates"), list) and not family:
        collected.candidates.extend(_as_list(mapping["candidates"]))
    if isinstance(mapping.get("folds"), list) and not family:
        collected.folds.extend(_as_list(mapping["folds"]))
    if isinstance(mapping.get("grids"), list):
        collected.grids.extend(_as_list(mapping["grids"]))
    strategy = mapping.get("strategy")
    if isinstance(strategy, Mapping):
        if isinstance(strategy.get("candidates"), list):
            collected.candidates.extend(_as_list(strategy["candidates"]))
        if isinstance(strategy.get("folds"), list):
            collected.folds.extend(_as_list(strategy["folds"]))
    if collected.input_summary is None and isinstance(
        mapping.get("attested_input_summary"), Mapping
    ):
        collected.input_summary = mapping["attested_input_summary"]
    if collected.pipeline is None and "pipeline" in mapping:
        collected.pipeline = mapping["pipeline"]
    if collected.pipeline is None and "stage" in key:
        collected.pipeline = mapping


def _collect_execution(
    collected: _Collected, key: str, payload: object, mapping: Mapping[str, object]
) -> None:
    """Artifacts are matched by file name first; fields inside a payload then win."""
    if any(token in key for token in ("trade", "exact", "survivor")):
        collected.trades = payload
    if any(token in key for token in ("wallet", "shared_10", "shared10")):
        collected.wallet = payload
    if "capacity" in key:
        collected.capacity = payload
    if any(token in key for token in ("validation", "gate", "promotion")):
        collected.gates = payload
    if "forward" in key:
        collected.forward = payload
    if isinstance(mapping.get("trades"), (list, Mapping)):
        collected.trades = mapping["trades"]
    if isinstance(mapping.get("trade_events"), (list, Mapping)):
        collected.trades = mapping["trade_events"]
    if isinstance(mapping.get("wallet"), Mapping):
        collected.wallet = mapping["wallet"]
    if isinstance(mapping.get("shared_10_wallet"), Mapping):
        collected.wallet = mapping["shared_10_wallet"]
    if isinstance(mapping.get("capacity"), (list, Mapping)):
        collected.capacity = mapping["capacity"]
    if isinstance(mapping.get("gates"), (list, Mapping)):
        collected.gates = mapping["gates"]
    if isinstance(mapping.get("forward"), Mapping):
        collected.forward = mapping["forward"]
    if isinstance(mapping.get("logs"), list):
        collected.log_events.extend(_as_list(mapping["logs"]))
    if any(token in key for token in ("log", "event")):
        if isinstance(payload, list):
            collected.log_events.extend(_as_list(payload))
        elif isinstance(mapping.get("events"), list):
            collected.log_events.extend(_as_list(mapping["events"]))


def _collect(payloads: Mapping[str, object]) -> _Collected:
    collected = _Collected()
    for path, payload in payloads.items():
        key = _payload_key(path)
        mapping: Mapping[str, object] = payload if isinstance(payload, Mapping) else {}
        payload_kind = mapping.get("kind")
        if key.startswith("progress") or (
            isinstance(payload_kind, str) and payload_kind.endswith("_progress")
        ):
            _collect_progress(collected, mapping)
        if key in {"metric", "metrics", "metric_ledger", "trial", "trials", "ledger"} or (
            key.endswith("_ledger")
        ):
            collected.add_metric_rows(_metric_rows(payload))
        if collected.input_summary is None and any(token in key for token in ("input", "attest")):
            collected.input_summary = payload
        if collected.interval is None and any(token in key for token in ("interval", "chronology")):
            collected.interval = payload
        family = _family_for(key)
        family_hint = mapping.get("family", mapping.get("family_name"))
        if family is None and isinstance(family_hint, str):
            family = _family_for(family_hint)
        if family is not None:
            _collect_family(collected, family, path, mapping)
        _collect_strategy(collected, key, family, mapping)
        _collect_execution(collected, key, payload, mapping)
    return collected


def _resources(
    telemetry_rows: list[dict[str, object]], progress_resources: Mapping[str, object]
) -> dict[str, object]:
    """Sum the services' task, memory and CPU counters, where they report them."""
    summary: dict[str, object] = {}
    totals = {"TasksCurrent": 0, "MemoryCurrent": 0, "CPUUsageNSec": 0}
    seen: set[str] = set()
    for row in telemetry_rows:
        for name in totals:
            try:
                totals[name] += int(str(row.get(name, "")))
                seen.add(name)
            except (TypeError, ValueError):
                pass
    if "TasksCurrent" in seen:
        summary["tasks_current"] = totals["TasksCurrent"]
    if "MemoryCurrent" in seen:
        summary["memory_bytes"] = totals["MemoryCurrent"]
        summary["memory_usage"] = totals["MemoryCurrent"]
    if "CPUUsageNSec" in seen:
        summary["cpu_usage_ns"] = totals["CPUUsageNSec"]
    summary.update(progress_resources)
    summary["services"] = telemetry_rows
    available = progress_resources or any(
        row.get("status") == "available" for row in telemetry_rows
    )
    return {"status": "sealed" if available else "pending", **summary}


def build_snapshot(
    reports: Sequence[ManifestReport],
    payloads: Mapping[str, object],
    telemetry_rows: list[dict[str, object]],
    generated_at: datetime,
) -> dict[str, object]:
    """The dashboard's whole state, built only from files that passed verification."""
    errors = [
        f"{report.root.name}: {report.error}"
        for report in reports
        if report.lifecycle == "error" and report.error
    ]
    statuses = [report.lifecycle for report in reports]
    if errors:
        overall = "error"
    elif any(state == "pending" for state in statuses) or not statuses:
        overall = "pending"
    elif any(state == "stale" for state in statuses):
        overall = "stale"
    else:
        overall = "sealed"

    manifest_rows = {report.root.name: report.as_dict() for report in reports}
    c = _collect(payloads)
    progress_status = c.progress.get("run_status") if isinstance(c.progress, Mapping) else None
    if progress_status == "error":
        overall = "error"
    elif progress_status in {"running", "pending"}:
        overall = "pending"
    elif progress_status == "complete" and not errors:
        # A finished producer stops heartbeating; its sealed state does not go stale.
        overall = "sealed"

    # A metric ledger is the richer, fold-level view. Prefer it to the registry-only
    # candidate list, so the monitor does not show provenance-only duplicates.
    candidates = c.candidates
    if c.metric_candidates:
        tail_summaries = _tail_exit_candidate_summaries(c.metric_candidates)
        candidates = list(tail_summaries if tail_summaries is not None else c.metric_candidates)

    locked = {"locked": True, "accessed": False, "status": "locked"}
    snapshot: dict[str, object] = {
        "schema_version": 1,
        "generated_at": iso_timestamp(generated_at),
        "status": overall,
        "errors": errors,
        "artifacts": manifest_rows,
        "pipeline": {
            "status": progress_status or overall,
            "current_stage": c.pipeline.get("current_stage")
            if isinstance(c.pipeline, Mapping)
            else None,
            "stages": [
                {
                    "name": report.root.name,
                    "status": report.lifecycle,
                    "updated_at": report.updated_at,
                    "error": report.error,
                }
                for report in reports
            ],
            "reported": jsonable(c.pipeline),
        },
        "resources": _resources(telemetry_rows, c.progress_resources),
        "progress": jsonable(c.progress),
        "data": {
            "status": _section_status(
                c.input_summary is not None or c.interval is not None, reports
            ),
            "attested_input_summary": jsonable(c.input_summary),
            "manifest": manifest_rows,
            "interval": jsonable(c.interval),
            "holdout": dict(locked),
        },
        "strategy": {
            "status": _section_status(
                bool(c.families or candidates or c.folds or c.grids), reports
            ),
            "families": jsonable(c.families),
            "grids": jsonable(c.grids),
            "candidates": jsonable(candidates),
            "folds": jsonable(c.folds),
        },
        "execution": {
            "status": _section_status(
                c.trades is not None or c.wallet is not None or c.capacity is not None, reports
            ),
            "trades": jsonable(c.trades),
            "wallet": jsonable(c.wallet),
            "shared_10_wallet": jsonable(c.wallet),
            "capacity": jsonable(c.capacity),
        },
        "validation": {
            "status": _section_status(c.gates is not None or c.forward is not None, reports),
            "gates": jsonable(c.gates),
            "forward": jsonable(c.forward),
            "holdout": dict(locked),
        },
        "logs": {
            "status": _section_status(bool(c.log_events), reports),
            "events": jsonable(c.log_events),
        },
        "holdout": dict(locked),
    }
    # Flat top-level copies serve lightweight clients, including the bundled monitor;
    # the sectioned shape above remains the source of truth.
    snapshot.update(
        {
            "manifest": {"status": overall, "artifacts": manifest_rows},
            "input_summary": jsonable(c.input_summary),
            "families": jsonable(c.families),
            "candidates": jsonable(candidates),
            "folds": jsonable(c.folds),
            "trades": jsonable(c.trades),
            "wallet": jsonable(c.wallet),
            "capacity": jsonable(c.capacity),
            "gates": jsonable(c.gates),
            "forward": jsonable(c.forward),
            "events": jsonable(c.log_events),
            "progress": jsonable(c.progress),
        }
    )
    return snapshot
