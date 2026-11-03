"""Research commands: build bars, replay, search, and run the demo."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Iterable
from dataclasses import asdict
from pathlib import Path
from tempfile import mkstemp
from typing import Any

from pumpquant.cli._output import print_json
from pumpquant.core.hashing import sha256_file
from pumpquant.core.io import iter_swap_event_batches, read_bars, read_swap_events, write_jsonl
from pumpquant.demo import run_demo
from pumpquant.execution.bar_replay import ExecutionConfig, MomentumSpec, run_backtest
from pumpquant.features.bars import build_bars, iter_bars_chain_ordered
from pumpquant.pricing.depth_ladder import AmmState, synthetic_depth
from pumpquant.research.search import (
    ExperimentLedger,
    momentum_grid_stream,
    momentum_low_discrepancy_stream,
    walk_forward_search,
)
from pumpquant.state.launch_times import LaunchTimesError, export_launch_times
from pumpquant.state.materializer import (
    MaterializationError,
    materialize_amm_states,
)
from pumpquant.validation.metrics import performance

_MAX_BAR_STREAM_FALLBACK_ROWS = 1_000_000


def _write_jsonl_atomic(path: str | Path, records: Any) -> int:
    """Publish JSONL through a sibling temporary file after full success."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = mkstemp(
        prefix=f".{target.name}.",
        suffix=".partial",
        dir=target.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    published = False
    try:
        count = write_jsonl(temporary, records)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        published = True
        return count
    finally:
        if not published:
            temporary.unlink(missing_ok=True)


def _fixed_notional_diagnostics(values: Any) -> dict[str, Any]:
    """Serialize only outcome statistics that do not imply an equity curve."""
    payload = asdict(values)
    payload.pop("compounded_return")
    payload.pop("sequence_max_drawdown")
    return payload


def _config_from_args(args: argparse.Namespace) -> ExecutionConfig:
    return ExecutionConfig(
        quote_notional=args.quote_notional,
        venue_fee_bps=args.fee_bps,
        adverse_selection_bps=args.adverse_bps,
        fixed_network_cost_quote=args.network_cost,
        priority_cost_quote=args.priority_cost,
        latency_bars=args.latency_bars,
        max_reserve_fraction=args.max_reserve_fraction,
        max_bar_volume_fraction=args.max_volume_fraction,
        landing_probability=args.landing_probability,
        random_seed=args.seed,
        use_observed_venue_fee=not args.ignore_observed_fees,
    )


def _add_execution_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--quote-notional", type=float, default=0.10)
    parser.add_argument("--fee-bps", type=float, default=125.0)
    parser.add_argument("--adverse-bps", type=float, default=20.0)
    parser.add_argument("--network-cost", type=float, default=0.00001)
    parser.add_argument("--priority-cost", type=float, default=0.00010)
    parser.add_argument("--latency-bars", type=int, default=1)
    parser.add_argument("--max-reserve-fraction", type=float, default=0.005)
    parser.add_argument("--max-volume-fraction", type=float, default=0.01)
    parser.add_argument("--landing-probability", type=float, default=1.0)
    parser.add_argument("--ignore-observed-fees", action="store_true")
    parser.add_argument("--seed", type=int, default=7)


def _cmd_bars(args: argparse.Namespace) -> int:
    launches = None
    if args.launch_times:
        with Path(args.launch_times).open(encoding="utf-8") as handle:
            launches = {key: int(value) for key, value in json.load(handle).items()}
    interval_ns = round(args.interval_ms * 1_000_000)
    if Path(args.events).suffix.lower() == ".parquet":

        def flatten_batches() -> Any:
            for batch in iter_swap_event_batches(args.events):
                yield from batch

        def bounded_fallback_events() -> Any:
            count = 0
            for batch in iter_swap_event_batches(args.events):
                for event in batch:
                    if count >= _MAX_BAR_STREAM_FALLBACK_ROWS:
                        raise ValueError(
                            "Parquet event order is not streamable and materializing fallback is "
                            f"disabled above {_MAX_BAR_STREAM_FALLBACK_ROWS:,} rows"
                        )
                    count += 1
                    yield event

        def parquet_row_count() -> int:
            try:
                import pyarrow.parquet as pq
            except ImportError as error:  # pragma: no cover - optional dependency
                raise RuntimeError(
                    "Parquet support requires: pip install 'pumpquant[data]'"
                ) from error
            parquet = pq.ParquetFile(Path(args.events))
            try:
                metadata = parquet.metadata
                return 0 if metadata is None else int(metadata.num_rows)
            finally:
                close = getattr(parquet, "close", None)
                if callable(close):
                    close()

        try:
            count = _write_jsonl_atomic(
                args.output,
                iter_bars_chain_ordered(
                    flatten_batches(),
                    interval_ns=interval_ns,
                    launch_times=launches,
                ),
            )
        except ValueError as error:
            # Streaming requires canonical physical order.  A local Parquet
            # archive may predate that contract, so retry through the legacy
            # materializing path only for the two ordering regressions and
            # only for bounded local inputs.
            message = str(error)
            if "ordering_key order" not in message and "time-bucket starts" not in message:
                raise
            row_count = parquet_row_count()
            if row_count > _MAX_BAR_STREAM_FALLBACK_ROWS:
                raise ValueError(
                    "Parquet event order is not streamable and materializing fallback is "
                    f"disabled above {_MAX_BAR_STREAM_FALLBACK_ROWS:,} rows "
                    f"(file has {row_count:,})"
                ) from error
            count = _write_jsonl_atomic(
                args.output,
                build_bars(
                    bounded_fallback_events(),
                    interval_ns=interval_ns,
                    launch_times=launches,
                ),
            )
    else:
        count = _write_jsonl_atomic(
            args.output,
            build_bars(
                read_swap_events(args.events),
                interval_ns=interval_ns,
                launch_times=launches,
            ),
        )
    print_json({"bars_written": count, "output": str(Path(args.output).resolve())})
    return 0


def _cmd_export_launch_times(args: argparse.Namespace) -> int:
    positional = list(args.inputs or [])
    option_inputs = list(args.input_paths or [])
    if args.output is None:
        if len(positional) < 2 or option_inputs:
            raise LaunchTimesError("launch-times requires --output, or positional INPUT... OUTPUT")
        output = positional.pop()
    else:
        output = args.output
    inputs = option_inputs + positional
    if not inputs:
        raise LaunchTimesError("launch-times requires at least one feature input")
    report = export_launch_times(
        inputs,
        output,
        strict=bool(args.strict),
        batch_size=args.batch_size,
    )
    print_json(report)
    return 0


def _cmd_amm_materialize(args: argparse.Namespace) -> int:
    fee_config: object | None = None
    if args.fee_config is not None:
        try:
            fee_config = json.loads(Path(args.fee_config).read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise MaterializationError("--fee-config is not valid JSON") from error
    manifest = materialize_amm_states(
        args.input,
        args.output,
        quote_notionals=args.quote_notionals,
        slippage_buckets_bps=args.slippage_buckets_bps,
        fee_config=fee_config,  # type: ignore[arg-type]
        fee_schedule=args.fee_schedule,
        batch_size=args.batch_size,
        start_slot=args.start_slot,
        end_slot=args.end_slot,
    )
    print_json(
        {
            "status": "published",
            "output": str(Path(args.output).resolve()),
            "row_count": manifest["row_count"],
            "quote_row_count": manifest["quote_row_count"],
            "config_sha256": manifest["config_sha256"],
            "labels": manifest["labels"],
        }
    )
    return 0


def _cmd_backtest(args: argparse.Namespace) -> int:
    bars = read_bars(args.bars)
    spec = MomentumSpec(
        momentum_lookback=args.momentum_lookback,
        entry_momentum=args.entry_momentum,
        entry_buy_imbalance=args.entry_imbalance,
        min_liquidity_quote=args.min_liquidity,
    )
    result = run_backtest(bars, spec, _config_from_args(args))
    print_json(
        {
            "fixed_notional_outcome_diagnostics": _fixed_notional_diagnostics(
                performance(result.returns)
            ),
            "metrics_warning": (
                "dependent fixed-notional outcomes, not a shared-capital portfolio "
                "equity curve or evidence of edge"
            ),
            "rejected_orders": result.rejected_orders,
            "missed_orders": result.missed_orders,
            "missed_entry_orders": result.missed_entry_orders,
            "missed_exit_orders": result.missed_exit_orders,
            "failed_order_cost_quote": result.failed_order_cost_quote,
            "unclosed_positions": result.unclosed_positions,
            "total_quote_pnl": result.total_quote_pnl,
            "attempted_capital_quote": result.attempted_capital_quote,
            "return_on_attempted_capital": result.return_on_attempted_capital,
            "trades": [asdict(trade) for trade in result.trades],
            "open_positions": [asdict(position) for position in result.open_positions],
        }
    )
    return 0


def _cmd_depth(args: argparse.Namespace) -> int:
    state = AmmState(
        base_reserve=args.base_reserve,
        quote_reserve=args.quote_reserve,
        fee_bps=args.fee_bps,
        real_base_reserve=args.real_base_reserve,
        real_quote_reserve=args.real_quote_reserve,
    )
    levels = synthetic_depth(state, args.sizes)
    print_json(
        {
            "marginal_price": state.marginal_price,
            "warning": "synthetic AMM quotes, not resting order-book levels",
            "levels": [asdict(level) for level in levels],
        }
    )
    return 0


def _append_trial_ledger(
    path: str | Path,
    *,
    experiment_id: str,
    data_sha256: str,
    config: ExecutionConfig,
    report: Any,
) -> int:
    count = 0
    with ExperimentLedger(path) as ledger:
        for fold in report.folds:
            selected_spec = None if fold.selected is None else fold.selected.spec
            for evaluation in fold.candidate_evaluations:
                spec_payload = asdict(evaluation.spec)
                evaluation_payload = asdict(evaluation)
                evaluation_payload["performance"] = _fixed_notional_diagnostics(
                    evaluation.performance
                )
                spec_hash = hashlib.sha256(
                    json.dumps(spec_payload, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
                payload = {
                    "ledger_schema_version": 2,
                    "experiment_id": experiment_id,
                    "data_sha256": data_sha256,
                    "fold": fold.number,
                    "test_start_ns": fold.test_start_ns,
                    "train_cutoff_ns": fold.train_cutoff_ns,
                    "candidate_sha256": spec_hash,
                    "selected": evaluation.spec == selected_spec,
                    "execution_config": asdict(config),
                    "evaluation": evaluation_payload,
                }
                ledger.append(payload)
                count += 1
    return count


def _cmd_search(args: argparse.Namespace) -> int:
    bars = read_bars(args.bars)
    config = _config_from_args(args)
    template = MomentumSpec(
        volume_lookback=args.volume_lookback,
        min_liquidity_quote=args.min_liquidity,
        min_trade_count=args.min_trade_count,
        min_age_seconds=args.min_age_seconds,
        max_age_seconds=args.max_age_seconds,
        exit_momentum=args.exit_momentum,
        trailing_stop=args.trailing_stop,
        max_holding_bars=args.max_holding_bars,
    )
    candidates: Iterable[MomentumSpec]
    if args.candidate_samples is not None:
        candidates = momentum_low_discrepancy_stream(
            args.candidate_samples,
            seed=args.candidate_seed,
            template=template,
        )
    else:
        candidates = momentum_grid_stream(
            momentum_lookbacks=args.momentum_lookbacks,
            entry_momenta=args.entry_momenta,
            entry_imbalances=args.entry_imbalances,
            take_profits=args.take_profits,
            stop_losses=args.stop_losses,
            template=template,
        )
    ledger_sink: ExperimentLedger | None = None
    data_sha256: str | None = None
    if args.ledger:
        if not args.experiment_id:
            raise ValueError("--experiment-id is required when --ledger is used")
        ledger_sink = ExperimentLedger(args.ledger)
        data_sha256 = sha256_file(args.bars)
    report = walk_forward_search(
        bars,
        candidates,
        config,
        min_train_tokens=args.min_train_tokens,
        test_tokens=args.test_tokens,
        embargo_ns=round(args.embargo_ms * 1_000_000),
        minimum_train_positions=args.minimum_train_positions,
        candidate_batch_size=args.candidate_batch_size,
        survivor_count=args.survivor_count,
        halving_rounds=args.halving_rounds,
        halving_eta=args.halving_eta,
        ledger=ledger_sink,
        experiment_id=args.experiment_id or "search",
        data_sha256=data_sha256,
        resume=args.resume,
    )
    ledger_rows = report.ledger_rows_appended
    print_json(
        {
            "candidate_count": report.candidate_count,
            "trial_count": report.trial_count,
            "evaluation_count": report.evaluation_count,
            "failed_trial_count": report.failed_trial_count,
            "pruned_trial_count": report.pruned_trial_count,
            "ledger_rows_appended": ledger_rows,
            "folds": [
                {
                    "number": fold.number,
                    "train_mints": len(fold.train_mints),
                    "test_mints": len(fold.test_mints),
                    "selected": asdict(fold.selected.spec) if fold.selected else None,
                    "train_position_outcomes": (
                        fold.selected.position_outcomes if fold.selected else 0
                    ),
                    "test_position_outcomes": (
                        fold.test_backtest.position_outcomes
                        if fold.test_backtest is not None
                        else 0
                    ),
                    "test_outcome_diagnostics": _fixed_notional_diagnostics(
                        fold.test_outcome_diagnostics
                    ),
                    "test_total_quote_pnl": (
                        fold.test_backtest.total_quote_pnl
                        if fold.test_backtest is not None
                        else 0.0
                    ),
                    "test_attempted_capital_quote": (
                        fold.test_backtest.attempted_capital_quote
                        if fold.test_backtest is not None
                        else 0.0
                    ),
                    "test_return_on_attempted_capital": (
                        fold.test_backtest.return_on_attempted_capital
                        if fold.test_backtest is not None
                        else 0.0
                    ),
                }
                for fold in report.folds
            ],
            "fixed_notional_outcome_diagnostics": _fixed_notional_diagnostics(
                report.outcome_diagnostics
            ),
            "diagnostic_psr": report.diagnostic_probabilistic_sharpe,
            "diagnostic_dsr": report.diagnostic_deflated_sharpe,
            "statistics_warning": (
                "not portfolio evidence: build a global cash/inventory equity curve "
                "and clustered calendar-time inference before interpreting PSR/DSR"
            ),
            "search_scope_warning": (
                "lazy generation bounds memory but does not execute literal trillions of "
                "full backtests; low-discrepancy sampling and successive halving are "
                "approximate search policies"
            ),
        }
    )
    return 0


def _cmd_preview(args: argparse.Namespace) -> int:
    from pumpquant.dashboard.preview import serve

    serve(args.port)
    return 0


def _cmd_demo(args: argparse.Namespace) -> int:
    run = run_demo(
        tokens=args.tokens,
        bars_per_token=args.bars_per_token,
        seed=args.seed,
        config=_config_from_args(args),
    )
    bars, report = run.bars, run.report
    print_json(
        {
            "notice": "synthetic smoke test only; these metrics are not market evidence",
            "bars": len(bars),
            "tokens": args.tokens,
            "candidate_count": report.candidate_count,
            "trial_count": report.trial_count,
            "folds": [
                {
                    "number": fold.number,
                    "selected": asdict(fold.selected.spec) if fold.selected else None,
                    "train_position_outcomes": (
                        fold.selected.position_outcomes if fold.selected else 0
                    ),
                    "test_outcome_diagnostics": _fixed_notional_diagnostics(
                        fold.test_outcome_diagnostics
                    ),
                }
                for fold in report.folds
            ],
            "fixed_notional_outcome_diagnostics": _fixed_notional_diagnostics(
                report.outcome_diagnostics
            ),
            "statistics_warning": (
                "PSR/DSR are retained internally as implementation diagnostics but "
                "are not valid edge evidence until computed on a calendar-time "
                "portfolio series with clustered inference"
            ),
        }
    )
    return 0


def register(subparsers: Any) -> None:
    bars_parser = subparsers.add_parser("bars", help="aggregate normalized swap JSONL or Parquet")
    bars_parser.add_argument("events")
    bars_parser.add_argument("output")
    bars_parser.add_argument("--interval-ms", type=float, default=1_000.0)
    bars_parser.add_argument("--launch-times", help="JSON mapping mint to launch timestamp ns")
    bars_parser.set_defaults(handler=_cmd_bars)

    launch_parser = subparsers.add_parser(
        "launch-times",
        help="verify lifecycle feature datasets and export bars launch_times JSON",
    )
    launch_parser.add_argument(
        "inputs",
        nargs="*",
        metavar="INPUT",
        help="feature directory, manifest.json, or features.parquet (repeatable)",
    )
    launch_parser.add_argument(
        "--input",
        dest="input_paths",
        action="append",
        default=[],
        help="verified lifecycle feature input (repeat for multiple inputs)",
    )
    launch_parser.add_argument(
        "--output",
        help="bars-compatible JSON mapping; positional INPUT... OUTPUT is also supported",
    )
    launch_parser.add_argument(
        "--strict",
        "--require-complete",
        dest="strict",
        action="store_true",
        help="fail if any mint lacks a true launch anchor",
    )
    launch_parser.add_argument("--batch-size", type=int, default=32_768)
    launch_parser.set_defaults(handler=_cmd_export_launch_times)

    amm_parser = subparsers.add_parser(
        "amm-materialize",
        help="stream normalized swaps into reconstructed AMM state Parquet",
    )
    amm_parser.add_argument("input", help="normalized dataset directory or swaps.parquet")
    amm_parser.add_argument("output", help="new output dataset directory")
    quote_group = amm_parser.add_mutually_exclusive_group()
    quote_group.add_argument(
        "--quote-notionals",
        type=float,
        nargs="+",
        help="explicit quote-token notionals for executable hypothetical levels",
    )
    quote_group.add_argument(
        "--slippage-buckets-bps",
        type=float,
        nargs="+",
        help="slippage targets in basis points for executable hypothetical levels",
    )
    amm_parser.add_argument(
        "--fee-schedule",
        help="version-1 JSON fee/config schedule with explicit slot windows",
    )
    amm_parser.add_argument(
        "--fee-config",
        help="version-1 JSON single fee configuration (no historical inference)",
    )
    amm_parser.add_argument("--batch-size", type=int, default=32_768)
    amm_parser.add_argument("--start-slot", type=int)
    amm_parser.add_argument("--end-slot", type=int)
    amm_parser.set_defaults(handler=_cmd_amm_materialize)

    backtest_parser = subparsers.add_parser("backtest", help="replay a baseline on bar JSONL")
    backtest_parser.add_argument("bars")
    backtest_parser.add_argument("--momentum-lookback", type=int, default=3)
    backtest_parser.add_argument("--entry-momentum", type=float, default=0.05)
    backtest_parser.add_argument("--entry-imbalance", type=float, default=0.15)
    backtest_parser.add_argument("--min-liquidity", type=float, default=5.0)
    _add_execution_flags(backtest_parser)
    backtest_parser.set_defaults(handler=_cmd_backtest)

    search_parser = subparsers.add_parser(
        "search", help="walk-forward search on normalized bar JSONL"
    )
    search_parser.add_argument("bars")
    search_parser.add_argument("--min-train-tokens", type=int, required=True)
    search_parser.add_argument("--test-tokens", type=int, required=True)
    search_parser.add_argument("--embargo-ms", type=float, default=0.0)
    search_parser.add_argument("--minimum-train-positions", type=int, default=5)
    search_parser.add_argument("--momentum-lookbacks", type=int, nargs="+", default=[2, 3, 5])
    search_parser.add_argument("--entry-momenta", type=float, nargs="+", default=[0.03, 0.06])
    search_parser.add_argument("--entry-imbalances", type=float, nargs="+", default=[0.1, 0.3])
    search_parser.add_argument("--take-profits", type=float, nargs="+", default=[0.2, 0.35])
    search_parser.add_argument("--stop-losses", type=float, nargs="+", default=[0.1, 0.2])
    search_parser.add_argument("--volume-lookback", type=int, default=5)
    search_parser.add_argument("--min-liquidity", type=float, default=5.0)
    search_parser.add_argument("--min-trade-count", type=int, default=3)
    search_parser.add_argument("--min-age-seconds", type=float, default=5.0)
    search_parser.add_argument("--max-age-seconds", type=float, default=600.0)
    search_parser.add_argument("--exit-momentum", type=float, default=-0.02)
    search_parser.add_argument("--trailing-stop", type=float, default=0.12)
    search_parser.add_argument("--max-holding-bars", type=int, default=10)
    search_parser.add_argument(
        "--candidate-samples",
        type=int,
        help="use a seeded lazy low-discrepancy stream with this many candidates",
    )
    search_parser.add_argument("--candidate-seed", type=int, default=0)
    search_parser.add_argument(
        "--candidate-batch-size",
        type=int,
        default=256,
        help="bounded candidate/racing frontier size",
    )
    search_parser.add_argument(
        "--survivor-count",
        type=int,
        help="retain at most this many training evaluations per fold",
    )
    search_parser.add_argument(
        "--halving-rounds",
        type=int,
        default=1,
        help="successive-halving rounds over expanding past-token cohorts",
    )
    search_parser.add_argument(
        "--halving-eta",
        type=float,
        default=3.0,
        help="racing reduction factor (>1; used when --halving-rounds > 1)",
    )
    search_parser.add_argument(
        "--resume",
        action="store_true",
        help="skip matching committed ledger trials by fold/spec hash",
    )
    search_parser.add_argument("--ledger", help="append every candidate trial as JSONL")
    search_parser.add_argument("--experiment-id")
    _add_execution_flags(search_parser)
    search_parser.set_defaults(handler=_cmd_search)

    depth_parser = subparsers.add_parser("depth", help="derive AMM executable depth")
    depth_parser.add_argument("--base-reserve", type=float, required=True)
    depth_parser.add_argument("--quote-reserve", type=float, required=True)
    depth_parser.add_argument("--real-base-reserve", type=float)
    depth_parser.add_argument("--real-quote-reserve", type=float)
    depth_parser.add_argument("--fee-bps", type=float, required=True)
    depth_parser.add_argument("--sizes", type=float, nargs="+", required=True)
    depth_parser.set_defaults(handler=_cmd_depth)

    demo_parser = subparsers.add_parser("demo", help="run a synthetic end-to-end smoke test")
    demo_parser.add_argument("--tokens", type=int, default=36)
    demo_parser.add_argument("--bars-per-token", type=int, default=30)
    _add_execution_flags(demo_parser)
    demo_parser.set_defaults(handler=_cmd_demo)

    preview_parser = subparsers.add_parser(
        "preview", help="serve the dashboard locally on the synthetic demo"
    )
    preview_parser.add_argument("--port", type=int, default=8765)
    preview_parser.set_defaults(handler=_cmd_preview)
