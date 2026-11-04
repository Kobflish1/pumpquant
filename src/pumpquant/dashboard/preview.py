"""Serve the dashboard locally on the synthetic demo, with no private data at all.

The real read-only backend runs unchanged; only its data sources are swapped for
the demo run. No artifact paths, services, provider calls or wallets are touched.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from pumpquant.dashboard.config import (
    DashboardConfig,
)
from pumpquant.dashboard.market import (
    MarketDataError,
    MarketDataProvider,
)
from pumpquant.dashboard.server import (
    LiveDashboard,
    create_server,
)
from pumpquant.demo import DemoRun, run_demo
from pumpquant.execution.bar_replay import RoundTrip

NOT_IN_DEMO = "not_part_of_synthetic_demo"


class OfflineMarketData(MarketDataProvider):
    """Market lookups are unavailable in the preview."""

    def sol_usd(self) -> dict[str, object]:
        raise MarketDataError("Offline preview: exchange rate is unavailable")

    def token_supply(self, mint: str) -> dict[str, object]:
        raise MarketDataError("Offline preview: token supply is unavailable")


def _trade_record(trade: RoundTrip, index: int, fold: int) -> dict[str, Any]:
    return {
        "trade_id": index,
        "mint": trade.mint,
        "fold": fold,
        "signal_time_ns": trade.entry_signal_time_ns,
        "entry_time_ns": trade.entry.time_ns,
        "exit_time_ns": trade.exit.time_ns,
        "entry_price": trade.entry.average_price,
        "exit_price": trade.exit.average_price,
        "quote_in": trade.entry.quote_amount,
        "quote_out": trade.exit.quote_amount,
        "fixed_cost_quote": trade.entry.fixed_cost_quote + trade.exit.fixed_cost_quote,
        "net_pnl_quote": trade.quote_pnl,
        "exit_reason": trade.exit_reason,
    }


class PreviewDashboard(LiveDashboard):
    """The live dashboard backend, fed by one deterministic demo run."""

    def __init__(self, port: int, run: DemoRun | None = None) -> None:
        super().__init__(
            DashboardConfig(
                artifact_roots={},
                service_names=(),
                host="127.0.0.1",
                port=port,
            ),
            telemetry=lambda names: [],
            market_data=OfflineMarketData(),
        )
        self.run = run or run_demo()
        self.ledgers = {
            f"synthetic-fold-{fold.number}": [
                _trade_record(trade, index, fold.number)
                for index, trade in enumerate(fold.test_backtest.trades)
            ]
            if fold.test_backtest
            else []
            for fold in self.run.report.folds
        }

    def candidate_trades(
        self, candidate_sha256: str, *, limit: int = 5000, offset: int = 0
    ) -> dict[str, Any]:
        trades = self.ledgers.get(candidate_sha256, [])
        return {
            "trades": trades[offset : offset + limit],
            "total": len(trades),
            "status": "synthetic",
        }

    def trade_chart(self, candidate_sha256: str, trade_id: int) -> dict[str, Any]:
        trade = self.ledgers[candidate_sha256][trade_id]
        bars = [
            {
                "time": bar.start_time_ns // 1_000_000_000,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "quote_volume": bar.quote_volume,
            }
            for bar in self.run.bars
            if bar.mint == trade["mint"]
        ]
        return {"mint": trade["mint"], "synthetic": True, "trades": [trade], "bars": bars}

    def token_supply(self, mint: str) -> dict[str, Any]:
        return {"token_supply": None, "status": "synthetic_price_only"}

    def fx_quote(self) -> dict[str, Any]:
        return {"sol_usd": None, "status": "disabled_for_offline_preview"}

    def atlas_summary(self) -> dict[str, Any]:
        return {"status": NOT_IN_DEMO}

    def atlas_tokens(self, **_: Any) -> dict[str, Any]:
        return {"tokens": [], "total": 0, "status": NOT_IN_DEMO}

    def atlas_patterns(self, **_: Any) -> dict[str, Any]:
        return {"patterns": [], "status": NOT_IN_DEMO}

    def atlas_ideas(self, **_: Any) -> dict[str, Any]:
        return {"ideas": [], "total": 0, "status": NOT_IN_DEMO}

    def atlas_velocity(self, **_: Any) -> dict[str, Any]:
        return {"rows": [], "thresholds": [], "status": NOT_IN_DEMO}

    def _build_snapshot(self, reports: Any) -> dict[str, Any]:
        snapshot: dict[str, Any] = super()._build_snapshot(reports)
        candidates = []
        for fold in self.run.report.folds:
            result, outcomes = fold.test_backtest, fold.test_outcome_diagnostics
            if result is None:
                continue
            candidates.append(
                {
                    "id": f"synthetic-fold-{fold.number}",
                    "name": f"Test period {fold.number + 1}",
                    "family": "momentum",
                    "fold": fold.number,
                    "status": "simulated",
                    "trades": outcomes.observations,
                    "currency": "SOL",
                    "net_pnl": result.total_quote_pnl,
                    "return_pct": result.return_on_attempted_capital,
                    "win_rate": outcomes.hit_rate,
                    "profit_factor": outcomes.profit_factor,
                    "sharpe": outcomes.sharpe,
                    "parameters": asdict(fold.selected.spec) if fold.selected else {},
                    "source": "Simulated by PumpQuant’s data generator (seed 7)",
                    "mechanism": (
                        "Shows how the testing method works. "
                        "It is not evidence that the strategy makes money."
                    ),
                }
            )
        snapshot["experiment_id"] = "Does a momentum rule hold up on data it has never seen?"
        snapshot["progress"] = {
            "experiment_id": "Momentum demo",
            "run_status": "complete",
            "current_stage": "Demo finished",
            "percent": 100,
            "message": (
                "PumpQuant simulated 36 practice tokens, tried 16 versions of a momentum "
                "rule, and tested each period’s best version on later data it had never seen."
            ),
        }
        snapshot["strategy"]["candidates"] = candidates
        snapshot["candidates"] = candidates
        snapshot["status"] = "sealed"
        snapshot["logs"]["events"] = [
            {
                "level": "info",
                "message": "Synthetic data only. These numbers are not market evidence.",
            }
        ]
        return snapshot


def serve(port: int = 8765) -> None:
    dashboard = PreviewDashboard(port)
    server = create_server(dashboard)
    dashboard.start()
    print(f"PumpQuant preview (synthetic data): http://127.0.0.1:{port}/", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        dashboard.stop()
        server.server_close()
