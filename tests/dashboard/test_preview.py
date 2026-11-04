from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from urllib.request import urlopen

import pytest

from pumpquant.dashboard.preview import PreviewDashboard
from pumpquant.dashboard.server import create_server
from pumpquant.demo import run_demo

RUN = run_demo()


@pytest.fixture(scope="module")
def base_url() -> Iterator[str]:
    """The real preview server on a free port, serving the demo run."""
    dashboard = PreviewDashboard(port=8765, run=RUN)
    server = create_server(dashboard, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def _get(url: str) -> tuple[int, bytes]:
    with urlopen(url, timeout=10) as response:
        return response.status, response.read()


def test_snapshot_lists_each_test_period_from_the_demo(base_url: str) -> None:
    _, body = _get(f"{base_url}/api/snapshot")
    snapshot = json.loads(body)
    periods = snapshot["candidates"]
    assert [period["name"] for period in periods] == [
        "Test period 1",
        "Test period 2",
        "Test period 3",
    ]
    assert [period["trades"] for period in periods] == [
        fold.test_outcome_diagnostics.observations for fold in RUN.report.folds
    ]
    assert snapshot["status"] == "sealed"


def test_trades_and_chart_come_from_the_same_replay(base_url: str) -> None:
    _, body = _get(f"{base_url}/api/candidate-trades?candidate=synthetic-fold-0")
    trades = json.loads(body)["trades"]
    replayed = RUN.report.folds[0].test_backtest
    assert replayed is not None
    assert len(trades) == len(replayed.trades)
    _, body = _get(f"{base_url}/api/trade-chart?candidate=synthetic-fold-0&trade_id=0")
    chart = json.loads(body)
    assert chart["synthetic"] is True
    assert chart["mint"] == trades[0]["mint"]
    assert len(chart["bars"]) == 30
    assert chart["bars"][0]["time"] <= trades[0]["signal_time_ns"] // 1_000_000_000


def test_static_pages_ship_inside_the_package(base_url: str) -> None:
    status, page = _get(f"{base_url}/")
    assert status == 200
    assert b"<html" in page.lower()
    status, script = _get(f"{base_url}/static/app.js")
    assert status == 200
    assert b"chartTime" in script


def test_market_lookups_stay_offline() -> None:
    dashboard = PreviewDashboard(port=8765, run=RUN)
    assert dashboard.fx_quote()["sol_usd"] is None
    assert dashboard.atlas_summary()["status"] == "not_part_of_synthetic_demo"
