from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

import pumpquant.dashboard.server as live_dashboard_module
from pumpquant.dashboard.server import (
    STATIC_ROOT,
    AtlasError,
    AtlasIdeasStore,
    AtlasSignalsStore,
    AtlasStore,
    ConfigError,
    DashboardConfig,
    EventBroker,
    LiveDashboard,
    MarketDataProvider,
    VelocityAtlasStore,
    _flatten_metric_candidate,
    _tail_exit_candidate_summaries,
    create_server,
    verify_sealed_manifest,
)

MINT = "1" * 32


def test_atlas_store_summary_and_ranked_tokens(tmp_path: Path) -> None:
    path = tmp_path / "atlas.sqlite"
    database = sqlite3.connect(path)
    database.execute(
        """
        CREATE TABLE tokens (
            mint TEXT PRIMARY KEY, name TEXT, symbol TEXT, launch_time_ns INTEGER,
            has_any_swap INTEGER, completed_bonding_curve INTEGER,
            migrated_to_pumpswap INTEGER, observed_pumpswap_swap INTEGER,
            swap_event_count INTEGER, quote_volume_sol REAL,
            first_market_cap_sol REAL, minimum_market_cap_sol REAL,
            peak_market_cap_sol REAL, terminal_market_cap_sol REAL,
            maximum_multiple REAL, minimum_multiple REAL, terminal_multiple REAL,
            seconds_to_completion REAL, seconds_to_2_0x REAL,
            seconds_to_5_0x REAL, seconds_to_10_0x REAL, seconds_to_20_0x REAL
        )
        """
    )
    database.executemany(
        "INSERT INTO tokens VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                "A",
                "Alpha",
                "A",
                1,
                1,
                1,
                1,
                1,
                10,
                2.0,
                1.0,
                0.5,
                20.0,
                3.0,
                20.0,
                0.5,
                3.0,
                4.0,
                1.0,
                2.0,
                3.0,
                4.0,
            ),
            (
                "B",
                "Beta",
                "B",
                2,
                0,
                0,
                0,
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ],
    )
    database.commit()
    database.close()

    store = AtlasStore(path)
    summary = store.summary()
    assert summary["launches"] == 2
    assert summary["reached_20x"] == 1
    assert summary["reached_20x_rate"] == 0.5
    ranked = store.tokens(limit=10, minimum_multiple=5)
    assert ranked["total"] == 1
    assert ranked["tokens"][0]["mint"] == "A"
    with pytest.raises(AtlasError, match="unsupported atlas sort"):
        store.tokens(sort="drop table")


def test_atlas_signals_store_returns_bounded_ranked_patterns(tmp_path: Path) -> None:
    path = tmp_path / "signals.sqlite"
    database = sqlite3.connect(path)
    database.execute(
        """
        CREATE TABLE segments (
            checkpoint_type TEXT, checkpoint_value REAL, feature TEXT, decile INTEGER,
            sample_size INTEGER, feature_min REAL, feature_median REAL, feature_max REAL,
            future_2x_rate REAL, future_5x_rate REAL, future_10x_rate REAL,
            checkpoint_2x_rate REAL, checkpoint_5x_rate REAL, checkpoint_10x_rate REAL,
            future_2x_lift REAL, future_5x_lift REAL, future_10x_lift REAL
        )
        """
    )
    database.executemany(
        "INSERT INTO segments VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                "age",
                60,
                "attention",
                10,
                900,
                0.1,
                0.2,
                0.4,
                0.3,
                0.1,
                0.02,
                0.1,
                0.03,
                0.01,
                3.0,
                3.33,
                2.0,
            ),
            (
                "age",
                60,
                "flow",
                9,
                600,
                -0.1,
                0.1,
                0.8,
                0.2,
                0.06,
                0.01,
                0.1,
                0.03,
                0.01,
                2.0,
                2.0,
                1.0,
            ),
        ],
    )
    database.commit()
    database.close()

    payload = AtlasSignalsStore(path).patterns(
        checkpoint_type="age", checkpoint_value=60, target=5, minimum_sample=500
    )
    assert [row["feature"] for row in payload["patterns"]] == ["attention", "flow"]
    assert payload["patterns"][0]["lift"] == 3.33
    with pytest.raises(AtlasError, match="unsupported signal target"):
        AtlasSignalsStore(path).patterns(target=20)


def test_atlas_ideas_store_filters_and_ranks_conservative_lift(tmp_path: Path) -> None:
    path = tmp_path / "ideas.sqlite"
    database = sqlite3.connect(path)
    database.execute(
        """
        CREATE TABLE ideas (
            idea_id TEXT PRIMARY KEY, idea_kind TEXT, age_seconds REAL,
            target_multiple INTEGER, signal_signature TEXT, feature_a TEXT,
            direction_a TEXT, feature_a_min REAL, feature_a_max REAL,
            feature_b TEXT, direction_b TEXT, feature_b_min REAL, feature_b_max REAL,
            sample_size INTEGER, hit_count INTEGER, baseline_rate REAL,
            outcome_rate REAL, lift REAL, wilson_lower_rate REAL,
            conservative_lift REAL, positive_age_count INTEGER,
            median_age_lift REAL, minimum_age_lift REAL
        )
        """
    )
    database.executemany(
        "INSERT INTO ideas VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                "a",
                "pair",
                60,
                5,
                "attention:high+vpin:low",
                "attention",
                "high",
                0.2,
                1.0,
                "vpin",
                "low",
                0.0,
                0.2,
                800,
                80,
                0.03,
                0.1,
                3.33,
                0.08,
                2.66,
                4,
                2.1,
                1.1,
            ),
            (
                "b",
                "single",
                60,
                5,
                "attention:high",
                "attention",
                "high",
                0.2,
                1.0,
                None,
                None,
                None,
                None,
                900,
                60,
                0.03,
                0.067,
                2.23,
                0.05,
                1.67,
                3,
                1.8,
                1.0,
            ),
        ],
    )
    database.commit()
    database.close()

    payload = AtlasIdeasStore(path).ideas(target=5, age_seconds=60, idea_kind="all")
    assert payload["total"] == 2
    assert [row["idea_id"] for row in payload["ideas"]] == ["a", "b"]
    pairs = AtlasIdeasStore(path).ideas(target=5, idea_kind="pair")
    assert pairs["total"] == 1
    with pytest.raises(AtlasError, match="unsupported opportunity idea filter"):
        AtlasIdeasStore(path).ideas(target=20)


def test_velocity_atlas_store_returns_threshold_time_curve(tmp_path: Path) -> None:
    path = tmp_path / "velocity.sqlite"
    database = sqlite3.connect(path)
    columns = [
        "threshold_usd INTEGER",
        "max_seconds INTEGER",
        "sample_size INTEGER",
        "completion_at_risk_tokens INTEGER",
        "eligible_launches INTEGER",
        "launch_reach_rate REAL",
        "at_risk_launch_reach_rate REAL",
        "launch_observation_hours REAL",
        "signals_per_hour REAL",
        "at_risk_signals_per_hour REAL",
        "completion_hits INTEGER",
        "completion_rate REAL",
        "completion_base_rate REAL",
        "completion_lift REAL",
        "completion_wilson_lower_rate REAL",
        "completion_300s_rate REAL",
        "completion_900s_rate REAL",
        "completion_1800s_rate REAL",
        "completion_3600s_rate REAL",
        "ath_50000_rate REAL",
        "ath_100000_rate REAL",
        "ath_200000_rate REAL",
        "ath_500000_rate REAL",
        "ath_1000000_rate REAL",
        "median_cross_seconds REAL",
        "median_observed_ath_usd REAL",
        "p90_observed_ath_usd REAL",
        "median_ath_from_threshold REAL",
        "at_risk_median_observed_ath_usd REAL",
        "at_risk_p90_observed_ath_usd REAL",
        "at_risk_median_ath_from_threshold REAL",
        "median_seconds_to_completion REAL",
    ]
    database.execute(f"CREATE TABLE velocity ({','.join(columns)})")
    database.executemany(
        f"INSERT INTO velocity VALUES ({','.join('?' for _ in columns)})",
        [
            (
                10_000,
                10,
                100,
                80,
                1_000,
                0.1,
                0.08,
                100,
                1.0,
                0.8,
                25,
                0.25,
                0.02,
                12.5,
                0.18,
                0.2,
                0.24,
                0.25,
                0.25,
                0.3,
                0.2,
                0.1,
                0.02,
                0.01,
                8.0,
                40_000,
                120_000,
                4.0,
                30_000,
                90_000,
                3.0,
                700,
            ),
            (
                10_000,
                30,
                150,
                120,
                1_000,
                0.15,
                0.12,
                100,
                1.5,
                1.2,
                30,
                0.2,
                0.02,
                10.0,
                0.15,
                0.16,
                0.19,
                0.2,
                0.2,
                0.25,
                0.15,
                0.08,
                0.01,
                0.005,
                15.0,
                35_000,
                100_000,
                3.5,
                28_000,
                80_000,
                2.8,
                800,
            ),
            (
                20_000,
                10,
                40,
                30,
                1_000,
                0.04,
                0.03,
                100,
                0.4,
                0.3,
                20,
                0.5,
                0.02,
                25.0,
                0.35,
                0.45,
                0.5,
                0.5,
                0.5,
                0.6,
                0.3,
                0.1,
                0.02,
                0.01,
                9.0,
                60_000,
                180_000,
                3.0,
                50_000,
                150_000,
                2.5,
                500,
            ),
        ],
    )
    database.commit()
    database.close()

    payload = VelocityAtlasStore(path).rows(threshold_usd=10_000)
    assert payload["thresholds"] == [10_000, 20_000]
    assert [row["max_seconds"] for row in payload["rows"]] == [10, 30]
    assert payload["rows"][0]["at_risk_median_ath_from_threshold"] == 3.0
    assert payload["rows"][0]["at_risk_signals_per_hour"] == 0.8
    assert payload["rows"][0]["completion_lift"] == 12.5
    with pytest.raises(AtlasError, match="unsupported market-cap velocity threshold"):
        VelocityAtlasStore(path).rows(threshold_usd=30_000)


def _write_json(root: Path, name: str, payload: object) -> tuple[int, str]:
    path = root / name
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(raw)
    return len(raw), hashlib.sha256(raw).hexdigest()


def _seal(root: Path, entries: list[dict[str, object]], *, status: str = "complete") -> None:
    (root / "manifest.json").write_text(
        json.dumps({"status": status, "artifacts": entries}, sort_keys=True),
        encoding="utf-8",
    )


def test_config_allowlists_exact_roots_and_units(tmp_path: Path) -> None:
    config = DashboardConfig.from_mapping(
        {
            "artifact_roots": {"run": "artifacts/run"},
            "service_names": ["pumpquant-worker.service"],
            "static_root": "dashboard/static",
        },
        base_dir=tmp_path,
    )
    assert config.artifact_roots == {"run": (tmp_path / "artifacts/run").resolve()}
    assert config.service_names == ("pumpquant-worker.service",)
    with pytest.raises(ConfigError):
        DashboardConfig.from_mapping(
            {"artifact_roots": {"holdout": str(tmp_path / "holdout")}},
            base_dir=tmp_path,
        )
    with pytest.raises(ConfigError):
        DashboardConfig.from_mapping(
            {"artifact_roots": {"run": str(tmp_path / "run")}, "service_names": ["../../etc"]},
            base_dir=tmp_path,
        )
    with pytest.raises(ConfigError):
        DashboardConfig.from_mapping(
            {"artifact_roots": {"run": str(tmp_path / "run")}, "holdout_root": "/tmp/holdout"},
            base_dir=tmp_path,
        )


def test_market_data_provider_caches_live_fx_and_finalized_supply() -> None:
    now = [1000.0]
    calls: list[tuple[str, bytes | None]] = []

    def fetch(url: str, body: bytes | None) -> dict[str, object]:
        calls.append((url, body))
        if body is None:
            return {"data": {"amount": "175.25", "currency": "USD"}}
        request = json.loads(body)
        assert request["method"] == "getTokenSupply"
        assert request["params"][0] == MINT
        return {
            "result": {
                "context": {"slot": 12345},
                "value": {"uiAmountString": "1000000000", "decimals": 6},
            }
        }

    provider = MarketDataProvider(fetch_json=fetch, clock=lambda: now[0])
    assert provider.sol_usd()["sol_usd"] == 175.25
    assert provider.sol_usd()["stale"] is False
    assert provider.token_supply(MINT)["token_supply"] == 1_000_000_000.0
    assert provider.token_supply(MINT)["slot"] == 12345
    assert len(calls) == 2


def test_market_data_provider_returns_explicitly_stale_fx_fallback() -> None:
    now = [1000.0]
    fail = [False]

    def fetch(_url: str, _body: bytes | None) -> dict[str, object]:
        if fail[0]:
            raise OSError("offline")
        return {"data": {"amount": "150"}}

    provider = MarketDataProvider(
        fetch_json=fetch,
        clock=lambda: now[0],
        fx_ttl_seconds=5,
    )
    provider.sol_usd()
    now[0] += 10
    fail[0] = True
    fallback = provider.sol_usd()
    assert fallback["sol_usd"] == 150.0
    assert fallback["stale"] is True
    assert fallback["age_seconds"] == 10.0


def test_manifest_last_gates_partial_and_verifies_bytes_and_hash(tmp_path: Path) -> None:
    root = tmp_path / "run"
    root.mkdir()
    (root / "partial.json").write_text('{"never":"parsed"}', encoding="utf-8")
    assert verify_sealed_manifest(root).lifecycle == "pending"

    size, digest = _write_json(root, "input-summary.json", {"rows": 4})
    _seal(root, [{"path": "input-summary.json", "byte_count": size, "sha256": digest}])
    report = verify_sealed_manifest(root)
    assert report.lifecycle == "sealed"
    assert [item.relative_path for item in report.artifacts] == ["input-summary.json"]

    _seal(root, [{"path": "input-summary.json", "byte_count": size + 1, "sha256": digest}])
    broken_size = verify_sealed_manifest(root)
    assert broken_size.lifecycle == "error"
    _seal(root, [{"path": "input-summary.json", "byte_count": size, "sha256": "0" * 64}])
    broken_hash = verify_sealed_manifest(root)
    assert broken_hash.lifecycle == "error"


def test_dashboard_caches_unchanged_sealed_artifact_hashes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "run"
    root.mkdir()
    size, digest = _write_json(root, "report.json", {"status": "complete"})
    _seal(root, [{"path": "report.json", "byte_count": size, "sha256": digest}])
    digest_calls = 0
    original = live_dashboard_module._digest_file

    def counted(path: Path) -> tuple[int, str]:
        nonlocal digest_calls
        digest_calls += 1
        return original(path)

    monkeypatch.setattr(live_dashboard_module, "_digest_file", counted)
    dashboard = LiveDashboard(
        DashboardConfig(artifact_roots={"run": root}), telemetry=lambda _services: []
    )
    dashboard.snapshot()
    first_calls = digest_calls
    dashboard.snapshot()
    assert digest_calls == first_calls

    size, digest = _write_json(root, "report.json", {"status": "complete", "revision": 2})
    _seal(root, [{"path": "report.json", "byte_count": size, "sha256": digest}])
    dashboard.snapshot()
    assert digest_calls > first_calls


def test_holdout_is_hard_error_and_snapshot_stays_locked(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        DashboardConfig(artifact_roots={"dev": tmp_path / "holdout-inputs"})

    config = DashboardConfig(artifact_roots={"dev": tmp_path / "dev"})
    dashboard = LiveDashboard(config, telemetry=lambda _services: [])
    snapshot = dashboard.snapshot()
    assert snapshot["holdout"] == {"locked": True, "accessed": False, "status": "locked"}
    assert snapshot["data"]["holdout"]["accessed"] is False
    assert snapshot["validation"]["holdout"]["locked"] is True


def test_snapshot_and_health_report_attested_data_without_fabricating_values(
    tmp_path: Path,
) -> None:
    root = tmp_path / "run"
    root.mkdir()
    size, digest = _write_json(root, "attested-input-summary.json", {"rows": 4, "sha256": "abc"})
    _seal(root, [{"path": "attested-input-summary.json", "byte_count": size, "sha256": digest}])
    config = DashboardConfig(artifact_roots={"run": root})
    dashboard = LiveDashboard(config, telemetry=lambda _services: [])
    snapshot = dashboard.snapshot()
    assert snapshot["status"] == "sealed"
    assert snapshot["data"]["attested_input_summary"] == {"rows": 4, "sha256": "abc"}
    assert snapshot["strategy"]["candidates"] == []
    assert snapshot["execution"]["wallet"] is None
    health = dashboard.health()
    assert health["status"] == "sealed"
    assert health["ok"] is True
    assert health["holdout"] == {"locked": True, "accessed": False}


def test_metric_normalizer_exposes_event_and_walk_forward_diagnostics() -> None:
    event_row = _flatten_metric_candidate(
        {
            "candidate": {
                "sha256": "event-hash",
                "axis": {
                    "family": "signed_flow_continuation",
                    "feature": "event_signed_quote_imbalance_5e",
                    "primary_source": "https://arxiv.org/abs/1011.6402",
                },
            },
            "metrics": {
                "total_quote_pnl": 0.5,
                "win_rate": 0.75,
                "sharpe_per_trade": 0.4,
            },
        }
    )
    assert event_row is not None
    assert event_row["candidate"] == "event-hash"
    assert event_row["family"] == "signed_flow_continuation"
    assert event_row["win_rate"] == 0.75

    walk_forward_row = _flatten_metric_candidate(
        {
            "fold": 2,
            "evaluation": {
                "candidate_sha256": "alpha-hash",
                "performance": {
                    "hit_rate": 0.6,
                    "sharpe": 0.8,
                    "sortino": 1.1,
                },
                "total_quote_pnl": 2.0,
                "probabilistic_sharpe": 0.9,
            },
        }
    )
    assert walk_forward_row is not None
    assert walk_forward_row["candidate"] == "alpha-hash"
    assert walk_forward_row["hit_rate"] == 0.6
    assert walk_forward_row["sharpe"] == 0.8
    assert walk_forward_row["probabilistic_sharpe"] == 0.9


def test_tail_exit_summaries_deduplicate_and_pool_only_oos_folds() -> None:
    common = {
        "kind": "pumpquant.fresh_tail_exit_metric",
        "candidate_sha256": "tail-hash",
        "candidate": "tail-exit:pressure",
        "mints": 2,
        "outcomes": 4,
        "wins": 1,
        "losses": 3,
        "breakevens": 0,
        "total_quote_pnl": -2.0,
        "mean_event_pnl_quote": -0.5,
        "gross_profit_quote": 1.0,
        "gross_loss_quote": 3.0,
        "pnl_std_quote": 1.0,
        "downside_deviation_quote": 0.75,
    }
    first_test = {**common, "fold": 1, "split": "test"}
    second_test = {
        **common,
        "fold": 2,
        "split": "test",
        "total_quote_pnl": 2.0,
        "mean_event_pnl_quote": 0.5,
        "wins": 3,
        "losses": 1,
        "gross_profit_quote": 3.0,
        "gross_loss_quote": 1.0,
    }
    training = {
        **common,
        "fold": 1,
        "split": "train",
        "total_quote_pnl": 1000.0,
    }

    duplicate_from_sealed_ledger = {**first_test, "publication": "final"}
    summaries = _tail_exit_candidate_summaries(
        [first_test, duplicate_from_sealed_ledger, second_test, training]
    )

    assert summaries is not None
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary["fold"] == "2 OOS folds"
    assert summary["currency"] == "SOL"
    assert summary["quote_currency"] == "SOL"
    assert summary["source_metric_rows"] == 2
    assert summary["outcomes"] == 8
    assert summary["trades"] == 8
    assert summary["total_trades"] == 8
    assert summary["mints"] == 4
    assert summary["total_quote_pnl"] == pytest.approx(0.0)
    assert summary["win_rate"] == pytest.approx(0.5)
    assert summary["profit_factor"] == pytest.approx(1.0)
    assert summary["pnl_std_quote"] == pytest.approx((8.0 / 7.0) ** 0.5)


def test_snapshot_publishes_metric_ledger_rows_as_strategy_candidates(tmp_path: Path) -> None:
    root = tmp_path / "run"
    root.mkdir()
    size, digest = _write_json(
        root,
        "metric-ledger.json",
        [
            {
                "candidate": {
                    "sha256": "sealed-hash",
                    "name": "flow-continuation",
                    "axis": {"family": "signed_flow_continuation"},
                },
                "metrics": {
                    "fold": 1,
                    "split": "test",
                    "total_quote_pnl": 1.5,
                    "win_rate": 0.625,
                    "profit_factor": 1.8,
                },
            }
        ],
    )
    _seal(root, [{"path": "metric-ledger.json", "byte_count": size, "sha256": digest}])
    snapshot = LiveDashboard(
        DashboardConfig(artifact_roots={"run": root}), telemetry=lambda _services: []
    ).snapshot()
    candidates = snapshot["strategy"]["candidates"]
    assert isinstance(candidates, list)
    assert candidates[0]["candidate"] == "flow-continuation"
    assert candidates[0]["win_rate"] == 0.625
    assert candidates[0]["profit_factor"] == 1.8


def test_snapshot_recognizes_tail_exit_progress_and_announcement(tmp_path: Path) -> None:
    root = tmp_path / "tail-exit-progress"
    root.mkdir()
    payload = {
        "kind": "pumpquant.fresh_tail_exit_progress",
        "experiment_id": "fresh-tail-exit-v1",
        "run_status": "running",
        "research_announcement": {
            "candidate_count": 40,
            "cumulative_trial_denominator": 1519,
            "sources": [{"source": "https://arxiv.org/abs/2408.03594"}],
        },
        "live_candidates": [
            {
                "candidate": {
                    "sha256": "tail-hash",
                    "name": "tail-exit:pressure",
                    "axis": {
                        "family": "signed_intensity_pressure_continuation",
                        "primary_source": "https://arxiv.org/abs/2408.03594",
                    },
                },
                "metrics": {"total_quote_pnl": -0.1, "fold": 1, "split": "test"},
            }
        ],
    }
    size, digest = _write_json(root, "current.json", payload)
    _seal(root, [{"path": "current.json", "byte_count": size, "sha256": digest}])
    snapshot = LiveDashboard(
        DashboardConfig(artifact_roots={"tail_progress": root}),
        telemetry=lambda _services: [],
    ).snapshot()
    assert snapshot["status"] == "pending"
    assert snapshot["progress"]["research_announcement"]["candidate_count"] == 40
    assert snapshot["strategy"]["candidates"][0]["candidate"] == "tail-exit:pressure"


def test_completed_progress_remains_sealed_after_live_stale_timeout(tmp_path: Path) -> None:
    root = tmp_path / "completed-progress"
    root.mkdir()
    size, digest = _write_json(
        root,
        "current.json",
        {
            "kind": "pumpquant.fresh_tail_exit_progress",
            "run_status": "complete",
            "overall_percent": 100.0,
            "eta_seconds": 0.0,
        },
    )
    _seal(root, [{"path": "current.json", "byte_count": size, "sha256": digest}])

    snapshot = LiveDashboard(
        DashboardConfig(
            artifact_roots={"progress": root},
            stale_after_seconds=0.0,
        ),
        telemetry=lambda _services: [],
    ).snapshot()

    assert snapshot["status"] == "sealed"
    assert snapshot["progress"]["overall_percent"] == 100.0
    assert snapshot["progress"]["eta_seconds"] == 0.0


def test_event_generation_and_sse_payload() -> None:
    broker = EventBroker()
    first = broker.publish("snapshot", {"status": "pending"})
    second = broker.publish("snapshot", {"status": "sealed"})
    assert [event.id for event in broker.since(first.id)] == [second.id]
    # Snapshot events are unnamed so the browser EventSource ``onmessage``
    # handler receives them; custom event types still carry an event line.
    assert b"event: snapshot" not in second.sse()
    assert b'"status":"sealed"' in second.sse()


def test_v2_frontend_uses_producer_eta_and_monotonic_sequence() -> None:
    # The full research terminal; index.html is the plain-language overview.
    app = (STATIC_ROOT / "terminal.js").read_text()
    index = (STATIC_ROOT / "terminal.html").read_text()
    tradingview = (
        STATIC_ROOT / "vendor" / "lightweight-charts.standalone.production.js"
    ).read_text()
    assert "Calibrating" not in app
    assert "progressSequence" in app
    assert "sequence < state.progressSequence" in app
    assert "const transportTimestamp = timestampMs(state.receivedAt)" in app
    assert "timestampMs(has(state.sourceUpdatedAt)" not in app
    assert 'candidateTrades: "/api/candidate-trades"' in app
    assert "const sol =" in app
    assert "money(selected.netPnl, selected.currency)" in app
    assert "createSeriesMarkers" in app
    assert "window.LightweightCharts" in app
    assert "getVisibleLogicalRange" in app
    assert "setVisibleLogicalRange(visibleRange)" in app
    # Fit once now and once after layout settles; the reset button fits on demand.
    assert "if (!sameView) { timeScale.fitContent();" in app
    assert app.count("fitContent()") == 3
    assert "HistogramSeries" in app
    assert "aggregateBars" in app
    assert 'id="custom-timeframe-toggle"' in index
    assert 'id="chart-reset"' in index
    assert 'id="currency-usd"' in index
    assert "TradingView Lightweight Charts" in index
    assert "TradingView Lightweight Charts™ v5.2.0" in tradingview[:300]
    assert 'id="run-work-age"' in index
    assert 'id="atlas-body"' in index
    assert 'href="#atlas"' in index
    assert 'atlasSummary: "/api/atlas-summary"' in app
    assert 'atlasTokens: "/api/atlas-tokens"' in app
    assert 'atlasPatterns: "/api/atlas-patterns"' in app
    assert 'atlasIdeas: "/api/atlas-ideas"' in app
    assert 'atlasVelocity: "/api/atlas-velocity"' in app
    assert 'id="idea-body"' in index
    assert 'id="velocity-body"' in index


def test_fixed_http_routes_ignore_arbitrary_query_paths(tmp_path: Path) -> None:
    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text("ok", encoding="utf-8")

    def market_fetch(_url: str, body: bytes | None) -> dict[str, object]:
        if body is None:
            return {"data": {"amount": "160.5"}}
        return {
            "result": {
                "context": {"slot": 99},
                "value": {"uiAmountString": "1000000000"},
            }
        }

    dashboard = LiveDashboard(
        DashboardConfig(artifact_roots={}, static_root=static),
        telemetry=lambda _services: [],
        market_data=MarketDataProvider(fetch_json=market_fetch),
    )
    try:
        server = create_server(dashboard, host="127.0.0.1", port=0)
    except PermissionError:
        pytest.skip("socket binding is unavailable in this sandbox")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        with urllib.request.urlopen(base + "/api/snapshot?path=/etc/passwd", timeout=3) as response:
            snapshot = json.loads(response.read())
        assert snapshot["holdout"]["accessed"] is False
        with urllib.request.urlopen(base + "/api/health?unit=evil.service", timeout=3) as response:
            health = json.loads(response.read())
        assert health["service"] == "pumpquant-live-dashboard"
        with urllib.request.urlopen(base + "/api/fx", timeout=3) as response:
            fx = json.loads(response.read())
        assert fx["sol_usd"] == 160.5
        with urllib.request.urlopen(base + f"/api/token-supply?mint={MINT}", timeout=3) as response:
            supply = json.loads(response.read())
        assert supply["token_supply"] == 1_000_000_000.0
        with pytest.raises(urllib.error.HTTPError) as malformed:
            urllib.request.urlopen(base + "/api/candidate-trades?broken", timeout=3)
        assert malformed.value.code == 400
        assert json.loads(malformed.value.read())["error"] == "malformed query string"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
