from __future__ import annotations

import pytest

from pumpquant.research.event_interactions import (
    DEFAULT_EVENT_INTERACTION_GATE_AXES,
    DEFAULT_EVENT_INTERACTION_SIGNAL_AXES,
    EventInteractionCandidate,
    create_event_interaction_thresholds_sql,
    default_event_interaction_candidates,
    event_interaction_aggregate_sql,
    event_interaction_signal_features,
    parse_event_interaction_aggregate_rows,
    parse_event_interaction_threshold_rows,
)


def test_interaction_registry_is_frozen_bounded_and_unique() -> None:
    candidates = default_event_interaction_candidates()

    assert len(DEFAULT_EVENT_INTERACTION_SIGNAL_AXES) == 12
    assert len(DEFAULT_EVENT_INTERACTION_GATE_AXES) == 5
    assert len(candidates) == 360
    assert len({candidate.sha256 for candidate in candidates}) == len(candidates)
    assert {candidate.holding_events for candidate in candidates} == {5, 20, 50}
    assert {candidate.signal_axis.feature for candidate in candidates} == {
        "event_signed_quote_imbalance_5e",
        "event_signed_quote_imbalance_20e",
        "event_signed_flow_acceleration_5e_20e",
        "event_reserve_momentum_5e",
        "event_reserve_momentum_20e",
        "event_rolling_vwap_deviation_20e",
    }
    assert all(len(candidate.gate_axis.quantiles) == 2 for candidate in candidates)
    assert all("threshold" not in candidate.name for candidate in candidates)


def test_interaction_sql_fits_train_cutoff_and_one_gate_only() -> None:
    candidates = default_event_interaction_candidates((5,))[:2]
    thresholds = create_event_interaction_thresholds_sql(candidates, 5)
    aggregate = event_interaction_aggregate_sql(candidates, 5)

    assert "WHERE memberships.split = 'train'" in thresholds
    assert "label_end_5_available_at_ns < memberships.cutoff_ns" in thresholds
    assert "labels.event_signed_quote_imbalance_5e IS NOT NULL" in thresholds
    assert "labels.bonding_progress IS NOT NULL" in thresholds
    assert "quantile_cont" in thresholds
    assert "INNER JOIN event_interaction_thresholds_5 AS thresholds USING (fold)" in aggregate
    assert "thresholds.c000_signal_threshold" in aggregate
    assert "thresholds.c000_gate_threshold" in aggregate
    assert "FOLLOWING" not in thresholds
    assert "FOLLOWING" not in aggregate

    # Every candidate has one signal predicate and one gate predicate.  There
    # is no syntax for a second gate axis in the generated predicate.
    assert aggregate.count("labels.bonding_progress") >= 1
    assert "labels.wallet_hhi" not in aggregate
    assert "labels.creator_dump_pressure" not in aggregate


def test_interaction_wallet_gate_requires_uncensored_history() -> None:
    candidates = default_event_interaction_candidates((5,))
    wallet = next(
        candidate for candidate in candidates if candidate.gate_axis.feature == "wallet_hhi"
    )
    thresholds = create_event_interaction_thresholds_sql((wallet,), 5)
    aggregate = event_interaction_aggregate_sql((wallet,), 5)

    assert "AND NOT labels.first_seen_censored" in thresholds
    assert "AND NOT labels.first_seen_censored" in aggregate


def test_interaction_wide_rows_parse_with_signal_and_gate_thresholds() -> None:
    candidate = default_event_interaction_candidates((5,))[0]
    assert isinstance(candidate, EventInteractionCandidate)
    thresholds = parse_event_interaction_threshold_rows(
        (
            {
                "fold": 1,
                "c000_signal_threshold": 0.25,
                "c000_gate_threshold": 0.50,
            },
        ),
        (candidate,),
        5,
    )
    assert thresholds[0].candidate_sha256 == candidate.sha256
    assert thresholds[0].signal_threshold == pytest.approx(0.25)
    assert thresholds[0].gate_threshold == pytest.approx(0.50)

    metrics = parse_event_interaction_aggregate_rows(
        (
            {
                "fold": 1,
                "split": "train",
                "c000_outcomes": 20,
                "c000_mints": 4,
                "c000_total_pnl": 0.2,
                "c000_mean_event": 0.01,
                "c000_mean_token": 0.02,
                "c000_std_token": 0.01,
                "c000_positive_tokens": 0.75,
            },
        ),
        (candidate,),
        5,
    )
    assert metrics[0].candidate_sha256 == candidate.sha256
    assert metrics[0].token_t_stat == pytest.approx(4.0)


def test_interaction_signal_features_include_exact_inputs() -> None:
    assert set(event_interaction_signal_features()) == {
        "bonding_progress",
        "creator_dump_pressure",
        "event_reserve_momentum_20e",
        "event_reserve_momentum_5e",
        "event_rolling_vwap_deviation_20e",
        "event_signed_flow_acceleration_5e_20e",
        "event_signed_quote_imbalance_20e",
        "event_signed_quote_imbalance_5e",
        "event_volatility_ratio_5e_50e",
        "first_seen_censored",
        "gross_directional_impact_bps",
        "wallet_hhi",
    }


def test_full_horizon_interaction_sql_binds_in_duckdb() -> None:
    duckdb = pytest.importorskip("duckdb")
    candidates = default_event_interaction_candidates((5,))
    feature_columns = ", ".join(
        f"{feature} DOUBLE" for feature in event_interaction_signal_features()
    )
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            "CREATE TABLE event_labels ("
            "mint VARCHAR, eligible_5 BOOLEAN, "
            "label_end_5_available_at_ns BIGINT, pnl_5 DOUBLE, "
            f"{feature_columns})"
        )
        connection.execute(
            "CREATE TABLE memberships (fold INTEGER, split VARCHAR, mint VARCHAR, cutoff_ns BIGINT)"
        )
        connection.execute(create_event_interaction_thresholds_sql(candidates, 5))
        connection.execute(event_interaction_aggregate_sql(candidates, 5))
    finally:
        connection.close()
