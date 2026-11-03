from __future__ import annotations

import pytest

from pumpquant.research.fresh_event_research import (
    FRESH_AXES,
    candidate_order_sha256,
    candidates,
    create_fresh_raw_adapter_sql,
    registry_payload,
    trial_denominator,
)


def test_fresh_individual_registry_is_bounded_and_deterministic() -> None:
    values = candidates()
    assert len(FRESH_AXES) == 30
    assert len(values) == 240
    assert len({item.sha256 for item in values}) == 240
    assert trial_denominator() == 1479
    assert candidate_order_sha256() == (
        "e56c7cc383d0076550b783d468baed8b99eb8238e4083aac2f09f42766636eb2"
    )
    payload = registry_payload()
    assert payload["candidate_count"] == 240
    assert payload["candidate_order_sha256"] == candidate_order_sha256()
    assert payload["outcome_policy"]["combinations"] == (
        "locked until genuine individual survivors exist"
    )


def test_fresh_adapter_is_explicit_about_unavailable_participant_state() -> None:
    sql = create_fresh_raw_adapter_sql()
    assert "TRUE AS first_seen_censored" in sql
    assert "CAST(NULL AS DOUBLE) AS wallet_hhi" in sql
    assert "CAST(NULL AS DOUBLE) AS new_wallet_flow_share" in sql
    assert "reserve_mark_return" in sql
    with pytest.raises(ValueError):
        create_fresh_raw_adapter_sql(input_table="raw;drop")
