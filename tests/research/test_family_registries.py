from __future__ import annotations

import pytest

from pumpquant.research.families import amm_impact_ofi, persistent_pressure, progress_speed


def test_each_registry_is_32_unique_candidates() -> None:
    for candidates in (
        progress_speed.default_progress_speed_candidates(),
        persistent_pressure.default_persistent_pressure_candidates(),
        amm_impact_ofi.default_amm_impact_ofi_candidates(),
    ):
        assert len(candidates) == 32
        assert len({candidate.name for candidate in candidates}) == 32
        assert len({candidate.sha256 for candidate in candidates}) == 32


def test_trial_counts_chain_across_the_preregistered_families() -> None:
    # Each family was registered after the one before it, so its prior trial count is
    # exactly the earlier family's cumulative count: the multiplicity never resets.
    assert progress_speed.cumulative_trial_count() == 1_175
    assert persistent_pressure.PRIOR_ATTEMPTED_CANDIDATES == 1_175
    assert persistent_pressure.cumulative_trial_count() == 1_207
    assert amm_impact_ofi.PRIOR_ATTEMPTED_CANDIDATES == 1_207
    assert amm_impact_ofi.cumulative_trial_count() == 1_239
    thresholds = [
        progress_speed.bonferroni_z_threshold(),
        persistent_pressure.bonferroni_z_threshold(),
        amm_impact_ofi.bonferroni_z_threshold(),
    ]
    assert thresholds == sorted(thresholds)


def test_registry_payloads_hold_no_results() -> None:
    for payload in (progress_speed.registry_payload(), amm_impact_ofi.registry_payload()):
        assert payload["candidate_count"] == 32
        encoded = str(payload).lower()
        assert "pnl" not in encoded
        assert "winner" not in encoded


def test_amm_candidates_name_their_feature() -> None:
    candidate = amm_impact_ofi.AmmImpactOfiCandidate(
        amm_impact_ofi.AmmOfiMeasure.DEPTH_PRESSURE, 5, 0.75, 20
    )
    assert candidate.signal_feature == "amm_depth_pressure_5e"
    with pytest.raises(ValueError):
        amm_impact_ofi.AmmImpactOfiCandidate(
            amm_impact_ofi.AmmOfiMeasure.DEPTH_PRESSURE, 10, 0.75, 20
        )


@pytest.mark.parametrize(
    "build",
    [
        amm_impact_ofi.create_amm_impact_ofi_source_sql,
        progress_speed.create_progress_crossing_source_sql,
    ],
)
def test_source_sql_only_accepts_plain_identifiers(build: object) -> None:
    assert callable(build)
    sql = build(input_table="events", output_view="source")
    assert "events" in sql and "source" in sql
    with pytest.raises(ValueError):
        build(input_table="events; DROP TABLE x", output_view="source")
