from __future__ import annotations

import pytest

from pumpquant.research.families.persistent_pressure import (
    PersistentPressureCandidate,
    PressureMeasure,
    bonferroni_z_threshold,
    cumulative_trial_count,
    default_persistent_pressure_candidates,
    registry_payload,
)


def test_registry_is_exact_stable_and_unique() -> None:
    candidates = default_persistent_pressure_candidates()
    assert len(candidates) == 32
    assert len({candidate.name for candidate in candidates}) == 32
    assert len({candidate.sha256 for candidate in candidates}) == 32
    assert cumulative_trial_count() == 1_207
    assert bonferroni_z_threshold() == pytest.approx(3.9360026984861083)


def test_feature_mapping_uses_same_measure_on_nonoverlapping_windows() -> None:
    price = PersistentPressureCandidate(PressureMeasure.PRICE, 5, 0.75, 0.60, 20)
    flow = PersistentPressureCandidate(PressureMeasure.FLOW, 20, 0.90, 0.75, 50)
    assert price.pressure_feature == "shock_price_5e"
    assert price.confirmation_feature == "recovery_price_2e"
    assert flow.pressure_feature == "shock_flow_20e"
    assert flow.confirmation_feature == "recovery_flow_2e"


def test_registry_payload_contains_no_results_or_fitted_thresholds() -> None:
    payload = registry_payload()
    encoded = str(payload).lower()
    assert payload["candidate_count"] == 32
    assert payload["prior_attempted_candidate_count"] == 1_175
    assert "pnl" not in encoded
    assert "winner" not in encoded
    assert "fitted" not in encoded


@pytest.mark.parametrize(
    "kwargs",
    [
        {"measure": "price"},
        {"lookback_events": 10},
        {"pressure_quantile": 0.80},
        {"confirmation_quantile": 0.80},
        {"holding_events": 10},
    ],
)
def test_unregistered_axis_values_fail(kwargs: dict[str, object]) -> None:
    values: dict[str, object] = {
        "measure": PressureMeasure.PRICE,
        "lookback_events": 5,
        "pressure_quantile": 0.75,
        "confirmation_quantile": 0.60,
        "holding_events": 20,
    }
    values.update(kwargs)
    with pytest.raises(ValueError):
        PersistentPressureCandidate(**values)  # type: ignore[arg-type]
