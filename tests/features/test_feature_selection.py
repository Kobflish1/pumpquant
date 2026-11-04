from __future__ import annotations

import numpy as np

from pumpquant.features.feature_selection import (
    select_information_distinct_features,
    threshold_partition_signature,
)


def test_monotonic_transforms_have_the_same_threshold_partitions() -> None:
    values = np.array([1.0, 2.0, 2.0, 4.0, np.nan, 8.0])
    transformed = 3.0 * values + 7.0
    assert threshold_partition_signature(
        values, (0.25, 0.5, 0.75)
    ) == threshold_partition_signature(transformed, (0.25, 0.5, 0.75))


def test_selection_removes_duplicates_and_limits_first_pass_crowding() -> None:
    matrix = np.column_stack(
        (
            np.arange(10, dtype=float),
            np.arange(10, dtype=float) * 2,
            np.array([0, 1] * 5, dtype=float),
            np.array([0, 0, 1, 1, 2, 2, 3, 3, 4, 4], dtype=float),
        )
    )
    selected, audit = select_information_distinct_features(
        ranked_features=("flow_a", "flow_copy", "flow_b", "actor"),
        feature_names=("flow_a", "flow_copy", "flow_b", "actor"),
        pooled_values=matrix,
        quantiles=(0.25, 0.5, 0.75),
        feature_groups={
            "flow_a": "flow",
            "flow_copy": "flow",
            "flow_b": "flow",
            "actor": "actor",
        },
        limit=3,
        maximum_per_group=1,
    )
    assert selected == ["flow_a", "actor", "flow_b"]
    assert audit["duplicate_partition_skipped"] == ["flow_copy"]
