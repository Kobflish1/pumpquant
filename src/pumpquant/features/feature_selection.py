"""Checkpoint-specific information canonicalisation for PumpAPI features."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence

import numpy as np


def threshold_partition_signature(
    values: np.ndarray,
    quantiles: Sequence[float],
) -> bytes | None:
    """Hash the exact quantile partitions a threshold search can produce.

    Monotonic transforms with identical missingness generate the same
    signature and therefore cannot create a different quantile entry rule.
    """

    array = np.asarray(values, dtype=np.float64)
    finite = np.isfinite(array)
    if finite.sum() == 0:
        return None
    observed = array[finite]
    if np.min(observed) == np.max(observed):
        return None
    thresholds = np.quantile(observed, quantiles)
    digest = hashlib.sha256()
    digest.update(np.packbits(finite, bitorder="little").tobytes())
    for threshold in thresholds:
        selected = finite & (array <= threshold)
        digest.update(np.packbits(selected, bitorder="little").tobytes())
    return digest.digest()


def select_information_distinct_features(
    *,
    ranked_features: Iterable[str],
    feature_names: Sequence[str],
    pooled_values: np.ndarray,
    quantiles: Sequence[float],
    feature_groups: Mapping[str, str],
    limit: int,
    maximum_per_group: int = 4,
) -> tuple[list[str], dict[str, object]]:
    """Select ranked features without constant or duplicate threshold behaviour."""

    if limit < 1 or maximum_per_group < 1:
        raise ValueError("limits must be positive")
    matrix = np.asarray(pooled_values, dtype=np.float64)
    if matrix.ndim != 2 or matrix.shape[1] != len(feature_names):
        raise ValueError("feature matrix shape does not match feature names")
    indices = {name: index for index, name in enumerate(feature_names)}
    signatures: set[bytes] = set()
    group_counts: dict[str, int] = {}
    selected: list[str] = []
    constant_or_empty: list[str] = []
    duplicate_partition: list[str] = []
    group_deferred: list[str] = []

    for feature in ranked_features:
        if feature not in indices or feature in selected:
            continue
        signature = threshold_partition_signature(matrix[:, indices[feature]], quantiles)
        if signature is None:
            constant_or_empty.append(feature)
            continue
        if signature in signatures:
            duplicate_partition.append(feature)
            continue
        group = feature_groups.get(feature, "unclassified")
        if group_counts.get(group, 0) >= maximum_per_group:
            group_deferred.append(feature)
            continue
        selected.append(feature)
        signatures.add(signature)
        group_counts[group] = group_counts.get(group, 0) + 1
        if len(selected) == limit:
            break

    # Diversity is a guard against one information family occupying the whole
    # shortlist, not a reason to return too few candidates.
    if len(selected) < limit:
        for feature in group_deferred:
            signature = threshold_partition_signature(matrix[:, indices[feature]], quantiles)
            if signature is None or signature in signatures:
                continue
            selected.append(feature)
            signatures.add(signature)
            group = feature_groups.get(feature, "unclassified")
            group_counts[group] = group_counts.get(group, 0) + 1
            if len(selected) == limit:
                break

    return selected, {
        "selected_count": len(selected),
        "constant_or_empty_skipped": constant_or_empty,
        "duplicate_partition_skipped": duplicate_partition,
        "group_deferred": group_deferred,
        "selected_group_counts": group_counts,
        "maximum_per_group_first_pass": maximum_per_group,
    }


__all__ = [
    "select_information_distinct_features",
    "threshold_partition_signature",
]
