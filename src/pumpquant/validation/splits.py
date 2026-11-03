"""Chronological launch-cohort splits with a pre-test embargo.

This module does not yet purge overlapping feature/label intervals; that
requires event-level label start/end metadata and remains a separate milestone.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar


class HasLaunchTime(Protocol):
    @property
    def mint(self) -> str: ...

    @property
    def launch_time_ns(self) -> int: ...


T = TypeVar("T", bound=HasLaunchTime)


@dataclass(frozen=True, slots=True)
class WalkForwardFold(Generic[T]):
    number: int
    train: tuple[T, ...]
    test: tuple[T, ...]
    test_start_ns: int
    test_end_ns: int


def embargoed_walk_forward(
    cohorts: Iterable[T],
    *,
    min_train_size: int,
    test_size: int,
    embargo_ns: int = 0,
    step_size: int | None = None,
) -> tuple[WalkForwardFold[T], ...]:
    """Create expanding chronological folds while keeping whole mints isolated.

    The unit of splitting is a token, not an individual bar.  This prevents a
    strategy from learning a token's later path while being tested on its early
    path.  ``embargo_ns`` additionally removes launches immediately preceding a
    test cohort, which is useful when labels or features span a time horizon.
    """
    if min_train_size < 1:
        raise ValueError("min_train_size must be positive")
    if test_size < 1:
        raise ValueError("test_size must be positive")
    if embargo_ns < 0:
        raise ValueError("embargo_ns cannot be negative")
    step = test_size if step_size is None else step_size
    if step < 1:
        raise ValueError("step_size must be positive")

    ordered = sorted(cohorts, key=lambda item: (item.launch_time_ns, item.mint))
    seen_mints: set[str] = set()
    for cohort in ordered:
        if cohort.mint in seen_mints:
            raise ValueError(f"duplicate cohort for mint {cohort.mint!r}")
        seen_mints.add(cohort.mint)

    folds: list[WalkForwardFold[T]] = []
    test_start_index = min_train_size
    fold_number = 0
    while test_start_index + test_size <= len(ordered):
        test = ordered[test_start_index : test_start_index + test_size]
        test_start_ns = test[0].launch_time_ns
        train_cutoff = test_start_ns - embargo_ns
        train = tuple(
            cohort for cohort in ordered[:test_start_index] if cohort.launch_time_ns < train_cutoff
        )
        if len(train) >= min_train_size:
            folds.append(
                WalkForwardFold(
                    number=fold_number,
                    train=train,
                    test=tuple(test),
                    test_start_ns=test_start_ns,
                    test_end_ns=test[-1].launch_time_ns,
                )
            )
            fold_number += 1
        test_start_index += step
    return tuple(folds)
