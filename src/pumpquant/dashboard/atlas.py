"""Read-only queries over the sealed opportunity-atlas SQLite files."""

from __future__ import annotations

import math
import sqlite3
from pathlib import Path

from pumpquant.dashboard.config import (
    DashboardError,
)

ATLAS_DATABASE = "atlas.sqlite"


ATLAS_SIGNALS_DATABASE = "signals.sqlite"


ATLAS_IDEAS_DATABASE = "ideas.sqlite"


VELOCITY_DATABASE = "velocity.sqlite"


class AtlasError(DashboardError):
    """A sealed opportunity-atlas query failed validation."""


class AtlasStore:
    """Small read-only query surface over the sealed atlas SQLite file."""

    SORTS = {
        "multiple": "maximum_multiple DESC NULLS LAST, mint",
        "launch": "launch_time_ns DESC, mint",
        "market_cap": "peak_market_cap_sol DESC NULLS LAST, mint",
        "volume": "quote_volume_sol DESC NULLS LAST, mint",
    }

    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=3.0)
        connection.row_factory = sqlite3.Row
        return connection

    def summary(self) -> dict[str, object]:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT count(*) AS launches,
                       sum(has_any_swap) AS active,
                       sum(completed_bonding_curve) AS completed,
                       sum(migrated_to_pumpswap) AS migrated,
                       sum(maximum_multiple >= 2) AS reached_2x,
                       sum(maximum_multiple >= 5) AS reached_5x,
                       sum(maximum_multiple >= 10) AS reached_10x,
                       sum(maximum_multiple >= 20) AS reached_20x
                FROM tokens
                """
            ).fetchone()
            if row is None:
                raise AtlasError("atlas summary is unavailable")
            payload = dict(row)
            launches = int(payload["launches"] or 0)
            for target in (2, 5, 10, 20):
                count = int(payload[f"reached_{target}x"] or 0)
                payload[f"reached_{target}x_rate"] = count / launches if launches else None
            payload["active_rate"] = int(payload["active"] or 0) / launches if launches else None
            payload["completion_rate"] = (
                int(payload["completed"] or 0) / launches if launches else None
            )
            payload["basis"] = "gross reserve-mark paths; not executable strategy returns"
            return payload
        finally:
            connection.close()

    def tokens(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        sort: str = "multiple",
        minimum_multiple: float = 0.0,
    ) -> dict[str, object]:
        if not 1 <= limit <= 500 or offset < 0:
            raise AtlasError("atlas pagination is outside its bounds")
        order = self.SORTS.get(sort)
        if order is None:
            raise AtlasError("unsupported atlas sort")
        if not math.isfinite(minimum_multiple) or minimum_multiple < 0:
            raise AtlasError("minimum_multiple must be finite and non-negative")
        connection = self._connect()
        try:
            total = int(
                connection.execute(
                    "SELECT count(*) FROM tokens WHERE coalesce(maximum_multiple,0) >= ?",
                    (minimum_multiple,),
                ).fetchone()[0]
            )
            rows = connection.execute(
                f"""
                SELECT mint,name,symbol,launch_time_ns,has_any_swap,
                       completed_bonding_curve,migrated_to_pumpswap,
                       swap_event_count,quote_volume_sol,first_market_cap_sol,
                       peak_market_cap_sol,terminal_market_cap_sol,
                       maximum_multiple,minimum_multiple,terminal_multiple,
                       seconds_to_completion,seconds_to_2_0x,seconds_to_5_0x,
                       seconds_to_10_0x,seconds_to_20_0x
                FROM tokens
                WHERE coalesce(maximum_multiple,0) >= ?
                ORDER BY {order}
                LIMIT ? OFFSET ?
                """,
                (minimum_multiple, limit, offset),
            ).fetchall()
            return {
                "total": total,
                "limit": limit,
                "offset": offset,
                "sort": sort,
                "minimum_multiple": minimum_multiple,
                "tokens": [dict(row) for row in rows],
            }
        finally:
            connection.close()


class AtlasSignalsStore:
    """Read-only causal checkpoint lift queries over the sealed signals atlas."""

    CHECKPOINT_TYPES = frozenset({"age", "progress", "multiple", "lifecycle"})
    TARGETS = frozenset({2, 5, 10})

    def __init__(self, path: Path) -> None:
        self.path = path

    def patterns(
        self,
        *,
        checkpoint_type: str = "age",
        checkpoint_value: float = 60.0,
        target: int = 5,
        minimum_sample: int = 500,
        limit: int = 50,
    ) -> dict[str, object]:
        if checkpoint_type not in self.CHECKPOINT_TYPES:
            raise AtlasError("unsupported signal checkpoint type")
        if target not in self.TARGETS:
            raise AtlasError("unsupported signal target")
        if not math.isfinite(checkpoint_value) or checkpoint_value < 0:
            raise AtlasError("signal checkpoint value must be finite and non-negative")
        if minimum_sample < 1 or not 1 <= limit <= 200:
            raise AtlasError("signal query bounds are invalid")
        rate = f"future_{target}x_rate"
        baseline = f"checkpoint_{target}x_rate"
        lift = f"future_{target}x_lift"
        connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=3.0)
        connection.row_factory = sqlite3.Row
        try:
            rows = connection.execute(
                f"""
                SELECT checkpoint_type,checkpoint_value,feature,decile,sample_size,
                       feature_min,feature_median,feature_max,
                       {rate} AS outcome_rate,{baseline} AS baseline_rate,{lift} AS lift
                FROM segments
                WHERE checkpoint_type=? AND checkpoint_value=? AND sample_size>=?
                  AND {lift} IS NOT NULL
                ORDER BY {lift} DESC,sample_size DESC,feature,decile
                LIMIT ?
                """,
                (checkpoint_type, checkpoint_value, minimum_sample, limit),
            ).fetchall()
            return {
                "checkpoint_type": checkpoint_type,
                "checkpoint_value": checkpoint_value,
                "target": target,
                "minimum_sample": minimum_sample,
                "patterns": [dict(row) for row in rows],
                "basis": "causal first-crossing predictors; gross future reserve-mark outcome",
            }
        finally:
            connection.close()


class AtlasIdeasStore:
    """Read-only ranked development-mined opportunity ideas."""

    TARGETS = frozenset({2, 5, 10})
    KINDS = frozenset({"all", "single", "pair"})

    def __init__(self, path: Path) -> None:
        self.path = path

    def ideas(
        self,
        *,
        target: int = 5,
        age_seconds: float | None = None,
        idea_kind: str = "all",
        minimum_sample: int = 300,
        minimum_conservative_lift: float = 1.0,
        minimum_positive_ages: int = 2,
        limit: int = 100,
    ) -> dict[str, object]:
        if target not in self.TARGETS or idea_kind not in self.KINDS:
            raise AtlasError("unsupported opportunity idea filter")
        if age_seconds is not None and (not math.isfinite(age_seconds) or age_seconds < 0):
            raise AtlasError("idea age must be finite and non-negative")
        if minimum_sample < 1 or minimum_positive_ages < 1 or not 1 <= limit <= 500:
            raise AtlasError("idea query bounds are invalid")
        if not math.isfinite(minimum_conservative_lift) or minimum_conservative_lift < 0:
            raise AtlasError("idea conservative lift must be finite and non-negative")
        clauses = [
            "target_multiple=?",
            "sample_size>=?",
            "conservative_lift>=?",
            "positive_age_count>=?",
        ]
        parameters: list[object] = [
            target,
            minimum_sample,
            minimum_conservative_lift,
            minimum_positive_ages,
        ]
        if age_seconds is not None:
            clauses.append("age_seconds=?")
            parameters.append(age_seconds)
        if idea_kind != "all":
            clauses.append("idea_kind=?")
            parameters.append(idea_kind)
        where = " AND ".join(clauses)
        connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=3.0)
        connection.row_factory = sqlite3.Row
        try:
            total = int(
                connection.execute(
                    f"SELECT count(*) FROM ideas WHERE {where}", parameters
                ).fetchone()[0]
            )
            rows = connection.execute(
                f"""
                SELECT idea_id,idea_kind,age_seconds,target_multiple,signal_signature,
                       feature_a,direction_a,feature_a_min,feature_a_max,
                       feature_b,direction_b,feature_b_min,feature_b_max,
                       sample_size,hit_count,baseline_rate,outcome_rate,lift,
                       wilson_lower_rate,conservative_lift,positive_age_count,
                       median_age_lift,minimum_age_lift
                FROM ideas WHERE {where}
                ORDER BY conservative_lift DESC,sample_size DESC,idea_id LIMIT ?
                """,
                (*parameters, limit),
            ).fetchall()
            return {
                "total": total,
                "target": target,
                "age_seconds": age_seconds,
                "idea_kind": idea_kind,
                "ideas": [dict(row) for row in rows],
                "basis": "development-mined hypotheses; not execution-tested strategy profit",
            }
        finally:
            connection.close()


class VelocityAtlasStore:
    """Read-only market-cap speed and lifecycle outcome matrix."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def rows(self, *, threshold_usd: int = 10_000) -> dict[str, object]:
        if threshold_usd <= 0 or threshold_usd > 10_000_000:
            raise AtlasError("market-cap velocity threshold is outside its bounds")
        connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=3.0)
        connection.row_factory = sqlite3.Row
        try:
            rows = connection.execute(
                """
                SELECT threshold_usd,max_seconds,sample_size,completion_at_risk_tokens,
                       eligible_launches,
                       launch_reach_rate,at_risk_launch_reach_rate,
                       launch_observation_hours,signals_per_hour,at_risk_signals_per_hour,
                       completion_hits,completion_rate,
                       completion_base_rate,completion_lift,completion_wilson_lower_rate,
                       completion_300s_rate,completion_900s_rate,
                       completion_1800s_rate,completion_3600s_rate,
                       ath_50000_rate,ath_100000_rate,ath_200000_rate,
                       ath_500000_rate,ath_1000000_rate,
                       median_cross_seconds,median_observed_ath_usd,p90_observed_ath_usd,
                       median_ath_from_threshold,
                       at_risk_median_observed_ath_usd,
                       at_risk_p90_observed_ath_usd,
                       at_risk_median_ath_from_threshold,
                       median_seconds_to_completion
                FROM velocity WHERE threshold_usd=? ORDER BY max_seconds
                """,
                (threshold_usd,),
            ).fetchall()
            if not rows:
                raise AtlasError("unsupported market-cap velocity threshold")
            thresholds = [
                int(row[0])
                for row in connection.execute(
                    "SELECT DISTINCT threshold_usd FROM velocity ORDER BY threshold_usd"
                ).fetchall()
            ]
            return {
                "threshold_usd": threshold_usd,
                "thresholds": thresholds,
                "rows": [dict(row) for row in rows],
                "basis": "event-time USD market cap; observed development lifecycle and ATH",
            }
        finally:
            connection.close()
