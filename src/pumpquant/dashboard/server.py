"""Read-only live dashboard backend for PumpQuant.

The dashboard intentionally has a small, boring trust boundary.  A configured
artifact root can only be read after its manifest has been published and
verified.  The manifest is the seal: payload files are never discovered by a
directory walk, and every listed payload is checked for its declared size and
SHA-256 digest before it is parsed.

This module uses only the Python standard library.  It is useful both as a
small library (``LiveDashboard``/``verify_sealed_manifest``) and as the backend
used by :mod:`scripts.run_live_dashboard`.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import subprocess
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen

from pumpquant.dashboard.evidence import (
    TOKEN_EVIDENCE_DATABASE,
    TokenEvidenceError,
    TokenEvidenceStore,
)

__all__ = [
    "ATLAS_DATABASE",
    "ATLAS_SIGNALS_DATABASE",
    "ATLAS_IDEAS_DATABASE",
    "VELOCITY_DATABASE",
    "AtlasError",
    "AtlasSignalsStore",
    "AtlasIdeasStore",
    "VelocityAtlasStore",
    "AtlasStore",
    "ArtifactRoot",
    "ConfigError",
    "DashboardConfig",
    "DashboardError",
    "DashboardHTTPServer",
    "DashboardRequestHandler",
    "Event",
    "EventBroker",
    "LiveDashboard",
    "ManifestError",
    "ManifestReport",
    "MarketDataError",
    "MarketDataProvider",
    "VerifiedArtifact",
    "collect_service_telemetry",
    "create_server",
    "load_dashboard_config",
    "normalize_lifecycle",
    "verify_sealed_manifest",
]


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
DEFAULT_MANIFEST = "manifest.json"
LIFECYCLE_STATES = frozenset({"pending", "sealed", "error", "stale"})
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_SERVICE_RE = re.compile(r"^[A-Za-z0-9_.@:%+\-]+\.(?:service|timer|socket)$")
_NAME_RE = re.compile(r"^[A-Za-z0-9_.@%+\-]+$")
_HOLDOUT_RE = re.compile(r"(?:^|[^a-z0-9])holdout(?:$|[^a-z0-9])", re.I)
_MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
_COINBASE_SOL_USD_URL = "https://api.coinbase.com/v2/prices/SOL-USD/spot"
_SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"
ATLAS_DATABASE = "atlas.sqlite"
ATLAS_SIGNALS_DATABASE = "signals.sqlite"
ATLAS_IDEAS_DATABASE = "ideas.sqlite"
VELOCITY_DATABASE = "velocity.sqlite"

# ``systemctl show`` is deliberately constrained.  In particular, no
# Environment/EnvironmentFile/ExecStart command line fields are requested.
SYSTEMCTL_FIELDS = (
    "Id",
    "ActiveState",
    "SubState",
    "Result",
    "MainPID",
    "ExecMainStartTimestamp",
    "CPUUsageNSec",
    "MemoryCurrent",
    "TasksCurrent",
    "NRestarts",
    "StatusText",
)

_FAMILY_ALIASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "shock_recovery",
        (
            "shock_recovery",
            "shock-recovery",
            "shockrecovery",
        ),
    ),
    (
        "progress_speed",
        (
            "progress_speed",
            "progress-speed",
            "progressspeed",
        ),
    ),
    (
        "persistent_pressure",
        (
            "persistent_pressure",
            "persistent-pressure",
            "persistentpressure",
        ),
    ),
    (
        "amm_impact_ofi",
        (
            "amm_impact_ofi",
            "amm-impact-ofi",
            "ammimpactofi",
            "impact_ofi",
        ),
    ),
)


class DashboardError(RuntimeError):
    """Base class for dashboard configuration and artifact errors."""


class ConfigError(DashboardError, ValueError):
    """Raised when a dashboard configuration would widen the read boundary."""


class ManifestError(DashboardError, ValueError):
    """Raised for an invalid, incomplete, or tampered manifest."""


class MarketDataError(DashboardError):
    """A bounded external market-data lookup failed validation or retrieval."""


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


def _fetch_market_json(url: str, body: bytes | None = None) -> Mapping[str, object]:
    request = Request(
        url,
        data=body,
        method="POST" if body is not None else "GET",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "PumpQuant-dashboard/1",
        },
    )
    with urlopen(request, timeout=4.0) as response:  # noqa: S310 - constant URLs only
        raw = response.read(1_000_001)
    if len(raw) > 1_000_000:
        raise MarketDataError("market-data response exceeds one megabyte")
    payload = json.loads(raw)
    if not isinstance(payload, Mapping):
        raise MarketDataError("market-data response is not an object")
    return payload


class MarketDataProvider:
    """Thread-safe, stale-aware cache for live SOL/USD and SPL token supply."""

    def __init__(
        self,
        *,
        fetch_json: Callable[[str, bytes | None], Mapping[str, object]] | None = None,
        clock: Callable[[], float] | None = None,
        fx_ttl_seconds: float = 30.0,
        supply_ttl_seconds: float = 21_600.0,
    ) -> None:
        self._fetch_json = fetch_json or _fetch_market_json
        self._clock = clock or time.time
        self._fx_ttl = max(1.0, float(fx_ttl_seconds))
        self._supply_ttl = max(60.0, float(supply_ttl_seconds))
        self._lock = threading.RLock()
        self._fx_cache: tuple[float, float] | None = None
        self._supply_cache: dict[str, tuple[float, float, int]] = {}

    def sol_usd(self) -> dict[str, object]:
        now = self._clock()
        with self._lock:
            cached = self._fx_cache
            if cached is not None and now - cached[0] < self._fx_ttl:
                return self._fx_payload(cached, now, stale=False)
            try:
                payload = self._fetch_json(_COINBASE_SOL_USD_URL, None)
                data = payload.get("data")
                if not isinstance(data, Mapping):
                    raise MarketDataError("Coinbase spot response has no data object")
                value = float(data.get("amount", 0.0))
                if not math.isfinite(value) or value <= 0.0:
                    raise MarketDataError("Coinbase SOL/USD spot is invalid")
                cached = (now, value)
                self._fx_cache = cached
                return self._fx_payload(cached, now, stale=False)
            except (OSError, ValueError, TypeError, json.JSONDecodeError, MarketDataError) as exc:
                if cached is None:
                    raise MarketDataError(f"SOL/USD spot unavailable: {exc}") from exc
                return self._fx_payload(cached, now, stale=True)

    @staticmethod
    def _fx_payload(cached: tuple[float, float], now: float, *, stale: bool) -> dict[str, object]:
        fetched_at, value = cached
        return {
            "pair": "SOL-USD",
            "sol_usd": value,
            "fetched_at_epoch": fetched_at,
            "age_seconds": max(0.0, now - fetched_at),
            "stale": stale,
            "source": _COINBASE_SOL_USD_URL,
        }

    def token_supply(self, mint: str) -> dict[str, object]:
        if not isinstance(mint, str) or not _MINT_RE.fullmatch(mint):
            raise TokenEvidenceError("mint must be a 32-44 character base58 public key")
        now = self._clock()
        with self._lock:
            cached = self._supply_cache.get(mint)
            if cached is not None and now - cached[0] < self._supply_ttl:
                return self._supply_payload(mint, cached, now, stale=False)
            request_body = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTokenSupply",
                    "params": [mint, {"commitment": "finalized"}],
                },
                separators=(",", ":"),
            ).encode()
            try:
                payload = self._fetch_json(_SOLANA_RPC_URL, request_body)
                result = payload.get("result")
                if not isinstance(result, Mapping):
                    raise MarketDataError("Solana token-supply response has no result")
                context = result.get("context")
                value = result.get("value")
                if not isinstance(context, Mapping) or not isinstance(value, Mapping):
                    raise MarketDataError("Solana token-supply result is incomplete")
                supply = float(value.get("uiAmountString", 0.0))
                slot = int(context.get("slot", 0))
                if not math.isfinite(supply) or supply <= 0.0 or slot <= 0:
                    raise MarketDataError("Solana token supply or context slot is invalid")
                cached = (now, supply, slot)
                self._supply_cache[mint] = cached
                return self._supply_payload(mint, cached, now, stale=False)
            except (OSError, ValueError, TypeError, json.JSONDecodeError, MarketDataError) as exc:
                if cached is None:
                    raise MarketDataError(f"token supply unavailable: {exc}") from exc
                return self._supply_payload(mint, cached, now, stale=True)

    @staticmethod
    def _supply_payload(
        mint: str, cached: tuple[float, float, int], now: float, *, stale: bool
    ) -> dict[str, object]:
        fetched_at, supply, slot = cached
        return {
            "mint": mint,
            "token_supply": supply,
            "slot": slot,
            "fetched_at_epoch": fetched_at,
            "age_seconds": max(0.0, now - fetched_at),
            "stale": stale,
            "source": _SOLANA_RPC_URL,
        }


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime | None = None) -> str:
    value = value or _utc_now()
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _jsonable(value: Any) -> Any:
    """Return a deterministic JSON-safe copy of a payload.

    Artifact payloads are expected to be JSON, but recursively filtering them
    protects the HTTP encoder from accidental non-JSON values in test doubles
    or callers constructing a snapshot by hand.
    """

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return str(value)


def _canonical_json(value: Any) -> str:
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"))


def normalize_lifecycle(value: object) -> str:
    """Normalize producer-specific lifecycle labels.

    Unknown labels are intentionally treated as ``pending``.  A dashboard
    must never turn an unfamiliar state into an apparently sealed result.
    """

    if isinstance(value, str):
        normalized = value.strip().lower().replace("_", "-")
        aliases = {
            "complete": "sealed",
            "completed": "sealed",
            "success": "sealed",
            "ok": "sealed",
            "published": "sealed",
            "sealed": "sealed",
            "stale": "stale",
            "error": "error",
            "failed": "error",
            "failure": "error",
            "pending": "pending",
            "running": "pending",
            "in-progress": "pending",
            "partial": "pending",
            "waiting": "pending",
        }
        return aliases.get(normalized, "pending")
    return "pending"


def _contains_holdout(value: str | Path) -> bool:
    """Detect a holdout path component without false positives in temp dirs.

    Test runners and ordinary workspaces often include words such as
    ``test_holdout_*`` in an ancestor directory.  A root is forbidden when it
    itself is named ``holdout``/``holdout-*``/``holdout_*``; an unrelated
    ancestor containing the word is not an included holdout root.
    """

    text = str(value).replace("\\", "/")
    parts = [part for part in text.split("/") if part]
    leaf = parts[-1].lower() if parts else ""
    return (
        any(
            part.lower() == "holdout"
            or part.lower().startswith("holdout-")
            or part.lower().startswith("holdout_")
            for part in parts
        )
        or "holdout" in leaf
    )


def _resolve_config_path(raw: object, *, base_dir: Path, label: str) -> Path:
    if not isinstance(raw, str) or not raw.strip():
        raise ConfigError(f"{label} must be a non-empty path string")
    candidate = Path(raw)
    candidate_path = base_dir / candidate if not candidate.is_absolute() else candidate
    if candidate_path.exists() and candidate_path.is_symlink():
        raise ConfigError(f"{label} must not be a symlink")
    # A root is exact and may be absent while a service is pending, but a
    # symlink that already exists is rejected so it cannot redirect later.
    resolved = (base_dir / candidate if not candidate.is_absolute() else candidate).resolve(
        strict=False
    )
    if _contains_holdout(resolved) or _contains_holdout(raw):
        raise ConfigError(f"{label} points at a holdout root, which is forbidden")
    return resolved


@dataclass(frozen=True)
class ArtifactRoot:
    """One exact, allowlisted artifact root."""

    name: str
    path: Path
    manifest_name: str = DEFAULT_MANIFEST

    def __post_init__(self) -> None:
        raw_path = Path(self.path)
        if raw_path.exists() and raw_path.is_symlink():
            raise ConfigError(f"artifact root is a symlink: {raw_path}")
        path = raw_path.resolve(strict=False)
        object.__setattr__(self, "path", path)
        if not _NAME_RE.fullmatch(self.name):
            raise ConfigError(f"invalid artifact root name: {self.name!r}")
        if "holdout" in self.name.lower() or _contains_holdout(path):
            raise ConfigError("holdout roots are not allowed")
        manifest = Path(self.manifest_name)
        if (
            manifest.is_absolute()
            or len(manifest.parts) != 1
            or manifest.name in {".", ".."}
            or _contains_holdout(manifest.name)
        ):
            raise ConfigError("manifest_name must be one safe file name")
        if self.path.exists() and self.path.is_symlink():
            raise ConfigError(f"artifact root is a symlink: {self.path}")


@dataclass(frozen=True)
class DashboardConfig:
    """Validated dashboard configuration.

    ``artifact_roots`` and ``service_names`` are immutable tuples/maps after
    construction.  The class accepts mappings for ergonomic test setup while
    copying them so callers cannot mutate the read boundary under a running
    server.
    """

    artifact_roots: Mapping[str, Path] = field(default_factory=dict)
    service_names: tuple[str, ...] = ()
    static_root: Path = Path("dashboard/static")
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    manifest_name: str = DEFAULT_MANIFEST
    stale_after_seconds: float = 24 * 60 * 60
    poll_interval_seconds: float = 5.0

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host.strip():
            raise ConfigError("host must be a non-empty string")
        if not isinstance(self.port, int) or not 1 <= self.port <= 65535:
            raise ConfigError("port must be between 1 and 65535")
        if self.stale_after_seconds < 0:
            raise ConfigError("stale_after_seconds must be non-negative")
        if self.poll_interval_seconds <= 0:
            raise ConfigError("poll_interval_seconds must be positive")
        manifest = Path(self.manifest_name)
        if manifest.is_absolute() or len(manifest.parts) != 1 or _contains_holdout(manifest.name):
            raise ConfigError("manifest_name must be one safe file name")
        roots: dict[str, Path] = {}
        for name, raw_path in dict(self.artifact_roots).items():
            if (
                not isinstance(name, str)
                or not _NAME_RE.fullmatch(name)
                or "holdout" in name.lower()
            ):
                raise ConfigError(f"invalid artifact root name: {name!r}")
            original_path = Path(raw_path)
            if original_path.exists() and original_path.is_symlink():
                raise ConfigError(f"artifact root {name!r} must not be a symlink")
            path = original_path.resolve(strict=False)
            if _contains_holdout(path):
                raise ConfigError(f"artifact root {name!r} points at a holdout")
            if path.exists() and path.is_symlink():
                raise ConfigError(f"artifact root {name!r} must not be a symlink")
            roots[name] = path
        services: list[str] = []
        for service in tuple(self.service_names):
            if (
                not isinstance(service, str)
                or not _SERVICE_RE.fullmatch(service)
                or "holdout" in service.lower()
            ):
                raise ConfigError(f"invalid systemd service name: {service!r}")
            if service not in services:
                services.append(service)
        static = Path(self.static_root).resolve(strict=False)
        if static.exists() and static.is_symlink():
            raise ConfigError("static_root must not be a symlink")
        object.__setattr__(self, "artifact_roots", roots)
        object.__setattr__(self, "service_names", tuple(services))
        object.__setattr__(self, "static_root", static)
        object.__setattr__(self, "manifest_name", manifest.name)

    @classmethod
    def from_mapping(
        cls, raw: Mapping[str, object], *, base_dir: Path | None = None
    ) -> DashboardConfig:
        if not isinstance(raw, Mapping):
            raise ConfigError("dashboard config must be a JSON object")
        for key in raw:
            if isinstance(key, str) and "holdout" in key.lower():
                raise ConfigError(f"dashboard config must not include holdout field: {key}")
        base = (base_dir or Path.cwd()).resolve()
        roots_raw = raw.get(
            "artifact_roots", raw.get("artifact_allowlist", raw.get("artifacts", {}))
        )
        if not isinstance(roots_raw, Mapping):
            raise ConfigError("artifact_roots must be an object mapping names to exact paths")
        roots: dict[str, Path] = {}
        for name, value in roots_raw.items():
            if not isinstance(name, str):
                raise ConfigError("artifact root names must be strings")
            roots[name] = _resolve_config_path(value, base_dir=base, label=f"artifact_roots.{name}")

        services_raw = raw.get(
            "service_names", raw.get("systemd_services", raw.get("services", ()))
        )
        if isinstance(services_raw, Mapping):
            services_raw = services_raw.get(
                "allowlist", services_raw.get("names", tuple(services_raw.values()))
            )
        if not isinstance(services_raw, Sequence) or isinstance(services_raw, (str, bytes)):
            raise ConfigError("service_names must be an array of exact systemd unit names")
        services = tuple(services_raw)
        static_raw = raw.get("static_root", "dashboard/static")
        static = _resolve_config_path(static_raw, base_dir=base, label="static_root")
        port_raw = raw.get("port", DEFAULT_PORT)
        stale_raw = raw.get("stale_after_seconds", 24 * 60 * 60)
        poll_raw = raw.get("poll_interval_seconds", 5.0)
        if isinstance(port_raw, bool) or not isinstance(port_raw, (int, str)):
            raise ConfigError("port must be an integer")
        if isinstance(stale_raw, bool) or not isinstance(stale_raw, (int, float, str)):
            raise ConfigError("stale_after_seconds must be numeric")
        if isinstance(poll_raw, bool) or not isinstance(poll_raw, (int, float, str)):
            raise ConfigError("poll_interval_seconds must be numeric")
        try:
            port = int(port_raw)
            stale_after_seconds = float(stale_raw)
            poll_interval_seconds = float(poll_raw)
        except (TypeError, ValueError) as exc:
            raise ConfigError("port and timing values must be numeric") from exc
        return cls(
            artifact_roots=roots,
            service_names=services,
            static_root=static,
            host=str(raw.get("host", DEFAULT_HOST)),
            port=port,
            manifest_name=str(raw.get("manifest_name", DEFAULT_MANIFEST)),
            stale_after_seconds=stale_after_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )


def load_dashboard_config(path: str | Path | None = None) -> DashboardConfig:
    """Load and validate a JSON dashboard config.

    Relative paths are resolved relative to the config file, not the process's
    current directory.  With no file, an empty allowlist is returned; this is
    safe (the server serves a pending dashboard) and avoids guessing artifact
    locations.
    """

    if path is None:
        return DashboardConfig(static_root=(Path.cwd() / "dashboard" / "static").resolve())
    original_config_path = Path(path)
    if original_config_path.exists() and original_config_path.is_symlink():
        raise ConfigError("config file must not be a symlink")
    config_path = original_config_path.resolve(strict=False)
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"dashboard config does not exist: {config_path}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"unable to read dashboard config: {config_path}") from exc
    return DashboardConfig.from_mapping(raw, base_dir=config_path.parent)


def _safe_relative_path(raw: object) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ManifestError("manifest artifact path must be a non-empty string")
    # POSIX artifact manifests are portable; reject Windows separators too so a
    # path authored on another host cannot become traversal on this one.
    if "\\" in raw:
        raise ManifestError(f"unsafe artifact path: {raw!r}")
    relative = Path(raw)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise ManifestError(f"unsafe artifact path: {raw!r}")
    normalized = relative.as_posix()
    if _contains_holdout(normalized):
        raise ManifestError("manifest references a holdout artifact")
    return normalized


def _digest_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            count += len(block)
            digest.update(block)
    return count, digest.hexdigest()


def _path_has_symlink(path: Path, root: Path) -> bool:
    """Return true when any component below ``root`` is a symlink."""

    cursor = root
    if cursor.is_symlink():
        return True
    try:
        relative = path.relative_to(root)
    except ValueError:
        return True
    for component in relative.parts:
        cursor = cursor / component
        if cursor.is_symlink():
            return True
    return False


def _manifest_entries(manifest: Mapping[str, object]) -> list[Mapping[str, object]]:
    raw = manifest.get("artifacts", manifest.get("files", ()))
    if isinstance(raw, Mapping):
        entries: list[Mapping[str, object]] = []
        for path, metadata in raw.items():
            if not isinstance(metadata, Mapping):
                raise ManifestError("manifest files mapping values must be objects")
            entries.append({**dict(metadata), "path": path})
        return entries
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ManifestError("complete manifest must list artifacts")
    entries = []
    for item in raw:
        if isinstance(item, str):
            raise ManifestError("artifact entries need byte_count and sha256")
        if not isinstance(item, Mapping):
            raise ManifestError("manifest artifact entries must be objects")
        entries.append(item)
    return entries


def _entry_size(entry: Mapping[str, object]) -> int:
    value = entry.get("byte_count", entry.get("bytes", entry.get("size")))
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ManifestError("manifest artifact byte_count must be a non-negative integer")
    return value


def _entry_hash(entry: Mapping[str, object]) -> str:
    value = entry.get("sha256", entry.get("hash"))
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ManifestError("manifest artifact sha256 must be a 64-character hex digest")
    return value.lower()


@dataclass(frozen=True)
class VerifiedArtifact:
    relative_path: str
    path: Path
    byte_count: int
    sha256: str
    metadata: Mapping[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.relative_path,
            "byte_count": self.byte_count,
            "sha256": self.sha256,
            **_jsonable(dict(self.metadata)),
        }


@dataclass(frozen=True)
class ManifestReport:
    root: Path
    manifest_path: Path
    lifecycle: str
    manifest_sha256: str | None = None
    manifest_byte_count: int | None = None
    manifest: Mapping[str, object] = field(default_factory=dict)
    artifacts: tuple[VerifiedArtifact, ...] = ()
    error: str | None = None
    updated_at: str | None = None

    @property
    def status(self) -> str:
        return self.lifecycle

    def as_dict(self, *, include_root: bool = True) -> dict[str, object]:
        result: dict[str, object] = {
            "lifecycle": self.lifecycle,
            "status": self.lifecycle,
            "manifest": {
                "path": self.manifest_path.name,
                "sha256": self.manifest_sha256,
                "byte_count": self.manifest_byte_count,
                "status": (
                    self.manifest.get(
                        "status", self.manifest.get("lifecycle", self.manifest.get("state"))
                    )
                    if self.manifest
                    else None
                ),
                "updated_at": self.updated_at,
            },
            "artifacts": [item.as_dict() for item in self.artifacts],
            "error": self.error,
        }
        if include_root:
            result["root"] = str(self.root)
        return result

    def __getitem__(self, key: str) -> object:
        """Provide a small mapping-compatible view for callers/tests."""

        if key == "lifecycle" or key == "status":
            return self.lifecycle
        return self.as_dict()[key]

    def get(self, key: str, default: object = None) -> object:
        try:
            return self[key]
        except KeyError:
            return default


def _complete_status(manifest: Mapping[str, object]) -> bool:
    status = manifest.get("status", manifest.get("lifecycle", manifest.get("state")))
    if normalize_lifecycle(status) == "sealed":
        return True
    return manifest.get("sealed") is True or manifest.get("complete") is True


def verify_sealed_manifest(
    root: str | Path,
    *,
    manifest_name: str = DEFAULT_MANIFEST,
    stale_after_seconds: float = 24 * 60 * 60,
    now: float | None = None,
) -> ManifestReport:
    """Inspect one exact root and verify its manifest and listed artifacts.

    Missing roots/manifests and explicitly non-complete manifests are pending.
    Malformed manifests, traversal, symlinks, missing files, size mismatches,
    and digest mismatches are errors.  A complete manifest whose mtime exceeds
    ``stale_after_seconds`` remains fully verified but is labelled stale.
    """

    original_root_path = Path(root)
    if original_root_path.exists() and original_root_path.is_symlink():
        return ManifestReport(
            root=original_root_path.resolve(strict=False),
            manifest_path=original_root_path / manifest_name,
            lifecycle="error",
            error="artifact root must not be a symlink",
        )
    root_path = original_root_path.resolve(strict=False)
    manifest_path = root_path / manifest_name
    if _contains_holdout(root_path) or _contains_holdout(manifest_name):
        return ManifestReport(
            root=root_path,
            manifest_path=manifest_path,
            lifecycle="error",
            error="holdout roots are forbidden",
        )
    try:
        if not root_path.exists() or not root_path.is_dir():
            return ManifestReport(root=root_path, manifest_path=manifest_path, lifecycle="pending")
        if root_path.is_symlink() or not manifest_path.exists() or not manifest_path.is_file():
            return ManifestReport(root=root_path, manifest_path=manifest_path, lifecycle="pending")
        if manifest_path.is_symlink():
            raise ManifestError("manifest must not be a symlink")
        raw = manifest_path.read_bytes()
        manifest_sha256 = hashlib.sha256(raw).hexdigest()
        if not raw:
            raise ManifestError("manifest is empty")
        parsed = json.loads(raw.decode("utf-8"))
        if not isinstance(parsed, Mapping):
            raise ManifestError("manifest must be a JSON object")
        if not _complete_status(parsed):
            return ManifestReport(
                root=root_path,
                manifest_path=manifest_path,
                lifecycle="pending",
                manifest_sha256=manifest_sha256,
                manifest_byte_count=len(raw),
                manifest=dict(parsed),
                updated_at=_file_updated_at(manifest_path),
            )
        entries = _manifest_entries(parsed)
        verified: list[VerifiedArtifact] = []
        seen: set[str] = set()
        for entry in entries:
            entry_status = entry.get("status", entry.get("lifecycle"))
            if entry_status is not None and normalize_lifecycle(entry_status) != "sealed":
                raise ManifestError("manifest lists an artifact that is not sealed")
            relative = _safe_relative_path(
                entry.get("path", entry.get("relative_path", entry.get("name")))
            )
            if relative in seen:
                raise ManifestError(f"manifest repeats artifact path: {relative}")
            seen.add(relative)
            path = root_path / relative
            # ``resolve`` catches a symlink nested below an otherwise valid
            # root.  The final path must remain below the exact root.
            resolved = path.resolve(strict=False)
            try:
                resolved.relative_to(root_path)
            except ValueError as exc:
                raise ManifestError(f"artifact path escapes root: {relative}") from exc
            if _path_has_symlink(path, root_path) or not path.exists() or not path.is_file():
                raise ManifestError(f"manifested artifact is missing or symlinked: {relative}")
            expected_size = _entry_size(entry)
            expected_hash = _entry_hash(entry)
            actual_size, actual_hash = _digest_file(path)
            if actual_size != expected_size:
                raise ManifestError(
                    f"artifact byte count mismatch for {relative}: {actual_size} != {expected_size}"
                )
            if actual_hash != expected_hash:
                raise ManifestError(f"artifact sha256 mismatch for {relative}")
            verified.append(
                VerifiedArtifact(
                    relative_path=relative,
                    path=path,
                    byte_count=actual_size,
                    sha256=actual_hash,
                    metadata=dict(entry),
                )
            )
        age = max(0.0, (now if now is not None else time.time()) - manifest_path.stat().st_mtime)
        lifecycle = "stale" if age > stale_after_seconds else "sealed"
        return ManifestReport(
            root=root_path,
            manifest_path=manifest_path,
            lifecycle=lifecycle,
            manifest_sha256=manifest_sha256,
            manifest_byte_count=len(raw),
            manifest=dict(parsed),
            artifacts=tuple(verified),
            updated_at=_file_updated_at(manifest_path),
        )
    except (OSError, UnicodeError, json.JSONDecodeError, ManifestError) as exc:
        return ManifestReport(
            root=root_path,
            manifest_path=manifest_path,
            lifecycle="error",
            error=str(exc),
        )


def _file_updated_at(path: Path) -> str | None:
    try:
        return _iso(datetime.fromtimestamp(path.stat().st_mtime, UTC))
    except OSError:
        return None


def _parse_payload(artifact: VerifiedArtifact) -> tuple[str, object] | None:
    suffix = artifact.path.suffix.lower()
    if suffix not in {".json", ".jsonl", ".ndjson"}:
        return None
    try:
        raw = artifact.path.read_text(encoding="utf-8")
        if suffix == ".json":
            value = json.loads(raw)
        else:
            value = [json.loads(line) for line in raw.splitlines() if line.strip()]
    except (OSError, UnicodeError, json.JSONDecodeError):
        # A verified non-JSON payload is still a sealed artifact.  It simply
        # cannot contribute structured dashboard fields.
        return None
    return artifact.relative_path, value


def _payload_key(path: str) -> str:
    return Path(path).stem.lower().replace("-", "_")


def _family_for(value: str) -> str | None:
    lowered = value.lower().replace("/", "_")
    for family, aliases in _FAMILY_ALIASES:
        if any(alias in lowered for alias in aliases):
            return family
    return None


def _as_list(value: object) -> list[object]:
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if value is None:
        return []
    return [_jsonable(value)]


def _metric_rows(value: object) -> list[object]:
    """Extract metric records from a JSON object or a report wrapper."""

    if isinstance(value, list):
        return _as_list(value)
    if not isinstance(value, Mapping):
        return []
    if any(key in value for key in ("candidate", "metrics", "evaluation")):
        return [_jsonable(value)]
    for key in ("rows", "ledger", "trials", "metrics", "candidates"):
        nested = value.get(key)
        if isinstance(nested, (list, Mapping)):
            rows = _metric_rows(nested)
            if rows:
                return rows
    return []


def _flatten_metric_candidate(item: Mapping[str, object]) -> dict[str, object] | None:
    """Flatten a metric-ledger row into the dashboard's candidate shape.

    The expanded event grid intentionally stores provenance under ``candidate``
    (and the statistics under ``metrics``) so the ledger is self-describing.
    The GUI needs one row with both pieces, while preserving every original
    field for audit/detail views.
    """

    candidate = item.get("candidate")
    metrics = item.get("metrics")
    # The full walk-forward/alpha runner predates the event grid and calls
    # this object ``evaluation``.  Its Performance dataclass is nested one
    # level deeper, so normalize both producer contracts before the browser
    # sees them.
    evaluation = item.get("evaluation")
    backtest = item.get("backtest")
    candidate_map = candidate if isinstance(candidate, Mapping) else {}
    evaluation_map = evaluation if isinstance(evaluation, Mapping) else {}
    metrics_map = metrics if isinstance(metrics, Mapping) else {}
    performance = evaluation_map.get("performance")
    performance_map = performance if isinstance(performance, Mapping) else {}
    backtest_map = backtest if isinstance(backtest, Mapping) else {}
    if not candidate_map and not metrics_map and not evaluation_map:
        return None
    axis = candidate_map.get("axis")
    axis_map = axis if isinstance(axis, Mapping) else {}
    flattened: dict[str, object] = {
        **_jsonable(dict(candidate_map)),
        **_jsonable(dict(axis_map)),
        **_jsonable(dict(evaluation_map)),
        **_jsonable(dict(performance_map)),
        **_jsonable(dict(metrics_map)),
        **_jsonable(dict(backtest_map)),
        **_jsonable(dict(item)),
    }
    # Keep the candidate's stable display identity and provenance explicit;
    # ``item`` itself may otherwise only contain a hash and a nested object.
    if "candidate" not in flattened:
        flattened["candidate"] = candidate_map.get(
            "name", candidate_map.get("sha256", candidate_map.get("candidate_sha256"))
        )
    if "candidate_sha256" not in flattened:
        flattened["candidate_sha256"] = candidate_map.get(
            "sha256",
            evaluation_map.get("candidate_sha256", candidate_map.get("candidate_sha256")),
        )
    if "family" not in flattened:
        flattened["family"] = axis_map.get("family")
    if (
        "candidate" not in flattened
        or flattened.get("candidate") in (None, "")
        or isinstance(flattened.get("candidate"), Mapping)
    ):
        flattened["candidate"] = (
            candidate_map.get("name")
            or candidate_map.get("sha256")
            or evaluation_map.get("candidate_sha256")
            or "Unnamed research"
        )
    return flattened


def _tail_exit_candidate_summaries(
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]] | None:
    """Return one exact additive OOS summary per registered tail/exit idea.

    The live progress snapshot and final metric ledger intentionally contain
    the same fold records. Deduplicate them first, then combine only test-fold
    rows. Additive statistics and pooled event variance remain exact; fields
    without sufficient components are left untouched or null.
    """

    if not any(row.get("kind") == "pumpquant.fresh_tail_exit_metric" for row in rows):
        return None
    # The same scientific row can arrive through the live snapshot and the
    # sealed ledger with different publication metadata. Its identity is the
    # registered candidate and chronological fold/split, not wrapper bytes.
    unique_by_fold: dict[tuple[str, str, str], Mapping[str, object]] = {}
    for row in rows:
        if row.get("kind") != "pumpquant.fresh_tail_exit_metric":
            continue
        candidate_id = row.get("candidate_sha256")
        fold = row.get("fold")
        split = row.get("split")
        if not isinstance(candidate_id, str) or not candidate_id:
            continue
        identity = (candidate_id, str(fold), str(split))
        unique_by_fold[identity] = row
    grouped: dict[str, list[Mapping[str, object]]] = {}
    for row in unique_by_fold.values():
        candidate_id = row.get("candidate_sha256")
        assert isinstance(candidate_id, str)
        grouped.setdefault(candidate_id, []).append(row)

    def number(row: Mapping[str, object], key: str) -> float | None:
        value = row.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None

    result: list[dict[str, object]] = []
    for candidate_id, candidate_rows in grouped.items():
        test_rows = [row for row in candidate_rows if row.get("split") == "test"]
        selected = test_rows or candidate_rows
        first = dict(selected[0])
        outcomes = sum(int(number(row, "outcomes") or 0) for row in selected)
        mints = sum(int(number(row, "mints") or 0) for row in selected)
        wins = sum(int(number(row, "wins") or 0) for row in selected)
        losses = sum(int(number(row, "losses") or 0) for row in selected)
        breakevens = sum(int(number(row, "breakevens") or 0) for row in selected)
        total_pnl = sum(number(row, "total_quote_pnl") or 0.0 for row in selected)
        gross_profit = sum(number(row, "gross_profit_quote") or 0.0 for row in selected)
        gross_loss = sum(number(row, "gross_loss_quote") or 0.0 for row in selected)
        mean_event = total_pnl / outcomes if outcomes else None
        sum_squares = 0.0
        downside_squares = 0.0
        variance_complete = outcomes >= 2
        downside_complete = outcomes > 0
        for row in selected:
            count = int(number(row, "outcomes") or 0)
            mean = number(row, "mean_event_pnl_quote")
            std = number(row, "pnl_std_quote")
            downside = number(row, "downside_deviation_quote")
            if count >= 2 and mean is not None and std is not None:
                sum_squares += (count - 1) * std * std + count * mean * mean
            else:
                variance_complete = False
            if count > 0 and downside is not None:
                downside_squares += count * downside * downside
            else:
                downside_complete = False
        pnl_std = (
            math.sqrt(max(0.0, (sum_squares - outcomes * mean_event * mean_event) / (outcomes - 1)))
            if variance_complete and mean_event is not None
            else None
        )
        downside_deviation = (
            math.sqrt(max(0.0, downside_squares / outcomes)) if downside_complete else None
        )
        average_win = gross_profit / wins if wins else None
        average_loss = gross_loss / losses if losses else None
        first.update(
            {
                "candidate": first.get("candidate", candidate_id),
                "candidate_sha256": candidate_id,
                "currency": "SOL",
                "quote_currency": "SOL",
                "split": "test",
                "fold": f"{len(selected)} OOS folds",
                "fold_count": len(selected),
                "outcomes": outcomes,
                "trades": outcomes,
                "total_trades": outcomes,
                "mints": mints,
                "total_quote_pnl": total_pnl,
                "mean_event_pnl_quote": mean_event,
                "expectancy_quote": mean_event,
                "wins": wins,
                "losses": losses,
                "breakevens": breakevens,
                "win_rate": wins / (wins + losses) if wins + losses else None,
                "loss_rate": losses / (wins + losses) if wins + losses else None,
                "average_win_quote": average_win,
                "average_loss_quote": average_loss,
                "payoff_ratio": (
                    average_win / average_loss
                    if average_win is not None and average_loss is not None and average_loss > 0.0
                    else None
                ),
                "gross_profit_quote": gross_profit,
                "gross_loss_quote": gross_loss,
                "profit_factor": gross_profit / gross_loss if gross_loss > 0.0 else None,
                "pnl_std_quote": pnl_std,
                "downside_deviation_quote": downside_deviation,
                "sharpe_per_trade": (
                    mean_event / pnl_std
                    if mean_event is not None and pnl_std is not None and pnl_std > 0.0
                    else None
                ),
                "sortino_per_trade": (
                    mean_event / downside_deviation
                    if mean_event is not None
                    and downside_deviation is not None
                    and downside_deviation > 0.0
                    else None
                ),
                "source_metric_rows": len(selected),
                "status": "oos_aggregate_complete",
            }
        )
        result.append(first)
    return sorted(result, key=lambda row: str(row.get("candidate", "")))


def _section_status(available: bool, reports: Sequence[ManifestReport]) -> str:
    if not available:
        return "pending"
    if any(report.lifecycle == "error" for report in reports):
        return "error"
    if any(report.lifecycle == "stale" for report in reports):
        return "stale"
    return "sealed"


class EventBroker:
    """Small in-process event log used by the SSE endpoint and tests."""

    def __init__(self, *, max_events: int = 256) -> None:
        self._events: deque[Event] = deque(maxlen=max(1, max_events))
        self._condition = threading.Condition()
        self._next_id = 1

    def publish(self, event: str, data: Mapping[str, object]) -> Event:
        with self._condition:
            item = Event(self._next_id, event, _jsonable(dict(data)), _iso())
            self._next_id += 1
            self._events.append(item)
            self._condition.notify_all()
            return item

    def latest(self) -> Event | None:
        with self._condition:
            return self._events[-1] if self._events else None

    def since(self, event_id: int | None = None) -> list[Event]:
        with self._condition:
            if event_id is None:
                return list(self._events)
            return [event for event in self._events if event.id > event_id]

    def wait_since(self, event_id: int | None, timeout: float | None = None) -> list[Event]:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while True:
                events = [
                    event for event in self._events if event_id is None or event.id > event_id
                ]
                if events:
                    return events
                if deadline is None:
                    self._condition.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return []
                self._condition.wait(remaining)

    def __iter__(self) -> Iterator[Event]:
        return iter(self.since())


@dataclass(frozen=True)
class Event:
    id: int
    event: str
    data: object
    created_at: str

    def sse(self) -> bytes:
        body = _canonical_json(self.data)
        event_line = "" if self.event in {"", "message", "snapshot"} else f"event: {self.event}\n"
        return (f"id: {self.id}\n{event_line}data: {body}\n\n").encode()


def _read_proc(pid: int) -> dict[str, object]:
    if pid <= 0:
        return {}
    result: dict[str, object] = {"pid": pid}
    status_path = Path("/proc") / str(pid) / "status"
    try:
        for line in status_path.read_text(encoding="utf-8", errors="replace").splitlines():
            key, separator, value = line.partition(":")
            if not separator or key not in {"Name", "State", "VmRSS", "Threads"}:
                continue
            result[key.lower()] = value.strip()
    except OSError:
        return result
    return result


def collect_service_telemetry(
    service_names: Sequence[str],
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> list[dict[str, object]]:
    """Read allowlisted systemd fields and non-sensitive ``/proc`` status."""

    run = runner or subprocess.run
    rows: list[dict[str, object]] = []
    properties = ",".join(SYSTEMCTL_FIELDS)
    for service in service_names:
        if not isinstance(service, str) or not _SERVICE_RE.fullmatch(service):
            raise ConfigError(f"invalid systemd service name: {service!r}")
        row: dict[str, object] = {"service": service, "status": "unavailable"}
        try:
            completed = run(
                ["systemctl", "show", service, f"--property={properties}", "--value", "--no-pager"],
                check=False,
                capture_output=True,
                text=True,
                timeout=3,
            )
            if completed.returncode != 0:
                row["error"] = (completed.stderr or "systemctl show failed").strip()[:240]
            else:
                fields: dict[str, str] = {}
                # ``--value`` is not portable across older systemd versions
                # when requesting multiple properties; parse either key=value
                # output or the documented property-order values.
                lines = [line.strip() for line in completed.stdout.splitlines()]
                if any("=" in line for line in lines):
                    for line in lines:
                        key, separator, value = line.partition("=")
                        if separator and key in SYSTEMCTL_FIELDS:
                            fields[key] = value
                else:
                    fields = dict(zip(SYSTEMCTL_FIELDS, lines, strict=False))
                row.update({key: value for key, value in fields.items() if value != ""})
                row["status"] = "available"
                try:
                    pid = int(fields.get("MainPID", "0"))
                except ValueError:
                    pid = 0
                row["pid"] = pid
                row["process"] = _read_proc(pid)
        except (OSError, subprocess.SubprocessError) as exc:
            row["error"] = str(exc)[:240]
        rows.append(row)
    return rows


class LiveDashboard:
    """Read-only state assembler and event producer."""

    def __init__(
        self,
        config: DashboardConfig,
        *,
        clock: Callable[[], datetime] | None = None,
        telemetry: Callable[[Sequence[str]], list[dict[str, object]]] | None = None,
        market_data: MarketDataProvider | None = None,
    ) -> None:
        self.config = config
        self._clock = clock or _utc_now
        self._telemetry = telemetry or collect_service_telemetry
        self._market_data = market_data or MarketDataProvider()
        self.events = EventBroker()
        self._last_snapshot_digest: str | None = None
        self._poll_stop = threading.Event()
        self._poll_thread: threading.Thread | None = None
        self._last_snapshot: dict[str, object] | None = None
        self._report_cache: dict[Path, tuple[tuple[object, ...], ManifestReport]] = {}
        self._report_cache_lock = threading.RLock()

    def _now(self) -> datetime:
        value = self._clock()
        return value if value.tzinfo else value.replace(tzinfo=UTC)

    def _reports(self) -> list[ManifestReport]:
        reports: list[ManifestReport] = []
        now = self._now().timestamp()
        with self._report_cache_lock:
            for path in self.config.artifact_roots.values():
                cached = self._report_cache.get(path)
                if cached is not None:
                    fingerprint, report = cached
                    try:
                        current = self._report_fingerprint(report)
                    except OSError:
                        current = ()
                    if current == fingerprint:
                        manifest_mtime = report.manifest_path.stat().st_mtime
                        lifecycle = (
                            "stale"
                            if now - manifest_mtime > self.config.stale_after_seconds
                            else "sealed"
                        )
                        reports.append(replace(report, lifecycle=lifecycle))
                        continue
                report = verify_sealed_manifest(
                    path,
                    manifest_name=self.config.manifest_name,
                    stale_after_seconds=self.config.stale_after_seconds,
                    now=now,
                )
                if report.lifecycle in {"sealed", "stale"}:
                    self._report_cache[path] = (self._report_fingerprint(report), report)
                else:
                    self._report_cache.pop(path, None)
                reports.append(report)
        return reports

    @staticmethod
    def _report_fingerprint(report: ManifestReport) -> tuple[object, ...]:
        paths = (report.manifest_path, *(artifact.path for artifact in report.artifacts))
        values: list[object] = []
        for path in paths:
            stat = path.lstat()
            values.extend(
                (
                    str(path),
                    stat.st_dev,
                    stat.st_ino,
                    stat.st_size,
                    stat.st_mtime_ns,
                    stat.st_ctime_ns,
                    stat.st_mode,
                )
            )
        return tuple(values)

    @staticmethod
    def _token_evidence_store(
        reports: Sequence[ManifestReport],
    ) -> TokenEvidenceStore:
        matches = [
            artifact.path
            for report in reports
            if report.lifecycle in {"sealed", "stale"}
            for artifact in report.artifacts
            if artifact.relative_path == TOKEN_EVIDENCE_DATABASE
        ]
        if len(matches) != 1:
            raise TokenEvidenceError("one sealed token evidence database is required")
        return TokenEvidenceStore(matches[0])

    @staticmethod
    def _atlas_store(reports: Sequence[ManifestReport]) -> AtlasStore:
        matches = [
            artifact.path
            for report in reports
            if report.lifecycle in {"sealed", "stale"}
            and report.manifest.get("kind") == "pumpquant.opportunity_atlas_core"
            for artifact in report.artifacts
            if artifact.relative_path == ATLAS_DATABASE
        ]
        if len(matches) != 1:
            raise AtlasError("one sealed opportunity atlas database is required")
        return AtlasStore(matches[0])

    @staticmethod
    def _atlas_signals_store(reports: Sequence[ManifestReport]) -> AtlasSignalsStore:
        matches = [
            artifact.path
            for report in reports
            if report.lifecycle in {"sealed", "stale"}
            and report.manifest.get("kind") == "pumpquant.opportunity_atlas_signals"
            for artifact in report.artifacts
            if artifact.relative_path == ATLAS_SIGNALS_DATABASE
        ]
        if len(matches) != 1:
            raise AtlasError("one sealed opportunity atlas signals database is required")
        return AtlasSignalsStore(matches[0])

    @staticmethod
    def _atlas_ideas_store(reports: Sequence[ManifestReport]) -> AtlasIdeasStore:
        matches = [
            artifact.path
            for report in reports
            if report.lifecycle in {"sealed", "stale"}
            and report.manifest.get("kind") == "pumpquant.opportunity_idea_registry"
            for artifact in report.artifacts
            if artifact.relative_path == ATLAS_IDEAS_DATABASE
        ]
        if len(matches) != 1:
            raise AtlasError("one sealed opportunity idea database is required")
        return AtlasIdeasStore(matches[0])

    @staticmethod
    def _velocity_atlas_store(reports: Sequence[ManifestReport]) -> VelocityAtlasStore:
        matches = [
            artifact.path
            for report in reports
            if report.lifecycle in {"sealed", "stale"}
            and report.manifest.get("kind") == "pumpquant.market_cap_velocity_atlas"
            for artifact in report.artifacts
            if artifact.relative_path == VELOCITY_DATABASE
        ]
        if len(matches) != 1:
            raise AtlasError("one sealed market-cap velocity database is required")
        return VelocityAtlasStore(matches[0])

    def candidate_trades(
        self,
        candidate_sha256: str,
        *,
        limit: int = 5_000,
        offset: int = 0,
    ) -> dict[str, object]:
        store = self._token_evidence_store(self._reports())
        return store.list_trades(candidate_sha256, limit=limit, offset=offset)

    def trade_chart(self, candidate_sha256: str, trade_id: int) -> dict[str, object]:
        store = self._token_evidence_store(self._reports())
        return store.trade_chart(candidate_sha256, trade_id)

    def fx_quote(self) -> dict[str, object]:
        return self._market_data.sol_usd()

    def token_supply(self, mint: str) -> dict[str, object]:
        return self._market_data.token_supply(mint)

    def atlas_summary(self) -> dict[str, object]:
        return self._atlas_store(self._reports()).summary()

    def atlas_tokens(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        sort: str = "multiple",
        minimum_multiple: float = 0.0,
    ) -> dict[str, object]:
        return self._atlas_store(self._reports()).tokens(
            limit=limit,
            offset=offset,
            sort=sort,
            minimum_multiple=minimum_multiple,
        )

    def atlas_patterns(
        self,
        *,
        checkpoint_type: str = "age",
        checkpoint_value: float = 60.0,
        target: int = 5,
        minimum_sample: int = 500,
        limit: int = 50,
    ) -> dict[str, object]:
        return self._atlas_signals_store(self._reports()).patterns(
            checkpoint_type=checkpoint_type,
            checkpoint_value=checkpoint_value,
            target=target,
            minimum_sample=minimum_sample,
            limit=limit,
        )

    def atlas_ideas(
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
        return self._atlas_ideas_store(self._reports()).ideas(
            target=target,
            age_seconds=age_seconds,
            idea_kind=idea_kind,
            minimum_sample=minimum_sample,
            minimum_conservative_lift=minimum_conservative_lift,
            minimum_positive_ages=minimum_positive_ages,
            limit=limit,
        )

    def atlas_velocity(self, *, threshold_usd: int = 10_000) -> dict[str, object]:
        return self._velocity_atlas_store(self._reports()).rows(threshold_usd=threshold_usd)

    @staticmethod
    def _payloads(reports: Sequence[ManifestReport]) -> dict[str, object]:
        payloads: dict[str, object] = {}
        for report in reports:
            if report.lifecycle not in {"sealed", "stale"}:
                continue
            for artifact in report.artifacts:
                parsed = _parse_payload(artifact)
                if parsed is not None:
                    payloads[parsed[0]] = parsed[1]
        return payloads

    def _build_snapshot(self, reports: Sequence[ManifestReport]) -> dict[str, object]:
        payloads = self._payloads(reports)
        errors = [
            f"{report.root.name}: {report.error}"
            for report in reports
            if report.lifecycle == "error" and report.error
        ]
        statuses = [report.lifecycle for report in reports]
        if errors:
            overall = "error"
        elif any(state == "pending" for state in statuses) or not statuses:
            overall = "pending"
        elif any(state == "stale" for state in statuses):
            overall = "stale"
        else:
            overall = "sealed"

        # Manifest metadata is useful to the UI, while payload fields below are
        # populated only from files that made it through verification.
        manifest_rows = {report.root.name: report.as_dict() for report in reports}
        input_summary: object = None
        interval: object = None
        families: list[dict[str, object]] = []
        candidates: list[object] = []
        metric_candidates: list[dict[str, object]] = []
        folds: list[object] = []
        grids: list[object] = []
        trades: object = None
        wallet: object = None
        capacity: object = None
        gates: object = None
        forward: object = None
        log_events: list[object] = []
        pipeline_payload: object = None
        progress_payload: Mapping[str, object] | None = None
        progress_resources: dict[str, object] = {}

        for path, payload in payloads.items():
            key = _payload_key(path)
            mapping = payload if isinstance(payload, Mapping) else {}
            payload_kind = mapping.get("kind") if isinstance(mapping, Mapping) else None
            is_progress = key.startswith("progress") or (
                isinstance(payload_kind, str) and payload_kind.endswith("_progress")
            )
            if is_progress and isinstance(mapping, Mapping):
                progress_payload = mapping
                live_rows = mapping.get("live_candidates")
                if isinstance(live_rows, list):
                    for item in live_rows:
                        if isinstance(item, Mapping):
                            flattened = _flatten_metric_candidate(item)
                            if flattened is not None:
                                metric_candidates.append(flattened)
                reported_pipeline = mapping.get("pipeline")
                pipeline_payload = (
                    reported_pipeline if isinstance(reported_pipeline, Mapping) else mapping
                )
                reported_interval = mapping.get("interval", mapping.get("development_interval"))
                if isinstance(reported_interval, Mapping):
                    interval = reported_interval
                reported_resources = mapping.get("resources")
                if isinstance(reported_resources, Mapping):
                    progress_resources.update(_jsonable(dict(reported_resources)))
            if key in {
                "metric",
                "metrics",
                "metric_ledger",
                "trial",
                "trials",
                "ledger",
            } or key.endswith("_ledger"):
                for item in _metric_rows(payload):
                    if isinstance(item, Mapping):
                        flattened = _flatten_metric_candidate(item)
                        if flattened is not None:
                            metric_candidates.append(flattened)
            if input_summary is None and any(token in key for token in ("input", "attest")):
                input_summary = payload
            if interval is None and any(token in key for token in ("interval", "chronology")):
                interval = payload
            family = _family_for(key)
            if family is None and isinstance(mapping, Mapping):
                family_hint = mapping.get("family", mapping.get("family_name"))
                if isinstance(family_hint, str):
                    family = _family_for(family_hint)
            if family is not None:
                family_payload: dict[str, object] = {"family": family, "artifact": path}
                if isinstance(mapping, Mapping):
                    family_payload.update(_jsonable(dict(mapping)))
                    if isinstance(mapping.get("candidates"), list):
                        candidates.extend(_as_list(mapping["candidates"]))
                    if isinstance(mapping.get("folds"), list):
                        folds.extend(_as_list(mapping["folds"]))
                    if isinstance(mapping.get("grid"), list):
                        grids.extend(_as_list(mapping["grid"]))
                elif isinstance(payload, list):
                    family_payload["rows"] = _as_list(payload)
                    candidates.extend(_as_list(payload))
                families.append(family_payload)
            if isinstance(mapping, Mapping):
                if isinstance(mapping.get("families"), list):
                    families.extend(_as_list(mapping["families"]))  # type: ignore[arg-type]
                family_grids = mapping.get("family_grids")
                if isinstance(family_grids, Mapping):
                    for family_name, family_grid in family_grids.items():
                        family_id = _family_for(str(family_name)) or str(family_name)
                        family_entry: dict[str, object] = {
                            "family": family_id,
                            "grid": _jsonable(family_grid),
                        }
                        if isinstance(family_grid, Mapping):
                            if isinstance(family_grid.get("candidates"), list):
                                candidates.extend(_as_list(family_grid["candidates"]))
                            if isinstance(family_grid.get("folds"), list):
                                folds.extend(_as_list(family_grid["folds"]))
                        families.append(family_entry)
                if isinstance(mapping.get("candidates"), list) and not family:
                    candidates.extend(_as_list(mapping["candidates"]))
                if isinstance(mapping.get("folds"), list) and not family:
                    folds.extend(_as_list(mapping["folds"]))
                if isinstance(mapping.get("grids"), list):
                    grids.extend(_as_list(mapping["grids"]))
                strategy = mapping.get("strategy")
                if isinstance(strategy, Mapping):
                    if isinstance(strategy.get("candidates"), list):
                        candidates.extend(_as_list(strategy["candidates"]))
                    if isinstance(strategy.get("folds"), list):
                        folds.extend(_as_list(strategy["folds"]))
                if input_summary is None and isinstance(
                    mapping.get("attested_input_summary"), Mapping
                ):
                    input_summary = mapping["attested_input_summary"]
                if pipeline_payload is None and "pipeline" in mapping:
                    pipeline_payload = mapping["pipeline"]
                if pipeline_payload is None and "stage" in key:
                    pipeline_payload = mapping
            if any(token in key for token in ("trade", "exact", "survivor")):
                trades = payload
            if any(token in key for token in ("wallet", "shared_10", "shared10")):
                wallet = payload
            if "capacity" in key:
                capacity = payload
            if any(token in key for token in ("validation", "gate", "promotion")):
                gates = payload
            if "forward" in key:
                forward = payload
            if isinstance(mapping, Mapping):
                if isinstance(mapping.get("trades"), (list, Mapping)):
                    trades = mapping["trades"]
                if isinstance(mapping.get("trade_events"), (list, Mapping)):
                    trades = mapping["trade_events"]
                if isinstance(mapping.get("wallet"), Mapping):
                    wallet = mapping["wallet"]
                if isinstance(mapping.get("shared_10_wallet"), Mapping):
                    wallet = mapping["shared_10_wallet"]
                if isinstance(mapping.get("capacity"), (list, Mapping)):
                    capacity = mapping["capacity"]
                if isinstance(mapping.get("gates"), (list, Mapping)):
                    gates = mapping["gates"]
                if isinstance(mapping.get("forward"), Mapping):
                    forward = mapping["forward"]
                if isinstance(mapping.get("logs"), list):
                    log_events.extend(_as_list(mapping["logs"]))
            if any(token in key for token in ("log", "event")):
                if isinstance(payload, list):
                    log_events.extend(_as_list(payload))
                elif isinstance(mapping.get("events"), list):
                    log_events.extend(_as_list(mapping["events"]))

        progress_status = (
            progress_payload.get("run_status") if isinstance(progress_payload, Mapping) else None
        )
        if progress_status == "error":
            overall = "error"
        elif progress_status in {"running", "pending"}:
            overall = "pending"
        elif progress_status == "complete" and not errors:
            # A finished producer intentionally stops heartbeating. Its sealed
            # state does not become operationally stale as wall time advances.
            overall = "sealed"

        # A metric ledger is the richer, fold-level view.  Prefer it over the
        # registry-only candidate list when present so the monitor does not show
        # hundreds of provenance-only duplicates alongside their statistics.
        if metric_candidates:
            tail_summaries = _tail_exit_candidate_summaries(metric_candidates)
            candidates = list(tail_summaries if tail_summaries is not None else metric_candidates)

        telemetry_rows = self._telemetry(self.config.service_names)
        resource_summary: dict[str, object] = {}
        tasks_total = 0
        memory_total = 0
        cpu_total = 0
        tasks_seen = memory_seen = cpu_seen = False
        for row in telemetry_rows:
            try:
                tasks_total += int(str(row.get("TasksCurrent", "")))
                tasks_seen = True
            except (TypeError, ValueError):
                pass
            try:
                memory_total += int(str(row.get("MemoryCurrent", "")))
                memory_seen = True
            except (TypeError, ValueError):
                pass
            try:
                cpu_total += int(str(row.get("CPUUsageNSec", "")))
                cpu_seen = True
            except (TypeError, ValueError):
                pass
        if tasks_seen:
            resource_summary["tasks_current"] = tasks_total
        if memory_seen:
            resource_summary["memory_bytes"] = memory_total
            resource_summary["memory_usage"] = memory_total
        if cpu_seen:
            resource_summary["cpu_usage_ns"] = cpu_total
        resource_summary.update(progress_resources)
        resource_summary["services"] = telemetry_rows
        resource_status = (
            "sealed"
            if progress_resources or any(row.get("status") == "available" for row in telemetry_rows)
            else "pending"
        )
        stage_rows = []
        for report in reports:
            stage_rows.append(
                {
                    "name": report.root.name,
                    "status": report.lifecycle,
                    "updated_at": report.updated_at,
                    "error": report.error,
                }
            )
        snapshot: dict[str, object] = {
            "schema_version": 1,
            "generated_at": _iso(self._now()),
            "status": overall,
            "errors": errors,
            "artifacts": manifest_rows,
            "pipeline": {
                "status": progress_status or overall,
                "current_stage": pipeline_payload.get("current_stage")
                if isinstance(pipeline_payload, Mapping)
                else None,
                "stages": stage_rows,
                "reported": _jsonable(pipeline_payload),
            },
            "resources": {
                "status": resource_status,
                **resource_summary,
            },
            "progress": _jsonable(progress_payload),
            "data": {
                "status": _section_status(
                    input_summary is not None or interval is not None, reports
                ),
                "attested_input_summary": _jsonable(input_summary),
                "manifest": manifest_rows,
                "interval": _jsonable(interval),
                "holdout": {"locked": True, "accessed": False, "status": "locked"},
            },
            "strategy": {
                "status": _section_status(bool(families or candidates or folds or grids), reports),
                "families": _jsonable(families),
                "grids": _jsonable(grids),
                "candidates": _jsonable(candidates),
                "folds": _jsonable(folds),
            },
            "execution": {
                "status": _section_status(
                    trades is not None or wallet is not None or capacity is not None,
                    reports,
                ),
                "trades": _jsonable(trades),
                "wallet": _jsonable(wallet),
                "shared_10_wallet": _jsonable(wallet),
                "capacity": _jsonable(capacity),
            },
            "validation": {
                "status": _section_status(gates is not None or forward is not None, reports),
                "gates": _jsonable(gates),
                "forward": _jsonable(forward),
                "holdout": {"locked": True, "accessed": False, "status": "locked"},
            },
            "logs": {
                "status": _section_status(bool(log_events), reports),
                "events": _jsonable(log_events),
            },
            "holdout": {"locked": True, "accessed": False, "status": "locked"},
        }
        # Stable convenience aliases keep the API friendly to lightweight
        # clients (including the bundled static monitor) while the canonical
        # sectioned shape above remains the source of truth.
        snapshot.update(
            {
                "manifest": {"status": overall, "artifacts": manifest_rows},
                "input_summary": _jsonable(input_summary),
                "families": _jsonable(families),
                "candidates": _jsonable(candidates),
                "folds": _jsonable(folds),
                "trades": _jsonable(trades),
                "wallet": _jsonable(wallet),
                "capacity": _jsonable(capacity),
                "gates": _jsonable(gates),
                "forward": _jsonable(forward),
                "events": _jsonable(log_events),
                "progress": _jsonable(progress_payload),
            }
        )
        return snapshot

    def snapshot(self) -> dict[str, object]:
        snapshot = self._build_snapshot(self._reports())
        # generated_at changes on every poll; it must not create a spurious SSE
        # event when no sealed artifact or telemetry state changed.
        digest_payload = dict(snapshot)
        digest_payload.pop("generated_at", None)
        digest = hashlib.sha256(_canonical_json(digest_payload).encode("utf-8")).hexdigest()
        self._last_snapshot = snapshot
        if digest != self._last_snapshot_digest:
            self._last_snapshot_digest = digest
            self.events.publish("snapshot", snapshot)
        return snapshot

    def health(self) -> dict[str, object]:
        snapshot = self.snapshot()
        status = snapshot.get("status", "pending")
        artifact_status: dict[str, object] = {}
        raw_artifacts = snapshot.get("artifacts")
        if isinstance(raw_artifacts, Mapping):
            for name, report in raw_artifacts.items():
                if isinstance(report, Mapping):
                    artifact_status[str(name)] = report.get("status")
        return {
            "service": "pumpquant-live-dashboard",
            "status": status,
            "ok": status not in {"error"},
            "generated_at": snapshot.get("generated_at"),
            "artifact_status": artifact_status,
            "errors": snapshot.get("errors", []),
            "holdout": {"locked": True, "accessed": False},
        }

    def start(self) -> None:
        if self._poll_thread is not None and self._poll_thread.is_alive():
            return
        self._poll_stop.clear()

        # Publish one snapshot immediately.  Waiting for the first poll
        # interval made a freshly opened monitor look disconnected even when
        # the server was healthy and the artifact roots were readable.
        try:
            self.snapshot()
        except Exception as exc:  # pragma: no cover - defensive monitor loop
            self.events.publish("error", {"error": str(exc)[:240]})

        def run() -> None:
            while not self._poll_stop.wait(self.config.poll_interval_seconds):
                try:
                    self.snapshot()
                except Exception as exc:  # pragma: no cover - defensive monitor loop
                    self.events.publish("error", {"error": str(exc)[:240]})

        self._poll_thread = threading.Thread(target=run, name="pumpquant-dashboard", daemon=True)
        self._poll_thread.start()

    def stop(self) -> None:
        self._poll_stop.set()
        thread = self._poll_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=max(1.0, self.config.poll_interval_seconds + 0.5))
        self._poll_thread = None


def _send_json(
    handler: BaseHTTPRequestHandler, payload: Mapping[str, object], status: int = 200
) -> None:
    body = (_canonical_json(payload) + "\n").encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    if handler.command != "HEAD":
        handler.wfile.write(body)


class DashboardRequestHandler(BaseHTTPRequestHandler):
    """Fixed-route HTTP handler with bounded evidence query parameters."""

    server: DashboardHTTPServer

    def handle(self) -> None:  # noqa: D401 - stdlib hook
        """Treat a browser closing a poll as normal, without noisy tracebacks."""

        try:
            super().handle()
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            # Browser reloads and cancelled fetches can close the socket between
            # request parsing and response delivery.  They are not dashboard
            # failures and should not obscure the useful service output.
            return

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        try:
            parsed = urlsplit(self.path)
            route = parsed.path
            if route == "/api/snapshot":
                _send_json(self, self.server.dashboard.snapshot())
                return
            if route == "/api/health":
                _send_json(self, self.server.dashboard.health())
                return
            if route == "/api/fx":
                self._serve_fx(self._parse_query(parsed.query) if parsed.query else {})
                return
            if route == "/api/token-supply":
                self._serve_token_supply(self._parse_query(parsed.query))
                return
            if route == "/api/events":
                self._serve_events()
                return
            if route == "/api/candidate-trades":
                self._serve_candidate_trades(self._parse_query(parsed.query))
                return
            if route == "/api/trade-chart":
                self._serve_trade_chart(self._parse_query(parsed.query))
                return
            if route == "/api/atlas-summary":
                if parsed.query:
                    raise AtlasError("atlas summary accepts no query parameters")
                _send_json(self, self.server.dashboard.atlas_summary())
                return
            if route == "/api/atlas-tokens":
                self._serve_atlas_tokens(self._parse_query(parsed.query) if parsed.query else {})
                return
            if route == "/api/atlas-patterns":
                self._serve_atlas_patterns(self._parse_query(parsed.query) if parsed.query else {})
                return
            if route == "/api/atlas-ideas":
                self._serve_atlas_ideas(self._parse_query(parsed.query) if parsed.query else {})
                return
            if route == "/api/atlas-velocity":
                self._serve_atlas_velocity(self._parse_query(parsed.query) if parsed.query else {})
                return
            if route == "/" or route == "/index.html":
                self._serve_static("index.html")
                return
            if route.startswith("/static/"):
                self._serve_static(route.removeprefix("/static/"))
                return
            self.send_error(HTTPStatus.NOT_FOUND)
        except (AtlasError, TokenEvidenceError) as exc:
            _send_json(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)

    @staticmethod
    def _parse_query(raw_query: str) -> Mapping[str, list[str]]:
        try:
            return parse_qs(raw_query, strict_parsing=True)
        except ValueError as exc:
            raise TokenEvidenceError("malformed query string") from exc

    @staticmethod
    def _one_query_value(query: Mapping[str, list[str]], name: str) -> str:
        values = query.get(name, [])
        if len(values) != 1 or not values[0]:
            raise TokenEvidenceError(f"exactly one {name} is required")
        return values[0]

    def _serve_candidate_trades(self, query: Mapping[str, list[str]]) -> None:
        try:
            unknown = set(query) - {"candidate", "limit", "offset"}
            if unknown:
                raise TokenEvidenceError("unsupported candidate-trades query parameter")
            candidate = self._one_query_value(query, "candidate")
            limit = int(query.get("limit", ["5000"])[0])
            offset = int(query.get("offset", ["0"])[0])
            payload = self.server.dashboard.candidate_trades(
                candidate,
                limit=limit,
                offset=offset,
            )
        except (TokenEvidenceError, ValueError) as exc:
            _send_json(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        _send_json(self, payload)

    def _serve_trade_chart(self, query: Mapping[str, list[str]]) -> None:
        try:
            if set(query) != {"candidate", "trade_id"}:
                raise TokenEvidenceError("candidate and trade_id are required")
            candidate = self._one_query_value(query, "candidate")
            trade_id = int(self._one_query_value(query, "trade_id"))
            payload = self.server.dashboard.trade_chart(candidate, trade_id)
        except (TokenEvidenceError, ValueError) as exc:
            _send_json(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        _send_json(self, payload)

    def _serve_atlas_tokens(self, query: Mapping[str, list[str]]) -> None:
        try:
            unknown = set(query) - {"limit", "offset", "sort", "minimum_multiple"}
            if unknown:
                raise AtlasError("unsupported atlas-tokens query parameter")
            payload = self.server.dashboard.atlas_tokens(
                limit=int(query.get("limit", ["100"])[0]),
                offset=int(query.get("offset", ["0"])[0]),
                sort=query.get("sort", ["multiple"])[0],
                minimum_multiple=float(query.get("minimum_multiple", ["0"])[0]),
            )
        except (AtlasError, ValueError) as exc:
            _send_json(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        _send_json(self, payload)

    def _serve_atlas_patterns(self, query: Mapping[str, list[str]]) -> None:
        try:
            allowed = {"checkpoint_type", "checkpoint_value", "target", "minimum_sample", "limit"}
            if set(query) - allowed:
                raise AtlasError("unsupported atlas-patterns query parameter")
            payload = self.server.dashboard.atlas_patterns(
                checkpoint_type=query.get("checkpoint_type", ["age"])[0],
                checkpoint_value=float(query.get("checkpoint_value", ["60"])[0]),
                target=int(query.get("target", ["5"])[0]),
                minimum_sample=int(query.get("minimum_sample", ["500"])[0]),
                limit=int(query.get("limit", ["50"])[0]),
            )
        except (AtlasError, ValueError) as exc:
            _send_json(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        _send_json(self, payload)

    def _serve_atlas_ideas(self, query: Mapping[str, list[str]]) -> None:
        try:
            allowed = {
                "target",
                "age_seconds",
                "idea_kind",
                "minimum_sample",
                "minimum_conservative_lift",
                "minimum_positive_ages",
                "limit",
            }
            if set(query) - allowed:
                raise AtlasError("unsupported atlas-ideas query parameter")
            age_raw = query.get("age_seconds", [""])[0]
            payload = self.server.dashboard.atlas_ideas(
                target=int(query.get("target", ["5"])[0]),
                age_seconds=float(age_raw) if age_raw else None,
                idea_kind=query.get("idea_kind", ["all"])[0],
                minimum_sample=int(query.get("minimum_sample", ["300"])[0]),
                minimum_conservative_lift=float(query.get("minimum_conservative_lift", ["1"])[0]),
                minimum_positive_ages=int(query.get("minimum_positive_ages", ["2"])[0]),
                limit=int(query.get("limit", ["100"])[0]),
            )
        except (AtlasError, ValueError) as exc:
            _send_json(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        _send_json(self, payload)

    def _serve_atlas_velocity(self, query: Mapping[str, list[str]]) -> None:
        try:
            if set(query) - {"threshold_usd"}:
                raise AtlasError("unsupported atlas-velocity query parameter")
            payload = self.server.dashboard.atlas_velocity(
                threshold_usd=int(query.get("threshold_usd", ["10000"])[0])
            )
        except (AtlasError, ValueError) as exc:
            _send_json(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        _send_json(self, payload)

    def _serve_fx(self, query: Mapping[str, list[str]]) -> None:
        if query:
            _send_json(
                self,
                {"error": "fx endpoint accepts no query parameters"},
                HTTPStatus.BAD_REQUEST,
            )
            return
        try:
            payload = self.server.dashboard.fx_quote()
        except MarketDataError as exc:
            _send_json(self, {"error": str(exc)}, HTTPStatus.SERVICE_UNAVAILABLE)
            return
        _send_json(self, payload)

    def _serve_token_supply(self, query: Mapping[str, list[str]]) -> None:
        try:
            if set(query) != {"mint"}:
                raise TokenEvidenceError("exactly one mint is required")
            payload = self.server.dashboard.token_supply(self._one_query_value(query, "mint"))
        except TokenEvidenceError as exc:
            _send_json(self, {"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        except MarketDataError as exc:
            _send_json(self, {"error": str(exc)}, HTTPStatus.SERVICE_UNAVAILABLE)
            return
        _send_json(self, payload)

    def do_HEAD(self) -> None:  # noqa: N802
        self.do_GET()

    def _serve_static(self, relative: str) -> None:
        if not relative or "\\" in relative:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        try:
            safe = _safe_relative_path(relative)
            path = self.server.dashboard.config.static_root / safe
            resolved = path.resolve(strict=False)
            resolved.relative_to(self.server.dashboard.config.static_root)
            if (
                _path_has_symlink(path, self.server.dashboard.config.static_root)
                or not path.is_file()
            ):
                raise FileNotFoundError(path)
            body = path.read_bytes()
        except (ManifestError, FileNotFoundError, OSError, ValueError):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        content_type = "text/plain; charset=utf-8"
        if path.suffix == ".html":
            content_type = "text/html; charset=utf-8"
        elif path.suffix == ".css":
            content_type = "text/css; charset=utf-8"
        elif path.suffix == ".js":
            content_type = "text/javascript; charset=utf-8"
        elif path.suffix == ".woff2":
            content_type = "font/woff2"
        elif path.suffix == ".svg":
            content_type = "image/svg+xml"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _serve_events(self) -> None:
        dashboard = self.server.dashboard
        dashboard.snapshot()
        try:
            last_header = self.headers.get("Last-Event-ID")
            last_id = int(last_header) if last_header and last_header.isdigit() else None
        except ValueError:
            last_id = None
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        try:
            events = dashboard.events.since(last_id)
            if not events:
                latest = dashboard.events.latest()
                if latest is not None and (last_id is None or latest.id > last_id):
                    events = [latest]
            current_id = last_id
            for event in events:
                self.wfile.write(event.sse())
                self.wfile.flush()
                current_id = event.id
            # Keep the connection open for live updates.  A client can close
            # after the initial event; BrokenPipe/ConnectionReset are normal.
            while True:
                pending = dashboard.events.wait_since(current_id, timeout=15.0)
                if not pending:
                    self.wfile.write(b": keep-alive\n\n")
                    self.wfile.flush()
                    continue
                for event in pending:
                    self.wfile.write(event.sse())
                    self.wfile.flush()
                    current_id = event.id
        except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
            return

    def log_message(self, format: str, *args: object) -> None:
        # The dashboard is often run under systemd; access logs belong in the
        # service journal only when an operator explicitly enables them.
        return


class DashboardHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address: tuple[str, int], dashboard: LiveDashboard):
        self.dashboard = dashboard
        super().__init__(address, DashboardRequestHandler)

    def server_close(self) -> None:
        self.dashboard.stop()
        super().server_close()


def create_server(
    dashboard: LiveDashboard,
    *,
    host: str | None = None,
    port: int | None = None,
) -> DashboardHTTPServer:
    """Create a fixed-route server bound to the configured/default address."""

    return DashboardHTTPServer(
        (
            dashboard.config.host if host is None else host,
            dashboard.config.port if port is None else port,
        ),
        dashboard,
    )
