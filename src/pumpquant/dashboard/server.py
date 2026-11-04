"""Read-only live dashboard backend for PumpQuant.

The dashboard has a small, boring trust boundary.  A configured
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
import subprocess
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from pumpquant.dashboard.atlas import (
    ATLAS_DATABASE,
    ATLAS_IDEAS_DATABASE,
    ATLAS_SIGNALS_DATABASE,
    VELOCITY_DATABASE,
    AtlasError,
    AtlasIdeasStore,
    AtlasSignalsStore,
    AtlasStore,
    VelocityAtlasStore,
)
from pumpquant.dashboard.config import (
    SERVICE_NAME_RE,
    ConfigError,
    DashboardConfig,
)
from pumpquant.dashboard.evidence import (
    TOKEN_EVIDENCE_DATABASE,
    TokenEvidenceError,
    TokenEvidenceStore,
)
from pumpquant.dashboard.manifests import (
    ManifestError,
    ManifestReport,
    iso_timestamp,
    jsonable,
    path_has_symlink,
    safe_relative_path,
    utc_now,
    verify_sealed_manifest,
)
from pumpquant.dashboard.market import (
    MarketDataError,
    MarketDataProvider,
)
from pumpquant.dashboard.snapshot import (
    _parse_payload,
    build_snapshot,
)

__all__ = [
    "DashboardHTTPServer",
    "DashboardRequestHandler",
    "Event",
    "EventBroker",
    "LiveDashboard",
    "collect_service_telemetry",
    "create_server",
]


# ``systemctl show`` is constrained.  In particular, no
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


def _canonical_json(value: Any) -> str:
    return json.dumps(jsonable(value), sort_keys=True, separators=(",", ":"))


class EventBroker:
    """Small in-process event log used by the SSE endpoint and tests."""

    def __init__(self, *, max_events: int = 256) -> None:
        self._events: deque[Event] = deque(maxlen=max(1, max_events))
        self._condition = threading.Condition()
        self._next_id = 1

    def publish(self, event: str, data: Mapping[str, object]) -> Event:
        with self._condition:
            item = Event(self._next_id, event, jsonable(dict(data)), iso_timestamp())
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
        if not isinstance(service, str) or not SERVICE_NAME_RE.fullmatch(service):
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
        self._clock = clock or utc_now
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
        return build_snapshot(
            reports,
            self._payloads(reports),
            self._telemetry(self.config.service_names),
            self._now(),
        )

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
            safe = safe_relative_path(relative)
            path = self.server.dashboard.config.static_root / safe
            resolved = path.resolve(strict=False)
            resolved.relative_to(self.server.dashboard.config.static_root)
            if (
                path_has_symlink(path, self.server.dashboard.config.static_root)
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
        # service journal only when an operator enables them.
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
