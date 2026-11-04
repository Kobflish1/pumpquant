"""Dashboard configuration: which artifact roots and services it may read, and where it listens."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from importlib.resources import files
from pathlib import Path

DEFAULT_HOST = "127.0.0.1"


DEFAULT_PORT = 8765


DEFAULT_MANIFEST = "manifest.json"


# The pages, scripts, fonts and chart library ship inside the package, so an installed
# wheel serves the same dashboard as a source checkout.
STATIC_ROOT = Path(str(files("pumpquant.dashboard") / "static"))


SERVICE_NAME_RE = re.compile(r"^[A-Za-z0-9_.@:%+\-]+\.(?:service|timer|socket)$")


_NAME_RE = re.compile(r"^[A-Za-z0-9_.@%+\-]+$")


_HOLDOUT_RE = re.compile(r"(?:^|[^a-z0-9])holdout(?:$|[^a-z0-9])", re.I)


class DashboardError(RuntimeError):
    """Base class for dashboard configuration and artifact errors."""


class ConfigError(DashboardError, ValueError):
    """Raised when a dashboard configuration would widen the read boundary."""


def contains_holdout(value: str | Path) -> bool:
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
    if contains_holdout(resolved) or contains_holdout(raw):
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
        if "holdout" in self.name.lower() or contains_holdout(path):
            raise ConfigError("holdout roots are not allowed")
        manifest = Path(self.manifest_name)
        if (
            manifest.is_absolute()
            or len(manifest.parts) != 1
            or manifest.name in {".", ".."}
            or contains_holdout(manifest.name)
        ):
            raise ConfigError("manifest_name must be one safe file name")
        if self.path.exists() and self.path.is_symlink():
            raise ConfigError(f"artifact root is a symlink: {self.path}")


@dataclass(frozen=True)
class DashboardConfig:
    """Validated dashboard configuration.

    ``artifact_roots`` and ``service_names`` are tuples/maps after
    construction.  The class accepts mappings for ergonomic test setup while
    copying them so callers cannot mutate the read boundary under a running
    server.
    """

    artifact_roots: Mapping[str, Path] = field(default_factory=dict)
    service_names: tuple[str, ...] = ()
    static_root: Path = STATIC_ROOT
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
        if manifest.is_absolute() or len(manifest.parts) != 1 or contains_holdout(manifest.name):
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
            if contains_holdout(path):
                raise ConfigError(f"artifact root {name!r} points at a holdout")
            if path.exists() and path.is_symlink():
                raise ConfigError(f"artifact root {name!r} must not be a symlink")
            roots[name] = path
        services: list[str] = []
        for service in tuple(self.service_names):
            if (
                not isinstance(service, str)
                or not SERVICE_NAME_RE.fullmatch(service)
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
        static = (
            _resolve_config_path(raw["static_root"], base_dir=base, label="static_root")
            if "static_root" in raw
            else STATIC_ROOT
        )
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
        return DashboardConfig()
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
