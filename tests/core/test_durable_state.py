from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

import pumpquant.core.durable_state as durable_state


def _temporary_files(directory: Path) -> list[Path]:
    return list(directory.glob("*.tmp"))


def test_overwrite_is_atomic_and_private(tmp_path) -> None:
    target = tmp_path / "nested" / "state.json"

    durable_state.atomic_private_json_write(
        target,
        {"status": "started", "attempt": 1},
        exclusive=False,
    )

    assert json.loads(target.read_text(encoding="utf-8")) == {
        "attempt": 1,
        "status": "started",
    }
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert stat.S_IMODE(target.parent.stat().st_mode) == 0o700
    assert not _temporary_files(target.parent)

    durable_state.atomic_private_json_write(
        target,
        {"status": "completed"},
        exclusive=False,
    )
    assert json.loads(target.read_text(encoding="utf-8")) == {"status": "completed"}
    assert stat.S_IMODE(target.stat().st_mode) == 0o600


def test_exclusive_collision_preserves_existing_bytes(tmp_path) -> None:
    target = tmp_path / "state.json"
    target.write_bytes(b"old bytes\n")

    with pytest.raises(FileExistsError):
        durable_state.atomic_private_json_write(target, {"new": True}, exclusive=True)

    assert target.read_bytes() == b"old bytes\n"
    assert not _temporary_files(tmp_path)


@pytest.mark.parametrize("operation", ["replace", "link"])
def test_publication_failure_cleans_up_temp_file(tmp_path, monkeypatch, operation: str) -> None:
    target = tmp_path / "state.json"

    def fail(*args, **kwargs):
        raise OSError(f"simulated {operation} failure")

    monkeypatch.setattr(durable_state.os, operation, fail)
    with pytest.raises(OSError, match=f"simulated {operation} failure"):
        durable_state.atomic_private_json_write(
            target,
            {"status": "started"},
            exclusive=operation == "link",
        )

    assert not target.exists()
    assert not _temporary_files(tmp_path)


def test_destination_symlink_is_not_followed(tmp_path) -> None:
    victim = tmp_path / "victim.json"
    victim.write_bytes(b"keep this file\n")
    target = tmp_path / "state.json"
    target.symlink_to(victim)

    durable_state.atomic_private_json_write(target, {"safe": True}, exclusive=False)

    assert victim.read_bytes() == b"keep this file\n"
    assert not target.is_symlink()
    assert json.loads(target.read_text(encoding="utf-8")) == {"safe": True}


def test_exclusive_symlink_collision_does_not_touch_victim(tmp_path) -> None:
    victim = tmp_path / "victim.json"
    victim.write_bytes(b"keep this file\n")
    target = tmp_path / "state.json"
    target.symlink_to(victim)

    with pytest.raises(FileExistsError):
        durable_state.atomic_private_json_write(target, {"unsafe": True}, exclusive=True)

    assert target.is_symlink()
    assert victim.read_bytes() == b"keep this file\n"
    assert not _temporary_files(tmp_path)


@pytest.mark.parametrize("payload", [["not", "an", "object"], {"value": object()}])
def test_rejects_non_object_or_non_serializable_payload(tmp_path, payload) -> None:
    target = tmp_path / "state.json"

    with pytest.raises(TypeError):
        durable_state.atomic_private_json_write(target, payload, exclusive=False)

    assert not target.exists()
    assert not list(tmp_path.glob("*.tmp"))
