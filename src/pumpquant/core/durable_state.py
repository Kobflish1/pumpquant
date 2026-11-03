"""Crash-atomic, private JSON state files.

The execution path uses small JSON objects for local recovery state.  This
module deliberately has no cloud or database dependencies: it writes a fully
serialized object to a private temporary file, makes that file durable, and
then publishes it with an atomic directory operation.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any


def _encode_object(payload: Mapping[str, Any]) -> bytes:
    """Return the canonical on-disk representation of a JSON object."""
    if not isinstance(payload, Mapping):
        raise TypeError("payload must be a mapping representing a JSON object")
    try:
        # Materialize the mapping before serializing it.  Besides accepting
        # Mapping implementations that json does not know about, this gives
        # callers a stable snapshot while the file is being written.
        value = dict(payload)
        encoded = json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError, OverflowError) as error:
        raise TypeError("payload must be a JSON-serializable object") from error
    return (encoded + "\n").encode("utf-8")


def _write_all(descriptor: int, data: bytes) -> None:
    """Write all bytes, handling short writes and interrupted syscalls."""
    offset = 0
    while offset < len(data):
        try:
            written = os.write(descriptor, data[offset:])
        except InterruptedError:
            continue
        if written <= 0:
            raise OSError("writing durable state made no progress")
        offset += written


def _fsync_directory(directory: Path) -> None:
    """Flush a directory entry update to stable storage."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(str(directory), flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_private_parent(parent: Path) -> None:
    """Create each missing parent component with mode 0700.

    ``Path.mkdir(parents=True, mode=...)`` uses the interpreter's default
    directory mode for intermediate components.  Creating missing components
    one at a time keeps every directory created by this writer private.
    """
    missing: list[Path] = []
    current = parent
    while not current.exists():
        missing.append(current)
        ancestor = current.parent
        if ancestor == current:  # pragma: no cover - filesystem root exists
            break
        current = ancestor
    for directory in reversed(missing):
        directory.mkdir(mode=0o700, exist_ok=True)


def atomic_private_json_write(
    path: str | os.PathLike[str],
    payload: Mapping[str, Any],
    *,
    exclusive: bool,
) -> None:
    """Atomically persist a private JSON object at *path*.

    Parent directories are created with mode ``0700``.  The temporary file is
    created beside the destination with mode ``0600``, fully written and
    fsynced, and then published atomically.  With ``exclusive=False`` the
    destination is replaced via :func:`os.replace`; with ``exclusive=True`` a
    hard-link publication provides an atomic no-clobber operation.  In either
    mode a destination symlink is never opened or followed: rename replaces
    the symlink entry itself, while hard-link publication fails if it exists.
    """
    if not isinstance(exclusive, bool):
        raise TypeError("exclusive must be a bool")

    target = Path(path)
    encoded = _encode_object(payload)

    # mkdir's mode is subject to the process umask, and therefore never grants
    # more access than 0700.  Existing directories are left untouched.
    _ensure_private_parent(target.parent)

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    temporary_path: str | None = temporary_name
    try:
        try:
            # mkstemp is specified to use 0600, but fchmod makes that
            # contract explicit even when a platform or test double differs.
            os.fchmod(descriptor, 0o600)
            _write_all(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            try:
                os.close(descriptor)
            finally:
                descriptor = -1

        if exclusive:
            # A link is created only when the destination does not already
            # exist.  It is atomic with respect to competing creators and
            # cannot clobber an existing destination (including a symlink).
            os.link(temporary_name, target)
            os.unlink(temporary_name)
        else:
            # rename/replace operates on the directory entry itself, so an
            # existing symlink at target is replaced rather than followed.
            os.replace(temporary_name, target)
        temporary_path = None
        _fsync_directory(target.parent)
    finally:
        if temporary_path is not None:
            with suppress(OSError):
                os.unlink(temporary_path)


# A descriptive short alias is convenient for callers that do not need to
# mention the privacy guarantees in every call site.
write_json_object = atomic_private_json_write


__all__ = ["atomic_private_json_write", "write_json_object"]
