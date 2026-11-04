"""Content hashes that bind manifests, ledgers and archives to exact bytes."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


def canonical_json(value: object, *, newline: bool = True) -> bytes:
    """Encode JSON so equal values give equal bytes: sorted keys, no spaces, no NaN.

    Files end with a newline; pass ``newline=False`` for a value that is hashed or
    embedded on its own.
    """
    text = json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":"))
    return (text + "\n" if newline else text).encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: str | Path) -> str:
    with open(path, "rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def sha256_file_with_size(path: str | Path) -> tuple[int, str]:
    """Hash a file and report how many bytes were hashed, read in one pass."""
    with open(path, "rb") as handle:
        digest = hashlib.file_digest(handle, "sha256")
        return handle.tell(), digest.hexdigest()
