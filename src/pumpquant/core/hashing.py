"""Content hashes that bind manifests, ledgers and archives to exact bytes."""

from __future__ import annotations

import hashlib
from pathlib import Path


def sha256_file(path: str | Path) -> str:
    with open(path, "rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def sha256_file_with_size(path: str | Path) -> tuple[int, str]:
    """Hash a file and report how many bytes were hashed, read in one pass."""
    with open(path, "rb") as handle:
        digest = hashlib.file_digest(handle, "sha256")
        return handle.tell(), digest.hexdigest()
