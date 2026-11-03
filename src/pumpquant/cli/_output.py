"""JSON output shared by every command."""

from __future__ import annotations

import json
import math
from typing import Any


def json_safe(value: Any) -> Any:
    """Replace non-finite floats with null so the output is valid JSON."""
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def print_json(payload: Any) -> None:
    print(json.dumps(json_safe(payload), indent=2, sort_keys=True))
