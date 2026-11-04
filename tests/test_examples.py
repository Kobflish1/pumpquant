"""The README's Quick start prints exactly what the README shows."""

from __future__ import annotations

import runpy
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).parents[1] / "examples"

PRINTED = {
    "periods.py": (
        "period 1: 14 outcomes, -0.127 SOL\n"
        "period 2: 16 outcomes, +0.268 SOL\n"
        "period 3: 17 outcomes, +0.076 SOL\n"
    ),
    "quote.py": (
        "0.1 SOL buys 3,529,605 tokens\none trade later it buys 3,308,022 tokens, 6.3% fewer\n"
    ),
}


@pytest.mark.parametrize("script", sorted(PRINTED))
def test_example_prints_the_readme_output(script: str) -> None:
    output = StringIO()
    with redirect_stdout(output):
        runpy.run_path(str(EXAMPLES / script), run_name="__main__")
    assert output.getvalue() == PRINTED[script]
