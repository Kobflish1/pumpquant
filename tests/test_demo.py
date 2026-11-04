from __future__ import annotations

from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from pumpquant.cli import main

RECORDED = Path(__file__).parents[1] / "examples" / "synthetic-demo.json"


def test_demo_output_matches_the_recorded_run() -> None:
    output = StringIO()
    with redirect_stdout(output):
        assert main(["demo"]) == 0
    assert output.getvalue() == RECORDED.read_text()
