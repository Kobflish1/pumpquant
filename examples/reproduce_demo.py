"""Check the synthetic CLI's deterministic shape without external data."""

import io
import json
from contextlib import redirect_stdout

from pumpquant.cli import main

buffer = io.StringIO()
with redirect_stdout(buffer):
    exit_code = main(["demo", "--tokens", "36", "--bars-per-token", "30", "--seed", "7"])
report = json.loads(buffer.getvalue())
assert exit_code == 0
assert report["bars"] == 1080
assert report["candidate_count"] == 16
assert report["trial_count"] == 48
assert len(report["folds"]) == 3
assert report["fixed_notional_outcome_diagnostics"]["observations"] == 47
print(
    json.dumps(
        {
            "data": "synthetic",
            "bars": report["bars"],
            "candidates": report["candidate_count"],
            "folds": len(report["folds"]),
            "candidate_fold_trials": report["trial_count"],
            "out_of_sample_position_outcomes": report["fixed_notional_outcome_diagnostics"][
                "observations"
            ],
            "notice": report["notice"],
        },
        indent=2,
    )
)
