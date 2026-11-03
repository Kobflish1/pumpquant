from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from pumpquant.cli import main
from pumpquant.core.io import (
    _record_payload,
    iter_swap_event_batches,
    read_bars,
    read_swap_events,
    write_jsonl,
    write_parquet,
)
from pumpquant.core.schema import Side, SwapEvent, VenuePhase
from pumpquant.core.synthetic import generate_synthetic_bars


class IoCliTests(unittest.TestCase):
    @staticmethod
    def _event(event_id: str, slot: int) -> SwapEvent:
        return SwapEvent(
            event_id=event_id,
            market_id="POOL",
            mint="TOKEN",
            quote_mint="SOL",
            slot=slot,
            tx_index=0,
            instruction_index=0,
            block_time_ns=slot,
            observed_at_ns=slot,
            phase=VenuePhase.BONDING_CURVE,
            side=Side.BUY,
            base_amount=1.0,
            quote_amount=2.0,
            price=2.0,
            virtual_base_reserve=100.0,
            virtual_quote_reserve=200.0,
            real_base_reserve=90.0,
            real_quote_reserve=190.0,
        )

    def test_bar_jsonl_round_trip(self) -> None:
        bars = generate_synthetic_bars(tokens=1, bars_per_token=6)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bars.jsonl"
            self.assertEqual(write_jsonl(path, bars), len(bars))
            self.assertEqual(read_bars(path), bars)

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "requires pyarrow")
    def test_swap_event_parquet_batches_preserve_order_and_cli_input(self) -> None:
        events = [self._event("second", 2), self._event("first", 1), self._event("third", 3)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.parquet"
            self.assertEqual(write_parquet(path, events), len(events))
            batches = list(iter_swap_event_batches(path, batch_size=2))
            self.assertEqual(
                [[event.event_id for event in batch] for batch in batches],
                [["second", "first"], ["third"]],
            )
            self.assertEqual(read_swap_events(path), events)
            output_path = Path(directory) / "bars.jsonl"
            with redirect_stdout(StringIO()):
                status = main(["bars", str(path), str(output_path)])
            self.assertEqual(status, 0)
            self.assertEqual(len(read_bars(output_path)), 1)

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "requires pyarrow")
    def test_swap_event_parquet_batches_validate_version_and_record_type(self) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        event = self._event("event", 1)
        with tempfile.TemporaryDirectory() as directory:
            version_path = Path(directory) / "bad-version.parquet"
            bad_version = _record_payload(event)
            bad_version["schema_version"] = 999
            pq.write_table(pa.Table.from_pylist([bad_version]), version_path)
            with self.assertRaisesRegex(ValueError, "unsupported schema_version"):
                list(iter_swap_event_batches(version_path))

            type_path = Path(directory) / "bad-type.parquet"
            bad_type = _record_payload(event)
            bad_type["record_type"] = "bar"
            pq.write_table(pa.Table.from_pylist([bad_type]), type_path)
            with self.assertRaisesRegex(ValueError, "record_type must be 'swap_event'"):
                list(iter_swap_event_batches(type_path))

    def test_depth_cli(self) -> None:
        with redirect_stdout(StringIO()):
            status = main(
                [
                    "depth",
                    "--base-reserve",
                    "1000",
                    "--quote-reserve",
                    "100",
                    "--fee-bps",
                    "125",
                    "--sizes",
                    "0.1",
                    "1",
                ]
            )
        self.assertEqual(status, 0)

    def test_search_cli_appends_every_trial_to_ledger(self) -> None:
        bars = generate_synthetic_bars(tokens=8, bars_per_token=10)
        with tempfile.TemporaryDirectory() as directory:
            bars_path = Path(directory) / "bars.jsonl"
            ledger_path = Path(directory) / "trials.jsonl"
            write_jsonl(bars_path, bars)
            output = StringIO()
            with redirect_stdout(output):
                status = main(
                    [
                        "search",
                        str(bars_path),
                        "--min-train-tokens",
                        "3",
                        "--test-tokens",
                        "2",
                        "--minimum-train-positions",
                        "1",
                        "--momentum-lookbacks",
                        "1",
                        "--entry-momenta",
                        "-1",
                        "--entry-imbalances",
                        "-1",
                        "--take-profits",
                        "0.1",
                        "--stop-losses",
                        "0.9",
                        "--volume-lookback",
                        "2",
                        "--min-liquidity",
                        "0",
                        "--min-trade-count",
                        "1",
                        "--min-age-seconds",
                        "0",
                        "--max-age-seconds",
                        "100",
                        "--quote-notional",
                        "0.1",
                        "--max-reserve-fraction",
                        "0.1",
                        "--max-volume-fraction",
                        "0.1",
                        "--ledger",
                        str(ledger_path),
                        "--experiment-id",
                        "test-run",
                    ]
                )
            self.assertEqual(status, 0)
            payload = json.loads(output.getvalue())
            self.assertEqual(payload["trial_count"], 2)
            self.assertEqual(payload["ledger_rows_appended"], 2)
            ledger_records = [json.loads(line) for line in ledger_path.read_text().splitlines()]
            self.assertEqual(len(ledger_records), 2)
            diagnostics = payload["fixed_notional_outcome_diagnostics"]
            self.assertNotIn("compounded_return", diagnostics)
            self.assertNotIn("sequence_max_drawdown", diagnostics)
            for record in ledger_records:
                trial_diagnostics = record["evaluation"]["performance"]
                self.assertNotIn("compounded_return", trial_diagnostics)
                self.assertNotIn("sequence_max_drawdown", trial_diagnostics)


if __name__ == "__main__":
    unittest.main()
