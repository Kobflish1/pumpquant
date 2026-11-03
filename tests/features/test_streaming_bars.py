from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from importlib.util import find_spec
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pumpquant.cli import main
from pumpquant.core.io import read_bars, write_parquet
from pumpquant.core.schema import Side, SwapEvent, VenuePhase
from pumpquant.features.bars import build_bars, iter_bars_chain_ordered


def _event(
    event_id: str,
    *,
    market_id: str,
    slot: int,
    block_time_ns: int,
    tx_index: int = 0,
    phase: VenuePhase = VenuePhase.BONDING_CURVE,
    side: Side = Side.BUY,
    price: float = 1.0,
    quote_amount: float = 1.0,
    trader: str | None = None,
) -> SwapEvent:
    return SwapEvent(
        event_id=event_id,
        market_id=market_id,
        mint=f"{market_id}-MINT",
        quote_mint="SOL",
        slot=slot,
        tx_index=tx_index,
        instruction_index=0,
        block_time_ns=block_time_ns,
        observed_at_ns=block_time_ns + 10,
        phase=phase,
        side=side,
        base_amount=2.0,
        quote_amount=quote_amount,
        price=price,
        virtual_base_reserve=100.0 - slot,
        virtual_quote_reserve=200.0 + slot,
        real_base_reserve=90.0 - slot,
        real_quote_reserve=190.0 + slot,
        trader=trader,
    )


class StreamingBarsTests(unittest.TestCase):
    def test_matches_build_bars_for_interleaved_markets_and_phases(self) -> None:
        events = [
            _event("a0", market_id="A", slot=1, block_time_ns=100, trader="alice"),
            _event(
                "b0",
                market_id="B",
                slot=2,
                block_time_ns=200,
                side=Side.SELL,
                trader="bob",
            ),
            _event(
                "a1",
                market_id="A",
                slot=3,
                block_time_ns=300,
                tx_index=1,
                phase=VenuePhase.PUMPSWAP,
                price=2.0,
                quote_amount=2.0,
                trader="alice",
            ),
            _event(
                "b1",
                market_id="B",
                slot=4,
                block_time_ns=1_100,
                price=3.0,
                quote_amount=3.0,
                trader="carol",
            ),
        ]
        launches = {"A-MINT": 0, "B-MINT": 0}
        expected = build_bars(events, interval_ns=1_000, launch_times=launches)
        actual = list(iter_bars_chain_ordered(events, interval_ns=1_000, launch_times=launches))
        expected_time_major = sorted(
            expected,
            key=lambda bar: (bar.start_time_ns, bar.market_id, bar.phase.value),
        )
        self.assertEqual(actual, expected_time_major)

    def test_rejects_duplicate_order_regression_and_bucket_regression(self) -> None:
        first = _event("first", market_id="A", slot=1, block_time_ns=100)
        duplicate = replace(first, event_id="duplicate")
        with self.assertRaisesRegex(ValueError, "ordering_key"):
            list(iter_bars_chain_ordered([first, duplicate], 1_000))
        with self.assertRaisesRegex(ValueError, "duplicate event_id"):
            list(iter_bars_chain_ordered([first, first], 1_000))

        later = _event("later", market_id="A", slot=2, block_time_ns=1_100)
        earlier_bucket = _event("earlier", market_id="A", slot=3, block_time_ns=100)
        with self.assertRaisesRegex(ValueError, "time-bucket starts"):
            list(iter_bars_chain_ordered([later, earlier_bucket], 1_000))

    def test_input_is_not_consumed_until_iteration(self) -> None:
        consumed: list[str] = []

        def events():
            consumed.append("first")
            yield _event("first", market_id="A", slot=1, block_time_ns=100)

        iterator = iter_bars_chain_ordered(events(), 1_000)
        self.assertEqual(consumed, [])
        next(iterator)
        self.assertEqual(consumed, ["first"])

    @unittest.skipUnless(find_spec("pyarrow"), "requires pyarrow")
    def test_cli_rewrites_partial_stream_output_on_late_regression(self) -> None:
        first = _event("first", market_id="A", slot=1, block_time_ns=100)
        second = _event("second", market_id="A", slot=2, block_time_ns=1_100)
        regression = _event("regression", market_id="A", slot=3, block_time_ns=100)
        with TemporaryDirectory() as directory:
            events_path = Path(directory) / "events.parquet"
            output_path = Path(directory) / "bars.jsonl"
            write_parquet(events_path, [first, second, regression])
            with redirect_stdout(StringIO()):
                self.assertEqual(
                    main(
                        [
                            "bars",
                            str(events_path),
                            str(output_path),
                            "--interval-ms",
                            "0.001",
                        ]
                    ),
                    0,
                )
            self.assertEqual(
                read_bars(output_path),
                build_bars([first, second, regression], interval_ns=1_000),
            )

    @unittest.skipUnless(find_spec("pyarrow"), "requires pyarrow")
    def test_cli_stream_failure_preserves_target_and_cleans_sibling(self) -> None:
        first = _event("first", market_id="A", slot=1, block_time_ns=100)
        second = _event("second", market_id="A", slot=2, block_time_ns=1_100)
        regression = _event("regression", market_id="A", slot=3, block_time_ns=100)
        with TemporaryDirectory() as directory:
            events_path = Path(directory) / "events.parquet"
            output_path = Path(directory) / "bars.jsonl"
            write_parquet(events_path, [first, second, regression])
            output_path.write_text("existing output\n", encoding="utf-8")

            with (
                patch("pumpquant.cli.research._MAX_BAR_STREAM_FALLBACK_ROWS", 2),
                self.assertRaisesRegex(ValueError, "materializing fallback"),
                redirect_stdout(StringIO()),
            ):
                main(
                    [
                        "bars",
                        str(events_path),
                        str(output_path),
                        "--interval-ms",
                        "0.001",
                    ]
                )

            self.assertEqual(output_path.read_text(encoding="utf-8"), "existing output\n")
            self.assertEqual(list(Path(directory).glob(".bars.jsonl.*.partial")), [])


if __name__ == "__main__":
    unittest.main()
