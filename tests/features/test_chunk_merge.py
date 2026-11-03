from __future__ import annotations

import hashlib
import json
import unittest
from importlib.util import find_spec
from pathlib import Path
from tempfile import TemporaryDirectory

from pumpquant.core.io import read_bars, write_parquet
from pumpquant.core.schema import Side, SwapEvent, VenuePhase
from pumpquant.features.bars import build_bars
from pumpquant.features.streaming_bars import (
    NORMALIZED_KIND,
    PHYSICAL_ORDER,
    ChunkBarsError,
    iter_merged_swap_events,
    write_chunk_bars,
)


def _event(
    event_id: str,
    *,
    slot: int,
    block_time_ns: int,
    mint: str = "MINT",
) -> SwapEvent:
    return SwapEvent(
        event_id=event_id,
        market_id="MARKET",
        mint=mint,
        quote_mint="SOL",
        slot=slot,
        tx_index=0,
        instruction_index=0,
        block_time_ns=block_time_ns,
        observed_at_ns=block_time_ns,
        phase=VenuePhase.BONDING_CURVE,
        side=Side.BUY,
        base_amount=1.0,
        quote_amount=1.0,
        price=1.0,
        virtual_base_reserve=100.0,
        virtual_quote_reserve=100.0,
        real_base_reserve=100.0,
        real_quote_reserve=100.0,
    )


@unittest.skipUnless(find_spec("pyarrow"), "requires pyarrow")
class ChunkBarsTests(unittest.TestCase):
    def _chunk(self, root: Path, name: str, events: list[SwapEvent]) -> Path:
        directory = root / name
        directory.mkdir()
        parquet = directory / "swaps.parquet"
        write_parquet(parquet, events)
        manifest = {
            "kind": NORMALIZED_KIND,
            "version": 1,
            "row_count": len(events),
            "physical_order": list(PHYSICAL_ORDER),
            "files": {
                "swaps.parquet": {
                    "byte_count": parquet.stat().st_size,
                    "sha256": hashlib.sha256(parquet.read_bytes()).hexdigest(),
                }
            },
        }
        (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return directory

    def test_global_merge_aggregates_bars_across_chunk_boundary(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = [_event("a", slot=1, block_time_ns=100)]
            second = [_event("b", slot=2, block_time_ns=200)]
            chunks = [self._chunk(root, "a", first), self._chunk(root, "b", second)]
            merged = list(iter_merged_swap_events(chunks, batch_size=1))
            self.assertEqual([event.event_id for event in merged], ["a", "b"])

            output = root / "bars.jsonl"
            report = write_chunk_bars(chunks, output, 1_000, {"MINT": 0})
            self.assertEqual(report["bars_written"], 1)
            self.assertEqual(
                read_bars(output),
                sorted(
                    build_bars(first + second, 1_000, {"MINT": 0}),
                    key=lambda bar: (bar.start_time_ns, bar.market_id, bar.phase.value),
                ),
            )

    def test_duplicate_or_source_regression_is_rejected(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._chunk(
                root,
                "regression",
                [_event("a", slot=2, block_time_ns=200), _event("b", slot=1, block_time_ns=100)],
            )
            with self.assertRaisesRegex(ChunkBarsError, "ordering regression"):
                list(iter_merged_swap_events([root / "regression"]))

            self._chunk(root, "a", [_event("a", slot=2, block_time_ns=200)])
            self._chunk(root, "b", [_event("a", slot=2, block_time_ns=200)])
            with self.assertRaisesRegex(ChunkBarsError, "duplicate/overlapping"):
                list(iter_merged_swap_events([root / "a", root / "b"]))

    def test_hash_failure_is_atomic_and_require_anchor_is_explicit(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            chunk = self._chunk(root, "a", [_event("a", slot=1, block_time_ns=100)])
            output = root / "bars.jsonl"
            output.write_text("keep\n", encoding="utf-8")
            parquet = chunk / "swaps.parquet"
            parquet.write_bytes(parquet.read_bytes() + b"tampered")
            with self.assertRaisesRegex(ChunkBarsError, "byte/hash"):
                write_chunk_bars([chunk], output, 1_000, {})
            self.assertEqual(output.read_text(encoding="utf-8"), "keep\n")

            # A mapping is never inferred from the event.  Strict mode fails,
            # while permissive mode preserves the existing None age contract.
            fixed = self._chunk(root, "fixed", [_event("a", slot=1, block_time_ns=100)])
            with self.assertRaisesRegex(ChunkBarsError, "no true launch-time"):
                write_chunk_bars([fixed], output, 1_000, {}, require_all_mints=True)
            write_chunk_bars([fixed], output, 1_000, {})
            self.assertIsNone(read_bars(output)[0].token_age_seconds)

    def test_manifest_order_seal_and_inventoried_output_collision(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            chunk = self._chunk(root, "a", [_event("a", slot=1, block_time_ns=100)])
            manifest_path = chunk / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            del manifest["physical_order"]
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ChunkBarsError, "physical_order"):
                write_chunk_bars([chunk], root / "bars.jsonl", 1_000, {"MINT": 0})

            # Add a correctly inventoried sidecar and ensure output cannot
            # overwrite any manifested file, not only swaps.parquet.
            chunk = self._chunk(root, "b", [_event("b", slot=2, block_time_ns=200)])
            sidecar = chunk / "audit.json"
            sidecar.write_text("audit", encoding="utf-8")
            manifest = json.loads((chunk / "manifest.json").read_text(encoding="utf-8"))
            manifest["files"]["audit.json"] = {
                "byte_count": sidecar.stat().st_size,
                "sha256": hashlib.sha256(sidecar.read_bytes()).hexdigest(),
            }
            (chunk / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ChunkBarsError, "distinct"):
                write_chunk_bars([chunk], sidecar, 1_000, {"MINT": 0})

    def test_anchored_only_skips_unanchored_swaps_and_reports_count(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            chunks = [
                self._chunk(root, "a", [_event("a", slot=1, block_time_ns=100)]),
                self._chunk(root, "b", [_event("b", slot=2, block_time_ns=200, mint="OTHER")]),
            ]
            output = root / "bars.jsonl"
            report = write_chunk_bars(
                chunks,
                output,
                1_000,
                {"MINT": 0},
                anchored_only=True,
            )
            self.assertEqual(report["swaps_skipped_unanchored"], 1)
            self.assertEqual(read_bars(output)[0].mint, "MINT")
            with self.assertRaisesRegex(ChunkBarsError, "mutually exclusive"):
                write_chunk_bars(
                    chunks,
                    output,
                    1_000,
                    {"MINT": 0},
                    anchored_only=True,
                    require_all_mints=True,
                )


if __name__ == "__main__":
    unittest.main()
