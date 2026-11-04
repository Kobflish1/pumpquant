# Status and verification

This is a research source snapshot with a reproducible demo on generated data. The original project traded live. A later strategy failed, the assumptions behind its backtest were invalidated, and the candidates that followed were not validated. Nothing here is a validated strategy or authorised to trade.

## Verified for this release

Checked on 4 November 2026 on macOS arm64, with Python 3.12.14 and 3.14.7 and Rust 1.97.1:

| Check | Result | What it establishes |
| :-- | :-- | :-- |
| `pumpquant demo`, seed 7 | 1,080 bars, 16 candidates, 48 fold trials, 3 folds, 47 test outcomes, byte-identical to `examples/synthetic-demo.json`, checked by `tests/test_demo.py` | The generate, feature, replay and search path runs and is deterministic |
| Python tests | 489 passed on 3.12 and on 3.14 | Decoding, availability, exact math, replay, wallet validation, source adapters and the read-only dashboard behave as specified on controlled fixtures |
| Static checks | ruff clean; mypy `--strict` clean on 115 source files | Consistent style and fully typed code |
| Rust | Both crates format-clean, zero compiler warnings; 11 decoder and 5 search-kernel tests passed | Parser and CLI error contracts; the search kernels build and pass their tests |
| Python/Rust decoder parity | 16 valid, 4 targeted malformed or wrong-version, and 200 mutated or random-byte fixtures matched | The two decoders agree on the bounded wire contract |
| Dashboard preview | Served by the real read-only backend on demo data; snapshot, trade and chart endpoints checked | The dashboard runs with no private inputs |

`make check` runs every row above except the preview. The test run reports four warnings because `fork` is used in a multi-threaded process. The search workers fork on purpose so they can share large read-only arrays without copying; the warning comes from the test runner's own threads.

The decoder parity script also prints timings from a 16-record run. They exist to exercise the harness and are not a benchmark. No throughput claim is made for either crate.

## Reproduce

```sh
python -m pip install -e '.[dev,data,research,archive]'
make check
pumpquant preview
```

`make check` needs a Rust toolchain. The decoder crate has no third-party dependencies; the search crate fetches Rayon and Serde on its first build. The GitHub workflow in `.github/workflows/checks.yml` runs the same targets but has not yet run on GitHub, so no CI result is claimed.

## Outside the evidence

- Private market archives, live ledgers, balances, transaction signatures and operating configs are not included. The [case study](PARITY_CASE_STUDY.md) reports findings from the historical audit; this package does not reproduce that audit.
- The tests exclude original checks tied to omitted cloud provisioning, private approvals, live runner wrappers and historical artifact files. Passing this suite is not a claim that every original test was run.
- Generated data cannot show a market edge. The dashboard labels it as simulated, shows fixed-notional outcomes, and leaves funded-portfolio views empty.
- Provider-backed acquisition, chain submission, adaptive fee learning and live integrations were not exercised for this release.
- No billable or on-chain actions were taken. Installing public packages was the only network activity.
- Redistribution rights for the whole repository are not yet granted. Existing crate metadata and third-party notices are kept as they were.
