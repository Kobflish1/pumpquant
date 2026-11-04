# Status and verification

## Verified for this release

Checked on 4 November 2026 on macOS arm64, with Python 3.12.14, 3.13.15 and 3.14.7 and Rust 1.97.1:

| Check | Result | What it establishes |
| :-- | :-- | :-- |
| `pumpquant demo`, seed 7 | 1,080 bars, 16 candidates, 48 fold trials, 3 folds, 47 test outcomes, byte-identical to `examples/synthetic-demo.json` (asserted by `tests/test_demo.py`) | The generate, feature, replay and search path runs and is deterministic |
| Python tests | 508 passed on 3.12, on 3.13 and on 3.14 | Decoding, availability, exact math, replay, wallet validation, source adapters, the BigQuery coordinator and the read-only dashboard behave as specified on controlled fixtures |
| Static checks | ruff clean; mypy `--strict` clean on 112 source files | Consistent style and fully typed code |
| Rust | Both crates format-clean and built with warnings as errors; 11 decoder and 5 search-kernel tests passed | Parser and CLI error contracts; the search kernels build and pass their tests |
| Python/Rust decoder parity | 16 valid, 4 targeted malformed or wrong-version, and 200 mutated or random-byte fixtures matched | The two decoders agree on the bounded wire contract |
| Examples | `examples/periods.py` and `examples/quote.py` run and print the output shown in the README, asserted byte for byte by `tests/test_examples.py` | The Quick start is real |
| Installed wheel | Built with `uv build`, installed with `[data,research]` into a clean Python 3.12 environment, then `pumpquant preview` served the pages, scripts, fonts and API from the installed package | The dashboard does not depend on a source checkout |
| Test coverage | 77% of statements, measured with pytest-cov | Where the remaining gaps are (below) |

`make check` runs every row above except the wheel install and the coverage run. The test run reports four warnings because `fork` is used in a multi-threaded process. The search workers fork on purpose so they can share large read-only arrays without copying; the warning comes from the test runner's own threads.

The decoder parity script also prints timings from a 16-record run. They exist to exercise the harness and are not a benchmark. No throughput claim is made for either crate.

## Reproduce

```sh
python -m pip install -e '.[dev]'
make check
pumpquant preview
```

`make check` needs a Rust toolchain. The decoder crate has no third-party dependencies; the search crate fetches Rayon and Serde on its first build. The GitHub workflow in `.github/workflows/checks.yml` runs the same targets on Python 3.12, 3.13 and 3.14 but has not yet run on GitHub, so no CI result is claimed.

## Python versions

PumpQuant requires Python 3.12 or later. On 3.11 the suite was run too: 506 of 508 tests pass, and the two that fail differ only in the last digit of a floating-point sum. Python 3.12 made `sum()` of floats more accurate, so the recorded demo output, which is checked byte for byte, is the 3.12 result.

## Data

PumpQuant processes only public on-chain market data from Solana. Wallet addresses, which are pseudonymous, are aggregated into market features (for example repeat-buyer flow or creator selling) and are never linked to people. The project reads no personal, account or off-chain records. This repository contains no real market data: the demo data, the test fixtures and the parity fixtures are generated, and the only addresses in it are public program IDs and test token mints.

## Scope

This is a research source snapshot with a reproducible demo on generated data. The original project traded live; a later strategy failed, the assumptions behind its backtest were invalidated, and the candidates that followed were not validated. Nothing here is a validated strategy or authorised to trade.

- Private market archives, live ledgers, balances, transaction signatures and operating configs are not included. The [case study](PARITY_CASE_STUDY.md) reports findings from the historical audit; this package does not reproduce that audit.
- The tests exclude original checks tied to omitted cloud provisioning, private approvals, live runner wrappers and historical artifact files. Passing this suite is not a claim that every original test was run.
- The largest modules with the least coverage are the event screens in `research/event_screen.py` and `research/event_research.py` (about a third of their statements run in tests) and the execute path of `acquisition/bigquery/daily.py`, which needs a billed BigQuery job; its validation and dry-run paths are tested.
- Generated data cannot show a market edge. The dashboard labels it as simulated, shows fixed-notional outcomes, and leaves funded-portfolio views empty.
- Provider-backed acquisition, chain submission, adaptive fee learning and live integrations were not exercised for this release.
- The [roadmap](../ROADMAP.md) lists the work still ahead, with what done looks like for each item.
- No billable or on-chain actions were taken. Installing public packages and a Python 3.11 build for the version check were the only network activity.

## Licence

PumpQuant is released under the [MIT License](../LICENSE). The bundled Lightweight Charts library (Apache-2.0) and the dashboard fonts (SIL Open Font License) keep their own licences; [Provenance](PROVENANCE.md) lists them with their notices.
