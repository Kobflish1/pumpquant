# Provenance and dependencies

This repository is a curated release of PumpQuant's original source. Private research artifacts and operating documents are excluded. The package version is kept.

## What changed for this release

The research logic is the original. The changes are about structure and correctness:

- The flat package was grouped into subpackages by stage. Every move is listed in the [module map](MODULE-MAP.md), and tests now mirror the package layout.
- The command line was split into `cli/research.py` and `cli/bigquery.py`. The demo pipeline moved into `demo.py`, so the CLI and the dashboard preview share one implementation. Legacy command aliases were removed.
- Twenty identical file-hash helpers became one, in `core/hashing.py`. A 557-line re-export module became a short package map.
- Two defects were fixed. Reading `.zst` archives line by line failed on Python 3.11 to 3.13, where the fallback reader could not iterate lines; the reader is now buffered. A dashboard test still pointed at the old front-end file.
- The code was formatted with ruff, and strict mypy now passes everywhere. Fixes were made by narrowing types and naming invariants, not by silencing checks.
- In the Rust search crate, an unused function parameter and an unused manifest field were removed so it builds without warnings.
- The dashboard front end was redesigned. A plain-language overview (`index.html`) now leads to the full research terminal (`terminal.html`), served by the same read-only backend.

## The November 2026 cleanup

A second pass after an independent review:

- **Removed.** `audits/correction_v2` (about 9,900 lines that re-ran the corrected research grids by importing private runner scripts that are not in this snapshot, so none of it could run here), plus four other modules and ten functions and classes that nothing called. 226 legacy aliases went too; callers use the canonical names.
- **One home for hashing.** `core/hashing.py` now holds `canonical_json` and `sha256_bytes`, replacing the per-module copies. Every persisted hash is byte-identical; the parity fixtures and the recorded demo output still match.
- **Long functions split** along their natural seams, and `dashboard/server.py` split into configuration, manifest verification, the atlas and market lookups, and snapshot assembly. The BigQuery daily and batch archive runs moved out of the CLI into `acquisition/bigquery/daily.py`.
- **Plain names.** Codenames became descriptive module and crate names; the [module map](MODULE-MAP.md) lists each one. Persisted `kind` strings keep their spelling.
- **Packaging.** The dashboard's pages, scripts and fonts moved inside the package and load through `importlib.resources`, so an installed wheel serves `pumpquant preview`.
- **Defects found on the way.** A search test patched a function the search no longer called, so it could not fail; it now patches the evaluation that resumption must skip. Many docstrings still pointed at the old flat module names. The replay chart labelled the demo's clock as times of day; it now shows time since the token's launch.
- **Python 3.12+.** On 3.11, two tests differ in the last digit of a float sum, because 3.12 made `sum()` more accurate; the requirement now says so.
- **Wording.** Docstrings were edited for plain words and UK spelling.
- **Licence and dependencies.** The repository is now MIT-licensed. Four declared dependencies that no code imports were removed: Polars, orjson, websockets (with the `stream` extra that held only it) and Hypothesis.
- **Examples under test.** `tests/test_examples.py` checks that both Quick start scripts print exactly what the README shows. `examples/quote.py` was shortened to the steps it needs.

## Licences and notices

PumpQuant is released under the [MIT License](../LICENSE), declared in `pyproject.toml` as `license = "MIT"`. The two Rust crates were already MIT, and keep that declaration in their manifests.

The MIT License covers PumpQuant's own code; the bundled third-party files keep their own licences. The dashboard bundles Lightweight Charts 5.2.0 under Apache-2.0. Its distribution header is kept, the version-specific [LICENSE](../src/pumpquant/dashboard/static/vendor/LICENSE) and [NOTICE](../src/pumpquant/dashboard/static/vendor/NOTICE) files sit beside it, and the chart visibly credits TradingView. The dashboard fonts, Mona Sans and Commit Mono, are used under the SIL Open Font License; their licence files are in `src/pumpquant/dashboard/static/fonts/`.

The event schema lock identifies the upstream public protocol documentation revision and its content hashes. Source modules cite the upstream SDK or protocol definitions used for arithmetic and decoding. Those references record technical provenance; they do not imply endorsement or grant rights over an upstream project.

## Dependencies

Python dependencies and the optional extras are declared in `pyproject.toml`. Rust dependencies are recorded in each crate's manifest and lockfile. The demo and the preview make no provider calls. Acquisition and execution modules need separately configured services or datasets.
