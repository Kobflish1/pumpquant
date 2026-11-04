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

## Licences and notices

No repository-wide licence has been granted. The Rust manifests already declare MIT for those crates, and that declaration is kept. It does not cover the rest of the repository. Redistribution rights for the complete package should be resolved before public release.

The dashboard bundles Lightweight Charts 5.2.0 under Apache-2.0. Its distribution header is kept, the version-specific [LICENSE](../src/pumpquant/dashboard/static/vendor/LICENSE) and [NOTICE](../src/pumpquant/dashboard/static/vendor/NOTICE) files sit beside it, and the chart visibly credits TradingView. The dashboard fonts, Mona Sans and Commit Mono, are used under the SIL Open Font License; their licence files are in `src/pumpquant/dashboard/static/fonts/`.

The event schema lock identifies the upstream public protocol documentation revision and its content hashes. Source modules cite the upstream SDK or protocol definitions used for arithmetic and decoding. Those references record technical provenance; they do not imply endorsement or grant rights over an upstream project.

## Dependencies

Python dependencies and the optional extras are declared in `pyproject.toml`. Rust dependencies are recorded in each crate's manifest and lockfile. The demo and the preview make no provider calls. Acquisition and execution modules need separately configured services or datasets.
