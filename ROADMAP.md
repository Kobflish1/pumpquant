# Roadmap

What comes next, measured on 4 November 2026. Each item says what done looks like. [Status](docs/STATUS.md) lists what is already verified.

The plan runs in three steps:

- **Now:** finish this first public release (release, code structure, tests, Rust and README).
- **Next:** turn the engine into a product people can use without reading its code, and test the integrations it will need.
- **Later:** live trading from a tested strategy, and markets beyond Pump.fun.

## Now

### Release

Shipped with this release:

- **CI on GitHub Actions.** `.github/workflows/checks.yml` runs the same targets as `make check` on Python 3.12, 3.13 and 3.14, and the README badge links to its runs.
- **The first release.** [`v0.1.0`](https://github.com/Kobflish1/pumpquant/releases/tag/v0.1.0), with the built wheel and source archive attached.

Every item below has an [open issue](https://github.com/Kobflish1/pumpquant/issues).

### Code structure

The aim is readable units with clear names, not mechanical chopping. Every split keeps the recorded demo output, the 220 decoder parity fixtures and every persisted hash byte-identical, and is proved by recording the touched functions' return values across the test suite before and after.

- **Split the three modules over 1,500 lines** along their real seams:
  - `decoding/program_events.py` (2,338 lines): the event records and their checks, the table of state-event layouts (about 1,100 lines on its own), and the byte reader with the log decoders;
  - `acquisition/bigquery/archive.py` (1,764 lines): schema and value decoding, Parquet shards and the manifest, and the S3 upload;
  - `validation/capacity_gate.py` (1,621 lines): exchange rates and daily attribution, inventory capacity, the shared-wallet replay, and the starting-capital ladder with its report.
- **Split the 30 functions over 150 lines**, starting with the longest:
  - `features/events.py`: `iter_causal_event_features` (377 lines) and `event_feature_input_from_raw` (244);
  - `audits/live_rule/execution_audit.py`: `summarize_live_execution` (343) and `replay_fee_skip_schedule` (153);
  - `validation/capacity_bridge.py`: `build_exact_capacity_replay_factory` (325), with its inner `factory` (263);
  - `research/tail_exit.py`: `create_tail_exit_labels_sql` (314), and `research/event_screen.py`: `create_event_labels_sql` (191). Most of their length is SQL, so the split is into named query stages;
  - `features/alpha.py`: `_market_features` (276);
  - `audits/live_rule/replay.py`: `simulate_path` (267);
  - `acquisition/transaction_hydrator.py`: `_hydrate_segment` (264);
  - `acquisition/signature_inventory.py`: `build_signature_inventory` (241);
  - `features/bars.py`: `iter_bars_chain_ordered` (233), with its inner `_iter` (205);
  - `execution/pump_replay.py`: `simulate_pump_surface` (226) and `simulate_pump_path` (201);
  - `state/lifecycle_catalog.py`: `materialize_lifecycle_catalog` (220);
  - `state/materializer.py`: `materialize_amm_states` (218);
  - `validation/capacity_gate.py`: `replay_shared_wallet` (217) and the settings check in `__post_init__` (210);
  - `features/lifecycle.py`: `materialize_lifecycle_features` (202);
  - `execution/shared_wallet.py`: `replay_dynamic_shared_wallet` (202);
  - `research/distributed_reducer.py`: `reduce_distributed_ledgers` (200);
  - `execution/lifecycle_replay.py`: `simulate_reference_pump_surface` (198);
  - `acquisition/bigquery/approval.py`: `validate_approval_artifact` (192);
  - `acquisition/bigquery/storage_archive.py`: `archive_completed_query_job` (187);
  - `cli/bigquery.py`: `_register_archiving` (180), and `cli/research.py`: `register` (160), each into one function per command group;
  - `execution/priority_fees.py`: `select_priority_fee` (155);
  - `acquisition/block_inventory_audit.py`: `audit_block_inventory` (152).

### Tests

Statement coverage is 77%. The largest gaps:

- **`research/event_screen.py`** (32% of 515 statements) **and `research/event_research.py`** (37% of 207). Add tests that run each screen end to end on a small generated archive and check its labels and its ledger.
- **The execute path of `acquisition/bigquery/daily.py`** (31% of 504). Its validation and dry-run paths are tested; the execute path needs a billed BigQuery job. Add a fake client that records the jobs it is given, so the path can be tested without one.
- **`dashboard/server.py`** (56%). Cover the error responses and the snapshot assembly for runs other than the demo.
- **Fork warnings.** The test run reports four warnings because the search workers fork inside a multi-threaded process. Move the workers to shared memory with the `forkserver` start method, or prove the fork is safe and say why in a comment.

### Rust

- **Run clippy** on both crates with warnings as errors, fix what it finds, and add it to `make check` and CI. It has not been run yet.
- **Benchmarks.** No throughput is claimed for either crate. Add a reproducible benchmark for the decoder and the search kernels, with the machine and inputs recorded, before making any speed claim.

### README

- **A phone layout for the figures.** The scenes now drop to one column on narrow screens, but the four framed figures (terminal, architecture, audit and `make check`) are composed at 880 px and read small at phone width. Compose narrow variants. GitHub rewrites a `<picture>` source whose media query mixes width and colour scheme when a reader has picked a theme, so each variant must be chosen by width alone or drawn to work in both themes.
- **Keep polishing the visuals.** Every figure, scene and the film follow one frame language. Done when each new feature arrives with figures in that language, captured from the real product, in both themes.

## Next

### The product

- **Desktop apps for macOS and Windows.** Native apps, for Apple silicon Macs and for Windows, become the main way to use PumpQuant; the browser dashboard stays for quick, local viewing. Done when a signed app on each platform opens a run, replays its trades and starts a new test.
- **A guided strategy builder.** Build a strategy step by step and test it on unseen data without writing code. Done when a new user can go from a blank strategy to a tested result in the app, and the strategy it saves runs unchanged from the command line.
- **Easy data import.** Load market data from common file formats such as CSV and Parquet, or fetch official chain data in one step. Done when both paths produce the same archive format the engine already reads, with a check that reports anything missing.
- **Plainer wording and simpler screens.** Every term explained where it first appears, and the fewest steps for each common task. Done when every screen has been reviewed against a short glossary and a first-time walkthrough.

### Tested integrations

These were not exercised for this release, and each needs offline tests against recorded responses before it can be called supported:

- provider-backed acquisition (RPC and stream sources, BigQuery exports);
- chain submission and adaptive priority-fee learning;
- the live integrations the original system used.

## Later

- **From a tested strategy to live trading in a few steps.** Built on the tested integrations above, with a live status view for every running strategy: positions, fills and how live results compare with the backtest. Done when a strategy can be moved to live trading with explicit limits and stopped at any time, and every live trade is reconciled against the chain, as the original system did.
- **More chains and launch platforms.** Support for other blockchains and for launch platforms beyond Pump.fun, each with its own exact pricing and decoder checks. Done when a second platform passes the same parity and pricing tests as Pump.fun.

More will be added here as the work goes on.
