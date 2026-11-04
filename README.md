<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/icon-dark.png">
    <img src="assets/icon-light.png" width="88" alt="PumpQuant">
  </picture>
</p>

<h3 align="center">Backtesting and research engine for Pump.fun tokens on Solana</h3>

<p align="center">
  <a href="#quick-start"><b>Quick start</b></a> ·
  <a href="#background"><b>Background</b></a> ·
  <a href="docs/ARCHITECTURE.md"><b>Architecture</b></a> ·
  <a href="docs/PARITY_CASE_STUDY.md"><b>Case study</b></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11%2B-0a84ff?style=flat-square&labelColor=1c1c1e" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/Rust-decoder-64d2ff?style=flat-square&labelColor=1c1c1e" alt="Rust decoder">
  <img src="https://img.shields.io/badge/mypy-strict-30d158?style=flat-square&labelColor=1c1c1e" alt="mypy strict">
  <img src="https://img.shields.io/badge/tests-489%20passing-30d158?style=flat-square&labelColor=1c1c1e" alt="489 tests passing">
</p>

<br>

<picture>
  <source srcset="assets/hero.avif" type="image/avif">
  <img src="assets/hero.webp" width="100%" alt="PumpQuant: the title resolves in glass, then the research dashboard rises into view.">
</picture>

<br>

PumpQuant replays every [Pump.fun](https://pump.fun) trade with the latency, fees and failed transactions a live trader would have faced, and tests each strategy only on data it has never seen.

- **Exact pricing**: integer quotes that match the Pump and PumpSwap programs, rounding included
- **No look-ahead**: every feature is computed only from data that had arrived at decision time
- **Realistic execution**: latency, retries, priority fees, failed transactions and shared capital
- **Walk-forward search**: chronological train and test periods, with every trial logged
- **Strict decoding**: Solana transactions and program events, with a Rust decoder checked against the Python one
- **Dashboard**: a local, read-only view of every run, period and trade

<br>

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/scene-answer-light.png">
    <img src="assets/scene-answer.png" width="49%" alt="The dashboard's answer screen: profitable in 2 of 3 test periods, +0.217 SOL combined across 47 outcomes.">
  </picture>
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/scene-periods-light.png">
    <img src="assets/scene-periods.png" width="49%" alt="Test periods side by side: -0.127 SOL and +0.268 SOL, each with its trades as bars.">
  </picture>
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/scene-replay-light.png">
    <img src="assets/scene-replay.png" width="49%" alt="A replayed trade on token #19: signal, buy one second later, stop-loss sell, -0.024 SOL after fees.">
  </picture>
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="assets/scene-method-light.png">
    <img src="assets/scene-method.png" width="49%" alt="The walk-forward method: learn on earlier tokens, then test on the next six, drawn to scale.">
  </picture>
</p>
<p align="center"><sub>The local dashboard, running on the bundled demo. Simulated data.</sub></p>

## Quick start

```sh
pip install -e '.[dev,data,research,archive]'
pumpquant demo
```

The demo generates 36 tokens, searches 16 versions of a momentum rule and tests each period's best version on the tokens that launched next:

```python
from pumpquant.demo import run_demo

run = run_demo(tokens=36, seed=7)
for fold in run.report.folds:
    result = fold.test_backtest
    print(f"period {fold.number + 1}: {result.position_outcomes} outcomes, {result.total_quote_pnl:+.3f} SOL")
```

```text
period 1: 14 outcomes, -0.127 SOL
period 2: 16 outcomes, +0.268 SOL
period 3: 17 outcomes, +0.076 SOL
```

To explore the same run in the dashboard:

```sh
pumpquant preview    # http://127.0.0.1:8765
```

The demo data is generated. It shows how the method works, not whether a strategy makes money.

## Background

PumpQuant started as a live trading system. Its early stage was profitable, but a later strategy's live results drifted from its backtest. A trade-by-trade audit of 49 live round trips found six differences between the backtest and reality, including:

| Backtest assumption | Live run |
| :-- | :-- |
| Features could use every trade in the decision's second | A live decision only sees trades that have already arrived |
| About 34% of take-profit exits captured | 6 of 49 trades (12%); 10 crossed the target |
| Fill time measured from send to processing | 1.9 s median and 4.9 s 99th percentile from decision to fill |
| Exits fill once the price is reached | Retries and slow RPCs pushed the slowest exit to about 71 s |

The engine was rebuilt so none of these can recur. Under the corrected model the strategy lost money in all 16 test windows and was retired. The full audit is in the [case study](docs/PARITY_CASE_STUDY.md).

## Project layout

```text
src/pumpquant/
  acquisition/   download and archive transactions, blocks and streams
  decoding/      Solana wire format and Pump/PumpSwap program events
  state/         market state rebuilt in chain order
  features/      features computed at the decision boundary
  pricing/       exact quotes in integer protocol units
  execution/     replay with latency, retries, fees and shared capital
  research/      walk-forward search and the trial ledger
  validation/    statistical and economic checks
  audits/        the historical investigations
  dashboard/     the read-only dashboard server and preview
rust/
  pump-decoder-kernel/   transaction decoder, checked against Python
  m4-growth-search/      parallel search kernels
```

## Development

```sh
make check
```

This runs ruff, mypy in strict mode, the 489 tests, the Rust tests, and a parity check of the Rust decoder against the Python one on 220 fixtures. See [Status](docs/STATUS.md) for the latest verified results.

## Documentation

- [Architecture](docs/ARCHITECTURE.md): how the pipeline fits together
- [Engineering decisions](docs/DECISIONS.md): the main design choices and their trade-offs
- [Case study](docs/PARITY_CASE_STUDY.md): the audit that led to the rebuild
- [Status](docs/STATUS.md): what was verified for this release, and what is out of scope

## License

License terms have not been set yet. Third-party notices are listed in [Provenance](docs/PROVENANCE.md).

<br>

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/icon-dark.png">
    <img src="assets/icon-light.png" width="36" alt="">
  </picture>
  <br>
  <sub>Built by <a href="https://github.com/Kobflish1">Elias Hassari</a></sub>
</p>
