<picture>
  <source srcset="assets/hero.avif" type="image/avif">
  <img src="assets/hero.webp" width="100%" alt="PumpQuant: the title resolves in glass, then the research dashboard rises into view.">
</picture>

<br>

**PumpQuant tests trading ideas for newly launched Solana tokens the way they would really have played out: with the delays, fees, failed transactions and capital limits a live trade runs into.**

It began as a live trading system, and its early stage was profitable. When a later strategy's live results stopped matching its backtest, tracing the gap uncovered six mismatches between backtest and reality. PumpQuant was rebuilt so none of them can recur, and this repository is that engine.

<img src="assets/glance.png" width="100%" alt="What it is: a research engine for Pump.fun tokens on Solana that decodes raw chain data, rebuilds every market in order and prices each trade exactly. The problem: in these markets seconds decide the trade, and a backtest that ignores delays, fees and failed transactions flatters every idea. How it works: each decision sees only what had arrived, then is replayed with latency, retries and costs on data it has never seen. What it caught: a strategy that looked better on paper than it could ever be live; six mismatches, the subtlest a look-ahead flaw; under honest timing it failed all 16 test windows and was retired. Built with Python 3.11+, Rust, DuckDB, Polars and PyArrow. 489 tests, strict typing, Rust–Python parity.">

## How the flaw was found

The live run and the backtest disagreed, so the run was reconciled trade by trade.

<img src="assets/audit.png" width="100%" alt="Take-profit exits captured: the backtest expected 34 percent, the live run captured 12 percent; under the model's own rate a result this low had a 0.04 percent chance. Of 49 live round trips, 10 crossed their take-profit price and only 6 captured it. Decision to fill, measured end to end: entry 1.9 seconds at the median and 4.9 seconds at the 99th percentile; the slowest exit took about 71 seconds after retries. From the internal audit of the historical live run; the private ledgers are not part of this repository.">

Each gap became a rule in the code:

<img src="assets/rules.png" width="100%" alt="Four rules. A decision only sees what had arrived: trades from the rest of a boundary second are excluded. The whole path to a fill is measured: monitoring cadence, retries and slow RPC responses count. A strategy includes how it exits: exits replay repricing, failed sends and attempt limits. Uncertainty is reported honestly: simulation precision and real market uncertainty are kept apart.">

The full account is in the [case study](docs/PARITY_CASE_STUDY.md).

## How a strategy is tested

<img src="assets/method.png" width="100%" alt="Four steps beside a to-scale diagram: in each of three periods the rule learns on earlier tokens, then is tested on the next six it has never seen.">

A rule is tuned on earlier tokens, frozen, then tested on the tokens that launched next. Every candidate tried is recorded with hashes of the rule and the data, so a good result can be weighed against how many ideas were tried to find it.

## Every trade, replayed

<img src="assets/replay.png" width="100%" alt="A candlestick chart of one simulated trade, marking the signal, the buy one second later and the stop-loss sell, with the result after fees.">

Each trade replays the signal, the buy at the price then available, and the exit when its rule fired, with fees and failed attempts counted. This chart comes from the bundled demo, which runs on generated data.

## Scope

- **A research engine, not a trading bot.** The strategy that traded was retired by this engine, and no later candidate has yet cleared its gates.
- **The demo runs on generated data,** so anyone can run the whole method in a second. It shows how the method works, not whether a strategy makes money.
- **Private data stays private:** market archives, ledgers, wallets, balances and operating configuration are not included.
- **Provider-backed acquisition is included** but was not exercised for this release.
- **Licence:** none has been granted yet. Third-party notices are kept in [Provenance](docs/PROVENANCE.md).

## Run it

```sh
python -m pip install -e '.[dev,data,research,archive]'
pumpquant demo        # the whole pipeline on generated data, in about a second
pumpquant preview     # the dashboard above, at http://127.0.0.1:8765
make check            # lint, strict types, tests, Rust, and Python/Rust parity
```

## Inside

<img src="assets/pipeline.png" width="100%" alt="The pipeline, one package per stage. acquisition: archive raw chain data, sealed with content hashes. decoding: wire bytes to Pump and PumpSwap events, failing closed. state: rebuild each market in chain order with the moment each event became visible. features: compute signals only from what had arrived. pricing: quote trades exactly in integer protocol units. execution: replay latency, retries, fees, failures and shared capital. research: search chronologically with every candidate recorded. validation: statistical and economic gates on unseen data.">

<img src="assets/proof.png" width="100%" alt="489 tests passing on Python 3.12 and 3.14. 115 source files under strict type checking. 220 of 220 decoder checks where Rust and Python agree. 0 lint errors and compiler warnings. make check runs all of it in one command.">

Go deeper: the design choices and their trade-offs in [Engineering decisions](docs/DECISIONS.md), the pipeline in [Architecture](docs/ARCHITECTURE.md), and everything verified for this release in [Status](docs/STATUS.md).
