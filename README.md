<picture>
  <source srcset="assets/hero.avif" type="image/avif">
  <img src="assets/hero.webp" width="100%" alt="PumpQuant: the title resolves in glass, then the research dashboard rises into view.">
</picture>

<br>

**PumpQuant tests trading ideas for newly launched Solana tokens the way they would really have played out: with the delays, fees, failed transactions and capital limits a live trade runs into.**

It started as a live trading system. Its early stage made money. A later strategy lost money, even though its backtest said it should win. Tracing why showed that the backtest had quietly let each decision see trades it could not have seen live. PumpQuant was rebuilt around that lesson, the rebuilt engine rejected the strategy, and this repository is that engine.

<br>

**What it is.** A research engine for [Pump.fun](https://pump.fun) tokens on Solana. It decodes raw chain data, rebuilds each market in order, prices every trade exactly, and judges a strategy only on data it has never seen.

**The problem.** In markets that move within seconds, the delay, the fee and the transaction that fails decide the trade. A backtest that ignores them flatters the idea it is meant to test.

**How it works.** Every decision is made from what was observable at that moment, then replayed against the market with latency, retries and costs, across chronological test periods with every trial recorded.

**What it found.** Once timing was modelled honestly, the strategy that had passed its backtest lost money in all 16 of its test windows. It was retired.

**Built with.** Python 3.11+ under strict type checking, Rust for the wire decoder and search kernels, DuckDB, Polars and PyArrow.

<br>

## The investigation

The live run and the backtest disagreed, so the run was reconciled trade by trade. The figures below come from that internal audit of the historical run. The private ledgers behind them are not part of this repository.

| Measured | Live run |
| :-- | :-- |
| Take-profit exits captured, where the backtest expected about 34% | 6 of 49 trades, about 12% |
| Positions that crossed their take-profit price | 10, of which only 6 were captured |
| Profitable round trips | 11 of 49 |
| Entry, from decision to fill | 1.9 s median, 4.9 s at the 99th percentile |
| Slowest exit, after retries | about 71 s |

Each gap became a rule in the code:

- **A decision only sees what had arrived.** Features stopped including trades from the rest of the boundary second, which a live decision near its start could not have seen.
- **The whole path to a fill is measured,** not just the moment a transaction lands: including how often positions are checked, retries and slow RPC responses.
- **A strategy includes how it exits.** Crossing a take-profit price is not the same as capturing it, so exits are replayed with repricing, failed sends and attempt limits.
- **Uncertainty is reported honestly.** A tight interval over simulated paths measures the simulation, not the future market, so the two are kept apart.

The full account is in the [case study](docs/PARITY_CASE_STUDY.md).

<br>

## How a strategy is tested

<img src="assets/method.png" width="100%" alt="Four steps beside a to-scale diagram: in each of three periods the rule learns on earlier tokens, then is tested on the next six it has never seen.">

The search is chronological. A rule is tuned on earlier tokens, frozen, and then tested on the tokens that launched next. A token never straddles the line, so a test can't learn from a token's later life. Every candidate tried is written to an append-only ledger with hashes of the rule and the data, so a good result can be weighed against how many ideas were tried to find it.

## Every trade, replayed

<img src="assets/replay.png" width="100%" alt="A candlestick chart of one simulated trade, marking the signal, the buy one second later and the stop-loss sell, with the result after fees.">

Each trade is replayed against the market as it stood at that moment: the signal, the buy a moment later at whatever price was then available, and the exit when its rule fired. Fees and failed attempts count against the result. The chart above comes from the bundled demo, which runs on generated data.

<br>

## Run it

```sh
python -m pip install -e '.[dev,data,research,archive]'
pumpquant demo        # the whole pipeline on generated data, in about a second
pumpquant preview     # the dashboard above, served at http://127.0.0.1:8765
make check            # lint, strict types, tests, Rust, and Python/Rust parity
```

The demo and the dashboard preview need none of the optional extras. They make no network calls and touch no wallet.

## Inside

```text
src/pumpquant/
  acquisition/   fetch and archive raw transactions, blocks and streams
  decoding/      Solana wire format and Pump/PumpSwap program events
  state/         market state rebuilt in order, with when it became observable
  features/      causal features at an explicit decision boundary
  pricing/       exact quotes in integer protocol units
  execution/     replay with latency, retries, fees, failures and shared capital
  research/      chronological search with a full trial ledger
  validation/    the statistical and economic gates a candidate must pass
  audits/        historical investigations, including the one that retired the strategy
  dashboard/     the read-only research dashboard and its local preview
rust/
  pump-decoder-kernel/   strict transaction decoder, checked against the Python one
  m4-growth-search/      parallel search kernels
```

- **Exact arithmetic.** Quotes use integer atomic units with the protocol's own rounding, and the two AMM venues keep their different fee and reserve rules rather than being treated as one.
- **Strict at every boundary.** Decoders fail closed on malformed input, archives are sealed with content hashes, and long jobs resume from crash-safe state.
- **Verified.** 489 tests, and strict mypy across all 115 source files. The Rust decoder matches the Python one on 220 valid, malformed and randomised transactions.

Design choices and their trade-offs are written up in [Engineering decisions](docs/DECISIONS.md). The pipeline is described in [Architecture](docs/ARCHITECTURE.md).

<br>

## What this is, and isn't

- **No strategy here is validated or live.** The one that traded was retired, and no later candidate has been validated.
- **The demo runs on generated data.** It shows how the method works, never whether a strategy makes money.
- **Private data is excluded:** market archives, ledgers, wallets, balances and operating configuration.
- **Acquisition from data providers is included but was not exercised for this release.** It needs separately configured accounts.

What was checked, and how to reproduce it, is listed in [Status](docs/STATUS.md).

<sub>No open-source licence has been granted yet. Third-party notices are kept in <a href="docs/PROVENANCE.md">Provenance</a>.</sub>
