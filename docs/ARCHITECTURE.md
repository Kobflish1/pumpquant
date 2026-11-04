# Architecture

PumpQuant follows the path a trade idea takes, from raw chain data to a verdict. Each stage is a package under `src/pumpquant/`, and each hands the next a stricter guarantee.

```text
acquisition   archived transactions, blocks and live stream frames
    → decoding     strict wire, instruction and program-event decoding
    → state        ordered market state, with chain time and observed availability
    → features     causal features at an explicit decision boundary
    → pricing      exact, budget-specific AMM entry and routed exit
    → execution    latency, monitoring, retries, fees, failures and shared capital
    → research     chronological search with a full trial ledger
    → validation   statistical and economic gates
```

`audits/` holds the historical investigations that shaped these rules. `dashboard/` is the read-only window onto a run, and `cli/` exposes it all as the `pumpquant` command.

## Two clocks

`core/schema.py` defines event and bar records. `features/bars.py` builds bars and keeps the time each one became observable. `execution/bar_replay.py` uses that availability when it prepares features and schedules fills. An event's place in chain order and the moment a decision maker could have seen it answer different questions, and a fill can never use reserves from before a delayed observation.

`tests/execution/test_bar_replay.py` covers signal-to-fill ordering and a delayed snapshot that must not fill against past chain state. `tests/research/test_search.py` checks that a training fold excludes bars unavailable at its cutoff, and that the selected and test populations never overlap.

## Decoding and provenance

`decoding/` moves from bytes to meaning in steps: `solana_wire.py`, `raw_instructions.py`, `instruction_intent.py`, `instruction_accounts.py` and `program_events.py`. The schema lock in `schemas/` records the upstream program definitions and their hashes. It contains public protocol identifiers only.

`rust/pump-decoder-kernel` implements a deliberately smaller contract: canonical transaction base64, legacy and v0 message structure, the first signature, and the ordered static account keys. It does not resolve address lookup tables or claim full Solana runtime validation. Keeping that seam narrow is what makes the Python/Rust comparison meaningful.

`acquisition/` supports provider-backed downloads, manifests, inventories and audits. These paths are separate from the demo. They can make remote or billable calls when explicitly configured, and this package ships no account configuration.

## Pricing and execution

`pricing/bonding_curve.py` quotes Pump trades in integer atomic units. `pricing/pumpswap.py` separates fee handling, the effective reserves that set price impact, and the physical inventory that limits a sell. It keeps the SDK-style and Rust-client-style buy conventions as two named functions instead of declaring them equivalent.

`execution/bar_replay.py` is the introductory replay. `execution/pump_replay.py` and `execution/lifecycle_replay.py` cover richer contracts. `execution/empirical.py`, `execution/realism.py` and `execution/priority_fees.py` model measured execution and its costs, and `execution/policy_selection.py` compares supported action bundles against simply skipping. These modules have different input contracts on purpose; they are not one universal simulator.

A lifecycle trade must include entry, observation cadence, trigger time, exit landing, retries, paid failures, timeout and the venue route after migration. The historical audit showed why a first take-profit crossing alone is not a usable label.

## Search and validation

`research/search.py` runs chronological, token-level walk-forward selection with an embargo, explicit candidates, deterministic hashes and an append-only experiment ledger. Its return statistics describe fixed-notional outcomes and do not by themselves define a funded equity curve.

`execution/shared_wallet.py` handles cash and overlapping capital. `validation/goal.py` requires explicit outcomes at exact budgets and scenarios, real consecutive validation windows, capital release times and purged chronological folds. Its family-level SPA test needs NumPy and `arch`. Interpolating an unpriced order size, or repeating one day to stand in for a multi-day window, is outside that contract.

`rust/growth-search` holds the Rayon search kernels used to scan large, already-labelled research surfaces. No throughput figure is claimed for them.

## The demo and the dashboard

`demo.py` generates 36 tokens, searches 16 momentum variants and tests each period's winner on later tokens. `pumpquant demo` prints the result, and `pumpquant preview` serves the dashboard on it through the real read-only backend in `dashboard/server.py`. The preview swaps only the data sources: no artifact paths, services, provider calls or wallets are touched, currency conversion is disabled, and the dashboard labels the data as simulated.

## Included and omitted

Included: the Python package, its tests, the protocol schema lock, two Rust crates, the decoder parity script and the dashboard. Omitted: historical market data and private results, account and transaction ledgers, operating configs, wallet material, provisioning scripts, live runner wrappers and deployment recipes. The correction-v2 audit refers to private runner scripts that are not part of this snapshot.
