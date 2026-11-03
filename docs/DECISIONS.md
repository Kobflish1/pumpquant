# Engineering decisions

## 1. Observation time is a first-class input

**Decision.** Preserve availability separately from chain timestamp/order, and schedule decisions and fills against what was knowable then.

**Why.** A historical inclusive integer-second checkpoint included transactions from the whole boundary second. A live decision near that second's beginning could not see them. Deterministic sorting alone did not remove that look-ahead.

**Tradeoff.** Causal reconstruction rejects convenient snapshots and sometimes loses apparent signal. Historical receive timestamps may be unavailable; a synthetic proxy must remain labeled as such. Unit tests can enforce the contract, but cannot recover missing historical observation evidence.

**Inspect.** `features/bars.py`, `execution/bar_replay.py`, `tests/execution/test_bar_replay.py`, `tests/research/test_search.py`.

## 2. Price the action at its exact budget

**Decision.** Use integer protocol quotes and explicit budget/scenario outcomes before wallet validation.

**Why.** AMM impact, fee rounding, physical inventory, and reserve caps make a large action economically different from a scaled small action. PumpSwap effective quote reserves influence price; physical reserves constrain sell solvency. A buyback split must not be charged again as an extra trader fee.

**Tradeoff.** A multi-budget outcome cube is more expensive to construct and store. Unsupported sizes fail validation instead of receiving optimistic interpolation.

**Inspect.** `pricing/bonding_curve.py`, `pricing/pumpswap.py`, `pricing/quote_surface.py`, `validation/goal.py`.

## 3. A strategy includes how it closes

**Decision.** Evaluate monitoring cadence, full trigger-to-fill latency, retries, repricing, paid failures, timeout, and migration routing together with entry.

**Why.** Theoretical TP crossings and captured TP exits differed materially during historical live use. Attempt-to-processed latency also understated the complete decision-to-fill path.

**Tradeoff.** Full replay is slower and requires richer telemetry. Empirical support can be narrow; a calibrated fee arm in one regime does not establish a universal controller. Unsupported actions must remain unsupported.

**Inspect.** `execution/empirical.py`, `execution/realism.py`, `execution/lifecycle_replay.py`, `execution/policy_selection.py`.

## 4. A trial ledger records the whole search

**Decision.** Count examined candidates, preserve failed or pruned evaluations, hash rules and data identities, and select only within chronological training boundaries.

**Why.** A selected positive result has a different meaning after a broad search. Multiple threshold representations can describe the same behavior; candidate counts must distinguish canonical rules from repeated parameter rows.

**Tradeoff.** Bookkeeping adds storage and makes impressive raw search counts less useful as headlines. That cost buys an inspectable selection procedure. A stable sign across folds is useful evidence, but does not itself correct multiplicity.

**Inspect.** `research/search.py`, `validation/splits.py`, `validation/goal.py`, `tests/research/test_search.py`.

## 5. Separate uncertainty sources

**Decision.** Distinguish sampling uncertainty, regime risk, and Monte Carlo numerical error.

**Why.** A tight interval over generated bootstrap paths only quantifies simulation precision conditional on the empirical distribution. It is not confidence that a future market will satisfy the same success probability.

**Tradeoff.** Honest uncertainty often prevents a clean deployment claim. Block resampling and family-level controls help test the observed record; they cannot create missing regimes or replace untouched confirmation.

**Inspect.** `validation/first_passage.py`, `validation/goal.py`, and the [historical parity case study](PARITY_CASE_STUDY.md).

## 6. Keep the native boundary small where possible

**Decision.** Retain Python for orchestration and a separately testable Rust wire decoder, alongside specialized parallel search kernels.

**Why.** Strict parsing has a compact contract suited to differential tests. Native search has a different purpose: aggregating large, already-labeled research surfaces.

**Tradeoff.** Two implementations create parity work. The decoder's bounded output intentionally omits lookup resolution and semantic event decoding. No throughput number is asserted without its original benchmark inputs and environment.
