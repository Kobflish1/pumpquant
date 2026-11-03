# When historical and live mechanics disagreed

The original project traded live. One early stage completed profitably; a later low-liquidity strategy failed. The failure was investigated and invalidated the later strategy's authorization. A successful earlier run does not establish that its signal remains usable under another latency or capital regime.

This case study is a sanitized account of the historical internal audit. Private ledgers, transaction signatures, balances, wallet addresses, and market archives are excluded. The numerical findings below are historical audit reports, not results independently reproduced from the public package.

## The discrepancy

The later historical model reported a captured take-profit rate of about 34.4%. Finalized reconciliation of the later live run found 49 economic round trips: 11 positive, 38 negative. Ten positions crossed their TP threshold, but only six TP exits were captured.

Under that historical TP-rate model, six or fewer captured TPs in 49 trades had reported binomial probability about 0.0412%. That calculation depends on the model's rate and binomial assumptions. It was a discrepancy diagnostic, not a probability of future profit or proof of a single cause.

## What the investigation found

| Mismatch | Why it mattered | Engineering consequence |
| --- | --- | --- |
| Inclusive boundary-second features | Historical decisions saw transactions unavailable to the live decision | Separate observation availability from chain ordering; use a strict causal boundary |
| Partial latency measurement | Attempt-to-processed was treated as full decision-to-fill | Measure the complete entry and exit path, including retries and RPC tails |
| Different retry/TP behavior | A quoted crossing did not guarantee a captured exit | Replay repricing, trigger revalidation, failed sends, and actual attempt limits |
| Continuous replay versus periodic observation | Historical transitions received unrealistically immediate reactions | Include the live monitoring clock |
| Fee-gated and structural exclusions | Live eligible decisions differed from the modeled population | Model the same population and skipped decisions |
| Bootstrap interval interpretation | Generated paths were treated like independent market evidence | Report numerical simulation error separately from market uncertainty |

The audit reported entry latency around 1.9 seconds at the median and 4.9 seconds at P99. Exit latency had a severe retry/RPC tail reaching roughly 71 seconds. These describe the historical run; they are not current service measurements or performance promises.

After correcting the causal clock and execution mechanics, the frozen low-liquidity rule had negative mean return across all four inspected development/consumed periods, with all 16 chronological folds negative. It was rejected. Subsequent research found real conditional probability structure and some positive development candidates; no candidate in the retained status record became a currently validated strategy.

## What this package can demonstrate

The code includes tests that reject past-state fills after a delayed observation, exclude features unavailable at a training cutoff, account for failed entry attempts, distinguish AMM pricing conventions, and require explicitly replayed budgets for wallet validation. The synthetic demo executes those introductory data/search interfaces locally.

These checks establish implementation behavior on controlled fixtures. They do not reconstruct the private live audit, prove a market edge, or validate every historical research claim. The lasting contribution is the testable execution contract and the preserved explanation of why the earlier model failed.
