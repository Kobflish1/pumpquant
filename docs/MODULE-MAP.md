# Module map

In this release the flat `pumpquant` package was grouped into subpackages and some modules were renamed to read naturally inside them. The code inside each module is unchanged except for imports, typing and the cleanups listed in [Provenance](PROVENANCE.md). Persisted artifact identifiers such as `"pumpquant.signature_inventory"` keep their original spelling, so manifests written before the move still validate.

| Original module | Now |
| :-- | :-- |
| `bigquery_approval` | `acquisition.bigquery.approval` |
| `bigquery_archive` | `acquisition.bigquery.archive` |
| `google_bigquery` | `acquisition.bigquery.client` |
| `bigquery_coordinator` | `acquisition.bigquery.coordinator` |
| `bigquery_parallel` | `acquisition.bigquery.parallel` |
| `bigquery_source` | `acquisition.bigquery.source` |
| `bigquery_storage_archive` | `acquisition.bigquery.storage_archive` |
| `block_archive` | `acquisition.block_archive` |
| `block_inventory_audit` | `acquisition.block_inventory_audit` |
| `pumpapi_replay` | `acquisition.pumpapi_replay` |
| `signature_inventory` | `acquisition.signature_inventory` |
| `raw_stream_archive` | `acquisition.stream_archive` |
| `raw_stream_audit` | `acquisition.stream_audit` |
| `transaction_hydrator` | `acquisition.transaction_hydrator` |
| `m4_live` | `audits.live_rule.engine` |
| `m4_post_stair_audit` | `audits.live_rule.execution_audit` |
| `executable_m4_replay` | `audits.live_rule.replay` |
| `m4_execution_revalidation` | `audits.live_rule.revalidation` |
| `durable_state` | `core.durable_state` |
| `io` | `core.io` |
| `schema` | `core.schema` |
| `synthetic` | `core.synthetic` |
| `token_evidence` | `dashboard.evidence` |
| `live_dashboard` | `dashboard.server`, split into `config`, `manifests`, `atlas`, `market` and `snapshot` |
| `instruction_accounts` | `decoding.instruction_accounts` |
| `instruction_intent` | `decoding.instruction_intent` |
| `program_events` | `decoding.program_events` |
| `raw_instructions` | `decoding.raw_instructions` |
| `solana_wire` | `decoding.solana_wire` |
| `execution` | `execution.bar_replay` |
| `empirical_execution` | `execution.empirical` |
| `full_lifecycle_replay` | `execution.lifecycle_replay` |
| `joint_profit_optimizer` | `execution.policy_selection` |
| `priority_fee_optimizer` | `execution.priority_fees` |
| `executable_pump_replay` | `execution.pump_replay` |
| `execution_realism` | `execution.realism` |
| `shared_wallet_replay` | `execution.shared_wallet` |
| `execution_telemetry` | `execution.telemetry` |
| `velocity_risk` | `execution.velocity_risk` |
| `alpha_features` | `features.alpha` |
| `features` | `features.bars` |
| `event_features` | `features.events` |
| `lifecycle_features` | `features.lifecycle` |
| `pumpapi_features` | `features.checkpoint_features` |
| `pumpapi_causal_linear` | `features.checkpoint_states` |
| `pumpapi_feature_selection` | `features.feature_selection` |
| `chunk_bars` | `features.streaming_bars` |
| `protocol_math` | `pricing.bonding_curve` |
| `microstructure` | `pricing.depth_ladder` |
| `pumpswap_math` | `pricing.pumpswap` |
| `pumpswap_quote_surface` | `pricing.quote_surface` |
| `alpha_strategy` | `research.alpha_strategy` |
| `alpha_tournament` | `research.alpha_tournament` |
| `distributed_reducer` | `research.distributed_reducer` |
| `distributed_search` | `research.distributed_search` |
| `event_interactions` | `research.event_interactions` |
| `event_research` | `research.event_research` |
| `event_screen` | `research.event_screen` |
| `amm_impact_ofi` | `research.families.amm_impact_ofi` |
| `persistent_pressure` | `research.families.persistent_pressure` |
| `progress_speed` | `research.families.progress_speed` |
| `shock_recovery` | `research.families.shock_recovery` |
| `shock_recovery_grid` | `research.families.shock_recovery_grid` |
| `fresh_event_research` | `research.fresh_event_research` |
| `search` | `research.search` |
| `tail_exit_research` | `research.tail_exit` |
| `tail_exit_runtime` | `research.tail_exit_runtime` |
| `launch_times` | `state.launch_times` |
| `lifecycle_catalog` | `state.lifecycle_catalog` |
| `lifecycle_launch_times` | `state.launch_times` (merged with `launch_times`) |
| `amm_state_materializer` | `state.materializer` |
| `amm_state_reconstruction` | `state.reconstruction` |
| `capacity_exact_bridge` | `validation.capacity_bridge` |
| `capacity_gate` | `validation.capacity_gate` |
| `first_passage` | `validation.first_passage` |
| `goal_validation` | `validation.goal` |
| `metrics` | `validation.metrics` |
| `validation` | `validation.splits` |
| `strategy_validation` | `validation.strategy_diagnostics` |
| `exact_survivor_gate` | `validation.survivor_gate` |
| `cli` | `cli` (split into `cli/research.py` and `cli/bigquery.py`) |
| | `demo` (new: the demo pipeline shared by the CLI and the preview) |
| | `core.hashing` (new: one home for the file, byte and canonical-JSON hashes) |
| | `acquisition.bigquery.daily` (new: the guarded daily and batch archive runs, moved out of the CLI) |
| | `dashboard.preview` (new: replaces `examples/dashboard_preview.py`) |

## Removed in the November 2026 cleanup

Code that nothing called and no test covered was deleted rather than kept:

| Module | Why it went |
| :-- | :-- |
| `audits.correction_v2` | About 9,900 lines that re-ran the corrected research grids by importing private runner scripts that are not in this snapshot, so none of it could run here. |
| `state.config_schedule` | A fee-schedule builder with no caller and no tests. |
| `research.direct_goal_outcomes` | Contract types with no caller. |
| `features.pumpapi_state_action`, `features.pumpapi_payoffs` | Superseded by `features.checkpoint_states`; no caller. |

The Rust crate `m4-growth-search` is now `growth-search`, and its binaries are `interaction_search`, `lifecycle_triples` and `crossday_triples`. Persisted `kind` strings and default artifact paths keep their original spelling.
