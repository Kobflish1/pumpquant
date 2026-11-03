use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const FOLDS: usize = 4;
#[cfg(test)]
const PROFILES: usize = 5;

#[derive(Debug, Deserialize)]
struct PeriodShape {
    label: String,
    rows: usize,
}

#[derive(Debug, Deserialize)]
struct CheckpointShape {
    checkpoint_ms: i64,
    periods: Vec<PeriodShape>,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    status: String,
    holdout_read: bool,
    wallet_lamports: i64,
    fold_count: usize,
    profile_count: usize,
    scenario_ids: Vec<String>,
    feature_names: Vec<String>,
    #[serde(default)]
    strategy_feature_names: Option<Vec<String>>,
    quantiles: Vec<f64>,
    #[serde(default = "default_anchor_directions")]
    anchor_directions: Vec<String>,
    minimum_period_signals: usize,
    minimum_fold_signals: usize,
    #[serde(default = "default_true")]
    require_zero_catastrophic_exit: bool,
    declared_two_condition_rules: u64,
    declared_exact_rule_exit_trials: u64,
    checkpoints: Vec<CheckpointShape>,
}

fn default_anchor_directions() -> Vec<String> {
    vec!["ge".to_string()]
}

fn default_true() -> bool {
    true
}

fn unordered_two_condition_rule_count(
    feature_count: usize,
    threshold_count: usize,
    direction_count: usize,
) -> u64 {
    let feature_pairs = feature_count.saturating_mul(feature_count.saturating_sub(1)) / 2;
    let variants_per_feature = threshold_count.saturating_mul(direction_count);
    feature_pairs.saturating_mul(variants_per_feature.saturating_mul(variants_per_feature)) as u64
}

#[derive(Debug, Deserialize)]
struct CheckpointManifest {
    checkpoint_ms: i64,
    periods: Vec<PeriodShape>,
    secondary_thresholds: HashMap<String, Vec<Option<f64>>>,
    #[serde(default)]
    strategy_feature_names: Option<Vec<String>>,
}

struct PeriodData {
    label: String,
    rows: usize,
    features: Vec<f64>,
    folds: Vec<u8>,
    pnl: Vec<Vec<f64>>,
    catastrophic: Vec<Vec<u8>>,
}

#[derive(Clone)]
struct Anchor {
    feature_index: usize,
    feature_name: String,
    threshold_index: usize,
    threshold: f64,
    direction_ge: bool,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct RuleKey {
    anchor_feature: usize,
    anchor_threshold: usize,
    anchor_direction_ge: bool,
    secondary_feature: usize,
    secondary_quantile: usize,
    direction_ge: bool,
    profile: usize,
}

#[derive(Clone)]
struct StageOneCandidate {
    key: RuleKey,
}

#[derive(Debug, Clone, Serialize)]
struct ScenarioMetrics {
    n: usize,
    mean_pnl_lamports: f64,
    mean_wallet_log_growth: f64,
    largest_winner_removed_mean_pnl: f64,
    fold_n: [usize; FOLDS],
    fold_mean_pnl_lamports: [f64; FOLDS],
    catastrophic_exit_failures: usize,
}

#[derive(Debug, Clone, Serialize)]
struct PointSurvivor {
    checkpoint_ms: i64,
    anchor_feature: String,
    anchor_threshold: f64,
    anchor_threshold_index: usize,
    anchor_direction: &'static str,
    secondary_feature: String,
    secondary_direction: &'static str,
    secondary_quantile: f64,
    secondary_threshold: f64,
    secondary_quantile_index: usize,
    profile_id: usize,
    periods: HashMap<String, Vec<ScenarioMetrics>>,
    passes_adjacent_plateau: bool,
    worst_mean_wallet_log_growth: f64,
}

struct EvaluatedCandidate {
    result: PointSurvivor,
    support_pass: bool,
    mean_pass: bool,
    log_pass: bool,
    largest_winner_pass: bool,
    folds_pass: bool,
    catastrophic_pass: bool,
}

#[derive(Debug, Serialize)]
struct RejectionCounts {
    support: usize,
    mean_pnl: usize,
    wallet_log_growth: usize,
    largest_winner_removed: usize,
    chronological_folds: usize,
    catastrophic_exit: usize,
}

#[derive(Debug, Serialize)]
struct CheckpointResult {
    status: &'static str,
    checkpoint_ms: i64,
    declared_rules: u64,
    declared_rule_exit_trials: u64,
    stage_one_positive_baseline_candidate_count: usize,
    economic_gate_near_miss_count: usize,
    point_gate_survivor_count: usize,
    plateau_survivor_count: usize,
    rejection_counts_among_stage_one_candidates: RejectionCounts,
    economic_gate_near_misses: Vec<PointSurvivor>,
    top_near_misses: Vec<PointSurvivor>,
    elapsed_seconds: f64,
    survivors: Vec<PointSurvivor>,
}

#[derive(Debug, Serialize)]
struct FinalResult {
    kind: &'static str,
    version: u32,
    status: &'static str,
    development_only: bool,
    holdout_read: bool,
    architecture: &'static str,
    logical_cpus: usize,
    declared_two_condition_rules: u64,
    declared_exact_rule_exit_trials: u64,
    point_gate_survivor_count: usize,
    plateau_survivor_count: usize,
    elapsed_seconds: f64,
    checkpoints: Vec<CheckpointResult>,
}

fn read_f64(path: &Path) -> io::Result<Vec<f64>> {
    let bytes = fs::read(path)?;
    if bytes.len() % 8 != 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad f64 file"));
    }
    Ok(bytes
        .chunks_exact(8)
        .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
        .collect())
}

fn lower_bound(values: &[f64], target: f64) -> usize {
    values.partition_point(|value| *value < target)
}

fn upper_bound(values: &[f64], target: f64) -> usize {
    values.partition_point(|value| *value <= target)
}

fn outcome_offset(profile: usize, row: usize, rows: usize) -> usize {
    profile * rows + row
}

fn load_period(
    directory: &Path,
    period_index: usize,
    shape: &PeriodShape,
    features: usize,
    profiles: usize,
    scenarios: usize,
) -> Result<PeriodData, Box<dyn std::error::Error>> {
    let feature_values =
        read_f64(&directory.join(format!("period-{period_index}-features.f64le")))?;
    let folds = fs::read(directory.join(format!("period-{period_index}-folds.u8")))?;
    if feature_values.len() != shape.rows * features || folds.len() != shape.rows {
        return Err("period input shape mismatch".into());
    }
    let mut pnl = Vec::new();
    let mut catastrophic = Vec::new();
    for scenario in 0..scenarios {
        let scenario_pnl = read_f64(&directory.join(format!(
            "period-{period_index}-scenario-{scenario}-pnl.f64le"
        )))?;
        let scenario_catastrophic = fs::read(directory.join(format!(
            "period-{period_index}-scenario-{scenario}-catastrophic.u8"
        )))?;
        if scenario_pnl.len() != profiles * shape.rows
            || scenario_catastrophic.len() != profiles * shape.rows
        {
            return Err("outcome input shape mismatch".into());
        }
        pnl.push(scenario_pnl);
        catastrophic.push(scenario_catastrophic);
    }
    Ok(PeriodData {
        label: shape.label.clone(),
        rows: shape.rows,
        features: feature_values,
        folds,
        pnl,
        catastrophic,
    })
}

fn stage_one_for_pair(
    anchor: &Anchor,
    secondary_feature: usize,
    thresholds: &[f64],
    periods: &[PeriodData],
    feature_count: usize,
    minimum_period_signals: usize,
    minimum_fold_signals: usize,
    profile_count: usize,
) -> Vec<StageOneCandidate> {
    let qn = thresholds.len();
    let mut period_counts = Vec::new();
    let mut period_folds = Vec::new();
    let mut period_sums = Vec::new();
    for period in periods {
        let mut le_count_delta = vec![0i64; qn + 1];
        let mut ge_count_delta = vec![0i64; qn + 1];
        let mut le_fold_delta = vec![[0i64; FOLDS]; qn + 1];
        let mut ge_fold_delta = vec![[0i64; FOLDS]; qn + 1];
        let mut le_sum_delta = vec![vec![0.0f64; profile_count]; qn + 1];
        let mut ge_sum_delta = vec![vec![0.0f64; profile_count]; qn + 1];
        for row in 0..period.rows {
            let anchor_value = period.features[row * feature_count + anchor.feature_index];
            let value = period.features[row * feature_count + secondary_feature];
            if !anchor_value.is_finite()
                || if anchor.direction_ge {
                    anchor_value < anchor.threshold
                } else {
                    anchor_value > anchor.threshold
                }
                || !value.is_finite()
            {
                continue;
            }
            let fold = period.folds[row] as usize;
            if fold >= FOLDS {
                continue;
            }
            let le_start = lower_bound(thresholds, value);
            if le_start < qn {
                le_count_delta[le_start] += 1;
                le_fold_delta[le_start][fold] += 1;
                for profile in 0..profile_count {
                    le_sum_delta[le_start][profile] +=
                        period.pnl[0][outcome_offset(profile, row, period.rows)];
                }
            }
            let ge_end = upper_bound(thresholds, value);
            if ge_end > 0 {
                ge_count_delta[0] += 1;
                ge_count_delta[ge_end] -= 1;
                ge_fold_delta[0][fold] += 1;
                ge_fold_delta[ge_end][fold] -= 1;
                for profile in 0..profile_count {
                    let value = period.pnl[0][outcome_offset(profile, row, period.rows)];
                    ge_sum_delta[0][profile] += value;
                    ge_sum_delta[ge_end][profile] -= value;
                }
            }
        }
        for q in 1..qn {
            le_count_delta[q] += le_count_delta[q - 1];
            ge_count_delta[q] += ge_count_delta[q - 1];
            for fold in 0..FOLDS {
                le_fold_delta[q][fold] += le_fold_delta[q - 1][fold];
                ge_fold_delta[q][fold] += ge_fold_delta[q - 1][fold];
            }
            for profile in 0..profile_count {
                le_sum_delta[q][profile] += le_sum_delta[q - 1][profile];
                ge_sum_delta[q][profile] += ge_sum_delta[q - 1][profile];
            }
        }
        period_counts.push((le_count_delta, ge_count_delta));
        period_folds.push((le_fold_delta, ge_fold_delta));
        period_sums.push((le_sum_delta, ge_sum_delta));
    }

    let mut candidates = Vec::new();
    for direction_ge in [false, true] {
        for q in 0..qn {
            for profile in 0..profile_count {
                let passes = (0..periods.len()).all(|period| {
                    let counts = if direction_ge {
                        &period_counts[period].1
                    } else {
                        &period_counts[period].0
                    };
                    let folds = if direction_ge {
                        &period_folds[period].1
                    } else {
                        &period_folds[period].0
                    };
                    let sums = if direction_ge {
                        &period_sums[period].1
                    } else {
                        &period_sums[period].0
                    };
                    counts[q] >= minimum_period_signals as i64
                        && folds[q]
                            .iter()
                            .all(|count| *count >= minimum_fold_signals as i64)
                        && sums[q][profile] > 0.0
                });
                if passes {
                    candidates.push(StageOneCandidate {
                        key: RuleKey {
                            anchor_feature: anchor.feature_index,
                            anchor_threshold: anchor.threshold_index,
                            anchor_direction_ge: anchor.direction_ge,
                            secondary_feature,
                            secondary_quantile: q,
                            direction_ge,
                            profile,
                        },
                    });
                }
            }
        }
    }
    candidates
}

fn exact_metrics(
    period: &PeriodData,
    anchor: &Anchor,
    secondary_feature: usize,
    secondary_threshold: f64,
    direction_ge: bool,
    profile: usize,
    scenario: usize,
    feature_count: usize,
    wallet_lamports: f64,
) -> Option<ScenarioMetrics> {
    let mut n = 0usize;
    let mut sum = 0.0;
    let mut log_sum = 0.0;
    let mut maximum = f64::NEG_INFINITY;
    let mut fold_n = [0usize; FOLDS];
    let mut fold_sum = [0.0f64; FOLDS];
    let mut catastrophic = 0usize;
    for row in 0..period.rows {
        let anchor_value = period.features[row * feature_count + anchor.feature_index];
        let value = period.features[row * feature_count + secondary_feature];
        let selected = anchor_value.is_finite()
            && if anchor.direction_ge {
                anchor_value >= anchor.threshold
            } else {
                anchor_value <= anchor.threshold
            }
            && value.is_finite()
            && if direction_ge {
                value >= secondary_threshold
            } else {
                value <= secondary_threshold
            };
        if !selected {
            continue;
        }
        let fold = period.folds[row] as usize;
        if fold >= FOLDS {
            continue;
        }
        let offset = outcome_offset(profile, row, period.rows);
        let pnl = period.pnl[scenario][offset];
        if !pnl.is_finite() || pnl <= -wallet_lamports {
            return None;
        }
        n += 1;
        sum += pnl;
        log_sum += (pnl / wallet_lamports).ln_1p();
        maximum = maximum.max(pnl);
        fold_n[fold] += 1;
        fold_sum[fold] += pnl;
        catastrophic += period.catastrophic[scenario][offset] as usize;
    }
    if n < 2 {
        return None;
    }
    Some(ScenarioMetrics {
        n,
        mean_pnl_lamports: sum / n as f64,
        mean_wallet_log_growth: log_sum / n as f64,
        largest_winner_removed_mean_pnl: (sum - maximum) / (n - 1) as f64,
        fold_n,
        fold_mean_pnl_lamports: std::array::from_fn(|fold| {
            if fold_n[fold] == 0 {
                f64::NAN
            } else {
                fold_sum[fold] / fold_n[fold] as f64
            }
        }),
        catastrophic_exit_failures: catastrophic,
    })
}

fn atomic_json(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let temporary = path.with_extension("json.partial");
    let mut body = serde_json::to_vec_pretty(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    body.push(b'\n');
    fs::write(&temporary, body)?;
    fs::rename(temporary, path)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or("usage: pumpapi_interactions INPUT OUTPUT")?,
    );
    let output = PathBuf::from(
        std::env::args()
            .nth(2)
            .ok_or("usage: pumpapi_interactions INPUT OUTPUT")?,
    );
    fs::create_dir_all(&output)?;
    let manifest: Manifest = serde_json::from_slice(&fs::read(input.join("manifest.json"))?)?;
    if manifest.status != "complete" || manifest.holdout_read {
        return Err("input is not complete development-only data".into());
    }
    if manifest.fold_count != FOLDS
        || manifest.profile_count == 0
        || manifest.scenario_ids.is_empty()
    {
        return Err("manifest execution shape mismatch".into());
    }
    let logical_cpus = std::thread::available_parallelism()?.get();
    let feature_count = manifest.feature_names.len();
    let feature_index: HashMap<&str, usize> = manifest
        .feature_names
        .iter()
        .enumerate()
        .map(|(index, name)| (name.as_str(), index))
        .collect();
    let run_started = Instant::now();
    let global_completed = AtomicU64::new(0);
    let mut checkpoint_results = Vec::new();
    for checkpoint_shape in &manifest.checkpoints {
        let checkpoint_started = Instant::now();
        let directory = input.join(format!("checkpoint-{}", checkpoint_shape.checkpoint_ms));
        let checkpoint_manifest: CheckpointManifest =
            serde_json::from_slice(&fs::read(directory.join("manifest.json"))?)?;
        if checkpoint_manifest.checkpoint_ms != checkpoint_shape.checkpoint_ms
            || checkpoint_manifest.periods.len() != checkpoint_shape.periods.len()
        {
            return Err("checkpoint manifest mismatch".into());
        }
        let periods: Vec<PeriodData> = checkpoint_shape
            .periods
            .iter()
            .enumerate()
            .map(|(index, shape)| {
                load_period(
                    &directory,
                    index,
                    shape,
                    feature_count,
                    manifest.profile_count,
                    manifest.scenario_ids.len(),
                )
            })
            .collect::<Result<_, _>>()?;
        let threshold_table: Vec<Vec<f64>> = manifest
            .feature_names
            .iter()
            .map(|feature| {
                checkpoint_manifest
                    .secondary_thresholds
                    .get(feature)
                    .ok_or("secondary threshold feature missing")?
                    .iter()
                    .copied()
                    .collect::<Option<Vec<f64>>>()
                    .ok_or("all-null secondary threshold family".into())
            })
            .collect::<Result<_, Box<dyn std::error::Error>>>()?;
        let checkpoint_feature_names = checkpoint_manifest
            .strategy_feature_names
            .as_ref()
            .or(manifest.strategy_feature_names.as_ref());
        let strategy_feature_indices: Vec<usize> = checkpoint_feature_names
            .map(|names| {
                names
                    .iter()
                    .map(|name| {
                        feature_index
                            .get(name.as_str())
                            .copied()
                            .ok_or("strategy feature missing from feature matrix")
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_else(|| (0..feature_count).collect());
        let mut anchors = Vec::new();
        for &index in &strategy_feature_indices {
            for direction in &manifest.anchor_directions {
                let direction_ge = match direction.as_str() {
                    "ge" => true,
                    "le" => false,
                    _ => return Err("anchor direction must be ge or le".into()),
                };
                for (threshold_index, &threshold) in threshold_table[index].iter().enumerate() {
                    anchors.push(Anchor {
                        feature_index: index,
                        feature_name: manifest.feature_names[index].clone(),
                        threshold_index,
                        threshold,
                        direction_ge,
                    });
                }
            }
        }
        anchors.sort_by(|a, b| {
            a.feature_index
                .cmp(&b.feature_index)
                .then(a.direction_ge.cmp(&b.direction_ge))
                .then(a.threshold_index.cmp(&b.threshold_index))
        });

        let stage_one: Vec<StageOneCandidate> = anchors
            .par_iter()
            .flat_map_iter(|anchor| {
                strategy_feature_indices
                    .iter()
                    .copied()
                    // A AND B is identical to B AND A. Retain exactly one
                    // canonical ordering while preserving every threshold and
                    // direction combination for both features.
                    .filter(|&secondary| secondary > anchor.feature_index)
                    .flat_map(|secondary| {
                        let result = stage_one_for_pair(
                            anchor,
                            secondary,
                            &threshold_table[secondary],
                            &periods,
                            feature_count,
                            manifest.minimum_period_signals,
                            manifest.minimum_fold_signals,
                            manifest.profile_count,
                        );
                        global_completed
                            .fetch_add((manifest.quantiles.len() * 2) as u64, Ordering::Relaxed);
                        result
                    })
            })
            .collect();

        let evaluated: Vec<EvaluatedCandidate> = stage_one
            .par_iter()
            .filter_map(|candidate| {
                let anchor = anchors.iter().find(|value| {
                    value.feature_index == candidate.key.anchor_feature
                        && value.threshold_index == candidate.key.anchor_threshold
                        && value.direction_ge == candidate.key.anchor_direction_ge
                })?;
                let threshold = threshold_table[candidate.key.secondary_feature]
                    [candidate.key.secondary_quantile];
                let mut period_results = HashMap::new();
                let mut worst_log = f64::INFINITY;
                let mut support_pass = true;
                let mut mean_pass = true;
                let mut log_pass = true;
                let mut largest_winner_pass = true;
                let mut folds_pass = true;
                let mut catastrophic_pass = true;
                for period in &periods {
                    let mut scenarios = Vec::new();
                    for scenario in 0..manifest.scenario_ids.len() {
                        let metrics = exact_metrics(
                            period,
                            anchor,
                            candidate.key.secondary_feature,
                            threshold,
                            candidate.key.direction_ge,
                            candidate.key.profile,
                            scenario,
                            feature_count,
                            manifest.wallet_lamports as f64,
                        )?;
                        support_pass &= metrics.n >= manifest.minimum_period_signals
                            && metrics
                                .fold_n
                                .iter()
                                .all(|count| *count >= manifest.minimum_fold_signals);
                        mean_pass &= metrics.mean_pnl_lamports > 0.0;
                        log_pass &= metrics.mean_wallet_log_growth > 0.0;
                        largest_winner_pass &= metrics.largest_winner_removed_mean_pnl > 0.0;
                        folds_pass &= metrics
                            .fold_mean_pnl_lamports
                            .iter()
                            .all(|mean| mean.is_finite() && *mean > 0.0);
                        catastrophic_pass &= metrics.catastrophic_exit_failures == 0;
                        worst_log = worst_log.min(metrics.mean_wallet_log_growth);
                        scenarios.push(metrics);
                    }
                    period_results.insert(period.label.clone(), scenarios);
                }
                Some(EvaluatedCandidate {
                    result: PointSurvivor {
                        checkpoint_ms: checkpoint_shape.checkpoint_ms,
                        anchor_feature: anchor.feature_name.clone(),
                        anchor_threshold: anchor.threshold,
                        anchor_threshold_index: anchor.threshold_index,
                        anchor_direction: if anchor.direction_ge { "ge" } else { "le" },
                        secondary_feature: manifest.feature_names[candidate.key.secondary_feature]
                            .clone(),
                        secondary_direction: if candidate.key.direction_ge {
                            "ge"
                        } else {
                            "le"
                        },
                        secondary_quantile: manifest.quantiles[candidate.key.secondary_quantile],
                        secondary_threshold: threshold,
                        secondary_quantile_index: candidate.key.secondary_quantile,
                        profile_id: candidate.key.profile + 1,
                        periods: period_results,
                        passes_adjacent_plateau: false,
                        worst_mean_wallet_log_growth: worst_log,
                    },
                    support_pass,
                    mean_pass,
                    log_pass,
                    largest_winner_pass,
                    folds_pass,
                    catastrophic_pass,
                })
            })
            .collect();
        let rejection_counts = RejectionCounts {
            support: evaluated.iter().filter(|value| !value.support_pass).count(),
            mean_pnl: evaluated.iter().filter(|value| !value.mean_pass).count(),
            wallet_log_growth: evaluated.iter().filter(|value| !value.log_pass).count(),
            largest_winner_removed: evaluated
                .iter()
                .filter(|value| !value.largest_winner_pass)
                .count(),
            chronological_folds: evaluated.iter().filter(|value| !value.folds_pass).count(),
            catastrophic_exit: evaluated
                .iter()
                .filter(|value| !value.catastrophic_pass)
                .count(),
        };
        let economic_gate_near_miss_count = evaluated
            .iter()
            .filter(|value| {
                value.support_pass && value.mean_pass && value.log_pass && value.largest_winner_pass
            })
            .count();
        let economic_gate_near_misses: Vec<PointSurvivor> = evaluated
            .iter()
            .filter(|value| {
                value.support_pass && value.mean_pass && value.log_pass && value.largest_winner_pass
            })
            .map(|value| value.result.clone())
            .collect();
        let mut near_miss_indices: Vec<usize> = (0..evaluated.len()).collect();
        near_miss_indices.sort_by(|&left, &right| {
            let score = |value: &EvaluatedCandidate| {
                [
                    value.support_pass,
                    value.mean_pass,
                    value.log_pass,
                    value.largest_winner_pass,
                    value.folds_pass,
                    value.catastrophic_pass,
                ]
                .into_iter()
                .filter(|gate| *gate)
                .count()
            };
            score(&evaluated[right])
                .cmp(&score(&evaluated[left]))
                .then_with(|| {
                    evaluated[right]
                        .result
                        .worst_mean_wallet_log_growth
                        .total_cmp(&evaluated[left].result.worst_mean_wallet_log_growth)
                })
        });
        let top_near_misses: Vec<PointSurvivor> = near_miss_indices
            .into_iter()
            .take(20)
            .map(|index| evaluated[index].result.clone())
            .collect();
        let point_survivors: Vec<PointSurvivor> = evaluated
            .into_iter()
            .filter(|value| {
                value.support_pass
                    && value.mean_pass
                    && value.log_pass
                    && value.largest_winner_pass
                    && value.folds_pass
                    && (!manifest.require_zero_catastrophic_exit || value.catastrophic_pass)
            })
            .map(|value| value.result)
            .collect();

        let survivor_keys: HashSet<RuleKey> = point_survivors
            .iter()
            .map(|survivor| RuleKey {
                anchor_feature: *feature_index.get(survivor.anchor_feature.as_str()).unwrap(),
                anchor_threshold: survivor.anchor_threshold_index,
                anchor_direction_ge: survivor.anchor_direction == "ge",
                secondary_feature: *feature_index
                    .get(survivor.secondary_feature.as_str())
                    .unwrap(),
                secondary_quantile: survivor.secondary_quantile_index,
                direction_ge: survivor.secondary_direction == "ge",
                profile: survivor.profile_id - 1,
            })
            .collect();
        let mut survivors = point_survivors;
        for survivor in &mut survivors {
            let key = RuleKey {
                anchor_feature: *feature_index.get(survivor.anchor_feature.as_str()).unwrap(),
                anchor_threshold: survivor.anchor_threshold_index,
                anchor_direction_ge: survivor.anchor_direction == "ge",
                secondary_feature: *feature_index
                    .get(survivor.secondary_feature.as_str())
                    .unwrap(),
                secondary_quantile: survivor.secondary_quantile_index,
                direction_ge: survivor.secondary_direction == "ge",
                profile: survivor.profile_id - 1,
            };
            let anchor_neighbors = [
                key.anchor_threshold.checked_sub(1),
                Some(key.anchor_threshold + 1),
            ];
            let secondary_neighbors = [
                key.secondary_quantile.checked_sub(1),
                Some(key.secondary_quantile + 1),
            ];
            survivor.passes_adjacent_plateau =
                anchor_neighbors.into_iter().flatten().any(|neighbor| {
                    survivor_keys.contains(&RuleKey {
                        anchor_threshold: neighbor,
                        ..key.clone()
                    })
                }) || secondary_neighbors.into_iter().flatten().any(|neighbor| {
                    survivor_keys.contains(&RuleKey {
                        secondary_quantile: neighbor,
                        ..key.clone()
                    })
                });
        }
        survivors.sort_by(|a, b| {
            b.passes_adjacent_plateau
                .cmp(&a.passes_adjacent_plateau)
                .then_with(|| {
                    b.worst_mean_wallet_log_growth
                        .total_cmp(&a.worst_mean_wallet_log_growth)
                })
        });
        let rules_per_checkpoint = unordered_two_condition_rule_count(
            strategy_feature_indices.len(),
            manifest.quantiles.len(),
            manifest.anchor_directions.len(),
        );
        let result = CheckpointResult {
            status: "complete",
            checkpoint_ms: checkpoint_shape.checkpoint_ms,
            declared_rules: rules_per_checkpoint,
            declared_rule_exit_trials: rules_per_checkpoint * manifest.profile_count as u64,
            stage_one_positive_baseline_candidate_count: stage_one.len(),
            economic_gate_near_miss_count,
            point_gate_survivor_count: survivors.len(),
            plateau_survivor_count: survivors
                .iter()
                .filter(|value| value.passes_adjacent_plateau)
                .count(),
            rejection_counts_among_stage_one_candidates: rejection_counts,
            economic_gate_near_misses,
            top_near_misses,
            elapsed_seconds: checkpoint_started.elapsed().as_secs_f64(),
            survivors,
        };
        atomic_json(
            &output.join(format!(
                "checkpoint-{}.json",
                checkpoint_shape.checkpoint_ms
            )),
            &result,
        )?;
        println!(
            "checkpoint={}/{} rules={} point_survivors={} plateau_survivors={} elapsed={:.3}s",
            checkpoint_results.len() + 1,
            manifest.checkpoints.len(),
            result.declared_rules,
            result.point_gate_survivor_count,
            result.plateau_survivor_count,
            result.elapsed_seconds
        );
        checkpoint_results.push(result);
    }
    let computed_rule_count: u64 = checkpoint_results
        .iter()
        .map(|checkpoint| checkpoint.declared_rules)
        .sum();
    if computed_rule_count != manifest.declared_two_condition_rules {
        return Err(format!(
            "manifest declared {} two-condition rules, canonical engine computed {}",
            manifest.declared_two_condition_rules, computed_rule_count
        )
        .into());
    }
    let computed_rule_exit_trials =
        computed_rule_count.saturating_mul(manifest.profile_count as u64);
    if computed_rule_exit_trials != manifest.declared_exact_rule_exit_trials {
        return Err(format!(
            "manifest declared {} exact rule/exit trials, canonical engine computed {}",
            manifest.declared_exact_rule_exit_trials, computed_rule_exit_trials
        )
        .into());
    }
    let final_result = FinalResult {
        kind: "pumpquant.pumpapi_buy_pressure_interactions_m4",
        version: 1,
        status: "complete",
        development_only: true,
        holdout_read: false,
        architecture: std::env::consts::ARCH,
        logical_cpus,
        declared_two_condition_rules: manifest.declared_two_condition_rules,
        declared_exact_rule_exit_trials: manifest.declared_exact_rule_exit_trials,
        point_gate_survivor_count: checkpoint_results
            .iter()
            .map(|value| value.point_gate_survivor_count)
            .sum(),
        plateau_survivor_count: checkpoint_results
            .iter()
            .map(|value| value.plateau_survivor_count)
            .sum(),
        elapsed_seconds: run_started.elapsed().as_secs_f64(),
        checkpoints: checkpoint_results,
    };
    atomic_json(&output.join("result.json"), &final_result)?;
    println!("{}", serde_json::to_string(&final_result)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unordered_rule_count_removes_only_commutative_duplicates() {
        assert_eq!(unordered_two_condition_rule_count(20, 9, 2), 61_560);
        assert_eq!(unordered_two_condition_rule_count(1, 9, 2), 0);
    }

    fn toy_period(label: &str, shift: f64) -> PeriodData {
        let rows = 40;
        let mut features = Vec::with_capacity(rows * 2);
        for row in 0..rows {
            features.push(if row % 3 == 0 { 0.9 } else { 0.4 });
            features.push((row as f64 + shift) / 10.0);
        }
        let folds = (0..rows).map(|row| (row % 4) as u8).collect();
        let mut baseline = vec![0.0; PROFILES * rows];
        for profile in 0..PROFILES {
            for row in 0..rows {
                baseline[outcome_offset(profile, row, rows)] =
                    (row as f64 - 12.0) * (profile + 1) as f64;
            }
        }
        PeriodData {
            label: label.to_string(),
            rows,
            features,
            folds,
            pnl: vec![baseline.clone(), baseline],
            catastrophic: vec![vec![0; PROFILES * rows], vec![0; PROFILES * rows]],
        }
    }

    #[test]
    fn bucketed_stage_one_matches_brute_force() {
        let periods = vec![toy_period("a", 0.0), toy_period("b", 0.25)];
        let anchor = Anchor {
            feature_index: 0,
            feature_name: "anchor".to_string(),
            threshold_index: 0,
            threshold: 0.8,
            direction_ge: true,
        };
        let thresholds = vec![0.5, 1.5, 2.5, 3.5];
        let actual: HashSet<RuleKey> =
            stage_one_for_pair(&anchor, 1, &thresholds, &periods, 2, 1, 1, PROFILES)
                .into_iter()
                .map(|candidate| candidate.key)
                .collect();
        let mut expected = HashSet::new();
        for direction_ge in [false, true] {
            for (quantile, &threshold) in thresholds.iter().enumerate() {
                for profile in 0..PROFILES {
                    let passes = periods.iter().all(|period| {
                        let mut count = 0usize;
                        let mut fold_count = [0usize; FOLDS];
                        let mut sum = 0.0;
                        for row in 0..period.rows {
                            let first = period.features[row * 2];
                            let second = period.features[row * 2 + 1];
                            let selected = first >= anchor.threshold
                                && if direction_ge {
                                    second >= threshold
                                } else {
                                    second <= threshold
                                };
                            if selected {
                                count += 1;
                                fold_count[period.folds[row] as usize] += 1;
                                sum += period.pnl[0][outcome_offset(profile, row, period.rows)];
                            }
                        }
                        count >= 1 && fold_count.iter().all(|value| *value >= 1) && sum > 0.0
                    });
                    if passes {
                        expected.insert(RuleKey {
                            anchor_feature: 0,
                            anchor_threshold: 0,
                            anchor_direction_ge: true,
                            secondary_feature: 1,
                            secondary_quantile: quantile,
                            direction_ge,
                            profile,
                        });
                    }
                }
            }
        }
        assert_eq!(actual, expected);
    }
}
