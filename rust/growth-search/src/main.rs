use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const TOP_PER_CHECKPOINT: usize = 100;

#[derive(Debug, Deserialize)]
struct Shape {
    checkpoints: usize,
    mints: usize,
    features: usize,
    profiles: usize,
    scenarios: usize,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    status: String,
    holdout_read: bool,
    shape: Shape,
    checkpoints: Vec<i32>,
    feature_names: Vec<String>,
    profile_ids: Vec<String>,
    scenario_ids: Vec<String>,
    #[serde(default)]
    primary_scenario_id: Option<String>,
    tail_probabilities: Vec<f64>,
    folds: usize,
    minimum_selected_signals_total: usize,
    minimum_selected_signals_per_fold: usize,
    minimum_closed_trades_total: usize,
}

#[derive(Clone)]
struct Condition {
    feature_name: String,
    direction_high: bool,
    probability: f64,
    bits: Vec<u64>,
}

#[derive(Clone)]
struct RuleMask {
    rule_id: String,
    bits: Vec<u64>,
    multiplicity: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Winner {
    rule_id: String,
    checkpoint_seconds: i32,
    profile_id: String,
    signals: usize,
    signals_per_hour: f64,
    primary_fills: usize,
    primary_fills_per_hour: f64,
    primary_win_rate: f64,
    minimum_win_rate: f64,
    primary_geometric_growth_per_hour: f64,
    worst_required_geometric_growth_per_hour: f64,
    primary_mean_account_return: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct CheckpointResult {
    status: String,
    checkpoint_seconds: i32,
    generated_rules: usize,
    unique_behavioral_rules: usize,
    evaluated_rule_exit_variants: u64,
    elapsed_seconds: f64,
    logical_cpus: usize,
    architecture: String,
    winners: Vec<Winner>,
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
    checkpoint_count: usize,
    generated_rules: usize,
    unique_behavioral_rules: usize,
    evaluated_rule_exit_variants: u64,
    elapsed_seconds: f64,
    winners: Vec<Winner>,
}

fn read_f64(path: &Path) -> io::Result<Vec<f64>> {
    let bytes = fs::read(path)?;
    if bytes.len() % 8 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid f64 byte length",
        ));
    }
    Ok(bytes
        .chunks_exact(8)
        .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
        .collect())
}

fn read_i64(path: &Path) -> io::Result<Vec<i64>> {
    let bytes = fs::read(path)?;
    if bytes.len() % 8 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid i64 byte length",
        ));
    }
    Ok(bytes
        .chunks_exact(8)
        .map(|chunk| i64::from_le_bytes(chunk.try_into().unwrap()))
        .collect())
}

fn quantile(values: &mut [f64], probability: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable_by(f64::total_cmp);
    let raw = (values.len() - 1) as f64 * probability;
    let index = raw.round_ties_even().clamp(0.0, (values.len() - 1) as f64) as usize;
    Some(values[index])
}

fn set_bit(bits: &mut [u64], index: usize) {
    bits[index / 64] |= 1u64 << (index % 64);
}

#[cfg(test)]
fn indices(bits: &[u64], mint_count: usize) -> Vec<usize> {
    let mut result = Vec::new();
    indices_into(bits, mint_count, &mut result);
    result
}

fn indices_into(bits: &[u64], mint_count: usize, result: &mut Vec<usize>) {
    result.clear();
    for (word_index, &word) in bits.iter().enumerate() {
        let mut remaining = word;
        while remaining != 0 {
            let bit = remaining.trailing_zeros() as usize;
            let index = word_index * 64 + bit;
            if index < mint_count {
                result.push(index);
            }
            remaining &= remaining - 1;
        }
    }
}

fn condition_id(condition: &Condition) -> String {
    format!(
        "{}:{}:q{}",
        condition.feature_name,
        if condition.direction_high {
            "high"
        } else {
            "low"
        },
        condition.probability
    )
}

fn build_conditions(
    manifest: &Manifest,
    checkpoint_index: usize,
    features: &[f64],
) -> Vec<Vec<Condition>> {
    let n = manifest.shape.mints;
    let f = manifest.shape.features;
    let words = n.div_ceil(64);
    let boundaries: Vec<usize> = (0..=manifest.folds + 1)
        .map(|index| n * index / (manifest.folds + 1))
        .collect();
    (0..f)
        .into_par_iter()
        .map(|feature_index| {
            let mut variants = Vec::new();
            for &direction_high in &[false, true] {
                for &probability in &manifest.tail_probabilities {
                    let mut bits = vec![0u64; words];
                    for fold in 0..manifest.folds {
                        let train_end = boundaries[fold + 1];
                        let validation_start = train_end;
                        let validation_end = boundaries[fold + 2];
                        let mut training: Vec<f64> = (0..train_end)
                            .filter_map(|mint| {
                                let offset = (checkpoint_index * n + mint) * f + feature_index;
                                let value = features[offset];
                                value.is_finite().then_some(value)
                            })
                            .collect();
                        let threshold_probability = if direction_high {
                            1.0 - probability
                        } else {
                            probability
                        };
                        let Some(threshold) = quantile(&mut training, threshold_probability) else {
                            continue;
                        };
                        for mint in validation_start..validation_end {
                            let offset = (checkpoint_index * n + mint) * f + feature_index;
                            let value = features[offset];
                            if value.is_finite()
                                && ((!direction_high && value <= threshold)
                                    || (direction_high && value >= threshold))
                            {
                                set_bit(&mut bits, mint);
                            }
                        }
                    }
                    variants.push(Condition {
                        feature_name: manifest.feature_names[feature_index].clone(),
                        direction_high,
                        probability,
                        bits,
                    });
                }
            }
            variants
        })
        .collect()
}

fn build_unique_rules(conditions: &[Vec<Condition>]) -> (usize, Vec<RuleMask>) {
    let singles: Vec<RuleMask> = conditions
        .par_iter()
        .flat_map_iter(|variants| {
            variants.iter().map(|condition| RuleMask {
                rule_id: condition_id(condition),
                bits: condition.bits.clone(),
                multiplicity: 1,
            })
        })
        .collect();
    let feature_pairs: Vec<(usize, usize)> = (0..conditions.len())
        .flat_map(|left| (left + 1..conditions.len()).map(move |right| (left, right)))
        .collect();
    let pairs: Vec<RuleMask> = feature_pairs
        .into_par_iter()
        .flat_map_iter(|(left, right)| {
            conditions[left].iter().flat_map(move |first| {
                conditions[right].iter().map(move |second| RuleMask {
                    rule_id: format!("{}&{}", condition_id(first), condition_id(second)),
                    bits: first
                        .bits
                        .iter()
                        .zip(&second.bits)
                        .map(|(a, b)| a & b)
                        .collect(),
                    multiplicity: 1,
                })
            })
        })
        .collect();
    let generated = singles.len() + pairs.len();
    let mut unique: HashMap<Vec<u64>, (String, u64)> = HashMap::with_capacity(generated);
    for rule in singles.into_iter().chain(pairs) {
        let entry = unique.entry(rule.bits).or_insert((rule.rule_id, 0));
        entry.1 += rule.multiplicity;
    }
    let rules: Vec<RuleMask> = unique
        .into_iter()
        .map(|(bits, (rule_id, multiplicity))| RuleMask {
            rule_id,
            bits,
            multiplicity,
        })
        .collect();
    debug_assert_eq!(
        rules.iter().map(|rule| rule.multiplicity).sum::<u64>(),
        generated as u64
    );
    (generated, rules)
}

fn outcome_offset(
    manifest: &Manifest,
    checkpoint: usize,
    profile: usize,
    scenario: usize,
    mint: usize,
) -> usize {
    (((checkpoint * manifest.shape.profiles + profile) * manifest.shape.scenarios + scenario)
        * manifest.shape.mints)
        + mint
}

fn insert_top(top: &mut Vec<Winner>, winner: Winner) {
    top.push(winner);
    top.sort_unstable_by(|a, b| {
        b.worst_required_geometric_growth_per_hour
            .total_cmp(&a.worst_required_geometric_growth_per_hour)
            .then_with(|| {
                b.primary_geometric_growth_per_hour
                    .total_cmp(&a.primary_geometric_growth_per_hour)
            })
            .then_with(|| b.minimum_win_rate.total_cmp(&a.minimum_win_rate))
            .then_with(|| {
                b.primary_fills_per_hour
                    .total_cmp(&a.primary_fills_per_hour)
            })
    });
    if top.len() > TOP_PER_CHECKPOINT {
        top.truncate(TOP_PER_CHECKPOINT);
    }
}

#[allow(clippy::too_many_arguments)]
fn evaluate_rule(
    manifest: &Manifest,
    checkpoint_index: usize,
    checkpoint_seconds: i32,
    rule: &RuleMask,
    selected: &mut Vec<usize>,
    returns: &[f64],
    log_returns: &[f64],
    entered: &[u8],
    closed: &[u8],
    fold_by_mint: &[u8],
    observation_hours: f64,
    primary_scenario: usize,
) -> Vec<Winner> {
    let n = manifest.shape.mints;
    indices_into(&rule.bits, n, selected);
    if selected.len() < manifest.minimum_selected_signals_total {
        return Vec::new();
    }
    let mut fold_support = [0usize; 4];
    for &mint in selected.iter() {
        let fold = fold_by_mint[mint] as usize;
        if fold < manifest.folds {
            fold_support[fold] += 1;
        }
    }
    if fold_support
        .iter()
        .any(|&count| count < manifest.minimum_selected_signals_per_fold)
    {
        return Vec::new();
    }
    let mut winners = Vec::new();
    for profile in 0..manifest.shape.profiles {
        let primary_base = outcome_offset(manifest, checkpoint_index, profile, primary_scenario, 0);
        let primary_closed = selected
            .iter()
            .filter(|&&mint| closed[primary_base + mint] != 0)
            .count();
        if primary_closed < manifest.minimum_closed_trades_total {
            continue;
        }
        let mut scenario_log_rates = vec![0.0f64; manifest.shape.scenarios];
        let mut scenario_win_rates = vec![0.0f64; manifest.shape.scenarios];
        let mut primary_fills = 0usize;
        let mut primary_wins = 0usize;
        let mut primary_return_sum = 0.0f64;
        let mut valid = true;
        for scenario in 0..manifest.shape.scenarios {
            let mut total_log = 0.0f64;
            let mut fold_logs = [0.0f64; 4];
            let mut fills = 0usize;
            let mut wins = 0usize;
            let base = outcome_offset(manifest, checkpoint_index, profile, scenario, 0);
            for &mint in selected.iter() {
                let offset = base + mint;
                let account_return = returns[offset];
                let log_return = log_returns[offset];
                total_log += log_return;
                if entered[offset] != 0 {
                    fills += 1;
                    if account_return > 0.0 {
                        wins += 1;
                    }
                }
                if scenario == primary_scenario {
                    primary_return_sum += account_return;
                }
                let fold = fold_by_mint[mint] as usize;
                if fold < manifest.folds {
                    fold_logs[fold] += log_return;
                }
            }
            if !valid
                || total_log <= 0.0
                || fold_logs.iter().any(|&value| value <= 0.0)
                || fills == 0
            {
                valid = false;
                break;
            }
            scenario_log_rates[scenario] = total_log / observation_hours;
            scenario_win_rates[scenario] = wins as f64 / fills as f64;
            if scenario == primary_scenario {
                primary_fills = fills;
                primary_wins = wins;
            }
        }
        if !valid {
            continue;
        }
        let primary_rate = scenario_log_rates[primary_scenario].exp_m1();
        let worst_rate = scenario_log_rates
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min)
            .exp_m1();
        winners.push(Winner {
            rule_id: rule.rule_id.clone(),
            checkpoint_seconds,
            profile_id: manifest.profile_ids[profile].clone(),
            signals: selected.len(),
            signals_per_hour: selected.len() as f64 / observation_hours,
            primary_fills,
            primary_fills_per_hour: primary_fills as f64 / observation_hours,
            primary_win_rate: primary_wins as f64 / primary_fills as f64,
            minimum_win_rate: scenario_win_rates
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min),
            primary_geometric_growth_per_hour: primary_rate,
            worst_required_geometric_growth_per_hour: worst_rate,
            primary_mean_account_return: primary_return_sum / selected.len() as f64,
        });
    }
    winners
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
    let input = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("artifacts/sqd-m4-growth-kernel-inputs-v1"));
    let output = std::env::args()
        .nth(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("artifacts/sqd-m4-growth-search-v1"));
    fs::create_dir_all(&output)?;
    let manifest: Manifest = serde_json::from_slice(&fs::read(input.join("manifest.json"))?)?;
    if manifest.status != "complete" || manifest.holdout_read {
        return Err("kernel input manifest is not a complete development-only artifact".into());
    }
    if std::env::consts::ARCH != "aarch64" {
        return Err(format!("expected aarch64, found {}", std::env::consts::ARCH).into());
    }
    if manifest.shape.checkpoints != manifest.checkpoints.len()
        || manifest.shape.features != manifest.feature_names.len()
        || manifest.shape.profiles != manifest.profile_ids.len()
        || manifest.shape.scenarios != manifest.scenario_ids.len()
        || manifest.folds != 4
    {
        return Err("manifest shape/name mismatch".into());
    }
    let logical_cpus = thread::available_parallelism()?.get();
    let features = read_f64(&input.join("features.f64le"))?;
    let times = read_i64(&input.join("first-block-time.i64le"))?;
    let returns = read_f64(&input.join("account-returns.f64le"))?;
    let entered = fs::read(input.join("entered.u8"))?;
    let closed = fs::read(input.join("closed.u8"))?;
    let expected_features =
        manifest.shape.checkpoints * manifest.shape.mints * manifest.shape.features;
    let expected_outcomes = manifest.shape.checkpoints
        * manifest.shape.profiles
        * manifest.shape.scenarios
        * manifest.shape.mints;
    if features.len() != expected_features
        || times.len() != manifest.shape.checkpoints * manifest.shape.mints
        || returns.len() != expected_outcomes
        || entered.len() != expected_outcomes
        || closed.len() != expected_outcomes
    {
        return Err("kernel input byte counts do not match manifest shapes".into());
    }
    if let Some(index) = returns
        .par_iter()
        .position_any(|value| !value.is_finite() || *value <= -1.0)
    {
        return Err(format!("invalid account return at kernel offset {index}").into());
    }
    let log_returns: Vec<f64> = returns.par_iter().map(|value| value.ln_1p()).collect();
    let primary_scenario_id = manifest
        .primary_scenario_id
        .as_deref()
        .unwrap_or("primary_l2_tol500_c55_a30");
    let primary_scenario = manifest
        .scenario_ids
        .iter()
        .position(|value| value == primary_scenario_id)
        .ok_or("primary scenario missing")?;
    let run_started = Instant::now();
    let variants_per_feature = (manifest.tail_probabilities.len() * 2) as u64;
    let feature_count = manifest.shape.features as u64;
    let declared_rules_per_checkpoint = feature_count * variants_per_feature
        + (feature_count * (feature_count - 1) / 2) * variants_per_feature * variants_per_feature;
    let declared_variants_total = declared_rules_per_checkpoint
        * manifest.shape.checkpoints as u64
        * manifest.shape.profiles as u64;
    let global_completed_variants = Arc::new(AtomicU64::new(0));
    let mut checkpoint_results = Vec::new();
    for (checkpoint_index, &checkpoint_seconds) in manifest.checkpoints.iter().enumerate() {
        let checkpoint_path = output.join(format!("checkpoint-{checkpoint_seconds:03}.json"));
        if checkpoint_path.exists() {
            let saved: CheckpointResult = serde_json::from_slice(&fs::read(&checkpoint_path)?)?;
            if saved.status == "complete" {
                println!("checkpoint={checkpoint_seconds}s resumed from sealed result");
                global_completed_variants
                    .fetch_add(saved.evaluated_rule_exit_variants, Ordering::Relaxed);
                checkpoint_results.push(saved);
                continue;
            }
        }
        let started = Instant::now();
        let conditions = build_conditions(&manifest, checkpoint_index, &features);
        let (generated_rules, rules) = build_unique_rules(&conditions);
        let checkpoint_completed = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let progress_checkpoint_completed = Arc::clone(&checkpoint_completed);
        let progress_global_completed = Arc::clone(&global_completed_variants);
        let progress_stop = Arc::clone(&stop);
        let total_rules = rules.len() as u64;
        let profiles = manifest.shape.profiles as u64;
        let monitor = thread::spawn(move || {
            while !progress_stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(5));
                let checkpoint_done = progress_checkpoint_completed.load(Ordering::Relaxed);
                let global_done = progress_global_completed.load(Ordering::Relaxed);
                if global_done == 0 {
                    continue;
                }
                let elapsed = run_started.elapsed().as_secs_f64();
                let rate = global_done as f64 / elapsed;
                let eta = declared_variants_total.saturating_sub(global_done) as f64 / rate;
                let percent = 100.0 * global_done as f64 / declared_variants_total as f64;
                println!(
                    "overall={global_done}/{declared_variants_total} variants \
                     ({percent:.4}%); checkpoint={checkpoint_seconds}s \
                     behavioral-rules={checkpoint_done}/{total_rules}; \
                     rate={rate:.1} variants/s; elapsed={elapsed:.1}s; ETA={eta:.1}s"
                );
            }
        });
        let n = manifest.shape.mints;
        let validation_start = n / (manifest.folds + 1);
        let boundaries: Vec<usize> = (0..=manifest.folds + 1)
            .map(|index| n * index / (manifest.folds + 1))
            .collect();
        let mut fold_by_mint = vec![u8::MAX; n];
        for fold in 0..manifest.folds {
            for value in &mut fold_by_mint[boundaries[fold + 1]..boundaries[fold + 2]] {
                *value = fold as u8;
            }
        }
        let observation_hours = (times[checkpoint_index * n + n - 1]
            - times[checkpoint_index * n + validation_start])
            as f64
            / 3600.0;
        let winners = rules
            .par_iter()
            .fold(
                || (Vec::new(), Vec::with_capacity(n)),
                |(mut top, mut selected): (Vec<Winner>, Vec<usize>), rule| {
                    for winner in evaluate_rule(
                        &manifest,
                        checkpoint_index,
                        checkpoint_seconds,
                        rule,
                        &mut selected,
                        &returns,
                        &log_returns,
                        &entered,
                        &closed,
                        &fold_by_mint,
                        observation_hours,
                        primary_scenario,
                    ) {
                        insert_top(&mut top, winner);
                    }
                    checkpoint_completed.fetch_add(1, Ordering::Relaxed);
                    global_completed_variants
                        .fetch_add(rule.multiplicity * profiles, Ordering::Relaxed);
                    (top, selected)
                },
            )
            .reduce(
                || (Vec::new(), Vec::new()),
                |(mut left, left_selected), (right, _right_selected)| {
                    for winner in right {
                        insert_top(&mut left, winner);
                    }
                    (left, left_selected)
                },
            )
            .0;
        stop.store(true, Ordering::Relaxed);
        monitor.join().map_err(|_| "progress monitor panicked")?;
        let result = CheckpointResult {
            status: "complete".to_string(),
            checkpoint_seconds,
            generated_rules,
            unique_behavioral_rules: rules.len(),
            evaluated_rule_exit_variants: generated_rules as u64 * manifest.shape.profiles as u64,
            elapsed_seconds: started.elapsed().as_secs_f64(),
            logical_cpus,
            architecture: std::env::consts::ARCH.to_string(),
            winners,
        };
        atomic_json(&checkpoint_path, &result)?;
        checkpoint_results.push(result);
    }
    let mut all_winners = Vec::new();
    for result in &checkpoint_results {
        for winner in &result.winners {
            insert_top(&mut all_winners, winner.clone());
        }
    }
    let final_result = FinalResult {
        kind: "pumpquant.sqd_m4_geometric_growth_search",
        version: 1,
        status: "complete",
        development_only: true,
        holdout_read: false,
        architecture: std::env::consts::ARCH,
        logical_cpus,
        checkpoint_count: checkpoint_results.len(),
        generated_rules: checkpoint_results
            .iter()
            .map(|value| value.generated_rules)
            .sum(),
        unique_behavioral_rules: checkpoint_results
            .iter()
            .map(|value| value.unique_behavioral_rules)
            .sum(),
        evaluated_rule_exit_variants: checkpoint_results
            .iter()
            .map(|value| value.evaluated_rule_exit_variants)
            .sum(),
        elapsed_seconds: run_started.elapsed().as_secs_f64(),
        winners: all_winners,
    };
    atomic_json(&output.join("result.json"), &final_result)?;
    println!("{}", serde_json::to_string(&final_result)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_matches_python_ties_to_even_indexing() {
        let mut values = vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(quantile(&mut values, 0.5), Some(2.0));
    }

    #[test]
    fn bit_indices_round_trip_across_words() {
        let mut bits = vec![0u64; 3];
        for index in [0, 63, 64, 129] {
            set_bit(&mut bits, index);
        }
        assert_eq!(indices(&bits, 130), vec![0, 63, 64, 129]);
    }

    #[test]
    fn top_ranking_prefers_worst_scenario_growth() {
        let winner = |name: &str, worst: f64, primary: f64| Winner {
            rule_id: name.to_string(),
            checkpoint_seconds: 10,
            profile_id: "profile".to_string(),
            signals: 50,
            signals_per_hour: 1.0,
            primary_fills: 40,
            primary_fills_per_hour: 0.8,
            primary_win_rate: 0.4,
            minimum_win_rate: 0.4,
            primary_geometric_growth_per_hour: primary,
            worst_required_geometric_growth_per_hour: worst,
            primary_mean_account_return: 0.01,
        };
        let mut top = Vec::new();
        insert_top(&mut top, winner("higher-primary", 0.01, 0.10));
        insert_top(&mut top, winner("higher-worst", 0.02, 0.03));
        assert_eq!(top[0].rule_id, "higher-worst");
    }
}
