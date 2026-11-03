use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const FOLDS: usize = 4;
const ANCHOR: &str = "unique_buyer_imbalance";
const SECONDARY: &str = "market_cap_per_virtual_quote";
const ANCHOR_THRESHOLDS: [usize; 2] = [7, 8];
const SECONDARY_THRESHOLDS: [usize; 3] = [6, 7, 8];

#[derive(Deserialize)]
struct PeriodShape {
    label: String,
    rows: usize,
}
#[derive(Deserialize)]
struct CheckpointShape {
    checkpoint_ms: i64,
    periods: Vec<PeriodShape>,
}
#[derive(Deserialize)]
struct Manifest {
    status: String,
    holdout_read: bool,
    wallet_lamports: i64,
    profile_count: usize,
    feature_names: Vec<String>,
    quantiles: Vec<f64>,
    minimum_period_signals: usize,
    minimum_fold_signals: usize,
    checkpoints: Vec<CheckpointShape>,
    declared_exact_rule_exit_trials: u64,
}
#[derive(Deserialize)]
struct CheckpointManifest {
    secondary_thresholds: HashMap<String, Vec<Option<f64>>>,
}

struct Period {
    label: String,
    rows: usize,
    features: Vec<f64>,
    folds: Vec<u8>,
    pnl: Vec<f64>,
    catastrophic: Vec<u8>,
}

#[derive(Clone, Copy, Hash, Eq, PartialEq)]
struct Key {
    anchor_q: usize,
    secondary_q: usize,
    third: usize,
    third_q: usize,
    third_ge: bool,
    profile: usize,
}

#[derive(Clone, Serialize)]
struct Metrics {
    n: usize,
    mean_pnl_lamports: f64,
    mean_wallet_log_growth: f64,
    largest_winner_removed_mean_pnl: f64,
    fold_n: [usize; FOLDS],
    fold_mean_pnl_lamports: [f64; FOLDS],
    catastrophic_exit_failures: usize,
}

#[derive(Clone, Serialize)]
struct Survivor {
    anchor_feature: &'static str,
    anchor_quantile: f64,
    anchor_threshold: f64,
    secondary_feature: &'static str,
    secondary_quantile: f64,
    secondary_threshold: f64,
    third_feature: String,
    third_direction: &'static str,
    third_quantile: f64,
    third_threshold: f64,
    profile_id: usize,
    periods: HashMap<String, Metrics>,
    worst_wallet_log_growth: f64,
    anchor_plateau: bool,
    secondary_plateau: bool,
    third_plateau: bool,
    full_three_dimension_plateau: bool,
    #[serde(skip)]
    key: Key,
}

#[derive(Serialize)]
struct ResultPayload {
    kind: &'static str,
    version: u32,
    status: &'static str,
    development_only: bool,
    source_pair_search_trials: u64,
    declared_targeted_triple_exit_trials: u64,
    point_survivor_count: usize,
    full_plateau_survivor_count: usize,
    elapsed_seconds: f64,
    survivors: Vec<Survivor>,
}

fn read_f64(path: &Path) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    let bytes = fs::read(path)?;
    if bytes.len() % 8 != 0 {
        return Err("invalid f64 file".into());
    }
    Ok(bytes
        .chunks_exact(8)
        .map(|x| f64::from_le_bytes(x.try_into().unwrap()))
        .collect())
}

fn load_period(
    directory: &Path,
    index: usize,
    shape: &PeriodShape,
    features: usize,
    profiles: usize,
) -> Result<Period, Box<dyn std::error::Error>> {
    let values = read_f64(&directory.join(format!("period-{index}-features.f64le")))?;
    let folds = fs::read(directory.join(format!("period-{index}-folds.u8")))?;
    let pnl = read_f64(&directory.join(format!("period-{index}-scenario-0-pnl.f64le")))?;
    let catastrophic =
        fs::read(directory.join(format!("period-{index}-scenario-0-catastrophic.u8")))?;
    if values.len() != shape.rows * features
        || folds.len() != shape.rows
        || pnl.len() != shape.rows * profiles
        || catastrophic.len() != shape.rows * profiles
    {
        return Err("shape mismatch".into());
    }
    Ok(Period {
        label: shape.label.clone(),
        rows: shape.rows,
        features: values,
        folds,
        pnl,
        catastrophic,
    })
}

fn evaluate(
    key: Key,
    periods: &[Period],
    feature_count: usize,
    anchor_index: usize,
    secondary_index: usize,
    thresholds: &[Vec<f64>],
    wallet: f64,
    minimum_period: usize,
    minimum_fold: usize,
) -> Option<(HashMap<String, Metrics>, f64)> {
    let anchor_threshold = thresholds[anchor_index][key.anchor_q];
    let secondary_threshold = thresholds[secondary_index][key.secondary_q];
    let third_threshold = thresholds[key.third][key.third_q];
    let mut output = HashMap::new();
    let mut worst_log = f64::INFINITY;
    for period in periods {
        let mut n = 0usize;
        let mut fold_n = [0usize; FOLDS];
        let mut sum = 0.0;
        let mut log_sum = 0.0;
        let mut maximum = f64::NEG_INFINITY;
        let mut fold_sum = [0.0; FOLDS];
        let mut catastrophic = 0usize;
        for row in 0..period.rows {
            let a = period.features[row * feature_count + anchor_index];
            let b = period.features[row * feature_count + secondary_index];
            let c = period.features[row * feature_count + key.third];
            if a < anchor_threshold
                || b < secondary_threshold
                || !c.is_finite()
                || if key.third_ge {
                    c < third_threshold
                } else {
                    c > third_threshold
                }
            {
                continue;
            }
            let fold = period.folds[row] as usize;
            if fold >= FOLDS {
                continue;
            }
            let offset = key.profile * period.rows + row;
            let value = period.pnl[offset];
            n += 1;
            fold_n[fold] += 1;
            sum += value;
            fold_sum[fold] += value;
            log_sum += (1.0 + value / wallet).max(1e-12).ln();
            maximum = maximum.max(value);
            catastrophic += period.catastrophic[offset] as usize;
        }
        if n < minimum_period || fold_n.iter().any(|count| *count < minimum_fold) || n < 2 {
            return None;
        }
        let mean = sum / n as f64;
        let mean_log = log_sum / n as f64;
        let removed = (sum - maximum) / (n - 1) as f64;
        let fold_means = std::array::from_fn(|fold| fold_sum[fold] / fold_n[fold] as f64);
        if mean <= 0.0
            || mean_log <= 0.0
            || removed <= 0.0
            || fold_means.iter().any(|x| *x <= 0.0 || !x.is_finite())
        {
            return None;
        }
        worst_log = worst_log.min(mean_log);
        output.insert(
            period.label.clone(),
            Metrics {
                n,
                mean_pnl_lamports: mean,
                mean_wallet_log_growth: mean_log,
                largest_winner_removed_mean_pnl: removed,
                fold_n,
                fold_mean_pnl_lamports: fold_means,
                catastrophic_exit_failures: catastrophic,
            },
        );
    }
    if output.len() == periods.len() {
        Some((output, worst_log))
    } else {
        None
    }
}

fn neighbor(set: &HashSet<Key>, key: Key, dimension: usize) -> bool {
    let candidates: Vec<Key> = match dimension {
        0 => ANCHOR_THRESHOLDS
            .iter()
            .copied()
            .filter(|q| *q != key.anchor_q)
            .map(|q| Key { anchor_q: q, ..key })
            .collect(),
        1 => SECONDARY_THRESHOLDS
            .iter()
            .copied()
            .filter(|q| (*q as isize - key.secondary_q as isize).abs() == 1)
            .map(|q| Key {
                secondary_q: q,
                ..key
            })
            .collect(),
        _ => [key.third_q.checked_sub(1), Some(key.third_q + 1)]
            .into_iter()
            .flatten()
            .filter(|q| *q < 9)
            .map(|q| Key { third_q: q, ..key })
            .collect(),
    };
    candidates
        .into_iter()
        .any(|candidate| set.contains(&candidate))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = PathBuf::from(std::env::args().nth(1).ok_or("usage INPUT OUTPUT")?);
    let output = PathBuf::from(std::env::args().nth(2).ok_or("usage INPUT OUTPUT")?);
    fs::create_dir_all(&output)?;
    let began = Instant::now();
    let manifest: Manifest = serde_json::from_slice(&fs::read(input.join("manifest.json"))?)?;
    if manifest.status != "complete" || manifest.holdout_read || manifest.checkpoints.len() != 1 {
        return Err("invalid input".into());
    }
    let checkpoint_dir = input.join(format!(
        "checkpoint-{}",
        manifest.checkpoints[0].checkpoint_ms
    ));
    let checkpoint: CheckpointManifest =
        serde_json::from_slice(&fs::read(checkpoint_dir.join("manifest.json"))?)?;
    let feature_index: HashMap<&str, usize> = manifest
        .feature_names
        .iter()
        .enumerate()
        .map(|(i, name)| (name.as_str(), i))
        .collect();
    let anchor_index = *feature_index.get(ANCHOR).ok_or("anchor absent")?;
    let secondary_index = *feature_index.get(SECONDARY).ok_or("secondary absent")?;
    let periods: Vec<Period> = manifest.checkpoints[0]
        .periods
        .iter()
        .enumerate()
        .map(|(i, shape)| {
            load_period(
                &checkpoint_dir,
                i,
                shape,
                manifest.feature_names.len(),
                manifest.profile_count,
            )
        })
        .collect::<Result<_, _>>()?;
    let all_thresholds: Vec<Vec<f64>> = manifest
        .feature_names
        .iter()
        .map(|name| {
            checkpoint
                .secondary_thresholds
                .get(name)
                .ok_or("threshold missing")?
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>()
                .ok_or("null threshold".into())
        })
        .collect::<Result<_, Box<dyn std::error::Error>>>()?;
    let thresholds = all_thresholds;
    let third_indices: Vec<usize> = (0..manifest.feature_names.len())
        .filter(|index| *index != anchor_index && *index != secondary_index)
        .collect();
    let mut keys = Vec::new();
    for anchor_q in ANCHOR_THRESHOLDS {
        for secondary_q in SECONDARY_THRESHOLDS {
            for &third in &third_indices {
                for third_ge in [false, true] {
                    for third_q in 0..manifest.quantiles.len() {
                        for profile in 0..manifest.profile_count {
                            keys.push(Key {
                                anchor_q,
                                secondary_q,
                                third,
                                third_q,
                                third_ge,
                                profile,
                            });
                        }
                    }
                }
            }
        }
    }
    let declared = keys.len() as u64;
    let mut survivors: Vec<Survivor> = keys
        .par_iter()
        .filter_map(|key| {
            let (period_metrics, worst) = evaluate(
                *key,
                &periods,
                manifest.feature_names.len(),
                anchor_index,
                secondary_index,
                &thresholds,
                manifest.wallet_lamports as f64,
                manifest.minimum_period_signals,
                manifest.minimum_fold_signals,
            )?;
            Some(Survivor {
                anchor_feature: ANCHOR,
                anchor_quantile: manifest.quantiles[key.anchor_q],
                anchor_threshold: thresholds[anchor_index][key.anchor_q],
                secondary_feature: SECONDARY,
                secondary_quantile: manifest.quantiles[key.secondary_q],
                secondary_threshold: thresholds[secondary_index][key.secondary_q],
                third_feature: manifest.feature_names[key.third].clone(),
                third_direction: if key.third_ge { "ge" } else { "le" },
                third_quantile: manifest.quantiles[key.third_q],
                third_threshold: thresholds[key.third][key.third_q],
                profile_id: key.profile + 1,
                periods: period_metrics,
                worst_wallet_log_growth: worst,
                anchor_plateau: false,
                secondary_plateau: false,
                third_plateau: false,
                full_three_dimension_plateau: false,
                key: *key,
            })
        })
        .collect();
    let survivor_keys: HashSet<Key> = survivors.iter().map(|item| item.key).collect();
    for survivor in &mut survivors {
        survivor.anchor_plateau = neighbor(&survivor_keys, survivor.key, 0);
        survivor.secondary_plateau = neighbor(&survivor_keys, survivor.key, 1);
        survivor.third_plateau = neighbor(&survivor_keys, survivor.key, 2);
        survivor.full_three_dimension_plateau =
            survivor.anchor_plateau && survivor.secondary_plateau && survivor.third_plateau;
    }
    survivors.sort_by(|a, b| {
        b.full_three_dimension_plateau
            .cmp(&a.full_three_dimension_plateau)
            .then_with(|| {
                b.worst_wallet_log_growth
                    .total_cmp(&a.worst_wallet_log_growth)
            })
    });
    let payload = ResultPayload {
        kind: "pumpquant.pumpapi_lifecycle_targeted_triples",
        version: 1,
        status: "complete",
        development_only: true,
        source_pair_search_trials: manifest.declared_exact_rule_exit_trials,
        declared_targeted_triple_exit_trials: declared,
        point_survivor_count: survivors.len(),
        full_plateau_survivor_count: survivors
            .iter()
            .filter(|item| item.full_three_dimension_plateau)
            .count(),
        elapsed_seconds: began.elapsed().as_secs_f64(),
        survivors,
    };
    fs::write(
        output.join("result.json"),
        serde_json::to_vec_pretty(&payload)?,
    )?;
    println!("{}", serde_json::to_string(&payload)?);
    Ok(())
}
