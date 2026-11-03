use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const FOLDS: usize = 4;

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
#[derive(Deserialize)]
struct PairResult {
    checkpoints: Vec<PairCheckpoint>,
}
#[derive(Deserialize)]
struct PairCheckpoint {
    economic_gate_near_misses: Vec<Pair>,
}
#[derive(Deserialize)]
struct Pair {
    anchor_feature: String,
    anchor_threshold_index: usize,
    anchor_direction: String,
    secondary_feature: String,
    secondary_quantile_index: usize,
    secondary_direction: String,
    profile_id: usize,
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
    a: usize,
    aq: usize,
    age: bool,
    b: usize,
    bq: usize,
    bge: bool,
    third: usize,
    tq: usize,
    tge: bool,
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
    anchor_feature: String,
    anchor_direction: &'static str,
    anchor_quantile: f64,
    anchor_threshold: f64,
    secondary_feature: String,
    secondary_direction: &'static str,
    secondary_quantile: f64,
    secondary_threshold: f64,
    third_feature: String,
    third_direction: &'static str,
    third_quantile: f64,
    third_threshold: f64,
    profile_id: usize,
    periods: HashMap<String, Metrics>,
    worst_wallet_log_growth: f64,
    third_dimension_plateau: bool,
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
    parent_economic_near_misses: usize,
    declared_targeted_triple_exit_trials: u64,
    point_survivor_count: usize,
    third_dimension_plateau_count: usize,
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
    thresholds: &[Vec<f64>],
    wallet: f64,
    minimum_period: usize,
    minimum_fold: usize,
) -> Option<(HashMap<String, Metrics>, f64)> {
    let mut output = HashMap::new();
    let mut worst_log = f64::INFINITY;
    for period in periods {
        let mut n = 0usize;
        let mut fold_n = [0usize; FOLDS];
        let mut fold_sum = [0.0; FOLDS];
        let mut sum = 0.0;
        let mut log_sum = 0.0;
        let mut maximum = f64::NEG_INFINITY;
        let mut catastrophic = 0usize;
        for row in 0..period.rows {
            let value = |feature: usize| period.features[row * feature_count + feature];
            let a = value(key.a);
            let b = value(key.b);
            let c = value(key.third);
            if !a.is_finite()
                || !b.is_finite()
                || !c.is_finite()
                || if key.age {
                    a < thresholds[key.a][key.aq]
                } else {
                    a > thresholds[key.a][key.aq]
                }
                || if key.bge {
                    b < thresholds[key.b][key.bq]
                } else {
                    b > thresholds[key.b][key.bq]
                }
                || if key.tge {
                    c < thresholds[key.third][key.tq]
                } else {
                    c > thresholds[key.third][key.tq]
                }
            {
                continue;
            }
            let fold = period.folds[row] as usize;
            if fold >= FOLDS {
                continue;
            }
            let offset = key.profile * period.rows + row;
            let pnl = period.pnl[offset];
            n += 1;
            fold_n[fold] += 1;
            sum += pnl;
            fold_sum[fold] += pnl;
            log_sum += (1.0 + pnl / wallet).max(1e-12).ln();
            maximum = maximum.max(pnl);
            catastrophic += period.catastrophic[offset] as usize;
        }
        if n < minimum_period || n < 2 || fold_n.iter().any(|count| *count < minimum_fold) {
            return None;
        }
        let mean = sum / n as f64;
        let mean_log = log_sum / n as f64;
        let removed = (sum - maximum) / (n - 1) as f64;
        let fold_means = std::array::from_fn(|fold| fold_sum[fold] / fold_n[fold] as f64);
        if mean <= 0.0
            || mean_log <= 0.0
            || removed <= 0.0
            || catastrophic > 0
            || fold_means
                .iter()
                .any(|value| !value.is_finite() || *value <= 0.0)
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

fn adjacent(key: Key, dimension: usize) -> Vec<Key> {
    let (q, maximum) = match dimension {
        0 => (key.aq, 9),
        1 => (key.bq, 9),
        _ => (key.tq, 9),
    };
    [q.checked_sub(1), (q + 1 < maximum).then_some(q + 1)]
        .into_iter()
        .flatten()
        .map(|next| match dimension {
            0 => Key { aq: next, ..key },
            1 => Key { bq: next, ..key },
            _ => Key { tq: next, ..key },
        })
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or("usage INPUT PAIR_RESULT OUTPUT")?,
    );
    let pair_path = PathBuf::from(
        std::env::args()
            .nth(2)
            .ok_or("usage INPUT PAIR_RESULT OUTPUT")?,
    );
    let output = PathBuf::from(
        std::env::args()
            .nth(3)
            .ok_or("usage INPUT PAIR_RESULT OUTPUT")?,
    );
    fs::create_dir_all(&output)?;
    let began = Instant::now();
    let manifest: Manifest = serde_json::from_slice(&fs::read(input.join("manifest.json"))?)?;
    let pair: PairResult = serde_json::from_slice(&fs::read(pair_path)?)?;
    if manifest.status != "complete"
        || manifest.holdout_read
        || manifest.checkpoints.len() != 1
        || pair.checkpoints.len() != 1
    {
        return Err("invalid input".into());
    }
    let directory = input.join(format!(
        "checkpoint-{}",
        manifest.checkpoints[0].checkpoint_ms
    ));
    let checkpoint: CheckpointManifest =
        serde_json::from_slice(&fs::read(directory.join("manifest.json"))?)?;
    let index: HashMap<&str, usize> = manifest
        .feature_names
        .iter()
        .enumerate()
        .map(|(i, name)| (name.as_str(), i))
        .collect();
    let thresholds: Vec<Vec<f64>> = manifest
        .feature_names
        .iter()
        .map(|name| {
            checkpoint
                .secondary_thresholds
                .get(name)
                .ok_or("threshold absent")?
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>()
                .ok_or("null threshold".into())
        })
        .collect::<Result<_, Box<dyn std::error::Error>>>()?;
    let periods: Vec<Period> = manifest.checkpoints[0]
        .periods
        .iter()
        .enumerate()
        .map(|(i, shape)| {
            load_period(
                &directory,
                i,
                shape,
                manifest.feature_names.len(),
                manifest.profile_count,
            )
        })
        .collect::<Result<_, _>>()?;
    let mut keys = HashSet::new();
    for parent in &pair.checkpoints[0].economic_gate_near_misses {
        let a = *index
            .get(parent.anchor_feature.as_str())
            .ok_or("anchor missing")?;
        let b = *index
            .get(parent.secondary_feature.as_str())
            .ok_or("secondary missing")?;
        for third in 0..manifest.feature_names.len() {
            if third == a || third == b {
                continue;
            }
            for tge in [false, true] {
                for tq in 0..manifest.quantiles.len() {
                    keys.insert(Key {
                        a,
                        aq: parent.anchor_threshold_index,
                        age: parent.anchor_direction == "ge",
                        b,
                        bq: parent.secondary_quantile_index,
                        bge: parent.secondary_direction == "ge",
                        third,
                        tq,
                        tge,
                        profile: parent.profile_id - 1,
                    });
                }
            }
        }
    }
    let keys: Vec<Key> = keys.into_iter().collect();
    let mut survivors: Vec<Survivor> = keys
        .par_iter()
        .filter_map(|key| {
            let (period_metrics, worst) = evaluate(
                *key,
                &periods,
                manifest.feature_names.len(),
                &thresholds,
                manifest.wallet_lamports as f64,
                manifest.minimum_period_signals,
                manifest.minimum_fold_signals,
            )?;
            Some(Survivor {
                anchor_feature: manifest.feature_names[key.a].clone(),
                anchor_direction: if key.age { "ge" } else { "le" },
                anchor_quantile: manifest.quantiles[key.aq],
                anchor_threshold: thresholds[key.a][key.aq],
                secondary_feature: manifest.feature_names[key.b].clone(),
                secondary_direction: if key.bge { "ge" } else { "le" },
                secondary_quantile: manifest.quantiles[key.bq],
                secondary_threshold: thresholds[key.b][key.bq],
                third_feature: manifest.feature_names[key.third].clone(),
                third_direction: if key.tge { "ge" } else { "le" },
                third_quantile: manifest.quantiles[key.tq],
                third_threshold: thresholds[key.third][key.tq],
                profile_id: key.profile + 1,
                periods: period_metrics,
                worst_wallet_log_growth: worst,
                third_dimension_plateau: false,
                full_three_dimension_plateau: false,
                key: *key,
            })
        })
        .collect();
    let survivor_keys: HashSet<Key> = survivors.iter().map(|item| item.key).collect();
    for survivor in &mut survivors {
        survivor.third_dimension_plateau = adjacent(survivor.key, 2)
            .iter()
            .any(|key| survivor_keys.contains(key));
        survivor.full_three_dimension_plateau = [0, 1, 2].iter().all(|dimension| {
            adjacent(survivor.key, *dimension)
                .iter()
                .any(|key| survivor_keys.contains(key))
        });
    }
    survivors.sort_by(|a, b| {
        b.full_three_dimension_plateau
            .cmp(&a.full_three_dimension_plateau)
            .then_with(|| b.third_dimension_plateau.cmp(&a.third_dimension_plateau))
            .then_with(|| {
                b.worst_wallet_log_growth
                    .total_cmp(&a.worst_wallet_log_growth)
            })
    });
    let payload = ResultPayload {
        kind: "pumpquant.pumpapi_lifecycle_45s_crossday_targeted_triples",
        version: 1,
        status: "complete",
        development_only: true,
        source_pair_search_trials: manifest.declared_exact_rule_exit_trials,
        parent_economic_near_misses: pair.checkpoints[0].economic_gate_near_misses.len(),
        declared_targeted_triple_exit_trials: keys.len() as u64,
        point_survivor_count: survivors.len(),
        third_dimension_plateau_count: survivors
            .iter()
            .filter(|item| item.third_dimension_plateau)
            .count(),
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
