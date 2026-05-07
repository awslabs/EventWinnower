use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialSourceProcessorInit;
use rand::Rng;
use rand::RngExt;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{thread, time::Duration};

/// Global atomic counter so each KeyGenProcessor instance gets a unique id,
/// ensuring separate key-spaces even across threads.
static KEYGEN_INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// An active key with its remaining event count
struct ActiveKey {
    key: u64,
    remaining: u64,
    /// The original event count assigned to this slot; used to reset remaining on replacement
    lifetime: u64,
    /// true until the first event for this key has been emitted
    new: bool,
}

#[derive(SerialSourceProcessorInit)]
pub struct KeyGenProcessor {
    active_set: Vec<ActiveKey>,
    next_key_id: u64,
    max_count: Option<u64>,
    output_count: u64,
    sleep_ms: Option<u64>,
    batch_size: usize,
    max_events_per_key: u64,
    min_events_per_key: u64,
    alpha: f64,
    output_label: String,
    /// String prefix for keys: "{user_prefix}{instance_id}_"
    key_prefix: String,
    cache: VecDeque<usize>,
    cache_size: usize,
    cache_probability: f64,
    cache_hits: u64,
    /// Pool of persistent keys that appear sparsely but never expire
    persistent_keys: Vec<u64>,
    /// Whether each persistent key has been emitted yet (parallel to persistent_keys)
    persistent_new: Vec<bool>,
    /// Probability [0.0, 1.0] that a given event draws from the persistent pool
    persistent_probability: f64,
    /// Count of events drawn from the persistent pool
    persistent_hits: u64,
    /// Fractional variance applied to slot lifetime on key replacement
    lifetime_variance: f64,
    /// Count of distinct keys actually emitted
    distinct_keys_emitted: u64,
    /// Label for the "new key" boolean field
    new_key_label: String,
    /// Optional label for the "persistent key" boolean field; None = field not emitted
    persistent_label: String,
}

#[derive(Parser)]
/// Generate data key events with power-law distributed event counts per key.
///
/// Shape can be specified via one of (mutually exclusive):
///   --alpha           raw exponent (must be > 1.0)
///   --singleton-pct   fraction of keys with count=1 (0.0-1.0)
///   --median          median events per key
///   --mean            mean events per key
///   --top-pct X:Y     "X% of events from top Y% of keys" (e.g. 80:20)
///
/// If none given, defaults to alpha=1.5.
/// A shape summary is always printed to stderr.
#[command(version, long_about = None, arg_required_else_help(false))]
struct KeyGenArgs {
    /// Size of the active key set
    #[arg(short = 'k', long, default_value_t = 10000)]
    active_keys: usize,

    /// Maximum number of events to generate (unlimited if not set)
    #[arg(short = 'c', long)]
    max_count: Option<u64>,

    /// Power-law exponent directly (higher = more skewed toward low counts)
    #[arg(short = 'a', long, group = "shape")]
    alpha: Option<f64>,

    /// Target fraction of keys that are singletons (count=1), e.g. 0.5 for 50%
    #[arg(long, group = "shape")]
    singleton_pct: Option<f64>,

    /// Target median events per key
    #[arg(long, group = "shape")]
    median: Option<f64>,

    /// Target mean events per key
    #[arg(long, group = "shape")]
    mean: Option<f64>,

    /// Pareto-style skew: "X:Y" means X% of events from top Y% of keys (e.g. 80:20)
    #[arg(long, group = "shape")]
    top_pct: Option<String>,

    /// Maximum number of events a single key can generate
    #[arg(short = 'm', long, default_value_t = 100000)]
    max_events_per_key: u64,

    /// Minimum number of events a single key can generate
    #[arg(long, default_value_t = 1)]
    min_events_per_key: u64,

    /// Sleep between batches in milliseconds
    #[arg(short, long)]
    sleep_ms: Option<u64>,

    /// Number of events per batch
    #[arg(short, long, default_value_t = 1)]
    batch_size: usize,

    /// Output field name for the key
    #[arg(short = 'l', long, default_value = "key")]
    label: String,

    /// Optional prefix for generated key strings
    #[arg(short = 'p', long, default_value = "")]
    prefix: String,

    /// Size of the cache buffer for temporal locality (0 = disabled)
    #[arg(long, default_value_t = 100)]
    cache_size: usize,

    /// Probability [0.0, 1.0] of selecting from cache vs full active set
    #[arg(long, default_value_t = 0.2)]
    cache_probability: f64,

    /// Number of persistent keys that appear sparsely but never expire (0 = disabled)
    #[arg(long, default_value_t = 0)]
    persistent_keys: usize,

    /// Probability [0.0, 1.0] that a given event draws from the persistent key pool
    #[arg(long, default_value_t = 0.001)]
    persistent_probability: f64,

    /// Fractional variance [0.0, 1.0] applied to slot lifetime on key replacement (0 = exact)
    #[arg(long, default_value_t = 0.1)]
    lifetime_variance: f64,

    /// If set, add a boolean field with this name indicating first-time key emission
    #[arg(long, default_value = "is_new")]
    new_key_label: String,

    /// If set, add a boolean field with this name indicating the key came from the persistent pool
    #[arg(long, default_value = "is_persistent")]
    persistent_label: String,
}

// ---------------------------------------------------------------------------
// Power-law analytics on continuous distribution over [min, M]
//
// PDF:  f(x) = e * x^(e-1) / (M^e - min^e),  where e = 1 - alpha
// CDF:  F(x) = (x^e - min^e) / (M^e - min^e)
// Quantile: Q(u) = (u*(M^e - min^e) + min^e)^(1/e)
//
// All metrics below are closed-form (no numerical integration, no MC).
// ---------------------------------------------------------------------------

/// CDF: P(X <= x) on [min, M]
fn cdf(x: f64, alpha: f64, min: f64, m: f64) -> f64 {
    let e = 1.0 - alpha;
    (x.powf(e) - min.powf(e)) / (m.powf(e) - min.powf(e))
}

/// Inverse CDF (quantile function) on [min, M]
fn quantile(u: f64, alpha: f64, min: f64, m: f64) -> f64 {
    let e = 1.0 - alpha;
    (u * (m.powf(e) - min.powf(e)) + min.powf(e)).powf(1.0 / e)
}

/// P(key gets count=min): continuous value rounds to min when in [min, min+0.5)
fn minimum_prob(alpha: f64, min: f64, m: f64) -> f64 {
    cdf(min + 0.5, alpha, min, m)
}

/// Median: quantile at u=0.5
fn dist_median(alpha: f64, min: f64, m: f64) -> f64 {
    quantile(0.5, alpha, min, m)
}

/// Closed-form mean on [min, M]
fn dist_mean(alpha: f64, min: f64, m: f64) -> f64 {
    let e = 1.0 - alpha;
    let denom = m.powf(e) - min.powf(e);
    let ep1 = e + 1.0; // = 2 - alpha
    if ep1.abs() < 1e-12 {
        // alpha ≈ 2
        e * (m.ln() - min.ln()) / denom
    } else {
        e * (m.powf(ep1) - min.powf(ep1)) / (ep1 * denom)
    }
}

/// Partial mean from threshold t to M (unnormalized — this is E[X * 1(X>=t)]).
fn partial_mean_above(t: f64, alpha: f64, min: f64, m: f64) -> f64 {
    let e = 1.0 - alpha;
    let denom = m.powf(e) - min.powf(e);
    let ep1 = e + 1.0;
    if ep1.abs() < 1e-12 {
        e * (m.ln() - t.ln()) / denom
    } else {
        e * (m.powf(ep1) - t.powf(ep1)) / (ep1 * denom)
    }
}

/// Fraction of total events from the top `frac` fraction of keys (by count).
fn top_event_share(alpha: f64, min: f64, m: f64, frac: f64) -> f64 {
    let threshold = quantile(1.0 - frac, alpha, min, m);
    let partial = partial_mean_above(threshold, alpha, min, m);
    let total = dist_mean(alpha, min, m);
    if total <= 0.0 {
        0.0
    } else {
        partial / total
    }
}

/// Print a shape summary to stderr.
fn print_shape_summary(alpha: f64, min: f64, m: f64) {
    let med = dist_median(alpha, min, m);
    let mn = dist_mean(alpha, min, m);
    let minp = minimum_prob(alpha, min, m);
    let t1 = top_event_share(alpha, min, m, 0.01);
    let t10 = top_event_share(alpha, min, m, 0.10);
    let t20 = top_event_share(alpha, min, m, 0.20);
    eprintln!("keygen shape: alpha={alpha:.4}  min={min:.0}  max={m:.0}");
    eprintln!("  median={med:.1}  mean={mn:.1}  minimum_pct={minp:.3}");
    eprintln!(
        "  top 1%→{:.0}% of events  top 10%→{:.0}%  top 20%→{:.0}%",
        t1 * 100.0,
        t10 * 100.0,
        t20 * 100.0
    );
}

// ---------------------------------------------------------------------------
// Solver: binary search for alpha given a target metric value
// ---------------------------------------------------------------------------

fn solve_alpha<F>(
    target: f64,
    metric_fn: F,
    increasing: bool,
    metric_name: &str,
) -> Result<f64, anyhow::Error>
where
    F: Fn(f64) -> f64,
{
    let lo_bound = 1.00001_f64;
    let hi_bound = 200.0_f64;

    let val_lo = metric_fn(lo_bound);
    let val_hi = metric_fn(hi_bound);
    let (range_lo, range_hi) = if increasing { (val_lo, val_hi) } else { (val_hi, val_lo) };

    if target < range_lo || target > range_hi {
        return Err(anyhow::anyhow!(
            "{metric_name}={target:.4} is not achievable (range [{range_lo:.4}, {range_hi:.4}] \
             with current -m). Try adjusting --max-events-per-key."
        ));
    }

    let mut lo = lo_bound;
    let mut hi = hi_bound;
    for _ in 0..200 {
        let mid = (lo + hi) / 2.0;
        let val = metric_fn(mid);
        let go_higher = if increasing { val < target } else { val > target };
        if go_higher {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Ok((lo + hi) / 2.0)
}

/// Solver for --top-pct. The top_event_share(alpha, m) function is non-monotonic
/// in alpha (peaks at intermediate alpha), so we:
/// 1. Find the peak alpha via ternary search
/// 2. If the target is below the floor (alpha→1 value), find a smaller -m where
///    the floor is low enough, then search normally
/// 3. If the target is above the peak, find a larger -m where the peak is high
///    enough, then search normally
/// 4. Otherwise binary search on the left (heavy-tail) side of the peak
///
/// This means --top-pct "just works" for common targets like 80:20 — the
/// processor auto-tunes -m if needed and reports what it chose.
fn solve_alpha_top_pct(
    target_event_frac: f64,
    key_frac: f64,
    min: f64,
    m: &mut f64,
) -> Result<f64, anyhow::Error> {
    let lo_bound = 1.00001_f64;

    // Helper: find peak of top_event_share for a given max
    let find_peak = |max: f64| -> (f64, f64) {
        let mut tlo = lo_bound;
        let mut thi = 10.0_f64;
        for _ in 0..200 {
            let m1 = tlo + (thi - tlo) / 3.0;
            let m2 = thi - (thi - tlo) / 3.0;
            if top_event_share(m1, min, max, key_frac) < top_event_share(m2, min, max, key_frac) {
                tlo = m1;
            } else {
                thi = m2;
            }
        }
        let pa = (tlo + thi) / 2.0;
        (pa, top_event_share(pa, min, max, key_frac))
    };

    let floor_val = top_event_share(lo_bound, min, *m, key_frac);
    let (_peak_alpha, peak_val) = find_peak(*m);

    if target_event_frac > peak_val {
        // Need a larger max to get more concentration at the peak.
        // Binary search for the right max value.
        let mut m_lo = *m;
        let mut m_hi = *m * 1e10; // generous upper bound
        for _ in 0..100 {
            let m_mid = (m_lo * m_hi).sqrt(); // geometric midpoint
            let (_, pv) = find_peak(m_mid);
            if pv < target_event_frac {
                m_lo = m_mid;
            } else {
                m_hi = m_mid;
            }
        }
        *m = (m_lo * m_hi).sqrt();
        eprintln!("keygen: auto-adjusted max_events_per_key to {:.0} for top-pct target", *m);
    } else if target_event_frac < floor_val {
        // Need a smaller max so the floor (at alpha→1) is below the target.
        let mut m_lo = 2.0_f64;
        let mut m_hi = *m;
        for _ in 0..100 {
            let m_mid = (m_lo * m_hi).sqrt();
            let fv = top_event_share(lo_bound, min, m_mid, key_frac);
            if fv > target_event_frac {
                m_hi = m_mid;
            } else {
                m_lo = m_mid;
            }
        }
        *m = (m_lo * m_hi).sqrt();
        eprintln!("keygen: auto-adjusted max_events_per_key to {:.0} for top-pct target", *m);
    }

    // Now search on the left (increasing) side of the peak
    let (peak_alpha, _) = find_peak(*m);
    let mut lo = lo_bound;
    let mut hi = peak_alpha;
    for _ in 0..200 {
        let mid = (lo + hi) / 2.0;
        let val = top_event_share(mid, min, *m, key_frac);
        if val < target_event_frac {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Ok((lo + hi) / 2.0)
}

fn parse_top_pct(s: &str) -> Result<(f64, f64), anyhow::Error> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("--top-pct format is X:Y (e.g. 80:20)"));
    }
    let event_pct: f64 = parts[0].parse().map_err(|_| anyhow::anyhow!("bad X in X:Y"))?;
    let key_pct: f64 = parts[1].parse().map_err(|_| anyhow::anyhow!("bad Y in X:Y"))?;
    if !(0.0..=100.0).contains(&event_pct) || !(0.0..=100.0).contains(&key_pct) {
        return Err(anyhow::anyhow!("X and Y must be between 0 and 100"));
    }
    Ok((event_pct / 100.0, key_pct / 100.0))
}

/// Sample from a power-law distribution on [min, max] with exponent alpha.
fn power_law_sample(rng: &mut impl Rng, alpha: f64, min: u64, max: u64) -> u64 {
    let u: f64 = rng.random();
    let x = quantile(u, alpha, min as f64, max as f64);
    (x.round() as u64).clamp(min, max)
}

impl KeyGenProcessor {
    /// Create a brand-new active key with a fresh power-law lifetime (used at init)
    fn new_active_key(&mut self) -> ActiveKey {
        let key = self.next_key_id;
        self.next_key_id += 1;
        let lifetime = power_law_sample(
            &mut rand::rng(),
            self.alpha,
            self.min_events_per_key,
            self.max_events_per_key,
        );
        ActiveKey { key, remaining: lifetime, lifetime, new: true }
    }
}

/// Replace an expired key in a slot, preserving the slot's original lifetime
/// with optional proportional variance. Free function to avoid borrow conflicts.
fn replace_slot(
    slot: &mut ActiveKey,
    next_key_id: &mut u64,
    lifetime_variance: f64,
    min_events_per_key: u64,
    max_events_per_key: u64,
) {
    slot.key = *next_key_id;
    *next_key_id += 1;
    if lifetime_variance > 0.0 {
        let lo = (slot.lifetime as f64 * (1.0 - lifetime_variance)).max(min_events_per_key as f64);
        let hi = (slot.lifetime as f64 * (1.0 + lifetime_variance)).min(max_events_per_key as f64);
        let jittered = rand::rng().random_range(lo..=hi);
        slot.remaining = (jittered.round() as u64).clamp(min_events_per_key, max_events_per_key);
    } else {
        slot.remaining = slot.lifetime;
    }
    slot.new = true;
}

impl SerialSourceProcessor for KeyGenProcessor {
    fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        if let Some(sleep_ms) = self.sleep_ms {
            thread::sleep(Duration::from_millis(sleep_ms));
        }
        if let Some(max) = self.max_count {
            if self.output_count >= max {
                return Ok(vec![]);
            }
        }

        let mut events = Vec::new();
        let mut rng = rand::rng();

        for _ in 0..self.batch_size {
            if let Some(max) = self.max_count {
                if self.output_count >= max {
                    break;
                }
            }

            // Check if this event should draw from the persistent key pool
            let (key_value, is_new, is_persistent) = if !self.persistent_keys.is_empty()
                && rng.random::<f64>() < self.persistent_probability
            {
                let pidx = rng.random_range(0..self.persistent_keys.len());
                self.persistent_hits += 1;
                let was_new = self.persistent_new[pidx];
                self.persistent_new[pidx] = false;
                (self.persistent_keys[pidx], was_new, true)
            } else {
                let idx = if !self.cache.is_empty()
                    && self.cache_size > 0
                    && rng.random::<f64>() < self.cache_probability
                {
                    let cache_pos = rng.random_range(0..self.cache.len());
                    let cached_idx = self.cache[cache_pos];
                    if cached_idx < self.active_set.len()
                        && self.active_set[cached_idx].remaining > 0
                    {
                        self.cache_hits += 1;
                        cached_idx
                    } else {
                        self.cache.remove(cache_pos);
                        rng.random_range(0..self.active_set.len())
                    }
                } else {
                    rng.random_range(0..self.active_set.len())
                };

                let kv = self.active_set[idx].key;
                let was_new = self.active_set[idx].new;
                self.active_set[idx].new = false;
                self.active_set[idx].remaining -= 1;
                if self.active_set[idx].remaining == 0 {
                    replace_slot(
                        &mut self.active_set[idx],
                        &mut self.next_key_id,
                        self.lifetime_variance,
                        self.min_events_per_key,
                        self.max_events_per_key,
                    );
                    self.cache.retain(|&ci| ci != idx);
                } else if self.cache_size > 0 {
                    if self.cache.len() >= self.cache_size {
                        self.cache.pop_front();
                    }
                    self.cache.push_back(idx);
                }
                (kv, was_new, false)
            };

            let mut output = serde_json::Map::new();
            output.insert(
                self.output_label.clone(),
                serde_json::Value::String(format!("{}{}", self.key_prefix, key_value)),
            );
            output.insert(self.new_key_label.clone(), serde_json::Value::Bool(is_new));
            if is_new {
                self.distinct_keys_emitted += 1;
            }
            output.insert(self.persistent_label.clone(), serde_json::Value::Bool(is_persistent));
            events.push(Rc::new(RefCell::new(serde_json::Value::Object(output))));
            self.output_count += 1;
        }
        Ok(events)
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("generate data key events with power-law distributed frequency".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = KeyGenArgs::try_parse_from(argv)?;

        if args.active_keys == 0 {
            return Err(anyhow::anyhow!("active_keys must be > 0"));
        }
        if args.max_events_per_key == 0 {
            return Err(anyhow::anyhow!("max_events_per_key must be > 0"));
        }
        if args.min_events_per_key == 0 {
            return Err(anyhow::anyhow!("min_events_per_key must be > 0"));
        }
        if args.min_events_per_key > args.max_events_per_key {
            return Err(anyhow::anyhow!("min_events_per_key must be <= max_events_per_key"));
        }
        if !(0.0..=1.0).contains(&args.cache_probability) {
            return Err(anyhow::anyhow!("cache_probability must be between 0.0 and 1.0"));
        }
        if !(0.0..=1.0).contains(&args.persistent_probability) {
            return Err(anyhow::anyhow!("persistent_probability must be between 0.0 and 1.0"));
        }
        if !(0.0..=1.0).contains(&args.lifetime_variance) {
            return Err(anyhow::anyhow!("lifetime_variance must be between 0.0 and 1.0"));
        }

        let mut m = args.max_events_per_key as f64;
        let min = args.min_events_per_key as f64;

        let alpha = if let Some(a) = args.alpha {
            if a <= 1.0 {
                return Err(anyhow::anyhow!("alpha must be > 1.0"));
            }
            a
        } else if let Some(pct) = args.singleton_pct {
            if !(0.0..=1.0).contains(&pct) {
                return Err(anyhow::anyhow!("singleton_pct must be between 0.0 and 1.0"));
            }
            solve_alpha(pct, |a| minimum_prob(a, min, m), true, "singleton_pct")?
        } else if let Some(med) = args.median {
            if med < min || med > m {
                return Err(anyhow::anyhow!(
                    "median must be between min_events_per_key and max_events_per_key"
                ));
            }
            solve_alpha(med, |a| dist_median(a, min, m), false, "median")?
        } else if let Some(mn) = args.mean {
            if mn < min || mn > m {
                return Err(anyhow::anyhow!(
                    "mean must be between min_events_per_key and max_events_per_key"
                ));
            }
            solve_alpha(mn, |a| dist_mean(a, min, m), false, "mean")?
        } else if let Some(ref spec) = args.top_pct {
            let (event_frac, key_frac) = parse_top_pct(spec)?;
            solve_alpha_top_pct(event_frac, key_frac, min, &mut m)?
        } else {
            1.5
        };

        let max_events_per_key = m.round() as u64;
        let min_events_per_key = args.min_events_per_key;
        print_shape_summary(alpha, min, m);

        let instance_id = KEYGEN_INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let key_prefix = format!("{}{}.", args.prefix, instance_id);

        let mut processor = KeyGenProcessor {
            active_set: Vec::with_capacity(args.active_keys),
            next_key_id: 0,
            max_count: args.max_count,
            output_count: 0,
            sleep_ms: args.sleep_ms,
            batch_size: args.batch_size,
            max_events_per_key,
            min_events_per_key,
            alpha,
            output_label: args.label,
            key_prefix,
            cache: VecDeque::with_capacity(args.cache_size),
            cache_size: args.cache_size,
            cache_probability: args.cache_probability,
            cache_hits: 0,
            persistent_keys: Vec::new(),
            persistent_new: Vec::new(),
            persistent_probability: args.persistent_probability,
            persistent_hits: 0,
            lifetime_variance: args.lifetime_variance,
            distinct_keys_emitted: 0,
            new_key_label: args.new_key_label,
            persistent_label: args.persistent_label,
        };

        // Allocate persistent keys first — they consume key IDs from the same
        // namespace but never expire or rotate out of the active set.
        for _ in 0..args.persistent_keys {
            let kid = processor.next_key_id;
            processor.next_key_id += 1;
            processor.persistent_keys.push(kid);
            processor.persistent_new.push(true);
        }

        if args.persistent_keys > 0 {
            eprintln!(
                "keygen: {} persistent keys (probability {:.4})",
                args.persistent_keys, args.persistent_probability
            );
        }

        for _ in 0..args.active_keys {
            let key = processor.new_active_key();
            processor.active_set.push(key);
        }

        Ok(processor)
    }

    fn stats(&self) -> Option<String> {
        let distinct_pct = if self.output_count > 0 {
            (self.distinct_keys_emitted as f64 / self.output_count as f64) * 100.0
        } else {
            0.0
        };
        Some(format!(
            "output:{}\ndistinct_keys:{}\ndistinct_pct:{:.2}%\nnext_key:{}\nactive_keys:{}\ncache_hits:{}\npersistent_keys:{}\npersistent_hits:{}",
            self.output_count,
            self.distinct_keys_emitted,
            distinct_pct,
            self.next_key_id,
            self.active_set.len(),
            self.cache_hits,
            self.persistent_keys.len(),
            self.persistent_hits,
        ))
    }
}
