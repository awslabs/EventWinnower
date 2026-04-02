use crate::processors::processor::*;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::num::NonZeroUsize;

const CACHE_SIZE: usize = 200000;

enum SamplingMode {
    Random {
        rate: f64,
        rng: StdRng,
    },
    FirstN {
        count: usize,
        emitted: usize,
    },
    Reservoir {
        size: usize,
        reservoir: Vec<Event>,
        rng: StdRng,
    },
    PerKey {
        count: usize,
        key_path: jmespath::Expression<'static>,
        counts: LruCache<String, usize>,
    },
}

#[derive(Parser)]
/// Statistical sampling (random, first-N, reservoir, per-key)
#[command(
    version,
    long_about = None,
    arg_required_else_help(true),
    after_help = r#"MODES (exactly one required):

  -r, --rate R       Random sampling. Each event is included independently with
                     probability R (0.0 to 1.0). Rate 0.0 drops everything;
                     rate 1.0 passes everything through.

  -n, --count N      First-N sampling. Emit the first N events, then drop the
                     rest. Combine with -k for per-key sampling instead.

  --reservoir N      Reservoir sampling (Algorithm R). Collect a uniform random
                     sample of exactly N events from a stream of unknown size.
                     Events are buffered and only output on flush.

  -n N -k EXPR       Per-key sampling. Emit the first N events for each unique
                     key value extracted by the JMESPath expression. Keys are
                     tracked in an LRU cache (see --cache-size).

EXAMPLES:

  10% random sample:
    sample -r 0.1

  Keep all events (passthrough):
    sample -r 1.0

  First 1000 events only:
    sample -n 1000

  Reservoir sample of 500 from an unbounded stream:
    sample --reservoir 500

  First 5 events per account:
    sample -k account -n 5

  Deterministic random sample (reproducible with seed):
    sample -r 0.1 --seed 42

  Per-key with larger cache for high-cardinality keys:
    sample -k user_id -n 3 --cache-size 500000

INPUT:
  {"account": "A", "score": 0.9}
  {"account": "A", "score": 0.8}
  {"account": "B", "score": 0.7}

OUTPUT (with -k account -n 1):
  {"account": "A", "score": 0.9}
  {"account": "B", "score": 0.7}
"#,
)]
struct SampleProcessorArgs {
    /// Probability of including each event (0.0 to 1.0). Selects random sampling mode
    #[arg(short = 'r', long, value_name = "RATE")]
    rate: Option<f64>,

    /// Number of events to emit. First-N mode without -k, per-key mode with -k
    #[arg(short = 'n', long, value_name = "N")]
    count: Option<usize>,

    /// Reservoir size for Algorithm R uniform random sampling. Output on flush only
    #[arg(long, value_name = "N")]
    reservoir: Option<usize>,

    /// JMESPath expression to extract the grouping key for per-key sampling
    #[arg(short = 'k', long, value_name = "EXPR")]
    key: Option<String>,

    /// Seed for the random number generator (enables deterministic, reproducible results)
    #[arg(long, value_name = "SEED")]
    seed: Option<u64>,

    /// LRU cache capacity for per-key sampling (limits memory for high-cardinality keys)
    #[arg(short = 'c', long, default_value_t = CACHE_SIZE, value_name = "N")]
    cache_size: usize,
}

#[derive(SerialProcessorInit)]
pub struct SampleProcessor {
    mode: SamplingMode,
    input_count: u64,
    output_count: u64,
}

impl SerialProcessor for SampleProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("statistical sampling (random, first-N, reservoir, per-key)".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = SampleProcessorArgs::try_parse_from(argv)?;
        let mode = determine_mode(&args)?;
        Ok(SampleProcessor { mode, input_count: 0, output_count: 0 })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;
        match &mut self.mode {
            SamplingMode::Random { rate, rng } => {
                let r: f64 = rng.random();
                if r < *rate {
                    self.output_count += 1;
                    Ok(vec![input])
                } else {
                    Ok(vec![])
                }
            }
            SamplingMode::FirstN { count, emitted } => {
                if *emitted < *count {
                    *emitted += 1;
                    self.output_count += 1;
                    Ok(vec![input])
                } else {
                    Ok(vec![])
                }
            }
            SamplingMode::Reservoir { size, reservoir, rng } => {
                let i = self.input_count as usize;
                if i <= *size {
                    reservoir.push(input);
                } else {
                    let j = rng.random_range(0..i);
                    if j < *size {
                        reservoir[j] = input;
                    }
                }
                Ok(vec![])
            }
            SamplingMode::PerKey { count, key_path, counts } => {
                let result = key_path.search(&input)?;
                if result.is_null() {
                    return Ok(vec![]);
                }
                let key_str = match result.as_ref() {
                    jmespath::Variable::String(s) => s.clone(),
                    other => format!("{}", other),
                };

                let current = counts.get_or_insert_mut(key_str, || 0);
                if *current < *count {
                    *current += 1;
                    self.output_count += 1;
                    Ok(vec![input])
                } else {
                    Ok(vec![])
                }
            }
        }
    }

    fn flush(&mut self) -> Vec<Event> {
        if let SamplingMode::Reservoir { reservoir, .. } = &mut self.mode {
            self.output_count += reservoir.len() as u64;
            std::mem::take(reservoir)
        } else {
            vec![]
        }
    }

    fn stats(&self) -> Option<String> {
        let rate = if self.input_count > 0 {
            self.output_count as f64 / self.input_count as f64
        } else {
            0.0
        };
        Some(format!(
            "input: {}, sampled: {}, rate: {:.4}",
            self.input_count, self.output_count, rate
        ))
    }
}

fn create_rng(seed: Option<u64>) -> StdRng {
    match seed {
        Some(s) => StdRng::seed_from_u64(s),
        None => StdRng::from_rng(&mut rand::rng()),
    }
}

fn determine_mode(args: &SampleProcessorArgs) -> Result<SamplingMode, anyhow::Error> {
    match (&args.rate, &args.count, &args.reservoir, &args.key) {
        (Some(rate), None, None, None) => {
            if *rate < 0.0 || *rate > 1.0 {
                return Err(anyhow::anyhow!("Rate must be between 0.0 and 1.0, got {}", rate));
            }
            Ok(SamplingMode::Random { rate: *rate, rng: create_rng(args.seed) })
        }
        (None, Some(count), None, None) => {
            if *count == 0 {
                return Err(anyhow::anyhow!("Count must be positive, got 0"));
            }
            Ok(SamplingMode::FirstN { count: *count, emitted: 0 })
        }
        (None, None, Some(size), None) => {
            if *size == 0 {
                return Err(anyhow::anyhow!("Reservoir size must be positive, got 0"));
            }
            Ok(SamplingMode::Reservoir {
                size: *size,
                reservoir: Vec::with_capacity(*size),
                rng: create_rng(args.seed),
            })
        }
        (None, Some(count), None, Some(key)) => {
            if *count == 0 {
                return Err(anyhow::anyhow!("Count must be positive, got 0"));
            }
            let key_path = jmespath::compile(key)?;
            Ok(SamplingMode::PerKey {
                count: *count,
                key_path,
                counts: LruCache::new(
                    NonZeroUsize::new(args.cache_size).expect("non-zero cache size"),
                ),
            })
        }
        (None, None, None, None) => {
            Err(anyhow::anyhow!("No sampling mode specified. Use -r, -n, or --reservoir"))
        }
        _ => Err(anyhow::anyhow!("Conflicting options. Specify only one sampling mode")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processors::processor::event_new;
    use serde_json::json;

    fn make_event(i: usize) -> Event {
        event_new(json!({"index": i}))
    }

    // ─── Random Sampling Tests ──────────────────────────────────────────────

    #[test]
    fn test_random_rate_zero_drops_all() {
        let args = vec!["sample".into(), "-r".into(), "0.0".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();
        for i in 0..100 {
            let result = proc.process(make_event(i)).unwrap();
            assert!(result.is_empty());
        }
        assert_eq!(proc.output_count, 0);
    }

    #[test]
    fn test_random_rate_one_includes_all() {
        let args = vec!["sample".into(), "-r".into(), "1.0".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();
        for i in 0..100 {
            let result = proc.process(make_event(i)).unwrap();
            assert_eq!(result.len(), 1);
        }
        assert_eq!(proc.output_count, 100);
    }

    #[test]
    fn test_random_rate_half_with_seed_deterministic() {
        let args = vec!["sample".into(), "-r".into(), "0.5".into(), "--seed".into(), "42".into()];
        let mut proc1 = SampleProcessor::new(&args).unwrap();
        let mut proc2 = SampleProcessor::new(&args).unwrap();

        for i in 0..200 {
            let r1 = proc1.process(make_event(i)).unwrap().len();
            let r2 = proc2.process(make_event(i)).unwrap().len();
            assert_eq!(r1, r2);
        }
    }

    #[test]
    fn test_random_rate_negative_rejected() {
        let args = vec!["sample".into(), "-r".into(), "-0.1".into()];
        assert!(SampleProcessor::new(&args).is_err());
    }

    #[test]
    fn test_random_rate_above_one_rejected() {
        let args = vec!["sample".into(), "-r".into(), "1.1".into()];
        assert!(SampleProcessor::new(&args).is_err());
    }

    // ─── First-N Sampling Tests ─────────────────────────────────────────────

    #[test]
    fn test_first_n_emits_first_n_then_drops() {
        let args = vec!["sample".into(), "-n".into(), "5".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();
        for i in 0..10 {
            let result = proc.process(make_event(i)).unwrap();
            if i < 5 {
                assert_eq!(result.len(), 1);
            } else {
                assert!(result.is_empty());
            }
        }
        assert_eq!(proc.output_count, 5);
    }

    #[test]
    fn test_first_n_fewer_than_n_all_emitted() {
        let args = vec!["sample".into(), "-n".into(), "100".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();
        for i in 0..10 {
            let result = proc.process(make_event(i)).unwrap();
            assert_eq!(result.len(), 1);
        }
        assert_eq!(proc.output_count, 10);
    }

    #[test]
    fn test_first_n_count_one() {
        let args = vec!["sample".into(), "-n".into(), "1".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();
        let r0 = proc.process(make_event(0)).unwrap();
        assert_eq!(r0.len(), 1);
        let r1 = proc.process(make_event(1)).unwrap();
        assert!(r1.is_empty());
    }

    #[test]
    fn test_first_n_count_zero_rejected() {
        let args = vec!["sample".into(), "-n".into(), "0".into()];
        assert!(SampleProcessor::new(&args).is_err());
    }

    // ─── Reservoir Sampling Tests ───────────────────────────────────────────

    #[test]
    fn test_reservoir_process_always_returns_empty() {
        let args = vec!["sample".into(), "--reservoir".into(), "5".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();
        for i in 0..20 {
            let result = proc.process(make_event(i)).unwrap();
            assert!(result.is_empty());
        }
    }

    #[test]
    fn test_reservoir_flush_returns_reservoir_events() {
        let args =
            vec!["sample".into(), "--reservoir".into(), "5".into(), "--seed".into(), "42".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();
        for i in 0..20 {
            proc.process(make_event(i)).unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 5);
    }

    #[test]
    fn test_reservoir_fewer_than_n_returns_all() {
        let args = vec!["sample".into(), "--reservoir".into(), "10".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();
        for i in 0..3 {
            proc.process(make_event(i)).unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 3);
    }

    #[test]
    fn test_reservoir_fills_with_first_n() {
        let args = vec!["sample".into(), "--reservoir".into(), "5".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();
        for i in 0..5 {
            proc.process(make_event(i)).unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 5);
        // Verify they are the first 5 events in order
        for (idx, ev) in flushed.iter().enumerate() {
            let val = ev.borrow();
            assert_eq!(val["index"], idx);
        }
    }

    #[test]
    fn test_reservoir_seed_deterministic() {
        let args =
            vec!["sample".into(), "--reservoir".into(), "5".into(), "--seed".into(), "99".into()];
        let mut proc1 = SampleProcessor::new(&args).unwrap();
        let mut proc2 = SampleProcessor::new(&args).unwrap();
        for i in 0..50 {
            proc1.process(make_event(i)).unwrap();
            proc2.process(make_event(i)).unwrap();
        }
        let f1 = proc1.flush();
        let f2 = proc2.flush();
        assert_eq!(f1.len(), f2.len());
        for (a, b) in f1.iter().zip(f2.iter()) {
            assert_eq!(*a.borrow(), *b.borrow());
        }
    }

    // ─── Per-Key Sampling Tests ─────────────────────────────────────────────

    #[test]
    fn test_per_key_first_n_per_key_emitted() {
        let args = vec!["sample".into(), "-n".into(), "2".into(), "-k".into(), "account".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();

        // 5 events for same key, only first 2 should pass
        for i in 0..5 {
            let ev = event_new(json!({"account": "acct_a", "index": i}));
            let result = proc.process(ev).unwrap();
            if i < 2 {
                assert_eq!(result.len(), 1);
            } else {
                assert!(result.is_empty());
            }
        }
        assert_eq!(proc.output_count, 2);
    }

    #[test]
    fn test_per_key_different_keys_independent() {
        let args = vec!["sample".into(), "-n".into(), "1".into(), "-k".into(), "account".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();

        let ev_a = event_new(json!({"account": "a"}));
        let ev_b = event_new(json!({"account": "b"}));
        let ev_a2 = event_new(json!({"account": "a"}));

        assert_eq!(proc.process(ev_a).unwrap().len(), 1);
        assert_eq!(proc.process(ev_b).unwrap().len(), 1);
        assert!(proc.process(ev_a2).unwrap().is_empty()); // a already at limit
        assert_eq!(proc.output_count, 2);
    }

    #[test]
    fn test_per_key_null_key_skipped() {
        let args =
            vec!["sample".into(), "-n".into(), "5".into(), "-k".into(), "missing_field".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();

        let ev = event_new(json!({"other": "value"}));
        let result = proc.process(ev).unwrap();
        assert!(result.is_empty());
        assert_eq!(proc.output_count, 0);
    }

    // ─── Mode Selection Tests ───────────────────────────────────────────────

    #[test]
    fn test_mode_rate_selects_random() {
        let args = vec!["sample".into(), "-r".into(), "0.5".into()];
        let proc = SampleProcessor::new(&args).unwrap();
        assert!(matches!(proc.mode, SamplingMode::Random { .. }));
    }

    #[test]
    fn test_mode_count_selects_first_n() {
        let args = vec!["sample".into(), "-n".into(), "10".into()];
        let proc = SampleProcessor::new(&args).unwrap();
        assert!(matches!(proc.mode, SamplingMode::FirstN { .. }));
    }

    #[test]
    fn test_mode_reservoir_selects_reservoir() {
        let args = vec!["sample".into(), "--reservoir".into(), "10".into()];
        let proc = SampleProcessor::new(&args).unwrap();
        assert!(matches!(proc.mode, SamplingMode::Reservoir { .. }));
    }

    #[test]
    fn test_mode_count_with_key_selects_per_key() {
        let args = vec!["sample".into(), "-n".into(), "5".into(), "-k".into(), "account".into()];
        let proc = SampleProcessor::new(&args).unwrap();
        assert!(matches!(proc.mode, SamplingMode::PerKey { .. }));
    }

    #[test]
    fn test_mode_conflicting_rate_and_reservoir_rejected() {
        let args =
            vec!["sample".into(), "-r".into(), "0.5".into(), "--reservoir".into(), "10".into()];
        assert!(SampleProcessor::new(&args).is_err());
    }

    #[test]
    fn test_mode_no_options_rejected() {
        let args = vec!["sample".into()];
        assert!(SampleProcessor::new(&args).is_err());
    }

    // ─── Stats Tests ────────────────────────────────────────────────────────

    #[test]
    fn test_stats_input_count_accurate() {
        let args = vec!["sample".into(), "-r".into(), "1.0".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();
        for i in 0..25 {
            proc.process(make_event(i)).unwrap();
        }
        let stats = proc.stats().unwrap();
        assert!(stats.contains("input: 25"));
    }

    #[test]
    fn test_stats_output_count_accurate() {
        let args = vec!["sample".into(), "-n".into(), "3".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();
        for i in 0..10 {
            proc.process(make_event(i)).unwrap();
        }
        let stats = proc.stats().unwrap();
        assert!(stats.contains("sampled: 3"));
    }

    #[test]
    fn test_stats_rate_calculation() {
        let args = vec!["sample".into(), "-n".into(), "5".into()];
        let mut proc = SampleProcessor::new(&args).unwrap();
        for i in 0..10 {
            proc.process(make_event(i)).unwrap();
        }
        let stats = proc.stats().unwrap();
        // 5/10 = 0.5
        assert!(stats.contains("rate: 0.5"));
    }
}
