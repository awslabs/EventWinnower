use crate::processors::processor::*;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;

const CACHE_SIZE: usize = 200000;

#[derive(Parser)]
#[command(
    version,
    about = "Identify co-occurring field values for a given key and output correlation statistics",
    long_about = r#"Identify co-occurring field values for a given key and output correlation statistics

The keycorrelate processor finds which field values tend to appear together
for the same key. It is useful for discovering patterns like "accounts with
high cyber-attack scores also tend to have high deception scores."

HOW IT WORKS:
  1. Events stream in. For each event, the processor extracts a key (e.g.
     account) and a correlation value (e.g. policy name or score label).
  2. Optionally, a threshold filters out low-scoring records before they
     enter correlation analysis.
  3. Values are collected per key in an LRU cache (bounded memory).
  4. On flush, the processor generates all unordered pairs of values that
     co-occur for the same key, counts how many keys share each pair, and
     outputs the top correlated pairs with support statistics.

THRESHOLD FILTERING:
  When --threshold is set, only records meeting the score threshold are
  included. The score is taken from --score (explicit JMESPath) or parsed
  from the correlation field itself if numeric. Records below threshold
  are silently dropped.

OUTPUT FORMAT (one event per qualifying pair):
  {
    "pair": ["value_a", "value_b"],     // alphabetically ordered
    "co_occurrence_count": 5,           // keys where both appear
    "support": 0.83,                    // co_occurrence / total_keys
    "keys": ["key1", "key2", ...]       // which keys had both
  }

EXAMPLES:
  # Find which policies co-occur per account
  ... | flatten evals -k "{account:account}" \
      | keycorrelate policy -k account

  # Only correlate high-scoring policies (score >= 0.5)
  ... | flatten evals -k "{account:account}" \
      | keycorrelate policy -k account -s score -t 0.5

  # Top 20 pairs with at least 3 co-occurrences
  ... | keycorrelate policy -k account -n 20 --min-support 3

  # Use correlation field value itself as the score for threshold
  ... | keycorrelate score_value -k account -t 0.7

ALIASES: keycorrelate, key_correlate, correlate
"#,
    arg_required_else_help(true)
)]
struct KeyCorrelateArgs {
    /// JMESPath expression for the correlation field to analyze
    #[arg(required(true), value_name = "JMESPATH")]
    correlation_field: Vec<String>,

    /// JMESPath expression for the grouping key (e.g. account, user_id)
    #[arg(short, long, required(true), value_name = "JMESPATH")]
    key: String,

    /// Minimum score to include a record in correlation analysis
    #[arg(short, long, value_name = "FLOAT")]
    threshold: Option<f64>,

    /// Maximum number of correlated pairs to output
    #[arg(short = 'n', long, default_value_t = 10, value_name = "COUNT")]
    top: usize,

    /// Minimum co-occurrence count for a pair to appear in output
    #[arg(long, default_value_t = 2, value_name = "COUNT")]
    min_support: usize,

    /// JMESPath expression for an explicit score field (used with --threshold)
    #[arg(short, long, value_name = "JMESPATH")]
    score: Option<String>,

    /// LRU cache capacity for tracking keys (bounds memory usage)
    #[arg(short, long, default_value_t = CACHE_SIZE, value_name = "SIZE")]
    cache_size: usize,
}

#[derive(SerialProcessorInit)]
#[allow(dead_code)]
pub struct KeyCorrelateProcessor<'a> {
    correlation_path: jmespath::Expression<'a>,
    key_path: jmespath::Expression<'a>,
    score_path: Option<jmespath::Expression<'a>>,
    threshold: Option<f64>,
    top_n: usize,
    min_support: usize,
    state: LruCache<String, HashSet<String>>,
    input_count: u64,
    passed_threshold_count: u64,
    output_count: u64,
}

fn generate_all_pairs(values: &HashSet<String>) -> Vec<(String, String)> {
    let mut values_vec: Vec<&String> = values.iter().collect();
    values_vec.sort();
    let mut pairs = Vec::new();
    for i in 0..values_vec.len() {
        for j in (i + 1)..values_vec.len() {
            pairs.push((values_vec[i].clone(), values_vec[j].clone()));
        }
    }
    pairs
}

struct PairStats {
    co_occurrence_count: usize,
    keys: Vec<String>,
}

impl SerialProcessor for KeyCorrelateProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("identify co-occurring field values for a given key".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = KeyCorrelateArgs::try_parse_from(argv)?;
        let correlation_path = jmespath::compile(&args.correlation_field.join(" "))?;
        let key_path = jmespath::compile(&args.key)?;

        let score_path = match args.score {
            Some(ref score) => Some(jmespath::compile(score)?),
            None => None,
        };

        Ok(KeyCorrelateProcessor {
            correlation_path,
            key_path,
            score_path,
            threshold: args.threshold,
            top_n: args.top,
            min_support: args.min_support,
            state: LruCache::new(NonZeroUsize::new(args.cache_size).expect("non-zero cache size")),
            input_count: 0,
            passed_threshold_count: 0,
            output_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        // Extract key using key_path JMESPath
        let key = self.key_path.search(&input)?;
        if key.is_null() {
            return Ok(vec![]);
        }

        // Extract correlation field using correlation_path JMESPath
        let correlation_value = self.correlation_path.search(&input)?;
        if correlation_value.is_null() {
            return Ok(vec![]);
        }

        // Apply threshold filtering if configured
        if let Some(threshold) = self.threshold {
            let score = if let Some(ref score_path) = self.score_path {
                // Use explicit score path
                let score_result = score_path.search(&input)?;
                match score_result.as_number() {
                    Some(v) => Some(v),
                    None => {
                        if score_result.is_string() {
                            score_result.as_string().and_then(|s| s.parse::<f64>().ok())
                        } else {
                            None
                        }
                    }
                }
            } else {
                // Try to parse correlation field value as numeric score
                match correlation_value.as_number() {
                    Some(v) => Some(v),
                    None => {
                        if correlation_value.is_string() {
                            correlation_value.as_string().and_then(|s| s.parse::<f64>().ok())
                        } else {
                            None
                        }
                    }
                }
            };

            match score {
                Some(s) if s >= threshold => {} // passes threshold
                _ => return Ok(vec![]),         // below threshold or non-numeric
            }
        }

        // Convert key and correlation value to strings
        let keystr = if let Some(s) = key.as_string() { s.to_string() } else { key.to_string() };

        let valstr = if let Some(s) = correlation_value.as_string() {
            s.to_string()
        } else {
            correlation_value.to_string()
        };

        // Add correlation value to key's HashSet in LruCache
        if let Some(values) = self.state.get_mut(&keystr) {
            values.insert(valstr);
        } else {
            let mut new_set = HashSet::new();
            new_set.insert(valstr);
            let _evicted = self.state.push(keystr, new_set);
            // No output on eviction - correlation requires all data at flush
        }

        self.passed_threshold_count += 1;
        Ok(vec![])
    }

    fn flush(&mut self) -> Vec<Event> {
        let total_keys = self.state.len();
        if total_keys == 0 {
            return vec![];
        }

        // 7.1: Pair counting and aggregation
        let mut pair_stats: HashMap<(String, String), PairStats> = HashMap::new();

        for (key, values) in self.state.iter() {
            if values.len() < 2 {
                continue;
            }

            let pairs = generate_all_pairs(values);
            for pair in pairs {
                let stats = pair_stats
                    .entry(pair)
                    .or_insert(PairStats { co_occurrence_count: 0, keys: Vec::new() });
                stats.co_occurrence_count += 1;
                stats.keys.push(key.clone());
            }
        }

        // 7.3: Filter by min_support
        let mut filtered: Vec<_> = pair_stats
            .into_iter()
            .filter(|(_, stats)| stats.co_occurrence_count >= self.min_support)
            .collect();

        // 7.3: Sort by co_occurrence_count desc, then support desc for ties
        filtered.sort_by(|a, b| {
            b.1.co_occurrence_count.cmp(&a.1.co_occurrence_count).then_with(|| {
                // 7.2: Support = co_occurrence_count / total_keys
                let support_a = a.1.co_occurrence_count as f64 / total_keys as f64;
                let support_b = b.1.co_occurrence_count as f64 / total_keys as f64;
                support_b.partial_cmp(&support_a).unwrap_or(std::cmp::Ordering::Equal)
            })
        });

        // 7.3: Take top_n pairs
        // 7.4: Create output events
        let results: Vec<Event> = filtered
            .into_iter()
            .take(self.top_n)
            .map(|((a, b), stats)| {
                let support = stats.co_occurrence_count as f64 / total_keys as f64;
                event_new(serde_json::json!({
                    "pair": [a, b],
                    "co_occurrence_count": stats.co_occurrence_count,
                    "support": support,
                    "keys": stats.keys,
                }))
            })
            .collect();

        // 7.4: Update output_count and clear cache
        self.output_count += results.len() as u64;
        self.state.clear();

        results
    }

    fn stats(&self) -> Option<String> {
        Some(format!(
            "input:{}\npassed_threshold:{}\nunique_keys:{}\noutput:{}",
            self.input_count,
            self.passed_threshold_count,
            self.state.len(),
            self.output_count
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn create_event(val: serde_json::Value) -> Event {
        Rc::new(RefCell::new(val))
    }

    fn new_processor(args: &[&str]) -> KeyCorrelateProcessor<'static> {
        let argv: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        KeyCorrelateProcessor::new(&argv).unwrap()
    }

    // Helper to extract output fields from a flush event
    fn get_pair(event: &Event) -> (String, String) {
        let b = event.borrow();
        let pair = b.get("pair").unwrap().as_array().unwrap();
        (pair[0].as_str().unwrap().to_string(), pair[1].as_str().unwrap().to_string())
    }

    fn get_co_occurrence_count(event: &Event) -> usize {
        event.borrow().get("co_occurrence_count").unwrap().as_u64().unwrap() as usize
    }

    fn get_support(event: &Event) -> f64 {
        event.borrow().get("support").unwrap().as_f64().unwrap()
    }

    fn get_keys(event: &Event) -> Vec<String> {
        event
            .borrow()
            .get("keys")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect()
    }

    // ==================== Initialization ====================

    #[test]
    fn test_initialization_valid_args() {
        let p = new_processor(&["keycorrelate", "policy", "-k", "account"]);
        assert_eq!(p.top_n, 10);
        assert_eq!(p.min_support, 2);
        assert!(p.threshold.is_none());
        assert!(p.score_path.is_none());
    }

    #[test]
    fn test_initialization_custom_args() {
        let p = new_processor(&[
            "keycorrelate",
            "policy",
            "-k",
            "account",
            "-t",
            "0.5",
            "-n",
            "20",
            "--min-support",
            "3",
            "-s",
            "score",
            "-c",
            "500",
        ]);
        assert_eq!(p.top_n, 20);
        assert_eq!(p.min_support, 3);
        assert_eq!(p.threshold, Some(0.5));
        assert!(p.score_path.is_some());
    }

    #[test]
    fn test_initialization_missing_key_fails() {
        let argv: Vec<String> = ["keycorrelate", "policy"].iter().map(|s| s.to_string()).collect();
        assert!(KeyCorrelateProcessor::new(&argv).is_err());
    }

    // ==================== Process: field extraction ====================

    #[test]
    fn test_process_valid_event_returns_empty() {
        let mut p = new_processor(&["keycorrelate", "policy", "-k", "account"]);
        let event = create_event(serde_json::json!({"account": "acct1", "policy": "firewall"}));
        let result = p.process(event).unwrap();
        assert!(result.is_empty());
        assert_eq!(p.input_count, 1);
        assert_eq!(p.passed_threshold_count, 1);
    }

    #[test]
    fn test_process_null_key_skips() {
        let mut p = new_processor(&["keycorrelate", "policy", "-k", "account"]);
        let event = create_event(serde_json::json!({"policy": "firewall"}));
        let result = p.process(event).unwrap();
        assert!(result.is_empty());
        assert_eq!(p.input_count, 1);
        assert_eq!(p.passed_threshold_count, 0);
    }

    #[test]
    fn test_process_null_correlation_field_skips() {
        let mut p = new_processor(&["keycorrelate", "policy", "-k", "account"]);
        let event = create_event(serde_json::json!({"account": "acct1"}));
        let result = p.process(event).unwrap();
        assert!(result.is_empty());
        assert_eq!(p.input_count, 1);
        assert_eq!(p.passed_threshold_count, 0);
    }

    // ==================== Threshold filtering ====================

    #[test]
    fn test_threshold_with_score_path_above() {
        let mut p =
            new_processor(&["keycorrelate", "policy", "-k", "account", "-s", "score", "-t", "0.5"]);
        let event =
            create_event(serde_json::json!({"account": "a1", "policy": "fw", "score": 0.8}));
        p.process(event).unwrap();
        assert_eq!(p.passed_threshold_count, 1);
    }

    #[test]
    fn test_threshold_with_score_path_below() {
        let mut p =
            new_processor(&["keycorrelate", "policy", "-k", "account", "-s", "score", "-t", "0.5"]);
        let event =
            create_event(serde_json::json!({"account": "a1", "policy": "fw", "score": 0.2}));
        p.process(event).unwrap();
        assert_eq!(p.passed_threshold_count, 0);
    }

    #[test]
    fn test_threshold_at_exact_boundary() {
        let mut p =
            new_processor(&["keycorrelate", "policy", "-k", "account", "-s", "score", "-t", "0.5"]);
        let event =
            create_event(serde_json::json!({"account": "a1", "policy": "fw", "score": 0.5}));
        p.process(event).unwrap();
        assert_eq!(p.passed_threshold_count, 1);
    }

    #[test]
    fn test_threshold_parsed_from_correlation_field() {
        let mut p = new_processor(&["keycorrelate", "val", "-k", "account", "-t", "5.0"]);
        // No --score, so correlation field "val" is parsed as numeric
        let event = create_event(serde_json::json!({"account": "a1", "val": 7.0}));
        p.process(event).unwrap();
        assert_eq!(p.passed_threshold_count, 1);

        let event2 = create_event(serde_json::json!({"account": "a1", "val": 3.0}));
        p.process(event2).unwrap();
        assert_eq!(p.passed_threshold_count, 1); // still 1, second was below
    }

    #[test]
    fn test_no_threshold_includes_all() {
        let mut p = new_processor(&["keycorrelate", "policy", "-k", "account"]);
        for i in 0..5 {
            let event =
                create_event(serde_json::json!({"account": "a1", "policy": format!("p{}", i)}));
            p.process(event).unwrap();
        }
        assert_eq!(p.passed_threshold_count, 5);
    }

    // ==================== Pair generation ====================

    #[test]
    fn test_generate_all_pairs_two_values() {
        let mut set = HashSet::new();
        set.insert("a".to_string());
        set.insert("b".to_string());
        let pairs = generate_all_pairs(&set);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0], ("a".to_string(), "b".to_string()));
    }

    #[test]
    fn test_generate_all_pairs_three_values() {
        let mut set = HashSet::new();
        set.insert("a".to_string());
        set.insert("b".to_string());
        set.insert("c".to_string());
        let pairs = generate_all_pairs(&set);
        assert_eq!(pairs.len(), 3);
        // All pairs should be alphabetically ordered
        for (a, b) in &pairs {
            assert!(a < b);
        }
    }

    #[test]
    fn test_generate_all_pairs_one_value() {
        let mut set = HashSet::new();
        set.insert("a".to_string());
        let pairs = generate_all_pairs(&set);
        assert_eq!(pairs.len(), 0);
    }

    #[test]
    fn test_generate_all_pairs_empty() {
        let set = HashSet::new();
        let pairs = generate_all_pairs(&set);
        assert_eq!(pairs.len(), 0);
    }

    #[test]
    fn test_generate_all_pairs_four_values() {
        let mut set = HashSet::new();
        for v in &["a", "b", "c", "d"] {
            set.insert(v.to_string());
        }
        let pairs = generate_all_pairs(&set);
        assert_eq!(pairs.len(), 6); // 4*3/2
    }

    // ==================== Flush: end-to-end correlation ====================

    #[test]
    fn test_flush_empty_no_events() {
        let mut p = new_processor(&["keycorrelate", "policy", "-k", "account"]);
        let result = p.flush();
        assert!(result.is_empty());
    }

    #[test]
    fn test_flush_single_key_two_values() {
        let mut p =
            new_processor(&["keycorrelate", "policy", "-k", "account", "--min-support", "1"]);
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "cyber-attacks"})))
            .unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "deception"})))
            .unwrap();

        let result = p.flush();
        assert_eq!(result.len(), 1);
        assert_eq!(get_pair(&result[0]), ("cyber-attacks".to_string(), "deception".to_string()));
        assert_eq!(get_co_occurrence_count(&result[0]), 1);
        assert_eq!(get_support(&result[0]), 1.0);
        assert_eq!(get_keys(&result[0]), vec!["a1"]);
    }

    #[test]
    fn test_flush_two_keys_same_pair() {
        let mut p =
            new_processor(&["keycorrelate", "policy", "-k", "account", "--min-support", "1"]);
        // Key a1 has {cyber-attacks, deception}
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "cyber-attacks"})))
            .unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "deception"})))
            .unwrap();
        // Key a2 has {cyber-attacks, deception}
        p.process(create_event(serde_json::json!({"account": "a2", "policy": "cyber-attacks"})))
            .unwrap();
        p.process(create_event(serde_json::json!({"account": "a2", "policy": "deception"})))
            .unwrap();

        let result = p.flush();
        assert_eq!(result.len(), 1);
        assert_eq!(get_co_occurrence_count(&result[0]), 2);
        assert_eq!(get_support(&result[0]), 1.0); // 2/2 keys
        let keys = get_keys(&result[0]);
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_flush_min_support_filters() {
        let mut p =
            new_processor(&["keycorrelate", "policy", "-k", "account", "--min-support", "2"]);
        // Only a1 has {A, B} -> co_occurrence = 1, below min_support=2
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "A"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "B"}))).unwrap();

        let result = p.flush();
        assert!(result.is_empty()); // filtered out
    }

    #[test]
    fn test_flush_top_n_limits_output() {
        let mut p = new_processor(&[
            "keycorrelate",
            "policy",
            "-k",
            "account",
            "--min-support",
            "1",
            "-n",
            "2",
        ]);
        // a1 has {A, B, C} -> 3 pairs, but top_n=2
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "A"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "B"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "C"}))).unwrap();

        let result = p.flush();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_flush_sorted_by_co_occurrence_desc() {
        let mut p =
            new_processor(&["keycorrelate", "policy", "-k", "account", "--min-support", "1"]);
        // Pair (A, B) appears in 2 keys, pair (A, C) in 1 key
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "A"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "B"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "C"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a2", "policy": "A"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a2", "policy": "B"}))).unwrap();

        let result = p.flush();
        // (A, B) has count=2, others have count=1
        assert_eq!(get_pair(&result[0]), ("A".to_string(), "B".to_string()));
        assert_eq!(get_co_occurrence_count(&result[0]), 2);
    }

    #[test]
    fn test_flush_duplicate_values_deduplicated() {
        let mut p =
            new_processor(&["keycorrelate", "policy", "-k", "account", "--min-support", "1"]);
        // Same value "A" sent twice for same key -> should only count once
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "A"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "A"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "B"}))).unwrap();

        let result = p.flush();
        assert_eq!(result.len(), 1); // only one pair (A, B)
        assert_eq!(get_co_occurrence_count(&result[0]), 1);
    }

    #[test]
    fn test_flush_single_value_key_no_pairs() {
        let mut p =
            new_processor(&["keycorrelate", "policy", "-k", "account", "--min-support", "1"]);
        // a1 has only one value -> no pairs generated
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "A"}))).unwrap();

        let result = p.flush();
        assert!(result.is_empty());
    }

    #[test]
    fn test_double_flush_second_empty() {
        let mut p =
            new_processor(&["keycorrelate", "policy", "-k", "account", "--min-support", "1"]);
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "A"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "B"}))).unwrap();

        let first = p.flush();
        assert_eq!(first.len(), 1);

        let second = p.flush();
        assert!(second.is_empty());
    }

    // ==================== Output format ====================

    #[test]
    fn test_output_format_fields() {
        let mut p =
            new_processor(&["keycorrelate", "policy", "-k", "account", "--min-support", "1"]);
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "X"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "Y"}))).unwrap();

        let result = p.flush();
        let event = &result[0];
        let b = event.borrow();

        assert!(b.get("pair").unwrap().is_array());
        assert!(b.get("co_occurrence_count").unwrap().is_u64());
        assert!(b.get("support").unwrap().is_f64());
        assert!(b.get("keys").unwrap().is_array());

        // Pair should be alphabetically ordered
        let pair = b.get("pair").unwrap().as_array().unwrap();
        let a = pair[0].as_str().unwrap();
        let bb = pair[1].as_str().unwrap();
        assert!(a < bb);
    }

    // ==================== Stats ====================

    #[test]
    fn test_stats_after_processing() {
        let mut p =
            new_processor(&["keycorrelate", "policy", "-k", "account", "--min-support", "1"]);
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "A"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "B"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a2", "policy": "C"}))).unwrap();

        let stats = p.stats().unwrap();
        assert!(stats.contains("input:3"));
        assert!(stats.contains("passed_threshold:3"));
        assert!(stats.contains("unique_keys:2"));
        assert!(stats.contains("output:0"));

        p.flush();
        let stats2 = p.stats().unwrap();
        assert!(stats2.contains("unique_keys:0")); // cleared after flush
    }

    // ==================== LRU eviction ====================

    #[test]
    fn test_lru_eviction_bounded_memory() {
        let mut p = new_processor(&[
            "keycorrelate",
            "policy",
            "-k",
            "account",
            "--min-support",
            "1",
            "-c",
            "2",
        ]);
        // Insert 3 keys into cache of size 2 -> first key evicted
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "A"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a1", "policy": "B"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a2", "policy": "C"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a2", "policy": "D"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a3", "policy": "E"}))).unwrap();
        p.process(create_event(serde_json::json!({"account": "a3", "policy": "F"}))).unwrap();

        // Only 2 keys should remain in cache
        let result = p.flush();
        // a1 was evicted, so only a2 and a3 pairs remain
        assert!(result.len() <= 2); // at most 1 pair per key
    }
}
