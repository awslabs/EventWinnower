use crate::processors::processor::*;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use ordered_float::OrderedFloat;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;

const CACHE_SIZE: usize = 200000;

#[derive(Parser)]
/// Assign rank positions to events within groups
#[command(
    version,
    long_about = None,
    arg_required_else_help(true),
    after_help = r#"GROUPING (one of --key or --global, default groups by key):

  -k, --key EXPR   JMESPath expression for grouping key. Events with the same
                   key are ranked together within their group.

  -g, --global     Rank all events together in a single group (no key needed).

SORT ORDER (default: descending):

  --desc           Highest value gets rank 1 (default). Best for "top N" queries.
  --asc            Lowest value gets rank 1. Best for "bottom N" queries.

RANKING STRATEGY:

  Standard (default):  Ties share a rank, next rank skips.
                       Values [100, 90, 90, 80] → Ranks [1, 2, 2, 4]

  Dense (--dense):     Ties share a rank, next rank is consecutive.
                       Values [100, 90, 90, 80] → Ranks [1, 2, 2, 3]

OUTPUT FIELDS:

  {rank_label}        Integer rank position (default field name: "rank")
  {percentile_label}  Percentile 0-100 (only with --percentile, default: "percentile")
                      Formula (desc): ((group_size - rank) / (group_size - 1)) * 100
                      Formula (asc):  ((rank - 1) / (group_size - 1)) * 100

EXAMPLES:

  Rank policies by score within each account (highest first):
    rank score -k account --desc

  Rank with percentiles to find 99th percentile per account:
    rank score -k account --desc --percentile

  Rank all events globally, lowest first, dense ranking:
    rank latency --global --asc --dense

  Custom output labels:
    rank score -k account --rank-label position --percentile-label pct --percentile

  Larger cache for high-cardinality keys:
    rank score -k account --cache-size 500000

INPUT:
  {"account": "acc-1", "policy": "p1", "score": 95}
  {"account": "acc-1", "policy": "p2", "score": 80}
  {"account": "acc-1", "policy": "p3", "score": 95}

OUTPUT (rank score -k account --desc --percentile):
  {"account": "acc-1", "policy": "p1", "score": 95, "rank": 1, "percentile": 100.0}
  {"account": "acc-1", "policy": "p3", "score": 95, "rank": 1, "percentile": 100.0}
  {"account": "acc-1", "policy": "p2", "score": 80, "rank": 3, "percentile": 0.0}
"#,
)]
struct RankArgs {
    /// JMESPath query for the value to rank by
    #[arg(required(true))]
    value: Vec<String>,

    /// JMESPath query for grouping key
    #[arg(short, long, conflicts_with = "global")]
    key: Option<String>,

    /// Rank all events globally (no grouping)
    #[arg(short, long, default_value_t = false, conflicts_with = "key")]
    global: bool,

    /// Sort descending (highest value = rank 1)
    #[arg(long, default_value_t = false, conflicts_with = "asc")]
    desc: bool,

    /// Sort ascending (lowest value = rank 1)
    #[arg(long, default_value_t = false, conflicts_with = "desc")]
    asc: bool,

    /// Include percentile in output
    #[arg(short, long, default_value_t = false)]
    percentile: bool,

    /// Use dense ranking (1,1,2 instead of 1,1,3)
    #[arg(short, long, default_value_t = false)]
    dense: bool,

    /// Field name for rank output
    #[arg(long, default_value = "rank")]
    rank_label: String,

    /// Field name for percentile output
    #[arg(long, default_value = "percentile")]
    percentile_label: String,

    /// Maximum number of groups to track
    #[arg(short, long, default_value_t = CACHE_SIZE, value_name = "N")]
    cache_size: usize,
}

#[derive(SerialProcessorInit)]
pub struct RankProcessor<'a> {
    value_path: jmespath::Expression<'a>,
    key_path: Option<jmespath::Expression<'a>>,
    state: LruCache<String, Vec<(OrderedFloat<f64>, Event)>>,
    descending: bool,
    dense_rank: bool,
    include_percentile: bool,
    rank_label: String,
    percentile_label: String,
    input_count: u64,
    groups_count: u64,
    output_count: u64,
}

impl SerialProcessor for RankProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("assign rank positions to events within groups".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = RankArgs::try_parse_from(argv)?;

        let value_path = jmespath::compile(&args.value.join(" "))?;

        let key_path = match args.key {
            Some(key) => Some(jmespath::compile(&key)?),
            _ => None,
        };

        // Default to descending if neither --asc nor --desc specified
        let descending = !args.asc;

        Ok(RankProcessor {
            value_path,
            key_path,
            state: LruCache::new(NonZeroUsize::new(args.cache_size).expect("non-zero cache size")),
            descending,
            dense_rank: args.dense,
            include_percentile: args.percentile,
            rank_label: args.rank_label,
            percentile_label: args.percentile_label,
            input_count: 0,
            groups_count: 0,
            output_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        let value_match = self.value_path.search(&input)?;

        let value = match value_match.as_number() {
            Some(value) => value,
            None => {
                return Ok(vec![]);
            }
        };

        let key = match &self.key_path {
            Some(key_path) => {
                let key = key_path.search(&input)?;
                if key.is_null() {
                    return Ok(vec![]);
                }
                if let Some(s) = key.as_string() {
                    s.to_string()
                } else {
                    key.to_string()
                }
            }
            _ => "global".to_string(),
        };

        // Check if key exists - if so, append (value, event)
        if let Some(group) = self.state.get_mut(&key) {
            group.push((OrderedFloat(value), input));
            return Ok(vec![]);
        }

        // Key doesn't exist - insert and potentially evict
        self.groups_count += 1;
        if let Some((_expired_key, expired_events)) =
            self.state.push(key, vec![(OrderedFloat(value), input)])
        {
            let ranked = assign_ranks(
                expired_events,
                self.descending,
                self.dense_rank,
                self.include_percentile,
                &self.rank_label,
                &self.percentile_label,
            );
            self.output_count += ranked.len() as u64;
            return Ok(ranked);
        }

        Ok(vec![])
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events: Vec<Event> = vec![];
        while let Some((_key, group)) = self.state.pop_lru() {
            let mut ranked = assign_ranks(
                group,
                self.descending,
                self.dense_rank,
                self.include_percentile,
                &self.rank_label,
                &self.percentile_label,
            );
            self.output_count += ranked.len() as u64;
            events.append(&mut ranked);
        }
        events
    }

    fn stats(&self) -> Option<String> {
        Some(format!(
            "input: {}, groups: {}, output: {}",
            self.input_count, self.groups_count, self.output_count
        ))
    }
}

fn assign_ranks(
    mut events: Vec<(OrderedFloat<f64>, Event)>,
    descending: bool,
    dense: bool,
    include_percentile: bool,
    rank_label: &str,
    percentile_label: &str,
) -> Vec<Event> {
    // Sort by value
    if descending {
        events.sort_by(|a, b| b.0.cmp(&a.0));
    } else {
        events.sort_by(|a, b| a.0.cmp(&b.0));
    }

    let group_size = events.len();
    let mut results = Vec::with_capacity(group_size);
    let mut current_rank: usize = 1;
    let mut dense_rank_counter: usize = 1;

    for (i, (value, event)) in events.iter().enumerate() {
        let rank = if i == 0 {
            1
        } else if *value == events[i - 1].0 {
            // Tie - use same rank as previous
            if dense {
                dense_rank_counter
            } else {
                current_rank
            }
        } else {
            // New value
            if dense {
                dense_rank_counter += 1;
                dense_rank_counter
            } else {
                current_rank = i + 1;
                current_rank
            }
        };

        // Clone event and add rank
        let mut output = event.borrow().clone();
        output[rank_label] = serde_json::json!(rank);

        // Add percentile if requested
        if include_percentile {
            let percentile = calculate_percentile(rank, group_size, descending);
            output[percentile_label] = serde_json::json!(percentile);
        }

        results.push(Rc::new(RefCell::new(output)));
    }

    results
}

fn calculate_percentile(rank: usize, group_size: usize, descending: bool) -> f64 {
    if group_size == 1 {
        return if descending { 100.0 } else { 0.0 };
    }

    let percentile = if descending {
        ((group_size - rank) as f64 / (group_size - 1) as f64) * 100.0
    } else {
        ((rank - 1) as f64 / (group_size - 1) as f64) * 100.0
    };

    // Round to 2 decimal places
    (percentile * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ==================== Helper Functions ====================

    fn create_test_event(key: &str, value: f64) -> Event {
        Rc::new(RefCell::new(json!({
            "key": key,
            "value": value
        })))
    }

    fn create_custom_event(fields: serde_json::Value) -> Event {
        Rc::new(RefCell::new(fields))
    }

    fn make_processor(args: &[&str]) -> RankProcessor<'static> {
        let argv: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        RankProcessor::new(&argv).unwrap()
    }

    fn get_rank(event: &Event, label: &str) -> u64 {
        event.borrow().get(label).unwrap().as_u64().unwrap()
    }

    fn get_percentile_val(event: &Event, label: &str) -> f64 {
        event.borrow().get(label).unwrap().as_f64().unwrap()
    }

    // ==================== 6.1 Basic Functionality Tests ====================

    #[test]
    fn test_single_event_valid_value_no_eviction() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "-c", "10"]);
        let event = create_test_event("k1", 42.0);
        let result = proc.process(event).unwrap();
        assert!(result.is_empty());
        assert_eq!(proc.input_count, 1);
    }

    #[test]
    fn test_multiple_events_same_key_accumulate() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "-c", "10"]);
        for i in 1..=5 {
            let event = create_test_event("same", i as f64 * 10.0);
            let result = proc.process(event).unwrap();
            assert!(result.is_empty());
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 5);
    }

    #[test]
    fn test_null_value_jmespath_skipped() {
        let mut proc = make_processor(&["rank", "missing_field", "-k", "key", "-c", "10"]);
        let event = create_test_event("k1", 42.0);
        let result = proc.process(event).unwrap();
        assert!(result.is_empty());
        let flushed = proc.flush();
        assert!(flushed.is_empty());
    }

    #[test]
    fn test_non_numeric_value_skipped() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "-c", "10"]);
        let event = create_custom_event(json!({"key": "k1", "value": "not_a_number"}));
        let result = proc.process(event).unwrap();
        assert!(result.is_empty());
        let flushed = proc.flush();
        assert!(flushed.is_empty());
    }

    #[test]
    fn test_null_key_skipped() {
        let mut proc = make_processor(&["rank", "value", "-k", "missing_key", "-c", "10"]);
        let event = create_test_event("k1", 42.0);
        let result = proc.process(event).unwrap();
        assert!(result.is_empty());
        let flushed = proc.flush();
        assert!(flushed.is_empty());
    }

    // ==================== 6.2 Sorting and Ranking Tests ====================

    #[test]
    fn test_descending_order() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--desc"]);
        for v in &[50.0, 100.0, 75.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 3);
        // Descending: 100=rank1, 75=rank2, 50=rank3
        let ranks: Vec<(f64, u64)> = flushed
            .iter()
            .map(|e| {
                let b = e.borrow();
                (b["value"].as_f64().unwrap(), b["rank"].as_u64().unwrap())
            })
            .collect();
        assert!(ranks.contains(&(100.0, 1)));
        assert!(ranks.contains(&(75.0, 2)));
        assert!(ranks.contains(&(50.0, 3)));
    }

    #[test]
    fn test_ascending_order() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--asc"]);
        for v in &[50.0, 100.0, 75.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        let ranks: Vec<(f64, u64)> = flushed
            .iter()
            .map(|e| {
                let b = e.borrow();
                (b["value"].as_f64().unwrap(), b["rank"].as_u64().unwrap())
            })
            .collect();
        assert!(ranks.contains(&(50.0, 1)));
        assert!(ranks.contains(&(75.0, 2)));
        assert!(ranks.contains(&(100.0, 3)));
    }

    #[test]
    fn test_standard_ranking_with_ties() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--desc"]);
        for v in &[100.0, 90.0, 90.0, 80.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        let mut ranks: Vec<(f64, u64)> = flushed
            .iter()
            .map(|e| {
                let b = e.borrow();
                (b["value"].as_f64().unwrap(), b["rank"].as_u64().unwrap())
            })
            .collect();
        ranks.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        // Standard: 100→1, 90→2, 90→2, 80→4
        assert_eq!(ranks[0], (100.0, 1));
        assert_eq!(ranks[1], (90.0, 2));
        assert_eq!(ranks[2], (90.0, 2));
        assert_eq!(ranks[3], (80.0, 4));
    }

    #[test]
    fn test_dense_ranking_with_ties() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--desc", "--dense"]);
        for v in &[100.0, 90.0, 90.0, 80.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        let mut ranks: Vec<(f64, u64)> = flushed
            .iter()
            .map(|e| {
                let b = e.borrow();
                (b["value"].as_f64().unwrap(), b["rank"].as_u64().unwrap())
            })
            .collect();
        ranks.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        // Dense: 100→1, 90→2, 90→2, 80→3
        assert_eq!(ranks[0], (100.0, 1));
        assert_eq!(ranks[1], (90.0, 2));
        assert_eq!(ranks[2], (90.0, 2));
        assert_eq!(ranks[3], (80.0, 3));
    }

    #[test]
    fn test_mixed_values_multiple_ties() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--desc"]);
        for v in &[100.0, 100.0, 80.0, 80.0, 60.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        let mut ranks: Vec<(f64, u64)> = flushed
            .iter()
            .map(|e| {
                let b = e.borrow();
                (b["value"].as_f64().unwrap(), b["rank"].as_u64().unwrap())
            })
            .collect();
        ranks.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        // Standard: 100→1,1, 80→3,3, 60→5
        assert_eq!(ranks[0].1, 1);
        assert_eq!(ranks[1].1, 1);
        assert_eq!(ranks[2].1, 3);
        assert_eq!(ranks[3].1, 3);
        assert_eq!(ranks[4].1, 5);
    }

    // ==================== 6.3 Percentile Tests ====================

    #[test]
    fn test_percentile_descending() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--desc", "--percentile"]);
        // 5 events: values 50,40,30,20,10
        for v in &[50.0, 40.0, 30.0, 20.0, 10.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        // Desc: rank1=50 pct=100, rank2=40 pct=75, rank3=30 pct=50, rank4=20 pct=25, rank5=10 pct=0
        for e in &flushed {
            let b = e.borrow();
            let val = b["value"].as_f64().unwrap();
            let pct = b["percentile"].as_f64().unwrap();
            match val as u64 {
                50 => assert_eq!(pct, 100.0),
                40 => assert_eq!(pct, 75.0),
                30 => assert_eq!(pct, 50.0),
                20 => assert_eq!(pct, 25.0),
                10 => assert_eq!(pct, 0.0),
                _ => panic!("unexpected value"),
            }
        }
    }

    #[test]
    fn test_percentile_ascending() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--asc", "--percentile"]);
        for v in &[10.0, 20.0, 30.0, 40.0, 50.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        // Asc: rank1=10 pct=0, rank2=20 pct=25, rank3=30 pct=50, rank4=40 pct=75, rank5=50 pct=100
        for e in &flushed {
            let b = e.borrow();
            let val = b["value"].as_f64().unwrap();
            let pct = b["percentile"].as_f64().unwrap();
            match val as u64 {
                10 => assert_eq!(pct, 0.0),
                20 => assert_eq!(pct, 25.0),
                30 => assert_eq!(pct, 50.0),
                40 => assert_eq!(pct, 75.0),
                50 => assert_eq!(pct, 100.0),
                _ => panic!("unexpected value"),
            }
        }
    }

    #[test]
    fn test_percentile_single_event_desc() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--desc", "--percentile"]);
        proc.process(create_test_event("g", 42.0)).unwrap();
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 1);
        assert_eq!(get_percentile_val(&flushed[0], "percentile"), 100.0);
    }

    #[test]
    fn test_percentile_single_event_asc() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--asc", "--percentile"]);
        proc.process(create_test_event("g", 42.0)).unwrap();
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 1);
        assert_eq!(get_percentile_val(&flushed[0], "percentile"), 0.0);
    }

    #[test]
    fn test_percentile_rounding() {
        // 4 events desc: ranks 1,2,3,4. group_size=4
        // rank2 pct = (4-2)/(4-1)*100 = 66.666... → 66.67
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--desc", "--percentile"]);
        for v in &[40.0, 30.0, 20.0, 10.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        for e in &flushed {
            let b = e.borrow();
            if b["value"].as_f64().unwrap() == 30.0 {
                assert_eq!(b["percentile"].as_f64().unwrap(), 66.67);
            }
        }
    }

    // ==================== 6.4 Eviction Tests ====================

    #[test]
    fn test_eviction_on_capacity() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "-c", "2", "--desc"]);
        // Fill cache: key_0, key_1
        proc.process(create_test_event("key_0", 10.0)).unwrap();
        proc.process(create_test_event("key_1", 20.0)).unwrap();
        // Add key_2 → should evict key_0 (LRU)
        let result = proc.process(create_test_event("key_2", 30.0)).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_evicted_group_is_lru() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "-c", "3", "--desc"]);
        // Add key_0, key_1, key_2
        for i in 0..3 {
            proc.process(create_test_event(&format!("key_{}", i), i as f64 * 10.0)).unwrap();
        }
        // Access key_0 to make it recently used
        proc.process(create_test_event("key_0", 5.0)).unwrap();
        // Add key_3 → should evict key_1 (LRU)
        let result = proc.process(create_test_event("key_3", 30.0)).unwrap();
        assert_eq!(result.len(), 1);
        let evicted_key = result[0].borrow()["key"].as_str().unwrap().to_string();
        assert_eq!(evicted_key, "key_1");
    }

    #[test]
    fn test_evicted_events_are_ranked() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "-c", "2", "--desc"]);
        // Add two events to key_0
        proc.process(create_test_event("key_0", 100.0)).unwrap();
        proc.process(create_test_event("key_0", 50.0)).unwrap();
        // Add key_1
        proc.process(create_test_event("key_1", 20.0)).unwrap();
        // Add key_2 → evicts key_0
        let result = proc.process(create_test_event("key_2", 30.0)).unwrap();
        assert_eq!(result.len(), 2);
        // Verify ranks assigned
        for e in &result {
            assert!(e.borrow().get("rank").is_some());
        }
        let mut ranks: Vec<(f64, u64)> = result
            .iter()
            .map(|e| {
                let b = e.borrow();
                (b["value"].as_f64().unwrap(), b["rank"].as_u64().unwrap())
            })
            .collect();
        ranks.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        assert_eq!(ranks[0], (100.0, 1));
        assert_eq!(ranks[1], (50.0, 2));
    }

    // ==================== 6.5 Flush Tests ====================

    #[test]
    fn test_flush_no_events() {
        let mut proc = make_processor(&["rank", "value", "-k", "key"]);
        let flushed = proc.flush();
        assert!(flushed.is_empty());
    }

    #[test]
    fn test_flush_single_group() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--desc"]);
        proc.process(create_test_event("g", 10.0)).unwrap();
        proc.process(create_test_event("g", 20.0)).unwrap();
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 2);
        for e in &flushed {
            assert!(e.borrow().get("rank").is_some());
        }
    }

    #[test]
    fn test_flush_multiple_groups() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--desc"]);
        for i in 0..3 {
            proc.process(create_test_event(&format!("g{}", i), i as f64 * 10.0)).unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 3);
        assert_eq!(proc.output_count, 3);
    }

    #[test]
    fn test_double_flush() {
        let mut proc = make_processor(&["rank", "value", "-k", "key"]);
        proc.process(create_test_event("g", 10.0)).unwrap();
        let first = proc.flush();
        assert_eq!(first.len(), 1);
        let second = proc.flush();
        assert!(second.is_empty());
    }

    // ==================== 6.6 Configuration Tests ====================

    #[test]
    fn test_custom_rank_label() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--rank-label", "position"]);
        proc.process(create_test_event("g", 10.0)).unwrap();
        let flushed = proc.flush();
        let b = flushed[0].borrow();
        assert!(b.get("position").is_some());
        assert!(b.get("rank").is_none());
    }

    #[test]
    fn test_custom_percentile_label() {
        let mut proc = make_processor(&[
            "rank",
            "value",
            "-k",
            "key",
            "--percentile",
            "--percentile-label",
            "pct",
        ]);
        proc.process(create_test_event("g", 10.0)).unwrap();
        let flushed = proc.flush();
        let b = flushed[0].borrow();
        assert!(b.get("pct").is_some());
        assert!(b.get("percentile").is_none());
    }

    #[test]
    fn test_custom_cache_size() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "-c", "2", "--desc"]);
        proc.process(create_test_event("k0", 10.0)).unwrap();
        proc.process(create_test_event("k1", 20.0)).unwrap();
        let result = proc.process(create_test_event("k2", 30.0)).unwrap();
        assert_eq!(result.len(), 1); // eviction occurred
    }

    #[test]
    fn test_global_mode() {
        let mut proc = make_processor(&["rank", "value", "--global", "--desc"]);
        proc.process(create_test_event("a", 10.0)).unwrap();
        proc.process(create_test_event("b", 20.0)).unwrap();
        let flushed = proc.flush();
        // All events ranked together in one group
        assert_eq!(flushed.len(), 2);
    }

    #[test]
    fn test_default_values() {
        let mut proc = make_processor(&["rank", "value", "-k", "key"]);
        proc.process(create_test_event("g", 10.0)).unwrap();
        let flushed = proc.flush();
        let b = flushed[0].borrow();
        assert!(b.get("rank").is_some()); // default rank label
    }

    // ==================== 6.7 Output Format Tests ====================

    #[test]
    fn test_original_fields_preserved() {
        let mut proc = make_processor(&["rank", "score", "-k", "account", "--desc"]);
        let event = create_custom_event(json!({
            "account": "acc-1",
            "policy": "p1",
            "score": 95
        }));
        proc.process(event).unwrap();
        let flushed = proc.flush();
        let b = flushed[0].borrow();
        assert_eq!(b["account"], "acc-1");
        assert_eq!(b["policy"], "p1");
        assert_eq!(b["score"], 95);
    }

    #[test]
    fn test_rank_field_added() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--desc"]);
        proc.process(create_test_event("g", 42.0)).unwrap();
        let flushed = proc.flush();
        assert_eq!(get_rank(&flushed[0], "rank"), 1);
    }

    #[test]
    fn test_percentile_field_added_when_flag_set() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--desc", "--percentile"]);
        proc.process(create_test_event("g", 42.0)).unwrap();
        let flushed = proc.flush();
        let b = flushed[0].borrow();
        assert!(b.get("percentile").is_some());
    }

    #[test]
    fn test_no_percentile_field_without_flag() {
        let mut proc = make_processor(&["rank", "value", "-k", "key", "--desc"]);
        proc.process(create_test_event("g", 42.0)).unwrap();
        let flushed = proc.flush();
        let b = flushed[0].borrow();
        assert!(b.get("percentile").is_none());
    }
}
