use crate::processors::processor::*;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::rc::Rc;

const CACHE_SIZE: usize = 200000;
const MAX_FLUSH: usize = 1000;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AggregateFunction {
    Max,
    Min,
    Avg,
    Sum,
    Count,
}

#[derive(Clone, Debug)]
pub struct AggregateState {
    value: f64,
    count: u64,
}

impl AggregateState {
    fn new(value: f64) -> Self {
        Self { value, count: 1 }
    }

    fn update(&mut self, new_value: f64, func: AggregateFunction) {
        match func {
            AggregateFunction::Max => self.value = self.value.max(new_value),
            AggregateFunction::Min => self.value = self.value.min(new_value),
            AggregateFunction::Sum => self.value += new_value,
            AggregateFunction::Avg => {
                self.value = (self.value * self.count as f64 + new_value) / (self.count + 1) as f64;
                self.count += 1;
            }
            AggregateFunction::Count => self.value += 1.0,
        }
    }

    fn get_value(&self) -> f64 {
        self.value
    }
}

#[derive(Parser)]
/// Reshape long-format data to wide-format by aggregating across events (cross-event pivot/crosstab)
#[command(
    version,
    long_about = None,
    arg_required_else_help(true),
    after_help = r#"EXAMPLES:

  Build score profiles per account (max score per policy):
    pivot -k account -c policy -v score

  Same with explicit max aggregation:
    pivot -k account -c policy -v score -a max

  Count events per source-destination pair:
    pivot -k src_ip -c dst_port -v count -a count

  Average latency per service endpoint, fill missing with 0:
    pivot -k service -c endpoint -v latency -a avg --fill 0

  Sum bytes per host per protocol:
    pivot -k host -c protocol -v bytes -a sum

INPUT (long format - multiple rows per key):
  {"account": "123", "policy": "cyber-attacks", "score": 0.95}
  {"account": "123", "policy": "deception",     "score": 0.92}
  {"account": "123", "policy": "fraud",         "score": 0.80}

OUTPUT (wide format - one row per key with all columns):
  {"account": "123", "cyber-attacks": 0.95, "deception": 0.92, "fraud": 0.80}

NOTES:
  Results are emitted on LRU cache eviction or at flush (end of stream).
  Use --fill to control the value for columns not seen for a given key.
  Aliases: pivot, crosstab"#
)]
struct PivotProcessorArgs {
    /// JMESPath query for the row key field (becomes the row identifier)
    #[arg(short, long, required = true)]
    key: String,

    /// JMESPath query for the column name field (values become output field names)
    #[arg(short, long, required = true)]
    column: String,

    /// JMESPath query for the cell value field (must be numeric)
    #[arg(short, long, required = true)]
    value: String,

    /// Aggregation function for duplicate key-column pairs: max, min, avg, sum, count
    #[arg(short, long, default_value = "max")]
    aggregate: String,

    /// JSON fill value for columns missing from a key's output (default: null)
    #[arg(long)]
    fill: Option<String>,

    /// Maximum number of keys held in the LRU cache before eviction
    #[arg(long, default_value_t = CACHE_SIZE)]
    cache_size: usize,
}

type PivotState = HashMap<String, AggregateState>;

fn parse_aggregate_function(s: &str) -> Result<AggregateFunction, anyhow::Error> {
    match s.to_lowercase().as_str() {
        "max" => Ok(AggregateFunction::Max),
        "min" => Ok(AggregateFunction::Min),
        "avg" | "average" => Ok(AggregateFunction::Avg),
        "sum" => Ok(AggregateFunction::Sum),
        "count" => Ok(AggregateFunction::Count),
        _ => anyhow::bail!(
            "Invalid aggregation function: {}. Valid options: max, min, avg, sum, count",
            s
        ),
    }
}

#[derive(SerialProcessorInit)]
pub struct PivotProcessor<'a> {
    key_path: jmespath::Expression<'a>,
    column_path: jmespath::Expression<'a>,
    value_path: jmespath::Expression<'a>,
    aggregate_func: AggregateFunction,
    fill_value: Option<serde_json::Value>,
    key_field_name: String,
    state: LruCache<String, PivotState>,
    all_columns: HashSet<String>,
    input_count: u64,
    output_count: u64,
}

impl<'a> PivotProcessor<'a> {
    fn create_pivot_event(
        &self,
        key: String,
        pivot_state: &PivotState,
    ) -> Result<Event, anyhow::Error> {
        let mut output = serde_json::Map::new();
        output.insert(self.key_field_name.clone(), serde_json::to_value(&key)?);

        for column in &self.all_columns {
            let value = if let Some(state) = pivot_state.get(column) {
                serde_json::to_value(state.get_value())?
            } else {
                self.fill_value.clone().unwrap_or(serde_json::Value::Null)
            };
            output.insert(column.clone(), value);
        }

        let output_value: serde_json::Value = output.into();
        Ok(Rc::new(RefCell::new(output_value)))
    }
}
impl SerialProcessor for PivotProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("reshape long-format data to wide-format by aggregating across events (cross-event pivot/crosstab)".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = PivotProcessorArgs::try_parse_from(argv)?;
        let key_path = jmespath::compile(&args.key)?;
        let column_path = jmespath::compile(&args.column)?;
        let value_path = jmespath::compile(&args.value)?;
        let aggregate_func = parse_aggregate_function(&args.aggregate)?;

        let fill_value = match &args.fill {
            Some(fill_str) => Some(serde_json::from_str(fill_str)?),
            None => None,
        };

        let key_field_name = args.key.clone();

        Ok(PivotProcessor {
            key_path,
            column_path,
            value_path,
            aggregate_func,
            fill_value,
            key_field_name,
            state: LruCache::new(NonZeroUsize::new(args.cache_size).expect("non-zero cache size")),
            all_columns: HashSet::new(),
            input_count: 0,
            output_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        let key = self.key_path.search(&input)?;
        if key.is_null() {
            return Ok(vec![]);
        }

        let column = self.column_path.search(&input)?;
        if column.is_null() {
            return Ok(vec![]);
        }

        let value_match = self.value_path.search(&input)?;
        if value_match.is_null() {
            return Ok(vec![]);
        }

        let value = match value_match.as_number() {
            Some(v) => v,
            None => return Ok(vec![]),
        };

        let keystr = if let Some(s) = key.as_string() { s.to_string() } else { key.to_string() };

        let colstr =
            if let Some(s) = column.as_string() { s.to_string() } else { column.to_string() };

        self.all_columns.insert(colstr.clone());

        // Check if key exists - if so, update/insert column with aggregation
        if let Some(pivot_state) = self.state.get_mut(&keystr) {
            pivot_state
                .entry(colstr)
                .and_modify(|state| state.update(value, self.aggregate_func))
                .or_insert(AggregateState::new(value));
            return Ok(vec![]);
        }

        // Key doesn't exist - insert and potentially evict
        let mut new_state = HashMap::new();
        new_state.insert(colstr, AggregateState::new(value));

        if let Some((evicted_key, evicted_state)) = self.state.push(keystr, new_state) {
            let event = self.create_pivot_event(evicted_key, &evicted_state)?;
            self.output_count += 1;
            return Ok(vec![event]);
        }

        Ok(vec![])
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events: Vec<Event> = vec![];
        while let Some((key, state)) = self.state.pop_lru() {
            if let Ok(event) = self.create_pivot_event(key, &state) {
                events.push(event);
                self.output_count += 1;
            }

            if events.len() >= MAX_FLUSH {
                return events;
            }
        }
        events
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input: {}, output: {}", self.input_count, self.output_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: create a pivot processor with given args
    fn make_processor(args: &[&str]) -> PivotProcessor<'static> {
        let argv: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        PivotProcessor::new(&argv).unwrap()
    }

    // Helper: create a default pivot processor (key=account, column=policy, value=score, cache=3)
    fn default_processor() -> PivotProcessor<'static> {
        make_processor(&[
            "pivot",
            "-k",
            "account",
            "-c",
            "policy",
            "-v",
            "score",
            "--cache-size",
            "3",
        ])
    }

    // Helper: create a test event
    fn event(json: serde_json::Value) -> Event {
        Rc::new(RefCell::new(json))
    }

    // Helper: get a string field from an output event
    fn get_str(ev: &Event, field: &str) -> String {
        ev.borrow().get(field).unwrap().as_str().unwrap().to_string()
    }

    // Helper: get a f64 field from an output event
    fn get_f64(ev: &Event, field: &str) -> f64 {
        ev.borrow().get(field).unwrap().as_f64().unwrap()
    }

    // ==================== 7.1 Basic Functionality Tests ====================

    #[test]
    fn test_single_event_no_eviction() {
        let mut proc = default_processor();
        let result = proc
            .process(event(serde_json::json!({
                "account": "123", "policy": "cyber", "score": 0.95
            })))
            .unwrap();
        assert!(result.is_empty());
        assert_eq!(proc.input_count, 1);
        assert_eq!(proc.output_count, 0);
    }

    #[test]
    fn test_multiple_events_same_key_different_columns() {
        let mut proc = default_processor();
        proc.process(event(
            serde_json::json!({"account": "123", "policy": "cyber", "score": 0.95}),
        ))
        .unwrap();
        proc.process(event(
            serde_json::json!({"account": "123", "policy": "fraud", "score": 0.80}),
        ))
        .unwrap();

        let flushed = proc.flush();
        assert_eq!(flushed.len(), 1);
        assert_eq!(get_str(&flushed[0], "account"), "123");
        assert_eq!(get_f64(&flushed[0], "cyber"), 0.95);
        assert_eq!(get_f64(&flushed[0], "fraud"), 0.80);
    }

    #[test]
    fn test_same_key_column_aggregation() {
        let mut proc = default_processor();
        // Default aggregation is max
        proc.process(event(
            serde_json::json!({"account": "123", "policy": "cyber", "score": 0.50}),
        ))
        .unwrap();
        proc.process(event(
            serde_json::json!({"account": "123", "policy": "cyber", "score": 0.95}),
        ))
        .unwrap();
        proc.process(event(
            serde_json::json!({"account": "123", "policy": "cyber", "score": 0.70}),
        ))
        .unwrap();

        let flushed = proc.flush();
        assert_eq!(flushed.len(), 1);
        assert_eq!(get_f64(&flushed[0], "cyber"), 0.95);
    }

    #[test]
    fn test_null_key_skipped() {
        let mut proc = default_processor();
        let result = proc
            .process(event(serde_json::json!({
                "policy": "cyber", "score": 0.95
            })))
            .unwrap();
        assert!(result.is_empty());
        let flushed = proc.flush();
        assert!(flushed.is_empty());
    }

    #[test]
    fn test_null_column_skipped() {
        let mut proc = default_processor();
        let result = proc
            .process(event(serde_json::json!({
                "account": "123", "score": 0.95
            })))
            .unwrap();
        assert!(result.is_empty());
        let flushed = proc.flush();
        assert!(flushed.is_empty());
    }

    #[test]
    fn test_null_value_skipped() {
        let mut proc = default_processor();
        let result = proc
            .process(event(serde_json::json!({
                "account": "123", "policy": "cyber"
            })))
            .unwrap();
        assert!(result.is_empty());
        let flushed = proc.flush();
        assert!(flushed.is_empty());
    }

    // ==================== 7.2 Aggregation Tests ====================

    #[test]
    fn test_max_aggregation() {
        let mut proc = default_processor();
        for v in [0.50, 0.95, 0.70] {
            proc.process(event(serde_json::json!({"account": "a", "policy": "p", "score": v})))
                .unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(get_f64(&flushed[0], "p"), 0.95);
    }

    #[test]
    fn test_min_aggregation() {
        let mut proc = make_processor(&[
            "pivot",
            "-k",
            "account",
            "-c",
            "policy",
            "-v",
            "score",
            "-a",
            "min",
            "--cache-size",
            "3",
        ]);
        for v in [0.50, 0.95, 0.70] {
            proc.process(event(serde_json::json!({"account": "a", "policy": "p", "score": v})))
                .unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(get_f64(&flushed[0], "p"), 0.50);
    }

    #[test]
    fn test_sum_aggregation() {
        let mut proc = make_processor(&[
            "pivot",
            "-k",
            "account",
            "-c",
            "policy",
            "-v",
            "score",
            "-a",
            "sum",
            "--cache-size",
            "3",
        ]);
        for v in [1.0, 2.0, 3.0] {
            proc.process(event(serde_json::json!({"account": "a", "policy": "p", "score": v})))
                .unwrap();
        }
        let flushed = proc.flush();
        assert!((get_f64(&flushed[0], "p") - 6.0).abs() < 1e-9);
    }

    #[test]
    fn test_avg_aggregation() {
        let mut proc = make_processor(&[
            "pivot",
            "-k",
            "account",
            "-c",
            "policy",
            "-v",
            "score",
            "-a",
            "avg",
            "--cache-size",
            "3",
        ]);
        for v in [1.0, 2.0, 3.0] {
            proc.process(event(serde_json::json!({"account": "a", "policy": "p", "score": v})))
                .unwrap();
        }
        let flushed = proc.flush();
        assert!((get_f64(&flushed[0], "p") - 2.0).abs() < 1e-9);
    }

    #[test]
    fn test_count_aggregation() {
        let mut proc = make_processor(&[
            "pivot",
            "-k",
            "account",
            "-c",
            "policy",
            "-v",
            "score",
            "-a",
            "count",
            "--cache-size",
            "3",
        ]);
        for v in [1.0, 2.0, 3.0] {
            proc.process(event(serde_json::json!({"account": "a", "policy": "p", "score": v})))
                .unwrap();
        }
        let flushed = proc.flush();
        // count: initial 1.0 + 2 updates = 3.0
        assert!((get_f64(&flushed[0], "p") - 3.0).abs() < 1e-9);
    }

    #[test]
    fn test_default_aggregation_is_max() {
        // default_processor uses no explicit -a, so default is max
        let mut proc = make_processor(&[
            "pivot",
            "-k",
            "account",
            "-c",
            "policy",
            "-v",
            "score",
            "--cache-size",
            "3",
        ]);
        for v in [0.50, 0.95, 0.70] {
            proc.process(event(serde_json::json!({"account": "a", "policy": "p", "score": v})))
                .unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(get_f64(&flushed[0], "p"), 0.95);
    }

    // ==================== 7.3 Eviction Tests ====================

    #[test]
    fn test_eviction_on_capacity() {
        let mut proc = make_processor(&[
            "pivot",
            "-k",
            "account",
            "-c",
            "policy",
            "-v",
            "score",
            "--cache-size",
            "2",
        ]);
        // Fill cache with 2 keys
        proc.process(event(serde_json::json!({"account": "a1", "policy": "p1", "score": 1.0})))
            .unwrap();
        proc.process(event(serde_json::json!({"account": "a2", "policy": "p1", "score": 2.0})))
            .unwrap();
        // Third key triggers eviction
        let result = proc
            .process(event(serde_json::json!({"account": "a3", "policy": "p1", "score": 3.0})))
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(proc.output_count, 1);
    }

    #[test]
    fn test_evicted_key_is_lru() {
        let mut proc = make_processor(&[
            "pivot",
            "-k",
            "account",
            "-c",
            "policy",
            "-v",
            "score",
            "--cache-size",
            "2",
        ]);
        proc.process(event(serde_json::json!({"account": "a1", "policy": "p1", "score": 1.0})))
            .unwrap();
        proc.process(event(serde_json::json!({"account": "a2", "policy": "p1", "score": 2.0})))
            .unwrap();
        // Access a1 to make it recently used
        proc.process(event(serde_json::json!({"account": "a1", "policy": "p2", "score": 1.5})))
            .unwrap();
        // New key should evict a2 (LRU)
        let result = proc
            .process(event(serde_json::json!({"account": "a3", "policy": "p1", "score": 3.0})))
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(get_str(&result[0], "account"), "a2");
    }

    #[test]
    fn test_eviction_event_format() {
        let mut proc = make_processor(&[
            "pivot",
            "-k",
            "account",
            "-c",
            "policy",
            "-v",
            "score",
            "--cache-size",
            "1",
        ]);
        proc.process(event(serde_json::json!({"account": "a1", "policy": "cyber", "score": 0.95})))
            .unwrap();
        proc.process(event(serde_json::json!({"account": "a1", "policy": "fraud", "score": 0.80})))
            .unwrap();
        // Evict a1 by inserting a2
        let result = proc
            .process(event(serde_json::json!({"account": "a2", "policy": "cyber", "score": 0.50})))
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(get_str(&result[0], "account"), "a1");
        assert_eq!(get_f64(&result[0], "cyber"), 0.95);
        assert_eq!(get_f64(&result[0], "fraud"), 0.80);
    }

    // ==================== 7.4 Flush Tests ====================

    #[test]
    fn test_flush_empty() {
        let mut proc = default_processor();
        let flushed = proc.flush();
        assert!(flushed.is_empty());
    }

    #[test]
    fn test_flush_single_key() {
        let mut proc = default_processor();
        proc.process(event(serde_json::json!({"account": "a1", "policy": "cyber", "score": 0.95})))
            .unwrap();
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 1);
        assert_eq!(get_str(&flushed[0], "account"), "a1");
    }

    #[test]
    fn test_flush_multiple_keys() {
        let mut proc = default_processor();
        for i in 0..3 {
            proc.process(event(
                serde_json::json!({"account": format!("a{}", i), "policy": "p", "score": i as f64}),
            ))
            .unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 3);
        assert_eq!(proc.output_count, 3);
    }

    #[test]
    fn test_double_flush() {
        let mut proc = default_processor();
        proc.process(event(serde_json::json!({"account": "a1", "policy": "p", "score": 1.0})))
            .unwrap();
        let first = proc.flush();
        assert_eq!(first.len(), 1);
        let second = proc.flush();
        assert!(second.is_empty());
    }

    // ==================== 7.5 Fill Value Tests ====================

    #[test]
    fn test_fill_value_specified() {
        let mut proc = make_processor(&[
            "pivot",
            "-k",
            "account",
            "-c",
            "policy",
            "-v",
            "score",
            "--fill",
            "0",
            "--cache-size",
            "3",
        ]);
        proc.process(event(serde_json::json!({"account": "a1", "policy": "cyber", "score": 0.95})))
            .unwrap();
        proc.process(event(serde_json::json!({"account": "a2", "policy": "fraud", "score": 0.80})))
            .unwrap();
        let flushed = proc.flush();
        // a1 should have fill=0 for "fraud", a2 should have fill=0 for "cyber"
        for ev in &flushed {
            let borrowed = ev.borrow();
            let acct = borrowed.get("account").unwrap().as_str().unwrap();
            if acct == "a1" {
                assert_eq!(borrowed.get("fraud").unwrap().as_f64().unwrap(), 0.0);
            } else {
                assert_eq!(borrowed.get("cyber").unwrap().as_f64().unwrap(), 0.0);
            }
        }
    }

    #[test]
    fn test_default_null_fill() {
        let mut proc = default_processor();
        proc.process(event(serde_json::json!({"account": "a1", "policy": "cyber", "score": 0.95})))
            .unwrap();
        proc.process(event(serde_json::json!({"account": "a2", "policy": "fraud", "score": 0.80})))
            .unwrap();
        let flushed = proc.flush();
        for ev in &flushed {
            let borrowed = ev.borrow();
            let acct = borrowed.get("account").unwrap().as_str().unwrap();
            if acct == "a1" {
                assert!(borrowed.get("fraud").unwrap().is_null());
            } else {
                assert!(borrowed.get("cyber").unwrap().is_null());
            }
        }
    }

    #[test]
    fn test_fill_applied_to_missing_only() {
        let mut proc = make_processor(&[
            "pivot",
            "-k",
            "account",
            "-c",
            "policy",
            "-v",
            "score",
            "--fill",
            "0",
            "--cache-size",
            "3",
        ]);
        proc.process(event(serde_json::json!({"account": "a1", "policy": "cyber", "score": 0.95})))
            .unwrap();
        proc.process(event(serde_json::json!({"account": "a1", "policy": "fraud", "score": 0.80})))
            .unwrap();
        proc.process(event(serde_json::json!({"account": "a2", "policy": "cyber", "score": 0.50})))
            .unwrap();
        let flushed = proc.flush();
        for ev in &flushed {
            let borrowed = ev.borrow();
            let acct = borrowed.get("account").unwrap().as_str().unwrap();
            if acct == "a1" {
                // a1 has both columns - no fill needed
                assert_eq!(borrowed.get("cyber").unwrap().as_f64().unwrap(), 0.95);
                assert_eq!(borrowed.get("fraud").unwrap().as_f64().unwrap(), 0.80);
            } else {
                // a2 only has cyber, fraud should be filled with 0
                assert_eq!(borrowed.get("cyber").unwrap().as_f64().unwrap(), 0.50);
                assert_eq!(borrowed.get("fraud").unwrap().as_f64().unwrap(), 0.0);
            }
        }
    }

    // ==================== 7.6 Configuration Tests ====================

    #[test]
    fn test_required_arguments() {
        let result = PivotProcessor::new(&[
            "pivot".to_string(),
            "-k".to_string(),
            "account".to_string(),
            "-c".to_string(),
            "policy".to_string(),
            "-v".to_string(),
            "score".to_string(),
        ]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_missing_key_arg() {
        let result = PivotProcessor::new(&[
            "pivot".to_string(),
            "-c".to_string(),
            "policy".to_string(),
            "-v".to_string(),
            "score".to_string(),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_custom_aggregation_function() {
        let proc =
            make_processor(&["pivot", "-k", "account", "-c", "policy", "-v", "score", "-a", "sum"]);
        assert_eq!(proc.aggregate_func, AggregateFunction::Sum);
    }

    #[test]
    fn test_custom_fill_value() {
        let proc = make_processor(&[
            "pivot", "-k", "account", "-c", "policy", "-v", "score", "--fill", "0",
        ]);
        assert_eq!(proc.fill_value, Some(serde_json::json!(0)));
    }

    #[test]
    fn test_custom_cache_size() {
        let mut proc = make_processor(&[
            "pivot",
            "-k",
            "account",
            "-c",
            "policy",
            "-v",
            "score",
            "--cache-size",
            "2",
        ]);
        proc.process(event(serde_json::json!({"account": "a1", "policy": "p", "score": 1.0})))
            .unwrap();
        proc.process(event(serde_json::json!({"account": "a2", "policy": "p", "score": 2.0})))
            .unwrap();
        let result = proc
            .process(event(serde_json::json!({"account": "a3", "policy": "p", "score": 3.0})))
            .unwrap();
        // Cache size 2 means third key triggers eviction
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_invalid_aggregation_function() {
        let result = PivotProcessor::new(&[
            "pivot".to_string(),
            "-k".to_string(),
            "account".to_string(),
            "-c".to_string(),
            "policy".to_string(),
            "-v".to_string(),
            "score".to_string(),
            "-a".to_string(),
            "invalid".to_string(),
        ]);
        assert!(result.is_err());
    }

    // ==================== Stats Test ====================

    #[test]
    fn test_stats() {
        let mut proc = default_processor();
        for i in 0..3 {
            proc.process(event(
                serde_json::json!({"account": format!("a{}", i), "policy": "p", "score": i as f64}),
            ))
            .unwrap();
        }
        proc.flush();
        let stats = proc.stats().unwrap();
        assert!(stats.contains("input: 3"));
        assert!(stats.contains("output: 3"));
    }
}
