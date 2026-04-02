use crate::processors::processor::*;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;

const CACHE_SIZE: usize = 200000;
const MAX_FLUSH: usize = 1000;

#[derive(Parser)]
/// Compute percentile statistics (min, p50, p90, p99, p100) by key
#[command(version, long_about = None, arg_required_else_help(true))]
struct KeyPercentileArgs {
    #[arg(required(true))]
    value: Vec<String>,

    #[arg(short, long)]
    key: Option<String>,

    #[arg(short = 'l', long, default_value = "key")]
    key_label: String,

    #[arg(short, long, default_value = "percentiles")]
    stats_label: String,

    #[arg(short, long, default_value_t = CACHE_SIZE)]
    cache_size: usize,
}

#[derive(SerialProcessorInit)]
pub struct KeyPercentileProcessor<'a> {
    value_path: jmespath::Expression<'a>,
    key_path: Option<jmespath::Expression<'a>>,
    state: LruCache<String, Vec<f64>>,
    key_label: String,
    stats_label: String,
    input_count: u64,
    output_count: u64,
}

impl SerialProcessor for KeyPercentileProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("compute percentile statistics (min, p50, p90, p99, p100) by key".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = KeyPercentileArgs::try_parse_from(argv)?;
        let value_path = jmespath::compile(&args.value.join(" "))?;

        let key_path = match args.key {
            Some(key) => Some(jmespath::compile(&key)?),
            _ => None,
        };

        Ok(KeyPercentileProcessor {
            value_path,
            key_path,
            state: LruCache::new(NonZeroUsize::new(args.cache_size).expect("non-zero cache size")),
            key_label: args.key_label,
            stats_label: args.stats_label,
            input_count: 0,
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
            _ => "total".to_string(),
        };

        // Check if key exists - if so, append value
        if let Some(values) = self.state.get_mut(&key) {
            values.push(value);
            return Ok(vec![]);
        }

        // Key doesn't exist - insert and potentially evict
        if let Some((expired_key, mut expired_values)) = self.state.push(key, vec![value]) {
            let event = self.create_event(expired_key, &mut expired_values)?;
            self.output_count += 1;
            return Ok(vec![event]);
        }

        Ok(vec![])
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events: Vec<Event> = vec![];
        while let Some((key, mut values)) = self.state.pop_lru() {
            if let Ok(event) = self.create_event(key, &mut values) {
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

impl KeyPercentileProcessor<'_> {
    fn create_event(&self, key: String, values: &mut [f64]) -> Result<Event, anyhow::Error> {
        let stats = calculate_percentiles(values);

        let mut output = serde_json::Map::new();
        output.insert(self.key_label.clone(), serde_json::to_value(key)?);

        let mut percentiles = serde_json::Map::new();
        percentiles.insert("min".to_string(), serde_json::to_value(stats.min)?);
        percentiles.insert("p50".to_string(), serde_json::to_value(stats.p50)?);
        percentiles.insert("p90".to_string(), serde_json::to_value(stats.p90)?);
        percentiles.insert("p99".to_string(), serde_json::to_value(stats.p99)?);
        percentiles.insert("p100".to_string(), serde_json::to_value(stats.p100)?);

        output.insert(self.stats_label.clone(), serde_json::Value::Object(percentiles));

        let output_value: serde_json::Value = output.into();
        Ok(Rc::new(RefCell::new(output_value)))
    }
}

struct PercentileStats {
    min: f64,
    p50: f64,
    p90: f64,
    p99: f64,
    p100: f64,
}

fn calculate_percentiles(values: &mut [f64]) -> PercentileStats {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let min = values[0];
    let p100 = values[values.len() - 1];
    let p50 = percentile_at(values, 0.50);
    let p90 = percentile_at(values, 0.90);
    let p99 = percentile_at(values, 0.99);

    PercentileStats { min, p50, p90, p99, p100 }
}

fn percentile_at(sorted_values: &[f64], percentile: f64) -> f64 {
    let len = sorted_values.len();
    if len == 1 {
        return sorted_values[0];
    }

    let index = percentile * (len - 1) as f64;
    let lower = index.floor() as usize;
    let upper = index.ceil() as usize;

    if lower == upper {
        sorted_values[lower]
    } else {
        let fraction = index - lower as f64;
        sorted_values[lower] * (1.0 - fraction) + sorted_values[upper] * fraction
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a test event with a key and value
    fn create_test_event(key: &str, value: f64) -> Event {
        let json = serde_json::json!({
            "key": key,
            "value": value
        });
        Rc::new(RefCell::new(json))
    }

    // Helper function to create a test event with custom fields
    fn create_custom_event(fields: serde_json::Value) -> Event {
        Rc::new(RefCell::new(fields))
    }

    // Helper function to extract percentiles from an output event
    fn get_percentiles(event: &Event, stats_label: &str) -> (f64, f64, f64, f64, f64) {
        let borrowed = event.borrow();
        let percentiles = borrowed.get(stats_label).unwrap();
        (
            percentiles.get("min").unwrap().as_f64().unwrap(),
            percentiles.get("p50").unwrap().as_f64().unwrap(),
            percentiles.get("p90").unwrap().as_f64().unwrap(),
            percentiles.get("p99").unwrap().as_f64().unwrap(),
            percentiles.get("p100").unwrap().as_f64().unwrap(),
        )
    }

    // Helper function to extract key from an output event
    fn get_key(event: &Event, key_label: &str) -> String {
        event.borrow().get(key_label).unwrap().as_str().unwrap().to_string()
    }

    // ==================== Basic Functionality Tests (6.1) ====================

    #[test]
    fn test_keypercentile_processor_initialization() {
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
        ];
        let processor = KeyPercentileProcessor::new(&args);
        assert!(processor.is_ok());
    }

    #[test]
    fn test_single_event_valid_value_no_eviction() {
        // Test single event with valid value (no eviction, empty result)
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        let event = create_test_event("test_key", 42.0);
        let result = processor.process(event).unwrap();

        // No eviction should occur with single event
        assert!(result.is_empty());
        assert_eq!(processor.input_count, 1);
        assert_eq!(processor.output_count, 0);
    }

    #[test]
    fn test_multiple_events_same_key_values_accumulate() {
        // Test multiple events with same key (values accumulate)
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        // Add multiple events with the same key
        for i in 1..=5 {
            let event = create_test_event("same_key", i as f64 * 10.0);
            let result = processor.process(event).unwrap();
            assert!(result.is_empty()); // No eviction
        }

        // Flush and verify all values were accumulated
        let flushed = processor.flush();
        assert_eq!(flushed.len(), 1);

        let (min, p50, _p90, _p99, p100) = get_percentiles(&flushed[0], "percentiles");
        assert_eq!(min, 10.0);
        assert_eq!(p100, 50.0);
        // p50 of [10, 20, 30, 40, 50] should be 30.0
        assert_eq!(p50, 30.0);
    }

    #[test]
    fn test_null_value_jmespath_result_empty() {
        // Test events with null value JMESPath result (empty results)
        let args = vec![
            "keypercentile".to_string(),
            "missing_field".to_string(), // This field doesn't exist
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        let event = create_test_event("test_key", 42.0);
        let result = processor.process(event).unwrap();

        // Should return empty as value path returns null
        assert!(result.is_empty());
        assert_eq!(processor.input_count, 1);

        // Flush should also be empty since no values were collected
        let flushed = processor.flush();
        assert!(flushed.is_empty());
    }

    #[test]
    fn test_non_numeric_value_empty_result() {
        // Test events with non-numeric values (empty results)
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        // Create event with string value instead of numeric
        let event = create_custom_event(serde_json::json!({
            "key": "test_key",
            "value": "not_a_number"
        }));
        let result = processor.process(event).unwrap();

        // Should return empty as value is not numeric
        assert!(result.is_empty());
        assert_eq!(processor.input_count, 1);

        // Flush should also be empty
        let flushed = processor.flush();
        assert!(flushed.is_empty());
    }

    #[test]
    fn test_null_key_empty_result() {
        // Test events with null key (empty results)
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "missing_key".to_string(), // This field doesn't exist
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        let event = create_test_event("test_key", 42.0);
        let result = processor.process(event).unwrap();

        // Should return empty as key path returns null
        assert!(result.is_empty());
        assert_eq!(processor.input_count, 1);

        // Flush should also be empty
        let flushed = processor.flush();
        assert!(flushed.is_empty());
    }

    // ==================== Percentile Calculation Tests (6.2) ====================

    #[test]
    fn test_percentile_calculation_single_value() {
        // Test single value (all percentiles equal that value)
        let mut values = vec![42.0];
        let stats = calculate_percentiles(&mut values);

        assert_eq!(stats.min, 42.0);
        assert_eq!(stats.p50, 42.0);
        assert_eq!(stats.p90, 42.0);
        assert_eq!(stats.p99, 42.0);
        assert_eq!(stats.p100, 42.0);
    }

    #[test]
    fn test_percentile_calculation_two_values_interpolation() {
        // Test two values (verify interpolation)
        let mut values = vec![0.0, 100.0];
        let stats = calculate_percentiles(&mut values);

        assert_eq!(stats.min, 0.0);
        assert_eq!(stats.p100, 100.0);
        // p50 should be 50.0 (linear interpolation between 0 and 100)
        assert_eq!(stats.p50, 50.0);
        // p90 should be 90.0
        assert_eq!(stats.p90, 90.0);
        // p99 should be 99.0
        assert_eq!(stats.p99, 99.0);
    }

    #[test]
    fn test_percentile_calculation_known_dataset() {
        // Test known dataset with expected percentiles
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let stats = calculate_percentiles(&mut values);

        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.p100, 10.0);
        // p50 of 10 values: index = 0.5 * 9 = 4.5, interpolate between values[4]=5 and values[5]=6
        assert!((stats.p50 - 5.5).abs() < 0.01);
    }

    #[test]
    fn test_percentile_min_equals_minimum() {
        // Verify min = minimum
        let mut values = vec![100.0, 50.0, 25.0, 75.0, 10.0];
        let stats = calculate_percentiles(&mut values);

        assert_eq!(stats.min, 10.0);
    }

    #[test]
    fn test_percentile_p100_equals_maximum() {
        // Verify p100 = maximum
        let mut values = vec![100.0, 50.0, 25.0, 75.0, 10.0];
        let stats = calculate_percentiles(&mut values);

        assert_eq!(stats.p100, 100.0);
    }

    #[test]
    fn test_percentile_ordering() {
        // Verify ordering: min ≤ p50 ≤ p90 ≤ p99 ≤ p100
        let mut values = vec![1.0, 5.0, 10.0, 50.0, 100.0];
        let stats = calculate_percentiles(&mut values);

        assert!(stats.min <= stats.p50);
        assert!(stats.p50 <= stats.p90);
        assert!(stats.p90 <= stats.p99);
        assert!(stats.p99 <= stats.p100);
    }

    #[test]
    fn test_percentile_ordering_with_duplicates() {
        // Verify ordering with duplicate values
        let mut values = vec![5.0, 5.0, 5.0, 5.0, 5.0];
        let stats = calculate_percentiles(&mut values);

        assert_eq!(stats.min, 5.0);
        assert_eq!(stats.p50, 5.0);
        assert_eq!(stats.p90, 5.0);
        assert_eq!(stats.p99, 5.0);
        assert_eq!(stats.p100, 5.0);
    }

    // ==================== Eviction Tests (6.3) ====================

    #[test]
    fn test_eviction_at_capacity() {
        // Test fill cache to capacity, add new key, verify eviction output
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "3".to_string(), // Small cache size
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        // Fill cache to capacity
        for i in 0..3 {
            let event = create_test_event(&format!("key_{}", i), (i as f64) * 10.0);
            let result = processor.process(event).unwrap();
            assert!(result.is_empty()); // No eviction yet
        }

        // Add new key - should trigger eviction
        let event = create_test_event("key_3", 30.0);
        let result = processor.process(event).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(processor.output_count, 1);
    }

    #[test]
    fn test_evicted_key_is_lru() {
        // Test evicted key is the least recently used
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "3".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        // Add keys in order: key_0, key_1, key_2
        for i in 0..3 {
            let event = create_test_event(&format!("key_{}", i), (i as f64) * 10.0);
            processor.process(event).unwrap();
        }

        // Access key_0 to make it recently used
        let event = create_test_event("key_0", 5.0);
        processor.process(event).unwrap();

        // Add new key - should evict key_1 (LRU)
        let event = create_test_event("key_3", 30.0);
        let result = processor.process(event).unwrap();

        assert_eq!(result.len(), 1);
        let evicted_key = get_key(&result[0], "key");
        assert_eq!(evicted_key, "key_1");
    }

    #[test]
    fn test_eviction_event_format() {
        // Test eviction event format (key_label and stats_label fields)
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "1".to_string(), // Cache size of 1 for immediate eviction
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        // Add first key
        let event = create_test_event("first_key", 42.0);
        processor.process(event).unwrap();

        // Add second key - should evict first_key
        let event = create_test_event("second_key", 100.0);
        let result = processor.process(event).unwrap();

        assert_eq!(result.len(), 1);

        // Verify event format
        let output = result[0].borrow();
        assert!(output.get("key").is_some());
        assert!(output.get("percentiles").is_some());

        let percentiles = output.get("percentiles").unwrap();
        assert!(percentiles.get("min").is_some());
        assert!(percentiles.get("p50").is_some());
        assert!(percentiles.get("p90").is_some());
        assert!(percentiles.get("p99").is_some());
        assert!(percentiles.get("p100").is_some());
    }

    // ==================== Flush Tests (6.4) ====================

    #[test]
    fn test_flush_no_events_processed() {
        // Test flush with no events processed (empty result)
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        let flushed = processor.flush();
        assert!(flushed.is_empty());
    }

    #[test]
    fn test_flush_single_key() {
        // Test flush with single key (one event)
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        let event = create_test_event("single_key", 42.0);
        processor.process(event).unwrap();

        let flushed = processor.flush();
        assert_eq!(flushed.len(), 1);

        let key = get_key(&flushed[0], "key");
        assert_eq!(key, "single_key");
    }

    #[test]
    fn test_flush_multiple_keys() {
        // Test flush with multiple keys (multiple events)
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        // Add events with different keys
        for i in 0..5 {
            let event = create_test_event(&format!("key_{}", i), (i as f64) * 10.0);
            processor.process(event).unwrap();
        }

        let flushed = processor.flush();
        assert_eq!(flushed.len(), 5);
        assert_eq!(processor.output_count, 5);
    }

    #[test]
    fn test_double_flush_second_empty() {
        // Test double flush (second returns empty)
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        // Add some events
        for i in 0..3 {
            let event = create_test_event(&format!("key_{}", i), (i as f64) * 10.0);
            processor.process(event).unwrap();
        }

        // First flush
        let first_flush = processor.flush();
        assert_eq!(first_flush.len(), 3);

        // Second flush should be empty
        let second_flush = processor.flush();
        assert!(second_flush.is_empty());
    }

    // ==================== Configuration Tests (6.5) ====================

    #[test]
    fn test_custom_key_label() {
        // Test custom key_label
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-l".to_string(),
            "custom_key".to_string(),
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        let event = create_test_event("test_key", 42.0);
        processor.process(event).unwrap();

        let flushed = processor.flush();
        assert_eq!(flushed.len(), 1);

        // Verify custom key label is used
        let output = flushed[0].borrow();
        assert!(output.get("custom_key").is_some());
        assert!(output.get("key").is_none());
    }

    #[test]
    fn test_custom_stats_label() {
        // Test custom stats_label
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-s".to_string(),
            "custom_stats".to_string(),
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        let event = create_test_event("test_key", 42.0);
        processor.process(event).unwrap();

        let flushed = processor.flush();
        assert_eq!(flushed.len(), 1);

        // Verify custom stats label is used
        let output = flushed[0].borrow();
        assert!(output.get("custom_stats").is_some());
        assert!(output.get("percentiles").is_none());
    }

    #[test]
    fn test_custom_cache_size() {
        // Test custom cache_size
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "2".to_string(), // Very small cache
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        // Add 3 keys - should trigger eviction on the third
        let event1 = create_test_event("key_1", 10.0);
        let result1 = processor.process(event1).unwrap();
        assert!(result1.is_empty());

        let event2 = create_test_event("key_2", 20.0);
        let result2 = processor.process(event2).unwrap();
        assert!(result2.is_empty());

        let event3 = create_test_event("key_3", 30.0);
        let result3 = processor.process(event3).unwrap();
        assert_eq!(result3.len(), 1); // Eviction occurred
    }

    #[test]
    fn test_default_values() {
        // Test default values
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        let event = create_test_event("test_key", 42.0);
        processor.process(event).unwrap();

        let flushed = processor.flush();
        assert_eq!(flushed.len(), 1);

        // Verify default labels are used
        let output = flushed[0].borrow();
        assert!(output.get("key").is_some()); // Default key_label
        assert!(output.get("percentiles").is_some()); // Default stats_label
    }

    #[test]
    fn test_no_key_path_uses_total() {
        // Test no key path (uses "total")
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        // Add multiple events - all should go to "total" key
        for i in 1..=5 {
            let event = create_test_event("any_key", i as f64 * 10.0);
            processor.process(event).unwrap();
        }

        let flushed = processor.flush();
        assert_eq!(flushed.len(), 1);

        let key = get_key(&flushed[0], "key");
        assert_eq!(key, "total");

        // Verify all values were accumulated under "total"
        let (min, _p50, _p90, _p99, p100) = get_percentiles(&flushed[0], "percentiles");
        assert_eq!(min, 10.0);
        assert_eq!(p100, 50.0);
    }

    #[test]
    fn test_stats_output() {
        // Test stats() method
        let args = vec![
            "keypercentile".to_string(),
            "value".to_string(),
            "-k".to_string(),
            "key".to_string(),
            "-c".to_string(),
            "10".to_string(),
        ];
        let mut processor = KeyPercentileProcessor::new(&args).unwrap();

        // Process some events
        for i in 0..5 {
            let event = create_test_event(&format!("key_{}", i), (i as f64) * 10.0);
            processor.process(event).unwrap();
        }

        processor.flush();

        let stats = processor.stats().unwrap();
        assert!(stats.contains("input: 5"));
        assert!(stats.contains("output: 5"));
    }
}
