use crate::processors::processor::*;
use clap::Parser;
use clap::ValueEnum;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use std::num::NonZeroUsize;

const CACHE_SIZE: usize = 200000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum AnomalyMethod {
    Zscore,
    Iqr,
}

#[derive(Parser)]
/// Detect anomalies using Z-score or IQR methods
#[command(
    version,
    long_about = None,
    arg_required_else_help(true),
    after_help = r#"METHODS:

  zscore (default)  Mark events where |z-score| exceeds --threshold.
                    Z-score = (value - mean) / std_dev.
                    Groups with one event or zero std dev are never anomalous.

  iqr               Mark events outside Q1 - m*IQR .. Q3 + m*IQR where m
                    is --multiplier. Groups with fewer than 4 events are
                    never anomalous.

OUTPUT FIELDS:

  is_anomaly      boolean indicating whether the event is an outlier
  anomaly_score   absolute z-score (zscore) or distance from nearest
                  bound (iqr); 0.0 when not anomalous

EXAMPLES:

  Detect anomalous abuse scores per account using Z-score:
    anomaly score -k account --method zscore --threshold 3

  Detect outliers using IQR with custom multiplier:
    anomaly score -k account --method iqr --multiplier 1.5

  Global anomaly detection (no grouping key):
    anomaly score --method zscore --threshold 2.5

INPUT:
  {"account": "user_123", "score": 95}

OUTPUT (zscore, anomalous):
  {"account": "user_123", "score": 95, "is_anomaly": true, "anomaly_score": 3.5}

OUTPUT (iqr, within bounds):
  {"account": "user_456", "score": 50, "is_anomaly": false, "anomaly_score": 0.0}
"#,
)]
struct AnomalyProcessorArgs {
    /// JMESPath expression to extract the numeric value to analyze
    #[arg(required(true))]
    value: Vec<String>,

    /// JMESPath expression to extract the grouping key from each event
    #[arg(short, long, value_name = "EXPR")]
    key: Option<String>,

    /// Statistical method for anomaly detection
    #[arg(long, value_enum, default_value = "zscore")]
    method: AnomalyMethod,

    /// Z-score threshold beyond which a value is anomalous (zscore method)
    #[arg(long, default_value_t = 3.0, value_name = "NUM")]
    threshold: f64,

    /// IQR multiplier for calculating anomaly bounds (iqr method)
    #[arg(long, default_value_t = 1.5, value_name = "NUM")]
    multiplier: f64,

    /// LRU cache capacity for group state management
    #[arg(short, long, default_value_t = CACHE_SIZE, value_name = "N")]
    cache_size: usize,
}

#[derive(SerialProcessorInit)]
pub struct AnomalyProcessor<'a> {
    value_path: jmespath::Expression<'a>,
    key_path: Option<jmespath::Expression<'a>>,
    method: AnomalyMethod,
    threshold: f64,
    multiplier: f64,
    state: LruCache<String, Vec<(Event, f64)>>,
    input_count: u64,
    anomaly_count: u64,
    group_count: u64,
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

fn calculate_zscore_stats(values: &[f64]) -> Option<(f64, f64)> {
    if values.len() < 2 {
        return None;
    }

    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
    let std_dev = variance.sqrt();

    if std_dev == 0.0 {
        return None;
    }

    Some((mean, std_dev))
}

fn calculate_iqr_stats(values: &mut [f64], multiplier: f64) -> Option<(f64, f64, f64)> {
    if values.len() < 4 {
        return None;
    }

    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let q1 = percentile_at(values, 0.25);
    let q3 = percentile_at(values, 0.75);
    let iqr = q3 - q1;

    let lower_bound = q1 - (multiplier * iqr);
    let upper_bound = q3 + (multiplier * iqr);

    Some((lower_bound, upper_bound, iqr))
}

impl SerialProcessor for AnomalyProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("detect anomalies using Z-score or IQR methods".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = AnomalyProcessorArgs::try_parse_from(argv)?;
        let value_path = jmespath::compile(&args.value.join(" "))?;

        let key_path = match args.key {
            Some(key) => Some(jmespath::compile(&key)?),
            _ => None,
        };

        Ok(AnomalyProcessor {
            value_path,
            key_path,
            method: args.method,
            threshold: args.threshold,
            multiplier: args.multiplier,
            state: LruCache::new(NonZeroUsize::new(args.cache_size).expect("non-zero cache size")),
            input_count: 0,
            anomaly_count: 0,
            group_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
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

        self.input_count += 1;

        if let Some(values) = self.state.get_mut(&key) {
            values.push((input, value));
            return Ok(vec![]);
        }

        self.state.push(key, vec![(input, value)]);

        Ok(vec![])
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events: Vec<Event> = vec![];

        while let Some((_key, group)) = self.state.pop_lru() {
            self.group_count += 1;

            match self.method {
                AnomalyMethod::Zscore => {
                    let values: Vec<f64> = group.iter().map(|(_, v)| *v).collect();
                    let stats = calculate_zscore_stats(&values);

                    for (event, val) in &group {
                        let (is_anomaly, score) = match stats {
                            Some((mean, std_dev)) => {
                                let zscore = (val - mean) / std_dev;
                                let abs_zscore = zscore.abs();
                                (abs_zscore > self.threshold, abs_zscore)
                            }
                            None => (false, 0.0),
                        };

                        if is_anomaly {
                            self.anomaly_count += 1;
                        }

                        if let Ok(mut borrowed) = event.try_borrow_mut() {
                            if let Some(obj) = borrowed.as_object_mut() {
                                obj.insert(
                                    "is_anomaly".to_string(),
                                    serde_json::Value::Bool(is_anomaly),
                                );
                                obj.insert("anomaly_score".to_string(), serde_json::json!(score));
                            }
                        }

                        events.push(event.clone());
                    }
                }
                AnomalyMethod::Iqr => {
                    let mut values: Vec<f64> = group.iter().map(|(_, v)| *v).collect();
                    let stats = calculate_iqr_stats(&mut values, self.multiplier);

                    for (event, val) in &group {
                        let (is_anomaly, score) = match stats {
                            Some((lower_bound, upper_bound, _iqr)) => {
                                if *val < lower_bound {
                                    (true, lower_bound - val)
                                } else if *val > upper_bound {
                                    (true, val - upper_bound)
                                } else {
                                    (false, 0.0)
                                }
                            }
                            None => (false, 0.0),
                        };

                        if is_anomaly {
                            self.anomaly_count += 1;
                        }

                        if let Ok(mut borrowed) = event.try_borrow_mut() {
                            if let Some(obj) = borrowed.as_object_mut() {
                                obj.insert(
                                    "is_anomaly".to_string(),
                                    serde_json::Value::Bool(is_anomaly),
                                );
                                obj.insert("anomaly_score".to_string(), serde_json::json!(score));
                            }
                        }

                        events.push(event.clone());
                    }
                }
            }
        }

        events
    }

    fn stats(&self) -> Option<String> {
        Some(format!(
            "input: {}, anomalies: {}, groups: {}",
            self.input_count, self.anomaly_count, self.group_count
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn create_test_event(key: &str, value: f64) -> Event {
        Rc::new(RefCell::new(serde_json::json!({
            "key": key,
            "value": value
        })))
    }

    fn create_custom_event(fields: serde_json::Value) -> Event {
        Rc::new(RefCell::new(fields))
    }

    fn new_processor(args: &[&str]) -> AnomalyProcessor<'static> {
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        AnomalyProcessor::new(&args).unwrap()
    }

    // ==================== 6.1 Basic Functionality Tests ====================

    #[test]
    fn test_single_event_valid_value_stored_empty_result() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        let event = create_test_event("k1", 42.0);
        let result = proc.process(event).unwrap();
        assert!(result.is_empty());
        assert_eq!(proc.input_count, 1);
    }

    #[test]
    fn test_multiple_events_same_key_grouped() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        for i in 0..5 {
            let event = create_test_event("same", i as f64 * 10.0);
            assert!(proc.process(event).unwrap().is_empty());
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 5);
    }

    #[test]
    fn test_null_jmespath_result_skipped() {
        let mut proc = new_processor(&["anomaly", "missing_field", "-k", "key"]);
        let event = create_test_event("k1", 42.0);
        let result = proc.process(event).unwrap();
        assert!(result.is_empty());
        assert_eq!(proc.input_count, 0);
        assert!(proc.flush().is_empty());
    }

    #[test]
    fn test_non_numeric_value_skipped() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        let event = create_custom_event(serde_json::json!({
            "key": "k1",
            "value": "not_a_number"
        }));
        let result = proc.process(event).unwrap();
        assert!(result.is_empty());
        assert_eq!(proc.input_count, 0);
        assert!(proc.flush().is_empty());
    }

    #[test]
    fn test_null_key_skipped() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "missing_key"]);
        let event = create_test_event("k1", 42.0);
        let result = proc.process(event).unwrap();
        assert!(result.is_empty());
        assert_eq!(proc.input_count, 0);
        assert!(proc.flush().is_empty());
    }

    // ==================== 6.2 Z-score Method Tests ====================

    #[test]
    fn test_zscore_known_dataset() {
        // Dataset: [10, 20, 30, 40, 100]
        // mean = 40, std_dev = sqrt(((−30)²+(−20)²+(−10)²+0²+60²)/5) = sqrt(1000) ≈ 31.623
        // zscore(100) = (100−40)/31.623 ≈ 1.897
        let mut proc = new_processor(&["anomaly", "value", "-k", "key", "--threshold", "1.5"]);
        let values = [10.0, 20.0, 30.0, 40.0, 100.0];
        for v in &values {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 5);

        // 100 should be anomalous with threshold 1.5
        let last = flushed[4].borrow();
        assert!(last.get("is_anomaly").unwrap().as_bool().unwrap());
        let score = last.get("anomaly_score").unwrap().as_f64().unwrap();
        assert!((score - 1.897).abs() < 0.01);
    }

    #[test]
    fn test_zscore_threshold_boundary() {
        // 5 values: [1, 2, 3, 4, 5], mean=3, std_dev=sqrt(2)≈1.414
        // zscore(5) = (5-3)/1.414 ≈ 1.414
        // With threshold exactly 1.414, value should NOT be anomaly (not strictly greater)
        let mut proc = new_processor(&["anomaly", "value", "-k", "key", "--threshold", "1.42"]);
        for v in 1..=5 {
            proc.process(create_test_event("g", v as f64)).unwrap();
        }
        let flushed = proc.flush();
        // zscore(5) ≈ 1.414 < 1.42, so not anomaly
        let last = flushed[4].borrow();
        assert!(!last.get("is_anomaly").unwrap().as_bool().unwrap());
    }

    #[test]
    fn test_zscore_single_event_not_anomaly() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        proc.process(create_test_event("g", 999.0)).unwrap();
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 1);
        let e = flushed[0].borrow();
        assert!(!e.get("is_anomaly").unwrap().as_bool().unwrap());
        assert_eq!(e.get("anomaly_score").unwrap().as_f64().unwrap(), 0.0);
    }

    #[test]
    fn test_zscore_all_identical_not_anomaly() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        for _ in 0..5 {
            proc.process(create_test_event("g", 42.0)).unwrap();
        }
        let flushed = proc.flush();
        for ev in &flushed {
            let e = ev.borrow();
            assert!(!e.get("is_anomaly").unwrap().as_bool().unwrap());
            assert_eq!(e.get("anomaly_score").unwrap().as_f64().unwrap(), 0.0);
        }
    }

    // ==================== 6.3 IQR Method Tests ====================

    #[test]
    fn test_iqr_known_dataset() {
        // Dataset: [1, 2, 3, 4, 5, 6, 7, 8, 9, 100]
        // sorted: [1,2,3,4,5,6,7,8,9,100]
        // Q1 = percentile_at(0.25) index=2.25 → 3*0.75 + 4*0.25 = 3.25
        // Q3 = percentile_at(0.75) index=6.75 → 7*0.25 + 8*0.75 = 7.75
        // IQR = 4.5, lower = 3.25 - 1.5*4.5 = -3.5, upper = 7.75 + 1.5*4.5 = 14.5
        // 100 > 14.5 → anomaly, score = 100 - 14.5 = 85.5
        let mut proc = new_processor(&["anomaly", "value", "-k", "key", "--method", "iqr"]);
        let values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 100.0];
        for v in &values {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 10);

        let last = flushed[9].borrow();
        assert!(last.get("is_anomaly").unwrap().as_bool().unwrap());
        let score = last.get("anomaly_score").unwrap().as_f64().unwrap();
        assert!((score - 85.5).abs() < 0.01);
    }

    #[test]
    fn test_iqr_value_at_boundary_not_anomaly() {
        // [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        // Q1=3.25, Q3=7.75, IQR=4.5, lower=-3.5, upper=14.5
        // 14.5 is exactly at upper bound → not anomaly
        let mut proc = new_processor(&["anomaly", "value", "-k", "key", "--method", "iqr"]);
        let values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 14.5];
        for v in &values {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        // 14.5 should be at or within bounds (not strictly greater)
        // Recalculate with 14.5 in the dataset:
        // sorted: [1,2,3,4,5,6,7,8,9,14.5]
        // Q1 index=2.25 → 3*0.75+4*0.25=3.25
        // Q3 index=6.75 → 7*0.25+8*0.75=7.75
        // upper = 7.75+6.75 = 14.5
        // 14.5 is NOT > 14.5, so not anomaly
        let last = flushed[9].borrow();
        assert!(!last.get("is_anomaly").unwrap().as_bool().unwrap());
    }

    #[test]
    fn test_iqr_fewer_than_4_events_not_anomaly() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key", "--method", "iqr"]);
        for v in &[1.0, 2.0, 1000.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 3);
        for ev in &flushed {
            let e = ev.borrow();
            assert!(!e.get("is_anomaly").unwrap().as_bool().unwrap());
            assert_eq!(e.get("anomaly_score").unwrap().as_f64().unwrap(), 0.0);
        }
    }

    #[test]
    fn test_iqr_outlier_below_lower_bound() {
        // [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] + outlier -100
        // Need to ensure -100 is below lower bound
        let mut proc = new_processor(&["anomaly", "value", "-k", "key", "--method", "iqr"]);
        let values = [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, -100.0];
        for v in &values {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        // Find the -100 event and verify it's anomalous
        let outlier = flushed
            .iter()
            .find(|e| e.borrow().get("value").unwrap().as_f64().unwrap() == -100.0)
            .unwrap();
        let e = outlier.borrow();
        assert!(e.get("is_anomaly").unwrap().as_bool().unwrap());
        assert!(e.get("anomaly_score").unwrap().as_f64().unwrap() > 0.0);
    }

    // ==================== 6.4 Flush Tests ====================

    #[test]
    fn test_flush_no_events_empty() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        assert!(proc.flush().is_empty());
    }

    #[test]
    fn test_flush_single_group_all_returned() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        for v in &[10.0, 20.0, 30.0] {
            proc.process(create_test_event("g1", *v)).unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 3);
    }

    #[test]
    fn test_flush_multiple_groups_all_returned() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        for i in 0..3 {
            for v in &[10.0, 20.0, 30.0] {
                proc.process(create_test_event(&format!("g{}", i), *v)).unwrap();
            }
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 9);
        assert_eq!(proc.group_count, 3);
    }

    #[test]
    fn test_double_flush_second_empty() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        for v in &[10.0, 20.0, 30.0] {
            proc.process(create_test_event("g1", *v)).unwrap();
        }
        let first = proc.flush();
        assert_eq!(first.len(), 3);
        let second = proc.flush();
        assert!(second.is_empty());
    }

    // ==================== 6.5 Configuration Tests ====================

    #[test]
    fn test_custom_threshold() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key", "--threshold", "0.5"]);
        // [1, 2, 3, 4, 100] with low threshold should flag more anomalies
        for v in &[1.0, 2.0, 3.0, 4.0, 100.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        let anomaly_count = flushed
            .iter()
            .filter(|e| e.borrow().get("is_anomaly").unwrap().as_bool().unwrap())
            .count();
        assert!(anomaly_count > 0);
    }

    #[test]
    fn test_custom_multiplier() {
        // With very large multiplier, nothing should be anomalous
        let mut proc = new_processor(&[
            "anomaly",
            "value",
            "-k",
            "key",
            "--method",
            "iqr",
            "--multiplier",
            "100.0",
        ]);
        for v in &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 100.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        let anomaly_count = flushed
            .iter()
            .filter(|e| e.borrow().get("is_anomaly").unwrap().as_bool().unwrap())
            .count();
        assert_eq!(anomaly_count, 0);
    }

    #[test]
    fn test_custom_cache_size() {
        let proc = new_processor(&["anomaly", "value", "-k", "key", "-c", "5"]);
        assert_eq!(proc.state.cap().get(), 5);
    }

    #[test]
    fn test_default_values() {
        let proc = new_processor(&["anomaly", "value", "-k", "key"]);
        assert_eq!(proc.method, AnomalyMethod::Zscore);
        assert_eq!(proc.threshold, 3.0);
        assert_eq!(proc.multiplier, 1.5);
    }

    #[test]
    fn test_no_key_path_global_grouping() {
        let mut proc = new_processor(&["anomaly", "value"]);
        // Events with different "key" values should all go to "global" group
        for i in 0..5 {
            proc.process(create_test_event(&format!("k{}", i), i as f64 * 10.0)).unwrap();
        }
        let flushed = proc.flush();
        assert_eq!(flushed.len(), 5);
        assert_eq!(proc.group_count, 1);
    }

    // ==================== 6.6 Output Format Tests ====================

    #[test]
    fn test_output_has_is_anomaly_bool() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        for v in &[1.0, 2.0, 3.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        for ev in &flushed {
            let e = ev.borrow();
            let field = e.get("is_anomaly").expect("is_anomaly field missing");
            assert!(field.is_boolean());
        }
    }

    #[test]
    fn test_output_has_anomaly_score_f64() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        for v in &[1.0, 2.0, 3.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        let flushed = proc.flush();
        for ev in &flushed {
            let e = ev.borrow();
            let field = e.get("anomaly_score").expect("anomaly_score field missing");
            assert!(field.is_number());
        }
    }

    #[test]
    fn test_original_fields_preserved() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        proc.process(create_custom_event(serde_json::json!({
            "key": "k1",
            "value": 42.0,
            "extra": "preserved"
        })))
        .unwrap();
        let flushed = proc.flush();
        let e = flushed[0].borrow();
        assert_eq!(e.get("key").unwrap().as_str().unwrap(), "k1");
        assert_eq!(e.get("value").unwrap().as_f64().unwrap(), 42.0);
        assert_eq!(e.get("extra").unwrap().as_str().unwrap(), "preserved");
        assert!(e.get("is_anomaly").is_some());
        assert!(e.get("anomaly_score").is_some());
    }

    // ==================== Stats Test ====================

    #[test]
    fn test_stats_output() {
        let mut proc = new_processor(&["anomaly", "value", "-k", "key"]);
        for v in &[1.0, 2.0, 3.0] {
            proc.process(create_test_event("g", *v)).unwrap();
        }
        proc.flush();
        let stats = proc.stats().unwrap();
        assert!(stats.contains("input: 3"));
        assert!(stats.contains("groups: 1"));
    }
}
