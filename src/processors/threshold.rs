use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use std::num::NonZeroUsize;

const CACHE_SIZE: usize = 200000;

#[derive(SerialProcessorInit)]
pub struct ThresholdProcessor<'a> {
    key_path: jmespath::Expression<'a>,
    value_path: Option<jmespath::Expression<'a>>,
    threshold: f64,
    state: LruCache<String, f64>,
    threshold_label: String,
    reset_on_threshold: bool,
    input_count: u64,
    output_count: u64,
    missing_key_count: u64,
    missing_value_count: u64,
}

#[derive(Parser)]
/// Accumulate values by key and emit events when threshold is reached
#[command(version, long_about = None, arg_required_else_help(true))]
struct ThresholdArgs {
    #[arg(required(true))]
    key: Vec<String>,

    #[arg(short, long)]
    value_path: Option<String>,

    #[arg(short, long)]
    threshold: f64,

    #[arg(short = 'l', long, default_value = "threshold_value")]
    threshold_label: String,

    #[arg(short, long, default_value_t = CACHE_SIZE)]
    cache_size: usize,

    #[arg(short, long)]
    reset_on_threshold: bool,
}

impl SerialProcessor for ThresholdProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("sum values by key and emit events when threshold is reached".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = ThresholdArgs::try_parse_from(argv)?;
        let key_path = jmespath::compile(&args.key.join(" "))?;

        let value_path = match args.value_path {
            Some(path) => Some(jmespath::compile(&path)?),
            None => None,
        };

        Ok(Self {
            key_path,
            value_path,
            threshold: args.threshold,
            state: LruCache::new(NonZeroUsize::new(args.cache_size).expect("non-zero cache size")),
            threshold_label: args.threshold_label,
            reset_on_threshold: args.reset_on_threshold,
            input_count: 0,
            output_count: 0,
            missing_key_count: 0,
            missing_value_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        // Extract the key from the event
        let key = self.key_path.search(&input)?;
        if key.is_null() {
            self.missing_key_count += 1;
            return Ok(vec![]);
        }

        let keystr = key.to_string();

        // Extract the value from the event (or use 1.0 as default)
        let value = match &self.value_path {
            Some(value_path) => {
                let value_result = value_path.search(&input)?;
                if value_result.is_null() {
                    self.missing_value_count += 1;
                    return Ok(vec![]);
                }

                // Try to convert to f64
                match value_result.as_number() {
                    Some(v) => v,
                    None => {
                        // Try to parse as string if it's not a number
                        if value_result.is_string() {
                            match value_result.as_string().expect("is string").parse::<f64>() {
                                Ok(v) => v,
                                Err(_) => {
                                    self.missing_value_count += 1;
                                    return Ok(vec![]);
                                }
                            }
                        } else {
                            self.missing_value_count += 1;
                            return Ok(vec![]);
                        }
                    }
                }
            }
            None => 1.0, // Default counter value
        };

        // Get current state for this key and add the new value
        let current_value = self.state.get(&keystr).copied().unwrap_or(0.0);
        let new_value = current_value + value;

        // Check if threshold is reached
        if new_value >= self.threshold {
            // Add the threshold value to the event
            if let Some(event_obj) = input.borrow_mut().as_object_mut() {
                event_obj.insert(self.threshold_label.clone(), serde_json::to_value(new_value)?);
            }

            // Update the state - either reset to 0 or keep the accumulated value
            if self.reset_on_threshold {
                self.state.put(keystr, 0.0);
            } else {
                self.state.put(keystr, new_value);
            }

            self.output_count += 1;
            Ok(vec![input])
        } else {
            // Threshold not reached, update state and don't emit the event
            self.state.put(keystr, new_value);
            Ok(vec![])
        }
    }

    fn flush(&mut self) -> Vec<Event> {
        // No buffering in this processor, so nothing to flush
        vec![]
    }

    fn stats(&self) -> Option<String> {
        Some(format!(
            "input:{}\noutput:{}\nmissing_key:{}\nmissing_value:{}",
            self.input_count, self.output_count, self.missing_key_count, self.missing_value_count
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    // Helper function to create a test event
    fn create_test_event(key: &str, value: Option<f64>) -> Event {
        let mut map = serde_json::Map::new();
        map.insert("key".to_string(), serde_json::Value::String(key.to_string()));

        if let Some(v) = value {
            map.insert(
                "value".to_string(),
                serde_json::Value::Number(serde_json::Number::from_f64(v).unwrap()),
            );
        }

        Rc::new(RefCell::new(serde_json::Value::Object(map)))
    }

    // Helper function to extract threshold value from an event
    fn get_threshold_value(event: &Event, label: &str) -> Option<f64> {
        event.borrow().get(label).and_then(|v| v.as_f64())
    }

    #[test]
    fn test_basic_functionality() {
        let args = vec![
            "threshold".to_string(),
            "key".to_string(),
            "--threshold".to_string(),
            "3".to_string(),
        ];
        let mut processor = ThresholdProcessor::new(&args).unwrap();

        // Test counter mode and multiple keys
        let events = vec![
            create_test_event("key1", None), // key1: 1 < 3
            create_test_event("key2", None), // key2: 1 < 3
            create_test_event("key1", None), // key1: 2 < 3
            create_test_event("key1", None), // key1: 3 >= 3, triggers
            create_test_event("key2", None), // key2: 2 < 3
            create_test_event("key2", None), // key2: 3 >= 3, triggers
        ];

        let results: Vec<_> = events.into_iter().map(|e| processor.process(e).unwrap()).collect();

        assert!(results[0].is_empty());
        assert!(results[1].is_empty());
        assert!(results[2].is_empty());
        assert_eq!(results[3].len(), 1);
        assert_eq!(get_threshold_value(&results[3][0], "threshold_value"), Some(3.0));
        assert!(results[4].is_empty());
        assert_eq!(results[5].len(), 1);
        assert_eq!(get_threshold_value(&results[5][0], "threshold_value"), Some(3.0));
    }

    #[test]
    fn test_value_accumulation_with_negatives() {
        let args = vec![
            "threshold".to_string(),
            "key".to_string(),
            "--value-path".to_string(),
            "value".to_string(),
            "--threshold".to_string(),
            "10".to_string(),
        ];
        let mut processor = ThresholdProcessor::new(&args).unwrap();

        // Test value accumulation including negative values
        let events = vec![
            create_test_event("test_key", Some(15.0)), // 15 >= 10, triggers
            create_test_event("test_key", Some(-3.0)), // 12 >= 10, triggers
            create_test_event("test_key", Some(-8.0)), // 4 < 10, no trigger
            create_test_event("test_key", Some(7.0)),  // 11 >= 10, triggers
        ];

        let results: Vec<_> = events.into_iter().map(|e| processor.process(e).unwrap()).collect();

        assert_eq!(results[0].len(), 1);
        assert_eq!(get_threshold_value(&results[0][0], "threshold_value"), Some(15.0));
        assert_eq!(results[1].len(), 1);
        assert_eq!(get_threshold_value(&results[1][0], "threshold_value"), Some(12.0));
        assert!(results[2].is_empty());
        assert_eq!(results[3].len(), 1);
        assert_eq!(get_threshold_value(&results[3][0], "threshold_value"), Some(11.0));
    }

    #[test]
    fn test_reset_behavior() {
        // Test with reset
        let args_reset = vec![
            "threshold".to_string(),
            "key".to_string(),
            "--threshold".to_string(),
            "3".to_string(),
            "--reset-on-threshold".to_string(),
        ];
        let mut processor_reset = ThresholdProcessor::new(&args_reset).unwrap();

        // Test without reset
        let args_no_reset = vec![
            "threshold".to_string(),
            "key".to_string(),
            "--threshold".to_string(),
            "3".to_string(),
        ];
        let mut processor_no_reset = ThresholdProcessor::new(&args_no_reset).unwrap();

        // Process same events with both processors
        for _ in 0..4 {
            let event_reset = create_test_event("test_key", None);
            let event_no_reset = create_test_event("test_key", None);

            let result_reset = processor_reset.process(event_reset).unwrap();
            let result_no_reset = processor_no_reset.process(event_no_reset).unwrap();

            // Both should trigger on 3rd event
            if processor_reset.input_count == 3 {
                assert_eq!(result_reset.len(), 1);
                assert_eq!(result_no_reset.len(), 1);
            }
            // 4th event: reset processor shouldn't trigger, no-reset should
            else if processor_reset.input_count == 4 {
                assert!(result_reset.is_empty()); // Reset to 0, now at 1
                assert_eq!(result_no_reset.len(), 1); // Continues from 3 to 4
                assert_eq!(get_threshold_value(&result_no_reset[0], "threshold_value"), Some(4.0));
            }
        }
    }

    #[test]
    fn test_error_conditions() {
        // Test missing key
        let args = vec![
            "threshold".to_string(),
            "missing_key".to_string(),
            "--threshold".to_string(),
            "1".to_string(),
        ];
        let mut processor = ThresholdProcessor::new(&args).unwrap();
        let event = create_test_event("key", None);
        let result = processor.process(event).unwrap();
        assert!(result.is_empty());
        assert_eq!(processor.missing_key_count, 1);

        // Test missing value
        let args = vec![
            "threshold".to_string(),
            "key".to_string(),
            "--value-path".to_string(),
            "missing_value".to_string(),
            "--threshold".to_string(),
            "1".to_string(),
        ];
        let mut processor = ThresholdProcessor::new(&args).unwrap();
        let event = create_test_event("test_key", None);
        let result = processor.process(event).unwrap();
        assert!(result.is_empty());
        assert_eq!(processor.missing_value_count, 1);
    }
}
