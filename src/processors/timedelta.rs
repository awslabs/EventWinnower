use crate::processors::processor::*;
use anyhow::Result;
use chrono::DateTime;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use std::num::NonZeroUsize;

const CACHE_SIZE: usize = 200000;

/// Generic function to extract timestamp from an event using JMESPath
/// Returns Some(timestamp_milliseconds) on success, None on failure
/// Uses millisecond precision for sub-second timing
pub fn extract_timestamp_from_event(
    event: &Event,
    time_path: &jmespath::Expression,
    time_format: Option<&str>,
) -> Option<u64> {
    // Search for the timestamp field in the event
    let event_timestring = match time_path.search(event) {
        Ok(timestring) => timestring,
        _ => return None,
    };

    // Check if the timestamp field is null
    if event_timestring.is_null() {
        return None;
    }

    // Extract the string value
    let timestamp_str = event_timestring.as_string()?;

    // Parse the timestamp based on format
    match time_format {
        Some(format) => {
            // Use custom format with parse_from_str for NaiveDateTime
            match chrono::NaiveDateTime::parse_from_str(timestamp_str, format) {
                Ok(naive_dt) => {
                    let timestamp_millis = naive_dt.and_utc().timestamp_millis() as u64;
                    Some(timestamp_millis)
                }
                Err(_) => None,
            }
        }
        None => {
            // Use default RFC3339 format
            match DateTime::parse_from_rfc3339(timestamp_str) {
                Ok(dt) => {
                    let timestamp_millis = dt.timestamp_millis() as u64;
                    Some(timestamp_millis)
                }
                Err(_) => None,
            }
        }
    }
}

#[derive(SerialProcessorInit)]
pub struct TimeDeltaProcessor<'a> {
    key_path: jmespath::Expression<'a>,
    time_path: jmespath::Expression<'a>,
    time_format: Option<String>,
    state: LruCache<String, u64>,
    delta_label: String,
    emit_first: bool,
    input_count: u64,
    output_count: u64,
    missing_timestamp_count: u64,
    first_event_skipped_count: u64,
}

#[derive(Parser)]
/// Compute time deltas in milliseconds between events with the same key
#[command(version, long_about = None, arg_required_else_help(true))]
struct TimeDeltaArgs {
    #[arg(required(true))]
    key: Vec<String>,

    #[arg(short, long, default_value = "timestamp")]
    time_path: String,

    #[arg(short = 'f', long)]
    time_format: Option<String>,

    #[arg(short, long, default_value = "time_delta_milliseconds")]
    delta_label: String,

    #[arg(short, long)]
    emit_first: bool,

    #[arg(short, long, default_value_t = CACHE_SIZE)]
    cache_size: usize,
}

impl SerialProcessor for TimeDeltaProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("compute time deltas in milliseconds between events with the same key".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = TimeDeltaArgs::try_parse_from(argv)?;
        let key_path = jmespath::compile(&args.key.join(" "))?;

        let time_path = jmespath::compile(&args.time_path)?;

        Ok(Self {
            key_path,
            time_path,
            time_format: args.time_format,
            state: LruCache::new(NonZeroUsize::new(args.cache_size).expect("non-zero cache size")),
            delta_label: args.delta_label,
            emit_first: args.emit_first,
            input_count: 0,
            output_count: 0,
            missing_timestamp_count: 0,
            first_event_skipped_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        // Extract the key from the event
        let key = self.key_path.search(&input)?;
        if key.is_null() {
            return Ok(vec![]);
        }

        // Extract the timestamp from the event
        let event_time = match extract_timestamp_from_event(
            &input,
            &self.time_path,
            self.time_format.as_deref(),
        ) {
            Some(timestamp) => timestamp,
            None => {
                self.missing_timestamp_count += 1;
                return Ok(vec![]);
            }
        };

        let keystr = key.to_string();

        // Look up the previous timestamp for this key
        match self.state.get(&keystr) {
            Some(&previous_time) => {
                // Calculate time delta
                let time_delta = event_time.abs_diff(previous_time);

                // Update the state with the new timestamp
                self.state.put(keystr, event_time);

                // Add the time delta to the event
                if let Some(event_obj) = input.borrow_mut().as_object_mut() {
                    event_obj.insert(self.delta_label.clone(), serde_json::to_value(time_delta)?);
                }

                self.output_count += 1;
                Ok(vec![input])
            }
            None => {
                // First time seeing this key
                self.state.put(keystr, event_time);

                if self.emit_first {
                    // Emit the first event without a time delta (or with delta = 0)
                    if let Some(event_obj) = input.borrow_mut().as_object_mut() {
                        event_obj.insert(self.delta_label.clone(), serde_json::to_value(0u64)?);
                    }
                    self.output_count += 1;
                    Ok(vec![input])
                } else {
                    // Skip the first event for this key
                    self.first_event_skipped_count += 1;
                    Ok(vec![])
                }
            }
        }
    }

    fn flush(&mut self) -> Vec<Event> {
        // No buffering in this processor, so nothing to flush
        vec![]
    }

    fn stats(&self) -> Option<String> {
        Some(format!(
            "input:{}\noutput:{}\nfirst_event_skipped:{}\nmissing_timestamp:{}",
            self.input_count,
            self.output_count,
            self.first_event_skipped_count,
            self.missing_timestamp_count
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;
    use std::rc::Rc;

    // Helper function to create a test event with a key and timestamp
    fn create_test_event(key: &str, timestamp: Option<&str>) -> Event {
        let mut map = serde_json::Map::new();
        map.insert("key".to_string(), serde_json::Value::String(key.to_string()));

        if let Some(ts) = timestamp {
            map.insert("timestamp".to_string(), serde_json::Value::String(ts.to_string()));
        }

        Rc::new(RefCell::new(serde_json::Value::Object(map)))
    }

    // Helper function to extract time delta from an event
    fn get_time_delta(event: &Event, label: &str) -> Option<u64> {
        event.borrow().get(label).and_then(|v| v.as_u64())
    }

    #[test]
    fn test_time_delta_processor_basic() {
        let args = vec!["timedelta".to_string(), "key".to_string()];
        let mut processor = TimeDeltaProcessor::new(&args).unwrap();

        // Process first event (will be skipped)
        let event1 = create_test_event("test_key", Some("2023-01-01T00:00:00Z"));
        let result1 = processor.process(event1).unwrap();
        assert!(result1.is_empty());

        // Process second event (should compute delta)
        let event2 = create_test_event("test_key", Some("2023-01-01T00:00:30Z"));
        let result2 = processor.process(event2).unwrap();

        assert_eq!(result2.len(), 1);
        assert_eq!(get_time_delta(&result2[0], "time_delta_milliseconds"), Some(30000));
    }

    #[test]
    fn test_time_delta_processor_multiple_keys() {
        let args = vec!["timedelta".to_string(), "key".to_string()];
        let mut processor = TimeDeltaProcessor::new(&args).unwrap();

        // Process events with different keys
        let event1 = create_test_event("key1", Some("2023-01-01T00:00:00Z"));
        let event2 = create_test_event("key2", Some("2023-01-01T00:00:10Z"));
        let event3 = create_test_event("key1", Some("2023-01-01T00:00:20Z"));

        processor.process(event1).unwrap(); // Skipped
        processor.process(event2).unwrap(); // Skipped
        let result3 = processor.process(event3).unwrap();

        assert_eq!(result3.len(), 1);
        assert_eq!(get_time_delta(&result3[0], "time_delta_milliseconds"), Some(20000));
    }

    #[test]
    fn test_time_delta_processor_custom_format() {
        let args = vec![
            "timedelta".to_string(),
            "key".to_string(),
            "--time-format".to_string(),
            "%Y-%m-%d %H:%M:%S".to_string(),
        ];
        let mut processor = TimeDeltaProcessor::new(&args).unwrap();

        // Create events with custom timestamp format
        let mut map1 = serde_json::Map::new();
        map1.insert("key".to_string(), serde_json::Value::String("test_key".to_string()));
        map1.insert(
            "timestamp".to_string(),
            serde_json::Value::String("2023-01-01 00:00:00".to_string()),
        );
        let event1 = Rc::new(RefCell::new(serde_json::Value::Object(map1)));

        let mut map2 = serde_json::Map::new();
        map2.insert("key".to_string(), serde_json::Value::String("test_key".to_string()));
        map2.insert(
            "timestamp".to_string(),
            serde_json::Value::String("2023-01-01 00:00:30".to_string()),
        );
        let event2 = Rc::new(RefCell::new(serde_json::Value::Object(map2)));

        processor.process(event1).unwrap(); // Skipped
        let result2 = processor.process(event2).unwrap();

        assert_eq!(result2.len(), 1);
        assert_eq!(get_time_delta(&result2[0], "time_delta_milliseconds"), Some(30000));
    }

    #[test]
    fn test_time_delta_processor_missing_timestamp() {
        let args = vec![
            "timedelta".to_string(),
            "key".to_string(),
            "--time-path".to_string(),
            "missing_field".to_string(),
        ];
        let mut processor = TimeDeltaProcessor::new(&args).unwrap();
        let event = create_test_event("test_key", Some("2023-01-01T00:00:00Z"));

        let result = processor.process(event).unwrap();

        assert!(result.is_empty());
        assert_eq!(processor.missing_timestamp_count, 1);
    }

    #[test]
    fn test_time_delta_processor_emit_first() {
        let args = vec!["timedelta".to_string(), "key".to_string(), "--emit-first".to_string()];
        let mut processor = TimeDeltaProcessor::new(&args).unwrap();
        let event = create_test_event("test_key", Some("2023-01-01T00:00:00Z"));

        let result = processor.process(event).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(get_time_delta(&result[0], "time_delta_milliseconds"), Some(0));
    }
}
