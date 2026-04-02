use crate::processors::processor::*;
use crate::processors::timedelta::extract_timestamp_from_event;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use std::num::NonZeroUsize;

const CACHE_SIZE: usize = 200000;

#[derive(Clone, Debug)]
struct StateInfo {
    state_value: String,
    timestamp: u64,
}

#[derive(SerialProcessorInit)]
pub struct StateTransitionDeltaProcessor<'a> {
    key_path: jmespath::Expression<'a>,
    state_path: jmespath::Expression<'a>,
    time_path: jmespath::Expression<'a>,
    time_format: Option<String>,
    state: LruCache<String, StateInfo>,
    delta_label: String,
    previous_state_label: String,
    current_state_label: String,
    input_count: u64,
    output_count: u64,
    missing_timestamp_count: u64,
    missing_key_count: u64,
    missing_state_count: u64,
    first_event_skipped_count: u64,
    same_state_count: u64,
}

#[derive(Parser)]
/// Compute time deltas in milliseconds between state transitions for events with the same key
#[command(version, long_about = None, arg_required_else_help(true))]
struct StateTransitionDeltaArgs {
    #[arg(required(true))]
    key: Vec<String>,

    #[arg(short, long, required(true))]
    state_path: String,

    #[arg(short, long, default_value = "timestamp")]
    time_path: String,

    #[arg(short = 'f', long)]
    time_format: Option<String>,

    #[arg(short, long, default_value = "state_transition_delta_milliseconds")]
    delta_label: String,

    #[arg(long, default_value = "previous_state")]
    previous_state_label: String,

    #[arg(long, default_value = "current_state")]
    current_state_label: String,

    #[arg(short, long, default_value_t = CACHE_SIZE)]
    cache_size: usize,
}

impl SerialProcessor for StateTransitionDeltaProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("compute time deltas in milliseconds between state transitions for events with the same key".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = StateTransitionDeltaArgs::try_parse_from(argv)?;
        let key_path = jmespath::compile(&args.key.join(" "))?;
        let state_path = jmespath::compile(&args.state_path)?;
        let time_path = jmespath::compile(&args.time_path)?;

        Ok(Self {
            key_path,
            state_path,
            time_path,
            time_format: args.time_format,
            state: LruCache::new(NonZeroUsize::new(args.cache_size).expect("non-zero cache size")),
            delta_label: args.delta_label,
            previous_state_label: args.previous_state_label,
            current_state_label: args.current_state_label,
            input_count: 0,
            output_count: 0,
            missing_timestamp_count: 0,
            missing_key_count: 0,
            missing_state_count: 0,
            first_event_skipped_count: 0,
            same_state_count: 0,
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

        // Extract the state value from the event
        let state_value = self.state_path.search(&input)?;
        if state_value.is_null() {
            self.missing_state_count += 1;
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
        let state_str =
            if let Some(s) = state_value.as_string() { s.clone() } else { state_value.to_string() };

        // Look up the previous state for this key
        match self.state.get(&keystr).cloned() {
            Some(previous_state_info) => {
                if previous_state_info.state_value == state_str {
                    // Same state, just update the timestamp
                    let updated_state_info =
                        StateInfo { state_value: state_str, timestamp: event_time };
                    self.state.put(keystr, updated_state_info);
                    self.same_state_count += 1;
                    Ok(vec![])
                } else {
                    // State transition detected - compute time delta
                    let time_delta = event_time.abs_diff(previous_state_info.timestamp);

                    // Update the state with the new state and timestamp
                    let new_state_info =
                        StateInfo { state_value: state_str.clone(), timestamp: event_time };
                    self.state.put(keystr, new_state_info);

                    // Add the transition information to the event
                    if let Some(event_obj) = input.borrow_mut().as_object_mut() {
                        event_obj
                            .insert(self.delta_label.clone(), serde_json::to_value(time_delta)?);
                        event_obj.insert(
                            self.previous_state_label.clone(),
                            serde_json::Value::String(previous_state_info.state_value.clone()),
                        );
                        event_obj.insert(
                            self.current_state_label.clone(),
                            serde_json::Value::String(state_str.clone()),
                        );
                    }

                    self.output_count += 1;
                    Ok(vec![input])
                }
            }
            None => {
                // First time seeing this key - store state and skip event
                let state_info =
                    StateInfo { state_value: state_str.clone(), timestamp: event_time };
                self.state.put(keystr, state_info);
                self.first_event_skipped_count += 1;
                Ok(vec![])
            }
        }
    }

    fn flush(&mut self) -> Vec<Event> {
        // No buffering in this processor, so nothing to flush
        vec![]
    }

    fn stats(&self) -> Option<String> {
        Some(format!(
            "input:{}\noutput:{}\nfirst_event_skipped:{}\nsame_state_skipped:{}\nmissing_timestamp:{}\nmissing_key:{}\nmissing_state:{}",
            self.input_count,
            self.output_count,
            self.first_event_skipped_count,
            self.same_state_count,
            self.missing_timestamp_count,
            self.missing_key_count,
            self.missing_state_count
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    // Helper function to create a test event with a key, state, and timestamp
    fn create_test_event(key: &str, state: &str, timestamp: Option<&str>) -> Event {
        let mut map = serde_json::Map::new();
        map.insert("key".to_string(), serde_json::Value::String(key.to_string()));
        map.insert("state".to_string(), serde_json::Value::String(state.to_string()));

        if let Some(ts) = timestamp {
            map.insert("timestamp".to_string(), serde_json::Value::String(ts.to_string()));
        }

        Rc::new(RefCell::new(serde_json::Value::Object(map)))
    }

    // Helper function to extract time delta from an event
    fn get_time_delta(event: &Event, label: &str) -> Option<u64> {
        event.borrow().get(label).and_then(|v| v.as_u64())
    }

    // Helper function to extract string value from an event
    fn get_string_value(event: &Event, label: &str) -> Option<String> {
        event.borrow().get(label).and_then(|v| v.as_str()).map(|s| s.to_string())
    }

    #[test]
    fn test_state_transition_delta_processor_basic() {
        let args = vec![
            "statetransitiondelta".to_string(),
            "key".to_string(),
            "--state-path".to_string(),
            "state".to_string(),
        ];
        let mut processor = StateTransitionDeltaProcessor::new(&args).unwrap();

        // Process first event (will be skipped)
        let event1 = create_test_event("test_key", "running", Some("2023-01-01T00:00:00Z"));
        let result1 = processor.process(event1).unwrap();
        assert!(result1.is_empty());

        // Process second event with same state (should be skipped)
        let event2 = create_test_event("test_key", "running", Some("2023-01-01T00:00:10Z"));
        let result2 = processor.process(event2).unwrap();
        assert!(result2.is_empty());

        // Process third event with different state (should compute delta)
        let event3 = create_test_event("test_key", "stopped", Some("2023-01-01T00:00:30Z"));
        let result3 = processor.process(event3).unwrap();

        assert_eq!(result3.len(), 1);
        assert_eq!(get_time_delta(&result3[0], "state_transition_delta_milliseconds"), Some(20000));
        assert_eq!(get_string_value(&result3[0], "previous_state"), Some("running".to_string()));
        assert_eq!(get_string_value(&result3[0], "current_state"), Some("stopped".to_string()));
    }

    #[test]
    fn test_state_transition_delta_processor_multiple_keys() {
        let args = vec![
            "statetransitiondelta".to_string(),
            "key".to_string(),
            "--state-path".to_string(),
            "state".to_string(),
        ];
        let mut processor = StateTransitionDeltaProcessor::new(&args).unwrap();

        // Process events with different keys
        let event1 = create_test_event("key1", "running", Some("2023-01-01T00:00:00Z"));
        let event2 = create_test_event("key2", "running", Some("2023-01-01T00:00:10Z"));
        let event3 = create_test_event("key1", "stopped", Some("2023-01-01T00:00:20Z"));

        processor.process(event1).unwrap(); // Skipped (first for key1)
        processor.process(event2).unwrap(); // Skipped (first for key2)
        let result3 = processor.process(event3).unwrap();

        assert_eq!(result3.len(), 1);
        assert_eq!(get_time_delta(&result3[0], "state_transition_delta_milliseconds"), Some(20000));
        assert_eq!(get_string_value(&result3[0], "previous_state"), Some("running".to_string()));
        assert_eq!(get_string_value(&result3[0], "current_state"), Some("stopped".to_string()));
    }

    #[test]
    fn test_state_transition_delta_processor_missing_fields() {
        let args = vec![
            "statetransitiondelta".to_string(),
            "key".to_string(),
            "--state-path".to_string(),
            "state".to_string(),
        ];
        let mut processor = StateTransitionDeltaProcessor::new(&args).unwrap();

        // Event missing key (create event without key field)
        let mut map1 = serde_json::Map::new();
        map1.insert("state".to_string(), serde_json::Value::String("running".to_string()));
        map1.insert(
            "timestamp".to_string(),
            serde_json::Value::String("2023-01-01T00:00:00Z".to_string()),
        );
        let event1 = Rc::new(RefCell::new(serde_json::Value::Object(map1)));
        let result1 = processor.process(event1).unwrap();
        assert!(result1.is_empty());

        // Event missing state (create event without state field)
        let mut map2 = serde_json::Map::new();
        map2.insert("key".to_string(), serde_json::Value::String("test_key".to_string()));
        map2.insert(
            "timestamp".to_string(),
            serde_json::Value::String("2023-01-01T00:00:00Z".to_string()),
        );
        let event2 = Rc::new(RefCell::new(serde_json::Value::Object(map2)));
        let result2 = processor.process(event2).unwrap();
        assert!(result2.is_empty());

        // Event missing timestamp
        let event3 = create_test_event("test_key", "running", None);
        let result3 = processor.process(event3).unwrap();
        assert!(result3.is_empty());

        assert_eq!(processor.missing_key_count, 1);
        assert_eq!(processor.missing_state_count, 1);
        assert_eq!(processor.missing_timestamp_count, 1);
    }

    #[test]
    fn test_state_transition_delta_processor_custom_labels() {
        let args = vec![
            "statetransitiondelta".to_string(),
            "key".to_string(),
            "--state-path".to_string(),
            "state".to_string(),
            "--delta-label".to_string(),
            "custom_delta".to_string(),
            "--previous-state-label".to_string(),
            "prev_state".to_string(),
            "--current-state-label".to_string(),
            "curr_state".to_string(),
        ];
        let mut processor = StateTransitionDeltaProcessor::new(&args).unwrap();

        // Process first event
        let event1 = create_test_event("test_key", "running", Some("2023-01-01T00:00:00Z"));
        processor.process(event1).unwrap();

        // Process second event with state transition
        let event2 = create_test_event("test_key", "stopped", Some("2023-01-01T00:00:30Z"));
        let result2 = processor.process(event2).unwrap();

        assert_eq!(result2.len(), 1);
        assert_eq!(get_time_delta(&result2[0], "custom_delta"), Some(30000));
        assert_eq!(get_string_value(&result2[0], "prev_state"), Some("running".to_string()));
        assert_eq!(get_string_value(&result2[0], "curr_state"), Some("stopped".to_string()));
    }
}
