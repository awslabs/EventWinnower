use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use std::num::NonZeroUsize;

const DEFAULT_STATE: usize = 16 * 1024 * 1024;

#[derive(SerialProcessorInit)]
pub struct KeyCountTotalProcessor<'a> {
    path: jmespath::Expression<'a>,
    state: LruCache<String, u64>,
    state_rep: LruCache<String, (u64, Event)>,
    representative_event_label: Option<String>,
    key_label: String,
    count_label: String,
    input_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// Counts key occurrences, outputs on eviction or flush
#[command(version, long_about = None, arg_required_else_help(true))]
struct KeyCountTotalArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short = 'k', long, default_value = "key")]
    key_label: String,

    #[arg(short = 'c', long, default_value = "keycount")]
    count_label: String,

    #[arg(short, long, default_value_t = DEFAULT_STATE)]
    max_state: usize,

    /// Track a representative event per key
    #[arg(short, long)]
    representative_event_label: Option<String>,
}

impl SerialProcessor for KeyCountTotalProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("counts key occurrences, outputs on eviction or flush".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = KeyCountTotalArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;
        let null_capacity = NonZeroUsize::new(1).expect("valid");
        let capacity = NonZeroUsize::new(args.max_state)
            .ok_or_else(|| anyhow::anyhow!("max_state must be greater than 0"))?;
        let (state, state_rep) = if args.representative_event_label.is_some() {
            (LruCache::new(null_capacity), LruCache::new(capacity))
        } else {
            (LruCache::new(capacity), LruCache::new(null_capacity))
        };

        Ok(Self {
            path,
            state,
            state_rep,
            representative_event_label: args.representative_event_label,
            key_label: args.key_label,
            count_label: args.count_label,
            input_count: 0,
            output_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        let result = self.path.search(&input)?;
        self.input_count += 1;

        if result.is_null() {
            return Ok(vec![]);
        }

        // Use as_string() for string values to avoid extra quotes from to_string()
        let key = if let Some(s) = result.as_string() { s.to_string() } else { result.to_string() };

        // Check if key exists in cache - if yes, increment and return empty vec
        if self.representative_event_label.is_some() {
            return self.process_with_rep(input, key);
        }

        if let Some(count) = self.state.get_mut(&key) {
            *count += 1;
            return Ok(vec![]);
        }

        // Key doesn't exist, use push() to insert with count=1
        // push() returns the evicted entry if capacity is exceeded
        let evicted = self.state.push(key, 1);

        // If push() returns evicted entry, create output event with evicted key/count
        if let Some((evicted_key, evicted_count)) = evicted {
            self.output_count += 1;
            let output = serde_json::json!({
                &self.key_label: evicted_key,
                &self.count_label: evicted_count,
            });
            Ok(vec![event_new(output)])
        } else {
            Ok(vec![])
        }
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events = Vec::new();

        // Drain all entries from LruCache
        if let Some(rep_label) = &self.representative_event_label {
            while let Some((key, (count, rep_event))) = self.state_rep.pop_lru() {
                self.output_count += 1;
                let output = serde_json::json!({
                    &self.key_label: key,
                    &self.count_label: count,
                    rep_label: rep_event,
                });
                events.push(event_new(output));
            }
        } else {
            while let Some((key, count)) = self.state.pop_lru() {
                self.output_count += 1;
                let output = serde_json::json!({
                    &self.key_label: key,
                    &self.count_label: count,
                });
                events.push(event_new(output));
            }
        }

        events
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\noutput:{}", self.input_count, self.output_count))
    }
}

impl KeyCountTotalProcessor<'_> {
    fn process_with_rep(&mut self, input: Event, key: String) -> Result<Vec<Event>, anyhow::Error> {
        if let Some((count, _rep)) = self.state_rep.get_mut(&key) {
            *count += 1;
            return Ok(vec![]);
        }

        // Key doesn't exist, use push() to insert with count=1
        // push() returns the evicted entry if capacity is exceeded
        let evicted = self.state_rep.push(key, (1, input.clone()));

        // If push() returns evicted entry, create output event with evicted key/count
        if let Some((evicted_key, (evicted_count, evicted_event))) = evicted {
            self.output_count += 1;
            let output = serde_json::json!({
                &self.key_label: evicted_key,
                &self.count_label: evicted_count,
                self.representative_event_label.as_ref().expect("label must exist"): evicted_event,
            });
            Ok(vec![event_new(output)])
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cell::RefCell;
    use std::rc::Rc;

    // 4.1 Basic functionality tests
    // Requirements: 1.1, 1.2, 1.3, 3.2

    #[test]
    fn test_single_event_valid_key_no_eviction() -> Result<(), anyhow::Error> {
        // Test single event with valid key (no eviction, empty result)
        let args = shlex::split("proc foo.id -m 10").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        let val = json!({
            "foo": {
                "id": "key1"
            }
        });
        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;

        // No eviction should occur, so output should be empty
        assert_eq!(output.len(), 0);
        Ok(())
    }

    #[test]
    fn test_multiple_events_same_key_counts_accumulate() -> Result<(), anyhow::Error> {
        // Test multiple events with same key (no eviction, counts accumulate)
        let args = shlex::split("proc foo.id -m 10").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        // Process same key multiple times
        for _ in 0..5 {
            let val = json!({
                "foo": {
                    "id": "key1"
                }
            });
            let event = Rc::new(RefCell::new(val));
            let output = processor.process(event)?;
            assert_eq!(output.len(), 0); // No eviction
        }

        // Flush to get the accumulated count
        let flush_output = processor.flush();
        assert_eq!(flush_output.len(), 1);
        assert_eq!(flush_output[0].borrow()["keycount"], 5);
        Ok(())
    }

    #[test]
    fn test_null_jmespath_result_empty_output() -> Result<(), anyhow::Error> {
        // Test events with null JMESPath result (empty results)
        let args = shlex::split("proc foo.nonexistent -m 10").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        let val = json!({
            "foo": {
                "id": "key1"
            }
        });
        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;

        // Null JMESPath result should return empty
        assert_eq!(output.len(), 0);

        // Flush should also be empty since nothing was counted
        let flush_output = processor.flush();
        assert_eq!(flush_output.len(), 0);
        Ok(())
    }

    // 4.2 Eviction tests
    // Requirements: 2.2, 2.3, 3.1, 3.3, 3.4

    #[test]
    fn test_eviction_when_cache_full() -> Result<(), anyhow::Error> {
        // Test fill cache to capacity, add new key, verify eviction output
        let args = shlex::split("proc foo.id -m 3").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        // Fill cache with 3 keys
        for i in 0..3 {
            let val = json!({
                "foo": {
                    "id": format!("key{}", i)
                }
            });
            let event = Rc::new(RefCell::new(val));
            let output = processor.process(event)?;
            assert_eq!(output.len(), 0); // No eviction yet
        }

        // Add a 4th key - should evict the LRU key (key0)
        let val = json!({
            "foo": {
                "id": "key3"
            }
        });
        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].borrow()["key"], "key0");
        assert_eq!(output[0].borrow()["keycount"], 1);
        Ok(())
    }

    #[test]
    fn test_evicted_key_is_lru() -> Result<(), anyhow::Error> {
        // Test evicted key is the least recently used
        let args = shlex::split("proc foo.id -m 3").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        // Add 3 keys
        for i in 0..3 {
            let val = json!({
                "foo": {
                    "id": format!("key{}", i)
                }
            });
            let event = Rc::new(RefCell::new(val));
            processor.process(event)?;
        }

        // Access key0 again to make it most recently used
        let val = json!({
            "foo": {
                "id": "key0"
            }
        });
        let event = Rc::new(RefCell::new(val));
        processor.process(event)?;

        // Add a new key - should evict key1 (now the LRU)
        let val = json!({
            "foo": {
                "id": "key3"
            }
        });
        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].borrow()["key"], "key1");
        Ok(())
    }

    #[test]
    fn test_eviction_event_format() -> Result<(), anyhow::Error> {
        // Test eviction event format (key_label and count_label fields)
        let args = shlex::split("proc foo.id -m 1 -k my_key -c my_count").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        // Add first key
        let val = json!({
            "foo": {
                "id": "first"
            }
        });
        let event = Rc::new(RefCell::new(val));
        processor.process(event)?;

        // Add second key - should evict first
        let val = json!({
            "foo": {
                "id": "second"
            }
        });
        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;

        assert_eq!(output.len(), 1);
        let result = output[0].borrow();
        assert_eq!(result["my_key"], "first");
        assert_eq!(result["my_count"], 1);
        Ok(())
    }

    // 4.3 Flush tests
    // Requirements: 4.1, 4.2, 4.3, 4.4

    #[test]
    fn test_flush_no_events_empty_result() -> Result<(), anyhow::Error> {
        // Test flush with no events processed (empty result)
        let args = shlex::split("proc foo.id -m 10").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        let flush_output = processor.flush();
        assert_eq!(flush_output.len(), 0);
        Ok(())
    }

    #[test]
    fn test_flush_single_key() -> Result<(), anyhow::Error> {
        // Test flush with single key (one event)
        let args = shlex::split("proc foo.id -m 10").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        let val = json!({
            "foo": {
                "id": "only_key"
            }
        });
        let event = Rc::new(RefCell::new(val));
        processor.process(event)?;

        let flush_output = processor.flush();
        assert_eq!(flush_output.len(), 1);
        assert_eq!(flush_output[0].borrow()["key"], "only_key");
        assert_eq!(flush_output[0].borrow()["keycount"], 1);
        Ok(())
    }

    #[test]
    fn test_flush_multiple_keys() -> Result<(), anyhow::Error> {
        // Test flush with multiple keys (multiple events)
        let args = shlex::split("proc foo.id -m 10").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        // Add 3 different keys
        for i in 0..3 {
            let val = json!({
                "foo": {
                    "id": format!("key{}", i)
                }
            });
            let event = Rc::new(RefCell::new(val));
            processor.process(event)?;
        }

        let flush_output = processor.flush();
        assert_eq!(flush_output.len(), 3);
        Ok(())
    }

    #[test]
    fn test_double_flush_second_empty() -> Result<(), anyhow::Error> {
        // Test double flush (second returns empty)
        let args = shlex::split("proc foo.id -m 10").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        let val = json!({
            "foo": {
                "id": "key1"
            }
        });
        let event = Rc::new(RefCell::new(val));
        processor.process(event)?;

        let first_flush = processor.flush();
        assert_eq!(first_flush.len(), 1);

        let second_flush = processor.flush();
        assert_eq!(second_flush.len(), 0);
        Ok(())
    }

    // 4.4 Configuration tests
    // Requirements: 5.2, 5.3, 5.4

    #[test]
    fn test_custom_key_label() -> Result<(), anyhow::Error> {
        // Test custom key_label
        let args = shlex::split("proc foo.id -k custom_key -m 10").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        let val = json!({
            "foo": {
                "id": "test"
            }
        });
        let event = Rc::new(RefCell::new(val));
        processor.process(event)?;

        let flush_output = processor.flush();
        assert_eq!(flush_output.len(), 1);
        assert!(flush_output[0].borrow().get("custom_key").is_some());
        assert!(flush_output[0].borrow().get("key").is_none());
        Ok(())
    }

    #[test]
    fn test_custom_count_label() -> Result<(), anyhow::Error> {
        // Test custom count_label
        let args = shlex::split("proc foo.id -c custom_count -m 10").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        let val = json!({
            "foo": {
                "id": "test"
            }
        });
        let event = Rc::new(RefCell::new(val));
        processor.process(event)?;

        let flush_output = processor.flush();
        assert_eq!(flush_output.len(), 1);
        assert!(flush_output[0].borrow().get("custom_count").is_some());
        assert!(flush_output[0].borrow().get("keycount").is_none());
        Ok(())
    }

    #[test]
    fn test_custom_max_state() -> Result<(), anyhow::Error> {
        // Test custom max_state
        let args = shlex::split("proc foo.id -m 2").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        // Add 2 keys (at capacity)
        for i in 0..2 {
            let val = json!({
                "foo": {
                    "id": format!("key{}", i)
                }
            });
            let event = Rc::new(RefCell::new(val));
            let output = processor.process(event)?;
            assert_eq!(output.len(), 0);
        }

        // Add 3rd key - should trigger eviction
        let val = json!({
            "foo": {
                "id": "key2"
            }
        });
        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);
        Ok(())
    }

    #[test]
    fn test_default_values() -> Result<(), anyhow::Error> {
        // Test default values
        let args = shlex::split("proc foo.id").unwrap();
        let mut processor = KeyCountTotalProcessor::new(&args)?;

        let val = json!({
            "foo": {
                "id": "test"
            }
        });
        let event = Rc::new(RefCell::new(val));
        processor.process(event)?;

        let flush_output = processor.flush();
        assert_eq!(flush_output.len(), 1);
        // Default key_label is "key"
        assert!(flush_output[0].borrow().get("key").is_some());
        // Default count_label is "keycount"
        assert!(flush_output[0].borrow().get("keycount").is_some());
        Ok(())
    }
}
