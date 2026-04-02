use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

fn hash64<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() // This returns the final u64 hash value
}

#[derive(SerialProcessorInit)]
pub struct UniqLruProcessor<'a> {
    path: jmespath::Expression<'a>,
    state: LruCache<u64, bool>,
    input_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// extract out a jmespath object from event, append to event
#[command(version, long_about = None, arg_required_else_help(true))]
struct UniqLruArgs {
    #[arg(required(true))]
    path: Vec<String>,

    /// Local LRU cache size for deduplication (default: 10000)
    #[arg(short, long, default_value_t = NonZeroUsize::new(1000000).unwrap())]
    cache_size: NonZeroUsize,
}

impl SerialProcessor for UniqLruProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("extract unique events".to_string())
    }
    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = UniqLruArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;
        let state = LruCache::new(args.cache_size);

        Ok(Self { path, state, input_count: 0, output_count: 0 })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        let result = self.path.search(&input)?;
        self.input_count += 1;

        if result.is_null() {
            Ok(vec![])
        } else if self.state.put(hash64(&result.to_string()), true).is_none() {
            self.output_count += 1;
            Ok(vec![input])
        } else {
            Ok(vec![])
        }
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\noutput:{}", self.input_count, self.output_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use serde_json::json;

    #[test]
    fn test_unique_events_pass_through() -> Result<()> {
        let mut processor = UniqLruProcessor::new(&["uniq_lru".to_string(), "id".to_string()])?;

        let event1 = event_new(json!({"id": "abc123"}));
        let event2 = event_new(json!({"id": "def456"}));
        let event3 = event_new(json!({"id": "ghi789"}));

        let result1 = processor.process(event1)?;
        let result2 = processor.process(event2)?;
        let result3 = processor.process(event3)?;

        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 1);
        assert_eq!(result3.len(), 1);
        Ok(())
    }

    #[test]
    fn test_duplicate_events_filtered() -> Result<()> {
        let mut processor = UniqLruProcessor::new(&["uniq_lru".to_string(), "id".to_string()])?;

        let event1 = event_new(json!({"id": "same_id"}));
        let event2 = event_new(json!({"id": "same_id"}));
        let event3 = event_new(json!({"id": "same_id"}));

        let result1 = processor.process(event1)?;
        let result2 = processor.process(event2)?;
        let result3 = processor.process(event3)?;

        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 0); // Duplicate filtered
        assert_eq!(result3.len(), 0); // Duplicate filtered
        Ok(())
    }

    #[test]
    fn test_null_path_result_filtered() -> Result<()> {
        let mut processor =
            UniqLruProcessor::new(&["uniq_lru".to_string(), "missing_field".to_string()])?;

        let event = event_new(json!({"other_field": "value"}));
        let result = processor.process(event)?;

        assert_eq!(result.len(), 0);
        Ok(())
    }

    #[test]
    fn test_mixed_unique_and_duplicate_events() -> Result<()> {
        let mut processor = UniqLruProcessor::new(&["uniq_lru".to_string(), "id".to_string()])?;

        let events = vec![
            event_new(json!({"id": "a"})),
            event_new(json!({"id": "b"})),
            event_new(json!({"id": "a"})), // duplicate
            event_new(json!({"id": "c"})),
            event_new(json!({"id": "b"})), // duplicate
            event_new(json!({"id": "d"})),
        ];

        let mut output_count = 0;
        for event in events {
            let result = processor.process(event)?;
            output_count += result.len();
        }

        assert_eq!(output_count, 4); // a, b, c, d
        Ok(())
    }

    #[test]
    fn test_stats_tracking() -> Result<()> {
        let mut processor = UniqLruProcessor::new(&["uniq_lru".to_string(), "id".to_string()])?;

        processor.process(event_new(json!({"id": "a"})))?;
        processor.process(event_new(json!({"id": "b"})))?;
        processor.process(event_new(json!({"id": "a"})))?; // duplicate
        processor.process(event_new(json!({"id": "c"})))?;

        let stats = processor.stats().unwrap();
        assert!(stats.contains("input:4"));
        assert!(stats.contains("output:3"));
        Ok(())
    }

    #[test]
    fn test_nested_jmespath_expression() -> Result<()> {
        let mut processor =
            UniqLruProcessor::new(&["uniq_lru".to_string(), "data.nested.id".to_string()])?;

        let event1 = event_new(json!({"data": {"nested": {"id": "unique1"}}}));
        let event2 = event_new(json!({"data": {"nested": {"id": "unique2"}}}));
        let event3 = event_new(json!({"data": {"nested": {"id": "unique1"}}})); // duplicate

        let result1 = processor.process(event1)?;
        let result2 = processor.process(event2)?;
        let result3 = processor.process(event3)?;

        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 1);
        assert_eq!(result3.len(), 0);
        Ok(())
    }

    #[test]
    fn test_custom_cache_size() -> Result<()> {
        let mut processor = UniqLruProcessor::new(&[
            "uniq_lru".to_string(),
            "id".to_string(),
            "--cache-size".to_string(),
            "2".to_string(),
        ])?;

        // Fill cache with 2 items
        processor.process(event_new(json!({"id": "a"})))?;
        processor.process(event_new(json!({"id": "b"})))?;

        // Add third item, should evict "a"
        processor.process(event_new(json!({"id": "c"})))?;

        // "a" should now be treated as new since it was evicted
        let result = processor.process(event_new(json!({"id": "a"})))?;
        assert_eq!(result.len(), 1);

        // "c" should still be in cache
        let result_c = processor.process(event_new(json!({"id": "c"})))?;
        assert_eq!(result_c.len(), 0);
        Ok(())
    }

    #[test]
    fn test_lru_eviction_behavior() -> Result<()> {
        let mut processor = UniqLruProcessor::new(&[
            "uniq_lru".to_string(),
            "id".to_string(),
            "--cache-size".to_string(),
            "3".to_string(),
        ])?;

        // Add items a, b, c
        processor.process(event_new(json!({"id": "a"})))?;
        processor.process(event_new(json!({"id": "b"})))?;
        processor.process(event_new(json!({"id": "c"})))?;

        // Access "a" again to make it recently used (but it's a duplicate so filtered)
        let result_a = processor.process(event_new(json!({"id": "a"})))?;
        assert_eq!(result_a.len(), 0);

        // Add "d" - should evict "b" (least recently used)
        processor.process(event_new(json!({"id": "d"})))?;

        // "b" should be evicted and treated as new
        let result_b = processor.process(event_new(json!({"id": "b"})))?;
        assert_eq!(result_b.len(), 1);

        // "a" should still be in cache
        let result_a2 = processor.process(event_new(json!({"id": "a"})))?;
        assert_eq!(result_a2.len(), 0);
        Ok(())
    }

    #[test]
    fn test_different_value_types() -> Result<()> {
        let mut processor = UniqLruProcessor::new(&["uniq_lru".to_string(), "value".to_string()])?;

        // String value
        let result1 = processor.process(event_new(json!({"value": "string"})))?;
        assert_eq!(result1.len(), 1);

        // Number value
        let result2 = processor.process(event_new(json!({"value": 123})))?;
        assert_eq!(result2.len(), 1);

        // Boolean value
        let result3 = processor.process(event_new(json!({"value": true})))?;
        assert_eq!(result3.len(), 1);

        // Array value
        let result4 = processor.process(event_new(json!({"value": [1, 2, 3]})))?;
        assert_eq!(result4.len(), 1);

        // Object value
        let result5 = processor.process(event_new(json!({"value": {"nested": "obj"}})))?;
        assert_eq!(result5.len(), 1);

        // Duplicate string
        let result6 = processor.process(event_new(json!({"value": "string"})))?;
        assert_eq!(result6.len(), 0);
        Ok(())
    }

    #[test]
    fn test_complex_jmespath_expression() -> Result<()> {
        let mut processor = UniqLruProcessor::new(&[
            "uniq_lru".to_string(),
            "[source_ip,".to_string(),
            "dest_ip]".to_string(),
        ])?;

        let event1 = event_new(json!({"source_ip": "10.0.0.1", "dest_ip": "10.0.0.2"}));
        let event2 = event_new(json!({"source_ip": "10.0.0.1", "dest_ip": "10.0.0.3"}));
        let event3 = event_new(json!({"source_ip": "10.0.0.1", "dest_ip": "10.0.0.2"})); // duplicate

        let result1 = processor.process(event1)?;
        let result2 = processor.process(event2)?;
        let result3 = processor.process(event3)?;

        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 1);
        assert_eq!(result3.len(), 0);
        Ok(())
    }

    #[test]
    fn test_empty_string_value() -> Result<()> {
        let mut processor = UniqLruProcessor::new(&["uniq_lru".to_string(), "id".to_string()])?;

        let event1 = event_new(json!({"id": ""}));
        let event2 = event_new(json!({"id": ""})); // duplicate empty string

        let result1 = processor.process(event1)?;
        let result2 = processor.process(event2)?;

        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 0);
        Ok(())
    }

    #[test]
    fn test_original_event_preserved() -> Result<()> {
        let mut processor = UniqLruProcessor::new(&["uniq_lru".to_string(), "id".to_string()])?;

        let event = event_new(json!({
            "id": "test123",
            "other_field": "preserved_value",
            "nested": {"data": "also_preserved"}
        }));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert_eq!(output["id"], "test123");
        assert_eq!(output["other_field"], "preserved_value");
        assert_eq!(output["nested"]["data"], "also_preserved");
        Ok(())
    }

    #[test]
    fn test_get_simple_description() {
        let description = UniqLruProcessor::get_simple_description();
        assert!(description.is_some());
        assert!(description.unwrap().contains("unique"));
    }
}
