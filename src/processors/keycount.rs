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
    s.finish()
}

#[derive(SerialProcessorInit)]
pub struct KeyCountProcessor<'a> {
    path: jmespath::Expression<'a>,
    state: LruCache<u64, u64>,
    output_field: String,
    input_count: u64,
}

#[derive(Parser)]
/// Count occurrences of JMESPath key values
#[command(version, long_about = None, arg_required_else_help(true))]
struct KeyCountArgs {
    #[arg(required(true))]
    path: Vec<String>,

    /// Output field name for the count (default: _count)
    #[arg(short, long, default_value = "count")]
    output: String,

    /// Local LRU cache size for tracking keys (default: 1000000)
    #[arg(short, long, default_value_t = NonZeroUsize::new(1000000).unwrap())]
    cache_size: NonZeroUsize,
}

impl SerialProcessor for KeyCountProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("count occurrences of JMESPath key values".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = KeyCountArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;
        let state = LruCache::new(args.cache_size);

        Ok(Self { path, state, output_field: args.output, input_count: 0 })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        let result = self.path.search(&input)?;

        if result.is_null() {
            return Ok(vec![input]);
        }

        let hash = hash64(&result.to_string());
        let count: &mut u64 = self.state.get_or_insert_mut(hash, || 0);
        *count += 1;

        let current_count = *count;

        // Append count to event
        {
            let mut event = input.borrow_mut();
            event[&self.output_field] = serde_json::json!(current_count);
        }

        Ok(vec![input])
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}", self.input_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use serde_json::json;

    #[test]
    fn test_counts_per_key() -> Result<()> {
        let mut processor = KeyCountProcessor::new(&["keycount".to_string(), "id".to_string()])?;

        let r1 = processor.process(event_new(json!({"id": "a"})))?;
        let r2 = processor.process(event_new(json!({"id": "a"})))?;
        let r3 = processor.process(event_new(json!({"id": "b"})))?;
        let r4 = processor.process(event_new(json!({"id": "a"})))?;

        assert_eq!(r1[0].borrow()["count"], 1);
        assert_eq!(r2[0].borrow()["count"], 2);
        assert_eq!(r3[0].borrow()["count"], 1);
        assert_eq!(r4[0].borrow()["count"], 3);
        Ok(())
    }
}
