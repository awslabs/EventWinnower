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
pub struct FirstNProcessor<'a> {
    path: jmespath::Expression<'a>,
    state: LruCache<u64, u32>,
    max_count: u32,
    input_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// Pass the first N occurrences of each key, then block subsequent occurrences
#[command(version, long_about = None, arg_required_else_help(true))]
struct FirstNArgs {
    #[arg(required(true))]
    path: Vec<String>,

    /// Number of occurrences per key to pass before blocking (default: 10)
    #[arg(short = 'n', long, default_value_t = 10)]
    max_count: u32,

    /// Local LRU cache size for tracking keys (default: 1000000)
    #[arg(short, long, default_value_t = NonZeroUsize::new(1000000).unwrap())]
    cache_size: NonZeroUsize,
}

impl SerialProcessor for FirstNProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("pass first N occurrences per key".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = FirstNArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;
        let state = LruCache::new(args.cache_size);

        Ok(Self { path, state, max_count: args.max_count, input_count: 0, output_count: 0 })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        let result = self.path.search(&input)?;

        if result.is_null() {
            return Ok(vec![]);
        }

        let hash = hash64(&result.to_string());
        let count: &mut u32 = self.state.get_or_insert_mut(hash, || 0);
        *count += 1;

        if *count <= self.max_count {
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
    fn test_passes_first_n_per_key() -> Result<()> {
        let mut processor = FirstNProcessor::new(&[
            "firstn".to_string(),
            "id".to_string(),
            "-n".to_string(),
            "2".to_string(),
        ])?;

        let r1 = processor.process(event_new(json!({"id": "a"})))?;
        let r2 = processor.process(event_new(json!({"id": "a"})))?;
        let r3 = processor.process(event_new(json!({"id": "a"})))?;
        let r4 = processor.process(event_new(json!({"id": "b"})))?;
        let r5 = processor.process(event_new(json!({"id": "b"})))?;
        let r6 = processor.process(event_new(json!({"id": "b"})))?;

        assert_eq!(r1.len(), 1); // 1st of "a" - pass
        assert_eq!(r2.len(), 1); // 2nd of "a" - pass
        assert_eq!(r3.len(), 0); // 3rd of "a" - blocked
        assert_eq!(r4.len(), 1); // 1st of "b" - pass
        assert_eq!(r5.len(), 1); // 2nd of "b" - pass
        assert_eq!(r6.len(), 0); // 3rd of "b" - blocked
        Ok(())
    }
}
