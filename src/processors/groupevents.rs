use crate::processors::processor::*;
use anyhow::Result;
use chrono::DateTime;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP_SIZE: usize = 2;
const GROUP_EVENTS_MIN_GROUP_SIZE: usize = 1;
const MAX_GROUP_SIZE: usize = 100;
const CACHE_SIZE: usize = 200000;
const MAX_FLUSH: usize = 1000;

#[derive(SerialProcessorInit)]
pub struct GroupEventsProcessor<'a> {
    key_path: jmespath::Expression<'a>,
    state: LruCache<String, Vec<Event>>,
    max_group_size: usize,
    min_group_size: usize,
    input_count: usize,
    output_count: usize,
}
#[derive(Parser)]
/// Group multiple events based on a common key
#[command(version, long_about = None, arg_required_else_help(true))]
struct GroupEventsArgs {
    #[arg(required(true))]
    key: Vec<String>,

    #[arg(short, long, default_value_t = MAX_GROUP_SIZE)]
    max_group_size: usize,

    // group size worth printing when flushing buffers
    #[arg(short, long, default_value_t = GROUP_EVENTS_MIN_GROUP_SIZE)]
    output_group_size: usize,

    #[arg(short, long, default_value_t = CACHE_SIZE)]
    cache_size: usize,
}

impl SerialProcessor for GroupEventsProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("group multiple events based on a common key".to_string())
    }
    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = GroupEventsArgs::try_parse_from(argv)?;
        let key_path = jmespath::compile(&args.key.join(" "))?;

        Ok(GroupEventsProcessor {
            key_path,
            state: LruCache::new(NonZeroUsize::new(args.cache_size).expect("non-zero cache size")),
            max_group_size: args.max_group_size,
            min_group_size: args.output_group_size,
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

        // lookup group in state
        // or insert a new event - check if event got expired when inserting
        let mut output_events: Vec<Event> = Vec::new();
        let keystr = if let Some(s) = key.as_string() { s.to_string() } else { key.to_string() };
        let group = match self.state.get_mut(&keystr) {
            Some(group) => group,
            None => {
                if let Some((_, expired_group)) = self.state.push(keystr.clone(), Vec::new()) {
                    //we expired something when adding this new key
                    if expired_group.len() >= self.min_group_size {
                        output_events.push(create_group_event(&expired_group)?);
                    }
                }
                self.state.get_mut(&keystr).expect("just inserted")
            }
        };

        group.push(input.clone());

        if group.len() >= self.max_group_size {
            output_events.push(create_group_event(group)?);

            //delete from LRU state
            self.state.pop(&keystr);
        }

        self.output_count += output_events.len();
        Ok(output_events)
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events: Vec<Event> = vec![];
        while let Some((_, group)) = self.state.pop_lru() {
            if group.len() >= self.min_group_size {
                if let Ok(group_event) = create_group_event(&group) {
                    events.push(group_event);
                    if events.len() > MAX_FLUSH {
                        break;
                    }
                }
            }
        }
        self.output_count += events.len();
        events
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\noutput:{}", self.input_count, self.output_count))
    }
}

fn create_group_event(group: &Vec<Event>) -> Result<Event, anyhow::Error> {
    let mut output = serde_json::Map::new();
    output.insert("group_length".to_string(), serde_json::to_value(group.len())?);
    output.insert("group".to_string(), serde_json::to_value(group)?);
    let output_value: serde_json::Value = output.into();
    Ok(Rc::new(RefCell::new(output_value)))
}

// group events around the same key and co-occuring within a time window
#[derive(SerialProcessorInit)]
pub struct BurstGroupProcessor<'a> {
    key_path: jmespath::Expression<'a>,
    time_path: Option<jmespath::Expression<'a>>,
    state: LruCache<String, BurstGroup>,
    gap_seconds: u64,
    max_group_size: usize,
    min_group_size: usize,
    input_cnt: u64,
    output_cnt: u64,
    skipped_group: u64,
    missing_timestamp_cnt: u64,
}

#[derive(Default)]
pub struct BurstGroup {
    group: Vec<Event>,
    last_time: u64,
}

const GAP_MAX_DEFAULT: u64 = 300; //5 minutes

#[derive(Parser)]
/// Group multiple events based on a common key around a time window
#[command(version, long_about = None, arg_required_else_help(true))]
struct BurstGroupArgs {
    #[arg(required(true))]
    key: Vec<String>,

    #[arg(short, long)]
    time_path: Option<String>,

    //maximum gap in seconds for a group
    #[arg(short, long, default_value_t = GAP_MAX_DEFAULT)]
    gap_seconds: u64,

    #[arg(short, long, default_value_t = MAX_GROUP_SIZE)]
    max_group_size: usize,

    // group size worth printing when flushing buffers
    #[arg(short, long, default_value_t = GROUP_SIZE)]
    output_group_size: usize,

    #[arg(short, long, default_value_t = CACHE_SIZE)]
    cache_size: usize,
}

impl SerialProcessor for BurstGroupProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("group events in time-based bursts".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = BurstGroupArgs::try_parse_from(argv)?;
        let key_path = jmespath::compile(&args.key.join(" "))?;

        let time_path = match args.time_path {
            Some(path) => Some(jmespath::compile(&path)?),
            _ => None,
        };

        Ok(Self {
            key_path,
            time_path,
            state: LruCache::new(NonZeroUsize::new(args.cache_size).expect("non-zero cache size")),
            gap_seconds: args.gap_seconds,
            max_group_size: args.max_group_size,
            min_group_size: args.output_group_size,
            input_cnt: 0,
            output_cnt: 0,
            skipped_group: 0,
            missing_timestamp_cnt: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_cnt += 1;
        let key = self.key_path.search(&input)?;
        if key.is_null() {
            return Ok(vec![]);
        }

        let event_time = match &self.time_path {
            Some(time_path) => {
                let event_timestring = match time_path.search(&input) {
                    Ok(timestring) => timestring,
                    _ => {
                        self.missing_timestamp_cnt += 1;
                        return Ok(vec![]);
                    }
                };
                if let Ok(dt) = DateTime::parse_from_rfc3339(
                    event_timestring.as_string().unwrap_or(&"".to_string()),
                ) {
                    dt.timestamp() as u64
                } else {
                    self.missing_timestamp_cnt += 1;
                    return Ok(vec![]);
                }
            }
            _ => SystemTime::now().duration_since(UNIX_EPOCH).expect("since 1970").as_secs(),
        };

        let mut output_events: Vec<Event> = Vec::new();
        let keystr = if let Some(s) = key.as_string() { s.to_string() } else { key.to_string() };
        let burst_group = match self.state.get_mut(&keystr) {
            Some(group) => group,
            None => {
                if let Some((_, expired_group)) =
                    self.state.push(keystr.clone(), BurstGroup::default())
                {
                    //we expired something when adding this new key
                    if expired_group.group.len() >= self.min_group_size {
                        output_events.push(create_group_event(&expired_group.group)?);
                        self.output_cnt += 1;
                    } else {
                        self.skipped_group += 1;
                    }
                }
                self.state.get_mut(&keystr).expect("just inserted")
            }
        };

        //have we exceeded the max gap time?
        if !burst_group.group.is_empty()
            && (burst_group.last_time < event_time)
            && ((event_time - burst_group.last_time) > self.gap_seconds)
        {
            //flush this old group before adding a new group at key
            if burst_group.group.len() >= self.min_group_size {
                output_events.push(create_group_event(&burst_group.group)?);
                self.output_cnt += 1;
            } else {
                self.skipped_group += 1;
            }
            burst_group.group = Vec::new();
        }
        burst_group.last_time = std::cmp::max(event_time, burst_group.last_time);
        burst_group.group.push(input.clone());

        if burst_group.group.len() >= self.max_group_size {
            output_events.push(create_group_event(&burst_group.group)?);
            self.output_cnt += 1;
            self.state.pop(&keystr);
        }
        Ok(output_events)
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events: Vec<Event> = vec![];
        while let Some((_, burst_group)) = self.state.pop_lru() {
            if burst_group.group.len() >= self.min_group_size {
                if let Ok(group_event) = create_group_event(&burst_group.group) {
                    events.push(group_event);
                    self.output_cnt += 1;
                    if events.len() > MAX_FLUSH {
                        break;
                    }
                }
            } else {
                self.skipped_group += 1;
            }
        }
        events
    }

    fn stats(&self) -> Option<String> {
        Some(format!(
            "input:{}\noutput:{}\nless_than_min_size_groups:{}\nmissing_timestamp:{}",
            self.input_cnt, self.output_cnt, self.skipped_group, self.missing_timestamp_cnt
        ))
    }
}

#[derive(SerialProcessorInit)]
pub struct LastNProcessor<'a> {
    key_path: jmespath::Expression<'a>,
    state: LruCache<String, VecDeque<Event>>,
    max_group_size: usize,
    min_group_size: usize,
    group_label: String,
    count_label: String,
}

#[derive(Parser)]
/// Statefully store the last event at a given key, output at end of stream
#[command(version, long_about = None, arg_required_else_help(true))]
struct LastNArgs {
    #[arg(required(true))]
    key: Vec<String>,

    #[arg(short, long, default_value_t = 1)]
    max_group_size: usize,

    // group size worth printing when flushing buffers
    #[arg(short, long, default_value_t = 1)]
    output_group_size: usize,

    #[arg(short, long, default_value_t = CACHE_SIZE)]
    cache_size: usize,

    #[arg(short, long, default_value = "group")]
    group_label: String,
}

impl SerialProcessor for LastNProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("output last N events".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = LastNArgs::try_parse_from(argv)?;
        let key_path = jmespath::compile(&args.key.join(" "))?;

        let count_label = args.group_label.clone() + "_length";

        Ok(LastNProcessor {
            key_path,
            state: LruCache::new(NonZeroUsize::new(args.cache_size).expect("non-zero cache size")),
            max_group_size: args.max_group_size,
            min_group_size: args.output_group_size,
            group_label: args.group_label,
            count_label,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        let key = self.key_path.search(&input)?;
        if key.is_null() {
            return Ok(vec![]);
        }

        let keystr = if let Some(s) = key.as_string() { s.to_string() } else { key.to_string() };
        let group = match self.state.get_mut(&keystr) {
            Some(group) => group,
            None => {
                let mut new_entry: VecDeque<Event> = VecDeque::new();
                new_entry.push_front(input.clone());
                if let Some((_, expired_group)) = self.state.push(keystr, new_entry) {
                    let output = self.create_group_event_from_queue(&expired_group)?;
                    return Ok(vec![output]);
                } else {
                    return Ok(vec![]);
                }
            }
        };

        if group.len() >= self.max_group_size {
            group.pop_back();
        }
        //keep a reference to past events in the state table
        group.push_front(input.clone());
        Ok(vec![])
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events: Vec<Event> = vec![];
        while let Some((_, group)) = self.state.pop_lru() {
            if group.len() >= self.min_group_size {
                if let Ok(group_event) = self.create_group_event_from_queue(&group) {
                    events.push(group_event);
                }
            }

            if events.len() >= MAX_FLUSH {
                return events;
            }
        }
        events
    }
}

impl LastNProcessor<'_> {
    fn create_group_event_from_queue(
        &self,
        group: &VecDeque<Event>,
    ) -> Result<Event, anyhow::Error> {
        if self.max_group_size == 1 {
            Ok(group.front().expect("has one record").clone())
        } else {
            let mut output = serde_json::Map::new();

            output.insert(self.count_label.to_string(), serde_json::to_value(group.len())?);
            output.insert(self.group_label.to_string(), serde_json::to_value(group)?);
            let output_value: serde_json::Value = output.into();
            Ok(Rc::new(RefCell::new(output_value)))
        }
    }
}

#[derive(SerialProcessorInit)]
pub struct KeySumProcessor<'a> {
    value_path: jmespath::Expression<'a>,
    key_path: Option<jmespath::Expression<'a>>,
    state: LruCache<String, f64>,
    sum_label: String,
    key_label: String,
}

#[derive(Parser)]
/// Sum values for a given key and value, output upon exit
#[command(version, long_about = None, arg_required_else_help(true))]
struct KeySumArgs {
    #[arg(required(true))]
    value: Vec<String>,

    #[arg(short, long)]
    key: Option<String>,

    #[arg(short, long, default_value = "count")]
    sum_label: String,

    #[arg(short, long, default_value = "key")]
    label_key: String,

    #[arg(short, long, default_value_t = CACHE_SIZE)]
    cache_size: usize,
}

impl SerialProcessor for KeySumProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("sum numeric values by JMESPath key".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = KeySumArgs::try_parse_from(argv)?;
        let value_path = jmespath::compile(&args.value.join(" "))?;

        let key_path = match args.key {
            Some(key) => Some(jmespath::compile(&key)?),
            _ => None,
        };

        Ok(KeySumProcessor {
            value_path,
            key_path,
            state: LruCache::new(NonZeroUsize::new(args.cache_size).expect("non-zero cache size")),
            sum_label: args.sum_label,
            key_label: args.label_key,
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
                //if result is a string - keep it a string
                if let Some(s) = key.as_string() {
                    s.to_string()
                } else {
                    key.to_string()
                }
            }
            _ => "total".to_string(),
        };

        //update state
        match self.state.get_mut(&key) {
            Some(state) => *state += value,
            None => {
                //does pushing something in, push something out?
                if let Some((expired_key, expired_state)) = self.state.push(key, value) {
                    let event = self.create_event(expired_key, expired_state)?;
                    return Ok(vec![event]);
                }
            }
        };

        Ok(vec![])
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events: Vec<Event> = vec![];
        while let Some((key, value)) = self.state.pop_lru() {
            if let Ok(event) = self.create_event(key, value) {
                events.push(event);
            }

            if events.len() >= MAX_FLUSH {
                return events;
            }
        }
        events
    }
}

impl KeySumProcessor<'_> {
    fn create_event(&self, key: String, sum: f64) -> Result<Event, anyhow::Error> {
        let mut output = serde_json::Map::new();

        output.insert(self.key_label.to_string(), serde_json::to_value(key)?);
        output.insert(self.sum_label.to_string(), serde_json::to_value(sum)?);

        let output_value: serde_json::Value = output.into();
        Ok(Rc::new(RefCell::new(output_value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a test event with a key and timestamp
    fn create_test_event(key: &str, timestamp: Option<&str>) -> Event {
        let mut map = serde_json::Map::new();
        map.insert("key".to_string(), serde_json::Value::String(key.to_string()));

        if let Some(ts) = timestamp {
            map.insert("timestamp".to_string(), serde_json::Value::String(ts.to_string()));
        }

        Rc::new(RefCell::new(serde_json::Value::Object(map)))
    }

    // Helper function to extract group length from a group event
    fn get_group_length(event: &Event) -> usize {
        event.borrow().get("group_length").unwrap().as_u64().unwrap() as usize
    }

    // ---- GroupEventsProcessor tests ----

    fn make_event(key: &str) -> Event {
        Rc::new(RefCell::new(serde_json::json!({ "key": key })))
    }

    fn make_event_val(obj: serde_json::Value) -> Event {
        Rc::new(RefCell::new(obj))
    }

    #[test]
    fn test_group_events_initialization() {
        let args = vec!["group_events".into(), "key".into()];
        let p = GroupEventsProcessor::new(&args).unwrap();
        assert_eq!(p.max_group_size, MAX_GROUP_SIZE);
        assert_eq!(p.min_group_size, GROUP_EVENTS_MIN_GROUP_SIZE);
        assert_eq!(p.input_count, 0);
        assert_eq!(p.output_count, 0);
    }

    #[test]
    fn test_group_events_custom_args() {
        let args = vec![
            "group_events".into(),
            "key".into(),
            "--max-group-size".into(),
            "5".into(),
            "--output-group-size".into(),
            "3".into(),
            "--cache-size".into(),
            "100".into(),
        ];
        let p = GroupEventsProcessor::new(&args).unwrap();
        assert_eq!(p.max_group_size, 5);
        assert_eq!(p.min_group_size, 3);
    }

    #[test]
    fn test_group_events_null_key_dropped() {
        let args = vec!["group_events".into(), "missing_field".into()];
        let mut p = GroupEventsProcessor::new(&args).unwrap();
        let result = p.process(make_event("val")).unwrap();
        assert!(result.is_empty());
        assert_eq!(p.input_count, 1);
    }

    #[test]
    fn test_group_events_accumulates_until_max() {
        let args = vec!["group_events".into(), "key".into(), "--max-group-size".into(), "3".into()];
        let mut p = GroupEventsProcessor::new(&args).unwrap();

        assert!(p.process(make_event("a")).unwrap().is_empty());
        assert!(p.process(make_event("a")).unwrap().is_empty());

        let result = p.process(make_event("a")).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].borrow()["group_length"], 3);
        assert_eq!(p.output_count, 1);
    }

    #[test]
    fn test_group_events_separate_keys() {
        let args = vec!["group_events".into(), "key".into(), "--max-group-size".into(), "2".into()];
        let mut p = GroupEventsProcessor::new(&args).unwrap();

        assert!(p.process(make_event("a")).unwrap().is_empty());
        assert!(p.process(make_event("b")).unwrap().is_empty());

        // completing key "a"
        let result = p.process(make_event("a")).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].borrow()["group_length"], 2);

        // key "b" still buffered
        assert!(p.process(make_event("b")).unwrap().len() == 1);
    }

    #[test]
    fn test_group_events_flush_respects_min_group_size() {
        let args =
            vec!["group_events".into(), "key".into(), "--output-group-size".into(), "3".into()];
        let mut p = GroupEventsProcessor::new(&args).unwrap();

        // Only 2 events for key "a" — below min_group_size of 3
        p.process(make_event("a")).unwrap();
        p.process(make_event("a")).unwrap();

        let flushed = p.flush();
        assert!(
            flushed.is_empty(),
            "groups smaller than min_group_size should be dropped on flush"
        );
    }

    #[test]
    fn test_group_events_flush_outputs_qualifying_groups() {
        let args =
            vec!["group_events".into(), "key".into(), "--output-group-size".into(), "1".into()];
        let mut p = GroupEventsProcessor::new(&args).unwrap();

        p.process(make_event("a")).unwrap();
        p.process(make_event("b")).unwrap();

        let flushed = p.flush();
        assert_eq!(flushed.len(), 2);
        assert_eq!(p.output_count, 2);
    }

    #[test]
    fn test_group_events_lru_eviction() {
        let args = vec![
            "group_events".into(),
            "key".into(),
            "--cache-size".into(),
            "2".into(),
            "--output-group-size".into(),
            "1".into(),
        ];
        let mut p = GroupEventsProcessor::new(&args).unwrap();

        p.process(make_event("a")).unwrap();
        p.process(make_event("b")).unwrap();

        // inserting "c" should evict "a" (LRU)
        let result = p.process(make_event("c")).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].borrow()["group_length"], 1);
        assert_eq!(p.output_count, 1);
    }

    #[test]
    fn test_group_events_evicted_group_below_min_dropped() {
        let args = vec![
            "group_events".into(),
            "key".into(),
            "--cache-size".into(),
            "2".into(),
            "--output-group-size".into(),
            "2".into(), // need at least 2 to emit
        ];
        let mut p = GroupEventsProcessor::new(&args).unwrap();

        // one event each — both below min
        p.process(make_event("a")).unwrap();
        p.process(make_event("b")).unwrap();

        // evicts "a" (size 1 < min 2) — should NOT produce output
        let result = p.process(make_event("c")).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_group_events_stats() {
        let args = vec!["group_events".into(), "key".into(), "--max-group-size".into(), "2".into()];
        let mut p = GroupEventsProcessor::new(&args).unwrap();

        p.process(make_event("a")).unwrap();
        p.process(make_event("a")).unwrap(); // triggers output
        p.process(make_event("b")).unwrap();

        let stats = p.stats().unwrap();
        assert!(stats.contains("input:3"));
        assert!(stats.contains("output:1"));
    }

    #[test]
    fn test_group_events_group_contains_original_events() {
        let args = vec!["group_events".into(), "key".into(), "--max-group-size".into(), "2".into()];
        let mut p = GroupEventsProcessor::new(&args).unwrap();

        p.process(make_event_val(serde_json::json!({"key": "x", "val": 1}))).unwrap();
        let result = p.process(make_event_val(serde_json::json!({"key": "x", "val": 2}))).unwrap();

        assert_eq!(result.len(), 1);
        let group = result[0].borrow();
        let arr = group["group"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["val"], 1);
        assert_eq!(arr[1]["val"], 2);
    }

    #[test]
    fn test_group_events_non_string_key() {
        // jmespath key that resolves to a number
        let args = vec!["group_events".into(), "num".into(), "--max-group-size".into(), "2".into()];
        let mut p = GroupEventsProcessor::new(&args).unwrap();

        let e1 = make_event_val(serde_json::json!({"num": 42}));
        let e2 = make_event_val(serde_json::json!({"num": 42}));

        p.process(e1).unwrap();
        let result = p.process(e2).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].borrow()["group_length"], 2);
    }

    #[test]
    fn test_group_events_state_cleared_after_max() {
        let args = vec!["group_events".into(), "key".into(), "--max-group-size".into(), "2".into()];
        let mut p = GroupEventsProcessor::new(&args).unwrap();

        p.process(make_event("a")).unwrap();
        let r = p.process(make_event("a")).unwrap();
        assert_eq!(r.len(), 1); // group emitted, key removed from state

        // "a" should be gone from state — new event starts a fresh group
        assert!(p.process(make_event("a")).unwrap().is_empty());

        // flush emits the single buffered event (min_group_size=1)
        let flushed = p.flush();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].borrow()["group_length"], 1);
    }

    #[test]
    fn test_group_events_no_args_fails() {
        let args = vec!["group_events".into()];
        assert!(GroupEventsProcessor::new(&args).is_err());
    }

    // ---- BurstGroupProcessor tests ----

    #[test]
    fn test_burst_group_processor_initialization() {
        // Test with minimal arguments
        let args = vec!["burst_group".to_string(), "key".to_string()];

        let processor = BurstGroupProcessor::new(&args).unwrap();
        assert_eq!(processor.gap_seconds, GAP_MAX_DEFAULT);
        assert_eq!(processor.max_group_size, MAX_GROUP_SIZE);
        assert_eq!(processor.min_group_size, GROUP_SIZE);
        assert_eq!(processor.input_cnt, 0);
        assert_eq!(processor.output_cnt, 0);
        assert_eq!(processor.skipped_group, 0);
        assert_eq!(processor.missing_timestamp_cnt, 0);
        assert!(processor.time_path.is_none());

        // Test with all arguments
        let args = vec![
            "burst_group".to_string(),
            "key".to_string(),
            "--time-path".to_string(),
            "timestamp".to_string(),
            "--gap-seconds".to_string(),
            "60".to_string(),
            "--max-group-size".to_string(),
            "5".to_string(),
            "--output-group-size".to_string(),
            "3".to_string(),
            "--cache-size".to_string(),
            "100".to_string(),
        ];

        let processor = BurstGroupProcessor::new(&args).unwrap();
        assert_eq!(processor.gap_seconds, 60);
        assert_eq!(processor.max_group_size, 5);
        assert_eq!(processor.min_group_size, 3);
        assert!(processor.time_path.is_some());
    }

    #[test]
    fn test_burst_group_processor_null_key() {
        let args = vec!["burst_group".to_string(), "missing_key".to_string()];

        let mut processor = BurstGroupProcessor::new(&args).unwrap();
        let event = create_test_event("test_key", Some("2023-01-01T00:00:00Z"));

        // Process event with a key that doesn't exist in the event
        let result = processor.process(event).unwrap();

        // Should return empty vector as the key is null
        assert!(result.is_empty());
        assert_eq!(processor.input_cnt, 1);
        assert_eq!(processor.output_cnt, 0);
    }

    #[test]
    fn test_burst_group_processor_missing_timestamp() {
        let args = vec![
            "burst_group".to_string(),
            "key".to_string(),
            "--time-path".to_string(),
            "missing_timestamp".to_string(),
        ];

        let mut processor = BurstGroupProcessor::new(&args).unwrap();
        let event = create_test_event("test_key", Some("2023-01-01T00:00:00Z"));

        // Process event with a timestamp path that doesn't exist in the event
        let result = processor.process(event).unwrap();

        // Should return empty vector as the timestamp is missing
        assert!(result.is_empty());
        assert_eq!(processor.input_cnt, 1);
        assert_eq!(processor.missing_timestamp_cnt, 1);
    }

    #[test]
    fn test_burst_group_processor_invalid_timestamp() {
        let args = vec![
            "burst_group".to_string(),
            "key".to_string(),
            "--time-path".to_string(),
            "timestamp".to_string(),
        ];

        let mut processor = BurstGroupProcessor::new(&args).unwrap();

        // Create event with invalid timestamp format
        let mut map = serde_json::Map::new();
        map.insert("key".to_string(), serde_json::Value::String("test_key".to_string()));
        map.insert(
            "timestamp".to_string(),
            serde_json::Value::String("invalid-timestamp".to_string()),
        );
        let event = Rc::new(RefCell::new(serde_json::Value::Object(map)));

        // Process event with invalid timestamp
        let result = processor.process(event).unwrap();

        // Should return empty vector as the timestamp is invalid
        assert!(result.is_empty());
        assert_eq!(processor.input_cnt, 1);
        assert_eq!(processor.missing_timestamp_cnt, 1);
    }

    #[test]
    fn test_burst_group_processor_max_group_size() {
        let args = vec![
            "burst_group".to_string(),
            "key".to_string(),
            "--time-path".to_string(),
            "timestamp".to_string(),
            "--max-group-size".to_string(),
            "3".to_string(),
        ];

        let mut processor = BurstGroupProcessor::new(&args).unwrap();

        // Create and process 3 events with the same key and timestamps close together
        let event1 = create_test_event("test_key", Some("2023-01-01T00:00:00Z"));
        let event2 = create_test_event("test_key", Some("2023-01-01T00:00:10Z"));
        let event3 = create_test_event("test_key", Some("2023-01-01T00:00:20Z"));

        let result1 = processor.process(event1).unwrap();
        assert!(result1.is_empty());

        let result2 = processor.process(event2).unwrap();
        assert!(result2.is_empty());

        // Third event should trigger output as we reach max_group_size
        let result3 = processor.process(event3).unwrap();
        assert_eq!(result3.len(), 1);
        assert_eq!(get_group_length(&result3[0]), 3);
        assert_eq!(processor.output_cnt, 1);
    }

    #[test]
    fn test_burst_group_processor_time_gap() {
        let args = vec![
            "burst_group".to_string(),
            "key".to_string(),
            "--time-path".to_string(),
            "timestamp".to_string(),
            "--gap-seconds".to_string(),
            "30".to_string(),
            "--output-group-size".to_string(),
            "1".to_string(),
        ];

        let mut processor = BurstGroupProcessor::new(&args).unwrap();

        // Create and process 2 events with the same key but timestamps far apart
        let event1 = create_test_event("test_key", Some("2023-01-01T00:00:00Z"));
        let event2 = create_test_event("test_key", Some("2023-01-01T00:01:00Z")); // 60 seconds later

        let result1 = processor.process(event1).unwrap();
        assert!(result1.is_empty());

        // Second event should trigger output as the time gap exceeds gap_seconds (30)
        let result2 = processor.process(event2).unwrap();
        assert_eq!(result2.len(), 1);
        assert_eq!(get_group_length(&result2[0]), 1);
        assert_eq!(processor.output_cnt, 1);
    }

    #[test]
    fn test_burst_group_processor_multiple_keys() {
        let args = vec![
            "burst_group".to_string(),
            "key".to_string(),
            "--time-path".to_string(),
            "timestamp".to_string(),
            "--max-group-size".to_string(),
            "2".to_string(),
        ];

        let mut processor = BurstGroupProcessor::new(&args).unwrap();

        // Create and process events with different keys
        let event1 = create_test_event("key1", Some("2023-01-01T00:00:00Z"));
        let event2 = create_test_event("key2", Some("2023-01-01T00:00:10Z"));
        let event3 = create_test_event("key1", Some("2023-01-01T00:00:20Z"));

        let result1 = processor.process(event1).unwrap();
        assert!(result1.is_empty());

        let result2 = processor.process(event2).unwrap();
        assert!(result2.is_empty());

        // Third event should trigger output for key1 as we reach max_group_size
        let result3 = processor.process(event3).unwrap();
        assert_eq!(result3.len(), 1);
        assert_eq!(get_group_length(&result3[0]), 2);
        assert_eq!(processor.output_cnt, 1);
    }

    #[test]
    fn test_burst_group_processor_flush() {
        let args = vec![
            "burst_group".to_string(),
            "key".to_string(),
            "--time-path".to_string(),
            "timestamp".to_string(),
            "--output-group-size".to_string(),
            "1".to_string(),
        ];

        let mut processor = BurstGroupProcessor::new(&args).unwrap();

        // Create and process events with different keys
        let event1 = create_test_event("key1", Some("2023-01-01T00:00:00Z"));
        let event2 = create_test_event("key2", Some("2023-01-01T00:00:10Z"));

        processor.process(event1).unwrap();
        processor.process(event2).unwrap();

        // Flush should output both groups
        let flush_results = processor.flush();
        assert_eq!(flush_results.len(), 2);
        assert_eq!(processor.output_cnt, 2);
    }

    #[test]
    fn test_burst_group_processor_stats() {
        let args = vec!["burst_group".to_string(), "key".to_string()];

        let mut processor = BurstGroupProcessor::new(&args).unwrap();

        // Process some events to generate stats
        let event = create_test_event("test_key", None);
        processor.process(event.clone()).unwrap();
        processor.process(event.clone()).unwrap();
        processor.process(event).unwrap();
        processor.flush();

        // Check stats output
        let stats = processor.stats().unwrap();
        assert!(stats.contains("input:3"));
        assert!(stats.contains("output:1"));
        assert!(stats.contains("less_than_min_size_groups:0"));
        assert!(stats.contains("missing_timestamp:0"));
    }

    #[test]
    fn test_burst_group_processor_lru_eviction() {
        // Create a processor with a very small cache size
        let args = vec![
            "burst_group".to_string(),
            "key".to_string(),
            "--time-path".to_string(),
            "timestamp".to_string(),
            "--cache-size".to_string(),
            "2".to_string(), // Only 2 keys can be stored
            "--output-group-size".to_string(),
            "1".to_string(),
        ];

        let mut processor = BurstGroupProcessor::new(&args).unwrap();

        // Create and process events with different keys
        let event1 = create_test_event("key1", Some("2023-01-01T00:00:00Z"));
        let event2 = create_test_event("key2", Some("2023-01-01T00:00:10Z"));
        let event3 = create_test_event("key3", Some("2023-01-01T00:00:20Z")); // This should evict key1

        let result1 = processor.process(event1).unwrap();
        assert!(result1.is_empty());

        let result2 = processor.process(event2).unwrap();
        assert!(result2.is_empty());

        // Third event should evict key1 and output it
        let result3 = processor.process(event3).unwrap();
        assert_eq!(result3.len(), 1);
        assert_eq!(processor.output_cnt, 1);
    }

    #[test]
    fn test_burst_group_processor_no_time_path() {
        // Test processor without time_path specified
        let args = vec![
            "burst_group".to_string(),
            "key".to_string(),
            "--max-group-size".to_string(),
            "2".to_string(),
        ];

        let mut processor = BurstGroupProcessor::new(&args).unwrap();

        // Create and process events with the same key
        let event1 = create_test_event("test_key", None);
        let event2 = create_test_event("test_key", None);

        let result1 = processor.process(event1).unwrap();
        assert!(result1.is_empty());

        // Second event should trigger output as we reach max_group_size
        let result2 = processor.process(event2).unwrap();
        assert_eq!(result2.len(), 1);
        assert_eq!(get_group_length(&result2[0]), 2);
        assert_eq!(processor.output_cnt, 1);
    }

    #[test]
    fn test_burst_group_processor_time_ordering() {
        let args = vec![
            "burst_group".to_string(),
            "key".to_string(),
            "--time-path".to_string(),
            "timestamp".to_string(),
            "--gap-seconds".to_string(),
            "30".to_string(),
        ];

        let mut processor = BurstGroupProcessor::new(&args).unwrap();

        // Create events with timestamps out of order
        let event1 = create_test_event("test_key", Some("2023-01-01T00:00:20Z")); // Later timestamp
        let event2 = create_test_event("test_key", Some("2023-01-01T00:00:10Z")); // Earlier timestamp

        processor.process(event1).unwrap();

        // The processor should use the max timestamp, so this shouldn't create a new group
        let result = processor.process(event2).unwrap();
        assert!(result.is_empty());

        // Verify the last_time is still the max of the two timestamps
        let state = processor.state.peek("test_key").unwrap();
        assert_eq!(
            state.last_time,
            DateTime::parse_from_rfc3339("2023-01-01T00:00:20Z").unwrap().timestamp() as u64
        );
        assert_eq!(state.group.len(), 2);
    }

    #[test]
    fn test_burst_group_processor_min_group_size() {
        let args = vec![
            "burst_group".to_string(),
            "key".to_string(),
            "--time-path".to_string(),
            "timestamp".to_string(),
            "--gap-seconds".to_string(),
            "30".to_string(),
            "--output-group-size".to_string(),
            "3".to_string(), // Require at least 3 events to output
        ];

        let mut processor = BurstGroupProcessor::new(&args).unwrap();

        // Create events with a time gap that exceeds gap_seconds
        let event1 = create_test_event("test_key", Some("2023-01-01T00:00:00Z"));
        let event2 = create_test_event("test_key", Some("2023-01-01T00:00:10Z"));
        let event3 = create_test_event("test_key", Some("2023-01-01T00:01:00Z")); // 50 seconds after event2

        processor.process(event1).unwrap();
        processor.process(event2).unwrap();

        // This should trigger a time gap, but the group only has 2 events which is less than min_group_size
        let result = processor.process(event3).unwrap();
        assert!(result.is_empty());
        let result = processor.flush();
        assert!(result.is_empty());
        assert_eq!(processor.skipped_group, 2);
    }
}
