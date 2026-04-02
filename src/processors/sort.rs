use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use ordered_float::OrderedFloat;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::mem::take;

/// Entry for SortProcessor - stores the sort key and event together
#[derive(Debug, Clone)]
struct SortEntry {
    key: String,
    event: Event,
}

#[derive(Debug, SerialProcessorInit)]
pub struct SortProcessor<'a> {
    path: jmespath::Expression<'a>,
    events: Vec<SortEntry>,
    max_event_buffer: usize,
    reverse_sort: bool,
    input_count: usize,
    output_count: usize,
}

#[derive(Parser)]
/// Extract and decode strings from events using JMESPath expressions
#[command(version, long_about = None, arg_required_else_help(true))]
struct SortArgs {
    //jmespath of key to sort on
    #[arg(required(true))]
    key: Vec<String>,

    #[arg(short, long, default_value_t = 10000)]
    max_event_buffer: usize,

    #[arg(short, long, default_value_t = false)]
    reverse_sort: bool,
}

impl SerialProcessor for SortProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("sort events by JMESPath key".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = SortArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.key.join(" "))?;
        Ok(SortProcessor {
            path,
            events: Vec::new(),
            max_event_buffer: args.max_event_buffer,
            reverse_sort: args.reverse_sort,
            input_count: 0,
            output_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        let result = self.path.search(&input)?;

        // Handle null/empty key results gracefully
        if result.is_null() {
            return Ok(vec![]);
        }

        self.events.push(SortEntry { key: result.to_string(), event: input });

        if self.events.len() >= self.max_event_buffer {
            Ok(self.flush())
        } else {
            Ok(vec![])
        }
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events = take(&mut self.events);

        if self.reverse_sort {
            events.sort_by(|a, b| b.key.cmp(&a.key));
        } else {
            events.sort_by(|a, b| a.key.cmp(&b.key));
        }

        let result: Vec<Event> = events.into_iter().map(|e| e.event).collect();
        self.output_count += result.len();
        result
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\noutput:{}", self.input_count, self.output_count))
    }
}

/// Entry for NumSortProcessor - stores the numeric sort key and event together
#[derive(Debug, Clone)]
struct NumSortEntry {
    key: OrderedFloat<f64>,
    event: Event,
}

#[derive(Debug, SerialProcessorInit)]
pub struct NumSortProcessor<'a> {
    path: jmespath::Expression<'a>,
    events: Vec<NumSortEntry>,
    max_event_buffer: usize,
    reverse_sort: bool,
    input_count: usize,
    output_count: usize,
}

#[derive(Parser)]
/// Extract and decode strings from events using JMESPath expressions
#[command(version, long_about = None, arg_required_else_help(true))]
struct NumSortArgs {
    //jmespath of key to sort on
    #[arg(required(true))]
    key: Vec<String>,

    #[arg(short, long, default_value_t = 10000)]
    max_event_buffer: usize,

    #[arg(short, long, default_value_t = false)]
    reverse_sort: bool,
}

impl SerialProcessor for NumSortProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("sort events numerically by JMESPath value".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = NumSortArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.key.join(" "))?;
        Ok(NumSortProcessor {
            path,
            events: Vec::new(),
            max_event_buffer: args.max_event_buffer,
            reverse_sort: args.reverse_sort,
            input_count: 0,
            output_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        let result = self.path.search(&input)?;

        // Handle null/empty key results gracefully
        let num = if let Some(num) = result.as_number() {
            num
        } else {
            return Ok(vec![]);
        };

        self.events.push(NumSortEntry { key: OrderedFloat(num), event: input });

        if self.events.len() >= self.max_event_buffer {
            Ok(self.flush())
        } else {
            Ok(vec![])
        }
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events = take(&mut self.events);

        if self.reverse_sort {
            events.sort_by(|a, b| b.key.cmp(&a.key));
        } else {
            events.sort_by(|a, b| a.key.cmp(&b.key));
        }

        let result: Vec<Event> = events.into_iter().map(|e| e.event).collect();
        self.output_count += result.len();
        result
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\noutput:{}", self.input_count, self.output_count))
    }
}

/// Entry for the min-heap used by TopProcessor
/// Stores the sort key and the event together
#[derive(Debug, Clone)]
struct TopEntry {
    key: OrderedFloat<f64>,
    event: Event,
}

impl PartialEq for TopEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for TopEntry {}

impl PartialOrd for TopEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

#[derive(Debug, SerialProcessorInit)]
pub struct TopProcessor<'a> {
    path: jmespath::Expression<'a>,
    // Min-heap for top N largest (we keep smallest at top to efficiently evict)
    min_heap: BinaryHeap<Reverse<TopEntry>>,
    // Max-heap for top N smallest (reverse sort)
    max_heap: BinaryHeap<TopEntry>,
    number_of_elements: usize,
    reverse_sort: bool,
    input_count: usize,
    output_count: usize,
}

#[derive(Parser)]
/// Extract and decode strings from events using JMESPath expressions
#[command(version, long_about = None, arg_required_else_help(true))]
struct TopArgs {
    //jmespath of key to sort on
    #[arg(required(true))]
    key: Vec<String>,

    #[arg(short, long, default_value_t = 10)]
    number_of_elements: usize,

    #[arg(short, long, default_value_t = false)]
    reverse_sort: bool,
}

impl SerialProcessor for TopProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("get top N events by numeric JMESPath value".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = TopArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.key.join(" "))?;
        Ok(TopProcessor {
            path,
            min_heap: BinaryHeap::new(),
            max_heap: BinaryHeap::new(),
            number_of_elements: args.number_of_elements,
            reverse_sort: args.reverse_sort,
            input_count: 0,
            output_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        let result = self.path.search(&input)?;

        let num = if let Some(num) = result.as_number() {
            num
        } else {
            return Ok(vec![]);
        };

        let entry = TopEntry { key: OrderedFloat(num), event: input };

        if self.reverse_sort {
            // For reverse sort (bottom N / smallest values), use max-heap
            // Keep the N smallest values by evicting the largest when full
            if self.max_heap.len() < self.number_of_elements {
                self.max_heap.push(entry);
            } else if let Some(top) = self.max_heap.peek() {
                if entry.key < top.key {
                    self.max_heap.pop();
                    self.max_heap.push(entry);
                }
            }
        } else {
            // For normal sort (top N / largest values), use min-heap
            // Keep the N largest values by evicting the smallest when full
            if self.min_heap.len() < self.number_of_elements {
                self.min_heap.push(Reverse(entry));
            } else if let Some(Reverse(top)) = self.min_heap.peek() {
                if entry.key > top.key {
                    self.min_heap.pop();
                    self.min_heap.push(Reverse(entry));
                }
            }
        }

        Ok(vec![])
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events: Vec<Event> = if self.reverse_sort {
            let heap = take(&mut self.max_heap);
            heap.into_iter().map(|e| e.event).collect()
        } else {
            let heap = take(&mut self.min_heap);
            heap.into_iter().map(|Reverse(e)| e.event).collect()
        };

        // Sort by key for consistent output order (descending for top, ascending for reverse)
        events.sort_by(|a, b| {
            let key_a = self.path.search(a).ok().and_then(|r| r.as_number()).unwrap_or(0.0);
            let key_b = self.path.search(b).ok().and_then(|r| r.as_number()).unwrap_or(0.0);
            if self.reverse_sort {
                OrderedFloat(key_a).cmp(&OrderedFloat(key_b))
            } else {
                OrderedFloat(key_b).cmp(&OrderedFloat(key_a))
            }
        });

        self.output_count += events.len();
        events
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\noutput:{}", self.input_count, self.output_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn make_event(value: f64, id: &str) -> Event {
        Rc::new(RefCell::new(json!({"value": value, "id": id})))
    }

    #[test]
    fn test_top_processor_basic() {
        let mut processor = TopProcessor::new(&[
            "top".to_string(),
            "value".to_string(),
            "-n".to_string(),
            "3".to_string(),
        ])
        .unwrap();

        // Process 5 events
        processor.process(make_event(10.0, "a")).unwrap();
        processor.process(make_event(50.0, "b")).unwrap();
        processor.process(make_event(30.0, "c")).unwrap();
        processor.process(make_event(20.0, "d")).unwrap();
        processor.process(make_event(40.0, "e")).unwrap();

        let results = processor.flush();

        // Should get top 3: 50, 40, 30
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].borrow()["value"], 50.0);
        assert_eq!(results[1].borrow()["value"], 40.0);
        assert_eq!(results[2].borrow()["value"], 30.0);
    }

    #[test]
    fn test_top_processor_with_duplicates() {
        let mut processor = TopProcessor::new(&[
            "top".to_string(),
            "value".to_string(),
            "-n".to_string(),
            "3".to_string(),
        ])
        .unwrap();

        // Process events with duplicate values
        processor.process(make_event(10.0, "a")).unwrap();
        processor.process(make_event(10.0, "b")).unwrap();
        processor.process(make_event(10.0, "c")).unwrap();
        processor.process(make_event(10.0, "d")).unwrap();
        processor.process(make_event(10.0, "e")).unwrap();

        let results = processor.flush();

        // Should get 3 events even with duplicate values
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_top_processor_fewer_than_n() {
        let mut processor = TopProcessor::new(&[
            "top".to_string(),
            "value".to_string(),
            "-n".to_string(),
            "10".to_string(),
        ])
        .unwrap();

        // Process only 3 events when N=10
        processor.process(make_event(30.0, "a")).unwrap();
        processor.process(make_event(10.0, "b")).unwrap();
        processor.process(make_event(20.0, "c")).unwrap();

        let results = processor.flush();

        // Should get all 3 events, sorted descending
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].borrow()["value"], 30.0);
        assert_eq!(results[1].borrow()["value"], 20.0);
        assert_eq!(results[2].borrow()["value"], 10.0);
    }

    #[test]
    fn test_top_processor_reverse_sort() {
        let mut processor = TopProcessor::new(&[
            "top".to_string(),
            "value".to_string(),
            "-n".to_string(),
            "3".to_string(),
            "-r".to_string(),
        ])
        .unwrap();

        // Process 5 events
        processor.process(make_event(10.0, "a")).unwrap();
        processor.process(make_event(50.0, "b")).unwrap();
        processor.process(make_event(30.0, "c")).unwrap();
        processor.process(make_event(20.0, "d")).unwrap();
        processor.process(make_event(40.0, "e")).unwrap();

        let results = processor.flush();

        // Should get bottom 3: 10, 20, 30 (ascending order)
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].borrow()["value"], 10.0);
        assert_eq!(results[1].borrow()["value"], 20.0);
        assert_eq!(results[2].borrow()["value"], 30.0);
    }

    #[test]
    fn test_top_processor_null_values_ignored() {
        let mut processor = TopProcessor::new(&[
            "top".to_string(),
            "value".to_string(),
            "-n".to_string(),
            "3".to_string(),
        ])
        .unwrap();

        processor.process(make_event(10.0, "a")).unwrap();
        processor.process(Rc::new(RefCell::new(json!({"id": "no_value"})))).unwrap(); // No value field
        processor.process(make_event(20.0, "b")).unwrap();
        processor
            .process(Rc::new(RefCell::new(json!({"value": null, "id": "null_value"}))))
            .unwrap();
        processor.process(make_event(30.0, "c")).unwrap();

        let results = processor.flush();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].borrow()["value"], 30.0);
        assert_eq!(results[1].borrow()["value"], 20.0);
        assert_eq!(results[2].borrow()["value"], 10.0);
    }

    #[test]
    fn test_top_processor_stats() {
        let mut processor = TopProcessor::new(&[
            "top".to_string(),
            "value".to_string(),
            "-n".to_string(),
            "2".to_string(),
        ])
        .unwrap();

        processor.process(make_event(10.0, "a")).unwrap();
        processor.process(make_event(20.0, "b")).unwrap();
        processor.process(make_event(30.0, "c")).unwrap();

        let _ = processor.flush();

        let stats = processor.stats().unwrap();
        assert!(stats.contains("input:3"));
        assert!(stats.contains("output:2"));
    }

    #[test]
    fn test_sort_processor_basic() {
        let mut processor = SortProcessor::new(&["sort".to_string(), "name".to_string()]).unwrap();

        processor.process(Rc::new(RefCell::new(json!({"name": "charlie"})))).unwrap();
        processor.process(Rc::new(RefCell::new(json!({"name": "alice"})))).unwrap();
        processor.process(Rc::new(RefCell::new(json!({"name": "bob"})))).unwrap();

        let results = processor.flush();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].borrow()["name"], "alice");
        assert_eq!(results[1].borrow()["name"], "bob");
        assert_eq!(results[2].borrow()["name"], "charlie");
    }

    #[test]
    fn test_sort_processor_with_duplicates() {
        let mut processor = SortProcessor::new(&["sort".to_string(), "name".to_string()]).unwrap();

        processor.process(Rc::new(RefCell::new(json!({"name": "alice", "id": 1})))).unwrap();
        processor.process(Rc::new(RefCell::new(json!({"name": "alice", "id": 2})))).unwrap();
        processor.process(Rc::new(RefCell::new(json!({"name": "alice", "id": 3})))).unwrap();
        processor.process(Rc::new(RefCell::new(json!({"name": "bob", "id": 4})))).unwrap();

        let results = processor.flush();

        // Should get all 4 events even with duplicate names
        assert_eq!(results.len(), 4);
        // First 3 should be alice (in some order), last should be bob
        assert_eq!(results[0].borrow()["name"], "alice");
        assert_eq!(results[1].borrow()["name"], "alice");
        assert_eq!(results[2].borrow()["name"], "alice");
        assert_eq!(results[3].borrow()["name"], "bob");
    }

    #[test]
    fn test_sort_processor_reverse() {
        let mut processor =
            SortProcessor::new(&["sort".to_string(), "name".to_string(), "-r".to_string()])
                .unwrap();

        processor.process(Rc::new(RefCell::new(json!({"name": "alice"})))).unwrap();
        processor.process(Rc::new(RefCell::new(json!({"name": "charlie"})))).unwrap();
        processor.process(Rc::new(RefCell::new(json!({"name": "bob"})))).unwrap();

        let results = processor.flush();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].borrow()["name"], "charlie");
        assert_eq!(results[1].borrow()["name"], "bob");
        assert_eq!(results[2].borrow()["name"], "alice");
    }

    #[test]
    fn test_numsort_processor_basic() {
        let mut processor =
            NumSortProcessor::new(&["numsort".to_string(), "value".to_string()]).unwrap();

        processor.process(make_event(30.0, "a")).unwrap();
        processor.process(make_event(10.0, "b")).unwrap();
        processor.process(make_event(20.0, "c")).unwrap();

        let results = processor.flush();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].borrow()["value"], 10.0);
        assert_eq!(results[1].borrow()["value"], 20.0);
        assert_eq!(results[2].borrow()["value"], 30.0);
    }

    #[test]
    fn test_numsort_processor_with_duplicates() {
        let mut processor =
            NumSortProcessor::new(&["numsort".to_string(), "value".to_string()]).unwrap();

        processor.process(make_event(10.0, "a")).unwrap();
        processor.process(make_event(10.0, "b")).unwrap();
        processor.process(make_event(10.0, "c")).unwrap();
        processor.process(make_event(20.0, "d")).unwrap();

        let results = processor.flush();

        // Should get all 4 events even with duplicate values
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].borrow()["value"], 10.0);
        assert_eq!(results[1].borrow()["value"], 10.0);
        assert_eq!(results[2].borrow()["value"], 10.0);
        assert_eq!(results[3].borrow()["value"], 20.0);
    }

    #[test]
    fn test_numsort_processor_reverse() {
        let mut processor =
            NumSortProcessor::new(&["numsort".to_string(), "value".to_string(), "-r".to_string()])
                .unwrap();

        processor.process(make_event(10.0, "a")).unwrap();
        processor.process(make_event(30.0, "b")).unwrap();
        processor.process(make_event(20.0, "c")).unwrap();

        let results = processor.flush();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].borrow()["value"], 30.0);
        assert_eq!(results[1].borrow()["value"], 20.0);
        assert_eq!(results[2].borrow()["value"], 10.0);
    }
}
