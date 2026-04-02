use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lru::LruCache;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::num::NonZeroUsize;

const CACHE_SIZE: usize = 200000;

enum ComparisonMode {
    Previous,
    Baseline(HashMap<String, f64>),
    Value(f64),
}

#[derive(Parser)]
/// Compute differences between current values and reference values
#[command(
    version,
    long_about = None,
    arg_required_else_help(true),
    after_help = r#"MODES (exactly one required):

  --previous     Track changes over time per key. First occurrence passes through
                 unchanged; subsequent occurrences get diff fields computed against
                 the stored previous value. Uses an LRU cache (see --cache-size).

  --baseline F   Compare against reference values loaded from a JSON file.
                 Keys not found in the file pass through unchanged.

  --value N      Compare every event against a fixed constant.

OUTPUT FIELDS:

  {field}_diff        = current - reference
  {field}_pct_change  = ((current - reference) / reference) * 100
                        (null when reference is 0)

EXAMPLES:

  Track week-over-week score changes per account:
    diff -k account -f score --previous

  Compare scores against known baselines:
    diff -k account -f score --baseline baselines.json

  Measure deviation from a threshold:
    diff -k account -f score --value 0.5

  Previous mode with larger cache:
    diff -k account -f score --previous --cache-size 500000

BASELINE FILE FORMAT:

  {"account_123": 0.75, "account_456": 0.60, ...}

INPUT:
  {"account": "123", "score": 0.95}

OUTPUT (with --value 0.5):
  {"account": "123", "score": 0.95, "score_diff": 0.45, "score_pct_change": 90.0}
"#,
)]
struct DiffProcessorArgs {
    /// JMESPath expression to extract the grouping key from each event
    #[arg(short, long, required = true)]
    key: String,

    /// JMESPath expression to extract the numeric value to diff
    #[arg(short, long, required = true)]
    field: String,

    /// Compare to previous value for same key
    #[arg(long, conflicts_with_all = ["baseline", "value"])]
    previous: bool,

    /// Compare to baseline values from a JSON file
    #[arg(long, conflicts_with_all = ["previous", "value"], value_name = "FILE")]
    baseline: Option<String>,

    /// Compare to a fixed constant value
    #[arg(long, conflicts_with_all = ["previous", "baseline"], value_name = "NUM")]
    value: Option<f64>,

    /// LRU cache capacity for previous mode
    #[arg(long, default_value_t = CACHE_SIZE, value_name = "N")]
    cache_size: usize,
}

#[derive(SerialProcessorInit)]
pub struct DiffProcessor<'a> {
    key_path: jmespath::Expression<'a>,
    field_path: jmespath::Expression<'a>,
    field_name: String,
    mode: ComparisonMode,
    previous_cache: Option<LruCache<String, f64>>,
    input_count: u64,
    diff_count: u64,
}

fn load_baseline_file(path: &str) -> Result<HashMap<String, f64>> {
    let file = File::open(path).map_err(|e| {
        anyhow::anyhow!(
            "Failed to open baseline file '{}': {}. Expected format: {{\"key1\": 0.5, \"key2\": 0.8, ...}}",
            path,
            e
        )
    })?;
    let reader = BufReader::new(file);
    let baseline: HashMap<String, f64> = serde_json::from_reader(reader).map_err(|e| {
        anyhow::anyhow!(
            "Failed to parse baseline file '{}': {}. Expected format: {{\"key1\": 0.5, \"key2\": 0.8, ...}}",
            path,
            e
        )
    })?;
    Ok(baseline)
}

/// Compute diff and pct_change between current and reference values.
/// Returns (diff, Option<pct_change>). pct_change is None when reference is zero.
fn compute_diff(current: f64, reference: f64) -> (f64, Option<f64>) {
    let diff = current - reference;
    let pct_change = if reference == 0.0 { None } else { Some((diff / reference) * 100.0) };
    (diff, pct_change)
}

/// Add diff and pct_change fields to an event.
/// The field names are `{field_name}_diff` and `{field_name}_pct_change`.
/// pct_change is set to null if the reference value was zero.
fn add_diff_fields(event: &Event, field_name: &str, diff: f64, pct_change: Option<f64>) {
    if let Some(obj) = event.borrow_mut().as_object_mut() {
        obj.insert(format!("{}_diff", field_name), serde_json::Value::from(diff));
        obj.insert(
            format!("{}_pct_change", field_name),
            match pct_change {
                Some(v) => serde_json::Value::from(v),
                None => serde_json::Value::Null,
            },
        );
    }
}

impl SerialProcessor for DiffProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some(
            "compute differences between current values and reference values (previous, baseline, or fixed)"
                .to_string(),
        )
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = DiffProcessorArgs::try_parse_from(argv)?;

        // Validate exactly one mode is specified
        if !args.previous && args.baseline.is_none() && args.value.is_none() {
            return Err(anyhow::anyhow!(
                "Exactly one comparison mode must be specified: --previous, --baseline <file>, or --value <number>"
            ));
        }

        let key_path = jmespath::compile(&args.key)?;
        let field_path = jmespath::compile(&args.field)?;

        // Extract field name from field path for output field naming
        let field_name = args.field.split('.').next_back().unwrap_or(&args.field).to_string();

        let (mode, previous_cache) = if args.previous {
            (
                ComparisonMode::Previous,
                Some(LruCache::new(
                    NonZeroUsize::new(args.cache_size).expect("non-zero cache size"),
                )),
            )
        } else if let Some(baseline_path) = &args.baseline {
            let baseline = load_baseline_file(baseline_path)?;
            (ComparisonMode::Baseline(baseline), None)
        } else if let Some(value) = args.value {
            (ComparisonMode::Value(value), None)
        } else {
            unreachable!("clap validation ensures one mode is specified")
        };

        Ok(Self {
            key_path,
            field_path,
            field_name,
            mode,
            previous_cache,
            input_count: 0,
            diff_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        // Extract field value
        let field_result = self.field_path.search(&input)?;
        let current_value = match field_result.as_number() {
            Some(v) => v,
            None => return Ok(vec![input]), // Non-numeric or null: pass through unchanged
        };

        match &self.mode {
            ComparisonMode::Previous => {
                // Extract key
                let key = self.key_path.search(&input)?;
                if key.is_null() {
                    return Ok(vec![input]); // Null key: pass through unchanged
                }
                let keystr =
                    if let Some(s) = key.as_string() { s.to_string() } else { key.to_string() };

                let cache = self.previous_cache.as_mut().expect("cache exists in previous mode");

                match cache.get(&keystr).copied() {
                    Some(previous_value) => {
                        // Key seen before: compute diff, update cache, add fields
                        let (diff, pct_change) = compute_diff(current_value, previous_value);
                        cache.put(keystr, current_value);
                        add_diff_fields(&input, &self.field_name, diff, pct_change);
                        self.diff_count += 1;
                        Ok(vec![input])
                    }
                    None => {
                        // First occurrence: store value, pass through unchanged
                        cache.put(keystr, current_value);
                        Ok(vec![input])
                    }
                }
            }
            ComparisonMode::Baseline(baseline) => {
                // Extract key
                let key = self.key_path.search(&input)?;
                if key.is_null() {
                    return Ok(vec![input]); // Null key: pass through unchanged
                }
                let keystr =
                    if let Some(s) = key.as_string() { s.to_string() } else { key.to_string() };

                match baseline.get(&keystr) {
                    Some(&reference_value) => {
                        let (diff, pct_change) = compute_diff(current_value, reference_value);
                        add_diff_fields(&input, &self.field_name, diff, pct_change);
                        self.diff_count += 1;
                        Ok(vec![input])
                    }
                    None => {
                        // Key not in baseline: pass through unchanged
                        Ok(vec![input])
                    }
                }
            }
            ComparisonMode::Value(reference_value) => {
                let (diff, pct_change) = compute_diff(current_value, *reference_value);
                add_diff_fields(&input, &self.field_name, diff, pct_change);
                self.diff_count += 1;
                Ok(vec![input])
            }
        }
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\ndiff:{}", self.input_count, self.diff_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cell::RefCell;
    use std::io::Write;
    use std::rc::Rc;
    use tempfile::NamedTempFile;

    fn make_event(val: serde_json::Value) -> Event {
        Rc::new(RefCell::new(val))
    }

    fn get_f64(event: &Event, key: &str) -> Option<f64> {
        event.borrow().get(key).and_then(|v| v.as_f64())
    }

    fn is_null_field(event: &Event, key: &str) -> bool {
        event.borrow().get(key).is_some_and(|v| v.is_null())
    }

    fn has_field(event: &Event, key: &str) -> bool {
        event.borrow().get(key).is_some()
    }

    fn create_baseline_file(data: &serde_json::Value) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        serde_json::to_writer(&mut file, data).unwrap();
        file.flush().unwrap();
        file
    }

    // ── 6.1 Basic functionality tests ────────────────────────────────────

    #[test]
    fn test_init_previous_mode() {
        let args = shlex::split("diff -k account -f score --previous").unwrap();
        DiffProcessor::new(&args).expect("should init in previous mode");
    }

    #[test]
    fn test_init_baseline_mode() {
        let baseline = json!({"a": 1.0});
        let file = create_baseline_file(&baseline);
        let args = vec![
            "diff".into(),
            "-k".into(),
            "account".into(),
            "-f".into(),
            "score".into(),
            "--baseline".into(),
            file.path().to_str().unwrap().into(),
        ];
        DiffProcessor::new(&args).expect("should init in baseline mode");
    }

    #[test]
    fn test_init_value_mode() {
        let args = shlex::split("diff -k account -f score --value 0.5").unwrap();
        DiffProcessor::new(&args).expect("should init in value mode");
    }

    #[test]
    fn test_init_no_mode_fails() {
        let args = shlex::split("diff -k account -f score").unwrap();
        assert!(DiffProcessor::new(&args).is_err());
    }

    #[test]
    fn test_null_key_pass_through() {
        let args = shlex::split("diff -k account -f score --previous").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"score": 0.5})); // no "account" key
        let output = proc.process(event).unwrap();

        assert_eq!(output.len(), 1);
        assert!(!has_field(&output[0], "score_diff"));
        assert!(!has_field(&output[0], "score_pct_change"));
    }

    #[test]
    fn test_null_field_pass_through() {
        let args = shlex::split("diff -k account -f score --previous").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "a1"})); // no "score" field
        let output = proc.process(event).unwrap();

        assert_eq!(output.len(), 1);
        assert!(!has_field(&output[0], "score_diff"));
    }

    #[test]
    fn test_non_numeric_field_pass_through() {
        let args = shlex::split("diff -k account -f score --previous").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "a1", "score": "not_a_number"}));
        let output = proc.process(event).unwrap();

        assert_eq!(output.len(), 1);
        assert!(!has_field(&output[0], "score_diff"));
    }

    // ── 6.2 Diff calculation tests ───────────────────────────────────────

    #[test]
    fn test_diff_calculation() {
        let args = shlex::split("diff -k account -f score --value 0.5").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "a1", "score": 0.8}));
        let output = proc.process(event).unwrap();

        let diff = get_f64(&output[0], "score_diff").unwrap();
        assert!((diff - 0.3).abs() < 1e-10);
    }

    #[test]
    fn test_pct_change_calculation() {
        let args = shlex::split("diff -k account -f score --value 0.5").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "a1", "score": 0.75}));
        let output = proc.process(event).unwrap();

        // pct_change = (0.75 - 0.5) / 0.5 * 100 = 50.0
        let pct = get_f64(&output[0], "score_pct_change").unwrap();
        assert!((pct - 50.0).abs() < 1e-10);
    }

    #[test]
    fn test_division_by_zero_pct_change_null() {
        let args = shlex::split("diff -k account -f score --value 0.0").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "a1", "score": 5.0}));
        let output = proc.process(event).unwrap();

        assert!(is_null_field(&output[0], "score_pct_change"));
        let diff = get_f64(&output[0], "score_diff").unwrap();
        assert!((diff - 5.0).abs() < 1e-10);
    }

    // ── 6.3 Previous mode tests ──────────────────────────────────────────

    #[test]
    fn test_previous_first_occurrence_no_diff() {
        let args = shlex::split("diff -k account -f score --previous").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "a1", "score": 0.5}));
        let output = proc.process(event).unwrap();

        assert_eq!(output.len(), 1);
        assert!(!has_field(&output[0], "score_diff"));
    }

    #[test]
    fn test_previous_subsequent_occurrence_has_diff() {
        let args = shlex::split("diff -k account -f score --previous").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let e1 = make_event(json!({"account": "a1", "score": 0.5}));
        proc.process(e1).unwrap();

        let e2 = make_event(json!({"account": "a1", "score": 0.8}));
        let output = proc.process(e2).unwrap();

        let diff = get_f64(&output[0], "score_diff").unwrap();
        assert!((diff - 0.3).abs() < 1e-10);
        let pct = get_f64(&output[0], "score_pct_change").unwrap();
        // (0.8 - 0.5) / 0.5 * 100 = 60.0
        assert!((pct - 60.0).abs() < 1e-10);
    }

    #[test]
    fn test_previous_cache_update() {
        let args = shlex::split("diff -k account -f score --previous").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        proc.process(make_event(json!({"account": "a1", "score": 1.0}))).unwrap();
        proc.process(make_event(json!({"account": "a1", "score": 3.0}))).unwrap();

        // Third event should diff against 3.0 (updated), not 1.0
        let output = proc.process(make_event(json!({"account": "a1", "score": 5.0}))).unwrap();
        let diff = get_f64(&output[0], "score_diff").unwrap();
        assert!((diff - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_previous_lru_eviction() {
        let args = shlex::split("diff -k account -f score --previous --cache-size 2").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        // Fill cache with 2 keys
        proc.process(make_event(json!({"account": "a1", "score": 1.0}))).unwrap();
        proc.process(make_event(json!({"account": "a2", "score": 2.0}))).unwrap();

        // Add a third key, evicting "a1" (LRU)
        proc.process(make_event(json!({"account": "a3", "score": 3.0}))).unwrap();

        // "a1" should now be treated as first occurrence again (no diff)
        let output = proc.process(make_event(json!({"account": "a1", "score": 5.0}))).unwrap();
        assert!(!has_field(&output[0], "score_diff"));
    }

    // ── 6.4 Baseline mode tests ─────────────────────────────────────────

    #[test]
    fn test_baseline_key_found() {
        let baseline = json!({"a1": 0.5, "a2": 0.8});
        let file = create_baseline_file(&baseline);
        let args = vec![
            "diff".into(),
            "-k".into(),
            "account".into(),
            "-f".into(),
            "score".into(),
            "--baseline".into(),
            file.path().to_str().unwrap().into(),
        ];
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "a1", "score": 0.75}));
        let output = proc.process(event).unwrap();

        let diff = get_f64(&output[0], "score_diff").unwrap();
        assert!((diff - 0.25).abs() < 1e-10);
    }

    #[test]
    fn test_baseline_key_not_found() {
        let baseline = json!({"a1": 0.5});
        let file = create_baseline_file(&baseline);
        let args = vec![
            "diff".into(),
            "-k".into(),
            "account".into(),
            "-f".into(),
            "score".into(),
            "--baseline".into(),
            file.path().to_str().unwrap().into(),
        ];
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "unknown", "score": 0.9}));
        let output = proc.process(event).unwrap();

        assert!(!has_field(&output[0], "score_diff"));
    }

    #[test]
    fn test_baseline_file_not_found() {
        let args =
            shlex::split("diff -k account -f score --baseline /nonexistent/file.json").unwrap();
        let result = DiffProcessor::new(&args);
        match result {
            Err(e) => {
                let err_msg = format!("{}", e);
                assert!(err_msg.contains("Failed to open baseline file"));
                assert!(err_msg.contains("Expected format"));
            }
            Ok(_) => panic!("should fail for missing baseline file"),
        }
    }

    #[test]
    fn test_baseline_invalid_json() {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "not valid json").unwrap();
        file.flush().unwrap();

        let args = vec![
            "diff".into(),
            "-k".into(),
            "account".into(),
            "-f".into(),
            "score".into(),
            "--baseline".into(),
            file.path().to_str().unwrap().into(),
        ];
        let result = DiffProcessor::new(&args);
        match result {
            Err(e) => {
                let err_msg = format!("{}", e);
                assert!(err_msg.contains("Failed to parse baseline file"));
                assert!(err_msg.contains("Expected format"));
            }
            Ok(_) => panic!("should fail for invalid JSON"),
        }
    }

    // ── 6.5 Value mode tests ────────────────────────────────────────────

    #[test]
    fn test_value_mode_positive() {
        let args = shlex::split("diff -k account -f score --value 10.0").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "a1", "score": 15.0}));
        let output = proc.process(event).unwrap();

        let diff = get_f64(&output[0], "score_diff").unwrap();
        assert!((diff - 5.0).abs() < 1e-10);
    }

    #[test]
    fn test_value_mode_negative() {
        // Use --value=-5.0 to avoid clap interpreting -5.0 as a flag
        let args = shlex::split("diff -k account -f score --value=-5.0").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "a1", "score": 3.0}));
        let output = proc.process(event).unwrap();

        // diff = 3.0 - (-5.0) = 8.0
        let diff = get_f64(&output[0], "score_diff").unwrap();
        assert!((diff - 8.0).abs() < 1e-10);
    }

    #[test]
    fn test_value_mode_zero_reference() {
        let args = shlex::split("diff -k account -f score --value 0.0").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "a1", "score": 7.0}));
        let output = proc.process(event).unwrap();

        let diff = get_f64(&output[0], "score_diff").unwrap();
        assert!((diff - 7.0).abs() < 1e-10);
        assert!(is_null_field(&output[0], "score_pct_change"));
    }

    // ── 6.6 Output format tests ─────────────────────────────────────────

    #[test]
    fn test_original_fields_preserved() {
        let args = shlex::split("diff -k account -f score --value 1.0").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "a1", "score": 2.0, "policy": "cyber"}));
        let output = proc.process(event).unwrap();

        let borrowed = output[0].borrow();
        assert_eq!(borrowed["account"], "a1");
        assert_eq!(borrowed["score"], 2.0);
        assert_eq!(borrowed["policy"], "cyber");
    }

    #[test]
    fn test_diff_field_naming() {
        let args = shlex::split("diff -k account -f score --value 1.0").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        let event = make_event(json!({"account": "a1", "score": 3.0}));
        let output = proc.process(event).unwrap();

        assert!(has_field(&output[0], "score_diff"));
        assert!(has_field(&output[0], "score_pct_change"));
    }

    // ── Stats test ──────────────────────────────────────────────────────

    #[test]
    fn test_stats() {
        let args = shlex::split("diff -k account -f score --value 1.0").unwrap();
        let mut proc = DiffProcessor::new(&args).unwrap();

        proc.process(make_event(json!({"account": "a1", "score": 2.0}))).unwrap();
        proc.process(make_event(json!({"account": "a2"}))).unwrap(); // no score, pass through
        proc.process(make_event(json!({"account": "a3", "score": 5.0}))).unwrap();

        let stats = proc.stats().unwrap();
        assert!(stats.contains("input:3"));
        assert!(stats.contains("diff:2"));
    }
}
