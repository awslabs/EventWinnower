use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use std::collections::HashMap;

/// Join Processor - enrich events by joining with reference data from file
#[derive(SerialProcessorInit)]
pub struct JoinProcessor<'a> {
    key_path: jmespath::Expression<'a>,
    reference_data: HashMap<String, serde_json::Value>,
    left_join: bool,
    suffix: Option<String>,
    input_count: u64,
    matched_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// Enrich events by joining with reference data from a JSON file
#[command(
    version,
    arg_required_else_help(true),
    long_about = r#"
Enrich events by joining with reference data loaded from an external JSON file.
Matches events to reference data using a configurable key field extracted via JMESPath.

Reference file formats:
  Array format: [{"account": "123", "region": "us-west-2"}, ...]
    - Each object must contain the join key field
    - Objects are indexed by the join key value

  Object format: {"123": {"region": "us-west-2"}, ...}
    - Keys are used directly as join keys
    - Values are the reference data to merge

EXAMPLE:
  json | join -k account -f accounts.json
  json | join -k account -f accounts.json --inner
  json | join -k account -f accounts.json -s "_ref"
"#
)]
struct JoinArgs {
    /// JMESPath expression to extract the join key from events
    #[arg(short = 'k', long, required = true)]
    key: String,

    /// Path to the reference data JSON file
    #[arg(short = 'f', long, required = true)]
    file: String,

    /// Perform a left join (keep all input events, default)
    #[arg(long, conflicts_with = "inner")]
    left: bool,

    /// Perform an inner join (only output matched events)
    #[arg(long, conflicts_with = "left")]
    inner: bool,

    /// Suffix to append to reference data field names
    #[arg(short = 's', long)]
    suffix: Option<String>,
}

/// Convert a JSON value to a string key for matching
fn value_to_string(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => val.to_string(),
    }
}

/// Load reference data from a JSON file and index it by the specified key field.
///
/// Supports two formats:
/// - Array of objects: indexed by the value of `key_field` in each object
/// - Object: keys used directly as lookup keys
pub fn load_reference_data(
    file_path: &str,
    key_field: &str,
) -> Result<HashMap<String, serde_json::Value>, anyhow::Error> {
    let content = std::fs::read_to_string(file_path)
        .map_err(|e| anyhow::anyhow!("Failed to read reference file '{}': {}", file_path, e))?;
    let json: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Failed to parse reference file '{}': {}", file_path, e))?;

    match json {
        serde_json::Value::Array(arr) => {
            let mut map = HashMap::new();
            for item in arr {
                if let Some(key) = item.get(key_field) {
                    let key_str = value_to_string(key);
                    map.insert(key_str, item);
                }
            }
            Ok(map)
        }
        serde_json::Value::Object(obj) => Ok(obj.into_iter().collect()),
        _ => Err(anyhow::anyhow!(
            "Reference file must contain a JSON array or object, got: {}",
            match &json {
                serde_json::Value::Null => "null",
                serde_json::Value::Bool(_) => "boolean",
                serde_json::Value::Number(_) => "number",
                serde_json::Value::String(_) => "string",
                _ => "unknown",
            }
        )),
    }
}

impl SerialProcessor for JoinProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("enrich events by joining with reference data".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = JoinArgs::try_parse_from(argv)?;
        let key_path = jmespath::compile(&args.key)
            .map_err(|e| anyhow::anyhow!("Invalid JMESPath expression '{}': {}", args.key, e))?;

        let reference_data = load_reference_data(&args.file, &args.key)?;

        // Determine join type: --inner means inner join, otherwise left join (default)
        let left_join = !args.inner;

        Ok(Self {
            key_path,
            reference_data,
            left_join,
            suffix: args.suffix,
            input_count: 0,
            matched_count: 0,
            output_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        // Extract join key from event using JMESPath
        let key_result = self.key_path.search(&input)?;

        // Convert key to string for lookup, treat null/missing as no match
        let key_str = if key_result.is_null() {
            None
        } else if let Some(s) = key_result.as_string() {
            Some(s.to_string())
        } else {
            Some(key_result.to_string())
        };

        // Look up reference data
        let matched = key_str.as_ref().and_then(|k| self.reference_data.get(k));

        match matched {
            Some(ref_data) => {
                self.matched_count += 1;
                // Merge reference data fields into event
                merge_fields(&input, ref_data, &self.suffix);
                self.output_count += 1;
                Ok(vec![input])
            }
            None => {
                if self.left_join {
                    // Left join: output event unchanged
                    self.output_count += 1;
                    Ok(vec![input])
                } else {
                    // Inner join: drop event
                    Ok(vec![])
                }
            }
        }
    }

    fn stats(&self) -> Option<String> {
        Some(format!(
            "join: input={}, matched={}, output={}",
            self.input_count, self.matched_count, self.output_count
        ))
    }
}

/// Merge reference data fields into an event, with optional suffix on field names
fn merge_fields(event: &Event, reference: &serde_json::Value, suffix: &Option<String>) {
    if let Some(ref_obj) = reference.as_object() {
        if let Some(event_obj) = event.borrow_mut().as_object_mut() {
            for (key, value) in ref_obj {
                let field_name = match suffix {
                    Some(s) => format!("{}{}", key, s),
                    None => key.clone(),
                };
                event_obj.insert(field_name, value.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::io::Write;
    use std::rc::Rc;
    use tempfile::NamedTempFile;

    fn make_event(val: serde_json::Value) -> Event {
        Rc::new(RefCell::new(val))
    }

    fn create_ref_file(data: &serde_json::Value) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        serde_json::to_writer(&mut file, data).unwrap();
        file.flush().unwrap();
        file
    }

    fn make_processor(ref_file: &NamedTempFile, extra_args: &[&str]) -> JoinProcessor<'static> {
        let mut args: Vec<String> = vec![
            "join".into(),
            "-k".into(),
            "account".into(),
            "-f".into(),
            ref_file.path().to_str().unwrap().into(),
        ];
        for a in extra_args {
            args.push(a.to_string());
        }
        JoinProcessor::new(&args).unwrap()
    }

    // --- reference data loading ---

    #[test]
    fn test_load_array_format() {
        let data = serde_json::json!([
            {"account": "123", "region": "us-west-2"},
            {"account": "456", "region": "us-east-1"}
        ]);
        let file = create_ref_file(&data);
        let map = load_reference_data(file.path().to_str().unwrap(), "account").unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map["123"]["region"], "us-west-2");
        assert_eq!(map["456"]["region"], "us-east-1");
    }

    #[test]
    fn test_load_object_format() {
        let data = serde_json::json!({
            "123": {"region": "us-west-2"},
            "456": {"region": "us-east-1"}
        });
        let file = create_ref_file(&data);
        let map = load_reference_data(file.path().to_str().unwrap(), "account").unwrap();
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("123"));
        assert!(map.contains_key("456"));
    }

    #[test]
    fn test_load_file_not_found() {
        let result = load_reference_data("/nonexistent/file.json", "account");
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("Failed to read"));
    }

    #[test]
    fn test_load_invalid_json() {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "not valid json").unwrap();
        file.flush().unwrap();
        let result = load_reference_data(file.path().to_str().unwrap(), "account");
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("Failed to parse"));
    }

    #[test]
    fn test_load_invalid_format_string() {
        let data = serde_json::json!("just a string");
        let file = create_ref_file(&data);
        let result = load_reference_data(file.path().to_str().unwrap(), "account");
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("must contain a JSON array or object"));
    }

    #[test]
    fn test_load_invalid_format_number() {
        let data = serde_json::json!(42);
        let file = create_ref_file(&data);
        let result = load_reference_data(file.path().to_str().unwrap(), "account");
        assert!(result.is_err());
    }

    // --- left join (default) ---

    #[test]
    fn test_left_join_match() {
        let data = serde_json::json!([
            {"account": "123", "region": "us-west-2", "tier": "premium"}
        ]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &[]);

        let event = make_event(serde_json::json!({"account": "123", "action": "login"}));
        let output = proc.process(event).unwrap();

        assert_eq!(output.len(), 1);
        let borrowed = output[0].borrow();
        assert_eq!(borrowed["account"], "123");
        assert_eq!(borrowed["action"], "login");
        assert_eq!(borrowed["region"], "us-west-2");
        assert_eq!(borrowed["tier"], "premium");
    }

    #[test]
    fn test_left_join_no_match_passes_through() {
        let data = serde_json::json!([
            {"account": "123", "region": "us-west-2"}
        ]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &[]);

        let event = make_event(serde_json::json!({"account": "999", "action": "login"}));
        let output = proc.process(event).unwrap();

        assert_eq!(output.len(), 1);
        let borrowed = output[0].borrow();
        assert_eq!(borrowed["account"], "999");
        assert_eq!(borrowed["action"], "login");
        assert!(borrowed.get("region").is_none());
    }

    #[test]
    fn test_default_is_left_join() {
        let data = serde_json::json!([{"account": "123", "region": "us-west-2"}]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &[]);

        // no match should still output (left join behavior)
        let event = make_event(serde_json::json!({"account": "no_match"}));
        let output = proc.process(event).unwrap();
        assert_eq!(output.len(), 1);
    }

    // --- inner join ---

    #[test]
    fn test_inner_join_match() {
        let data = serde_json::json!([
            {"account": "123", "region": "us-west-2"}
        ]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &["--inner"]);

        let event = make_event(serde_json::json!({"account": "123", "action": "login"}));
        let output = proc.process(event).unwrap();

        assert_eq!(output.len(), 1);
        let borrowed = output[0].borrow();
        assert_eq!(borrowed["region"], "us-west-2");
    }

    #[test]
    fn test_inner_join_no_match_drops_event() {
        let data = serde_json::json!([
            {"account": "123", "region": "us-west-2"}
        ]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &["--inner"]);

        let event = make_event(serde_json::json!({"account": "999", "action": "login"}));
        let output = proc.process(event).unwrap();

        assert_eq!(output.len(), 0);
    }

    // --- key extraction ---

    #[test]
    fn test_missing_key_treated_as_no_match() {
        let data = serde_json::json!([{"account": "123", "region": "us-west-2"}]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &["--inner"]);

        // event has no "account" field
        let event = make_event(serde_json::json!({"action": "login"}));
        let output = proc.process(event).unwrap();
        assert_eq!(output.len(), 0); // inner join drops it
    }

    #[test]
    fn test_null_key_treated_as_no_match() {
        let data = serde_json::json!([{"account": "123", "region": "us-west-2"}]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &["--inner"]);

        let event = make_event(serde_json::json!({"account": null}));
        let output = proc.process(event).unwrap();
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn test_numeric_key_matching() {
        let data = serde_json::json!({"42": {"region": "us-west-2"}});
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &[]);

        let event = make_event(serde_json::json!({"account": 42}));
        let output = proc.process(event).unwrap();

        assert_eq!(output.len(), 1);
        let borrowed = output[0].borrow();
        assert_eq!(borrowed["region"], "us-west-2");
    }

    // --- field merging ---

    #[test]
    fn test_merge_overwrites_conflicting_fields() {
        let data = serde_json::json!([
            {"account": "123", "status": "active"}
        ]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &[]);

        // event already has "status" field
        let event = make_event(serde_json::json!({"account": "123", "status": "unknown"}));
        let output = proc.process(event).unwrap();

        let borrowed = output[0].borrow();
        // reference data value should win
        assert_eq!(borrowed["status"], "active");
    }

    #[test]
    fn test_merge_with_suffix() {
        let data = serde_json::json!([
            {"account": "123", "status": "active", "region": "us-west-2"}
        ]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &["-s", "_ref"]);

        let event = make_event(serde_json::json!({"account": "123", "status": "unknown"}));
        let output = proc.process(event).unwrap();

        let borrowed = output[0].borrow();
        // original field preserved
        assert_eq!(borrowed["status"], "unknown");
        // reference fields have suffix
        assert_eq!(borrowed["status_ref"], "active");
        assert_eq!(borrowed["region_ref"], "us-west-2");
        assert_eq!(borrowed["account_ref"], "123");
    }

    #[test]
    fn test_merge_preserves_value_types() {
        let data = serde_json::json!([
            {"account": "123", "count": 42, "active": true, "tags": ["a", "b"], "meta": {"x": 1}, "empty": null}
        ]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &[]);

        let event = make_event(serde_json::json!({"account": "123"}));
        let output = proc.process(event).unwrap();

        let borrowed = output[0].borrow();
        assert!(borrowed["count"].is_number());
        assert_eq!(borrowed["count"], 42);
        assert!(borrowed["active"].is_boolean());
        assert_eq!(borrowed["active"], true);
        assert!(borrowed["tags"].is_array());
        assert!(borrowed["meta"].is_object());
        assert!(borrowed["empty"].is_null());
    }

    // --- statistics ---

    #[test]
    fn test_stats_accuracy() {
        let data = serde_json::json!([{"account": "123", "region": "us-west-2"}]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &[]);

        proc.process(make_event(serde_json::json!({"account": "123"}))).unwrap();
        proc.process(make_event(serde_json::json!({"account": "999"}))).unwrap();
        proc.process(make_event(serde_json::json!({"account": "123"}))).unwrap();

        let stats = proc.stats().unwrap();
        assert!(stats.contains("input=3"));
        assert!(stats.contains("matched=2"));
        assert!(stats.contains("output=3"));
    }

    #[test]
    fn test_stats_inner_join() {
        let data = serde_json::json!([{"account": "123", "region": "us-west-2"}]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &["--inner"]);

        proc.process(make_event(serde_json::json!({"account": "123"}))).unwrap();
        proc.process(make_event(serde_json::json!({"account": "999"}))).unwrap();

        let stats = proc.stats().unwrap();
        assert!(stats.contains("input=2"));
        assert!(stats.contains("matched=1"));
        assert!(stats.contains("output=1"));
    }

    // --- empty reference data ---

    #[test]
    fn test_empty_reference_data() {
        let data = serde_json::json!([]);
        let file = create_ref_file(&data);
        let mut proc = make_processor(&file, &[]);

        let event = make_event(serde_json::json!({"account": "123"}));
        let output = proc.process(event).unwrap();
        // left join: passes through unchanged
        assert_eq!(output.len(), 1);
    }
}
