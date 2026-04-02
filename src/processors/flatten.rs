use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use std::cell::RefCell;
use std::rc::Rc;

/// FlattenProcessor expands array elements into separate records while merging
/// user-specified parent fields. Each array element becomes a separate output event.
#[derive(SerialProcessorInit)]
pub struct FlattenProcessor<'a> {
    array_path: jmespath::Expression<'a>,
    keep_path: Option<jmespath::Expression<'a>>,
    pass_through: bool,
    preserve_parent: bool,
    element_label: String,
    input_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// Flatten array elements into separate records with parent fields
#[command(version, long_about = None, arg_required_else_help(true))]
struct FlattenArgs {
    /// JMESPath to the array to flatten
    #[arg(required(true))]
    path: Vec<String>,

    /// JMESPath expression to select parent fields to include (e.g., "{account: account, region: region}")
    #[arg(short, long)]
    keep: Option<String>,

    /// Pass events through unchanged when extraction fails
    #[arg(short, long)]
    pass_through: bool,

    /// Preserve parent field values when conflicts occur with array element fields
    #[arg(long)]
    preserve_parent: bool,

    /// Label for primitive array elements (only used when array contains strings, numbers, or booleans instead of objects)
    #[arg(short, long, default_value = "value")]
    element_label: String,
}

impl SerialProcessor for FlattenProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("flatten array elements into separate records with parent fields".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = FlattenArgs::try_parse_from(argv)?;
        let array_path = jmespath::compile(&args.path.join(" "))?;

        let keep_path = match args.keep {
            Some(keep) => Some(jmespath::compile(&keep)?),
            None => None,
        };

        Ok(Self {
            array_path,
            keep_path,
            pass_through: args.pass_through,
            preserve_parent: args.preserve_parent,
            element_label: args.element_label,
            input_count: 0,
            output_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        // Extract the target array using JMESPath
        let result = self.array_path.search(&input)?;

        // Handle null or non-array results
        if result.is_null() {
            if self.pass_through {
                self.output_count += 1;
                return Ok(vec![input]);
            }
            return Ok(vec![]);
        }

        // Check if result is an array
        let result_array = match result.as_array() {
            Some(arr) => arr,
            None => {
                if self.pass_through {
                    self.output_count += 1;
                    return Ok(vec![input]);
                }
                return Ok(vec![]);
            }
        };

        // Handle empty array
        if result_array.is_empty() {
            return Ok(vec![]);
        }

        // Extract parent fields if keep_path is configured
        let parent_fields: Option<serde_json::Value> = if let Some(keep) = &self.keep_path {
            let keep_result = keep.search(&input)?;
            if !keep_result.is_null() {
                Some(serde_json::to_value(keep_result)?)
            } else {
                None
            }
        } else {
            None
        };

        let mut output_events = Vec::new();

        for element in result_array.iter() {
            // Skip null elements
            if element.is_null() {
                continue;
            }

            let element_value = serde_json::to_value(element)?;
            let output_value = self.merge_fields(&parent_fields, &element_value);

            output_events.push(Rc::new(RefCell::new(output_value)));
            self.output_count += 1;
        }

        Ok(output_events)
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\noutput:{}", self.input_count, self.output_count))
    }
}

impl FlattenProcessor<'_> {
    /// Merge parent fields with array element fields
    fn merge_fields(
        &self,
        parent_fields: &Option<serde_json::Value>,
        element: &serde_json::Value,
    ) -> serde_json::Value {
        // Start with parent fields if present
        let mut result = if let Some(serde_json::Value::Object(parent_map)) = parent_fields {
            serde_json::Value::Object(parent_map.clone())
        } else {
            serde_json::Value::Object(serde_json::Map::new())
        };

        // Merge element fields
        if let serde_json::Value::Object(ref mut result_map) = result {
            match element {
                serde_json::Value::Object(element_map) => {
                    // Element is an object - merge its fields
                    for (key, value) in element_map.iter() {
                        if self.preserve_parent && result_map.contains_key(key) {
                            // Skip - preserve parent value
                            continue;
                        }
                        result_map.insert(key.clone(), value.clone());
                    }
                }
                _ => {
                    // Element is a primitive - add with configured label
                    if self.preserve_parent && result_map.contains_key(&self.element_label) {
                        // Skip - preserve parent value
                    } else {
                        result_map.insert(self.element_label.clone(), element.clone());
                    }
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn flatten_basic() -> Result<(), anyhow::Error> {
        let mut processor = FlattenProcessor::new(&["flatten".to_string(), "items".to_string()])?;

        let input = Rc::new(RefCell::new(json!({
            "account": "123",
            "items": [{"x": 1}, {"x": 2}]
        })));

        let output = processor.process(input)?;
        assert_eq!(output.len(), 2);
        assert_eq!(*output[0].borrow(), json!({"x": 1}));
        assert_eq!(*output[1].borrow(), json!({"x": 2}));
        Ok(())
    }

    #[test]
    fn flatten_with_keep() -> Result<(), anyhow::Error> {
        let mut processor = FlattenProcessor::new(&[
            "flatten".to_string(),
            "items".to_string(),
            "-k".to_string(),
            "{account: account}".to_string(),
        ])?;

        let input = Rc::new(RefCell::new(json!({
            "account": "123",
            "items": [{"x": 1}, {"x": 2}]
        })));

        let output = processor.process(input)?;
        assert_eq!(output.len(), 2);
        assert_eq!(*output[0].borrow(), json!({"account": "123", "x": 1}));
        assert_eq!(*output[1].borrow(), json!({"account": "123", "x": 2}));
        Ok(())
    }

    #[test]
    fn flatten_primitives() -> Result<(), anyhow::Error> {
        let mut processor = FlattenProcessor::new(&["flatten".to_string(), "tags".to_string()])?;

        let input = Rc::new(RefCell::new(json!({
            "tags": ["a", "b", "c"]
        })));

        let output = processor.process(input)?;
        assert_eq!(output.len(), 3);
        assert_eq!(*output[0].borrow(), json!({"value": "a"}));
        assert_eq!(*output[1].borrow(), json!({"value": "b"}));
        assert_eq!(*output[2].borrow(), json!({"value": "c"}));
        Ok(())
    }

    #[test]
    fn flatten_primitives_custom_label() -> Result<(), anyhow::Error> {
        let mut processor = FlattenProcessor::new(&[
            "flatten".to_string(),
            "tags".to_string(),
            "-e".to_string(),
            "tag_value".to_string(),
        ])?;

        let input = Rc::new(RefCell::new(json!({
            "tags": ["a", "b"]
        })));

        let output = processor.process(input)?;
        assert_eq!(output.len(), 2);
        assert_eq!(*output[0].borrow(), json!({"tag_value": "a"}));
        assert_eq!(*output[1].borrow(), json!({"tag_value": "b"}));
        Ok(())
    }

    #[test]
    fn flatten_empty_array() -> Result<(), anyhow::Error> {
        let mut processor = FlattenProcessor::new(&["flatten".to_string(), "items".to_string()])?;

        let input = Rc::new(RefCell::new(json!({
            "items": []
        })));

        let output = processor.process(input)?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    #[test]
    fn flatten_null_elements_skipped() -> Result<(), anyhow::Error> {
        let mut processor = FlattenProcessor::new(&["flatten".to_string(), "items".to_string()])?;

        let input = Rc::new(RefCell::new(json!({
            "items": [{"x": 1}, null, {"x": 3}]
        })));

        let output = processor.process(input)?;
        assert_eq!(output.len(), 2);
        assert_eq!(*output[0].borrow(), json!({"x": 1}));
        assert_eq!(*output[1].borrow(), json!({"x": 3}));
        Ok(())
    }

    #[test]
    fn flatten_pass_through_on_null() -> Result<(), anyhow::Error> {
        let mut processor = FlattenProcessor::new(&[
            "flatten".to_string(),
            "missing".to_string(),
            "-p".to_string(),
        ])?;

        let input = Rc::new(RefCell::new(json!({
            "other": "data"
        })));

        let output = processor.process(input)?;
        assert_eq!(output.len(), 1);
        assert_eq!(*output[0].borrow(), json!({"other": "data"}));
        Ok(())
    }

    #[test]
    fn flatten_drop_on_null_without_pass_through() -> Result<(), anyhow::Error> {
        let mut processor = FlattenProcessor::new(&["flatten".to_string(), "missing".to_string()])?;

        let input = Rc::new(RefCell::new(json!({
            "other": "data"
        })));

        let output = processor.process(input)?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    #[test]
    fn flatten_conflict_element_wins_by_default() -> Result<(), anyhow::Error> {
        let mut processor = FlattenProcessor::new(&[
            "flatten".to_string(),
            "items".to_string(),
            "-k".to_string(),
            "{id: id}".to_string(),
        ])?;

        let input = Rc::new(RefCell::new(json!({
            "id": "parent_id",
            "items": [{"id": "element_id", "x": 1}]
        })));

        let output = processor.process(input)?;
        assert_eq!(output.len(), 1);
        assert_eq!(*output[0].borrow(), json!({"id": "element_id", "x": 1}));
        Ok(())
    }

    #[test]
    fn flatten_conflict_preserve_parent() -> Result<(), anyhow::Error> {
        let mut processor = FlattenProcessor::new(&[
            "flatten".to_string(),
            "items".to_string(),
            "-k".to_string(),
            "{id: id}".to_string(),
            "--preserve-parent".to_string(),
        ])?;

        let input = Rc::new(RefCell::new(json!({
            "id": "parent_id",
            "items": [{"id": "element_id", "x": 1}]
        })));

        let output = processor.process(input)?;
        assert_eq!(output.len(), 1);
        assert_eq!(*output[0].borrow(), json!({"id": "parent_id", "x": 1}));
        Ok(())
    }

    #[test]
    fn flatten_stats() -> Result<(), anyhow::Error> {
        let mut processor = FlattenProcessor::new(&["flatten".to_string(), "items".to_string()])?;

        let input = Rc::new(RefCell::new(json!({
            "items": [{"x": 1}, {"x": 2}, {"x": 3}]
        })));

        processor.process(input)?;
        let stats = processor.stats().unwrap();
        assert!(stats.contains("input:1"));
        assert!(stats.contains("output:3"));
        Ok(())
    }

    #[test]
    fn flatten_nested_path() -> Result<(), anyhow::Error> {
        let mut processor =
            FlattenProcessor::new(&["flatten".to_string(), "data.items".to_string()])?;

        let input = Rc::new(RefCell::new(json!({
            "data": {
                "items": [{"x": 1}, {"x": 2}]
            }
        })));

        let output = processor.process(input)?;
        assert_eq!(output.len(), 2);
        assert_eq!(*output[0].borrow(), json!({"x": 1}));
        assert_eq!(*output[1].borrow(), json!({"x": 2}));
        Ok(())
    }

    #[test]
    fn flatten_pass_through_non_array() -> Result<(), anyhow::Error> {
        let mut processor =
            FlattenProcessor::new(&["flatten".to_string(), "items".to_string(), "-p".to_string()])?;

        let input = Rc::new(RefCell::new(json!({
            "items": "not_an_array"
        })));

        let output = processor.process(input)?;
        assert_eq!(output.len(), 1);
        assert_eq!(*output[0].borrow(), json!({"items": "not_an_array"}));
        Ok(())
    }
}
