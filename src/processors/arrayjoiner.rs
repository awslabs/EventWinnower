use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;

#[derive(SerialProcessorInit, Debug)]
pub struct ArrayJoinerProcessor<'a> {
    path: jmespath::Expression<'a>,
    separator: String,
    label: String,
    pass_through: bool,
}

#[derive(Parser)]
/// Join string arrays from JMESPath queries into single strings with configurable separators
#[command(version, long_about = None, arg_required_else_help(true))]
struct ArrayJoinerArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long, default_value = " ")]
    separator: String,

    #[arg(short, long, default_value = "joined")]
    label: String,

    #[arg(short, long)]
    pass_through: bool,
}

impl ArrayJoinerProcessor<'_> {
    /// Helper function to handle pass-through vs drop behavior
    fn handle_failure(
        &self,
        input: Event,
        error_msg: Option<&str>,
    ) -> Result<Vec<Event>, anyhow::Error> {
        if self.pass_through {
            // Pass event through unchanged
            Ok(vec![input])
        } else {
            // Drop event
            if let Some(msg) = error_msg {
                eprintln!("{msg}");
            }
            Ok(vec![])
        }
    }
}

impl SerialProcessor for ArrayJoinerProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("join string arrays into single strings with separators".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = ArrayJoinerArgs::try_parse_from(argv)?;

        // Compile JMESPath expression from path arguments
        let jmespath_expr = args.path.join(" ");
        let path = jmespath::compile(&jmespath_expr).map_err(|e| {
            anyhow::anyhow!("Failed to compile JMESPath expression '{}': {}", jmespath_expr, e)
        })?;

        Ok(Self {
            path,
            separator: args.separator,
            label: args.label,
            pass_through: args.pass_through,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        // Execute JMESPath query on input event
        let result = match self.path.search(&input) {
            Ok(result) => result,
            Err(e) => {
                return self.handle_failure(
                    input,
                    Some(&format!("JMESPath evaluation failed: {e}. Dropping event.")),
                );
            }
        };

        // Handle null results from JMESPath queries
        if result.is_null() {
            return self.handle_failure(input, None);
        }

        // Validate that extracted data is an array
        if !result.is_array() {
            return self.handle_failure(input, None);
        }

        // Extract array and validate string elements
        let array = result.as_array().expect("is array");

        // Treat empty arrays as failure condition
        if array.is_empty() {
            return self.handle_failure(input, None);
        }

        let mut string_elements = Vec::new();

        for element in array {
            if element.is_string() {
                string_elements.push(element.as_string().expect("is string"));
            } else {
                return self.handle_failure(input, None);
            }
        }

        // Join array elements using configured separator
        let string_refs: Vec<&str> = string_elements.iter().map(|s| s.as_str()).collect();
        let joined_string = string_refs.join(&self.separator);

        // Add combined string to event at configured output label
        if let Some(event) = input.borrow_mut().as_object_mut() {
            event.insert(self.label.clone(), serde_json::Value::String(joined_string));
        }

        Ok(vec![input])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn create_event(value: serde_json::Value) -> Event {
        Rc::new(RefCell::new(value))
    }

    #[test]
    fn test_basic_array_joining() {
        let mut processor =
            ArrayJoinerProcessor::new(&["arrayjoiner".to_string(), "tags".to_string()]).unwrap();

        let input = create_event(json!({"tags": ["tag1", "tag2", "tag3"]}));
        let result = processor.process(input).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].borrow()["joined"], "tag1 tag2 tag3");
    }

    #[test]
    fn test_custom_separator_and_label() {
        let mut processor = ArrayJoinerProcessor::new(&[
            "arrayjoiner".to_string(),
            "items".to_string(),
            "--separator".to_string(),
            ",".to_string(),
            "--label".to_string(),
            "result".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({"items": ["a", "b", "c"]}));
        let result = processor.process(input).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].borrow()["result"], "a,b,c");
    }

    #[test]
    fn test_jmespath_extraction() {
        let mut processor =
            ArrayJoinerProcessor::new(&["arrayjoiner".to_string(), "users[*].name".to_string()])
                .unwrap();

        let input = create_event(json!({
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25}
            ]
        }));
        let result = processor.process(input).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].borrow()["joined"], "Alice Bob");
    }

    #[test]
    fn test_validation_drops_invalid_data() {
        let mut processor =
            ArrayJoinerProcessor::new(&["arrayjoiner".to_string(), "data".to_string()]).unwrap();

        // Non-array data
        let input = create_event(json!({"data": "not_an_array"}));
        assert_eq!(processor.process(input).unwrap().len(), 0);

        // Mixed types in array
        let input = create_event(json!({"data": ["string", 123]}));
        assert_eq!(processor.process(input).unwrap().len(), 0);
    }

    #[test]
    fn test_pass_through_mode() {
        let mut processor = ArrayJoinerProcessor::new(&[
            "arrayjoiner".to_string(),
            "data".to_string(),
            "--pass-through".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({"data": "not_an_array"}));
        let result = processor.process(input).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].borrow()["data"], "not_an_array");
        assert!(result[0].borrow().get("joined").is_none());

        // Empty array should pass through in pass-through mode
        let input = create_event(json!({"data": []}));
        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].borrow()["data"], json!([]));
        assert!(result[0].borrow().get("joined").is_none());
    }

    #[test]
    fn test_edge_cases() {
        let mut processor =
            ArrayJoinerProcessor::new(&["arrayjoiner".to_string(), "data".to_string()]).unwrap();

        // Empty array should be dropped
        let input = create_event(json!({"data": []}));
        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 0);

        // Single element
        let input = create_event(json!({"data": ["only"]}));
        let result = processor.process(input).unwrap();
        assert_eq!(result[0].borrow()["joined"], "only");
    }
}
