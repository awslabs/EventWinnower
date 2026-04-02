use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use regex::Regex;

#[derive(SerialProcessorInit)]
pub struct StringSplitterProcessor<'a> {
    path: jmespath::Expression<'a>,
    delimiter: String,
    label: String,
    use_regex: bool,
    regex_pattern: Option<Regex>,
}

#[derive(Parser)]
/// Split a string from a jmespath into an array and insert into event
#[command(version, long_about = None, arg_required_else_help(true))]
struct StringSplitterArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long, required(true))]
    delimiter: String,

    #[arg(short, long, default_value = "split")]
    label: String,

    #[arg(short, long)]
    regex: bool,
}

impl SerialProcessor for StringSplitterProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("split string from JMESPath into array".to_string())
    }
    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = StringSplitterArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        let regex_pattern = if args.regex { Some(Regex::new(&args.delimiter)?) } else { None };

        Ok(Self {
            path,
            delimiter: args.delimiter,
            label: args.label,
            use_regex: args.regex,
            regex_pattern,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        let result = self.path.search(&input)?;

        if result.is_null() {
            return Ok(vec![input]);
        }

        let working = if result.is_string() {
            result.as_string().expect("is string")
        } else {
            &result.to_string()
        };

        let split_result: Vec<String> = if self.use_regex {
            if let Some(regex) = &self.regex_pattern {
                regex.split(working).map(|s| s.to_string()).collect()
            } else {
                // Fallback to string split if regex compilation failed
                working.split(&self.delimiter).map(|s| s.to_string()).collect()
            }
        } else {
            working.split(&self.delimiter).map(|s| s.to_string()).collect()
        };

        if split_result.is_empty() {
            return Ok(vec![input]);
        }

        if let Some(event) = input.borrow_mut().as_object_mut() {
            event.insert(self.label.clone(), serde_json::to_value(split_result)?);
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
    fn test_basic_string_splitting_with_comma_delimiter() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            ",".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "apple,banana,cherry"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let split_array = output["split"].as_array().unwrap();
        assert_eq!(split_array.len(), 3);
        assert_eq!(split_array[0], "apple");
        assert_eq!(split_array[1], "banana");
        assert_eq!(split_array[2], "cherry");
    }

    #[test]
    fn test_string_splitting_with_pipe_delimiter() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "message".to_string(),
            "--delimiter".to_string(),
            "|".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "message": "field1|field2|field3"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let split_array = output["split"].as_array().unwrap();
        assert_eq!(split_array.len(), 3);
        assert_eq!(split_array[0], "field1");
        assert_eq!(split_array[1], "field2");
        assert_eq!(split_array[2], "field3");
    }

    #[test]
    fn test_string_splitting_with_semicolon_delimiter() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "text".to_string(),
            "--delimiter".to_string(),
            ";".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "text": "item1;item2;item3;item4"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let split_array = output["split"].as_array().unwrap();
        assert_eq!(split_array.len(), 4);
        assert_eq!(split_array[0], "item1");
        assert_eq!(split_array[1], "item2");
        assert_eq!(split_array[2], "item3");
        assert_eq!(split_array[3], "item4");
    }

    #[test]
    fn test_custom_label_assignment() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            ",".to_string(),
            "--label".to_string(),
            "custom_array".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "a,b,c"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let split_array = output["custom_array"].as_array().unwrap();
        assert_eq!(split_array.len(), 3);
        assert_eq!(split_array[0], "a");
        assert_eq!(split_array[1], "b");
        assert_eq!(split_array[2], "c");

        // Verify default label is not present
        assert!(!output.as_object().unwrap().contains_key("split"));
    }

    #[test]
    fn test_default_label_behavior() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            ",".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "x,y,z"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        // Verify default label "split" is used
        assert!(output.as_object().unwrap().contains_key("split"));
        let split_array = output["split"].as_array().unwrap();
        assert_eq!(split_array.len(), 3);
        assert_eq!(split_array[0], "x");
        assert_eq!(split_array[1], "y");
        assert_eq!(split_array[2], "z");
    }

    #[test]
    fn test_handling_empty_string() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            ",".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": ""
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let split_array = output["split"].as_array().unwrap();
        // Empty string split should result in array with one empty string element
        assert_eq!(split_array.len(), 1);
        assert_eq!(split_array[0], "");
    }

    #[test]
    fn test_handling_single_character_input() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            ",".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "a"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let split_array = output["split"].as_array().unwrap();
        // Single character without delimiter should result in array with one element
        assert_eq!(split_array.len(), 1);
        assert_eq!(split_array[0], "a");
    }

    #[test]
    fn test_split_arrays_inserted_as_json_arrays() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            ",".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "one,two,three"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        // Verify the split result is a proper JSON array
        assert!(output["split"].is_array());
        let split_array = output["split"].as_array().unwrap();
        assert_eq!(split_array.len(), 3);

        // Verify each element is a JSON string
        for (i, expected) in ["one", "two", "three"].iter().enumerate() {
            assert!(split_array[i].is_string());
            assert_eq!(split_array[i].as_str().unwrap(), *expected);
        }
    }

    #[test]
    fn test_overwrite_existing_label_values() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            ",".to_string(),
            "--label".to_string(),
            "existing_field".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "a,b,c",
            "existing_field": "old_value"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        // Verify the existing field was overwritten with the split array
        assert!(output["existing_field"].is_array());
        let split_array = output["existing_field"].as_array().unwrap();
        assert_eq!(split_array.len(), 3);
        assert_eq!(split_array[0], "a");
        assert_eq!(split_array[1], "b");
        assert_eq!(split_array[2], "c");
    }

    #[test]
    fn test_string_with_consecutive_delimiters() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            ",".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "a,,b,c"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let split_array = output["split"].as_array().unwrap();
        // Should include empty string elements for consecutive delimiters
        assert_eq!(split_array.len(), 4);
        assert_eq!(split_array[0], "a");
        assert_eq!(split_array[1], ""); // Empty string from consecutive delimiters
        assert_eq!(split_array[2], "b");
        assert_eq!(split_array[3], "c");
    }

    #[test]
    fn test_string_with_delimiter_at_start_and_end() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            ",".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": ",a,b,c,"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let split_array = output["split"].as_array().unwrap();
        // Should include empty strings at start and end
        assert_eq!(split_array.len(), 5);
        assert_eq!(split_array[0], ""); // Empty string at start
        assert_eq!(split_array[1], "a");
        assert_eq!(split_array[2], "b");
        assert_eq!(split_array[3], "c");
        assert_eq!(split_array[4], ""); // Empty string at end
    }

    #[test]
    fn test_regex_splitting_with_digit_patterns() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            r"\d+".to_string(),
            "--regex".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "word123another456text789end"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let split_array = output["split"].as_array().unwrap();
        // Should split on digit sequences: "word", "another", "text", "end"
        assert_eq!(split_array.len(), 4);
        assert_eq!(split_array[0], "word");
        assert_eq!(split_array[1], "another");
        assert_eq!(split_array[2], "text");
        assert_eq!(split_array[3], "end");
    }

    #[test]
    fn test_regex_splitting_with_word_boundary_patterns() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            r"\b".to_string(),
            "--regex".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "hello world test"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let split_array = output["split"].as_array().unwrap();
        // Word boundaries split at word boundaries, creating more segments than expected
        // Actual result: ["", "hello", " ", "world", " ", "test", ""]
        assert_eq!(split_array.len(), 7);
        assert_eq!(split_array[0], "");
        assert_eq!(split_array[1], "hello");
        assert_eq!(split_array[2], " ");
        assert_eq!(split_array[3], "world");
        assert_eq!(split_array[4], " ");
        assert_eq!(split_array[5], "test");
        assert_eq!(split_array[6], "");
    }

    #[test]
    fn test_regex_splitting_with_custom_character_classes() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            r"[,;:]".to_string(),
            "--regex".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "item1,item2;item3:item4"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let split_array = output["split"].as_array().unwrap();
        // Should split on any of the characters in the character class
        assert_eq!(split_array.len(), 4);
        assert_eq!(split_array[0], "item1");
        assert_eq!(split_array[1], "item2");
        assert_eq!(split_array[2], "item3");
        assert_eq!(split_array[3], "item4");
    }

    #[test]
    fn test_regex_splitting_with_complex_patterns() {
        let mut processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            r"[0-9]+[a-z]*".to_string(),
            "--regex".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "start123abc middle456def end789xyz final"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        let split_array = output["split"].as_array().unwrap();
        // Should split on patterns like "123abc", "456def", "789xyz"
        assert_eq!(split_array.len(), 4);
        assert_eq!(split_array[0], "start");
        assert_eq!(split_array[1], " middle");
        assert_eq!(split_array[2], " end");
        assert_eq!(split_array[3], " final");
    }

    #[test]
    fn test_regex_mode_vs_fixed_string_mode_different_results() {
        // Test with fixed string mode
        let mut fixed_processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            r"\d+".to_string(),
        ])
        .unwrap();

        // Test with regex mode
        let mut regex_processor = StringSplitterProcessor::new(&[
            "stringsplitter".to_string(),
            "data".to_string(),
            "--delimiter".to_string(),
            r"\d+".to_string(),
            "--regex".to_string(),
        ])
        .unwrap();

        let input_fixed = create_event(json!({
            "data": "word123another456text"
        }));

        let input_regex = create_event(json!({
            "data": "word123another456text"
        }));

        let fixed_result = fixed_processor.process(input_fixed).unwrap();
        let regex_result = regex_processor.process(input_regex).unwrap();

        let fixed_output = fixed_result[0].borrow();
        let regex_output = regex_result[0].borrow();

        let fixed_array = fixed_output["split"].as_array().unwrap();
        let regex_array = regex_output["split"].as_array().unwrap();

        // Fixed string mode: looks for literal "\d+" string (not found, so single element)
        assert_eq!(fixed_array.len(), 1);
        assert_eq!(fixed_array[0], "word123another456text");

        // Regex mode: splits on digit patterns
        assert_eq!(regex_array.len(), 3);
        assert_eq!(regex_array[0], "word");
        assert_eq!(regex_array[1], "another");
        assert_eq!(regex_array[2], "text");

        // Verify they produce different results
        assert_ne!(fixed_array.len(), regex_array.len());
    }
}
