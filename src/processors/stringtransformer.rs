use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;

#[derive(Debug, Clone)]
enum TransformationType {
    Uppercase,
    Lowercase,
    CharacterReplace { from_chars: String, to_chars: String },
}

#[derive(Debug)]
struct TransformationConfig<'a> {
    jmespath: jmespath::Expression<'a>,
    transformation: TransformationType,
    output_label: String,
}

#[derive(Debug, SerialProcessorInit)]
pub struct StringTransformerProcessor<'a> {
    transformations: Vec<TransformationConfig<'a>>,
    input_count: u64,
    transformation_success_count: u64,
}

#[derive(Parser)]
/// Transform strings in events using JMESPath expressions with case conversion and character replacement
#[command(version, long_about = None, arg_required_else_help(true))]
struct StringTransformerArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long)]
    uppercase: bool,

    #[arg(short, long)]
    lowercase: bool,

    #[arg(short = 'f', long = "from")]
    from_chars: Option<String>,

    #[arg(short = 't', long = "to")]
    to_chars: Option<String>,

    #[arg(long, default_value = "transformed")]
    label: String,
}

impl SerialProcessor for StringTransformerProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("transform strings with case conversion and char replacement".to_string())
    }
    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = StringTransformerArgs::try_parse_from(argv)?;

        // Validate that --from and --to are provided together
        match (&args.from_chars, &args.to_chars) {
            (Some(_), None) => {
                return Err(anyhow::anyhow!("--from requires --to to be specified"));
            }
            (None, Some(_)) => {
                return Err(anyhow::anyhow!("--to requires --from to be specified"));
            }
            _ => {}
        }

        // Compile JMESPath expression
        let jmespath_expr = args.path.join(" ");
        let jmespath = jmespath::compile(&jmespath_expr).map_err(|e| {
            anyhow::anyhow!("Failed to compile JMESPath expression '{}': {}", jmespath_expr, e)
        })?;

        // Build transformations in the correct order: case conversion first, then character replacement
        let mut transformations = Vec::new();

        // check if name of function has special meaning
        let local_uppercase = argv[0].to_lowercase() == "upper";
        let local_lowercase = argv[0].to_lowercase() == "lower";

        // Check if we have any transformations specified
        let has_case_conversion =
            args.uppercase || args.lowercase || local_lowercase || local_uppercase;
        let has_character_replacement = args.from_chars.is_some() && args.to_chars.is_some();

        if !has_case_conversion && !has_character_replacement {
            return Err(anyhow::anyhow!(
                "Must specify either --uppercase, --lowercase, or both --from and --to"
            ));
        }

        // Add case conversion transformation first (if specified)
        if local_uppercase || args.uppercase {
            let transformation_config = TransformationConfig {
                jmespath: jmespath.clone(),
                transformation: TransformationType::Uppercase,
                output_label: args.label.clone(),
            };
            transformations.push(transformation_config);
        } else if local_lowercase || args.lowercase {
            let transformation_config = TransformationConfig {
                jmespath: jmespath.clone(),
                transformation: TransformationType::Lowercase,
                output_label: args.label.clone(),
            };
            transformations.push(transformation_config);
        }

        // Add character replacement transformation second (if specified)
        if let (Some(from_chars), Some(to_chars)) = (args.from_chars, args.to_chars) {
            // Process escape sequences in character sets
            let processed_from = Self::process_escape_sequences(&from_chars).map_err(|e| {
                anyhow::anyhow!(
                    "Invalid escape sequence in --from argument '{}': {}",
                    from_chars,
                    e
                )
            })?;
            let processed_to = Self::process_escape_sequences(&to_chars).map_err(|e| {
                anyhow::anyhow!("Invalid escape sequence in --to argument '{}': {}", to_chars, e)
            })?;

            let transformation_config = TransformationConfig {
                jmespath: jmespath.clone(),
                transformation: TransformationType::CharacterReplace {
                    from_chars: processed_from,
                    to_chars: processed_to,
                },
                output_label: args.label.clone(),
            };
            transformations.push(transformation_config);
        }

        Ok(Self { transformations, input_count: 0, transformation_success_count: 0 })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        // Graceful error handling: if processing fails, preserve the original event
        let result = {
            // Check if we have multiple transformations that should be chained
            // For now, we'll chain transformations when we have multiple transformations
            // In the future, we could add logic to check if they use the same JMESPath expression
            let should_chain_transformations = self.transformations.len() > 1;

            if should_chain_transformations {
                // Chain transformations: apply them in sequence to the same data
                self.process_chained_transformations(input.clone())
            } else {
                // Process each transformation independently
                self.process_independent_transformations(input.clone())
            }
        };

        match result {
            Ok(events) => Ok(events),
            Err(e) => {
                eprintln!("String transformation failed: {e}. Preserving original event.");
                // Return the original event unchanged to ensure graceful degradation
                Ok(vec![input])
            }
        }
    }

    fn stats(&self) -> Option<String> {
        Some(format!(
            "input:{}\ntransformation_success:{}",
            self.input_count, self.transformation_success_count
        ))
    }
}

impl StringTransformerProcessor<'_> {
    fn process_chained_transformations(
        &mut self,
        input: Event,
    ) -> Result<Vec<Event>, anyhow::Error> {
        // Apply JMESPath expression to extract target data (using the first transformation's expression)
        let result = match self.transformations[0].jmespath.search(&input) {
            Ok(result) => result,
            Err(e) => {
                eprintln!(
                    "JMESPath evaluation failed: {e}. Skipping transformation for this event."
                );
                return Ok(vec![input]);
            }
        };

        // Handle null/empty results gracefully
        if result.is_null() {
            // JMESPath expression returned null result. No transformation applied.
            return Ok(vec![input]);
        }

        // Handle both single strings and arrays of strings
        if result.is_array() {
            let mut transformed_array = Vec::new();
            let mut any_transformed = false;

            if let Some(arr) = result.as_array() {
                for item in arr {
                    let initial_string = if item.is_string() {
                        item.as_string().expect("is string").to_string()
                    } else {
                        item.to_string()
                    };

                    // Apply all transformations in sequence
                    let mut current_string = initial_string;
                    for transformation_config in &self.transformations {
                        if let Some(transformed) = self.apply_transformation(
                            &transformation_config.transformation,
                            &current_string,
                        ) {
                            current_string = transformed;
                            any_transformed = true;
                        }
                    }

                    transformed_array.push(serde_json::Value::String(current_string));
                }

                if any_transformed {
                    self.transformation_success_count += 1;
                    match input.try_borrow_mut() {
                        Ok(mut event_ref) => {
                            if let Some(event) = event_ref.as_object_mut() {
                                // Use the last transformation's output label for the final result
                                let final_label =
                                    &self.transformations.last().unwrap().output_label;
                                event.insert(
                                    final_label.clone(),
                                    serde_json::Value::Array(transformed_array),
                                );
                            } else {
                                eprintln!("Event is not a JSON object. Cannot append transformation results.");
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to borrow event for modification: {e}. Transformation results not added.");
                        }
                    }
                }
            }
        } else {
            // Handle single values (strings or convert to string)
            let initial_string = if result.is_string() {
                result.as_string().expect("is string").to_owned()
            } else {
                result.to_string()
            };

            // Apply all transformations in sequence
            let mut current_string = initial_string;
            let mut any_transformed = false;
            for transformation_config in &self.transformations {
                if let Some(transformed) = self
                    .apply_transformation(&transformation_config.transformation, &current_string)
                {
                    current_string = transformed;
                    any_transformed = true;
                }
            }

            if any_transformed {
                self.transformation_success_count += 1;
                match input.try_borrow_mut() {
                    Ok(mut event_ref) => {
                        if let Some(event) = event_ref.as_object_mut() {
                            // Use the last transformation's output label for the final result
                            let final_label = &self.transformations.last().unwrap().output_label;
                            event.insert(final_label.clone(), current_string.into());
                        } else {
                            eprintln!(
                                "Event is not a JSON object. Cannot append transformation results."
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to borrow event for modification: {e}. Transformation results not added.");
                    }
                }
            }
        }

        Ok(vec![input])
    }

    fn process_independent_transformations(
        &mut self,
        input: Event,
    ) -> Result<Vec<Event>, anyhow::Error> {
        // Process each transformation configuration independently
        for config in &self.transformations {
            // Apply JMESPath expression to extract target data
            let result = match config.jmespath.search(&input) {
                Ok(result) => result,
                Err(e) => {
                    eprintln!("JMESPath evaluation failed: {e}. Skipping this transformation.");
                    continue;
                }
            };

            // Handle null/empty results gracefully
            if result.is_null() {
                // JMESPath expression returned null result. Skipping transformation.
                continue;
            }

            // Handle both single strings and arrays of strings
            if result.is_array() {
                let mut transformed_array = Vec::new();
                let mut any_transformed = false;

                if let Some(arr) = result.as_array() {
                    for item in arr {
                        if item.is_string() {
                            let s = item.as_string().expect("is string");
                            if let Some(transformed) =
                                self.apply_transformation(&config.transformation, s)
                            {
                                transformed_array.push(serde_json::Value::String(transformed));
                                any_transformed = true;
                            } else {
                                transformed_array.push(serde_json::Value::String(s.to_string()));
                            }
                        } else {
                            // Handle non-string items by converting to string first
                            eprintln!("Non-string data found in array at JMESPath location. Converting to string for transformation: {item:?}");
                            let string_value = item.to_string();
                            if let Some(transformed) =
                                self.apply_transformation(&config.transformation, &string_value)
                            {
                                transformed_array.push(serde_json::Value::String(transformed));
                                any_transformed = true;
                            } else {
                                // Convert JMESPath Variable to serde_json::Value
                                match serde_json::to_value(&**item) {
                                    Ok(json_value) => transformed_array.push(json_value),
                                    Err(e) => {
                                        eprintln!("Failed to convert non-string array item to JSON value: {e}. Skipping item.");
                                        // Skip this item entirely
                                    }
                                }
                            }
                        }
                    }

                    if any_transformed {
                        self.transformation_success_count += 1;
                        match input.try_borrow_mut() {
                            Ok(mut event_ref) => {
                                if let Some(event) = event_ref.as_object_mut() {
                                    event.insert(
                                        config.output_label.clone(),
                                        serde_json::Value::Array(transformed_array),
                                    );
                                } else {
                                    eprintln!("Event is not a JSON object. Cannot append transformation results for label '{}'.", config.output_label);
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to borrow event for modification: {}. Transformation results for label '{}' not added.", e, config.output_label);
                            }
                        }
                    }
                }
            } else {
                // Handle single values (strings or convert to string)
                let target_string = if result.is_string() {
                    result.as_string().expect("is string").to_owned()
                } else {
                    eprintln!("Non-string data found at JMESPath location. Converting to string for transformation: {result:?}");
                    result.to_string()
                };

                if let Some(transformed) =
                    self.apply_transformation(&config.transformation, &target_string)
                {
                    self.transformation_success_count += 1;
                    match input.try_borrow_mut() {
                        Ok(mut event_ref) => {
                            if let Some(event) = event_ref.as_object_mut() {
                                event.insert(config.output_label.clone(), transformed.into());
                            } else {
                                eprintln!("Event is not a JSON object. Cannot append transformation results for label '{}'.", config.output_label);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to borrow event for modification: {}. Transformation results for label '{}' not added.", e, config.output_label);
                        }
                    }
                }
            }
        }

        Ok(vec![input])
    }

    fn process_escape_sequences(input: &str) -> Result<String> {
        let mut result = String::new();
        let mut chars = input.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '\\' {
                if let Some(&next_ch) = chars.peek() {
                    match next_ch {
                        'n' => {
                            result.push('\n');
                            chars.next(); // consume the 'n'
                        }
                        'r' => {
                            result.push('\r');
                            chars.next(); // consume the 'r'
                        }
                        't' => {
                            result.push('\t');
                            chars.next(); // consume the 't'
                        }
                        '\\' => {
                            result.push('\\');
                            chars.next(); // consume the second '\'
                        }
                        '"' => {
                            result.push('"');
                            chars.next(); // consume the '"'
                        }
                        '0' => {
                            result.push('\0');
                            chars.next(); // consume the '0'
                        }
                        _ => {
                            return Err(anyhow::anyhow!("Invalid escape sequence: \\{}", next_ch));
                        }
                    }
                } else {
                    return Err(anyhow::anyhow!("Incomplete escape sequence at end of string"));
                }
            } else {
                result.push(ch);
            }
        }

        Ok(result)
    }

    fn apply_transformation(
        &self,
        transformation: &TransformationType,
        input: &str,
    ) -> Option<String> {
        match transformation {
            TransformationType::Uppercase => Some(input.to_uppercase()),
            TransformationType::Lowercase => Some(input.to_lowercase()),
            TransformationType::CharacterReplace { from_chars, to_chars } => {
                Some(self.character_replace(input, from_chars, to_chars))
            }
        }
    }

    fn character_replace(&self, input: &str, from_chars: &str, to_chars: &str) -> String {
        let from_chars: Vec<char> = from_chars.chars().collect();
        let to_chars: Vec<char> = to_chars.chars().collect();

        // Handle edge case where from_chars is empty
        if from_chars.is_empty() {
            return input.to_string();
        }

        input
            .chars()
            .filter_map(|ch| {
                if let Some(pos) = from_chars.iter().position(|&c| c == ch) {
                    if to_chars.is_empty() {
                        // Character deletion - return None to filter out
                        None
                    } else if pos < to_chars.len() {
                        // Map to corresponding character
                        Some(to_chars[pos])
                    } else {
                        // Use last character if target set is shorter
                        Some(to_chars[to_chars.len() - 1])
                    }
                } else {
                    // Character not in from_chars, keep as is
                    Some(ch)
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_escape_sequences_newline() {
        let result = StringTransformerProcessor::process_escape_sequences("hello\\nworld").unwrap();
        assert_eq!(result, "hello\nworld");
    }

    #[test]
    fn test_process_escape_sequences_carriage_return() {
        let result = StringTransformerProcessor::process_escape_sequences("hello\\rworld").unwrap();
        assert_eq!(result, "hello\rworld");
    }

    #[test]
    fn test_process_escape_sequences_tab() {
        let result = StringTransformerProcessor::process_escape_sequences("hello\\tworld").unwrap();
        assert_eq!(result, "hello\tworld");
    }

    #[test]
    fn test_process_escape_sequences_backslash() {
        let result =
            StringTransformerProcessor::process_escape_sequences("hello\\\\world").unwrap();
        assert_eq!(result, "hello\\world");
    }

    #[test]
    fn test_process_escape_sequences_quote() {
        let result =
            StringTransformerProcessor::process_escape_sequences("hello\\\"world").unwrap();
        assert_eq!(result, "hello\"world");
    }

    #[test]
    fn test_process_escape_sequences_null() {
        let result = StringTransformerProcessor::process_escape_sequences("hello\\0world").unwrap();
        assert_eq!(result, "hello\0world");
    }

    #[test]
    fn test_process_escape_sequences_multiple() {
        let result = StringTransformerProcessor::process_escape_sequences(
            "line1\\nline2\\tindented\\r\\nend",
        )
        .unwrap();
        assert_eq!(result, "line1\nline2\tindented\r\nend");
    }

    #[test]
    fn test_process_escape_sequences_all_types() {
        let result =
            StringTransformerProcessor::process_escape_sequences("\\n\\r\\t\\\\\\\"\\0").unwrap();
        assert_eq!(result, "\n\r\t\\\"\0");
    }

    #[test]
    fn test_process_escape_sequences_no_escapes() {
        let result = StringTransformerProcessor::process_escape_sequences("hello world").unwrap();
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_process_escape_sequences_empty_string() {
        let result = StringTransformerProcessor::process_escape_sequences("").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_process_escape_sequences_only_backslash() {
        let result = StringTransformerProcessor::process_escape_sequences("\\\\").unwrap();
        assert_eq!(result, "\\");
    }

    #[test]
    fn test_process_escape_sequences_consecutive_escapes() {
        let result = StringTransformerProcessor::process_escape_sequences("\\n\\n\\t\\t").unwrap();
        assert_eq!(result, "\n\n\t\t");
    }

    #[test]
    fn test_process_escape_sequences_mixed_content() {
        let result =
            StringTransformerProcessor::process_escape_sequences("start\\nmiddle\\tend").unwrap();
        assert_eq!(result, "start\nmiddle\tend");
    }

    #[test]
    fn test_process_escape_sequences_invalid_escape() {
        let result = StringTransformerProcessor::process_escape_sequences("hello\\xworld");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid escape sequence: \\x"));
    }

    #[test]
    fn test_process_escape_sequences_invalid_escape_z() {
        let result = StringTransformerProcessor::process_escape_sequences("hello\\zworld");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid escape sequence: \\z"));
    }

    #[test]
    fn test_process_escape_sequences_invalid_escape_digit() {
        let result = StringTransformerProcessor::process_escape_sequences("hello\\1world");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid escape sequence: \\1"));
    }

    #[test]
    fn test_process_escape_sequences_incomplete_escape_at_end() {
        let result = StringTransformerProcessor::process_escape_sequences("hello\\");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Incomplete escape sequence at end of string"));
    }

    #[test]
    fn test_process_escape_sequences_incomplete_escape_only_backslash() {
        let result = StringTransformerProcessor::process_escape_sequences("\\");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Incomplete escape sequence at end of string"));
    }

    #[test]
    fn test_process_escape_sequences_backslash_before_invalid() {
        let result = StringTransformerProcessor::process_escape_sequences("valid\\ntext\\qinvalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid escape sequence: \\q"));
    }

    #[test]
    fn test_process_escape_sequences_unicode_characters() {
        let result =
            StringTransformerProcessor::process_escape_sequences("hello\\n世界\\ttest").unwrap();
        assert_eq!(result, "hello\n世界\ttest");
    }

    #[test]
    fn test_process_escape_sequences_special_characters() {
        let result =
            StringTransformerProcessor::process_escape_sequences("@#$%^&*()\\n{}[]|").unwrap();
        assert_eq!(result, "@#$%^&*()\n{}[]|");
    }

    #[test]
    fn test_process_escape_sequences_real_world_example() {
        // Test a realistic example that might be used in character replacement
        let result = StringTransformerProcessor::process_escape_sequences("\\n\\r\\t").unwrap();
        assert_eq!(result, "\n\r\t");
    }

    #[test]
    fn test_process_escape_sequences_character_replacement_example() {
        // Test example from design doc: replacing newlines, carriage returns, and tabs with spaces
        let from_chars = StringTransformerProcessor::process_escape_sequences("\\n\\r\\t").unwrap();
        let to_chars = StringTransformerProcessor::process_escape_sequences(" ").unwrap();
        assert_eq!(from_chars, "\n\r\t");
        assert_eq!(to_chars, " ");
    }

    #[test]
    fn test_uppercase_transformation() {
        let processor = create_test_processor();
        let transformation = TransformationType::Uppercase;
        let result = processor.apply_transformation(&transformation, "hello world");
        assert_eq!(result, Some("HELLO WORLD".to_string()));
    }

    #[test]
    fn test_lowercase_transformation() {
        let processor = create_test_processor();
        let transformation = TransformationType::Lowercase;
        let result = processor.apply_transformation(&transformation, "HELLO WORLD");
        assert_eq!(result, Some("hello world".to_string()));
    }

    #[test]
    fn test_character_replace_basic() {
        let processor = create_test_processor();
        let result = processor.character_replace("hello world", "lo", "xy");
        assert_eq!(result, "hexxy wyrxd");
    }

    #[test]
    fn test_character_replace_deletion() {
        let processor = create_test_processor();
        let result = processor.character_replace("hello world", "lo", "");
        assert_eq!(result, "he wrd");
    }

    #[test]
    fn test_character_replace_shorter_target() {
        let processor = create_test_processor();
        let result = processor.character_replace("abcdef", "abc", "x");
        assert_eq!(result, "xxxdef");
    }

    #[test]
    fn test_character_replace_no_matches() {
        let processor = create_test_processor();
        let result = processor.character_replace("hello", "xyz", "abc");
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_character_replace_unicode() {
        let processor = create_test_processor();
        let result = processor.character_replace("héllo wörld", "éö", "eo");
        assert_eq!(result, "hello world");
    }

    fn create_test_processor() -> StringTransformerProcessor<'static> {
        StringTransformerProcessor {
            transformations: vec![],
            input_count: 0,
            transformation_success_count: 0,
        }
    }

    // JMESPath integration tests
    #[test]
    fn test_jmespath_simple_string_extraction() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args =
            vec!["stringtransformer".to_string(), "message".to_string(), "--uppercase".to_string()];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "message": "hello world"
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        assert_eq!(processed_event["transformed"], "HELLO WORLD");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_nested_string_extraction() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "data.text".to_string(),
            "--lowercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "data": {
                "text": "HELLO WORLD"
            }
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        assert_eq!(processed_event["transformed"], "hello world");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_array_string_extraction() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "messages".to_string(),
            "--uppercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "messages": ["hello", "world", "test"]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        let transformed = processed_event["transformed"].as_array().unwrap();
        assert_eq!(transformed[0], "HELLO");
        assert_eq!(transformed[1], "WORLD");
        assert_eq!(transformed[2], "TEST");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_array_with_character_replacement() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "items".to_string(),
            "--from".to_string(),
            "aeiou".to_string(),
            "--to".to_string(),
            "*".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "items": ["hello", "world", "beautiful"]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        let transformed = processed_event["transformed"].as_array().unwrap();
        assert_eq!(transformed[0], "h*ll*");
        assert_eq!(transformed[1], "w*rld");
        assert_eq!(transformed[2], "b***t*f*l");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_null_result_handling() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "nonexistent".to_string(),
            "--uppercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "message": "hello world"
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        // Should not have added the transformed field since JMESPath returned null
        assert!(!processed_event.as_object().unwrap().contains_key("transformed"));
        assert_eq!(processor.transformation_success_count, 0);
    }

    #[test]
    fn test_jmespath_empty_array_handling() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "empty_array".to_string(),
            "--uppercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "empty_array": []
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        // Empty array should not add a transformed field since no transformations occurred
        assert!(!processed_event.as_object().unwrap().contains_key("transformed"));
        assert_eq!(processor.transformation_success_count, 0); // No actual transformations occurred
    }

    #[test]
    fn test_jmespath_mixed_array_types() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args =
            vec!["stringtransformer".to_string(), "mixed".to_string(), "--uppercase".to_string()];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "mixed": ["hello", 123, true, "world"]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        let transformed = processed_event["transformed"].as_array().unwrap();
        assert_eq!(transformed[0], "HELLO");
        assert_eq!(transformed[1], "123"); // Number converted to string and uppercased
        assert_eq!(transformed[2], "TRUE"); // Boolean converted to string and uppercased
        assert_eq!(transformed[3], "WORLD");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_statistics_tracking() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args =
            vec!["stringtransformer".to_string(), "message".to_string(), "--uppercase".to_string()];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        // Process multiple events
        let event1_data = serde_json::json!({"message": "hello"});
        let event1 = Rc::new(RefCell::new(event1_data));

        let event2_data = serde_json::json!({"message": "world"});
        let event2 = Rc::new(RefCell::new(event2_data));

        let event3_data = serde_json::json!({"other": "data"}); // No matching field
        let event3 = Rc::new(RefCell::new(event3_data));

        processor.process(event1).unwrap();
        processor.process(event2).unwrap();
        processor.process(event3).unwrap();

        // Check statistics
        assert_eq!(processor.input_count, 3);
        assert_eq!(processor.transformation_success_count, 2); // Only 2 successful transformations

        let stats = processor.stats().unwrap();
        assert!(stats.contains("input:3"));
        assert!(stats.contains("transformation_success:2"));
    }

    #[test]
    fn test_error_handling_invalid_jmespath() {
        let args = vec![
            "stringtransformer".to_string(),
            "invalid[".to_string(), // Invalid JMESPath expression
            "--uppercase".to_string(),
        ];

        // Should fail during initialization due to invalid JMESPath
        let result = StringTransformerProcessor::new(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to compile JMESPath expression"));
    }

    #[test]
    fn test_error_handling_invalid_escape_sequences() {
        let args = vec![
            "stringtransformer".to_string(),
            "message".to_string(),
            "--from".to_string(),
            "\\x".to_string(), // Invalid escape sequence
            "--to".to_string(),
            " ".to_string(),
        ];

        // Should fail during initialization due to invalid escape sequence
        let result = StringTransformerProcessor::new(&args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid escape sequence in --from argument"));
    }

    #[test]
    fn test_graceful_degradation_on_processing_error() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args =
            vec!["stringtransformer".to_string(), "message".to_string(), "--uppercase".to_string()];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        // Create an event that should process successfully
        let event_data = serde_json::json!({"message": "hello world"});
        let event = Rc::new(RefCell::new(event_data));

        // Process should succeed
        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        // Verify statistics were updated
        assert_eq!(processor.input_count, 1);
        assert_eq!(processor.transformation_success_count, 1);

        // Verify transformation was applied
        let processed_event = result[0].borrow();
        assert_eq!(processed_event["transformed"], "HELLO WORLD");
    }

    #[test]
    fn test_jmespath_complex_expression() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "data[?type=='text'].content".to_string(),
            "--lowercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "data": [
                {"type": "text", "content": "HELLO"},
                {"type": "number", "content": "123"},
                {"type": "text", "content": "WORLD"}
            ]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        let transformed = processed_event["transformed"].as_array().unwrap();
        assert_eq!(transformed[0], "hello");
        assert_eq!(transformed[1], "world");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_custom_output_label() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "message".to_string(),
            "--uppercase".to_string(),
            "--label".to_string(),
            "custom_output".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "message": "hello world"
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        assert_eq!(processed_event["custom_output"], "HELLO WORLD");
        assert!(!processed_event.as_object().unwrap().contains_key("transformed"));
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_compilation_error() {
        let args = vec![
            "stringtransformer".to_string(),
            "invalid[".to_string(), // Invalid JMESPath expression
            "--uppercase".to_string(),
        ];
        let result = StringTransformerProcessor::new(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_jmespath_non_string_single_value() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args =
            vec!["stringtransformer".to_string(), "number".to_string(), "--uppercase".to_string()];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "number": 42
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        assert_eq!(processed_event["transformed"], "42");
        assert_eq!(processor.transformation_success_count, 1);
    }

    // Multiple transformation support tests
    #[test]
    fn test_multiple_transformations_case_then_character_replace() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "message".to_string(),
            "--uppercase".to_string(),
            "--from".to_string(),
            "AEIOU".to_string(),
            "--to".to_string(),
            "*".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "message": "hello world"
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        // Should apply uppercase first: "HELLO WORLD", then character replacement: "H*LL* W*RLD"
        assert_eq!(processed_event["transformed"], "H*LL* W*RLD");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_multiple_transformations_lowercase_then_character_replace() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "message".to_string(),
            "--lowercase".to_string(),
            "--from".to_string(),
            "aeiou".to_string(),
            "--to".to_string(),
            "*".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "message": "HELLO WORLD"
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        // Should apply lowercase first: "hello world", then character replacement: "h*ll* w*rld"
        assert_eq!(processed_event["transformed"], "h*ll* w*rld");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_multiple_transformations_with_array() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "messages".to_string(),
            "--uppercase".to_string(),
            "--from".to_string(),
            "AEIOU".to_string(),
            "--to".to_string(),
            "*".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "messages": ["hello", "world", "test"]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        let transformed = processed_event["transformed"].as_array().unwrap();
        // Each string should be uppercased then have vowels replaced
        assert_eq!(transformed[0], "H*LL*");
        assert_eq!(transformed[1], "W*RLD");
        assert_eq!(transformed[2], "T*ST");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_multiple_transformations_character_deletion() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "message".to_string(),
            "--lowercase".to_string(),
            "--from".to_string(),
            "aeiou".to_string(),
            "--to".to_string(),
            "".to_string(), // Empty string for deletion
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "message": "HELLO WORLD"
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        // Should apply lowercase first: "hello world", then delete vowels: "hll wrld"
        assert_eq!(processed_event["transformed"], "hll wrld");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_multiple_transformations_with_escape_sequences() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "message".to_string(),
            "--uppercase".to_string(),
            "--from".to_string(),
            "\\n\\r\\t".to_string(),
            "--to".to_string(),
            " ".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "message": "hello\nworld\ttest\rend"
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        // Should apply uppercase first, then replace whitespace characters with spaces
        assert_eq!(processed_event["transformed"], "HELLO WORLD TEST END");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_single_transformation_still_works() {
        use std::cell::RefCell;
        use std::rc::Rc;

        // Test that single transformations still work as before
        let args =
            vec!["stringtransformer".to_string(), "message".to_string(), "--uppercase".to_string()];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "message": "hello world"
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        assert_eq!(processed_event["transformed"], "HELLO WORLD");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_transformation_order_requirement() {
        use std::cell::RefCell;
        use std::rc::Rc;

        // Test that case conversion is applied before character replacement
        // This test verifies requirement 6.2
        let args = vec![
            "stringtransformer".to_string(),
            "message".to_string(),
            "--uppercase".to_string(),
            "--from".to_string(),
            "L".to_string(), // Only uppercase L should be replaced
            "--to".to_string(),
            "X".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "message": "hello" // lowercase 'l' should become 'L' then 'X'
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        // Should be "HEXXO" - uppercase first (HELLO), then replace L with X (HEXXO)
        assert_eq!(processed_event["transformed"], "HEXXO");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_multiple_transformations_configuration_validation() {
        // Test that we can create a processor with both transformations
        let args = vec![
            "stringtransformer".to_string(),
            "message".to_string(),
            "--uppercase".to_string(),
            "--from".to_string(),
            "ABC".to_string(),
            "--to".to_string(),
            "123".to_string(),
        ];
        let processor = StringTransformerProcessor::new(&args).unwrap();

        // Should have 2 transformations
        assert_eq!(processor.transformations.len(), 2);

        // First should be uppercase
        match &processor.transformations[0].transformation {
            TransformationType::Uppercase => {}
            _ => panic!("First transformation should be uppercase"),
        }

        // Second should be character replacement
        match &processor.transformations[1].transformation {
            TransformationType::CharacterReplace { from_chars, to_chars } => {
                assert_eq!(from_chars, "ABC");
                assert_eq!(to_chars, "123");
            }
            _ => panic!("Second transformation should be character replacement"),
        }
    }

    #[test]
    fn test_multiple_transformations_statistics() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "message".to_string(),
            "--uppercase".to_string(),
            "--from".to_string(),
            "AEIOU".to_string(),
            "--to".to_string(),
            "*".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        // Process multiple events
        for i in 0..3 {
            let event_data = serde_json::json!({
                "message": format!("hello {}", i)
            });
            let event = Rc::new(RefCell::new(event_data));
            processor.process(event).unwrap();
        }

        // Each event should count as one successful transformation (even though it has multiple steps)
        assert_eq!(processor.input_count, 3);
        assert_eq!(processor.transformation_success_count, 3);
    }

    // Additional JMESPath integration tests (Task 10)

    #[test]
    fn test_jmespath_deeply_nested_expression() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "data.nested.deep.message".to_string(),
            "--uppercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "data": {
                "nested": {
                    "deep": {
                        "message": "hello world"
                    }
                }
            }
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        assert_eq!(processed_event["transformed"], "HELLO WORLD");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_array_index_access() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "messages[1]".to_string(),
            "--lowercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "messages": ["FIRST", "SECOND", "THIRD"]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        assert_eq!(processed_event["transformed"], "second");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_array_slice() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "messages[1:3]".to_string(),
            "--uppercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "messages": ["first", "second", "third", "fourth"]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        let transformed = processed_event["transformed"].as_array().unwrap();
        assert_eq!(transformed.len(), 2);
        assert_eq!(transformed[0], "SECOND");
        assert_eq!(transformed[1], "THIRD");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_filter_expression() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "items[?active==`true`].name".to_string(),
            "--uppercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "items": [
                {"name": "item1", "active": true},
                {"name": "item2", "active": false},
                {"name": "item3", "active": true}
            ]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        let transformed = processed_event["transformed"].as_array().unwrap();
        assert_eq!(transformed.len(), 2);
        assert_eq!(transformed[0], "ITEM1");
        assert_eq!(transformed[1], "ITEM3");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_projection_expression() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "users[*].name".to_string(),
            "--lowercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "users": [
                {"name": "ALICE", "age": 30},
                {"name": "BOB", "age": 25},
                {"name": "CHARLIE", "age": 35}
            ]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        let transformed = processed_event["transformed"].as_array().unwrap();
        assert_eq!(transformed.len(), 3);
        assert_eq!(transformed[0], "alice");
        assert_eq!(transformed[1], "bob");
        assert_eq!(transformed[2], "charlie");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_pipe_expression() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "data | keys(@) | [0]".to_string(),
            "--uppercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "data": {
                "message": "hello",
                "status": "active"
            }
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        // The first key should be transformed to uppercase
        let transformed_value = processed_event["transformed"].as_str().unwrap();
        assert!(transformed_value == "MESSAGE" || transformed_value == "STATUS");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_function_expression() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "join(' ', messages)".to_string(),
            "--uppercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "messages": ["hello", "world", "test"]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        assert_eq!(processed_event["transformed"], "HELLO WORLD TEST");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_null_result_various_scenarios() {
        use std::cell::RefCell;
        use std::rc::Rc;

        // Test non-existent nested path
        let args = vec![
            "stringtransformer".to_string(),
            "data.missing.path".to_string(),
            "--uppercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "data": {
                "existing": "value"
            }
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        assert!(!processed_event.as_object().unwrap().contains_key("transformed"));
        assert_eq!(processor.transformation_success_count, 0);
    }

    #[test]
    fn test_jmespath_null_result_filter_no_matches() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "items[?status=='inactive'].name".to_string(),
            "--uppercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "items": [
                {"name": "item1", "status": "active"},
                {"name": "item2", "status": "active"}
            ]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        // Filter returns empty array, which should not add transformed field
        assert!(!processed_event.as_object().unwrap().contains_key("transformed"));
        assert_eq!(processor.transformation_success_count, 0);
    }

    #[test]
    fn test_jmespath_empty_string_result() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args =
            vec!["stringtransformer".to_string(), "message".to_string(), "--uppercase".to_string()];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "message": ""
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        assert_eq!(processed_event["transformed"], "");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_array_with_null_elements() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "messages".to_string(),
            "--uppercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "messages": ["hello", null, "world", null]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        let transformed = processed_event["transformed"].as_array().unwrap();
        assert_eq!(transformed.len(), 4);
        assert_eq!(transformed[0], "HELLO");
        assert_eq!(transformed[1], "NULL"); // null converted to string "null" then uppercased
        assert_eq!(transformed[2], "WORLD");
        assert_eq!(transformed[3], "NULL");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_array_with_empty_strings() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "messages".to_string(),
            "--from".to_string(),
            "aeiou".to_string(),
            "--to".to_string(),
            "*".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "messages": ["hello", "", "world", ""]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        let transformed = processed_event["transformed"].as_array().unwrap();
        assert_eq!(transformed.len(), 4);
        assert_eq!(transformed[0], "h*ll*");
        assert_eq!(transformed[1], ""); // Empty string remains empty
        assert_eq!(transformed[2], "w*rld");
        assert_eq!(transformed[3], "");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_complex_nested_array_processing() {
        use std::cell::RefCell;
        use std::rc::Rc;

        // Test processing a specific group's members
        let args = vec![
            "stringtransformer".to_string(),
            "groups[0].members[*].name".to_string(),
            "--lowercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "groups": [
                {
                    "name": "group1",
                    "members": [
                        {"name": "ALICE"},
                        {"name": "BOB"}
                    ]
                },
                {
                    "name": "group2",
                    "members": [
                        {"name": "CHARLIE"},
                        {"name": "DIANA"}
                    ]
                }
            ]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        let transformed = processed_event["transformed"].as_array().unwrap();
        assert_eq!(transformed.len(), 2);
        assert_eq!(transformed[0], "alice");
        assert_eq!(transformed[1], "bob");
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_multiselect_hash() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args = vec![
            "stringtransformer".to_string(),
            "data.{title: title, desc: description}".to_string(),
            "--uppercase".to_string(),
        ];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "data": {
                "title": "hello world",
                "description": "test description",
                "other": "ignored"
            }
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        // The multiselect hash result should be converted to string and uppercased
        let transformed_str = processed_event["transformed"].as_str().unwrap();
        assert!(transformed_str.contains("HELLO WORLD"));
        assert!(transformed_str.contains("TEST DESCRIPTION"));
        assert_eq!(processor.transformation_success_count, 1);
    }

    #[test]
    fn test_jmespath_boolean_and_number_handling() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let args =
            vec!["stringtransformer".to_string(), "values".to_string(), "--uppercase".to_string()];
        let mut processor = StringTransformerProcessor::new(&args).unwrap();

        let event_data = serde_json::json!({
            "values": [true, false, 42, 3.15, "hello"]
        });
        let event = Rc::new(RefCell::new(event_data));

        let result = processor.process(event.clone()).unwrap();
        assert_eq!(result.len(), 1);

        let processed_event = result[0].borrow();
        let transformed = processed_event["transformed"].as_array().unwrap();
        assert_eq!(transformed.len(), 5);
        assert_eq!(transformed[0], "TRUE");
        assert_eq!(transformed[1], "FALSE");
        assert_eq!(transformed[2], "42");
        assert_eq!(transformed[3], "3.15");
        assert_eq!(transformed[4], "HELLO");
        assert_eq!(processor.transformation_success_count, 1);
    }

    // Additional comprehensive unit tests for basic functionality (Task 9)

    #[test]
    fn test_uppercase_transformation_comprehensive() {
        let processor = create_test_processor();
        let transformation = TransformationType::Uppercase;

        // Test basic uppercase
        assert_eq!(
            processor.apply_transformation(&transformation, "hello world"),
            Some("HELLO WORLD".to_string())
        );

        // Test mixed case
        assert_eq!(
            processor.apply_transformation(&transformation, "HeLLo WoRLd"),
            Some("HELLO WORLD".to_string())
        );

        // Test already uppercase
        assert_eq!(
            processor.apply_transformation(&transformation, "HELLO WORLD"),
            Some("HELLO WORLD".to_string())
        );

        // Test with numbers and special characters
        assert_eq!(
            processor.apply_transformation(&transformation, "hello123!@#world"),
            Some("HELLO123!@#WORLD".to_string())
        );

        // Test empty string
        assert_eq!(processor.apply_transformation(&transformation, ""), Some("".to_string()));

        // Test single character
        assert_eq!(processor.apply_transformation(&transformation, "a"), Some("A".to_string()));

        // Test whitespace only
        assert_eq!(processor.apply_transformation(&transformation, "   "), Some("   ".to_string()));
    }

    #[test]
    fn test_lowercase_transformation_comprehensive() {
        let processor = create_test_processor();
        let transformation = TransformationType::Lowercase;

        // Test basic lowercase
        assert_eq!(
            processor.apply_transformation(&transformation, "HELLO WORLD"),
            Some("hello world".to_string())
        );

        // Test mixed case
        assert_eq!(
            processor.apply_transformation(&transformation, "HeLLo WoRLd"),
            Some("hello world".to_string())
        );

        // Test already lowercase
        assert_eq!(
            processor.apply_transformation(&transformation, "hello world"),
            Some("hello world".to_string())
        );

        // Test with numbers and special characters
        assert_eq!(
            processor.apply_transformation(&transformation, "HELLO123!@#WORLD"),
            Some("hello123!@#world".to_string())
        );

        // Test empty string
        assert_eq!(processor.apply_transformation(&transformation, ""), Some("".to_string()));

        // Test single character
        assert_eq!(processor.apply_transformation(&transformation, "A"), Some("a".to_string()));

        // Test whitespace only
        assert_eq!(processor.apply_transformation(&transformation, "   "), Some("   ".to_string()));
    }

    #[test]
    fn test_character_replacement_various_character_sets() {
        let processor = create_test_processor();

        // Test vowel replacement
        assert_eq!(processor.character_replace("hello world", "aeiou", "*"), "h*ll* w*rld");

        // Test consonant replacement
        assert_eq!(processor.character_replace("hello world", "hlwrd", "HLWRD"), "HeLLo WoRLD");

        // Test digit replacement
        assert_eq!(processor.character_replace("abc123def456", "123456", "ABCDEF"), "abcABCdefDEF");

        // Test special character replacement
        assert_eq!(
            processor.character_replace("hello!@#world$%^", "!@#$%^", "______"),
            "hello___world___"
        );

        // Test single character to single character
        assert_eq!(processor.character_replace("aaaaa", "a", "b"), "bbbbb");

        // Test multiple characters to single character
        assert_eq!(processor.character_replace("abcdef", "abc", "x"), "xxxdef");

        // Test single character to multiple characters (uses first target char)
        assert_eq!(processor.character_replace("aaa", "a", "xyz"), "xxx");

        // Test overlapping character sets
        assert_eq!(processor.character_replace("abcabc", "abc", "xyz"), "xyzxyz");

        // Test case sensitivity
        assert_eq!(processor.character_replace("AaBbCc", "abc", "123"), "A1B2C3");

        // Test whitespace replacement
        assert_eq!(processor.character_replace("hello world test", " ", "_"), "hello_world_test");
    }

    #[test]
    fn test_character_deletion_comprehensive() {
        let processor = create_test_processor();

        // Test vowel deletion
        assert_eq!(processor.character_replace("hello world", "aeiou", ""), "hll wrld");

        // Test consonant deletion
        assert_eq!(processor.character_replace("hello world", "hlwrd", ""), "eo o");

        // Test digit deletion
        assert_eq!(processor.character_replace("abc123def456", "123456", ""), "abcdef");

        // Test special character deletion
        assert_eq!(processor.character_replace("hello!@#world$%^", "!@#$%^", ""), "helloworld");

        // Test whitespace deletion
        assert_eq!(processor.character_replace("hello world test", " \t\n", ""), "helloworldtest");

        // Test delete all characters
        assert_eq!(processor.character_replace("abc", "abc", ""), "");

        // Test delete from empty string
        assert_eq!(processor.character_replace("", "abc", ""), "");

        // Test delete non-existent characters
        assert_eq!(processor.character_replace("hello", "xyz", ""), "hello");

        // Test delete single character multiple times
        assert_eq!(processor.character_replace("aaabbbccc", "b", ""), "aaaccc");
    }

    #[test]
    fn test_escape_sequence_handling_in_character_sets() {
        let processor = create_test_processor();

        // Test newline escape sequence
        let from_chars = StringTransformerProcessor::process_escape_sequences("\\n").unwrap();
        let to_chars = StringTransformerProcessor::process_escape_sequences(" ").unwrap();
        assert_eq!(
            processor.character_replace("hello\nworld", &from_chars, &to_chars),
            "hello world"
        );

        // Test tab escape sequence
        let from_chars = StringTransformerProcessor::process_escape_sequences("\\t").unwrap();
        let to_chars = StringTransformerProcessor::process_escape_sequences(" ").unwrap();
        assert_eq!(
            processor.character_replace("hello\tworld", &from_chars, &to_chars),
            "hello world"
        );

        // Test carriage return escape sequence
        let from_chars = StringTransformerProcessor::process_escape_sequences("\\r").unwrap();
        let to_chars = StringTransformerProcessor::process_escape_sequences(" ").unwrap();
        assert_eq!(
            processor.character_replace("hello\rworld", &from_chars, &to_chars),
            "hello world"
        );

        // Test backslash escape sequence
        let from_chars = StringTransformerProcessor::process_escape_sequences("\\\\").unwrap();
        let to_chars = StringTransformerProcessor::process_escape_sequences("/").unwrap();
        assert_eq!(
            processor.character_replace("hello\\world", &from_chars, &to_chars),
            "hello/world"
        );

        // Test quote escape sequence
        let from_chars = StringTransformerProcessor::process_escape_sequences("\\\"").unwrap();
        let to_chars = StringTransformerProcessor::process_escape_sequences("'").unwrap();
        assert_eq!(
            processor.character_replace("hello\"world", &from_chars, &to_chars),
            "hello'world"
        );

        // Test null character escape sequence
        let from_chars = StringTransformerProcessor::process_escape_sequences("\\0").unwrap();
        let to_chars = StringTransformerProcessor::process_escape_sequences(" ").unwrap();
        assert_eq!(
            processor.character_replace("hello\0world", &from_chars, &to_chars),
            "hello world"
        );

        // Test multiple escape sequences in from_chars
        let from_chars = StringTransformerProcessor::process_escape_sequences("\\n\\r\\t").unwrap();
        let to_chars = StringTransformerProcessor::process_escape_sequences(" ").unwrap();
        assert_eq!(
            processor.character_replace("line1\nline2\rline3\tindented", &from_chars, &to_chars),
            "line1 line2 line3 indented"
        );

        // Test multiple escape sequences in to_chars
        let from_chars = StringTransformerProcessor::process_escape_sequences("abc").unwrap();
        let to_chars = StringTransformerProcessor::process_escape_sequences("\\n\\r\\t").unwrap();
        assert_eq!(processor.character_replace("abc", &from_chars, &to_chars), "\n\r\t");

        // Test escape sequences with regular characters
        let from_chars = StringTransformerProcessor::process_escape_sequences("a\\nb").unwrap();
        let to_chars = StringTransformerProcessor::process_escape_sequences("x y").unwrap();
        assert_eq!(processor.character_replace("a\nb", &from_chars, &to_chars), "x y");

        // Test deletion with escape sequences
        let from_chars = StringTransformerProcessor::process_escape_sequences("\\n\\r\\t").unwrap();
        let to_chars = StringTransformerProcessor::process_escape_sequences("").unwrap();
        assert_eq!(
            processor.character_replace("hello\nworld\ttest\rend", &from_chars, &to_chars),
            "helloworldtestend"
        );
    }

    #[test]
    fn test_character_replacement_edge_cases() {
        let processor = create_test_processor();

        // Test empty from_chars
        assert_eq!(processor.character_replace("hello", "", "abc"), "hello");

        // Test longer target than source (should use only matching positions)
        assert_eq!(processor.character_replace("abc", "ab", "12345"), "12c");

        // Test repeated characters in from_chars (first occurrence wins)
        assert_eq!(
            processor.character_replace("aaa", "aa", "12"),
            "111" // First 'a' maps to '1', second 'a' also maps to '1'
        );

        // Test Unicode characters
        assert_eq!(processor.character_replace("héllo wörld", "éö", "eo"), "hello world");

        // Test emoji and special Unicode
        assert_eq!(
            processor.character_replace("hello 😀 world 🌍", "😀🌍", "😊🌎"),
            "hello 😊 world 🌎"
        );

        // Test very long strings
        let long_input = "a".repeat(1000);
        let expected = "b".repeat(1000);
        assert_eq!(processor.character_replace(&long_input, "a", "b"), expected);

        // Test all ASCII printable characters
        let ascii_chars: String = (32..127).map(|i| i as u8 as char).collect();
        let result = processor.character_replace(&ascii_chars, " ", "_");
        assert!(result.contains("_"));
        assert!(!result.contains(" "));
    }

    #[test]
    fn test_case_transformation_with_unicode() {
        let processor = create_test_processor();

        // Test uppercase with Unicode
        let uppercase = TransformationType::Uppercase;
        assert_eq!(
            processor.apply_transformation(&uppercase, "héllo wörld"),
            Some("HÉLLO WÖRLD".to_string())
        );

        // Test lowercase with Unicode
        let lowercase = TransformationType::Lowercase;
        assert_eq!(
            processor.apply_transformation(&lowercase, "HÉLLO WÖRLD"),
            Some("héllo wörld".to_string())
        );

        // Test with mixed scripts
        assert_eq!(
            processor.apply_transformation(&uppercase, "hello 世界"),
            Some("HELLO 世界".to_string())
        );

        // Test with Cyrillic
        assert_eq!(
            processor.apply_transformation(&uppercase, "привет мир"),
            Some("ПРИВЕТ МИР".to_string())
        );

        // Test with Greek
        assert_eq!(
            processor.apply_transformation(&lowercase, "ΓΕΙΑ ΣΟΥ ΚΟΣΜΟΣ"),
            Some("γεια σου κοσμος".to_string())
        );
    }

    #[test]
    fn test_character_replacement_performance_patterns() {
        let processor = create_test_processor();

        // Test many-to-one mapping
        assert_eq!(processor.character_replace("abcdef", "abcdef", "x"), "xxxxxx");

        // Test one-to-many mapping (should use first character)
        assert_eq!(processor.character_replace("a", "a", "xyz"), "x");

        // Test alternating pattern
        assert_eq!(processor.character_replace("ababab", "ab", "12"), "121212");

        // Test no replacement needed
        assert_eq!(processor.character_replace("hello", "xyz", "abc"), "hello");

        // Test complete replacement
        assert_eq!(processor.character_replace("abc", "abc", "123"), "123");
    }

    #[test]
    fn test_transformation_preserves_non_alphabetic_characters() {
        let processor = create_test_processor();

        // Test uppercase preserves numbers and symbols
        let uppercase = TransformationType::Uppercase;
        assert_eq!(
            processor.apply_transformation(&uppercase, "hello123!@#$%^&*()world"),
            Some("HELLO123!@#$%^&*()WORLD".to_string())
        );

        // Test lowercase preserves numbers and symbols
        let lowercase = TransformationType::Lowercase;
        assert_eq!(
            processor.apply_transformation(&lowercase, "HELLO123!@#$%^&*()WORLD"),
            Some("hello123!@#$%^&*()world".to_string())
        );

        // Test with various whitespace characters
        assert_eq!(
            processor.apply_transformation(&uppercase, "hello\n\t\r world"),
            Some("HELLO\n\t\r WORLD".to_string())
        );

        // Test with punctuation
        assert_eq!(
            processor.apply_transformation(&lowercase, "HELLO, WORLD! HOW ARE YOU?"),
            Some("hello, world! how are you?".to_string())
        );
    }
}
