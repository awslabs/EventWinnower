use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use data_encoding::{BASE32, BASE64, BASE64URL, HEXLOWER_PERMISSIVE};
use eventwinnower_macros::SerialProcessorInit;

#[derive(Debug, Clone)]
enum DecodingMethod {
    Base64,
    Base64Url,
    Base32,
    Hex,
}

impl std::str::FromStr for DecodingMethod {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "base64" => Ok(DecodingMethod::Base64),
            "base64url" => Ok(DecodingMethod::Base64Url),
            "base32" => Ok(DecodingMethod::Base32),
            "hex" => Ok(DecodingMethod::Hex),
            _ => Err(anyhow::anyhow!(
                "Invalid decoding method: {}. Valid options are: base64, base64url, base32, hex",
                s
            )),
        }
    }
}

#[derive(Debug, SerialProcessorInit)]
pub struct StringDecoderProcessor<'a> {
    path: jmespath::Expression<'a>,
    decoder: DecodingMethod,
    label: String,
    input_count: u64,
    decode_success_count: u64,
}

#[derive(Parser)]
/// Extract and decode strings from events using JMESPath expressions
#[command(version, long_about = None, arg_required_else_help(true))]
struct StringDecoderArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long, default_value = "base64")]
    method: String,

    #[arg(short, long, default_value = "decoded")]
    label: String,
}

impl SerialProcessor for StringDecoderProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("decode strings from JMESPath using base64/hex/base32".to_string())
    }
    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = StringDecoderArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;
        let decoder = args.method.parse::<DecodingMethod>()?;

        Ok(Self { path, decoder, label: args.label, input_count: 0, decode_success_count: 0 })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        // Increment input count for each processed event
        self.input_count += 1;

        // Apply JMESPath expression to extract target string
        let result = self.path.search(&input)?;

        // Handle null/empty results gracefully
        if result.is_null() {
            return Ok(vec![input]);
        }

        // Convert non-string results to strings
        let target_string = if result.is_string() {
            result.as_string().expect("is string").to_owned()
        } else {
            result.to_string()
        };

        // Strip whitespace from input strings before decoding
        let trimmed_string = target_string.trim();

        // Handle empty strings
        if trimmed_string.is_empty() {
            return Ok(vec![input]);
        }

        // Attempt to decode the string
        match self.decode_string(trimmed_string) {
            Ok(decoded_content) => {
                // Increment decode success count for successful decode operation
                self.decode_success_count += 1;

                // Add decoded content to event with specified label
                if let Some(event) = input.borrow_mut().as_object_mut() {
                    event.insert(self.label.clone(), decoded_content);
                }
                Ok(vec![input])
            }
            Err(_) => {
                // Handle decoding failures gracefully by passing through original events
                Ok(vec![input])
            }
        }
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\ndecode_success:{}", self.input_count, self.decode_success_count))
    }
}

impl StringDecoderProcessor<'_> {
    fn decode_string(&self, input: &str) -> Result<serde_json::Value> {
        match self.decoder {
            DecodingMethod::Base64 => {
                let decoded_bytes = BASE64.decode(input.as_bytes())?;
                self.bytes_to_json_value(decoded_bytes)
            }
            DecodingMethod::Base64Url => {
                let decoded_bytes = BASE64URL.decode(input.as_bytes())?;
                self.bytes_to_json_value(decoded_bytes)
            }
            DecodingMethod::Base32 => {
                let decoded_bytes = BASE32.decode(input.as_bytes())?;
                self.bytes_to_json_value(decoded_bytes)
            }
            DecodingMethod::Hex => {
                let decoded_bytes = HEXLOWER_PERMISSIVE.decode(input.as_bytes())?;
                self.bytes_to_json_value(decoded_bytes)
            }
        }
    }

    fn bytes_to_json_value(&self, bytes: Vec<u8>) -> Result<serde_json::Value> {
        // Try to convert to UTF-8 string first
        match String::from_utf8(bytes) {
            Ok(utf8_string) => Ok(serde_json::Value::String(utf8_string)),
            Err(_) => {
                // Handle non-UTF8 decoded results by returning an error
                Err(anyhow::anyhow!("Decoded content is not valid UTF-8"))
            }
        }
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
    fn test_successful_base64_decoding() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "base64".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "SGVsbG8gV29ybGQ=" // "Hello World" in base64
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert_eq!(output["decoded"], "Hello World");
        assert_eq!(processor.input_count, 1);
        assert_eq!(processor.decode_success_count, 1);
    }

    #[test]
    fn test_successful_base64url_decoding() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "base64url".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "SGVsbG8gV29ybGQ=" // "Hello World" in base64url
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert_eq!(output["decoded"], "Hello World");
        assert_eq!(processor.decode_success_count, 1);
    }

    #[test]
    fn test_successful_base32_decoding() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "base32".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "JBSWY3DPEBLW64TMMQ======" // "Hello World" in base32
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert_eq!(output["decoded"], "Hello World");
        assert_eq!(processor.decode_success_count, 1);
    }

    #[test]
    fn test_successful_hex_decoding() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "hex".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "48656c6c6f20576f726c64" // "Hello World" in hex
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert_eq!(output["decoded"], "Hello World");
        assert_eq!(processor.decode_success_count, 1);
    }

    #[test]
    fn test_invalid_base64_string() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "base64".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "invalid_base64!"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert!(!output.as_object().unwrap().contains_key("decoded"));
        assert_eq!(processor.input_count, 1);
        assert_eq!(processor.decode_success_count, 0);
    }

    #[test]
    fn test_invalid_hex_string() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "hex".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "invalid_hex_string"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert!(!output.as_object().unwrap().contains_key("decoded"));
        assert_eq!(processor.decode_success_count, 0);
    }

    #[test]
    fn test_jmespath_expression_with_nested_data() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "payload.encoded_data".to_string(),
            "--method".to_string(),
            "base64".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "payload": {
                "encoded_data": "SGVsbG8gV29ybGQ="
            }
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert_eq!(output["decoded"], "Hello World");
    }

    #[test]
    fn test_jmespath_expression_with_array_access() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "items[0].data".to_string(),
            "--method".to_string(),
            "base64".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "items": [
                {"data": "SGVsbG8gV29ybGQ="},
                {"data": "other"}
            ]
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert_eq!(output["decoded"], "Hello World");
    }

    #[test]
    fn test_jmespath_expression_returns_null() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "nonexistent_field".to_string(),
            "--method".to_string(),
            "base64".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "SGVsbG8gV29ybGQ="
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert!(!output.as_object().unwrap().contains_key("decoded"));
        assert_eq!(processor.decode_success_count, 0);
    }

    #[test]
    fn test_jmespath_expression_returns_number() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "number_field".to_string(),
            "--method".to_string(),
            "hex".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "number_field": "48656c6c6f" // This will be converted to string "48656c6c6f"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert_eq!(output["decoded"], "Hello");
    }

    #[test]
    fn test_custom_output_label() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "base64".to_string(),
            "--label".to_string(),
            "custom_field".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "SGVsbG8gV29ybGQ="
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert_eq!(output["custom_field"], "Hello World");
        assert!(!output.as_object().unwrap().contains_key("decoded"));
    }

    #[test]
    fn test_default_method_is_base64() {
        let processor =
            StringDecoderProcessor::new(&["stringdecoder".to_string(), "data".to_string()])
                .unwrap();

        assert!(matches!(processor.decoder, DecodingMethod::Base64));
    }

    #[test]
    fn test_default_label_is_decoded() {
        let processor =
            StringDecoderProcessor::new(&["stringdecoder".to_string(), "data".to_string()])
                .unwrap();

        assert_eq!(processor.label, "decoded");
    }

    #[test]
    fn test_invalid_decoding_method() {
        let result = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "invalid_method".to_string(),
        ]);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid decoding method"));
    }

    #[test]
    fn test_empty_string_input() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "base64".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": ""
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert!(!output.as_object().unwrap().contains_key("decoded"));
        assert_eq!(processor.decode_success_count, 0);
    }

    #[test]
    fn test_whitespace_only_string() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "base64".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "   \n\t  "
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert!(!output.as_object().unwrap().contains_key("decoded"));
        assert_eq!(processor.decode_success_count, 0);
    }

    #[test]
    fn test_string_with_leading_trailing_whitespace() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "base64".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "  SGVsbG8gV29ybGQ=  \n"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert_eq!(output["decoded"], "Hello World");
        assert_eq!(processor.decode_success_count, 1);
    }

    #[test]
    fn test_overwrite_existing_field() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "base64".to_string(),
            "--label".to_string(),
            "existing_field".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "data": "SGVsbG8gV29ybGQ=",
            "existing_field": "old_value"
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert_eq!(output["existing_field"], "Hello World");
    }

    #[test]
    fn test_metrics_tracking() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "base64".to_string(),
        ])
        .unwrap();

        // Process successful decode
        let input1 = create_event(json!({"data": "SGVsbG8gV29ybGQ="}));
        processor.process(input1).unwrap();

        // Process failed decode
        let input2 = create_event(json!({"data": "invalid_base64!"}));
        processor.process(input2).unwrap();

        // Process null result
        let input3 = create_event(json!({"other_field": "value"}));
        processor.process(input3).unwrap();

        assert_eq!(processor.input_count, 3);
        assert_eq!(processor.decode_success_count, 1);

        let stats = processor.stats().unwrap();
        assert!(stats.contains("input:3"));
        assert!(stats.contains("decode_success:1"));
    }

    #[test]
    fn test_invalid_jmespath_expression() {
        let result = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "invalid[[[jmespath".to_string(),
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn test_case_insensitive_method_parsing() {
        let processor1 = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "BASE64".to_string(),
        ])
        .unwrap();
        assert!(matches!(processor1.decoder, DecodingMethod::Base64));

        let processor2 = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "data".to_string(),
            "--method".to_string(),
            "HEX".to_string(),
        ])
        .unwrap();
        assert!(matches!(processor2.decoder, DecodingMethod::Hex));
    }

    #[test]
    fn test_complex_jmespath_with_filtering() {
        let mut processor = StringDecoderProcessor::new(&[
            "stringdecoder".to_string(),
            "logs[?level=='info'].message | [0]".to_string(), // Get first element from filtered array
            "--method".to_string(),
            "base64".to_string(),
        ])
        .unwrap();

        let input = create_event(json!({
            "logs": [
                {"level": "error", "message": "error_msg"},
                {"level": "info", "message": "SGVsbG8gV29ybGQ="},
                {"level": "debug", "message": "debug_msg"}
            ]
        }));

        let result = processor.process(input).unwrap();
        assert_eq!(result.len(), 1);

        let output = result[0].borrow();
        assert_eq!(output["decoded"], "Hello World");
    }
}
