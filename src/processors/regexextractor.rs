use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use regex::Regex;

#[derive(SerialProcessorInit)]
pub struct RegexExtractorProcessor<'a> {
    source_path: jmespath::Expression<'a>,
    regex: Regex,
    target_label: String,
    input_count: u64,
    output_count: u64,
    capture_group: Option<usize>,
}

#[derive(Parser)]
/// Extract substring from string using regex pattern via JMESPath
///
/// This processor extracts substrings from event fields using regular expressions.
/// It uses JMESPath to select the source field, applies a regex pattern to extract
/// a substring, and appends the result to the event with a configurable label.
///
/// Examples:
///   Extract date from message:
///     regex message -p '\d{4}-\d{2}-\d{2}' -t extracted_date
///
///   Extract domain from URL using capture group:
///     regex url -p 'https?://([^/]+)' -c 1 -t domain
///
///   Extract from nested field:
///     regex 'request.headers.UserAgent' -p 'Mozilla/[\d.]+' -t browser
#[command(version, arg_required_else_help(true))]
struct RegexExtractorArgs {
    /// JMESPath expression to select the source field from the event
    #[arg(required(true))]
    source_path: Vec<String>,

    /// Regular expression pattern to match against the source string
    #[arg(short, long, required(true))]
    pattern: String,

    /// Label for the extracted value in the output event
    #[arg(short, long, default_value = "regex_extract")]
    target_label: String,

    /// Capture group number to extract (0 = entire match, 1+ = specific groups)
    #[arg(short, long)]
    capture_group: Option<usize>,
}

impl SerialProcessor for RegexExtractorProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("extract substring from string using regex pattern".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = RegexExtractorArgs::try_parse_from(argv)?;
        let source_path = jmespath::compile(&args.source_path.join(" "))?;
        let regex = Regex::new(&args.pattern)?;

        Ok(Self {
            source_path,
            regex,
            target_label: args.target_label,
            input_count: 0,
            output_count: 0,
            capture_group: args.capture_group,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        // Extract the source field using JMESPath
        let result = self.source_path.search(&input)?;

        if result.is_null() {
            return Ok(vec![input]);
        }

        let source_string = if result.is_string() {
            result.as_string().expect("is string")
        } else {
            &result.to_string()
        };

        if source_string.is_empty() {
            self.output_count += 1;
            return Ok(vec![input]);
        }

        // Apply regex extraction
        let extracted_value = if let Some(captures) = self.regex.captures(source_string) {
            if let Some(group_index) = self.capture_group {
                // Extract specific capture group
                if let Some(capture) = captures.get(group_index) {
                    capture.as_str().to_string()
                } else {
                    // Capture group doesn't exist, return empty string
                    String::new()
                }
            } else {
                // Extract entire match (group 0)
                captures.get(0).map(|m| m.as_str().to_string()).unwrap_or_default()
            }
        } else {
            // No match found, return empty string
            String::new()
        };

        // Only add the field if we extracted something
        if !extracted_value.is_empty() {
            if let Some(event) = input.borrow_mut().as_object_mut() {
                event.insert(self.target_label.clone(), serde_json::Value::String(extracted_value));
            }
        }

        self.output_count += 1;
        Ok(vec![input])
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

    #[test]
    fn test_regex_extractor_basic() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "message".to_string(),
            "--pattern".to_string(),
            r"\d{4}-\d{2}-\d{2}".to_string(), // Date pattern
        ])?;

        let input_event = json!({
            "message": "Error occurred on 2023-12-25 at midnight",
            "level": "error"
        });
        let event = Rc::new(RefCell::new(input_event));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        assert_eq!(output_event["message"], "Error occurred on 2023-12-25 at midnight");
        assert_eq!(output_event["level"], "error");
        assert_eq!(output_event["regex_extract"], "2023-12-25");

        Ok(())
    }

    #[test]
    fn test_regex_extractor_custom_label() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "url".to_string(),
            "--pattern".to_string(),
            r"https?://([^/]+)".to_string(), // Extract domain from URL
            "--target-label".to_string(),
            "extracted_domain".to_string(),
            "--capture-group".to_string(),
            "1".to_string(),
        ])?;

        let input_event = json!({
            "url": "https://api.example.com/v1/users",
            "method": "GET"
        });
        let event = Rc::new(RefCell::new(input_event));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        assert_eq!(output_event["url"], "https://api.example.com/v1/users");
        assert_eq!(output_event["method"], "GET");
        assert_eq!(output_event["extracted_domain"], "api.example.com");

        Ok(())
    }

    #[test]
    fn test_regex_extractor_no_match() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "message".to_string(),
            "--pattern".to_string(),
            r"\d{4}-\d{2}-\d{2}".to_string(), // Date pattern
        ])?;

        let input_event = json!({
            "message": "No date in this message",
            "level": "info"
        });
        let event = Rc::new(RefCell::new(input_event.clone()));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        assert_eq!(output_event["message"], "No date in this message");
        assert_eq!(output_event["level"], "info");

        // Should not have regex_extract field when no match
        assert!(output_event.get("regex_extract").is_none());

        Ok(())
    }

    #[test]
    fn test_regex_extractor_jmespath_complex() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "request.headers.UserAgent".to_string(),
            "--pattern".to_string(),
            r"Mozilla/[\d.]+".to_string(), // Extract Mozilla version
            "--target-label".to_string(),
            "browser_info".to_string(),
        ])?;

        let input_event = json!({
            "request": {
                "headers": {
                    "UserAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/html"
                },
                "method": "GET"
            }
        });
        let event = Rc::new(RefCell::new(input_event));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        assert_eq!(output_event["browser_info"], "Mozilla/5.0");

        Ok(())
    }

    #[test]
    fn test_regex_extractor_capture_group_not_found() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "message".to_string(),
            "--pattern".to_string(),
            r"(\d{4})-(\d{2})-(\d{2})".to_string(), // Date with capture groups
            "--capture-group".to_string(),
            "5".to_string(), // Non-existent capture group
        ])?;

        let input_event = json!({
            "message": "Date: 2023-12-25"
        });
        let event = Rc::new(RefCell::new(input_event.clone()));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        // Should not have regex_extract field when capture group doesn't exist
        assert!(output_event.get("regex_extract").is_none());

        Ok(())
    }

    #[test]
    fn test_regex_extractor_missing_source_field() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "missing_field".to_string(),
            "--pattern".to_string(),
            r"\d+".to_string(),
        ])?;

        let input_event = json!({
            "other_field": "value"
        });
        let event = Rc::new(RefCell::new(input_event.clone()));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        assert_eq!(output_event["other_field"], "value");
        assert!(output_event.get("regex_extract").is_none());

        Ok(())
    }

    #[test]
    fn test_regex_extractor_null_source_field() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "message".to_string(),
            "--pattern".to_string(),
            r"\d+".to_string(),
        ])?;

        let input_event = json!({
            "message": null,
            "other_field": "value"
        });
        let event = Rc::new(RefCell::new(input_event.clone()));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        assert_eq!(output_event["message"], serde_json::Value::Null);
        assert_eq!(output_event["other_field"], "value");
        assert!(output_event.get("regex_extract").is_none());

        Ok(())
    }

    #[test]
    fn test_regex_extractor_empty_source_field() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "message".to_string(),
            "--pattern".to_string(),
            r"\d+".to_string(),
        ])?;

        let input_event = json!({
            "message": "",
            "other_field": "value"
        });
        let event = Rc::new(RefCell::new(input_event.clone()));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        assert_eq!(output_event["message"], "");
        assert_eq!(output_event["other_field"], "value");
        assert!(output_event.get("regex_extract").is_none());

        Ok(())
    }

    #[test]
    fn test_regex_extractor_non_string_source() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "count".to_string(),
            "--pattern".to_string(),
            r"\d+".to_string(),
        ])?;

        let input_event = json!({
            "count": 12345,
            "other_field": "value"
        });
        let event = Rc::new(RefCell::new(input_event));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        assert_eq!(output_event["count"], 12345);
        assert_eq!(output_event["regex_extract"], "12345");

        Ok(())
    }

    #[test]
    fn test_regex_extractor_multiple_capture_groups() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "timestamp".to_string(),
            "--pattern".to_string(),
            r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})".to_string(),
            "--capture-group".to_string(),
            "2".to_string(), // Extract month
            "--target-label".to_string(),
            "month".to_string(),
        ])?;

        let input_event = json!({
            "timestamp": "2023-12-25T14:30:45Z"
        });
        let event = Rc::new(RefCell::new(input_event));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        assert_eq!(output_event["month"], "12");

        Ok(())
    }

    #[test]
    fn test_regex_extractor_stats() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "message".to_string(),
            "--pattern".to_string(),
            r"\d+".to_string(),
        ])?;

        // Initial stats
        let stats = processor.stats().unwrap();
        assert!(stats.contains("input:0"));
        assert!(stats.contains("output:0"));

        // Process one event
        let input_event = json!({"message": "Number: 123"});
        let event = Rc::new(RefCell::new(input_event));
        processor.process(event)?;

        let stats = processor.stats().unwrap();
        assert!(stats.contains("input:1"));
        assert!(stats.contains("output:1"));

        Ok(())
    }

    #[test]
    fn test_regex_extractor_ip_address_extraction() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "log_line".to_string(),
            "--pattern".to_string(),
            r"\b(?:\d{1,3}\.){3}\d{1,3}\b".to_string(), // IP address pattern
            "--target-label".to_string(),
            "client_ip".to_string(),
        ])?;

        let input_event = json!({
            "log_line": "192.168.1.100 - - [25/Dec/2023:14:30:45 +0000] \"GET /api/users HTTP/1.1\" 200 1234"
        });
        let event = Rc::new(RefCell::new(input_event));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        assert_eq!(output_event["client_ip"], "192.168.1.100");

        Ok(())
    }

    #[test]
    fn test_regex_extractor_email_extraction() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "text".to_string(),
            "--pattern".to_string(),
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b".to_string(), // Email pattern
            "--target-label".to_string(),
            "email".to_string(),
        ])?;

        let input_event = json!({
            "text": "Please contact support at help@example.com for assistance."
        });
        let event = Rc::new(RefCell::new(input_event));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        assert_eq!(output_event["email"], "help@example.com");

        Ok(())
    }

    #[test]
    fn test_regex_extractor_url_path_extraction() -> Result<(), anyhow::Error> {
        let mut processor = RegexExtractorProcessor::new(&[
            "regex-extractor".to_string(),
            "request_url".to_string(),
            "--pattern".to_string(),
            r"https?://[^/]+(/[^\s?]*)?".to_string(), // URL path pattern
            "--capture-group".to_string(),
            "1".to_string(),
            "--target-label".to_string(),
            "url_path".to_string(),
        ])?;

        let input_event = json!({
            "request_url": "https://api.example.com/v1/users/123?include=profile"
        });
        let event = Rc::new(RefCell::new(input_event));

        let result = processor.process(event)?;
        assert_eq!(result.len(), 1);

        let output_event = result[0].borrow();
        assert_eq!(output_event["url_path"], "/v1/users/123");

        Ok(())
    }
}
