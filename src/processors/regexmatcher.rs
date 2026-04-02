use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use regex::Regex;

#[derive(SerialProcessorInit)]
pub struct RegexMatcherProcessor<'a> {
    source_path: jmespath::Expression<'a>,
    patterns: Vec<Regex>,
    input_count: u64,
    output_count: u64,
    match_count: u64,
}

#[derive(Parser)]
/// Filter events by matching a JMESPath field against one or more regex patterns
///
/// This processor evaluates a field selected via JMESPath against one or more regex patterns.
/// If the field matches any of the provided patterns, the event is passed through as output.
/// If no patterns match, the event is filtered out (no output).
///
/// Examples:
///   Pass events where message contains an error code:
///     regex-matcher message -p 'ERR-\d{4}'
///
///   Pass events with multiple pattern options:
///     regex-matcher status -p 'ERROR' -p 'CRITICAL' -p 'FATAL'
///
///   Pass events from nested field matching IP pattern:
///     regex-matcher 'request.client_ip' -p '\b(?:\d{1,3}\.){3}\d{1,3}\b'
#[command(version, arg_required_else_help(true))]
struct RegexMatcherArgs {
    /// JMESPath expression to select the source field from the event
    #[arg(required(true))]
    source_path: Vec<String>,

    /// Regular expression pattern(s) to match against the source string.
    /// If multiple patterns are provided, the event passes if ANY pattern matches
    #[arg(short, long, required(true))]
    pattern: Vec<String>,
}

impl SerialProcessor for RegexMatcherProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("filter events by matching JMESPath field against regex patterns".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = RegexMatcherArgs::try_parse_from(argv)?;
        let source_path = jmespath::compile(&args.source_path.join(" "))?;

        let patterns: Result<Vec<Regex>, regex::Error> =
            args.pattern.iter().map(|p| Regex::new(p)).collect();
        let patterns = patterns?;

        Ok(Self { source_path, patterns, input_count: 0, output_count: 0, match_count: 0 })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        // Extract the source field using JMESPath
        let result = self.source_path.search(&input)?;

        if result.is_null() {
            return Ok(vec![]);
        }

        let source_string = if result.is_string() {
            result.as_string().expect("is string").to_string()
        } else {
            result.to_string()
        };

        if source_string.is_empty() {
            return Ok(vec![]);
        }

        // Check if any pattern matches
        let matches = self.patterns.iter().any(|pattern| pattern.is_match(&source_string));

        if matches {
            self.match_count += 1;
            self.output_count += 1;
            Ok(vec![input])
        } else {
            Ok(vec![])
        }
    }

    fn stats(&self) -> Option<String> {
        Some(format!(
            "input:{}\noutput:{}\nmatch:{}",
            self.input_count, self.output_count, self.match_count
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn test_process(
        source_path: &str,
        patterns: Vec<&str>,
        event: serde_json::Value,
    ) -> Result<(usize, u64), anyhow::Error> {
        let mut args = vec!["regex-matcher".to_string(), source_path.to_string()];
        for pattern in patterns {
            args.push("--pattern".to_string());
            args.push(pattern.to_string());
        }

        let mut processor = RegexMatcherProcessor::new(&args)?;
        let result = processor.process(Rc::new(RefCell::new(event)))?;
        Ok((result.len(), processor.match_count))
    }

    #[test]
    fn test_single_pattern_match() -> Result<(), anyhow::Error> {
        let (output_len, match_count) = test_process(
            "message",
            vec![r"ERR-\d{4}"],
            json!({"message": "Error code ERR-5001 occurred"}),
        )?;
        assert_eq!(output_len, 1);
        assert_eq!(match_count, 1);
        Ok(())
    }

    #[test]
    fn test_single_pattern_no_match() -> Result<(), anyhow::Error> {
        let (output_len, match_count) =
            test_process("message", vec![r"ERR-\d{4}"], json!({"message": "Everything is fine"}))?;
        assert_eq!(output_len, 0);
        assert_eq!(match_count, 0);
        Ok(())
    }

    #[test]
    fn test_multiple_patterns_any_match() -> Result<(), anyhow::Error> {
        let (output_len, match_count) = test_process(
            "status",
            vec!["ERROR", "CRITICAL", "FATAL"],
            json!({"status": "CRITICAL"}),
        )?;
        assert_eq!(output_len, 1);
        assert_eq!(match_count, 1);
        Ok(())
    }

    #[test]
    fn test_multiple_patterns_none_match() -> Result<(), anyhow::Error> {
        let (output_len, match_count) = test_process(
            "status",
            vec!["ERROR", "CRITICAL", "FATAL"],
            json!({"status": "WARNING"}),
        )?;
        assert_eq!(output_len, 0);
        assert_eq!(match_count, 0);
        Ok(())
    }

    #[test]
    fn test_nested_jmespath_match() -> Result<(), anyhow::Error> {
        let (output_len, match_count) = test_process(
            "request.headers.UserAgent",
            vec![r"Mozilla/[\d.]+"],
            json!({
                "request": {
                    "headers": {
                        "UserAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                    }
                }
            }),
        )?;
        assert_eq!(output_len, 1);
        assert_eq!(match_count, 1);
        Ok(())
    }

    #[test]
    fn test_nested_jmespath_no_match() -> Result<(), anyhow::Error> {
        let (output_len, match_count) = test_process(
            "request.headers.UserAgent",
            vec![r"Chrome/[\d.]+"],
            json!({
                "request": {
                    "headers": {
                        "UserAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                    }
                }
            }),
        )?;
        assert_eq!(output_len, 0);
        assert_eq!(match_count, 0);
        Ok(())
    }

    #[test]
    fn test_missing_field() -> Result<(), anyhow::Error> {
        let (output_len, match_count) =
            test_process("missing_field", vec![r"\d+"], json!({"other_field": "value"}))?;
        assert_eq!(output_len, 0);
        assert_eq!(match_count, 0);
        Ok(())
    }

    #[test]
    fn test_null_field() -> Result<(), anyhow::Error> {
        let (output_len, match_count) =
            test_process("message", vec![r"\d+"], json!({"message": null}))?;
        assert_eq!(output_len, 0);
        assert_eq!(match_count, 0);
        Ok(())
    }

    #[test]
    fn test_empty_field() -> Result<(), anyhow::Error> {
        let (output_len, match_count) =
            test_process("message", vec![r"\d+"], json!({"message": ""}))?;
        assert_eq!(output_len, 0);
        assert_eq!(match_count, 0);
        Ok(())
    }

    #[test]
    fn test_non_string_field() -> Result<(), anyhow::Error> {
        let (output_len, match_count) =
            test_process("count", vec![r"123\d+"], json!({"count": 12345}))?;
        assert_eq!(output_len, 1);
        assert_eq!(match_count, 1);
        Ok(())
    }

    #[test]
    fn test_case_sensitive() -> Result<(), anyhow::Error> {
        let (output_len, match_count) =
            test_process("status", vec!["ERROR"], json!({"status": "error"}))?;
        assert_eq!(output_len, 0);
        assert_eq!(match_count, 0);
        Ok(())
    }

    #[test]
    fn test_partial_match() -> Result<(), anyhow::Error> {
        let (output_len, match_count) = test_process(
            "message",
            vec!["error"],
            json!({"message": "An error occurred in the system"}),
        )?;
        assert_eq!(output_len, 1);
        assert_eq!(match_count, 1);
        Ok(())
    }

    #[test]
    fn test_complex_pattern() -> Result<(), anyhow::Error> {
        let (output_len, match_count) = test_process(
            "timestamp",
            vec![r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"],
            json!({"timestamp": "2023-12-25T14:30:45Z"}),
        )?;
        assert_eq!(output_len, 1);
        assert_eq!(match_count, 1);
        Ok(())
    }
}
