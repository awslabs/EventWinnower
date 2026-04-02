use crate::processors::processor::*;
use crate::processors::Event;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    // Regex patterns for complete URL matching and replacement
    static ref HTTP_URL_PATTERN: Regex = Regex::new(r"http://([^/\s'<>?#]+)").unwrap();
    static ref HTTPS_URL_PATTERN: Regex = Regex::new(r"https://([^/\s'<>?#]+)").unwrap();
    static ref FTP_URL_PATTERN: Regex = Regex::new(r"ftp://([^/\s'<>?#]+)").unwrap();
}

/// Defangs URLs in the input string by replacing protocols and dots in domains
///
/// This function performs the following transformations:
/// - http:// -> hxxp://
/// - https:// -> hxxps://
/// - ftp:// -> fxp://
/// - Replaces dots (.) with [.] in domain portions only for defanged protocols
///
/// # Arguments
/// * `input` - The input string that may contain URLs
///
/// # Returns
/// A string with all URLs defanged
///
/// # Examples
/// ```
/// use eventwinnower::processors::defanger::defang_urls;
///
/// let input = r#"{"url": "https://example.com/path"}"#;
/// let result = defang_urls(input);
/// assert_eq!(result, r#"{"url": "hxxps://example[.]com/path"}"#);
/// ```
pub fn defang_urls(input: &str) -> String {
    let mut result = input.to_string();

    // Replace HTTP URLs - only lowercase protocols are matched
    result = HTTP_URL_PATTERN
        .replace_all(&result, |caps: &regex::Captures| {
            let domain = &caps[1];
            let defanged_domain = domain.replace('.', "[.]");
            format!("hxxp://{defanged_domain}")
        })
        .to_string();

    // Replace HTTPS URLs - only lowercase protocols are matched
    result = HTTPS_URL_PATTERN
        .replace_all(&result, |caps: &regex::Captures| {
            let domain = &caps[1];
            let defanged_domain = domain.replace('.', "[.]");
            format!("hxxps://{defanged_domain}")
        })
        .to_string();

    // Replace FTP URLs - only lowercase protocols are matched
    result = FTP_URL_PATTERN
        .replace_all(&result, |caps: &regex::Captures| {
            let domain = &caps[1];
            let defanged_domain = domain.replace('.', "[.]");
            format!("fxp://{defanged_domain}")
        })
        .to_string();

    result
}

#[derive(Debug, SerialProcessorInit)]
pub struct DefangProcessor<'a> {
    path: jmespath::Expression<'a>,
    label: String,
    input_count: u64,
}

#[derive(Parser)]
/// defang URL strings for use in displays
#[command(version, long_about = None, arg_required_else_help(true))]
struct DefangArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long, default_value = "defang")]
    label: String,
}

impl SerialProcessor for DefangProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("defang URL strings for use in displays".to_string())
    }
    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = DefangArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        Ok(Self { path, label: args.label, input_count: 0 })
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

        let outstring = defang_urls(&target_string);

        // Add decoded content to event with specified label
        if let Some(event) = input.borrow_mut().as_object_mut() {
            event.insert(self.label.clone(), serde_json::Value::String(outstring));
        }
        // Handle decoding failures gracefully by passing through original events
        Ok(vec![input])
    }

    fn stats(&self) -> Option<String> {
        Some(format!("events:{}\n", self.input_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defang_http_url() {
        let input = r#"{"url": "http://example.com/path"}"#;
        let expected = r#"{"url": "hxxp://example[.]com/path"}"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_defang_https_url() {
        let input = r#"{"url": "https://example.com/path"}"#;
        let expected = r#"{"url": "hxxps://example[.]com/path"}"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_defang_ftp_url() {
        let input = r#"{"url": "ftp://files.example.com/file.txt"}"#;
        let expected = r#"{"url": "fxp://files[.]example[.]com/file.txt"}"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_defang_url_with_port() {
        let input = r#"{"url": "https://example.com:8080/path"}"#;
        let expected = r#"{"url": "hxxps://example[.]com:8080/path"}"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_defang_url_with_path_and_params() {
        let input = r#"{"url": "https://example.com:8080/path?param=value&other=test"}"#;
        let expected = r#"{"url": "hxxps://example[.]com:8080/path?param=value&other=test"}"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_defang_url_with_fragment() {
        let input = r#"{"url": "https://example.com/path#section"}"#;
        let expected = r#"{"url": "hxxps://example[.]com/path#section"}"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_no_urls_unchanged() {
        let input = r#"{"message": "no urls here", "count": 42}"#;
        assert_eq!(defang_urls(input), input);
    }

    #[test]
    fn test_multiple_urls() {
        let input = r#"{"urls": ["http://site1.com", "https://site2.org", "ftp://files.net"]}"#;
        let expected =
            r#"{"urls": ["hxxp://site1[.]com", "hxxps://site2[.]org", "fxp://files[.]net"]}"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_mixed_content_with_urls() {
        let input = r#"{"message": "Check out https://example.com and http://test.org for more info", "id": 123}"#;
        let expected = r#"{"message": "Check out hxxps://example[.]com and hxxp://test[.]org for more info", "id": 123}"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_subdomain_urls() {
        let input = r#"{"url": "https://api.v2.example.com/endpoint"}"#;
        let expected = r#"{"url": "hxxps://api[.]v2[.]example[.]com/endpoint"}"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_url_in_quotes() {
        let input = r#"Visit "https://secure.example.com" for details"#;
        let expected = r#"Visit "hxxps://secure[.]example[.]com" for details"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_url_with_username_password() {
        let input = r#"{"url": "https://user:pass@example.com/path"}"#;
        let expected = r#"{"url": "hxxps://user:pass@example[.]com/path"}"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_preserve_dots_in_paths() {
        let input = r#"{"url": "https://example.com/file.txt?param=value.json"}"#;
        let expected = r#"{"url": "hxxps://example[.]com/file.txt?param=value.json"}"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_empty_string() {
        let input = "";
        assert_eq!(defang_urls(input), input);
    }

    #[test]
    fn test_malformed_urls_unchanged() {
        let input = r#"{"text": "http:// and https:// without domains"}"#;
        // These should remain unchanged since they don't have valid domains
        assert_eq!(defang_urls(input), input);
    }

    #[test]
    fn test_case_sensitivity() {
        let input = r#"{"url": "HTTP://EXAMPLE.COM/PATH"}"#;
        // Should not match uppercase protocols (following typical URL defanging practices)
        assert_eq!(defang_urls(input), input);
    }

    #[test]
    fn test_ip_address_urls() {
        let input = r#"{"url": "https://192.168.1.1:8080/api"}"#;
        let expected = r#"{"url": "hxxps://192[.]168[.]1[.]1:8080/api"}"#;
        assert_eq!(defang_urls(input), expected);
    }

    #[test]
    fn test_localhost_urls() {
        let input = r#"{"url": "http://localhost:3000/app"}"#;
        let expected = r#"{"url": "hxxp://localhost:3000/app"}"#;
        assert_eq!(defang_urls(input), expected);
    }
}
