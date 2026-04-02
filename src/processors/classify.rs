use crate::processors::processor::*;
use anyhow::bail;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
enum ComparisonOperator {
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,
    Equal,
    NotEqual,
}

#[derive(Debug, Clone)]
enum RuleValue {
    Number(f64),
    String(String),
}

struct ClassificationRule {
    operator: ComparisonOperator,
    value: RuleValue,
    label: String,
}

#[derive(SerialProcessorInit)]
pub struct ClassifyProcessor<'a> {
    output_field: String,
    field_path: jmespath::Expression<'a>,
    rules: Vec<ClassificationRule>,
    default_label: Option<String>,
    input_count: u64,
    category_counts: HashMap<String, u64>,
    unclassified_count: u64,
}

#[derive(Parser)]
/// Add categorical labels to events based on classification rules
#[command(
    version,
    long_about = None,
    arg_required_else_help(true),
    after_help = r#"RULE FORMAT:

  "field_expression operator value -> label"

  Operators: >  >=  <  <=  ==  !=

  Values can be numeric (0.9, 100) or strings (active, 'quoted').
  Quoted strings ('value' or "value") have quotes stripped before comparison.

EVALUATION:

  Rules are evaluated in order. The first matching rule wins and its label
  is written to the output field. If no rule matches, --default is used.
  If no default is set, the output field is not added to the event.

  Numeric strings (e.g. "0.95") are coerced to numbers when compared
  against a numeric rule value.

EXAMPLES:

  Risk classification by score:
    classify risk -f score --rules "score > 0.9 -> critical" \
                           --rules "score > 0.7 -> high" \
                           --rules "score > 0.5 -> medium" \
                           --default low

  Status-based categorization:
    classify category -f status --rules "status == 'active' -> active" \
                                --rules "status == 'pending' -> pending" \
                                --default unknown

  Volume tier classification:
    classify tier -f count --rules "count >= 1000 -> enterprise" \
                           --rules "count >= 100 -> business" \
                           --default starter

INPUT:
  {"score": 0.95, "status": "active"}

OUTPUT (with risk example above):
  {"score": 0.95, "status": "active", "risk": "critical"}
"#,
)]
struct ClassifyArgs {
    /// Output field name for the classification label
    #[arg(required(true))]
    output_field: String,

    /// JMESPath expression for the field to evaluate
    #[arg(short = 'f', long)]
    field: String,

    /// Classification rules in format "condition -> label"
    #[arg(long = "rules")]
    rules: Vec<String>,

    /// Default label when no rules match
    #[arg(long)]
    default: Option<String>,
}

fn parse_operator(op: &str) -> Result<ComparisonOperator, anyhow::Error> {
    match op {
        ">" => Ok(ComparisonOperator::GreaterThan),
        ">=" => Ok(ComparisonOperator::GreaterOrEqual),
        "<" => Ok(ComparisonOperator::LessThan),
        "<=" => Ok(ComparisonOperator::LessOrEqual),
        "==" => Ok(ComparisonOperator::Equal),
        "!=" => Ok(ComparisonOperator::NotEqual),
        _ => bail!("Invalid operator '{}'. Valid operators: >, >=, <, <=, ==, !=", op),
    }
}

/// Strip surrounding quotes (single or double) from a string value
fn strip_quotes(s: &str) -> &str {
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

/// Parse a rule string in format "field_expression operator value -> label"
fn parse_rule(rule: &str) -> Result<ClassificationRule, anyhow::Error> {
    // Split on "->" to get condition and label
    let parts: Vec<&str> = rule.splitn(2, "->").collect();
    if parts.len() != 2 {
        bail!(
            "Invalid rule '{}': expected format 'field_expression operator value -> label'",
            rule
        );
    }

    let condition = parts[0].trim();
    let label = parts[1].trim().to_string();

    if label.is_empty() {
        bail!("Invalid rule '{}': label cannot be empty", rule);
    }

    // Parse the condition: "field_expression operator value"
    // We need to find the operator, which could be >=, <=, !=, ==, >, <
    // Try two-character operators first, then single-character
    let (operator_str, value_str) = if let Some(pos) = find_operator(condition) {
        pos
    } else {
        bail!("Invalid rule '{}': could not find a valid operator (>, >=, <, <=, ==, !=)", rule);
    };

    let operator = parse_operator(operator_str)?;
    let value_trimmed = value_str.trim();

    if value_trimmed.is_empty() {
        bail!("Invalid rule '{}': value cannot be empty", rule);
    }

    // Determine if value is numeric or string
    let stripped = strip_quotes(value_trimmed);
    let value = if stripped != value_trimmed {
        // Was quoted, treat as string
        RuleValue::String(stripped.to_string())
    } else if let Ok(num) = value_trimmed.parse::<f64>() {
        RuleValue::Number(num)
    } else {
        RuleValue::String(value_trimmed.to_string())
    };

    Ok(ClassificationRule { operator, value, label })
}

/// Find the operator in a condition string, returning (operator, remaining_value)
fn find_operator(condition: &str) -> Option<(&str, &str)> {
    // Try two-character operators first: >=, <=, ==, !=
    for op in &[">=", "<=", "==", "!="] {
        if let Some(pos) = condition.find(op) {
            let value = &condition[pos + op.len()..];
            return Some((op, value));
        }
    }
    // Try single-character operators: >, <
    for op in &[">", "<"] {
        if let Some(pos) = condition.find(op) {
            let value = &condition[pos + op.len()..];
            return Some((op, value));
        }
    }
    None
}

impl ClassifyProcessor<'_> {
    fn compare_numbers(&self, field_val: f64, rule_val: f64, op: &ComparisonOperator) -> bool {
        match op {
            ComparisonOperator::GreaterThan => field_val > rule_val,
            ComparisonOperator::GreaterOrEqual => field_val >= rule_val,
            ComparisonOperator::LessThan => field_val < rule_val,
            ComparisonOperator::LessOrEqual => field_val <= rule_val,
            ComparisonOperator::Equal => (field_val - rule_val).abs() < f64::EPSILON,
            ComparisonOperator::NotEqual => (field_val - rule_val).abs() >= f64::EPSILON,
        }
    }

    fn compare_strings(&self, field_val: &str, rule_val: &str, op: &ComparisonOperator) -> bool {
        match op {
            ComparisonOperator::Equal => field_val == rule_val,
            ComparisonOperator::NotEqual => field_val != rule_val,
            ComparisonOperator::GreaterThan => field_val > rule_val,
            ComparisonOperator::GreaterOrEqual => field_val >= rule_val,
            ComparisonOperator::LessThan => field_val < rule_val,
            ComparisonOperator::LessOrEqual => field_val <= rule_val,
        }
    }

    fn evaluate_rule(&self, field_value: &serde_json::Value, rule: &ClassificationRule) -> bool {
        match (&rule.value, field_value) {
            // Numeric rule value vs JSON number
            (RuleValue::Number(rule_num), serde_json::Value::Number(n)) => {
                if let Some(field_num) = n.as_f64() {
                    self.compare_numbers(field_num, *rule_num, &rule.operator)
                } else {
                    false
                }
            }
            // Numeric rule value vs JSON string (numeric string coercion)
            (RuleValue::Number(rule_num), serde_json::Value::String(s)) => {
                if let Ok(field_num) = s.parse::<f64>() {
                    self.compare_numbers(field_num, *rule_num, &rule.operator)
                } else {
                    false
                }
            }
            // String rule value vs JSON string
            (RuleValue::String(rule_str), serde_json::Value::String(field_str)) => {
                self.compare_strings(field_str, rule_str, &rule.operator)
            }
            // Type mismatch - rule does not match
            _ => false,
        }
    }
}

impl SerialProcessor for ClassifyProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("add categorical labels based on rules".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = ClassifyArgs::try_parse_from(argv)?;
        let field_path = jmespath::compile(&args.field)?;

        let mut rules = Vec::new();
        for rule_str in &args.rules {
            rules.push(parse_rule(rule_str)?);
        }

        Ok(Self {
            output_field: args.output_field,
            field_path,
            rules,
            default_label: args.default,
            input_count: 0,
            category_counts: HashMap::new(),
            unclassified_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        // Extract field value using JMESPath
        let result = self.field_path.search(&input)?;

        let mut classified = false;

        if !result.is_null() {
            // Convert JMESPath result to serde_json::Value for rule evaluation
            let field_value: serde_json::Value = serde_json::to_value(&*result)?;

            // Evaluate rules in order — first match wins
            for rule in &self.rules {
                if self.evaluate_rule(&field_value, rule) {
                    // Assign the matching label to the output field
                    if let Some(event_obj) = input.borrow_mut().as_object_mut() {
                        event_obj.insert(
                            self.output_field.clone(),
                            serde_json::Value::String(rule.label.clone()),
                        );
                    }
                    *self.category_counts.entry(rule.label.clone()).or_insert(0) += 1;
                    classified = true;
                    break;
                }
            }
        }

        if !classified {
            if let Some(ref default) = self.default_label {
                // Apply default label
                if let Some(event_obj) = input.borrow_mut().as_object_mut() {
                    event_obj.insert(
                        self.output_field.clone(),
                        serde_json::Value::String(default.clone()),
                    );
                }
                *self.category_counts.entry(default.clone()).or_insert(0) += 1;
            } else {
                // No match, no default — don't add the output field
                self.unclassified_count += 1;
            }
        }

        // Always output the event (streaming behavior)
        Ok(vec![input])
    }

    fn stats(&self) -> Option<String> {
        let mut lines = vec![format!("input:{}", self.input_count)];
        let mut sorted_categories: Vec<_> = self.category_counts.iter().collect();
        sorted_categories.sort_by_key(|(label, _)| (*label).clone());
        for (label, count) in sorted_categories {
            lines.push(format!("{}:{}", label, count));
        }
        lines.push(format!("unclassified:{}", self.unclassified_count));
        Some(lines.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rule_numeric() {
        let rule = parse_rule("score > 0.9 -> critical").unwrap();
        assert_eq!(rule.operator, ComparisonOperator::GreaterThan);
        assert!(matches!(rule.value, RuleValue::Number(n) if (n - 0.9).abs() < f64::EPSILON));
        assert_eq!(rule.label, "critical");
    }

    #[test]
    fn test_parse_rule_string_quoted() {
        let rule = parse_rule("status == 'active' -> active").unwrap();
        assert_eq!(rule.operator, ComparisonOperator::Equal);
        assert!(matches!(&rule.value, RuleValue::String(s) if s == "active"));
        assert_eq!(rule.label, "active");
    }

    #[test]
    fn test_parse_rule_string_double_quoted() {
        let rule = parse_rule("status == \"pending\" -> waiting").unwrap();
        assert_eq!(rule.operator, ComparisonOperator::Equal);
        assert!(matches!(&rule.value, RuleValue::String(s) if s == "pending"));
        assert_eq!(rule.label, "waiting");
    }

    #[test]
    fn test_parse_rule_string_unquoted() {
        let rule = parse_rule("status != error -> ok").unwrap();
        assert_eq!(rule.operator, ComparisonOperator::NotEqual);
        assert!(matches!(&rule.value, RuleValue::String(s) if s == "error"));
        assert_eq!(rule.label, "ok");
    }

    #[test]
    fn test_parse_rule_all_operators() {
        let ops = vec![
            (">", ComparisonOperator::GreaterThan),
            (">=", ComparisonOperator::GreaterOrEqual),
            ("<", ComparisonOperator::LessThan),
            ("<=", ComparisonOperator::LessOrEqual),
            ("==", ComparisonOperator::Equal),
            ("!=", ComparisonOperator::NotEqual),
        ];
        for (op_str, expected_op) in ops {
            let rule_str = format!("score {} 5 -> label", op_str);
            let rule = parse_rule(&rule_str).unwrap();
            assert_eq!(rule.operator, expected_op, "Failed for operator {}", op_str);
        }
    }

    #[test]
    fn test_parse_rule_whitespace_tolerance() {
        let rule = parse_rule("  score  >  0.9  ->  critical  ").unwrap();
        assert_eq!(rule.operator, ComparisonOperator::GreaterThan);
        assert!(matches!(rule.value, RuleValue::Number(n) if (n - 0.9).abs() < f64::EPSILON));
        assert_eq!(rule.label, "critical");
    }

    #[test]
    fn test_parse_rule_invalid_no_arrow() {
        assert!(parse_rule("score > 0.9").is_err());
    }

    #[test]
    fn test_parse_rule_invalid_no_operator() {
        assert!(parse_rule("score 0.9 -> label").is_err());
    }

    #[test]
    fn test_parse_rule_invalid_empty_label() {
        assert!(parse_rule("score > 0.9 ->  ").is_err());
    }

    #[test]
    fn test_parse_rule_negative_number() {
        let rule = parse_rule("score > -1.5 -> negative").unwrap();
        assert!(matches!(rule.value, RuleValue::Number(n) if (n - (-1.5)).abs() < f64::EPSILON));
    }

    #[test]
    fn test_classify_processor_new() {
        let args = shlex::split(
            "classify risk -f score --rules \"score > 0.9 -> critical\" --rules \"score > 0.5 -> medium\" --default low"
        ).unwrap();
        let processor = ClassifyProcessor::new(&args);
        assert!(processor.is_ok());
        let p = processor.unwrap();
        assert_eq!(p.output_field, "risk");
        assert_eq!(p.rules.len(), 2);
        assert_eq!(p.default_label, Some("low".to_string()));
    }

    #[test]
    fn test_classify_processor_invalid_jmespath() {
        let args =
            shlex::split("classify risk -f \"[invalid\" --rules \"score > 0.9 -> critical\"")
                .unwrap();
        assert!(ClassifyProcessor::new(&args).is_err());
    }

    #[test]
    fn test_classify_processor_invalid_rule() {
        let args = shlex::split("classify risk -f score --rules \"bad rule no arrow\"").unwrap();
        assert!(ClassifyProcessor::new(&args).is_err());
    }

    // Helper to create a test event from a JSON value
    fn make_event(val: serde_json::Value) -> Event {
        event_new(val)
    }

    // Helper to create a ClassifyProcessor with given args string
    fn make_processor(args_str: &str) -> ClassifyProcessor<'static> {
        let args = shlex::split(args_str).unwrap();
        ClassifyProcessor::new(&args).unwrap()
    }

    // --- 7.1 Basic functionality tests ---

    #[test]
    fn test_single_rule_matching() {
        let mut p = make_processor("classify risk -f score --rules \"score > 0.9 -> critical\"");
        let event = make_event(serde_json::json!({"score": 0.95}));
        let result = p.process(event).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].borrow()["risk"], "critical");
    }

    #[test]
    fn test_multiple_rules_first_match_wins() {
        let mut p = make_processor(
            "classify risk -f score --rules \"score > 0.9 -> critical\" --rules \"score > 0.5 -> medium\""
        );
        // 0.95 matches both rules, but first-match-wins should give "critical"
        let event = make_event(serde_json::json!({"score": 0.95}));
        let result = p.process(event).unwrap();
        assert_eq!(result[0].borrow()["risk"], "critical");

        // 0.7 matches only the second rule
        let event2 = make_event(serde_json::json!({"score": 0.7}));
        let result2 = p.process(event2).unwrap();
        assert_eq!(result2[0].borrow()["risk"], "medium");
    }

    #[test]
    fn test_default_label_assignment() {
        let mut p = make_processor(
            "classify risk -f score --rules \"score > 0.9 -> critical\" --default low",
        );
        let event = make_event(serde_json::json!({"score": 0.1}));
        let result = p.process(event).unwrap();
        assert_eq!(result[0].borrow()["risk"], "low");
    }

    #[test]
    fn test_no_classification_no_match_no_default() {
        let mut p = make_processor("classify risk -f score --rules \"score > 0.9 -> critical\"");
        let event = make_event(serde_json::json!({"score": 0.1}));
        let result = p.process(event).unwrap();
        assert_eq!(result.len(), 1);
        // Output field should not exist
        assert!(result[0].borrow().get("risk").is_none());
    }

    // --- 7.2 Edge case tests ---

    #[test]
    fn test_missing_field_jmespath_returns_null() {
        let mut p = make_processor(
            "classify risk -f score --rules \"score > 0.9 -> critical\" --default unknown",
        );
        // Event has no "score" field
        let event = make_event(serde_json::json!({"other": 42}));
        let result = p.process(event).unwrap();
        assert_eq!(result.len(), 1);
        // Should get default since field is missing
        assert_eq!(result[0].borrow()["risk"], "unknown");
    }

    #[test]
    fn test_empty_rules_with_default() {
        let mut p = make_processor("classify risk -f score --default fallback");
        let event = make_event(serde_json::json!({"score": 0.5}));
        let result = p.process(event).unwrap();
        assert_eq!(result[0].borrow()["risk"], "fallback");
    }

    #[test]
    fn test_numeric_string_compared_to_number() {
        let mut p = make_processor(
            "classify risk -f score --rules \"score > 0.9 -> critical\" --default low",
        );
        // Field value is a string "0.95" but rule value is numeric 0.9
        let event = make_event(serde_json::json!({"score": "0.95"}));
        let result = p.process(event).unwrap();
        assert_eq!(result[0].borrow()["risk"], "critical");
    }

    #[test]
    fn test_special_characters_in_labels() {
        let mut p = make_processor(
            "classify category -f score --rules \"score > 0.5 -> high-risk_level.1\"",
        );
        let event = make_event(serde_json::json!({"score": 0.8}));
        let result = p.process(event).unwrap();
        assert_eq!(result[0].borrow()["category"], "high-risk_level.1");
    }

    // --- 7.3 Error handling tests ---

    #[test]
    fn test_invalid_jmespath_returns_error() {
        let args =
            shlex::split("classify risk -f \"[invalid\" --rules \"score > 0.9 -> critical\"")
                .unwrap();
        let result = ClassifyProcessor::new(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_rule_syntax_returns_descriptive_error() {
        let args = shlex::split("classify risk -f score --rules \"no arrow here\"").unwrap();
        let result = ClassifyProcessor::new(&args);
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(
            err_msg.contains("Invalid rule"),
            "Error should mention 'Invalid rule', got: {}",
            err_msg
        );
    }
}
