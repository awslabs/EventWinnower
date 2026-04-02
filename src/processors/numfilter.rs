use crate::processors::processor::*;
use anyhow::bail;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;

enum ComparisonOperator {
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,
    Equal,
    NotEqual,
    Between,
    Outside,
}

#[derive(SerialProcessorInit)]
pub struct NumFilterProcessor<'a> {
    path: jmespath::Expression<'a>,
    operator: ComparisonOperator,
    value: f64,
    value2: Option<f64>,
    not: bool,
    input_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// Filter events based on numeric comparisons using positional syntax
#[command(version, long_about = None, arg_required_else_help(true))]
struct NumFilterArgs {
    /// JMESPath expression for field extraction
    #[arg(required(true))]
    field: String,

    /// Comparison operator: >, >=, <, <=, ==, !=, between, outside
    #[arg(required(true))]
    operator: String,

    /// First comparison value (or min for between/outside)
    #[arg(required(true))]
    value: f64,

    /// Second value (max for between/outside)
    value2: Option<f64>,

    /// Negate the comparison result
    #[arg(short, long)]
    not: bool,
}

fn parse_operator(op: &str) -> Result<ComparisonOperator, anyhow::Error> {
    match op {
        ">" => Ok(ComparisonOperator::GreaterThan),
        ">=" => Ok(ComparisonOperator::GreaterOrEqual),
        "<" => Ok(ComparisonOperator::LessThan),
        "<=" => Ok(ComparisonOperator::LessOrEqual),
        "==" => Ok(ComparisonOperator::Equal),
        "!=" => Ok(ComparisonOperator::NotEqual),
        "between" => Ok(ComparisonOperator::Between),
        "outside" => Ok(ComparisonOperator::Outside),
        _ => bail!(
            "Invalid operator '{}'. Valid operators: >, >=, <, <=, ==, !=, between, outside",
            op
        ),
    }
}

impl SerialProcessor for NumFilterProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("filter events based on numeric comparisons".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = NumFilterArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.field)?;
        let operator = parse_operator(&args.operator)?;

        // Validate that between/outside have a second value
        if matches!(operator, ComparisonOperator::Between | ComparisonOperator::Outside)
            && args.value2.is_none()
        {
            bail!(
                "The '{}' operator requires two values: numfilter <field> {} <min> <max>",
                args.operator,
                args.operator
            );
        }

        Ok(Self {
            path,
            operator,
            value: args.value,
            value2: args.value2,
            not: args.not,
            input_count: 0,
            output_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;
        let result = self.path.search(&input)?;

        if result.is_null() {
            if self.not {
                self.output_count += 1;
                return Ok(vec![input]);
            }
            return Ok(vec![]);
        }

        // Try to extract numeric value
        let num = if let Some(n) = result.as_number() {
            n
        } else if let Some(s) = result.as_string() {
            match s.parse::<f64>() {
                Ok(n) => n,
                Err(_) => {
                    if self.not {
                        self.output_count += 1;
                        return Ok(vec![input]);
                    }
                    return Ok(vec![]);
                }
            }
        } else {
            if self.not {
                self.output_count += 1;
                return Ok(vec![input]);
            }
            return Ok(vec![]);
        };

        let comp = match self.operator {
            ComparisonOperator::GreaterThan => num > self.value,
            ComparisonOperator::GreaterOrEqual => num >= self.value,
            ComparisonOperator::LessThan => num < self.value,
            ComparisonOperator::LessOrEqual => num <= self.value,
            ComparisonOperator::Equal => (num - self.value).abs() < f64::EPSILON,
            ComparisonOperator::NotEqual => (num - self.value).abs() >= f64::EPSILON,
            ComparisonOperator::Between => {
                let max = self.value2.unwrap();
                num >= self.value && num <= max
            }
            ComparisonOperator::Outside => {
                let max = self.value2.unwrap();
                num < self.value || num > max
            }
        };

        let pass = if self.not { !comp } else { comp };

        if pass {
            self.output_count += 1;
            Ok(vec![input])
        } else {
            Ok(vec![])
        }
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
    fn numfilter_greater_than() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc score > 0.9").unwrap();
        let mut processor = NumFilterProcessor::new(&args)?;
        let event = Rc::new(RefCell::new(json!({"score": 0.95})));
        assert_eq!(processor.process(event)?.len(), 1);

        let event = Rc::new(RefCell::new(json!({"score": 0.5})));
        assert_eq!(processor.process(event)?.len(), 0);
        Ok(())
    }

    #[test]
    fn numfilter_between() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc score between 0.5 0.9").unwrap();
        let mut processor = NumFilterProcessor::new(&args)?;

        let event = Rc::new(RefCell::new(json!({"score": 0.7})));
        assert_eq!(processor.process(event)?.len(), 1);

        // Boundary inclusive
        let event = Rc::new(RefCell::new(json!({"score": 0.5})));
        assert_eq!(processor.process(event)?.len(), 1);

        let event = Rc::new(RefCell::new(json!({"score": 0.3})));
        assert_eq!(processor.process(event)?.len(), 0);
        Ok(())
    }

    #[test]
    fn numfilter_invalid_operator() {
        let args = shlex::split("proc score badop 5").unwrap();
        assert!(NumFilterProcessor::new(&args).is_err());
    }

    #[test]
    fn numfilter_between_missing_value2() {
        let args = shlex::split("proc score between 5").unwrap();
        assert!(NumFilterProcessor::new(&args).is_err());
    }

    #[test]
    fn numfilter_stats() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc score > 5").unwrap();
        let mut processor = NumFilterProcessor::new(&args)?;

        let event = Rc::new(RefCell::new(json!({"score": 10})));
        processor.process(event)?;
        let event = Rc::new(RefCell::new(json!({"score": 3})));
        processor.process(event)?;

        assert_eq!(processor.stats(), Some("input:2\noutput:1".to_string()));
        Ok(())
    }

    #[test]
    fn numfilter_not_inverts_comparison() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc score -n > 5").unwrap();
        let mut processor = NumFilterProcessor::new(&args)?;

        // Would normally pass (10 > 5), but --not inverts it
        let event = Rc::new(RefCell::new(json!({"score": 10})));
        assert_eq!(processor.process(event)?.len(), 0);

        // Would normally fail (3 > 5), but --not inverts it
        let event = Rc::new(RefCell::new(json!({"score": 3})));
        assert_eq!(processor.process(event)?.len(), 1);

        // Missing field: --not passes it through
        let event = Rc::new(RefCell::new(json!({"other": 1})));
        assert_eq!(processor.process(event)?.len(), 1);
        Ok(())
    }
}
