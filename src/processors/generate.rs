use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialSourceProcessorInit;
use rand::{distr::Alphanumeric, RngExt};
use rand_distr::{Distribution, LogNormal};
use std::cell::RefCell;
use std::rc::Rc;
use std::{thread, time::Duration};

enum GenValueType {
    Word,
    Number,
    Float,
    LogNormal,
}

const DEFAULT_GEN_TYPE: GenValueType = GenValueType::Number;

#[derive(SerialSourceProcessorInit)]
pub struct GenInputProcessor {
    labels: Vec<String>,
    value_type: Vec<GenValueType>,
    max_count: Option<u64>,
    output_count: u64,
    log_normal: LogNormal<f64>,
    sleep_ms: Option<u64>,
    batch_size: usize,
}

#[derive(Parser)]
/// Generate randomized input
#[command(version, long_about = None, arg_required_else_help(false))]
struct GenInputArgs {
    labels: Vec<String>,

    #[arg(short = 'c', long)]
    max_count: Option<u64>,

    #[arg(short, long)]
    sleep_ms: Option<u64>,

    #[arg(short, long, default_value_t = 1)]
    batch_size: usize,
}

impl GenInputProcessor {
    fn generate_event(&mut self) -> Result<Event, anyhow::Error> {
        let mut output = serde_json::Map::new();

        for i in 0..self.labels.len() {
            output.insert(
                self.labels[i].clone(),
                match self.value_type[i] {
                    GenValueType::Word => serde_json::to_value(
                        rand::rng()
                            .sample_iter(&Alphanumeric)
                            .take(8)
                            .map(char::from)
                            .collect::<String>(),
                    )?,
                    GenValueType::Number => serde_json::to_value(rand::rng().random::<u32>())?,
                    GenValueType::Float => serde_json::to_value(rand::rng().random::<f64>())?,
                    GenValueType::LogNormal => {
                        serde_json::to_value(self.log_normal.sample(&mut rand::rng()) as i64)?
                    }
                },
            );
        }
        let output_value: serde_json::Value = output.into();
        Ok(Rc::new(RefCell::new(output_value)))
    }
}
impl SerialSourceProcessor for GenInputProcessor {
    fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        if let Some(sleep_ms) = self.sleep_ms {
            thread::sleep(Duration::from_millis(sleep_ms));
        }

        if let Some(max) = self.max_count {
            if self.output_count >= max {
                return Ok(vec![]);
            }
        }

        let mut events = Vec::new();
        for _i in 0..self.batch_size {
            events.push(self.generate_event()?);
            self.output_count += 1;
        }
        Ok(events)
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("generate randomized input events".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let mut args = GenInputArgs::try_parse_from(argv)?;

        if args.labels.is_empty() {
            args.labels.push("key:word".to_string());
            args.labels.push("valueA".to_string());
            args.labels.push("valueB:log".to_string());
            args.labels.push("valueC:float".to_string());
        }

        let mut labels = Vec::new();
        let mut value_type = Vec::new();
        for arg_label in args.labels.iter() {
            let lsplit: Vec<&str> = arg_label.split(':').collect();

            labels.push(if lsplit.is_empty() || lsplit[0].is_empty() {
                "label".to_string() + labels.len().to_string().as_str()
            } else {
                lsplit[0].to_string()
            });
            value_type.push(if lsplit.len() > 1 {
                match lsplit[1].to_lowercase().as_str() {
                    "number" => GenValueType::Number,
                    "word" => GenValueType::Word,
                    "float" => GenValueType::Float,
                    "log" | "lognormal" => GenValueType::LogNormal,
                    _ => DEFAULT_GEN_TYPE,
                }
            } else {
                DEFAULT_GEN_TYPE
            });
        }

        let log_normal = LogNormal::new(2.0, 3.0)?;

        Ok(GenInputProcessor {
            labels,
            value_type,
            max_count: args.max_count,
            output_count: 0,
            log_normal,
            sleep_ms: args.sleep_ms,
            batch_size: args.batch_size,
        })
    }

    fn stats(&self) -> Option<String> {
        Some(format!("output:{}", self.output_count))
    }
}
