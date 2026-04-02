use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialSourceProcessorInit;
use std::cell::RefCell;
use std::io::{self};
use std::rc::Rc;

#[derive(SerialSourceProcessorInit)]
pub struct CsvInputProcessor {
    reader: csv::Reader<io::StdinLock<'static>>,
    lines: u64,
    labels: Vec<String>,
}

#[derive(Parser)]
/// read in CSV from stdin
#[command(version, long_about = None, arg_required_else_help(false))]
struct CsvInputArgs {
    labels: Vec<String>,

    #[arg(short, long, default_value_t = b',')]
    delim: u8,

    #[arg(long)]
    disable_double_quote: bool,

    #[arg(short, long)]
    flexible_fields: bool,

    #[arg(long)]
    disable_quote: bool,

    #[arg(short, long)]
    trim_spaces: bool,

    #[arg(long)]
    buffer_capacity: Option<usize>,
}

impl SerialSourceProcessor for CsvInputProcessor {
    fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        let mut record = csv::StringRecord::new();

        if self.reader.read_record(&mut record)? {
            let mut output = serde_json::Map::new();

            if record.len() > self.labels.len() {
                for i in self.labels.len()..record.len() {
                    self.labels.push("Column".to_string() + i.to_string().as_str());
                }
            }

            for rec in 0..record.len() {
                output.insert(
                    self.labels[rec].clone(),
                    serde_json::to_value(record[rec].to_string())?,
                );
            }

            let output_value: serde_json::Value = output.into();
            let event = Rc::new(RefCell::new(output_value));
            Ok(vec![event])
        } else {
            Ok(vec![])
        }
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("read CSV from stdin".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = CsvInputArgs::try_parse_from(argv)?;

        let stdin = io::stdin().lock();

        let mut binding = csv::ReaderBuilder::new();
        let rbuilder = binding
            .delimiter(args.delim)
            .double_quote(!args.disable_double_quote)
            .quoting(!args.disable_quote)
            .flexible(args.flexible_fields)
            .trim(if args.trim_spaces { csv::Trim::All } else { csv::Trim::None })
            .has_headers(args.labels.is_empty());

        let rbfinal = if let Some(capacity) = args.buffer_capacity {
            rbuilder.buffer_capacity(capacity)
        } else {
            rbuilder
        };

        let mut reader = rbfinal.from_reader(stdin);

        let labels: Vec<String> = if args.labels.is_empty() {
            reader.headers()?.iter().map(|x| x.to_string()).collect()
        } else {
            args.labels
        };

        Ok(CsvInputProcessor { reader, lines: 0, labels })
    }

    fn stats(&self) -> Option<String> {
        Some(format!("lines:{}", self.lines))
    }
}
