use crate::processors::processor::*;
use crate::workflow::shutdown::GLOBAL_SHUTDOWN;
use anyhow::{bail, Result};
use clap::Parser;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead};
use std::io::{prelude::*, BufWriter};
use std::rc::Rc;

use crate::processors::defanger::defang_urls;
use eventwinnower_macros::{SerialProcessorInit, SerialSourceProcessorInit};

use lazy_static::lazy_static;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(SerialSourceProcessorInit)]
pub struct JsonInputProcessor {
    reader: Box<dyn BufRead>,
    lines: u64,
    contains: Option<String>,
    batch_size: usize,
    dont_decode: bool,
    label: Option<String>,
}

#[derive(Parser)]
/// Read json lines from stdin
#[command(version, long_about = None, arg_required_else_help(false))]
struct JsonInputArgs {
    #[arg(short, long)]
    contains: Option<String>,

    #[arg(short, long, default_value_t = 1)]
    batch_size: usize,

    #[arg(short, long)]
    label: Option<String>,

    #[arg(short, long)]
    dont_decode: bool,
}

impl SerialSourceProcessor for JsonInputProcessor {
    fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        let mut output_events: Vec<Event> = vec![];
        loop {
            let mut buffer = String::new();
            if let Ok(len) = self.reader.read_line(&mut buffer) {
                if len == 0 {
                    // EOF
                    return Ok(output_events);
                }

                //check if optional filter
                if let Some(needle) = &self.contains {
                    if !buffer.contains(needle) {
                        continue;
                    }
                }

                let element = if self.dont_decode {
                    serde_json::Value::String(buffer.trim_end_matches(['\r', '\n']).to_string())
                } else {
                    serde_json::from_str(&buffer)?
                };
                let record: Event = if let Some(label) = &self.label {
                    let mut map = serde_json::Map::new();
                    map.insert(label.clone(), element);
                    Rc::new(RefCell::new(serde_json::Value::Object(map)))
                } else {
                    Rc::new(RefCell::new(element))
                };
                self.lines += 1;
                output_events.push(record);
                if output_events.len() >= self.batch_size {
                    return Ok(output_events);
                }
            } else {
                //some sort of read error
                return Ok(output_events);
            }
        }
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("parse STDIN".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = JsonInputArgs::try_parse_from(argv)?;

        if args.batch_size == 0 {
            bail!("batch size cannot be zero");
        }

        let stdin = io::stdin();
        Ok(JsonInputProcessor {
            reader: Box::new(stdin.lock()) as Box<dyn BufRead>,
            lines: 0,
            batch_size: args.batch_size,
            contains: args.contains,
            dont_decode: args.dont_decode || argv[0].contains("line"),
            label: args.label,
        })
    }

    fn stats(&self) -> Option<String> {
        Some(format!("lines:{}", self.lines))
    }
}

#[derive(Default, SerialProcessorInit)]
pub struct CountProcessor {
    input_count: u64,
    label: String,
    as_string: bool,
}

#[derive(Parser)]
/// Count events and append count to each event
#[command(version, long_about = None, arg_required_else_help(false))]
struct CountArgs {
    #[arg(short, long, default_value = "count")]
    label: String,

    #[arg(short = 's', long)]
    as_string: bool,
}

impl SerialProcessor for CountProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("count events and append count to each event".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = CountArgs::try_parse_from(argv)?;
        Ok(Self { input_count: 0, label: args.label, as_string: args.as_string })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        if let Some(event) = input.borrow_mut().as_object_mut() {
            if self.as_string {
                event.insert(self.label.clone(), self.input_count.to_string().into());
            } else {
                event.insert(self.label.clone(), self.input_count.into());
            }
        }

        Ok(vec![input])
    }

    fn stats(&self) -> Option<String> {
        Some(format!("events:{}", self.input_count))
    }
}

lazy_static! {
    static ref PRINT_PROCESSOR_THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);
}

#[derive(SerialProcessorInit)]
pub struct PrintProcessor {
    label: Option<String>,
    input_count: u64,
    writer: Box<dyn Write>,
    pretty_print: bool,
    defang_urls: bool,
    tsv_values: bool,
    raw_string_output: bool,
}

#[derive(Parser)]
/// Print out event, optionally save as file
#[command(version, long_about = None)]
struct PrintArgs {
    label: Vec<String>,

    #[arg(short, long)]
    filename: Option<String>,

    #[arg(short, long)]
    gzip: bool,

    #[arg(short, long)]
    pretty_print: bool,

    #[arg(short, long)]
    defang_urls: bool,

    #[arg(short, long)]
    tsv_values: bool,

    //if object is a raw string, print it
    #[arg(short, long)]
    raw_string_output: bool,

    #[arg(long)]
    append_thread_id_to_filename: bool,

    #[arg(long)]
    avoid_overwrite: bool,
}

impl SerialProcessor for PrintProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("print events, optionally save to file".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = PrintArgs::try_parse_from(argv)?;

        let label = if args.label.is_empty() { None } else { Some(args.label.join(" ")) };

        let writer: Box<dyn Write> = match args.filename {
            Some(filename) => {
                let filename_base = if args.append_thread_id_to_filename {
                    let current = PRINT_PROCESSOR_THREAD_COUNT.fetch_add(1, Ordering::SeqCst);
                    format!("{filename}.{:02}{}", current, if args.gzip { ".json.gz" } else { "" })
                } else {
                    filename.clone()
                };

                let mut filename_working = filename_base.clone();
                // Check if file exists and avoid overwrite if flag is set
                if args.avoid_overwrite {
                    let mut counter = 1;
                    while std::path::Path::new(&filename_working).exists() {
                        filename_working = format!("{filename_base}.{counter}");
                        counter += 1;
                    }
                }

                if args.gzip {
                    Box::new(BufWriter::new(GzEncoder::new(
                        File::create(filename_working)?,
                        Compression::default(),
                    )))
                } else {
                    Box::new(BufWriter::new(File::create(filename_working)?))
                }
            }
            None => Box::new(std::io::stdout()),
        };

        Ok(PrintProcessor {
            label,
            input_count: 0,
            writer,
            pretty_print: args.pretty_print,
            defang_urls: args.defang_urls,
            tsv_values: args.tsv_values,
            raw_string_output: args.raw_string_output,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        if let Some(label) = &self.label {
            self.writer.write_fmt(format_args!("{label}"))?;
        }

        self.input_count += 1;

        if self.tsv_values {
            self.tsv_write(&input)?;
            return Ok(vec![input]);
        }

        let serialized_json = if self.raw_string_output {
            {
                let iborrow = input.borrow();
                if let Some(istr) = iborrow.as_str() {
                    istr.to_string()
                } else {
                    iborrow.to_string()
                }
            }
        } else if self.pretty_print {
            serde_json::to_string_pretty(&input)?
        } else {
            input.borrow().to_string()
        };

        let output_json =
            if self.defang_urls { defang_urls(&serialized_json) } else { serialized_json };

        self.writer.write_fmt(format_args!("{output_json}\n"))?;

        Ok(vec![input])
    }

    fn flush(&mut self) -> Vec<Event> {
        match self.writer.flush() {
            Ok(_) => {
                eprintln!("flushed print");
            }
            Err(err) => {
                eprintln!("unable to flush {err:?}");
            }
        };
        vec![]
    }

    fn stats(&self) -> Option<String> {
        Some(format!("events:{}", self.input_count))
    }
}

impl PrintProcessor {
    fn tsv_write(&mut self, input: &Event) -> Result<(), anyhow::Error> {
        let mut cnt = 0;
        if let Some(event) = input.borrow().as_object() {
            for (_key, val) in event {
                if cnt > 0 {
                    self.writer.write_fmt(format_args!("\t"))?;
                }
                self.writer.write_fmt(format_args!("{val}"))?;
                cnt += 1;
            }
            self.writer.write_fmt(format_args!("\n"))?;
        }
        Ok(())
    }
}

#[derive(SerialProcessorInit)]
pub struct RebatchProcessor {
    batch_size: usize,
    buffer: Vec<Event>,
    event_count: u64,
}

#[derive(Parser)]
/// Rebatch events if batch_size > current batch
#[command(version, long_about = None)]
struct RebatchArgs {
    #[arg(default_value_t = 10)]
    batch_size: usize,
}

impl SerialProcessor for RebatchProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("rebatch events if batch size > current batch".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = RebatchArgs::try_parse_from(argv)?;

        Ok(RebatchProcessor { batch_size: args.batch_size, buffer: Vec::new(), event_count: 0 })
    }

    fn process_batch(&mut self, events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        let mut output_events = Vec::new();
        for event in events {
            self.buffer.push(event.clone());
            self.event_count += 1;
            if self.buffer.len() >= self.batch_size {
                output_events.append(&mut self.buffer);
            }
        }
        Ok(output_events)
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut output_events = Vec::new();
        output_events.append(&mut self.buffer);
        output_events
    }

    fn stats(&self) -> Option<String> {
        Some(format!("events:{}", self.event_count))
    }
}

#[derive(SerialProcessorInit)]
pub struct AnnotateProcessor {
    annotation: Option<String>,
    label: String,
    timestamp: bool,
    events: u64,
}

#[derive(Parser)]
/// Annotate event
#[command(version, long_about = None)]
struct AnnotateArgs {
    annotation: Vec<String>,

    #[arg(short, long, default_value = "annotation")]
    label: String,

    #[arg(short, long)]
    timestamp: bool,
}

impl SerialProcessor for AnnotateProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("annotate events with custom labels and timestamps".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = AnnotateArgs::try_parse_from(argv)?;

        let annotation =
            if args.annotation.is_empty() { None } else { Some(args.annotation.join(" ")) };

        Ok(AnnotateProcessor {
            annotation,
            label: args.label,
            timestamp: args.timestamp,
            events: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.events += 1;

        if let Some(event) = input.borrow_mut().as_object_mut() {
            if let Some(annotation) = &self.annotation {
                event.insert(self.label.clone(), annotation.clone().into());
            }
            if self.timestamp {
                event.insert(
                    "timestamp".to_string(),
                    chrono::Utc::now()
                        .to_rfc3339_opts(chrono::format::SecondsFormat::Millis, true)
                        .into(),
                );
            }
        }

        Ok(vec![input])
    }

    fn stats(&self) -> Option<String> {
        Some(format!("events:{}", self.events))
    }
}

#[derive(SerialProcessorInit)]
pub struct EnumerateProcessor {
    count: u64,
    label: String,
    match_set: Option<HashSet<u64>>,
}

#[derive(Parser)]
/// assign number to each value
#[command(version, long_about = None)]
struct EnumerateArgs {
    #[arg(short, long, default_value = "enumeration")]
    label: String,

    #[arg(short, long)]
    file_match: Option<String>,

    #[arg(short, long)]
    match_position: Vec<u64>,
}

impl SerialProcessor for EnumerateProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("assign sequential numbers to events".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = EnumerateArgs::try_parse_from(argv)?;

        let mut match_set = if let Some(file_match) = args.file_match {
            let file = File::open(file_match)?;
            let reader = io::BufReader::new(file);
            let mut match_set = HashSet::new();
            for line in reader.lines() {
                match_set.insert(line?.parse::<u64>()?);
            }
            Some(match_set)
        } else {
            None
        };

        if !args.match_position.is_empty() {
            if let Some(ref mut match_set) = match_set {
                for position in args.match_position {
                    match_set.insert(position);
                }
            } else {
                let mut new_set = HashSet::new();
                for position in args.match_position {
                    new_set.insert(position);
                }
                match_set = Some(new_set);
            }
        }

        Ok(EnumerateProcessor { count: 0, label: args.label, match_set })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        if let Some(match_set) = &self.match_set {
            if !match_set.contains(&self.count) {
                self.count += 1;
                return Ok(vec![]);
            }
        }
        if let Some(event) = input.borrow_mut().as_object_mut() {
            event.insert(self.label.clone(), serde_json::to_value(self.count)?);
        }

        self.count += 1;

        Ok(vec![input])
    }

    fn stats(&self) -> Option<String> {
        Some(format!("events:{}", self.count))
    }
}

#[derive(SerialProcessorInit)]
pub struct HeadProcessor {
    num_events: u64,
    input_count: u64,
    output_count: u64,
    force_shutdown: bool,
}

#[derive(Parser)]
/// assign number to each value
#[command(version, long_about = None)]
struct HeadArgs {
    #[arg(short, long, default_value_t = 10)]
    num_events: u64,

    #[arg(short, long)]
    force_shutdown_after_limit: bool,
}

impl SerialProcessor for HeadProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("output first N events".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = HeadArgs::try_parse_from(argv)?;

        Ok(HeadProcessor {
            num_events: args.num_events,
            input_count: 0,
            output_count: 0,
            force_shutdown: args.force_shutdown_after_limit,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;
        if self.input_count <= self.num_events {
            self.output_count += 1;
            Ok(vec![input])
        } else {
            if self.force_shutdown {
                GLOBAL_SHUTDOWN.store(true, Ordering::Relaxed);
            }
            Ok(vec![])
        }
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\noutput:{}", self.input_count, self.output_count))
    }
}

#[derive(SerialSourceProcessorInit)]
pub struct NullSourceProcessor;
impl SerialSourceProcessor for NullSourceProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("null source processor (generates no events)".to_string())
    }

    fn new(_argv: &[String]) -> Result<Self, anyhow::Error> {
        Ok(Self {})
    }

    fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        Ok(vec![])
    }
}

#[derive(SerialSourceProcessorInit)]
pub struct LambdaSourceProcessor;
impl SerialSourceProcessor for LambdaSourceProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("lambda psuedo-source processor (used in AWS Lambda context)".to_string())
    }

    fn new(_argv: &[String]) -> Result<Self, anyhow::Error> {
        Ok(Self {})
    }

    fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        Ok(vec![])
    }
}

#[derive(SerialProcessorInit)]
pub struct NullProcessor;
impl SerialProcessor for NullProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("null processor (passes events through unchanged)".to_string())
    }

    fn new(_argv: &[String]) -> Result<Self, anyhow::Error> {
        Ok(Self {})
    }
    fn process_batch(&mut self, events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        Ok(events.into())
    }
}

#[derive(SerialProcessorInit)]
pub struct AccumulateProcessor {
    buffer: Vec<Event>,
    output_count: usize,
}
impl SerialProcessor for AccumulateProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("accumulate events until flush".to_string())
    }

    fn new(_argv: &[String]) -> Result<Self, anyhow::Error> {
        Ok(Self { buffer: Vec::new(), output_count: 0 })
    }
    fn process_batch(&mut self, events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        events.iter().for_each(|event| self.buffer.push(event.clone()));
        Ok(vec![])
    }
    fn flush(&mut self) -> Vec<Event> {
        self.output_count += self.buffer.len();
        std::mem::take(&mut self.buffer)
    }
    fn stats(&self) -> Option<String> {
        Some(format!("output:{}", self.output_count))
    }
}
