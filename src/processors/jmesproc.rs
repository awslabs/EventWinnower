use crate::processors::processor::*;
use aho_corasick::AhoCorasick;
use anyhow::bail;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::rc::Rc;

#[derive(SerialProcessorInit)]
pub struct JMESPathProcessor<'a> {
    path: jmespath::Expression<'a>,
    label: String,
    pass_if_eval_true: bool,
}

#[derive(Parser)]
/// extract out a jmespath object from event, append to event
#[command(version, long_about = None, arg_required_else_help(true))]
struct JMESPathArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long, default_value = "jmespath")]
    label: String,

    #[arg(short, long)]
    pass_if_eval_true: bool,
}

impl SerialProcessor for JMESPathProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("extract JMESPath object from event".to_string())
    }
    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let do_match = argv[0] == "match";
        let args = JMESPathArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        Ok(Self { path, label: args.label, pass_if_eval_true: do_match || args.pass_if_eval_true })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        let result = self.path.search(&input)?;

        if result.is_null() {
            Ok(vec![])
        } else if self.pass_if_eval_true {
            match result.as_boolean() {
                //if true
                Some(eval) if eval => Ok(vec![input]),
                _ => Ok(vec![]),
            }
        } else {
            if let Some(event) = input.borrow_mut().as_object_mut() {
                event.insert(self.label.clone(), serde_json::to_value(result)?);
            }
            Ok(vec![input])
        }
    }
}

#[derive(SerialProcessorInit)]
pub struct SelectProcessor<'a> {
    path: jmespath::Expression<'a>,
    ungroup_array: bool,
    input_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// Select elements of an event from a jmespath
#[command(version, long_about = None, arg_required_else_help(true))]
struct SelectProcessorArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long, default_value_t = false)]
    ungroup_array: bool,
}

impl SerialProcessor for SelectProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("select and transform events using JMESPath".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = SelectProcessorArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        Ok(Self { path, ungroup_array: args.ungroup_array, input_count: 0, output_count: 0 })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;
        let result = self.path.search(&input)?;

        if result.is_null() {
            Ok(vec![])
        } else {
            let val = serde_json::to_value(result)?;
            if self.ungroup_array && val.is_array() {
                let mut output_events: Vec<Event> = Vec::new();
                for vout in val.as_array().expect("is valid array").iter() {
                    output_events.push(Rc::new(RefCell::new(serde_json::to_value(vout)?)));
                }
                self.output_count += output_events.len() as u64;
                Ok(output_events)
            } else {
                let record: Event = Rc::new(RefCell::new(val));
                self.output_count += 1;
                Ok(vec![record])
            }
        }
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\noutput:{}", self.input_count, self.output_count))
    }
}

#[derive(SerialProcessorInit)]
pub struct SubstringProcessor<'a> {
    path: jmespath::Expression<'a>,
    offset: usize,
    depth: usize,
    label: String,
    from_end: bool,
}

#[derive(Parser)]
/// Append a substring to the event based on a jmespath
#[command(version, long_about = None, arg_required_else_help(true))]
struct SubstringArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long)]
    depth: usize,

    #[arg(short, long, default_value_t = 0)]
    offset: usize,

    #[arg(short, long)]
    label: Option<String>,

    #[arg(short, long)]
    from_end: bool,
}

impl SerialProcessor for SubstringProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("append substring to event based on JMESPath".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = SubstringArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        let label = args.label.unwrap_or("substring".to_string());

        Ok(Self { path, label, depth: args.depth, offset: args.offset, from_end: args.from_end })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        let result = self.path.search(&input)?;

        if result.is_null() {
            return Ok(vec![input]);
        }

        let working = if result.is_string() {
            result.as_string().expect("is string")
        } else {
            &result.to_string()
        };

        if self.offset >= working.len() {
            return Ok(vec![input]);
        }

        let working_depth = if self.depth > (working.len() - self.offset) {
            working.len() - self.offset
        } else {
            self.depth
        };

        let substring = if !self.from_end {
            let end = working_depth + self.offset;
            working[self.offset..end].to_owned()
        } else {
            let start = working.len() - self.offset - working_depth;
            let end = working.len() - self.offset;
            working[start..end].to_owned()
        };

        if substring.is_empty() {
            return Ok(vec![input]);
        }

        if let Some(event) = input.borrow_mut().as_object_mut() {
            event.insert(self.label.clone(), serde_json::to_value(substring)?);
        }

        Ok(vec![input])
    }
}

#[derive(SerialProcessorInit)]
pub struct ContainsProcessor<'a> {
    path: jmespath::Expression<'a>,
    needles: AhoCorasick,
    not: bool,
    input_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// Filter events for fixed string match against a jmespath
#[command(version, long_about = None, arg_required_else_help(true))]
struct ContainsArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long)]
    match_string: Vec<String>,

    #[arg(short, long)]
    file_with_matches: Option<String>,

    #[arg(short, long)]
    not: bool,
}

impl SerialProcessor for ContainsProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("filter events for fixed string match against JMESPath".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let mut args = ContainsArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        if let Some(filename) = args.file_with_matches {
            let buffile = io::BufReader::new(File::open(filename)?);
            for line in buffile.lines().map_while(Result::ok) {
                args.match_string.push(line.to_string());
            }
        }

        if args.match_string.is_empty() {
            bail!("must have needles to match")
        }

        Ok(Self {
            path,
            needles: AhoCorasick::builder()
                .build(&args.match_string)
                .map_err(|e| anyhow::anyhow!("failed to build match patterns: {}", e))?,
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
            } else {
                return Ok(vec![]);
            }
        }

        let mut is_match = false;

        if !result.is_null()
            && ((result.is_string()
                && self.needles.is_match(result.as_string().expect("is string")))
                || self.needles.is_match(&result.to_string()))
        {
            is_match = true
        }
        if (is_match && !self.not) || (!is_match && self.not) {
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

enum NumMatchOperation {
    NotEquals,
    Equals,
    GreaterThan,
    LessThan,
}

#[derive(SerialProcessorInit)]
pub struct NumMatchProcessor<'a> {
    path: jmespath::Expression<'a>,
    operation: NumMatchOperation,
    value: Vec<f64>,
    input_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// Filter events for fixed string match against a jmespath
#[command(version, long_about = None, arg_required_else_help(true))]
struct NumMatchArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long, num_args = 1..)]
    value: Vec<f64>,

    #[arg(short, long)]
    greater_than: bool,

    #[arg(short, long)]
    less_than: bool,

    #[arg(short, long)]
    equal: bool,

    #[arg(short, long)]
    not_equal: bool,
}

impl SerialProcessor for NumMatchProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("filter events for numeric comparison against JMESPath".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = NumMatchArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        let operation = if args.greater_than {
            NumMatchOperation::GreaterThan
        } else if args.less_than {
            NumMatchOperation::LessThan
        } else if args.not_equal {
            NumMatchOperation::NotEquals
        } else {
            NumMatchOperation::Equals
        };

        Ok(Self { path, operation, value: args.value, input_count: 0, output_count: 0 })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;
        let result = self.path.search(&input)?;

        if let Some(event_value) = result.as_number() {
            let comp = match self.operation {
                NumMatchOperation::Equals => self.value.contains(&event_value),
                NumMatchOperation::NotEquals => !self.value.contains(&event_value),
                NumMatchOperation::GreaterThan => self.value.iter().all(|&x| event_value > x),
                NumMatchOperation::LessThan => self.value.iter().all(|&x| event_value < x),
            };

            if comp {
                self.output_count += 1;
                Ok(vec![input])
            } else {
                Ok(vec![])
            }
        } else {
            match self.operation {
                NumMatchOperation::NotEquals => Ok(vec![input]),
                _ => Ok(vec![]),
            }
        }
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\noutput:{}", self.input_count, self.output_count))
    }
}

fn result_to_haystack(result: jmespath::Rcvar) -> Vec<String> {
    if result.is_string() {
        vec![result.as_string().expect("is string").to_owned()]
    } else if result.is_array() {
        result
            .as_array()
            .expect("is array")
            .iter()
            .map(|element| {
                if element.is_string() {
                    element.as_string().expect("is string").to_owned()
                } else {
                    element.to_string()
                }
            })
            .collect::<Vec<String>>()
    } else {
        vec![result.to_string()]
    }
}

#[derive(SerialProcessorInit)]
pub struct StartsWithProcessor<'a> {
    path: jmespath::Expression<'a>,
    needles: Vec<String>,
    not: bool,
    input_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// Filter events for fixed string match against the inital characters in a jmespath
#[command(version, long_about = None, arg_required_else_help(true))]
struct StartsWithArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long, required(true))]
    match_string: Vec<String>,

    #[arg(short, long)]
    not: bool,
}

impl SerialProcessor for StartsWithProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("filter events where JMESPath value starts with string".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = ContainsArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        Ok(Self {
            path,
            needles: args.match_string,
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
            } else {
                return Ok(vec![]);
            }
        }

        let haystack = result_to_haystack(result);

        let mut is_match = false;
        for needle in self.needles.iter() {
            if haystack.iter().any(|hay| hay.starts_with(needle)) {
                is_match = true;
                break;
            }
        }

        if (is_match && !self.not) || (!is_match && self.not) {
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

#[derive(SerialProcessorInit)]
pub struct EndsWithProcessor<'a> {
    path: jmespath::Expression<'a>,
    needles: Vec<String>,
    not: bool,
    input_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// Filter events for fixed string match against the ending characters in a jmespath
#[command(version, long_about = None, arg_required_else_help(true))]
struct EndsWithArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long, required(true))]
    match_string: Vec<String>,

    #[arg(short, long)]
    not: bool,
}

impl SerialProcessor for EndsWithProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("filter events where JMESPath value ends with string".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = ContainsArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        Ok(Self {
            path,
            needles: args.match_string,
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
            } else {
                return Ok(vec![]);
            }
        }

        let haystack = result_to_haystack(result);

        let mut is_match = false;
        for needle in self.needles.iter() {
            if haystack.iter().any(|hay| hay.ends_with(needle)) {
                is_match = true;
                break;
            }
        }

        if (is_match && !self.not) || (!is_match && self.not) {
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

#[derive(SerialProcessorInit)]
pub struct ComponentsProcessor<'a> {
    path: jmespath::Expression<'a>,
    parent_label: String,
    child_label: String,
}

#[derive(Parser)]
/// finds connected components given array of edges and parent/child vertex labels
#[command(version, long_about = None, arg_required_else_help(true))]
struct ComponentsArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long, default_value = "child")]
    child: String,

    #[arg(short, long, default_value = "parent")]
    parent: String,
}

impl SerialProcessor for ComponentsProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("extract URL components from JMESPath value".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = ComponentsArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        Ok(Self { path, parent_label: args.parent, child_label: args.child })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        let result = self.path.search(&input)?;

        if !result.is_array() {
            return Ok(vec![input]);
        }

        let mut components: HashMap<serde_json::Value, usize> = HashMap::new();
        let mut next_component: usize = 0;
        if let Some(result_array) = result.as_array() {
            for val in result_array.iter() {
                if !val.is_object() {
                    continue;
                }
                if let Some(val_map) = val.as_object() {
                    if let Some(parent) = val_map.get(&self.parent_label) {
                        let parent_str = serde_json::to_value(parent)?;

                        let pval =
                            components.get(&parent_str).unwrap_or(&next_component).to_owned();
                        if let Some(child) = val_map.get(&self.child_label) {
                            let child_str = serde_json::to_value(child)?;

                            let cval =
                                components.get(&child_str).unwrap_or(&next_component).to_owned();

                            if pval < cval {
                                if cval != next_component {
                                    //replace all cval with pval
                                    self.replace_components(&mut components, cval, pval);
                                }
                                components.insert(child_str.clone(), pval);
                            } else if cval < pval {
                                if pval != next_component {
                                    //replace all pval with cval
                                    self.replace_components(&mut components, pval, cval);
                                }
                                components.insert(parent_str.clone(), cval);
                            } else if pval == next_component {
                                components.insert(child_str.clone(), next_component);
                                components.insert(parent_str.clone(), next_component);
                                next_component += 1;
                            }
                        }
                    }
                }
            }
        }

        //TODO insert component array, component size
        if let Some(event) = input.borrow_mut().as_object_mut() {
            event.insert("component_count".to_string(), serde_json::to_value(next_component)?);

            let mut comp_array: Vec<serde_json::Value> = Vec::new();
            let rev = self.reverse_map(components);
            for (key, value) in rev.iter() {
                let mut comp_map = serde_json::Map::new();
                comp_map.insert("component".to_string(), serde_json::to_value(key)?);
                comp_map
                    .insert("members".to_string(), serde_json::value::Value::Array(value.clone()));
                comp_array.push(serde_json::value::Value::Object(comp_map));
            }
            if !comp_array.is_empty() {
                event.insert("components".to_string(), serde_json::value::Value::Array(comp_array));
            }
        }

        Ok(vec![input])
    }
}

impl ComponentsProcessor<'_> {
    fn replace_components(
        &self,
        components: &mut HashMap<serde_json::Value, usize>,
        find_val: usize,
        replace_val: usize,
    ) {
        for (_key, val) in components.iter_mut() {
            if *val == find_val {
                *val = replace_val;
            }
        }
    }

    fn reverse_map(
        &self,
        components: HashMap<serde_json::Value, usize>,
    ) -> HashMap<usize, Vec<serde_json::Value>> {
        let mut out: HashMap<usize, Vec<serde_json::Value>> = HashMap::new();

        for (key, val) in components.iter() {
            out.entry(*val)
                .and_modify(|membership| membership.push(key.clone()))
                .or_insert(vec![key.clone()]);
        }

        out
    }
}

#[derive(SerialProcessorInit)]
pub struct SplitArrayProcessor<'a> {
    path: jmespath::Expression<'a>,
    keep: Option<jmespath::Expression<'a>>,
    input_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// create events from each element in an array
#[command(version, long_about = None, arg_required_else_help(true))]
struct SplitArrayArgs {
    #[arg(required(true))]
    path: Vec<String>,

    #[arg(short, long)]
    keep: Option<String>,
}

impl SerialProcessor for SplitArrayProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("split array elements into separate events".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = SplitArrayArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        let keep = match args.keep {
            Some(keep) => Some(jmespath::compile(&keep)?),
            None => None,
        };

        Ok(Self { path, keep, input_count: 0, output_count: 0 })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;
        let result = self.path.search(&input)?;

        let keep_elements = if let Some(keep) = &self.keep {
            keep.search(&input).ok().map(|v| serde_json::to_value(v).ok())
        } else {
            None
        };

        let mut output_events = Vec::new();
        if let Some(result_array) = result.as_array() {
            for val in result_array.iter() {
                let mut event = serde_json::to_value(val)?;
                self.output_count += 1;
                if let serde_json::Value::Object(ref mut event_map) = event {
                    if let Some(Some(serde_json::Value::Object(ref elements))) = keep_elements {
                        for (key, value) in elements.iter() {
                            event_map.insert(key.clone(), value.clone());
                        }
                    }
                }
                output_events.push(Rc::new(RefCell::new(event)));
            }
        }
        Ok(output_events)
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\noutput:{}", self.input_count, self.output_count))
    }
}

#[derive(SerialProcessorInit)]
pub struct JsonDecodeProcessor<'a> {
    path: Option<jmespath::Expression<'a>>,
    label: Option<String>,
    input_count: u64,
    decoded_count: u64,
}

#[derive(Parser)]
/// extract out a json object from event, append to event
#[command(version, long_about = None, arg_required_else_help(false))]
struct JsonDecodeArgs {
    #[arg(required(false))]
    path: Vec<String>,

    #[arg(short, long)]
    label: Option<String>,
}

impl SerialProcessor for JsonDecodeProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("decode JSON strings from JMESPath values".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = JsonDecodeArgs::try_parse_from(argv)?;
        let path = if args.path.is_empty() {
            None
        } else {
            Some(jmespath::compile(&args.path.join(" "))?)
        };

        Ok(Self { path, label: args.label, input_count: 0, decoded_count: 0 })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        let result = if let Some(path) = &self.path {
            path.search(&input)?
        } else {
            let decode = serde_json::from_str(input.borrow().as_str().expect("input as string"))?;
            let event = if let Some(label) = &self.label {
                let mut vmap = serde_json::Map::new();
                vmap.insert(label.clone(), decode);
                serde_json::Value::Object(vmap)
            } else {
                decode
            };
            return Ok(vec![Rc::new(RefCell::new(event))]);
        };

        if let Some(encoded) = result.as_string() {
            let decoded = match serde_json::from_str(encoded) {
                Ok(val) => val,
                Err(err) => {
                    eprintln!("failed to decode json: {err}");
                    return Ok(vec![input]);
                }
            };
            self.decoded_count += 1;
            if let Some(event) = input.borrow_mut().as_object_mut() {
                if let Some(label) = &self.label {
                    event.insert(label.to_string(), decoded);
                } else {
                    for (key, value) in decoded.as_object().unwrap().iter() {
                        event.insert(key.to_string(), value.clone());
                    }
                }
            }
        }
        Ok(vec![input])
    }
    fn stats(&self) -> Option<String> {
        Some(format!("input:{}\ndecoded:{}", self.input_count, self.decoded_count))
    }
}

#[derive(SerialProcessorInit)]
pub struct HasFieldProcessor<'a> {
    path: jmespath::Expression<'a>,
    input_count: u64,
    output_count: u64,
}

#[derive(Parser)]
/// Pass events that contain a given JMESPath field
#[command(version, long_about = None, arg_required_else_help(true))]
struct HasFieldArgs {
    #[arg(required(true))]
    path: Vec<String>,
}

impl SerialProcessor for HasFieldProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("pass events that contain a given JMESPath field".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = HasFieldArgs::try_parse_from(argv)?;
        let path = jmespath::compile(&args.path.join(" "))?;

        Ok(Self { path, input_count: 0, output_count: 0 })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;
        let result = self.path.search(&input)?;

        if result.is_null() {
            Ok(vec![])
        } else {
            self.output_count += 1;
            Ok(vec![input])
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

    // JMESPathProcessor Tests
    #[test]
    fn jmespath_processor_basic() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar").unwrap();
        let mut processor = JMESPathProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "bar": "test_value"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["jmespath"], "test_value");
        Ok(())
    }

    #[test]
    fn jmespath_processor_custom_label() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar -l custom_label").unwrap();
        let mut processor = JMESPathProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "bar": "test_value"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["custom_label"], "test_value");
        Ok(())
    }

    #[test]
    fn jmespath_processor_pass_if_eval_true() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.enabled -p").unwrap();
        let mut processor = JMESPathProcessor::new(&args)?;

        // Test with true value
        let val = json!({
            "foo": {
                "enabled": true
            }
        });
        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        // Test with false value
        let val = json!({
            "foo": {
                "enabled": false
            }
        });
        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    #[test]
    fn jmespath_processor_null_result() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc nonexistent.path").unwrap();
        let mut processor = JMESPathProcessor::new(&args)?;
        let val = json!({
            "foo": "bar"
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    // SelectProcessor Tests
    #[test]
    fn select_processor_basic() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar").unwrap();
        let mut processor = SelectProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "bar": "selected_value"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(*result, json!("selected_value"));
        Ok(())
    }

    #[test]
    fn select_processor_ungroup_array() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.items -u").unwrap();
        let mut processor = SelectProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "items": ["item1", "item2", "item3"]
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 3);

        assert_eq!(*output[0].borrow(), json!("item1"));
        assert_eq!(*output[1].borrow(), json!("item2"));
        assert_eq!(*output[2].borrow(), json!("item3"));
        Ok(())
    }

    #[test]
    fn substring_test() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar -d 2").unwrap();
        let mut subp = SubstringProcessor::new(&args)?;
        let val = json!({ "foo" : { "bar": "eval", }, });

        let event = Rc::new(RefCell::new(val));
        let mut output = subp.process(event.clone())?;
        assert_eq!(output.len(), 1);
        if let Some(output_event) = output.pop() {
            let output = output_event.borrow();
            assert_eq!(output["substring"], serde_json::to_value("ev").unwrap());
        }

        let val = json!({ "foo" : { "bar": "eval", }, });
        let event = Rc::new(RefCell::new(val));
        let args = shlex::split("proc foo.bar -d 3 -o 1 -l xx").unwrap();
        let mut subp = SubstringProcessor::new(&args)?;
        let mut output = subp.process(event.clone())?;
        assert_eq!(output.len(), 1);
        if let Some(output_event) = output.pop() {
            let output = output_event.borrow();
            assert_eq!(output["xx"], serde_json::to_value("val").unwrap());
        }

        let val = json!({ "foo" : { "bar": "eval", }, });
        let event = Rc::new(RefCell::new(val));
        let args = shlex::split("proc foo.bar -d 3 -o 4 -l xx").unwrap();
        let mut subp = SubstringProcessor::new(&args)?;
        let mut output = subp.process(event.clone())?;
        assert_eq!(output.len(), 1);
        if let Some(output_event) = output.pop() {
            let output = output_event.borrow();
            assert!(output["xx"].is_null());
        }

        let args = shlex::split("proc foo.bar -f -d 2 -o 1 -l xx").unwrap();
        let mut subp = SubstringProcessor::new(&args)?;
        let mut output = subp.process(event.clone())?;
        assert_eq!(output.len(), 1);
        if let Some(output_event) = output.pop() {
            let output = output_event.borrow();
            assert_eq!(output["xx"], serde_json::to_value("va").unwrap());
        }
        Ok(())
    }

    #[test]
    fn startswith_test() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar -m eval").unwrap();
        let mut fup = StartsWithProcessor::new(&args)?;

        let val = json!({
            "foo" : {
                "bar": "evaluation",
            },
        });

        let event = Rc::new(RefCell::new(val));
        let mut output = fup.process(event.clone())?;

        assert_eq!(output.len(), 1);
        if let Some(output_event) = output.pop() {
            let output = output_event.borrow();
            assert_eq!(output["foo"], json!({"bar":"evaluation"}));
        }

        let args = shlex::split("proc foo.bar -m eval --not").unwrap();
        let mut fup = StartsWithProcessor::new(&args)?;

        let output = fup.process(event.clone())?;
        assert_eq!(output.len(), 0);

        let args = shlex::split("proc foo.bar -m valu").unwrap();
        let mut fup = StartsWithProcessor::new(&args)?;
        let output = fup.process(event.clone())?;
        assert_eq!(output.len(), 0);

        let args = shlex::split("proc foo.bar -m valu --not").unwrap();
        let mut fup = StartsWithProcessor::new(&args)?;
        let output = fup.process(event.clone())?;
        assert_eq!(output.len(), 1);
        Ok(())
    }

    #[test]
    fn ends_with_test() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar -m tion").unwrap();
        let mut fup = EndsWithProcessor::new(&args)?;

        let val = json!({
            "foo" : {
                "bar": "evaluation",
            },
        });

        let event = Rc::new(RefCell::new(val));
        let mut output = fup.process(event.clone())?;

        assert_eq!(output.len(), 1);
        if let Some(output_event) = output.pop() {
            let output = output_event.borrow();
            assert_eq!(output["foo"], json!({"bar":"evaluation"}));
        }

        let args = shlex::split("proc foo.bar -m tion --not").unwrap();
        let mut fup = EndsWithProcessor::new(&args)?;

        let output = fup.process(event.clone())?;
        assert_eq!(output.len(), 0);

        let args = shlex::split("proc foo.bar -m tio").unwrap();
        let mut fup = EndsWithProcessor::new(&args)?;
        let output = fup.process(event.clone())?;
        assert_eq!(output.len(), 0);

        let args = shlex::split("proc foo.bar -m tio --not").unwrap();
        let mut fup = EndsWithProcessor::new(&args)?;
        let output = fup.process(event.clone())?;
        assert_eq!(output.len(), 1);

        let val = json!({
            "foo" : {
                "bar": ["evaluation", "stuff", "rotate", "member"],
            },
        });

        let event = Rc::new(RefCell::new(val));
        let args = shlex::split("proc foo.bar -m tate").unwrap();
        let mut fup = EndsWithProcessor::new(&args)?;
        let output = fup.process(event.clone())?;
        assert_eq!(output.len(), 1);

        Ok(())
    }

    // ContainsProcessor Tests
    #[test]
    fn contains_processor_basic() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar -m test").unwrap();
        let mut processor = ContainsProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "bar": "this is a test string"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);
        Ok(())
    }

    #[test]
    fn contains_processor_no_match() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar -m xyz").unwrap();
        let mut processor = ContainsProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "bar": "this is a test string"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    #[test]
    fn contains_processor_not_flag() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar -m xyz -n").unwrap();
        let mut processor = ContainsProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "bar": "this is a test string"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);
        Ok(())
    }

    // NumMatchProcessor Tests
    #[test]
    fn num_match_processor_equals() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.count -v 42 -v 53 -e").unwrap();
        let mut processor = NumMatchProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "count": 42
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        let val = json!({ "foo": { "count": 53 } });
        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        let val = json!({ "foo": { "count": 21 } });
        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    #[test]
    fn num_match_processor_not_equals() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.count -v 42 -v 53 --not-equal").unwrap();
        let mut processor = NumMatchProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "count": 42
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 0);

        let val = json!({ "foo": { "count": 53 } });
        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 0);

        let val = json!({ "foo": { "count": 21 } });
        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);
        Ok(())
    }

    #[test]
    fn num_match_processor_greater_than() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.count -v 10 -g").unwrap();
        let mut processor = NumMatchProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "count": 15
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);
        Ok(())
    }

    #[test]
    fn num_match_processor_less_than() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.count -v 10 -l").unwrap();
        let mut processor = NumMatchProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "count": 5
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);
        Ok(())
    }

    // SplitArrayProcessor Tests
    #[test]
    fn split_array_processor_basic() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.items").unwrap();
        let mut processor = SplitArrayProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "items": [
                    {"name": "item1", "value": 10},
                    {"name": "item2", "value": 20}
                ]
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 2);

        assert_eq!(*output[0].borrow(), json!({"name": "item1", "value": 10}));
        assert_eq!(*output[1].borrow(), json!({"name": "item2", "value": 20}));
        Ok(())
    }

    #[test]
    fn split_array_processor_with_keep() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.items -k metadata").unwrap();
        let mut processor = SplitArrayProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "items": [
                    {"name": "item1"},
                    {"name": "item2"}
                ]
            },
            "metadata": {
                "source": "test",
                "timestamp": "2023-01-01"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 2);

        let result1 = output[0].borrow();
        assert_eq!(result1["name"], "item1");
        assert_eq!(result1["source"], "test");
        assert_eq!(result1["timestamp"], "2023-01-01");
        Ok(())
    }

    // JsonDecodeProcessor Tests
    #[test]
    fn json_decode_processor_basic() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.encoded").unwrap();
        let mut processor = JsonDecodeProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "encoded": "{\"decoded_key\": \"decoded_value\"}"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["decoded_key"], "decoded_value");
        Ok(())
    }

    #[test]
    fn json_decode_processor_with_label() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.encoded -l decoded_data").unwrap();
        let mut processor = JsonDecodeProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "encoded": "{\"key\": \"value\"}"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["decoded_data"], json!({"key": "value"}));
        Ok(())
    }

    // ComponentsProcessor Tests
    #[test]
    fn components_processor_basic() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc edges").unwrap();
        let mut processor = ComponentsProcessor::new(&args)?;
        let val = json!({
            "edges": [
                {"parent": "A", "child": "B"},
                {"parent": "B", "child": "C"},
                {"parent": "D", "child": "E"}
            ]
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["component_count"], 2);
        assert!(result["components"].is_array());
        Ok(())
    }

    #[test]
    fn components_processor_custom_labels() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc connections -c source -p target").unwrap();
        let mut processor = ComponentsProcessor::new(&args)?;
        let val = json!({
            "connections": [
                {"target": "X", "source": "Y"},
                {"target": "Y", "source": "Z"}
            ]
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(result["component_count"], 1);
        Ok(())
    }

    // HasFieldProcessor Tests
    #[test]
    fn has_field_processor_field_exists() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar").unwrap();
        let mut processor = HasFieldProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "bar": "some_value"
            },
            "other": "data"
        });

        let event = Rc::new(RefCell::new(val.clone()));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        // Should return the whole input event
        let result = output[0].borrow();
        assert_eq!(*result, val);
        Ok(())
    }

    #[test]
    fn has_field_processor_field_missing() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.nonexistent").unwrap();
        let mut processor = HasFieldProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "bar": "some_value"
            },
            "other": "data"
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 0); // Should return no events
        Ok(())
    }

    #[test]
    fn has_field_processor_nested_field_exists() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc data.nested.field").unwrap();
        let mut processor = HasFieldProcessor::new(&args)?;
        let val = json!({
            "data": {
                "nested": {
                    "field": "some_value"
                }
            }
        });

        let event = Rc::new(RefCell::new(val.clone()));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(*result, val);
        Ok(())
    }

    #[test]
    fn has_field_processor_null_field() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc data.nested.field").unwrap();
        let mut processor = HasFieldProcessor::new(&args)?;
        let val = json!({
            "data": {
                "nested": {
                    "field": null
                }
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 0); // JMESPath treats null as "not found"
        Ok(())
    }

    #[test]
    fn has_field_processor_array_field() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc items[0].name").unwrap();
        let mut processor = HasFieldProcessor::new(&args)?;
        let val = json!({
            "items": [
                {"name": "first_item"},
                {"name": "second_item"}
            ]
        });

        let event = Rc::new(RefCell::new(val.clone()));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert_eq!(*result, val);
        Ok(())
    }

    #[test]
    fn has_field_processor_array_field_missing() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc items[5].name").unwrap();
        let mut processor = HasFieldProcessor::new(&args)?;
        let val = json!({
            "items": [
                {"name": "first_item"},
                {"name": "second_item"}
            ]
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 0); // Array index out of bounds should return no events
        Ok(())
    }

    #[test]
    fn has_field_processor_stats() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar").unwrap();
        let mut processor = HasFieldProcessor::new(&args)?;

        // Process event with field
        let val1 = json!({"foo": {"bar": "value"}});
        let event1 = Rc::new(RefCell::new(val1));
        processor.process(event1)?;

        // Process event without field
        let val2 = json!({"foo": {"baz": "value"}});
        let event2 = Rc::new(RefCell::new(val2));
        processor.process(event2)?;

        let stats = processor.stats().unwrap();
        assert!(stats.contains("input:2"));
        assert!(stats.contains("output:1"));
        Ok(())
    }

    // Additional edge case tests
    #[test]
    fn substring_processor_edge_cases() -> Result<(), anyhow::Error> {
        // Test with empty string
        let args = shlex::split("proc foo.bar -d 5").unwrap();
        let mut processor = SubstringProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "bar": ""
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        let result = output[0].borrow();
        assert!(result["substring"].is_null());
        Ok(())
    }

    #[test]
    fn select_processor_null_result() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc nonexistent.path").unwrap();
        let mut processor = SelectProcessor::new(&args)?;
        let val = json!({
            "foo": "bar"
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 0);
        Ok(())
    }

    #[test]
    fn contains_processor_multiple_needles() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.bar -m needle1 -m needle2").unwrap();
        let mut processor = ContainsProcessor::new(&args)?;

        let val = json!({
            "foo": {
                "bar": "this contains needle2 in it"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);
        Ok(())
    }

    #[test]
    fn num_match_processor_non_numeric() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.value -v 42 -e").unwrap();
        let mut processor = NumMatchProcessor::new(&args)?;

        let val = json!({
            "foo": {
                "value": "not_a_number"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 0); // Non-numeric values should be filtered out
        Ok(())
    }

    #[test]
    fn json_decode_processor_invalid_json() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.encoded").unwrap();
        let mut processor = JsonDecodeProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "encoded": "invalid json string"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 1);

        // Should return original event when JSON decode fails
        let result = output[0].borrow();
        assert_eq!(result["foo"]["encoded"], "invalid json string");
        Ok(())
    }

    #[test]
    fn split_array_processor_non_array() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo.not_array").unwrap();
        let mut processor = SplitArrayProcessor::new(&args)?;
        let val = json!({
            "foo": {
                "not_array": "just a string"
            }
        });

        let event = Rc::new(RefCell::new(val));
        let output = processor.process(event)?;
        assert_eq!(output.len(), 0); // Non-arrays should produce no output
        Ok(())
    }
}
