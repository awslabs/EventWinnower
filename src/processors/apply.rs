use crate::processors::processor::*;
use crate::processors::subset::PathType;
use crate::processors::PROCESSORS;
use anyhow::Result;
use async_trait::async_trait;
use eventwinnower_macros::AsyncProcessorInit;
use serde_json::Value;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(AsyncProcessorInit)]
pub struct ApplyProcessor {
    path: PathType,
    processor: ProcessorType,
}

#[async_trait(?Send)]
impl AsyncProcessor for ApplyProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("apply processor to nested element or array".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        if argv.len() < 3 {
            if argv.len() == 2 && (argv[1] == "-h" || argv[1] == "--help") {
                println!(
                    "apply - processor to apply another processor to a nested element or array"
                );
                println!("Usage: apply <path> <processor> [processor arguments]\n");
                return Err(anyhow::anyhow!(""));
            }
            return Err(anyhow::anyhow!("need arguments to apply processor {:?}", argv));
        }

        let processor = if let Some(processor) = PROCESSORS.get(&argv[2]) {
            if let Some(valid_processor) = processor.init(&argv[2..], 1000).await?.as_non_source() {
                valid_processor
            } else {
                return Err(anyhow::anyhow!("invalid processor {:?}", argv));
            }
        } else {
            return Err(anyhow::anyhow!("unknown processor {:?}", argv));
        };

        Ok(Self { path: PathType::new(argv[1].as_str()), processor })
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        //TODO - handle nested paths (see subset processor for ideas)
        //if it is an array - apply to all events in that array - keep was gets passed
        //it is is not an array - apply to object - keep key-result
        if let Some((mut value, event_map, key)) = self.path.extract(&mut input.borrow_mut()) {
            if let Some(val_array) = value.as_array_mut() {
                let mut out: Vec<Value> = Vec::new();
                for element in val_array.iter_mut() {
                    let ev = Rc::new(RefCell::new(element.to_owned()));
                    let result = match self.processor {
                        ProcessorType::Async(ref mut proc) => proc.process(ev).await?,
                        ProcessorType::Serial(ref mut proc) => proc.process(ev)?,
                        _ => vec![],
                    };
                    result.into_iter().for_each(|v| out.push(v.take()));
                }

                if !out.is_empty() {
                    event_map.insert(key, Value::Array(out));
                }
            } else {
                let ev = Rc::new(RefCell::new(value));
                let mut result = match self.processor {
                    ProcessorType::Async(ref mut proc) => proc.process(ev).await?,
                    ProcessorType::Serial(ref mut proc) => proc.process(ev)?,
                    _ => vec![],
                };
                if result.len() == 1 {
                    event_map.insert(key, result.pop().expect("has result").take());
                } else if !result.is_empty() {
                    let mut out: Vec<Value> = Vec::new();
                    result.into_iter().for_each(|v| out.push(v.take()));
                    event_map.insert(key, Value::Array(out));
                }
            }
        }
        Ok(vec![input])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // DenestProcessor Tests
    #[tokio::test]
    async fn apply_test_single() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc foo upper bar --label upper").unwrap();
        let mut subp = ApplyProcessor::new(&args).await?;
        let val = json!({
            "foo" : { "bar": "eval", },
            "things" : 123,
            "other" :"stuff"
        });
        let event = Rc::new(RefCell::new(val));
        let mut output = subp.process(event.clone()).await?;
        if let Some(out_event) = output.pop() {
            let output = out_event.borrow();
            eprintln!("apply output test: {output}");
            assert_eq!(output["foo"]["upper"], json!("EVAL"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn apply_test_array() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc buz.foo contains bar -m 1").unwrap();
        let mut subp = ApplyProcessor::new(&args).await?;
        let val = json!({
            "buz":{"foo" : [{ "bar": "eval"},{"bar":"stuff1"},{"bar":"other"},{"bar":"stuff12"}]},
            "things" : 123,
            "other" :"stuff"
        });
        let event = Rc::new(RefCell::new(val));
        let mut output = subp.process(event.clone()).await?;
        if let Some(out_event) = output.pop() {
            let output = out_event.borrow();
            if let Some(oset) = output["buz"]["foo"].as_array() {
                assert_eq!(oset.len(), 2);
                assert_eq!(oset[0]["bar"], json!("stuff1"));
                assert_eq!(oset[1]["bar"], json!("stuff12"));
            }
        }
        Ok(())
    }
}
