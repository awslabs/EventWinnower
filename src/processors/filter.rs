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
pub struct FilterProcessor {
    path: PathType,
    processor: ProcessorType,
}

#[async_trait(?Send)]
impl AsyncProcessor for FilterProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("filter array elements through a processor and collect results".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        if argv.len() < 3 {
            if argv.len() == 2 && (argv[1] == "-h" || argv[1] == "--help") {
                println!("filter - filter array elements through a processor and collect results");
                println!("Usage: <path> <processor> [processor arguments] \n");
                return Err(anyhow::anyhow!(""));
            }
            return Err(anyhow::anyhow!("need arguments to filter processor {:?}", argv));
        }

        let processor = if let Some(processor) = PROCESSORS.get(&argv[2]) {
            if let Some(valid_processor) = processor.init(&argv[2..], 1000).await?.as_non_source() {
                valid_processor
            } else {
                return Err(anyhow::anyhow!("invalid processor {:?}", argv));
            }
        } else {
            return Err(anyhow::anyhow!("unknown processor {:?}", argv[2]));
        };

        Ok(Self { path: PathType::new(argv[1].as_str()), processor })
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        if let Some((value, event_map, key)) = self.path.extract(&mut input.borrow_mut()) {
            if let Some(val_array) = value.as_array() {
                let mut filtered: Vec<Value> = Vec::new();

                for element in val_array.iter() {
                    let ev = Rc::new(RefCell::new(element.to_owned()));
                    let result = match self.processor {
                        ProcessorType::Async(ref mut proc) => proc.process(ev).await?,
                        ProcessorType::Serial(ref mut proc) => proc.process(ev)?,
                        _ => vec![],
                    };
                    // If processor returns results, include the element in filtered output
                    if !result.is_empty() {
                        filtered.push(element.clone());
                    }
                }

                // Attach filtered results to event at specified label
                event_map.insert(key.clone(), Value::Array(filtered));
            }
        }
        Ok(vec![input])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn filter_test_basic() -> Result<(), anyhow::Error> {
        let args = shlex::split("proc items contains name -m foo").unwrap();
        let mut filter = FilterProcessor::new(&args).await?;
        let val = json!({
            "items": [
                { "name": "foo_item" },
                { "name": "bar_item" },
                { "name": "foo_bar" }
            ]
        });
        let event = Rc::new(RefCell::new(val));
        let mut output = filter.process(event).await?;
        if let Some(out_event) = output.pop() {
            let output = out_event.borrow();
            if let Some(filtered) = output["items"].as_array() {
                assert_eq!(filtered.len(), 2);
            }
        }
        Ok(())
    }
}
