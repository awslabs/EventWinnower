use crate::processors::{aws::get_aws_config, processor::*};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use clap::Parser;
use eventwinnower_macros::AsyncProcessorInit;
use futures::future::join_all;
use std::fs;

use aws_sdk_bedrockruntime::{
    error::SdkError,
    operation::converse::ConverseError,
    types::{ContentBlock, ConversationRole, Message, SystemContentBlock},
    Client,
};

const MODEL_ID: &str = "global.anthropic.claude-sonnet-4-5-20250929-v1:0";

/// call bedrock GenAI model per event and output result
#[derive(AsyncProcessorInit)]
pub struct BedrockProcessor<'a> {
    prompt: String,
    path: Option<jmespath::Expression<'a>>,
    bedrock_client: Client,
    messages: u64,
    label: String,
    output_tags: Vec<String>,
    model_id: String,
    retry_throttle_seconds: u64,
    batch_size: usize,
}

#[derive(Parser)]
/// call bedrock GenAI model per event
#[command(version, long_about = None, arg_required_else_help(false))]
struct BedrockArgs {
    prompt: Option<String>,

    #[arg(short, long)]
    file_prompt: Option<String>,

    #[arg(short, long)]
    jmespath_subset: Option<String>,

    #[arg(short, long)]
    output_tags: Vec<String>,

    #[arg(short, long, default_value = MODEL_ID)]
    model_id: String,

    #[arg(short, long, default_value = "bedrock")]
    label: String,

    #[arg(short, long, default_value_t = 5)]
    retry_throttle_seconds: u64,

    #[arg(short, long, default_value_t = 10)]
    batch_size: usize,
}

#[async_trait(?Send)]
impl AsyncProcessor for BedrockProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("call Bedrock GenAI model per event".to_string())
    }
    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = BedrockArgs::try_parse_from(argv)?;

        let prompt = if let Some(prompt) = args.prompt {
            prompt
        } else if let Some(filename) = args.file_prompt {
            fs::read_to_string(filename)?
        } else {
            return Err(anyhow!("must specify bedrock prompt"));
        };

        let config = get_aws_config().await;
        let bedrock_client = Client::new(&config);

        let path = match args.jmespath_subset {
            Some(subset) => Some(jmespath::compile(&subset)?),
            _ => None,
        };

        Ok(Self {
            path,
            prompt,
            bedrock_client,
            messages: 0,
            label: args.label,
            output_tags: args.output_tags,
            model_id: args.model_id,
            retry_throttle_seconds: args.retry_throttle_seconds,
            batch_size: args.batch_size,
        })
    }

    async fn stats(&self) -> Option<String> {
        Some(format!("messages:{}", self.messages))
    }

    async fn process_batch(&mut self, events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        self.messages += events.len() as u64;
        let mut output_events: Vec<Event> = vec![];
        for chunk in events.chunks(self.batch_size) {
            let mut future_results = Vec::new();
            for event in chunk {
                future_results.push(Box::pin(self.process_async(event.clone())));
            }
            let join_result = join_all(future_results).await;
            let merged: Result<Vec<Vec<Event>>, anyhow::Error> = join_result.into_iter().collect();

            let mut results = match merged {
                Ok(rvec) => rvec.into_iter().flatten().collect(),
                Err(e) => return Err(e),
            };
            output_events.append(&mut results);
        }
        Ok(output_events)
    }
}

impl BedrockProcessor<'_> {
    async fn process_async(&self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        let data = match &self.path {
            Some(path) => path.search(&input)?.to_string(),
            _ => serde_json::to_string(&input)?,
        };

        let data_block = ContentBlock::Text(data);
        loop {
            let response = self
                .bedrock_client
                .converse()
                .model_id(&self.model_id)
                .system(SystemContentBlock::Text(self.prompt.clone()))
                .messages(
                    Message::builder()
                        .role(ConversationRole::User)
                        .content(data_block.clone())
                        .build()?,
                )
                .send()
                .await;

            let response = match response {
                Ok(response) => response,
                Err(e) => match e {
                    SdkError::ServiceError(ref se) => match se.err() {
                        ConverseError::ThrottlingException(_) => {
                            eprintln!("throttling exception, retrying");
                            tokio::time::sleep(std::time::Duration::from_secs(
                                self.retry_throttle_seconds,
                            ))
                            .await;
                            continue;
                        }
                        _ => {
                            return Err(anyhow!("error calling bedrock: {:?}", e));
                        }
                    },
                    _ => {
                        return Err(anyhow!("error calling bedrock: {:?}", e));
                    }
                },
            };

            let output_text = response
                .output()
                .ok_or(anyhow!("no output"))?
                .as_message()
                .map_err(|_| anyhow!("output not a message"))?
                .content()
                .first()
                .ok_or(anyhow!("no content in message"))?
                .as_text()
                .map_err(|_| anyhow!("content is not text"))?
                .to_string();

            if let Some(event) = input.borrow_mut().as_object_mut() {
                for tag in &self.output_tags {
                    if let Some(finding) = self.get_tag(tag, &output_text) {
                        event.insert(tag.clone(), serde_json::Value::String(finding.clone()));
                    }
                }

                let mut out_array: Vec<serde_json::Value> = Vec::new();
                for line in output_text.lines() {
                    if !line.is_empty() {
                        out_array.push(serde_json::Value::String(line.to_string()));
                    }
                }
                if !out_array.is_empty() {
                    event.insert(self.label.clone(), serde_json::Value::Array(out_array));
                }
            }
            return Ok(vec![input]);
        }
    }

    fn get_tag(&self, tag: &str, source: &str) -> Option<String> {
        let tag_start = format!("<{tag}>");
        let tag_end = format!("</{tag}>");

        if let Some(start_position) = source.find(&tag_start) {
            let search_from = start_position + tag_start.len();
            let source = &source[search_from..];
            if let Some(end_position) = source.find(&tag_end) {
                return Some(source[..end_position].to_string());
            }
        }
        None
    }
}
