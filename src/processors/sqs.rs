use crate::processors::aws::get_aws_config;
use crate::processors::processor::*;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use clap::Parser;
use eventwinnower_macros::{AsyncProcessorInit, AsyncSourceProcessorInit};
use serde_json::Value;
use std::cell::RefCell;
use std::rc::Rc;

use aws_sdk_sqs as sqs;

use aws_sdk_sqs::config::http::HttpResponse;
use aws_sdk_sqs::error::SdkError;
use aws_sdk_sqs::operation::delete_message::DeleteMessageError;
use aws_sdk_sqs::operation::delete_message::DeleteMessageOutput;
use aws_sdk_sqs::operation::get_queue_url::GetQueueUrlError;
use aws_sdk_sqs::operation::get_queue_url::GetQueueUrlOutput;
use aws_sdk_sqs::operation::receive_message::ReceiveMessageError;
use aws_sdk_sqs::operation::receive_message::ReceiveMessageOutput;

#[allow(unused_imports)]
use mockall::automock;

#[allow(unused_imports)]
#[cfg(test)]
pub use MockSqsImpl as Sqs;

#[allow(unused_imports)]
#[cfg(not(test))]
pub use SqsImpl as Sqs;

#[allow(dead_code)]
pub struct SqsImpl {
    inner: aws_sdk_sqs::Client,
}

#[cfg_attr(test, automock)]
impl SqsImpl {
    #[allow(dead_code)]
    pub fn new(inner: aws_sdk_sqs::Client) -> Self {
        Self { inner }
    }

    #[allow(dead_code)]
    pub async fn get_queue_url(
        &self,
        queue_name: &str,
    ) -> Result<GetQueueUrlOutput, SdkError<GetQueueUrlError, HttpResponse>> {
        self.inner.get_queue_url().queue_name(queue_name).send().await
    }

    #[allow(dead_code)]
    pub async fn receive_message(
        &self,
        queue_url: &str,
        wait_time: i32,
        max_messages: i32,
    ) -> Result<ReceiveMessageOutput, SdkError<ReceiveMessageError, HttpResponse>> {
        self.inner
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(max_messages)
            .wait_time_seconds(wait_time)
            .send()
            .await
    }

    #[allow(dead_code)]
    pub async fn delete_message(
        &self,
        queue_url: &str,
        handle: &str,
    ) -> Result<DeleteMessageOutput, SdkError<DeleteMessageError, HttpResponse>> {
        self.inner.delete_message().queue_url(queue_url).receipt_handle(handle).send().await
    }
}

/// read a list of s3 objects from stdin
#[derive(AsyncSourceProcessorInit)]
pub struct SqsInputProcessor {
    queue_name: String,
    queue_url: Option<String>,
    sqs_client: Sqs,
    messages: u64,
    wait_time: i32,
    keep_polling: bool,
    timeouts: u64,
    sns_message_parse: Option<String>,
}

#[derive(Parser)]
/// read from an SQS queue
#[command(version, long_about = None, arg_required_else_help(true))]
struct SqsInputArgs {
    #[arg(required(true))]
    queue_name: String,

    #[arg(short, long, default_value_t = 20)]
    wait_time_seconds: i32,

    #[arg(short, long)]
    keep_polling: bool,

    #[arg(short, long)]
    sns_message_parse: Option<String>,
}

impl SqsInputProcessor {
    async fn receive_message(&mut self) -> Result<Vec<sqs::types::Message>, anyhow::Error> {
        if self.queue_url.is_none() {
            let queue_url_result = self.sqs_client.get_queue_url(&self.queue_name).await?;
            self.queue_url = queue_url_result.queue_url().map(|url| url.to_string());
        }

        if let Some(queue_url) = &self.queue_url {
            let recv_message_output =
                self.sqs_client.receive_message(queue_url, self.wait_time, 1).await?;

            Ok(recv_message_output.messages.unwrap_or_default())
        } else {
            Err(anyhow!("Queue Url not found for queue_name: {}", self.queue_name))
        }
    }
}

pub(crate) fn sns_message_body(message_str: &str) -> Result<Value, anyhow::Error> {
    let message_body: Value = serde_json::from_str(message_str)?;
    if let Some(msg) = message_body.get("Message") {
        Ok(serde_json::from_str(msg.as_str().unwrap_or_default())?)
    } else {
        Ok(serde_json::Value::Null)
    }
}

fn decode_url_string(s: &str) -> String {
    urlencoding::decode(s).map(|decoded| decoded.to_string()).unwrap_or_else(|_| s.to_string())
}

pub(crate) fn message_to_s3_bucket_key(
    message_str: &str,
) -> Result<Option<(String, String)>, anyhow::Error> {
    let message_body: Value = serde_json::from_str(message_str)?;
    if let Some(msg) = message_body.get("Message") {
        let message_parsed: Value = serde_json::from_str(
            msg.as_str().ok_or_else(|| anyhow::anyhow!("SNS Message field is not a string"))?,
        )?;

        if let Some(serde_json::Value::Array(records)) = message_parsed.get("Records") {
            if records.is_empty() {
                return Ok(None);
            }
            let rec = &records[0];

            if let Some(s3) = rec.get("s3") {
                let bucket = s3.get("bucket").and_then(|bucket| {
                    bucket.get("name").and_then(|name| name.as_str().map(|s| s.to_string()))
                });
                let key = s3.get("object").and_then(|obj| {
                    obj.get("key").and_then(|key| key.as_str().map(decode_url_string))
                });
                if let (Some(bucket_val), Some(key_val)) = (bucket, key) {
                    return Ok(Some((bucket_val, key_val)));
                }
            }
        }
    }
    Ok(None)
}

#[async_trait(?Send)]
impl AsyncSourceProcessor for SqsInputProcessor {
    //call to get a single new event - keep looping unti no more files to read
    /// returns Ok(vec![]) if we are done with processing files
    async fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        let mut out: Vec<Event> = Vec::new();

        for message in self.receive_message().await? {
            let mut event = serde_json::Map::new();

            if let Some(body) = message.body {
                if let Some(sns_parse) = &self.sns_message_parse {
                    event.insert(sns_parse.clone(), sns_message_body(&body)?);
                } else if let Some((bucket, key)) = message_to_s3_bucket_key(&body)? {
                    event.insert("bucket_name".to_string(), serde_json::to_value(bucket)?);
                    event.insert("object_key".to_string(), serde_json::to_value(key)?);
                } else {
                    event.insert("sqs_body".to_string(), serde_json::to_value(body)?);
                }
            }
            if let Some(receipt_handle) = message.receipt_handle {
                event.insert(
                    "sqs_receipt_handle".to_string(),
                    serde_json::to_value(receipt_handle)?,
                );
            }
            if let Some(attributes) = message.attributes {
                event.insert(
                    "sqs_sent_timestamp".to_string(),
                    serde_json::to_value(
                        &attributes[&sqs::types::MessageSystemAttributeName::SentTimestamp],
                    )?,
                );
            }
            if !event.is_empty() {
                let output_value: serde_json::Value = event.into();
                out.push(Rc::new(RefCell::new(output_value)));
                self.messages += 1;
            }
        }

        if out.is_empty() && self.keep_polling {
            let poll_output = serde_json::Value::String("sqs_polling_timeout".to_string());
            out.push(Rc::new(RefCell::new(poll_output)));
            self.timeouts += 1;
        }

        Ok(out)
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("read messages from SQS queue".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = SqsInputArgs::try_parse_from(argv)?;
        let config = get_aws_config().await;
        let sqs_client = Sqs::new(sqs::Client::new(&config));
        Ok(SqsInputProcessor {
            sqs_client,
            queue_name: args.queue_name,
            queue_url: None,
            messages: 0,
            wait_time: args.wait_time_seconds,
            keep_polling: args.keep_polling,
            timeouts: 0,
            sns_message_parse: args.sns_message_parse,
        })
    }

    async fn stats(&self) -> Option<String> {
        Some(format!("messages:{}\ntimeouts:{}", self.messages, self.timeouts))
    }
}

#[derive(AsyncProcessorInit)]
pub struct SqsRemoveProcessor {
    sqs_client: Sqs,
    queue_url: Option<String>,
    messages: u64,
    handle: String,
    queue_name: String,
}

#[derive(Parser)]
/// removed messages from an SQS queue based on handle
#[command(version, about, long_about = None, arg_required_else_help(true))]
struct SqsRemoveArgs {
    #[arg(required(true))]
    queue_name: String,

    #[arg(short, long, default_value = "sqs_receipt_handle")]
    receipt_handle_label: String,
}

#[async_trait(?Send)]
impl AsyncProcessor for SqsRemoveProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("remove messages from SQS queue".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = SqsRemoveArgs::try_parse_from(argv)?;
        let config = get_aws_config().await;
        let sqs_client = Sqs::new(sqs::Client::new(&config));
        Ok(Self {
            sqs_client,
            messages: 0,
            handle: args.receipt_handle_label,
            queue_name: args.queue_name,
            queue_url: None,
        })
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        if let Some(event) = input.borrow().as_object() {
            if let Some(handle) = event.get(&self.handle) {
                if let Some(hvalstring) = handle.as_str() {
                    self.remove_handle(hvalstring).await?;
                } else if let Some(handle_array) = handle.as_array() {
                    for hval in handle_array.iter() {
                        if let Some(hvalstring) = hval.as_str() {
                            self.remove_handle(hvalstring).await?;
                        }
                    }
                }
            }
        }
        Ok(vec![input])
    }

    async fn stats(&self) -> Option<String> {
        Some(format!("messages:{}", self.messages))
    }
}

impl SqsRemoveProcessor {
    async fn remove_handle(&mut self, handle: &str) -> Result<(), anyhow::Error> {
        if self.queue_url.is_none() {
            let queue_url_result = self.sqs_client.get_queue_url(&self.queue_name).await?;
            self.queue_url = queue_url_result.queue_url().map(|url| url.to_string());
        }

        if let Some(queue_url) = &self.queue_url {
            self.sqs_client.delete_message(queue_url, handle).await?;
            self.messages += 1;
            Ok(())
        } else {
            Err(anyhow!("Queue Url not found for queue_name: {}", self.queue_name))
        }
    }
}
