use crate::processors::aws::get_aws_config;
use crate::processors::processor::*;
use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use eventwinnower_macros::AsyncProcessorInit;

use crate::processors::defanger::defang_urls;
use aws_sdk_firehose as firehose;
use aws_sdk_firehose::config::http::HttpResponse;
use aws_sdk_firehose::error::SdkError;
use aws_sdk_firehose::operation::{
    put_record_batch::PutRecordBatchError, put_record_batch::PutRecordBatchOutput,
};
use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::Record;

#[allow(unused_imports)]
use mockall::automock;

#[allow(unused_imports)]
#[cfg(test)]
pub use MockFirehoseImpl as Firehose;

#[allow(unused_imports)]
#[cfg(not(test))]
pub use FirehoseImpl as Firehose;

#[allow(dead_code)]
pub struct FirehoseImpl {
    inner: firehose::Client,
}

#[cfg_attr(test, automock)]
impl FirehoseImpl {
    #[allow(dead_code)]
    pub fn new(inner: firehose::Client) -> Self {
        Self { inner }
    }

    #[allow(dead_code)]
    pub async fn put_record_batch(
        &self,
        stream_name: &str,
        records: Vec<Record>,
    ) -> Result<PutRecordBatchOutput, SdkError<PutRecordBatchError, HttpResponse>> {
        self.inner
            .put_record_batch()
            .delivery_stream_name(stream_name)
            .set_records(Some(records))
            .send()
            .await
    }
}

/// output serialized json to firehose stream
#[derive(AsyncProcessorInit)]
pub struct FirehoseOutputProcessor {
    stream_name: String,
    firehose_client: Firehose,
    messages: u64,
    defang_urls: bool,
}

#[derive(Parser)]
/// write serialized json objects to a firehose stream
#[command(version, long_about = None, arg_required_else_help(true))]
struct FirehoseOutputArgs {
    #[arg(required(true))]
    stream_name: String,

    #[arg(short, long)]
    defang_urls: bool,
}

#[async_trait(?Send)]
impl AsyncProcessor for FirehoseOutputProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("send events to Kinesis Data Firehose".to_string())
    }
    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = FirehoseOutputArgs::try_parse_from(argv)?;
        let config = get_aws_config().await;
        let firehose_client = Firehose::new(firehose::Client::new(&config));

        Ok(Self {
            stream_name: args.stream_name,
            firehose_client,
            messages: 0,
            defang_urls: args.defang_urls,
        })
    }

    async fn process_batch(&mut self, events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        let mut records = Vec::new();
        for event in events {
            let serialized_json = serde_json::to_string(event)?;

            let output_json =
                if self.defang_urls { defang_urls(&serialized_json) } else { serialized_json };

            let mut data = output_json.into_bytes();
            data.push(b'\n');
            records.push(Record::builder().data(Blob::new(data)).build()?);
            self.messages += 1;
        }
        if records.is_empty() {
            return Ok(events.to_vec());
        }
        self.firehose_client.put_record_batch(&self.stream_name, records).await?;
        Ok(events.to_vec())
    }

    async fn stats(&self) -> Option<String> {
        Some(format!("messages:{}", self.messages))
    }
}
