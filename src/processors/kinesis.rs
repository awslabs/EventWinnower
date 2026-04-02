use crate::processors::aws::get_aws_config;
use crate::processors::processor::*;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use clap::Parser;
use eventwinnower_macros::{AsyncProcessorInit, AsyncSourceProcessorInit};
use rand::{distr::Alphanumeric, RngExt};
use serde_json::Value;
use std::cell::RefCell;
use std::rc::Rc;

use aws_sdk_kinesis as kinesis;
use aws_sdk_kinesis::config::http::HttpResponse;
use aws_sdk_kinesis::error::SdkError;
use aws_sdk_kinesis::operation::{
    create_stream::CreateStreamError, create_stream::CreateStreamOutput,
    delete_stream::DeleteStreamError, delete_stream::DeleteStreamOutput,
    describe_stream::DescribeStreamError, describe_stream::DescribeStreamOutput,
    get_records::GetRecordsError, get_records::GetRecordsOutput,
    get_shard_iterator::GetShardIteratorError, get_shard_iterator::GetShardIteratorOutput,
    put_record::PutRecordError, put_record::PutRecordOutput, put_records::PutRecordsError,
    put_records::PutRecordsOutput,
};
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::{PutRecordsRequestEntry, ShardIteratorType};

#[allow(unused_imports)]
use mockall::automock;

#[allow(unused_imports)]
#[cfg(test)]
pub use MockKinesisImpl as Kinesis;

#[allow(unused_imports)]
#[cfg(not(test))]
pub use KinesisImpl as Kinesis;

#[allow(dead_code)]
pub struct KinesisImpl {
    inner: kinesis::Client,
}

#[cfg_attr(test, automock)]
impl KinesisImpl {
    #[allow(dead_code)]
    pub fn new(inner: kinesis::Client) -> Self {
        Self { inner }
    }

    #[allow(dead_code)]
    pub async fn create_stream(
        &self,
        stream_name: &str,
        shard_count: i32,
    ) -> Result<CreateStreamOutput, SdkError<CreateStreamError, HttpResponse>> {
        self.inner.create_stream().stream_name(stream_name).shard_count(shard_count).send().await
    }

    #[allow(dead_code)]
    pub async fn delete_stream(
        &self,
        stream_arn: &str,
    ) -> Result<DeleteStreamOutput, SdkError<DeleteStreamError, HttpResponse>> {
        self.inner.delete_stream().stream_arn(stream_arn).send().await
    }

    #[allow(dead_code)]
    pub async fn describe_stream(
        &self,
        stream_name: &str,
        limit: Option<i32>,
    ) -> Result<DescribeStreamOutput, SdkError<DescribeStreamError, HttpResponse>> {
        self.inner.describe_stream().stream_name(stream_name).set_limit(limit).send().await
    }

    #[allow(dead_code)]
    pub async fn get_shard_iterator_latest(
        &self,
        stream_arn: &str,
        shard_id: &str,
    ) -> Result<GetShardIteratorOutput, SdkError<GetShardIteratorError, HttpResponse>> {
        self.inner
            .get_shard_iterator()
            .stream_arn(stream_arn)
            .shard_id(shard_id)
            .shard_iterator_type(ShardIteratorType::Latest)
            .send()
            .await
    }

    #[allow(dead_code)]
    pub async fn put_record(
        &self,
        stream_arn: &str,
        data: Blob,
        partition_key: &str,
    ) -> Result<PutRecordOutput, SdkError<PutRecordError, HttpResponse>> {
        self.inner
            .put_record()
            .stream_arn(stream_arn)
            .data(data)
            .partition_key(partition_key)
            .send()
            .await
    }

    #[allow(dead_code)]
    pub async fn put_records(
        &self,
        stream_arn: &str,
        records: Vec<PutRecordsRequestEntry>,
    ) -> Result<PutRecordsOutput, SdkError<PutRecordsError, HttpResponse>> {
        self.inner.put_records().stream_arn(stream_arn).set_records(Some(records)).send().await
    }

    #[allow(dead_code)]
    pub async fn get_records(
        &self,
        stream_arn: &str,
        shard_iterator: &str,
        limit: Option<i32>,
    ) -> Result<GetRecordsOutput, SdkError<GetRecordsError, HttpResponse>> {
        self.inner
            .get_records()
            .stream_arn(stream_arn)
            .shard_iterator(shard_iterator)
            .set_limit(limit)
            .send()
            .await
    }
}

/// read kinesis objects
#[derive(AsyncSourceProcessorInit)]
pub struct KinesisInputProcessor {
    stream_arn: String,
    shard_id: String,
    shard_iterator: Option<String>,
    kinesis_client: Kinesis,
    messages: u64,
    poll_count: u64,
    poll_ms: u64,
    loop_poll_count: usize,
}

#[derive(Parser)]
/// read objects from Kinesis
#[command(version, long_about = None, arg_required_else_help(true))]
struct KinesisInputArgs {
    #[arg(required(true))]
    stream_name: String,

    #[arg(short, long, default_value_t = 0)]
    shard: usize,

    #[arg(short, long, default_value_t = 500)]
    poll_ms: u64,

    #[arg(short, long, default_value_t = 60)]
    loop_poll_count: usize,
}

#[async_trait(?Send)]
impl AsyncSourceProcessor for KinesisInputProcessor {
    // call to get a single new event - keep looping unti no more files to read
    /// returns Ok(vec![]) if we are done with processing files
    async fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        let mut out: Vec<Event> = Vec::new();
        let mut loop_count = 0;
        loop {
            let shard_iterator = if let Some(shard_iterator) = &self.shard_iterator {
                shard_iterator
            } else {
                let shard_iterator_output = self
                    .kinesis_client
                    .get_shard_iterator_latest(&self.stream_arn, &self.shard_id)
                    .await?;
                if let Some(shard_iterator) = shard_iterator_output.shard_iterator {
                    self.shard_iterator = Some(shard_iterator.clone());
                    &shard_iterator.to_owned()
                } else {
                    return Err(anyhow!("No shard iterator found"));
                }
            };

            let records_output =
                self.kinesis_client.get_records(&self.stream_arn, shard_iterator, None).await?;

            if let Some(next_shard_iterator) = records_output.next_shard_iterator {
                self.shard_iterator = Some(next_shard_iterator.clone());
            } else {
                self.shard_iterator = None;
            }

            for record in records_output.records {
                self.messages += 1;
                let parsed: Value = serde_json::from_slice(&record.data.into_inner())?;

                out.push(Rc::new(RefCell::new(parsed)));
            }

            if !out.is_empty() {
                return Ok(out);
            } else if loop_count > self.loop_poll_count {
                return Ok(vec![]);
            } else {
                self.poll_count += 1;
                tokio::time::sleep(tokio::time::Duration::from_millis(self.poll_ms)).await;
            }
            loop_count += 1;
        }
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("read events from Kinesis stream".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = KinesisInputArgs::try_parse_from(argv)?;
        let config = get_aws_config().await;
        let kinesis_client = Kinesis::new(kinesis::Client::new(&config));

        let stream = kinesis_client.describe_stream(&args.stream_name, None).await?;

        let (stream_arn, shard_id) = if let Some(desc) = stream.stream_description {
            if desc.shards.len() > args.shard {
                let shard = &desc.shards[args.shard];
                (desc.stream_arn.clone(), shard.shard_id.clone())
            } else {
                return Err(anyhow!(
                    "No stream shard exceeds shard length for stream: {}:{}",
                    args.stream_name,
                    args.shard
                ));
            }
        } else {
            return Err(anyhow!("No stream description found for stream: {}", args.stream_name));
        };

        Ok(KinesisInputProcessor {
            stream_arn,
            shard_id,
            shard_iterator: None,
            kinesis_client,
            messages: 0,
            poll_count: 0,
            poll_ms: args.poll_ms,
            loop_poll_count: args.loop_poll_count,
        })
    }

    async fn stats(&self) -> Option<String> {
        Some(format!("messages:{}\npoll_count:{}", self.messages, self.poll_count))
    }
}

/// output serialized json to Kinesis stream
#[derive(AsyncProcessorInit)]
pub struct KinesisOutputProcessor<'a> {
    stream_arn: String,
    kinesis_client: Kinesis,
    partition_key_path: Option<jmespath::Expression<'a>>,
    messages: u64,
    delete: bool,
}

#[derive(Parser)]
/// write serialized json objects to a kinesis stream
#[command(version, long_about = None, arg_required_else_help(true))]
struct KinesisOutputArgs {
    #[arg(required(true))]
    stream_name: String,

    #[arg(short, long)]
    partition_key_path: Option<String>,

    #[arg(short, long)]
    create: bool,

    #[arg(short, long, default_value_t = 1)]
    shards: i32,

    #[arg(short, long)]
    delete: bool,
}

#[async_trait(?Send)]
impl AsyncProcessor for KinesisOutputProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("send events to Kinesis stream".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = KinesisOutputArgs::try_parse_from(argv)?;
        let partition_key_path = if let Some(path) = args.partition_key_path {
            Some(jmespath::compile(path.as_str())?)
        } else {
            None
        };
        let config = get_aws_config().await;
        let kinesis_client = Kinesis::new(kinesis::Client::new(&config));

        if args.create {
            kinesis_client.create_stream(&args.stream_name, args.shards).await?;
            debug_log!("Created stream: {}", args.stream_name);
            tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
        }
        let stream = kinesis_client.describe_stream(&args.stream_name, None).await?;
        let stream_arn = if let Some(desc) = stream.stream_description {
            debug_log!("stream arn: {}", desc.stream_arn);
            desc.stream_arn.clone()
        } else {
            return Err(anyhow!("No stream description found for stream: {}", args.stream_name));
        };

        Ok(Self {
            stream_arn,
            kinesis_client,
            partition_key_path,
            messages: 0,
            delete: (args.create && args.delete),
        })
    }

    async fn process_batch(&mut self, events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        let mut records: Vec<PutRecordsRequestEntry> = Vec::new();
        for event in events {
            let partition_key = if let Some(path) = &self.partition_key_path {
                path.search(event)?.to_string()
            } else {
                rand::rng().sample_iter(&Alphanumeric).take(8).map(char::from).collect::<String>()
            };
            let data = serde_json::to_vec(event)?;
            records.push(
                PutRecordsRequestEntry::builder()
                    .data(Blob::new(data))
                    .partition_key(partition_key)
                    .build()?,
            );
        }
        self.kinesis_client.put_records(&self.stream_arn, records).await?;
        Ok(events.to_vec())
    }

    async fn flush(&mut self) -> Vec<Event> {
        if self.delete {
            let result = self.kinesis_client.delete_stream(&self.stream_arn).await;
            if result.is_err() {
                eprintln!("Failed to delete stream: {}", self.stream_arn);
            } else {
                debug_log!("Deleted stream: {}", self.stream_arn);
                self.delete = false;
            }
        }
        vec![]
    }

    async fn stats(&self) -> Option<String> {
        Some(format!("messages:{}", self.messages))
    }
}
