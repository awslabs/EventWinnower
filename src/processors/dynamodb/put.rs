use crate::processors::aws::get_aws_config;
use crate::processors::dynamodb::common::{execute_batch_write_with_retry, json_to_dynamodb_item};
use crate::processors::processor::*;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::types::WriteRequest;
use clap::Parser;
use eventwinnower_macros::AsyncProcessorInit;

#[derive(AsyncProcessorInit)]
pub struct DynamoDBPutProcessor<'a> {
    table_name: String,
    jmespath_expr: Option<jmespath::Expression<'a>>,
    dynamodb_client: aws_sdk_dynamodb::Client,
    writes: u64,
    throttles: u64,
    failures: u64,
    retry_backoff_ms: u64,
    batch_size: usize,
    total_time_us: u64,
}

#[derive(Parser)]
/// Write event data to DynamoDB using BatchWriteItem operation
#[command(version, long_about = None, arg_required_else_help(true))]
struct DynamoDBPutArgs {
    /// DynamoDB table name
    #[arg(required = true)]
    table_name: String,

    /// JMESPath expression to extract data from event (optional)
    #[arg(short, long)]
    jmespath: Option<String>,

    /// Initial backoff in milliseconds for exponential backoff retry logic
    #[arg(long, default_value_t = 100)]
    retry_backoff_ms: u64,

    /// AWS region for DynamoDB
    #[arg(short, long)]
    region: Option<String>,

    /// Batch size for BatchWriteItem (max 25)
    #[arg(long, default_value_t = 25)]
    batch_size: usize,
}

#[async_trait(?Send)]
impl<'a> AsyncProcessor for DynamoDBPutProcessor<'a> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("write event data to DynamoDB using BatchWriteItem".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = DynamoDBPutArgs::try_parse_from(argv)?;

        // Validate required parameters
        if args.table_name.is_empty() {
            return Err(anyhow!("table_name is required"));
        }

        if args.retry_backoff_ms == 0 {
            return Err(anyhow!("retry_backoff_ms must be greater than 0"));
        }

        if args.batch_size == 0 || args.batch_size > 25 {
            return Err(anyhow!("batch_size must be between 1 and 25"));
        }

        // Parse JMESPath expression if provided
        let jmespath_expr = if let Some(expr_str) = args.jmespath {
            Some(jmespath::compile(&expr_str)?)
        } else {
            None
        };

        // Initialize AWS DynamoDB client with optional region configuration
        let config = if let Some(region_str) = args.region {
            aws_config::defaults(BehaviorVersion::latest())
                .region(aws_sdk_dynamodb::config::Region::new(region_str))
                .load()
                .await
        } else {
            get_aws_config().await
        };
        let dynamodb_client = aws_sdk_dynamodb::Client::new(&config);

        Ok(DynamoDBPutProcessor {
            table_name: args.table_name,
            jmespath_expr,
            dynamodb_client,
            writes: 0,
            throttles: 0,
            failures: 0,
            retry_backoff_ms: args.retry_backoff_ms,
            batch_size: args.batch_size,
            total_time_us: 0,
        })
    }

    async fn process_batch(&mut self, events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        let start_time = std::time::Instant::now();

        // Convert events to DynamoDB items
        let mut items = Vec::new();
        for event in events {
            let data_to_write: serde_json::Value = if let Some(expr) = &self.jmespath_expr {
                let rcvar_result = expr.search(event)?;
                serde_json::from_str(&rcvar_result.to_string())?
            } else {
                event.borrow().clone()
            };

            // Validate that extracted data is a JSON object
            if !data_to_write.is_object() {
                let type_name = match &data_to_write {
                    serde_json::Value::Null => "null",
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::String(_) => "string",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::Object(_) => "object",
                };
                return Err(anyhow!("Extracted data must be a JSON object, got: {type_name}"));
            }

            let item = json_to_dynamodb_item(&data_to_write)?;
            items.push(item);
        }

        // Process items in batches of up to batch_size
        for chunk in items.chunks(self.batch_size) {
            let write_requests: Vec<WriteRequest> = chunk
                .iter()
                .map(|item| {
                    WriteRequest::builder()
                        .put_request(
                            aws_sdk_dynamodb::types::PutRequest::builder()
                                .set_item(Some(item.clone()))
                                .build()
                                .expect("Failed to build PutRequest"),
                        )
                        .build()
                })
                .collect();

            let throttles = execute_batch_write_with_retry(
                &self.dynamodb_client,
                &self.table_name,
                &write_requests,
                self.retry_backoff_ms,
            )
            .await?;

            self.throttles += throttles;
            self.writes += chunk.len() as u64;
        }

        // Accumulate elapsed time in microseconds
        self.total_time_us += start_time.elapsed().as_micros() as u64;

        // Return original events unchanged
        Ok(events.to_vec())
    }

    async fn stats(&self) -> Option<String> {
        let avg_time_us = if self.writes > 0 { self.total_time_us / self.writes } else { 0 };

        Some(format!(
            "writes:{}\nthrottles:{}\nfailures:{}\navg_time_per_record_usec:{}",
            self.writes, self.throttles, self.failures, avg_time_us
        ))
    }
}
