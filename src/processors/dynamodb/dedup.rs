use crate::processors::aws::get_aws_config;
use crate::processors::dynamodb::common::{
    ensure_table_exists, execute_batch_get_item_with_retry, execute_batch_write_with_retry,
};
use crate::processors::processor::*;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::types::WriteRequest;
use clap::Parser;
use eventwinnower_macros::AsyncProcessorInit;
use lru::LruCache;
use std::collections::HashMap;
use std::num::NonZeroUsize;

#[derive(AsyncProcessorInit)]
pub struct DynamoDBDedupProcessor<'a> {
    table_name: String,
    key_jmespath: jmespath::Expression<'a>,
    key_attribute_name: String,
    ttl_seconds: Option<u64>,
    ttl_attribute_name: String,
    dynamodb_client: aws_sdk_dynamodb::Client,
    gets: u64,
    puts: u64,
    throttles: u64,
    retry_backoff_ms: u64,
    batch_size: usize,
    total_time_us: u64,
    deduped_count: u64,
    new_count: u64,
    ttl_refreshed_count: u64,
    state: LruCache<String, bool>,
    cache_hits: u64,
    input: u64,
}

#[derive(Parser)]
/// Deduplicate events using DynamoDB as a state cache
#[command(version, long_about = None, arg_required_else_help(true))]
struct DynamoDBDedupArgs {
    /// DynamoDB table name
    #[arg(required = true)]
    table_name: String,

    /// JMESPath expression to extract dedup key from event (required)
    #[arg(short, long)]
    key: String,

    /// DynamoDB key attribute field name (default: dedup_key)
    #[arg(long, default_value = "key")]
    key_attribute_name: String,

    /// TTL duration in seconds (optional, calculates expiration as current_time + ttl_seconds)
    #[arg(long)]
    ttl_seconds: Option<u64>,

    /// DynamoDB attribute name for TTL (default: ttl)
    #[arg(long, default_value = "ttl")]
    ttl_attribute_name: String,

    /// Initial backoff in milliseconds for exponential backoff retry logic
    #[arg(long, default_value_t = 500)]
    retry_backoff_ms: u64,

    /// AWS region for DynamoDB
    #[arg(short, long)]
    region: Option<String>,

    /// Batch size for BatchWriteItem (max 25)
    #[arg(long, default_value_t = 25)]
    batch_size: usize,

    /// Local LRU cache size for deduplication (default: 10000)
    #[arg(long, default_value_t = NonZeroUsize::new(100000).unwrap())]
    local_cache_size: NonZeroUsize,

    /// Create the DynamoDB table if it doesn't exist
    #[arg(long)]
    create_table: bool,
}

#[async_trait(?Send)]
impl<'a> AsyncProcessor for DynamoDBDedupProcessor<'a> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("deduplicate events using DynamoDB as a state cache".to_string())
    }

    async fn new_with_instance_id(
        argv: &[String],
        _instance_id: usize,
    ) -> Result<Self, anyhow::Error> {
        let args = DynamoDBDedupArgs::try_parse_from(argv)?;

        // Validate required parameters
        if args.table_name.is_empty() {
            return Err(anyhow!("table_name is required"));
        }

        if args.key.is_empty() {
            return Err(anyhow!("key JMESPath expression is required"));
        }

        if args.retry_backoff_ms == 0 {
            return Err(anyhow!("retry_backoff_ms must be greater than 0"));
        }

        if args.batch_size == 0 || args.batch_size > 25 {
            return Err(anyhow!("batch_size must be between 1 and 25"));
        }

        // Compile JMESPath expressions
        let key_jmespath = jmespath::compile(&args.key)?;

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

        // Ensure table exists if requested
        if args.create_table {
            let ttl_field = args.ttl_seconds.map(|_| args.ttl_attribute_name.as_str());
            ensure_table_exists(
                &dynamodb_client,
                &args.table_name,
                &args.key_attribute_name,
                ttl_field,
            )
            .await?;
        }

        let state = LruCache::new(args.local_cache_size);

        Ok(DynamoDBDedupProcessor {
            table_name: args.table_name,
            key_jmespath,
            key_attribute_name: args.key_attribute_name,
            ttl_seconds: args.ttl_seconds,
            ttl_attribute_name: args.ttl_attribute_name,
            dynamodb_client,
            gets: 0,
            puts: 0,
            throttles: 0,
            retry_backoff_ms: args.retry_backoff_ms,
            batch_size: args.batch_size,
            total_time_us: 0,
            deduped_count: 0,
            new_count: 0,
            ttl_refreshed_count: 0,
            state,
            cache_hits: 0,
            input: 0,
        })
    }

    async fn process_batch(&mut self, events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        let start_time = std::time::Instant::now();

        self.input += events.len() as u64;

        // Filter events using local cache first, treating keys as strings
        let filtered_events = self.filter_events_local_cache(events)?;

        let result = if !filtered_events.is_empty() {
            self.process_batch_dynamo(&filtered_events).await?
        } else {
            vec![]
        };

        // Accumulate elapsed time in microseconds
        self.total_time_us += start_time.elapsed().as_micros() as u64;

        Ok(result)
    }

    async fn stats(&self) -> Option<String> {
        let avg_time_us = if self.input > 0 { self.total_time_us / self.input } else { 0 };

        Some(format!(
            "input:{}\ngets:{}\nputs:{}\nthrottles:{}\ndeduped:{}\nnew:{}\nttl_refreshed:{}\nlocal_cache_hits:{}\navg_time_per_operation_usec:{avg_time_us}",
            self.input, self.gets, self.puts, self.throttles, self.deduped_count, self.new_count, self.ttl_refreshed_count, self.cache_hits
        ))
    }
}

impl<'a> DynamoDBDedupProcessor<'a> {
    fn filter_events_local_cache(
        &mut self,
        events: &[Event],
    ) -> Result<Vec<(Event, String)>, anyhow::Error> {
        // Filter events using local cache first, treating keys as strings
        let mut filtered_events: Vec<(Event, String)> = Vec::new();

        for event in events {
            let key_result = self.key_jmespath.search(event)?;
            let key_string = key_result.to_string();

            // Check local cache first
            if self.state.put(key_string.clone(), true).is_some() {
                // Cache hit - this key was already seen locally, drop it
                self.cache_hits += 1;
                self.deduped_count += 1;
            } else {
                // Cache miss - need to query DynamoDB
                filtered_events.push((event.clone(), key_string));
            }
        }
        Ok(filtered_events)
    }

    /// Process batch of events asynchronously (already filtered by local cache)
    async fn process_batch_dynamo(
        &mut self,
        filtered_events: &[(Event, String)],
    ) -> Result<Vec<Event>, anyhow::Error> {
        // Build DynamoDB keys from string keys
        let dynamodb_keys: Vec<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>> =
            filtered_events
                .iter()
                .map(|(_, key_string)| {
                    let mut item = HashMap::new();
                    item.insert(
                        self.key_attribute_name.clone(),
                        aws_sdk_dynamodb::types::AttributeValue::S(key_string.clone()),
                    );
                    item
                })
                .collect();

        let mut new_events = Vec::new();

        // Batch get items from DynamoDB to check which keys exist
        let (existing_items, throttles) = execute_batch_get_item_with_retry(
            &self.dynamodb_client,
            &self.table_name,
            dynamodb_keys.clone(),
            self.retry_backoff_ms,
        )
        .await?;
        self.throttles += throttles;
        self.gets += dynamodb_keys.len() as u64;

        // Build a map of existing items for quick lookup
        let mut existing_items_map: HashMap<
            String,
            HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
        > = HashMap::new();

        for item in &existing_items {
            if let Some(aws_sdk_dynamodb::types::AttributeValue::S(key_str)) =
                item.get(&self.key_attribute_name)
            {
                existing_items_map.insert(key_str.clone(), item.clone());
            }
        }

        // Get current time once for TTL calculations
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| anyhow!("Failed to get current time: {e}"))?
            .as_secs();

        // Collect items to write (new items and TTL refreshes)
        let mut write_items = Vec::new();

        for (event, key_string) in filtered_events.iter() {
            if !existing_items_map.contains_key(key_string) {
                // This is a new item - store the key and TTL
                let mut item = HashMap::new();
                item.insert(
                    self.key_attribute_name.clone(),
                    aws_sdk_dynamodb::types::AttributeValue::S(key_string.clone()),
                );

                // Add TTL attribute if provided
                if let Some(ttl_secs) = self.ttl_seconds {
                    let expiration_time = current_time + ttl_secs;
                    item.insert(
                        self.ttl_attribute_name.clone(),
                        aws_sdk_dynamodb::types::AttributeValue::N(expiration_time.to_string()),
                    );
                }

                write_items.push(item);
                new_events.push(event.clone());
                self.new_count += 1;
            } else {
                // This is an existing item - check if TTL needs refresh
                if let Some(ttl_secs) = self.ttl_seconds {
                    let refresh_threshold = current_time + (ttl_secs / 2);

                    if let Some(existing_item) = existing_items_map.get_mut(key_string) {
                        if let Some(aws_sdk_dynamodb::types::AttributeValue::N(ttl_str)) =
                            existing_item.get_mut(&self.ttl_attribute_name)
                        {
                            if let Ok(ttl_value) = ttl_str.parse::<u64>() {
                                // If TTL expires before refresh threshold, refresh it
                                if ttl_value < refresh_threshold {
                                    let new_expiration = current_time + ttl_secs;
                                    *ttl_str = new_expiration.to_string();
                                    write_items.push(existing_item.clone());
                                    self.ttl_refreshed_count += 1;
                                }
                            }
                        }
                    }
                }

                self.deduped_count += 1;
            }
        }

        // Batch write items to DynamoDB (new items and TTL refreshes)
        if !write_items.is_empty() {
            for chunk in write_items.chunks(self.batch_size) {
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
                self.puts += chunk.len() as u64;
            }
        }

        Ok(new_events)
    }
}
