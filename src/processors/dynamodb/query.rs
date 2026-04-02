use crate::processors::aws::get_aws_config;
use crate::processors::dynamodb::common::{
    attribute_value_to_json, execute_batch_get_item_with_retry, execute_get_item_with_retry,
    json_to_dynamodb_item,
};
use crate::processors::processor::*;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use clap::Parser;
use eventwinnower_macros::AsyncProcessorInit;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Clone, Copy)]
pub enum QueryType {
    GetItem,
    Query,
    Scan,
}

impl FromStr for QueryType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "getitem" | "get_item" | "get" => Ok(QueryType::GetItem),
            "query" => Ok(QueryType::Query),
            "scan" => Ok(QueryType::Scan),
            _ => Err(anyhow!("Invalid query type: {s}. Must be 'getitem', 'query', or 'scan'")),
        }
    }
}

#[derive(AsyncProcessorInit)]
pub struct DynamoDBQueryProcessor<'a> {
    table_name: String,
    query_type: QueryType,
    jmespath_expr: Option<jmespath::Expression<'a>>,
    dynamodb_client: aws_sdk_dynamodb::Client,
    queries: u64,
    throttles: u64,
    failures: u64,
    retry_backoff_ms: u64,
    label: String,
    pass_all_events: bool,
    total_time_us: u64,
}

#[derive(Parser)]
/// Query DynamoDB and attach results to events
#[command(version, long_about = None, arg_required_else_help(true))]
struct DynamoDBQueryArgs {
    /// DynamoDB table name
    #[arg(required = true)]
    table_name: String,

    /// Query type: getitem, query, or scan
    #[arg(short, long, default_value = "getitem")]
    query_type: String,

    /// JMESPath expression to extract query parameters from event
    #[arg(short, long)]
    jmespath: Option<String>,

    /// Label for attaching results to event
    #[arg(short, long, default_value = "dynamodb_result")]
    label: String,

    /// Pass through events even when query returns no results
    #[arg(short, long)]
    pass_all_events: bool,

    /// Initial backoff in milliseconds for exponential backoff retry logic
    #[arg(long, default_value_t = 100)]
    retry_backoff_ms: u64,

    /// AWS region for DynamoDB
    #[arg(short, long)]
    region: Option<String>,
}

#[async_trait(?Send)]
impl<'a> AsyncProcessor for DynamoDBQueryProcessor<'a> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("query DynamoDB and attach results to events".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = DynamoDBQueryArgs::try_parse_from(argv)?;

        // Validate required parameters
        if args.table_name.is_empty() {
            return Err(anyhow!("table_name is required"));
        }

        if args.retry_backoff_ms == 0 {
            return Err(anyhow!("retry_backoff_ms must be greater than 0"));
        }

        // Parse query type
        let query_type = QueryType::from_str(&args.query_type)?;

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

        Ok(DynamoDBQueryProcessor {
            table_name: args.table_name,
            query_type,
            jmespath_expr,
            dynamodb_client,
            queries: 0,
            throttles: 0,
            failures: 0,
            retry_backoff_ms: args.retry_backoff_ms,
            label: args.label,
            pass_all_events: args.pass_all_events,
            total_time_us: 0,
        })
    }

    async fn process_batch(&mut self, events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        self.process_batch_async(events).await
    }

    async fn stats(&self) -> Option<String> {
        let avg_time_us = if self.queries > 0 { self.total_time_us / self.queries } else { 0 };

        Some(format!(
            "queries:{}\nthrottles:{}\nfailures:{}\navg_time_per_record_usec:{}",
            self.queries, self.throttles, self.failures, avg_time_us
        ))
    }
}

impl<'a> DynamoDBQueryProcessor<'a> {
    /// Execute Query operation with exponential backoff retry logic
    async fn execute_query_with_retry(
        &mut self,
        key_condition_expression: &str,
        expression_attribute_values: HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
        filter_expression: Option<&str>,
    ) -> Result<Vec<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>> {
        let mut backoff_ms = self.retry_backoff_ms;
        let max_retries = 5;
        let mut attempt = 0;

        loop {
            attempt += 1;

            let mut query_builder = self
                .dynamodb_client
                .query()
                .table_name(&self.table_name)
                .key_condition_expression(key_condition_expression)
                .set_expression_attribute_values(Some(expression_attribute_values.clone()));

            if let Some(filter_expr) = filter_expression {
                query_builder = query_builder.filter_expression(filter_expr);
            }

            match query_builder.send().await {
                Ok(response) => {
                    return Ok(response.items.unwrap_or_default());
                }
                Err(e) => {
                    let error_str = e.to_string();

                    // Check if error is throttling
                    if error_str.contains("ProvisionedThroughputExceededException")
                        || error_str.contains("ThrottlingException")
                    {
                        self.throttles += 1;

                        if attempt >= max_retries {
                            self.failures += 1;
                            return Err(anyhow!("Max retries exceeded after throttling: {e}"));
                        }

                        // Wait with exponential backoff
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                        backoff_ms *= 2;
                    } else {
                        // Non-throttling error
                        self.failures += 1;
                        return Err(anyhow!("DynamoDB Query failed: {e}"));
                    }
                }
            }
        }
    }

    /// Execute Scan operation with exponential backoff retry logic
    async fn execute_scan_with_retry(
        &mut self,
        filter_expression: Option<&str>,
        expression_attribute_values: Option<
            HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
        >,
    ) -> Result<Vec<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>> {
        let mut backoff_ms = self.retry_backoff_ms;
        let max_retries = 5;
        let mut attempt = 0;

        loop {
            attempt += 1;

            let mut scan_builder = self.dynamodb_client.scan().table_name(&self.table_name);

            if let Some(filter_expr) = filter_expression {
                scan_builder = scan_builder.filter_expression(filter_expr);
            }

            if let Some(attr_values) = &expression_attribute_values {
                scan_builder =
                    scan_builder.set_expression_attribute_values(Some(attr_values.clone()));
            }

            match scan_builder.send().await {
                Ok(response) => {
                    return Ok(response.items.unwrap_or_default());
                }
                Err(e) => {
                    let error_str = e.to_string();

                    // Check if error is throttling
                    if error_str.contains("ProvisionedThroughputExceededException")
                        || error_str.contains("ThrottlingException")
                    {
                        self.throttles += 1;

                        if attempt >= max_retries {
                            self.failures += 1;
                            return Err(anyhow!("Max retries exceeded after throttling: {e}"));
                        }

                        // Wait with exponential backoff
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                        backoff_ms *= 2;
                    } else {
                        // Non-throttling error
                        self.failures += 1;
                        return Err(anyhow!("DynamoDB Scan failed: {e}"));
                    }
                }
            }
        }
    }

    /// Process batch of events asynchronously
    async fn process_batch_async(&mut self, events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        let start_time = std::time::Instant::now();

        let result = match self.query_type {
            QueryType::GetItem => {
                // Use BatchGetItem for efficient batch processing
                let mut keys = Vec::new();

                for event in events {
                    let query_params: serde_json::Value = if let Some(expr) = &self.jmespath_expr {
                        let rcvar_result = expr.search(event)?;
                        serde_json::from_str(&rcvar_result.to_string())?
                    } else {
                        event.borrow().clone()
                    };

                    if !query_params.is_object() {
                        return Err(anyhow!(
                            "GetItem requires query parameters to be a JSON object"
                        ));
                    }

                    let key = json_to_dynamodb_item(&query_params)?;
                    keys.push(key);
                }

                let (results, throttles) = execute_batch_get_item_with_retry(
                    &self.dynamodb_client,
                    &self.table_name,
                    keys.clone(),
                    self.retry_backoff_ms,
                )
                .await?;
                self.throttles += throttles;
                self.queries += events.len() as u64;

                // Map results back to events by matching their keys
                let mut output_events = Vec::new();
                for event in events {
                    let query_params: serde_json::Value = if let Some(expr) = &self.jmespath_expr {
                        let rcvar_result = expr.search(event)?;
                        serde_json::from_str(&rcvar_result.to_string())?
                    } else {
                        event.borrow().clone()
                    };

                    let event_key = json_to_dynamodb_item(&query_params)?;

                    // Find matching result by comparing keys
                    let mut found_result = false;
                    for result in &results {
                        // Check if this result matches the event's key
                        let mut keys_match = true;
                        for (key_name, key_val) in &event_key {
                            if result.get(key_name) != Some(key_val) {
                                keys_match = false;
                                break;
                            }
                        }

                        if keys_match {
                            let result_json = {
                                let mut map = serde_json::Map::new();
                                for (key, val) in result {
                                    map.insert(key.clone(), attribute_value_to_json(val)?);
                                }
                                serde_json::Value::Object(map)
                            };

                            let event_mut = event.clone();
                            if let Some(event_obj) = event_mut.borrow_mut().as_object_mut() {
                                event_obj.insert(self.label.clone(), result_json);
                            }
                            output_events.push(event_mut);
                            found_result = true;
                            break;
                        }
                    }

                    if !found_result && self.pass_all_events {
                        output_events.push(event.clone());
                    }
                }

                Ok(output_events)
            }
            QueryType::Query | QueryType::Scan => {
                // For Query and Scan, process each event individually
                let mut output_events = Vec::new();
                for event in events {
                    match self.process_single_event_async(event.clone()).await {
                        Ok(mut results) => output_events.append(&mut results),
                        Err(e) => return Err(e),
                    }
                }
                Ok(output_events)
            }
        };

        // Accumulate elapsed time in microseconds
        self.total_time_us += start_time.elapsed().as_micros() as u64;

        result
    }

    /// Process event asynchronously
    async fn process_single_event_async(
        &mut self,
        input: Event,
    ) -> Result<Vec<Event>, anyhow::Error> {
        let start_time = std::time::Instant::now();

        // Extract query parameters from input event using JMESPath expression if provided
        let query_params: serde_json::Value = if let Some(expr) = &self.jmespath_expr {
            let rcvar_result = expr.search(&input)?;
            serde_json::from_str(&rcvar_result.to_string())?
        } else {
            // Use entire event if no expression provided
            input.borrow().clone()
        };

        // Execute appropriate DynamoDB operation based on query_type
        let results = match self.query_type {
            QueryType::GetItem => {
                // Validate that query_params is a JSON object for GetItem
                if !query_params.is_object() {
                    return Err(anyhow!("GetItem requires query parameters to be a JSON object"));
                }

                let key = json_to_dynamodb_item(&query_params)?;
                let (item, throttles) = execute_get_item_with_retry(
                    &self.dynamodb_client,
                    &self.table_name,
                    &key,
                    self.retry_backoff_ms,
                )
                .await?;
                self.throttles += throttles;
                match item {
                    Some(item) => vec![item],
                    None => vec![],
                }
            }
            QueryType::Query => {
                // Validate that query_params is a JSON object for Query
                if !query_params.is_object() {
                    return Err(anyhow!("Query requires query parameters to be a JSON object"));
                }

                let obj = query_params.as_object().unwrap();

                // Extract key condition expression (required)
                let key_condition_expression =
                    obj.get("key_condition_expression").and_then(|v| v.as_str()).ok_or_else(
                        || anyhow!("Query requires 'key_condition_expression' parameter"),
                    )?;

                // Extract expression attribute values (required)
                let expression_attribute_values =
                    obj.get("expression_attribute_values").ok_or_else(|| {
                        anyhow!("Query requires 'expression_attribute_values' parameter")
                    })?;

                let attr_values = json_to_dynamodb_item(expression_attribute_values)?;

                // Extract optional filter expression
                let filter_expression = obj.get("filter_expression").and_then(|v| v.as_str());

                self.execute_query_with_retry(
                    key_condition_expression,
                    attr_values,
                    filter_expression,
                )
                .await?
            }
            QueryType::Scan => {
                // Validate that query_params is a JSON object for Scan
                if !query_params.is_object() {
                    return Err(anyhow!("Scan requires query parameters to be a JSON object"));
                }

                let obj = query_params.as_object().unwrap();

                // Extract optional filter expression
                let filter_expression = obj.get("filter_expression").and_then(|v| v.as_str());

                // Extract optional expression attribute values
                let expression_attribute_values =
                    if let Some(attr_vals) = obj.get("expression_attribute_values") {
                        Some(json_to_dynamodb_item(attr_vals)?)
                    } else {
                        None
                    };

                self.execute_scan_with_retry(filter_expression, expression_attribute_values).await?
            }
        };

        self.queries += 1;

        // Handle empty results according to pass_all_events flag
        let result = if results.is_empty() {
            if self.pass_all_events {
                // Return original event unchanged
                Ok(vec![input])
            } else {
                // Drop event
                Ok(vec![])
            }
        } else {
            // Convert results to JSON and attach to event
            let results_json: Vec<serde_json::Value> = results
                .iter()
                .map(|item| {
                    let mut map = serde_json::Map::new();
                    for (key, val) in item {
                        map.insert(key.clone(), attribute_value_to_json(val)?);
                    }
                    Ok(serde_json::Value::Object(map))
                })
                .collect::<Result<Vec<_>>>()?;

            // Attach results to event using configured label
            let result_value = if results_json.len() == 1 {
                results_json.into_iter().next().unwrap()
            } else {
                serde_json::Value::Array(results_json)
            };

            if let Some(event_obj) = input.borrow_mut().as_object_mut() {
                event_obj.insert(self.label.clone(), result_value);
            }

            Ok(vec![input])
        };

        // Accumulate elapsed time in microseconds
        self.total_time_us += start_time.elapsed().as_micros() as u64;

        result
    }
}
