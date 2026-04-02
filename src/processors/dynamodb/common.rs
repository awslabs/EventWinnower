use anyhow::{anyhow, Result};
use std::collections::HashMap;

use aws_sdk_dynamodb::Error as DynamoDbError;

/// Execute BatchWriteItem operation with exponential backoff retry logic
pub async fn execute_batch_write_with_retry(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
    write_requests: &[aws_sdk_dynamodb::types::WriteRequest],
    retry_backoff_ms: u64,
) -> Result<u64> {
    let mut backoff_ms = retry_backoff_ms;
    let max_retries = 5;
    let mut attempt = 0;
    let mut throttles = 0;

    loop {
        attempt += 1;

        let mut request_items = HashMap::new();
        request_items.insert(table_name.to_string(), write_requests.to_vec());

        match client.batch_write_item().set_request_items(Some(request_items)).send().await {
            Ok(response) => {
                // Check if there are unprocessed items
                if let Some(unprocessed) = response.unprocessed_items() {
                    if !unprocessed.is_empty() {
                        // Retry unprocessed items
                        if attempt >= max_retries {
                            return Err(anyhow!(
                                "Max retries exceeded with {} unprocessed items",
                                unprocessed.len()
                            ));
                        }

                        throttles += 1;
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                        backoff_ms *= 2;
                        continue;
                    }
                }
                return Ok(throttles);
            }
            Err(e) => {
                let error_str = e.to_string();

                // Check if error is throttling
                if error_str.contains("ProvisionedThroughputExceededException")
                    || error_str.contains("ThrottlingException")
                {
                    throttles += 1;

                    if attempt >= max_retries {
                        return Err(anyhow!(
                            "BatchWriteItem Max retries exceeded after throttling: {e}"
                        ));
                    }

                    // Wait with exponential backoff
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms *= 2;
                } else {
                    // Non-throttling error
                    eprintln!("DynamoDB BatchWriteItem error {:?}", e);
                    return Err(anyhow!("DynamoDB BatchWriteItem failed: {e}"));
                }
            }
        }
    }
}

/// Execute GetItem operation with exponential backoff retry logic
pub async fn execute_get_item_with_retry(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
    key: &HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    retry_backoff_ms: u64,
) -> Result<(Option<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>, u64)> {
    let mut backoff_ms = retry_backoff_ms;
    let max_retries = 5;
    let mut attempt = 0;
    let mut throttles = 0;

    loop {
        attempt += 1;

        match client.get_item().table_name(table_name).set_key(Some(key.clone())).send().await {
            Ok(response) => {
                return Ok((response.item, throttles));
            }
            Err(e) => {
                let error_str = e.to_string();

                // Check if error is throttling
                if error_str.contains("ProvisionedThroughputExceededException")
                    || error_str.contains("ThrottlingException")
                {
                    throttles += 1;

                    if attempt >= max_retries {
                        return Err(anyhow!("GetItem Max retries exceeded after throttling: {e}"));
                    }

                    // Wait with exponential backoff
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms *= 2;
                } else {
                    // Non-throttling error
                    eprintln!("DynamoDB GetItem error {:?}", e);
                    return Err(anyhow!("DynamoDB GetItem failed: {e}"));
                }
            }
        }
    }
}

/// Execute BatchGetItem operation with exponential backoff retry logic
/// Automatically chunks keys into batches of 100 (DynamoDB BatchGetItem limit)
pub async fn execute_batch_get_item_with_retry(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
    keys: Vec<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>,
    retry_backoff_ms: u64,
) -> Result<(Vec<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>, u64)> {
    let mut all_items = Vec::new();
    let mut total_throttles = 0;
    const BATCH_SIZE: usize = 100;

    // Process keys in chunks of 100 (DynamoDB BatchGetItem limit)
    for chunk in keys.chunks(BATCH_SIZE) {
        let (items, throttles) =
            execute_batch_get_item_chunk(client, table_name, chunk.to_vec(), retry_backoff_ms)
                .await?;
        all_items.extend(items);
        total_throttles += throttles;
    }

    Ok((all_items, total_throttles))
}

/// Execute a single BatchGetItem chunk with exponential backoff retry logic
async fn execute_batch_get_item_chunk(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
    keys: Vec<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>,
    retry_backoff_ms: u64,
) -> Result<(Vec<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>, u64)> {
    let mut backoff_ms = retry_backoff_ms;
    let max_retries = 5;
    let mut attempt = 0;
    let mut all_items = Vec::new();
    let mut remaining_keys = keys;
    let mut throttles = 0;

    loop {
        attempt += 1;

        if remaining_keys.is_empty() {
            return Ok((all_items, throttles));
        }

        let mut request_items = HashMap::new();
        request_items.insert(
            table_name.to_string(),
            aws_sdk_dynamodb::types::KeysAndAttributes::builder()
                .set_keys(Some(remaining_keys.clone()))
                .build()
                .expect("Failed to build KeysAndAttributes"),
        );

        match client.batch_get_item().set_request_items(Some(request_items)).send().await {
            Ok(response) => {
                // Collect items from response
                if let Some(responses) = response.responses() {
                    if let Some(items) = responses.get(table_name) {
                        all_items.extend(items.clone());
                    }
                }

                // Check for unprocessed keys
                if let Some(unprocessed) = response.unprocessed_keys() {
                    if let Some(unprocessed_attrs) = unprocessed.get(table_name) {
                        let unprocessed_keys_slice = unprocessed_attrs.keys();
                        if !unprocessed_keys_slice.is_empty() {
                            remaining_keys = unprocessed_keys_slice.to_vec();

                            if attempt >= max_retries {
                                return Err(anyhow!(
                                    "BatchGetItem Max retries exceeded with {} unprocessed keys",
                                    remaining_keys.len()
                                ));
                            }

                            throttles += 1;
                            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms))
                                .await;
                            backoff_ms *= 2;
                            continue;
                        }
                    }
                }

                return Ok((all_items, throttles));
            }
            Err(e) => {
                let error_str = e.to_string();

                // Check if error is throttling
                if error_str.contains("ProvisionedThroughputExceededException")
                    || error_str.contains("ThrottlingException")
                {
                    throttles += 1;

                    if attempt >= max_retries {
                        return Err(anyhow!("Max retries exceeded after throttling: {e}"));
                    }

                    // Wait with exponential backoff
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms *= 2;
                } else {
                    // Non-throttling error
                    eprintln!("DynamoDB BatchGetItem error {:?}", e);
                    return Err(anyhow!("DynamoDB BatchGetItem failed: {e}"));
                }
            }
        }
    }
}

/// Convert a JSON value to a DynamoDB AttributeValue
pub fn json_to_attribute_value(
    value: &serde_json::Value,
) -> Result<aws_sdk_dynamodb::types::AttributeValue> {
    match value {
        serde_json::Value::Null => Ok(aws_sdk_dynamodb::types::AttributeValue::Null(true)),
        serde_json::Value::Bool(b) => Ok(aws_sdk_dynamodb::types::AttributeValue::Bool(*b)),
        serde_json::Value::Number(n) => {
            let num_str = n.to_string();
            Ok(aws_sdk_dynamodb::types::AttributeValue::N(num_str))
        }
        serde_json::Value::String(s) => Ok(aws_sdk_dynamodb::types::AttributeValue::S(s.clone())),
        serde_json::Value::Array(arr) => {
            let mut list = Vec::new();
            for item in arr {
                list.push(json_to_attribute_value(item)?);
            }
            Ok(aws_sdk_dynamodb::types::AttributeValue::L(list))
        }
        serde_json::Value::Object(obj) => {
            let mut map = HashMap::new();
            for (key, val) in obj {
                map.insert(key.clone(), json_to_attribute_value(val)?);
            }
            Ok(aws_sdk_dynamodb::types::AttributeValue::M(map))
        }
    }
}

/// Convert a JSON object to DynamoDB AttributeValue format
pub fn json_to_dynamodb_item(
    json_obj: &serde_json::Value,
) -> Result<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>> {
    let mut item = HashMap::new();

    if let Some(obj) = json_obj.as_object() {
        for (key, value) in obj {
            item.insert(key.clone(), json_to_attribute_value(value)?);
        }
    }

    Ok(item)
}

/// Convert a DynamoDB AttributeValue to JSON
pub fn attribute_value_to_json(
    value: &aws_sdk_dynamodb::types::AttributeValue,
) -> Result<serde_json::Value> {
    match value {
        aws_sdk_dynamodb::types::AttributeValue::S(s) => Ok(serde_json::Value::String(s.clone())),
        aws_sdk_dynamodb::types::AttributeValue::N(n) => {
            // Try to parse as integer first, then float
            if let Ok(i) = n.parse::<i64>() {
                Ok(serde_json::Value::Number(i.into()))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(serde_json::json!(f))
            } else {
                Ok(serde_json::Value::String(n.clone()))
            }
        }
        aws_sdk_dynamodb::types::AttributeValue::B(b) => {
            Ok(serde_json::Value::String(hex::encode(b)))
        }
        aws_sdk_dynamodb::types::AttributeValue::Ss(ss) => Ok(serde_json::Value::Array(
            ss.iter().map(|s| serde_json::Value::String(s.clone())).collect(),
        )),
        aws_sdk_dynamodb::types::AttributeValue::Ns(ns) => {
            let numbers: Result<Vec<_>> = ns
                .iter()
                .map(|n| {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(serde_json::Value::Number(i.into()))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(serde_json::json!(f))
                    } else {
                        Ok(serde_json::Value::String(n.clone()))
                    }
                })
                .collect();
            Ok(serde_json::Value::Array(numbers?))
        }
        aws_sdk_dynamodb::types::AttributeValue::Bs(bs) => Ok(serde_json::Value::Array(
            bs.iter().map(|b| serde_json::Value::String(hex::encode(b))).collect(),
        )),
        aws_sdk_dynamodb::types::AttributeValue::M(m) => {
            let mut map = serde_json::Map::new();
            for (key, val) in m {
                map.insert(key.clone(), attribute_value_to_json(val)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        aws_sdk_dynamodb::types::AttributeValue::L(l) => {
            let items: Result<Vec<_>> = l.iter().map(attribute_value_to_json).collect();
            Ok(serde_json::Value::Array(items?))
        }
        aws_sdk_dynamodb::types::AttributeValue::Null(_) => Ok(serde_json::Value::Null),
        aws_sdk_dynamodb::types::AttributeValue::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        _ => Err(anyhow::anyhow!("Unsupported DynamoDB AttributeValue type")),
    }
}

/// Ensures a DynamoDB table exists, creating it if necessary
///
/// This function first attempts to describe the table to check if it already exists.
/// If the table doesn't exist, it creates the table and waits for it to become active.
/// If a TTL field name is provided and the table was newly created, it also configures TTL.
///
/// # Arguments
/// * `client` - The DynamoDB client
/// * `table_name` - Name of the table
/// * `key_field_name` - Name of the key attribute (e.g., "dedup_key")
/// * `ttl_field_name` - Optional TTL attribute name to configure (only applied to newly created tables)
pub async fn ensure_table_exists(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
    key_field_name: &str,
    ttl_field_name: Option<&str>,
) -> Result<()> {
    // First, try to describe the table to see if it exists
    match client.describe_table().table_name(table_name).send().await {
        Ok(_) => {
            eprintln!("Table '{}' already exists", table_name);
            Ok(())
        }
        Err(e) => {
            let dynamo_err = DynamoDbError::from(e);
            if let DynamoDbError::ResourceNotFoundException(_) = dynamo_err {
                // Table doesn't exist, create it
                eprintln!(
                    "Table '{}' not found, creating with key field '{}'",
                    table_name, key_field_name
                );
                create_table(client, table_name, key_field_name).await?;
                wait_for_table_active(client, table_name).await?;

                // Configure TTL if provided
                if let Some(ttl_attr) = ttl_field_name {
                    configure_ttl(client, table_name, ttl_attr).await?;
                    wait_for_ttl_to_be_active(client, table_name, ttl_attr).await?;
                }

                Ok(())
            } else {
                eprintln!("Failed to describe table '{}': {:?}", table_name, dynamo_err);
                Err(anyhow!("Failed to check table status: {}", dynamo_err))
            }
        }
    }
}

/// Creates a DynamoDB table with a configurable key field name
///
/// # Arguments
/// * `client` - The DynamoDB client
/// * `table_name` - Name of the table to create
/// * `key_field_name` - Name of the key attribute (e.g., "dedup_key")
pub async fn create_table(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
    key_field_name: &str,
) -> Result<()> {
    debug_log!("Creating table '{}' with key field '{}'", table_name, key_field_name);

    let result = client
        .create_table()
        .table_name(table_name)
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name(key_field_name)
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .map_err(|e| anyhow!("Failed to build KeySchemaElement: {}", e))?,
        )
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name(key_field_name)
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .map_err(|e| anyhow!("Failed to build AttributeDefinition: {}", e))?,
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await;

    match result {
        Ok(_) => {
            eprintln!("Table '{}' creation initiated", table_name);
            Ok(())
        }
        Err(e) => {
            // Check if the error is because the table already exists
            if e.to_string().contains("ResourceInUseException") {
                eprintln!("Table '{}' already exists (created concurrently)", table_name);
                Ok(())
            } else {
                eprintln!("Failed to create table '{}': {}", table_name, e);
                Err(anyhow!("Failed to create table: {}", e))
            }
        }
    }
}

/// Waits for a DynamoDB table to become active
pub async fn wait_for_table_active(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
) -> Result<()> {
    eprintln!("Waiting for table '{}' to become active", table_name);

    let mut attempts = 0;
    const MAX_ATTEMPTS: u32 = 30; // Wait up to 30 seconds (30 * 1 second)
    const WAIT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

    loop {
        match client.describe_table().table_name(table_name).send().await {
            Ok(output) => {
                if let Some(table) = output.table() {
                    match table.table_status() {
                        Some(aws_sdk_dynamodb::types::TableStatus::Active) => {
                            eprintln!("Table '{}' is now active", table_name);
                            return Ok(());
                        }
                        Some(status) => {
                            eprintln!("Table '{}' status: {:?}", table_name, status);
                        }
                        None => {
                            eprintln!("Table '{}' status is unknown", table_name);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to describe table '{}': {}", table_name, e);
                return Err(anyhow!("Failed to check table status: {}", e));
            }
        }

        attempts += 1;
        if attempts >= MAX_ATTEMPTS {
            return Err(anyhow!("Table did not become active within {} attempts", MAX_ATTEMPTS));
        }

        eprintln!(
            "Table '{}' not yet active, waiting {} seconds (attempt {}/{})",
            table_name,
            WAIT_INTERVAL.as_secs(),
            attempts,
            MAX_ATTEMPTS
        );
        tokio::time::sleep(WAIT_INTERVAL).await;
    }
}

/// Configures Time-To-Live (TTL) on a DynamoDB table
///
/// # Arguments
/// * `client` - The DynamoDB client
/// * `table_name` - Name of the table
/// * `ttl_attribute_name` - Name of the attribute to use for TTL (e.g., "expires_at")
pub async fn configure_ttl(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
    ttl_attribute_name: &str,
) -> Result<()> {
    debug_log!(
        "Configuring TTL for table '{}' with attribute '{}'",
        table_name,
        ttl_attribute_name
    );

    let result = client
        .update_time_to_live()
        .table_name(table_name)
        .time_to_live_specification(
            aws_sdk_dynamodb::types::TimeToLiveSpecification::builder()
                .attribute_name(ttl_attribute_name)
                .enabled(true)
                .build()
                .map_err(|e| anyhow!("Failed to build TimeToLiveSpecification: {}", e))?,
        )
        .send()
        .await;

    match result {
        Ok(_) => {
            eprintln!("TTL configuration initiated for table '{}'", table_name);
            Ok(())
        }
        Err(e) => {
            // Check if TTL is already enabled
            if e.to_string().contains("ValidationException")
                && e.to_string().contains("TimeToLive is already")
            {
                eprintln!("TTL already configured for table '{}'", table_name);
                Ok(())
            } else {
                eprintln!("Failed to configure TTL for table '{}': {}", table_name, e);
                Err(anyhow!("Failed to configure TTL: {}", e))
            }
        }
    }
}

/// Waits for TTL to become active on a DynamoDB table
///
/// # Arguments
/// * `client` - The DynamoDB client
/// * `table_name` - Name of the table
/// * `ttl_attribute_name` - Name of the TTL attribute to verify
pub async fn wait_for_ttl_to_be_active(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
    ttl_attribute_name: &str,
) -> Result<()> {
    eprintln!(
        "Waiting for TTL to become active on table '{}' for attribute '{}'",
        table_name, ttl_attribute_name
    );

    let mut attempts = 0;
    const MAX_ATTEMPTS: u32 = 30; // Wait up to 30 seconds (30 * 1 second)
    const WAIT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

    loop {
        match client.describe_time_to_live().table_name(table_name).send().await {
            Ok(output) => {
                if let Some(ttl_description) = output.time_to_live_description() {
                    // Verify the attribute name matches
                    if let Some(attr_name) = ttl_description.attribute_name() {
                        if attr_name != ttl_attribute_name {
                            eprintln!(
                                "Warning: TTL attribute name mismatch. Expected '{}', found '{}'",
                                ttl_attribute_name, attr_name
                            );
                        }
                    }

                    match ttl_description.time_to_live_status() {
                        Some(aws_sdk_dynamodb::types::TimeToLiveStatus::Enabled) => {
                            eprintln!(
                                "TTL is now active on table '{}' for attribute '{}'",
                                table_name, ttl_attribute_name
                            );
                            return Ok(());
                        }
                        Some(aws_sdk_dynamodb::types::TimeToLiveStatus::Enabling) => {
                            eprintln!(
                                "TTL is being enabled on table '{}' for attribute '{}'",
                                table_name, ttl_attribute_name
                            );
                        }
                        Some(status) => {
                            eprintln!("TTL status on table '{}': {:?}", table_name, status);
                        }
                        None => {
                            eprintln!("TTL status on table '{}' is unknown", table_name);
                        }
                    }
                } else {
                    eprintln!("No TTL description found for table '{}'", table_name);
                }
            }
            Err(e) => {
                eprintln!("Failed to describe TTL for table '{}': {}", table_name, e);
                return Err(anyhow!("Failed to check TTL status: {}", e));
            }
        }

        attempts += 1;
        if attempts >= MAX_ATTEMPTS {
            return Err(anyhow!("TTL did not become active within {} attempts", MAX_ATTEMPTS));
        }

        eprintln!(
            "TTL not yet active on table '{}', waiting {} seconds (attempt {}/{})",
            table_name,
            WAIT_INTERVAL.as_secs(),
            attempts,
            MAX_ATTEMPTS
        );
        tokio::time::sleep(WAIT_INTERVAL).await;
    }
}
