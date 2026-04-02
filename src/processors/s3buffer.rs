use crate::processors::processor::*;
use anyhow::{bail, Context, Result};
use clap::Parser;
use eventwinnower_macros::AsyncProcessorInit;
use std::io::Write;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;

use crate::workflow::trigger::Trigger;

#[allow(unused_imports)]
use mockall::automock;

#[cfg(test)]
pub use MockS3BufferClientImpl as S3BufferClient;
#[cfg(not(test))]
pub use S3BufferClientImpl as S3BufferClient;

#[allow(dead_code)]
pub struct S3BufferClientImpl {
    inner: S3Client,
}

#[cfg_attr(test, automock)]
impl S3BufferClientImpl {
    #[allow(dead_code)]
    pub fn new(inner: S3Client) -> Self {
        Self { inner }
    }

    #[allow(dead_code)]
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: ByteStream,
        content_type: &str,
    ) -> Result<PutObjectOutput, aws_sdk_s3::error::SdkError<PutObjectError>> {
        self.inner
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .content_type(content_type)
            .send()
            .await
    }
}
use flate2::write::GzEncoder;
use flate2::Compression;
use zstd::stream::write::Encoder as ZstdEncoder;

/// Compression algorithm options for the S3 buffer processor
#[derive(Debug, Clone, PartialEq)]
enum CompressionType {
    Gzip,
    Zstd,
}

impl std::str::FromStr for CompressionType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "gzip" => Ok(CompressionType::Gzip),
            "zstd" => Ok(CompressionType::Zstd),
            _ => bail!("Invalid compression type: {}. Valid options are 'gzip' or 'zstd'", s),
        }
    }
}

/// Command-line arguments for the S3 buffer processor
#[derive(Parser)]
/// Buffer events and write them to S3 when thresholds are met
#[command(version, long_about = None)]
struct S3BufferArgs {
    /// S3 bucket name to write events to
    #[arg(required = true)]
    bucket_name: String,

    /// Base directory prefix for S3 objects (e.g., "events/")
    #[arg(short, long, default_value = "")]
    directory_prefix: String,

    /// File name prefix for S3 objects (e.g., "data-")
    #[arg(short, long, default_value = "")]
    file_prefix: String,

    /// Buffer size threshold in KB before flushing to S3
    #[arg(short, long, default_value_t = 1024)]
    size_threshold_kb: usize,

    /// Event count threshold before flushing to S3
    #[arg(short, long, default_value_t = 1000)]
    count_threshold: u64,

    /// Time threshold in seconds before flushing to S3
    #[arg(short, long, default_value_t = 60)]
    time_threshold_seconds: u64,

    /// Compression algorithm to use (gzip or zstd)
    #[arg(long, default_value = "gzip")]
    compression: CompressionType,

    /// AWS region (optional, defaults to environment)
    #[arg(short, long)]
    region: Option<String>,
}

/// S3 Buffer Processor
///
/// Collects events, serializes them to JSONL format, and writes them to S3
/// when configurable thresholds are met (size, count, or time).
#[derive(AsyncProcessorInit)]
pub struct S3BufferProcessor {
    // AWS S3 client
    s3_client: S3BufferClient,

    // S3 destination configuration
    bucket_name: String,
    directory_prefix: String,
    file_prefix: String,

    // Buffer and event tracking
    buffer: Vec<u8>,
    event_count: u64,

    // Threshold configuration
    size_threshold: usize,
    count_threshold: u64,
    time_threshold: Duration,
    last_flush_time: Instant,

    // Compression configuration
    compression_type: CompressionType,

    // Metrics
    total_events_processed: u64,
    total_bytes_written: u64,
    total_objects_created: u64,
    size_threshold_triggers: u64,
    count_threshold_triggers: u64,
    time_threshold_triggers: u64,
    // Async Handling
}

impl S3BufferProcessor {
    /// Check if any thresholds have been met (Subtask 6.1, 6.2)
    fn check_thresholds(&mut self) -> bool {
        // Check size threshold (Subtask 6.1)
        if self.buffer.len() >= self.size_threshold {
            self.size_threshold_triggers += 1;
            eprintln!(
                "Size threshold reached: {} bytes >= {} bytes",
                self.buffer.len(),
                self.size_threshold
            );
            return true;
        }

        // Check count threshold (Subtask 6.2)
        if self.event_count >= self.count_threshold {
            self.count_threshold_triggers += 1;
            eprintln!(
                "Count threshold reached: {} events >= {} events",
                self.event_count, self.count_threshold
            );
            return true;
        }

        // Time threshold is checked in the trigger method (Subtask 6.3)
        false
    }

    /// Compress data using gzip (Subtask 4.1)
    fn compress_gzip(&self, data: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).context("Failed to write data to gzip encoder")?;
        encoder.finish().context("Failed to finish gzip compression")
    }

    /// Compress data using zstd (Subtask 4.2)
    fn compress_zstd(&self, data: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
        let mut encoder =
            ZstdEncoder::new(Vec::new(), 0).context("Failed to create zstd encoder")?;
        encoder.write_all(data).context("Failed to write data to zstd encoder")?;
        encoder.finish().context("Failed to finish zstd compression")
    }

    /// Compress data using the configured compression algorithm
    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
        match self.compression_type {
            CompressionType::Gzip => self.compress_gzip(data),
            CompressionType::Zstd => self.compress_zstd(data),
        }
    }

    /// Generate S3 key with year/month/day/hour partitioning (Subtask 5.1)
    ///
    /// If a timestamp is provided, it will be used instead of the current time.
    /// Otherwise, the current UTC time will be used.
    fn generate_s3_key(&self, timestamp_opt: Option<chrono::DateTime<chrono::Utc>>) -> String {
        // Use provided timestamp or current UTC time
        let datetime = timestamp_opt.unwrap_or_else(chrono::Utc::now);

        // Extract year, month, day, and hour for partitioning
        let year = datetime.format("%Y").to_string();
        let month = datetime.format("%m").to_string();
        let day = datetime.format("%d").to_string();
        let hour = datetime.format("%H").to_string();

        // Generate timestamp for the filename in ISO 8601 format
        let timestamp = datetime.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

        // Get the appropriate file extension based on compression type
        let extension = match self.compression_type {
            CompressionType::Gzip => "gz",
            CompressionType::Zstd => "zst",
        };

        // Format the key with directory prefix, partitions, file prefix, timestamp, and extensions
        format!(
            "{}/{}/{}/{}/{}/{}{}.json.{}",
            self.directory_prefix, year, month, day, hour, self.file_prefix, timestamp, extension
        )
    }
}

#[async_trait(?Send)]
impl AsyncProcessor for S3BufferProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("buffer events and upload to S3 on trigger".to_string())
    }
    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = S3BufferArgs::try_parse_from(argv)?;

        // Initialize AWS SDK configuration with error handling
        let mut config_builder = aws_config::defaults(BehaviorVersion::latest());

        // Set region if provided
        if let Some(region) = args.region {
            config_builder = config_builder.region(Region::new(region));
        }

        // Load the AWS configuration with error handling
        let config = config_builder.load().await;

        // Initialize the S3 client
        let s3_client = S3BufferClient::new(S3Client::new(&config));

        // We don't need to verify the client by listing buckets
        // as the role might not have those permissions
        eprintln!("Initialized AWS S3 client");

        // Convert KB to bytes for size threshold
        let size_threshold = args.size_threshold_kb * 1024;

        // Convert seconds to Duration for time threshold
        let time_threshold = Duration::from_secs(args.time_threshold_seconds);

        Ok(S3BufferProcessor {
            s3_client,
            bucket_name: args.bucket_name,
            directory_prefix: args.directory_prefix,
            file_prefix: args.file_prefix,
            buffer: Vec::new(),
            event_count: 0,
            size_threshold,
            count_threshold: args.count_threshold,
            time_threshold,
            last_flush_time: Instant::now(),
            compression_type: args.compression,
            total_events_processed: 0,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        })
    }

    async fn process_batch(&mut self, input_events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        for event in input_events {
            // Serialize event to JSON and add to buffer (Subtask 3.1)
            let json_str =
                serde_json::to_string(&event).context("Failed to serialize event to JSON")?;

            // Add newline to create JSONL format (Subtask 3.1)
            self.buffer.extend_from_slice(json_str.as_bytes());
            self.buffer.push(b'\n');

            // Increment counters (Subtask 3.2)
            self.event_count += 1;
            self.total_events_processed += 1;
        }

        // Check if any thresholds have been met (Subtask 6.1, 6.2)
        if self.check_thresholds() {
            eprintln!("Threshold met - flushing buffer");
            self.flush().await;
        }

        // Return the event for further processing in the pipeline
        Ok(input_events.to_vec())
    }

    async fn flush(&mut self) -> Vec<Event> {
        // Only flush if there's something in the buffer (Subtask 6.4)
        if self.buffer.is_empty() {
            return vec![];
        }

        eprintln!(
            "Flushing buffer with {} events and {} bytes",
            self.event_count,
            self.buffer.len()
        );

        // Compress the buffer using the configured algorithm
        let compressed_data = match self.compress_data(&self.buffer) {
            Ok(data) => data,
            Err(err) => {
                eprintln!("Error compressing data: {err}");
                return vec![];
            }
        };

        // Generate the S3 key with partitioning
        let key = self.generate_s3_key(None);

        // Upload to S3
        let put_result = self
            .s3_client
            .put_object(
                &self.bucket_name,
                &key,
                aws_sdk_s3::primitives::ByteStream::from(compressed_data.clone()),
                "application/json",
            )
            .await;
        match put_result {
            Ok(_) => {
                debug_log!("Successfully uploaded to s3://{}/{}", self.bucket_name, key);

                // Update metrics
                self.total_bytes_written += compressed_data.len() as u64;
                self.total_objects_created += 1;

                // Reset buffer and counters
                self.buffer.clear();
                self.event_count = 0;
                self.last_flush_time = Instant::now();
            }
            Err(err) => {
                eprintln!("Error uploading to S3: {err}");
            }
        }

        vec![]
    }

    async fn trigger(&mut self, _trigger_type: Trigger) -> Vec<Event> {
        // Check time threshold (Subtask 6.3)
        let current_time = Instant::now();
        let time_elapsed = current_time.duration_since(self.last_flush_time);

        if time_elapsed >= self.time_threshold {
            self.time_threshold_triggers += 1;
            eprintln!(
                "Time threshold triggered after {} seconds - flushing buffer",
                time_elapsed.as_secs()
            );
            return self.flush().await;
        }

        vec![]
    }

    async fn stats(&self) -> Option<String> {
        // Calculate average object size if any objects have been created
        let avg_object_size = if self.total_objects_created > 0 {
            self.total_bytes_written / self.total_objects_created
        } else {
            0
        };

        // Calculate total triggers
        let total_triggers = self.size_threshold_triggers
            + self.count_threshold_triggers
            + self.time_threshold_triggers;

        // Calculate compression ratio if any bytes have been written
        let compression_ratio = if self.total_bytes_written > 0 && self.total_events_processed > 0 {
            // This is a rough estimate based on average event size before compression
            let estimated_raw_size = self.total_events_processed * 500; // Assuming average event size of 500 bytes
            format!("{:.2}", estimated_raw_size as f64 / self.total_bytes_written as f64)
        } else {
            "N/A".to_string()
        };

        // Format the statistics
        Some(format!(
            "events_processed:{}\n\
             objects_created:{}\n\
             bytes_written:{}\n\
             avg_object_size_bytes:{}\n\
             size_triggers:{}\n\
             count_triggers:{}\n\
             time_triggers:{}\n\
             total_triggers:{}\n\
             est_compression_ratio:{}\n\
             current_buffer_size:{}\n\
             current_event_count:{}",
            self.total_events_processed,
            self.total_objects_created,
            self.total_bytes_written,
            avg_object_size,
            self.size_threshold_triggers,
            self.count_threshold_triggers,
            self.time_threshold_triggers,
            total_triggers,
            compression_ratio,
            self.buffer.len(),
            self.event_count
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use serde_json::json;
    use std::cell::RefCell;
    use std::io::Read;
    use std::rc::Rc;

    #[test]
    fn test_check_thresholds_size() {
        let mut processor = S3BufferProcessor {
            s3_client: MockS3BufferClientImpl::default(),
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: vec![0; 2048], // 2KB buffer (above the 1KB threshold)
            event_count: 10,       // Below the count threshold
            size_threshold: 1024,  // 1KB threshold
            count_threshold: 100,  // 100 events threshold
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Gzip,
            total_events_processed: 10,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        // Check thresholds - should trigger on size
        assert!(processor.check_thresholds());
        assert_eq!(processor.size_threshold_triggers, 1);
        assert_eq!(processor.count_threshold_triggers, 0);
    }

    #[test]
    fn test_check_thresholds_count() {
        let mut processor = S3BufferProcessor {
            s3_client: MockS3BufferClientImpl::default(),
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: vec![0; 512], // 512B buffer (below the 1KB threshold)
            event_count: 150,     // Above the count threshold
            size_threshold: 1024, // 1KB threshold
            count_threshold: 100, // 100 events threshold
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Gzip,
            total_events_processed: 150,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        // Check thresholds - should trigger on count
        assert!(processor.check_thresholds());
        assert_eq!(processor.size_threshold_triggers, 0);
        assert_eq!(processor.count_threshold_triggers, 1);
    }

    #[test]
    fn test_check_thresholds_no_trigger() {
        let mut processor = S3BufferProcessor {
            s3_client: MockS3BufferClientImpl::default(),
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: vec![0; 512], // 512B buffer (below the 1KB threshold)
            event_count: 50,      // Below the count threshold
            size_threshold: 1024, // 1KB threshold
            count_threshold: 100, // 100 events threshold
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Gzip,
            total_events_processed: 50,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        // Check thresholds - should not trigger
        assert!(!processor.check_thresholds());
        assert_eq!(processor.size_threshold_triggers, 0);
        assert_eq!(processor.count_threshold_triggers, 0);
    }

    #[test]
    fn test_generate_s3_key() {
        let processor = S3BufferProcessor {
            s3_client: MockS3BufferClientImpl::default(),
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: vec![],
            event_count: 0,
            size_threshold: 1024,
            count_threshold: 100,
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Gzip,
            total_events_processed: 0,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        // Test with a fixed timestamp
        let timestamp = chrono::DateTime::parse_from_rfc3339("2023-06-15T14:30:45.123Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        let key = processor.generate_s3_key(Some(timestamp));

        // Key should follow the pattern: {directory_prefix}/{year}/{month}/{day}/{hour}/{file_prefix}{timestamp}.json.{extension}
        assert_eq!(key, "test-prefix/2023/06/15/14/test-file-2023-06-15T14:30:45.123Z.json.gz");

        // Test with zstd compression
        let processor_zstd =
            S3BufferProcessor { compression_type: CompressionType::Zstd, ..processor };

        let key_zstd = processor_zstd.generate_s3_key(Some(timestamp));
        assert_eq!(
            key_zstd,
            "test-prefix/2023/06/15/14/test-file-2023-06-15T14:30:45.123Z.json.zst"
        );
    }

    #[test]
    fn test_compress_gzip() -> Result<(), anyhow::Error> {
        let processor = S3BufferProcessor {
            s3_client: MockS3BufferClientImpl::default(),
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: vec![],
            event_count: 0,
            size_threshold: 1024,
            count_threshold: 100,
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Gzip,
            total_events_processed: 0,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        // Create test data with repeating content to ensure good compression
        let test_data = vec![b'a'; 1000];

        // Compress the data
        let compressed = processor.compress_gzip(&test_data)?;

        // Compressed data should be smaller than original
        assert!(compressed.len() < test_data.len());

        // Decompress to verify
        let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;

        // Verify the decompressed data matches the original
        assert_eq!(decompressed, test_data);

        Ok(())
    }

    #[test]
    fn test_compress_zstd() -> Result<(), anyhow::Error> {
        let processor = S3BufferProcessor {
            s3_client: MockS3BufferClientImpl::default(),
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: vec![],
            event_count: 0,
            size_threshold: 1024,
            count_threshold: 100,
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Zstd,
            total_events_processed: 0,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        // Create test data with repeating content to ensure good compression
        let test_data = vec![b'a'; 1000];

        // Compress the data
        let compressed = processor.compress_zstd(&test_data)?;

        // Compressed data should be smaller than original
        assert!(compressed.len() < test_data.len());

        // Decompress to verify
        let decompressed = zstd::decode_all(&compressed[..])?;

        // Verify the decompressed data matches the original
        assert_eq!(decompressed, test_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_process() -> Result<(), anyhow::Error> {
        let mut processor = S3BufferProcessor {
            s3_client: MockS3BufferClientImpl::default(),
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: vec![],
            event_count: 0,
            size_threshold: 1024,
            count_threshold: 100,
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Gzip,
            total_events_processed: 0,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        // Create a test event
        let test_event = json!({"test_key": "test_value"});
        let event = Rc::new(RefCell::new(test_event));

        // Process the event
        let result = processor.process_batch(std::slice::from_ref(&event)).await?;

        // Verify the event was returned unchanged
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].borrow().to_string(), event.borrow().to_string());

        // Verify the buffer contains the serialized event with a newline
        let expected_buffer = format!("{}\n", serde_json::to_string(&*event.borrow())?);
        assert_eq!(String::from_utf8(processor.buffer.clone())?, expected_buffer);

        // Verify the counters were incremented
        assert_eq!(processor.event_count, 1);
        assert_eq!(processor.total_events_processed, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_flush() -> Result<(), anyhow::Error> {
        let mut mock = MockS3BufferClientImpl::default();

        // Set up the mock to expect a put_object call
        mock.expect_put_object()
            .withf(|bucket, key, _, content_type| {
                bucket == "test-bucket"
                    && key.contains("/test-file-")
                    && key.contains(".json.gz")
                    && content_type == "application/json"
            })
            .returning(|_, _, _, _| Ok(PutObjectOutput::builder().build()));

        let mut processor = S3BufferProcessor {
            s3_client: mock,
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: b"{\"test_key\":\"test_value\"}\n".to_vec(),
            event_count: 1,
            size_threshold: 1024,
            count_threshold: 100,
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Gzip,
            total_events_processed: 1,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        // Flush the buffer
        processor.flush().await;

        // Verify the metrics were updated
        assert_eq!(processor.total_objects_created, 1);
        assert!(processor.total_bytes_written > 0);
        assert_eq!(processor.buffer.len(), 0);
        assert_eq!(processor.event_count, 0);

        Ok(())
    }

    // Integration tests

    #[tokio::test]
    async fn test_integration_with_processor_chain() -> Result<(), anyhow::Error> {
        // Create a mock S3 client that expects a put_object call
        let mut mock = MockS3BufferClientImpl::default();
        mock.expect_put_object().returning(|_, _, _, _| Ok(PutObjectOutput::builder().build()));

        // Create a processor with a small threshold to trigger flush
        let mut processor = S3BufferProcessor {
            s3_client: mock,
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: vec![],
            event_count: 0,
            size_threshold: 500, // Small size threshold to trigger flush
            count_threshold: 2,  // Small count threshold to trigger flush
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Gzip,
            total_events_processed: 0,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        // Process two events to trigger the count threshold
        let event1 = Rc::new(RefCell::new(json!({"test_key": "test_value_1"})));
        let event2 = Rc::new(RefCell::new(json!({"test_key": "test_value_2"})));

        // Process the first event
        let result1 = processor.process_batch(std::slice::from_ref(&event1)).await?;
        assert_eq!(result1.len(), 1);
        assert_eq!(processor.event_count, 1);
        assert_eq!(processor.total_events_processed, 1);
        assert_eq!(processor.total_objects_created, 0); // No flush yet

        // Process the second event, which should trigger a flush
        let result2 = processor.process_batch(std::slice::from_ref(&event2)).await?;
        assert_eq!(result2.len(), 1);
        assert_eq!(processor.event_count, 0); // Reset after flush
        assert_eq!(processor.total_events_processed, 2);
        assert_eq!(processor.total_objects_created, 1); // Flushed once
        assert_eq!(processor.count_threshold_triggers, 1); // Triggered by count

        Ok(())
    }

    #[tokio::test]
    async fn test_integration_with_time_threshold() -> Result<(), anyhow::Error> {
        // Create a mock S3 client that expects a put_object call
        let mut mock = MockS3BufferClientImpl::default();
        mock.expect_put_object().returning(|_, _, _, _| Ok(PutObjectOutput::builder().build()));

        // Create a processor with a very short time threshold
        let mut processor = S3BufferProcessor {
            s3_client: mock,
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: b"{\"test_key\":\"test_value\"}\n".to_vec(),
            event_count: 1,
            size_threshold: 1024,
            count_threshold: 100,
            time_threshold: Duration::from_millis(10), // Very short time threshold
            last_flush_time: Instant::now().checked_sub(Duration::from_millis(20)).unwrap(), // Set last flush time in the past
            compression_type: CompressionType::Gzip,
            total_events_processed: 1,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        // Trigger the time threshold
        let result = processor.trigger(Trigger::Timer).await;
        assert!(result.is_empty()); // No events returned from trigger

        // Verify the time threshold was triggered and the buffer was flushed
        assert_eq!(processor.time_threshold_triggers, 1);
        assert_eq!(processor.total_objects_created, 1);
        assert_eq!(processor.buffer.len(), 0); // Buffer was cleared

        Ok(())
    }

    #[tokio::test]
    async fn test_integration_with_error_handling() -> Result<(), anyhow::Error> {
        // Create a mock S3 client that returns an error
        let mut mock = MockS3BufferClientImpl::default();
        mock.expect_put_object().returning(|_, _, _, _| {
            Err(aws_sdk_s3::error::SdkError::construction_failure("Test error"))
        });

        // Create a processor with data in the buffer
        let mut processor = S3BufferProcessor {
            s3_client: mock,
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: b"{\"test_key\":\"test_value\"}\n".to_vec(),
            event_count: 1,
            size_threshold: 1024,
            count_threshold: 100,
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Gzip,
            total_events_processed: 1,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        // Try to flush the buffer, which should fail
        let result = processor.flush().await;
        assert!(result.is_empty()); // No events returned from flush

        // Verify that the metrics were not updated and the buffer was not cleared
        assert_eq!(processor.total_objects_created, 0); // No objects created
        assert_eq!(processor.total_bytes_written, 0); // No bytes written
        assert!(!processor.buffer.is_empty()); // Buffer was not cleared
        assert_eq!(processor.event_count, 1); // Event count was not reset

        Ok(())
    }

    #[tokio::test]
    async fn test_integration_with_stats() -> Result<(), anyhow::Error> {
        // Create a processor with some metrics
        let processor = S3BufferProcessor {
            s3_client: MockS3BufferClientImpl::default(),
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: b"{\"test_key\":\"test_value\"}\n".to_vec(),
            event_count: 5,
            size_threshold: 1024,
            count_threshold: 100,
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Gzip,
            total_events_processed: 105,
            total_bytes_written: 5000,
            total_objects_created: 10,
            size_threshold_triggers: 3,
            count_threshold_triggers: 5,
            time_threshold_triggers: 2,
        };

        // Get the stats
        let stats = processor.stats().await.unwrap();

        // Verify that the stats contain all the expected metrics
        assert!(stats.contains("events_processed:105"));
        assert!(stats.contains("objects_created:10"));
        assert!(stats.contains("bytes_written:5000"));
        assert!(stats.contains("size_triggers:3"));
        assert!(stats.contains("count_triggers:5"));
        assert!(stats.contains("time_triggers:2"));
        assert!(stats.contains("total_triggers:10")); // 3 + 5 + 2
        assert!(stats.contains("current_buffer_size:"));
        assert!(stats.contains("current_event_count:5"));

        Ok(())
    }

    #[tokio::test]
    async fn test_integration_with_different_compression_types() -> Result<(), anyhow::Error> {
        // Create mock S3 clients for both compression types
        let mut mock_gzip = MockS3BufferClientImpl::default();
        mock_gzip
            .expect_put_object()
            .withf(|_, key, _, _| key.ends_with(".json.gz"))
            .returning(|_, _, _, _| Ok(PutObjectOutput::builder().build()));

        let mut mock_zstd = MockS3BufferClientImpl::default();
        mock_zstd
            .expect_put_object()
            .withf(|_, key, _, _| key.ends_with(".json.zst"))
            .returning(|_, _, _, _| Ok(PutObjectOutput::builder().build()));

        // Create processors with different compression types
        let mut processor_gzip = S3BufferProcessor {
            s3_client: mock_gzip,
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: b"{\"test_key\":\"test_value\"}\n".to_vec(),
            event_count: 1,
            size_threshold: 1024,
            count_threshold: 100,
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Gzip,
            total_events_processed: 1,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        let mut processor_zstd = S3BufferProcessor {
            s3_client: mock_zstd,
            bucket_name: "test-bucket".to_string(),
            directory_prefix: "test-prefix".to_string(),
            file_prefix: "test-file-".to_string(),
            buffer: b"{\"test_key\":\"test_value\"}\n".to_vec(),
            event_count: 1,
            size_threshold: 1024,
            count_threshold: 100,
            time_threshold: Duration::from_secs(60),
            last_flush_time: Instant::now(),
            compression_type: CompressionType::Zstd,
            total_events_processed: 1,
            total_bytes_written: 0,
            total_objects_created: 0,
            size_threshold_triggers: 0,
            count_threshold_triggers: 0,
            time_threshold_triggers: 0,
        };

        // Flush both processors
        processor_gzip.flush().await;
        processor_zstd.flush().await;

        // Verify that both processors flushed successfully
        assert_eq!(processor_gzip.total_objects_created, 1);
        assert_eq!(processor_zstd.total_objects_created, 1);

        Ok(())
    }
}
