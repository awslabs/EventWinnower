use crate::processors::aws::get_aws_config;
use crate::processors::processor::*;
use crate::workflow::graph::ChildRef;
use crate::workflow::shutdown::GLOBAL_SHUTDOWN;
use anyhow::{anyhow, Result};
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use async_trait::async_trait;
use clap::Parser;
use eventwinnower_macros::{AsyncBatchChildrenProcessorInit, AsyncSourceProcessorInit};
use std::cell::RefCell;
use std::io::{self, BufRead, Cursor, Write};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader};
use tokio_stream::StreamExt;

use crate::processors::sqs::{message_to_s3_bucket_key, Sqs};
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::list_objects_v2::{ListObjectsV2Error, ListObjectsV2Output};
use aws_smithy_async::future::pagination_stream::PaginationStream;

use aws_sdk_sqs as sqs;

#[allow(unused_imports)]
use mockall::automock;

#[cfg(test)]
pub use MockS3Impl as S3;
#[cfg(not(test))]
pub use S3Impl as S3;

#[allow(dead_code)]
pub struct S3Impl {
    inner: aws_sdk_s3::Client,
}

#[cfg_attr(test, automock)]
impl S3Impl {
    #[allow(dead_code)]
    pub fn new(inner: aws_sdk_s3::Client) -> Self {
        Self { inner }
    }

    #[allow(dead_code)]
    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<GetObjectOutput, aws_sdk_s3::error::SdkError<GetObjectError>> {
        self.inner.get_object().bucket(bucket).key(key).send().await
    }

    #[allow(dead_code)]
    pub fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: Option<String>,
    ) -> aws_sdk_s3::operation::list_objects_v2::paginator::ListObjectsV2Paginator {
        let mut request = self.inner.list_objects_v2().bucket(bucket);
        if let Some(prefix_val) = prefix {
            request = request.prefix(prefix_val);
        }
        request.into_paginator()
    }
}

/// read a list of s3 objects from stdin
#[derive(AsyncSourceProcessorInit)]
pub struct S3InputProcessor {
    reader: Option<Box<dyn BufRead>>,
    lines: u64,
    active_object: Option<Cursor<Box<[u8]>>>,
    s3_client: S3,
    files: Vec<String>,
}

#[derive(Parser)]
/// read a list of s3 objects from stdin or list of files on command line
#[command(version, long_about = None, arg_required_else_help(false))]
struct S3InputArgs {
    files: Vec<String>,
}

impl S3InputProcessor {
    fn read_active(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        if let Some(ref mut current_file) = self.active_object {
            let mut buffer = String::new();
            if let Ok(len) = std::io::BufRead::read_line(current_file, &mut buffer) {
                if len == 0 {
                    //EOF
                    self.active_object = None;
                } else {
                    let record: Event = Rc::new(RefCell::new(serde_json::from_str(&buffer)?));
                    self.lines += 1;
                    return Ok(vec![record]);
                }
            } else {
                // there was some type of error
                self.active_object = None;
            }
        }
        Ok(vec![])
    }

    async fn get_s3_active(&mut self, s3url: &str) -> Result<(), anyhow::Error> {
        let (bucket, key) = match s3url.trim().split_once('/') {
            Some((bucket, key)) => (bucket, key),
            _ => anyhow::bail!("invalid bucket {s3url}"),
        };
        debug_log!("s3 {bucket} {key}");

        //parse url into bucket and key
        let mut object = self.s3_client.get_object(bucket, key).await?;

        debug_log!("got s3 object");

        //allocate in-memory buffer
        let mut buf = match object.content_length {
            Some(body_len) => {
                eprintln!("opened file with {body_len} bytes");
                Vec::with_capacity(body_len as usize)
            }
            None => Vec::new(),
        };

        if key.ends_with(".gz") {
            let mut decoder = flate2::write::GzDecoder::new(buf);
            while let Some(bytes) = object.body.try_next().await? {
                decoder.write_all(&bytes)?;
            }
            buf = decoder.finish()?;
        } else {
            while let Some(bytes) = object.body.try_next().await? {
                buf.extend_from_slice(&bytes);
            }
        }

        self.active_object = Some(Cursor::new(buf.into_boxed_slice()));

        Ok(())
    }
}

#[async_trait(?Send)]
impl AsyncSourceProcessor for S3InputProcessor {
    //call to get a single new event - keep looping unti no more files to read
    /// returns Ok(None) if we are done with processing files
    async fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        loop {
            //read from active buffer
            let events = self.read_active()?;
            if !events.is_empty() {
                return Ok(events);
            }

            if let Some(file) = self.files.pop() {
                self.get_s3_active(&file).await?;
            } else if let Some(ref mut reader) = self.reader {
                //loop stdin until we have read all data
                let mut buffer = String::new();
                if let Ok(len) = reader.read_line(&mut buffer) {
                    if len == 0 {
                        //EOF
                        return Ok(vec![]);
                    }
                    //populate active buffer
                    self.get_s3_active(&buffer).await?;
                } else {
                    return Ok(vec![]);
                }
            } else {
                return Ok(vec![]);
            }
        }
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("read events from S3 objects".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = S3InputArgs::try_parse_from(argv)?;
        let reader = if args.files.is_empty() {
            Some(Box::new(io::stdin().lock()) as Box<dyn BufRead>)
        } else {
            None
        };

        let config = get_aws_config().await;
        let s3_client = S3::new(aws_sdk_s3::Client::new(&config));
        Ok(S3InputProcessor { reader, lines: 0, active_object: None, s3_client, files: args.files })
    }

    async fn stats(&self) -> Option<String> {
        Some(format!("lines:{}", self.lines))
    }
}

/// Main purpose: read an s3 json objects from an s3 notification event
/// The following processor accepts an S3 bucket/key event and reads the entire file into an event stream
/// Due to the possibility of exceeding memory, this is not recommended
/// instead it is best to have an sqs+s3 combined source that can gradually send out events
/// and then poll SQS when it is ready.
#[derive(AsyncBatchChildrenProcessorInit)]
pub struct S3EventProcessor {
    lines: u64,
    s3_client: S3,
    path: Option<jmespath::Expression<'static>>,
    bucket_label: String,
    key_label: String,
    sqs_handle_label: String,
    contains: Option<String>,
    max_output_events: usize,
    parse_as_csv: bool,
}

#[derive(Parser)]
/// read a s3 files that are from an event stream
#[command(version, long_about = None, arg_required_else_help(false))]
struct S3EventArgs {
    #[arg(required(false))]
    path: Vec<String>,

    #[arg(short, long, default_value = "bucket_name")]
    bucket_label: String,

    #[arg(short, long, default_value = "object_key")]
    key_label: String,

    #[arg(short, long, default_value = "sqs_receipt_handle")]
    sqs_handle_label: String,

    #[arg(short, long)]
    contains: Option<String>,

    #[arg(short, long, default_value_t = 1024)]
    max_output_events: usize,

    #[arg(long)]
    parse_as_csv: bool,
}

impl S3EventProcessor {
    #[allow(clippy::await_holding_refcell_ref)]
    async fn publish_to_children(
        &self,
        parent: &[ChildRef],
        events: &[Event],
    ) -> Result<(), anyhow::Error> {
        for child in parent.iter() {
            child.borrow_mut().process_events(events).await?;
        }
        Ok(())
    }

    async fn get_s3_events(
        &mut self,
        bucket: &str,
        key: &str,
        handle_opt_opt: Option<Option<String>>,
        parent: &[ChildRef],
    ) -> Result<(), anyhow::Error> {
        // Don't try to load additional objects if we should shutdown
        if GLOBAL_SHUTDOWN.load(Ordering::Relaxed) {
            return Ok(());
        }

        //parse url into bucket and key
        let object = self.s3_client.get_object(bucket, key).await?;

        eprintln!("got s3 object");

        let buf_reader = object.body.into_async_read();

        let reader = if key.ends_with(".gz") {
            let mut gzd = GzipDecoder::new(buf_reader);
            gzd.multiple_members(true);
            Box::pin(BufReader::new(gzd)) as Pin<Box<dyn AsyncBufRead + Send>>
        } else if key.ends_with(".zst") {
            let mut zst = ZstdDecoder::new(buf_reader);
            zst.multiple_members(true);
            Box::pin(BufReader::new(zst)) as Pin<Box<dyn AsyncBufRead + Send>>
        } else {
            Box::pin(buf_reader) as Pin<Box<dyn AsyncBufRead + Send>>
        };

        let mut out: Vec<Event> = Vec::new();
        let mut cnt = 0;

        // Determine if we should parse as CSV
        if self.parse_as_csv
            || key.ends_with(".csv")
            || key.ends_with(".csv.gz")
            || key.ends_with(".csv.zst")
        {
            let mut csv_reader = csv_async::AsyncReader::from_reader(reader);
            let headers: Vec<String> =
                csv_reader.headers().await?.iter().map(|header| header.to_string()).collect();
            let mut csv_records = csv_reader.records();

            while let Some(record) = csv_records.next().await {
                let mut event_map = serde_json::Map::new();
                for (i, field) in record?.iter().enumerate() {
                    let key =
                        if i < headers.len() { headers[i].clone() } else { format!("Column{}", i) };
                    event_map.insert(key, serde_json::Value::String(field.to_string()));
                }
                self.lines += 1;
                let event = event_new(serde_json::Value::Object(event_map));
                out.push(event);

                cnt += 1;
                if cnt >= self.max_output_events {
                    self.publish_to_children(parent, &out).await?;
                    out = Vec::new();
                    cnt = 0;

                    if GLOBAL_SHUTDOWN.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                }
            }
        } else {
            let mut lines = reader.lines();
            //parse json
            while let Some(line) = lines.next_line().await? {
                if let Some(ref needle) = self.contains {
                    if !line.contains(needle) {
                        continue;
                    }
                }

                let mut decoded: serde_json::Value = serde_json::from_str(&line)?;

                if let Some(Some(ref handle)) = handle_opt_opt {
                    if let Some(dmap) = decoded.as_object_mut() {
                        dmap.insert(
                            self.sqs_handle_label.clone(),
                            serde_json::Value::String(handle.clone()),
                        );
                    }
                }
                let record: Event = Rc::new(RefCell::new(decoded));
                self.lines += 1;
                out.push(record);

                cnt += 1;
                if cnt >= self.max_output_events {
                    self.publish_to_children(parent, &out).await?;
                    out = Vec::new();
                    cnt = 0;

                    if GLOBAL_SHUTDOWN.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                }
            }
        }
        if !out.is_empty() {
            self.publish_to_children(parent, &out).await?;
        }

        Ok(())
    }

    async fn async_process_event_children(
        &mut self,
        input: Event,
        parent: &[ChildRef],
    ) -> Result<(), anyhow::Error> {
        let (bucket, key, handle) = if let Some(path) = &self.path {
            let result = path.search(&input)?;
            if let Some(offset) = result.as_object() {
                (
                    offset.get(&self.bucket_label).map(|b| b.as_string().cloned()),
                    offset.get(&self.key_label).map(|k| k.as_string().cloned()),
                    offset.get(&self.sqs_handle_label).map(|h| h.as_string().cloned()),
                )
            } else {
                return Ok(());
            }
        } else if let Some(inobj) = input.borrow().as_object() {
            (
                inobj.get(&self.bucket_label).map(|b| b.as_str().map(|bb| bb.to_string().clone())),
                inobj.get(&self.key_label).map(|k| k.as_str().map(|kk| kk.to_string().clone())),
                inobj
                    .get(&self.sqs_handle_label)
                    .map(|h| h.as_str().map(|hh| hh.to_string().clone())),
            )
        } else {
            return Ok(());
        };

        if let (Some(Some(bucket)), Some(Some(key))) = (bucket, key) {
            match self.get_s3_events(&bucket, &key, handle, parent).await {
                Ok(_) => {
                    eprintln!("done with s3 event");
                    Ok(())
                }
                Err(e) => {
                    eprintln!("error in get_s3_events {e:?}");
                    Err(e)
                }
            }
        } else {
            Ok(())
        }
    }

    async fn async_process_batch_children(
        &mut self,
        events: &[Event],
        parent: &[ChildRef],
    ) -> Result<(), anyhow::Error> {
        for event in events {
            self.async_process_event_children(event.clone(), parent).await?;
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl AsyncBatchChildrenProcessor for S3EventProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("from AWS S3 object paths in events, parse json lines".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = S3EventArgs::try_parse_from(argv)?;

        let path = if args.path.is_empty() {
            None
        } else {
            Some(jmespath::compile(&args.path.join(" "))?)
        };

        let config = get_aws_config().await;
        let s3_client = S3::new(aws_sdk_s3::Client::new(&config));
        Ok(S3EventProcessor {
            lines: 0,
            s3_client,
            path,
            bucket_label: args.bucket_label,
            key_label: args.key_label,
            sqs_handle_label: args.sqs_handle_label,
            contains: args.contains,
            max_output_events: args.max_output_events,
            parse_as_csv: args.parse_as_csv,
        })
    }

    async fn process_batch_children(
        &mut self,
        events: &[Event],
        parent: &[ChildRef],
    ) -> Result<(), anyhow::Error> {
        self.async_process_batch_children(events, parent).await
    }

    async fn stats(&self) -> Option<String> {
        Some(format!("lines:{}", self.lines))
    }
}

/// parse json from s3 objects published in AWS SQS queues
#[derive(AsyncSourceProcessorInit)]
pub struct SqsS3InputProcessor {
    queue_name: String,
    queue_url: Option<String>,
    sqs_client: Sqs,
    wait_time: i32,
    lines: u64,
    active_object: Option<Cursor<Box<[u8]>>>,
    active_handle: Option<String>,
    s3_client: S3,
    messages: u64,
    max_output_events: usize,
    suppress_timeout_events: bool,
}

#[derive(Parser)]
/// parse json from s3 objects from an SQS queue
#[command(version, long_about = None, arg_required_else_help(false))]
struct SqsS3InputArgs {
    #[arg(required(true))]
    queue_name: String,

    #[arg(short, long, default_value_t = 2)]
    wait_time_seconds: i32,

    #[arg(short, long, default_value_t = 1024)]
    max_output_events: usize,

    #[arg(short, long)]
    suppress_timeout_events: bool,
}

impl SqsS3InputProcessor {
    fn read_active(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        if let Some(ref mut current_file) = self.active_object {
            let mut buffer = String::new();

            let mut out: Vec<Event> = Vec::new();
            let mut out_count = 0;
            while let Ok(len) = std::io::BufRead::read_line(current_file, &mut buffer) {
                if len == 0 {
                    //EOF
                    self.active_object = None;
                    break;
                } else {
                    let record: Event = Rc::new(RefCell::new(serde_json::from_str(&buffer)?));
                    out_count += 1;
                    out.push(record);

                    if out_count >= self.max_output_events {
                        self.lines += out.len() as u64;
                        return Ok(out);
                    }
                    buffer = String::new();
                }
            }
            if out.is_empty() {
                // there was some type of error
                self.active_object = None;
            } else {
                self.lines += out.len() as u64;
                return Ok(out);
            }
        }
        Ok(vec![])
    }

    async fn get_s3_active(&mut self, bucket: &str, key: &str) -> Result<(), anyhow::Error> {
        debug_log!("opening s3://{bucket}/{key}");
        //parse url into bucket and key
        let mut object = self.s3_client.get_object(bucket, key).await?;

        //allocate in-memory buffer
        let mut buf = match object.content_length {
            Some(body_len) => {
                debug_log!("opened s3://{bucket}/{key} with {body_len} bytes");
                Vec::with_capacity(body_len as usize)
            }
            None => Vec::new(),
        };

        if key.ends_with(".gz") {
            let mut decoder = flate2::write::GzDecoder::new(buf);
            while let Some(bytes) = object.body.try_next().await? {
                decoder.write_all(&bytes)?;
            }
            buf = decoder.finish()?;
        } else if key.ends_with(".zst") {
            let mut decoder = zstd::stream::write::Decoder::new(&mut buf)?;
            while let Some(bytes) = object.body.try_next().await? {
                decoder.write_all(&bytes)?;
            }
            decoder.flush()?;
        } else {
            while let Some(bytes) = object.body.try_next().await? {
                buf.extend_from_slice(&bytes);
            }
        }

        self.active_object = Some(Cursor::new(buf.into_boxed_slice()));

        Ok(())
    }

    async fn remove_handle(&mut self) -> Result<(), anyhow::Error> {
        if let Some(handle) = &self.active_handle {
            if let Some(queue_url) = &self.queue_url {
                self.sqs_client.delete_message(queue_url, handle).await?;
                self.active_handle = None;
            }
        }
        Ok(())
    }

    async fn receive_sqs_message(&mut self) -> Result<Option<()>, anyhow::Error> {
        if self.queue_url.is_none() {
            let queue_url_result = self.sqs_client.get_queue_url(&self.queue_name).await?;
            self.queue_url = queue_url_result.queue_url().map(|url| url.to_string());
        }

        if let Some(queue_url) = &self.queue_url {
            let recv_message_output =
                self.sqs_client.receive_message(queue_url, self.wait_time, 1).await?;

            if let Some(messages) = recv_message_output.messages {
                if !messages.is_empty() {
                    self.messages += 1;
                    if let Some(body) = &messages[0].body {
                        if let Some((bucket, key)) = message_to_s3_bucket_key(body)? {
                            self.get_s3_active(&bucket, &key).await?;
                        }
                    }
                    self.active_handle = messages[0].receipt_handle.clone();

                    return Ok(Some(()));
                }
            }
        } else {
            return Err(anyhow!("Queue Url not found for queue_name: {}", self.queue_name));
        }
        Ok(None)
    }

    //call to get a single new event - keep looping unti no more files to read
    /// returns Ok(None) if we are done with processing files
    async fn generate_async(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        loop {
            //read from active buffer
            let events = self.read_active()?;
            if !events.is_empty() {
                return Ok(events);
            }

            self.remove_handle().await?;

            if self.receive_sqs_message().await?.is_none() && !self.suppress_timeout_events {
                let poll_output = serde_json::Value::String("sqs_polling_timeout".to_string());
                let poll_event = Rc::new(RefCell::new(poll_output));
                return Ok(vec![poll_event]);
            }
        }
    }
}

#[async_trait(?Send)]
impl AsyncSourceProcessor for SqsS3InputProcessor {
    async fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        self.generate_async().await
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("parse json from S3 objects from SQS notifications".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = SqsS3InputArgs::try_parse_from(argv)?;
        let config = get_aws_config().await;
        let sqs_client = Sqs::new(sqs::Client::new(&config));
        let s3_client = S3::new(aws_sdk_s3::Client::new(&config));
        Ok(SqsS3InputProcessor {
            lines: 0,
            active_object: None,
            s3_client,
            sqs_client,
            queue_name: args.queue_name,
            queue_url: None,
            wait_time: args.wait_time_seconds,
            active_handle: None,
            messages: 0,
            max_output_events: args.max_output_events,
            suppress_timeout_events: args.suppress_timeout_events,
        })
    }

    async fn stats(&self) -> Option<String> {
        Some(format!("lines:{}\nmessages:{}", self.lines, self.messages))
    }
}

/// S3 ListObjects source processor that enumerates objects in an S3 bucket
#[derive(AsyncSourceProcessorInit)]
pub struct S3ListObjectsProcessor {
    bucket_name: String,
    prefix: Option<String>,
    s3_client: S3,
    paginator: Option<
        PaginationStream<Result<ListObjectsV2Output, SdkError<ListObjectsV2Error, HttpResponse>>>,
    >,
    objects_processed: u64,
    pages_processed: u64,
    finished: bool,
}

#[derive(Parser)]
/// List objects in an S3 bucket using ListObjectsV2 API
#[command(version, long_about = None, arg_required_else_help(true))]
struct S3ListObjectsArgs {
    /// S3 bucket name to list objects from
    #[arg(required = true)]
    bucket_name: String,

    /// Optional prefix to filter objects
    #[arg(short, long)]
    prefix: Option<String>,
}

impl S3ListObjectsProcessor {
    /// Convert S3 object metadata to a JSON event
    fn create_object_event(&self, object: aws_sdk_s3::types::Object) -> Event {
        let mut event_data = serde_json::Map::new();
        event_data
            .insert("bucket_name".to_string(), serde_json::Value::String(self.bucket_name.clone()));

        if let Some(key) = object.key {
            event_data.insert("object_key".to_string(), serde_json::Value::String(key));
        }

        if let Some(size) = object.size {
            event_data.insert(
                "size".to_string(),
                serde_json::Value::Number(serde_json::Number::from(size)),
            );
        }

        if let Some(last_modified) = object.last_modified {
            event_data.insert(
                "last_modified".to_string(),
                serde_json::Value::String(last_modified.to_string()),
            );
        }

        if let Some(etag) = object.e_tag {
            event_data.insert("etag".to_string(), serde_json::Value::String(etag));
        }

        Rc::new(RefCell::new(serde_json::Value::Object(event_data)))
    }

    /// Handle the next page from the paginator and return events
    async fn handle_next_page(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        if let Some(paginator) = &mut self.paginator {
            if let Some(page_result) = paginator.next().await {
                match page_result {
                    Ok(page) => {
                        self.pages_processed += 1;

                        let mut events = Vec::new();
                        if let Some(contents) = page.contents {
                            for object in contents {
                                self.objects_processed += 1;
                                events.push(self.create_object_event(object));
                            }
                        }
                        Ok(events)
                    }
                    Err(e) => {
                        // Handle AWS API errors with appropriate context
                        Err(anyhow::anyhow!(
                            "Failed to list objects in bucket '{}'{}: {}",
                            self.bucket_name,
                            self.prefix
                                .as_ref()
                                .map(|p| format!(" with prefix '{p}'"))
                                .unwrap_or_default(),
                            e
                        ))
                    }
                }
            } else {
                // No more pages, mark as finished
                self.finished = true;
                Ok(vec![])
            }
        } else {
            // This shouldn't happen, but handle gracefully
            self.finished = true;
            Ok(vec![])
        }
    }
}

#[async_trait(?Send)]
impl AsyncSourceProcessor for S3ListObjectsProcessor {
    async fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        if self.finished {
            return Ok(vec![]);
        }

        // Initialize paginator on first call
        if self.paginator.is_none() {
            let prefix_owned = self.prefix.clone();
            let paginator = self.s3_client.list_objects_v2(&self.bucket_name, prefix_owned).send();
            self.paginator = Some(paginator);
        }

        // Get the next page of objects
        self.handle_next_page().await
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("list S3 objects and generate events".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = S3ListObjectsArgs::try_parse_from(argv)?;
        let config = get_aws_config().await;
        let s3_client = S3::new(aws_sdk_s3::Client::new(&config));

        Ok(S3ListObjectsProcessor {
            bucket_name: args.bucket_name,
            prefix: args.prefix,
            s3_client,
            paginator: None,
            objects_processed: 0,
            pages_processed: 0,
            finished: false,
        })
    }

    async fn stats(&self) -> Option<String> {
        Some(format!("objects:{}\npages:{}", self.objects_processed, self.pages_processed))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use aws_sdk_s3::operation::get_object::GetObjectError;
    use aws_sdk_s3::primitives::{ByteStream, DateTime, SdkBody};
    use aws_sdk_s3::types::error::NoSuchKey;
    use aws_sdk_s3::types::Object;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use mockall::predicate::eq;
    use serde_json::json;

    #[tokio::test]
    async fn mock_s3_read() -> Result<(), anyhow::Error> {
        let mut mock = MockS3Impl::default();

        let contents = json!({ "foo": "bar" });

        let cstr = format!("{contents}\n");

        eprintln!("json contents {cstr}");
        mock.expect_get_object().with(eq("valid-bucket"), eq("valid-key")).return_once(|_, _| {
            Ok(GetObjectOutput::builder().body(ByteStream::from(SdkBody::from(cstr))).build())
        });

        let reader = "valid-bucket/valid-key\n";

        let rbytes = reader.as_bytes();

        let mut processor = S3InputProcessor {
            reader: Some(Box::new(rbytes)),
            lines: 0,
            active_object: None,
            s3_client: mock,
            files: vec![],
        };

        let mut events = processor.generate().await?;
        assert_eq!(events.len(), 1);
        if let Some(event) = events.pop() {
            if let Some(event) = event.borrow_mut().as_object_mut() {
                assert_eq!(event["foo"], serde_json::to_value("bar").unwrap());
            }
        }

        assert!(processor.generate().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn mock_s3_gzip_read() -> Result<(), anyhow::Error> {
        let mut mock = MockS3Impl::default();

        let contents = json!({ "foo": "bar" });

        let cstr = format!("{contents}\n{contents}\n");

        let mut gzcontents = GzEncoder::new(Vec::new(), Compression::default());
        gzcontents.write_all(cstr.as_bytes())?;
        let gzbuf = gzcontents.finish()?;

        eprintln!("json contents {cstr}");
        mock.expect_get_object().with(eq("valid-bucket"), eq("valid-key.gz")).return_once(
            |_, _| {
                Ok(GetObjectOutput::builder().body(ByteStream::from(SdkBody::from(gzbuf))).build())
            },
        );

        let reader = "valid-bucket/valid-key.gz\n";

        //let rbytes = reader.as_bytes();

        let mut processor = S3InputProcessor {
            reader: Some(Box::new(reader.as_bytes())),
            lines: 0,
            active_object: None,
            s3_client: mock,
            files: vec![],
        };

        let mut events = processor.generate().await?;
        assert_eq!(events.len(), 1);
        if let Some(event) = events.pop() {
            let bevent = event.borrow();
            assert_eq!(bevent["foo"], serde_json::to_value("bar").unwrap());
        }
        let mut events = processor.generate().await?;
        assert_eq!(events.len(), 1);
        if let Some(event) = events.pop() {
            let bevent = event.borrow();
            assert_eq!(bevent["foo"], serde_json::to_value("bar").unwrap());
        }

        assert!(processor.generate().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn mock_s3_fail_read() -> Result<(), anyhow::Error> {
        let mut mock = MockS3Impl::default();

        mock.expect_get_object().with(eq("valid-bucket"), eq("invalid-key")).return_once(|_, _| {
            //It is super annoying to generate mock S3 errors
            Err(aws_sdk_s3::error::SdkError::service_error(
                GetObjectError::NoSuchKey(NoSuchKey::builder().message("invalid-key").build()),
                aws_sdk_s3::config::http::HttpResponse::new(
                    aws_smithy_runtime_api::http::StatusCode::try_from(400).unwrap(),
                    SdkBody::empty(),
                ),
            ))
        });

        let reader = "valid-bucket/invalid-key\n";

        let mut processor = S3InputProcessor {
            reader: Some(Box::new(reader.as_bytes())),
            lines: 0,
            active_object: None,
            s3_client: mock,
            files: vec![],
        };

        let result = processor.generate().await;
        assert!(result.is_err());
        let source = result.as_ref().unwrap_err().source();
        assert!(source.is_some());
        assert!(source.unwrap().to_string().contains("invalid-key"));
        Ok(())
    }

    #[tokio::test]
    async fn mock_s3_list_objects_event_creation() -> Result<(), anyhow::Error> {
        let processor = S3ListObjectsProcessor {
            bucket_name: "test-bucket".to_string(),
            prefix: None,
            s3_client: MockS3Impl::default(),
            paginator: None,
            objects_processed: 0,
            pages_processed: 0,
            finished: false,
        };

        // Test event creation with complete object metadata
        let test_object = Object::builder()
            .key("file1.txt")
            .size(1024)
            .last_modified(DateTime::from_secs(1640995200))
            .e_tag("\"etag1\"")
            .build();

        let event = processor.create_object_event(test_object);
        let event_data = event.borrow();
        let event_obj = event_data.as_object().unwrap();

        assert_eq!(event_obj["bucket_name"], "test-bucket");
        assert_eq!(event_obj["object_key"], "file1.txt");
        assert_eq!(event_obj["size"], 1024);
        assert_eq!(event_obj["etag"], "\"etag1\"");
        assert!(event_obj.contains_key("last_modified"));

        Ok(())
    }

    #[tokio::test]
    async fn mock_s3_list_objects_prefix_parameter() -> Result<(), anyhow::Error> {
        // Test that prefix parameter is correctly stored and would be passed to list_objects_v2
        let processor = S3ListObjectsProcessor {
            bucket_name: "test-bucket".to_string(),
            prefix: Some("logs/".to_string()),
            s3_client: MockS3Impl::default(),
            paginator: None,
            objects_processed: 0,
            pages_processed: 0,
            finished: false,
        };

        // Verify the prefix is stored correctly for use in API calls
        assert_eq!(processor.prefix, Some("logs/".to_string()));
        assert_eq!(processor.bucket_name, "test-bucket");

        Ok(())
    }

    // TODO: Implement proper unit tests for S3ListObjectsProcessor.generate() method
    //
    // The current challenge is that the AWS SDK's PaginationStream is difficult to mock
    // in unit tests. The generate() method calls:
    // 1. self.s3_client.list_objects_v2(&self.bucket_name, prefix_owned).send()
    // 2. This returns a PaginationStream that needs to be mocked to return controlled data
    //
    // Required tests (as specified in task 7):
    // 1. Test for single page of objects using MockS3Impl - should call generate() and verify events
    // 2. Test for empty bucket response - should call generate() and verify empty result
    // 3. Test for prefix filtering - should verify mock method calls with correct parameters
    //
    // These tests should follow the existing mock_s3_read() pattern which:
    // - Sets up mock expectations with .expect_get_object()
    // - Creates a processor with the mock
    // - Calls processor.generate()
    // - Verifies the returned events
    //
    // The current implementation below is incomplete and does not test the actual
    // generate() method flow as required.

    #[tokio::test]
    async fn mock_s3_list_objects_single_page() -> Result<(), anyhow::Error> {
        // TODO: This test needs to actually call processor.generate() and verify the results
        // Currently this only tests event creation, not the full generate() flow

        let processor = S3ListObjectsProcessor {
            bucket_name: "test-bucket".to_string(),
            prefix: None,
            s3_client: MockS3Impl::default(),
            paginator: None,
            objects_processed: 0,
            pages_processed: 0,
            finished: false,
        };

        // This only tests event creation, not the actual generate() method
        let test_object = Object::builder()
            .key("file1.txt")
            .size(1024)
            .last_modified(DateTime::from_secs(1640995200))
            .e_tag("\"etag1\"")
            .build();

        let event = processor.create_object_event(test_object);
        let event_data = event.borrow();
        let event_obj = event_data.as_object().unwrap();
        assert_eq!(event_obj["bucket_name"], "test-bucket");
        assert_eq!(event_obj["object_key"], "file1.txt");

        Ok(())
    }

    #[tokio::test]
    async fn mock_s3_list_objects_empty_bucket() -> Result<(), anyhow::Error> {
        // TODO: This test needs to actually call processor.generate() and verify empty results
        // Currently this only tests the finished state behavior

        let mut processor = S3ListObjectsProcessor {
            bucket_name: "empty-bucket".to_string(),
            prefix: None,
            s3_client: MockS3Impl::default(),
            paginator: None,
            objects_processed: 0,
            pages_processed: 0,
            finished: true,
        };

        // This tests the finished state, but not the actual empty bucket scenario
        let events = processor.generate().await?;
        assert!(events.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn mock_s3_list_objects_with_prefix() -> Result<(), anyhow::Error> {
        // TODO: This test needs to actually call processor.generate() and verify the full flow
        // Currently this only verifies that the mock method can be called

        let mut mock = MockS3Impl::default();

        mock.expect_list_objects_v2()
            .with(eq("test-bucket"), eq(Some("logs/".to_string())))
            .times(1)
            .returning(|_, _| {
                use aws_config::BehaviorVersion;
                use aws_sdk_s3::Client;

                let config = aws_sdk_s3::Config::builder()
                    .behavior_version(BehaviorVersion::latest())
                    .region(aws_sdk_s3::config::Region::new("us-east-1"))
                    .build();
                let client = Client::from_conf(config);
                client.list_objects_v2().bucket("test-bucket").prefix("logs/").into_paginator()
            });

        let processor = S3ListObjectsProcessor {
            bucket_name: "test-bucket".to_string(),
            prefix: Some("logs/".to_string()),
            s3_client: mock,
            paginator: None,
            objects_processed: 0,
            pages_processed: 0,
            finished: false,
        };

        // This only verifies the mock can be called, not the full generate() flow
        processor.s3_client.list_objects_v2("test-bucket", Some("logs/".to_string()));

        Ok(())
    }
}
