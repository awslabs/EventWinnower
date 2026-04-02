use crate::processors::aws::get_aws_config;
use crate::processors::processor::*;
use crate::workflow::env_preprocess::env_preprocess;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use clap::Parser;
use eventwinnower_macros::AsyncSourceProcessorInit;
use std::collections::HashMap;

use aws_sdk_athena::config::http::HttpResponse;
use aws_sdk_athena::error::SdkError;
use aws_sdk_athena::operation::get_query_results::{GetQueryResultsError, GetQueryResultsOutput};
use aws_sdk_athena::Client;
use aws_smithy_async::future::pagination_stream::PaginationStream;
use std::cell::RefCell;
use std::rc::Rc;

/// output athena query results
#[derive(AsyncSourceProcessorInit)]
pub struct AthenaInputProcessor {
    query: String,
    s3_path: String,
    workgroup: Option<String>,
    athena_client: Client,
    batch_size: i32,
    event_count: u64,
    paginator: Option<
        PaginationStream<
            Result<GetQueryResultsOutput, SdkError<GetQueryResultsError, HttpResponse>>,
        >,
    >,
    json_extract_columns: HashMap<String, bool>,
    finished: bool,
    rows: u64,
    emit_s3_path: bool,
    s3_path_emitted: bool,
}

#[derive(Parser)]
/// query using athena, output results
#[command(version, long_about = None, arg_required_else_help(false))]
struct AthenaInputArgs {
    #[arg(required(false))]
    query: Option<String>,

    #[arg(short = 'E', long)]
    replace_environment_variables: bool,

    #[arg(short, long)]
    file_query: Option<String>,

    #[arg(short, long)]
    s3_path: Option<String>,

    #[arg(short, long)]
    workgroup: Option<String>,

    #[arg(short, long)]
    json_extract_columns: Vec<String>,

    #[arg(short, long, default_value_t = 1000)]
    batch_size: i32,

    #[arg(short, long)]
    emit_s3_path: bool,
}

impl AthenaInputProcessor {
    async fn resolve_s3_path(
        athena_client: &Client,
        s3_path: Option<String>,
        workgroup: Option<String>,
    ) -> Result<String, anyhow::Error> {
        if let Some(s3_path) = s3_path {
            return Ok(s3_path);
        }

        if let Some(workgroup_name) = workgroup {
            let response =
                athena_client.get_work_group().work_group(&workgroup_name).send().await?;

            if let Some(work_group) = response.work_group() {
                if let Some(config) = work_group.configuration() {
                    if let Some(result_config) = config.result_configuration() {
                        if let Some(output_location) = result_config.output_location() {
                            return Ok(output_location.to_string());
                        }
                    }
                }
            }

            return Err(anyhow!("could not find output_location in workgroup configuration"));
        }

        Err(anyhow!("no s3_path or workgroup available"))
    }

    async fn start_query(&mut self) -> Result<String, anyhow::Error> {
        let result_config = aws_sdk_athena::types::ResultConfiguration::builder()
            .set_output_location(Some(self.s3_path.clone()))
            .build();

        let out = self
            .athena_client
            .start_query_execution()
            .query_string(&self.query)
            .result_configuration(result_config)
            .set_work_group(self.workgroup.clone())
            .send()
            .await?;
        if let Some(query_execution_id) = out.query_execution_id() {
            Ok(query_execution_id.to_string())
        } else {
            Err(anyhow::anyhow!("no query execution id returned"))
        }
    }

    async fn wait_on_query_execution(
        &mut self,
        query_execution_id: &str,
    ) -> Result<(), anyhow::Error> {
        loop {
            let get_status = self
                .athena_client
                .get_query_execution()
                .query_execution_id(query_execution_id)
                .send()
                .await?;

            if let Some(qexe) = get_status.query_execution() {
                if let Some(status) = qexe.status() {
                    if let Some(state) = status.state() {
                        eprintln!("query state: {}", state.as_str());
                        match state {
                            aws_sdk_athena::types::QueryExecutionState::Succeeded => {
                                return Ok(());
                            }
                            aws_sdk_athena::types::QueryExecutionState::Failed => {
                                return Err(anyhow::anyhow!(
                                    "query failed {:?}",
                                    status.athena_error()
                                ));
                            }
                            aws_sdk_athena::types::QueryExecutionState::Cancelled => {
                                return Err(anyhow::anyhow!("query cancelled"));
                            }
                            _ => {
                                //sleep for 2 seconds
                                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn generate_async(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        if self.finished {
            return Ok(vec![]);
        }

        if self.paginator.is_none() {
            let query_execution_id = self.start_query().await?;

            self.wait_on_query_execution(query_execution_id.as_str()).await?;

            // If emit_s3_path is set, emit the S3 path event and finish
            if self.emit_s3_path && !self.s3_path_emitted {
                self.s3_path_emitted = true;
                self.finished = true;

                // Parse S3 path into bucket and key
                let (bucket, mut key) = if self.s3_path.starts_with("s3://") {
                    let path = &self.s3_path[5..];
                    match path.split_once('/') {
                        Some((b, k)) => {
                            (b.to_string(), format!("{}/{}.csv", k, query_execution_id))
                        }
                        None => (path.to_string(), format!("{}.csv", query_execution_id)),
                    }
                } else {
                    match self.s3_path.split_once('/') {
                        Some((b, k)) => {
                            (b.to_string(), format!("{}/{}.csv", k, query_execution_id))
                        }
                        None => (self.s3_path.clone(), format!("{}.csv", query_execution_id)),
                    }
                };

                let mut event_map = serde_json::Map::new();
                event_map.insert("bucket_name".to_string(), serde_json::Value::String(bucket));

                if key.starts_with('/') {
                    key = key[1..].to_string();
                }
                event_map.insert("object_key".to_string(), serde_json::Value::String(key));

                let event: serde_json::Value = event_map.into();
                self.event_count += 1;
                return Ok(vec![Rc::new(RefCell::new(event))]);
            }

            let paginator = self
                .athena_client
                .get_query_results()
                .query_execution_id(query_execution_id)
                .into_paginator()
                .page_size(self.batch_size)
                .send();
            self.paginator = Some(paginator);
        }

        let mut out: Vec<Event> = Vec::new();
        if let Some(ref mut paginator) = self.paginator {
            if let Some(page) = paginator.next().await {
                let page = page?;
                if let Some(rs) = page.result_set() {
                    let column_info = if let Some(result_metadata) = rs.result_set_metadata() {
                        result_metadata.column_info()
                    } else {
                        return Err(anyhow!("no column info in athena result set"));
                    };
                    for row in rs.rows() {
                        self.rows += 1;
                        if self.rows == 1 {
                            // Ignore first row as it is column headers
                            continue;
                        }
                        let mut event_map = serde_json::Map::new();
                        for (i, column) in row.data().iter().enumerate() {
                            if let Some(true) = self.json_extract_columns.get(column_info[i].name())
                            {
                                if let Some(json) = column.var_char_value() {
                                    let val: serde_json::Value = serde_json::from_str(json)?;
                                    event_map.insert(column_info[i].name().to_string(), val);
                                }
                            } else {
                                event_map.insert(
                                    column_info[i].name().to_string(),
                                    serde_json::to_value(
                                        column.var_char_value().unwrap_or_default(),
                                    )?,
                                );
                            }
                        }
                        let event: serde_json::Value = event_map.into();

                        out.push(Rc::new(RefCell::new(event)));
                        self.event_count += 1;
                    }
                }
            } else {
                eprintln!("no more athena pages");
                self.finished = true;
            }
        }

        Ok(out)
    }
}

#[async_trait(?Send)]
impl AsyncSourceProcessor for AthenaInputProcessor {
    async fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        self.generate_async().await
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("query Athena and output results".to_string())
    }

    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = AthenaInputArgs::try_parse_from(argv)?;
        let config = get_aws_config().await;
        let athena_client = Client::new(&config);

        let query = if let Some(query) = args.query {
            query
        } else if let Some(file_query) = args.file_query {
            std::fs::read_to_string(file_query)?
        } else {
            return Err(anyhow::anyhow!("no athena query provided"));
        };

        let query =
            if args.replace_environment_variables { env_preprocess(&query)? } else { query };

        if args.s3_path.is_none() && args.workgroup.is_none() {
            return Err(anyhow::anyhow!("ERROR: no s3_path nor workgroup provided for athena"));
        }

        // Resolve S3 path in the constructor
        let s3_path =
            Self::resolve_s3_path(&athena_client, args.s3_path, args.workgroup.clone()).await?;

        let mut json_extract_columns: HashMap<String, bool> = HashMap::new();
        args.json_extract_columns.iter().for_each(|column| {
            json_extract_columns.insert(column.clone(), true);
        });

        Ok(Self {
            query,
            athena_client,
            event_count: 0,
            s3_path,
            workgroup: args.workgroup,
            batch_size: args.batch_size,
            json_extract_columns,
            paginator: None,
            finished: false,
            rows: 0,
            emit_s3_path: args.emit_s3_path,
            s3_path_emitted: false,
        })
    }

    async fn stats(&self) -> Option<String> {
        Some(format!("event_count:{}", self.event_count))
    }
}
