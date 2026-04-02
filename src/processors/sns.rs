use crate::processors::aws::get_aws_config;
use crate::processors::defanger::defang_urls;
use crate::processors::processor::*;
use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use eventwinnower_macros::AsyncProcessorInit;

use aws_sdk_sns::Client;

#[derive(AsyncProcessorInit)]
pub struct SnsPublishProcessor {
    sns_client: Client,
    topic_arn: String,
    messages: u64,
    subject: Option<String>,
    pretty_print: bool,
    defang_urls: bool,
    truncate_from_bottom: Option<usize>,
}

#[derive(Parser)]
/// removed messages from an sns queue based on handle
#[command(version, about, long_about = None, arg_required_else_help(true))]
struct SnsPublishArgs {
    #[arg(required(true))]
    topic_arn: String,

    #[arg(short, long)]
    create: bool,

    #[arg(short, long)]
    subject: Option<String>,

    #[arg(short, long)]
    pretty_print: bool,

    #[arg(short, long)]
    defang_urls: bool,

    #[arg(short, long)]
    truncate_from_bottom: Option<usize>,
}

#[async_trait(?Send)]
impl AsyncProcessor for SnsPublishProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("publish events to SNS topic".to_string())
    }
    async fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = SnsPublishArgs::try_parse_from(argv)?;
        let config = get_aws_config().await;
        let sns_client = Client::new(&config);

        let topic_arn = if args.create {
            let resp = sns_client.create_topic().name(&args.topic_arn).send().await?;
            if let Some(topic_arn) = resp.topic_arn() {
                debug_log!("created sns topic at arn: {topic_arn}");
                topic_arn.to_string()
            } else {
                return Err(anyhow::anyhow!("failed to create sns topic {}", args.topic_arn));
            }
        } else {
            args.topic_arn
        };

        Ok(Self {
            topic_arn,
            sns_client,
            messages: 0,
            subject: args.subject,
            pretty_print: args.pretty_print,
            defang_urls: args.defang_urls,
            truncate_from_bottom: args.truncate_from_bottom,
        })
    }

    async fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.messages += 1;

        let mut message = if self.pretty_print {
            serde_json::to_string_pretty(&input)?
        } else {
            serde_json::to_string(&input)?
        };

        // Apply URL defanging if enabled
        if self.defang_urls {
            message = defang_urls(&message);
        }

        if let Some(depth) = self.truncate_from_bottom {
            if message.len() > depth {
                message = message[(message.len() - depth)..].to_string();
            }
        }

        self.sns_client
            .publish()
            .topic_arn(&self.topic_arn)
            .set_subject(self.subject.clone())
            .message(message)
            .send()
            .await?;

        Ok(vec![input])
    }

    async fn stats(&self) -> Option<String> {
        Some(format!("messages:{}", self.messages))
    }
}
