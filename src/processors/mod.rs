#![allow(clippy::result_large_err)]
use lazy_static::lazy_static;
use std::collections::HashMap;

mod aftern;
mod anomaly;
mod apply;
mod arrayjoiner;
mod athena;
mod aws;
mod bedrock;
mod classify;
mod core;
mod csv;
pub mod defanger;
mod diff;
pub mod dynamodb;
mod exec;
mod filter;
mod firehose;
mod firstn;
mod flatten;
mod generate;
mod groupevents;
mod histogram;
mod jmesproc;
mod join;
mod keycorrelate;
mod keycount;
mod keycounttotal;
mod keygen;
mod keypercentile;
mod kinesis;
mod kvmap;
mod numfilter;
mod pivot;
pub mod processor;
mod rank;
mod regexextractor;
mod regexmatcher;
mod s3;
mod s3buffer;
mod sample;
mod sns;
mod sort;
mod sqs;
mod statetransitiondelta;
mod stringdecoder;
mod stringsplitter;
mod stringtransformer;
mod subset;
mod threshold;
mod timedelta;
mod uniq;
mod window;

use crate::processors::aftern::*;
use crate::processors::anomaly::*;
use crate::processors::apply::*;
use crate::processors::arrayjoiner::*;
use crate::processors::athena::*;
use crate::processors::bedrock::*;
use crate::processors::classify::*;
use crate::processors::core::*;
use crate::processors::csv::*;
use crate::processors::defanger::*;
use crate::processors::diff::*;
use crate::processors::dynamodb::*;
use crate::processors::exec::*;
use crate::processors::filter::*;
use crate::processors::firehose::*;
use crate::processors::firstn::*;
use crate::processors::flatten::*;
use crate::processors::generate::*;
use crate::processors::groupevents::*;
use crate::processors::histogram::*;
use crate::processors::jmesproc::*;
use crate::processors::join::*;
use crate::processors::keycorrelate::*;
use crate::processors::keycount::*;
use crate::processors::keycounttotal::*;
use crate::processors::keygen::*;
use crate::processors::keypercentile::*;
use crate::processors::kinesis::*;
use crate::processors::kvmap::*;
use crate::processors::numfilter::*;
use crate::processors::pivot::*;
use crate::processors::processor::*;
use crate::processors::rank::*;
use crate::processors::regexextractor::*;
use crate::processors::regexmatcher::*;
use crate::processors::s3::*;
use crate::processors::s3buffer::*;
use crate::processors::sample::*;
use crate::processors::sns::*;
use crate::processors::sort::*;
use crate::processors::sqs::*;
use crate::processors::statetransitiondelta::*;
use crate::processors::stringdecoder::*;
use crate::processors::stringsplitter::*;
use crate::processors::stringtransformer::*;
use crate::processors::subset::*;
use crate::processors::threshold::*;
pub use crate::processors::timedelta::extract_timestamp_from_event;
use crate::processors::timedelta::*;
use crate::processors::uniq::*;
use crate::processors::window::*;

lazy_static! {
    pub static ref PROCESSORS: HashMap<String, Box<dyn ProcessorInit>> = {
        let mut m = HashMap::new();

        // Built-in processors
        m.insert("json".to_string(), JsonInputProcessorInit::new());
        m.insert("in".to_string(), JsonInputProcessorInit::new());
        m.insert("line".to_string(), JsonInputProcessorInit::new());
        m.insert("count".to_string(), CountProcessorInit::new());
        m.insert("print".to_string(), PrintProcessorInit::new());
        m.insert("rebatch".to_string(), RebatchProcessorInit::new());
        m.insert("annotate".to_string(), AnnotateProcessorInit::new());
        m.insert("enumerate".to_string(), EnumerateProcessorInit::new());
        m.insert("head".to_string(), HeadProcessorInit::new());
        m.insert("null_source".to_string(), NullSourceProcessorInit::new());
        m.insert("lambda".to_string(), LambdaSourceProcessorInit::new());
        m.insert("null".to_string(), NullProcessorInit::new());
        m.insert("acc".to_string(), AccumulateProcessorInit::new());
        m.insert("exec".to_string(), ExecProcessorInit::new());

        // Time and state processors
        m.insert("timedelta".to_string(), TimeDeltaProcessorInit::new());
        m.insert("time_delta".to_string(), TimeDeltaProcessorInit::new());
        m.insert("threshold".to_string(), ThresholdProcessorInit::new());
        m.insert("statetransitiondelta".to_string(), StateTransitionDeltaProcessorInit::new());
        m.insert("state_delta".to_string(), StateTransitionDeltaProcessorInit::new());
        m.insert("diff".to_string(), DiffProcessorInit::new());

        // Input/output processors
        m.insert("gen".to_string(), GenInputProcessorInit::new());
        m.insert("generate".to_string(), GenInputProcessorInit::new());
        m.insert("keygen".to_string(), KeyGenProcessorInit::new());
        m.insert("key_gen".to_string(), KeyGenProcessorInit::new());
        m.insert("csv".to_string(), CsvInputProcessorInit::new());
        m.insert("s3buffer".to_string(), S3BufferProcessorInit::new());

        // String transformation processors
        m.insert("stringtransformer".to_string(), StringTransformerProcessorInit::new());
        m.insert("string_transformer".to_string(), StringTransformerProcessorInit::new());
        m.insert("upper".to_string(), StringTransformerProcessorInit::new());
        m.insert("lower".to_string(), StringTransformerProcessorInit::new());
        m.insert("transform".to_string(), StringTransformerProcessorInit::new());
        m.insert("stringdecoder".to_string(), StringDecoderProcessorInit::new());
        m.insert("stringdecode".to_string(), StringDecoderProcessorInit::new());
        m.insert("string_decode".to_string(), StringDecoderProcessorInit::new());
        m.insert("decode".to_string(), StringDecoderProcessorInit::new());
        m.insert("stringsplitter".to_string(), StringSplitterProcessorInit::new());
        m.insert("string_splitter".to_string(), StringSplitterProcessorInit::new());
        m.insert("split".to_string(), StringSplitterProcessorInit::new());
        m.insert("split_string".to_string(), StringSplitterProcessorInit::new());
        m.insert("defang".to_string(), DefangProcessorInit::new());

        // Regex processors
        m.insert("regexextractor".to_string(), RegexExtractorProcessorInit::new());
        m.insert("regex_extractor".to_string(), RegexExtractorProcessorInit::new());
        m.insert("regex_extract".to_string(), RegexExtractorProcessorInit::new());
        m.insert("regex".to_string(), RegexExtractorProcessorInit::new());
        m.insert("regexmatcher".to_string(), RegexMatcherProcessorInit::new());
        m.insert("regex_matcher".to_string(), RegexMatcherProcessorInit::new());
        m.insert("regex_match".to_string(), RegexMatcherProcessorInit::new());

        // Array processors
        m.insert("arrayjoiner".to_string(), ArrayJoinerProcessorInit::new());
        m.insert("array_joiner".to_string(), ArrayJoinerProcessorInit::new());
        m.insert("join_array".to_string(), ArrayJoinerProcessorInit::new());
        m.insert("flatten".to_string(), FlattenProcessorInit::new());

        // Filter and collection processors
        m.insert("filterarray".to_string(), FilterProcessorInit::new());
        m.insert("filter_array".to_string(), FilterProcessorInit::new());
        m.insert("denest".to_string(), DenestProcessorInit::new());
        m.insert("nest".to_string(), NestProcessorInit::new());
        m.insert("remove".to_string(), RemoveProcessorInit::new());
        m.insert("sort".to_string(), SortProcessorInit::new());
        m.insert("numsort".to_string(), NumSortProcessorInit::new());
        m.insert("top".to_string(), TopProcessorInit::new());

        // Event grouping processors
        m.insert("groupevents".to_string(), GroupEventsProcessorInit::new());
        m.insert("burstgroup".to_string(), BurstGroupProcessorInit::new());
        m.insert("lastn".to_string(), LastNProcessorInit::new());
        m.insert("sum".to_string(), KeySumProcessorInit::new());
        m.insert("keypercentile".to_string(), KeyPercentileProcessorInit::new());
        m.insert("key_percentile".to_string(), KeyPercentileProcessorInit::new());
        m.insert("percentile".to_string(), KeyPercentileProcessorInit::new());
        m.insert("keycorrelate".to_string(), KeyCorrelateProcessorInit::new());
        m.insert("key_correlate".to_string(), KeyCorrelateProcessorInit::new());
        m.insert("correlate".to_string(), KeyCorrelateProcessorInit::new());
        m.insert("histogram".to_string(), HistogramProcessorInit::new());
        m.insert("anomaly".to_string(), AnomalyProcessorInit::new());
        m.insert("classify".to_string(), ClassifyProcessorInit::new());
        m.insert("apply".to_string(), ApplyProcessorInit::new());
        m.insert("window".to_string(), WindowProcessorInit::new());
        m.insert("timeseries".to_string(), WindowProcessorInit::new());

        // JMESPath processors
        m.insert("jmespath".to_string(), JMESPathProcessorInit::new());
        m.insert("match".to_string(), JMESPathProcessorInit::new());
        m.insert("select".to_string(), SelectProcessorInit::new());
        m.insert("substring".to_string(), SubstringProcessorInit::new());
        m.insert("contains".to_string(), ContainsProcessorInit::new());
        m.insert("nummatch".to_string(), NumMatchProcessorInit::new());
        m.insert("num_match".to_string(), NumMatchProcessorInit::new());
        m.insert("numfilter".to_string(), NumFilterProcessorInit::new());
        m.insert("num_filter".to_string(), NumFilterProcessorInit::new());
        m.insert("starts_with".to_string(), StartsWithProcessorInit::new());
        m.insert("starts".to_string(), StartsWithProcessorInit::new());
        m.insert("ends_with".to_string(), EndsWithProcessorInit::new());
        m.insert("ends".to_string(), EndsWithProcessorInit::new());
        m.insert("components".to_string(), ComponentsProcessorInit::new());
        m.insert("split_array".to_string(), SplitArrayProcessorInit::new());
        m.insert("splitarray".to_string(), SplitArrayProcessorInit::new());
        m.insert("jsondecode".to_string(), JsonDecodeProcessorInit::new());
        m.insert("json_decode".to_string(), JsonDecodeProcessorInit::new());
        m.insert("decodejson".to_string(), JsonDecodeProcessorInit::new());
        m.insert("has_field".to_string(), HasFieldProcessorInit::new());
        m.insert("hasfield".to_string(), HasFieldProcessorInit::new());
        m.insert("haslabel".to_string(), HasFieldProcessorInit::new());
        m.insert("uniq".to_string(), UniqLruProcessorInit::new());
        m.insert("firstn".to_string(), FirstNProcessorInit::new());
        m.insert("first_n".to_string(), FirstNProcessorInit::new());
        m.insert("aftern".to_string(), AfterNProcessorInit::new());
        m.insert("after_n".to_string(), AfterNProcessorInit::new());
        m.insert("keycount".to_string(), KeyCountProcessorInit::new());
        m.insert("key_count".to_string(), KeyCountProcessorInit::new());
        m.insert("keycounttotal".to_string(), KeyCountTotalProcessorInit::new());
        m.insert("key_count_total".to_string(), KeyCountTotalProcessorInit::new());

        // KVMap processor
        m.insert("kvmap".to_string(), KVMapProcessorInit::new());

        // Join processor
        m.insert("join".to_string(), JoinProcessorInit::new());

        // Pivot processor
        m.insert("pivot".to_string(), PivotProcessorInit::new());
        m.insert("crosstab".to_string(), PivotProcessorInit::new());

    // Rank processor
        m.insert("rank".to_string(), RankProcessorInit::new());

        // Sampling processor
        m.insert("sample".to_string(), SampleProcessorInit::new());
        m.insert("sampling".to_string(), SampleProcessorInit::new());

        // AWS processors
        m.insert("sqs".to_string(), SqsInputProcessorInit::new());
        m.insert("sqs_remove".to_string(), SqsRemoveProcessorInit::new());
        m.insert("s3".to_string(), S3InputProcessorInit::new());
        m.insert("s3list".to_string(), S3ListObjectsProcessorInit::new());
        m.insert("s3_list".to_string(), S3ListObjectsProcessorInit::new());
        m.insert("listobjects".to_string(), S3ListObjectsProcessorInit::new());
        m.insert("sqss3".to_string(), SqsS3InputProcessorInit::new());
        m.insert("s3event".to_string(), S3EventProcessorInit::new());
        m.insert("kinesis_in".to_string(), KinesisInputProcessorInit::new());
        m.insert("kinesis_out".to_string(), KinesisOutputProcessorInit::new());
        m.insert("firehose".to_string(), FirehoseOutputProcessorInit::new());
        m.insert("sns".to_string(), SnsPublishProcessorInit::new());
        m.insert("bedrock".to_string(), BedrockProcessorInit::new());
        m.insert("athena".to_string(), AthenaInputProcessorInit::new());

        // DynamoDB processors
        m.insert("dynamodbput".to_string(), DynamoDBPutProcessorInit::new());
        m.insert("dynamodb_put".to_string(), DynamoDBPutProcessorInit::new());
        m.insert("dynamodbquery".to_string(), DynamoDBQueryProcessorInit::new());
        m.insert("dynamodb_query".to_string(), DynamoDBQueryProcessorInit::new());
        m.insert("dynamodbget".to_string(), DynamoDBQueryProcessorInit::new());
        m.insert("dynamodb_get".to_string(), DynamoDBQueryProcessorInit::new());
        m.insert("dynamodbdedup".to_string(), DynamoDBDedupProcessorInit::new());
        m.insert("dynamodb_dedup".to_string(), DynamoDBDedupProcessorInit::new());

        // emit the Processor Map
        m
    };
}

#[allow(dead_code)]
fn sorted_process_list() -> Vec<String> {
    let mut procs: Vec<String> = PROCESSORS.keys().cloned().collect();
    procs.sort();
    procs
}

pub struct ProcessHelp {}

impl ProcessHelp {
    #[allow(dead_code)]
    pub fn print_process_list() {
        let procs = sorted_process_list();

        // Find the maximum processor name length for alignment
        let max_name_length = procs.iter().map(|name| name.len()).max().unwrap_or(0);

        for proc in procs.iter() {
            if let Some(description) = PROCESSORS[proc].get_simple_description() {
                println!("{proc:max_name_length$} - {description}");
            } else {
                println!("{proc}");
            }
        }
    }

    #[allow(dead_code)]
    pub async fn print_process_list_full() {
        for proc in sorted_process_list().iter() {
            Self::print_process_help(proc).await;
            println!("-------------------------------");
        }
    }

    #[allow(dead_code)]
    pub async fn print_process_help(proc: &str) {
        if let Err(err) = PROCESSORS[proc].init(&[proc.to_string(), "--help".to_string()], 0).await
        {
            println!("{proc}\n{err}");
        } else {
            println!("{proc}");
        }
    }
}
