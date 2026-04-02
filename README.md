# Event Winnower

A data-stream processing framework written in Rust that enables complex data transformations through composable processors connected via a bash-like graph-based workflow language.  This work includes options to leverage Amazon AWS services and can serve as a reference implementation of Rust integration. 

## Overview

Event Winnower is a flexible data processing graph engine that allows you to build complex workflows by connecting processors in directed acyclic workflow graphs (DAGs). With its familiar bash-like pipe syntax (`|`) and variable system (`$var`), Event Winnower makes it easy to create complex data flow patterns including branching, merging, and stream processing. It supports multiple input sources (JSON, CSV, S3, SQS, Kinesis) and output destinations (S3, SQS, Kinesis, Firehose, SNS) with a rich set of transformation and data enrichment processors.  It is intended for rapid, iterative prototyping on the desktop as well as scalable deployments within cloud environments.  It is especially suited for JSON data streams and most processing elements support selecting subsets of data from events using [JMESpath](https://jmespath.org/) syntax.


## Use Cases

- take a realtime stream of data from S3 Notifications from an SNS+SQS queue, process all the events to look for novel items and write those novel items to S3 using firehose
- query athena periodically, transform the items, selectively query bedrock, and send results to Slack from a hook using SNS.
- process lots of files from an S3 prefix, filter them using matching and stateful filters, write results to kinesis
- correlate events that co-occur in time that share a common key-space or set of fields
- take a kinesis stream, lookup ips in maxmind, filter ones that are not external, send data to a new kinesis stream
- prototype a processing idea using test files then rapidly converting it to handle a realtime stream of data to send output to other systems

## Features

- **Processing Graph Architecture**: Build complex DAGs using variables for branching and merging of data streams
- **Stream Processing**: Low latency, event-at-a-time processing as well as batch-events at a time processing
- **Multiple Input Sources**: JSON files, CSV, S3 objects, SQS queues, Kinesis streams, Athena queries
- **Rich Processing Library**: 50+ built-in processors for filtering, transformation, aggregation, and analysis
- **AWS Integration**: Native support for S3, SQS, Kinesis, Firehose, SNS, Bedrock, DynamoDB, and Athena
- **Concurrent Processing**: Multi-threaded execution with configurable thread pools
- **Stateful Analytics**: Store intermediate results and create complex data flow patterns
- **IP Enrichment**: MaxMind GeoIP integration for location and ISP data
- **Text Analysis**: N-gram generation, tokenization, fuzzy clustering, and deduplication

## Missing Features

- Cloudwatch aggregate metrics logging


## Installation

Build from source using Cargo:

```bash
cargo build --release
```

The binary will be available at `target/release/eventwinnower`.

## Basic Usage

### Command Line Interface

```bash
# Basic pipeline from command line
eventwinnower 'json | match "price > `20`" | print'

# Load workflow from file
eventwinnower -f workflow.txt

# List available processors
eventwinnower -p

# Get detailed help for all processors
eventwinnower -P

# Get help for specific processor
eventwinnower -H json
```

## Processing Graph Syntax

While simple linear pipelines use pipe-separated syntax, Event Winnower supports full processing graphs through variables:

### Linear Pipeline (Simple Case)
```
source | processor1 args | processor2 args | output
```

### Processing Graph (Full Power)
```
# Multiple sources, branching, merging, multiple outputs
source1 | processor1 | $intermediate
source2 | processor2 | $intermediate
$intermediate | filter1 | $branch1
$intermediate | filter2 | $branch2
$branch1 | output1
$branch2 | output2
```

where _$intermediate_, _$branch1_ and _$branch2_ are variables that simply connect the output of processors to the input of the next downstream processor, allowing for branching and joining of data streams.

### Variables and Stream Processing

The workflow system supports variables for both branching and joining data streams:

#### Branching (One-to-Many)
```
# Store results in variables
source | processor1 | $variable_name

# Use variables in multiple pipelines
$variable_name | processor2 | output1
$variable_name | processor3 | output2
```

#### Stream Joining (Many-to-One)
```
# Multiple pipelines feeding the same output variable
$input | select foo | $stream
$stream | contains bar -m this | $output
$stream | contains bar -m other | $output

# Combined output from multiple sources
$output | print
```

#### Complex Stream Processing
```
# Multiple inputs converging and diverging
source1 | processor1 | $merged
source2 | processor2 | $merged
$merged | filter1 | $branch1
$merged | filter2 | $branch2
$branch1 | output1
$branch2 | output2
```

Variables start with `$` and can be used to:
- Connect processing graph edges - they define data flow connections
- Create branching workflows where one input feeds multiple processing paths
- **Join multiple streams** - multiple pipelines can send data to the same variable
- Build complex DAG (Directed Acyclic Graph) workflows
- Avoid recomputing expensive operations by reusing graph connections

### Environment Variables in Workflows

Event Winnower supports environment variable substitution in workflow scripts using the `${VARIABLE_NAME}` syntax. This allows you to parameterize your workflows and reference system environment variables at runtime.

#### Syntax
```
${ENVIRONMENT_VARIABLE}
```

Environment variables are detected and replaced with their values during workflow preprocessing. If an environment variable is not found, it will be ignored and left as-is in the output.

#### Examples

Create a workflow file `process.txt`:
```
# Read from S3 bucket specified by environment variable
s3 ${BUCKET_NAME}/logs/ | $logs

# Filter by status from environment variable
$logs | match "level == \"${LOG_LEVEL}\"" | $filtered

# Output to parameterized S3 path
$filtered | s3buffer ${OUTPUT_BUCKET}/processed/
```

Run with environment variables:
```bash
export BUCKET_NAME="my-data-bucket"
export LOG_LEVEL="ERROR"
export OUTPUT_BUCKET="results-bucket"

eventwinnower -f process.txt
```

Another example with SQS and dynamic queue names:
```
# workflow.txt
sqs ${INPUT_QUEUE} | $messages
$messages | match "priority == \"high\"" | sqs ${PRIORITY_QUEUE}
$messages | match "priority == \"low\"" | sqs ${STANDARD_QUEUE}
```

```bash
export INPUT_QUEUE="incoming-messages"
export PRIORITY_QUEUE="high-priority-queue"
export STANDARD_QUEUE="standard-queue"

eventwinnower -f workflow.txt
```

#### Use Cases
- **Dynamic S3 paths**: Reference bucket names, prefixes, or dates from environment variables
- **Queue/Stream names**: Parameterize SQS queue names, Kinesis streams, or SNS topics
- **Configuration values**: Pass thresholds, filters, or other parameters without modifying workflow files
- **Multi-environment workflows**: Use the same workflow file across dev, staging, and production by setting different environment variables

### Basic Examples

#### Simple Linear Processing
```bash
# Read JSON file and filter by price
cat test.json | eventwinnower 'json | match "price > `30`" | print'

# Count unique values
cat test.json | eventwinnower 'json | select name | uniq name | count'
```

#### Graph-Based Processing
```bash
# Branch data to multiple outputs
cat test.json | eventwinnower '
json | $data
$data | match "price > `50`" | print
$data | s3buffer processed/
'

eventwinnower '
s3 bucket1/data.json.gz bucket2/data.json.zst | select "{id:id, val:value}" | $out
$out | uniq id | print
'
```

## Core Processors

### Input Sources
- `json` - Read JSON lines from stdin
- `csv` - Read CSV file from stdin
- `s3 <bucket/key>` - Read from S3 object
- `s3list <bucket/prefix>` - List S3 objects
- `sqss3 <queue>` - Read from SQS queue containing S3+SNS file notifications
- `kinesis_in <stream>` - Read from Kinesis stream
- `athena <query>` - Execute Athena query

### Filtering & Selection
- `match <jmespath>` - Filter using JMESPath expressions
- `select <jmespath>` - Select specific fields
- `contains <jmespath> -m <match>` - Filter records containing text
- `starts_with <jmespath> -m <match>` - Filter by prefix
- `ends_with <jmespath> -m <match>` - Filter by suffix
- `uniq <key:jmespath>` - select only uniq events based on specified key

### Transformation
- `transform <string:jmespath> [-ul]` - Transform field values (upper, lower, etc.)
- `decode <string:jmespath> -m <method>` - Decode strings (base64, hex, url)
- `split <string:jmespath> -d <delimiter>` - Split strings into arrays
- `arrayjoiner <array:jmespath> -s <separator>` - Join arrays into strings
- `annotate <annotation> -l <label>` - Add fields to records
- `remove <field>` - Remove fields

### Aggregation & Analysis
- `count` - Count records
- `sum <value:jmespath> -k <key:jmespath>` - Sum numeric values
- `sort <key:jmespath>` - Sort by key
- `top <key:jmespath>` - Get top N by value

### AWS Output
- `kinesis_out <stream>` - Send to Kinesis stream
- `firehose <stream>` - Send to Kinesis Firehose
- `sns <topic>` - Publish to SNS topic
- `s3buffer <bucket_name>` - Buffer and write to S3

## Advanced Examples

### High-Performance S3 Processing
```bash
# Process thousands of S3 objects in parallel with thread pool
export EVENTWINNOWER_THREADS=20
export EVENTWINNOWER_THREADPOOL=1
eventwinnower "s3list 'my-bucket/logs/' -p '2024/' | 
          head -n 1000 | 
          contains ERROR -m 1 | 
          s3buffer processed-errors/"
```

### Multi-threaded Processing
```bash
# spin up duplicate threads - each thread runs independently
# listen on an SQS queue, process s3 files, select events, write to firehose
EVENTWINNOWER_THREADS=8 eventwinnower 'sqs QUEUE_NAME | s3event | contains my.haystack -m needle | transform --uppercase name | firehose STREAM_NAME'
```

### Branching Workflows with Variables
```bash
# Process data once, output to multiple destinations
cat data.json | eventwinnower '
json | select "id, name, status, timestamp" | $clean
$clean | match "status == \"error\"" | sns error-alerts
$clean | match "status == \"success\"" | s3buffer success-logs/
$clean | keycount status | print
'
```

### Stream Joining and Complex Workflows
```bash
# Join multiple data sources using different source processors
eventwinnower '
# Read from different sources and merge streams
s3 bucket/web-logs.json | select "{timestamp:timestamp, message:message, level:level}" | $combined
sqs api-log-queue | select "{timestamp:timestamp, message:message, level:level}" | $combined

# Filter combined stream into different outputs
$combined | match "level == \"ERROR\"" | $errors
$combined | match "level == \"WARN\"" | $errors
$combined | match "level == \"INFO\"" | $info

# Multiple outputs from joined streams
$errors | sns critical-alerts
$errors | s3buffer error-logs/
$info | s3buffer info-logs/
'
```

### AWS Data Pipeline
```bash
# Read from S3, process, and output to multiple destinations
eventwinnower 's3 my-bucket/logs/app.log | match "level == \"ERROR\"" | sns error-topic | s3buffer processed-errors/'
```

### Data Transformation Examples
```bash
# Apply processor to nested data
cat data.json | eventwinnower 'json | apply tags upper name | print'
```

### Complex Workflow with Variables
Create a file `workflow.txt`:
```
# Read and preprocess data
json | $input

# Filter and prepare data
$input | match "price > `0`" | select "{id:id, name:name, price:price, tags:tags}" | $filtered

# Process tags
$filtered | split "," tags | arrayjoiner "|" tags | $processed

# Multiple outputs from same data
$processed | annotate processed_at "`date`" | s3buffer output-bucket/processed/
$processed | select "{name:name, price:price}" | csv_out summary.csv
$filtered | keycount tags | print
```

Run with: `cat input.json | eventwinnower -f workflow.txt`

## Performance Tuning

### Thread Configuration

Event Winnower supports two threading models:

#### Thread Pool Mode (Recommended for I/O-heavy workloads)
```bash
# Use 20 threads with thread pool - ideal for S3 processing
export EVENTWINNOWER_THREADS=20
export EVENTWINNOWER_THREADPOOL=1
eventwinnower "s3list 'bucket/prefix' | head -n 1024 | contains foo -m bar"
```

In thread pool mode:
- Source processors (like `s3list`) run in a single thread
- Downstream processing is load-balanced across the thread pool
- Excellent for I/O-bound operations like S3 reads, API calls, network operations
- Provides better resource utilization and throughput

#### Threaded Mode (Better for CPU-intensive workloads)
```bash
# Use 8 threads with threaded execution
EVENTWINNOWER_THREADS=8 eventwinnower 'workflow'
```

In threaded mode:
- Each thread processes the entire workflow independently
- Better for CPU-bound operations like text processing, calculations
- Simpler execution model

### Trigger Timer
```bash
# Set trigger timer for batch processing (in seconds)
eventwinnower -T 30 'sqs my-queue | batch_process | output'
```

## Environment Variables

- `EVENTWINNOWER_WORKFLOW_FILE`: Path to workflow file
- `EVENTWINNOWER_THREADS`: Number of threads for parallel processing
- `EVENTWINNOWER_THREADPOOL`: Enable thread pool mode (recommended for I/O-heavy workloads like S3 processing)
- `EVENTWINNOWER_VERBOSE`: Enable verbose debug logging to stderr (e.g., `EVENTWINNOWER_VERBOSE=1`)

## Error Handling

- Processors validate input and provide descriptive error messages
- Use `-h` flag with any processor to see usage information
- Check AWS credentials and permissions for AWS-related processors
- Ensure required files (MaxMind databases) are present for IP enrichment

## Contributing New Processors

Every new processor is likely a remix of some existing processor.  We suggest finding a processor code base to use as a template - and copy that code to a new rust file under /src/processors.

### A Processor Consists of
- A Structure that derives SerialProcessorInit, SerialSourceProcessorInit, AsyncProcessorInit or AsyncSourceProcessorInit
- A command line parser (most use clapp formatted arguments)
- An implementation of get_simple_description() - for single line help
- An implementations of new() to initialize the process instance based on command line argument
- An implementation of process() or process_batch() - not both
  - which takes in event(s) and outputs events
- An optional implementation of stats() which is called on exit to report processing stats
- An optional implementation of flush() which is called on exit to flush out any buffered data and generate output events
- An optional implementation of trigger() which can be called on time or event triggers to produce event output
- if your processor is a source processor, you need to implement a generate() function rather than a process() or process_batch() function

When your processor code is ready to integrate, you will need to modify /src/processors/mod.rs to include your rust module, a ```use crate``` statement, and insert your module alias into the PROCESSORS hashmap.  Once that is included your module should now be accessible to be called from the command line.

### General guidance on rust standards for contributing
1. Follow Rust coding standards and run `cargo fmt` and `cargo clippy`
2. Add tests for new processors, make sure tests compile without errors or warnings
4. Ensure all builds pass: `cargo build --release`

## License

Internal Amazon use only. See package configuration for details.
