# Examples of using Event Winnower

The following examples assume that you will take the workflow examples, write them to a file, and call eventwinnower using

```bash
eventwinnower -f workflow_file
```




## Query Athena, process results
Query Athena, write results to a file.
Assume you have an athena query in a file called query.athena
```bash
athena -f query.athena --emit-s3-path -w my-workgroup | s3event | $events
$events | print -g -f myevents.json.gz
```




## Watch an SNS+SQS queue for S3 file watch notifications  
process new files and dedup events based on a set of keys, write output to a firehose stream
```bash
sqss3 sqs_name | dynamodbdedup dedup_table_name -k {field1:object.field1,field2:object.field2} -r us-east-1 --shared-cache-across-threads --create-table | firehose streamname
```

process new files and send to bedrock for analysis, output to stdout and to a compressed file
```bash
sqss3 sqs_name | select {value1:something.interesting,value2:something.else} | $input

$input | bedrock 'tell me what this is:' -j value1 | $out

$out | print -p
$out | print -g -f results.json.gz
```



## Process files from an S3 prefix, dedupe using DynamoDb table
you can do the following processing in parallel if you set environment variables to:
```bash
export EVENTWINNOWER_THREADS=16
export EVENTWINNOWER_THREADPOOL=1
```

workflow example:
```bash
s3list 'bucket' -p 'prefix' | s3event | dynamodbdedup dedup_table_name -k {field1:object.field1,field2:object.field2} -r us-east-1 --shared-cache-across-threads --create-table | print -g -f dedup_output --append-thread-id-to-filename
```

or write to a firehose stream
```bash
s3list 'bucket' -p 'prefix' | s3event | $events

# dedup events based on an extracted key, utilize dynamodb for persistant state
$events | dynamodbdedup dedup_table_name -k {field1:object.field1,field2:object.field2} -r us-east-1 --shared-cache-across-threads --create-table | $dedup

#write to a firehose stream
$dedup | firehose streamname
```


## Execute External Commands Per Event (exec)

The `exec` processor runs an external command for each event, passing the event data via stdin and capturing the output.

**Important:** Use `--` to separate exec options from the external command and its arguments.

### Basic Syntax
```bash
exec [OPTIONS] -- <command> [command_args...]
```

### Options
- `-p, --path <JMESPATH>` - JMESPath expression to select input data (default: entire event)
- `-l, --label <LABEL>` - Output field name (default: "exec_output")
- `-j, --json-decode` - Parse command output as JSON
- `--pass-through` - Return original event on command failure instead of dropping it

### Examples

**Process events with jq:**
```bash
generate -c 5| exec -- jq '.valueC' | print
```

**Extract a field and process with Python:**
```bash
s3event bucket/file.json | exec -p message -- python3 -c "import sys; print(sys.stdin.read().upper())" | print
```

**Use jq to transform data with JSON output:**
```bash
generate 10 | exec -j -- jq '{new_id: .id, doubled: (.id * 2)}' | print
```

**Custom output label:**
```bash
s3event bucket/file.json | exec -l processed_data -- cat | print
```

**Pass through events on failure:**
```bash
# Events continue even if command fails
s3event bucket/file.json | exec --pass-through -- some_command_that_might_fail | print
```

**Select specific field with JMESPath:**
```bash
# Only pass the 'data' field to the command
s3event bucket/file.json | exec -p data -j -- jq '.nested.value' | print
```

**Chain with other processors:**
```bash
s3list bucket -p prefix | s3event | exec -j -- jq '{summary: .key}' | print -p
```


