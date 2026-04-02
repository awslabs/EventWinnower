use crate::processors::processor::*;
use anyhow::Result;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use std::io::{Read, Write};
use std::process::{Command, Stdio};

/// Execute external command per event
#[derive(SerialProcessorInit)]
pub struct ExecProcessor<'a> {
    command: Vec<String>,
    path: Option<jmespath::Expression<'a>>,
    label: String,
    json_decode: bool,
    pass_through: bool,
    input_count: u64,
    success_count: u64,
    failure_count: u64,
    skip_count: u64,
}

#[derive(Parser)]
/// Execute external command per event, passing event data via STDIN.
///
/// WARNING: This processor runs arbitrary commands. Do not use with untrusted workflows.
#[command(
    version,
    long_about = "Execute external command per event, passing event data via STDIN.\nWARNING: This processor runs arbitrary commands. Do not use with untrusted workflows.",
    arg_required_else_help(true)
)]
struct ExecArgs {
    /// JMESPath expression to select input data
    #[arg(short, long)]
    path: Option<String>,

    /// Label for output field
    #[arg(short, long, default_value = "exec_output")]
    label: String,

    /// Decode output as JSON
    #[arg(short, long)]
    json_decode: bool,

    /// Pass through events on command failure
    #[arg(long)]
    pass_through: bool,

    /// External command and arguments (after --)
    #[arg(last = true, required = true)]
    command: Vec<String>,
}

impl SerialProcessor for ExecProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("execute external command per event (WARNING: runs arbitrary commands, do not use with untrusted workflows)".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = ExecArgs::try_parse_from(argv)?;

        // Validate command vector is non-empty
        if args.command.is_empty() {
            return Err(anyhow::anyhow!("No command specified after --"));
        }

        // Compile JMESPath expression if provided
        let path = match &args.path {
            Some(p) => Some(jmespath::compile(p)?),
            None => None,
        };

        Ok(Self {
            command: args.command,
            path,
            label: args.label,
            json_decode: args.json_decode,
            pass_through: args.pass_through,
            input_count: 0,
            success_count: 0,
            failure_count: 0,
            skip_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        // Extract input data using optional JMESPath
        let input_data = if let Some(ref path) = self.path {
            let result = path.search(&input)?;
            if result.is_null() {
                self.skip_count += 1;
                return Ok(vec![input]);
            }
            serde_json::to_value(&*result)?
        } else {
            input.borrow().clone()
        };

        // Serialize to JSON string with newline
        let json_input = serde_json::to_string(&input_data)? + "\n";

        // Spawn command with piped I/O
        let mut child = match Command::new(&self.command[0])
            .args(&self.command[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                self.failure_count += 1;
                return Err(anyhow::anyhow!(
                    "Failed to spawn command '{}': {}",
                    self.command[0],
                    e
                ));
            }
        };

        // Write JSON to stdin using .take() pattern
        if let Some(mut stdin) = child.stdin.take() {
            if let Err(e) = stdin.write_all(json_input.as_bytes()) {
                self.failure_count += 1;
                return Err(anyhow::anyhow!("Failed to write to stdin: {}", e));
            }
            // stdin is automatically closed when dropped here
        }

        // Read stdout to string (bounded to 10MB)
        const MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024;
        let mut output = String::new();
        if let Some(stdout) = child.stdout.take() {
            if let Err(e) = stdout.take(MAX_OUTPUT_BYTES).read_to_string(&mut output) {
                self.failure_count += 1;
                return Err(anyhow::anyhow!("Failed to read stdout: {}", e));
            }
        }

        // Read stderr for diagnostics
        let mut stderr_output = String::new();
        if let Some(mut stderr) = child.stderr.take() {
            let _ = stderr.read_to_string(&mut stderr_output);
        }

        // Wait for process completion
        let status = child.wait()?;

        // Handle non-zero exit code
        if !status.success() {
            self.failure_count += 1;
            if self.pass_through {
                return Ok(vec![input]);
            } else {
                return Ok(vec![]);
            }
        }

        // Trim trailing whitespace from output
        let trimmed_output = output.trim_end();

        // If output is empty, don't add label
        if trimmed_output.is_empty() {
            self.success_count += 1;
            return Ok(vec![input]);
        }

        // Process output: optionally decode as JSON
        let output_value = if self.json_decode {
            match serde_json::from_str(trimmed_output) {
                Ok(v) => v,
                Err(_) => serde_json::Value::String(trimmed_output.to_string()),
            }
        } else {
            serde_json::Value::String(trimmed_output.to_string())
        };

        // Insert output into event under configured label
        if let Some(event) = input.borrow_mut().as_object_mut() {
            event.insert(self.label.clone(), output_value);
        }

        self.success_count += 1;
        Ok(vec![input])
    }

    fn stats(&self) -> Option<String> {
        Some(format!(
            "input: {}, success: {}, failure: {}, skip: {}",
            self.input_count, self.success_count, self.failure_count, self.skip_count
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cell::RefCell;
    use std::rc::Rc;

    // Helper to create an event from JSON
    fn make_event(val: serde_json::Value) -> Event {
        Rc::new(RefCell::new(val))
    }

    // ==================== Command Parsing Tests ====================

    #[test]
    fn test_single_command() {
        let args = shlex::split("exec -- echo").unwrap();
        let proc = ExecProcessor::new(&args);
        assert!(proc.is_ok());
        let proc = proc.unwrap();
        assert_eq!(proc.command, vec!["echo"]);
    }

    #[test]
    fn test_command_with_args() {
        let args = shlex::split("exec -- cat -n").unwrap();
        let proc = ExecProcessor::new(&args);
        assert!(proc.is_ok());
        let proc = proc.unwrap();
        assert_eq!(proc.command, vec!["cat", "-n"]);
    }

    #[test]
    fn test_missing_command_error() {
        let args = shlex::split("exec --").unwrap();
        let proc = ExecProcessor::new(&args);
        assert!(proc.is_err());
    }

    #[test]
    fn test_no_separator_error() {
        let args = shlex::split("exec").unwrap();
        let proc = ExecProcessor::new(&args);
        assert!(proc.is_err());
    }

    // ==================== JMESPath Selection Tests ====================

    #[test]
    fn test_no_jmespath_uses_entire_event() {
        let args = shlex::split("exec -- cat").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"foo": "bar"}));
        let result = proc.process(event);
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_valid_jmespath_selection() {
        let args = shlex::split("exec -p data -- cat").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"data": "hello", "other": "ignored"}));
        let result = proc.process(event);
        assert!(result.is_ok());
    }

    #[test]
    fn test_jmespath_null_result_skips() {
        let args = shlex::split("exec -p missing_field -- cat").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"foo": "bar"}));
        let result = proc.process(event).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(proc.skip_count, 1);
        assert_eq!(proc.success_count, 0);
    }

    #[test]
    fn test_invalid_jmespath_error() {
        let args = shlex::split("exec -p '[invalid' -- cat").unwrap();
        let proc = ExecProcessor::new(&args);
        assert!(proc.is_err());
    }

    // ==================== Output Handling Tests ====================

    #[test]
    fn test_empty_output_no_label() {
        // Use a command that produces no output
        let args = shlex::split("exec -- true").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"input": "data"}));
        let result = proc.process(event).unwrap();
        assert_eq!(result.len(), 1);
        let output = result[0].borrow();
        // Should not have exec_output label when output is empty
        assert!(output.get("exec_output").is_none());
        assert_eq!(proc.success_count, 1);
    }

    #[test]
    fn test_string_output() {
        let args = shlex::split("exec -- echo hello").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"input": "data"}));
        let result = proc.process(event).unwrap();
        assert_eq!(result.len(), 1);
        let output = result[0].borrow();
        assert_eq!(output["exec_output"], "hello");
    }

    #[test]
    fn test_json_decode_valid_json() {
        let args = shlex::split(r#"exec -j -- echo '{"key":"value"}'"#).unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"input": "data"}));
        let result = proc.process(event).unwrap();
        assert_eq!(result.len(), 1);
        let output = result[0].borrow();
        // With -j flag, should decode JSON
        assert_eq!(output["exec_output"]["key"], "value");
    }

    #[test]
    fn test_json_decode_invalid_json_fallback() {
        let args = shlex::split("exec -j -- echo not_json").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"input": "data"}));
        let result = proc.process(event).unwrap();
        assert_eq!(result.len(), 1);
        let output = result[0].borrow();
        // Should fallback to string when JSON decode fails
        assert_eq!(output["exec_output"], "not_json");
    }

    #[test]
    fn test_custom_label() {
        let args = shlex::split("exec -l custom_output -- echo test").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"input": "data"}));
        let result = proc.process(event).unwrap();
        assert_eq!(result.len(), 1);
        let output = result[0].borrow();
        assert_eq!(output["custom_output"], "test");
        assert!(output.get("exec_output").is_none());
    }

    // ==================== Error Cases Tests ====================

    #[test]
    fn test_bad_command_error() {
        let args = shlex::split("exec -- nonexistent_command_xyz123").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"input": "data"}));
        let result = proc.process(event);
        assert!(result.is_err());
        assert_eq!(proc.failure_count, 1);
    }

    #[test]
    fn test_nonzero_exit_without_passthrough() {
        let args = shlex::split("exec -- false").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"input": "data"}));
        let result = proc.process(event).unwrap();
        // Without pass-through, should return empty vec
        assert_eq!(result.len(), 0);
        assert_eq!(proc.failure_count, 1);
    }

    // ==================== Pass-Through Behavior Tests ====================

    #[test]
    fn test_passthrough_on_failure() {
        let args = shlex::split("exec --pass-through -- false").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"input": "data"}));
        let result = proc.process(event).unwrap();
        // With pass-through, should return original event
        assert_eq!(result.len(), 1);
        let output = result[0].borrow();
        assert_eq!(output["input"], "data");
        assert_eq!(proc.failure_count, 1);
    }

    #[test]
    fn test_passthrough_preserves_event() {
        let args = shlex::split("exec --pass-through -- false").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"key1": "value1", "key2": 42}));
        let result = proc.process(event).unwrap();
        assert_eq!(result.len(), 1);
        let output = result[0].borrow();
        assert_eq!(output["key1"], "value1");
        assert_eq!(output["key2"], 42);
    }

    // ==================== Statistics Tests ====================

    #[test]
    fn test_statistics_accuracy() {
        let args = shlex::split("exec -- echo test").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();

        // Process a few events
        for _ in 0..3 {
            let event = make_event(json!({"data": "test"}));
            let _ = proc.process(event);
        }

        assert_eq!(proc.input_count, 3);
        assert_eq!(proc.success_count, 3);
        assert_eq!(proc.failure_count, 0);
        assert_eq!(proc.skip_count, 0);

        let stats = proc.stats().unwrap();
        assert!(stats.contains("input: 3"));
        assert!(stats.contains("success: 3"));
        assert!(stats.contains("failure: 0"));
        assert!(stats.contains("skip: 0"));
    }

    #[test]
    fn test_statistics_with_skips() {
        let args = shlex::split("exec -p missing -- echo test").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();

        let event = make_event(json!({"other": "data"}));
        let _ = proc.process(event);

        assert_eq!(proc.input_count, 1);
        assert_eq!(proc.skip_count, 1);
        assert_eq!(proc.success_count, 0);
    }

    // ==================== Integration-style Tests ====================

    #[test]
    fn test_cat_echoes_input() {
        let args = shlex::split("exec -- cat").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"message": "hello world"}));
        let result = proc.process(event).unwrap();
        assert_eq!(result.len(), 1);
        let output = result[0].borrow();
        // cat should echo back the JSON input
        let exec_output = output["exec_output"].as_str().unwrap();
        assert!(exec_output.contains("hello world"));
    }

    #[test]
    fn test_jmespath_with_cat() {
        let args = shlex::split("exec -p data -- cat").unwrap();
        let mut proc = ExecProcessor::new(&args).unwrap();
        let event = make_event(json!({"data": {"nested": "value"}, "other": "ignored"}));
        let result = proc.process(event).unwrap();
        assert_eq!(result.len(), 1);
        let output = result[0].borrow();
        let exec_output = output["exec_output"].as_str().unwrap();
        // Should only contain the selected data
        assert!(exec_output.contains("nested"));
        assert!(!exec_output.contains("ignored"));
    }
}
