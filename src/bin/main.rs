use anyhow::Result;
use eventwinnower::debug_log;
use eventwinnower::workflow::threaded::run_threaded_workflow;
use eventwinnower::workflow::threadpool::run_threadpool_workflow;
use std::env;

use std::path::PathBuf;
use std::time::Duration;

use clap::{value_parser, Arg, Command};

use eventwinnower::processors::ProcessHelp;
use eventwinnower::workflow::dot::generate_dot_output;
use eventwinnower::workflow::parse::{file_to_workflow_string, parse_workflow, run_workflow};

struct CommandLine {
    workflow: String,
    threshold: Option<Duration>,
    dot_output: Option<PathBuf>,
}

async fn parse_command_line_to_workflow() -> Result<Option<CommandLine>, anyhow::Error> {
    //https://medium.com/@itsuki.enjoy/rust-take-your-cli-to-the-next-level-with-clap-a0f05875ef45
    let cmdmatches = Command::new("Pipe Command")
        .about("Execute a series of piped commands")
        .args([
            Arg::new("cmdfile").short('f').long("file").value_parser(value_parser!(PathBuf)),
            Arg::new("list processors")
                .short('p')
                .long("proc")
                .num_args(0) //its a flag
                .required(false),
            Arg::new("verbose list processors")
                .short('P')
                .long("proc_verbose")
                .num_args(0) //its a flag
                .required(false),
            Arg::new("processor help").short('H').long("proc_help").num_args(1).required(false),
            Arg::new("trigger timer")
                .help("set trigger timer in seconds")
                .short('T')
                .long("trigger_timer")
                .num_args(1)
                .value_parser(clap::value_parser!(u64).range(1..))
                .required(false),
            Arg::new("dot output")
                .help("generate Graphviz DOT output file instead of running workflow")
                .short('d')
                .long("dot")
                .value_parser(value_parser!(PathBuf))
                .required(false),
            Arg::new("commands")
                .value_name("COMMANDS")
                .help("The commands to execute, separated by pipes (|)")
                .required_unless_present_any([
                    "cmdfile",
                    "list processors",
                    "verbose list processors",
                    "processor help",
                ])
                .num_args(0..),
        ])
        .get_matches();

    if cmdmatches.get_flag("list processors") {
        ProcessHelp::print_process_list();
        return Ok(None);
    }

    if cmdmatches.get_flag("verbose list processors") {
        ProcessHelp::print_process_list_full().await;
        return Ok(None);
    }

    let threshold = cmdmatches.get_one::<u64>("trigger timer").map(|t| Duration::from_secs(*t));
    let dot_output = cmdmatches.get_one::<PathBuf>("dot output").cloned();

    if let Some(proc) = cmdmatches.get_one::<String>("processor help") {
        ProcessHelp::print_process_help(proc).await;
        return Ok(None);
    }

    let file_workflow = match cmdmatches.get_one::<PathBuf>("cmdfile") {
        Some(cmdfile) => file_to_workflow_string(cmdfile.as_path())?,
        None => "".to_string(),
    };

    let commands: Vec<&str> = match cmdmatches.get_many::<String>("commands") {
        Some(cmds) => cmds.map(|s| s.as_str()).collect(),
        None => vec![""],
    };

    let cmd = commands.join(" ");

    let mut cleanstr = cmd.trim().to_owned();
    cleanstr.push(';');
    cleanstr.push_str(&file_workflow);
    debug_log!("combined_workflow: {cleanstr}");

    Ok(Some(CommandLine { workflow: cleanstr, threshold, dot_output }))
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let command = if let Some(envpath) = env::var_os("EVENTWINNOWER_WORKFLOW_FILE") {
        CommandLine {
            workflow: file_to_workflow_string(PathBuf::from(envpath).as_path())?,
            threshold: None,
            dot_output: None,
        }
    } else if let Some(command) = parse_command_line_to_workflow().await? {
        command
    } else {
        return Ok(());
    };

    // Check if DOT output is requested
    if let Some(dot_path) = command.dot_output {
        let (edges, vertices) = parse_workflow(&command.workflow)?;
        generate_dot_output(&edges, &vertices, &dot_path)?;
        return Ok(());
    }

    if let Ok(val) = env::var("EVENTWINNOWER_THREADS") {
        let thread_cnt: usize = val.parse().map_err(|_| {
            anyhow::anyhow!("EVENTWINNOWER_THREADS must be a valid number, got: {}", val)
        })?;
        if env::var("EVENTWINNOWER_THREADPOOL").is_ok() {
            eprintln!("starting eventwinnower threadpool:{thread_cnt}");
            run_threadpool_workflow(command.workflow.as_str(), true, command.threshold, thread_cnt)
                .await?;
        } else {
            eprintln!("starting eventwinnower threaded:{thread_cnt}");
            run_threaded_workflow(command.workflow.as_str(), true, command.threshold, thread_cnt)
                .await?;
        }
    } else {
        run_workflow(command.workflow.as_str(), true, command.threshold).await?;
    }
    Ok(())
}
