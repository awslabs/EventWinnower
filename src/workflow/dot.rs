use anyhow::Result;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use crate::workflow::parse::{WorkflowEdgeId, WorkflowVertex};

fn format_arguments_multiline(args: &[String], max_line_length: usize) -> String {
    if args.len() <= 1 {
        return String::new();
    }

    let args_str = args[1..].join(" ");
    if args_str.len() <= max_line_length {
        return args_str;
    }

    let mut lines = Vec::new();
    let mut current_line = String::new();

    for arg in &args[1..] {
        // Check if adding this argument would exceed the line length
        let potential_length = if current_line.is_empty() {
            arg.len()
        } else {
            current_line.len() + 1 + arg.len() // +1 for space
        };

        if potential_length > max_line_length && !current_line.is_empty() {
            lines.push(current_line);
            current_line = arg.clone();
        } else {
            if !current_line.is_empty() {
                current_line.push(' ');
            }
            current_line.push_str(arg);
        }
    }

    if !current_line.is_empty() {
        lines.push(current_line);
    }

    lines.join("\\n")
}

pub fn generate_dot_output(
    edges: &[WorkflowEdgeId],
    vertices: &[WorkflowVertex],
    output_path: &PathBuf,
) -> Result<(), anyhow::Error> {
    let mut file = File::create(output_path)?;

    writeln!(file, "digraph workflow {{")?;
    writeln!(file, "    rankdir=LR;")?;
    writeln!(file, "    node [shape=box, style=rounded];")?;
    writeln!(file)?;

    // Write vertices
    for vertex in vertices {
        match vertex {
            WorkflowVertex::Task(inst) => {
                let args_formatted = format_arguments_multiline(&inst.arguments, 30);
                let label = if args_formatted.is_empty() {
                    inst.program.clone()
                } else {
                    format!("{}\\n{}", inst.program, args_formatted)
                };
                writeln!(file, "    {} [label=\"{}\"];", inst.id, label)?;
            }
            WorkflowVertex::Variable(_) => {
                // Skip variables - do not include them in the graph
            }
            WorkflowVertex::None => {}
        }
    }

    writeln!(file)?;

    // Write edges
    for edge in edges {
        writeln!(file, "    {} -> {};", edge.parent, edge.child)?;
    }

    writeln!(file, "}}")?;

    eprintln!("DOT output written to: {}", output_path.display());
    Ok(())
}
