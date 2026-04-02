use logos::Logos;

use anyhow::Result;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::workflow::env_preprocess;
use crate::workflow::graph::{workflow_to_graph, ChildRef};
use crate::workflow::shutdown::{init_control_c_shutdown, GLOBAL_SHUTDOWN};
use crate::workflow::trigger::{TimerTrigger, TRIGGER_STATE};

#[derive(Logos, Debug, PartialEq)]
#[logos(skip r"[ \t\f]+")]
enum Token<'a> {
    #[regex("\'[^\']*\'")]
    SingleQuoteString(&'a str),

    #[regex("\"[^\"]*\"")]
    DoubleQuoteString(&'a str),

    #[token("|")]
    Pipe,

    #[regex("[;\n]", priority = 5)]
    Semicolon,

    #[regex("\\$[a-zA-Z][a-zA-Z0-9_]*")]
    Variable(&'a str),

    #[regex("[a-zA-Z][a-zA-Z0-9_]*", priority = 3)]
    ProgName(&'a str),

    #[regex(r"-+[^\s;|]*", priority = 2)]
    Arg(&'a str),

    #[regex(r"[^\s;|]+", priority = 1)]
    Val(&'a str),

    #[regex(r"#[^\n;]*", priority = 6)]
    Comment,
}

/// TODO:
///  support INPUT/OUTPUT ports, support multi-variables as input
enum Grammar {
    Start,    //start of line
    Program,  //program name give, awaiting argument
    Variable, //variable name given, awaiting pipe
}

#[derive(Debug, Clone)]
pub struct Instance {
    pub program: String,
    pub arguments: Vec<String>,
    pub id: usize,
    pub parent_cnt: u32,
    pub child_cnt: u32,
    pub init_offset: Option<usize>,
}

impl Instance {
    fn new(progname: &str, pid: usize) -> Instance {
        Instance {
            program: String::from(progname),
            arguments: vec![progname.to_string()],
            id: pid,
            parent_cnt: 0,
            child_cnt: 0,
            init_offset: None,
        }
    }

    fn append_arg(&mut self, arg: &str) {
        self.arguments.push(String::from(arg));
    }
}

#[derive(Clone)]
pub struct WorkflowEdgeId {
    pub parent: usize,
    pub child: usize,
}

impl WorkflowEdgeId {
    fn collapse(full_edges: &[WorkflowEdgeId], vertex: Vec<usize>) -> Vec<WorkflowEdgeId> {
        let mut revised: Vec<WorkflowEdgeId> = Vec::new();

        for edge in full_edges.iter() {
            if vertex.contains(&edge.child) {
                WorkflowEdgeId::edge_child_swap(full_edges, &mut revised, edge.child, edge.parent);
            } else if vertex.contains(&edge.parent) {
                //eprintln!("parent variable {} {}", edge.parent, edge.child);
            } else {
                revised.push(WorkflowEdgeId { parent: edge.parent, child: edge.child });
            }
        }

        revised
    }

    fn edge_child_swap(
        full_edges: &[WorkflowEdgeId],
        revised: &mut Vec<WorkflowEdgeId>,
        key: usize,
        swap: usize,
    ) {
        for edge in full_edges.iter() {
            if edge.parent == key {
                revised.push(WorkflowEdgeId { parent: swap, child: edge.child });
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum WorkflowVertex {
    Variable(Instance),
    Task(Instance),
    None,
}

impl WorkflowVertex {
    fn append_arg(&mut self, arg: &str) {
        if let WorkflowVertex::Task(inst) = self {
            inst.append_arg(arg)
        }
    }

    fn get_id(&self) -> Option<usize> {
        match self {
            WorkflowVertex::Task(inst) => Some(inst.id),
            WorkflowVertex::Variable(inst) => Some(inst.id),
            _ => None,
        }
    }

    fn find_prior_var(prior_vertex: &[WorkflowVertex], var_string: &str) -> Option<WorkflowVertex> {
        for vertex in prior_vertex.iter() {
            if let WorkflowVertex::Variable(v) = vertex {
                if v.program == var_string {
                    return Some((*vertex).clone());
                }
            }
        }
        None
    }

    fn get_variable_ids(vlist: &[WorkflowVertex]) -> Vec<usize> {
        let mut out: Vec<usize> = Vec::new();

        for v in vlist.iter() {
            if let WorkflowVertex::Variable(inst) = v {
                out.push(inst.id);
            }
        }

        out
    }

    fn add_parent_reference(vlist: &mut [WorkflowVertex], id: usize) {
        for v in vlist.iter_mut() {
            if let WorkflowVertex::Task(inst) = v {
                if inst.id == id {
                    inst.parent_cnt += 1;
                }
            }
        }
    }

    fn add_child_reference(vlist: &mut [WorkflowVertex], id: usize) {
        for v in vlist.iter_mut() {
            if let WorkflowVertex::Task(inst) = v {
                if inst.id == id {
                    inst.child_cnt += 1;
                    return;
                }
            }
        }
    }
}

fn complete_edge(
    priorid: &mut Option<usize>,
    current_vertex: WorkflowVertex,
    prior_vertex: &mut Vec<WorkflowVertex>,
    edges: &mut Vec<WorkflowEdgeId>,
    nextid: usize,
) {
    if !matches!(current_vertex, WorkflowVertex::None) {
        let cid = current_vertex.get_id().unwrap();

        if let Some(id) = *priorid {
            edges.push(WorkflowEdgeId { parent: id, child: cid });
        }

        let mut dupeid = false;

        if let Some(v) = prior_vertex.last() {
            if v.get_id().unwrap() == cid {
                dupeid = true;
            }
        }

        *priorid = current_vertex.get_id();

        if !dupeid && (cid == (nextid - 1)) {
            prior_vertex.push(current_vertex);
        }
    }
}

pub fn parse_workflow(
    workflow_string: &str,
) -> Result<(Vec<WorkflowEdgeId>, Vec<WorkflowVertex>), anyhow::Error> {
    let mut gstate: Grammar = Grammar::Start;

    let mut nextid: usize = 0;

    let mut priorid: Option<usize> = None;

    let mut edges: Vec<WorkflowEdgeId> = Vec::new();
    let mut prior_vertex: Vec<WorkflowVertex> = Vec::new();
    let mut current_vertex: WorkflowVertex = WorkflowVertex::None;

    let workflow_transform = env_preprocess::env_preprocess(workflow_string)?;

    let mut lexer = Token::lexer(workflow_transform.as_str());
    while let Some(tok) = lexer.next() {
        let _val = lexer.slice();
        //implement grammar
        match gstate {
            Grammar::Start => {
                complete_edge(&mut priorid, current_vertex, &mut prior_vertex, &mut edges, nextid);
                current_vertex = WorkflowVertex::None;

                match tok {
                    Ok(Token::ProgName(pname)) => {
                        gstate = Grammar::Program;
                        current_vertex = WorkflowVertex::Task(Instance::new(pname, nextid));
                        nextid += 1;
                    }
                    Ok(Token::Variable(var_string)) => {
                        gstate = Grammar::Variable;
                        match WorkflowVertex::find_prior_var(&prior_vertex, var_string) {
                            Some(v) => current_vertex = v,
                            None => {
                                current_vertex =
                                    WorkflowVertex::Variable(Instance::new(var_string, nextid));
                                nextid += 1;
                            }
                        }
                    }
                    Ok(Token::Comment) => (),
                    _ => (),
                }
            }
            Grammar::Program => match tok {
                Ok(Token::Pipe) => gstate = Grammar::Start,
                Ok(Token::Semicolon) => {
                    complete_edge(
                        &mut priorid,
                        current_vertex,
                        &mut prior_vertex,
                        &mut edges,
                        nextid,
                    );
                    current_vertex = WorkflowVertex::None;
                    priorid = None;
                    gstate = Grammar::Start;
                }
                Ok(Token::ProgName(arg)) => current_vertex.append_arg(arg),
                Ok(Token::SingleQuoteString(arg)) => {
                    current_vertex.append_arg(&arg[1..arg.len() - 1])
                }
                Ok(Token::DoubleQuoteString(arg)) => {
                    current_vertex.append_arg(&arg[1..arg.len() - 1])
                }
                Ok(Token::Arg(arg)) => current_vertex.append_arg(arg),
                Ok(Token::Val(arg)) => current_vertex.append_arg(arg),
                Ok(Token::Comment) => (),
                _ => eprintln!("unknown tag after arg"),
            },
            Grammar::Variable => match tok {
                Ok(Token::Pipe) => gstate = Grammar::Start,
                Ok(Token::Semicolon) => {
                    complete_edge(
                        &mut priorid,
                        current_vertex,
                        &mut prior_vertex,
                        &mut edges,
                        nextid,
                    );
                    current_vertex = WorkflowVertex::None;
                    priorid = None;
                    gstate = Grammar::Start;
                }
                Ok(Token::Comment) => (),
                _ => return Err(anyhow::anyhow!("unknown arguments to variable")),
            },
        }
    }

    let collapse_edges =
        WorkflowEdgeId::collapse(&edges, WorkflowVertex::get_variable_ids(&prior_vertex));

    for edge in collapse_edges.iter() {
        WorkflowVertex::add_parent_reference(&mut prior_vertex, edge.parent);
        WorkflowVertex::add_child_reference(&mut prior_vertex, edge.child);
    }

    Ok((collapse_edges, prior_vertex))
}

#[allow(dead_code)]
pub fn file_to_workflow_string(workflow_file: &Path) -> anyhow::Result<String, anyhow::Error> {
    eprintln!("loading workflow from file:{}", workflow_file.display());
    let fp = File::open(workflow_file)?;
    let buffer = BufReader::new(fp);

    let mut cmds: Vec<String> = Vec::new();
    let mut continuation = String::new();
    for line in buffer.lines().map_while(Result::ok) {
        let tline = line.trim();

        if tline.starts_with("#") || tline.starts_with("//") || tline.is_empty() {
            continue;
        }

        if let Some(prefix) = tline.strip_suffix('\\') {
            continuation.push_str(prefix);
            continuation.push(' ');
        } else {
            continuation.push_str(tline);
            cmds.push(continuation);
            continuation = String::new();
        }
    }
    if !continuation.is_empty() {
        cmds.push(continuation);
    }
    cmds.push(";".to_string());
    let single_string = cmds.join(";");
    Ok(single_string)
}

// init workflow for processing children (used by Lambda, where lambda is a psuedo-source)
pub async fn init_children_workflow(
    workflow: &str,
) -> Result<(Vec<ChildRef>, Vec<ChildRef>), anyhow::Error> {
    let (edges, vertex) = parse_workflow(workflow)?;
    let (mut sources, all_children) = workflow_to_graph(edges, vertex, false, true).await?;

    let mut immediate_children: Vec<ChildRef> = Vec::new();
    sources.iter_mut().for_each(|source| immediate_children.append(&mut source.clone_children()));

    Ok((immediate_children, all_children))
}

#[allow(clippy::await_holding_refcell_ref)]
pub async fn run_workflow(
    workflow: &str,
    handle_ctrlc: bool,
    timer_threshold: Option<Duration>,
) -> Result<(), anyhow::Error> {
    let (edges, vertex) = parse_workflow(workflow)?;

    debug_log!("Edge list:");
    for edge in edges.iter() {
        debug_log!("{} -> {}", edge.parent, edge.child);
    }

    let (mut sources, children) = workflow_to_graph(edges, vertex, true, true).await?;

    if handle_ctrlc {
        init_control_c_shutdown().await;
    }

    let mut timer = timer_threshold.map(TimerTrigger::new);

    eprintln!("-----------Ready-------------");
    loop {
        let mut has_data = false;
        for source in sources.iter_mut() {
            let events = source.generate().await?;
            has_data |= !events.is_empty();
            source.process_children(&events).await?;
        }

        let trigger_type_option =
            TRIGGER_STATE.lock().expect("trigger state should unlock").get_global_and_clear();
        if let Some(trigger_type) = trigger_type_option {
            let mut event_cnt = 0;
            for source in sources.iter_mut() {
                event_cnt += source.trigger_cascade(trigger_type).await?;
            }

            has_data |= event_cnt > 0;
        }

        if !has_data || GLOBAL_SHUTDOWN.load(Ordering::Relaxed) {
            break;
        }

        timer.as_mut().map(|t| t.poll());
    }
    eprintln!("-----------Flushing Buffers-------------");
    loop {
        let mut event_cnt = 0;
        for source in sources.iter_mut() {
            let events = source.flush().await;
            if events.is_empty() {
                event_cnt += source.flush_children().await?;
            } else {
                event_cnt += events.len();
                source.process_children(&events).await?;
            }
        }

        if event_cnt == 0 {
            break;
        }
    }

    eprintln!("-----------Done-------------");
    for source in sources.iter() {
        source.stats().await;
    }
    for child in children.iter() {
        child.borrow().stats().await;
    }

    Ok(())
}
