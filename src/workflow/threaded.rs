use anyhow::Result;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::executor::block_on;

use crate::workflow::graph::workflow_to_graph;
use crate::workflow::parse::parse_workflow;
use crate::workflow::parse::{WorkflowEdgeId, WorkflowVertex};
use crate::workflow::shutdown::{init_control_c_shutdown, GLOBAL_SHUTDOWN};
use crate::workflow::trigger::{TimerTriggerLocal, Trigger};
use tokio::runtime::{Handle, Runtime};

/// get or create a runtime handle for running async jobs from sync functions
pub fn get_runtime_handle() -> (Handle, Option<Runtime>) {
    match Handle::try_current() {
        Ok(h) => (h, None),
        Err(_) => {
            eprintln!("creating new tokio runtime for thread");
            let rt = Runtime::new().unwrap();
            (rt.handle().clone(), Some(rt))
        }
    }
}

pub async fn run_threaded_workflow(
    workflow: &str,
    handle_ctrlc: bool,
    timer_threshold: Option<Duration>,
    threads: usize,
) -> Result<(), anyhow::Error> {
    let (edges, vertex) = parse_workflow(workflow)?;

    debug_log!("Edge list:");
    for edge in edges.iter() {
        debug_log!("{} -> {}", edge.parent, edge.child);
    }

    if handle_ctrlc {
        init_control_c_shutdown().await;
    }

    //TODO - do this per thread..
    //clone edges,vertex per thread - init per-thread workflow
    //thread::spawn
    let mut handles: Vec<std::thread::JoinHandle<Result<(), anyhow::Error>>> = Vec::new();
    for i in 0..threads {
        let local_edges = edges.clone();
        let local_vertex = vertex.clone();
        let thread_id = i;

        let handle = std::thread::spawn(move || {
            block_on(workflow_thread(local_edges, local_vertex, timer_threshold, thread_id))
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked")?;
    }

    eprintln!("All threads have finished.");

    //wait until all threads are done.
    Ok(())
}

//run until input ends or signal stop
#[allow(clippy::await_holding_refcell_ref)]
async fn workflow_thread(
    edges: Vec<WorkflowEdgeId>,
    vertex: Vec<WorkflowVertex>,
    timer_threshold: Option<Duration>,
    thread_id: usize,
) -> Result<(), anyhow::Error> {
    let (thandle, _trt) = get_runtime_handle();
    let _guard = thandle.enter();

    let (mut sources, children) = workflow_to_graph(edges, vertex, true, true).await?;

    let mut timer = timer_threshold.map(TimerTriggerLocal::new);

    eprintln!("-----------Ready {thread_id} -------------");
    loop {
        let mut has_data = false;
        for source in sources.iter_mut() {
            let events = source.generate().await?;
            has_data |= !events.is_empty();
            source.process_children(&events).await?;
        }

        if Some(Trigger::Timer) == timer.as_mut().map(|t| t.poll()) {
            let mut event_cnt = 0;
            for source in sources.iter_mut() {
                event_cnt += source.trigger_cascade(Trigger::Timer).await?;
            }

            has_data |= event_cnt > 0;
        }

        if !has_data || GLOBAL_SHUTDOWN.load(Ordering::Relaxed) {
            break;
        }
    }
    eprintln!("-----------Flushing Buffers {thread_id}-------------");
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

    eprintln!("-----------Done {thread_id}-------------");
    for source in sources.iter() {
        source.stats().await;
    }
    for child in children.iter() {
        child.borrow().stats().await;
    }

    Ok(())
}
