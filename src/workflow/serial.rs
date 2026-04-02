use crate::workflow::parse::parse_workflow;
use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::workflow::graph::workflow_to_graph;
use crate::workflow::trigger::{TimerTrigger, TRIGGER_STATE};

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

    let signal_stop = Arc::new(AtomicBool::new(false));
    let handler_signal_stop = signal_stop.clone();
    let mut stop_cnt = 0;

    if handle_ctrlc {
        ctrlc::set_handler(move || {
            eprintln!("received Control+C");
            handler_signal_stop.store(true, Ordering::Relaxed);
            stop_cnt += 1;
            if stop_cnt > 3 {
                eprintln!("Force Stopping");
                std::process::exit(-1);
            }
        })
        .expect("Error setting signal handler");
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

        if !has_data || signal_stop.load(Ordering::Relaxed) {
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
