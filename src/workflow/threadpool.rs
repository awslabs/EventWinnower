use anyhow::Result;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::processors::processor::event_new;
use crate::workflow::graph::workflow_to_graph;
use crate::workflow::parse::parse_workflow;
use crate::workflow::parse::{WorkflowEdgeId, WorkflowVertex};
use crate::workflow::shutdown::{init_control_c_shutdown, GLOBAL_SHUTDOWN};
use crate::workflow::threaded::get_runtime_handle;
use crate::workflow::trigger::{TimerTriggerLocal, Trigger};
use futures::executor::block_on;

use crossbeam_channel::{bounded, Receiver, Sender};

type ChannelData = (usize, serde_json::Value);

// Run all sources in one thread, load balance the processing of events between threads from the sources
pub async fn run_threadpool_workflow(
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

    let (sender, receiver) = bounded(threads);

    //TODO - do this per thread..
    //clone edges,vertex per thread - init per-thread workflow
    //thread::spawn
    let mut handles = Vec::new();
    for i in 0..threads {
        let local_edges = edges.clone();
        let local_vertex = vertex.clone();
        let local_receiver = receiver.clone();
        let thread_id = i;

        let handle = std::thread::spawn(move || {
            block_on(workflow_thread(
                local_edges,
                local_vertex,
                timer_threshold,
                thread_id,
                local_receiver,
            ))
        });

        handles.push(handle);
    }

    //we no longer need this receiver
    drop(receiver);

    workflow_source_task(edges, vertex, sender).await?;

    for handle in handles {
        handle.join().expect("Thread panicked")?;
    }

    eprintln!("All threads have finished.");

    //wait until all threads are done.
    Ok(())
}

#[allow(clippy::await_holding_refcell_ref)]
async fn workflow_source_task(
    edges: Vec<WorkflowEdgeId>,
    vertex: Vec<WorkflowVertex>,
    sender: Sender<ChannelData>,
) -> Result<(), anyhow::Error> {
    let (thandle, _trt) = get_runtime_handle();
    let _guard = thandle.enter();
    let (mut sources, _children) =
        workflow_to_graph(edges.clone(), vertex.clone(), true, false).await?;

    let mut do_exit = false;
    loop {
        let mut has_data = false;
        for source in sources.iter_mut() {
            let events = source.generate().await?;
            for event in events {
                match sender.send((source.get_id(), event.borrow().clone())) {
                    Ok(_) => {
                        has_data = true;
                    }
                    Err(e) => {
                        eprintln!("unable to send to channel {e:?}");
                        do_exit = true;
                    }
                }
                if GLOBAL_SHUTDOWN.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
        if !has_data || do_exit || GLOBAL_SHUTDOWN.load(Ordering::Relaxed) {
            break;
        }
    }
    if !do_exit {
        eprintln!("-----------Flushing Buffers Sources-------------");
        loop {
            let mut event_cnt = 0;
            for source in sources.iter_mut() {
                let events = source.flush().await;
                for event in events {
                    match sender.send((source.get_id(), event.borrow().clone())) {
                        Ok(_) => event_cnt += 1,
                        Err(e) => {
                            eprintln!("error flushing to channel {e:?}");
                            do_exit = true;
                        }
                    }
                }
            }

            if do_exit || (event_cnt == 0) {
                break;
            }
        }
    }
    eprintln!("-----------Done Sources -------------");
    for source in sources.iter() {
        source.stats().await;
    }

    Ok(())
}

//run until input ends or signal stop
#[allow(clippy::await_holding_refcell_ref)]
async fn workflow_thread(
    edges: Vec<WorkflowEdgeId>,
    vertex: Vec<WorkflowVertex>,
    timer_threshold: Option<Duration>,
    thread_id: usize,
    receiver: Receiver<ChannelData>,
) -> Result<(), anyhow::Error> {
    let (thandle, _trt) = get_runtime_handle();
    let _guard = thandle.enter();
    let (sources, children) = workflow_to_graph(edges, vertex, false, true).await?;

    let mut timer = timer_threshold.map(TimerTriggerLocal::new);

    let mut source_map = HashMap::new();
    for source in sources {
        source_map.insert(source.get_id(), source);
    }

    eprintln!("-----------Ready {thread_id} -------------");
    let mut retry_cnt: usize = 0;
    let ten_millis = std::time::Duration::from_millis(10);
    let hundred_millis = std::time::Duration::from_millis(100);
    loop {
        let mut do_exit = false;

        match receiver.try_recv() {
            Ok((source_id, event)) => {
                retry_cnt = 0;
                if let Some(source) = source_map.get(&source_id) {
                    source.process_children(&[event_new(event)]).await?;
                }
            }
            Err(crossbeam_channel::TryRecvError::Empty) => {
                if retry_cnt < 20 {
                    std::thread::yield_now();
                } else if retry_cnt < 100 {
                    std::thread::sleep(ten_millis);
                } else {
                    std::thread::sleep(hundred_millis);
                }
                retry_cnt += 1;
            }
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                eprintln!("--- sending source stopped, exiting thread {thread_id}");
                do_exit = true;
            }
        }

        if !do_exit {
            if Some(Trigger::Timer) == timer.as_mut().map(|t| t.poll()) {
                for (_key, source) in source_map.iter_mut() {
                    source.trigger_cascade(Trigger::Timer).await?;
                }
            }
        } else {
            break;
        }
    }
    eprintln!("-----------Flushing Buffers {thread_id}-------------");
    loop {
        let mut event_cnt = 0;
        for (_key, source) in source_map.iter_mut() {
            event_cnt += source.flush_children().await?;
        }

        if event_cnt == 0 {
            break;
        }
    }

    eprintln!("-----------Done {thread_id}-------------");
    for child in children.iter() {
        child.borrow().stats().await;
    }

    Ok(())
}
