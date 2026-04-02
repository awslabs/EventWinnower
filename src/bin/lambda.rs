use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::Value;
use std::env;

use eventwinnower::processors::processor::{event_new, Event};
use eventwinnower::workflow::parse::{init_children_workflow, run_workflow};

use lazy_static::lazy_static;
use std::sync::Arc;
use std::sync::Mutex;

lazy_static! {
    static ref GLOBAL_WORKFLOW: Arc<Mutex<String>> = Arc::new(Mutex::new("".to_string()));
}

fn clone_workflow() -> String {
    let flow = GLOBAL_WORKFLOW.lock().unwrap();
    flow.clone()
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let workflow = if let Ok(workflow) = env::var("EVENTWINNOWER_WORKFLOW") {
        workflow + ";"
    } else {
        "gen -m 20 | print -p;".to_string()
    };
    //check if running in lambda
    if env::var_os("_HANDLER").is_none() {
        eprintln!("Not running in Lambda environment");

        run_workflow(&workflow, true, None).await?;

        return Ok(());
    } else {
        let mut flow = GLOBAL_WORKFLOW.lock().unwrap();
        *flow = workflow;
    }

    let handler = service_fn(handler);
    lambda_runtime::run(handler).await?;

    //TODO:
    //spin up N parallel independent threads to get passed value events locally
    // - via crossbeam_channels (see threadpool impl)
    // keep workflow context (especially AWS context) between jobs
    // but flush after every batch event
    //optional - shutdown thread on exit..

    Ok(())
}

#[allow(clippy::await_holding_refcell_ref)]
async fn handler(lambda_event: LambdaEvent<Value>) -> Result<Value, Error> {
    //reset workflow for every event
    let workflow = clone_workflow();

    let (event, _context) = lambda_event.into_parts();

    let events = vec![event_new(event)];
    let (immediate_children, all_children) = init_children_workflow(&workflow).await?;
    for child in immediate_children.iter() {
        child.borrow_mut().process_events(&events).await?;
    }
    let mut out: Vec<Event> = Vec::new();
    for child in all_children.iter().filter(|child| child.borrow().name == "acc") {
        out.append(&mut child.borrow_mut().flush_into().await);
    }

    //flush output
    eprintln!("Flushing Output-------------");
    loop {
        let mut event_cnt = 0;
        for child in all_children.iter() {
            let mut child_mut = child.borrow_mut();
            if child_mut.name == "acc" {
                let mut acc_out = child_mut.flush_into().await;
                event_cnt += acc_out.len();
                out.append(&mut acc_out);
            } else {
                event_cnt += child_mut.flush_no_cascade().await?;
            }
        }
        if event_cnt == 0 {
            break;
        }
    }

    //output stats
    eprintln!("Stats----------------");
    for child in all_children.iter() {
        child.borrow().stats().await;
    }
    //all_children.iter().for_each(|child| child.borrow().stats().await);

    if !out.is_empty() {
        let mut out_val: Vec<Value> = Vec::new();
        out.iter().for_each(|element| out_val.push(element.borrow_mut().take()));
        if out_val.len() == 1 {
            Ok(out_val.remove(0))
        } else {
            Ok(Value::Array(out_val))
        }
    } else {
        Ok(Value::Null)
    }
}
