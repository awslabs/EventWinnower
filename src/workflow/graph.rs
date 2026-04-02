use anyhow::{anyhow, Result};
use std::cell::RefCell;
use std::rc::Rc;

use crate::processors::processor::{Event, ProcessorType};
use crate::processors::*;
use crate::workflow::parse::*;
use crate::workflow::trigger::Trigger;
use async_recursion::async_recursion;

pub struct Source {
    name: String,
    processor: ProcessorType,
    //could have batch-oriented children (process_batch(events))
    // single-event children.. (process(event))
    children: Vec<Rc<RefCell<Child>>>,
    id: usize,
}

impl Source {
    async fn new(inst: &Instance, init_instance: bool) -> Result<Source, anyhow::Error> {
        let processor = if init_instance {
            if let Some(processor) = PROCESSORS.get(&inst.program) {
                if let Some(source_processor) =
                    processor.init(&inst.arguments, inst.id).await?.as_source()
                {
                    source_processor
                } else {
                    return Err(anyhow!("{} is not a source processor", inst.program));
                }
            } else {
                return Err(anyhow!("unable to find source processor named {}", inst.program));
            }
        } else {
            PROCESSORS["null_source"].init(&inst.arguments, inst.id).await?
        };
        Ok(Source { name: inst.program.clone(), processor, children: Vec::new(), id: inst.id })
    }

    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn process_children(&self, events: &[Event]) -> Result<(), anyhow::Error> {
        for child in self.children.iter() {
            let mut child_borrow = child.borrow_mut();
            if child_borrow.is_downstream_serial() {
                child_borrow.process_events_serial(events)?;
            } else {
                child_borrow.process_events(events).await?;
            }
        }
        Ok(())
    }

    pub fn set_serial_children(&mut self) {
        for child in self.children.iter() {
            let mut child_borrow = child.borrow_mut();
            child_borrow.set_has_serial_children();
        }
    }

    pub fn get_id(&self) -> usize {
        self.id
    }

    pub async fn flush(&mut self) -> Vec<Event> {
        match self.processor {
            ProcessorType::AsyncSource(ref mut source) => source.flush().await,
            _ => vec![],
        }
    }

    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn flush_children(&self) -> Result<usize, anyhow::Error> {
        let mut event_cnt = 0;
        for child in self.children.iter() {
            event_cnt += child.borrow_mut().flush().await?;
        }
        Ok(event_cnt)
    }

    // trigger source.. trigger children
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn trigger_cascade(&mut self, trigger_type: Trigger) -> Result<usize, anyhow::Error> {
        let events = match self.processor {
            ProcessorType::AsyncSource(ref mut source) => source.trigger(trigger_type).await,
            _ => return Ok(0),
        };
        self.process_children(&events).await?;
        let mut event_cnt = events.len();

        for child in self.children.iter() {
            event_cnt += child.borrow_mut().trigger_cascade(trigger_type).await?;
        }

        Ok(event_cnt)
    }

    pub async fn stats(&self) {
        let stats_option = match &self.processor {
            ProcessorType::AsyncSource(source) => source.stats().await,
            ProcessorType::SerialSource(source) => source.stats(),
            _ => None,
        };
        if let Some(stats) = stats_option {
            eprintln!("{} stats -----\n{}", self.name, stats);
        }
    }

    fn print_tree(&self) {
        debug_log!("{}", self.name);
        for child in self.children.iter() {
            child.borrow().print_tree(1);
        }
    }

    pub async fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        match self.processor {
            ProcessorType::AsyncSource(ref mut source) => source.generate().await,
            ProcessorType::SerialSource(ref mut source) => source.generate(),
            _ => Err(anyhow!("invalid source")),
        }
    }

    pub fn clone_children(&mut self) -> Vec<ChildRef> {
        self.children.clone()
    }
}

pub type ChildRef = Rc<RefCell<Child>>;

pub struct Child {
    pub name: String,
    processor: ProcessorType,
    children: Vec<ChildRef>,
    id: usize,
    is_serial: bool,
    has_serial_children: bool,
}

impl Child {
    async fn new(inst: &Instance, init_instance: bool) -> Result<Child, anyhow::Error> {
        let processor = if init_instance {
            if let Some(processor) = PROCESSORS.get(&inst.program) {
                if let Some(child_processor) =
                    processor.init(&inst.arguments, inst.id).await?.as_non_source()
                {
                    child_processor
                } else {
                    return Err(anyhow!("{} is not a valid processor", inst.program));
                }
            } else {
                return Err(anyhow!("unable to find processor named {}", inst.program));
            }
        } else {
            PROCESSORS["null"].init(&inst.arguments, inst.id).await?
        };
        let is_serial = processor.is_serial();
        Ok(Child {
            name: inst.program.clone(),
            processor,
            children: Vec::new(),
            id: inst.id,
            is_serial,
            has_serial_children: false,
        })
    }

    pub fn set_has_serial_children(&mut self) -> bool {
        if self.has_serial_children {
            return true;
        }
        if self.children.is_empty() {
            self.has_serial_children = true;
            return true;
        }
        let mut serial_children = true;
        for child in self.children.iter() {
            let mut child_borrow = child.borrow_mut();
            if !child_borrow.is_serial {
                serial_children = false;
            }
            if !child_borrow.set_has_serial_children() {
                serial_children = false;
            }
        }
        self.has_serial_children = serial_children;
        serial_children
    }

    pub fn is_downstream_serial(&self) -> bool {
        self.is_serial && self.has_serial_children
    }

    pub fn process_children_serial(&self, events: &[Event]) -> Result<(), anyhow::Error> {
        if !events.is_empty() {
            for child in self.children.iter() {
                child.borrow_mut().process_events_serial(events)?;
            }
        }
        Ok(())
    }

    pub fn process_events_serial(&mut self, events: &[Event]) -> Result<(), anyhow::Error> {
        if events.is_empty() {
            return Ok(());
        }
        match self.processor {
            ProcessorType::Serial(ref mut proc) => {
                let result_events = proc.process_batch(events)?;
                self.process_children_serial(&result_events)
            }
            _ => Ok(()),
        }
    }

    #[async_recursion(?Send)]
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn process_children(&self, events: &[Event]) -> Result<(), anyhow::Error> {
        if !events.is_empty() {
            for child in self.children.iter() {
                child.borrow_mut().process_events(events).await?;
            }
        }
        Ok(())
    }

    #[async_recursion(?Send)]
    pub async fn process_events(&mut self, events: &[Event]) -> Result<(), anyhow::Error> {
        if events.is_empty() {
            return Ok(());
        }
        match self.processor {
            ProcessorType::Async(ref mut proc) => {
                let result_events = proc.process_batch(events).await?;
                self.process_children(&result_events).await
            }
            ProcessorType::AsyncBatchChildren(ref mut proc) => {
                proc.process_batch_children(events, &self.children).await
            }
            ProcessorType::Serial(ref mut proc) => {
                let result_events = proc.process_batch(events)?;
                self.process_children(&result_events).await
            }
            _ => Ok(()),
        }
    }

    #[async_recursion(?Send)]
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn flush(&mut self) -> Result<usize, anyhow::Error> {
        let mut event_cnt = 0;
        let events = match self.processor {
            ProcessorType::Async(ref mut proc) => proc.flush().await,
            ProcessorType::AsyncBatchChildren(ref mut proc) => proc.flush().await,
            ProcessorType::Serial(ref mut proc) => proc.flush(),
            _ => vec![],
        };
        if events.is_empty() {
            for child in self.children.iter() {
                event_cnt += child.borrow_mut().flush().await?;
            }
        } else {
            event_cnt += events.len();
            self.process_children(&events).await?;
        }
        Ok(event_cnt)
    }

    pub async fn flush_into(&mut self) -> Vec<Event> {
        match self.processor {
            ProcessorType::Async(ref mut proc) => proc.flush().await,
            ProcessorType::AsyncBatchChildren(ref mut proc) => proc.flush().await,
            ProcessorType::Serial(ref mut proc) => proc.flush(),
            _ => vec![],
        }
    }

    pub async fn flush_no_cascade(&mut self) -> Result<usize, anyhow::Error> {
        let mut event_cnt = 0;
        let events = match self.processor {
            ProcessorType::Async(ref mut proc) => proc.flush().await,
            ProcessorType::AsyncBatchChildren(ref mut proc) => proc.flush().await,
            ProcessorType::Serial(ref mut proc) => proc.flush(),
            _ => vec![],
        };
        if !events.is_empty() {
            event_cnt += events.len();
            self.process_children(&events).await?;
        }
        Ok(event_cnt)
    }

    // trigger this.. trigger children
    #[async_recursion(?Send)]
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn trigger_cascade(&mut self, trigger_type: Trigger) -> Result<usize, anyhow::Error> {
        let events = match self.processor {
            ProcessorType::Async(ref mut proc) => proc.trigger(trigger_type).await,
            ProcessorType::Serial(ref mut proc) => proc.trigger(trigger_type),
            _ => vec![],
        };
        self.process_children(&events).await?;
        let mut event_cnt = events.len();

        for child in self.children.iter() {
            event_cnt += child.borrow_mut().trigger_cascade(trigger_type).await?;
        }

        Ok(event_cnt)
    }

    pub async fn stats(&self) {
        let stats_option = match &self.processor {
            ProcessorType::Async(source) => source.stats().await,
            ProcessorType::AsyncBatchChildren(source) => source.stats().await,
            ProcessorType::Serial(source) => source.stats(),
            _ => None,
        };
        if let Some(stats) = stats_option {
            eprintln!("{} stats -----\n{}", self.name, stats);
        }
    }

    fn print_tree(&self, offset: usize) {
        debug_log!("{}{}", (0..offset).map(|_| "-").collect::<String>(), self.name);
        for child in self.children.iter() {
            child.borrow().print_tree(offset + 1);
        }
    }
}

pub async fn workflow_to_graph(
    edges: Vec<WorkflowEdgeId>,
    mut vertex: Vec<WorkflowVertex>,
    init_sources: bool,
    init_children: bool,
) -> Result<(Vec<Source>, Vec<ChildRef>), anyhow::Error> {
    let mut sources: Vec<Source> = Vec::new();
    let mut children: Vec<ChildRef> = Vec::new();

    debug_log!("Vertex list:");
    for vert in vertex.iter_mut() {
        if let WorkflowVertex::Task(inst) = vert {
            if (inst.parent_cnt > 0) && (inst.child_cnt == 0) {
                sources.push(Source::new(inst, init_sources).await?);
                debug_log!("Vertex {:?} - Source", &vert);
            } else {
                children.push(Rc::new(RefCell::new(Child::new(inst, init_children).await?)));
                inst.init_offset = Some(children.len() - 1);
                debug_log!("Vertex {:?}", &vert);
            }
        }
    }

    for source in sources.iter_mut() {
        for edge in edges.iter() {
            if source.id == edge.parent {
                if let WorkflowVertex::Task(inst) = &vertex[edge.child] {
                    source.children.push(children[inst.init_offset.expect("mapped child")].clone());
                }
            }
        }
    }

    for childref in children.iter() {
        let mut child = childref.borrow_mut();
        for edge in edges.iter() {
            if child.id == edge.parent {
                if let WorkflowVertex::Task(inst) = &vertex[edge.child] {
                    child.children.push(children[inst.init_offset.expect("mapped child")].clone());
                }
            }
        }
    }

    for source in sources.iter_mut() {
        source.set_serial_children();
        source.print_tree();
    }

    Ok((sources, children))
}
