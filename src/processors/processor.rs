use anyhow::Result;
use async_trait::async_trait;
use std::cell::RefCell;
use std::rc::Rc;

use crate::workflow::graph::ChildRef;
use crate::workflow::trigger::Trigger;

pub type Event = Rc<RefCell<serde_json::Value>>;

pub fn event_new(val: serde_json::Value) -> Event {
    Rc::new(RefCell::new(val))
}

#[async_trait(?Send)]
pub trait AsyncProcessor {
    async fn new(_argv: &[String]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Err(anyhow::anyhow!("Not implemented"))
    }

    async fn new_with_instance_id(
        argv: &[String],
        _instance_id: usize,
    ) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Self::new(argv).await
    }

    // process a single event
    async fn process(&mut self, _input: Event) -> Result<Vec<Event>, anyhow::Error> {
        Err(anyhow::anyhow!("Not implemented"))
    }

    // process batches of events
    async fn process_batch(&mut self, events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        let mut output_events: Vec<Event> = vec![];
        for event in events {
            let mut output = self.process(event.clone()).await?;
            output_events.append(&mut output);
        }
        Ok(output_events)
    }

    /// function called just before exiting to flush out buffered data
    async fn flush(&mut self) -> Vec<Event> {
        vec![]
    }

    /// function called every T minutes for windowing and flushing of buffers
    async fn trigger(&mut self, _trigger_type: Trigger) -> Vec<Event> {
        vec![]
    }

    /// function called at exit to report stats on execution
    async fn stats(&self) -> Option<String> {
        None
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        None
    }
}

#[async_trait(?Send)]
pub trait AsyncBatchChildrenProcessor {
    async fn new(_argv: &[String]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Err(anyhow::anyhow!("Not implemented"))
    }

    async fn new_with_instance_id(
        argv: &[String],
        _instance_id: usize,
    ) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Self::new(argv).await
    }

    /// function called just before exiting to flush out buffered data
    async fn flush(&mut self) -> Vec<Event> {
        vec![]
    }

    /// function called at exit to report stats on execution
    async fn stats(&self) -> Option<String> {
        None
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        None
    }

    async fn process_batch_children(
        &mut self,
        _events: &[Event],
        _parent: &[ChildRef],
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

/// For storing a Processor's create function into a named hashmap
#[async_trait(?Send)]
pub trait ProcessorInit: Sync {
    //calls the Processor's new function
    async fn init(
        &self,
        _argv: &[String],
        _instance_id: usize,
    ) -> Result<ProcessorType, anyhow::Error> {
        Ok(ProcessorType::Null)
    }

    fn get_simple_description(&self) -> Option<String> {
        None
    }
}

pub trait SerialProcessor {
    fn new(_argv: &[String]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Err(anyhow::anyhow!("Not implemented"))
    }

    fn new_with_instance_id(argv: &[String], _instance_id: usize) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Self::new(argv)
    }

    // process a single event
    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        Ok(vec![input])
    }

    // process batches of events
    fn process_batch(&mut self, events: &[Event]) -> Result<Vec<Event>, anyhow::Error> {
        let mut output_events: Vec<Event> = vec![];
        for event in events {
            let mut output = self.process(event.clone())?;
            output_events.append(&mut output);
        }
        Ok(output_events)
    }

    /// function called just before exiting to flush out buffered data
    fn flush(&mut self) -> Vec<Event> {
        vec![]
    }

    /// function called every T minutes for windowing and flushing of buffers
    fn trigger(&mut self, _trigger_type: Trigger) -> Vec<Event> {
        vec![]
    }

    /// function called at exit to report stats on execution
    fn stats(&self) -> Option<String> {
        None
    }

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        None
    }
}

#[async_trait(?Send)]
pub trait AsyncSourceProcessor {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        None
    }

    async fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error>;

    async fn new(_argv: &[String]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Err(anyhow::anyhow!("Not implemented"))
    }

    async fn new_with_instance_id(
        argv: &[String],
        _instance_id: usize,
    ) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Self::new(argv).await
    }

    /// function called just before exiting to flush out buffered data
    async fn flush(&mut self) -> Vec<Event> {
        vec![]
    }

    /// function called every T minutes for windowing and flushing of buffers
    async fn trigger(&mut self, _trigger_type: Trigger) -> Vec<Event> {
        vec![]
    }

    /// function called at exit to report stats on execution
    async fn stats(&self) -> Option<String> {
        None
    }
}

pub trait SerialSourceProcessor {
    fn new(_argv: &[String]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Err(anyhow::anyhow!("Not implemented"))
    }

    fn new_with_instance_id(argv: &[String], _instance_id: usize) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Self::new(argv)
    }

    fn generate(&mut self) -> Result<Vec<Event>, anyhow::Error>;

    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        None
    }

    /// function called at exit to report stats on execution
    fn stats(&self) -> Option<String> {
        None
    }
}

pub enum ProcessorType {
    AsyncSource(Box<dyn AsyncSourceProcessor>),
    Async(Box<dyn AsyncProcessor>),
    AsyncBatchChildren(Box<dyn AsyncBatchChildrenProcessor>),
    SerialSource(Box<dyn SerialSourceProcessor>),
    Serial(Box<dyn SerialProcessor>),
    Null,
}

impl ProcessorType {
    pub fn as_source(self) -> Option<Self> {
        match self {
            ProcessorType::AsyncSource(_) | ProcessorType::SerialSource(_) => Some(self),
            _ => None,
        }
    }
    pub fn as_non_source(self) -> Option<Self> {
        match self {
            ProcessorType::Async(_)
            | ProcessorType::Serial(_)
            | ProcessorType::AsyncBatchChildren(_) => Some(self),
            _ => None,
        }
    }

    pub fn is_serial(&self) -> bool {
        matches!(self, ProcessorType::Serial(_) | ProcessorType::SerialSource(_))
    }
}
