use lazy_static::lazy_static;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Trigger {
    Timer,
    Event,
    None,
}

impl Trigger {
    pub fn is_none(&self) -> bool {
        matches!(self, Trigger::None)
    }
    pub fn is_triggered(&self) -> bool {
        !matches!(self, Trigger::None)
    }
}

#[derive(Clone)]
pub struct TriggerState {
    global: Trigger,
}

impl TriggerState {
    pub fn new() -> TriggerState {
        TriggerState { global: Trigger::None }
    }
    pub fn set_global(&mut self, trigger: Trigger) {
        self.global = trigger;
    }
    pub fn get_global(&self) -> Option<Trigger> {
        if self.global.is_none() {
            return None;
        }
        Some(self.global)
    }

    pub fn get_global_and_clear(&mut self) -> Option<Trigger> {
        let out = self.get_global();
        self.global = Trigger::None;
        out
    }
}

impl Default for TriggerState {
    fn default() -> Self {
        Self::new()
    }
}

lazy_static! {
    pub static ref TRIGGER_STATE: Mutex<TriggerState> = Mutex::new(TriggerState::new());
}

pub struct TimerTrigger {
    threshold: Duration,
    start_time: Instant,
}

impl TimerTrigger {
    pub fn new(threshold: Duration) -> Self {
        Self { threshold, start_time: Instant::now() }
    }

    pub fn poll(&mut self) -> Result<(), anyhow::Error> {
        let now = Instant::now();
        if now.duration_since(self.start_time) > self.threshold {
            TRIGGER_STATE.lock().expect("trigger state should unlock").set_global(Trigger::Timer);
            self.start_time = now;
        }
        Ok(())
    }
}

pub struct TimerTriggerLocal {
    threshold: Duration,
    start_time: Instant,
}
impl TimerTriggerLocal {
    pub fn new(threshold: Duration) -> Self {
        Self { threshold, start_time: Instant::now() }
    }
    pub fn poll(&mut self) -> Trigger {
        let now = Instant::now();
        if now.duration_since(self.start_time) > self.threshold {
            self.start_time = now;
            Trigger::Timer
        } else {
            Trigger::None
        }
    }
}
