use crate::processors::processor::*;
use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

// Default values
const DEFAULT_WINDOW_SIZE: &str = "1h";
const DEFAULT_EMIT_MODE: &str = "on-close";
const DEFAULT_AGGREGATE: &str = "count";
const DEFAULT_ALLOWED_LATENESS: &str = "0s";

/// Parse duration strings like 30s, 5m, 1h, 1d, 500ms
pub fn parse_duration(s: &str) -> Result<Duration, anyhow::Error> {
    let s = s.trim();
    if let Some(stripped) = s.strip_suffix("ms") {
        let num: u64 =
            stripped.parse().map_err(|_| anyhow::anyhow!("Invalid duration format: {}", s))?;
        Ok(Duration::from_millis(num))
    } else if let Some(stripped) = s.strip_suffix('s') {
        let num: u64 =
            stripped.parse().map_err(|_| anyhow::anyhow!("Invalid duration format: {}", s))?;
        Ok(Duration::from_secs(num))
    } else if let Some(stripped) = s.strip_suffix('m') {
        let num: u64 =
            stripped.parse().map_err(|_| anyhow::anyhow!("Invalid duration format: {}", s))?;
        Ok(Duration::from_secs(num * 60))
    } else if let Some(stripped) = s.strip_suffix('h') {
        let num: u64 =
            stripped.parse().map_err(|_| anyhow::anyhow!("Invalid duration format: {}", s))?;
        Ok(Duration::from_secs(num * 3600))
    } else if let Some(stripped) = s.strip_suffix('d') {
        let num: u64 =
            stripped.parse().map_err(|_| anyhow::anyhow!("Invalid duration format: {}", s))?;
        Ok(Duration::from_secs(num * 86400))
    } else {
        Err(anyhow::anyhow!("Invalid duration format: {}", s))
    }
}

/// Aggregation type for window computations
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum AggregationType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregationType {
    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        match s.to_lowercase().as_str() {
            "count" => Ok(AggregationType::Count),
            "sum" => Ok(AggregationType::Sum),
            "avg" => Ok(AggregationType::Avg),
            "min" => Ok(AggregationType::Min),
            "max" => Ok(AggregationType::Max),
            _ => Err(anyhow::anyhow!(
                "Invalid aggregation type: {}. Valid types: count, sum, avg, min, max",
                s
            )),
        }
    }
}

/// Emit mode for when to output window results
#[derive(Clone, Copy, PartialEq, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum EmitMode {
    OnClose,
    OnUpdate,
    OnFlush,
}

impl EmitMode {
    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        match s.to_lowercase().as_str() {
            "on-close" => Ok(EmitMode::OnClose),
            "on-update" => Ok(EmitMode::OnUpdate),
            "on-flush" => Ok(EmitMode::OnFlush),
            _ => Err(anyhow::anyhow!(
                "Invalid emit mode: {}. Valid modes: on-close, on-update, on-flush",
                s
            )),
        }
    }
}

/// Key for identifying a window (window_start + optional key)
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct WindowKey {
    pub window_start: i64, // Unix timestamp in milliseconds
    pub key: Option<String>,
}

/// State for a single window
#[derive(Clone, Debug)]
pub struct WindowState {
    pub window_start: i64,
    pub window_end: i64,
    pub key: Option<String>,
    pub count: u64,
    pub value_count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl WindowState {
    pub fn new(window_start: i64, window_end: i64, key: Option<String>) -> Self {
        WindowState {
            window_start,
            window_end,
            key,
            count: 0,
            value_count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    /// Add a value to the window aggregation.
    /// count always increments (tracks total events).
    /// value_count, sum, min, max only update when a valid numeric value is provided.
    /// Events with None values are skipped for numeric aggregation per Requirement 3.7.
    pub fn add_value(&mut self, value: Option<f64>) {
        self.count += 1;
        if let Some(v) = value {
            self.value_count += 1;
            self.sum += v;
            if v < self.min {
                self.min = v;
            }
            if v > self.max {
                self.max = v;
            }
        }
    }

    /// Get the average value (sum / count of events with valid values)
    pub fn get_avg(&self) -> Option<f64> {
        if self.value_count > 0 {
            Some(self.sum / self.value_count as f64)
        } else {
            None
        }
    }
}

/// State for a session window
#[derive(Clone, Debug)]
pub struct SessionState {
    pub start_time: i64,
    pub last_event_time: i64,
    pub count: u64,
    pub value_count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl SessionState {
    pub fn new(timestamp: i64) -> Self {
        SessionState {
            start_time: timestamp,
            last_event_time: timestamp,
            count: 0,
            value_count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    /// Add a value to the session aggregation.
    /// count always increments. value_count, sum, min, max only update for valid values.
    pub fn add_value(&mut self, timestamp: i64, value: Option<f64>) {
        self.count += 1;
        self.last_event_time = self.last_event_time.max(timestamp);
        if let Some(v) = value {
            self.value_count += 1;
            self.sum += v;
            if v < self.min {
                self.min = v;
            }
            if v > self.max {
                self.max = v;
            }
        }
    }

    /// Get the average value (sum / count of events with valid values)
    pub fn get_avg(&self) -> Option<f64> {
        if self.value_count > 0 {
            Some(self.sum / self.value_count as f64)
        } else {
            None
        }
    }
}

#[derive(Parser)]
#[command(
    version,
    about = "Aggregate events within time windows (tumbling, sliding, session)",
    long_about = r#"Aggregate events within time windows (tumbling, sliding, session)

The window processor groups events by time and computes aggregate statistics.
It supports three window types:

  TUMBLING WINDOWS (default)
    Non-overlapping, fixed-size windows aligned to the window size.
    Each event belongs to exactly one window.

  SLIDING WINDOWS (--slide)
    Overlapping windows that advance by a slide interval.
    Events may belong to multiple windows.

  SESSION WINDOWS (--session-gap)
    Dynamic windows based on activity gaps per key.
    A session closes when no events arrive within the gap duration.

TIMESTAMP FORMATS:
  The processor auto-detects: ISO8601/RFC3339, Unix epoch (seconds or
  milliseconds), and float seconds. Use --time-format for custom formats.

DURATION SYNTAX:
  500ms, 30s, 5m, 1h, 1d (milliseconds, seconds, minutes, hours, days)

EMIT MODES:
  on-close   Emit when window closes (default, best for batch processing)
  on-update  Emit after each event (real-time streaming)
  on-flush   Emit only on flush (manual control)

EXAMPLES:
  # Count events per hour (tumbling window)
  ... | window timestamp -w 1h

  # Sum request bytes per 5-minute window, grouped by account
  ... | window timestamp -w 5m -k account -a sum -f bytes

  # Sliding window: 1-hour windows every 15 minutes
  ... | window timestamp -w 1h -s 15m -a avg -f latency

  # Session windows: group user activity with 30-minute gap
  ... | window timestamp --session-gap 30m -k user_id

  # Handle late arrivals up to 5 minutes
  ... | window timestamp -w 1h --allowed-lateness 5m

  # Limit memory with max concurrent windows
  ... | window timestamp -w 1h -k account --max-windows 1000

OUTPUT FIELDS:
  window_start  ISO8601 timestamp of window start
  window_end    ISO8601 timestamp of window end
  duration      Window duration in seconds
  key           Grouping key (if -k specified)
  count         Number of events in window
  sum/avg/min/max  Aggregation result (based on -a)
"#,
    arg_required_else_help(true)
)]
struct WindowArgs {
    /// JMESPath expression to extract timestamp from events
    #[arg(required = true, value_name = "JMESPATH")]
    time_field: Vec<String>,

    /// Window size duration (e.g., 30s, 5m, 1h, 1d, 500ms)
    #[arg(short = 'w', long, default_value = DEFAULT_WINDOW_SIZE, value_name = "DURATION")]
    window_size: String,

    /// Slide interval for overlapping windows (creates sliding windows)
    #[arg(short = 's', long, value_name = "DURATION")]
    slide: Option<String>,

    /// JMESPath expression to group events by key
    #[arg(short = 'k', long, value_name = "JMESPATH")]
    key: Option<String>,

    /// Aggregation function: count, sum, avg, min, max
    #[arg(short = 'a', long, default_value = DEFAULT_AGGREGATE, value_name = "TYPE")]
    aggregate: String,

    /// JMESPath to numeric field for aggregation (required for sum/avg/min/max)
    #[arg(short = 'f', long, value_name = "JMESPATH")]
    field: Option<String>,

    /// When to emit window results: on-close, on-update, on-flush
    #[arg(long, default_value = DEFAULT_EMIT_MODE, value_name = "MODE")]
    emit: String,

    /// Gap duration for session windows (mutually exclusive with --slide)
    #[arg(long, value_name = "DURATION")]
    session_gap: Option<String>,

    /// Custom strftime format for parsing timestamps
    #[arg(long, value_name = "FORMAT")]
    time_format: Option<String>,

    /// Accept late events within this duration past the watermark
    #[arg(long, default_value = DEFAULT_ALLOWED_LATENESS, value_name = "DURATION")]
    allowed_lateness: String,

    /// Maximum concurrent open windows (oldest evicted when exceeded)
    #[arg(long, value_name = "COUNT")]
    max_windows: Option<usize>,
}

#[derive(SerialProcessorInit)]
pub struct WindowProcessor<'a> {
    // Configuration
    time_path: jmespath::Expression<'a>,
    key_path: Option<jmespath::Expression<'a>>,
    agg_field_path: Option<jmespath::Expression<'a>>,
    window_size: Duration,
    slide_interval: Option<Duration>,
    session_gap: Option<Duration>,
    aggregation: AggregationType,
    emit_mode: EmitMode,
    time_format: Option<String>,
    allowed_lateness: Duration,
    max_windows: Option<usize>,

    // State
    windows: HashMap<WindowKey, WindowState>,
    sessions: HashMap<String, SessionState>, // For session windows
    watermark: i64,                          // Unix timestamp in milliseconds

    // Statistics
    input_count: u64,
    output_count: u64,
    skipped_invalid_timestamp: u64,
    skipped_late_events: u64,
}

impl SerialProcessor for WindowProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("aggregate events within time windows (tumbling, sliding, session)".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = WindowArgs::try_parse_from(argv)?;

        // Validate mutually exclusive options
        if args.slide.is_some() && args.session_gap.is_some() {
            return Err(anyhow::anyhow!(
                "--slide and --session-gap are mutually exclusive options"
            ));
        }

        // Parse aggregation type
        let aggregation = AggregationType::from_str(&args.aggregate)?;

        // Validate that field is required for sum/avg/min/max
        if matches!(
            aggregation,
            AggregationType::Sum
                | AggregationType::Avg
                | AggregationType::Min
                | AggregationType::Max
        ) && args.field.is_none()
        {
            return Err(anyhow::anyhow!(
                "--field is required when aggregation type is sum, avg, min, or max"
            ));
        }

        // Parse emit mode
        let emit_mode = EmitMode::from_str(&args.emit)?;

        // Parse durations
        let window_size = parse_duration(&args.window_size)?;
        let slide_interval = match &args.slide {
            Some(s) => Some(parse_duration(s)?),
            None => None,
        };
        let session_gap = match &args.session_gap {
            Some(s) => Some(parse_duration(s)?),
            None => None,
        };
        let allowed_lateness = parse_duration(&args.allowed_lateness)?;

        // Compile JMESPath expressions
        let time_path = jmespath::compile(&args.time_field.join(" "))?;

        let key_path = match &args.key {
            Some(k) => Some(jmespath::compile(k)?),
            None => None,
        };

        let agg_field_path = match &args.field {
            Some(f) => Some(jmespath::compile(f)?),
            None => None,
        };

        Ok(WindowProcessor {
            time_path,
            key_path,
            agg_field_path,
            window_size,
            slide_interval,
            session_gap,
            aggregation,
            emit_mode,
            time_format: args.time_format,
            allowed_lateness,
            max_windows: args.max_windows,
            windows: HashMap::new(),
            sessions: HashMap::new(),
            watermark: 0,
            input_count: 0,
            output_count: 0,
            skipped_invalid_timestamp: 0,
            skipped_late_events: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        // Extract and parse timestamp
        let timestamp = match self.parse_event_timestamp(&input) {
            Some(ts) => ts,
            None => {
                self.skipped_invalid_timestamp += 1;
                return Ok(vec![]);
            }
        };

        // Check for late arrival
        if self.is_late(timestamp) {
            self.skipped_late_events += 1;
            return Ok(vec![]);
        }

        // Extract key if configured
        let key = self.extract_key(&input)?;

        // Extract aggregation field value if configured
        let agg_value = self.extract_agg_value(&input)?;

        // Handle session windows separately
        if self.session_gap.is_some() {
            return self.process_session_window(timestamp, key, agg_value);
        }

        // Calculate window(s) for this event
        let window_starts = self.calculate_windows(timestamp);

        let mut output = Vec::new();

        // Collect window states to emit for on-update mode
        let mut windows_to_emit: Vec<WindowState> = Vec::new();

        for window_start in window_starts {
            let window_end = window_start + self.window_size.as_millis() as i64;
            let window_key = WindowKey { window_start, key: key.clone() };

            // Check max windows limit before adding new window
            if !self.windows.contains_key(&window_key) {
                if let Some(max) = self.max_windows {
                    if self.windows.len() >= max {
                        // Evict oldest window
                        if let Some(evicted) = self.evict_oldest_window() {
                            output.push(evicted);
                        }
                    }
                }
            }

            // Get or create window state
            let window_state = self
                .windows
                .entry(window_key)
                .or_insert_with(|| WindowState::new(window_start, window_end, key.clone()));

            // Update aggregation
            window_state.add_value(agg_value);

            // Handle on-update emit mode - clone state for later emission
            if self.emit_mode == EmitMode::OnUpdate {
                windows_to_emit.push(window_state.clone());
            }
        }

        // Emit windows for on-update mode (after releasing mutable borrow)
        for window_state in windows_to_emit {
            output.push(self.create_output_event(&window_state)?);
        }

        // Update watermark
        self.update_watermark(timestamp);

        // Close expired windows (for on-close mode)
        if self.emit_mode == EmitMode::OnClose {
            let mut expired = self.close_expired_windows()?;
            output.append(&mut expired);
        }

        self.output_count += output.len() as u64;
        Ok(output)
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut output = Vec::new();

        // Flush session windows
        if self.session_gap.is_some() {
            let sessions: Vec<_> = self.sessions.drain().collect();
            for (key, session) in sessions {
                if let Ok(event) = self.create_session_output_event(&key, &session) {
                    output.push(event);
                }
            }
        } else {
            // Flush all open windows
            let windows: Vec<_> = self.windows.drain().collect();
            for (_, window_state) in windows {
                if let Ok(event) = self.create_output_event(&window_state) {
                    output.push(event);
                }
            }
        }

        self.output_count += output.len() as u64;
        output
    }

    fn stats(&self) -> Option<String> {
        Some(format!(
            "input:{}, output:{}, skipped_invalid_timestamp:{}, skipped_late_events:{}",
            self.input_count,
            self.output_count,
            self.skipped_invalid_timestamp,
            self.skipped_late_events
        ))
    }
}

impl WindowProcessor<'_> {
    /// Parse timestamp from event using configured path and format
    fn parse_event_timestamp(&self, event: &Event) -> Option<i64> {
        let timestamp_value = self.time_path.search(event).ok()?;

        if timestamp_value.is_null() {
            return None;
        }

        // Try to get as string first
        if let Some(timestamp_str) = timestamp_value.as_string() {
            return self.parse_timestamp_string(timestamp_str);
        }

        // Try to get as number (Unix epoch)
        if let Some(num) = timestamp_value.as_number() {
            let num_i64 = num as i64;
            // Heuristic: if > 1e12, assume milliseconds, otherwise seconds
            if num_i64 > 1_000_000_000_000 {
                return Some(num_i64);
            } else {
                return Some(num_i64 * 1000);
            }
        }

        None
    }

    /// Parse a timestamp string into milliseconds
    fn parse_timestamp_string(&self, value: &str) -> Option<i64> {
        // Try custom format first if specified
        if let Some(fmt) = &self.time_format {
            if let Ok(dt) = NaiveDateTime::parse_from_str(value, fmt) {
                return Some(dt.and_utc().timestamp_millis());
            }
        }

        // Try ISO8601/RFC3339
        if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
            return Some(dt.timestamp_millis());
        }

        // Try Unix epoch (seconds) - numeric string
        if let Ok(secs) = value.parse::<i64>() {
            // Heuristic: if > 1e12, assume milliseconds
            if secs > 1_000_000_000_000 {
                return Some(secs);
            } else {
                return Some(secs * 1000);
            }
        }

        // Try Unix epoch (float seconds)
        if let Ok(secs) = value.parse::<f64>() {
            return Some((secs * 1000.0) as i64);
        }

        None
    }

    /// Extract key from event
    fn extract_key(&self, event: &Event) -> Result<Option<String>, anyhow::Error> {
        match &self.key_path {
            Some(key_path) => {
                let key = key_path.search(event)?;
                if key.is_null() {
                    Ok(None)
                } else if let Some(s) = key.as_string() {
                    Ok(Some(s.to_string()))
                } else {
                    Ok(Some(key.to_string()))
                }
            }
            None => Ok(None),
        }
    }

    /// Extract aggregation field value from event
    fn extract_agg_value(&self, event: &Event) -> Result<Option<f64>, anyhow::Error> {
        match &self.agg_field_path {
            Some(field_path) => {
                let value = field_path.search(event)?;
                if value.is_null() {
                    Ok(None)
                } else {
                    Ok(value.as_number())
                }
            }
            None => Ok(None),
        }
    }

    /// Calculate window start(s) for a given timestamp
    fn calculate_windows(&self, timestamp: i64) -> Vec<i64> {
        let window_size_ms = self.window_size.as_millis() as i64;

        match &self.slide_interval {
            Some(slide) => {
                // Sliding windows
                let slide_ms = slide.as_millis() as i64;
                let mut windows = Vec::new();

                // Find the earliest window that could contain this timestamp
                let earliest_start = ((timestamp - window_size_ms) / slide_ms + 1) * slide_ms;
                let mut start = earliest_start.max(0);

                while start <= timestamp {
                    if start + window_size_ms > timestamp {
                        windows.push(start);
                    }
                    start += slide_ms;
                }
                windows
            }
            None => {
                // Tumbling windows - aligned to window size
                let window_start = (timestamp / window_size_ms) * window_size_ms;
                vec![window_start]
            }
        }
    }

    /// Update watermark with new timestamp
    fn update_watermark(&mut self, timestamp: i64) {
        self.watermark = self.watermark.max(timestamp);
    }

    /// Check if an event is late
    fn is_late(&self, timestamp: i64) -> bool {
        let allowed_lateness_ms = self.allowed_lateness.as_millis() as i64;
        timestamp < self.watermark - allowed_lateness_ms
    }

    /// Close expired windows based on watermark
    fn close_expired_windows(&mut self) -> Result<Vec<Event>, anyhow::Error> {
        let cutoff = self.watermark - self.allowed_lateness.as_millis() as i64;

        let expired_keys: Vec<WindowKey> = self
            .windows
            .iter()
            .filter(|(_, state)| state.window_end <= cutoff)
            .map(|(key, _)| key.clone())
            .collect();

        let mut output = Vec::new();
        for key in expired_keys {
            if let Some(state) = self.windows.remove(&key) {
                output.push(self.create_output_event(&state)?);
            }
        }
        Ok(output)
    }

    /// Evict the oldest window when max_windows limit is reached
    fn evict_oldest_window(&mut self) -> Option<Event> {
        // Find the window with the smallest window_start
        let oldest_key = self
            .windows
            .iter()
            .min_by_key(|(_, state)| state.window_start)
            .map(|(key, _)| key.clone());

        if let Some(key) = oldest_key {
            if let Some(state) = self.windows.remove(&key) {
                return self.create_output_event(&state).ok();
            }
        }
        None
    }

    /// Process event for session windows
    fn process_session_window(
        &mut self,
        timestamp: i64,
        key: Option<String>,
        agg_value: Option<f64>,
    ) -> Result<Vec<Event>, anyhow::Error> {
        let session_gap_ms = self.session_gap.unwrap().as_millis() as i64;
        let key_str = key.clone().unwrap_or_else(|| "total".to_string());

        let mut output = Vec::new();

        // Check if we have an existing session for this key
        if let Some(session) = self.sessions.get(&key_str) {
            let gap = timestamp - session.last_event_time;

            if gap >= session_gap_ms {
                // Gap exceeded - close current session and start new one
                let closed_session = self.sessions.remove(&key_str).unwrap();
                output.push(self.create_session_output_event(&key_str, &closed_session)?);

                // Start new session
                let mut new_session = SessionState::new(timestamp);
                new_session.add_value(timestamp, agg_value);
                self.sessions.insert(key_str.clone(), new_session);
            } else {
                // Add to existing session
                let session = self.sessions.get_mut(&key_str).unwrap();
                session.add_value(timestamp, agg_value);
            }
        } else {
            // No existing session - start new one
            let mut new_session = SessionState::new(timestamp);
            new_session.add_value(timestamp, agg_value);
            self.sessions.insert(key_str.clone(), new_session);
        }

        // Update watermark
        self.update_watermark(timestamp);

        // Handle on-update emit mode for sessions
        if self.emit_mode == EmitMode::OnUpdate && output.is_empty() {
            // Emit current session state
            if let Some(session) = self.sessions.get(&key_str) {
                output.push(self.create_session_output_event(&key_str, session)?);
            }
        }

        self.output_count += output.len() as u64;
        Ok(output)
    }

    /// Create output event from window state
    fn create_output_event(&self, state: &WindowState) -> Result<Event, anyhow::Error> {
        let mut output = serde_json::Map::new();

        // Add window boundaries as ISO8601
        let window_start_dt = DateTime::<Utc>::from_timestamp_millis(state.window_start)
            .unwrap_or(DateTime::UNIX_EPOCH);
        let window_end_dt = DateTime::<Utc>::from_timestamp_millis(state.window_end)
            .unwrap_or(DateTime::UNIX_EPOCH);

        output.insert(
            "window_start".to_string(),
            serde_json::Value::String(window_start_dt.to_rfc3339()),
        );
        output.insert(
            "window_end".to_string(),
            serde_json::Value::String(window_end_dt.to_rfc3339()),
        );

        // Add duration in seconds
        let duration_secs = (state.window_end - state.window_start) / 1000;
        output.insert("duration".to_string(), serde_json::to_value(duration_secs)?);

        // Add key if configured
        if let Some(ref key) = state.key {
            output.insert("key".to_string(), serde_json::Value::String(key.clone()));
        }

        // Always add count
        output.insert("count".to_string(), serde_json::to_value(state.count)?);

        // Add aggregation-specific fields
        match self.aggregation {
            AggregationType::Sum => {
                if state.min.is_finite() {
                    output.insert("sum".to_string(), serde_json::to_value(state.sum)?);
                }
            }
            AggregationType::Avg => {
                if let Some(avg) = state.get_avg() {
                    output.insert("sum".to_string(), serde_json::to_value(state.sum)?);
                    output.insert("avg".to_string(), serde_json::to_value(avg)?);
                }
            }
            AggregationType::Min => {
                if state.min.is_finite() {
                    output.insert("min".to_string(), serde_json::to_value(state.min)?);
                }
            }
            AggregationType::Max => {
                if state.max.is_finite() {
                    output.insert("max".to_string(), serde_json::to_value(state.max)?);
                }
            }
            AggregationType::Count => {
                // Count is already added above
            }
        }

        let output_value: serde_json::Value = output.into();
        Ok(Rc::new(RefCell::new(output_value)))
    }

    /// Create output event from session state
    fn create_session_output_event(
        &self,
        key: &str,
        session: &SessionState,
    ) -> Result<Event, anyhow::Error> {
        let mut output = serde_json::Map::new();

        // Add session boundaries as ISO8601
        let start_dt = DateTime::<Utc>::from_timestamp_millis(session.start_time)
            .unwrap_or(DateTime::UNIX_EPOCH);
        let end_dt = DateTime::<Utc>::from_timestamp_millis(session.last_event_time)
            .unwrap_or(DateTime::UNIX_EPOCH);

        output.insert("window_start".to_string(), serde_json::Value::String(start_dt.to_rfc3339()));
        output.insert("window_end".to_string(), serde_json::Value::String(end_dt.to_rfc3339()));

        // Add duration in seconds
        let duration_secs = (session.last_event_time - session.start_time) / 1000;
        output.insert("duration".to_string(), serde_json::to_value(duration_secs)?);

        // Add key
        if key != "total" {
            output.insert("key".to_string(), serde_json::Value::String(key.to_string()));
        }

        // Always add count
        output.insert("count".to_string(), serde_json::to_value(session.count)?);

        // Add aggregation-specific fields
        match self.aggregation {
            AggregationType::Sum => {
                if session.min.is_finite() {
                    output.insert("sum".to_string(), serde_json::to_value(session.sum)?);
                }
            }
            AggregationType::Avg => {
                if let Some(avg) = session.get_avg() {
                    output.insert("sum".to_string(), serde_json::to_value(session.sum)?);
                    output.insert("avg".to_string(), serde_json::to_value(avg)?);
                }
            }
            AggregationType::Min => {
                if session.min.is_finite() {
                    output.insert("min".to_string(), serde_json::to_value(session.min)?);
                }
            }
            AggregationType::Max => {
                if session.max.is_finite() {
                    output.insert("max".to_string(), serde_json::to_value(session.max)?);
                }
            }
            AggregationType::Count => {
                // Count is already added above
            }
        }

        let output_value: serde_json::Value = output.into();
        Ok(Rc::new(RefCell::new(output_value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_seconds() {
        let duration = parse_duration("30s").unwrap();
        assert_eq!(duration, Duration::from_secs(30));
    }

    #[test]
    fn test_parse_duration_minutes() {
        let duration = parse_duration("5m").unwrap();
        assert_eq!(duration, Duration::from_secs(300));
    }

    #[test]
    fn test_parse_duration_hours() {
        let duration = parse_duration("1h").unwrap();
        assert_eq!(duration, Duration::from_secs(3600));
    }

    #[test]
    fn test_parse_duration_days() {
        let duration = parse_duration("1d").unwrap();
        assert_eq!(duration, Duration::from_secs(86400));
    }

    #[test]
    fn test_parse_duration_milliseconds() {
        let duration = parse_duration("500ms").unwrap();
        assert_eq!(duration, Duration::from_millis(500));
    }

    #[test]
    fn test_parse_duration_invalid() {
        let result = parse_duration("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_duration_with_whitespace() {
        let duration = parse_duration("  30s  ").unwrap();
        assert_eq!(duration, Duration::from_secs(30));
    }

    #[test]
    fn test_aggregation_type_from_str() {
        assert_eq!(AggregationType::from_str("count").unwrap(), AggregationType::Count);
        assert_eq!(AggregationType::from_str("sum").unwrap(), AggregationType::Sum);
        assert_eq!(AggregationType::from_str("avg").unwrap(), AggregationType::Avg);
        assert_eq!(AggregationType::from_str("min").unwrap(), AggregationType::Min);
        assert_eq!(AggregationType::from_str("max").unwrap(), AggregationType::Max);
        assert_eq!(AggregationType::from_str("COUNT").unwrap(), AggregationType::Count);
        assert!(AggregationType::from_str("invalid").is_err());
    }

    #[test]
    fn test_emit_mode_from_str() {
        assert_eq!(EmitMode::from_str("on-close").unwrap(), EmitMode::OnClose);
        assert_eq!(EmitMode::from_str("on-update").unwrap(), EmitMode::OnUpdate);
        assert_eq!(EmitMode::from_str("on-flush").unwrap(), EmitMode::OnFlush);
        assert_eq!(EmitMode::from_str("ON-CLOSE").unwrap(), EmitMode::OnClose);
        assert!(EmitMode::from_str("invalid").is_err());
    }

    #[test]
    fn test_window_state_add_value() {
        let mut state = WindowState::new(0, 1000, None);
        state.add_value(Some(10.0));
        state.add_value(Some(20.0));
        state.add_value(Some(5.0));

        assert_eq!(state.count, 3);
        assert_eq!(state.sum, 35.0);
        assert_eq!(state.min, 5.0);
        assert_eq!(state.max, 20.0);
    }

    #[test]
    fn test_window_state_get_avg() {
        let mut state = WindowState::new(0, 1000, None);
        state.add_value(Some(10.0));
        state.add_value(Some(20.0));

        assert_eq!(state.get_avg(), Some(15.0));
    }

    #[test]
    fn test_window_state_no_values() {
        let state = WindowState::new(0, 1000, None);
        assert_eq!(state.count, 0);
        assert_eq!(state.get_avg(), None);
    }

    #[test]
    fn test_session_state_add_value() {
        let mut state = SessionState::new(1000);
        state.add_value(1000, Some(10.0));
        state.add_value(2000, Some(20.0));

        assert_eq!(state.count, 2);
        assert_eq!(state.sum, 30.0);
        assert_eq!(state.min, 10.0);
        assert_eq!(state.max, 20.0);
        assert_eq!(state.last_event_time, 2000);
    }

    #[test]
    fn test_window_processor_initialization() {
        let args = vec!["window".to_string(), "timestamp".to_string()];
        let processor = WindowProcessor::new(&args).unwrap();

        assert_eq!(processor.window_size, Duration::from_secs(3600)); // 1h default
        assert_eq!(processor.aggregation, AggregationType::Count);
        assert_eq!(processor.emit_mode, EmitMode::OnClose);
        assert!(processor.slide_interval.is_none());
        assert!(processor.session_gap.is_none());
    }

    #[test]
    fn test_window_processor_with_options() {
        let args = vec![
            "window".to_string(),
            "timestamp".to_string(),
            "-w".to_string(),
            "5m".to_string(),
            "-k".to_string(),
            "account".to_string(),
            "-a".to_string(),
            "sum".to_string(),
            "-f".to_string(),
            "value".to_string(),
        ];
        let processor = WindowProcessor::new(&args).unwrap();

        assert_eq!(processor.window_size, Duration::from_secs(300));
        assert_eq!(processor.aggregation, AggregationType::Sum);
        assert!(processor.key_path.is_some());
        assert!(processor.agg_field_path.is_some());
    }

    #[test]
    fn test_window_processor_mutually_exclusive_error() {
        let args = vec![
            "window".to_string(),
            "timestamp".to_string(),
            "--slide".to_string(),
            "1m".to_string(),
            "--session-gap".to_string(),
            "5m".to_string(),
        ];
        let result = WindowProcessor::new(&args);
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("mutually exclusive"));
    }

    #[test]
    fn test_window_processor_field_required_for_sum() {
        let args = vec![
            "window".to_string(),
            "timestamp".to_string(),
            "-a".to_string(),
            "sum".to_string(),
        ];
        let result = WindowProcessor::new(&args);
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("--field is required"));
    }

    #[test]
    fn test_calculate_tumbling_windows() {
        let args =
            vec!["window".to_string(), "timestamp".to_string(), "-w".to_string(), "1h".to_string()];
        let processor = WindowProcessor::new(&args).unwrap();

        // Timestamp at 1:30 should be in window starting at 1:00
        let timestamp = 3600000 + 1800000; // 1.5 hours in ms
        let windows = processor.calculate_windows(timestamp);

        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0], 3600000); // 1 hour in ms
    }

    #[test]
    fn test_calculate_sliding_windows() {
        let args = vec![
            "window".to_string(),
            "timestamp".to_string(),
            "-w".to_string(),
            "1h".to_string(),
            "-s".to_string(),
            "30m".to_string(),
        ];
        let processor = WindowProcessor::new(&args).unwrap();

        // Timestamp at 1:15 should be in windows starting at 0:30 and 1:00
        let timestamp = 3600000 + 900000; // 1.25 hours in ms
        let windows = processor.calculate_windows(timestamp);

        assert_eq!(windows.len(), 2);
    }

    #[test]
    fn test_window_state_add_value_with_none() {
        let mut state = WindowState::new(0, 1000, None);
        state.add_value(Some(10.0));
        state.add_value(None); // non-numeric / missing value
        state.add_value(Some(20.0));

        assert_eq!(state.count, 3); // all events counted
        assert_eq!(state.value_count, 2); // only events with values
        assert_eq!(state.sum, 30.0);
        assert_eq!(state.min, 10.0);
        assert_eq!(state.max, 20.0);
        // avg should be 30/2 = 15, not 30/3 = 10
        assert_eq!(state.get_avg(), Some(15.0));
    }

    #[test]
    fn test_window_state_all_none_values() {
        let mut state = WindowState::new(0, 1000, None);
        state.add_value(None);
        state.add_value(None);

        assert_eq!(state.count, 2);
        assert_eq!(state.value_count, 0);
        assert_eq!(state.get_avg(), None);
        assert!(!state.min.is_finite()); // still INFINITY
        assert!(!state.max.is_finite()); // still NEG_INFINITY
    }

    #[test]
    fn test_session_state_add_value_with_none() {
        let mut state = SessionState::new(1000);
        state.add_value(1000, Some(10.0));
        state.add_value(2000, None);
        state.add_value(3000, Some(20.0));

        assert_eq!(state.count, 3);
        assert_eq!(state.value_count, 2);
        assert_eq!(state.sum, 30.0);
        assert_eq!(state.min, 10.0);
        assert_eq!(state.max, 20.0);
        assert_eq!(state.last_event_time, 3000);
        assert_eq!(state.get_avg(), Some(15.0));
    }
}
