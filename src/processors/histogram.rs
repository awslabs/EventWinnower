use crate::processors::processor::*;
use clap::Parser;
use eventwinnower_macros::SerialProcessorInit;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Parser)]
/// Bucket numeric values into ranges and count occurrences
#[command(
    version,
    long_about = None,
    arg_required_else_help(true),
    after_help = r#"BUCKET SPECIFICATION FORMATS:

  Fixed-width buckets (min:max:width):
    -b "0:1:0.1"      → [0.0-0.1), [0.1-0.2), ..., [0.9-1.0]
    -b "0:100:10"      → [0.0-10.0), [10.0-20.0), ..., [90.0-100.0]

  Custom edge buckets (comma-separated values):
    -b "0,0.5,0.7,0.9,1.0"  → [0.0-0.5), [0.5-0.7), [0.7-0.9), [0.9-1.0]

  Quantile-based buckets (quantile:N):
    -b "quantile:10"  → Deciles (10 equal-frequency buckets)
    -b "quantile:4"   → Quartiles (4 equal-frequency buckets)

  Logarithmic scale buckets (log:min:max:base):
    -b "log:1:1000:10"  → [1.0-10.0), [10.0-100.0), [100.0-1000.0]

EXAMPLES:
  histogram score -b "0:1:0.1"
  histogram score -k account -b "0,0.5,0.7,0.9,1.0"
  histogram latency -b "log:1:10000:10" --normalize
  histogram value -k category -b "quantile:10" --cumulative
"#
)]
struct HistogramArgs {
    /// Numeric field to histogram (JMESPath expression)
    #[arg(required(true))]
    field: Vec<String>,

    /// Group histograms by key (JMESPath expression)
    #[arg(short, long)]
    key: Option<String>,

    /// Bucket specification (see formats below)
    #[arg(short, long, default_value = "0:100:10")]
    buckets: String,

    /// Label for histogram output field
    #[arg(short, long, default_value = "histogram")]
    label: String,

    /// Output cumulative counts
    #[arg(long, default_value_t = false)]
    cumulative: bool,

    /// Output percentages instead of counts
    #[arg(long, default_value_t = false)]
    normalize: bool,
}

// ─── Bucket Specification ────────────────────────────────────────────────────

#[derive(Clone, Debug)]
enum BucketSpec {
    FixedWidth { min: f64, max: f64, width: f64 },
    CustomEdges { edges: Vec<f64> },
    Quantile { divisions: usize },
    LogScale { min: f64, max: f64, base: f64 },
}

impl BucketSpec {
    fn parse(spec: &str) -> Result<Self, anyhow::Error> {
        // Try quantile: "quantile:N"
        if let Some(stripped) = spec.strip_prefix("quantile:") {
            let n: usize = stripped.parse().map_err(|_| {
                anyhow::anyhow!(
                    "Invalid quantile specification: expected 'quantile:N' where N is an integer"
                )
            })?;
            if n < 2 {
                return Err(anyhow::anyhow!("Quantile divisions must be >= 2, got {}", n));
            }
            return Ok(BucketSpec::Quantile { divisions: n });
        }

        // Try log scale: "log:min:max:base"
        if let Some(stripped) = spec.strip_prefix("log:") {
            let parts: Vec<&str> = stripped.split(':').collect();
            if parts.len() != 3 {
                return Err(anyhow::anyhow!(
                    "Invalid log scale specification: expected 'log:min:max:base'"
                ));
            }
            let min: f64 = parts[0]
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid log scale min value: '{}'", parts[0]))?;
            let max: f64 = parts[1]
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid log scale max value: '{}'", parts[1]))?;
            let base: f64 = parts[2]
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid log scale base value: '{}'", parts[2]))?;
            if min <= 0.0 {
                return Err(anyhow::anyhow!("Log scale min must be > 0, got {}", min));
            }
            if base <= 1.0 {
                return Err(anyhow::anyhow!("Log scale base must be > 1, got {}", base));
            }
            if min >= max {
                return Err(anyhow::anyhow!(
                    "Log scale min ({}) must be less than max ({})",
                    min,
                    max
                ));
            }
            return Ok(BucketSpec::LogScale { min, max, base });
        }

        // Try fixed-width: "min:max:width" (exactly 3 colon-separated numbers)
        let colon_parts: Vec<&str> = spec.split(':').collect();
        if colon_parts.len() == 3 {
            if let (Ok(min), Ok(max), Ok(width)) = (
                colon_parts[0].parse::<f64>(),
                colon_parts[1].parse::<f64>(),
                colon_parts[2].parse::<f64>(),
            ) {
                if min >= max {
                    return Err(anyhow::anyhow!(
                        "Fixed-width min ({}) must be less than max ({})",
                        min,
                        max
                    ));
                }
                if width <= 0.0 {
                    return Err(anyhow::anyhow!(
                        "Fixed-width bucket width must be > 0, got {}",
                        width
                    ));
                }
                return Ok(BucketSpec::FixedWidth { min, max, width });
            }
        }

        // Try custom edges: "a,b,c,..."
        if spec.contains(',') {
            let edges: Result<Vec<f64>, _> =
                spec.split(',').map(|s| s.trim().parse::<f64>()).collect();
            match edges {
                Ok(edges) => {
                    if edges.len() < 2 {
                        return Err(anyhow::anyhow!(
                            "Custom edges require at least 2 values, got {}",
                            edges.len()
                        ));
                    }
                    for i in 1..edges.len() {
                        if edges[i] <= edges[i - 1] {
                            return Err(anyhow::anyhow!(
                                "Custom edges must be in ascending order: {} is not > {}",
                                edges[i],
                                edges[i - 1]
                            ));
                        }
                    }
                    return Ok(BucketSpec::CustomEdges { edges });
                }
                Err(_) => {
                    return Err(anyhow::anyhow!(
                        "Invalid custom edge specification: could not parse values in '{}'",
                        spec
                    ));
                }
            }
        }

        Err(anyhow::anyhow!("Invalid bucket specification: '{}'", spec))
    }

    fn get_boundaries(&self) -> Vec<f64> {
        match self {
            BucketSpec::FixedWidth { min, max, width } => {
                let mut boundaries = vec![*min];
                let mut current = *min + *width;
                while current < *max {
                    boundaries.push(current);
                    current += *width;
                }
                // Ensure max is always the last boundary
                if (*boundaries.last().unwrap() - *max).abs() > f64::EPSILON {
                    boundaries.push(*max);
                }
                boundaries
            }
            BucketSpec::CustomEdges { edges } => edges.clone(),
            BucketSpec::Quantile { .. } => {
                // Boundaries are computed at flush time from collected values
                vec![]
            }
            BucketSpec::LogScale { min, max, base } => {
                let mut boundaries = vec![*min];
                let mut current = *min;
                loop {
                    current *= *base;
                    if current > *max {
                        break;
                    }
                    boundaries.push(current);
                }
                if (*boundaries.last().unwrap() - *max).abs() > f64::EPSILON {
                    boundaries.push(*max);
                }
                boundaries
            }
        }
    }
}

// ─── Buckets ─────────────────────────────────────────────────────────────────

struct Buckets {
    boundaries: Vec<f64>,
    names: Vec<String>,
    counts: Vec<u64>,
}

impl Buckets {
    fn new(spec: &BucketSpec) -> Self {
        let boundaries = spec.get_boundaries();
        let names = Self::generate_names(&boundaries);
        let counts = vec![0u64; names.len()];
        Buckets { boundaries, names, counts }
    }

    fn from_boundaries(boundaries: Vec<f64>) -> Self {
        let names = Self::generate_names(&boundaries);
        let counts = vec![0u64; names.len()];
        Buckets { boundaries, names, counts }
    }

    fn generate_names(boundaries: &[f64]) -> Vec<String> {
        if boundaries.is_empty() {
            return vec![];
        }
        let mut names = Vec::new();
        // Underflow bucket
        names.push(format!("<{}", boundaries[0]));
        // Regular buckets
        for i in 0..boundaries.len() - 1 {
            if i == boundaries.len() - 2 {
                // Final bucket: closed interval
                names.push(format!("[{}-{}]", boundaries[i], boundaries[i + 1]));
            } else {
                // Left-closed, right-open
                names.push(format!("[{}-{})", boundaries[i], boundaries[i + 1]));
            }
        }
        // Overflow bucket
        names.push(format!(">{}", boundaries[boundaries.len() - 1]));
        names
    }

    fn find_bucket(&self, value: f64) -> usize {
        if self.boundaries.is_empty() {
            return 0;
        }
        // Underflow
        if value < self.boundaries[0] {
            return 0; // underflow bucket
        }
        let last = self.boundaries.len() - 1;
        // Overflow
        if value > self.boundaries[last] {
            return self.names.len() - 1; // overflow bucket
        }
        // Binary search among boundaries
        // Bucket index offset by 1 because index 0 is underflow
        match self.boundaries.binary_search_by(|b| b.partial_cmp(&value).unwrap()) {
            Ok(idx) => {
                // Exact match on a boundary
                if idx == last {
                    // Value equals max boundary → final regular bucket
                    idx // bucket index = last boundary index (offset by underflow)
                } else {
                    // Value equals an interior boundary → bucket starting at this boundary
                    idx + 1
                }
            }
            Err(idx) => {
                // Value falls between boundaries[idx-1] and boundaries[idx]
                idx // offset by underflow: bucket at index idx
            }
        }
    }

    fn increment(&mut self, bucket_idx: usize) {
        if bucket_idx < self.counts.len() {
            self.counts[bucket_idx] += 1;
        }
    }

    fn get_counts(&self) -> &[u64] {
        &self.counts
    }

    fn get_cumulative_counts(&self) -> Vec<u64> {
        let mut cumulative = Vec::with_capacity(self.counts.len());
        let mut running = 0u64;
        for &count in &self.counts {
            running += count;
            cumulative.push(running);
        }
        cumulative
    }

    fn get_normalized_counts(&self, total: u64) -> Vec<f64> {
        if total == 0 {
            return vec![0.0; self.counts.len()];
        }
        self.counts.iter().map(|&c| c as f64 / total as f64).collect()
    }
}

// ─── Histogram State (per key) ───────────────────────────────────────────────

struct HistogramState {
    buckets: Buckets,
    values: Vec<f64>,
    total: u64,
    min: f64,
    max: f64,
    sum: f64,
}

impl HistogramState {
    fn new(spec: &BucketSpec) -> Self {
        HistogramState {
            buckets: Buckets::new(spec),
            values: Vec::new(),
            total: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
        }
    }

    fn add_value(&mut self, value: f64) {
        let bucket_idx = self.buckets.find_bucket(value);
        self.buckets.increment(bucket_idx);
        self.values.push(value);
        self.total += 1;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        self.sum += value;
    }

    fn compute_mean(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        self.sum / self.total as f64
    }

    fn compute_median(&mut self) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        self.values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let len = self.values.len();
        if len.is_multiple_of(2) {
            (self.values[len / 2 - 1] + self.values[len / 2]) / 2.0
        } else {
            self.values[len / 2]
        }
    }

    fn compute_quantile_boundaries(&mut self, divisions: usize) {
        if self.values.is_empty() {
            return;
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let mut boundaries = vec![sorted[0]];
        for i in 1..divisions {
            let idx = (i * sorted.len()) / divisions;
            let idx = idx.min(sorted.len() - 1);
            boundaries.push(sorted[idx]);
        }
        boundaries.push(sorted[sorted.len() - 1]);

        // Deduplicate boundaries while preserving order
        boundaries.dedup();

        self.buckets = Buckets::from_boundaries(boundaries);
        // Re-bucket all values
        for &value in &self.values {
            let bucket_idx = self.buckets.find_bucket(value);
            self.buckets.increment(bucket_idx);
        }
    }
}

// ─── Histogram Processor ─────────────────────────────────────────────────────

#[derive(SerialProcessorInit)]
pub struct HistogramProcessor<'a> {
    field_path: jmespath::Expression<'a>,
    key_path: Option<jmespath::Expression<'a>>,
    bucket_spec: BucketSpec,
    label: String,
    cumulative: bool,
    normalize: bool,
    state: HashMap<String, HistogramState>,
    input_count: u64,
    output_count: u64,
}

impl SerialProcessor for HistogramProcessor<'_> {
    fn get_simple_description() -> Option<String>
    where
        Self: Sized,
    {
        Some("bucket numeric values into ranges and count occurrences".to_string())
    }

    fn new(argv: &[String]) -> Result<Self, anyhow::Error> {
        let args = HistogramArgs::try_parse_from(argv)?;
        let field_path = jmespath::compile(&args.field.join(" "))?;

        let key_path = match args.key {
            Some(key) => Some(jmespath::compile(&key)?),
            _ => None,
        };

        let bucket_spec = BucketSpec::parse(&args.buckets)?;

        Ok(HistogramProcessor {
            field_path,
            key_path,
            bucket_spec,
            label: args.label,
            cumulative: args.cumulative,
            normalize: args.normalize,
            state: HashMap::new(),
            input_count: 0,
            output_count: 0,
        })
    }

    fn process(&mut self, input: Event) -> Result<Vec<Event>, anyhow::Error> {
        self.input_count += 1;

        let value_match = self.field_path.search(&input)?;
        let value = match value_match.as_number() {
            Some(v) => v,
            None => return Ok(vec![]),
        };

        let key = match &self.key_path {
            Some(key_path) => {
                let key_result = key_path.search(&input)?;
                if key_result.is_null() {
                    return Ok(vec![]);
                }
                if let Some(s) = key_result.as_string() {
                    s.to_string()
                } else {
                    key_result.to_string()
                }
            }
            None => "total".to_string(),
        };

        let bucket_spec = &self.bucket_spec;
        let state = self.state.entry(key).or_insert_with(|| HistogramState::new(bucket_spec));
        state.add_value(value);

        Ok(vec![])
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events: Vec<Event> = Vec::new();

        for (key, state) in self.state.iter_mut() {
            // For quantile buckets, compute boundaries now
            if let BucketSpec::Quantile { divisions } = &self.bucket_spec {
                state.compute_quantile_boundaries(*divisions);
            }

            if let Ok(event) = Self::create_event(
                key,
                state,
                &self.label,
                self.cumulative,
                self.normalize,
                &self.key_path,
            ) {
                events.push(event);
                self.output_count += 1;
            }
        }

        self.state.clear();
        events
    }

    fn stats(&self) -> Option<String> {
        Some(format!("input: {}, output: {}", self.input_count, self.output_count))
    }
}

impl HistogramProcessor<'_> {
    fn create_event(
        key: &str,
        state: &mut HistogramState,
        label: &str,
        cumulative: bool,
        normalize: bool,
        key_path: &Option<jmespath::Expression>,
    ) -> Result<Event, anyhow::Error> {
        let mut output = serde_json::Map::new();

        // Add key if key_path is configured
        if key_path.is_some() {
            output.insert("key".to_string(), serde_json::to_value(key)?);
        }

        // Build histogram object
        let mut histogram = serde_json::Map::new();
        let names = &state.buckets.names;

        if cumulative && normalize {
            let cumulative_counts = state.buckets.get_cumulative_counts();
            let total = state.total;
            for (i, name) in names.iter().enumerate() {
                let val = if total == 0 { 0.0 } else { cumulative_counts[i] as f64 / total as f64 };
                histogram.insert(name.clone(), serde_json::to_value(val)?);
            }
        } else if cumulative {
            let cumulative_counts = state.buckets.get_cumulative_counts();
            for (i, name) in names.iter().enumerate() {
                histogram.insert(name.clone(), serde_json::to_value(cumulative_counts[i])?);
            }
        } else if normalize {
            let normalized = state.buckets.get_normalized_counts(state.total);
            for (i, name) in names.iter().enumerate() {
                histogram.insert(name.clone(), serde_json::to_value(normalized[i])?);
            }
        } else {
            let counts = state.buckets.get_counts();
            for (i, name) in names.iter().enumerate() {
                histogram.insert(name.clone(), serde_json::to_value(counts[i])?);
            }
        }

        output.insert(label.to_string(), serde_json::Value::Object(histogram));
        output.insert("total".to_string(), serde_json::to_value(state.total)?);
        output.insert("min".to_string(), serde_json::to_value(state.min)?);
        output.insert("max".to_string(), serde_json::to_value(state.max)?);
        output.insert("mean".to_string(), serde_json::to_value(state.compute_mean())?);
        output.insert("median".to_string(), serde_json::to_value(state.compute_median())?);

        let output_value: serde_json::Value = output.into();
        Ok(Rc::new(RefCell::new(output_value)))
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── BucketSpec Parsing Tests ─────────────────────────────────────────

    #[test]
    fn test_parse_fixed_width() {
        let spec = BucketSpec::parse("0:100:10").unwrap();
        match spec {
            BucketSpec::FixedWidth { min, max, width } => {
                assert_eq!(min, 0.0);
                assert_eq!(max, 100.0);
                assert_eq!(width, 10.0);
            }
            _ => panic!("Expected FixedWidth"),
        }
    }

    #[test]
    fn test_parse_fixed_width_fractional() {
        let spec = BucketSpec::parse("0:1:0.1").unwrap();
        match spec {
            BucketSpec::FixedWidth { min, max, width } => {
                assert_eq!(min, 0.0);
                assert_eq!(max, 1.0);
                assert!((width - 0.1).abs() < f64::EPSILON);
            }
            _ => panic!("Expected FixedWidth"),
        }
    }

    #[test]
    fn test_parse_custom_edges() {
        let spec = BucketSpec::parse("0,0.5,0.7,0.9,1.0").unwrap();
        match spec {
            BucketSpec::CustomEdges { edges } => {
                assert_eq!(edges, vec![0.0, 0.5, 0.7, 0.9, 1.0]);
            }
            _ => panic!("Expected CustomEdges"),
        }
    }

    #[test]
    fn test_parse_quantile() {
        let spec = BucketSpec::parse("quantile:10").unwrap();
        match spec {
            BucketSpec::Quantile { divisions } => {
                assert_eq!(divisions, 10);
            }
            _ => panic!("Expected Quantile"),
        }
    }

    #[test]
    fn test_parse_quantile_quartiles() {
        let spec = BucketSpec::parse("quantile:4").unwrap();
        match spec {
            BucketSpec::Quantile { divisions } => {
                assert_eq!(divisions, 4);
            }
            _ => panic!("Expected Quantile"),
        }
    }

    #[test]
    fn test_parse_log_scale() {
        let spec = BucketSpec::parse("log:1:1000:10").unwrap();
        match spec {
            BucketSpec::LogScale { min, max, base } => {
                assert_eq!(min, 1.0);
                assert_eq!(max, 1000.0);
                assert_eq!(base, 10.0);
            }
            _ => panic!("Expected LogScale"),
        }
    }

    // ── Invalid Spec Tests ───────────────────────────────────────────────

    #[test]
    fn test_parse_fixed_width_min_ge_max() {
        assert!(BucketSpec::parse("100:0:10").is_err());
    }

    #[test]
    fn test_parse_fixed_width_zero_width() {
        assert!(BucketSpec::parse("0:100:0").is_err());
    }

    #[test]
    fn test_parse_fixed_width_negative_width() {
        assert!(BucketSpec::parse("0:100:-5").is_err());
    }

    #[test]
    fn test_parse_custom_edges_not_ascending() {
        assert!(BucketSpec::parse("0,0.5,0.3,1.0").is_err());
    }

    #[test]
    fn test_parse_custom_edges_single_value() {
        assert!(BucketSpec::parse("0.5").is_err());
    }

    #[test]
    fn test_parse_quantile_less_than_2() {
        assert!(BucketSpec::parse("quantile:1").is_err());
    }

    #[test]
    fn test_parse_log_scale_min_zero() {
        assert!(BucketSpec::parse("log:0:1000:10").is_err());
    }

    #[test]
    fn test_parse_log_scale_base_one() {
        assert!(BucketSpec::parse("log:1:1000:1").is_err());
    }

    #[test]
    fn test_parse_log_scale_min_negative() {
        assert!(BucketSpec::parse("log:-1:1000:10").is_err());
    }

    #[test]
    fn test_parse_invalid_spec() {
        assert!(BucketSpec::parse("garbage").is_err());
    }

    // ── Boundary Generation Tests ────────────────────────────────────────

    #[test]
    fn test_fixed_width_boundaries() {
        let spec = BucketSpec::FixedWidth { min: 0.0, max: 100.0, width: 10.0 };
        let boundaries = spec.get_boundaries();
        assert_eq!(boundaries.len(), 11); // 0, 10, 20, ..., 100
        assert_eq!(boundaries[0], 0.0);
        assert_eq!(boundaries[10], 100.0);
    }

    #[test]
    fn test_log_scale_boundaries() {
        let spec = BucketSpec::LogScale { min: 1.0, max: 1000.0, base: 10.0 };
        let boundaries = spec.get_boundaries();
        assert_eq!(boundaries, vec![1.0, 10.0, 100.0, 1000.0]);
    }

    #[test]
    fn test_custom_edges_boundaries() {
        let spec = BucketSpec::CustomEdges { edges: vec![0.0, 0.5, 0.7, 0.9, 1.0] };
        let boundaries = spec.get_boundaries();
        assert_eq!(boundaries, vec![0.0, 0.5, 0.7, 0.9, 1.0]);
    }
}
