use serde::Serialize;
use serde_json::Map;
use std::collections::BTreeMap;

use soulbase_types::prelude::Envelope;

#[derive(Clone, Debug, Serialize)]
pub struct LogEvent {
    pub ts_ms: i64,
    pub level: LogLevel,
    pub msg: String,
    #[serde(default)]
    pub labels: BTreeMap<&'static str, String>,
    #[serde(default)]
    pub fields: Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Info,
    Warn,
    Error,
    Critical,
}

#[derive(Clone, Debug)]
pub struct MetricSpec {
    pub name: &'static str,
    pub kind: MetricKind,
    pub help: &'static str,
    pub buckets_ms: Option<&'static [u64]>,
    pub stable_labels: &'static [&'static str],
}

#[derive(Clone, Debug)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Clone, Debug, Default)]
pub struct SpanCtx {
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub parent_span_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct EvidenceEnvelope<T: Serialize> {
    pub envelope: Envelope<T>,
}
