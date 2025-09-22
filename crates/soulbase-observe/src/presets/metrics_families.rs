use crate::model::{MetricKind, MetricSpec};

pub static HTTP_REQUESTS_TOTAL: MetricSpec = MetricSpec {
    name: "http_requests_total",
    kind: MetricKind::Counter,
    help: "HTTP requests",
    buckets_ms: None,
    stable_labels: &["tenant", "route_id", "code"],
};

pub static HTTP_LATENCY_MS: MetricSpec = MetricSpec {
    name: "http_latency_ms_bucket",
    kind: MetricKind::Histogram,
    help: "HTTP latency (ms)",
    buckets_ms: Some(&[5, 10, 20, 50, 100, 200, 500, 1000, 2000]),
    stable_labels: &["route_id"],
};
