use crate::observe;
use std::time::Duration;

pub fn record_backend(op: &'static str, latency: Duration, rows: usize, code: Option<&str>) {
    observe::record(op, None, Some("surreal"), latency, rows, code);
}
