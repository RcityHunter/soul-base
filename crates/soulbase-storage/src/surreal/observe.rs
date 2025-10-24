use crate::observe;
use std::time::Duration;

pub fn record_backend(op: &'static str, latency: Duration, rows: usize, code: Option<&str>) {
    observe::record(op, None, Some("surreal"), latency, rows, code);
}

#[cfg(test)]
mod tests {
    use super::record_backend;
    use std::time::Duration;

    #[test]
    fn record_backend_is_noop_without_observe() {
        record_backend("surreal.test", Duration::from_millis(1), 0, None);
    }
}
