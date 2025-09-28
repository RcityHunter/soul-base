#![cfg(feature = "observe-prometheus")]

use soulbase_net::metrics::NetMetrics;
use soulbase_observe::sdk::PrometheusHub;

#[test]
fn prometheus_metrics_observe_requests() {
    let hub = PrometheusHub::new();
    let metrics = NetMetrics::with_prometheus_hub(&hub);

    metrics.record_request();
    metrics.record_retry();
    metrics.record_failure();

    let output = hub.gather().expect("prometheus scrape");
    assert!(output.contains("soulbase_net_request_total"));
    assert!(output.contains("soulbase_net_retry_total"));
    assert!(output.contains("soulbase_net_failure_total"));
}
