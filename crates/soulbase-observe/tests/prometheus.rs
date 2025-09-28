#![cfg(feature = "prometheus")]

use futures::executor::block_on;
use soulbase_observe::model::{LogLevel, MetricKind, MetricSpec};
use soulbase_observe::pipeline::redactor::NoopRedactor;
use soulbase_observe::pipeline::router::BroadcastRouter;
use soulbase_observe::sdk::{LogBuilder, Logger as _, Meter as _, PrometheusHub};
use soulbase_observe::ObserveCtx;

static TEST_COUNTER: MetricSpec = MetricSpec {
    name: "test_counter_total",
    kind: MetricKind::Counter,
    help: "Test counter",
    buckets_ms: None,
    stable_labels: &[],
};

#[test]
fn prometheus_hub_records_metrics_and_logs() {
    let hub = PrometheusHub::<BroadcastRouter>::new();
    let meter = hub.meter();
    let counter = meter.counter(&TEST_COUNTER);
    counter.inc(2);

    let output = hub.gather().expect("gather");
    assert!(output.contains(TEST_COUNTER.name));
    assert!(output.contains("2"));

    let logger = hub.logger();
    let ctx = ObserveCtx::for_tenant("tenant-a");
    let event = LogBuilder::new(LogLevel::Info, "hello world").finish(&ctx, &NoopRedactor);
    block_on(async { logger.log(&ctx, event).await });
}
