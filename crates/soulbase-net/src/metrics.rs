use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(feature = "observe")]
use soulbase_observe::model::{MetricKind, MetricSpec};
#[cfg(feature = "observe")]
use soulbase_observe::sdk::metrics::{CounterHandle, Meter};

#[cfg(feature = "observe")]
pub mod spec {
    use soulbase_observe::model::{MetricKind, MetricSpec};

    pub const REQUEST_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_net_request_total",
        kind: MetricKind::Counter,
        help: "Total number of outbound requests.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const RETRY_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_net_retry_total",
        kind: MetricKind::Counter,
        help: "Total number of retry attempts.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const FAILURE_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_net_failure_total",
        kind: MetricKind::Counter,
        help: "Total number of failed requests.",
        buckets_ms: None,
        stable_labels: &[],
    };
}

#[cfg(feature = "observe")]
#[derive(Clone)]
struct ObservedHandles {
    requests: CounterHandle,
    retries: CounterHandle,
    failures: CounterHandle,
}

#[cfg(feature = "observe")]
impl ObservedHandles {
    fn new(meter: &dyn Meter) -> Self {
        Self {
            requests: meter.counter(&spec::REQUEST_TOTAL),
            retries: meter.counter(&spec::RETRY_TOTAL),
            failures: meter.counter(&spec::FAILURE_TOTAL),
        }
    }
}

#[derive(Clone)]
pub struct NetMetrics {
    inner: Arc<Inner>,
    #[cfg(feature = "observe")]
    observed: Option<ObservedHandles>,
}

#[derive(Default)]
struct Inner {
    request_total: AtomicU64,
    retry_total: AtomicU64,
    failure_total: AtomicU64,
}

impl Default for NetMetrics {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
            #[cfg(feature = "observe")]
            observed: None,
        }
    }
}

impl NetMetrics {
    #[cfg(feature = "observe")]
    pub fn with_meter(meter: &dyn Meter) -> Self {
        Self {
            inner: Arc::new(Inner::default()),
            observed: Some(ObservedHandles::new(meter)),
        }
    }

    pub fn record_request(&self) {
        self.inner.request_total.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.requests.inc(1);
        }
    }

    pub fn record_retry(&self) {
        self.inner.retry_total.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.retries.inc(1);
        }
    }

    pub fn record_failure(&self) {
        self.inner.failure_total.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.failures.inc(1);
        }
    }
}
