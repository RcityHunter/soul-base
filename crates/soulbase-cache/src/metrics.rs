use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(feature = "observe")]
use soulbase_observe::model::{MetricKind, MetricSpec};
#[cfg(feature = "observe")]
use soulbase_observe::sdk::metrics::{CounterHandle, HistogramHandle, Meter};

#[cfg(feature = "observe")]
pub mod spec {
    use soulbase_observe::model::{MetricKind, MetricSpec};

    pub const CACHE_HIT_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_cache_hit_total",
        kind: MetricKind::Counter,
        help: "Total number of cache hits.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const CACHE_MISS_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_cache_miss_total",
        kind: MetricKind::Counter,
        help: "Total number of cache misses.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const CACHE_LOAD_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_cache_load_total",
        kind: MetricKind::Counter,
        help: "Total number of cache loader executions.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const CACHE_ERROR_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_cache_error_total",
        kind: MetricKind::Counter,
        help: "Total number of cache errors.",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const CACHE_LOAD_LATENCY_MS: MetricSpec = MetricSpec {
        name: "soulbase_cache_load_latency_ms",
        kind: MetricKind::Histogram,
        help: "Observed cache loader latency in milliseconds.",
        buckets_ms: Some(&[5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560, 5120]),
        stable_labels: &[],
    };
}

#[cfg(feature = "observe")]
#[derive(Clone)]
struct ObservedHandles {
    hits: CounterHandle,
    misses: CounterHandle,
    loads: CounterHandle,
    errors: CounterHandle,
    load_latency: HistogramHandle,
}

#[cfg(feature = "observe")]
impl ObservedHandles {
    fn new(meter: &dyn Meter) -> Self {
        Self {
            hits: meter.counter(&spec::CACHE_HIT_TOTAL),
            misses: meter.counter(&spec::CACHE_MISS_TOTAL),
            loads: meter.counter(&spec::CACHE_LOAD_TOTAL),
            errors: meter.counter(&spec::CACHE_ERROR_TOTAL),
            load_latency: meter.histogram(&spec::CACHE_LOAD_LATENCY_MS),
        }
    }
}

#[derive(Clone)]
pub struct SimpleStats {
    inner: Arc<Inner>,
    #[cfg(feature = "observe")]
    observed: Option<ObservedHandles>,
}

#[derive(Default)]
struct Inner {
    hits: AtomicU64,
    misses: AtomicU64,
    loads: AtomicU64,
    errors: AtomicU64,
}

impl Default for SimpleStats {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
            #[cfg(feature = "observe")]
            observed: None,
        }
    }
}

impl SimpleStats {
    #[cfg(feature = "observe")]
    pub fn with_meter(meter: &dyn Meter) -> Self {
        Self {
            inner: Arc::new(Inner::default()),
            observed: Some(ObservedHandles::new(meter)),
        }
    }

    pub fn record_hit(&self) {
        self.inner.hits.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.hits.inc(1);
        }
    }

    pub fn record_miss(&self) {
        self.inner.misses.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.misses.inc(1);
        }
    }

    pub fn record_load(&self) {
        self.inner.loads.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.loads.inc(1);
        }
    }

    pub fn record_error(&self) {
        self.inner.errors.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.errors.inc(1);
        }
    }

    pub fn observe_load_time(&self, duration_ms: u64) {
        #[cfg(feature = "observe")]
        if let Some(obs) = &self.observed {
            obs.load_latency.observe(duration_ms);
        }
        #[cfg(not(feature = "observe"))]
        let _ = duration_ms;
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            hits: self.inner.hits.load(Ordering::Relaxed),
            misses: self.inner.misses.load(Ordering::Relaxed),
            loads: self.inner.loads.load(Ordering::Relaxed),
            errors: self.inner.errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct StatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub loads: u64,
    pub errors: u64,
}
