use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(feature = "observe")]
use soulbase_observe::model::{MetricKind, MetricSpec};
#[cfg(feature = "observe")]
use soulbase_observe::sdk::metrics::{CounterHandle, Meter};

#[cfg(feature = "observe")]
pub mod spec {
    use soulbase_observe::model::{MetricKind, MetricSpec};

    pub const PUT_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_blob_put_total",
        kind: MetricKind::Counter,
        help: "Total number of blob put operations",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const GET_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_blob_get_total",
        kind: MetricKind::Counter,
        help: "Total number of blob get operations",
        buckets_ms: None,
        stable_labels: &[],
    };

    pub const DELETE_TOTAL: MetricSpec = MetricSpec {
        name: "soulbase_blob_delete_total",
        kind: MetricKind::Counter,
        help: "Total number of blob delete operations",
        buckets_ms: None,
        stable_labels: &[],
    };
}

#[cfg(feature = "observe")]
#[derive(Clone)]
struct ObservedHandles {
    puts: CounterHandle,
    gets: CounterHandle,
    deletes: CounterHandle,
}

#[cfg(feature = "observe")]
impl ObservedHandles {
    fn new(meter: &dyn Meter) -> Self {
        Self {
            puts: meter.counter(&spec::PUT_TOTAL),
            gets: meter.counter(&spec::GET_TOTAL),
            deletes: meter.counter(&spec::DELETE_TOTAL),
        }
    }
}

#[derive(Clone, Default)]
pub struct BlobStats {
    inner: Arc<Inner>,
    #[cfg(feature = "observe")]
    observed: Option<ObservedHandles>,
}

#[derive(Default)]
struct Inner {
    puts: AtomicU64,
    gets: AtomicU64,
    deletes: AtomicU64,
}

impl BlobStats {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(feature = "observe")]
    pub fn with_meter(meter: &dyn Meter) -> Self {
        Self {
            inner: Arc::new(Inner::default()),
            observed: Some(ObservedHandles::new(meter)),
        }
    }

    pub fn record_put(&self) {
        self.inner.puts.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(observed) = &self.observed {
            observed.puts.inc(1);
        }
    }

    pub fn record_get(&self) {
        self.inner.gets.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(observed) = &self.observed {
            observed.gets.inc(1);
        }
    }

    pub fn record_delete(&self) {
        self.inner.deletes.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "observe")]
        if let Some(observed) = &self.observed {
            observed.deletes.inc(1);
        }
    }

    pub fn snapshot(&self) -> BlobStatsSnapshot {
        BlobStatsSnapshot {
            puts: self.inner.puts.load(Ordering::Relaxed),
            gets: self.inner.gets.load(Ordering::Relaxed),
            deletes: self.inner.deletes.load(Ordering::Relaxed),
        }
    }
}

impl fmt::Debug for BlobStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let snapshot = self.snapshot();
        f.debug_struct("BlobStats")
            .field("puts", &snapshot.puts)
            .field("gets", &snapshot.gets)
            .field("deletes", &snapshot.deletes)
            .finish()
    }
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobStatsSnapshot {
    pub puts: u64,
    pub gets: u64,
    pub deletes: u64,
}
