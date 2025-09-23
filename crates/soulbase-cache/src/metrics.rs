use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct SimpleStats {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    hits: AtomicU64,
    misses: AtomicU64,
    loads: AtomicU64,
    errors: AtomicU64,
}

impl SimpleStats {
    pub fn record_hit(&self) {
        self.inner.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.inner.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_load(&self) {
        self.inner.loads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.inner.errors.fetch_add(1, Ordering::Relaxed);
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
