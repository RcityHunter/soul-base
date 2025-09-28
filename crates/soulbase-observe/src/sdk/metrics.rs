use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

type CounterCallback = dyn Fn(u64) + Send + Sync;
type GaugeCallback = dyn Fn(u64) + Send + Sync;
type HistogramCallback = dyn Fn(u64) + Send + Sync;

use parking_lot::Mutex;

use crate::model::MetricSpec;

pub trait Meter: Send + Sync {
    fn counter(&self, spec: &'static MetricSpec) -> CounterHandle;
    fn gauge(&self, spec: &'static MetricSpec) -> GaugeHandle;
    fn histogram(&self, spec: &'static MetricSpec) -> HistogramHandle;
}

#[derive(Clone, Default)]
pub struct MeterRegistry {
    inner: Arc<Mutex<HashMap<&'static str, Arc<AtomicU64>>>>,
}

impl MeterRegistry {
    fn entry(&self, spec: &'static MetricSpec) -> Arc<AtomicU64> {
        let mut guard = self.inner.lock();
        guard
            .entry(spec.name)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }
}

impl Meter for MeterRegistry {
    fn counter(&self, spec: &'static MetricSpec) -> CounterHandle {
        CounterHandle::new(self.entry(spec))
    }

    fn gauge(&self, spec: &'static MetricSpec) -> GaugeHandle {
        GaugeHandle::new(self.entry(spec))
    }

    fn histogram(&self, spec: &'static MetricSpec) -> HistogramHandle {
        HistogramHandle::new(self.entry(spec))
    }
}

#[derive(Clone)]
pub struct CounterHandle {
    storage: Arc<AtomicU64>,
    callback: Option<Arc<CounterCallback>>,
}

impl CounterHandle {
    pub fn new(storage: Arc<AtomicU64>) -> Self {
        Self {
            storage,
            callback: None,
        }
    }

    pub fn with_callback(storage: Arc<AtomicU64>, callback: Arc<CounterCallback>) -> Self {
        Self {
            storage,
            callback: Some(callback),
        }
    }

    pub fn inc(&self, value: u64) {
        self.storage.fetch_add(value, Ordering::Relaxed);
        if let Some(cb) = &self.callback {
            cb(value);
        }
    }
}

impl Default for CounterHandle {
    fn default() -> Self {
        Self::new(Arc::new(AtomicU64::new(0)))
    }
}

#[derive(Clone)]
pub struct GaugeHandle {
    storage: Arc<AtomicU64>,
    callback: Option<Arc<GaugeCallback>>,
}

impl GaugeHandle {
    pub fn new(storage: Arc<AtomicU64>) -> Self {
        Self {
            storage,
            callback: None,
        }
    }

    pub fn with_callback(storage: Arc<AtomicU64>, callback: Arc<GaugeCallback>) -> Self {
        Self {
            storage,
            callback: Some(callback),
        }
    }

    pub fn set(&self, value: u64) {
        self.storage.store(value, Ordering::Relaxed);
        if let Some(cb) = &self.callback {
            cb(value);
        }
    }
}

impl Default for GaugeHandle {
    fn default() -> Self {
        Self::new(Arc::new(AtomicU64::new(0)))
    }
}

#[derive(Clone)]
pub struct HistogramHandle {
    storage: Arc<AtomicU64>,
    callback: Option<Arc<HistogramCallback>>,
}

impl HistogramHandle {
    pub fn new(storage: Arc<AtomicU64>) -> Self {
        Self {
            storage,
            callback: None,
        }
    }

    pub fn with_callback(storage: Arc<AtomicU64>, callback: Arc<HistogramCallback>) -> Self {
        Self {
            storage,
            callback: Some(callback),
        }
    }

    pub fn observe(&self, value: u64) {
        self.storage.store(value, Ordering::Relaxed);
        if let Some(cb) = &self.callback {
            cb(value);
        }
    }
}

impl Default for HistogramHandle {
    fn default() -> Self {
        Self::new(Arc::new(AtomicU64::new(0)))
    }
}

#[derive(Default)]
pub struct NoopMeter;

impl Meter for NoopMeter {
    fn counter(&self, _spec: &'static MetricSpec) -> CounterHandle {
        CounterHandle::default()
    }

    fn gauge(&self, _spec: &'static MetricSpec) -> GaugeHandle {
        GaugeHandle::default()
    }

    fn histogram(&self, _spec: &'static MetricSpec) -> HistogramHandle {
        HistogramHandle::default()
    }
}
