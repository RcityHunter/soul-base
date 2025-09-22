use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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
        CounterHandle {
            storage: self.entry(spec),
        }
    }

    fn gauge(&self, spec: &'static MetricSpec) -> GaugeHandle {
        GaugeHandle {
            storage: self.entry(spec),
        }
    }

    fn histogram(&self, spec: &'static MetricSpec) -> HistogramHandle {
        HistogramHandle {
            storage: self.entry(spec),
        }
    }
}

#[derive(Clone)]
pub struct CounterHandle {
    storage: Arc<AtomicU64>,
}

impl CounterHandle {
    pub fn inc(&self, value: u64) {
        self.storage.fetch_add(value, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct GaugeHandle {
    storage: Arc<AtomicU64>,
}

impl GaugeHandle {
    pub fn set(&self, value: u64) {
        self.storage.store(value, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct HistogramHandle {
    storage: Arc<AtomicU64>,
}

impl HistogramHandle {
    pub fn observe(&self, value: u64) {
        self.storage.store(value, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct NoopMeter;

impl Meter for NoopMeter {
    fn counter(&self, _spec: &'static MetricSpec) -> CounterHandle {
        CounterHandle {
            storage: Arc::new(AtomicU64::new(0)),
        }
    }

    fn gauge(&self, _spec: &'static MetricSpec) -> GaugeHandle {
        GaugeHandle {
            storage: Arc::new(AtomicU64::new(0)),
        }
    }

    fn histogram(&self, _spec: &'static MetricSpec) -> HistogramHandle {
        HistogramHandle {
            storage: Arc::new(AtomicU64::new(0)),
        }
    }
}
