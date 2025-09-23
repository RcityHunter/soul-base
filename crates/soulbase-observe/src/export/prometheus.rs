use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use prometheus::{Counter, Encoder, Gauge, Histogram, HistogramOpts, Opts, Registry, TextEncoder};

use crate::ctx::ObserveCtx;
use crate::export::Exporter;
use crate::model::{EvidenceEnvelope, LogEvent, MetricKind, MetricSpec};
use crate::ObserveError;

#[derive(Clone)]
pub struct PrometheusExporter {
    registry: Registry,
    counters: Arc<Mutex<HashMap<&'static str, Counter>>>,
    gauges: Arc<Mutex<HashMap<&'static str, Gauge>>>,
    histograms: Arc<Mutex<HashMap<&'static str, Histogram>>>,
}

impl PrometheusExporter {
    pub fn new() -> Self {
        Self {
            registry: Registry::new(),
            counters: Arc::new(Mutex::new(HashMap::new())),
            gauges: Arc::new(Mutex::new(HashMap::new())),
            histograms: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn ensure_counter(&self, spec: &'static MetricSpec) -> Result<Counter, ObserveError> {
        let mut guard = self.counters.lock();
        if let Some(existing) = guard.get(spec.name) {
            return Ok(existing.clone());
        }
        let counter = Counter::with_opts(Opts::new(spec.name, spec.help))
            .map_err(|e| ObserveError::internal(&format!("prometheus counter opts: {e}")))?;
        self.registry
            .register(Box::new(counter.clone()))
            .map_err(|e| ObserveError::internal(&format!("prometheus register: {e}")))?;
        guard.insert(spec.name, counter.clone());
        Ok(counter)
    }

    fn ensure_gauge(&self, spec: &'static MetricSpec) -> Result<Gauge, ObserveError> {
        let mut guard = self.gauges.lock();
        if let Some(existing) = guard.get(spec.name) {
            return Ok(existing.clone());
        }
        let gauge = Gauge::with_opts(Opts::new(spec.name, spec.help))
            .map_err(|e| ObserveError::internal(&format!("prometheus gauge opts: {e}")))?;
        self.registry
            .register(Box::new(gauge.clone()))
            .map_err(|e| ObserveError::internal(&format!("prometheus register: {e}")))?;
        guard.insert(spec.name, gauge.clone());
        Ok(gauge)
    }

    fn ensure_histogram(&self, spec: &'static MetricSpec) -> Result<Histogram, ObserveError> {
        let mut guard = self.histograms.lock();
        if let Some(existing) = guard.get(spec.name) {
            return Ok(existing.clone());
        }
        let buckets: Vec<f64> = spec
            .buckets_ms
            .unwrap_or(&[5, 10, 20, 50, 100, 200, 500, 1000])
            .iter()
            .map(|v| *v as f64)
            .collect();
        let opts = HistogramOpts::new(spec.name, spec.help).buckets(buckets);
        let histogram = Histogram::with_opts(opts)
            .map_err(|e| ObserveError::internal(&format!("prometheus histogram opts: {e}")))?;
        self.registry
            .register(Box::new(histogram.clone()))
            .map_err(|e| ObserveError::internal(&format!("prometheus register: {e}")))?;
        guard.insert(spec.name, histogram.clone());
        Ok(histogram)
    }

    pub fn gather(&self) -> Result<String, ObserveError> {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut buffer)
            .map_err(|e| ObserveError::internal(&format!("prometheus encode: {e}")))?;
        String::from_utf8(buffer)
            .map_err(|e| ObserveError::internal(&format!("prometheus utf8: {e}")))
    }
}

impl Default for PrometheusExporter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Exporter for PrometheusExporter {
    async fn emit_log(&self, _ctx: &ObserveCtx, _event: &LogEvent) -> Result<(), ObserveError> {
        Ok(())
    }

    async fn emit_metric(&self, spec: &'static MetricSpec, value: f64) -> Result<(), ObserveError> {
        match spec.kind {
            MetricKind::Counter => {
                let counter = self.ensure_counter(spec)?;
                counter.inc_by(value);
                Ok(())
            }
            MetricKind::Gauge => {
                let gauge = self.ensure_gauge(spec)?;
                gauge.set(value);
                Ok(())
            }
            MetricKind::Histogram => {
                let histogram = self.ensure_histogram(spec)?;
                histogram.observe(value);
                Ok(())
            }
        }
    }

    async fn emit_evidence<T: serde::Serialize + Send + Sync>(
        &self,
        _envelope: &EvidenceEnvelope<T>,
    ) -> Result<(), ObserveError> {
        Ok(())
    }
}
