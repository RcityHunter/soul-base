#![cfg(feature = "prometheus")]

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use crate::ctx::ObserveCtx;
use crate::export::prometheus::PrometheusExporter;
use crate::export::Exporter;
use crate::model::{EvidenceEnvelope, LogEvent, MetricSpec};
use crate::pipeline::router::{BroadcastRouter, ObserveRouter, RouteDecision};
use crate::sdk::evidence::EvidenceSink;
use crate::sdk::log::Logger;
use crate::sdk::metrics::{CounterHandle, GaugeHandle, HistogramHandle, Meter};
use crate::ObserveError;

#[derive(Clone)]
pub struct PrometheusLogger<R>
where
    R: ObserveRouter + Clone + Send + Sync + 'static,
{
    exporter: Arc<PrometheusExporter>,
    router: R,
}

#[async_trait]
impl<R> Logger for PrometheusLogger<R>
where
    R: ObserveRouter + Clone + Send + Sync + 'static,
{
    async fn log(&self, ctx: &ObserveCtx, event: LogEvent) {
        if !matches!(
            self.router.route_log(ctx, &event),
            RouteDecision::Deliver(_)
        ) {
            return;
        }
        if let Err(err) = self.exporter.emit_log(ctx, &event).await {
            warn!(target = "soulbase::observe", "emit_log failed: {err:?}");
        }
    }
}

#[derive(Clone)]
pub struct PrometheusEvidence {
    exporter: Arc<PrometheusExporter>,
}

#[async_trait]
impl EvidenceSink for PrometheusEvidence {
    async fn emit<T: serde::Serialize + Send + Sync>(
        &self,
        envelope: EvidenceEnvelope<T>,
    ) -> Result<(), ObserveError> {
        if let Err(err) = self.exporter.emit_evidence(&envelope).await {
            warn!(
                target = "soulbase::observe",
                "emit_evidence failed: {err:?}"
            );
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct PrometheusMeter<R>
where
    R: ObserveRouter + Clone + Send + Sync + 'static,
{
    exporter: Arc<PrometheusExporter>,
    router: R,
}

impl<R> PrometheusMeter<R>
where
    R: ObserveRouter + Clone + Send + Sync + 'static,
{
    fn new(exporter: Arc<PrometheusExporter>, router: R) -> Self {
        Self { exporter, router }
    }
}

impl<R> Meter for PrometheusMeter<R>
where
    R: ObserveRouter + Clone + Send + Sync + 'static,
{
    fn counter(&self, spec: &'static MetricSpec) -> CounterHandle {
        let storage = Arc::new(AtomicU64::new(0));
        let exporter = self.exporter.clone();
        let router = self.router.clone();
        CounterHandle::with_callback(
            storage,
            Arc::new(move |value| {
                if matches!(router.route_metric(spec), RouteDecision::Deliver(_)) {
                    if let Err(err) = exporter.inc_counter(spec, value) {
                        warn!(
                            target = "soulbase::observe",
                            "prometheus counter error: {err:?}"
                        );
                    }
                }
            }),
        )
    }

    fn gauge(&self, spec: &'static MetricSpec) -> GaugeHandle {
        let storage = Arc::new(AtomicU64::new(0));
        let exporter = self.exporter.clone();
        let router = self.router.clone();
        GaugeHandle::with_callback(
            storage,
            Arc::new(move |value| {
                if matches!(router.route_metric(spec), RouteDecision::Deliver(_)) {
                    if let Err(err) = exporter.set_gauge(spec, value) {
                        warn!(
                            target = "soulbase::observe",
                            "prometheus gauge error: {err:?}"
                        );
                    }
                }
            }),
        )
    }

    fn histogram(&self, spec: &'static MetricSpec) -> HistogramHandle {
        let storage = Arc::new(AtomicU64::new(0));
        let exporter = self.exporter.clone();
        let router = self.router.clone();
        HistogramHandle::with_callback(
            storage,
            Arc::new(move |value| {
                if matches!(router.route_metric(spec), RouteDecision::Deliver(_)) {
                    if let Err(err) = exporter.observe_histogram(spec, value) {
                        warn!(
                            target = "soulbase::observe",
                            "prometheus histogram error: {err:?}"
                        );
                    }
                }
            }),
        )
    }
}

pub struct PrometheusHub<R = BroadcastRouter>
where
    R: ObserveRouter + Clone + Send + Sync + 'static,
{
    exporter: Arc<PrometheusExporter>,
    router: R,
}

impl<R> PrometheusHub<R>
where
    R: ObserveRouter + Clone + Send + Sync + Default + 'static,
{
    pub fn new() -> Self {
        Self::with_router(R::default())
    }
}

impl<R> PrometheusHub<R>
where
    R: ObserveRouter + Clone + Send + Sync + 'static,
{
    pub fn with_router(router: R) -> Self {
        let exporter = Arc::new(PrometheusExporter::new());
        Self { exporter, router }
    }

    pub fn exporter(&self) -> Arc<PrometheusExporter> {
        self.exporter.clone()
    }

    pub fn logger(&self) -> PrometheusLogger<R> {
        PrometheusLogger {
            exporter: self.exporter.clone(),
            router: self.router.clone(),
        }
    }

    pub fn meter(&self) -> PrometheusMeter<R> {
        PrometheusMeter::new(self.exporter.clone(), self.router.clone())
    }

    pub fn evidence(&self) -> PrometheusEvidence {
        PrometheusEvidence {
            exporter: self.exporter.clone(),
        }
    }

    pub fn gather(&self) -> Result<String, ObserveError> {
        self.exporter.gather()
    }
}
