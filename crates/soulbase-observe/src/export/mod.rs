use crate::ctx::ObserveCtx;
use crate::{
    model::{EvidenceEnvelope, LogEvent, MetricSpec},
    ObserveError,
};
use async_trait::async_trait;

#[async_trait]
pub trait Exporter: Send + Sync {
    async fn emit_log(&self, ctx: &ObserveCtx, event: &LogEvent) -> Result<(), ObserveError>;
    async fn emit_metric(&self, spec: &'static MetricSpec, value: f64) -> Result<(), ObserveError>;
    async fn emit_evidence<T: serde::Serialize + Send + Sync>(
        &self,
        envelope: &EvidenceEnvelope<T>,
    ) -> Result<(), ObserveError>;
}

#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "logs-http")]
pub mod logs_http;
#[cfg(feature = "otlp")]
pub mod otlp;
#[cfg(feature = "prometheus")]
pub mod prometheus;
#[cfg(feature = "stdout")]
pub mod stdout;

#[derive(Default)]
pub struct NoopExporter;

#[async_trait]
impl Exporter for NoopExporter {
    async fn emit_log(&self, _ctx: &ObserveCtx, _event: &LogEvent) -> Result<(), ObserveError> {
        Ok(())
    }

    async fn emit_metric(
        &self,
        _spec: &'static MetricSpec,
        _value: f64,
    ) -> Result<(), ObserveError> {
        Ok(())
    }

    async fn emit_evidence<T: serde::Serialize + Send + Sync>(
        &self,
        _envelope: &EvidenceEnvelope<T>,
    ) -> Result<(), ObserveError> {
        Ok(())
    }
}
