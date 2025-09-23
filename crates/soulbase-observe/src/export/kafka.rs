use async_trait::async_trait;

use crate::ctx::ObserveCtx;
use crate::export::Exporter;
use crate::model::{EvidenceEnvelope, LogEvent, MetricSpec};
use crate::ObserveError;

#[derive(Default)]
pub struct KafkaExporter;

#[async_trait]
impl Exporter for KafkaExporter {
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
