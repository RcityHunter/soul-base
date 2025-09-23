use async_trait::async_trait;

use crate::ctx::ObserveCtx;
use crate::export::Exporter;
use crate::model::{EvidenceEnvelope, LogEvent, MetricSpec};
use crate::ObserveError;

#[derive(Default)]
pub struct StdoutExporter;

#[async_trait]
impl Exporter for StdoutExporter {
    async fn emit_log(&self, ctx: &ObserveCtx, event: &LogEvent) -> Result<(), ObserveError> {
        if matches!(
            event.level,
            crate::model::LogLevel::Warn
                | crate::model::LogLevel::Error
                | crate::model::LogLevel::Critical
        ) {
            println!("[warn+] tenant={} msg={}", ctx.tenant, event.msg);
        }
        Ok(())
    }

    async fn emit_metric(&self, spec: &'static MetricSpec, value: f64) -> Result<(), ObserveError> {
        println!("metric {} = {}", spec.name, value);
        Ok(())
    }

    async fn emit_evidence<T: serde::Serialize + Send + Sync>(
        &self,
        envelope: &EvidenceEnvelope<T>,
    ) -> Result<(), ObserveError> {
        println!(
            "evidence {}",
            serde_json::to_string(&envelope).unwrap_or_default()
        );
        Ok(())
    }
}
