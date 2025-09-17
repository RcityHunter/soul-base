use crate::context::{EnvelopeSeed, InterceptContext, ProtoRequest, ProtoResponse};
use crate::errors::InterceptError;
use crate::stages::{Stage, StageOutcome};
use async_trait::async_trait;
use soulbase_types::prelude::TraceContext;

pub struct ContextInitStage;

#[async_trait]
impl Stage for ContextInitStage {
    async fn handle(
        &self,
        cx: &mut InterceptContext,
        req: &mut dyn ProtoRequest,
        _rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        let request_id = req
            .header("X-Request-Id")
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        cx.request_id = request_id;

        cx.trace = TraceContext {
            trace_id: req.header("X-Trace-Id"),
            span_id: None,
            baggage: Default::default(),
        };
        cx.tenant_header = req.header("X-Soul-Tenant");
        cx.consent_token = req.header("X-Consent-Token");

        let tenant = cx
            .tenant_header
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let partition_suffix = req
            .path()
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or("-");

        cx.envelope_seed = EnvelopeSeed {
            correlation_id: req.header("X-Correlation-Id"),
            causation_id: req.header("X-Causation-Id"),
            partition_key: format!("{tenant}:{partition_suffix}"),
            produced_at_ms: chrono::Utc::now().timestamp_millis(),
        };

        Ok(StageOutcome::Continue)
    }
}
