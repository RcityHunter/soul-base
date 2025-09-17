use crate::context::{InterceptContext, ProtoRequest, ProtoResponse};
use crate::errors::InterceptError;
use crate::stages::{Stage, StageOutcome};
use async_trait::async_trait;

pub struct ResponseStampStage;

#[async_trait]
impl Stage for ResponseStampStage {
    async fn handle(
        &self,
        cx: &mut InterceptContext,
        _req: &mut dyn ProtoRequest,
        rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        rsp.insert_header("X-Request-Id", &cx.request_id);
        if let Some(trace_id) = &cx.trace.trace_id {
            rsp.insert_header("X-Trace-Id", trace_id);
        }
        if let Some(version) = &cx.config_version {
            rsp.insert_header("X-Config-Version", version);
        }
        if let Some(checksum) = &cx.config_checksum {
            rsp.insert_header("X-Config-Checksum", checksum);
        }
        if !cx.obligations.is_empty() {
            let kinds = cx
                .obligations
                .iter()
                .map(|o| o.kind.as_str())
                .collect::<Vec<_>>()
                .join(",");
            rsp.insert_header("X-Obligations", &kinds);
        }
        Ok(StageOutcome::Continue)
    }
}
