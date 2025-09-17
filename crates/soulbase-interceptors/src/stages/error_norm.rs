use crate::context::{InterceptContext, ProtoRequest, ProtoResponse};
use crate::errors::InterceptError;
use crate::stages::{Stage, StageOutcome};
use async_trait::async_trait;

/// Placeholder for error normalization hooks.
pub struct ErrorNormStage;

#[async_trait]
impl Stage for ErrorNormStage {
    async fn handle(
        &self,
        _cx: &mut InterceptContext,
        _req: &mut dyn ProtoRequest,
        _rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        Ok(StageOutcome::Continue)
    }
}
