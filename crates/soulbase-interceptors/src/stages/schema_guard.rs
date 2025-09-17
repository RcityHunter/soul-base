use crate::context::{InterceptContext, ProtoRequest, ProtoResponse};
use crate::errors::InterceptError;
use crate::stages::{Stage, StageOutcome};
use async_trait::async_trait;

pub struct SchemaGuardStage;

#[async_trait]
impl Stage for SchemaGuardStage {
    async fn handle(
        &self,
        _cx: &mut InterceptContext,
        _req: &mut dyn ProtoRequest,
        _rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        Ok(StageOutcome::Continue)
    }
}
