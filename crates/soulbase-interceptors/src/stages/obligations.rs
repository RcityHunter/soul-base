use crate::context::{InterceptContext, ProtoRequest, ProtoResponse};
use crate::errors::InterceptError;
use crate::stages::{Stage, StageOutcome};
use async_trait::async_trait;

pub struct ObligationsStage;

#[async_trait]
impl Stage for ObligationsStage {
    async fn handle(
        &self,
        cx: &mut InterceptContext,
        _req: &mut dyn ProtoRequest,
        _rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        let _kinds: Vec<String> = cx.obligations.iter().map(|o| o.kind.clone()).collect();
        Ok(StageOutcome::Continue)
    }
}
