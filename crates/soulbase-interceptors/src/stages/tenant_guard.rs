use crate::context::{InterceptContext, ProtoRequest, ProtoResponse};
use crate::errors::InterceptError;
use crate::stages::{Stage, StageOutcome};
use async_trait::async_trait;
use soulbase_errors::prelude::codes;

pub struct TenantGuardStage;

#[async_trait]
impl Stage for TenantGuardStage {
    async fn handle(
        &self,
        cx: &mut InterceptContext,
        _req: &mut dyn ProtoRequest,
        _rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        let subject = cx.subject.as_ref().ok_or_else(|| {
            InterceptError::from_public(codes::AUTH_UNAUTHENTICATED, "Please sign in.")
        })?;
        if let Some(header_tenant) = cx.tenant_header.as_ref() {
            if &subject.tenant.0 != header_tenant {
                return Err(InterceptError::from_public(
                    codes::AUTH_FORBIDDEN,
                    "Tenant mismatch",
                ));
            }
        }
        Ok(StageOutcome::Continue)
    }
}
