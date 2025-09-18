use crate::context::{InterceptContext, ProtoRequest, ProtoResponse};
use crate::errors::InterceptError;
use crate::stages::{Stage, StageOutcome};
use async_trait::async_trait;
use soulbase_auth::AuthFacade;
use soulbase_errors::prelude::*;

pub struct AuthzQuotaStage {
    pub facade: AuthFacade,
}

#[async_trait]
impl Stage for AuthzQuotaStage {
    async fn handle(
        &self,
        cx: &mut InterceptContext,
        _req: &mut dyn ProtoRequest,
        rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        let Some(authn_input) = cx.authn_input.clone() else {
            return write_error(
                rsp,
                InterceptError::from_public(codes::AUTH_UNAUTHENTICATED, "Please sign in."),
            )
            .await;
        };
        let Some(route) = cx.route.as_ref() else {
            return write_error(rsp, InterceptError::deny_policy("Route not bound")).await;
        };

        let decision = self
            .facade
            .authorize(
                authn_input,
                route.resource.clone(),
                route.action.clone(),
                route.attrs.clone(),
                None,
                cx.envelope_seed.correlation_id.clone(),
            )
            .await
            .map_err(|e| InterceptError::from_error(e.into_inner()))?;

        if !decision.allow {
            let msg = decision.reason.unwrap_or_else(|| "Forbidden".to_string());
            return write_error(
                rsp,
                InterceptError::from_public(codes::AUTH_FORBIDDEN, &msg),
            )
            .await;
        }

        cx.obligations = decision.obligations.clone();
        Ok(StageOutcome::Continue)
    }
}

async fn write_error(
    rsp: &mut dyn ProtoResponse,
    err: InterceptError,
) -> Result<StageOutcome, InterceptError> {
    let (status, json) = crate::errors::to_http_response(&err);
    rsp.set_status(status);
    rsp.write_json(&json).await?;
    Ok(StageOutcome::ShortCircuit)
}
