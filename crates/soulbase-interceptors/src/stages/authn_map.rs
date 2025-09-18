use crate::context::{InterceptContext, ProtoRequest, ProtoResponse};
use crate::errors::InterceptError;
use crate::stages::{Stage, StageOutcome};
use async_trait::async_trait;
use soulbase_auth::prelude::{Authenticator, AuthnInput};
use soulbase_errors::prelude::*;

pub struct AuthnMapStage {
    pub authenticator: Box<dyn Authenticator>,
}

#[async_trait]
impl Stage for AuthnMapStage {
    async fn handle(
        &self,
        cx: &mut InterceptContext,
        req: &mut dyn ProtoRequest,
        _rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        let Some(authorization) = req.header("Authorization") else {
            return Err(InterceptError::from_public(
                codes::AUTH_UNAUTHENTICATED,
                "Please sign in.",
            ));
        };
        let token = authorization
            .strip_prefix("Bearer ")
            .unwrap_or(&authorization)
            .to_string();
        let input = AuthnInput::BearerJwt(token.clone());
        cx.authn_input = Some(input.clone());

        let subject = self
            .authenticator
            .authenticate(input)
            .await
            .map_err(|e| InterceptError::from_error(e.into_inner()))?;
        cx.subject = Some(subject);
        Ok(StageOutcome::Continue)
    }
}
