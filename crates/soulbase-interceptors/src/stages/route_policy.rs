use crate::context::{InterceptContext, ProtoRequest, ProtoResponse, RouteBinding};
use crate::errors::InterceptError;
use crate::policy::dsl::RoutePolicy;
use crate::stages::{Stage, StageOutcome};
use async_trait::async_trait;
use soulbase_auth::prelude::{Action, ResourceUrn};

pub struct RoutePolicyStage {
    pub policy: RoutePolicy,
}

#[async_trait]
impl Stage for RoutePolicyStage {
    async fn handle(
        &self,
        cx: &mut InterceptContext,
        req: &mut dyn ProtoRequest,
        _rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        let Some(spec) = self.policy.match_http(req.method(), req.path()) else {
            return Err(crate::errors::InterceptError::deny_policy(
                "route not declared",
            ));
        };

        let resource = ResourceUrn(spec.bind.resource.clone());
        let action = match spec.bind.action.as_str() {
            "Read" => Action::Read,
            "Write" => Action::Write,
            "Invoke" => Action::Invoke,
            "List" => Action::List,
            "Admin" => Action::Admin,
            "Configure" => Action::Configure,
            _ => Action::Read,
        };

        let mut attrs = spec
            .bind
            .attrs_template
            .clone()
            .unwrap_or_else(|| serde_json::json!({}));
        if spec.bind.attrs_from_body {
            attrs = req
                .read_json()
                .await
                .unwrap_or_else(|_| serde_json::json!({}));
        }

        cx.route = Some(RouteBinding {
            resource,
            action,
            attrs,
        });
        Ok(StageOutcome::Continue)
    }
}
