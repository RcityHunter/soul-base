use super::*;
use crate::model::Decision;
use serde_json::Value;

pub struct LocalAuthorizer;

#[async_trait::async_trait]
impl super::Authorizer for LocalAuthorizer {
    async fn decide(
        &self,
        _request: &AuthzRequest,
        merged_attrs: &Value,
    ) -> Result<Decision, AuthError> {
        let allow = merged_attrs
            .get("allow")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        Ok(Decision {
            allow,
            reason: if allow {
                None
            } else {
                Some("deny-by-default".into())
            },
            obligations: Vec::new(),
            evidence: serde_json::json!({
                "policy": "local",
                "rule": if allow { "allow" } else { "deny" }
            }),
            cache_ttl_ms: if allow { 1000 } else { 0 },
        })
    }
}
