use crate::errors::AuthError;
use crate::model::{AuthzRequest, Decision};
use async_trait::async_trait;

pub mod local;

#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn decide(
        &self,
        request: &AuthzRequest,
        merged_attrs: &serde_json::Value,
    ) -> Result<Decision, AuthError>;
}
