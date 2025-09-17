use async_trait::async_trait;
use crate::errors::AuthError;
use crate::model::{AuthzRequest, Decision};

pub mod local;

#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn decide(&self, request: &AuthzRequest, merged_attrs: &serde_json::Value) -> Result<Decision, AuthError>;
}
