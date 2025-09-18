use crate::errors::AuthError;
use crate::model::AuthzRequest;
use async_trait::async_trait;

#[async_trait]
pub trait AttributeProvider: Send + Sync {
    async fn augment(&self, req: &AuthzRequest) -> Result<serde_json::Value, AuthError>;
}

pub struct DefaultAttributeProvider;

#[async_trait]
impl AttributeProvider for DefaultAttributeProvider {
    async fn augment(&self, _req: &AuthzRequest) -> Result<serde_json::Value, AuthError> {
        Ok(serde_json::json!({}))
    }
}
