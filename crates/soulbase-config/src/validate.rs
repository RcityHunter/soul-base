use crate::errors::ConfigError;

#[async_trait::async_trait]
pub trait Validator: Send + Sync {
    async fn validate_boot(&self, tree: &serde_json::Value) -> Result<(), ConfigError>;
    async fn validate_delta(
        &self,
        old: &serde_json::Value,
        new: &serde_json::Value,
    ) -> Result<(), ConfigError>;
}

pub struct BasicValidator;

#[async_trait::async_trait]
impl Validator for BasicValidator {
    async fn validate_boot(&self, _tree: &serde_json::Value) -> Result<(), ConfigError> {
        Ok(())
    }

    async fn validate_delta(
        &self,
        _old: &serde_json::Value,
        _new: &serde_json::Value,
    ) -> Result<(), ConfigError> {
        Ok(())
    }
}
