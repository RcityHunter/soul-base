use crate::errors::ConfigError;
use async_trait::async_trait;

#[async_trait]
pub trait SecretResolver: Send + Sync {
    fn id(&self) -> &'static str;
    async fn resolve(&self, uri: &str) -> Result<serde_json::Value, ConfigError>;
}

pub struct NoopSecretResolver;

#[async_trait]
impl SecretResolver for NoopSecretResolver {
    fn id(&self) -> &'static str {
        "noop"
    }

    async fn resolve(&self, uri: &str) -> Result<serde_json::Value, ConfigError> {
        Ok(serde_json::Value::String(uri.to_string()))
    }
}

pub fn is_secret_ref(value: &serde_json::Value) -> Option<&str> {
    value.as_str().and_then(|s| s.strip_prefix("secret://"))
}
