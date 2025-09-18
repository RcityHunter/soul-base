use crate::errors::InterceptError;
use async_trait::async_trait;

pub mod memory;

#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    async fn check_or_insert(&self, key: &str) -> Result<bool, InterceptError>;
}
