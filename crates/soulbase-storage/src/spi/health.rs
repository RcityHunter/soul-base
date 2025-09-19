use crate::errors::StorageError;
use async_trait::async_trait;

#[async_trait]
pub trait HealthCheck: Send + Sync {
    async fn ping(&self) -> Result<(), StorageError>;
}
