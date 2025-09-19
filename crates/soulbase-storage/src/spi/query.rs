use crate::errors::StorageError;
use async_trait::async_trait;

#[async_trait]
pub trait QueryExecutor: Send + Sync {
    async fn query(
        &self,
        statement: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, StorageError>;
}
