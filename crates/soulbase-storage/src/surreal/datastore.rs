use crate::errors::StorageError;
use crate::spi::query::QueryExecutor;
use async_trait::async_trait;

#[derive(Clone, Default)]
pub struct SurrealDatastore;

#[async_trait]
impl QueryExecutor for SurrealDatastore {
    async fn query(
        &self,
        _statement: &str,
        _params: serde_json::Value,
    ) -> Result<serde_json::Value, StorageError> {
        Err(StorageError::internal("surreal adapter not implemented"))
    }
}
