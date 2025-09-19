use crate::errors::StorageError;
use crate::model::Page;
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;

#[async_trait]
pub trait SearchStore: Send + Sync {
    async fn search(
        &self,
        _tenant: &TenantId,
        _query: &str,
        _limit: usize,
    ) -> Result<Page<serde_json::Value>, StorageError> {
        Err(StorageError::not_found("search not implemented"))
    }
}
