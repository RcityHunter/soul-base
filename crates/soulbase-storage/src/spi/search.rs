use crate::errors::StorageError;
use crate::model::Page;
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;

#[async_trait]
pub trait SearchStore: Send + Sync {
    async fn search(
        &self,
        tenant: &TenantId,
        query: &str,
        limit: usize,
    ) -> Result<Page<serde_json::Value>, StorageError>;
}
