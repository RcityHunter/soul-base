use crate::errors::StorageError;
use crate::model::{Entity, Page, QueryParams};
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;

#[async_trait]
pub trait Repository<E: Entity>: Send + Sync {
    async fn create(&self, tenant: &TenantId, entity: &E) -> Result<(), StorageError>;
    async fn upsert(
        &self,
        tenant: &TenantId,
        id: &str,
        patch: serde_json::Value,
        session: Option<&str>,
    ) -> Result<E, StorageError>;
    async fn get(&self, tenant: &TenantId, id: &str) -> Result<Option<E>, StorageError>;
    async fn select(&self, tenant: &TenantId, params: QueryParams)
        -> Result<Page<E>, StorageError>;
    async fn delete(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError>;
}
