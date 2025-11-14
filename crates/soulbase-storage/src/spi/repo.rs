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

    /// Upsert without returning the result entity (performance optimization)
    async fn upsert_returning_none(
        &self,
        tenant: &TenantId,
        id: &str,
        patch: serde_json::Value,
    ) -> Result<(), StorageError> {
        self.upsert(tenant, id, patch, None).await.map(|_| ())
    }

    /// True upsert: insert if not exists, update if exists (backward compatibility)
    async fn upsert_or_create(
        &self,
        tenant: &TenantId,
        id: &str,
        patch: serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn get(&self, tenant: &TenantId, id: &str) -> Result<Option<E>, StorageError>;
    async fn select(&self, tenant: &TenantId, params: QueryParams)
        -> Result<Page<E>, StorageError>;
    async fn delete(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError>;
}
