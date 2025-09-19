use crate::errors::StorageError;
use crate::model::Entity;
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;

#[async_trait]
pub trait VectorIndex<E: Entity>: Send + Sync {
    async fn upsert_vec(
        &self,
        tenant: &TenantId,
        id: &str,
        vector: &[f32],
    ) -> Result<(), StorageError>;
    async fn delete_vec(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError>;
    async fn knn(
        &self,
        tenant: &TenantId,
        vector: &[f32],
        k: usize,
        filter: Option<serde_json::Value>,
    ) -> Result<Vec<(E, f32)>, StorageError>;
}
