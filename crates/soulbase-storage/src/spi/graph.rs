use crate::errors::StorageError;
use crate::model::Entity;
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;

#[async_trait]
pub trait GraphStore<E: Entity>: Send + Sync {
    async fn relate(
        &self,
        tenant: &TenantId,
        from: &str,
        label: &str,
        to: &str,
        props: serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn out(
        &self,
        tenant: &TenantId,
        from: &str,
        label: &str,
        limit: usize,
    ) -> Result<Vec<E>, StorageError>;
    async fn detach(
        &self,
        tenant: &TenantId,
        from: &str,
        label: &str,
        to: &str,
    ) -> Result<(), StorageError>;
}
