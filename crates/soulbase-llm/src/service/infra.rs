use std::collections::HashMap;
use std::sync::Arc;

use soulbase_infra::{
    build_blob_store_for, build_cache_for, build_queue_for, BlobHandle, CacheHandle, InfraError,
    InfraNamespace, QueueHandle,
};
use soulbase_types::tenant::TenantId;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct InfraManager {
    cfg: InfraNamespace,
    caches: Arc<RwLock<HashMap<String, CacheHandle>>>,
    blobs: Arc<RwLock<HashMap<String, BlobHandle>>>,
    queues: Arc<RwLock<HashMap<String, QueueHandle>>>,
}

impl InfraManager {
    pub fn new(cfg: InfraNamespace) -> Self {
        Self {
            cfg,
            caches: Arc::new(RwLock::new(HashMap::new())),
            blobs: Arc::new(RwLock::new(HashMap::new())),
            queues: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn cache_for(&self, tenant: &TenantId) -> Result<CacheHandle, InfraError> {
        if let Some(existing) = self
            .caches
            .read()
            .await
            .get(&tenant.0)
            .map(|handle| handle.clone())
        {
            return Ok(existing);
        }

        let cfg = self.cfg.cache.clone().unwrap_or_default();
        let handle = build_cache_for(cfg, tenant).await?;
        self.caches
            .write()
            .await
            .insert(tenant.0.clone(), handle.clone());
        Ok(handle)
    }

    pub async fn blob_for(&self, tenant: &TenantId) -> Result<BlobHandle, InfraError> {
        if let Some(existing) = self
            .blobs
            .read()
            .await
            .get(&tenant.0)
            .map(|handle| handle.clone())
        {
            return Ok(existing);
        }

        let cfg = self.cfg.blob.clone().unwrap_or_default();
        let handle = build_blob_store_for(cfg, tenant).await?;
        self.blobs
            .write()
            .await
            .insert(tenant.0.clone(), handle.clone());
        Ok(handle)
    }

    pub async fn queue_for(&self, tenant: &TenantId) -> Result<QueueHandle, InfraError> {
        if let Some(existing) = self
            .queues
            .read()
            .await
            .get(&tenant.0)
            .map(|handle| handle.clone())
        {
            return Ok(existing);
        }

        let cfg = self.cfg.queue.clone().unwrap_or_default();
        let handle = build_queue_for(cfg, tenant).await?;
        self.queues
            .write()
            .await
            .insert(tenant.0.clone(), handle.clone());
        Ok(handle)
    }
}
