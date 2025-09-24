use async_trait::async_trait;

use crate::errors::CacheError;
use crate::key::CacheKey;
use crate::layer::local_lru::CacheEntry;

#[async_trait]
pub trait RemoteCache: Send + Sync {
    async fn get(&self, key: &CacheKey) -> Result<Option<CacheEntry>, CacheError>;
    async fn set(&self, key: &CacheKey, entry: CacheEntry) -> Result<(), CacheError>;
    async fn remove(&self, key: &CacheKey) -> Result<(), CacheError>;
}

pub type RemoteHandle = std::sync::Arc<dyn RemoteCache + 'static>;
