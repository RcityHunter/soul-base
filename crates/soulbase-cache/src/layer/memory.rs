use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use super::local_lru::CacheEntry;
use super::remote::RemoteCache;
use crate::errors::CacheError;
use crate::key::CacheKey;

#[derive(Clone, Debug, Default)]
pub struct MemoryBackend {
    inner: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl RemoteCache for MemoryBackend {
    async fn get(&self, key: &CacheKey) -> Result<Option<CacheEntry>, CacheError> {
        Ok(self.inner.lock().get(key.as_str()).cloned())
    }

    async fn set(&self, key: &CacheKey, entry: CacheEntry) -> Result<(), CacheError> {
        self.inner.lock().insert(key.as_str().to_string(), entry);
        Ok(())
    }

    async fn remove(&self, key: &CacheKey) -> Result<(), CacheError> {
        self.inner.lock().remove(key.as_str());
        Ok(())
    }
}
