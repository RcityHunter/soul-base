use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use super::local_lru::CacheEntry;
use crate::errors::CacheError;
use crate::key::CacheKey;

#[derive(Clone, Debug, Default)]
pub struct RedisStub {
    inner: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

impl RedisStub {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get(&self, key: &CacheKey) -> Result<Option<CacheEntry>, CacheError> {
        Ok(self.inner.lock().get(key.as_str()).cloned())
    }

    pub async fn set(&self, key: &CacheKey, entry: CacheEntry) -> Result<(), CacheError> {
        self.inner.lock().insert(key.as_str().to_string(), entry);
        Ok(())
    }

    pub async fn remove(&self, key: &CacheKey) -> Result<(), CacheError> {
        self.inner.lock().remove(key.as_str());
        Ok(())
    }
}
