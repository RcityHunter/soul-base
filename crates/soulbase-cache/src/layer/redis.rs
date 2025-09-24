#![cfg(feature = "redis")]

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use super::local_lru::CacheEntry;
use super::remote::RemoteCache;
use crate::config::RedisConfig;
use crate::errors::CacheError;
use crate::key::CacheKey;

#[derive(Clone)]
pub struct RedisBackend {
    manager: ConnectionManager,
    prefix: Arc<String>,
}

impl RedisBackend {
    pub async fn connect(config: RedisConfig) -> Result<Self, CacheError> {
        let client = redis::Client::open(config.url.as_str())
            .map_err(|err| CacheError::provider_unavailable(&format!("redis client: {err}")))?;
        let manager = ConnectionManager::new(client)
            .await
            .map_err(|err| CacheError::provider_unavailable(&format!("redis connect: {err}")))?;
        Ok(Self {
            manager,
            prefix: Arc::new(config.key_prefix),
        })
    }

    fn namespaced_key(&self, key: &CacheKey) -> String {
        format!("{}:{}", self.prefix, key.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireEntry {
    value: Vec<u8>,
    stored_at_ms: i64,
    ttl_ms: i64,
}

impl From<&CacheEntry> for WireEntry {
    fn from(entry: &CacheEntry) -> Self {
        Self {
            value: entry.value.as_ref().to_vec(),
            stored_at_ms: entry.stored_at_ms,
            ttl_ms: entry.ttl_ms,
        }
    }
}

impl From<WireEntry> for CacheEntry {
    fn from(entry: WireEntry) -> Self {
        CacheEntry::with_parts(Bytes::from(entry.value), entry.stored_at_ms, entry.ttl_ms)
    }
}

#[async_trait]
impl RemoteCache for RedisBackend {
    async fn get(&self, key: &CacheKey) -> Result<Option<CacheEntry>, CacheError> {
        let mut conn = self.manager.clone();
        let data: Option<Vec<u8>> = conn
            .get(self.namespaced_key(key))
            .await
            .map_err(|err| CacheError::provider_unavailable(&format!("redis get: {err}")))?;
        if let Some(bytes) = data {
            let entry: WireEntry = serde_json::from_slice(&bytes)
                .map_err(|err| CacheError::schema(&format!("redis payload decode: {err}")))?;
            Ok(Some(entry.into()))
        } else {
            Ok(None)
        }
    }

    async fn set(&self, key: &CacheKey, entry: CacheEntry) -> Result<(), CacheError> {
        let payload = serde_json::to_vec(&WireEntry::from(&entry))
            .map_err(|err| CacheError::schema(&format!("redis payload encode: {err}")))?;
        let mut conn = self.manager.clone();
        let namespaced = self.namespaced_key(key);
        conn.set::<_, _, ()>(namespaced, payload)
            .await
            .map_err(|err| CacheError::provider_unavailable(&format!("redis set: {err}")))?;
        Ok(())
    }

    async fn remove(&self, key: &CacheKey) -> Result<(), CacheError> {
        let mut conn = self.manager.clone();
        let namespaced = self.namespaced_key(key);
        conn.del::<_, ()>(namespaced)
            .await
            .map_err(|err| CacheError::provider_unavailable(&format!("redis del: {err}")))?;
        Ok(())
    }
}
