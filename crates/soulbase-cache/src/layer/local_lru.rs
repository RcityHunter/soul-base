use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use lru::LruCache;
use parking_lot::Mutex;

use crate::key::CacheKey;

#[derive(Clone)]
pub struct LocalLru {
    inner: Arc<Mutex<LruCache<String, CacheEntry>>>,
}

impl LocalLru {
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            inner: Arc::new(Mutex::new(LruCache::new(cap))),
        }
    }

    pub fn get(&self, key: &CacheKey) -> Option<CacheEntry> {
        self.inner.lock().get(key.as_str()).cloned()
    }

    pub fn put(&self, key: &CacheKey, entry: CacheEntry) {
        self.inner.lock().put(key.as_str().to_string(), entry);
    }

    pub fn remove(&self, key: &CacheKey) {
        self.inner.lock().pop(key.as_str());
    }
}

#[derive(Clone, Debug)]
pub struct CacheEntry {
    pub value: Bytes,
    pub stored_at_ms: i64,
    pub ttl_ms: i64,
}

impl CacheEntry {
    pub fn with_parts(value: Bytes, stored_at_ms: i64, ttl_ms: i64) -> Self {
        Self {
            value,
            stored_at_ms,
            ttl_ms,
        }
    }
    pub fn new(value: Bytes, ttl_ms: i64) -> Self {
        Self {
            value,
            stored_at_ms: Utc::now().timestamp_millis(),
            ttl_ms,
        }
    }

    pub fn is_fresh(&self, now_ms: i64) -> bool {
        now_ms - self.stored_at_ms <= self.ttl_ms
    }

    pub fn age(&self, now_ms: i64) -> i64 {
        now_ms - self.stored_at_ms
    }
}
impl LocalLru {
    pub fn remove_prefix(&self, prefix: &str) {
        let mut guard = self.inner.lock();
        let keys: Vec<String> = guard.iter().map(|(k, _)| k.clone()).collect();
        for key in keys {
            if key.starts_with(prefix) {
                guard.pop(&key);
            }
        }
    }
}
