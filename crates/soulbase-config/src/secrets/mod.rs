use crate::errors::ConfigError;
use async_trait::async_trait;
use chrono::Utc;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::Duration;

#[async_trait]
pub trait SecretResolver: Send + Sync {
    fn id(&self) -> &'static str;
    async fn resolve(&self, uri: &str) -> Result<serde_json::Value, ConfigError>;
}

pub struct NoopSecretResolver;

#[async_trait]
impl SecretResolver for NoopSecretResolver {
    fn id(&self) -> &'static str {
        "noop"
    }

    async fn resolve(&self, uri: &str) -> Result<serde_json::Value, ConfigError> {
        Ok(serde_json::Value::String(uri.to_string()))
    }
}

pub fn is_secret_ref(value: &serde_json::Value) -> Option<&str> {
    value.as_str().and_then(|s| s.strip_prefix("secret://"))
}

#[derive(Clone)]
struct CacheEntry {
    value: serde_json::Value,
    expires_at_ms: Option<i64>,
}

impl CacheEntry {
    fn is_valid(&self, now_ms: i64) -> bool {
        self.expires_at_ms.map_or(true, |exp| now_ms < exp)
    }
}

/// Wraps a resolver and caches resolved values with an optional TTL.
pub struct CachingResolver<R> {
    inner: R,
    ttl_ms: Option<i64>,
    cache: Mutex<HashMap<String, CacheEntry>>,
}

impl<R> CachingResolver<R> {
    pub fn new(inner: R, ttl: Option<Duration>) -> Self {
        Self {
            inner,
            ttl_ms: ttl.and_then(duration_to_i64_ms),
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn invalidate(&self, uri: &str) {
        self.cache.lock().remove(uri);
    }

    pub fn clear(&self) {
        self.cache.lock().clear();
    }

    fn cache_lookup(&self, uri: &str) -> Option<serde_json::Value> {
        let now = Utc::now().timestamp_millis();
        let mut guard = self.cache.lock();
        match guard.get(uri) {
            Some(entry) if entry.is_valid(now) => Some(entry.value.clone()),
            Some(_) => {
                guard.remove(uri);
                None
            }
            None => None,
        }
    }

    fn cache_store(&self, uri: &str, value: serde_json::Value) {
        let expires_at_ms = self
            .ttl_ms
            .map(|ttl| Utc::now().timestamp_millis().saturating_add(ttl));
        self.cache.lock().insert(
            uri.to_string(),
            CacheEntry {
                value,
                expires_at_ms,
            },
        );
    }
}

fn duration_to_i64_ms(duration: Duration) -> Option<i64> {
    let millis = duration.as_millis();
    if millis > i64::MAX as u128 {
        Some(i64::MAX)
    } else {
        Some(millis as i64)
    }
}

#[async_trait]
impl<R> SecretResolver for CachingResolver<R>
where
    R: SecretResolver + Send + Sync,
{
    fn id(&self) -> &'static str {
        self.inner.id()
    }

    async fn resolve(&self, uri: &str) -> Result<serde_json::Value, ConfigError> {
        if let Some(value) = self.cache_lookup(uri) {
            return Ok(value);
        }

        let resolved = self.inner.resolve(uri).await?;
        self.cache_store(uri, resolved.clone());
        Ok(resolved)
    }
}
