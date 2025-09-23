use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use tokio::task;

use crate::codec::{Codec, JsonCodec};
use crate::errors::CacheError;
use crate::key::CacheKey;
use crate::metrics::SimpleStats;
use crate::policy::CachePolicy;
use crate::r#trait::{Cache, Invalidation, SingleFlight, Stats};

use super::local_lru::{CacheEntry as LocalEntry, LocalLru};
use super::redis::RedisStub;
use super::singleflight::Flight;
use super::swr;

#[derive(Clone)]
pub struct TwoTierCache<C = JsonCodec>
where
    C: Codec + Clone,
{
    pub local: LocalLru,
    pub redis: Option<RedisStub>,
    pub codec: C,
    pub flight: Flight,
    pub stats: SimpleStats,
    pub default_policy: CachePolicy,
}

impl TwoTierCache<JsonCodec> {
    pub fn new(local: LocalLru, redis: Option<RedisStub>) -> Self {
        Self {
            local,
            redis,
            codec: JsonCodec,
            flight: Flight::default(),
            stats: SimpleStats::default(),
            default_policy: CachePolicy::default(),
        }
    }
}

impl<C> TwoTierCache<C>
where
    C: Codec + Clone,
{
    pub fn with_codec(local: LocalLru, redis: Option<RedisStub>, codec: C) -> Self {
        Self {
            local,
            redis,
            codec,
            flight: Flight::default(),
            stats: SimpleStats::default(),
            default_policy: CachePolicy::default(),
        }
    }

    async fn fetch_entry(&self, key: &CacheKey) -> Option<LocalEntry> {
        if let Some(entry) = self.local.get(key) {
            return Some(entry);
        }
        if let Some(redis) = &self.redis {
            if let Ok(Some(entry)) = redis.get(key).await {
                self.local.put(key, entry.clone());
                return Some(entry);
            }
        }
        None
    }

    async fn store_entry(
        &self,
        key: &CacheKey,
        bytes: Bytes,
        ttl_ms: i64,
    ) -> Result<(), CacheError> {
        let entry = LocalEntry::new(bytes.clone(), ttl_ms);
        self.local.put(key, entry.clone());
        if let Some(redis) = &self.redis {
            redis.set(key, entry).await?;
        }
        Ok(())
    }

    fn decode<T>(&self, bytes: &Bytes) -> Result<T, CacheError>
    where
        T: serde::de::DeserializeOwned + Send + Sync,
        C: Codec,
    {
        self.codec.decode(bytes)
    }

    fn encode<T>(&self, value: &T) -> Result<Bytes, CacheError>
    where
        T: serde::Serialize + Send + Sync,
        C: Codec,
    {
        self.codec.encode(value)
    }
}

#[async_trait]
impl<C> Cache for TwoTierCache<C>
where
    C: Codec + Clone + Send + Sync + 'static,
{
    async fn get<T>(&self, key: &CacheKey) -> Result<Option<T>, CacheError>
    where
        T: serde::de::DeserializeOwned + Send + Sync,
    {
        let now = Utc::now().timestamp_millis();
        if let Some(entry) = self.fetch_entry(key).await {
            if entry.is_fresh(now) {
                self.stats.record_hit();
                return self.decode(&entry.value).map(Some);
            }
        }
        self.stats.record_miss();
        Ok(None)
    }

    async fn get_or_load<T, F, Fut>(
        &self,
        key: &CacheKey,
        policy: &CachePolicy,
        loader: F,
    ) -> Result<T, CacheError>
    where
        T: serde::de::DeserializeOwned + serde::Serialize + Send + Sync + 'static,
        F: Send + Sync + Fn() -> Fut,
        Fut: Send + 'static + std::future::Future<Output = Result<T, CacheError>>,
    {
        let now = Utc::now().timestamp_millis();
        if let Some(entry) = self.fetch_entry(key).await {
            if entry.is_fresh(now) {
                self.stats.record_hit();
                return self.decode(&entry.value);
            }
            if let Some(swr_policy) = policy.swr.as_ref() {
                if swr::within_swr(now, entry.stored_at_ms, entry.ttl_ms, swr_policy) {
                    self.stats.record_hit();
                    let stale: T = self.decode(&entry.value)?;
                    let cache_clone = self.clone();
                    let key_clone = key.clone();
                    let policy_clone = policy.clone();
                    let future = loader();
                    task::spawn(async move {
                        let guard = cache_clone.flight.acquire(&key_clone).await;
                        match future.await {
                            Ok(value) => {
                                if let Ok(bytes) = cache_clone.encode(&value) {
                                    let ttl = policy_clone.effective_ttl_ms();
                                    let _ = cache_clone.store_entry(&key_clone, bytes, ttl).await;
                                }
                            }
                            Err(err) => {
                                cache_clone.stats.record_error();
                                tracing::debug!(
                                    target = "soulbase::cache",
                                    "SWR refresh error: {err:?}"
                                );
                            }
                        }
                        drop(guard);
                    });
                    return Ok(stale);
                }
            }
        }

        self.stats.record_miss();
        let guard = self.flight.acquire(key).await;
        let now = Utc::now().timestamp_millis();
        if let Some(entry) = self.fetch_entry(key).await {
            if entry.is_fresh(now) {
                drop(guard);
                self.stats.record_hit();
                return self.decode(&entry.value);
            }
        }

        self.stats.record_load();
        let value = loader().await.inspect_err(|err| {
            self.stats.record_error();
            tracing::debug!(target = "soulbase::cache", "loader error: {err:?}");
        })?;
        let bytes = self.encode(&value)?;
        let ttl = policy.effective_ttl_ms();
        self.store_entry(key, bytes, ttl).await?;
        drop(guard);
        Ok(value)
    }

    async fn invalidate(&self, key: &CacheKey) -> Result<(), CacheError> {
        self.local.remove(key);
        if let Some(redis) = &self.redis {
            redis.remove(key).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<C> Invalidation for TwoTierCache<C>
where
    C: Codec + Clone + Send + Sync + 'static,
{
    async fn invalidate(&self, key: &CacheKey) -> Result<(), CacheError> {
        Cache::invalidate(self, key).await
    }

    async fn invalidate_prefix(&self, prefix: &str) -> Result<(), CacheError> {
        self.local.remove_prefix(prefix);
        Ok(())
    }
}

impl<C> Stats for TwoTierCache<C>
where
    C: Codec + Clone + Send + Sync + 'static,
{
    fn record_hit(&self) {
        self.stats.record_hit();
    }

    fn record_miss(&self) {
        self.stats.record_miss();
    }

    fn record_load(&self) {
        self.stats.record_load();
    }

    fn record_error(&self) {
        self.stats.record_error();
    }
}
