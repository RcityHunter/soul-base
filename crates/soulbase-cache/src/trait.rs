use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::errors::CacheError;
use crate::key::CacheKey;
use crate::policy::CachePolicy;

#[async_trait]
pub trait Cache: Send + Sync {
    async fn get<T>(&self, key: &CacheKey) -> Result<Option<T>, CacheError>
    where
        T: DeserializeOwned + Send + Sync;

    async fn get_or_load<T, F, Fut>(
        &self,
        key: &CacheKey,
        policy: &CachePolicy,
        loader: F,
    ) -> Result<T, CacheError>
    where
        T: DeserializeOwned + Serialize + Send + Sync + 'static,
        F: Send + Sync + Fn() -> Fut,
        Fut: Send + 'static + std::future::Future<Output = Result<T, CacheError>>;

    async fn invalidate(&self, key: &CacheKey) -> Result<(), CacheError>;
}

#[async_trait]
pub trait Invalidation: Send + Sync {
    async fn invalidate(&self, key: &CacheKey) -> Result<(), CacheError>;
    async fn invalidate_prefix(&self, prefix: &str) -> Result<(), CacheError>;
}

#[async_trait]
pub trait SingleFlight: Send + Sync {
    type Guard: Send;
    async fn acquire(&self, key: &CacheKey) -> Self::Guard;
}

pub trait Stats: Send + Sync {
    fn record_hit(&self);
    fn record_miss(&self);
    fn record_load(&self);
    fn record_error(&self);
}
