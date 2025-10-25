use soulbase_cache::layer::local_lru::LocalLru;
use soulbase_cache::policy::CachePolicy;
use soulbase_cache::TwoTierCache;
use soulbase_config::prelude::ConfigSnapshot;
use soulbase_types::tenant::TenantId;

use crate::config::{self, CacheInfraConfig};
use crate::errors::InfraError;

#[cfg(feature = "redis")]
use crate::config::RedisCacheConfig;
#[cfg(feature = "redis")]
use soulbase_cache::config::RedisConfig;
#[cfg(feature = "redis")]
use soulbase_cache::layer::remote::RemoteHandle;
#[cfg(feature = "redis")]
use soulbase_cache::RedisBackend;
#[cfg(feature = "redis")]
use std::sync::Arc;

pub type CacheHandle = TwoTierCache;

pub async fn build_cache(
    snapshot: &ConfigSnapshot,
    tenant: &TenantId,
) -> Result<CacheHandle, InfraError> {
    let infra = config::load(snapshot)?;
    build_cache_for(infra.cache.unwrap_or_default(), tenant).await
}

pub async fn build_cache_for(
    cache_cfg: CacheInfraConfig,
    tenant: &TenantId,
) -> Result<CacheHandle, InfraError> {
    #[cfg(not(feature = "redis"))]
    let _ = tenant;

    let capacity = cache_cfg.local_capacity.max(1) as usize;
    let local = LocalLru::new(capacity);

    #[cfg(feature = "redis")]
    let remote = if let Some(redis_cfg) = cache_cfg.redis.clone() {
        Some(connect_redis(redis_cfg, tenant).await?)
    } else {
        None
    };

    #[cfg(not(feature = "redis"))]
    let remote = {
        if cache_cfg.redis.is_some() {
            return Err(InfraError::feature_disabled(
                "redis",
                "enable the 'redis' feature on soulbase-infra to use Redis cache",
            ));
        }
        None
    };

    let mut cache = TwoTierCache::new(local, remote);
    if let Some(ttl_seconds) = cache_cfg.default_ttl_seconds {
        cache.default_policy = CachePolicy {
            ttl_ms: (ttl_seconds as i64).saturating_mul(1_000),
            ..cache.default_policy
        };
    }
    Ok(cache)
}

#[cfg(feature = "redis")]
async fn connect_redis(
    redis_cfg: RedisCacheConfig,
    tenant: &TenantId,
) -> Result<RemoteHandle, InfraError> {
    let prefix = config::tenant_replace(&redis_cfg.key_prefix, tenant);
    let remote_cfg = RedisConfig::new(redis_cfg.url).with_prefix(prefix);
    let backend = RedisBackend::connect(remote_cfg)
        .await
        .map_err(InfraError::from)?;
    Ok(Arc::new(backend) as RemoteHandle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use soulbase_config::model::ProvenanceEntry;
    use soulbase_config::prelude::{ConfigSnapshot, SnapshotVersion};
    use soulbase_types::tenant::TenantId;

    fn dummy_snapshot() -> ConfigSnapshot {
        let tree = serde_json::json!({
            "infra": {
                "cache": {
                    "local_capacity": 64,
                    "default_ttl_seconds": 45
                }
            }
        });
        ConfigSnapshot::from_tree(
            tree,
            SnapshotVersion("test".into()),
            Vec::<ProvenanceEntry>::new(),
            None,
        )
    }

    #[tokio::test]
    async fn builds_memory_cache_when_redis_missing() {
        let snapshot = dummy_snapshot();
        let tenant = TenantId("tenant-a".into());
        let cache = build_cache(&snapshot, &tenant).await.expect("cache");
        assert!(cache.remote.is_none());
        assert_eq!(cache.default_policy.ttl_ms, 45_000);
    }
}
