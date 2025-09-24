# soulbase-cache (RIS)

Unified caching primitives for the Soul platform. Provides:
- Two-tier cache orchestrator with in-memory LRU and optional Redis backend (`redis` feature)
- SingleFlight deduplication, TTL/Jitter handling, SWR (stale-while-revalidate)
- Extensible codec trait (JSON by default), cache policy/admission controls
- Stats that can be wired into `soulbase-observe` meters (`observe` feature)

## Quick Start
```rust
use soulbase_cache::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), CacheError> {
    let cache = TwoTierCache::new(LocalLru::new(1_000), None);
    let key = build_key(KeyParts::new("tenant", "namespace", "payload"));
    let policy = CachePolicy::default();

    let value: String = cache
        .get_or_load(&key, &policy, || async { Ok::<_, CacheError>("value".to_string()) })
        .await?;
    println!("{}", value);
    Ok(())
}
```

To add Redis as the remote tier:
```rust
#[cfg(feature = "redis")]
async fn with_redis() -> Result<(), CacheError> {
    use soulbase_cache::{RedisBackend, RedisConfig, RemoteHandle};

    let backend = RedisBackend::connect(RedisConfig::new("redis://127.0.0.1:6379"))
        .await?;
    let remote: RemoteHandle = Arc::new(backend);
    let cache = TwoTierCache::new(LocalLru::new(1_000), Some(remote));
    // ...
    Ok(())
}
```

## Tests
```bash
cargo test -p soulbase-cache
cargo test -p soulbase-cache --features redis
```
