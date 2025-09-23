# soulbase-cache (RIS)

Unified caching primitives for the Soul platform. Provides:
- Two-tier cache orchestrator with in-memory LRU and optional Redis stub
- SingleFlight deduplication, TTL/Jitter handling, SWR (stale-while-revalidate)
- Extensible codec trait (JSON by default) and cache policy controls
- Basic stats counters ready to feed into `soulbase-observe`

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

## Tests
```bash
cargo test -p soulbase-cache
```
