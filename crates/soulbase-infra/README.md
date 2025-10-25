# soulbase-infra

Infra wiring helpers for Soulbase runtime components. Given a validated
`ConfigSnapshot` (see `soulbase-config`), this crate constructs production-grade
connectors for cache, blob, and queue backends.

## Features

- `redis`: enable Redis remote tier for the cache (`TwoTierCache`).
- `s3`: wire the blob store to AWS S3 via `aws-sdk-s3`.
- `kafka`: provide a Kafka transport implementing `soulbase-tx::outbox::Transport`.

Each builder consumes the `infra` namespace defined in `soulbase-config`:

```rust
use soulbase_config::prelude::*;
use soulbase_infra::{build_cache, build_blob_store, build_queue_transport};

let loader = Loader::builder()
    .add_file_sources(["examples/config/dev.json"])
    .with_registry(bootstrap_catalog().await?.registry)
    .with_validator(validator_as_trait(&bootstrap_catalog().await?.validator))
    .build();

let snapshot = loader.load_once().await?;
let tenant = soulbase_types::tenant::TenantId("acme".into());

let cache = build_cache(&snapshot, &tenant).await?;
let blob = build_blob_store(&snapshot, &tenant).await?;
let queue = build_queue_transport(&snapshot, &tenant).await?;
```

The builders respect `{tenant}` placeholders defined in configuration, ensuring
multi-tenant isolation across prefixes/buckets/topics.
