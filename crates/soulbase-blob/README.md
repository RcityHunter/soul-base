# soulbase-blob

Lightweight object storage facade for the Soul platform. The RIS implementation now ships with:

- unified models (`BlobRef`, `BlobMeta`, `Digest`, presign + multipart placeholders)
- trait-based abstraction (`BlobStore`, `RetentionExec`) with a production-ready FS adapter and optional S3 adapter (`backend-s3` feature)
- metrics surface via `BlobStats` (atomic counters by default, `observe` feature hooks into `soulbase-observe` counters)
- development FS adapter with atomic writes, SHA-256 ETag, HMAC presign URLs
- retention executor that removes stale `{tenant}/{namespace}/бн` prefixes by TTL

## Features

- `backend-s3`: enables the `aws-sdk-s3` powered adapter with retry-friendly helpers
- `observe`: integrates blob operation counters into the Soul observability pipeline
- `s3-test-suite`: opt-in integration tests against a live S3-compatible endpoint (requires `AWS_S3_TEST_BUCKET`)

## Quick start
```rust
use soulbase_blob::prelude::*;
use bytes::Bytes;

let store = FsBlobStore::new("/var/lib/soul/blob", "dev-secret");
let put = store
    .put(
        "artifacts",
        "tenantA/reports/202501/01/u1.json",
        Bytes::from_static(br"{}"),
        PutOpts::default(),
    )
    .await?;
let stats = store.metrics().snapshot();
println!("puts={} gets={}", stats.puts, stats.gets);
let url = store
    .presign_get("artifacts", &put.key, PresignGetOpts { expire_secs: 60 })
    .await?;
println!("presign = {url}");
```

## Next steps
- enable `backend-s3` when credentials/runtime are ready; extend `S3BlobStore` with policy-driven SSE/KMS
- tighten tenant/namespace validation & presign constraints before production rollout
- wire `observe` / `qos` features into billing and quota accounting once downstream pipelines are in place
