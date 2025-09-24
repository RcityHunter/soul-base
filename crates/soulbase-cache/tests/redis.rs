#![cfg(feature = "redis")]

use std::sync::Arc;

use soulbase_cache::prelude::*;
use soulbase_cache::{RedisBackend, RedisConfig};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

fn key_of(seed: &str) -> CacheKey {
    build_key(KeyParts::new(
        "tenant",
        format!("ns:{seed}"),
        format!("hash:{seed}"),
    ))
}

#[tokio::test]
async fn redis_backend_roundtrip() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let _ = mini_redis::server::run(listener, async {
            let _ = shutdown_rx.await;
        })
        .await;
    });

    let config = RedisConfig::new(format!("redis://{}", addr));
    let backend = RedisBackend::connect(config).await.expect("connect redis");
    let remote: RemoteHandle = Arc::new(backend);

    let cache = TwoTierCache::new(LocalLru::new(16), Some(remote));
    let key = key_of("redis-basic");
    let policy = CachePolicy::default();

    let value = cache
        .get_or_load(&key, &policy, || async {
            Ok::<_, CacheError>("ok".to_string())
        })
        .await
        .unwrap();
    assert_eq!(value, "ok");

    cache.local.remove(&key);
    let cached: Option<String> = cache.get(&key).await.unwrap();
    assert_eq!(cached.unwrap(), "ok");

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
