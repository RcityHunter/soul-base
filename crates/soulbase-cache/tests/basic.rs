use soulbase_cache::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::{sleep, Duration};

fn key_of(seed: &str) -> CacheKey {
    build_key(KeyParts::new(
        "tenant",
        format!("ns:{seed}"),
        format!("hash:{seed}"),
    ))
}

#[tokio::test]
async fn singleflight_ensures_single_loader() {
    static CNT: AtomicUsize = AtomicUsize::new(0);
    let cache = TwoTierCache::new(LocalLru::new(256), None);
    let key = key_of("singleflight");
    let policy = CachePolicy::default();

    let mut tasks = Vec::new();
    for _ in 0..50 {
        let cache = cache.clone();
        let key = key.clone();
        let policy = policy.clone();
        tasks.push(tokio::spawn(async move {
            cache
                .get_or_load(&key, &policy, || async {
                    if CNT.fetch_add(1, Ordering::SeqCst) == 0 {
                        sleep(Duration::from_millis(30)).await;
                    }
                    Ok::<_, CacheError>("ok".to_string())
                })
                .await
        }));
    }

    for task in tasks {
        let res = task.await.unwrap().unwrap();
        assert_eq!(res, "ok");
    }
    assert_eq!(CNT.load(Ordering::SeqCst), 1, "loader should run only once");
}

#[tokio::test]
async fn hit_after_first_miss() {
    let cache = TwoTierCache::new(LocalLru::new(128), None);
    let key = key_of("hit-miss");
    let policy = CachePolicy::default();

    let value = cache
        .get_or_load(&key, &policy, || async {
            Ok::<_, CacheError>("v1".to_string())
        })
        .await
        .unwrap();
    assert_eq!(value, "v1");

    let cached: Option<String> = cache.get(&key).await.unwrap();
    assert_eq!(cached.unwrap(), "v1");
}

#[tokio::test]
async fn swr_returns_stale_and_refreshes() {
    let cache = TwoTierCache::new(LocalLru::new(128), None);
    let key = key_of("swr");
    let policy = CachePolicy {
        ttl_ms: 80,
        swr: Some(SwrPolicy {
            enable: true,
            stale_ms: 200,
            refresh_concurrency: 2,
        }),
        ..CachePolicy::default()
    };

    static REV: AtomicUsize = AtomicUsize::new(0);

    let first = cache
        .get_or_load(&key, &policy, || async {
            REV.store(1, Ordering::SeqCst);
            Ok::<_, CacheError>("v1".to_string())
        })
        .await
        .unwrap();
    assert_eq!(first, "v1");

    sleep(Duration::from_millis(120)).await;

    let second = cache
        .get_or_load(&key, &policy, || async {
            let _ = REV.swap(2, Ordering::SeqCst);
            Ok::<_, CacheError>("v2".to_string())
        })
        .await
        .unwrap();
    assert_eq!(second, "v1", "stale value should be returned immediately");

    sleep(Duration::from_millis(120)).await;

    let third = cache
        .get_or_load(&key, &policy, || async {
            Ok::<_, CacheError>("v2".to_string())
        })
        .await
        .unwrap();
    assert_eq!(third, "v2");
}
