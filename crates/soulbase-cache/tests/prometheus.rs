#![cfg(feature = "observe-prometheus")]

use soulbase_cache::prelude::*;
use soulbase_observe::sdk::PrometheusHub;

#[tokio::test]
async fn prometheus_metrics_capture_cache_activity() {
    let hub = PrometheusHub::new();
    let cache = TwoTierCache::new(LocalLru::new(32), None).with_prometheus_hub(&hub);
    let key = build_key(KeyParts::new("tenant", "hit", "k"));
    let policy = CachePolicy::default();

    let value = cache
        .get_or_load(&key, &policy, || async {
            Ok::<_, CacheError>("value".to_string())
        })
        .await
        .unwrap();
    assert_eq!(value, "value");

    let cached: Option<String> = cache.get(&key).await.unwrap();
    assert_eq!(cached.unwrap(), "value");

    let metrics = hub.gather().expect("prometheus scrape");
    assert!(metrics.contains("soulbase_cache_miss_total"));
    assert!(metrics.contains("soulbase_cache_hit_total"));
}
