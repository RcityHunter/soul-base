use serde_json::json;
use soulbase_config::access;
use soulbase_config::prelude::*;
use soulbase_config::source::memory::MemorySource;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_bootstrap_and_boot_only_guard() {
    let handles = bootstrap_catalog().await.expect("bootstrap catalog");
    let namespaces = handles.registry.list_namespaces().await;
    assert!(
        namespaces.len() >= 4,
        "expected core namespaces to be registered"
    );

    let memory = Arc::new(MemorySource::new("memory"));
    let seed = json!({
        "llm": {
            "default_provider": "openai",
            "providers": {
                "openai": {
                    "endpoint": "https://api.openai.com",
                    "models": ["gpt-4o"],
                    "auth_secret": "secret://env/OPENAI_KEY",
                    "telemetry_bucket": "llm-openai"
                }
            },
            "budgets": [{
                "id": "default",
                "daily_usd": 100.0,
                "monthly_usd": 1000.0,
                "window_seconds": 86400
            }],
            "allow_tool_plan": true
        },
        "net": {
            "retry": {
                "max_attempts": 4,
                "backoff_ms": 200,
                "jitter": true
            },
            "rate_limit": {
                "tokens_per_second": 25,
                "burst": 100
            },
            "request_signer_secret": "secret://env/NET_SIGNER",
            "allow_insecure": false
        },
        "observe": {
            "tracing": {
                "sample_ratio": 0.5,
                "exporter": "otlp://collector:4317"
            },
            "metrics": {
                "prometheus_endpoint": "0.0.0.0:9000",
                "retention_days": 7
            },
            "audit_topic": "soulbase.audit"
        },
        "catalog": {
            "snapshot_dir": "var/config/snapshots",
            "changelog_file": "var/config/changelog.json",
            "signer_key": "secret://env/CONFIG_SIGNER"
        },
        "infra": {
            "cache": {
                "local_capacity": 512,
                "redis": {
                    "url": "redis://127.0.0.1:6379",
                    "key_prefix": "soulbase:{tenant}:cache"
                }
            },
            "blob": {
                "backend": "fs",
                "fs": {
                    "root": "var/blob",
                    "bucket": "soulbase-dev",
                    "presign_secret": "dev-secret",
                    "key_prefix": "tenants/{tenant}"
                }
            },
            "queue": {
                "kind": "kafka",
                "kafka": {
                    "brokers": ["localhost:9092"],
                    "topic_prefix": "soulbase.dev.{tenant}",
                    "linger_ms": 25,
                    "acks": "all"
                }
            }
        }
    });

    if let Some(map) = seed.as_object() {
        memory.merge(map.clone());
    }

    let loader = Loader::builder()
        .with_registry(handles.registry.clone())
        .with_validator(validator_as_trait(&handles.validator))
        .add_source(memory.clone() as Arc<dyn Source>)
        .build();

    let snapshot = loader.load_once().await.expect("snapshot");
    assert_eq!(
        snapshot
            .get::<String>(&KeyPath::new("llm.default_provider"))
            .unwrap(),
        "openai"
    );

    let mut updated = snapshot.tree().clone();
    if let Some(obj) = updated.as_object_mut() {
        access::set_path(obj, "net.allow_insecure", serde_json::Value::Bool(true));
    }

    let err = handles
        .validator
        .validate_delta(snapshot.tree(), &updated)
        .await
        .expect_err("boot-only change rejected");
    let dev_msg = err.into_inner().message_dev.expect("dev message");
    assert!(dev_msg.contains("net.allow_insecure"));

    let mut queue_update = snapshot.tree().clone();
    if let Some(obj) = queue_update.as_object_mut() {
        access::set_path(
            obj,
            "infra.queue.kafka.topic_prefix",
            serde_json::Value::String("override".into()),
        );
    }

    let err = handles
        .validator
        .validate_delta(snapshot.tree(), &queue_update)
        .await
        .expect_err("queue topic change requires restart");
    let dev_msg = err.into_inner().message_dev.expect("dev message");
    assert!(dev_msg.contains("infra.queue.kafka.topic_prefix"));
}
