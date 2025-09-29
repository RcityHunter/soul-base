use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use parking_lot::Mutex;
use soulbase_config::access;
use soulbase_config::prelude::*;
use soulbase_config::source::memory::MemorySource;
use soulbase_config::source::remote::RemoteSource;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sources_emit_changes_and_loader_hot_swaps() {
    let memory = Arc::new(MemorySource::new("memory"));
    memory.set("app.name", serde_json::Value::String("Soulseed".into()));

    let remote_state = Arc::new(Mutex::new(serde_json::Map::new()));
    {
        let mut guard = remote_state.lock();
        access::set_path(
            &mut *guard,
            "app.version",
            serde_json::Value::String("1".into()),
        );
    }
    let remote_state_clone = remote_state.clone();
    let remote = Arc::new(
        RemoteSource::new("remote", move || {
            let state = remote_state_clone.clone();
            async move { Ok(state.lock().clone()) }.boxed()
        })
        .with_interval(Duration::from_millis(50)),
    );

    let loader = Arc::new(Loader {
        sources: vec![
            memory.clone() as Arc<dyn Source>,
            remote.clone() as Arc<dyn Source>,
        ],
        secrets: vec![Arc::new(NoopSecretResolver) as Arc<dyn SecretResolver>],
        validator: Arc::new(BasicValidator),
        registry: Arc::new(InMemorySchemaRegistry::new()),
    });

    let (switch, guard) = loader.clone().load_and_watch().await.expect("watch");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let snapshot = switch.get();
    let name: String = snapshot
        .get(&KeyPath("app.name".into()))
        .expect("initial memory value");
    assert_eq!(name, "Soulseed");
    let version: String = snapshot
        .get(&KeyPath("app.version".into()))
        .expect("initial remote value");
    assert_eq!(version, "1");

    memory.set(
        "app.name",
        serde_json::Value::String("Soulseed-Next".into()),
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    let snapshot = switch.get();
    let name: String = snapshot
        .get(&KeyPath("app.name".into()))
        .expect("updated memory value");
    assert_eq!(name, "Soulseed-Next");

    {
        let mut guard = remote_state.lock();
        access::set_path(
            &mut *guard,
            "app.version",
            serde_json::Value::String("2".into()),
        );
    }
    tokio::time::sleep(Duration::from_millis(120)).await;
    let snapshot = switch.get();
    let version: String = snapshot
        .get(&KeyPath("app.version".into()))
        .expect("updated remote value");
    assert_eq!(version, "2");

    guard.shutdown().await;
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct AppSettingsSchema {
    name: String,
    version: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn boot_only_fields_block_hot_reload() {
    let memory = Arc::new(MemorySource::new("memory"));
    memory.set("app.name", serde_json::Value::String("Soulseed".into()));
    memory.set("app.version", serde_json::Value::String("1".into()));

    let registry = Arc::new(InMemorySchemaRegistry::new());
    let mut meta = HashMap::new();
    meta.insert(
        KeyPath::new("app.name"),
        FieldMeta::new(ReloadClass::BootOnly).with_description("App display name"),
    );
    meta.insert(
        KeyPath::new("app.version"),
        FieldMeta::new(ReloadClass::HotReloadSafe).with_description("Version"),
    );

    registry
        .register_namespace(
            &NamespaceId("app".into()),
            schemars::schema_for!(AppSettingsSchema),
            meta,
        )
        .await
        .expect("register schema");

    let validator = Arc::new(SchemaValidator::new(registry.clone())) as Arc<dyn Validator>;
    let loader = Arc::new(Loader {
        sources: vec![memory.clone() as Arc<dyn Source>],
        secrets: vec![Arc::new(NoopSecretResolver) as Arc<dyn SecretResolver>],
        validator,
        registry,
    });

    let (switch, guard) = loader.clone().load_and_watch().await.expect("watch");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let snapshot = switch.get();
    let initial_name: String = snapshot
        .get(&KeyPath("app.name".into()))
        .expect("initial name");
    assert_eq!(initial_name, "Soulseed");

    memory.set("app.name", serde_json::Value::String("Should-Block".into()));
    tokio::time::sleep(Duration::from_millis(120)).await;
    let snapshot = switch.get();
    let after_boot_only: String = snapshot
        .get(&KeyPath("app.name".into()))
        .expect("boot-only unchanged");
    assert_eq!(after_boot_only, "Soulseed");

    memory.set("app.name", serde_json::Value::String("Soulseed".into()));
    tokio::time::sleep(Duration::from_millis(120)).await;

    memory.set("app.version", serde_json::Value::String("2".into()));
    tokio::time::sleep(Duration::from_millis(120)).await;
    let snapshot = switch.get();
    let version: String = snapshot
        .get(&KeyPath("app.version".into()))
        .expect("version update");
    assert_eq!(version, "2");

    guard.shutdown().await;
}
