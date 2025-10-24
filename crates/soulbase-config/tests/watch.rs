use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use futures::FutureExt;
use parking_lot::Mutex;
use soulbase_config::access;
use soulbase_config::errors;
use soulbase_config::prelude::*;
use soulbase_config::source::memory::MemorySource;
use soulbase_config::source::remote::RemoteSource;

struct ToggleSecretResolver {
    enabled: Arc<AtomicBool>,
    hits: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl SecretResolver for ToggleSecretResolver {
    fn id(&self) -> &'static str {
        "toggle"
    }

    async fn resolve(&self, _uri: &str) -> Result<serde_json::Value, ConfigError> {
        if self.enabled.load(Ordering::SeqCst) {
            let next = self.hits.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(serde_json::Value::String(format!("resolved-{next}")))
        } else {
            Err(ConfigError::from(errors::io_provider_unavailable(
                "secret.toggle",
                "resolver disabled",
            )))
        }
    }
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secret_resolution_failure_reuses_previous_value() {
    let memory = Arc::new(MemorySource::new("memory"));
    memory.set(
        "secret.value",
        serde_json::Value::String("secret://toggle/value".into()),
    );

    let enabled = Arc::new(AtomicBool::new(true));
    let hits = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(ToggleSecretResolver {
        enabled: enabled.clone(),
        hits: hits.clone(),
    }) as Arc<dyn SecretResolver>;

    let loader = Arc::new(Loader {
        sources: vec![memory.clone() as Arc<dyn Source>],
        secrets: vec![resolver.clone()],
        validator: Arc::new(BasicValidator),
        registry: Arc::new(InMemorySchemaRegistry::new()),
    });

    let (switch, guard) = loader.clone().load_and_watch().await.expect("watcher");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let snapshot = switch.get();
    let initial: String = snapshot
        .get(&KeyPath("secret.value".into()))
        .expect("initial secret");
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    enabled.store(false, Ordering::SeqCst);
    memory.set("app.bump", json!(1));
    tokio::time::sleep(Duration::from_millis(120)).await;

    let snapshot = switch.get();
    let after_failure: String = snapshot
        .get(&KeyPath("secret.value".into()))
        .expect("secret after failure");
    assert_eq!(after_failure, initial);
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    enabled.store(true, Ordering::SeqCst);
    memory.set("app.bump", json!(2));
    tokio::time::sleep(Duration::from_millis(120)).await;

    let snapshot = switch.get();
    let after_recovery: String = snapshot
        .get(&KeyPath("secret.value".into()))
        .expect("secret after recovery");
    assert_ne!(after_recovery, initial);
    assert_eq!(hits.load(Ordering::SeqCst), 2);

    guard.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_source_failure_preserves_last_snapshot() {
    struct RemoteState {
        fail: bool,
        version: String,
    }

    let memory = Arc::new(MemorySource::new("memory"));
    memory.set("app.bump", json!(0));

    let state = Arc::new(Mutex::new(RemoteState {
        fail: false,
        version: "1".into(),
    }));

    let remote_state = state.clone();
    let remote = Arc::new(RemoteSource::new("remote", move || {
        let state = remote_state.clone();
        async move {
            let (fail, version) = {
                let guard = state.lock();
                (guard.fail, guard.version.clone())
            };
            if fail {
                Err(ConfigError::from(errors::io_provider_unavailable(
                    "remote", "disabled",
                )))
            } else {
                let mut map = serde_json::Map::new();
                access::set_path(&mut map, "app.version", serde_json::Value::String(version));
                Ok(map)
            }
        }
        .boxed()
    }));

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
    let initial_version: String = snapshot
        .get(&KeyPath("app.version".into()))
        .expect("initial remote value");
    assert_eq!(initial_version, "1");
    let initial_bump: u64 = snapshot
        .get(&KeyPath("app.bump".into()))
        .expect("initial bump");
    assert_eq!(initial_bump, 0);

    {
        let mut guard = state.lock();
        guard.fail = true;
        guard.version = "should-not-apply".into();
    }
    memory.set("app.bump", json!(1));
    tokio::time::sleep(Duration::from_millis(120)).await;

    let snapshot = switch.get();
    let after_failure_version: String = snapshot
        .get(&KeyPath("app.version".into()))
        .expect("version after failure");
    assert_eq!(after_failure_version, "1");
    let after_failure_bump: u64 = snapshot
        .get(&KeyPath("app.bump".into()))
        .expect("bump after failure");
    assert_eq!(after_failure_bump, 0);

    {
        let mut guard = state.lock();
        guard.fail = false;
        guard.version = "2".into();
    }
    memory.set("app.bump", json!(2));
    tokio::time::sleep(Duration::from_millis(120)).await;

    let snapshot = switch.get();
    let recovered_version: String = snapshot
        .get(&KeyPath("app.version".into()))
        .expect("version after recovery");
    assert_eq!(recovered_version, "2");
    let recovered_bump: u64 = snapshot
        .get(&KeyPath("app.bump".into()))
        .expect("bump after recovery");
    assert_eq!(recovered_bump, 2);

    guard.shutdown().await;
}
