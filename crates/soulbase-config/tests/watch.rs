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
