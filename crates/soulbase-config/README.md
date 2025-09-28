# soulbase-config

Schema-first configuration & secrets layer (multi-source merge, validation, immutable snapshots, hot-reload & rollback).

## Build & Test
~~~bash
cargo check
cargo test
~~~

## Example
- File + Env + CLI layered merge
- Remote key-value + in-memory sources with hot reload
- Secret resolvers with caching / resolver hints
- Basic validator

```rust
use futures::FutureExt;
use soulbase_config::access;
use soulbase_config::prelude::*;
use soulbase_config::source::{memory::MemorySource, remote::RemoteSource, Source};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), ConfigError> {
    let memory = Arc::new(MemorySource::new("memory"));
    memory.set("app.name", serde_json::Value::String("Soulseed".into()));

    let remote_state = Arc::new(std::sync::Mutex::new(serde_json::Map::new()));
    {
        let mut guard = remote_state.lock().unwrap();
        access::set_path(
            &mut *guard,
            "app.version",
            serde_json::Value::String("1".into()),
        );
    }

    let remote_clone = remote_state.clone();
    let remote = Arc::new(
        RemoteSource::new("remote", move || {
            let state = remote_clone.clone();
            async move {
                let map = state.lock().unwrap().clone();
                Ok::<_, ConfigError>(map)
            }
            .boxed()
        })
        .with_interval(Duration::from_secs(5))
        .with_backoff(Duration::from_secs(1)),
    );

    #[cfg(feature = "remote_http")]
    let http_remote = Arc::new(
        RemoteSource::from_http(
            "remote-http",
            reqwest::Client::new(),
            "https://example.com/config".parse().unwrap(),
        )
        .with_interval(Duration::from_secs(30)),
    );

    let mut sources: Vec<Arc<dyn Source>> = vec![memory.clone(), remote.clone()];
    #[cfg(feature = "remote_http")]
    sources.push(http_remote.clone());

    let loader = Arc::new(Loader {
        sources,
        secrets: vec![Arc::new(NoopSecretResolver) as Arc<dyn SecretResolver>],
        validator: Arc::new(BasicValidator),
        registry: Arc::new(InMemorySchemaRegistry::new()),
    });

    let (switch, guard) = loader.clone().load_and_watch().await?;
    let snapshot = switch.get();
    let name: String = snapshot.get(&KeyPath("app.name".into()))?;
    println!("app.name={name}");

    guard.shutdown().await;
    Ok(())
}
```
