use soulbase_config::access;
use soulbase_config::errors::schema_invalid;
use soulbase_config::prelude::*;
use soulbase_config::secrets::CachingResolver;
use soulbase_config::source::{cli::CliArgsSource, env::EnvSource, file::FileSource};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn load_minimal_snapshot_and_read() {
    let loader = Loader {
        sources: vec![
            Arc::new(FileSource { paths: vec![] }) as Arc<dyn Source>,
            Arc::new(EnvSource {
                prefix: "SOUL__".into(),
                separator: "__".into(),
            }) as Arc<dyn Source>,
            Arc::new(CliArgsSource {
                args: vec!["--app.name=Soulseed".into()],
            }) as Arc<dyn Source>,
        ],
        secrets: vec![Arc::new(NoopSecretResolver) as Arc<dyn SecretResolver>],
        validator: Arc::new(BasicValidator),
        registry: Arc::new(InMemorySchemaRegistry::new()),
    };

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let snapshot = runtime
        .block_on(async { loader.load_once().await })
        .expect("snapshot");

    let app_name: String = snapshot.get(&KeyPath("app.name".into())).expect("app.name");
    assert_eq!(app_name, "Soulseed");
    assert!(!snapshot.checksum().0.is_empty());
}

struct StaticSource;

#[async_trait::async_trait]
impl Source for StaticSource {
    fn id(&self) -> &'static str {
        "static"
    }

    async fn load(&self) -> Result<SourceSnapshot, ConfigError> {
        let mut map = serde_json::Map::new();
        access::set_path(
            &mut map,
            "secrets.key",
            serde_json::Value::String("secret://named/demo".into()),
        );
        access::set_path(
            &mut map,
            "secrets.auto",
            serde_json::Value::String("secret://value".into()),
        );
        Ok(SourceSnapshot {
            map,
            provenance: Vec::new(),
        })
    }
}

struct FailingResolver;

#[async_trait::async_trait]
impl SecretResolver for FailingResolver {
    fn id(&self) -> &'static str {
        "fail"
    }

    async fn resolve(&self, _uri: &str) -> Result<serde_json::Value, ConfigError> {
        Err(schema_invalid("test", "forced failure"))
    }
}

struct NamedResolver;

#[async_trait::async_trait]
impl SecretResolver for NamedResolver {
    fn id(&self) -> &'static str {
        "named"
    }

    async fn resolve(&self, uri: &str) -> Result<serde_json::Value, ConfigError> {
        if uri.starts_with("secret://named/") {
            Ok(serde_json::Value::String(format!("resolved::{uri}")))
        } else {
            Err(schema_invalid("test", "named resolver skipped"))
        }
    }
}

struct DefaultResolver;

#[async_trait::async_trait]
impl SecretResolver for DefaultResolver {
    fn id(&self) -> &'static str {
        "default"
    }

    async fn resolve(&self, uri: &str) -> Result<serde_json::Value, ConfigError> {
        Ok(serde_json::Value::String(format!("fallback::{uri}")))
    }
}

#[test]
fn resolves_secrets_with_hints_and_fallback() {
    let loader = Loader {
        sources: vec![Arc::new(StaticSource) as Arc<dyn Source>],
        secrets: vec![
            Arc::new(FailingResolver) as Arc<dyn SecretResolver>,
            Arc::new(NamedResolver) as Arc<dyn SecretResolver>,
            Arc::new(DefaultResolver) as Arc<dyn SecretResolver>,
        ],
        validator: Arc::new(BasicValidator),
        registry: Arc::new(InMemorySchemaRegistry::new()),
    };

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let snapshot = runtime
        .block_on(async { loader.load_once().await })
        .expect("snapshot");

    let named: String = snapshot
        .get(&KeyPath("secrets.key".into()))
        .expect("named secret");
    assert_eq!(named, "resolved::secret://named/demo");

    let fallback: String = snapshot
        .get(&KeyPath("secrets.auto".into()))
        .expect("auto secret");
    assert_eq!(fallback, "fallback::secret://value");
}

struct SecretOnlySource;

#[async_trait::async_trait]
impl Source for SecretOnlySource {
    fn id(&self) -> &'static str {
        "secret"
    }

    async fn load(&self) -> Result<SourceSnapshot, ConfigError> {
        let mut map = serde_json::Map::new();
        access::set_path(
            &mut map,
            "secret.value",
            serde_json::Value::String("secret://value".into()),
        );
        Ok(SourceSnapshot {
            map,
            provenance: Vec::new(),
        })
    }
}

struct CountingResolver {
    hits: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl SecretResolver for CountingResolver {
    fn id(&self) -> &'static str {
        "counting"
    }

    async fn resolve(&self, _uri: &str) -> Result<serde_json::Value, ConfigError> {
        let next = self.hits.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(serde_json::Value::String(format!("count-{next}")))
    }
}

#[test]
fn caching_resolver_reuses_until_expiry() {
    let counter = Arc::new(AtomicUsize::new(0));
    let caching = Arc::new(CachingResolver::new(
        CountingResolver {
            hits: counter.clone(),
        },
        Some(Duration::from_millis(50)),
    ));

    let loader = Loader {
        sources: vec![Arc::new(SecretOnlySource) as Arc<dyn Source>],
        secrets: vec![caching.clone() as Arc<dyn SecretResolver>],
        validator: Arc::new(BasicValidator),
        registry: Arc::new(InMemorySchemaRegistry::new()),
    };

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let snapshot = loader.load_once().await.expect("snapshot1");
        let first: String = snapshot
            .get(&KeyPath("secret.value".into()))
            .expect("first value");
        assert_eq!(first, "count-1");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        let snapshot2 = loader.load_once().await.expect("snapshot2");
        let second: String = snapshot2
            .get(&KeyPath("secret.value".into()))
            .expect("second value");
        assert_eq!(second, first);
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        tokio::time::sleep(Duration::from_millis(60)).await;

        let snapshot3 = loader.load_once().await.expect("snapshot3");
        let third: String = snapshot3
            .get(&KeyPath("secret.value".into()))
            .expect("third value");
        assert_ne!(third, first);
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    });
}
