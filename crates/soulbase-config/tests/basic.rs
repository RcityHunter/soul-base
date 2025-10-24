use futures::FutureExt;
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use soulbase_config::access;
use soulbase_config::errors::schema_invalid;
use soulbase_config::prelude::*;
use soulbase_config::secrets::{
    CachingResolver, EnvSecretResolver, FileSecretResolver, KvSecretResolver, SecretResolver,
};
use soulbase_config::source::{
    cli::CliArgsSource, env::EnvSource, file::FileSource, remote::RemoteSource,
};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

#[test]
fn loader_builder_combines_sources_in_order() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let path = env::temp_dir().join(format!("soulbase_cfg_{timestamp}.json"));
    fs::write(&path, r#"{"app":{"name":"from_file","from_file":true}}"#)
        .expect("write config file");

    env::set_var("SB_APP_NAME", "from_env");

    let remote_state = Arc::new(Mutex::new(serde_json::Map::new()));
    {
        let mut guard = remote_state.lock();
        access::set_path(
            &mut *guard,
            "app.name",
            serde_json::Value::String("from_remote".into()),
        );
        access::set_path(
            &mut *guard,
            "app.version",
            serde_json::Value::String("1".into()),
        );
    }
    let remote_clone = remote_state.clone();
    let remote = RemoteSource::new("remote", move || {
        let state = remote_clone.clone();
        async move { Ok(state.lock().clone()) }.boxed()
    });

    let loader = Loader::builder()
        .add_file_sources([path.clone()])
        .add_env_source("SB", "_")
        .add_source(remote)
        .build();

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let snapshot = runtime
        .block_on(async { loader.load_once().await })
        .expect("snapshot");

    let name: String = snapshot.get(&KeyPath("app.name".into())).expect("app.name");
    assert_eq!(name, "from_remote");

    let from_file: bool = snapshot
        .get(&KeyPath("app.from_file".into()))
        .expect("from_file");
    assert!(from_file);

    let version: String = snapshot
        .get(&KeyPath("app.version".into()))
        .expect("version");
    assert_eq!(version, "1");

    env::remove_var("SB_APP_NAME");
    let _ = fs::remove_file(&path);
}

#[test]
fn loader_builder_honors_cli_overrides_after_env_and_remote() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let path = env::temp_dir().join(format!("soulbase_cfg_precedence_{timestamp}.json"));
    fs::write(
        &path,
        r#"{"app":{"name":"from_file","file_only":true,"priority":"file"}}"#,
    )
    .expect("write config file");

    env::set_var("SB_APP_PRIORITY", "env");
    env::set_var("SB_APP_ENV_ONLY", "env-only");

    let remote_state = Arc::new(Mutex::new(serde_json::Map::new()));
    {
        let mut guard = remote_state.lock();
        access::set_path(
            &mut *guard,
            "app.priority",
            serde_json::Value::String("remote".into()),
        );
        access::set_path(
            &mut *guard,
            "app.remote_only",
            serde_json::Value::Bool(true),
        );
    }
    let remote_clone = remote_state.clone();
    let remote = RemoteSource::new("remote-precedence", move || {
        let state = remote_clone.clone();
        async move { Ok(state.lock().clone()) }.boxed()
    });

    let cli_args = CliArgsSource {
        args: vec!["--app.priority=cli".into(), "--app.cli_only=true".into()],
    };

    let loader = Loader::builder()
        .add_file_sources([path.clone()])
        .add_env_source("SB", "_")
        .add_source(remote)
        .add_source(cli_args)
        .build();

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let snapshot = runtime
        .block_on(async { loader.load_once().await })
        .expect("snapshot");

    let priority: String = snapshot
        .get(&KeyPath("app.priority".into()))
        .expect("priority key");
    assert_eq!(priority, "cli");

    let file_only: bool = snapshot
        .get(&KeyPath("app.file_only".into()))
        .expect("file flag");
    assert!(file_only);

    let remote_only: bool = snapshot
        .get(&KeyPath("app.remote_only".into()))
        .expect("remote flag");
    assert!(remote_only);

    let env_only: String = snapshot
        .get(&KeyPath("app.env_only".into()))
        .expect("env flag");
    assert_eq!(env_only, "env-only");

    let cli_only: bool = snapshot
        .get(&KeyPath("app.cli_only".into()))
        .expect("cli flag");
    assert!(cli_only);

    env::remove_var("SB_APP_PRIORITY");
    env::remove_var("SB_APP_ENV_ONLY");
    let _ = fs::remove_file(&path);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namespace_registration_applies_defaults_and_validation() {
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct AppCfg {
        limit: u32,
        secret: String,
    }

    let registry = Arc::new(InMemorySchemaRegistry::new());
    register_namespace_struct::<AppCfg, _>(
        &*registry,
        NamespaceId::new("app"),
        vec![
            (
                KeyPath::new("app.limit"),
                FieldMeta::new(ReloadClass::HotReloadSafe)
                    .with_default(serde_json::json!(5))
                    .with_description("Request limit"),
            ),
            (
                KeyPath::new("app.secret"),
                FieldMeta::new(ReloadClass::BootOnly)
                    .mark_sensitive()
                    .with_default(serde_json::json!("changeme"))
                    .with_description("Shared secret"),
            ),
        ],
    )
    .await
    .expect("register namespace");

    let validator = Arc::new(SchemaValidator::new(registry.clone())) as Arc<dyn Validator>;
    let loader = Arc::new(Loader {
        sources: vec![Arc::new(MemorySource::new("memory")) as Arc<dyn Source>],
        secrets: vec![Arc::new(NoopSecretResolver) as Arc<dyn SecretResolver>],
        validator,
        registry,
    });

    let snapshot = loader.load_once().await.expect("snapshot");
    let limit: u32 = snapshot
        .get(&KeyPath::new("app.limit"))
        .expect("default limit");
    assert_eq!(limit, 5);

    let err = loader
        .load_with(serde_json::json!({"app": {"limit": "oops"}}))
        .await
        .expect_err("invalid override");
    let error = err.into_inner();
    let dev_msg = error.message_dev.expect("dev message");
    assert!(dev_msg.contains("namespace 'app'"));
}

#[test]
fn env_secret_resolver_reads_env() {
    env::set_var("SB_CONF_SECRET", "super-secret");
    let resolver = EnvSecretResolver::new();
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let value = runtime
        .block_on(async { resolver.resolve("secret://env/SB_CONF_SECRET").await })
        .expect("env secret");
    assert_eq!(value, serde_json::Value::String("super-secret".into()));
    env::remove_var("SB_CONF_SECRET");
}

#[test]
fn file_secret_resolver_reads_file() {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("timestamp")
        .as_micros();
    let path = env::temp_dir().join(format!("soulbase_secret_{timestamp}.txt"));
    fs::write(
        &path,
        "on-disk-secret
",
    )
    .expect("write secret");

    let resolver = FileSecretResolver::new();
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let uri = format!("secret://file/{}", path.to_string_lossy());
    let value = runtime
        .block_on(async { resolver.resolve(&uri).await })
        .expect("file secret");
    assert_eq!(value, serde_json::Value::String("on-disk-secret".into()));

    let _ = fs::remove_file(&path);
}

#[test]
fn kv_secret_resolver_reads_from_store() {
    let store = Arc::new(Mutex::new(HashMap::new()));
    {
        let mut guard = store.lock();
        guard.insert(
            "api".to_string(),
            serde_json::Value::String("token-123".into()),
        );
    }

    let resolver = KvSecretResolver::new("kv", {
        let store = store.clone();
        move |key: String| {
            let store = store.clone();
            async move { Ok(store.lock().get(&key).cloned()) }
        }
    });

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let value = runtime
        .block_on(async { resolver.resolve("secret://kv/api").await })
        .expect("kv secret");
    assert_eq!(value, serde_json::Value::String("token-123".into()));
}
