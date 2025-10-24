use crate::{
    access,
    errors::{self, ConfigError},
    model::{ConfigMap, KeyPath, Layer, ProvenanceEntry, SnapshotVersion},
    schema::{InMemorySchemaRegistry, SchemaRegistry},
    secrets::{NoopSecretResolver, SecretResolver},
    snapshot::ConfigSnapshot,
    source::{env::EnvSource, file::FileSource, Source},
    switch::SnapshotSwitch,
    validate::{BasicValidator, Validator},
    watch::ChangeNotice,
};
use chrono::Utc;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::StreamExt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::warn;

pub struct Loader {
    pub sources: Vec<Arc<dyn Source>>,
    pub secrets: Vec<Arc<dyn SecretResolver>>,
    pub validator: Arc<dyn Validator>,
    pub registry: Arc<dyn SchemaRegistry>,
}

pub struct LoaderBuilder {
    sources: Vec<Arc<dyn Source>>,
    secrets: Vec<Arc<dyn SecretResolver>>,
    validator: Option<Arc<dyn Validator>>,
    registry: Option<Arc<dyn SchemaRegistry>>,
}

impl LoaderBuilder {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            secrets: Vec::new(),
            validator: None,
            registry: None,
        }
    }

    pub fn with_registry(mut self, registry: Arc<dyn SchemaRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    pub fn with_validator(mut self, validator: Arc<dyn Validator>) -> Self {
        self.validator = Some(validator);
        self
    }

    pub fn add_source<S>(mut self, source: S) -> Self
    where
        S: Source + 'static,
    {
        self.sources.push(Arc::new(source));
        self
    }

    pub fn add_source_arc(mut self, source: Arc<dyn Source>) -> Self {
        self.sources.push(source);
        self
    }

    pub fn add_file_sources<I, P>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        let paths = paths.into_iter().map(|p| p.into()).collect::<Vec<_>>();
        if !paths.is_empty() {
            self.sources
                .push(Arc::new(FileSource { paths }) as Arc<dyn Source>);
        }
        self
    }

    pub fn add_env_source(
        mut self,
        prefix: impl Into<String>,
        separator: impl Into<String>,
    ) -> Self {
        let source = EnvSource {
            prefix: prefix.into(),
            separator: separator.into(),
        };
        self.sources.push(Arc::new(source) as Arc<dyn Source>);
        self
    }

    pub fn add_secret_resolver<R>(mut self, resolver: R) -> Self
    where
        R: SecretResolver + 'static,
    {
        self.secrets.push(Arc::new(resolver));
        self
    }

    pub fn build(self) -> Loader {
        let registry = self
            .registry
            .unwrap_or_else(|| Arc::new(InMemorySchemaRegistry::new()));
        let validator = self
            .validator
            .unwrap_or_else(|| Arc::new(BasicValidator) as Arc<dyn Validator>);

        let secrets = if self.secrets.is_empty() {
            vec![Arc::new(NoopSecretResolver) as Arc<dyn SecretResolver>]
        } else {
            self.secrets
        };

        Loader {
            sources: self.sources,
            secrets,
            validator,
            registry,
        }
    }

    pub fn build_arc(self) -> Arc<Loader> {
        Arc::new(self.build())
    }
}

impl Loader {
    pub fn builder() -> LoaderBuilder {
        LoaderBuilder::new()
    }

    pub async fn load_and_watch(
        self: Arc<Self>,
    ) -> Result<(Arc<SnapshotSwitch>, WatchGuard), ConfigError> {
        let initial = Arc::new(self.load_once().await?);
        let switch = Arc::new(SnapshotSwitch::new(initial.clone()));

        let (tx, rx) = futures::channel::mpsc::channel::<ChangeNotice>(32);

        for source in &self.sources {
            if source.supports_watch() {
                source.watch(tx.clone()).await?;
            }
        }

        drop(tx);

        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let loader = self.clone();
        let switch_arc = switch.clone();
        let mut stop = stop_rx.fuse();
        let mut rx_stream = rx.fuse();
        let task = tokio::spawn(async move {
            loop {
                futures::select! {
                    _ = stop => break,
                    notice = rx_stream.next() => {
                        let Some(_notice) = notice else {
                            break;
                        };
                        let current = switch_arc.get();
                        match loader.load_next(&current).await {
                            Ok(snapshot) => {
                                switch_arc.swap(Arc::new(snapshot));
                            }
                            Err(err) => {
                                tracing::warn!(
                                    target = "soulbase::config",
                                    "watch reload rejected: {err:?}; keeping last snapshot",
                                );
                            }
                        }
                    }
                }
            }
        });

        Ok((
            switch,
            WatchGuard {
                cancel: Some(stop_tx),
                task: Some(task),
            },
        ))
    }
    pub async fn load_once(&self) -> Result<ConfigSnapshot, ConfigError> {
        let (map, provenance) = self.materialize(None).await?;
        let tree = serde_json::Value::Object(map);
        self.validator.validate_boot(&tree).await?;

        Ok(ConfigSnapshot::from_tree(
            tree,
            SnapshotVersion("v1".into()),
            provenance,
            None,
        ))
    }

    pub async fn load_next(&self, current: &ConfigSnapshot) -> Result<ConfigSnapshot, ConfigError> {
        let (map, provenance) = self.materialize(Some(current.tree())).await?;
        let tree = serde_json::Value::Object(map);
        self.validator.validate_delta(current.tree(), &tree).await?;
        let reload_policy = current.reload_policy().map(|s| s.to_string());

        Ok(ConfigSnapshot::from_tree(
            tree,
            SnapshotVersion("v1".into()),
            provenance,
            reload_policy,
        ))
    }

    pub async fn load_with(
        &self,
        overrides: serde_json::Value,
    ) -> Result<ConfigSnapshot, ConfigError> {
        let base = self.load_once().await?;
        let mut tree = base.tree().clone();
        let mut provenance = base.provenance().to_vec();
        let reload_policy = base.reload_policy().map(|s| s.to_string());

        let override_map = match overrides {
            serde_json::Value::Object(map) => map,
            other => {
                return Err(crate::errors::schema_invalid(
                    "overrides",
                    &format!("expected object, got {other:?}"),
                ));
            }
        };

        if let Some(obj) = tree.as_object_mut() {
            merge_object(obj, override_map);
            provenance.push(ProvenanceEntry {
                key: KeyPath("**".into()),
                source_id: "overrides".into(),
                layer: Layer::Cli,
                version: None,
                ts_ms: Utc::now().timestamp_millis(),
            });
        }

        resolve_secrets_value(&mut tree, &self.secrets, Some(base.tree())).await?;
        self.validator.validate_delta(base.tree(), &tree).await?;
        self.validator.validate_boot(&tree).await?;

        Ok(ConfigSnapshot::from_tree(
            tree,
            SnapshotVersion("v1-overrides".into()),
            provenance,
            reload_policy,
        ))
    }

    async fn materialize(
        &self,
        fallback: Option<&serde_json::Value>,
    ) -> Result<(ConfigMap, Vec<ProvenanceEntry>), ConfigError> {
        let (mut map, mut provenance) = collect_defaults(&self.registry).await;

        for source in &self.sources {
            let snapshot = source.load().await?;
            merge_into(&mut map, snapshot.map);
            provenance.extend(snapshot.provenance);
        }

        let mut root = serde_json::Value::Object(map);
        resolve_secrets_value(&mut root, &self.secrets, fallback).await?;
        match root {
            serde_json::Value::Object(map) => Ok((map, provenance)),
            _ => unreachable!(),
        }
    }
}

pub struct WatchGuard {
    cancel: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl WatchGuard {
    pub async fn shutdown(mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn collect_defaults(registry: &Arc<dyn SchemaRegistry>) -> (ConfigMap, Vec<ProvenanceEntry>) {
    let mut map = ConfigMap::new();
    let mut provenance = Vec::new();

    for (ns, view) in registry.list_namespaces().await {
        for (path, meta) in view.field_meta.iter() {
            if let Some(default) = meta.default_value.clone() {
                access::set_path(&mut map, &path.0, default);
                provenance.push(ProvenanceEntry {
                    key: path.clone(),
                    source_id: format!("schema:{}", ns.0),
                    layer: Layer::Defaults,
                    version: None,
                    ts_ms: Utc::now().timestamp_millis(),
                });
            }
        }
    }

    (map, provenance)
}

fn merge_into(dst: &mut ConfigMap, src: ConfigMap) {
    for (key, value) in src {
        match (dst.get_mut(&key), value) {
            (Some(serde_json::Value::Object(dst_obj)), serde_json::Value::Object(src_obj)) => {
                merge_object(dst_obj, src_obj);
            }
            (_, v) => {
                dst.insert(key, v);
            }
        }
    }
}

fn merge_object(
    dst: &mut serde_json::Map<String, serde_json::Value>,
    src: serde_json::Map<String, serde_json::Value>,
) {
    for (key, value) in src {
        match (dst.get_mut(&key), value) {
            (Some(serde_json::Value::Object(dst_obj)), serde_json::Value::Object(src_obj)) => {
                merge_object(dst_obj, src_obj);
            }
            (_, v) => {
                dst.insert(key, v);
            }
        }
    }
}

async fn resolve_secrets_value(
    value: &mut serde_json::Value,
    resolvers: &[Arc<dyn SecretResolver>],
    fallback: Option<&serde_json::Value>,
) -> Result<(), ConfigError> {
    if let serde_json::Value::Object(map) = value {
        resolve_secrets_map(map, resolvers, fallback).await
    } else {
        Ok(())
    }
}

async fn resolve_secrets_map(
    map: &mut serde_json::Map<String, serde_json::Value>,
    resolvers: &[Arc<dyn SecretResolver>],
    fallback: Option<&serde_json::Value>,
) -> Result<(), ConfigError> {
    let mut path = Vec::new();
    visit_map(map, resolvers, fallback, &mut path).await
}

fn resolver_hint(uri: &str) -> Option<&str> {
    let rest = uri.strip_prefix("secret://")?;
    let (candidate, _) = rest.split_once('/')?;
    if candidate.is_empty() {
        None
    } else {
        Some(candidate)
    }
}

async fn maybe_resolve_secret(
    candidate: &str,
    resolvers: &[Arc<dyn SecretResolver>],
) -> Result<Option<serde_json::Value>, ConfigError> {
    if !candidate.starts_with("secret://") {
        return Ok(None);
    }

    let hint = resolver_hint(candidate);
    let targets: Vec<&Arc<dyn SecretResolver>> = match hint {
        Some(id) => {
            let resolver = resolvers.iter().find(|resolver| resolver.id() == id);
            match resolver {
                Some(r) => vec![r],
                None => {
                    return Err(errors::schema_invalid(
                        "secret",
                        &format!("resolver '{id}' not configured"),
                    ));
                }
            }
        }
        None => resolvers.iter().collect::<Vec<_>>(),
    };

    let mut last_err: Option<ConfigError> = None;
    for resolver in targets {
        match resolver.resolve(candidate).await {
            Ok(value) => return Ok(Some(value)),
            Err(err) => {
                last_err = Some(err);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        errors::schema_invalid(
            "secret",
            &format!("no resolver handled uri (hint: {:?})", hint),
        )
    }))
}

fn fallback_for_key<'a>(
    fallback: Option<&'a serde_json::Value>,
    key: &str,
) -> Option<&'a serde_json::Value> {
    fallback.and_then(|value| match value {
        serde_json::Value::Object(map) => map.get(key),
        _ => None,
    })
}

fn fallback_for_index<'a>(
    fallback: Option<&'a serde_json::Value>,
    idx: usize,
) -> Option<&'a serde_json::Value> {
    fallback.and_then(|value| match value {
        serde_json::Value::Array(items) => items.get(idx),
        _ => None,
    })
}

fn path_display(path: &[String]) -> String {
    if path.is_empty() {
        "<root>".into()
    } else {
        path.join(".")
    }
}

fn clone_value(value: Option<&serde_json::Value>) -> Option<serde_json::Value> {
    value.cloned()
}

async fn visit_map(
    map: &mut serde_json::Map<String, serde_json::Value>,
    resolvers: &[Arc<dyn SecretResolver>],
    fallback: Option<&serde_json::Value>,
    path: &mut Vec<String>,
) -> Result<(), ConfigError> {
    let keys: Vec<String> = map.keys().cloned().collect();
    for key in keys {
        if let Some(value) = map.get_mut(&key) {
            path.push(key.clone());
            let child_fallback = fallback_for_key(fallback, &key);
            visit_value(value, resolvers, child_fallback, path).await?;
            path.pop();
        }
    }
    Ok(())
}

fn visit_value<'a>(
    value: &'a mut serde_json::Value,
    resolvers: &'a [Arc<dyn SecretResolver>],
    fallback: Option<&'a serde_json::Value>,
    path: &'a mut Vec<String>,
) -> BoxFuture<'a, Result<(), ConfigError>> {
    async move {
        match value {
            serde_json::Value::String(raw) => match maybe_resolve_secret(raw, resolvers).await {
                Ok(Some(resolved)) => {
                    *value = resolved;
                }
                Ok(None) => {}
                Err(err) => {
                    if let Some(previous) = clone_value(fallback) {
                        let location = path_display(path);
                        warn!(
                            target = "soulbase::config",
                            path = %location,
                            "secret resolution failed; reusing last known value: {err}"
                        );
                        *value = previous;
                    } else {
                        return Err(err);
                    }
                }
            },
            serde_json::Value::Object(map) => {
                visit_map(map, resolvers, fallback, path).await?;
            }
            serde_json::Value::Array(items) => {
                for (idx, item) in items.iter_mut().enumerate() {
                    path.push(idx.to_string());
                    let child_fallback = fallback_for_index(fallback, idx);
                    visit_value(item, resolvers, child_fallback, path).await?;
                    path.pop();
                }
            }
            _ => {}
        }
        Ok(())
    }
    .boxed()
}
