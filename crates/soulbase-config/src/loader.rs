use crate::{
    access,
    errors::ConfigError,
    model::{ConfigMap, KeyPath, Layer, ProvenanceEntry, SnapshotVersion},
    schema::SchemaRegistry,
    secrets::SecretResolver,
    snapshot::ConfigSnapshot,
    source::Source,
    validate::Validator,
};
use chrono::Utc;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::sync::Arc;

pub struct Loader {
    pub sources: Vec<Arc<dyn Source>>,
    pub secrets: Vec<Arc<dyn SecretResolver>>,
    pub validator: Arc<dyn Validator>,
    pub registry: Arc<dyn SchemaRegistry>,
}

impl Loader {
    pub async fn load_once(&self) -> Result<ConfigSnapshot, ConfigError> {
        let (mut map, mut provenance) = collect_defaults(&self.registry).await;

        for source in &self.sources {
            let snapshot = source.load().await?;
            merge_into(&mut map, snapshot.map);
            provenance.extend(snapshot.provenance);
        }

        resolve_secrets(&mut map, &self.secrets).await?;

        let tree = serde_json::Value::Object(map);
        self.validator.validate_boot(&tree).await?;

        Ok(ConfigSnapshot::from_tree(
            tree,
            SnapshotVersion("v1".into()),
            provenance,
            None,
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
                ))
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

        self.validator.validate_boot(&tree).await?;

        Ok(ConfigSnapshot::from_tree(
            tree,
            SnapshotVersion("v1-overrides".into()),
            provenance,
            reload_policy,
        ))
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

fn merge_object(dst: &mut serde_json::Map<String, serde_json::Value>, src: serde_json::Map<String, serde_json::Value>) {
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

async fn resolve_secrets(map: &mut ConfigMap, resolvers: &[Arc<dyn SecretResolver>]) -> Result<(), ConfigError> {
    fn visit<'a>(value: &'a mut serde_json::Value, resolvers: &'a [Arc<dyn SecretResolver>]) -> BoxFuture<'a, Result<(), ConfigError>> {
        async move {
            match value {
                serde_json::Value::String(s) => {
                    if s.starts_with("secret://") {
                        let uri = s.clone();
                        for resolver in resolvers {
                            let resolved = resolver.resolve(&uri).await?;
                            *value = resolved;
                            break;
                        }
                    }
                }
                serde_json::Value::Object(map) => {
                    for (_, child) in map.iter_mut() {
                        visit(child, resolvers).await?;
                    }
                }
                serde_json::Value::Array(items) => {
                    for item in items.iter_mut() {
                        visit(item, resolvers).await?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        .boxed()
    }

    let mut root = serde_json::Value::Object(std::mem::take(map));
    visit(&mut root, resolvers).await?;
    *map = root.as_object().cloned().unwrap_or_default();
    Ok(())
}
