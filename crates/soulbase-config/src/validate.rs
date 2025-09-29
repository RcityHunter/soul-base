use crate::errors::ConfigError;
use crate::{
    access, errors,
    model::{KeyPath, NamespaceId, ReloadClass},
    schema::{NamespaceView, SchemaRegistry},
};
use async_trait::async_trait;
use jsonschema::{Draft, JSONSchema};
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait Validator: Send + Sync {
    async fn validate_boot(&self, tree: &Value) -> Result<(), ConfigError>;
    async fn validate_delta(&self, old: &Value, new: &Value) -> Result<(), ConfigError>;
}

pub struct BasicValidator;

#[async_trait]
impl Validator for BasicValidator {
    async fn validate_boot(&self, _tree: &Value) -> Result<(), ConfigError> {
        Ok(())
    }

    async fn validate_delta(&self, _old: &Value, _new: &Value) -> Result<(), ConfigError> {
        Ok(())
    }
}

pub struct SchemaValidator {
    registry: Arc<dyn SchemaRegistry>,
    cache: RwLock<HashMap<String, Arc<JSONSchema>>>,
    allow_risky_hot_reload: bool,
}

impl SchemaValidator {
    pub fn new(registry: Arc<dyn SchemaRegistry>) -> Self {
        Self {
            registry,
            cache: RwLock::new(HashMap::new()),
            allow_risky_hot_reload: false,
        }
    }

    pub fn allow_risky_hot_reload(mut self, allow: bool) -> Self {
        self.allow_risky_hot_reload = allow;
        self
    }

    async fn namespaces(&self) -> Vec<(NamespaceId, NamespaceView)> {
        self.registry.list_namespaces().await
    }

    fn ensure_schema(
        &self,
        ns: &NamespaceId,
        view: &NamespaceView,
    ) -> Result<Arc<JSONSchema>, ConfigError> {
        if let Some(compiled) = self.cache.read().get(&ns.0).cloned() {
            return Ok(compiled);
        }

        let schema_value = serde_json::to_value(&view.json_schema).map_err(|err| {
            errors::schema_invalid(
                "json_schema",
                &format!("namespace '{}': failed to serialize schema: {err}", ns.0),
            )
        })?;

        let compiled = JSONSchema::options()
            .with_draft(Draft::Draft7)
            .should_validate_formats(true)
            .compile(&schema_value)
            .map_err(|err| {
                errors::schema_invalid("json_schema", &format!("namespace '{}': {err}", ns.0))
            })?;

        let compiled = Arc::new(compiled);
        self.cache
            .write()
            .entry(ns.0.clone())
            .or_insert_with(|| compiled.clone());
        Ok(compiled)
    }

    fn validate_tree(
        &self,
        namespaces: &[(NamespaceId, NamespaceView)],
        tree: &Value,
    ) -> Result<(), ConfigError> {
        for (ns, view) in namespaces {
            let compiled = self.ensure_schema(ns, view)?;
            let subject = access::get_path(tree, &ns.0)
                .cloned()
                .unwrap_or(Value::Null);
            compiled.validate(&subject).map_err(|mut errors| {
                let message = if let Some(first) = errors.next() {
                    format!("namespace '{}': {first}", ns.0)
                } else {
                    format!("namespace '{}' failed validation", ns.0)
                };
                errors::schema_invalid("json_schema", &message)
            })?;
        }
        Ok(())
    }

    fn enforce_reload_classes(
        &self,
        namespaces: &[(NamespaceId, NamespaceView)],
        old: &Value,
        new: &Value,
    ) -> Result<(), ConfigError> {
        for (ns, view) in namespaces {
            for (path, meta) in &view.field_meta {
                if !value_changed(old, new, path) {
                    continue;
                }

                match meta.reload {
                    ReloadClass::BootOnly => {
                        return Err(errors::schema_invalid(
                            "reload",
                            &format!("field '{}' requires restart (namespace '{}')", path.0, ns.0),
                        ));
                    }
                    ReloadClass::HotReloadRisky if !self.allow_risky_hot_reload => {
                        tracing::warn!(
                            target = "soulbase::config",
                            field = %path.0,
                            namespace = %ns.0,
                            "hot reload of risky field detected",
                        );
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Validator for SchemaValidator {
    async fn validate_boot(&self, tree: &Value) -> Result<(), ConfigError> {
        let namespaces = self.namespaces().await;
        self.validate_tree(&namespaces, tree)
    }

    async fn validate_delta(&self, old: &Value, new: &Value) -> Result<(), ConfigError> {
        let namespaces = self.namespaces().await;
        self.enforce_reload_classes(&namespaces, old, new)?;
        self.validate_tree(&namespaces, new)
    }
}

fn value_changed(old: &Value, new: &Value, path: &KeyPath) -> bool {
    let lhs = access::get_path(old, &path.0);
    let rhs = access::get_path(new, &path.0);
    match (lhs, rhs) {
        (None, None) => false,
        (Some(a), Some(b)) => a != b,
        _ => true,
    }
}
