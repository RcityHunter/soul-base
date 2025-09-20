use crate::{
    errors::ConfigError,
    model::{KeyPath, NamespaceId, ReloadClass},
};
use parking_lot::RwLock;
use schemars::schema::RootSchema;
use std::collections::HashMap;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FieldMeta {
    pub reload: ReloadClass,
    #[serde(default)]
    pub sensitive: bool,
    #[serde(default)]
    pub default_value: Option<serde_json::Value>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Clone)]
pub struct NamespaceView {
    pub json_schema: RootSchema,
    pub field_meta: HashMap<KeyPath, FieldMeta>,
}

#[async_trait::async_trait]
pub trait SchemaRegistry: Send + Sync {
    async fn register_namespace(
        &self,
        ns: &NamespaceId,
        schema: RootSchema,
        meta: HashMap<KeyPath, FieldMeta>,
    ) -> Result<(), ConfigError>;

    async fn get_namespace(&self, ns: &NamespaceId) -> Option<NamespaceView>;

    async fn list_namespaces(&self) -> Vec<(NamespaceId, NamespaceView)>;
}

pub struct InMemorySchemaRegistry {
    inner: RwLock<HashMap<String, NamespaceView>>,
}

impl InMemorySchemaRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemorySchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl SchemaRegistry for InMemorySchemaRegistry {
    async fn register_namespace(
        &self,
        ns: &NamespaceId,
        schema: RootSchema,
        meta: HashMap<KeyPath, FieldMeta>,
    ) -> Result<(), ConfigError> {
        self.inner.write().insert(
            ns.0.clone(),
            NamespaceView {
                json_schema: schema,
                field_meta: meta,
            },
        );
        Ok(())
    }

    async fn get_namespace(&self, ns: &NamespaceId) -> Option<NamespaceView> {
        self.inner.read().get(&ns.0).cloned()
    }

    async fn list_namespaces(&self) -> Vec<(NamespaceId, NamespaceView)> {
        self.inner
            .read()
            .iter()
            .map(|(name, view)| (NamespaceId(name.clone()), view.clone()))
            .collect()
    }
}
