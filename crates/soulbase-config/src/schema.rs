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

impl FieldMeta {
    pub fn new(reload: ReloadClass) -> Self {
        Self {
            reload,
            sensitive: false,
            default_value: None,
            description: None,
        }
    }

    pub fn mark_sensitive(mut self) -> Self {
        self.sensitive = true;
        self
    }

    pub fn with_sensitive(mut self, sensitive: bool) -> Self {
        self.sensitive = sensitive;
        self
    }

    pub fn with_default(mut self, value: serde_json::Value) -> Self {
        self.default_value = Some(value);
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
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

#[cfg(feature = "schema_json")]
pub async fn register_namespace_struct<T, R>(
    registry: &R,
    namespace: NamespaceId,
    field_meta: impl IntoIterator<Item = (KeyPath, FieldMeta)>,
) -> Result<(), ConfigError>
where
    T: schemars::JsonSchema,
    R: SchemaRegistry + ?Sized,
{
    let root_schema = schemars::gen::SchemaGenerator::default().into_root_schema_for::<T>();
    let field_meta = field_meta.into_iter().collect::<HashMap<_, _>>();
    registry
        .register_namespace(&namespace, root_schema, field_meta)
        .await
}
