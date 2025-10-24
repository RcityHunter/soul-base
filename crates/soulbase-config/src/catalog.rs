use crate::errors::ConfigError;
use crate::model::{KeyPath, NamespaceId, ReloadClass};
use crate::schema::{FieldMeta, InMemorySchemaRegistry, NamespaceView, SchemaRegistry};
use crate::validate::{SchemaValidator, Validator};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default)]
pub struct LlmBudget {
    pub id: String,
    pub daily_usd: f64,
    pub monthly_usd: f64,
    #[serde(default)]
    pub window_seconds: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default)]
pub struct LlmProvider {
    pub endpoint: String,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default)]
    pub auth_secret: Option<String>,
    #[serde(default)]
    pub telemetry_bucket: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default)]
pub struct LlmNamespace {
    pub default_provider: String,
    #[serde(default)]
    pub providers: BTreeMap<String, LlmProvider>,
    #[serde(default)]
    pub budgets: Vec<LlmBudget>,
    #[serde(default)]
    pub allow_tool_plan: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default)]
pub struct NetRetryPolicy {
    #[serde(default = "NetRetryPolicy::default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "NetRetryPolicy::default_backoff_ms")]
    pub backoff_ms: u64,
    #[serde(default)]
    pub jitter: bool,
}

impl NetRetryPolicy {
    fn default_max_attempts() -> u32 {
        3
    }

    fn default_backoff_ms() -> u64 {
        250
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default)]
pub struct NetRateLimit {
    #[serde(default)]
    pub tokens_per_second: Option<u32>,
    #[serde(default)]
    pub burst: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default)]
pub struct NetNamespace {
    #[serde(default)]
    pub retry: NetRetryPolicy,
    #[serde(default)]
    pub rate_limit: NetRateLimit,
    #[serde(default)]
    pub request_signer_secret: Option<String>,
    #[serde(default)]
    pub allow_insecure: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default)]
pub struct ObserveTracing {
    #[serde(default)]
    pub sample_ratio: f64,
    #[serde(default)]
    pub exporter: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default)]
pub struct ObserveMetrics {
    #[serde(default)]
    pub prometheus_endpoint: Option<String>,
    #[serde(default)]
    pub retention_days: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default)]
pub struct ObserveNamespace {
    #[serde(default)]
    pub tracing: ObserveTracing,
    #[serde(default)]
    pub metrics: ObserveMetrics,
    #[serde(default)]
    pub audit_topic: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default)]
pub struct ConfigCatalog {
    #[serde(default)]
    pub snapshot_dir: String,
    #[serde(default)]
    pub changelog_file: Option<String>,
    #[serde(default)]
    pub signer_key: Option<String>,
}

pub struct CatalogHandles {
    pub registry: Arc<dyn SchemaRegistry>,
    pub validator: Arc<SchemaValidator>,
}

#[derive(Debug, Serialize)]
pub struct FieldDoc {
    pub path: String,
    pub reload: ReloadClass,
    pub sensitive: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct NamespaceDoc {
    pub namespace: String,
    pub schema: schemars::schema::RootSchema,
    pub fields: Vec<FieldDoc>,
}

pub async fn bootstrap_catalog() -> Result<CatalogHandles, ConfigError> {
    let registry = Arc::new(InMemorySchemaRegistry::new());
    register_default_catalog(&*registry).await?;
    let validator = Arc::new(SchemaValidator::new(registry.clone()));
    Ok(CatalogHandles {
        registry,
        validator,
    })
}

pub async fn register_default_catalog<R>(registry: &R) -> Result<(), ConfigError>
where
    R: SchemaRegistry + ?Sized,
{
    register_llm_namespace(registry).await?;
    register_net_namespace(registry).await?;
    register_observe_namespace(registry).await?;
    register_catalog_namespace(registry).await?;
    Ok(())
}

async fn register_llm_namespace<R>(registry: &R) -> Result<(), ConfigError>
where
    R: SchemaRegistry + ?Sized,
{
    use crate::schema::register_namespace_struct;

    register_namespace_struct::<LlmNamespace, _>(
        registry,
        NamespaceId::new("llm"),
        vec![
            (
                KeyPath::new("llm.default_provider"),
                FieldMeta::new(ReloadClass::BootOnly)
                    .with_description("Default provider id used for chat APIs"),
            ),
            (
                KeyPath::new("llm.providers"),
                FieldMeta::new(ReloadClass::HotReloadRisky)
                    .with_description("Configured upstream providers and credentials")
                    .with_sensitive(true),
            ),
            (
                KeyPath::new("llm.budgets"),
                FieldMeta::new(ReloadClass::HotReloadSafe)
                    .with_description("Budget definitions for usage accounting"),
            ),
            (
                KeyPath::new("llm.allow_tool_plan"),
                FieldMeta::new(ReloadClass::HotReloadSafe)
                    .with_default(Value::Bool(true))
                    .with_description("Whether to enable LLM tool planning APIs"),
            ),
        ],
    )
    .await
}

async fn register_net_namespace<R>(registry: &R) -> Result<(), ConfigError>
where
    R: SchemaRegistry + ?Sized,
{
    use crate::schema::register_namespace_struct;

    register_namespace_struct::<NetNamespace, _>(
        registry,
        NamespaceId::new("net"),
        vec![
            (
                KeyPath::new("net.retry"),
                FieldMeta::new(ReloadClass::HotReloadSafe)
                    .with_description("Retry backoff strategy for outbound requests"),
            ),
            (
                KeyPath::new("net.rate_limit"),
                FieldMeta::new(ReloadClass::HotReloadSafe)
                    .with_description("Client side token bucket for outbound calls"),
            ),
            (
                KeyPath::new("net.request_signer_secret"),
                FieldMeta::new(ReloadClass::BootOnly)
                    .mark_sensitive()
                    .with_description("Secret reference used to sign outbound requests"),
            ),
            (
                KeyPath::new("net.allow_insecure"),
                FieldMeta::new(ReloadClass::BootOnly)
                    .with_default(Value::Bool(false))
                    .with_description("Allow HTTP endpoints and skip TLS verification"),
            ),
        ],
    )
    .await
}

async fn register_observe_namespace<R>(registry: &R) -> Result<(), ConfigError>
where
    R: SchemaRegistry + ?Sized,
{
    use crate::schema::register_namespace_struct;

    register_namespace_struct::<ObserveNamespace, _>(
        registry,
        NamespaceId::new("observe"),
        vec![
            (
                KeyPath::new("observe.tracing.sample_ratio"),
                FieldMeta::new(ReloadClass::HotReloadSafe)
                    .with_default(Value::Number(serde_json::Number::from_f64(1.0).unwrap()))
                    .with_description("Sampling ratio for distributed tracing"),
            ),
            (
                KeyPath::new("observe.tracing.exporter"),
                FieldMeta::new(ReloadClass::BootOnly)
                    .with_description("Exporter endpoint for OTLP spans"),
            ),
            (
                KeyPath::new("observe.metrics.prometheus_endpoint"),
                FieldMeta::new(ReloadClass::BootOnly).with_description("Prometheus pull endpoint"),
            ),
            (
                KeyPath::new("observe.metrics.retention_days"),
                FieldMeta::new(ReloadClass::HotReloadSafe)
                    .with_description("Number of days to retain metric snapshots"),
            ),
            (
                KeyPath::new("observe.audit_topic"),
                FieldMeta::new(ReloadClass::HotReloadRisky)
                    .with_description("Kafka topic used for audit trail fan-out"),
            ),
        ],
    )
    .await
}

async fn register_catalog_namespace<R>(registry: &R) -> Result<(), ConfigError>
where
    R: SchemaRegistry + ?Sized,
{
    use crate::schema::register_namespace_struct;

    register_namespace_struct::<ConfigCatalog, _>(
        registry,
        NamespaceId::new("catalog"),
        vec![
            (
                KeyPath::new("catalog.snapshot_dir"),
                FieldMeta::new(ReloadClass::BootOnly)
                    .with_default(Value::String("var/config/snapshots".into()))
                    .with_description("Directory where validated snapshots are stored"),
            ),
            (
                KeyPath::new("catalog.changelog_file"),
                FieldMeta::new(ReloadClass::BootOnly)
                    .with_description("Optional file that records change history"),
            ),
            (
                KeyPath::new("catalog.signer_key"),
                FieldMeta::new(ReloadClass::BootOnly)
                    .mark_sensitive()
                    .with_description("Secret reference used to sign snapshot manifests"),
            ),
        ],
    )
    .await
}

pub async fn export_namespace_docs(
    registry: &Arc<dyn SchemaRegistry>,
) -> Result<Vec<NamespaceDoc>, ConfigError> {
    let mut docs = Vec::new();
    let entries = registry.list_namespaces().await;
    for (ns, view) in entries {
        docs.push(namespace_doc(ns, view));
    }
    Ok(docs)
}

fn namespace_doc(namespace: NamespaceId, view: NamespaceView) -> NamespaceDoc {
    let fields = view
        .field_meta
        .into_iter()
        .map(|(path, meta)| FieldDoc {
            path: path.0,
            reload: meta.reload,
            sensitive: meta.sensitive,
            default_value: meta.default_value,
            description: meta.description,
        })
        .collect::<Vec<_>>();
    NamespaceDoc {
        namespace: namespace.0,
        schema: view.json_schema,
        fields,
    }
}

pub fn validator_as_trait(validator: &Arc<SchemaValidator>) -> Arc<dyn Validator> {
    validator.clone()
}
