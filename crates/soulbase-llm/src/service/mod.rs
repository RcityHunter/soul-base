mod infra;
mod outbox;
mod runtime;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    convert::Infallible,
    fs,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};

#[cfg(feature = "provider-gemini")]
use std::time::Duration;

use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, Request, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{delete, get, post},
    Router,
};
use chrono::{DateTime, Utc};
use futures_util::future::FutureExt;
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use soulbase_errors::prelude::codes;
#[cfg(any(
    feature = "provider-openai",
    feature = "provider-claude",
    feature = "provider-gemini"
))]
use soulbase_errors::prelude::PublicErrorView;
use soulbase_sandbox::prelude::{PolicyConfig, Sandbox};
use soulbase_tools::errors::ToolError;
use soulbase_tools::prelude::{
    InvokeRequest, InvokeStatus, Invoker, InvokerImpl, SurrealToolRegistry,
    ToolCall as ToolInvocation, ToolId, ToolOrigin, ToolRegistry,
};
use soulbase_types::prelude::TenantId;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use soulbase_auth::authn::oidc::{JwkConfig, OidcAuthenticator, OidcAuthenticatorStub, OidcConfig};
use soulbase_auth::model::Action;
use soulbase_auth::pdp::policy::{PolicyEffect, PolicyRule, PolicySet};
use soulbase_auth::quota::store::QuotaConfig;
use soulbase_auth::{prelude::Authenticator, AuthFacade};
use soulbase_interceptors::adapters::http::{handle_with_chain, AxumReq, AxumRes};
use soulbase_interceptors::context::InterceptContext;
use soulbase_interceptors::errors::InterceptError;
use soulbase_interceptors::prelude::*;
use soulbase_observe::prelude::{
    LogBuilder, LogLevel, Logger, Meter, MetricKind, MetricSpec, NoopRedactor, ObserveCtx,
    ObserveError, PrometheusEvidence, PrometheusHub,
};
use soulbase_storage::surreal::{
    schema_migrations, SurrealConfig, SurrealDatastore, SurrealSession, TABLE_LLM_EXPLAIN,
    TABLE_LLM_TOOL_PLAN,
};
use soulbase_storage::Migrator;
use soulbase_storage::{Datastore, QueryExecutor};
use soulbase_tx::prelude::SurrealTxStores;

use crate::prelude::*;
#[cfg(feature = "provider-claude")]
use crate::{ClaudeConfig, ClaudeProviderFactory};
#[cfg(feature = "provider-gemini")]
use crate::{GeminiConfig, GeminiProviderFactory};
use crate::{LocalProviderFactory, Registry};
#[cfg(feature = "provider-openai")]
use crate::{OpenAiConfig, OpenAiProviderFactory};
use infra::InfraManager;
use outbox::{OutboxDispatcher, OutboxInvoker};
use runtime::RuntimeConfig;

#[derive(Clone)]
pub struct AppState {
    registry: Arc<RwLock<Registry>>,
    policy: StructOutPolicy,
    chain: Arc<InterceptorChain>,
    meter: Arc<dyn Meter>,
    logger: Arc<dyn Logger>,
    plan_store: Arc<dyn PlanStore>,
    #[allow(dead_code)]
    tool_registry: Arc<dyn ToolRegistry>,
    tool_invoker: Arc<dyn Invoker>,
    explain_store: Arc<dyn ExplainStore>,
    observe: Arc<ObserveSystem>,
    runtime: Arc<RuntimeConfig>,
    infra: Arc<InfraManager>,
}

#[async_trait]
trait PlanStore: Send + Sync {
    async fn record(
        &self,
        tenant: &TenantId,
        key: &str,
        plan: &[ToolCallProposal],
    ) -> Result<(), LlmError>;
    async fn get(
        &self,
        tenant: &TenantId,
        key: &str,
    ) -> Result<Option<Vec<ToolCallProposal>>, LlmError>;
    async fn list(&self, tenant: &TenantId) -> Result<Vec<PlanSummary>, LlmError>;
    async fn remove(&self, tenant: &TenantId, key: &str) -> Result<bool, LlmError>;
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Default)]
struct MemoryPlanStore {
    inner: Arc<Mutex<HashMap<(String, String), Vec<ToolCallProposal>>>>,
}

#[async_trait]
impl PlanStore for MemoryPlanStore {
    async fn record(
        &self,
        tenant: &TenantId,
        key: &str,
        plan: &[ToolCallProposal],
    ) -> Result<(), LlmError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| LlmError::unknown("plan store poisoned"))?;
        guard.insert((tenant.0.clone(), key.to_string()), plan.to_vec());
        Ok(())
    }

    async fn get(
        &self,
        tenant: &TenantId,
        key: &str,
    ) -> Result<Option<Vec<ToolCallProposal>>, LlmError> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| LlmError::unknown("plan store poisoned"))?;
        Ok(guard.get(&(tenant.0.clone(), key.to_string())).cloned())
    }

    async fn list(&self, tenant: &TenantId) -> Result<Vec<PlanSummary>, LlmError> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| LlmError::unknown("plan store poisoned"))?;
        Ok(guard
            .iter()
            .filter_map(|((tenant_id, plan_id), steps)| {
                if tenant_id == &tenant.0 {
                    Some(PlanSummary {
                        plan_id: plan_id.clone(),
                        step_count: steps.len() as u32,
                    })
                } else {
                    None
                }
            })
            .collect())
    }

    async fn remove(&self, tenant: &TenantId, key: &str) -> Result<bool, LlmError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| LlmError::unknown("plan store poisoned"))?;
        Ok(guard.remove(&(tenant.0.clone(), key.to_string())).is_some())
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl MemoryPlanStore {
    fn record_sync(&self, tenant: &TenantId, key: &str, plan: Vec<ToolCallProposal>) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.insert((tenant.0.clone(), key.to_string()), plan);
        }
    }

    fn get_sync(&self, tenant: &TenantId, key: &str) -> Option<Vec<ToolCallProposal>> {
        self.inner
            .lock()
            .ok()
            .and_then(|guard| guard.get(&(tenant.0.clone(), key.to_string())).cloned())
    }

    fn remove_sync(&self, tenant: &TenantId, key: &str) -> bool {
        self.inner
            .lock()
            .map(|mut guard| guard.remove(&(tenant.0.clone(), key.to_string())).is_some())
            .unwrap_or(false)
    }
}

struct SurrealPlanStore {
    datastore: Arc<SurrealDatastore>,
}

impl SurrealPlanStore {
    fn new(datastore: Arc<SurrealDatastore>) -> Self {
        Self { datastore }
    }

    fn record_id(tenant: &TenantId, plan: &str) -> String {
        format!("{}:{}", tenant.0, plan)
    }

    async fn session(&self) -> Result<SurrealSession, LlmError> {
        self.datastore
            .session()
            .await
            .map_err(|err| LlmError::unknown(&format!("open surreal session: {err}")))
    }
}

#[async_trait]
impl PlanStore for SurrealPlanStore {
    async fn record(
        &self,
        tenant: &TenantId,
        key: &str,
        plan: &[ToolCallProposal],
    ) -> Result<(), LlmError> {
        let session = self.session().await?;
        let now_ms = Utc::now().timestamp_millis();
        session
            .query(
                "UPSERT type::thing($table, $id) CONTENT {
                    tenant: $tenant,
                    plan_id: $plan,
                    step_count: $step_count,
                    steps: $steps,
                    created_at: $now,
                    updated_at: $now
                } RETURN NONE",
                json!({
                    "table": TABLE_LLM_TOOL_PLAN,
                    "id": Self::record_id(tenant, key),
                    "tenant": tenant.0,
                    "plan": key,
                    "step_count": plan.len() as u32,
                    "steps": plan,
                    "now": now_ms,
                }),
            )
            .await
            .map_err(|err| LlmError::unknown(&format!("store tool plan: {err}")))?;
        Ok(())
    }

    async fn get(
        &self,
        tenant: &TenantId,
        key: &str,
    ) -> Result<Option<Vec<ToolCallProposal>>, LlmError> {
        let session = self.session().await?;
        let value = session
            .query(
                "SELECT steps FROM llm_tool_plan WHERE tenant = $tenant AND plan_id = $plan LIMIT 1",
                json!({
                    "tenant": tenant.0,
                    "plan": key,
                }),
            )
            .await
            .map_err(|err| LlmError::unknown(&format!("load tool plan: {err}")))?;
        match value {
            serde_json::Value::Array(mut rows) => {
                if let Some(serde_json::Value::Object(row)) = rows.pop() {
                    if let Some(steps) = row.get("steps") {
                        let plan: Vec<ToolCallProposal> = serde_json::from_value(steps.clone())
                            .map_err(|err| {
                                LlmError::unknown(&format!("decode tool plan steps: {err}"))
                            })?;
                        return Ok(Some(plan));
                    }
                }
                Ok(None)
            }
            other => Err(LlmError::unknown(&format!(
                "unexpected plan fetch response: {other}"
            ))),
        }
    }

    async fn list(&self, tenant: &TenantId) -> Result<Vec<PlanSummary>, LlmError> {
        let session = self.session().await?;
        let value = session
            .query(
                "SELECT plan_id, step_count FROM llm_tool_plan WHERE tenant = $tenant ORDER BY updated_at DESC",
                json!({
                    "tenant": tenant.0,
                }),
            )
            .await
            .map_err(|err| LlmError::unknown(&format!("list tool plans: {err}")))?;
        match value {
            serde_json::Value::Array(rows) => {
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    if let serde_json::Value::Object(map) = row {
                        if let (Some(plan_id), Some(step_count)) =
                            (map.get("plan_id"), map.get("step_count"))
                        {
                            let plan_id = plan_id.as_str().unwrap_or_default().to_string();
                            let step_count = step_count.as_u64().unwrap_or_default() as u32;
                            out.push(PlanSummary {
                                plan_id,
                                step_count,
                            });
                        }
                    }
                }
                Ok(out)
            }
            other => Err(LlmError::unknown(&format!(
                "unexpected plan list response: {other}"
            ))),
        }
    }

    async fn remove(&self, tenant: &TenantId, key: &str) -> Result<bool, LlmError> {
        let session = self.session().await?;
        let value = session
            .query(
                "DELETE type::thing($table, $id) RETURN BEFORE",
                json!({
                    "table": TABLE_LLM_TOOL_PLAN,
                    "id": Self::record_id(tenant, key),
                }),
            )
            .await
            .map_err(|err| LlmError::unknown(&format!("delete tool plan: {err}")))?;
        match value {
            serde_json::Value::Array(rows) => Ok(!rows.is_empty()),
            other => Err(LlmError::unknown(&format!(
                "unexpected plan delete response: {other}"
            ))),
        }
    }
}

#[async_trait]
trait ExplainStore: Send + Sync {
    async fn record(
        &self,
        tenant: &TenantId,
        plan_id: Option<&str>,
        provider: &str,
        fingerprint: Option<String>,
        metadata: serde_json::Value,
    ) -> Result<String, LlmError>;
    async fn get(&self, explain_id: &str) -> Result<Option<ExplainRecord>, LlmError>;
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Default)]
struct MemoryExplainStore {
    inner: Arc<Mutex<HashMap<String, ExplainRecord>>>,
}

#[async_trait]
impl ExplainStore for MemoryExplainStore {
    async fn record(
        &self,
        tenant: &TenantId,
        plan_id: Option<&str>,
        provider: &str,
        fingerprint: Option<String>,
        metadata: serde_json::Value,
    ) -> Result<String, LlmError> {
        let explain_id = format!(
            "exp_{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        let record = ExplainRecord {
            explain_id: explain_id.clone(),
            provider: provider.to_string(),
            fingerprint,
            tenant: tenant.clone(),
            plan_id: plan_id.map(|s| s.to_string()),
            created_at: Utc::now(),
            metadata,
        };
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| LlmError::unknown("explain store poisoned"))?;
        guard.insert(explain_id.clone(), record);
        Ok(explain_id)
    }

    async fn get(&self, explain_id: &str) -> Result<Option<ExplainRecord>, LlmError> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| LlmError::unknown("explain store poisoned"))?;
        Ok(guard.get(explain_id).cloned())
    }
}

struct SurrealExplainStore {
    datastore: Arc<SurrealDatastore>,
}

impl SurrealExplainStore {
    fn new(datastore: Arc<SurrealDatastore>) -> Self {
        Self { datastore }
    }

    fn record_id(tenant: &TenantId, explain: &str) -> String {
        format!("{}:{}", tenant.0, explain)
    }

    async fn session(&self) -> Result<SurrealSession, LlmError> {
        self.datastore
            .session()
            .await
            .map_err(|err| LlmError::unknown(&format!("open surreal session: {err}")))
    }
}

#[async_trait]
impl ExplainStore for SurrealExplainStore {
    async fn record(
        &self,
        tenant: &TenantId,
        plan_id: Option<&str>,
        provider: &str,
        fingerprint: Option<String>,
        metadata: serde_json::Value,
    ) -> Result<String, LlmError> {
        let session = self.session().await?;
        let now = Utc::now();
        let explain_id = format!("exp_{}", now.timestamp_nanos_opt().unwrap_or_default());
        session
            .query(
                "UPSERT type::thing($table, $id) CONTENT {
                    tenant: $tenant,
                    explain_id: $explain,
                    provider: $provider,
                    fingerprint: $fingerprint,
                    plan_id: $plan_id,
                    metadata: $metadata,
                    created_at: $created_at
                } RETURN NONE",
                json!({
                    "table": TABLE_LLM_EXPLAIN,
                    "id": Self::record_id(tenant, &explain_id),
                    "tenant": tenant.0,
                    "explain": explain_id,
                    "provider": provider,
                    "fingerprint": fingerprint,
                    "plan_id": plan_id,
                    "metadata": metadata,
                    "created_at": now.to_rfc3339(),
                }),
            )
            .await
            .map_err(|err| LlmError::unknown(&format!("store explain record: {err}")))?;
        Ok(explain_id)
    }

    async fn get(&self, explain_id: &str) -> Result<Option<ExplainRecord>, LlmError> {
        let session = self.session().await?;
        let value = session
            .query(
                "SELECT explain_id, provider, fingerprint, plan_id, metadata, created_at, tenant \
                 FROM llm_explain WHERE explain_id = $explain LIMIT 1",
                json!({ "explain": explain_id }),
            )
            .await
            .map_err(|err| LlmError::unknown(&format!("load explain record: {err}")))?;
        match value {
            serde_json::Value::Array(mut rows) => {
                if let Some(serde_json::Value::Object(map)) = rows.pop() {
                    let explain_id = map
                        .get("explain_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| LlmError::unknown("explain record missing id"))?
                        .to_string();
                    let provider = map
                        .get("provider")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| LlmError::unknown("explain record missing provider"))?
                        .to_string();
                    let fingerprint = map
                        .get("fingerprint")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let plan_id = map
                        .get("plan_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let metadata = map
                        .get("metadata")
                        .cloned()
                        .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
                    let tenant = map
                        .get("tenant")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| LlmError::unknown("explain record missing tenant"))?;
                    let created_at_str = map
                        .get("created_at")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| LlmError::unknown("explain record missing timestamp"))?;
                    let created_at = chrono::DateTime::parse_from_rfc3339(created_at_str)
                        .map_err(|err| {
                            LlmError::unknown(&format!("parse explain timestamp: {err}"))
                        })?
                        .with_timezone(&Utc);

                    Ok(Some(ExplainRecord {
                        explain_id,
                        provider,
                        fingerprint,
                        tenant: TenantId(tenant.to_string()),
                        plan_id,
                        created_at,
                        metadata,
                    }))
                } else {
                    Ok(None)
                }
            }
            other => Err(LlmError::unknown(&format!(
                "unexpected explain fetch response: {other}"
            ))),
        }
    }
}

#[derive(Clone)]
struct ObserveSystem {
    hub: Arc<PrometheusHub>,
    meter: Arc<dyn Meter>,
    logger: Arc<dyn Logger>,
    _evidence: PrometheusEvidence,
}

impl ObserveSystem {
    fn new() -> Self {
        let hub = Arc::new(PrometheusHub::new());
        let meter = hub.meter();
        let logger = hub.logger();
        let evidence = hub.evidence();
        Self {
            hub,
            meter: Arc::new(meter),
            logger: Arc::new(logger),
            _evidence: evidence,
        }
    }

    fn meter(&self) -> Arc<dyn Meter> {
        self.meter.clone()
    }

    fn logger(&self) -> Arc<dyn Logger> {
        self.logger.clone()
    }

    fn gather_metrics(&self) -> Result<String, ObserveError> {
        self.hub.gather()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExplainRecord {
    explain_id: String,
    provider: String,
    fingerprint: Option<String>,
    tenant: TenantId,
    plan_id: Option<String>,
    created_at: DateTime<Utc>,
    metadata: serde_json::Value,
}

struct AuthWiring {
    authenticator: Box<dyn Authenticator>,
    facade: Arc<AuthFacade>,
}

async fn init_surreal_datastore() -> anyhow::Result<Arc<SurrealDatastore>> {
    let endpoint = std::env::var("SURREAL_ENDPOINT").unwrap_or_else(|_| "mem://".into());
    let namespace = std::env::var("SURREAL_NAMESPACE").unwrap_or_else(|_| "soul".into());
    let database = std::env::var("SURREAL_DATABASE").unwrap_or_else(|_| "default".into());

    let mut config = SurrealConfig::new(endpoint, namespace, database);
    let username = std::env::var("SURREAL_USERNAME").ok();
    let password = std::env::var("SURREAL_PASSWORD").ok();
    if let (Some(user), Some(pass)) = (username, password) {
        config = config.with_credentials(user, pass);
    }

    let datastore = SurrealDatastore::connect(config)
        .await
        .context("connect surreal datastore")?;

    if let Err(err) = datastore
        .migrator()
        .apply_up(&schema_migrations())
        .await
        .context("apply core surreal migrations")
    {
        warn!(
            target = "soulbase::llm",
            "failed to apply surreal schema migrations: {err:?}"
        );
    }

    Ok(Arc::new(datastore))
}

fn load_oidc_config_from_env() -> anyhow::Result<Option<OidcConfig>> {
    let issuer = match std::env::var("LLM_OIDC_ISSUER") {
        Ok(v) if !v.is_empty() => v,
        _ => return Ok(None),
    };

    let mut builder = OidcConfig::builder(issuer);

    if let Ok(audiences) = std::env::var("LLM_OIDC_AUDIENCES") {
        for aud in audiences
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            builder = builder.audience(aud.to_string());
        }
    }

    if let Ok(claim) = std::env::var("LLM_OIDC_TENANT_CLAIM") {
        if !claim.is_empty() {
            builder = builder.tenant_claim(claim);
        }
    }

    if let Ok(claim) = std::env::var("LLM_OIDC_SUBJECT_CLAIM") {
        if !claim.is_empty() {
            builder = builder.subject_claim(claim);
        }
    }

    if let Ok(claim) = std::env::var("LLM_OIDC_ROLE_CLAIM") {
        if !claim.is_empty() {
            builder = builder.role_claim(claim);
        }
    }

    if let Ok(jwks_uri) = std::env::var("LLM_OIDC_JWKS_URL") {
        if !jwks_uri.is_empty() {
            builder = builder.jwks_uri(jwks_uri);
        }
    } else if let Ok(keys_json) = std::env::var("LLM_OIDC_JWKS_STATIC") {
        let keys: Vec<JwkConfig> =
            serde_json::from_str(&keys_json).context("decode LLM_OIDC_JWKS_STATIC as JWK set")?;
        builder = builder.static_keys(keys);
    } else {
        return Err(anyhow!(
            "missing LLM_OIDC_JWKS_URL or LLM_OIDC_JWKS_STATIC for OIDC configuration"
        ));
    }

    Ok(Some(builder.build()))
}

fn read_policy_from_path(path: &Path) -> anyhow::Result<Vec<PolicyRule>> {
    let bytes = fs::read(path).with_context(|| format!("read policy file {}", path.display()))?;
    let set: PolicySet =
        serde_json::from_slice(&bytes).context("decode policy file into PolicySet")?;
    Ok(set.rules)
}

fn load_policy_rules_from_env() -> anyhow::Result<Option<Vec<PolicyRule>>> {
    if let Ok(path) = std::env::var("LLM_POLICY_PATH") {
        let rules = read_policy_from_path(Path::new(&path))?;
        return Ok(Some(rules));
    }

    if let Ok(json) = std::env::var("LLM_POLICY_JSON") {
        let set: PolicySet =
            serde_json::from_str(&json).context("decode LLM_POLICY_JSON into PolicySet")?;
        return Ok(Some(set.rules));
    }

    Ok(None)
}

fn build_default_allow_policy() -> Vec<PolicyRule> {
    vec![PolicyRule {
        id: "allow-all-service".into(),
        description: Some("Temporary allow-all policy for LLM routes".into()),
        resources: vec![
            "urn:llm:chat".into(),
            "urn:llm:embed".into(),
            "urn:llm:rerank".into(),
            "urn:llm:plan".into(),
            "urn:llm:explain".into(),
        ],
        actions: vec![Action::Invoke, Action::Read, Action::List, Action::Write],
        effect: PolicyEffect::Allow,
        conditions: Vec::new(),
        obligations: Vec::new(),
    }]
}

fn build_auth_wiring(datastore: Option<&Arc<SurrealDatastore>>) -> anyhow::Result<AuthWiring> {
    let oidc_cfg = match load_oidc_config_from_env()? {
        Some(cfg) => cfg,
        None => {
            warn!(
                target = "soulbase::llm",
                "LLM_OIDC_ISSUER not set; using stub authenticator"
            );
            return Ok(AuthWiring {
                authenticator: Box::new(OidcAuthenticatorStub),
                facade: Arc::new(AuthFacade::minimal()),
            });
        }
    };

    let rules = load_policy_rules_from_env()?.unwrap_or_else(|| {
        warn!(
            target = "soulbase::llm",
            "LLM_POLICY_PATH/LLM_POLICY_JSON not provided; applying permissive default policy"
        );
        build_default_allow_policy()
    });

    let facade = AuthFacade::with_oidc_and_policy(oidc_cfg.clone(), rules)
        .context("initialise AuthFacade with OIDC policy")?;

    let facade = if let Some(store) = datastore {
        facade.with_surreal_stores(store, QuotaConfig::default())
    } else {
        facade
    };

    let authenticator =
        OidcAuthenticator::new(oidc_cfg).context("build OIDC authenticator for interceptors")?;

    Ok(AuthWiring {
        authenticator: Box::new(authenticator),
        facade: Arc::new(facade),
    })
}

#[derive(Deserialize)]
struct ChatPayload {
    model_id: String,
    messages: Vec<Message>,
    #[serde(default)]
    tool_specs: Vec<ToolSpec>,
    #[serde(default)]
    temperature: Option<f32>,
    #[serde(default)]
    top_p: Option<f32>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default)]
    stop: Vec<String>,
    #[serde(default)]
    seed: Option<u64>,
    #[serde(default)]
    frequency_penalty: Option<f32>,
    #[serde(default)]
    presence_penalty: Option<f32>,
    #[serde(default)]
    logit_bias: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    response_format: Option<ResponseFormat>,
    #[serde(default)]
    allow_sensitive: bool,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    metadata: serde_json::Value,
}

impl ChatPayload {
    fn into_request(self) -> ChatRequest {
        ChatRequest {
            model_id: self.model_id,
            messages: self.messages,
            tool_specs: self.tool_specs,
            temperature: self.temperature,
            top_p: self.top_p,
            max_tokens: self.max_tokens,
            stop: self.stop,
            seed: self.seed,
            frequency_penalty: self.frequency_penalty,
            presence_penalty: self.presence_penalty,
            logit_bias: self.logit_bias,
            response_format: self.response_format,
            idempotency_key: self.idempotency_key,
            cache_hint: None,
            allow_sensitive: self.allow_sensitive,
            metadata: self.metadata,
        }
    }
}

#[derive(Deserialize)]
struct EmbedPayload {
    model_id: String,
    items: Vec<EmbedItem>,
    #[serde(default)]
    normalize: bool,
    #[serde(default)]
    pooling: Option<String>,
}

impl EmbedPayload {
    fn into_request(self) -> EmbedRequest {
        EmbedRequest {
            model_id: self.model_id,
            items: self.items,
            normalize: self.normalize,
            pooling: self.pooling,
        }
    }
}

#[derive(Deserialize)]
struct RerankPayload {
    model_id: String,
    query: String,
    candidates: Vec<String>,
}

impl RerankPayload {
    fn into_request(self) -> RerankRequest {
        RerankRequest {
            model_id: self.model_id,
            query: self.query,
            candidates: self.candidates,
        }
    }
}

#[derive(Serialize)]
struct ErrorEvent<'a> {
    code: &'a str,
    message: &'a str,
}

const CHAT_REQUESTS_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_llm_chat_requests_total",
    kind: MetricKind::Counter,
    help: "Total chat invocations handled by the LLM service",
    buckets_ms: None,
    stable_labels: &[],
};

const CHAT_INPUT_TOKENS_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_llm_chat_input_tokens_total",
    kind: MetricKind::Counter,
    help: "Total chat input tokens consumed",
    buckets_ms: None,
    stable_labels: &[],
};

const CHAT_OUTPUT_TOKENS_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_llm_chat_output_tokens_total",
    kind: MetricKind::Counter,
    help: "Total chat output tokens produced",
    buckets_ms: None,
    stable_labels: &[],
};

const EMBED_REQUESTS_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_llm_embed_requests_total",
    kind: MetricKind::Counter,
    help: "Total embedding invocations handled by the LLM service",
    buckets_ms: None,
    stable_labels: &[],
};

const EMBED_ITEMS_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_llm_embed_items_total",
    kind: MetricKind::Counter,
    help: "Total embedding items processed",
    buckets_ms: None,
    stable_labels: &[],
};

const EMBED_INPUT_TOKENS_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_llm_embed_input_tokens_total",
    kind: MetricKind::Counter,
    help: "Total embedding input tokens (estimated)",
    buckets_ms: None,
    stable_labels: &[],
};

const RERANK_REQUESTS_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_llm_rerank_requests_total",
    kind: MetricKind::Counter,
    help: "Total rerank invocations handled by the LLM service",
    buckets_ms: None,
    stable_labels: &[],
};

const RERANK_INPUT_TOKENS_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_llm_rerank_input_tokens_total",
    kind: MetricKind::Counter,
    help: "Total rerank input tokens (estimated)",
    buckets_ms: None,
    stable_labels: &[],
};

#[derive(Clone, Default)]
struct RequestTelemetry {
    tenant: Option<String>,
    subject_kind: Option<String>,
    resource: Option<String>,
    action: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct PlanSummary {
    plan_id: String,
    step_count: u32,
}

fn extract_telemetry(ctx: &InterceptContext) -> RequestTelemetry {
    let tenant = ctx.subject.as_ref().map(|subject| subject.tenant.0.clone());
    let subject_kind = ctx
        .subject
        .as_ref()
        .map(|subject| format!("{:?}", subject.kind));
    let resource = ctx
        .route
        .as_ref()
        .map(|route| route.resource.0.clone())
        .filter(|s| !s.is_empty());
    let action = ctx
        .route
        .as_ref()
        .map(|route| format!("{:?}", route.action))
        .filter(|s| !s.is_empty());

    RequestTelemetry {
        tenant,
        subject_kind,
        resource,
        action,
    }
}

fn extract_plan_key(metadata: &serde_json::Value) -> Option<String> {
    match metadata {
        serde_json::Value::Object(map) => map
            .get("plan_id")
            .and_then(|value| value.as_str().map(|s| s.to_string())),
        _ => None,
    }
}

async fn record_tool_plan(
    store: &Arc<dyn PlanStore>,
    tenant: &TenantId,
    plan_key: Option<&str>,
    plan: &[ToolCallProposal],
) -> Result<(), LlmError> {
    if let Some(key) = plan_key {
        store.record(tenant, key, plan).await?;
    }
    Ok(())
}

fn telemetry_to_ctx(telemetry: &RequestTelemetry) -> Option<ObserveCtx> {
    telemetry.tenant.as_ref().map(|tenant| {
        let mut ctx = ObserveCtx::for_tenant(tenant.clone());
        ctx.subject_kind = telemetry.subject_kind.clone();
        ctx.resource = telemetry.resource.clone();
        ctx.action = telemetry.action.clone();
        ctx
    })
}

async fn record_chat_usage(
    meter: &Arc<dyn Meter>,
    logger: &Arc<dyn Logger>,
    telemetry: &RequestTelemetry,
    model_id: &str,
    usage: &Usage,
    source: &'static str,
) {
    let requests = u64::from(usage.requests.max(1));
    meter.counter(&CHAT_REQUESTS_TOTAL).inc(requests);
    meter
        .counter(&CHAT_INPUT_TOKENS_TOTAL)
        .inc(u64::from(usage.input_tokens));
    meter
        .counter(&CHAT_OUTPUT_TOKENS_TOTAL)
        .inc(u64::from(usage.output_tokens));

    let tenant = telemetry.tenant.as_deref().unwrap_or("unknown");
    let subject_kind = telemetry.subject_kind.as_deref().unwrap_or("n/a");
    let resource = telemetry.resource.as_deref().unwrap_or("n/a");
    let action = telemetry.action.as_deref().unwrap_or("n/a");

    info!(
        target: "soulbase_llm::usage",
        kind = "chat",
        source,
        tenant,
        subject_kind,
        resource,
        action,
        model_id,
        input_tokens = usage.input_tokens,
        output_tokens = usage.output_tokens,
        cached_tokens = usage.cached_tokens.unwrap_or(0),
        requests = usage.requests,
        "chat usage recorded"
    );

    if let Some(ctx) = telemetry_to_ctx(telemetry) {
        let mut builder = LogBuilder::new(LogLevel::Info, "llm chat usage")
            .label("model_id", model_id.to_string())
            .label("source", source)
            .field("input_tokens", json!(usage.input_tokens))
            .field("output_tokens", json!(usage.output_tokens))
            .field("requests", json!(usage.requests));
        if let Some(cached) = usage.cached_tokens {
            builder = builder.field("cached_tokens", json!(cached));
        }
        let event = builder.finish(&ctx, &NoopRedactor::default());
        logger.log(&ctx, event).await;
    }
}

async fn log_query_degradation(
    logger: &Arc<dyn Logger>,
    telemetry: &RequestTelemetry,
    meta: &Option<QueryStats>,
) {
    if let Some(stats) = meta {
        if let Some(reason) = stats.degradation_reason.as_ref() {
            let ctx = match telemetry_to_ctx(telemetry) {
                Some(ctx) => ctx,
                None => ObserveCtx::for_tenant("unknown".into()),
            };
            let indices = stats
                .indices_used
                .as_ref()
                .map(|v| json!(v))
                .unwrap_or_else(|| json!("<none>"));
            let event = LogBuilder::new(LogLevel::Warn, "storage query degraded")
                .field("reason", json!(reason))
                .field("full_scan", json!(stats.full_scan))
                .field("indices", indices)
                .finish(&ctx, &NoopRedactor::default());
            logger.log(&ctx, event).await;
        }
    }
}

async fn record_embed_usage(
    meter: &Arc<dyn Meter>,
    logger: &Arc<dyn Logger>,
    telemetry: &RequestTelemetry,
    model_id: &str,
    usage: &Usage,
    item_count: usize,
) {
    let requests = u64::from(usage.requests.max(1));
    meter.counter(&EMBED_REQUESTS_TOTAL).inc(requests);
    meter.counter(&EMBED_ITEMS_TOTAL).inc(item_count as u64);
    meter
        .counter(&EMBED_INPUT_TOKENS_TOTAL)
        .inc(u64::from(usage.input_tokens));

    let tenant = telemetry.tenant.as_deref().unwrap_or("unknown");
    let subject_kind = telemetry.subject_kind.as_deref().unwrap_or("n/a");
    let resource = telemetry.resource.as_deref().unwrap_or("n/a");
    let action = telemetry.action.as_deref().unwrap_or("n/a");

    info!(
        target: "soulbase_llm::usage",
        kind = "embed",
        tenant,
        subject_kind,
        resource,
        action,
        model_id,
        items = item_count,
        input_tokens = usage.input_tokens,
        requests = usage.requests,
        "embed usage recorded"
    );

    if let Some(ctx) = telemetry_to_ctx(telemetry) {
        let event = LogBuilder::new(LogLevel::Info, "llm embed usage")
            .label("model_id", model_id.to_string())
            .field("items", json!(item_count))
            .field("input_tokens", json!(usage.input_tokens))
            .field("requests", json!(usage.requests))
            .finish(&ctx, &NoopRedactor::default());
        logger.log(&ctx, event).await;
    }
}

async fn record_rerank_usage(
    meter: &Arc<dyn Meter>,
    logger: &Arc<dyn Logger>,
    telemetry: &RequestTelemetry,
    model_id: &str,
    usage: &Usage,
    candidate_count: usize,
) {
    let requests = u64::from(usage.requests.max(1));
    meter.counter(&RERANK_REQUESTS_TOTAL).inc(requests);
    meter
        .counter(&RERANK_INPUT_TOKENS_TOTAL)
        .inc(u64::from(usage.input_tokens));

    let tenant = telemetry.tenant.as_deref().unwrap_or("unknown");
    let subject_kind = telemetry.subject_kind.as_deref().unwrap_or("n/a");
    let resource = telemetry.resource.as_deref().unwrap_or("n/a");
    let action = telemetry.action.as_deref().unwrap_or("n/a");

    info!(
        target: "soulbase_llm::usage",
        kind = "rerank",
        tenant,
        subject_kind,
        resource,
        action,
        model_id,
        candidates = candidate_count,
        input_tokens = usage.input_tokens,
        output_tokens = usage.output_tokens,
        requests = usage.requests,
        "rerank usage recorded"
    );

    if let Some(ctx) = telemetry_to_ctx(telemetry) {
        let event = LogBuilder::new(LogLevel::Info, "llm rerank usage")
            .label("model_id", model_id.to_string())
            .field("candidates", json!(candidate_count))
            .field("input_tokens", json!(usage.input_tokens))
            .field("output_tokens", json!(usage.output_tokens))
            .finish(&ctx, &NoopRedactor::default());
        logger.log(&ctx, event).await;
    }
}

fn map_tool_error(err: ToolError) -> InterceptError {
    InterceptError::from_error(err.into_inner())
}

fn map_llm_error(err: LlmError) -> InterceptError {
    InterceptError::from_error(err.into_inner())
}

fn build_tooling(
    datastore: Arc<SurrealDatastore>,
    facade: Arc<AuthFacade>,
    observe: &Arc<ObserveSystem>,
    infra: Arc<InfraManager>,
) -> (Arc<dyn ToolRegistry>, Arc<dyn Invoker>) {
    let tool_registry: Arc<dyn ToolRegistry> =
        Arc::new(SurrealToolRegistry::new(datastore.clone()));
    let meter = observe.meter();
    let logger = observe.logger();
    let sandbox = Sandbox::minimal().with_telemetry(meter.clone(), logger.clone());
    let policy = PolicyConfig::default();
    let stores = SurrealTxStores::new(datastore);
    let outbox_store = Arc::new(stores.outbox());
    let dead_store = Arc::new(stores.dead());
    let dispatcher = Arc::new(OutboxDispatcher::new(
        infra,
        outbox_store.clone(),
        dead_store,
    ));
    let invoker_impl = InvokerImpl::new(tool_registry.clone(), facade, sandbox, policy)
        .with_observe(meter, logger)
        .with_outbox(outbox_store.clone());
    let invoker_impl = Arc::new(invoker_impl);
    let tool_invoker: Arc<dyn Invoker> =
        Arc::new(OutboxInvoker::new(invoker_impl.clone(), dispatcher));
    (tool_registry, tool_invoker)
}

fn create_app_state(
    plan_store: Arc<dyn PlanStore>,
    tool_registry: Arc<dyn ToolRegistry>,
    tool_invoker: Arc<dyn Invoker>,
    explain_store: Arc<dyn ExplainStore>,
    observe: Arc<ObserveSystem>,
    chain: InterceptorChain,
    runtime: Arc<RuntimeConfig>,
    infra: Arc<InfraManager>,
) -> AppState {
    let mut registry = Registry::new();
    install_providers(&mut registry);

    let meter = observe.meter();
    let logger = observe.logger();

    AppState {
        registry: Arc::new(RwLock::new(registry)),
        policy: StructOutPolicy::StrictReject,
        chain: Arc::new(chain),
        meter,
        logger,
        plan_store,
        tool_registry,
        tool_invoker,
        explain_store,
        observe,
        runtime,
        infra,
    }
}

pub async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let runtime = Arc::new(runtime::load_runtime_config().await?);
    let infra = Arc::new(InfraManager::new(runtime.infra.clone()));

    let state = default_state(runtime, infra).await?;
    let app = build_router(state);

    let listen = std::env::var("LLM_LISTEN").unwrap_or_else(|_| "127.0.0.1:9000".into());
    let addr: SocketAddr = listen.parse()?;
    info!("listening on {addr}");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/v1/chat", post(chat_handler))
        .route("/v1/chat/stream", post(chat_stream_handler))
        .route("/v1/embed", post(embed_handler))
        .route("/v1/rerank", post(rerank_handler))
        .route("/v1/plan", get(plan_list_handler))
        .route("/v1/plan/:plan_id", get(plan_handler))
        .route("/v1/plan/:plan_id/execute", post(plan_execute_handler))
        .route("/v1/plan/:plan_id", delete(plan_delete_handler))
        .route("/v1/explain/:explain_id", get(explain_handler))
        .route("/v1/explain/:explain_id/view", get(explain_view_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

pub async fn default_state(
    runtime: Arc<RuntimeConfig>,
    infra: Arc<InfraManager>,
) -> anyhow::Result<AppState> {
    let datastore = init_surreal_datastore().await?;
    let auth = build_auth_wiring(Some(&datastore))?;
    let observe = Arc::new(ObserveSystem::new());
    let plan_store: Arc<dyn PlanStore> = Arc::new(SurrealPlanStore::new(datastore.clone()));
    let explain_store: Arc<dyn ExplainStore> =
        Arc::new(SurrealExplainStore::new(datastore.clone()));
    let AuthWiring {
        authenticator,
        facade,
    } = auth;
    let facade_for_tools = facade.clone();
    let (tool_registry, tool_invoker) =
        build_tooling(datastore, facade_for_tools, &observe, infra.clone());
    let chain = build_chain(AuthWiring {
        authenticator,
        facade: facade.clone(),
    });
    Ok(create_app_state(
        plan_store,
        tool_registry,
        tool_invoker,
        explain_store,
        observe,
        chain,
        runtime,
        infra,
    ))
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    match state.observe.gather_metrics() {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/plain; version=0.0.4")
            .body(Body::from(body))
            .unwrap_or_else(|err| {
                intercept_error(InterceptError::internal(&format!(
                    "metrics response build failed: {err}"
                )))
            }),
        Err(err) => {
            let msg = format!("metrics gather failed: {err:?}");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(CONTENT_TYPE, "text/plain; charset=utf-8")
                .body(Body::from(msg))
                .unwrap_or_else(|err| {
                    intercept_error(InterceptError::internal(&format!(
                        "metrics error response build failed: {err}"
                    )))
                })
        }
    }
}

async fn chat_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let registry = state.registry.clone();
    let policy = state.policy.clone();
    let meter = state.meter.clone();
    let logger = state.logger.clone();
    let plan_store = state.plan_store.clone();
    let explain_store = state.explain_store.clone();

    handle_with_chain(req, &state.chain, move |ctx, preq| {
        let registry = registry.clone();
        let policy = policy.clone();
        let meter = meter.clone();
        let logger = logger.clone();
        let plan_store = plan_store.clone();
        let explain_store = explain_store.clone();
        let telemetry = extract_telemetry(ctx);
        Box::pin(async move {
            let value = preq.read_json().await?;
            let payload: ChatPayload = serde_json::from_value(value)
                .map_err(|err| InterceptError::schema(&format!("payload decode: {err}")))?;
            let plan_key = extract_plan_key(&payload.metadata);
            let req = payload.into_request();
            let model_id = req.model_id.clone();

            let model = {
                let registry_guard = registry.read().await;
                registry_guard.chat(&req.model_id)
            };
            let model = model.ok_or_else(|| InterceptError::deny_policy("model not registered"))?;

            let mut response = model
                .chat(req, &policy)
                .await
                .map_err(|err| InterceptError::from_error(err.into_inner()))?;

            record_chat_usage(
                &meter,
                &logger,
                &telemetry,
                &model_id,
                &response.usage,
                "sync",
            )
            .await;
            log_query_degradation(&logger, &telemetry, &response.usage.plan_meta).await;
            if let Some(ref tenant_id) = telemetry.tenant {
                let tenant = TenantId(tenant_id.to_string());
                record_tool_plan(
                    &plan_store,
                    &tenant,
                    plan_key.as_deref(),
                    &response.message.tool_calls,
                )
                .await
                .map_err(map_llm_error)?;

                let provider = response
                    .provider_meta
                    .get("provider")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let fingerprint = response
                    .provider_meta
                    .get("system_fingerprint")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let explain_id = explain_store
                    .record(
                        &tenant,
                        plan_key.as_deref(),
                        &provider,
                        fingerprint.clone(),
                        response.provider_meta.clone(),
                    )
                    .await
                    .map_err(map_llm_error)?;

                if let Some(obj) = response.provider_meta.as_object_mut() {
                    obj.insert("explain_id".into(), serde_json::Value::String(explain_id));
                }
            }

            serde_json::to_value(&response)
                .map_err(|err| InterceptError::internal(&format!("encode response: {err}")))
        })
    })
    .await
}

async fn embed_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let registry = state.registry.clone();
    let meter = state.meter.clone();
    let logger = state.logger.clone();

    handle_with_chain(req, &state.chain, move |ctx, preq| {
        let registry = registry.clone();
        let meter = meter.clone();
        let logger = logger.clone();
        let telemetry = extract_telemetry(ctx);
        Box::pin(async move {
            let value = preq.read_json().await?;
            let payload: EmbedPayload = serde_json::from_value(value)
                .map_err(|err| InterceptError::schema(&format!("payload decode: {err}")))?;
            let item_count = payload.items.len();
            let embed_req = payload.into_request();
            let model_id = embed_req.model_id.clone();

            let model = {
                let registry_guard = registry.read().await;
                registry_guard.embed(&model_id)
            };
            let model = model.ok_or_else(|| {
                InterceptError::deny_policy("model not registered or embeddings unsupported")
            })?;

            let response = model
                .embed(embed_req)
                .await
                .map_err(|err| InterceptError::from_error(err.into_inner()))?;

            record_embed_usage(
                &meter,
                &logger,
                &telemetry,
                &model_id,
                &response.usage,
                item_count,
            )
            .await;
            log_query_degradation(&logger, &telemetry, &response.usage.plan_meta).await;

            serde_json::to_value(response)
                .map_err(|err| InterceptError::internal(&format!("encode response: {err}")))
        })
    })
    .await
}

async fn rerank_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let registry = state.registry.clone();
    let meter = state.meter.clone();
    let logger = state.logger.clone();

    handle_with_chain(req, &state.chain, move |ctx, preq| {
        let registry = registry.clone();
        let meter = meter.clone();
        let logger = logger.clone();
        let telemetry = extract_telemetry(ctx);
        Box::pin(async move {
            let value = preq.read_json().await?;
            let payload: RerankPayload = serde_json::from_value(value)
                .map_err(|err| InterceptError::schema(&format!("payload decode: {err}")))?;
            let candidate_count = payload.candidates.len();
            let rerank_req = payload.into_request();
            let model_id = rerank_req.model_id.clone();

            let model = {
                let registry_guard = registry.read().await;
                registry_guard.rerank(&model_id)
            };
            let model = model.ok_or_else(|| {
                InterceptError::deny_policy("model not registered or rerank unsupported")
            })?;

            let response = model
                .rerank(rerank_req)
                .await
                .map_err(|err| InterceptError::from_error(err.into_inner()))?;

            record_rerank_usage(
                &meter,
                &logger,
                &telemetry,
                &model_id,
                &response.usage,
                candidate_count,
            )
            .await;

            serde_json::to_value(response)
                .map_err(|err| InterceptError::internal(&format!("encode response: {err}")))
        })
    })
    .await
}

async fn plan_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let store = state.plan_store.clone();

    handle_with_chain(req, &state.chain, move |ctx, preq| {
        let store = store.clone();
        let tenant = ctx
            .subject
            .as_ref()
            .map(|subject| subject.tenant.clone())
            .ok_or_else(|| InterceptError::deny_policy("tenant context missing"));

        Box::pin(async move {
            let tenant = tenant?;
            let raw_path = preq.path();
            let plan_id = raw_path
                .strip_prefix("/v1/plan/")
                .filter(|segment| !segment.is_empty())
                .ok_or_else(|| InterceptError::schema("plan id missing"))?;

            let plan = store
                .get(&tenant, plan_id)
                .await
                .map_err(map_llm_error)?
                .ok_or_else(|| {
                    InterceptError::from_public(codes::STORAGE_NOT_FOUND, "Plan not found")
                })?;

            Ok(json!({
                "plan_id": plan_id,
                "steps": plan,
            }))
        })
    })
    .await
}

async fn plan_list_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let store = state.plan_store.clone();

    handle_with_chain(req, &state.chain, move |ctx, _preq| {
        let store = store.clone();
        let tenant = ctx
            .subject
            .as_ref()
            .map(|subject| subject.tenant.clone())
            .ok_or_else(|| InterceptError::deny_policy("tenant context missing"));

        Box::pin(async move {
            let tenant = tenant?;
            let plans = store.list(&tenant).await.map_err(map_llm_error)?;
            Ok(json!({
                "plans": plans,
            }))
        })
    })
    .await
}

async fn plan_delete_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let store = state.plan_store.clone();

    handle_with_chain(req, &state.chain, move |ctx, preq| {
        let store = store.clone();
        let tenant = ctx
            .subject
            .as_ref()
            .map(|subject| subject.tenant.clone())
            .ok_or_else(|| InterceptError::deny_policy("tenant context missing"));

        Box::pin(async move {
            let tenant = tenant?;
            let raw_path = preq.path();
            let plan_id = raw_path
                .strip_prefix("/v1/plan/")
                .filter(|segment| !segment.is_empty())
                .ok_or_else(|| InterceptError::schema("plan id missing"))?;

            let removed = store
                .remove(&tenant, plan_id)
                .await
                .map_err(map_llm_error)?;
            if !removed {
                return Err(InterceptError::from_public(
                    codes::STORAGE_NOT_FOUND,
                    "Plan not found",
                ));
            }

            Ok(json!({
                "plan_id": plan_id,
                "deleted": true,
            }))
        })
    })
    .await
}

async fn explain_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let store = state.explain_store.clone();

    handle_with_chain(req, &state.chain, move |ctx, preq| {
        let store = store.clone();
        let tenant = ctx
            .subject
            .as_ref()
            .map(|subject| subject.tenant.clone())
            .ok_or_else(|| InterceptError::deny_policy("tenant context missing"));

        Box::pin(async move {
            let tenant = tenant?;
            let raw_path = preq.path();
            let explain_id = raw_path
                .strip_prefix("/v1/explain/")
                .filter(|segment| !segment.is_empty())
                .ok_or_else(|| InterceptError::schema("explain id missing"))?;

            let record = store
                .get(explain_id)
                .await
                .map_err(map_llm_error)?
                .ok_or_else(|| {
                    InterceptError::from_public(
                        codes::STORAGE_NOT_FOUND,
                        "Explain record not found",
                    )
                })?;

            if record.tenant != tenant {
                return Err(InterceptError::deny_policy(
                    "Explain record not owned by tenant",
                ));
            }

            Ok(json!({
                "explain_id": record.explain_id,
                "provider": record.provider,
                "fingerprint": record.fingerprint,
                "plan_id": record.plan_id,
                "created_at": record.created_at.to_rfc3339(),
                "metadata": record.metadata,
            }))
        })
    })
    .await
}

async fn explain_view_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let store = state.explain_store.clone();

    let response = handle_with_chain(req, &state.chain, move |ctx, preq| {
        let store = store.clone();
        let tenant = ctx
            .subject
            .as_ref()
            .map(|subject| subject.tenant.clone())
            .ok_or_else(|| InterceptError::deny_policy("tenant context missing"));

        Box::pin(async move {
            let tenant = tenant?;
            let raw_path = preq.path();
            let explain_id = raw_path
                .strip_prefix("/v1/explain/")
                .filter(|segment| !segment.is_empty())
                .ok_or_else(|| InterceptError::schema("explain id missing"))?;

            let record = store
                .get(explain_id)
                .await
                .map_err(map_llm_error)?
                .ok_or_else(|| {
                    InterceptError::from_public(
                        codes::STORAGE_NOT_FOUND,
                        "Explain record not found",
                    )
                })?;

            if record.tenant != tenant {
                return Err(InterceptError::deny_policy(
                    "Explain record not owned by tenant",
                ));
            }

            let html = render_explain_html(&record);
            Ok(json!({ "html": html }))
        })
    })
    .await;

    if !response.status().is_success() {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let bytes = match to_bytes(body, 1_048_576).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return intercept_error(InterceptError::internal(&format!(
                "read explain view payload: {err}"
            )));
        }
    };

    let value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(err) => {
            return intercept_error(InterceptError::internal(&format!(
                "decode explain view payload: {err}"
            )));
        }
    };

    let html = match value.get("html").and_then(|v| v.as_str()) {
        Some(html) => html.to_string(),
        None => {
            return intercept_error(InterceptError::internal("invalid explain view payload"));
        }
    };

    parts.headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );

    Response::from_parts(parts, Body::from(html))
}

fn render_explain_html(record: &ExplainRecord) -> String {
    let metadata_pretty = serde_json::to_string_pretty(&record.metadata)
        .unwrap_or_else(|_| "<unable to render metadata>".into());
    let degradation_summary = extract_degradation_summary(&record.metadata);
    let mut html = String::new();
    html.push_str("<html><head><title>Explain Record</title><style>body{font-family:monospace;padding:1.5rem;}h1{font-size:1.4rem;}pre{background:#f5f5f5;padding:1rem;border-radius:6px;overflow:auto;}table{border-collapse:collapse;margin-bottom:1rem;}td{padding:0.25rem 0.75rem;}</style></head><body>");
    html.push_str("<h1>Explain Record</h1>");
    html.push_str(&format!(
        "<table><tr><td><strong>ID</strong></td><td>{}</td></tr><tr><td><strong>Provider</strong></td><td>{}</td></tr><tr><td><strong>Created</strong></td><td>{}</td></tr>",
        escape_html(&record.explain_id),
        escape_html(&record.provider),
        record.created_at.to_rfc2822()
    ));

    if let Some(plan_id) = &record.plan_id {
        html.push_str(&format!(
            "<tr><td><strong>Plan ID</strong></td><td>{}</td></tr>",
            escape_html(plan_id)
        ));
    }

    if let Some(fingerprint) = &record.fingerprint {
        html.push_str(&format!(
            "<tr><td><strong>Fingerprint</strong></td><td>{}</td></tr>",
            escape_html(fingerprint)
        ));
    }

    if let Some(summary) = degradation_summary {
        html.push_str(&format!(
            "<tr><td><strong>Degradation</strong></td><td>{}</td></tr>",
            escape_html(&summary)
        ));
    }

    html.push_str("</table>");

    html.push_str("<h2>Metadata</h2>");
    html.push_str("<pre>");
    html.push_str(&escape_html(&metadata_pretty));
    html.push_str("</pre>");
    html.push_str("</body></html>");
    html
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn extract_degradation_summary(metadata: &serde_json::Value) -> Option<String> {
    metadata.get("degradation").and_then(|deg| {
        if let Some(obj) = deg.as_object() {
            if let Some(reasons) = obj.get("reasons").and_then(|r| r.as_array()) {
                Some(
                    reasons
                        .iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                )
            } else {
                obj.get("reason")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            }
        } else {
            deg.as_str().map(|s| s.to_string())
        }
    })
}

async fn plan_execute_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let store = state.plan_store.clone();
    let invoker = state.tool_invoker.clone();
    let explain_store = state.explain_store.clone();

    handle_with_chain(req, &state.chain, move |ctx, preq| {
        let store = store.clone();
        let invoker = invoker.clone();
        let explain_store = explain_store.clone();
        let subject = ctx
            .subject
            .clone()
            .ok_or_else(|| InterceptError::deny_policy("subject context missing"));

        Box::pin(async move {
            let subject = subject?;
            let tenant = subject.tenant.clone();
            let raw_path = preq.path();
            let plan_id = raw_path
                .strip_prefix("/v1/plan/")
                .and_then(|rest| rest.strip_suffix("/execute"))
                .filter(|segment| !segment.is_empty())
                .ok_or_else(|| InterceptError::schema("plan id missing"))?;

            let plan = store
                .get(&tenant, plan_id)
                .await
                .map_err(map_llm_error)?
                .ok_or_else(|| {
                    InterceptError::from_public(codes::STORAGE_NOT_FOUND, "Plan not found")
                })?;

            let mut results: Vec<serde_json::Value> = Vec::new();
            let mut overall_degradation: Vec<String> = Vec::new();

            for step in plan.iter() {
                let tool_id = ToolId(step.name.clone());
                let call = ToolInvocation {
                    tool_id,
                    call_id: step.call_id.clone(),
                    tenant: tenant.clone(),
                    actor: subject.clone(),
                    origin: ToolOrigin::Llm,
                    args: step.arguments.clone(),
                    consent: None,
                    idempotency_key: None,
                };

                let preflight_output = invoker.preflight(&call).await.map_err(map_tool_error)?;

                if !preflight_output.allow {
                    let reason = preflight_output
                        .reason
                        .as_deref()
                        .unwrap_or("tool invocation denied");
                    return Err(InterceptError::deny_policy(reason));
                }

                let spec = preflight_output
                    .spec
                    .clone()
                    .ok_or_else(|| InterceptError::internal("tool spec missing from preflight"))?;
                let profile_hash = preflight_output
                    .profile_hash
                    .clone()
                    .unwrap_or_else(|| "profile".into());

                let request = InvokeRequest {
                    spec,
                    call,
                    profile_hash,
                    obligations: preflight_output.obligations,
                    planned_ops: preflight_output.planned_ops,
                };

                let invoke_result = invoker.invoke(request).await.map_err(map_tool_error)?;

                let status_str = match invoke_result.status {
                    InvokeStatus::Ok => "ok",
                    InvokeStatus::Denied => "denied",
                    InvokeStatus::Error => "error",
                };

                if let Some(ref reasons) = invoke_result.degradation {
                    for reason in reasons {
                        if !overall_degradation.contains(reason) {
                            overall_degradation.push(reason.clone());
                        }
                    }
                }

                let mut entry = json!({
                    "call_id": step.call_id.0.clone(),
                    "tool_id": step.name.clone(),
                    "status": status_str,
                    "output": invoke_result.output,
                    "error_code": invoke_result.error_code,
                    "evidence_ref": invoke_result.evidence_ref.map(|id| id.0),
                });

                if let Some(reasons) = invoke_result.degradation {
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert("degradation".into(), json!({ "reasons": reasons }));
                    }
                }

                results.push(entry);
            }

            // Remove plan after successful execution to avoid replays.
            store
                .remove(&tenant, plan_id)
                .await
                .map_err(map_llm_error)?;

            let mut response = json!({
                "plan_id": plan_id,
                "results": results,
            });

            if !overall_degradation.is_empty() {
                if let Some(obj) = response.as_object_mut() {
                    obj.insert(
                        "degradation".into(),
                        json!({ "reasons": overall_degradation.clone() }),
                    );
                }
            }

            let explain_id = explain_store
                .record(&tenant, Some(plan_id), "tool-plan", None, response.clone())
                .await
                .map_err(map_llm_error)?;

            if let Some(obj) = response.as_object_mut() {
                obj.insert("explain_id".into(), json!(explain_id));
            }

            Ok(response)
        })
    })
    .await
}

async fn chat_stream_handler(State(state): State<AppState>, mut req: Request<Body>) -> Response {
    let plan_store = state.plan_store.clone();
    let explain_store = state.explain_store.clone();
    let logger = state.logger.clone();
    let (headers, telemetry) = match enforce_chain(&mut req, &state.chain).await {
        Ok(parts) => parts,
        Err(resp) => return resp,
    };

    let payload: ChatPayload = match parse_json(&mut req).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let plan_key = extract_plan_key(&payload.metadata);
    let chat_req = payload.into_request();
    let model_id = chat_req.model_id.clone();

    let model = {
        let registry = state.registry.read().await;
        registry.chat(&chat_req.model_id)
    };
    let model = match model {
        Some(model) => model,
        None => return intercept_error(InterceptError::deny_policy("model not registered")),
    };

    let stream = match model.chat_stream(chat_req, &state.policy).await {
        Ok(stream) => stream,
        Err(err) => return intercept_error(InterceptError::from_error(err.into_inner())),
    };

    let telemetry_stream = telemetry.clone();
    let model_id_stream = model_id.clone();
    let meter_stream = state.meter.clone();
    let plan_store_stream = plan_store.clone();
    let explain_store_stream = explain_store.clone();
    let logger_stream = logger.clone();
    let plan_key_stream = plan_key.clone();
    let tenant_stream = telemetry.tenant.clone();
    let plan_acc_stream: Arc<Mutex<Vec<ToolCallProposal>>> = Arc::new(Mutex::new(Vec::new()));
    let plan_acc_stream_inner = plan_acc_stream.clone();
    let provider_name = model_id.split(':').next().unwrap_or("unknown").to_string();

    let mapped = async_stream::stream! {
        let mut inner = stream;
        let started_at = Instant::now();
        let mut first_emitted = false;
        let mut usage_recorded = false;

        while let Some(delta) = inner.next().await {
            match delta {
                Ok(delta) => {
                    let mut envelope = delta;
                    if envelope.first_token_ms.is_none() && !first_emitted {
                        envelope.first_token_ms = Some(started_at.elapsed().as_millis().min(u128::from(u32::MAX)) as u32);
                        first_emitted = true;
                    }

                    if let Some(usage) = envelope.usage_partial.as_ref() {
                        if !usage_recorded {
                            record_chat_usage(&meter_stream, &logger_stream, &telemetry_stream, &model_id_stream, usage, "stream").await;
                            usage_recorded = true;
                        }
                    }

                    if let Some(ref proposal) = envelope.tool_call_delta {
                        if let Ok(mut guard) = plan_acc_stream.lock() {
                            guard.push(proposal.clone());
                        }
                    }

                    match serde_json::to_string(&envelope) {
                        Ok(json) => yield Ok::<Event, Infallible>(Event::default().data(json)),
                        Err(err) => {
                            error!("failed to encode chat delta: {err}");
                            let payload = serde_json::to_string(&ErrorEvent {
                                code: "stream.encoding",
                                message: "failed to encode delta",
                            }).unwrap_or_else(|_| "{\"code\":\"stream.encoding\",\"message\":\"failed\"}".into());
                            yield Ok(Event::default().event("error").data(payload));
                            break;
                        }
                    }
                }
                Err(err) => {
                    let obj = err.into_inner();
                    let view = obj.to_public();
                    let payload = serde_json::to_string(&ErrorEvent {
                        code: view.code,
                        message: view.message.as_str(),
                    }).unwrap_or_else(|_| "{\"code\":\"stream.error\",\"message\":\"internal\"}".into());
                    error!(code = %view.code, dev = obj.message_dev.as_deref().unwrap_or("n/a"), "stream error: {}", view.message);
                    yield Ok(Event::default().event("error").data(payload));
                    break;
                }
            }
        }

        if let (Some(key), Some(ref tenant_id)) = (plan_key_stream.as_deref(), tenant_stream.as_ref()) {
            let collected = plan_acc_stream_inner
                .lock()
                .map(|guard| guard.clone())
                .unwrap_or_default();
            if !collected.is_empty() {
                let tenant = TenantId(tenant_id.to_string());
                if let Err(err) = plan_store_stream
                    .record(&tenant, key, &collected)
                    .await
                {
                    let inner = err.into_inner();
                    warn!(
                        target = "soulbase::llm",
                        "failed to persist streamed plan: {:?}",
                        inner
                    );
                }
            }
        }

        if !usage_recorded {
            let usage = Usage {
                input_tokens: 0,
                output_tokens: 0,
                cached_tokens: None,
                image_units: None,
                audio_seconds: None,
                requests: 1,
            };
            record_chat_usage(&meter_stream, &logger_stream, &telemetry_stream, &model_id_stream, &usage, "stream").await;
        }

        if let Some(ref tenant_id) = tenant_stream {
            let tenant = TenantId(tenant_id.clone());
            match explain_store_stream
                .record(
                    &tenant,
                    plan_key_stream.as_deref(),
                    &provider_name,
                    None,
                    json!({
                        "provider": provider_name,
                    }),
                )
                .await
            {
                Ok(explain_id) => {
                    let explain_event = json!({ "explain_id": explain_id });
                    yield Ok::<Event, Infallible>(Event::default().event("explain").data(explain_event.to_string()));
                }
                Err(err) => {
                    let inner = err.into_inner();
                    warn!(
                        target = "soulbase::llm",
                        "failed to record explain event: {:?}",
                        inner
                    );
                }
            }
        }
    };

    let mut response = Sse::new(mapped)
        .keep_alive(KeepAlive::default())
        .into_response();
    for (key, value) in headers.iter() {
        response.headers_mut().insert(key.clone(), value.clone());
    }
    response
}

async fn enforce_chain(
    req: &mut Request<Body>,
    chain: &InterceptorChain,
) -> Result<(HeaderMap, RequestTelemetry), Response> {
    let cx = InterceptContext::default();
    let mut preq = AxumReq {
        req,
        cached_json: None,
    };
    let mut pres = AxumRes {
        headers: HeaderMap::new(),
        status: StatusCode::OK,
        body: None,
    };

    let telemetry_holder = Arc::new(Mutex::new(None::<RequestTelemetry>));
    let telemetry_clone = telemetry_holder.clone();

    match chain
        .run_with_handler(cx, &mut preq, &mut pres, move |ctx, _| {
            let snapshot = extract_telemetry(ctx);
            if let Ok(mut guard) = telemetry_clone.lock() {
                *guard = Some(snapshot);
            }
            async move { Ok(serde_json::Value::Null) }.boxed()
        })
        .await
    {
        Ok(()) => {
            if let Some(body) = pres.body {
                let bytes = serde_json::to_vec(&body).unwrap_or_default();
                let mut response = Response::builder()
                    .status(pres.status)
                    .body(Body::from(bytes))
                    .unwrap();
                *response.headers_mut() = pres.headers;
                Err(response)
            } else {
                let telemetry = telemetry_holder
                    .lock()
                    .ok()
                    .and_then(|guard| guard.clone())
                    .unwrap_or_default();
                Ok((pres.headers, telemetry))
            }
        }
        Err(err) => Err(intercept_error(err)),
    }
}

async fn parse_json<T: DeserializeOwned>(req: &mut Request<Body>) -> Result<T, Response> {
    let body = std::mem::take(req.body_mut());
    let bytes = to_bytes(body, 1_048_576)
        .await
        .map_err(|err| intercept_error(InterceptError::internal(&format!("read body: {err}"))))?;
    serde_json::from_slice(&bytes)
        .map_err(|err| intercept_error(InterceptError::schema(&format!("json parse: {err}"))))
}

fn intercept_error(err: InterceptError) -> Response {
    let (status, json) = soulbase_interceptors::errors::to_http_response(&err);
    let bytes = serde_json::to_vec(&json).unwrap_or_default();
    Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(bytes))
        .unwrap()
}

fn install_providers(registry: &mut Registry) {
    LocalProviderFactory::install(registry);
    #[cfg(feature = "provider-openai")]
    {
        if let Ok(api_key) = std::env::var("OPENAI_API_KEY") {
            let mut cfg = match OpenAiConfig::new(api_key) {
                Ok(cfg) => cfg,
                Err(err) => {
                    log_provider_error("openai", "invalid config", err.into_inner().to_public());
                    return;
                }
            };

            if let Ok(mapping) = std::env::var("OPENAI_MODEL_ALIAS") {
                for part in mapping.split(',') {
                    if let Some((alias, model)) = part.split_once(':') {
                        cfg = cfg.with_alias(alias.trim(), model.trim());
                    }
                }
            } else {
                cfg = cfg.with_alias("thin-waist", "gpt-4o-mini");
            }

            if let Ok(rpm) = std::env::var("OPENAI_RATE_LIMIT_RPM")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
            {
                cfg = cfg.with_rate_limit(rpm);
            }

            match OpenAiProviderFactory::new(cfg) {
                Ok(factory) => factory.install(registry),
                Err(err) => log_provider_error(
                    "openai",
                    "provider init failed",
                    err.into_inner().to_public(),
                ),
            }
        } else {
            warn!("OPENAI_API_KEY not set; only local provider available");
        }
    }
    #[cfg(not(feature = "provider-openai"))]
    {
        warn!("provider-openai feature disabled; only local provider available");
    }

    #[cfg(feature = "provider-claude")]
    {
        if let Ok(api_key) = std::env::var("ANTHROPIC_API_KEY") {
            let mut cfg = match ClaudeConfig::new(api_key) {
                Ok(cfg) => cfg,
                Err(err) => {
                    log_provider_error("claude", "invalid config", err.into_inner().to_public());
                    return;
                }
            };

            if let Ok(base_url) = std::env::var("ANTHROPIC_BASE_URL") {
                cfg = match cfg.with_base_url(base_url) {
                    Ok(cfg) => cfg,
                    Err(err) => {
                        log_provider_error(
                            "claude",
                            "invalid base url",
                            err.into_inner().to_public(),
                        );
                        return;
                    }
                };
            }

            if let Ok(version) = std::env::var("ANTHROPIC_VERSION") {
                cfg = cfg.with_version(version);
            }

            if let Ok(mapping) = std::env::var("CLAUDE_MODEL_ALIAS") {
                for part in mapping.split(',') {
                    if let Some((alias, model)) = part.split_once(':') {
                        cfg = cfg.with_alias(alias.trim(), model.trim());
                    }
                }
            }

            if let Ok(default_tokens) = std::env::var("CLAUDE_DEFAULT_MAX_TOKENS")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
            {
                cfg = cfg.with_default_max_tokens(default_tokens);
            }

            if let Ok(concurrency) = std::env::var("CLAUDE_MAX_CONCURRENCY")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
            {
                cfg = cfg.with_max_concurrency(concurrency);
            }

            if let Ok(rpm) = std::env::var("ANTHROPIC_RATE_LIMIT_RPM")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
            {
                cfg = cfg.with_rate_limit(rpm);
            }

            if let Ok(beta_headers) = std::env::var("ANTHROPIC_BETA_HEADERS") {
                for header in beta_headers.split(',') {
                    let header = header.trim();
                    if !header.is_empty() {
                        cfg = cfg.with_beta_header(header.to_string());
                    }
                }
            }

            match ClaudeProviderFactory::new(cfg) {
                Ok(factory) => factory.install(registry),
                Err(err) => log_provider_error(
                    "claude",
                    "provider init failed",
                    err.into_inner().to_public(),
                ),
            }
        } else {
            warn!("ANTHROPIC_API_KEY not set; Claude provider disabled");
        }
    }
    #[cfg(not(feature = "provider-claude"))]
    {
        warn!("provider-claude feature disabled; enable to register Anthropic provider");
    }

    #[cfg(feature = "provider-gemini")]
    {
        if let Ok(api_key) = std::env::var("GEMINI_API_KEY") {
            let mut cfg = match GeminiConfig::new(api_key) {
                Ok(cfg) => cfg,
                Err(err) => {
                    log_provider_error("gemini", "invalid config", err.into_inner().to_public());
                    return;
                }
            };

            if let Ok(base_url) = std::env::var("GEMINI_BASE_URL") {
                cfg = match cfg.with_base_url(base_url) {
                    Ok(cfg) => cfg,
                    Err(err) => {
                        log_provider_error(
                            "gemini",
                            "invalid base url",
                            err.into_inner().to_public(),
                        );
                        return;
                    }
                };
            }

            if let Ok(version) = std::env::var("GEMINI_API_VERSION") {
                cfg = cfg.with_version(version);
            }

            if let Ok(timeout_ms) = std::env::var("GEMINI_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
            {
                cfg = cfg.with_timeout(Duration::from_millis(timeout_ms));
            }

            if let Ok(concurrency) = std::env::var("GEMINI_MAX_CONCURRENCY")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
            {
                cfg = cfg.with_max_concurrency(concurrency);
            }

            if let Ok(mapping) = std::env::var("GEMINI_MODEL_ALIAS") {
                for part in mapping.split(',') {
                    if let Some((alias, model)) = part.split_once(':') {
                        cfg = cfg.with_alias(alias.trim(), model.trim());
                    }
                }
            }

            match GeminiProviderFactory::new(cfg) {
                Ok(factory) => factory.install(registry),
                Err(err) => log_provider_error(
                    "gemini",
                    "provider init failed",
                    err.into_inner().to_public(),
                ),
            }
        } else {
            warn!("GEMINI_API_KEY not set; Gemini provider disabled");
        }
    }
    #[cfg(not(feature = "provider-gemini"))]
    {
        warn!("provider-gemini feature disabled; enable to register Gemini provider");
    }
}

#[cfg(any(
    feature = "provider-openai",
    feature = "provider-claude",
    feature = "provider-gemini"
))]
fn log_provider_error(provider: &str, context: &str, view: PublicErrorView) {
    error!(code = %view.code, "{provider} {context}: {}", view.message);
}

fn build_chain(auth: AuthWiring) -> InterceptorChain {
    let policy = RoutePolicy::new(vec![
        RoutePolicySpec {
            when: MatchCond::Http {
                method: "POST".into(),
                path_glob: "/v1/chat".into(),
            },
            bind: RouteBindingSpec {
                resource: "urn:llm:chat".into(),
                action: "Invoke".into(),
                attrs_template: Some(json!({ "allow": true })),
                attrs_from_body: false,
            },
        },
        RoutePolicySpec {
            when: MatchCond::Http {
                method: "POST".into(),
                path_glob: "/v1/chat/stream".into(),
            },
            bind: RouteBindingSpec {
                resource: "urn:llm:chat".into(),
                action: "Invoke".into(),
                attrs_template: Some(json!({ "allow": true })),
                attrs_from_body: false,
            },
        },
        RoutePolicySpec {
            when: MatchCond::Http {
                method: "POST".into(),
                path_glob: "/v1/embed".into(),
            },
            bind: RouteBindingSpec {
                resource: "urn:llm:embed".into(),
                action: "Invoke".into(),
                attrs_template: Some(json!({ "allow": true })),
                attrs_from_body: false,
            },
        },
        RoutePolicySpec {
            when: MatchCond::Http {
                method: "POST".into(),
                path_glob: "/v1/rerank".into(),
            },
            bind: RouteBindingSpec {
                resource: "urn:llm:rerank".into(),
                action: "Invoke".into(),
                attrs_template: Some(json!({ "allow": true })),
                attrs_from_body: false,
            },
        },
        RoutePolicySpec {
            when: MatchCond::Http {
                method: "GET".into(),
                path_glob: "/v1/plan/*".into(),
            },
            bind: RouteBindingSpec {
                resource: "urn:llm:plan".into(),
                action: "Read".into(),
                attrs_template: Some(json!({ "allow": true })),
                attrs_from_body: false,
            },
        },
        RoutePolicySpec {
            when: MatchCond::Http {
                method: "GET".into(),
                path_glob: "/v1/plan".into(),
            },
            bind: RouteBindingSpec {
                resource: "urn:llm:plan".into(),
                action: "List".into(),
                attrs_template: Some(json!({ "allow": true })),
                attrs_from_body: false,
            },
        },
        RoutePolicySpec {
            when: MatchCond::Http {
                method: "POST".into(),
                path_glob: "/v1/plan/*".into(),
            },
            bind: RouteBindingSpec {
                resource: "urn:llm:plan".into(),
                action: "Invoke".into(),
                attrs_template: Some(json!({ "allow": true })),
                attrs_from_body: false,
            },
        },
        RoutePolicySpec {
            when: MatchCond::Http {
                method: "DELETE".into(),
                path_glob: "/v1/plan/*".into(),
            },
            bind: RouteBindingSpec {
                resource: "urn:llm:plan".into(),
                action: "Write".into(),
                attrs_template: Some(json!({ "allow": true })),
                attrs_from_body: false,
            },
        },
        RoutePolicySpec {
            when: MatchCond::Http {
                method: "GET".into(),
                path_glob: "/v1/explain/*".into(),
            },
            bind: RouteBindingSpec {
                resource: "urn:llm:explain".into(),
                action: "Read".into(),
                attrs_template: Some(json!({ "allow": true })),
                attrs_from_body: false,
            },
        },
    ]);

    let authenticator = auth.authenticator;
    let facade = auth.facade;

    InterceptorChain::new(vec![
        Box::new(ContextInitStage),
        Box::new(RoutePolicyStage { policy }),
        Box::new(AuthnMapStage { authenticator }),
        Box::new(AuthzQuotaStage { facade }),
        Box::new(ResponseStampStage),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use soulbase_tools::manifest::{
        CapabilityDecl, ConcurrencyKind, ConsentPolicy, IdempoKind, Limits, SafetyClass, SchemaDoc,
        SideEffect, ToolId, ToolManifest,
    };
    use soulbase_tools::prelude::{InMemoryRegistry as ToolRegistryMemory, ToolRegistry};
    use soulbase_types::prelude::{Id, TenantId};
    use std::sync::Arc;
    use tower::ServiceExt;

    const BODY_LIMIT: usize = 1_048_576;

    fn build_test_router() -> (Router, Arc<MemoryPlanStore>, Arc<ToolRegistryMemory>) {
        std::env::remove_var("OPENAI_API_KEY");
        std::env::remove_var("OPENAI_MODEL_ALIAS");

        let plan_store = Arc::new(MemoryPlanStore::default());
        let explain_store = Arc::new(MemoryExplainStore::default());
        let tool_registry = Arc::new(ToolRegistryMemory::new());
        let tool_registry_dyn: Arc<dyn ToolRegistry> = tool_registry.clone();
        let tool_auth = Arc::new(AuthFacade::minimal());
        let observe = Arc::new(ObserveSystem::new());
        let sandbox = Sandbox::minimal().with_telemetry(observe.meter(), observe.logger());
        let policy = PolicyConfig::default();
        let invoker_impl = InvokerImpl::new(
            tool_registry_dyn.clone(),
            tool_auth.clone(),
            sandbox,
            policy,
        )
        .with_observe(observe.meter(), observe.logger());
        let tool_invoker: Arc<dyn Invoker> = Arc::new(invoker_impl);
        let chain = build_chain(AuthWiring {
            authenticator: Box::new(OidcAuthenticatorStub),
            facade: tool_auth.clone(),
        });
        let plan_store_dyn: Arc<dyn PlanStore> = plan_store.clone();
        let explain_store_dyn: Arc<dyn ExplainStore> = explain_store.clone();
        let state = create_app_state(
            plan_store_dyn,
            tool_registry_dyn,
            tool_invoker,
            explain_store_dyn,
            observe,
            chain,
        );

        (build_router(state), plan_store, tool_registry)
    }

    async fn json_body(response: Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), BODY_LIMIT)
            .await
            .expect("read body");
        serde_json::from_slice(&bytes).expect("parse json")
    }

    fn schema_doc(value: serde_json::Value) -> SchemaDoc {
        #[cfg(feature = "schema_json")]
        {
            serde_json::from_value(value).expect("schema")
        }
        #[cfg(not(feature = "schema_json"))]
        {
            value
        }
    }

    fn sample_manifest(tool_id: &str) -> ToolManifest {
        ToolManifest {
            id: ToolId(tool_id.into()),
            version: "1.0.0".into(),
            display_name: "Sample Tool".into(),
            description: "Sample tool for tests".into(),
            tags: vec![],
            input_schema: schema_doc(json!({"type": "object"})),
            output_schema: schema_doc(json!({"type": "object"})),
            scopes: vec![],
            capabilities: vec![CapabilityDecl {
                domain: "tmp".into(),
                action: "use".into(),
                resource: "".into(),
                attrs: json!({}),
            }],
            side_effect: SideEffect::None,
            safety_class: SafetyClass::Low,
            consent: ConsentPolicy {
                required: false,
                max_ttl_ms: None,
            },
            limits: Limits {
                timeout_ms: 10_000,
                max_bytes_in: 1_024,
                max_bytes_out: 1_024,
                max_files: 0,
                max_depth: 1,
                max_concurrency: 1,
            },
            idempotency: IdempoKind::None,
            concurrency: ConcurrencyKind::Parallel,
        }
    }

    #[tokio::test]
    async fn embed_endpoint_returns_vectors() {
        let (app, _, _) = build_test_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/embed")
            .header(CONTENT_TYPE, "application/json")
            .header("Authorization", "Bearer user-1@tenant-a")
            .body(Body::from(
                json!({
                    "model_id": "local:emb",
                    "items": [
                        { "id": "a", "text": "the cat sat" },
                        { "id": "b", "text": "cat on mat" }
                    ],
                    "normalize": true,
                    "pooling": null
                })
                .to_string(),
            ))
            .expect("build request");

        let response = app.oneshot(request).await.expect("router call");
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        assert_eq!(json["dim"], 8);
        let vectors = json["vectors"].as_array().expect("vectors array");
        assert_eq!(vectors.len(), 2);
        assert_eq!(
            vectors.get(0).and_then(|v| v.as_array()).map(|v| v.len()),
            Some(8)
        );
    }

    #[tokio::test]
    async fn rerank_endpoint_orders_candidates() {
        let (app, _, _) = build_test_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/rerank")
            .header(CONTENT_TYPE, "application/json")
            .header("Authorization", "Bearer user-1@tenant-a")
            .body(Body::from(
                json!({
                    "model_id": "local:rerank",
                    "query": "cat mat",
                    "candidates": [
                        "the cat sat",
                        "cat on mat"
                    ]
                })
                .to_string(),
            ))
            .expect("build request");

        let response = app.oneshot(request).await.expect("router call");
        assert_eq!(response.status(), StatusCode::OK);

        let json = json_body(response).await;
        let ordering = json["ordering"]
            .as_array()
            .expect("ordering array")
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ordering, vec![1, 0]);
    }

    #[tokio::test]
    async fn embed_requires_authorization() {
        let (app, _, _) = build_test_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/embed")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({
                    "model_id": "local:emb",
                    "items": [{ "id": "a", "text": "hello" }],
                    "normalize": false
                })
                .to_string(),
            ))
            .expect("build request");

        let response = app.oneshot(request).await.expect("router call");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let json = json_body(response).await;
        assert_eq!(json["code"], "AUTH.UNAUTHENTICATED");
    }

    #[tokio::test]
    async fn chat_records_tool_plan_when_plan_id_present() {
        let (app, plan_store, _) = build_test_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/chat")
            .header(CONTENT_TYPE, "application/json")
            .header("Authorization", "Bearer user-1@tenant-a")
            .body(Body::from(
                json!({
                    "model_id": "local:echo",
                    "messages": [
                        {
                            "role": "User",
                            "segments": [
                                { "Text": { "text": "hello" } }
                            ]
                        }
                    ],
                    "metadata": {
                        "plan_id": "case-chat-1"
                    }
                })
                .to_string(),
            ))
            .expect("build request");

        let response = app.oneshot(request).await.expect("router call");
        assert_eq!(response.status(), StatusCode::OK);
        let stored = plan_store.get_sync(&TenantId("tenant-a".into()), "case-chat-1");
        assert!(stored.is_some());
    }

    #[tokio::test]
    async fn plan_handler_returns_plan_payload() {
        let (app, plan_store, _) = build_test_router();

        let chat_request = Request::builder()
            .method("POST")
            .uri("/v1/chat")
            .header(CONTENT_TYPE, "application/json")
            .header("Authorization", "Bearer user-1@tenant-a")
            .body(Body::from(
                json!({
                    "model_id": "local:echo",
                    "messages": [
                        {
                            "role": "User",
                            "segments": [
                                { "Text": { "text": "hello" } }
                            ]
                        }
                    ],
                    "metadata": {
                        "plan_id": "case-chat-2"
                    }
                })
                .to_string(),
            ))
            .expect("build request");

        let chat_response = app.clone().oneshot(chat_request).await.expect("chat call");
        assert_eq!(chat_response.status(), StatusCode::OK);

        let stored = plan_store.get_sync(&TenantId("tenant-a".into()), "case-chat-2");
        assert!(stored.is_some());

        let get_request = Request::builder()
            .method("GET")
            .uri("/v1/plan/case-chat-2")
            .header("Authorization", "Bearer user-1@tenant-a")
            .body(Body::empty())
            .expect("build get request");

        let response = app.clone().oneshot(get_request).await.expect("plan call");
        assert_eq!(response.status(), StatusCode::OK);

        let body = json_body(response).await;
        assert_eq!(body["plan_id"], "case-chat-2");
        assert!(body["steps"].is_array());
    }

    #[tokio::test]
    async fn plan_list_and_delete_behave() {
        let (app, _, _) = build_test_router();

        // Create a plan via chat
        let chat_request = Request::builder()
            .method("POST")
            .uri("/v1/chat")
            .header(CONTENT_TYPE, "application/json")
            .header("Authorization", "Bearer user-2@tenant-b")
            .body(Body::from(
                json!({
                    "model_id": "local:echo",
                    "messages": [
                        {
                            "role": "User",
                            "segments": [
                                { "Text": { "text": "hi" } }
                            ]
                        }
                    ],
                    "metadata": {
                        "plan_id": "case-chat-3"
                    }
                })
                .to_string(),
            ))
            .expect("build chat request");
        let chat_response = app.clone().oneshot(chat_request).await.expect("chat call");
        assert_eq!(chat_response.status(), StatusCode::OK);

        // List plans for tenant-b
        let list_request = Request::builder()
            .method("GET")
            .uri("/v1/plan")
            .header("Authorization", "Bearer user-2@tenant-b")
            .body(Body::empty())
            .expect("build list request");
        let list_response = app.clone().oneshot(list_request).await.expect("list call");
        assert_eq!(list_response.status(), StatusCode::OK);
        let list_body = json_body(list_response).await;
        let plans = list_body["plans"].as_array().expect("plans array");
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0]["plan_id"], "case-chat-3");
        assert_eq!(plans[0]["step_count"].as_u64().unwrap(), 0);

        // Delete the plan
        let delete_request = Request::builder()
            .method("DELETE")
            .uri("/v1/plan/case-chat-3")
            .header("Authorization", "Bearer user-2@tenant-b")
            .body(Body::empty())
            .expect("build delete request");
        let delete_response = app
            .clone()
            .oneshot(delete_request)
            .await
            .expect("delete call");
        assert_eq!(delete_response.status(), StatusCode::OK);

        // Ensure subsequent get is not found
        let get_request = Request::builder()
            .method("GET")
            .uri("/v1/plan/case-chat-3")
            .header("Authorization", "Bearer user-2@tenant-b")
            .body(Body::empty())
            .expect("build get request");
        let get_response = app.clone().oneshot(get_request).await.expect("get call");
        assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn plan_execute_runs_tool() {
        let (app, plan_store, tool_registry) = build_test_router();

        let tenant = TenantId("tenant-c".into());
        plan_store.record_sync(
            &tenant,
            "case-exec-1",
            vec![ToolCallProposal {
                name: "demo.tmp".into(),
                call_id: Id("call-demo".into()),
                arguments: json!({ "size_bytes": 64 }),
            }],
        );

        tool_registry
            .upsert(&tenant, sample_manifest("demo.tmp"))
            .await
            .expect("register tool");

        let exec_request = Request::builder()
            .method("POST")
            .uri("/v1/plan/case-exec-1/execute")
            .header("Authorization", "Bearer user-3@tenant-c")
            .body(Body::empty())
            .expect("build execute request");

        let response = app
            .clone()
            .oneshot(exec_request)
            .await
            .expect("execute call");
        assert_eq!(response.status(), StatusCode::OK);

        let body = json_body(response).await;
        assert_eq!(body["plan_id"], "case-exec-1");
        let results = body["results"].as_array().expect("results array");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["status"], "ok");
        assert!(results[0]["output"]["simulated"].as_bool().unwrap_or(false));

        // Plan should be removed after execution
        assert!(plan_store.get_sync(&tenant, "case-exec-1").is_none());
    }

    #[tokio::test]
    async fn explain_handler_returns_record() {
        let (app, _, _) = build_test_router();

        let chat_request = Request::builder()
            .method("POST")
            .uri("/v1/chat")
            .header(CONTENT_TYPE, "application/json")
            .header("Authorization", "Bearer user-4@tenant-d")
            .body(Body::from(
                json!({
                    "model_id": "local:echo",
                    "messages": [
                        {
                            "role": "User",
                            "segments": [
                                { "Text": { "text": "explain" } }
                            ]
                        }
                    ],
                    "metadata": {
                        "plan_id": "case-chat-4"
                    }
                })
                .to_string(),
            ))
            .expect("build chat request");

        let chat_response = app.clone().oneshot(chat_request).await.expect("chat call");
        assert_eq!(chat_response.status(), StatusCode::OK);
        let chat_body = json_body(chat_response).await;
        let explain_id = chat_body["provider_meta"]["explain_id"]
            .as_str()
            .expect("explain id present")
            .to_string();

        let explain_request = Request::builder()
            .method("GET")
            .uri(format!("/v1/explain/{explain_id}"))
            .header("Authorization", "Bearer user-4@tenant-d")
            .body(Body::empty())
            .expect("build explain request");

        let explain_response = app.oneshot(explain_request).await.expect("explain call");
        assert_eq!(explain_response.status(), StatusCode::OK);
        let explain_body = json_body(explain_response).await;
        assert_eq!(explain_body["explain_id"], explain_id);
        assert_eq!(explain_body["plan_id"], "case-chat-4");
        assert_eq!(explain_body["provider"], "local");
    }
}
