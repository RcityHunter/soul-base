use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Instant,
};

use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{header::CONTENT_TYPE, HeaderMap, Request, StatusCode},
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
#[cfg(feature = "provider-openai")]
use soulbase_errors::prelude::PublicErrorView;
use soulbase_sandbox::prelude::{PolicyConfig, Sandbox};
use soulbase_tools::errors::ToolError;
use soulbase_tools::prelude::{
    InMemoryRegistry as ToolRegistryMemory, InvokeRequest, InvokeStatus, Invoker, InvokerImpl,
    ToolCall as ToolInvocation, ToolId, ToolOrigin,
};
use soulbase_types::prelude::TenantId;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use soulbase_auth::authn::oidc::OidcAuthenticatorStub;
use soulbase_auth::AuthFacade;
use soulbase_interceptors::adapters::http::{handle_with_chain, AxumReq, AxumRes};
use soulbase_interceptors::context::InterceptContext;
use soulbase_interceptors::errors::InterceptError;
use soulbase_interceptors::prelude::*;
use soulbase_observe::prelude::{Meter, MeterRegistry, MetricKind, MetricSpec};

use crate::prelude::*;
use crate::{LocalProviderFactory, Registry};
#[cfg(feature = "provider-openai")]
use crate::{OpenAiConfig, OpenAiProviderFactory};

#[derive(Clone)]
pub struct AppState {
    registry: Arc<RwLock<Registry>>,
    policy: StructOutPolicy,
    chain: Arc<InterceptorChain>,
    meter: MeterRegistry,
    plan_store: ToolPlanStore,
    #[allow(dead_code)]
    tool_registry: Arc<ToolRegistryMemory>,
    tool_invoker: Arc<dyn Invoker>,
    explain_store: ExplainStore,
}

#[derive(Clone, Default)]
struct ToolPlanStore {
    inner: Arc<Mutex<HashMap<(String, String), Vec<ToolCallProposal>>>>,
}

impl ToolPlanStore {
    fn record(&self, tenant: &TenantId, key: &str, plan: Vec<ToolCallProposal>) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.insert((tenant.0.clone(), key.to_string()), plan);
        }
    }

    fn get(&self, tenant: &TenantId, key: &str) -> Option<Vec<ToolCallProposal>> {
        self.inner
            .lock()
            .ok()
            .and_then(|guard| guard.get(&(tenant.0.clone(), key.to_string())).cloned())
    }

    fn list(&self, tenant: &TenantId) -> Vec<PlanSummary> {
        self.inner
            .lock()
            .map(|guard| {
                guard
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
                    .collect()
            })
            .unwrap_or_default()
    }

    fn remove(&self, tenant: &TenantId, key: &str) -> bool {
        self.inner
            .lock()
            .map(|mut guard| guard.remove(&(tenant.0.clone(), key.to_string())).is_some())
            .unwrap_or(false)
    }
}

#[derive(Clone, Default)]
struct ExplainStore {
    inner: Arc<Mutex<HashMap<String, ExplainRecord>>>,
}

#[derive(Clone, Debug, Serialize)]
struct ExplainRecord {
    explain_id: String,
    provider: String,
    fingerprint: Option<String>,
    tenant: TenantId,
    plan_id: Option<String>,
    created_at: DateTime<Utc>,
    metadata: serde_json::Value,
}

impl ExplainStore {
    fn record(
        &self,
        tenant: &TenantId,
        plan_id: Option<&str>,
        provider: &str,
        fingerprint: Option<String>,
        metadata: serde_json::Value,
    ) -> String {
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
        if let Ok(mut guard) = self.inner.lock() {
            guard.insert(explain_id.clone(), record);
        }
        explain_id
    }

    fn get(&self, explain_id: &str) -> Option<ExplainRecord> {
        self.inner
            .lock()
            .ok()
            .and_then(|guard| guard.get(explain_id).cloned())
    }
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

fn record_tool_plan(
    store: &ToolPlanStore,
    tenant: &TenantId,
    plan_key: Option<&str>,
    plan: &[ToolCallProposal],
) {
    if let Some(key) = plan_key {
        let cloned = plan.to_vec();
        store.record(tenant, key, cloned);
    }
}

fn record_chat_usage(
    meter: &MeterRegistry,
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
}

fn record_embed_usage(
    meter: &MeterRegistry,
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
}

fn record_rerank_usage(
    meter: &MeterRegistry,
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
}

fn map_tool_error(err: ToolError) -> InterceptError {
    InterceptError::from_error(err.into_inner())
}

fn build_tooling() -> (Arc<ToolRegistryMemory>, Arc<dyn Invoker>) {
    let tool_registry = Arc::new(ToolRegistryMemory::new());
    let tool_registry_for_invoker: Arc<dyn soulbase_tools::prelude::ToolRegistry> =
        tool_registry.clone();
    let tool_auth = Arc::new(AuthFacade::minimal());
    let sandbox = Sandbox::minimal();
    let policy = PolicyConfig::default();
    let invoker_impl = InvokerImpl::new(tool_registry_for_invoker, tool_auth, sandbox, policy);
    let tool_invoker: Arc<dyn Invoker> = Arc::new(invoker_impl);
    (tool_registry, tool_invoker)
}

fn create_app_state(
    plan_store: ToolPlanStore,
    tool_registry: Arc<ToolRegistryMemory>,
    tool_invoker: Arc<dyn Invoker>,
) -> AppState {
    let mut registry = Registry::new();
    install_providers(&mut registry);

    AppState {
        registry: Arc::new(RwLock::new(registry)),
        policy: StructOutPolicy::StrictReject,
        chain: Arc::new(build_chain()),
        meter: MeterRegistry::default(),
        plan_store,
        tool_registry,
        tool_invoker,
        explain_store: ExplainStore::default(),
    }
}

pub async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let state = default_state();
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
        .with_state(state)
}

pub fn default_state() -> AppState {
    let plan_store = ToolPlanStore::default();
    let (tool_registry, tool_invoker) = build_tooling();
    create_app_state(plan_store, tool_registry, tool_invoker)
}

async fn chat_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let registry = state.registry.clone();
    let policy = state.policy.clone();
    let meter = state.meter.clone();
    let plan_store = state.plan_store.clone();
    let explain_store = state.explain_store.clone();

    handle_with_chain(req, &state.chain, move |ctx, preq| {
        let registry = registry.clone();
        let policy = policy.clone();
        let meter = meter.clone();
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

            record_chat_usage(&meter, &telemetry, &model_id, &response.usage, "sync");
            if let Some(ref tenant_id) = telemetry.tenant {
                let tenant = TenantId(tenant_id.to_string());
                record_tool_plan(
                    &plan_store,
                    &tenant,
                    plan_key.as_deref(),
                    &response.message.tool_calls,
                );

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
                let explain_id = explain_store.record(
                    &tenant,
                    plan_key.as_deref(),
                    &provider,
                    fingerprint.clone(),
                    response.provider_meta.clone(),
                );

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

    handle_with_chain(req, &state.chain, move |ctx, preq| {
        let registry = registry.clone();
        let meter = meter.clone();
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

            record_embed_usage(&meter, &telemetry, &model_id, &response.usage, item_count);

            serde_json::to_value(response)
                .map_err(|err| InterceptError::internal(&format!("encode response: {err}")))
        })
    })
    .await
}

async fn rerank_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let registry = state.registry.clone();
    let meter = state.meter.clone();

    handle_with_chain(req, &state.chain, move |ctx, preq| {
        let registry = registry.clone();
        let meter = meter.clone();
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
                &telemetry,
                &model_id,
                &response.usage,
                candidate_count,
            );

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

            let plan = store.get(&tenant, plan_id).ok_or_else(|| {
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
            let plans = store.list(&tenant);
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

            let removed = store.remove(&tenant, plan_id);
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

            let record = store.get(explain_id).ok_or_else(|| {
                InterceptError::from_public(codes::STORAGE_NOT_FOUND, "Explain record not found")
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

async fn plan_execute_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let store = state.plan_store.clone();
    let invoker = state.tool_invoker.clone();

    handle_with_chain(req, &state.chain, move |ctx, preq| {
        let store = store.clone();
        let invoker = invoker.clone();
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

            let plan = store.get(&tenant, plan_id).ok_or_else(|| {
                InterceptError::from_public(codes::STORAGE_NOT_FOUND, "Plan not found")
            })?;

            let mut results = Vec::new();

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

                results.push(json!({
                    "call_id": step.call_id.0.clone(),
                    "tool_id": step.name.clone(),
                    "status": status_str,
                    "output": invoke_result.output,
                    "error_code": invoke_result.error_code,
                    "evidence_ref": invoke_result.evidence_ref.map(|id| id.0),
                }));
            }

            // Remove plan after successful execution to avoid replays.
            store.remove(&tenant, plan_id);

            Ok(json!({
                "plan_id": plan_id,
                "results": results,
            }))
        })
    })
    .await
}

async fn chat_stream_handler(State(state): State<AppState>, mut req: Request<Body>) -> Response {
    let plan_store = state.plan_store.clone();
    let explain_store = state.explain_store.clone();
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
                            record_chat_usage(&meter_stream, &telemetry_stream, &model_id_stream, usage, "stream");
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
            if let Ok(collected) = plan_acc_stream_inner.lock() {
                let tenant = TenantId(tenant_id.to_string());
                plan_store_stream.record(&tenant, key, collected.clone());
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
            record_chat_usage(&meter_stream, &telemetry_stream, &model_id_stream, &usage, "stream");
        }

        if let Some(ref tenant_id) = tenant_stream {
            let tenant = TenantId(tenant_id.clone());
            let explain_id = explain_store_stream.record(
                &tenant,
                plan_key_stream.as_deref(),
                &provider_name,
                None,
                json!({
                    "provider": provider_name,
                }),
            );
            let explain_event = json!({ "explain_id": explain_id });
            yield Ok::<Event, Infallible>(Event::default().event("explain").data(explain_event.to_string()));
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
                    log_provider_error("invalid openai config", err.into_inner().to_public());
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
                Err(err) => {
                    log_provider_error("openai provider init failed", err.into_inner().to_public())
                }
            }
        } else {
            warn!("OPENAI_API_KEY not set; only local provider available");
        }
    }
    #[cfg(not(feature = "provider-openai"))]
    {
        warn!("provider-openai feature disabled; only local provider available");
    }
}

#[cfg(feature = "provider-openai")]
fn log_provider_error(context: &str, view: PublicErrorView) {
    error!(code = %view.code, "{context}: {}", view.message);
}

fn build_chain() -> InterceptorChain {
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
                action: "Delete".into(),
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

    InterceptorChain::new(vec![
        Box::new(ContextInitStage),
        Box::new(RoutePolicyStage { policy }),
        Box::new(AuthnMapStage {
            authenticator: Box::new(OidcAuthenticatorStub),
        }),
        Box::new(AuthzQuotaStage {
            facade: AuthFacade::minimal(),
        }),
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
    use soulbase_tools::prelude::ToolRegistry;
    use soulbase_types::prelude::{Id, TenantId};
    use tower::ServiceExt;

    const BODY_LIMIT: usize = 1_048_576;

    fn build_test_router() -> (Router, ToolPlanStore, Arc<ToolRegistryMemory>) {
        std::env::remove_var("OPENAI_API_KEY");
        std::env::remove_var("OPENAI_MODEL_ALIAS");

        let plan_store = ToolPlanStore::default();
        let (tool_registry, tool_invoker) = build_tooling();
        let state = create_app_state(plan_store.clone(), tool_registry.clone(), tool_invoker);

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
        let stored = plan_store.get(&TenantId("tenant-a".into()), "case-chat-1");
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

        let stored = plan_store.get(&TenantId("tenant-a".into()), "case-chat-2");
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
        plan_store.record(
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
        assert!(plan_store.get(&tenant, "case-exec-1").is_none());
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
