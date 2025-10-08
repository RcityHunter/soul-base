use std::{
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
    routing::post,
    Router,
};
use futures_util::future::FutureExt;
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
#[cfg(feature = "provider-openai")]
use soulbase_errors::prelude::PublicErrorView;
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
        .with_state(state)
}

pub fn default_state() -> AppState {
    let mut registry = Registry::new();
    install_providers(&mut registry);

    AppState {
        registry: Arc::new(RwLock::new(registry)),
        policy: StructOutPolicy::StrictReject,
        chain: Arc::new(build_chain()),
        meter: MeterRegistry::default(),
    }
}

async fn chat_handler(State(state): State<AppState>, req: Request<Body>) -> Response {
    let registry = state.registry.clone();
    let policy = state.policy.clone();
    let meter = state.meter.clone();

    handle_with_chain(req, &state.chain, move |ctx, preq| {
        let registry = registry.clone();
        let policy = policy.clone();
        let meter = meter.clone();
        let telemetry = extract_telemetry(ctx);
        Box::pin(async move {
            let value = preq.read_json().await?;
            let payload: ChatPayload = serde_json::from_value(value)
                .map_err(|err| InterceptError::schema(&format!("payload decode: {err}")))?;
            let req = payload.into_request();
            let model_id = req.model_id.clone();

            let model = {
                let registry_guard = registry.read().await;
                registry_guard.chat(&req.model_id)
            };
            let model = model.ok_or_else(|| InterceptError::deny_policy("model not registered"))?;

            let response = model
                .chat(req, &policy)
                .await
                .map_err(|err| InterceptError::from_error(err.into_inner()))?;

            record_chat_usage(&meter, &telemetry, &model_id, &response.usage, "sync");

            serde_json::to_value(response)
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

async fn chat_stream_handler(State(state): State<AppState>, mut req: Request<Body>) -> Response {
    let (headers, telemetry) = match enforce_chain(&mut req, &state.chain).await {
        Ok(parts) => parts,
        Err(resp) => return resp,
    };

    let payload: ChatPayload = match parse_json(&mut req).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };
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
    use tower::ServiceExt;

    const BODY_LIMIT: usize = 1_048_576;

    fn build_test_router() -> Router {
        std::env::remove_var("OPENAI_API_KEY");
        std::env::remove_var("OPENAI_MODEL_ALIAS");

        let mut registry = Registry::new();
        install_providers(&mut registry);

        let state = AppState {
            registry: Arc::new(RwLock::new(registry)),
            policy: StructOutPolicy::StrictReject,
            chain: Arc::new(build_chain()),
            meter: MeterRegistry::default(),
        };

        build_router(state)
    }

    async fn json_body(response: Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), BODY_LIMIT)
            .await
            .expect("read body");
        serde_json::from_slice(&bytes).expect("parse json")
    }

    #[tokio::test]
    async fn embed_endpoint_returns_vectors() {
        let app = build_test_router();

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
        let app = build_test_router();

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
        let app = build_test_router();

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
}
