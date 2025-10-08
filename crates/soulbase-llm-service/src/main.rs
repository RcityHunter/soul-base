use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Instant};

use axum::{
    body::Body,
    extract::State,
    http::{header::CONTENT_TYPE, HeaderMap, Request, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    routing::post,
    Json, Router,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
#[cfg(feature = "provider-openai")]
use soulbase_errors::prelude::PublicErrorView;
use soulbase_llm::prelude::*;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use soulbase_auth::authn::oidc::OidcAuthenticatorStub;
use soulbase_auth::AuthFacade;
use soulbase_interceptors::adapters::http::{AxumReq, AxumRes};
use soulbase_interceptors::errors::InterceptError;
use soulbase_interceptors::prelude::*;

#[derive(Clone)]
struct AppState {
    registry: Arc<RwLock<Registry>>,
    policy: StructOutPolicy,
    chain: Arc<InterceptorChain>,
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

#[derive(Serialize)]
struct ErrorEvent<'a> {
    code: &'a str,
    message: &'a str,
}

async fn chat_handler(
    State(state): State<AppState>,
    req: Request<Body>,
) -> axum::response::Response {
    let registry = state.registry.clone();
    let policy = state.policy.clone();

    let mut req = req;
    let cx = InterceptContext::default();
    let mut preq = AxumReq {
        req: &mut req,
        cached_json: None,
    };
    let mut pres = AxumRes {
        headers: HeaderMap::new(),
        status: StatusCode::OK,
        body: None,
    };

    let handler_registry = registry.clone();
    let handler_policy = policy.clone();

    let result = state
        .chain
        .run_with_handler(cx, &mut preq, &mut pres, move |_ctx, preq| {
            let registry = handler_registry.clone();
            let policy = handler_policy.clone();
            Box::pin(async move {
                let value = preq.read_json().await?;
                let payload: ChatPayload = serde_json::from_value(value)
                    .map_err(|err| InterceptError::schema(&format!("payload decode: {err}")))?;
                let req = payload.into_request();

                let model = {
                    let registry_guard = registry.read().await;
                    registry_guard.chat(&req.model_id)
                };
                let model =
                    model.ok_or_else(|| InterceptError::deny_policy("model not registered"))?;

                let response = model
                    .chat(req, &policy)
                    .await
                    .map_err(|err| InterceptError::from_error(err.into_inner()))?;

                serde_json::to_value(response)
                    .map_err(|err| InterceptError::internal(&format!("encode response: {err}")))
            })
        })
        .await;

    match result {
        Ok(()) => {
            let mut response = axum::response::Response::builder()
                .status(pres.status)
                .body(Body::empty())
                .unwrap();
            if let Some(body) = pres.body {
                let bytes = serde_json::to_vec(&body).unwrap_or_default();
                *response.body_mut() = Body::from(bytes);
            }
            *response.headers_mut() = pres.headers;
            response
        }
        Err(err) => {
            let (status, json) = soulbase_interceptors::errors::to_http_response(&err);
            let bytes = serde_json::to_vec(&json).unwrap_or_default();
            axum::response::Response::builder()
                .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(bytes))
                .unwrap()
        }
    }
}

async fn chat_stream_handler(
    State(state): State<AppState>,
    Json(payload): Json<ChatPayload>,
) -> Result<
    Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>,
    (axum::http::StatusCode, String),
> {
    let req = payload.into_request();

    let model = {
        let registry = state.registry.read().await;
        registry.chat(&req.model_id)
    };

    let Some(model) = model else {
        return Err((
            axum::http::StatusCode::NOT_FOUND,
            "model not registered".into(),
        ));
    };

    let stream = model.chat_stream(req, &state.policy).await.map_err(|err| {
        let obj = err.into_inner();
        let public = obj.to_public();
        error!(
            code = %public.code,
            dev = obj.message_dev.as_deref().unwrap_or("n/a"),
            "chat_stream error: {}",
            public.message
        );
        (axum::http::StatusCode::BAD_GATEWAY, public.message.clone())
    })?;

    let mapped = async_stream::stream! {
        let mut inner = stream;
        let started_at = Instant::now();
        let mut first_emitted = false;

        while let Some(delta) = inner.next().await {
            match delta {
                Ok(delta) => {
                    let mut envelope = delta;
                    if envelope.first_token_ms.is_none() && !first_emitted {
                        envelope.first_token_ms = Some(started_at.elapsed().as_millis().min(u128::from(u32::MAX)) as u32);
                        first_emitted = true;
                    }

                    match serde_json::to_string(&envelope) {
                        Ok(json) => yield Ok(Event::default().data(json)),
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
                    error!(
                        code = %view.code,
                        dev = obj.message_dev.as_deref().unwrap_or("n/a"),
                        "stream error: {}",
                        view.message
                    );
                    yield Ok(Event::default().event("error").data(payload));
                    break;
                }
            }
        }
    };

    Ok(Sse::new(mapped).keep_alive(KeepAlive::default()))
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let mut registry = Registry::new();
    install_providers(&mut registry);

    let state = AppState {
        registry: Arc::new(RwLock::new(registry)),
        policy: StructOutPolicy::StrictReject,
        chain: Arc::new(build_chain()),
    };

    let app = Router::new()
        .route("/v1/chat", post(chat_handler))
        .route("/v1/chat/stream", post(chat_stream_handler))
        .with_state(state);

    let listen = std::env::var("LLM_LISTEN").unwrap_or_else(|_| "127.0.0.1:9000".into());
    let addr: SocketAddr = listen.parse()?;
    info!("listening on {addr}");
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}
