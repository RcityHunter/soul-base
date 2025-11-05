use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use async_trait::async_trait;
use axum::body::Body;
use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware::{from_fn_with_state, Next},
    response::{IntoResponse, Response},
    routing::{any, get},
    Json, Router,
};
use config::Config;
use serde::{Deserialize, Serialize};
use serde_json::json;
use soulbase_errors::prelude::codes;
use soulbase_interceptors::{
    adapters::http::handle_with_chain,
    context::{InterceptContext, ProtoRequest, ProtoResponse},
    errors::InterceptError,
    stages::{
        context_init::ContextInitStage, error_norm::ErrorNormStage,
        response_stamp::ResponseStampStage, InterceptorChain, Stage, StageOutcome,
    },
};
#[cfg(feature = "registry_surreal")]
use soulbase_storage::surreal::{SurrealConfig, SurrealDatastore};
#[cfg(feature = "registry_surreal")]
use soulbase_tools::prelude::SurrealToolRegistry;
use soulbase_tools::prelude::{
    InMemoryRegistry, ListFilter, ToolError, ToolId, ToolManifest, ToolRegistry,
};
use soulbase_types::prelude::TenantId;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = GatewayConfig::load()?;
    let state = AppState::new(config.clone()).await?;

    let routes = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/metrics", get(metrics))
        .route("/routes", get(list_routes))
        .route("/*path", any(dynamic_dispatch));

    let app = routes
        .with_state(state.clone())
        .layer(from_fn_with_state(state.clone(), metrics_middleware));

    let addr: SocketAddr = format!("{}:{}", config.server.address, config.server.port)
        .parse()
        .context("invalid server address/port")?;

    info!(%addr, "gateway listening");
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind to {addr}"))?;

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .context("gateway server failure")?;

    Ok(())
}

fn init_tracing() {
    if tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish(),
    )
    .is_err()
    {
        // Subscriber already set by tests or external runtime.
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct GatewayConfig {
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    services: Vec<ServiceConfig>,
    #[serde(default)]
    tools: ToolBootstrap,
}

impl GatewayConfig {
    fn load() -> anyhow::Result<Self> {
        let config_file = env::var("GATEWAY_CONFIG_FILE")
            .unwrap_or_else(|_| "config/gateway.local.toml".to_string());

        let mut builder = Config::builder()
            .set_default("server.address", ServerConfig::default_address())?
            .set_default("server.port", ServerConfig::default_port())?;

        if Path::new(&config_file).exists() {
            builder = builder.add_source(config::File::from(Path::new(&config_file)));
        }

        builder = builder.add_source(config::Environment::with_prefix("GATEWAY").separator("__"));

        let config: GatewayConfig = builder
            .build()
            .context("failed to build configuration")?
            .try_deserialize()
            .context("failed to deserialize configuration")?;

        Ok(config)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ServerConfig {
    #[serde(default = "ServerConfig::default_address")]
    address: String,
    #[serde(default = "ServerConfig::default_port")]
    port: u16,
}

impl ServerConfig {
    fn default_address() -> String {
        "127.0.0.1".to_string()
    }

    fn default_port() -> u16 {
        8080
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: Self::default_address(),
            port: Self::default_port(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
struct AuthConfig {
    #[serde(default)]
    api_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
struct SchemaConfig {
    #[serde(default)]
    required_headers: Vec<String>,
    #[serde(default)]
    required_fields: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ServiceKind {
    ToolExecute,
    ToolList,
    Other,
}

impl Default for ServiceKind {
    fn default() -> Self {
        ServiceKind::Other
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ServiceConfig {
    name: String,
    route: String,
    #[serde(default = "ServiceConfig::default_enabled")]
    enabled: bool,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    auth: Option<AuthConfig>,
    #[serde(default)]
    schema: Option<SchemaConfig>,
    #[serde(default)]
    kind: ServiceKind,
}

impl ServiceConfig {
    fn default_enabled() -> bool {
        true
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ToolBootstrap {
    #[serde(default)]
    registry: ToolRegistryConfig,
    #[serde(default)]
    manifests: Vec<ToolManifestEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ToolRegistryConfig {
    InMemory,
    #[cfg(feature = "registry_surreal")]
    Surreal(SurrealRegistrySettings),
}

impl Default for ToolRegistryConfig {
    fn default() -> Self {
        Self::InMemory
    }
}

#[cfg(feature = "registry_surreal")]
#[derive(Debug, Clone, Deserialize, Serialize)]
struct SurrealRegistrySettings {
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    database: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

impl Default for ToolBootstrap {
    fn default() -> Self {
        Self {
            registry: ToolRegistryConfig::default(),
            manifests: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ToolManifestEntry {
    tenant: String,
    #[serde(flatten)]
    manifest: ToolManifest,
}

#[derive(Clone)]
struct AppState {
    config: Arc<GatewayConfig>,
    version: VersionInfo,
    service_index: Arc<HashMap<String, ServiceConfig>>,
    metrics: GatewayMetrics,
    interceptor_chain: Arc<InterceptorChain>,
    tool_service: Arc<ToolService>,
}

impl AppState {
    async fn new(config: GatewayConfig) -> anyhow::Result<Self> {
        let service_index: Arc<HashMap<_, _>> = Arc::new(
            config
                .services
                .iter()
                .filter(|svc| svc.enabled)
                .map(|svc| (svc.route.clone(), svc.clone()))
                .collect(),
        );

        let default_api_key = env::var("GATEWAY_API_KEY").ok().filter(|v| !v.is_empty());
        let auth_stage = GatewayAuthStage::new(service_index.clone(), default_api_key);
        let schema_stage = GatewaySchemaStage::new(service_index.clone());

        let stages: Vec<Box<dyn Stage>> = vec![
            Box::new(ContextInitStage),
            Box::new(auth_stage),
            Box::new(schema_stage),
            Box::new(ErrorNormStage),
            Box::new(ResponseStampStage),
        ];
        let chain = Arc::new(InterceptorChain::new(stages));

        let tool_service = Arc::new(ToolService::from_bootstrap(&config.tools).await?);

        Ok(Self {
            config: Arc::new(config),
            version: VersionInfo::from_env(),
            service_index,
            metrics: GatewayMetrics::default(),
            interceptor_chain: chain,
            tool_service,
        })
    }

    fn lookup_service(&self, path: &str) -> Option<&ServiceConfig> {
        self.service_index.get(path)
    }
}

#[derive(Clone)]
struct VersionInfo {
    version: String,
    commit: Option<String>,
}

impl VersionInfo {
    fn from_env() -> Self {
        Self {
            version: env::var("GATEWAY_VERSION")
                .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string()),
            commit: env::var("GIT_COMMIT_HASH").ok(),
        }
    }
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

#[derive(Serialize)]
struct VersionResponse {
    version: String,
    commit: Option<String>,
}

async fn version(State(state): State<AppState>) -> impl IntoResponse {
    Json(VersionResponse {
        version: state.version.version.clone(),
        commit: state.version.commit.clone(),
    })
}

#[derive(Serialize)]
struct RouteInfo {
    name: String,
    route: String,
    description: Option<String>,
}

async fn list_routes(State(state): State<AppState>) -> impl IntoResponse {
    let routes: Vec<_> = state
        .config
        .services
        .iter()
        .filter(|svc| svc.enabled)
        .map(|svc| RouteInfo {
            name: svc.name.clone(),
            route: svc.route.clone(),
            description: svc.description.clone(),
        })
        .collect();
    Json(routes)
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let snapshot = state.metrics.snapshot().await;
    Json(snapshot)
}

async fn dynamic_dispatch(State(state): State<AppState>, req: Request<Body>) -> Response {
    let path = req.uri().path();
    if let Some(service) = state.lookup_service(path) {
        let chain = state.interceptor_chain.clone();
        match service.kind {
            ServiceKind::ToolExecute => {
                let tool_service = state.tool_service.clone();
                return handle_with_chain(req, &chain, move |cx, preq| {
                    let tool_service = tool_service.clone();
                    Box::pin(async move {
                        let body = preq.read_json().await?;
                        let payload: ToolExecutePayload =
                            serde_json::from_value(body).map_err(|e| {
                                InterceptError::schema(&format!("invalid request body: {e}"))
                            })?;
                        let tenant = payload
                            .tenant
                            .or_else(|| cx.tenant_header.clone())
                            .ok_or_else(|| InterceptError::schema("tenant missing"))?;
                        let tenant_id = TenantId(tenant);
                        let output = tool_service
                            .execute(&tenant_id, &payload.tool_id, payload.input)
                            .await
                            .map_err(tool_error_to_intercept)?;
                        Ok(json!({
                            "status": "success",
                            "tool_id": payload.tool_id,
                            "result": output,
                        }))
                    })
                })
                .await;
            }
            ServiceKind::ToolList => {
                let tool_service = state.tool_service.clone();
                return handle_with_chain(req, &chain, move |cx, _| {
                    let tool_service = tool_service.clone();
                    Box::pin(async move {
                        let tenant = cx
                            .tenant_header
                            .clone()
                            .ok_or_else(|| InterceptError::schema("tenant missing"))?;
                        let tenant_id = TenantId(tenant);
                        let tools = tool_service
                            .list(&tenant_id)
                            .await
                            .map_err(tool_error_to_intercept)?;
                        Ok(json!({ "tools": tools }))
                    })
                })
                .await;
            }
            ServiceKind::Other => {
                return handle_with_chain(req, &chain, move |_cx, _| {
                    Box::pin(async move {
                        Ok(json!({
                            "error": "not_implemented",
                            "message": "Service handler not implemented",
                        }))
                    })
                })
                .await;
            }
        }
    }

    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": "route_not_registered",
            "path": path,
        })),
    )
        .into_response()
}

async fn metrics_middleware(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let path = req.uri().path().to_string();
    let start = Instant::now();
    let response = next.run(req).await;
    let status = response.status();
    state.metrics.record(&path, status, start.elapsed()).await;
    Ok(response)
}

#[derive(Clone, Default)]
struct GatewayMetrics {
    inner: Arc<tokio::sync::Mutex<MetricsInner>>,
}

#[derive(Default)]
struct MetricsInner {
    total_requests: u64,
    total_errors: u64,
    routes: HashMap<String, RouteStats>,
}

#[derive(Default)]
struct RouteStats {
    request_count: u64,
    error_count: u64,
    total_latency_ms: u64,
}

impl GatewayMetrics {
    async fn record(&self, route: &str, status: StatusCode, latency: Duration) {
        let mut inner = self.inner.lock().await;
        inner.total_requests += 1;
        if status.is_client_error() || status.is_server_error() {
            inner.total_errors += 1;
        }
        let stats = inner.routes.entry(route.to_string()).or_default();
        stats.request_count += 1;
        if status.is_client_error() || status.is_server_error() {
            stats.error_count += 1;
        }
        stats.total_latency_ms += latency.as_millis() as u64;
    }

    async fn snapshot(&self) -> MetricsSnapshot {
        let inner = self.inner.lock().await;
        let routes = inner
            .routes
            .iter()
            .map(|(route, stats)| RouteMetrics {
                route: route.clone(),
                requests: stats.request_count,
                errors: stats.error_count,
                avg_latency_ms: if stats.request_count > 0 {
                    Some(stats.total_latency_ms as f64 / stats.request_count as f64)
                } else {
                    None
                },
            })
            .collect();
        MetricsSnapshot {
            total_requests: inner.total_requests,
            total_errors: inner.total_errors,
            routes,
        }
    }
}

#[derive(Serialize)]
struct MetricsSnapshot {
    total_requests: u64,
    total_errors: u64,
    routes: Vec<RouteMetrics>,
}

#[derive(Serialize)]
struct RouteMetrics {
    route: String,
    requests: u64,
    errors: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    avg_latency_ms: Option<f64>,
}

struct GatewayAuthStage {
    routes: Arc<HashMap<String, ServiceConfig>>,
    default_api_key: Option<String>,
}

impl GatewayAuthStage {
    fn new(routes: Arc<HashMap<String, ServiceConfig>>, default_api_key: Option<String>) -> Self {
        Self {
            routes,
            default_api_key,
        }
    }
}

#[async_trait]
impl Stage for GatewayAuthStage {
    async fn handle(
        &self,
        _cx: &mut InterceptContext,
        req: &mut dyn ProtoRequest,
        _rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        if let Some(service) = self.routes.get(req.path()) {
            let expected = service
                .auth
                .as_ref()
                .and_then(|a| a.api_key.clone())
                .or_else(|| self.default_api_key.clone());
            if let Some(api_key) = expected {
                match req.header("x-api-key") {
                    Some(ref provided) if provided == &api_key => {}
                    _ => {
                        return Err(InterceptError::from_public(
                            codes::AUTH_UNAUTHENTICATED,
                            "API key required",
                        ))
                    }
                }
            }
        }
        Ok(StageOutcome::Continue)
    }
}

struct GatewaySchemaStage {
    routes: Arc<HashMap<String, ServiceConfig>>,
}

impl GatewaySchemaStage {
    fn new(routes: Arc<HashMap<String, ServiceConfig>>) -> Self {
        Self { routes }
    }
}

#[async_trait]
impl Stage for GatewaySchemaStage {
    async fn handle(
        &self,
        _cx: &mut InterceptContext,
        req: &mut dyn ProtoRequest,
        _rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        let Some(service) = self.routes.get(req.path()) else {
            return Ok(StageOutcome::Continue);
        };
        let Some(schema) = service.schema.as_ref() else {
            return Ok(StageOutcome::Continue);
        };

        for header in &schema.required_headers {
            if req.header(header).is_none() {
                return Err(InterceptError::schema(&format!(
                    "missing required header {header}"
                )));
            }
        }

        if !schema.required_fields.is_empty() && req.method().to_ascii_uppercase() != "GET" {
            let body = req.read_json().await?;
            for field in &schema.required_fields {
                if body.get(field).is_none() {
                    return Err(InterceptError::schema(&format!(
                        "missing required field {field}"
                    )));
                }
            }
        }

        Ok(StageOutcome::Continue)
    }
}

#[derive(Clone)]
struct ToolService {
    registry: Arc<dyn ToolRegistry>,
}

impl ToolService {
    async fn from_bootstrap(bootstrap: &ToolBootstrap) -> anyhow::Result<Self> {
        let registry: Arc<dyn ToolRegistry> = match &bootstrap.registry {
            ToolRegistryConfig::InMemory => Arc::new(InMemoryRegistry::new()),
            #[cfg(feature = "registry_surreal")]
            ToolRegistryConfig::Surreal(settings) => {
                let mut config = SurrealConfig::default();
                if let Some(endpoint) = &settings.endpoint {
                    config.endpoint = endpoint.clone();
                }
                if let Some(namespace) = &settings.namespace {
                    config.namespace = namespace.clone();
                }
                if let Some(database) = &settings.database {
                    config.database = database.clone();
                }
                config.username = settings.username.clone();
                config.password = settings.password.clone();

                let datastore = SurrealDatastore::connect(config).await?;
                Arc::new(SurrealToolRegistry::new(Arc::new(datastore)))
            }
        };

        let service = Self { registry };
        service.apply_manifests(&bootstrap.manifests).await?;
        Ok(service)
    }

    async fn apply_manifests(&self, manifests: &[ToolManifestEntry]) -> anyhow::Result<()> {
        for entry in manifests {
            let tenant = TenantId(entry.tenant.clone());
            self.registry
                .upsert(&tenant, entry.manifest.clone())
                .await?;
        }
        Ok(())
    }

    async fn execute(
        &self,
        tenant: &TenantId,
        tool_id: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        let spec = self
            .registry
            .get(tenant, &ToolId(tool_id.to_string()))
            .await?
            .ok_or_else(|| ToolError::not_found(tool_id))?;
        spec.manifest.validate_input(&input)?;
        let output = json!({ "echo": input, "version": spec.manifest.version });
        spec.manifest.validate_output(&output)?;
        Ok(output)
    }

    async fn list(&self, tenant: &TenantId) -> Result<Vec<serde_json::Value>, ToolError> {
        let filter = ListFilter::default();
        let specs = self.registry.list(tenant, &filter).await?;
        Ok(specs
            .into_iter()
            .map(|spec| {
                json!({
                    "tool_id": spec.manifest.id.0,
                    "version": spec.manifest.version,
                    "display_name": spec.manifest.display_name,
                    "description": spec.manifest.description,
                    "tags": spec.manifest.tags,
                })
            })
            .collect())
    }
}

#[derive(Deserialize)]
struct ToolExecutePayload {
    #[serde(default)]
    tenant: Option<String>,
    tool_id: String,
    #[serde(default)]
    input: serde_json::Value,
}

fn tool_error_to_intercept(err: ToolError) -> InterceptError {
    InterceptError::from_error(err.into_inner())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        sigterm.recv().await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown signal received");
}

#[cfg(test)]
mod tests {
    use super::*;
    use soulbase_tools::prelude::{
        ConcurrencyKind, ConsentPolicy, IdempoKind, Limits, SafetyClass, SideEffect,
    };

    fn demo_manifest() -> ToolManifest {
        ToolManifest {
            id: ToolId("demo.echo".into()),
            version: "1.0.0".into(),
            display_name: "Echo Tool".into(),
            description: "Echoes the input payload back.".into(),
            tags: vec!["demo".into()],
            input_schema: Default::default(),
            output_schema: Default::default(),
            scopes: vec![],
            capabilities: vec![],
            side_effect: SideEffect::None,
            safety_class: SafetyClass::Low,
            consent: ConsentPolicy {
                required: false,
                max_ttl_ms: None,
            },
            limits: Limits {
                timeout_ms: 1_000,
                max_bytes_in: 4_096,
                max_bytes_out: 4_096,
                max_files: 0,
                max_depth: 0,
                max_concurrency: 1,
            },
            idempotency: IdempoKind::None,
            concurrency: ConcurrencyKind::Parallel,
        }
    }

    #[tokio::test]
    async fn tool_service_bootstrap_registers_manifests() -> anyhow::Result<()> {
        let bootstrap = ToolBootstrap {
            registry: ToolRegistryConfig::InMemory,
            manifests: vec![ToolManifestEntry {
                tenant: "tenant-test".into(),
                manifest: demo_manifest(),
            }],
        };

        let service = ToolService::from_bootstrap(&bootstrap).await?;
        let tenant = TenantId("tenant-test".into());

        let listed = service.list(&tenant).await?;
        assert_eq!(listed.len(), 1);

        let payload = serde_json::json!({ "foo": "bar" });
        let executed = service
            .execute(&tenant, "demo.echo", payload.clone())
            .await?;
        assert_eq!(executed["echo"], payload);

        Ok(())
    }
}
