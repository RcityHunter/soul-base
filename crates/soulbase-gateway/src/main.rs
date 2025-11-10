use std::{
    collections::{hash_map::Entry, HashMap},
    env, fs,
    net::SocketAddr,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
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
use futures_util::StreamExt;
use hex::decode as hex_decode;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use soulbase_auth::{
    pdp::Authorizer,
    prelude::{
        Action, AuthnInput, AuthzRequest, Obligation, PolicyAuthorizer, PolicyRule, ResourceUrn,
    },
    AuthFacade,
};
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
use soulbase_llm::prelude::{
    ChatRequest, ChatResponse, ContentSegment, EmbedItem, EmbedRequest, EmbedResponse,
    FinishReason, LlmError, LocalProviderFactory, Message, Registry, ResponseFormat, Role,
    StructOutPolicy, ToolSpec, Usage,
};
#[cfg(feature = "provider-openai")]
use soulbase_llm::prelude::{OpenAiConfig, OpenAiProviderFactory};
use soulbase_sandbox::{
    evidence::EvidenceRecord,
    prelude::{ExecOp, PolicyConfig, Sandbox},
};
use soulbase_storage::mock::{InMemoryRepository, MockDatastore};
use soulbase_storage::model::{Entity, QueryParams};
#[cfg(any(feature = "registry_surreal", feature = "idempo_surreal"))]
use soulbase_storage::surreal::{SurrealConfig, SurrealDatastore};
use soulbase_storage::{errors::StorageError, spi::repo::Repository};
#[cfg(feature = "registry_surreal")]
use soulbase_tools::prelude::SurrealToolRegistry;
use soulbase_tools::{
    events::{ToolInvokeBegin, ToolInvokeEnd},
    invoker::InvokeEvidence,
    prelude::{
        IdempoKind, InMemoryRegistry, InvokeRequest, InvokeResult, InvokeStatus, Invoker,
        InvokerImpl, ListFilter, ToolCall, ToolError, ToolId, ToolManifest, ToolOrigin,
        ToolRegistry,
    },
};
#[cfg(feature = "idempo_surreal")]
use soulbase_tx::prelude::{SurrealIdempoStore, SurrealOutboxStore};
use soulbase_tx::{
    memory::InMemoryIdempoStore,
    memory::InMemoryOutboxStore,
    model::{MsgId, OutboxMessage},
    outbox::OutboxStore,
    prelude::{IdempoStore, TxError},
    util::now_ms,
};
use soulbase_types::prelude::{Consent, Id, Subject, SubjectKind, TenantId};
use tokio::net::TcpListener;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{info, warn};

#[cfg(any(feature = "registry_surreal", feature = "idempo_surreal"))]
type SharedSurreal = Option<Arc<SurrealDatastore>>;
#[cfg(not(any(feature = "registry_surreal", feature = "idempo_surreal")))]
type SharedSurreal = Option<Arc<()>>;

type HmacSha256 = Hmac<Sha256>;

struct GatewayStorage {
    repo_store: Arc<dyn Repository<RepoRecordEntity>>,
    observe_store: Arc<dyn Repository<ObserveEventEntity>>,
    graph_store: Arc<dyn Repository<GraphEdgeEntity>>,
}

impl GatewayStorage {
    async fn from_bootstrap(config: &StorageBootstrap) -> anyhow::Result<Self> {
        match &config.repo {
            StorageBackendConfig::Memory => {}
        }
        match &config.observe {
            StorageBackendConfig::Memory => {}
        }
        match &config.graph {
            StorageBackendConfig::Memory => {}
        }

        let datastore = MockDatastore::new();
        let repo_store: Arc<dyn Repository<RepoRecordEntity>> =
            Arc::new(InMemoryRepository::<RepoRecordEntity>::new(&datastore));
        let observe_store: Arc<dyn Repository<ObserveEventEntity>> =
            Arc::new(InMemoryRepository::<ObserveEventEntity>::new(&datastore));
        let graph_store: Arc<dyn Repository<GraphEdgeEntity>> =
            Arc::new(InMemoryRepository::<GraphEdgeEntity>::new(&datastore));

        Ok(Self {
            repo_store,
            observe_store,
            graph_store,
        })
    }

    fn repo_store(&self) -> Arc<dyn Repository<RepoRecordEntity>> {
        self.repo_store.clone()
    }

    fn observe_store(&self) -> Arc<dyn Repository<ObserveEventEntity>> {
        self.observe_store.clone()
    }

    fn graph_store(&self) -> Arc<dyn Repository<GraphEdgeEntity>> {
        self.graph_store.clone()
    }
}

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
    #[serde(default)]
    llm: LlmBootstrap,
    #[serde(default)]
    auth: AuthBootstrap,
    #[serde(default)]
    storage: StorageBootstrap,
    #[serde(default)]
    policy: PolicyBootstrap,
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

fn resolve_secret_source(
    literal: &Option<String>,
    env_key: &Option<String>,
    file_path: &Option<String>,
    field: &str,
) -> anyhow::Result<String> {
    if let Some(env_var) = env_key.as_ref() {
        let value = env::var(env_var)
            .with_context(|| format!("environment variable {env_var} for {field} not set"))?;
        return Ok(value);
    }
    if let Some(path) = file_path.as_ref() {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("read secret file {path} for {field}"))?;
        return Ok(contents.trim().to_string());
    }
    if let Some(value) = literal.as_ref() {
        if value.is_empty() {
            return Err(anyhow!("{field} literal secret cannot be empty"));
        }
        return Ok(value.clone());
    }
    Err(anyhow!(
        "{field} secret must be provided via literal/env/file"
    ))
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
    #[serde(default)]
    scopes: Vec<String>,
    #[serde(default = "default_allow_anonymous")]
    allow_anonymous: bool,
}

fn default_allow_anonymous() -> bool {
    true
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
    ToolPreflight,
    CollabExecute,
    CollabResolve,
    LlmComplete,
    LlmStream,
    LlmEmbed,
    GraphRecall,
    ObserveEmit,
    RepoAppend,
    ConfigTools,
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
    #[serde(default)]
    policy: Option<ServicePolicyConfig>,
}

impl ServiceConfig {
    fn default_enabled() -> bool {
        true
    }

    fn policy_resource(&self) -> String {
        self.policy
            .as_ref()
            .and_then(|p| p.resource.clone())
            .unwrap_or_else(|| format!("route:{}", self.route))
    }

    fn policy_action(&self) -> Action {
        self.policy
            .as_ref()
            .and_then(|p| p.action.clone())
            .unwrap_or_else(|| match self.kind {
                ServiceKind::ToolExecute | ServiceKind::RepoAppend => Action::Write,
                ServiceKind::CollabExecute | ServiceKind::CollabResolve => Action::Invoke,
                ServiceKind::ToolPreflight | ServiceKind::ToolList => Action::List,
                ServiceKind::LlmComplete | ServiceKind::LlmStream | ServiceKind::LlmEmbed => {
                    Action::Invoke
                }
                ServiceKind::GraphRecall => Action::Read,
                ServiceKind::ObserveEmit => Action::Write,
                ServiceKind::ConfigTools => Action::Configure,
                _ => Action::Read,
            })
    }

    fn policy_attrs(&self) -> Option<Value> {
        self.policy.as_ref().and_then(|p| p.attrs.clone())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ServicePolicyConfig {
    #[serde(default)]
    resource: Option<String>,
    #[serde(default)]
    action: Option<Action>,
    #[serde(default)]
    attrs: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ToolBootstrap {
    #[serde(default)]
    registry: ToolRegistryConfig,
    #[serde(default)]
    idempotency: IdempotencyBootstrap,
    #[serde(default)]
    events: EventsBootstrap,
    #[serde(default)]
    evidence: EvidenceBootstrap,
    #[serde(default)]
    manifests: Vec<ToolManifestEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ToolRegistryConfig {
    InMemory,
    #[cfg(feature = "registry_surreal")]
    Surreal(SurrealBackendSettings),
}

impl Default for ToolRegistryConfig {
    fn default() -> Self {
        Self::InMemory
    }
}

#[cfg(any(feature = "registry_surreal", feature = "idempo_surreal"))]
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
struct SurrealBackendSettings {
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

#[cfg(any(feature = "registry_surreal", feature = "idempo_surreal"))]
impl SurrealBackendSettings {
    fn is_empty(&self) -> bool {
        self.endpoint.is_none()
            && self.namespace.is_none()
            && self.database.is_none()
            && self.username.is_none()
            && self.password.is_none()
    }

    fn apply_to(&self, config: &mut SurrealConfig) {
        if let Some(endpoint) = &self.endpoint {
            config.endpoint = endpoint.clone();
        }
        if let Some(namespace) = &self.namespace {
            config.namespace = namespace.clone();
        }
        if let Some(database) = &self.database {
            config.database = database.clone();
        }
        if let Some(username) = &self.username {
            config.username = Some(username.clone());
        }
        if let Some(password) = &self.password {
            config.password = Some(password.clone());
        }
    }
}

impl Default for ToolBootstrap {
    fn default() -> Self {
        Self {
            registry: ToolRegistryConfig::default(),
            idempotency: IdempotencyBootstrap::default(),
            events: EventsBootstrap::default(),
            evidence: EvidenceBootstrap::default(),
            manifests: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
struct AuthBootstrap {
    #[serde(default)]
    tokens: Vec<ApiTokenConfig>,
    #[serde(default)]
    hmac_keys: Vec<HmacKeyConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
struct StorageBootstrap {
    #[serde(default)]
    repo: StorageBackendConfig,
    #[serde(default)]
    observe: StorageBackendConfig,
    #[serde(default)]
    graph: StorageBackendConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StorageBackendConfig {
    Memory,
}

impl Default for StorageBackendConfig {
    fn default() -> Self {
        StorageBackendConfig::Memory
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
struct PolicyBootstrap {
    #[serde(default)]
    rules: Vec<PolicyRule>,
}

impl PolicyBootstrap {
    fn authorizer(&self) -> Option<PolicyAuthorizer> {
        if self.rules.is_empty() {
            None
        } else {
            Some(PolicyAuthorizer::new(self.rules.clone()))
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ApiTokenConfig {
    name: String,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    token_env: Option<String>,
    #[serde(default)]
    token_file: Option<String>,
    tenant: String,
    #[serde(default)]
    subject: Option<AuthSubjectConfig>,
    #[serde(default)]
    scopes: Vec<String>,
}

impl ApiTokenConfig {
    fn resolve(&self) -> anyhow::Result<(String, ResolvedPrincipal)> {
        let token = resolve_secret_source(
            &self.token,
            &self.token_env,
            &self.token_file,
            &format!("auth.tokens[{}].token", self.name),
        )?;
        let tenant = TenantId(self.tenant.clone());
        let subject_cfg = self.subject.clone().unwrap_or(AuthSubjectConfig {
            kind: SubjectKind::Service,
            subject_id: self.name.clone(),
            claims: serde_json::Map::new(),
        });
        Ok((
            token,
            ResolvedPrincipal {
                tenant: tenant.clone(),
                subject: subject_cfg.into_subject(tenant),
                scopes: self.scopes.clone(),
            },
        ))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct HmacKeyConfig {
    key_id: String,
    #[serde(default)]
    secret: Option<String>,
    #[serde(default)]
    secret_env: Option<String>,
    #[serde(default)]
    secret_file: Option<String>,
    tenant: String,
    #[serde(default)]
    subject: Option<AuthSubjectConfig>,
    #[serde(default)]
    scopes: Vec<String>,
}

impl HmacKeyConfig {
    fn resolve(&self) -> anyhow::Result<ResolvedHmacKey> {
        let secret = resolve_secret_source(
            &self.secret,
            &self.secret_env,
            &self.secret_file,
            &format!("auth.hmac_keys[{}].secret", self.key_id),
        )?;
        let tenant = TenantId(self.tenant.clone());
        let subject_cfg = self.subject.clone().unwrap_or(AuthSubjectConfig {
            kind: SubjectKind::Service,
            subject_id: self.key_id.clone(),
            claims: serde_json::Map::new(),
        });
        Ok(ResolvedHmacKey {
            secret: secret.into_bytes(),
            principal: ResolvedPrincipal {
                tenant: tenant.clone(),
                subject: subject_cfg.into_subject(tenant),
                scopes: self.scopes.clone(),
            },
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct AuthSubjectConfig {
    #[serde(default = "default_subject_kind")]
    kind: SubjectKind,
    subject_id: String,
    #[serde(default = "default_claims")]
    claims: serde_json::Map<String, Value>,
}

impl AuthSubjectConfig {
    fn into_subject(self, tenant: TenantId) -> Subject {
        Subject {
            kind: self.kind,
            subject_id: Id(self.subject_id),
            tenant,
            claims: self.claims,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct LlmBootstrap {
    #[serde(default = "LlmBootstrap::default_complete_model")]
    default_complete_model: String,
    #[serde(default = "LlmBootstrap::default_stream_model")]
    default_stream_model: String,
    #[serde(default = "LlmBootstrap::default_embed_model")]
    default_embed_model: String,
    #[serde(default)]
    providers: Vec<LlmProviderConfig>,
    #[serde(default)]
    struct_out_policy: StructPolicyConfig,
}

impl LlmBootstrap {
    fn default_complete_model() -> String {
        "local:echo".to_string()
    }

    fn default_stream_model() -> String {
        "local:echo".to_string()
    }

    fn default_embed_model() -> String {
        "local:emb".to_string()
    }

    fn struct_policy(&self) -> StructOutPolicy {
        self.struct_out_policy.into_policy()
    }

    fn defaults(&self) -> LlmDefaults {
        LlmDefaults {
            chat_model: self.default_complete_model.clone(),
            stream_model: self.default_stream_model.clone(),
            embed_model: self.default_embed_model.clone(),
        }
    }

    fn install_providers(&self, registry: &mut Registry) -> anyhow::Result<()> {
        if self.providers.is_empty() {
            LocalProviderFactory::install(registry);
            return Ok(());
        }

        for provider in &self.providers {
            provider.install(registry)?;
        }
        Ok(())
    }
}

impl Default for LlmBootstrap {
    fn default() -> Self {
        Self {
            default_complete_model: Self::default_complete_model(),
            default_stream_model: Self::default_stream_model(),
            default_embed_model: Self::default_embed_model(),
            providers: vec![LlmProviderConfig::Local],
            struct_out_policy: StructPolicyConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum LlmProviderConfig {
    Local,
    #[cfg(feature = "provider-openai")]
    OpenAi {
        #[serde(default)]
        api_key: Option<String>,
        #[serde(default)]
        api_key_env: Option<String>,
        #[serde(default)]
        base_url: Option<String>,
        #[serde(default)]
        organization: Option<String>,
        #[serde(default)]
        project: Option<String>,
        #[serde(default)]
        timeout_secs: Option<u64>,
        #[serde(default)]
        max_concurrent_requests: Option<usize>,
        #[serde(default)]
        requests_per_minute: Option<u32>,
        #[serde(default)]
        aliases: HashMap<String, String>,
    },
}

impl LlmProviderConfig {
    fn install(&self, registry: &mut Registry) -> anyhow::Result<()> {
        match self {
            LlmProviderConfig::Local => {
                LocalProviderFactory::install(registry);
                Ok(())
            }
            #[cfg(feature = "provider-openai")]
            LlmProviderConfig::OpenAi {
                api_key,
                api_key_env,
                base_url,
                organization,
                project,
                timeout_secs,
                max_concurrent_requests,
                requests_per_minute,
                aliases,
            } => {
                let key = if let Some(key) = api_key.as_ref() {
                    key.clone()
                } else if let Some(env_var) = api_key_env.as_ref() {
                    env::var(env_var)
                        .with_context(|| format!("openai api key env {env_var} missing"))?
                } else {
                    return Err(anyhow::Error::msg(
                        "openai provider requires api_key or api_key_env",
                    ));
                };

                let mut cfg = OpenAiConfig::new(key).map_err(anyhow::Error::new)?;
                if let Some(url) = base_url {
                    cfg = cfg.with_base_url(url).map_err(anyhow::Error::new)?;
                }
                if let Some(org) = organization {
                    cfg = cfg.with_organization(org.clone());
                }
                if let Some(project) = project {
                    cfg = cfg.with_project(project.clone());
                }
                if let Some(timeout) = timeout_secs {
                    cfg = cfg.with_timeout(Duration::from_secs(*timeout));
                }
                if let Some(limit) = max_concurrent_requests {
                    cfg = cfg.with_max_concurrency(*limit);
                }
                if let Some(rpm) = requests_per_minute {
                    cfg = cfg.with_rate_limit(*rpm);
                }
                for (alias, target) in aliases {
                    cfg = cfg.with_alias(alias.clone(), target.clone());
                }
                let factory = OpenAiProviderFactory::new(cfg).map_err(anyhow::Error::new)?;
                factory.install(registry);
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum StructPolicyConfig {
    Off,
    StrictReject,
    StrictRepair { max_attempts: u8 },
}

impl Default for StructPolicyConfig {
    fn default() -> Self {
        StructPolicyConfig::Off
    }
}

impl StructPolicyConfig {
    fn into_policy(&self) -> StructOutPolicy {
        match self {
            StructPolicyConfig::Off => StructOutPolicy::Off,
            StructPolicyConfig::StrictReject => StructOutPolicy::StrictReject,
            StructPolicyConfig::StrictRepair { max_attempts } => StructOutPolicy::StrictRepair {
                max_attempts: *max_attempts,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct IdempotencyBootstrap {
    #[serde(default = "IdempotencyBootstrap::default_enabled")]
    enabled: bool,
    #[serde(default = "IdempotencyBootstrap::default_ttl_ms")]
    ttl_ms: i64,
    #[serde(default = "IdempotencyBootstrap::default_failure_ttl_ms")]
    failure_ttl_ms: i64,
    #[serde(default)]
    store: IdempoStoreConfig,
}

impl IdempotencyBootstrap {
    fn default_enabled() -> bool {
        true
    }

    fn default_ttl_ms() -> i64 {
        300_000
    }

    fn default_failure_ttl_ms() -> i64 {
        60_000
    }
}

impl Default for IdempotencyBootstrap {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_ms: Self::default_ttl_ms(),
            failure_ttl_ms: Self::default_failure_ttl_ms(),
            store: IdempoStoreConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum IdempoStoreConfig {
    Memory,
    #[cfg(feature = "idempo_surreal")]
    Surreal(SurrealBackendSettings),
}

impl Default for IdempoStoreConfig {
    fn default() -> Self {
        Self::Memory
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct EventsBootstrap {
    #[serde(default = "EventsBootstrap::default_enabled")]
    enabled: bool,
    #[serde(default)]
    store: EventStoreConfig,
}

impl EventsBootstrap {
    fn default_enabled() -> bool {
        true
    }
}

impl Default for EventsBootstrap {
    fn default() -> Self {
        Self {
            enabled: true,
            store: EventStoreConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum EventStoreConfig {
    Memory,
    #[cfg(feature = "idempo_surreal")]
    Surreal(SurrealBackendSettings),
}

impl Default for EventStoreConfig {
    fn default() -> Self {
        Self::Memory
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct EvidenceBootstrap {
    #[serde(default = "EvidenceBootstrap::default_enabled")]
    enabled: bool,
    #[serde(default)]
    store: EvidenceStoreConfig,
}

impl EvidenceBootstrap {
    fn default_enabled() -> bool {
        true
    }
}

impl Default for EvidenceBootstrap {
    fn default() -> Self {
        Self {
            enabled: true,
            store: EvidenceStoreConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum EvidenceStoreConfig {
    Memory,
    #[cfg(feature = "idempo_surreal")]
    Surreal(SurrealBackendSettings),
}

impl Default for EvidenceStoreConfig {
    fn default() -> Self {
        Self::Memory
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
    collab_service: Arc<CollabService>,
    llm_service: Arc<LlmService>,
    graph_service: Arc<GraphService>,
    observe_service: Arc<ObserveService>,
    repo_service: Arc<RepoService>,
    tool_registry: Arc<ToolConfigRegistry>,
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
        let auth_stage =
            GatewayAuthStage::new(service_index.clone(), default_api_key, &config.auth)?;
        let schema_stage = GatewaySchemaStage::new(service_index.clone());

        let policy_stage = GatewayPolicyStage::new(
            service_index.clone(),
            config.policy.authorizer().map(Arc::new),
        );

        let stages: Vec<Box<dyn Stage>> = vec![
            Box::new(ContextInitStage),
            Box::new(auth_stage),
            Box::new(schema_stage),
            Box::new(policy_stage),
            Box::new(ErrorNormStage),
            Box::new(ResponseStampStage),
        ];
        let chain = Arc::new(InterceptorChain::new(stages));

        let storage = Arc::new(GatewayStorage::from_bootstrap(&config.storage).await?);

        let tool_service = Arc::new(ToolService::from_bootstrap(&config.tools).await?);
        let tool_registry = Arc::new(ToolConfigRegistry::new(config.tools.manifests.clone()));
        let collab_service = Arc::new(CollabService::default());
        let llm_service = Arc::new(LlmService::from_bootstrap(&config.llm).await?);
        let graph_service = Arc::new(GraphService::new(storage.graph_store()));
        let observe_service = Arc::new(ObserveService::new(storage.observe_store()));
        let repo_service = Arc::new(RepoService::new(
            graph_service.clone(),
            storage.repo_store(),
        ));

        Ok(Self {
            config: Arc::new(config),
            version: VersionInfo::from_env(),
            service_index,
            metrics: GatewayMetrics::default(),
            interceptor_chain: chain,
            tool_service,
            collab_service,
            llm_service,
            graph_service,
            observe_service,
            repo_service,
            tool_registry,
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
                        let ToolExecutePayload {
                            tenant,
                            tool_id,
                            input,
                            actor,
                            origin,
                            idempotency_key,
                            consent,
                            call_id,
                        } = payload;
                        let tenant = tenant
                            .or_else(|| cx.tenant_header.clone())
                            .ok_or_else(|| InterceptError::schema("tenant missing"))?;
                        let tenant_id = TenantId(tenant.clone());
                        let subject = actor
                            .map(|actor| actor.into_subject(tenant_id.clone()))
                            .unwrap_or_else(|| default_actor(tenant_id.clone()));
                        let invocation = ToolInvocationRequest {
                            tenant: tenant_id.clone(),
                            tool_id: tool_id.clone(),
                            input,
                            actor: subject,
                            origin: resolve_origin(origin),
                            consent,
                            idempotency_key,
                            call_id: call_id.map(Id),
                        };
                        let outcome = tool_service
                            .execute(invocation)
                            .await
                            .map_err(tool_error_to_intercept)?;
                        let status_text = outcome.status_str();
                        let ToolExecutionResponse {
                            status: _,
                            output,
                            obligations,
                            budget_snapshot,
                            degradation,
                            error_code,
                            evidence_ref,
                            tool_id: executed_tool_id,
                        } = outcome;
                        let result_value = output.unwrap_or(serde_json::Value::Null);
                        Ok(json!({
                            "status": status_text,
                            "tool_id": executed_tool_id,
                            "tenant": tenant,
                            "result": result_value,
                            "obligations": obligations,
                            "budget": budget_snapshot,
                            "degradation": degradation,
                            "error_code": error_code,
                            "evidence_ref": evidence_ref,
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
            ServiceKind::CollabExecute => {
                let collab_service = state.collab_service.clone();
                return handle_with_chain(req, &chain, move |cx, preq| {
                    let collab_service = collab_service.clone();
                    Box::pin(async move {
                        let body = preq.read_json().await?;
                        let payload: CollabExecutePayload =
                            serde_json::from_value(body).map_err(|e| {
                                InterceptError::schema(&format!("invalid request body: {e}"))
                            })?;
                        let tenant = payload
                            .tenant
                            .clone()
                            .or_else(|| cx.tenant_header.clone())
                            .ok_or_else(|| InterceptError::schema("tenant missing"))?;
                        let tenant_id = TenantId(tenant);
                        let snapshot = collab_service
                            .execute(&tenant_id, payload)
                            .await
                            .map_err(collab_error_to_intercept)?;
                        Ok(snapshot.into_json())
                    })
                })
                .await;
            }
            ServiceKind::CollabResolve => {
                let collab_service = state.collab_service.clone();
                return handle_with_chain(req, &chain, move |cx, preq| {
                    let collab_service = collab_service.clone();
                    Box::pin(async move {
                        let body = preq.read_json().await?;
                        let payload: CollabResolvePayload =
                            serde_json::from_value(body).map_err(|e| {
                                InterceptError::schema(&format!("invalid request body: {e}"))
                            })?;
                        let tenant = payload
                            .tenant
                            .clone()
                            .or_else(|| cx.tenant_header.clone())
                            .ok_or_else(|| InterceptError::schema("tenant missing"))?;
                        let tenant_id = TenantId(tenant);
                        let snapshot = collab_service
                            .resolve(&tenant_id, payload)
                            .await
                            .map_err(collab_error_to_intercept)?;
                        Ok(snapshot.into_json())
                    })
                })
                .await;
            }
            ServiceKind::LlmComplete => {
                let llm_service = state.llm_service.clone();
                return handle_with_chain(req, &chain, move |cx, preq| {
                    let llm_service = llm_service.clone();
                    Box::pin(async move {
                        let body = preq.read_json().await?;
                        let payload: LlmCompletePayload =
                            serde_json::from_value(body).map_err(|e| {
                                InterceptError::schema(&format!("invalid request body: {e}"))
                            })?;
                        let tenant = payload
                            .tenant
                            .clone()
                            .or_else(|| cx.tenant_header.clone())
                            .ok_or_else(|| InterceptError::schema("tenant missing"))?;
                        let tenant_id = TenantId(tenant);
                        let response = llm_service
                            .complete(&tenant_id, payload)
                            .await
                            .map_err(llm_error_to_intercept)?;
                        Ok(response.into_json())
                    })
                })
                .await;
            }
            ServiceKind::LlmStream => {
                let llm_service = state.llm_service.clone();
                return handle_with_chain(req, &chain, move |cx, preq| {
                    let llm_service = llm_service.clone();
                    Box::pin(async move {
                        let body = preq.read_json().await?;
                        let payload: LlmStreamPayload =
                            serde_json::from_value(body).map_err(|e| {
                                InterceptError::schema(&format!("invalid request body: {e}"))
                            })?;
                        let tenant = payload
                            .tenant
                            .clone()
                            .or_else(|| cx.tenant_header.clone())
                            .ok_or_else(|| InterceptError::schema("tenant missing"))?;
                        let tenant_id = TenantId(tenant);
                        let response = llm_service
                            .stream(&tenant_id, payload)
                            .await
                            .map_err(llm_error_to_intercept)?;
                        Ok(response.into_json())
                    })
                })
                .await;
            }
            ServiceKind::LlmEmbed => {
                let llm_service = state.llm_service.clone();
                return handle_with_chain(req, &chain, move |cx, preq| {
                    let llm_service = llm_service.clone();
                    Box::pin(async move {
                        let body = preq.read_json().await?;
                        let payload: LlmEmbedPayload =
                            serde_json::from_value(body).map_err(|e| {
                                InterceptError::schema(&format!("invalid request body: {e}"))
                            })?;
                        let tenant = payload
                            .tenant
                            .clone()
                            .or_else(|| cx.tenant_header.clone())
                            .ok_or_else(|| InterceptError::schema("tenant missing"))?;
                        let tenant_id = TenantId(tenant);
                        let response = llm_service
                            .embed(&tenant_id, payload)
                            .await
                            .map_err(llm_error_to_intercept)?;
                        Ok(response.into_json())
                    })
                })
                .await;
            }
            ServiceKind::GraphRecall => {
                let graph_service = state.graph_service.clone();
                return handle_with_chain(req, &chain, move |cx, preq| {
                    let graph_service = graph_service.clone();
                    Box::pin(async move {
                        let body = preq.read_json().await?;
                        let payload: GraphRecallPayload =
                            serde_json::from_value(body).map_err(|e| {
                                InterceptError::schema(&format!("invalid request body: {e}"))
                            })?;
                        let tenant = payload
                            .tenant
                            .clone()
                            .or_else(|| cx.tenant_header.clone())
                            .ok_or_else(|| InterceptError::schema("tenant missing"))?;
                        let tenant_id = TenantId(tenant);
                        let response = graph_service
                            .recall(&tenant_id, payload.node_id, payload.label, payload.limit)
                            .await;
                        Ok(response.into_json())
                    })
                })
                .await;
            }
            ServiceKind::ObserveEmit => {
                let observe_service = state.observe_service.clone();
                return handle_with_chain(req, &chain, move |cx, preq| {
                    let observe_service = observe_service.clone();
                    Box::pin(async move {
                        let body = preq.read_json().await?;
                        let payload: ObserveEmitPayload =
                            serde_json::from_value(body).map_err(|e| {
                                InterceptError::schema(&format!("invalid request body: {e}"))
                            })?;
                        let tenant = payload
                            .tenant
                            .clone()
                            .or_else(|| cx.tenant_header.clone())
                            .ok_or_else(|| InterceptError::schema("tenant missing"))?;
                        let tenant_id = TenantId(tenant);
                        let response = observe_service
                            .emit(&tenant_id, payload)
                            .await
                            .map_err(observe_error_to_intercept)?;
                        Ok(response.into_json())
                    })
                })
                .await;
            }
            ServiceKind::RepoAppend => {
                let repo_service = state.repo_service.clone();
                return handle_with_chain(req, &chain, move |cx, preq| {
                    let repo_service = repo_service.clone();
                    Box::pin(async move {
                        let body = preq.read_json().await?;
                        let payload: RepoAppendPayload =
                            serde_json::from_value(body).map_err(|e| {
                                InterceptError::schema(&format!("invalid request body: {e}"))
                            })?;
                        let tenant = payload
                            .tenant
                            .clone()
                            .or_else(|| cx.tenant_header.clone())
                            .ok_or_else(|| InterceptError::schema("tenant missing"))?;
                        let tenant_id = TenantId(tenant);
                        let response = repo_service
                            .append(&tenant_id, payload)
                            .await
                            .map_err(repo_error_to_intercept)?;
                        Ok(response.into_json())
                    })
                })
                .await;
            }
            ServiceKind::ConfigTools => {
                #[allow(unused_mut)]
                let registry = state.tool_registry.clone();
                let tool_service = state.tool_service.clone();
                return handle_with_chain(req, &chain, move |_cx, preq| {
                    let registry = registry.clone();
                    let tool_service = tool_service.clone();
                    Box::pin(async move {
                        match preq.method().to_ascii_uppercase().as_str() {
                            "GET" => {
                                let snapshots = registry.list().await;
                                Ok(json!({ "versions": snapshots }))
                            }
                            "POST" => {
                                let body = preq.read_json().await?;
                                let payload: ToolConfigPublishPayload =
                                    serde_json::from_value(body).map_err(|err| {
                                        InterceptError::schema(&format!(
                                            "invalid publish payload: {err}"
                                        ))
                                    })?;
                                let snapshot = registry
                                    .publish(payload, tool_service.clone())
                                    .await
                                    .map_err(config_error_to_intercept)?;
                                Ok(json!({ "published": snapshot }))
                            }
                            other => Err(InterceptError::schema(&format!(
                                "unsupported method {other}"
                            ))),
                        }
                    })
                })
                .await;
            }
            ServiceKind::ToolPreflight => {
                let tool_service = state.tool_service.clone();
                return handle_with_chain(req, &chain, move |cx, preq| {
                    let tool_service = tool_service.clone();
                    Box::pin(async move {
                        let body = preq.read_json().await?;
                        let payload: ToolExecutePayload =
                            serde_json::from_value(body).map_err(|e| {
                                InterceptError::schema(&format!("invalid request body: {e}"))
                            })?;
                        let ToolExecutePayload {
                            tenant,
                            tool_id,
                            input,
                            actor,
                            origin,
                            idempotency_key,
                            consent,
                            call_id,
                        } = payload;
                        let tenant = tenant
                            .or_else(|| cx.tenant_header.clone())
                            .ok_or_else(|| InterceptError::schema("tenant missing"))?;
                        let tenant_id = TenantId(tenant.clone());
                        let subject = actor
                            .map(|actor| actor.into_subject(tenant_id.clone()))
                            .unwrap_or_else(|| default_actor(tenant_id.clone()));
                        let invocation = ToolInvocationRequest {
                            tenant: tenant_id.clone(),
                            tool_id: tool_id.clone(),
                            input,
                            actor: subject,
                            origin: resolve_origin(origin),
                            consent,
                            idempotency_key,
                            call_id: call_id.map(Id),
                        };
                        let outcome = tool_service
                            .preflight(invocation)
                            .await
                            .map_err(tool_error_to_intercept)?;
                        Ok(json!({
                            "allow": outcome.allow,
                            "tool_id": tool_id,
                            "tenant": tenant,
                            "reason": outcome.reason,
                            "profile_hash": outcome.profile_hash,
                            "obligations": outcome.obligations,
                            "budget": outcome.budget_snapshot,
                            "planned_ops": outcome.planned_ops,
                        }))
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
    tokens: Arc<HashMap<String, ResolvedPrincipal>>,
    hmac_keys: Arc<HashMap<String, ResolvedHmacKey>>,
}

impl GatewayAuthStage {
    fn new(
        routes: Arc<HashMap<String, ServiceConfig>>,
        default_api_key: Option<String>,
        auth: &AuthBootstrap,
    ) -> anyhow::Result<Self> {
        let mut token_map = HashMap::new();
        for cfg in &auth.tokens {
            let (token, principal) = cfg.resolve()?;
            token_map.insert(token, principal);
        }
        let mut hmac_map = HashMap::new();
        for cfg in &auth.hmac_keys {
            hmac_map.insert(cfg.key_id.clone(), cfg.resolve()?);
        }
        Ok(Self {
            routes,
            default_api_key,
            tokens: Arc::new(token_map),
            hmac_keys: Arc::new(hmac_map),
        })
    }

    fn verify_hmac(
        &self,
        spec: &str,
        req: &mut dyn ProtoRequest,
    ) -> Result<(ResolvedPrincipal, String), InterceptError> {
        let (key_id, signature_hex) = spec
            .split_once(':')
            .ok_or_else(|| unauthenticated("invalid hmac authorization format"))?;
        let key = self
            .hmac_keys
            .get(key_id)
            .ok_or_else(|| unauthenticated("unknown hmac key"))?;
        let signature = hex_decode(signature_hex)
            .map_err(|_| unauthenticated("invalid hmac signature encoding"))?;
        let date = req
            .header("x-sb-date")
            .ok_or_else(|| unauthenticated("missing x-sb-date header"))?;
        let canonical = format!(
            "{}\n{}\n{}",
            req.method().to_ascii_uppercase(),
            req.path(),
            date
        );
        let mut mac = HmacSha256::new_from_slice(&key.secret)
            .map_err(|_| unauthenticated("invalid hmac key"))?;
        mac.update(canonical.as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| unauthenticated("invalid hmac signature"))?;
        Ok((key.principal.clone(), key_id.to_string()))
    }
}

#[async_trait]
impl Stage for GatewayAuthStage {
    async fn handle(
        &self,
        cx: &mut InterceptContext,
        req: &mut dyn ProtoRequest,
        _rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        let Some(service) = self.routes.get(req.path()) else {
            return Ok(StageOutcome::Continue);
        };
        let auth_cfg = service.auth.as_ref();

        let mut principal: Option<ResolvedPrincipal> = None;
        let mut captured_authn: Option<AuthnInput> = None;
        if let Some(header) = req.header("authorization") {
            if let Some(token) = header.strip_prefix("Bearer ") {
                let resolved = self
                    .tokens
                    .get(token)
                    .ok_or_else(|| unauthenticated("invalid bearer token"))?;
                principal = Some(resolved.clone());
                captured_authn = Some(AuthnInput::BearerJwt(token.to_string()));
            } else if let Some(spec) = header.strip_prefix("SB-HMAC ") {
                let (resolved, key_id) = self.verify_hmac(spec, req)?;
                captured_authn = Some(AuthnInput::ServiceToken(format!("hmac:{key_id}")));
                principal = Some(resolved);
            }
        }

        let mut used_api_key = false;
        if principal.is_none() {
            let expected_key = auth_cfg
                .and_then(|cfg| cfg.api_key.clone())
                .or_else(|| self.default_api_key.clone());
            if let Some(api_key) = expected_key {
                used_api_key = true;
                match req.header("x-api-key") {
                    Some(ref provided) if provided == &api_key => {
                        captured_authn = Some(AuthnInput::ApiKey(api_key));
                    }
                    _ => return Err(unauthenticated("API key required")),
                }
            }
        }

        if let Some(cfg) = auth_cfg {
            if !cfg.scopes.is_empty() {
                let Some(principal_ref) = principal.as_ref() else {
                    return Err(unauthenticated(
                        "scoped route requires authenticated principal",
                    ));
                };
                cfg.ensure_scopes(principal_ref)?;
            }

            if !cfg.allow_anonymous && principal.is_none() && !used_api_key {
                return Err(unauthenticated("authentication required"));
            }
        }

        if let Some(principal) = principal {
            cx.subject = Some(principal.subject.clone());
            if cx.tenant_header.is_none() {
                cx.tenant_header = Some(principal.tenant.0.clone());
            }
            if let Some(authn) = captured_authn {
                cx.authn_input = Some(authn);
            }
        }
        Ok(StageOutcome::Continue)
    }
}

#[derive(Clone)]
struct ResolvedPrincipal {
    tenant: TenantId,
    subject: Subject,
    scopes: Vec<String>,
}

impl ResolvedPrincipal {
    fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope || s == "*")
    }
}

#[derive(Clone)]
struct ResolvedHmacKey {
    secret: Vec<u8>,
    principal: ResolvedPrincipal,
}

impl AuthConfig {
    fn ensure_scopes(&self, principal: &ResolvedPrincipal) -> Result<(), InterceptError> {
        for scope in &self.scopes {
            if !principal.has_scope(scope) {
                return Err(forbidden("insufficient scope"));
            }
        }
        Ok(())
    }
}

fn unauthenticated(msg: &str) -> InterceptError {
    InterceptError::from_public(codes::AUTH_UNAUTHENTICATED, msg)
}

fn forbidden(msg: &str) -> InterceptError {
    InterceptError::from_public(codes::AUTH_FORBIDDEN, msg)
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

struct GatewayPolicyStage {
    routes: Arc<HashMap<String, ServiceConfig>>,
    authorizer: Option<Arc<PolicyAuthorizer>>,
}

impl GatewayPolicyStage {
    fn new(
        routes: Arc<HashMap<String, ServiceConfig>>,
        authorizer: Option<Arc<PolicyAuthorizer>>,
    ) -> Self {
        Self { routes, authorizer }
    }

    fn default_attrs(
        &self,
        service: &ServiceConfig,
        cx: &InterceptContext,
        req: &dyn ProtoRequest,
    ) -> Value {
        let mut map = serde_json::Map::new();
        map.insert("service".into(), Value::String(service.name.clone()));
        map.insert("route".into(), Value::String(service.route.clone()));
        map.insert("method".into(), Value::String(req.method().to_string()));
        if let Some(tenant) = cx.tenant_header.as_ref() {
            map.insert("tenant".into(), Value::String(tenant.clone()));
        }
        if let Some(Value::Object(extra)) = service.policy_attrs() {
            for (k, v) in extra {
                map.insert(k, v);
            }
        }
        Value::Object(map)
    }
}

#[async_trait]
impl Stage for GatewayPolicyStage {
    async fn handle(
        &self,
        cx: &mut InterceptContext,
        req: &mut dyn ProtoRequest,
        _rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        let Some(authorizer) = &self.authorizer else {
            return Ok(StageOutcome::Continue);
        };
        let Some(service) = self.routes.get(req.path()) else {
            return Ok(StageOutcome::Continue);
        };
        let Some(subject) = cx.subject.clone() else {
            return Ok(StageOutcome::Continue);
        };

        let resource = ResourceUrn(service.policy_resource());
        let action = service.policy_action();
        let attrs = self.default_attrs(service, cx, req);
        let decision = authorizer
            .decide(
                &AuthzRequest {
                    subject: subject.clone(),
                    resource,
                    action: action.clone(),
                    attrs: attrs.clone(),
                    consent: None,
                    correlation_id: None,
                },
                &attrs,
            )
            .await
            .map_err(|err| InterceptError::internal(&format!("policy evaluate: {err}")))?;

        if decision.allow {
            cx.obligations.extend(decision.obligations);
            Ok(StageOutcome::Continue)
        } else {
            Err(InterceptError::deny_policy(
                decision.reason.as_deref().unwrap_or("policy-denied"),
            ))
        }
    }
}

#[derive(Clone)]
struct ToolService {
    registry: Arc<dyn ToolRegistry>,
    invoker: Arc<dyn Invoker>,
    idempotency: Option<Arc<IdempotencyContext>>,
    events: Option<Arc<ToolEventPublisher>>,
    evidence_store: Arc<dyn ToolEvidenceStore>,
}

impl ToolService {
    async fn from_bootstrap(bootstrap: &ToolBootstrap) -> anyhow::Result<Self> {
        let mut shared_surreal: SharedSurreal = None;
        let registry: Arc<dyn ToolRegistry> = match &bootstrap.registry {
            ToolRegistryConfig::InMemory => Arc::new(InMemoryRegistry::new()),
            #[cfg(feature = "registry_surreal")]
            ToolRegistryConfig::Surreal(settings) => {
                let mut config = SurrealConfig::default();
                settings.apply_to(&mut config);
                let datastore = Arc::new(SurrealDatastore::connect(config).await?);
                shared_surreal = Some(datastore.clone());
                Arc::new(SurrealToolRegistry::new(datastore))
            }
        };

        let idempotency =
            Self::init_idempotency(&bootstrap.idempotency, &mut shared_surreal).await?;
        let events = Self::init_events(&bootstrap.events, &mut shared_surreal).await?;
        let evidence_store =
            Self::init_evidence_store(&bootstrap.evidence, &mut shared_surreal).await?;

        let auth = Arc::new(AuthFacade::minimal());
        let sandbox = Sandbox::minimal();
        let policy_base = PolicyConfig::default();
        let invoker = Arc::new(InvokerImpl::new(
            registry.clone(),
            auth,
            sandbox,
            policy_base,
        ));

        let service = Self {
            registry,
            invoker,
            idempotency,
            events,
            evidence_store,
        };
        service.apply_manifests(&bootstrap.manifests).await?;
        Ok(service)
    }

    #[cfg(any(feature = "registry_surreal", feature = "idempo_surreal"))]
    async fn init_idempotency(
        config: &IdempotencyBootstrap,
        shared_surreal: &mut SharedSurreal,
    ) -> anyhow::Result<Option<Arc<IdempotencyContext>>> {
        if !config.enabled {
            return Ok(None);
        }

        let store: Arc<dyn IdempoStore> = match &config.store {
            IdempoStoreConfig::Memory => Arc::new(InMemoryIdempoStore::default()),
            #[cfg(feature = "idempo_surreal")]
            IdempoStoreConfig::Surreal(settings) => {
                let datastore = if !settings.is_empty() || shared_surreal.is_none() {
                    let mut cfg = SurrealConfig::default();
                    settings.apply_to(&mut cfg);
                    let datastore = Arc::new(SurrealDatastore::connect(cfg).await?);
                    *shared_surreal = Some(datastore.clone());
                    datastore
                } else {
                    shared_surreal.as_ref().unwrap().clone()
                };
                Arc::new(SurrealIdempoStore::new(datastore))
            }
        };

        Ok(Some(Arc::new(IdempotencyContext {
            store,
            ttl_ms: config.ttl_ms,
            failure_ttl_ms: config.failure_ttl_ms,
        })))
    }

    #[cfg(not(any(feature = "registry_surreal", feature = "idempo_surreal")))]
    async fn init_idempotency(
        config: &IdempotencyBootstrap,
        _shared_surreal: &mut SharedSurreal,
    ) -> anyhow::Result<Option<Arc<IdempotencyContext>>> {
        if !config.enabled {
            return Ok(None);
        }

        let store: Arc<dyn IdempoStore> = Arc::new(InMemoryIdempoStore::default());
        Ok(Some(Arc::new(IdempotencyContext {
            store,
            ttl_ms: config.ttl_ms,
            failure_ttl_ms: config.failure_ttl_ms,
        })))
    }

    #[cfg(any(feature = "registry_surreal", feature = "idempo_surreal"))]
    async fn init_events(
        config: &EventsBootstrap,
        shared_surreal: &mut SharedSurreal,
    ) -> anyhow::Result<Option<Arc<ToolEventPublisher>>> {
        if !config.enabled {
            return Ok(None);
        }

        let store: Arc<dyn OutboxStore> = match &config.store {
            EventStoreConfig::Memory => Arc::new(InMemoryOutboxStore::default()),
            #[cfg(feature = "idempo_surreal")]
            EventStoreConfig::Surreal(settings) => {
                let datastore = if !settings.is_empty() || shared_surreal.is_none() {
                    let mut cfg = SurrealConfig::default();
                    settings.apply_to(&mut cfg);
                    let datastore = Arc::new(SurrealDatastore::connect(cfg).await?);
                    *shared_surreal = Some(datastore.clone());
                    datastore
                } else {
                    shared_surreal.as_ref().unwrap().clone()
                };
                Arc::new(SurrealOutboxStore::new(datastore))
            }
        };

        Ok(Some(Arc::new(ToolEventPublisher::new(store))))
    }

    #[cfg(not(any(feature = "registry_surreal", feature = "idempo_surreal")))]
    async fn init_events(
        config: &EventsBootstrap,
        _shared_surreal: &mut SharedSurreal,
    ) -> anyhow::Result<Option<Arc<ToolEventPublisher>>> {
        if !config.enabled {
            return Ok(None);
        }

        let store: Arc<dyn OutboxStore> = Arc::new(InMemoryOutboxStore::default());
        Ok(Some(Arc::new(ToolEventPublisher::new(store))))
    }

    #[allow(unused_variables)]
    async fn init_evidence_store(
        config: &EvidenceBootstrap,
        shared_surreal: &mut SharedSurreal,
    ) -> anyhow::Result<Arc<dyn ToolEvidenceStore>> {
        if !config.enabled {
            return Ok(Arc::new(InMemoryEvidenceStore::default()));
        }

        let store: Arc<dyn ToolEvidenceStore> = match &config.store {
            EvidenceStoreConfig::Memory => Arc::new(InMemoryEvidenceStore::default()),
            #[cfg(feature = "idempo_surreal")]
            EvidenceStoreConfig::Surreal(settings) => {
                let datastore = if !settings.is_empty() || shared_surreal.is_none() {
                    let mut cfg = SurrealConfig::default();
                    settings.apply_to(&mut cfg);
                    let datastore = Arc::new(SurrealDatastore::connect(cfg).await?);
                    *shared_surreal = Some(datastore.clone());
                    datastore
                } else {
                    shared_surreal.as_ref().unwrap().clone()
                };
                Arc::new(SurrealEvidenceStore::new(datastore))
            }
        };

        Ok(store)
    }

    pub async fn apply_manifests(&self, manifests: &[ToolManifestEntry]) -> anyhow::Result<()> {
        for entry in manifests {
            let tenant = TenantId(entry.tenant.clone());
            self.registry
                .upsert(&tenant, entry.manifest.clone())
                .await?;
        }
        Ok(())
    }

    async fn preflight(
        &self,
        request: ToolInvocationRequest,
    ) -> Result<ToolPreflightResponse, ToolError> {
        let call = request.into_call();
        let result = self.invoker.preflight(&call).await?;
        Ok(ToolPreflightResponse {
            allow: result.allow,
            reason: result.reason,
            profile_hash: result.profile_hash,
            obligations: result.obligations,
            budget_snapshot: result.budget_snapshot,
            planned_ops: result.planned_ops,
        })
    }

    async fn check_idempotency(
        &self,
        call: &ToolCall,
        manifest: &ToolManifest,
    ) -> Result<IdempoDecision, ToolError> {
        match manifest.idempotency {
            IdempoKind::None => return Ok(IdempoDecision::Skip),
            _ => {}
        }

        let ctx = match &self.idempotency {
            Some(ctx) => ctx.clone(),
            None => {
                return Err(ToolError::unknown(
                    "idempotency store not configured for tool requiring idempotency",
                ))
            }
        };

        let key = match manifest.idempotency {
            IdempoKind::None => return Ok(IdempoDecision::Skip),
            IdempoKind::Keyed => call
                .idempotency_key
                .as_ref()
                .cloned()
                .ok_or_else(|| ToolError::schema("idempotency_key required"))?,
            IdempoKind::Global => format!("global::{}", manifest.id.0),
        };

        let hash =
            compute_idempotency_hash(&call.tenant, &manifest.id.0, &manifest.version, &call.args)?;

        match ctx
            .store
            .check_and_put(&call.tenant, &key, &hash, ctx.ttl_ms)
            .await
        {
            Ok(Some(digest)) => {
                let cached = cached_execution_from_digest(&digest)?;
                Ok(IdempoDecision::Cached(cached))
            }
            Ok(None) => Ok(IdempoDecision::Pending(IdempoPending { ctx, key, hash })),
            Err(err) => Err(tx_error_to_tool(err)),
        }
    }

    async fn execute(
        &self,
        request: ToolInvocationRequest,
    ) -> Result<ToolExecutionResponse, ToolError> {
        let tool_id = request.tool_id.clone();
        let call = request.into_call();
        let tenant = call.tenant.clone();
        let preflight = self.invoker.preflight(&call).await?;
        if !preflight.allow {
            let reason = preflight
                .reason
                .unwrap_or_else(|| "tool_invocation_denied".to_string());
            return Err(ToolError::policy(&reason));
        }

        let spec = preflight
            .spec
            .clone()
            .ok_or_else(|| ToolError::unknown("preflight missing spec"))?;
        let profile_hash = preflight
            .profile_hash
            .clone()
            .ok_or_else(|| ToolError::unknown("preflight missing profile hash"))?;
        let args_digest = digest_json(&call.args)?;

        let event_ctx = if let Some(publisher) = &self.events {
            let begin_event = ToolInvokeBegin {
                envelope_id: call.call_id.clone(),
                tenant: tenant.clone(),
                subject_id: call.actor.subject_id.clone(),
                tool_id: call.tool_id.clone(),
                call_id: call.call_id.clone(),
                profile_hash: profile_hash.clone(),
                args_digest: args_digest,
            };
            publisher.emit_begin(begin_event.clone()).await?;
            Some(ToolEventContext {
                publisher: publisher.clone(),
                tenant: begin_event.tenant,
                tool_id: begin_event.tool_id,
                call_id: begin_event.call_id,
            })
        } else {
            None
        };

        let idempo_decision = match self.check_idempotency(&call, &spec.manifest).await {
            Ok(decision) => decision,
            Err(err) => {
                if let Some(ctx) = &event_ctx {
                    ctx.emit_end(
                        "error",
                        Some(err.0.code.0.to_string()),
                        digest_json(&Value::Null)?,
                    )
                    .await?;
                }
                return Err(err);
            }
        };

        if let IdempoDecision::Cached(cached) = idempo_decision {
            if let Some(ctx) = &event_ctx {
                let output_value = cached.result.as_ref().cloned().unwrap_or(Value::Null);
                let digest = digest_json(&output_value)?;
                ctx.emit_end("cached", None, digest).await?;
            }
            return Ok(ToolExecutionResponse {
                status: InvokeStatus::Ok,
                output: cached.result.clone(),
                obligations: preflight.obligations,
                budget_snapshot: preflight.budget_snapshot,
                degradation: cached.degradation,
                error_code: None,
                evidence_ref: cached.evidence_ref,
                tool_id,
            });
        }

        let mut pending = match idempo_decision {
            IdempoDecision::Pending(p) => Some(p),
            _ => None,
        };

        let invoke_request = InvokeRequest {
            spec,
            call: call.clone(),
            profile_hash: profile_hash.clone(),
            obligations: preflight.obligations.clone(),
            planned_ops: preflight.planned_ops.clone(),
        };

        let invoke_result = match self.invoker.invoke(invoke_request).await {
            Ok(res) => res,
            Err(err) => {
                if let Some(pending) = pending.take() {
                    let message = tool_error_message(&err);
                    if let Err(store_err) = pending
                        .ctx
                        .store
                        .fail(
                            &tenant,
                            &pending.key,
                            &pending.hash,
                            &message,
                            pending.ctx.failure_ttl_ms,
                        )
                        .await
                    {
                        warn!(error = ?store_err, "idempotency fail update failed");
                    }
                }
                if let Some(ctx) = &event_ctx {
                    ctx.emit_end(
                        "error",
                        Some(err.0.code.0.to_string()),
                        digest_json(&Value::Null)?,
                    )
                    .await?;
                }
                return Err(err);
            }
        };

        let InvokeResult {
            status,
            error_code,
            output,
            evidence_ref,
            degradation,
            evidence,
        } = invoke_result;

        match status {
            InvokeStatus::Ok => {
                let evidence_ref_str = evidence_ref.as_ref().map(|id| id.0.clone());
                if let Some(pending) = pending.take() {
                    let cache = CachedExecution {
                        result: output.clone(),
                        degradation: degradation.clone(),
                        evidence_ref: evidence_ref_str.clone(),
                    };
                    let digest = cached_execution_to_digest(&cache)?;
                    pending
                        .ctx
                        .store
                        .finish(&tenant, &pending.key, &pending.hash, &digest)
                        .await
                        .map_err(tx_error_to_tool)?;
                }

                self.evidence_store
                    .store(&tenant, &call.call_id, &evidence)
                    .await?;

                if let Some(ctx) = &event_ctx {
                    let output_value = output.clone().unwrap_or(Value::Null);
                    let digest = digest_json(&output_value)?;
                    ctx.emit_end("ok", None, digest).await?;
                }

                Ok(ToolExecutionResponse {
                    status: InvokeStatus::Ok,
                    output,
                    obligations: preflight.obligations,
                    budget_snapshot: preflight.budget_snapshot,
                    degradation,
                    error_code: None,
                    evidence_ref: evidence_ref_str,
                    tool_id,
                })
            }
            InvokeStatus::Denied => {
                let code = error_code.unwrap_or_else(|| "tool_invocation_denied".to_string());
                if let Some(pending) = pending.take() {
                    if let Err(store_err) = pending
                        .ctx
                        .store
                        .fail(
                            &tenant,
                            &pending.key,
                            &pending.hash,
                            &code,
                            pending.ctx.failure_ttl_ms,
                        )
                        .await
                    {
                        warn!(error = ?store_err, "idempotency fail update failed");
                    }
                }
                if let Some(ctx) = &event_ctx {
                    ctx.emit_end("denied", Some(code.clone()), digest_json(&Value::Null)?)
                        .await?;
                }
                Err(ToolError::policy(&code))
            }
            InvokeStatus::Error => {
                let code = error_code.unwrap_or_else(|| "tool_invocation_failed".to_string());
                if let Some(pending) = pending.take() {
                    if let Err(store_err) = pending
                        .ctx
                        .store
                        .fail(
                            &tenant,
                            &pending.key,
                            &pending.hash,
                            &code,
                            pending.ctx.failure_ttl_ms,
                        )
                        .await
                    {
                        warn!(error = ?store_err, "idempotency fail update failed");
                    }
                }
                if let Some(ctx) = &event_ctx {
                    ctx.emit_end("error", Some(code.clone()), digest_json(&Value::Null)?)
                        .await?;
                }
                Err(ToolError::unknown(&code))
            }
        }
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

#[derive(Clone, Debug, Serialize)]
struct ToolConfigSnapshot {
    version: String,
    manifests: Vec<ToolManifestEntry>,
    created_at_ms: i64,
}

#[derive(Default)]
struct ToolConfigRegistry {
    snapshots: AsyncMutex<Vec<ToolConfigSnapshot>>,
}

impl ToolConfigRegistry {
    fn new(initial: Vec<ToolManifestEntry>) -> Self {
        Self {
            snapshots: AsyncMutex::new(vec![ToolConfigSnapshot {
                version: "bootstrap".into(),
                manifests: initial,
                created_at_ms: now_ms(),
            }]),
        }
    }

    async fn list(&self) -> Vec<ToolConfigSnapshot> {
        self.snapshots.lock().await.clone()
    }

    async fn publish(
        &self,
        payload: ToolConfigPublishPayload,
        tool_service: Arc<ToolService>,
    ) -> Result<ToolConfigSnapshot, ConfigRegistryError> {
        if payload.version.trim().is_empty() {
            return Err(ConfigRegistryError::Schema("version required".into()));
        }
        if payload.manifests.is_empty() {
            return Err(ConfigRegistryError::Schema(
                "manifests cannot be empty".into(),
            ));
        }
        {
            let history = self.snapshots.lock().await;
            if history.iter().any(|snap| snap.version == payload.version) {
                return Err(ConfigRegistryError::Conflict(
                    "version already exists".into(),
                ));
            }
        }
        tool_service
            .apply_manifests(&payload.manifests)
            .await
            .map_err(|err| ConfigRegistryError::Apply(err.to_string()))?;

        let snapshot = ToolConfigSnapshot {
            version: payload.version,
            manifests: payload.manifests,
            created_at_ms: now_ms(),
        };
        let mut guard = self.snapshots.lock().await;
        guard.push(snapshot.clone());
        Ok(snapshot)
    }
}

#[cfg(test)]
impl ToolService {
    async fn event_records(&self) -> Vec<ToolEventRecord> {
        match &self.events {
            Some(events) => events.snapshot().await,
            None => Vec::new(),
        }
    }

    async fn evidence_records(&self) -> Vec<StoredEvidence> {
        self.evidence_store.snapshot().await
    }
}

#[derive(Default)]
struct CollabService {
    records: AsyncMutex<HashMap<(String, String), CollabRecord>>,
}

impl CollabService {
    async fn execute(
        &self,
        tenant: &TenantId,
        payload: CollabExecutePayload,
    ) -> Result<CollabSnapshot, CollabError> {
        let now = now_ms();
        let mut guard = self.records.lock().await;
        let key = (tenant.0.clone(), payload.collab_id.clone());
        match guard.entry(key) {
            Entry::Vacant(vacant) => {
                let record = CollabRecord::new(tenant, &payload, now);
                let snapshot = record.snapshot();
                vacant.insert(record);
                Ok(snapshot)
            }
            Entry::Occupied(mut entry) => {
                let record = entry.get_mut();
                if record.status == CollabStatus::Resolved {
                    return Err(CollabError::Conflict(
                        "collaboration already resolved".into(),
                    ));
                }
                record.update_from_execute(&payload, now);
                Ok(record.snapshot())
            }
        }
    }

    async fn resolve(
        &self,
        tenant: &TenantId,
        payload: CollabResolvePayload,
    ) -> Result<CollabSnapshot, CollabError> {
        let now = now_ms();
        let mut guard = self.records.lock().await;
        let key = (tenant.0.clone(), payload.collab_id.clone());
        let record = guard
            .get_mut(&key)
            .ok_or_else(|| CollabError::NotFound("collaboration not found".into()))?;
        record.resolve(payload, now)?;
        Ok(record.snapshot())
    }
}

#[derive(Clone)]
struct CollabRecord {
    tenant: TenantId,
    collab_id: String,
    status: CollabStatus,
    participants: Vec<String>,
    context: Value,
    timeline: Vec<CollabTimelineEntry>,
    resolution: Option<Value>,
    created_at: i64,
    updated_at: i64,
}

impl CollabRecord {
    fn new(tenant: &TenantId, payload: &CollabExecutePayload, now: i64) -> Self {
        let mut record = CollabRecord {
            tenant: tenant.clone(),
            collab_id: payload.collab_id.clone(),
            status: CollabStatus::InProgress,
            participants: payload.participants.clone().unwrap_or_default(),
            context: payload.context.clone().unwrap_or(Value::Null),
            timeline: Vec::new(),
            resolution: None,
            created_at: now,
            updated_at: now,
        };
        record.push_event(
            "execute",
            payload.note.clone(),
            payload.context.clone(),
            now,
        );
        record
    }

    fn update_from_execute(&mut self, payload: &CollabExecutePayload, now: i64) {
        if let Some(participants) = &payload.participants {
            for participant in participants {
                if !self.participants.contains(participant) {
                    self.participants.push(participant.clone());
                }
            }
        }
        if let Some(ctx) = &payload.context {
            self.context = merge_json(&self.context, ctx.clone());
        }
        self.push_event(
            "execute",
            payload.note.clone(),
            payload.context.clone(),
            now,
        );
        self.updated_at = now;
    }

    fn resolve(&mut self, payload: CollabResolvePayload, now: i64) -> Result<(), CollabError> {
        if self.status == CollabStatus::Resolved {
            return Err(CollabError::Conflict(
                "collaboration already resolved".into(),
            ));
        }
        self.status = CollabStatus::Resolved;
        if let Some(outcome) = payload.outcome.clone() {
            self.resolution = Some(outcome.clone());
            self.push_event("resolved", payload.note.clone(), Some(outcome), now);
        } else {
            self.push_event("resolved", payload.note.clone(), None, now);
        }
        self.updated_at = now;
        Ok(())
    }

    fn push_event(&mut self, stage: &str, note: Option<String>, payload: Option<Value>, now: i64) {
        self.timeline.push(CollabTimelineEntry {
            stage: stage.to_string(),
            note,
            payload,
            timestamp_ms: now,
        });
    }

    fn snapshot(&self) -> CollabSnapshot {
        CollabSnapshot {
            tenant: self.tenant.clone(),
            collab_id: self.collab_id.clone(),
            status: self.status.as_str().to_string(),
            participants: self.participants.clone(),
            context: self.context.clone(),
            timeline: self.timeline.clone(),
            resolution: self.resolution.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(Clone, Debug)]
struct CollabTimelineEntry {
    stage: String,
    note: Option<String>,
    payload: Option<Value>,
    timestamp_ms: i64,
}

#[derive(Clone, Debug)]
struct CollabSnapshot {
    tenant: TenantId,
    collab_id: String,
    status: String,
    participants: Vec<String>,
    context: Value,
    timeline: Vec<CollabTimelineEntry>,
    resolution: Option<Value>,
    created_at: i64,
    updated_at: i64,
}

impl CollabSnapshot {
    fn into_json(self) -> Value {
        json!({
            "collab_id": self.collab_id,
            "tenant": self.tenant.0,
            "status": self.status,
            "participants": self.participants,
            "context": self.context,
            "timeline": self.timeline.into_iter().map(|entry| {
                json!({
                    "stage": entry.stage,
                    "note": entry.note,
                    "payload": entry.payload.unwrap_or(Value::Null),
                    "timestamp_ms": entry.timestamp_ms,
                })
            }).collect::<Vec<_>>(),
            "resolution": self.resolution.unwrap_or(Value::Null),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        })
    }
}

#[derive(Clone, Copy, PartialEq)]
enum CollabStatus {
    InProgress,
    Resolved,
}

impl CollabStatus {
    fn as_str(&self) -> &'static str {
        match self {
            CollabStatus::InProgress => "in_progress",
            CollabStatus::Resolved => "resolved",
        }
    }
}

#[derive(Deserialize)]
struct CollabExecutePayload {
    collab_id: String,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    participants: Option<Vec<String>>,
    #[serde(default)]
    context: Option<Value>,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Deserialize)]
struct CollabResolvePayload {
    collab_id: String,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    outcome: Option<Value>,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Debug)]
enum CollabError {
    Conflict(String),
    NotFound(String),
}

fn collab_error_to_intercept(err: CollabError) -> InterceptError {
    match err {
        CollabError::Conflict(msg) => InterceptError::from_public(codes::STORAGE_CONFLICT, &msg),
        CollabError::NotFound(msg) => InterceptError::from_public(codes::STORAGE_NOT_FOUND, &msg),
    }
}

#[derive(Clone, Debug, Serialize)]
struct GraphEdgeRecord {
    from: String,
    to: String,
    label: String,
    props: Value,
    timestamp_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GraphEdgeEntity {
    id: String,
    tenant: TenantId,
    from: String,
    to: String,
    label: String,
    props: Value,
    timestamp_ms: i64,
}

impl Entity for GraphEdgeEntity {
    const TABLE: &'static str = "gateway_graph_edges";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

impl From<GraphEdgeEntity> for GraphEdgeRecord {
    fn from(value: GraphEdgeEntity) -> Self {
        Self {
            from: value.from,
            to: value.to,
            label: value.label,
            props: value.props,
            timestamp_ms: value.timestamp_ms,
        }
    }
}

struct GraphService {
    store: Arc<dyn Repository<GraphEdgeEntity>>,
}

impl GraphService {
    fn new(store: Arc<dyn Repository<GraphEdgeEntity>>) -> Self {
        Self { store }
    }

    async fn record_edges(&self, tenant: &TenantId, default_from: &str, edges: &[GraphEdgeInput]) {
        for edge in edges {
            let to = edge.to.trim();
            if to.is_empty() {
                continue;
            }
            let from = edge
                .from
                .clone()
                .unwrap_or_else(|| default_from.to_string());
            let label = edge.label.clone().unwrap_or_else(|| "related".to_string());
            let props = if edge.props.is_null() {
                Value::Object(serde_json::Map::new())
            } else {
                edge.props.clone()
            };
            let timestamp = now_ms();
            let entity = GraphEdgeEntity {
                id: format!("{}::{}::{}", from, to, timestamp),
                tenant: tenant.clone(),
                from: from.clone(),
                to: to.to_string(),
                label,
                props,
                timestamp_ms: timestamp,
            };
            if let Err(err) = self.store.create(tenant, &entity).await {
                warn!(error = ?err, "persist graph edge failed");
            }
        }
    }

    async fn recall(
        &self,
        tenant: &TenantId,
        node_id: String,
        label: Option<String>,
        limit: Option<u32>,
    ) -> GraphRecallResponse {
        let mut filter = serde_json::Map::new();
        filter.insert("from".into(), Value::String(node_id.clone()));
        if let Some(label_str) = label.as_ref() {
            filter.insert("label".into(), Value::String(label_str.clone()));
        }
        let params = QueryParams {
            filter: Value::Object(filter),
            limit,
            ..Default::default()
        };
        let edges = match self.store.select(tenant, params).await {
            Ok(page) => page
                .items
                .into_iter()
                .map(GraphEdgeRecord::from)
                .collect::<Vec<_>>(),
            Err(err) => {
                warn!(error = ?err, "graph recall failed");
                Vec::new()
            }
        };
        let views = edges
            .into_iter()
            .map(GraphEdgeView::from)
            .collect::<Vec<_>>();
        GraphRecallResponse {
            tenant: tenant.clone(),
            node_id,
            label,
            edges: views,
        }
    }

    #[cfg(test)]
    async fn snapshot(&self, tenant: &TenantId, node_id: &str) -> Vec<GraphEdgeRecord> {
        let params = QueryParams {
            filter: json!({ "from": node_id }),
            ..Default::default()
        };
        match self.store.select(tenant, params).await {
            Ok(page) => page.items.into_iter().map(GraphEdgeRecord::from).collect(),
            Err(_) => Vec::new(),
        }
    }
}

struct RepoService {
    graph: Arc<GraphService>,
    store: Arc<dyn Repository<RepoRecordEntity>>,
    seq: AtomicU64,
}

impl RepoService {
    fn new(graph: Arc<GraphService>, store: Arc<dyn Repository<RepoRecordEntity>>) -> Self {
        Self {
            graph,
            store,
            seq: AtomicU64::new(1),
        }
    }

    async fn append(
        &self,
        tenant: &TenantId,
        payload: RepoAppendPayload,
    ) -> Result<RepoAppendResponse, RepoError> {
        let RepoAppendPayload {
            repo,
            tenant: _,
            record_id,
            data,
            edges,
        } = payload;

        if repo.trim().is_empty() {
            return Err(RepoError::Schema("repo missing".into()));
        }
        if data.is_null() {
            return Err(RepoError::Schema("record payload missing".into()));
        }

        let repo_name = repo;
        let record_id = record_id.unwrap_or_else(|| {
            format!(
                "{}::{}",
                repo_name,
                self.seq.fetch_add(1, Ordering::Relaxed)
            )
        });
        let record = RepoRecord {
            id: record_id.clone(),
            data,
            timestamp_ms: now_ms(),
        };

        self.graph.record_edges(tenant, &record_id, &edges).await;

        self.graph.record_edges(tenant, &record_id, &edges).await;

        let entity = RepoRecordEntity {
            id: record_id.clone(),
            tenant: tenant.clone(),
            repo: repo_name.clone(),
            data: record.data.clone(),
            timestamp_ms: record.timestamp_ms,
        };
        self.store
            .create(tenant, &entity)
            .await
            .map_err(storage_error_to_repo)?;

        Ok(RepoAppendResponse {
            tenant: tenant.clone(),
            repo: repo_name,
            record_id,
            stored_at: now_ms(),
        })
    }

    #[cfg(test)]
    async fn records(&self, tenant: &TenantId, repo: &str) -> Vec<RepoRecord> {
        let params = QueryParams {
            filter: json!({ "repo": repo }),
            ..Default::default()
        };
        match self.store.select(tenant, params).await {
            Ok(page) => page.items.into_iter().map(RepoRecord::from).collect(),
            Err(_) => Vec::new(),
        }
    }
}

struct ObserveService {
    store: Arc<dyn Repository<ObserveEventEntity>>,
    seq: AtomicU64,
}

impl ObserveService {
    fn new(store: Arc<dyn Repository<ObserveEventEntity>>) -> Self {
        Self {
            store,
            seq: AtomicU64::new(1),
        }
    }

    async fn emit(
        &self,
        tenant: &TenantId,
        payload: ObserveEmitPayload,
    ) -> Result<ObserveEmitResponse, ObserveError> {
        let ObserveEmitPayload {
            topic,
            tenant: _,
            event,
            severity,
            trace_id,
        } = payload;

        if topic.trim().is_empty() {
            return Err(ObserveError::Schema("topic missing".into()));
        }
        if event.is_null() {
            return Err(ObserveError::Schema("event payload missing".into()));
        }

        let event_id = format!("obs-{}", self.seq.fetch_add(1, Ordering::Relaxed));
        let record = ObserveEvent {
            event_id: event_id.clone(),
            tenant: tenant.clone(),
            topic: topic.clone(),
            event: event.clone(),
            severity: severity.clone(),
            trace_id: trace_id.clone(),
            timestamp_ms: now_ms(),
        };

        let entity = ObserveEventEntity {
            id: event_id.clone(),
            tenant: tenant.clone(),
            topic: record.topic.clone(),
            event: record.event.clone(),
            severity: record.severity.clone(),
            trace_id: record.trace_id.clone(),
            timestamp_ms: record.timestamp_ms,
        };

        self.store
            .create(tenant, &entity)
            .await
            .map_err(storage_error_to_observe)?;

        Ok(ObserveEmitResponse {
            tenant: tenant.clone(),
            topic,
            event_id,
            ingested_at: now_ms(),
        })
    }

    #[cfg(test)]
    async fn snapshot(&self, tenant: &TenantId) -> Vec<ObserveEvent> {
        match self.store.select(tenant, QueryParams::default()).await {
            Ok(page) => page.items.into_iter().map(ObserveEvent::from).collect(),
            Err(_) => Vec::new(),
        }
    }
}

#[derive(Clone)]
struct LlmDefaults {
    chat_model: String,
    stream_model: String,
    embed_model: String,
}

struct LlmService {
    registry: Arc<Registry>,
    defaults: LlmDefaults,
    struct_out_policy: StructOutPolicy,
    history: AsyncMutex<Vec<LlmHistoryEntry>>,
}

impl LlmService {
    async fn from_bootstrap(config: &LlmBootstrap) -> anyhow::Result<Self> {
        let mut registry = Registry::new();
        config.install_providers(&mut registry)?;
        Ok(Self {
            registry: Arc::new(registry),
            defaults: config.defaults(),
            struct_out_policy: config.struct_policy(),
            history: AsyncMutex::new(Vec::new()),
        })
    }

    fn chat_model(&self, requested: &Option<String>) -> String {
        requested
            .as_ref()
            .filter(|m| !m.is_empty())
            .cloned()
            .unwrap_or_else(|| self.defaults.chat_model.clone())
    }

    fn stream_model(&self, requested: &Option<String>) -> String {
        requested
            .as_ref()
            .filter(|m| !m.is_empty())
            .cloned()
            .unwrap_or_else(|| self.defaults.stream_model.clone())
    }

    fn embed_model(&self, requested: &Option<String>) -> String {
        requested
            .as_ref()
            .filter(|m| !m.is_empty())
            .cloned()
            .unwrap_or_else(|| self.defaults.embed_model.clone())
    }

    async fn complete(
        &self,
        tenant: &TenantId,
        payload: LlmCompletePayload,
    ) -> Result<LlmCompleteResponse, LlmError> {
        let LlmCompletePayload {
            prompt,
            tenant: _,
            model,
            temperature,
            top_p,
            max_tokens,
            stop,
            seed,
            frequency_penalty,
            presence_penalty,
            logit_bias,
            response_format,
            idempotency_key,
            cache_hint,
            allow_sensitive,
            metadata,
            tool_specs,
            messages,
            history,
        } = payload;

        let model_id = self.chat_model(&model);
        let model = self.registry.chat(&model_id).ok_or_else(|| {
            LlmError::provider_unavailable(&format!("chat model not found: {model_id}"))
        })?;

        let mut chat_messages: Vec<Message> = messages
            .iter()
            .chain(history.iter())
            .map(message_from_input)
            .collect();
        chat_messages.push(Message {
            role: Role::User,
            segments: vec![ContentSegment::Text {
                text: prompt.clone(),
            }],
            tool_calls: Vec::new(),
        });

        let metadata = metadata.unwrap_or_else(|| Value::Object(serde_json::Map::new()));

        let mut response = model
            .chat(
                ChatRequest {
                    model_id: model_id.clone(),
                    messages: chat_messages,
                    tool_specs,
                    temperature,
                    top_p,
                    max_tokens,
                    stop,
                    seed,
                    frequency_penalty,
                    presence_penalty,
                    logit_bias,
                    response_format,
                    idempotency_key,
                    cache_hint,
                    allow_sensitive,
                    metadata,
                },
                &self.struct_out_policy,
            )
            .await?;
        if response.usage.requests == 0 {
            response.usage.requests = 1;
        }

        let response_text = message_text(&response.message);
        {
            let mut history = self.history.lock().await;
            history.push(LlmHistoryEntry {
                tenant: tenant.clone(),
                prompt: prompt.clone(),
                response: response_text.clone(),
                timestamp_ms: now_ms(),
            });
        }

        Ok(LlmCompleteResponse {
            tenant: tenant.clone(),
            prompt,
            response_text,
            chat: response,
        })
    }

    async fn stream(
        &self,
        tenant: &TenantId,
        payload: LlmStreamPayload,
    ) -> Result<LlmStreamResponse, LlmError> {
        let LlmStreamPayload {
            prompt,
            tenant: _,
            model,
            chunk_size,
            temperature,
            top_p,
            max_tokens,
            stop,
            seed,
            frequency_penalty,
            presence_penalty,
            logit_bias,
            response_format,
            idempotency_key,
            cache_hint,
            allow_sensitive,
            metadata,
            tool_specs,
            messages,
            history,
        } = payload;

        let model_id = self.stream_model(&model);
        let model = self.registry.chat(&model_id).ok_or_else(|| {
            LlmError::provider_unavailable(&format!("chat model not found: {model_id}"))
        })?;

        let mut chat_messages: Vec<Message> = messages
            .iter()
            .chain(history.iter())
            .map(message_from_input)
            .collect();
        chat_messages.push(Message {
            role: Role::User,
            segments: vec![ContentSegment::Text {
                text: prompt.clone(),
            }],
            tool_calls: Vec::new(),
        });

        let metadata = metadata.unwrap_or_else(|| Value::Object(serde_json::Map::new()));

        let mut stream = model
            .chat_stream(
                ChatRequest {
                    model_id: model_id.clone(),
                    messages: chat_messages,
                    tool_specs,
                    temperature,
                    top_p,
                    max_tokens,
                    stop,
                    seed,
                    frequency_penalty,
                    presence_penalty,
                    logit_bias,
                    response_format,
                    idempotency_key,
                    cache_hint,
                    allow_sensitive,
                    metadata,
                },
                &self.struct_out_policy,
            )
            .await?;

        let mut aggregated = String::new();
        let mut raw_chunks: Vec<String> = Vec::new();
        let mut usage = Usage::default();
        let mut finish = None;

        while let Some(delta) = stream.next().await {
            let delta = delta?;
            if let Some(text) = delta.text_delta {
                aggregated.push_str(&text);
                raw_chunks.push(text);
            }
            if let Some(partial) = delta.usage_partial {
                usage = partial;
            }
            if let Some(reason) = delta.finish {
                finish = Some(reason);
            }
        }

        if usage.requests == 0 {
            usage.requests = 1;
        }

        let chunks = match chunk_size.filter(|c| *c > 0) {
            Some(words) => {
                let mut chunked = chunk_text_by_words(&aggregated, words as usize);
                if chunked.is_empty() && !aggregated.is_empty() {
                    chunked.push(LlmStreamChunk {
                        delta: aggregated.clone(),
                    });
                }
                chunked
            }
            None => raw_chunks
                .into_iter()
                .map(|delta| LlmStreamChunk { delta })
                .collect(),
        };

        Ok(LlmStreamResponse {
            tenant: tenant.clone(),
            model_id,
            response_text: aggregated,
            chunks,
            usage,
            finish,
        })
    }

    async fn embed(
        &self,
        tenant: &TenantId,
        payload: LlmEmbedPayload,
    ) -> Result<LlmEmbedResponse, LlmError> {
        let LlmEmbedPayload {
            text,
            tenant: _,
            model,
            normalize,
            dim: _,
        } = payload;

        let model_id = self.embed_model(&model);
        let model = self.registry.embed(&model_id).ok_or_else(|| {
            LlmError::provider_unavailable(&format!("embed model not found: {model_id}"))
        })?;

        let items = text
            .into_iter()
            .enumerate()
            .map(|(idx, content)| EmbedItem {
                id: format!("item-{idx}"),
                text: content,
            })
            .collect();

        let mut response = model
            .embed(EmbedRequest {
                model_id: model_id.clone(),
                items,
                normalize,
                pooling: None,
            })
            .await?;
        if response.usage.requests == 0 {
            response.usage.requests = 1;
        }

        Ok(LlmEmbedResponse {
            tenant: tenant.clone(),
            model_id,
            response,
        })
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(not(test), allow(dead_code))]
struct LlmHistoryEntry {
    tenant: TenantId,
    prompt: String,
    response: String,
    timestamp_ms: i64,
}

#[derive(Deserialize)]
struct GraphRecallPayload {
    #[serde(default)]
    tenant: Option<String>,
    node_id: String,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Serialize)]
struct GraphRecallResponse {
    tenant: TenantId,
    node_id: String,
    label: Option<String>,
    edges: Vec<GraphEdgeView>,
}

impl GraphRecallResponse {
    fn into_json(self) -> Value {
        json!({
            "tenant": self.tenant.0,
            "node_id": self.node_id,
            "label": self.label,
            "edges": self.edges,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
struct GraphEdgeView {
    from: String,
    to: String,
    label: String,
    props: Value,
    timestamp_ms: i64,
}

impl From<GraphEdgeRecord> for GraphEdgeView {
    fn from(record: GraphEdgeRecord) -> Self {
        Self {
            from: record.from,
            to: record.to,
            label: record.label,
            props: record.props,
            timestamp_ms: record.timestamp_ms,
        }
    }
}

#[derive(Clone, Deserialize)]
struct GraphEdgeInput {
    #[serde(default)]
    from: Option<String>,
    to: String,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    props: Value,
}

impl Default for GraphEdgeInput {
    fn default() -> Self {
        Self {
            from: None,
            to: String::new(),
            label: None,
            props: Value::Null,
        }
    }
}

#[derive(Deserialize, Default)]
struct RepoAppendPayload {
    repo: String,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    record_id: Option<String>,
    #[serde(default)]
    data: Value,
    #[serde(default)]
    edges: Vec<GraphEdgeInput>,
}

#[derive(Clone, Debug, Serialize)]
struct RepoRecord {
    id: String,
    data: Value,
    timestamp_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RepoRecordEntity {
    id: String,
    tenant: TenantId,
    repo: String,
    data: Value,
    timestamp_ms: i64,
}

impl Entity for RepoRecordEntity {
    const TABLE: &'static str = "gateway_repo_records";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

impl From<RepoRecordEntity> for RepoRecord {
    fn from(value: RepoRecordEntity) -> Self {
        Self {
            id: value.id,
            data: value.data,
            timestamp_ms: value.timestamp_ms,
        }
    }
}

#[derive(Serialize)]
struct RepoAppendResponse {
    tenant: TenantId,
    repo: String,
    record_id: String,
    stored_at: i64,
}

impl RepoAppendResponse {
    fn into_json(self) -> Value {
        json!({
            "tenant": self.tenant.0,
            "repo": self.repo,
            "record_id": self.record_id,
            "stored_at": self.stored_at,
        })
    }
}

#[derive(Deserialize, Default)]
struct ObserveEmitPayload {
    topic: String,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    event: Value,
    #[serde(default)]
    severity: Option<String>,
    #[serde(default)]
    trace_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ObserveEvent {
    event_id: String,
    tenant: TenantId,
    topic: String,
    event: Value,
    severity: Option<String>,
    trace_id: Option<String>,
    timestamp_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ObserveEventEntity {
    id: String,
    tenant: TenantId,
    topic: String,
    event: Value,
    severity: Option<String>,
    trace_id: Option<String>,
    timestamp_ms: i64,
}

impl Entity for ObserveEventEntity {
    const TABLE: &'static str = "gateway_observe_events";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

impl From<ObserveEventEntity> for ObserveEvent {
    fn from(value: ObserveEventEntity) -> Self {
        Self {
            event_id: value.id,
            tenant: value.tenant,
            topic: value.topic,
            event: value.event,
            severity: value.severity,
            trace_id: value.trace_id,
            timestamp_ms: value.timestamp_ms,
        }
    }
}

#[derive(Serialize)]
struct ObserveEmitResponse {
    tenant: TenantId,
    topic: String,
    event_id: String,
    ingested_at: i64,
}

impl ObserveEmitResponse {
    fn into_json(self) -> Value {
        json!({
            "tenant": self.tenant.0,
            "topic": self.topic,
            "event_id": self.event_id,
            "ingested_at": self.ingested_at,
        })
    }
}

#[derive(Deserialize, Default)]
struct LlmCompletePayload {
    prompt: String,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    model: Option<String>,
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
    logit_bias: serde_json::Map<String, Value>,
    #[serde(default)]
    response_format: Option<ResponseFormat>,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    cache_hint: Option<String>,
    #[serde(default)]
    allow_sensitive: bool,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    tool_specs: Vec<ToolSpec>,
    #[serde(default)]
    messages: Vec<LlmMessageInput>,
    #[serde(default)]
    history: Vec<LlmMessageInput>,
}

#[derive(Deserialize)]
struct LlmMessageInput {
    #[serde(default = "default_user_role")]
    role: String,
    content: String,
}

fn default_user_role() -> String {
    "user".to_string()
}

#[derive(Deserialize, Default)]
struct LlmStreamPayload {
    prompt: String,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    chunk_size: Option<u32>,
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
    logit_bias: serde_json::Map<String, Value>,
    #[serde(default)]
    response_format: Option<ResponseFormat>,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    cache_hint: Option<String>,
    #[serde(default)]
    allow_sensitive: bool,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    tool_specs: Vec<ToolSpec>,
    #[serde(default)]
    messages: Vec<LlmMessageInput>,
    #[serde(default)]
    history: Vec<LlmMessageInput>,
}

#[derive(Deserialize, Default)]
struct LlmEmbedPayload {
    text: Vec<String>,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    normalize: bool,
    #[cfg_attr(not(test), allow(dead_code))]
    #[serde(default)]
    dim: Option<u32>,
}

#[derive(Serialize)]
struct LlmCompleteResponse {
    tenant: TenantId,
    prompt: String,
    response_text: String,
    chat: ChatResponse,
}

impl LlmCompleteResponse {
    fn into_json(self) -> Value {
        json!({
            "tenant": self.tenant.0,
            "prompt": self.prompt,
            "response": self.response_text,
            "chat": {
                "model_id": self.chat.model_id,
                "message": serialize_message(&self.chat.message),
                "usage": self.chat.usage,
                "cost": self.chat.cost,
                "finish": self.chat.finish,
                "provider_meta": self.chat.provider_meta,
            }
        })
    }
}

#[derive(Serialize)]
struct LlmStreamResponse {
    tenant: TenantId,
    model_id: String,
    response_text: String,
    chunks: Vec<LlmStreamChunk>,
    usage: Usage,
    #[serde(skip_serializing_if = "Option::is_none")]
    finish: Option<FinishReason>,
}

impl LlmStreamResponse {
    fn into_json(self) -> Value {
        let mut payload = json!({
            "tenant": self.tenant.0,
            "model_id": self.model_id,
            "response": self.response_text,
            "chunks": self.chunks,
            "usage": self.usage,
        });
        if let Some(finish) = self.finish {
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("finish".to_string(), json!(finish));
            }
        }
        payload
    }
}

#[derive(Serialize)]
struct LlmStreamChunk {
    delta: String,
}

#[derive(Serialize)]
struct LlmEmbedResponse {
    tenant: TenantId,
    model_id: String,
    response: EmbedResponse,
}

impl LlmEmbedResponse {
    fn into_json(self) -> Value {
        json!({
            "tenant": self.tenant.0,
            "model_id": self.model_id,
            "dim": self.response.dim,
            "dtype": self.response.dtype,
            "vectors": self.response.vectors,
            "usage": self.response.usage,
            "cost": self.response.cost,
            "provider_meta": self.response.provider_meta,
        })
    }
}

fn message_from_input(input: &LlmMessageInput) -> Message {
    Message {
        role: match input.role.to_ascii_lowercase().as_str() {
            "system" => Role::System,
            "assistant" => Role::Assistant,
            "tool" => Role::Tool,
            _ => Role::User,
        },
        segments: vec![ContentSegment::Text {
            text: input.content.clone(),
        }],
        tool_calls: Vec::new(),
    }
}

fn message_text(msg: &Message) -> String {
    msg.segments
        .iter()
        .filter_map(|segment| match segment {
            ContentSegment::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn serialize_message(msg: &Message) -> Value {
    json!({
        "role": format!("{:?}", msg.role).to_ascii_lowercase(),
        "text": message_text(msg),
        "tool_calls": msg.tool_calls,
    })
}

fn chunk_text_by_words(text: &str, chunk_size: usize) -> Vec<LlmStreamChunk> {
    if chunk_size == 0 {
        return if text.is_empty() {
            Vec::new()
        } else {
            vec![LlmStreamChunk {
                delta: text.to_string(),
            }]
        };
    }
    let mut chunks = Vec::new();
    let mut buffer = Vec::new();
    for word in text.split_whitespace() {
        buffer.push(word);
        if buffer.len() == chunk_size {
            chunks.push(LlmStreamChunk {
                delta: buffer.join(" "),
            });
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        chunks.push(LlmStreamChunk {
            delta: buffer.join(" "),
        });
    }
    chunks
}

fn llm_error_to_intercept(err: LlmError) -> InterceptError {
    InterceptError::from_error(err.into_inner())
}

#[derive(Debug)]
enum RepoError {
    Schema(String),
    Storage(String),
}

fn repo_error_to_intercept(err: RepoError) -> InterceptError {
    match err {
        RepoError::Schema(msg) => InterceptError::schema(&msg),
        RepoError::Storage(msg) => InterceptError::internal(&msg),
    }
}

#[derive(Debug)]
enum ObserveError {
    Schema(String),
    Storage(String),
}

fn observe_error_to_intercept(err: ObserveError) -> InterceptError {
    match err {
        ObserveError::Schema(msg) => InterceptError::schema(&msg),
        ObserveError::Storage(msg) => InterceptError::internal(&msg),
    }
}

fn storage_error_to_repo(err: StorageError) -> RepoError {
    RepoError::Storage(err.to_string())
}

fn storage_error_to_observe(err: StorageError) -> ObserveError {
    ObserveError::Storage(err.to_string())
}

#[derive(Debug)]
enum ConfigRegistryError {
    Schema(String),
    Conflict(String),
    Apply(String),
}

fn config_error_to_intercept(err: ConfigRegistryError) -> InterceptError {
    match err {
        ConfigRegistryError::Schema(msg) => InterceptError::schema(&msg),
        ConfigRegistryError::Conflict(msg) => {
            InterceptError::from_public(codes::STORAGE_CONFLICT, &msg)
        }
        ConfigRegistryError::Apply(msg) => InterceptError::internal(&msg),
    }
}

#[derive(Deserialize)]
struct ToolExecutePayload {
    #[serde(default)]
    tenant: Option<String>,
    tool_id: String,
    #[serde(default)]
    input: serde_json::Value,
    #[serde(default)]
    actor: Option<ToolActorPayload>,
    #[serde(default)]
    origin: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    consent: Option<Consent>,
    #[serde(default)]
    call_id: Option<String>,
}

#[derive(Deserialize, Default)]
struct ToolConfigPublishPayload {
    version: String,
    #[serde(default)]
    manifests: Vec<ToolManifestEntry>,
}

#[derive(Deserialize)]
struct ToolActorPayload {
    #[serde(default = "default_subject_kind")]
    kind: SubjectKind,
    subject_id: String,
    #[serde(default = "default_claims")]
    claims: serde_json::Map<String, serde_json::Value>,
}

impl ToolActorPayload {
    fn into_subject(self, tenant: TenantId) -> Subject {
        Subject {
            kind: self.kind,
            subject_id: Id(self.subject_id),
            tenant,
            claims: self.claims,
        }
    }
}

fn default_subject_kind() -> SubjectKind {
    SubjectKind::Service
}

fn default_claims() -> serde_json::Map<String, serde_json::Value> {
    serde_json::Map::new()
}

fn default_actor(tenant: TenantId) -> Subject {
    Subject {
        kind: SubjectKind::Service,
        subject_id: Id("svc.gateway".to_string()),
        tenant,
        claims: serde_json::Map::new(),
    }
}

fn resolve_origin(origin: Option<String>) -> ToolOrigin {
    match origin {
        Some(origin) if origin.eq_ignore_ascii_case("llm") => ToolOrigin::Llm,
        Some(origin) if origin.eq_ignore_ascii_case("system") => ToolOrigin::System,
        _ => ToolOrigin::Api,
    }
}

struct ToolInvocationRequest {
    tenant: TenantId,
    tool_id: String,
    input: serde_json::Value,
    actor: Subject,
    origin: ToolOrigin,
    consent: Option<Consent>,
    idempotency_key: Option<String>,
    call_id: Option<Id>,
}

impl ToolInvocationRequest {
    fn into_call(self) -> ToolCall {
        let call_id = self.call_id.unwrap_or_else(Id::new_random);
        ToolCall {
            tool_id: ToolId(self.tool_id),
            call_id,
            tenant: self.tenant,
            actor: self.actor,
            origin: self.origin,
            args: self.input,
            consent: self.consent,
            idempotency_key: self.idempotency_key,
        }
    }
}

#[derive(Debug)]
struct ToolExecutionResponse {
    status: InvokeStatus,
    output: Option<serde_json::Value>,
    obligations: Vec<Obligation>,
    budget_snapshot: serde_json::Value,
    degradation: Option<Vec<String>>,
    error_code: Option<String>,
    evidence_ref: Option<String>,
    tool_id: String,
}

impl ToolExecutionResponse {
    fn status_str(&self) -> &'static str {
        match self.status {
            InvokeStatus::Ok => "ok",
            InvokeStatus::Denied => "denied",
            InvokeStatus::Error => "error",
        }
    }
}

#[derive(Debug)]
struct ToolPreflightResponse {
    allow: bool,
    reason: Option<String>,
    profile_hash: Option<String>,
    obligations: Vec<Obligation>,
    budget_snapshot: serde_json::Value,
    planned_ops: Vec<ExecOp>,
}

#[derive(Clone)]
struct IdempotencyContext {
    store: Arc<dyn IdempoStore>,
    ttl_ms: i64,
    failure_ttl_ms: i64,
}

enum IdempoDecision {
    Skip,
    Cached(CachedExecution),
    Pending(IdempoPending),
}

#[derive(Clone)]
struct IdempoPending {
    ctx: Arc<IdempotencyContext>,
    key: String,
    hash: String,
}

#[derive(Serialize, Deserialize)]
struct CachedExecution {
    result: Option<serde_json::Value>,
    #[serde(default)]
    degradation: Option<Vec<String>>,
    #[serde(default)]
    evidence_ref: Option<String>,
}

const EVENT_TOPIC_BEGIN: &str = "tool.invoke.begin";
const EVENT_TOPIC_END: &str = "tool.invoke.end";

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug)]
enum ToolEventRecord {
    Begin(ToolInvokeBegin),
    End(ToolInvokeEndRecord),
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug)]
struct ToolInvokeEndRecord {
    tenant: TenantId,
    tool_id: ToolId,
    call_id: Id,
    event: ToolInvokeEnd,
}

struct ToolEventPublisher {
    store: Arc<dyn OutboxStore>,
    records: AsyncMutex<Vec<ToolEventRecord>>,
}

impl ToolEventPublisher {
    fn new(store: Arc<dyn OutboxStore>) -> Self {
        Self {
            store,
            records: AsyncMutex::new(Vec::new()),
        }
    }

    async fn emit_begin(&self, event: ToolInvokeBegin) -> Result<(), ToolError> {
        self.enqueue(&event.tenant, &event.call_id, EVENT_TOPIC_BEGIN, &event)
            .await?;
        let mut guard = self.records.lock().await;
        guard.push(ToolEventRecord::Begin(event));
        Ok(())
    }

    async fn emit_end(
        &self,
        tenant: &TenantId,
        tool_id: &ToolId,
        call_id: &Id,
        event: ToolInvokeEnd,
    ) -> Result<(), ToolError> {
        self.enqueue(tenant, call_id, EVENT_TOPIC_END, &event)
            .await?;
        let mut guard = self.records.lock().await;
        guard.push(ToolEventRecord::End(ToolInvokeEndRecord {
            tenant: tenant.clone(),
            tool_id: tool_id.clone(),
            call_id: call_id.clone(),
            event,
        }));
        Ok(())
    }

    async fn enqueue<T: Serialize + ?Sized>(
        &self,
        tenant: &TenantId,
        call_id: &Id,
        channel: &str,
        payload: &T,
    ) -> Result<(), ToolError> {
        let payload_value = serde_json::to_value(payload)
            .map_err(|err| ToolError::unknown(&format!("serialize event payload: {err}")))?;
        let msg = OutboxMessage::new(
            tenant.clone(),
            MsgId(format!("{channel}:{}:{}", tenant.0, call_id.0)),
            channel.to_string(),
            payload_value,
            now_ms(),
        );
        self.store.enqueue(msg).await.map_err(tx_error_to_tool)
    }

    #[cfg(test)]
    async fn snapshot(&self) -> Vec<ToolEventRecord> {
        self.records.lock().await.clone()
    }
}

struct ToolEventContext {
    publisher: Arc<ToolEventPublisher>,
    tenant: TenantId,
    tool_id: ToolId,
    call_id: Id,
}

impl ToolEventContext {
    async fn emit_end(
        &self,
        status: &str,
        error_code: Option<String>,
        output_digest: String,
    ) -> Result<(), ToolError> {
        let event = ToolInvokeEnd {
            envelope_id: self.call_id.clone(),
            status: status.to_string(),
            error_code,
            budget_used_bytes_in: 0,
            budget_used_bytes_out: 0,
            output_digest,
        };
        self.publisher
            .emit_end(&self.tenant, &self.tool_id, &self.call_id, event)
            .await
    }
}

#[async_trait]
trait ToolEvidenceStore: Send + Sync {
    async fn store(
        &self,
        tenant: &TenantId,
        call_id: &Id,
        evidence: &InvokeEvidence,
    ) -> Result<(), ToolError>;

    #[cfg(test)]
    async fn snapshot(&self) -> Vec<StoredEvidence>;
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug)]
struct StoredEvidence {
    tenant: TenantId,
    call_id: Id,
    begins: Vec<EvidenceRecord>,
    ends: Vec<EvidenceRecord>,
}

#[derive(Default)]
struct InMemoryEvidenceStore {
    records: AsyncMutex<Vec<StoredEvidence>>,
}

#[async_trait]
impl ToolEvidenceStore for InMemoryEvidenceStore {
    async fn store(
        &self,
        tenant: &TenantId,
        call_id: &Id,
        evidence: &InvokeEvidence,
    ) -> Result<(), ToolError> {
        let stored = StoredEvidence {
            tenant: tenant.clone(),
            call_id: call_id.clone(),
            begins: evidence.begins.clone(),
            ends: evidence.ends.clone(),
        };
        let mut guard = self.records.lock().await;
        guard.push(stored);
        Ok(())
    }

    #[cfg(test)]
    async fn snapshot(&self) -> Vec<StoredEvidence> {
        self.records.lock().await.clone()
    }
}
fn tool_error_to_intercept(err: ToolError) -> InterceptError {
    InterceptError::from_error(err.into_inner())
}

fn compute_idempotency_hash(
    tenant: &TenantId,
    tool_id: &str,
    version: &str,
    args: &serde_json::Value,
) -> Result<String, ToolError> {
    let mut hasher = Sha256::new();
    hasher.update(tenant.0.as_bytes());
    hasher.update(tool_id.as_bytes());
    hasher.update(version.as_bytes());
    let payload = serde_json::to_vec(args)
        .map_err(|err| ToolError::unknown(&format!("encode idempotency payload: {err}")))?;
    hasher.update(payload);
    Ok(format!("{:x}", hasher.finalize()))
}

fn cached_execution_to_digest(cache: &CachedExecution) -> Result<String, ToolError> {
    serde_json::to_string(cache)
        .map_err(|err| ToolError::unknown(&format!("serialize idempotency cache: {err}")))
}

fn cached_execution_from_digest(digest: &str) -> Result<CachedExecution, ToolError> {
    serde_json::from_str(digest)
        .map_err(|err| ToolError::unknown(&format!("decode idempotency cache: {err}")))
}

fn tx_error_to_tool(err: TxError) -> ToolError {
    ToolError(Box::new(err.into_inner()))
}

fn tool_error_message(err: &ToolError) -> String {
    let obj = err.0.as_ref();
    obj.message_dev
        .clone()
        .unwrap_or_else(|| obj.message_user.clone())
}

fn merge_json(base: &Value, update: Value) -> Value {
    match (base, update) {
        (Value::Object(base_map), Value::Object(update_map)) => {
            let mut merged = base_map.clone();
            for (key, value) in update_map.into_iter() {
                let new_value = if let Some(existing) = merged.get(&key) {
                    merge_json(existing, value)
                } else {
                    value
                };
                merged.insert(key, new_value);
            }
            Value::Object(merged)
        }
        (_, other) => other,
    }
}

fn digest_json(value: &Value) -> Result<String, ToolError> {
    let mut hasher = Sha256::new();
    let payload = serde_json::to_vec(value)
        .map_err(|err| ToolError::unknown(&format!("serialize digest payload: {err}")))?;
    hasher.update(payload);
    Ok(format!("{:x}", hasher.finalize()))
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
        CapabilityDecl, ConcurrencyKind, ConsentPolicy, IdempoKind, Limits, SafetyClass, SideEffect,
    };
    use std::collections::HashMap;

    struct DummyRequest {
        method: String,
        path: String,
        headers: HashMap<String, String>,
        body: Value,
    }

    impl DummyRequest {
        fn new(method: &str, path: &str) -> Self {
            Self {
                method: method.into(),
                path: path.into(),
                headers: HashMap::new(),
                body: Value::Null,
            }
        }

        fn with_header(mut self, name: &str, value: &str) -> Self {
            self.headers
                .insert(name.to_ascii_lowercase(), value.to_string());
            self
        }
    }

    struct DummyResponse;

    #[async_trait]
    impl ProtoRequest for DummyRequest {
        fn method(&self) -> &str {
            &self.method
        }

        fn path(&self) -> &str {
            &self.path
        }

        fn header(&self, name: &str) -> Option<String> {
            self.headers.get(&name.to_ascii_lowercase()).cloned()
        }

        async fn read_json(&mut self) -> Result<Value, InterceptError> {
            Ok(self.body.clone())
        }
    }

    #[async_trait]
    impl ProtoResponse for DummyResponse {
        fn set_status(&mut self, _code: u16) {}

        fn insert_header(&mut self, _name: &str, _value: &str) {}

        async fn write_json(&mut self, _body: &Value) -> Result<(), InterceptError> {
            Ok(())
        }
    }

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
            capabilities: vec![CapabilityDecl {
                domain: "tmp".into(),
                action: "use".into(),
                resource: "/tmp".into(),
                attrs: serde_json::json!({}),
            }],
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

    #[cfg(feature = "idempo_surreal")]
    struct SurrealEvidenceStore {
        datastore: Arc<SurrealDatastore>,
    }

    #[cfg(feature = "idempo_surreal")]
    impl SurrealEvidenceStore {
        fn new(datastore: Arc<SurrealDatastore>) -> Self {
            Self { datastore }
        }

        async fn session(
            &self,
        ) -> Result<soulbase_storage::surreal::session::SurrealSession, ToolError> {
            self.datastore
                .session()
                .await
                .map_err(|err| ToolError::from(err))
        }
    }

    #[cfg(feature = "idempo_surreal")]
    #[async_trait]
    impl ToolEvidenceStore for SurrealEvidenceStore {
        async fn store(
            &self,
            tenant: &TenantId,
            call_id: &Id,
            evidence: &InvokeEvidence,
        ) -> Result<(), ToolError> {
            use serde_json::json;

            let session = self.session().await?;
            let payload = json!({
                "tenant": tenant.0,
                "call_id": call_id.0,
                "begins": evidence.begins,
                "ends": evidence.ends,
                "updated_at": now_ms(),
            });

            session
                .query(
                    "UPSERT tool_evidence CONTENT $data RETURN NONE",
                    json!({ "data": payload }),
                )
                .await
                .map_err(|err| ToolError::unknown(&format!("store evidence: {err}")))?;
            Ok(())
        }

        #[cfg(test)]
        async fn snapshot(&self) -> Vec<StoredEvidence> {
            Vec::new()
        }
    }

    fn keyed_manifest() -> ToolManifest {
        ToolManifest {
            idempotency: IdempoKind::Keyed,
            ..demo_manifest()
        }
    }

    #[tokio::test]
    async fn llm_complete_uses_registry_models() -> anyhow::Result<()> {
        let service = LlmService::from_bootstrap(&LlmBootstrap::default()).await?;
        let tenant = TenantId("tenant-llm".into());
        let response = service
            .complete(
                &tenant,
                LlmCompletePayload {
                    prompt: "hello thin waist".into(),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(response.chat.model_id, "local:echo");
        assert!(
            response.response_text.contains("hello thin waist"),
            "expected echo response, got {}",
            response.response_text
        );
        Ok(())
    }

    #[tokio::test]
    async fn llm_stream_builds_chunks() -> anyhow::Result<()> {
        let service = LlmService::from_bootstrap(&LlmBootstrap::default()).await?;
        let tenant = TenantId("tenant-llm".into());
        let response = service
            .stream(
                &tenant,
                LlmStreamPayload {
                    prompt: "streaming hello world".into(),
                    chunk_size: Some(2),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(response.model_id, "local:echo");
        assert!(!response.chunks.is_empty());
        assert!(
            response.response_text.contains("streaming hello world"),
            "expected aggregated text, got {}",
            response.response_text
        );
        assert!(response.usage.requests >= 1);
        Ok(())
    }

    #[tokio::test]
    async fn llm_embed_returns_vectors() -> anyhow::Result<()> {
        let service = LlmService::from_bootstrap(&LlmBootstrap::default()).await?;
        let tenant = TenantId("tenant-llm".into());
        let response = service
            .embed(
                &tenant,
                LlmEmbedPayload {
                    text: vec!["alpha".into(), "beta".into()],
                    normalize: true,
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(response.model_id, "local:emb");
        assert_eq!(response.response.vectors.len(), 2);
        assert!(response.response.dim > 0);
        Ok(())
    }

    #[tokio::test]
    async fn tool_service_preflight_allows_execution() -> anyhow::Result<()> {
        let bootstrap = ToolBootstrap {
            registry: ToolRegistryConfig::InMemory,
            idempotency: IdempotencyBootstrap::default(),
            events: EventsBootstrap::default(),
            evidence: EvidenceBootstrap::default(),
            manifests: vec![ToolManifestEntry {
                tenant: "tenant-test".into(),
                manifest: demo_manifest(),
            }],
        };

        let service = ToolService::from_bootstrap(&bootstrap).await?;
        let tenant = TenantId("tenant-test".into());
        let actor = Subject {
            kind: SubjectKind::Service,
            subject_id: Id("svc.test".into()),
            tenant: tenant.clone(),
            claims: serde_json::Map::new(),
        };
        let request = ToolInvocationRequest {
            tenant: tenant.clone(),
            tool_id: "demo.echo".into(),
            input: serde_json::json!({ "foo": "bar" }),
            actor,
            origin: ToolOrigin::Api,
            consent: None,
            idempotency_key: None,
            call_id: None,
        };

        let preflight = service.preflight(request).await?;
        assert!(preflight.allow);
        assert!(preflight.profile_hash.is_some());
        assert!(preflight.planned_ops.len() <= 1);
        assert!(service.event_records().await.is_empty());
        assert!(service.evidence_records().await.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn auth_stage_accepts_bearer_token_with_scope() -> anyhow::Result<()> {
        let mut routes = HashMap::new();
        routes.insert(
            "/secure".into(),
            ServiceConfig {
                name: "secure".into(),
                route: "/secure".into(),
                enabled: true,
                description: None,
                auth: Some(AuthConfig {
                    scopes: vec!["graph:read".into()],
                    allow_anonymous: false,
                    ..Default::default()
                }),
                schema: None,
                kind: ServiceKind::Other,
                policy: None,
            },
        );
        let auth = AuthBootstrap {
            tokens: vec![ApiTokenConfig {
                name: "cli".into(),
                token: Some("token-1".into()),
                token_env: None,
                token_file: None,
                tenant: "tenant-test".into(),
                subject: Some(AuthSubjectConfig {
                    kind: SubjectKind::Service,
                    subject_id: "svc.cli".into(),
                    claims: serde_json::Map::new(),
                }),
                scopes: vec!["graph:read".into()],
            }],
            ..Default::default()
        };
        let stage = GatewayAuthStage::new(Arc::new(routes), None, &auth).expect("auth stage");
        let mut cx = InterceptContext::default();
        let mut req =
            DummyRequest::new("POST", "/secure").with_header("authorization", "Bearer token-1");
        let mut rsp = DummyResponse;
        stage
            .handle(&mut cx, &mut req, &mut rsp)
            .await
            .expect("auth ok");
        assert_eq!(
            cx.subject
                .as_ref()
                .map(|s| s.subject_id.0.clone())
                .as_deref(),
            Some("svc.cli")
        );
        Ok(())
    }

    #[tokio::test]
    async fn auth_stage_rejects_missing_scope() {
        let mut routes = HashMap::new();
        routes.insert(
            "/repo".into(),
            ServiceConfig {
                name: "repo".into(),
                route: "/repo".into(),
                enabled: true,
                description: None,
                auth: Some(AuthConfig {
                    scopes: vec!["repo:write".into()],
                    allow_anonymous: false,
                    ..Default::default()
                }),
                schema: None,
                kind: ServiceKind::Other,
                policy: None,
            },
        );
        let auth = AuthBootstrap {
            tokens: vec![ApiTokenConfig {
                name: "cli".into(),
                token: Some("token-2".into()),
                token_env: None,
                token_file: None,
                tenant: "tenant-test".into(),
                subject: None,
                scopes: vec!["graph:read".into()],
            }],
            ..Default::default()
        };
        let stage = GatewayAuthStage::new(Arc::new(routes), None, &auth).expect("auth stage");
        let mut cx = InterceptContext::default();
        let mut req =
            DummyRequest::new("POST", "/repo").with_header("authorization", "Bearer token-2");
        let mut rsp = DummyResponse;
        let err = stage
            .handle(&mut cx, &mut req, &mut rsp)
            .await
            .expect_err("missing scope should fail");
        match err {
            InterceptError(obj) => assert_eq!(obj.code, codes::AUTH_FORBIDDEN),
        }
    }

    #[tokio::test]
    async fn auth_stage_accepts_hmac_signature() -> anyhow::Result<()> {
        let mut routes = HashMap::new();
        routes.insert(
            "/repo".into(),
            ServiceConfig {
                name: "repo".into(),
                route: "/repo".into(),
                enabled: true,
                description: None,
                auth: Some(AuthConfig {
                    scopes: vec!["repo:write".into()],
                    allow_anonymous: false,
                    ..Default::default()
                }),
                schema: None,
                kind: ServiceKind::Other,
                policy: None,
            },
        );
        let auth = AuthBootstrap {
            hmac_keys: vec![HmacKeyConfig {
                key_id: "local-hmac".into(),
                secret: Some("super-secret".into()),
                secret_env: None,
                secret_file: None,
                tenant: "tenant-test".into(),
                subject: None,
                scopes: vec!["repo:write".into()],
            }],
            ..Default::default()
        };
        let stage = GatewayAuthStage::new(Arc::new(routes), None, &auth).expect("auth stage");
        let date = "2025-01-01T00:00:00Z";
        let canonical = format!("POST\n/repo\n{date}");
        let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
        mac.update(canonical.as_bytes());
        let sig = hex_encode(mac.finalize().into_bytes());
        let authz = format!("SB-HMAC local-hmac:{sig}");

        let mut cx = InterceptContext::default();
        let mut req = DummyRequest::new("POST", "/repo")
            .with_header("authorization", &authz)
            .with_header("x-sb-date", date);
        let mut rsp = DummyResponse;
        stage
            .handle(&mut cx, &mut req, &mut rsp)
            .await
            .expect("hmac ok");
        assert!(cx.subject.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn auth_stage_reads_token_from_env() -> anyhow::Result<()> {
        let mut routes = HashMap::new();
        routes.insert(
            "/secure".into(),
            ServiceConfig {
                name: "secure".into(),
                route: "/secure".into(),
                enabled: true,
                description: None,
                auth: Some(AuthConfig {
                    scopes: vec!["graph:read".into()],
                    allow_anonymous: false,
                    ..Default::default()
                }),
                schema: None,
                kind: ServiceKind::Other,
                policy: None,
            },
        );
        let env_key = "GATEWAY_TEST_TOKEN";
        env::set_var(env_key, "env-token");
        let auth = AuthBootstrap {
            tokens: vec![ApiTokenConfig {
                name: "cli".into(),
                token_env: Some(env_key.into()),
                token: None,
                token_file: None,
                tenant: "tenant-test".into(),
                subject: None,
                scopes: vec!["graph:read".into()],
            }],
            ..Default::default()
        };
        let stage = GatewayAuthStage::new(Arc::new(routes), None, &auth).expect("auth stage");
        let mut cx = InterceptContext::default();
        let mut req =
            DummyRequest::new("POST", "/secure").with_header("authorization", "Bearer env-token");
        let mut rsp = DummyResponse;
        stage
            .handle(&mut cx, &mut req, &mut rsp)
            .await
            .expect("env token auth ok");
        env::remove_var(env_key);
        Ok(())
    }

    #[tokio::test]
    async fn auth_stage_errors_on_missing_secret() {
        let routes = Arc::new(HashMap::new());
        let auth = AuthBootstrap {
            tokens: vec![ApiTokenConfig {
                name: "cli".into(),
                token_env: Some("MISSING_TOKEN".into()),
                token: None,
                token_file: None,
                tenant: "tenant-test".into(),
                subject: None,
                scopes: vec![],
            }],
            ..Default::default()
        };
        match GatewayAuthStage::new(routes, None, &auth) {
            Ok(_) => panic!("expected missing secret to fail"),
            Err(err) => assert!(
                err.to_string()
                    .contains("environment variable MISSING_TOKEN"),
                "unexpected error: {err}"
            ),
        }
    }

    fn sample_service_config(route: &str, policy: Option<ServicePolicyConfig>) -> ServiceConfig {
        ServiceConfig {
            name: route.trim_start_matches('/').to_string(),
            route: route.to_string(),
            enabled: true,
            description: None,
            auth: Some(AuthConfig {
                allow_anonymous: false,
                ..Default::default()
            }),
            schema: None,
            kind: ServiceKind::RepoAppend,
            policy,
        }
    }

    fn sample_subject() -> Subject {
        Subject {
            kind: SubjectKind::Service,
            subject_id: Id("svc.test".into()),
            tenant: TenantId("tenant-policy".into()),
            claims: serde_json::Map::new(),
        }
    }

    #[tokio::test]
    async fn policy_stage_allows_matching_rule() -> anyhow::Result<()> {
        let mut routes = HashMap::new();
        routes.insert(
            "/repo".into(),
            sample_service_config(
                "/repo",
                Some(ServicePolicyConfig {
                    resource: Some("repo:timeline".into()),
                    action: Some(Action::Write),
                    attrs: None,
                }),
            ),
        );
        let rules = vec![PolicyRule {
            id: "allow-repo".into(),
            description: None,
            resources: vec!["repo:*".into()],
            actions: vec![Action::Write],
            effect: PolicyEffect::Allow,
            conditions: vec![],
            obligations: vec![],
        }];
        let stage = GatewayPolicyStage::new(
            Arc::new(routes),
            Some(Arc::new(PolicyAuthorizer::new(rules))),
        );
        let mut cx = InterceptContext::default();
        cx.subject = Some(sample_subject());
        let mut req = DummyRequest::new("POST", "/repo");
        let mut rsp = DummyResponse;
        stage
            .handle(&mut cx, &mut req, &mut rsp)
            .await
            .expect("policy allow");
        Ok(())
    }

    #[tokio::test]
    async fn policy_stage_denies_without_match() {
        let mut routes = HashMap::new();
        routes.insert(
            "/secure".into(),
            sample_service_config(
                "/secure",
                Some(ServicePolicyConfig {
                    resource: Some("secure:admin".into()),
                    action: Some(Action::Write),
                    attrs: None,
                }),
            ),
        );
        let rules = vec![PolicyRule {
            id: "deny-admin".into(),
            description: None,
            resources: vec!["secure:*".into()],
            actions: vec![Action::Write],
            effect: PolicyEffect::Deny,
            conditions: vec![],
            obligations: vec![],
        }];
        let stage = GatewayPolicyStage::new(
            Arc::new(routes),
            Some(Arc::new(PolicyAuthorizer::new(rules))),
        );
        let mut cx = InterceptContext::default();
        cx.subject = Some(sample_subject());
        let mut req = DummyRequest::new("POST", "/secure");
        let mut rsp = DummyResponse;
        let err = stage
            .handle(&mut cx, &mut req, &mut rsp)
            .await
            .expect_err("policy deny");
        match err {
            InterceptError(obj) => assert_eq!(obj.code, codes::POLICY_DENY_TOOL),
        }
    }

    #[tokio::test]
    async fn repo_append_records_and_edges() {
        let datastore = MockDatastore::new();
        let graph_store: Arc<dyn Repository<GraphEdgeEntity>> =
            Arc::new(InMemoryRepository::<GraphEdgeEntity>::new(&datastore));
        let graph = Arc::new(GraphService::new(graph_store.clone()));
        let repo_store: Arc<dyn Repository<RepoRecordEntity>> =
            Arc::new(InMemoryRepository::<RepoRecordEntity>::new(&datastore));
        let repo = RepoService::new(graph.clone(), repo_store);
        let tenant = TenantId("tenant-repo".into());

        let payload = RepoAppendPayload {
            repo: "timeline".into(),
            data: serde_json::json!({ "summary": "ok" }),
            edges: vec![GraphEdgeInput {
                from: None,
                to: "doc:beta".into(),
                label: Some("follows".into()),
                props: serde_json::json!({ "weight": 0.9 }),
            }],
            ..Default::default()
        };

        let response = repo.append(&tenant, payload).await.expect("append");
        assert_eq!(response.repo, "timeline");

        let records = repo.records(&tenant, "timeline").await;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, response.record_id);

        let edges = graph.snapshot(&tenant, &response.record_id).await;
        assert!(
            edges
                .iter()
                .any(|edge| edge.to == "doc:beta" && edge.label == "follows"),
            "expected to find doc:beta edge, got {:?}",
            edges
        );
    }

    #[tokio::test]
    async fn graph_recall_filters_by_label() {
        let datastore = MockDatastore::new();
        let store: Arc<dyn Repository<GraphEdgeEntity>> =
            Arc::new(InMemoryRepository::<GraphEdgeEntity>::new(&datastore));
        let service = GraphService::new(store);
        let tenant = TenantId("tenant-graph".into());

        service
            .record_edges(
                &tenant,
                "node:alpha",
                &[
                    GraphEdgeInput {
                        from: None,
                        to: "node:beta".into(),
                        label: Some("relates".into()),
                        props: serde_json::json!({}),
                    },
                    GraphEdgeInput {
                        from: None,
                        to: "node:gamma".into(),
                        label: Some("prefers".into()),
                        props: serde_json::json!({}),
                    },
                ],
            )
            .await;

        let recall = service
            .recall(
                &tenant,
                "node:alpha".into(),
                Some("prefers".into()),
                Some(5),
            )
            .await;
        assert_eq!(recall.edges.len(), 1);
        assert_eq!(recall.edges[0].to, "node:gamma");
        assert_eq!(recall.edges[0].label, "prefers");
    }

    #[tokio::test]
    async fn observe_emit_tracks_events() {
        let datastore = MockDatastore::new();
        let store: Arc<dyn Repository<ObserveEventEntity>> =
            Arc::new(InMemoryRepository::<ObserveEventEntity>::new(&datastore));
        let service = ObserveService::new(store.clone());
        let tenant = TenantId("tenant-observe".into());

        let payload = ObserveEmitPayload {
            topic: "demo.topic".into(),
            event: serde_json::json!({ "payload": "ok" }),
            ..Default::default()
        };
        let response = service.emit(&tenant, payload).await.expect("emit");
        assert!(response.event_id.starts_with("obs-"));

        let snapshot = service.snapshot(&tenant).await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].topic, "demo.topic");
    }

    #[tokio::test]
    async fn tool_service_bootstrap_registers_manifests() -> anyhow::Result<()> {
        let bootstrap = ToolBootstrap {
            registry: ToolRegistryConfig::InMemory,
            idempotency: IdempotencyBootstrap::default(),
            events: EventsBootstrap::default(),
            evidence: EvidenceBootstrap::default(),
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
        let actor = Subject {
            kind: SubjectKind::Service,
            subject_id: Id("svc.test".into()),
            tenant: tenant.clone(),
            claims: serde_json::Map::new(),
        };
        let request = ToolInvocationRequest {
            tenant: tenant.clone(),
            tool_id: "demo.echo".into(),
            input: payload.clone(),
            actor,
            origin: ToolOrigin::Api,
            consent: None,
            idempotency_key: None,
            call_id: None,
        };
        let executed = service.execute(request).await?;
        assert_eq!(executed.status, InvokeStatus::Ok);
        let result = executed.output.expect("tool output");
        assert!(result.get("simulated").is_some());

        let events = service.event_records().await;
        assert_eq!(events.len(), 2);
        match &events[0] {
            ToolEventRecord::Begin(begin) => {
                assert_eq!(begin.tenant, tenant);
                assert_eq!(begin.tool_id.0, "demo.echo");
                assert!(!begin.call_id.0.is_empty());
            }
            _ => panic!("expected begin event"),
        }
        match &events[1] {
            ToolEventRecord::End(rec) => {
                assert_eq!(rec.event.status, "ok");
                assert_eq!(rec.tenant, tenant);
                assert_eq!(rec.tool_id.0, "demo.echo");
                assert!(!rec.call_id.0.is_empty());
            }
            _ => panic!("expected end event"),
        }

        let evidence = service.evidence_records().await;
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].tenant, tenant);
        assert!(!evidence[0].begins.is_empty());
        assert!(!evidence[0].ends.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn tool_config_registry_publish_updates_tool_service() -> anyhow::Result<()> {
        let bootstrap = ToolBootstrap {
            manifests: vec![ToolManifestEntry {
                tenant: "tenant-test".into(),
                manifest: demo_manifest(),
            }],
            ..Default::default()
        };
        let service = Arc::new(ToolService::from_bootstrap(&bootstrap).await?);
        let registry = ToolConfigRegistry::new(bootstrap.manifests.clone());

        let payload = ToolConfigPublishPayload {
            version: "v2".into(),
            manifests: vec![ToolManifestEntry {
                tenant: "tenant-test".into(),
                manifest: ToolManifest {
                    version: "2.0.0".into(),
                    ..demo_manifest()
                },
            }],
        };

        let snapshot = registry
            .publish(payload, service.clone())
            .await
            .expect("publish");
        assert_eq!(snapshot.version, "v2");
        let listed = service
            .list(&TenantId("tenant-test".into()))
            .await
            .expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0]["version"], "2.0.0");
        Ok(())
    }

    #[tokio::test]
    async fn tool_config_registry_rejects_duplicate_version() -> anyhow::Result<()> {
        let bootstrap = ToolBootstrap {
            manifests: vec![ToolManifestEntry {
                tenant: "tenant-test".into(),
                manifest: demo_manifest(),
            }],
            ..Default::default()
        };
        let service = Arc::new(ToolService::from_bootstrap(&bootstrap).await?);
        let registry = ToolConfigRegistry::new(bootstrap.manifests.clone());
        let payload = ToolConfigPublishPayload {
            version: "bootstrap".into(),
            manifests: bootstrap.manifests.clone(),
        };
        let err = registry
            .publish(payload, service)
            .await
            .expect_err("should reject duplicate");
        match err {
            ConfigRegistryError::Conflict(_) => {}
            other => panic!("unexpected error: {:?}", other),
        }
        Ok(())
    }

    #[tokio::test]
    async fn tool_execution_respects_idempotency_cache() -> anyhow::Result<()> {
        let bootstrap = ToolBootstrap {
            registry: ToolRegistryConfig::InMemory,
            idempotency: IdempotencyBootstrap::default(),
            events: EventsBootstrap::default(),
            evidence: EvidenceBootstrap::default(),
            manifests: vec![ToolManifestEntry {
                tenant: "tenant-test".into(),
                manifest: keyed_manifest(),
            }],
        };

        let service = ToolService::from_bootstrap(&bootstrap).await?;
        let tenant = TenantId("tenant-test".into());
        let actor = Subject {
            kind: SubjectKind::Service,
            subject_id: Id("svc.test".into()),
            tenant: tenant.clone(),
            claims: serde_json::Map::new(),
        };

        let make_request = |payload: serde_json::Value| ToolInvocationRequest {
            tenant: tenant.clone(),
            tool_id: "demo.echo".into(),
            input: payload,
            actor: actor.clone(),
            origin: ToolOrigin::Api,
            consent: None,
            idempotency_key: Some("key-1".into()),
            call_id: None,
        };

        let first = service
            .execute(make_request(serde_json::json!({ "foo": "bar" })))
            .await?;
        let first_evidence = first.evidence_ref.clone();

        let second = service
            .execute(make_request(serde_json::json!({ "foo": "bar" })))
            .await?;
        assert_eq!(second.evidence_ref, first_evidence);

        let err = service
            .execute(make_request(serde_json::json!({ "foo": "baz" })))
            .await
            .expect_err("hash mismatch should raise");
        let message = tool_error_message(&err);
        assert!(
            message.contains("hash mismatch"),
            "expected hash mismatch message, got {message}"
        );

        let events = service.event_records().await;
        assert_eq!(events.len(), 6);
        match &events[1] {
            ToolEventRecord::End(rec) => assert_eq!(rec.event.status, "ok"),
            _ => panic!("expected end ok"),
        }
        match &events[3] {
            ToolEventRecord::End(rec) => assert_eq!(rec.event.status, "cached"),
            _ => panic!("expected end cached"),
        }
        match &events[5] {
            ToolEventRecord::End(rec) => {
                assert_eq!(rec.event.status, "error");
                assert!(!rec.call_id.0.is_empty());
            }
            _ => panic!("expected end error"),
        }

        let evidence = service.evidence_records().await;
        assert_eq!(evidence.len(), 1);
        assert!(!evidence[0].call_id.0.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn collab_service_execute_and_resolve() -> anyhow::Result<()> {
        let service = CollabService::default();
        let tenant = TenantId("tenant-collab".into());

        let snapshot = service
            .execute(
                &tenant,
                CollabExecutePayload {
                    tenant: None,
                    collab_id: "collab-1".into(),
                    participants: Some(vec!["alice".into(), "bob".into()]),
                    context: Some(serde_json::json!({ "topic": "test" })),
                    note: Some("bootstrap".into()),
                },
            )
            .await
            .expect("collab execute");
        assert_eq!(snapshot.status, "in_progress");
        assert_eq!(snapshot.collab_id, "collab-1");
        assert_eq!(snapshot.participants.len(), 2);
        assert_eq!(snapshot.timeline.len(), 1);

        let resolved = service
            .resolve(
                &tenant,
                CollabResolvePayload {
                    tenant: None,
                    collab_id: "collab-1".into(),
                    outcome: Some(serde_json::json!({"result": "success"})),
                    note: Some("completed".into()),
                },
            )
            .await
            .expect("collab resolve");
        assert_eq!(resolved.status, "resolved");
        assert_eq!(resolved.timeline.len(), 2);
        assert_eq!(
            resolved.resolution.unwrap_or_default(),
            serde_json::json!({"result": "success"})
        );

        Ok(())
    }

    #[tokio::test]
    async fn collab_service_conflict_on_resolved() -> anyhow::Result<()> {
        let service = CollabService::default();
        let tenant = TenantId("tenant-collab".into());

        service
            .execute(
                &tenant,
                CollabExecutePayload {
                    tenant: None,
                    collab_id: "collab-2".into(),
                    participants: None,
                    context: None,
                    note: None,
                },
            )
            .await
            .expect("collab execute");

        service
            .resolve(
                &tenant,
                CollabResolvePayload {
                    tenant: None,
                    collab_id: "collab-2".into(),
                    outcome: None,
                    note: None,
                },
            )
            .await
            .expect("collab resolve");

        let err = service
            .resolve(
                &tenant,
                CollabResolvePayload {
                    tenant: None,
                    collab_id: "collab-2".into(),
                    outcome: None,
                    note: None,
                },
            )
            .await
            .expect_err("expected conflict");

        match err {
            CollabError::Conflict(_) => {}
            other => panic!("unexpected error: {:?}", other),
        }

        Ok(())
    }
}
