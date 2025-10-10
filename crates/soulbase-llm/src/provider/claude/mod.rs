use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use reqwest::{header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE}, Client, Url};
use serde::{Deserialize, Serialize};

use crate::chat::{ChatRequest, ChatResponse};
use crate::embed::{EmbedRequest, EmbedResponse, EmbedModel};
use crate::errors::LlmError;
use crate::jsonsafe::StructOutPolicy;
use crate::model::{ChatDelta, ChatModel, ToolCallProposal};
use crate::provider::{DynChatModel, DynEmbedModel, ProviderCaps, ProviderCfg, ProviderFactory, Registry};

#[derive(Clone, Debug)]
pub struct ClaudeConfig {
    pub api_key: String,
    pub base_url: Url,
    pub request_timeout: Duration,
    pub model_aliases: std::collections::HashMap<String, String>,
}

impl ClaudeConfig {
    pub fn new(api_key: impl Into<String>) -> Result<Self, LlmError> {
        let base_url = Url::parse("https://api.anthropic.com/v1/")
            .map_err(|err| LlmError::unknown(&format!("claude base url parse failed: {err}")))?;
        Ok(Self {
            api_key: api_key.into(),
            base_url,
            request_timeout: Duration::from_secs(30),
            model_aliases: std::collections::HashMap::new(),
        })
    }

    pub fn with_alias(mut self, alias: impl Into<String>, target: impl Into<String>) -> Self {
        self.model_aliases.insert(alias.into(), target.into());
        self
    }

    pub fn with_base_url(mut self, base_url: impl AsRef<str>) -> Result<Self, LlmError> {
        self.base_url = Url::parse(base_url.as_ref())
            .map_err(|err| LlmError::unknown(&format!("claude base url parse failed: {err}")))?;
        Ok(self)
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

struct ClaudeShared {
    client: Client,
    base_url: Url,
    config: ClaudeConfig,
}

pub struct ClaudeProviderFactory {
    shared: Arc<ClaudeShared>,
}

impl ClaudeProviderFactory {
    pub fn new(config: ClaudeConfig) -> Result<Self, LlmError> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let auth = format!("Bearer {}", config.api_key);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth)
                .map_err(|err| LlmError::unknown(&format!("invalid claude api key: {err}")))?,
        );

        let client = Client::builder()
            .default_headers(headers)
            .timeout(config.request_timeout)
            .build()
            .map_err(|err| LlmError::unknown(&format!("claude client build failed: {err}")))?;

        Ok(Self {
            shared: Arc::new(ClaudeShared {
                client,
                base_url: config.base_url.clone(),
                config,
            }),
        })
    }

    pub fn install(self, registry: &mut Registry) {
        registry.register(Box::new(self));
    }
}

#[async_trait]
impl ProviderFactory for ClaudeProviderFactory {
    fn name(&self) -> &'static str {
        "claude"
    }

    fn caps(&self) -> ProviderCaps {
        ProviderCaps {
            chat: true,
            stream: false,
            tools: false,
            embeddings: false,
            rerank: false,
            multimodal: false,
            json_schema: false,
        }
    }

    fn create_chat(&self, model: &str, _cfg: &ProviderCfg) -> Option<Box<DynChatModel>> {
        let resolved = self
            .shared
            .config
            .model_aliases
            .get(model)
            .cloned()
            .unwrap_or_else(|| model.to_string());
        Some(Box::new(ClaudeChatModel {
            model: resolved,
            shared: self.shared.clone(),
        }))
    }

    fn create_embed(&self, _model: &str, _cfg: &ProviderCfg) -> Option<Box<DynEmbedModel>> {
        None
    }
}

struct ClaudeChatModel {
    model: String,
    shared: Arc<ClaudeShared>,
}

#[async_trait]
impl ChatModel for ClaudeChatModel {
    type Stream = futures_util::stream::BoxStream<'static, Result<ChatDelta, LlmError>>;

    async fn chat(
        &self,
        _req: ChatRequest,
        _enforce: &StructOutPolicy,
    ) -> Result<ChatResponse, LlmError> {
        Err(LlmError::unknown("claude provider not yet implemented"))
    }

    async fn chat_stream(
        &self,
        _req: ChatRequest,
        _enforce: &StructOutPolicy,
    ) -> Result<Self::Stream, LlmError> {
        Err(LlmError::unknown("claude provider does not support streaming in skeleton"))
    }
}

#[async_trait]
impl EmbedModel for ClaudeChatModel {
    async fn embed(&self, _req: EmbedRequest) -> Result<EmbedResponse, LlmError> {
        Err(LlmError::unknown("claude embeddings not implemented"))
    }
}
