use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{BoxStream, StreamExt};
use reqwest::{
    header::{HeaderMap, HeaderValue, CONTENT_TYPE},
    Client, StatusCode, Url,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    sync::{Mutex, Semaphore},
    time::{sleep, Duration as TokioDuration},
};

use crate::{
    chat::{
        ChatDelta, ChatModel, ChatRequest, ChatResponse, ResponseFormat, ResponseKind, ToolSpec,
    },
    errors::LlmError,
    jsonsafe::{enforce_json, StructOutPolicy},
    model::{ContentSegment, FinishReason, Message, Role, ToolCallProposal, Usage},
    provider::{DynChatModel, ProviderCaps, ProviderCfg, ProviderFactory, Registry},
};
use soulbase_types::prelude::Id;

const DEFAULT_BASE_URL: &str = "https://api.anthropic.com/v1/";
const MESSAGES_PATH: &str = "messages";
const DEFAULT_MAX_TOKENS: u32 = 1024;
const DEFAULT_VERSION: &str = "2023-06-01";
const TOOLS_BETA_HEADER: &str = "tools-2024-04-04";

#[derive(Clone, Debug)]
pub struct ClaudeConfig {
    pub api_key: String,
    pub base_url: Url,
    pub request_timeout: Duration,
    pub model_aliases: HashMap<String, String>,
    pub default_max_tokens: u32,
    pub max_concurrent_requests: usize,
    pub request_rate_per_minute: Option<u32>,
    pub anthropic_version: String,
    pub beta_headers: Vec<String>,
}

impl ClaudeConfig {
    pub fn new(api_key: impl Into<String>) -> Result<Self, LlmError> {
        let base_url = Url::parse(DEFAULT_BASE_URL)
            .map_err(|err| LlmError::unknown(&format!("claude base url parse failed: {err}")))?;
        Ok(Self {
            api_key: api_key.into(),
            base_url,
            request_timeout: Duration::from_secs(30),
            model_aliases: HashMap::new(),
            default_max_tokens: DEFAULT_MAX_TOKENS,
            max_concurrent_requests: 8,
            request_rate_per_minute: None,
            anthropic_version: DEFAULT_VERSION.to_string(),
            beta_headers: vec![TOOLS_BETA_HEADER.to_string()],
        })
    }

    pub fn with_base_url(mut self, base_url: impl AsRef<str>) -> Result<Self, LlmError> {
        self.base_url = Url::parse(base_url.as_ref())
            .map_err(|err| LlmError::unknown(&format!("claude base url parse failed: {err}")))?;
        if !self.base_url.path().ends_with('/') {
            self.base_url
                .set_path(&format!("{}/", self.base_url.path().trim_end_matches('/')));
        }
        Ok(self)
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_alias(mut self, alias: impl Into<String>, target: impl Into<String>) -> Self {
        self.model_aliases.insert(alias.into(), target.into());
        self
    }

    pub fn with_default_max_tokens(mut self, max_tokens: u32) -> Self {
        self.default_max_tokens = max_tokens.max(1);
        self
    }

    pub fn with_max_concurrency(mut self, limit: usize) -> Self {
        self.max_concurrent_requests = limit.max(1);
        self
    }

    pub fn with_rate_limit(mut self, rpm: u32) -> Self {
        self.request_rate_per_minute = Some(rpm.max(1));
        self
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.anthropic_version = version.into();
        self
    }

    pub fn with_beta_header(mut self, header: impl Into<String>) -> Self {
        self.beta_headers.push(header.into());
        self
    }
}

struct ClaudeShared {
    client: Client,
    config: ClaudeConfig,
    limiter: Arc<Semaphore>,
    rate_limiter: Option<RateLimiter>,
    messages_url: Url,
}

impl ClaudeShared {
    fn resolve_model(&self, requested: &str) -> String {
        self.config
            .model_aliases
            .get(requested)
            .cloned()
            .unwrap_or_else(|| requested.to_string())
    }

    fn max_tokens(&self, requested: Option<u32>) -> u32 {
        requested.unwrap_or(self.config.default_max_tokens).max(1)
    }
}

pub struct ClaudeProviderFactory {
    shared: Arc<ClaudeShared>,
}

impl ClaudeProviderFactory {
    pub fn new(config: ClaudeConfig) -> Result<Self, LlmError> {
        let mut header_map = HeaderMap::new();
        header_map.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        header_map.insert(
            "x-api-key",
            HeaderValue::from_str(&config.api_key)
                .map_err(|err| LlmError::unknown(&format!("invalid claude api key: {err}")))?,
        );
        header_map.insert(
            "anthropic-version",
            HeaderValue::from_str(&config.anthropic_version)
                .map_err(|err| LlmError::unknown(&format!("invalid claude version: {err}")))?,
        );

        for beta in &config.beta_headers {
            if !beta.trim().is_empty() {
                header_map.append(
                    "anthropic-beta",
                    HeaderValue::from_str(beta).map_err(|err| {
                        LlmError::unknown(&format!("invalid anthropic beta header: {err}"))
                    })?,
                );
            }
        }

        let client = Client::builder()
            .default_headers(header_map)
            .timeout(config.request_timeout)
            .build()
            .map_err(|err| LlmError::unknown(&format!("claude client build failed: {err}")))?;

        let messages_url = config
            .base_url
            .join(MESSAGES_PATH)
            .map_err(|err| LlmError::unknown(&format!("claude messages url join failed: {err}")))?;

        let limiter = Arc::new(Semaphore::new(config.max_concurrent_requests));
        let rate_limiter = config.request_rate_per_minute.map(RateLimiter::per_minute);

        Ok(Self {
            shared: Arc::new(ClaudeShared {
                client,
                config,
                limiter,
                rate_limiter,
                messages_url,
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
            stream: true,
            tools: true,
            embeddings: false,
            rerank: false,
            multimodal: false,
            json_schema: true,
        }
    }

    fn create_chat(&self, model: &str, _cfg: &ProviderCfg) -> Option<Box<DynChatModel>> {
        let resolved = self.shared.resolve_model(model);
        Some(Box::new(ClaudeChatModel {
            model: resolved,
            shared: self.shared.clone(),
        }))
    }
}

struct ClaudeChatModel {
    model: String,
    shared: Arc<ClaudeShared>,
}

#[async_trait]
impl ChatModel for ClaudeChatModel {
    type Stream = BoxStream<'static, Result<ChatDelta, LlmError>>;

    async fn chat(
        &self,
        req: ChatRequest,
        enforce: &StructOutPolicy,
    ) -> Result<ChatResponse, LlmError> {
        let request = build_messages_request(
            &self.model,
            &req,
            self.shared.max_tokens(req.max_tokens),
            false,
        )?;
        let response = self
            .execute(request, req.idempotency_key.as_deref(), false)
            .await?;
        build_chat_response(&req, enforce, response)
    }

    async fn chat_stream(
        &self,
        req: ChatRequest,
        enforce: &StructOutPolicy,
    ) -> Result<Self::Stream, LlmError> {
        let response_format_cfg = req.response_format.clone();
        let idempotency = req.idempotency_key.clone();
        let request = build_messages_request(
            &self.model,
            &req,
            self.shared.max_tokens(req.max_tokens),
            true,
        )?;
        let stream = self
            .execute_stream(
                request,
                idempotency.as_deref(),
                response_format_cfg,
                enforce,
            )
            .await?;
        Ok(stream)
    }
}

impl ClaudeChatModel {
    async fn execute(
        &self,
        request: MessagesRequest,
        idempotency_key: Option<&str>,
        stream: bool,
    ) -> Result<MessagesResponse, LlmError> {
        let _permit = self
            .shared
            .limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| LlmError::unknown(&format!("claude limiter closed: {err}")))?;

        if let Some(rate) = self.shared.rate_limiter.as_ref() {
            rate.acquire().await;
        }

        let mut req_builder = self
            .shared
            .client
            .post(self.shared.messages_url.clone())
            .json(&request);

        if stream {
            req_builder = req_builder.header("accept", "text/event-stream");
        }

        if let Some(key) = idempotency_key {
            if let Ok(value) = HeaderValue::from_str(key) {
                req_builder = req_builder.header("idempotency-key", value);
            }
        }

        let response = req_builder.send().await.map_err(|err| {
            LlmError::provider_unavailable(&format!("claude request error: {err}"))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".into());
            return Err(map_http_error(status, &body));
        }

        let payload = response.json::<MessagesResponse>().await.map_err(|err| {
            LlmError::provider_unavailable(&format!("claude response decode: {err}"))
        })?;
        Ok(payload)
    }

    async fn execute_stream(
        &self,
        request: MessagesRequest,
        idempotency_key: Option<&str>,
        response_format: Option<ResponseFormat>,
        enforce: &StructOutPolicy,
    ) -> Result<BoxStream<'static, Result<ChatDelta, LlmError>>, LlmError> {
        let _permit = self
            .shared
            .limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| LlmError::unknown(&format!("claude limiter closed: {err}")))?;

        if let Some(rate) = self.shared.rate_limiter.as_ref() {
            rate.acquire().await;
        }

        let mut req_builder = self
            .shared
            .client
            .post(self.shared.messages_url.clone())
            .json(&request)
            .header("accept", "text/event-stream");

        if let Some(key) = idempotency_key {
            if let Ok(value) = HeaderValue::from_str(key) {
                req_builder = req_builder.header("idempotency-key", value);
            }
        }

        let response = req_builder.send().await.map_err(|err| {
            LlmError::provider_unavailable(&format!("claude request error: {err}"))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".into());
            return Err(map_http_error(status, &body));
        }

        let stream_response = response;
        let enforce = enforce.clone();
        let format_cfg = response_format.clone();

        let stream = try_stream! {
            let mut reader = EventStreamReader::new(stream_response.bytes_stream());
            let mut aggregated_text = String::new();
            let mut first_token_emitted = false;
            let mut tool_states: HashMap<u32, ToolState> = HashMap::new();
            let mut usage: Option<Usage> = None;
            let mut finish_reason: Option<FinishReason> = None;
            let started_at = Instant::now();

            while let Some(event) = reader.next_event().await? {
                match event {
                    ClaudeStreamEvent::ContentTextDelta { _index: _, text } => {
                        aggregated_text.push_str(&text);
                        let first_token_ms = maybe_record_first_token(&mut first_token_emitted, started_at);
                        yield ChatDelta {
                            text_delta: Some(text),
                            tool_call_delta: None,
                            usage_partial: None,
                            finish: None,
                            first_token_ms,
                        };
                    }
                    ClaudeStreamEvent::ToolStarted { index, id, name } => {
                        tool_states.insert(index, ToolState::new(id, name));
                    }
                    ClaudeStreamEvent::ToolDelta { index, json_delta } => {
                        if let Some(state) = tool_states.get_mut(&index) {
                            state.append(&json_delta);
                        }
                    }
                    ClaudeStreamEvent::ToolCompleted { index } => {
                        if let Some(state) = tool_states.get_mut(&index) {
                            let proposal = state.finalize()?;
                            let first_token_ms = maybe_record_first_token(&mut first_token_emitted, started_at);
                            yield ChatDelta {
                                text_delta: None,
                                tool_call_delta: Some(proposal),
                                usage_partial: None,
                                finish: None,
                                first_token_ms,
                            };
                        }
                    }
                    ClaudeStreamEvent::Usage { input_tokens, output_tokens } => {
                        usage = Some(Usage {
                            input_tokens,
                            output_tokens,
                            cached_tokens: None,
                            image_units: None,
                            audio_seconds: None,
                            requests: 1,
                        });
                    }
                    ClaudeStreamEvent::Finish { stop_reason } => {
                        finish_reason = Some(map_finish_reason(stop_reason));
                    }
                    ClaudeStreamEvent::Ignored => {}
                }
            }

            if !aggregated_text.is_empty() {
                if let Some(format) = format_cfg.as_ref() {
                    match format.kind {
                        ResponseKind::Text => {}
                        ResponseKind::Json => {
                            let _ = enforce_json(&aggregated_text, &enforce)?;
                        }
                        ResponseKind::JsonSchema => {
                            let value = enforce_json(&aggregated_text, &enforce)?;
                            format.validate_schema(&value)?;
                        }
                    }
                }
            }

            let usage_final = usage.unwrap_or(Usage {
                input_tokens: 0,
                output_tokens: 0,
                cached_tokens: None,
                image_units: None,
                audio_seconds: None,
                requests: 1,
            });
            let finish = finish_reason.unwrap_or(FinishReason::Stop);

            yield ChatDelta {
                text_delta: None,
                tool_call_delta: None,
                usage_partial: Some(usage_final),
                finish: Some(finish),
                first_token_ms: None,
            };
        };

        Ok(Box::pin(stream))
    }
}

#[derive(Serialize)]
struct MessagesRequest {
    model: String,
    messages: Vec<RequestMessage>,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    stop_sequences: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<RequestTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<Value>,
}

#[derive(Serialize)]
struct RequestMessage {
    role: String,
    content: Vec<RequestContent>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum RequestContent {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        is_error: Option<bool>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        content: Vec<ToolResultContent>,
    },
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum ToolResultContent {
    #[serde(rename = "text")]
    Text { text: String },
}

#[derive(Serialize)]
struct RequestTool {
    #[serde(rename = "type")]
    kind: &'static str,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    input_schema: Value,
}

#[derive(Deserialize)]
struct MessagesResponse {
    id: String,
    model: Option<String>,
    #[serde(default)]
    content: Vec<ResponseContent>,
    #[serde(default)]
    stop_reason: Option<String>,
    #[serde(default)]
    stop_sequence: Option<String>,
    #[serde(default)]
    usage: Option<ResponseUsage>,
}

#[derive(Deserialize)]
struct ResponseUsage {
    input_tokens: u32,
    output_tokens: u32,
}

#[derive(Deserialize)]
struct ResponseContent {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    input: Option<Value>,
}

fn build_messages_request(
    model: &str,
    req: &ChatRequest,
    max_tokens: u32,
    stream: bool,
) -> Result<MessagesRequest, LlmError> {
    let mut system_prompts = Vec::new();
    let mut messages = Vec::new();

    for message in &req.messages {
        match message.role {
            Role::System => {
                let text = collect_text_segments(message)?;
                if !text.is_empty() {
                    system_prompts.push(text);
                }
            }
            Role::User => {
                messages.push(RequestMessage {
                    role: "user".into(),
                    content: build_user_content(message)?,
                });
            }
            Role::Assistant => {
                messages.push(RequestMessage {
                    role: "assistant".into(),
                    content: build_assistant_content(message)?,
                });
            }
            Role::Tool => {
                messages.push(RequestMessage {
                    role: "user".into(),
                    content: build_tool_result_content(message)?,
                });
            }
        }
    }

    if messages.is_empty() {
        return Err(LlmError::schema(
            "claude request requires at least one non-system message",
        ));
    }

    let system = if system_prompts.is_empty() {
        None
    } else {
        Some(system_prompts.join("\n"))
    };

    let tools = if req.tool_specs.is_empty() {
        None
    } else {
        let mapped = req
            .tool_specs
            .iter()
            .map(to_tool_definition)
            .collect::<Result<Vec<_>, LlmError>>()?;
        Some(mapped)
    };

    let tool_choice = tools.as_ref().map(|_| json!({ "type": "auto" }));

    let response_format = match req.response_format.as_ref() {
        Some(format) => match format.kind {
            ResponseKind::Text => None,
            ResponseKind::Json => Some(json!({ "type": "json" })),
            ResponseKind::JsonSchema => {
                let schema = format
                    .json_schema
                    .as_ref()
                    .ok_or_else(|| {
                        LlmError::schema("claude json_schema response requires schema payload")
                    })
                    .and_then(|schema| {
                        serde_json::to_value(schema).map_err(|err| {
                            LlmError::schema(&format!("claude response schema encode: {err}"))
                        })
                    })?;
                Some(json!({ "type": "json_schema", "json_schema": schema }))
            }
        },
        None => None,
    };

    Ok(MessagesRequest {
        model: model.to_string(),
        messages,
        max_tokens,
        system,
        temperature: req.temperature,
        top_p: req.top_p,
        stop_sequences: req.stop.clone(),
        metadata: non_null_metadata(&req.metadata),
        tools,
        tool_choice,
        stream: if stream { Some(true) } else { None },
        response_format,
    })
}

fn build_chat_response(
    request: &ChatRequest,
    enforce: &StructOutPolicy,
    response: MessagesResponse,
) -> Result<ChatResponse, LlmError> {
    let mut segments = Vec::new();
    let mut tool_calls = Vec::new();
    let mut aggregated_text = String::new();

    for block in response.content {
        match block.kind.as_str() {
            "text" => {
                if let Some(text) = block.text {
                    aggregated_text.push_str(&text);
                    segments.push(ContentSegment::Text { text });
                }
            }
            "tool_use" => {
                let id = block
                    .id
                    .ok_or_else(|| LlmError::provider_unavailable("claude tool_use missing id"))?;
                let name = block.name.ok_or_else(|| {
                    LlmError::provider_unavailable("claude tool_use missing name")
                })?;
                let input = block.input.unwrap_or(Value::Null);
                tool_calls.push(ToolCallProposal {
                    name,
                    call_id: Id(id),
                    arguments: input,
                });
            }
            other => {
                return Err(LlmError::unknown(&format!(
                    "claude returned unsupported content block '{other}'"
                )));
            }
        }
    }

    if let Some(format) = request.response_format.as_ref() {
        match format.kind {
            ResponseKind::Json => {
                let _ = enforce_json(&aggregated_text, enforce)?;
            }
            ResponseKind::JsonSchema => {
                let value = enforce_json(&aggregated_text, enforce)?;
                format.validate_schema(&value)?;
            }
            ResponseKind::Text => {}
        }
    }

    let usage = response
        .usage
        .map(|usage| Usage {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            cached_tokens: None,
            image_units: None,
            audio_seconds: None,
            requests: 1,
        })
        .unwrap_or_else(|| Usage {
            input_tokens: 0,
            output_tokens: 0,
            cached_tokens: None,
            image_units: None,
            audio_seconds: None,
            requests: 1,
        });

    let finish = map_finish_reason(response.stop_reason.clone());
    let provider_meta = json!({
        "provider": "claude",
        "response_id": response.id,
        "response_model": response.model,
        "stop_reason": response.stop_reason,
        "stop_sequence": response.stop_sequence,
    });

    Ok(ChatResponse {
        model_id: request.model_id.clone(),
        message: Message {
            role: Role::Assistant,
            segments,
            tool_calls,
        },
        usage,
        cost: None,
        finish,
        provider_meta,
    })
}

fn collect_text_segments(message: &Message) -> Result<String, LlmError> {
    let mut text = String::new();
    for segment in &message.segments {
        match segment {
            ContentSegment::Text { text: value } => text.push_str(value),
            _ => {
                return Err(LlmError::schema(
                    "claude provider supports text segments only for system messages",
                ))
            }
        }
    }
    Ok(text)
}

fn build_user_content(message: &Message) -> Result<Vec<RequestContent>, LlmError> {
    let mut blocks = Vec::new();
    for segment in &message.segments {
        match segment {
            ContentSegment::Text { text } => {
                blocks.push(RequestContent::Text { text: text.clone() })
            }
            _ => {
                return Err(LlmError::schema(
                    "claude provider currently supports text user segments only",
                ))
            }
        }
    }
    if blocks.is_empty() {
        return Err(LlmError::schema(
            "claude user message requires at least one text segment",
        ));
    }
    Ok(blocks)
}

fn build_assistant_content(message: &Message) -> Result<Vec<RequestContent>, LlmError> {
    let mut blocks = Vec::new();
    let mut text_buffer = String::new();

    for segment in &message.segments {
        match segment {
            ContentSegment::Text { text } => text_buffer.push_str(text),
            _ => {
                return Err(LlmError::schema(
                    "claude provider currently supports text assistant segments only",
                ))
            }
        }
    }

    if !text_buffer.is_empty() {
        blocks.push(RequestContent::Text { text: text_buffer });
    }

    for call in &message.tool_calls {
        blocks.push(RequestContent::ToolUse {
            id: call.call_id.0.clone(),
            name: call.name.clone(),
            input: call.arguments.clone(),
        });
    }

    if blocks.is_empty() {
        blocks.push(RequestContent::Text {
            text: String::new(),
        });
    }

    Ok(blocks)
}

fn build_tool_result_content(message: &Message) -> Result<Vec<RequestContent>, LlmError> {
    let mut text_buffer = String::new();
    for segment in &message.segments {
        match segment {
            ContentSegment::Text { text } => text_buffer.push_str(text),
            _ => {
                return Err(LlmError::schema(
                    "claude provider supports text tool outputs only",
                ))
            }
        }
    }

    let call = message.tool_calls.get(0).ok_or_else(|| {
        LlmError::schema("claude tool result message requires tool_call metadata")
    })?;

    Ok(vec![RequestContent::ToolResult {
        tool_use_id: call.call_id.0.clone(),
        is_error: None,
        content: if text_buffer.is_empty() {
            Vec::new()
        } else {
            vec![ToolResultContent::Text { text: text_buffer }]
        },
    }])
}

fn to_tool_definition(spec: &ToolSpec) -> Result<RequestTool, LlmError> {
    Ok(RequestTool {
        kind: "function",
        name: spec.id.clone(),
        description: Some(spec.description.clone()),
        input_schema: spec.input_schema.clone(),
    })
}

fn non_null_metadata(value: &Value) -> Option<Value> {
    match value {
        Value::Null => None,
        other => Some(other.clone()),
    }
}

fn map_finish_reason(reason: Option<String>) -> FinishReason {
    match reason.as_deref() {
        Some("end_turn") | Some("stop_sequence") | Some("api_timeout") => FinishReason::Stop,
        Some("max_tokens") => FinishReason::Length,
        Some("tool_use") => FinishReason::Tool,
        Some(other) => FinishReason::Other(other.to_string()),
        None => FinishReason::Stop,
    }
}

fn map_http_error(status: StatusCode, body: &str) -> LlmError {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            LlmError::provider_unavailable(&format!("claude auth failed: {body}"))
        }
        StatusCode::TOO_MANY_REQUESTS => {
            LlmError::provider_unavailable(&format!("claude rate limited request: {body}"))
        }
        StatusCode::BAD_REQUEST => LlmError::schema(&format!("claude rejected request: {body}")),
        _ => LlmError::provider_unavailable(&format!(
            "claude returned {}: {}",
            status.as_u16(),
            body
        )),
    }
}

struct ToolState {
    id: String,
    name: String,
    buffer: String,
}

impl ToolState {
    fn new(id: String, name: String) -> Self {
        Self {
            id,
            name,
            buffer: String::new(),
        }
    }

    fn append(&mut self, delta: &str) {
        self.buffer.push_str(delta);
    }

    fn finalize(&mut self) -> Result<ToolCallProposal, LlmError> {
        let value: Value = if self.buffer.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&self.buffer).map_err(|err| {
                LlmError::schema(&format!("claude tool input decode failed: {err}"))
            })?
        };
        Ok(ToolCallProposal {
            name: self.name.clone(),
            call_id: Id(self.id.clone()),
            arguments: value,
        })
    }
}

struct RateLimiter {
    state: Arc<Mutex<RateLimitState>>,
    capacity: u32,
    refill: TokioDuration,
}

struct RateLimitState {
    tokens: u32,
    last_refill: Instant,
}

impl RateLimiter {
    fn new(capacity: u32, interval: TokioDuration) -> Self {
        Self {
            state: Arc::new(Mutex::new(RateLimitState {
                tokens: capacity,
                last_refill: Instant::now(),
            })),
            capacity,
            refill: interval,
        }
    }

    fn per_minute(rpm: u32) -> Self {
        Self::new(rpm, TokioDuration::from_secs(60))
    }

    async fn acquire(&self) {
        loop {
            let mut guard = self.state.lock().await;
            let now = Instant::now();
            if guard.tokens == 0 {
                let elapsed = now.saturating_duration_since(guard.last_refill);
                if elapsed >= self.refill {
                    let intervals =
                        (elapsed.as_secs_f64() / self.refill.as_secs_f64()).floor() as u32;
                    if intervals > 0 {
                        guard.tokens =
                            (guard.tokens + intervals * self.capacity).min(self.capacity);
                        guard.last_refill = now;
                    }
                }
            }

            if guard.tokens > 0 {
                guard.tokens -= 1;
                return;
            }

            let wait = self
                .refill
                .checked_sub(now.saturating_duration_since(guard.last_refill))
                .unwrap_or_else(|| TokioDuration::from_millis(10));
            drop(guard);
            sleep(wait).await;
        }
    }
}

fn maybe_record_first_token(emitted: &mut bool, started_at: Instant) -> Option<u32> {
    if *emitted {
        None
    } else {
        *emitted = true;
        let ms = started_at.elapsed().as_millis().min(u128::from(u32::MAX)) as u32;
        Some(ms)
    }
}

enum ClaudeStreamEvent {
    ContentTextDelta {
        _index: u32,
        text: String,
    },
    ToolStarted {
        index: u32,
        id: String,
        name: String,
    },
    ToolDelta {
        index: u32,
        json_delta: String,
    },
    ToolCompleted {
        index: u32,
    },
    Usage {
        input_tokens: u32,
        output_tokens: u32,
    },
    Finish {
        stop_reason: Option<String>,
    },
    Ignored,
}

struct EventStreamReader<S>
where
    S: futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
{
    inner: S,
    buffer: Vec<u8>,
}

impl<S> EventStreamReader<S>
where
    S: futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
{
    fn new(stream: S) -> Self {
        Self {
            inner: stream,
            buffer: Vec::new(),
        }
    }

    async fn next_event(&mut self) -> Result<Option<ClaudeStreamEvent>, LlmError> {
        loop {
            if let Some(event) = self.try_parse_event()? {
                return Ok(Some(event));
            }

            match self.inner.next().await {
                Some(Ok(chunk)) => self.buffer.extend_from_slice(&chunk),
                Some(Err(err)) => {
                    return Err(LlmError::provider_unavailable(&format!(
                        "claude stream read error: {err}"
                    )))
                }
                None => {
                    if self.buffer.is_empty() {
                        return Ok(None);
                    }
                    // Drain remaining data.
                    return self.try_parse_event();
                }
            }
        }
    }

    fn try_parse_event(&mut self) -> Result<Option<ClaudeStreamEvent>, LlmError> {
        let delimiter = b"\n\n";
        if let Some(pos) = self
            .buffer
            .windows(delimiter.len())
            .position(|window| window == delimiter)
        {
            let event_bytes = self
                .buffer
                .drain(..pos + delimiter.len())
                .collect::<Vec<_>>();
            let event_str = String::from_utf8_lossy(&event_bytes);

            let mut event_name = None;
            let mut data = None;
            for line in event_str.lines() {
                let line = line.trim();
                if line.starts_with("event:") {
                    event_name = Some(line.trim_start_matches("event:").trim().to_string());
                } else if line.starts_with("data:") {
                    let value = line.trim_start_matches("data:").trim().to_string();
                    data = if let Some(existing) = data {
                        Some(existing + value.as_str())
                    } else {
                        Some(value)
                    };
                }
            }

            let name = event_name.unwrap_or_default();
            let payload = match data {
                Some(payload) if payload != "[DONE]" => payload,
                _ => return Ok(Some(ClaudeStreamEvent::Ignored)),
            };

            let event = parse_stream_event(&name, &payload)?;
            Ok(Some(event))
        } else {
            Ok(None)
        }
    }
}

fn parse_stream_event(event: &str, payload: &str) -> Result<ClaudeStreamEvent, LlmError> {
    match event {
        "content_block_start" => {
            let value: Value = serde_json::from_str(payload).map_err(|err| {
                LlmError::provider_unavailable(&format!(
                    "claude content_block_start decode: {err}; payload={payload}"
                ))
            })?;
            let index = value
                .get("index")
                .and_then(Value::as_u64)
                .ok_or_else(|| LlmError::provider_unavailable("claude stream missing index"))?;
            let block = value
                .get("content")
                .and_then(Value::as_object)
                .ok_or_else(|| LlmError::provider_unavailable("claude stream missing content"))?;
            let kind = block
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if kind == "tool_use" {
                let id = block
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        LlmError::provider_unavailable("claude tool_use content missing id")
                    })?
                    .to_string();
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        LlmError::provider_unavailable("claude tool_use content missing name")
                    })?
                    .to_string();
                Ok(ClaudeStreamEvent::ToolStarted {
                    index: index as u32,
                    id,
                    name,
                })
            } else {
                Ok(ClaudeStreamEvent::Ignored)
            }
        }
        "content_block_delta" => {
            let value: Value = serde_json::from_str(payload).map_err(|err| {
                LlmError::provider_unavailable(&format!(
                    "claude content_block_delta decode: {err}; payload={payload}"
                ))
            })?;
            let index = value
                .get("index")
                .and_then(Value::as_u64)
                .ok_or_else(|| LlmError::provider_unavailable("claude delta missing index"))?;
            let delta = value
                .get("delta")
                .and_then(Value::as_object)
                .ok_or_else(|| LlmError::provider_unavailable("claude delta missing payload"))?;
            let kind = delta
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match kind {
                "text_delta" => {
                    let text = delta
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    Ok(ClaudeStreamEvent::ContentTextDelta {
                        _index: index as u32,
                        text,
                    })
                }
                "input_json_delta" => {
                    let partial = delta
                        .get("partial_json")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    Ok(ClaudeStreamEvent::ToolDelta {
                        index: index as u32,
                        json_delta: partial,
                    })
                }
                _ => Ok(ClaudeStreamEvent::Ignored),
            }
        }
        "content_block_stop" => {
            let value: Value = serde_json::from_str(payload).map_err(|err| {
                LlmError::provider_unavailable(&format!(
                    "claude content_block_stop decode: {err}; payload={payload}"
                ))
            })?;
            let index = value
                .get("index")
                .and_then(Value::as_u64)
                .ok_or_else(|| LlmError::provider_unavailable("claude stop missing index"))?;
            Ok(ClaudeStreamEvent::ToolCompleted {
                index: index as u32,
            })
        }
        "message_delta" => {
            let value: Value = serde_json::from_str(payload).map_err(|err| {
                LlmError::provider_unavailable(&format!(
                    "claude message_delta decode: {err}; payload={payload}"
                ))
            })?;
            if let Some(usage) = value.get("delta").and_then(|d| d.get("usage")) {
                let input = usage
                    .get("input_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or_default() as u32;
                let output = usage
                    .get("output_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or_default() as u32;
                Ok(ClaudeStreamEvent::Usage {
                    input_tokens: input,
                    output_tokens: output,
                })
            } else if let Some(stop_reason) = value
                .get("delta")
                .and_then(|d| d.get("stop_reason"))
                .and_then(Value::as_str)
            {
                Ok(ClaudeStreamEvent::Finish {
                    stop_reason: Some(stop_reason.to_string()),
                })
            } else {
                Ok(ClaudeStreamEvent::Ignored)
            }
        }
        "message_stop" => {
            let value: Value = serde_json::from_str(payload).map_err(|err| {
                LlmError::provider_unavailable(&format!(
                    "claude message_stop decode: {err}; payload={payload}"
                ))
            })?;
            let stop_reason = value
                .get("stop_reason")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            Ok(ClaudeStreamEvent::Finish { stop_reason })
        }
        _ => Ok(ClaudeStreamEvent::Ignored),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_request(model_id: &str) -> ChatRequest {
        ChatRequest {
            model_id: model_id.to_string(),
            messages: vec![
                Message {
                    role: Role::System,
                    segments: vec![ContentSegment::Text {
                        text: "You are helpful.".into(),
                    }],
                    tool_calls: vec![],
                },
                Message {
                    role: Role::User,
                    segments: vec![ContentSegment::Text {
                        text: "Say hi".into(),
                    }],
                    tool_calls: vec![],
                },
            ],
            tool_specs: vec![],
            temperature: Some(0.1),
            top_p: None,
            max_tokens: Some(32),
            stop: vec![],
            seed: None,
            frequency_penalty: None,
            presence_penalty: None,
            logit_bias: serde_json::Map::new(),
            response_format: None,
            idempotency_key: Some("idem-123".into()),
            cache_hint: None,
            allow_sensitive: false,
            metadata: Value::Null,
        }
    }

    fn sample_response() -> Value {
        json!({
            "id": "msg_123",
            "model": "claude-3-haiku",
            "content": [{
                "type": "text",
                "text": "Hello there!"
            }],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 12,
                "output_tokens": 6
            }
        })
    }

    #[tokio::test]
    async fn chat_happy_path() {
        let server = MockServer::start().await;

        let response = ResponseTemplate::new(200).set_body_json(sample_response());
        Mock::given(method("POST"))
            .and(path(format!("/{}", MESSAGES_PATH)))
            .and(header("x-api-key", "test-key"))
            .respond_with(response)
            .mount(&server)
            .await;

        let cfg = ClaudeConfig::new("test-key")
            .unwrap()
            .with_base_url(&server.uri())
            .unwrap()
            .with_alias("haiku", "claude-3-haiku")
            .with_default_max_tokens(64);

        let factory = ClaudeProviderFactory::new(cfg).unwrap();
        let mut registry = Registry::new();
        factory.install(&mut registry);

        let model = registry.chat("claude:haiku").expect("model registered");
        let request = sample_request("claude:haiku");
        let response = model
            .chat(request.clone(), &StructOutPolicy::StrictReject)
            .await
            .expect("chat succeeds");

        assert_eq!(response.message.segments.len(), 1);
        assert_eq!(
            response.message.segments[0],
            ContentSegment::Text {
                text: "Hello there!".into()
            }
        );
        assert_eq!(response.usage.input_tokens, 12);
        assert_eq!(response.finish, FinishReason::Stop);
    }
}
