#![cfg(feature = "provider-openai")]

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_stream::try_stream;
use futures_util::stream::{BoxStream, StreamExt};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE},
    Client, StatusCode, Url,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use crate::{
    chat::{ChatDelta, ChatRequest, ChatResponse, ResponseFormat, ResponseKind, ToolSpec},
    errors::LlmError,
    jsonsafe::{enforce_json, StructOutPolicy},
    model::{ContentSegment, FinishReason, Message, Role, ToolCallProposal, Usage},
    provider::{DynChatModel, ProviderCaps, ProviderCfg, ProviderFactory, Registry},
};
use soulbase_types::prelude::Id;

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1/";
const CHAT_COMPLETIONS_PATH: &str = "chat/completions";

/// Configuration options for the OpenAI provider.
#[derive(Clone, Debug)]
pub struct OpenAiConfig {
    pub api_key: String,
    pub base_url: Url,
    pub organization: Option<String>,
    pub project: Option<String>,
    pub request_timeout: Duration,
    pub model_aliases: HashMap<String, String>,
    pub max_concurrent_requests: usize,
}

impl OpenAiConfig {
    pub fn new(api_key: impl Into<String>) -> Result<Self, LlmError> {
        let base_url = Url::parse(DEFAULT_BASE_URL)
            .map_err(|err| LlmError::unknown(&format!("openai base url parse failed: {err}")))?;
        Ok(Self {
            api_key: api_key.into(),
            base_url,
            organization: None,
            project: None,
            request_timeout: Duration::from_secs(30),
            model_aliases: HashMap::new(),
            max_concurrent_requests: 8,
        })
    }

    pub fn with_base_url(mut self, base_url: impl AsRef<str>) -> Result<Self, LlmError> {
        self.base_url = Url::parse(base_url.as_ref())
            .map_err(|err| LlmError::unknown(&format!("openai base url parse failed: {err}")))?;
        if !self.base_url.path().ends_with('/') {
            self.base_url
                .set_path(&format!("{}/", self.base_url.path().trim_end_matches('/')));
        }
        Ok(self)
    }

    pub fn with_organization(mut self, organization: impl Into<String>) -> Self {
        self.organization = Some(organization.into());
        self
    }

    pub fn with_project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_max_concurrency(mut self, limit: usize) -> Self {
        self.max_concurrent_requests = limit.max(1);
        self
    }

    pub fn with_alias(mut self, alias: impl Into<String>, target: impl Into<String>) -> Self {
        self.model_aliases.insert(alias.into(), target.into());
        self
    }
}

struct OpenAiShared {
    client: Client,
    config: OpenAiConfig,
    limiter: Arc<Semaphore>,
    chat_url: Url,
}

impl OpenAiShared {
    fn resolve_model(&self, requested: &str) -> String {
        self.config
            .model_aliases
            .get(requested)
            .cloned()
            .unwrap_or_else(|| requested.to_string())
    }
}

pub struct OpenAiProviderFactory {
    shared: Arc<OpenAiShared>,
}

impl OpenAiProviderFactory {
    pub fn new(config: OpenAiConfig) -> Result<Self, LlmError> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let auth = format!("Bearer {}", config.api_key);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth)
                .map_err(|err| LlmError::unknown(&format!("invalid openai api key: {err}")))?,
        );

        if let Some(org) = config.organization.as_ref() {
            headers.insert(
                HeaderName::from_static("openai-organization"),
                HeaderValue::from_str(org).map_err(|err| {
                    LlmError::unknown(&format!("invalid organization header: {err}"))
                })?,
            );
        }
        if let Some(project) = config.project.as_ref() {
            headers.insert(
                HeaderName::from_static("openai-project"),
                HeaderValue::from_str(project)
                    .map_err(|err| LlmError::unknown(&format!("invalid project header: {err}")))?,
            );
        }

        let client = Client::builder()
            .default_headers(headers)
            .timeout(config.request_timeout)
            .build()
            .map_err(|err| LlmError::unknown(&format!("openai client build failed: {err}")))?;

        let chat_url = config
            .base_url
            .join(CHAT_COMPLETIONS_PATH)
            .map_err(|err| LlmError::unknown(&format!("openai chat url join failed: {err}")))?;

        let limiter = Arc::new(Semaphore::new(config.max_concurrent_requests));

        Ok(Self {
            shared: Arc::new(OpenAiShared {
                client,
                chat_url,
                limiter,
                config,
            }),
        })
    }

    pub fn install(self, registry: &mut Registry) {
        registry.register(Box::new(self));
    }
}

impl ProviderFactory for OpenAiProviderFactory {
    fn name(&self) -> &'static str {
        "openai"
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
        Some(Box::new(OpenAiChatModel {
            model: resolved,
            shared: self.shared.clone(),
        }))
    }
}

struct OpenAiChatModel {
    model: String,
    shared: Arc<OpenAiShared>,
}

#[derive(Serialize)]
struct ChatCompletionRequest<'a> {
    model: &'a str,
    messages: Vec<OutboundMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    logit_bias: Option<HashMap<String, f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    presence_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    frequency_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<ResponseFormatSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<OutboundTool>>,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<StreamOptions>,
}

#[derive(Serialize)]
struct OutboundMessage {
    role: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OutboundToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

#[derive(Serialize)]
struct OutboundTool {
    #[serde(rename = "type")]
    kind: &'static str,
    function: OutboundFunction,
}

#[derive(Serialize)]
struct OutboundFunction {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    parameters: Value,
}

#[derive(Serialize)]
struct OutboundToolCall {
    id: String,
    #[serde(rename = "type")]
    kind: &'static str,
    function: OutboundFunctionCall,
}

#[derive(Serialize)]
struct OutboundFunctionCall {
    name: String,
    arguments: String,
}

#[derive(Serialize)]
struct ResponseFormatSpec {
    #[serde(rename = "type")]
    format_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    json_schema: Option<JsonSchemaSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    strict: Option<bool>,
}

#[derive(Serialize)]
struct JsonSchemaSpec {
    name: &'static str,
    schema: Value,
}

#[derive(Serialize)]
struct StreamOptions {
    include_usage: bool,
}

#[derive(Deserialize)]
struct ChatCompletionResponse {
    id: String,
    model: String,
    choices: Vec<ChatCompletionChoice>,
    usage: Option<UsagePayload>,
    #[serde(default)]
    system_fingerprint: Option<String>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct UsagePayload {
    prompt_tokens: Option<u32>,
    completion_tokens: Option<u32>,
    total_tokens: Option<u32>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct ChatCompletionChoice {
    index: u32,
    message: InboundMessage,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct InboundMessage {
    role: String,
    #[serde(default)]
    content: Option<InboundContent>,
    #[serde(default)]
    tool_calls: Vec<InboundToolCall>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum InboundContent {
    Text(String),
    Parts(Vec<InboundContentPart>),
}

#[derive(Deserialize)]
struct InboundContentPart {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: Option<String>,
}

#[derive(Deserialize)]
struct InboundToolCall {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    function: InboundFunctionCall,
}

#[derive(Deserialize)]
struct InboundFunctionCall {
    name: String,
    arguments: String,
}

#[derive(Deserialize, Default)]
#[allow(dead_code)]
struct StreamChunk {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    system_fingerprint: Option<String>,
    #[serde(default)]
    choices: Vec<StreamChoice>,
    #[serde(default)]
    usage: Option<UsagePayload>,
}

#[derive(Deserialize, Default)]
struct StreamChoice {
    index: u32,
    #[serde(default)]
    delta: StreamDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Deserialize, Default)]
#[allow(dead_code)]
struct StreamDelta {
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<StreamToolCallDelta>,
}

#[derive(Deserialize, Default)]
#[allow(dead_code)]
struct StreamToolCallDelta {
    #[serde(default)]
    index: Option<u32>,
    #[serde(default)]
    id: Option<String>,
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    function: Option<StreamFunctionDelta>,
}

#[derive(Deserialize, Default)]
struct StreamFunctionDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Default)]
struct ToolCallState {
    id: Option<String>,
    name: Option<String>,
    arguments_buf: String,
    last_emitted_len: usize,
    emitted: bool,
}

impl ToolCallState {
    fn apply(&mut self, delta: &StreamToolCallDelta) -> Result<Option<ToolCallProposal>, LlmError> {
        if let Some(id) = delta.id.as_ref() {
            if self.id.is_none() {
                self.id = Some(id.clone());
            }
        }
        if let Some(func) = delta.function.as_ref() {
            if let Some(name) = func.name.as_ref() {
                if self.name.is_none() {
                    self.name = Some(name.clone());
                }
            }
            if let Some(arguments) = func.arguments.as_ref() {
                self.arguments_buf.push_str(arguments);
            }
        }

        if self.arguments_buf.len() == self.last_emitted_len {
            return Ok(None);
        }

        let id = match self.id.as_ref() {
            Some(id) => id,
            None => return Ok(None),
        };
        let name = match self.name.as_ref() {
            Some(name) => name,
            None => return Ok(None),
        };

        match serde_json::from_str::<Value>(&self.arguments_buf) {
            Ok(arguments) => {
                self.last_emitted_len = self.arguments_buf.len();
                self.emitted = true;
                Ok(Some(ToolCallProposal {
                    name: name.clone(),
                    call_id: Id(id.clone()),
                    arguments,
                }))
            }
            Err(_) => Ok(None),
        }
    }

    fn proposal(&self) -> Option<ToolCallProposal> {
        if self.emitted {
            return None;
        }
        let id = self.id.as_ref()?;
        let name = self.name.as_ref()?;
        if self.arguments_buf.is_empty() {
            return None;
        }
        serde_json::from_str::<Value>(&self.arguments_buf)
            .ok()
            .map(|arguments| ToolCallProposal {
                name: name.clone(),
                call_id: Id(id.clone()),
                arguments,
            })
    }
}

#[async_trait::async_trait]
impl crate::chat::ChatModel for OpenAiChatModel {
    type Stream = BoxStream<'static, Result<ChatDelta, LlmError>>;

    async fn chat(
        &self,
        req: ChatRequest,
        enforce: &StructOutPolicy,
    ) -> Result<ChatResponse, LlmError> {
        let payload = build_request(&self.model, &req, false)?;
        let response = self
            .execute(payload, req.idempotency_key.as_deref())
            .await?;
        build_chat_response(&req, enforce, response)
    }

    async fn chat_stream(
        &self,
        req: ChatRequest,
        enforce: &StructOutPolicy,
    ) -> Result<Self::Stream, LlmError> {
        let shared = self.shared.clone();
        let model = self.model.clone();
        let policy = enforce.clone();
        let stream = try_stream! {
            let request = req;
            let payload = build_request(&model, &request, true)?;

            let permit = shared
                .limiter
                .clone()
                .acquire_owned()
                .await
                .map_err(|err| LlmError::unknown(&format!("openai limiter closed: {err}")))?;

            let mut request_builder = shared
                .client
                .post(shared.chat_url.clone())
                .json(&payload);

            if let Some(key) = request.idempotency_key.as_deref() {
                if let Ok(value) = HeaderValue::from_str(key) {
                    request_builder =
                        request_builder.header(HeaderName::from_static("idempotency-key"), value);
                }
            }

            let response = request_builder
                .send()
                .await
                .map_err(|err| LlmError::provider_unavailable(&format!("openai request error: {err}")))?;

            let status = response.status();
            if !status.is_success() {
                let body = format!("stream init failed (status {status})");
                drop(permit);
                Err(map_http_error(status, &body))?;
            }

            let mut body = response.bytes_stream();
            let mut buffer = String::new();
            let mut data_buf = String::new();
            let mut aggregated_text = String::new();
            let mut tool_states: HashMap<u32, ToolCallState> = HashMap::new();
            let mut usage_final: Option<Usage> = None;
            let mut finish_final: Option<FinishReason> = None;
            let mut done = false;
            let started_at = Instant::now();
            let mut first_token_emitted = false;

            while let Some(chunk) = body.next().await {
                let chunk = chunk
                    .map_err(|err| LlmError::provider_unavailable(&format!("openai stream chunk error: {err}")))?;
                let chunk_str = std::str::from_utf8(&chunk)
                    .map_err(|err| LlmError::provider_unavailable(&format!("openai stream utf8 error: {err}")))?;
                buffer.push_str(chunk_str);

                while let Some(pos) = buffer.find('\n') {
                    let line = buffer[..pos].to_string();
                    buffer = buffer[pos + 1..].to_string();

                    if line.starts_with("data:") {
                        let data = line
                            .trim_start_matches("data:")
                            .trim_start_matches(' ')
                            .to_string();
                        if !data.is_empty() {
                            if !data_buf.is_empty() {
                                data_buf.push('\n');
                            }
                            data_buf.push_str(&data);
                        }
                    } else if line.is_empty() {
                        if data_buf.is_empty() {
                            continue;
                        }
                        let data = data_buf.trim_end();
                        if data == "[DONE]" {
                            done = true;
                            data_buf.clear();
                            break;
                        }

                        let chunk: StreamChunk = serde_json::from_str(data).map_err(|err| {
                            LlmError::provider_unavailable(&format!(
                                "openai stream decode error: {err}; payload={data}"
                            ))
                        })?;

                        if let Some(usage) = chunk.usage {
                            usage_final = Some(Usage {
                                input_tokens: usage.prompt_tokens.unwrap_or_default(),
                                output_tokens: usage.completion_tokens.unwrap_or_default(),
                                cached_tokens: None,
                                image_units: None,
                                audio_seconds: None,
                                requests: 1,
                            });
                        }

                        for choice in chunk.choices {
                            if choice.index != 0 {
                                continue;
                            }

                            if let Some(content) = choice.delta.content {
                                aggregated_text.push_str(&content);
                                let first_token_ms =
                                    maybe_record_first_token(&mut first_token_emitted, started_at);
                                yield ChatDelta {
                                    text_delta: Some(content),
                                    tool_call_delta: None,
                                    usage_partial: None,
                                    finish: None,
                                    first_token_ms,
                                };
                            }

                            for tool_delta in choice.delta.tool_calls {
                                let index = tool_delta.index.unwrap_or(0);
                                let state = tool_states.entry(index).or_default();
                                if let Some(proposal) = state.apply(&tool_delta)? {
                                    let first_token_ms =
                                        maybe_record_first_token(&mut first_token_emitted, started_at);
                                    yield ChatDelta {
                                        text_delta: None,
                                        tool_call_delta: Some(proposal),
                                        usage_partial: None,
                                        finish: None,
                                        first_token_ms,
                                    };
                                }
                            }

                            if let Some(reason) = choice.finish_reason {
                                finish_final = Some(map_finish_reason(Some(reason)));
                            }
                        }

                        data_buf.clear();
                    }
                }

                if done {
                    break;
                }
            }

            for state in tool_states.values() {
                if let Some(proposal) = state.proposal() {
                    let first_token_ms =
                        maybe_record_first_token(&mut first_token_emitted, started_at);
                    yield ChatDelta {
                        text_delta: None,
                        tool_call_delta: Some(proposal),
                        usage_partial: None,
                        finish: None,
                        first_token_ms,
                    };
                }
            }

            if matches!(
                request
                    .response_format
                    .as_ref()
                    .map(|f| &f.kind),
                Some(ResponseKind::Json | ResponseKind::JsonSchema)
            ) && !aggregated_text.is_empty()
            {
                enforce_json(&aggregated_text, &policy)?;
            }

            let usage = usage_final.unwrap_or(Usage {
                input_tokens: 0,
                output_tokens: 0,
                cached_tokens: None,
                image_units: None,
                audio_seconds: None,
                requests: 1,
            });
            let finish = finish_final.unwrap_or(FinishReason::Stop);
            yield ChatDelta {
                text_delta: None,
                tool_call_delta: None,
                usage_partial: Some(usage),
                finish: Some(finish),
                first_token_ms: None,
            };
        };

        Ok(Box::pin(stream))
    }
}

impl OpenAiChatModel {
    async fn execute(
        &self,
        payload: ChatCompletionRequest<'_>,
        idempotency_key: Option<&str>,
    ) -> Result<ChatCompletionResponse, LlmError> {
        let _permit = self
            .shared
            .limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| LlmError::unknown(&format!("openai limiter closed: {err}")))?;

        let mut request = self
            .shared
            .client
            .post(self.shared.chat_url.clone())
            .json(&payload);

        if let Some(key) = idempotency_key {
            if let Ok(value) = HeaderValue::from_str(key) {
                request = request.header(HeaderName::from_static("idempotency-key"), value);
            }
        }

        let response = request.send().await.map_err(|err| {
            LlmError::provider_unavailable(&format!("openai request error: {err}"))
        })?;

        response
            .json::<ChatCompletionResponse>()
            .await
            .map_err(|err| {
                LlmError::provider_unavailable(&format!("openai response decode: {err}"))
            })
    }
}

fn aggregate_text(message: &Message) -> String {
    message
        .segments
        .iter()
        .filter_map(|segment| match segment {
            ContentSegment::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
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

fn build_request<'a>(
    model: &'a str,
    req: &'a ChatRequest,
    stream: bool,
) -> Result<ChatCompletionRequest<'a>, LlmError> {
    if req.allow_sensitive {
        return Err(LlmError::unknown(
            "OpenAI provider does not accept allow_sensitive",
        ));
    }

    let messages = req
        .messages
        .iter()
        .map(to_outbound_message)
        .collect::<Result<Vec<_>, _>>()?;

    let response_format = req
        .response_format
        .as_ref()
        .map(to_response_format)
        .transpose()?;

    let logit_bias = if req.logit_bias.is_empty() {
        None
    } else {
        let mut mapped = HashMap::new();
        for (token, value) in req.logit_bias.iter() {
            let bias = value.as_f64().ok_or_else(|| {
                LlmError::schema(&format!("non-numeric logit bias for token {token}"))
            })?;
            mapped.insert(token.clone(), bias as f32);
        }
        Some(mapped)
    };

    let tools = if req.tool_specs.is_empty() {
        None
    } else {
        Some(
            req.tool_specs
                .iter()
                .map(to_outbound_tool)
                .collect::<Result<Vec<_>, _>>()?,
        )
    };

    Ok(ChatCompletionRequest {
        model,
        messages,
        temperature: req.temperature,
        top_p: req.top_p,
        max_tokens: req.max_tokens,
        stop: if req.stop.is_empty() {
            None
        } else {
            Some(req.stop.clone())
        },
        logit_bias,
        presence_penalty: req.presence_penalty,
        frequency_penalty: req.frequency_penalty,
        response_format,
        tools,
        stream,
        stream_options: if stream {
            Some(StreamOptions {
                include_usage: true,
            })
        } else {
            None
        },
    })
}

fn to_outbound_message(message: &Message) -> Result<OutboundMessage, LlmError> {
    let role = match message.role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    };

    let mut content = String::new();
    for segment in &message.segments {
        match segment {
            ContentSegment::Text { text } => content.push_str(text),
            _ => {
                return Err(LlmError::schema(
                    "OpenAI provider currently supports text segments only",
                ))
            }
        }
    }

    let tool_calls = if message.tool_calls.is_empty() {
        None
    } else {
        let calls = message
            .tool_calls
            .iter()
            .map(|call| {
                let arguments = serde_json::to_string(&call.arguments).map_err(|err| {
                    LlmError::schema(&format!("tool call arguments encode: {err}"))
                })?;
                Ok(OutboundToolCall {
                    id: call.call_id.0.clone(),
                    kind: "function",
                    function: OutboundFunctionCall {
                        name: call.name.clone(),
                        arguments,
                    },
                })
            })
            .collect::<Result<Vec<_>, LlmError>>()?;
        Some(calls)
    };

    let tool_call_id = if role == "tool" {
        message.tool_calls.get(0).map(|call| call.call_id.0.clone())
    } else {
        None
    };

    Ok(OutboundMessage {
        role,
        content: Some(content),
        tool_calls,
        tool_call_id,
    })
}

fn to_outbound_tool(spec: &ToolSpec) -> Result<OutboundTool, LlmError> {
    Ok(OutboundTool {
        kind: "function",
        function: OutboundFunction {
            name: spec.id.clone(),
            description: Some(spec.description.clone()),
            parameters: spec.input_schema.clone(),
        },
    })
}

fn to_response_format(format: &ResponseFormat) -> Result<ResponseFormatSpec, LlmError> {
    match format.kind {
        ResponseKind::Text => Ok(ResponseFormatSpec {
            format_type: "text",
            json_schema: None,
            strict: None,
        }),
        ResponseKind::Json => Ok(ResponseFormatSpec {
            format_type: "json_object",
            json_schema: None,
            strict: Some(format.strict),
        }),
        ResponseKind::JsonSchema => {
            let schema_value = match &format.json_schema {
                Some(value) => serde_json::to_value(value)
                    .map_err(|err| LlmError::schema(&format!("response schema encode: {err}")))?,
                None => {
                    return Err(LlmError::schema(
                        "json_schema response format requires json_schema payload",
                    ))
                }
            };
            Ok(ResponseFormatSpec {
                format_type: "json_schema",
                json_schema: Some(JsonSchemaSpec {
                    name: "response",
                    schema: schema_value,
                }),
                strict: Some(format.strict),
            })
        }
    }
}

fn build_chat_response(
    request: &ChatRequest,
    enforce: &StructOutPolicy,
    response: ChatCompletionResponse,
) -> Result<ChatResponse, LlmError> {
    let mut choices = response.choices;
    if choices.is_empty() {
        return Err(LlmError::provider_unavailable("openai returned no choices"));
    }

    let choice = choices.remove(0);
    let finish_reason_meta = choice.finish_reason.clone();
    let finish = map_finish_reason(choice.finish_reason.clone());
    let message = convert_inbound_message(choice.message)?;
    let text = aggregate_text(&message);

    if matches!(
        request.response_format.as_ref().map(|f| &f.kind),
        Some(ResponseKind::Json | ResponseKind::JsonSchema)
    ) {
        enforce_json(&text, enforce)?;
    }

    let usage = response
        .usage
        .map(|u| Usage {
            input_tokens: u.prompt_tokens.unwrap_or_default(),
            output_tokens: u.completion_tokens.unwrap_or_default(),
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

    let provider_meta = json!({
        "provider": "openai",
        "response_id": response.id,
        "response_model": response.model,
        "finish_reason": finish_reason_meta,
        "system_fingerprint": response.system_fingerprint,
    });

    Ok(ChatResponse {
        model_id: request.model_id.clone(),
        message,
        usage,
        cost: None,
        finish,
        provider_meta,
    })
}

fn convert_inbound_message(message: InboundMessage) -> Result<Message, LlmError> {
    let role = match message.role.as_str() {
        "system" => Role::System,
        "user" => Role::User,
        "assistant" => Role::Assistant,
        "tool" => Role::Tool,
        other => {
            return Err(LlmError::unknown(&format!(
                "unsupported openai role '{other}'"
            )))
        }
    };

    let mut segments = Vec::new();
    if let Some(content) = message.content {
        match content {
            InboundContent::Text(text) => segments.push(ContentSegment::Text { text }),
            InboundContent::Parts(parts) => {
                for part in parts {
                    if part.kind == "text" {
                        if let Some(text) = part.text {
                            segments.push(ContentSegment::Text { text });
                        }
                    }
                }
            }
        }
    }

    let mut tool_calls = Vec::new();
    for call in message.tool_calls {
        if call.kind != "function" {
            continue;
        }
        let arguments = serde_json::from_str::<Value>(&call.function.arguments)
            .map_err(|err| LlmError::schema(&format!("tool call arguments decode: {err}")))?;
        tool_calls.push(ToolCallProposal {
            name: call.function.name,
            call_id: Id(call.id),
            arguments,
        });
    }

    Ok(Message {
        role,
        segments,
        tool_calls,
    })
}

fn map_finish_reason(reason: Option<String>) -> FinishReason {
    match reason.as_deref() {
        Some("stop") => FinishReason::Stop,
        Some("length") => FinishReason::Length,
        Some("tool_calls") => FinishReason::Tool,
        Some("content_filter") => FinishReason::Safety,
        Some(other) => FinishReason::Other(other.to_string()),
        None => FinishReason::Stop,
    }
}

fn map_http_error(status: StatusCode, body: &str) -> LlmError {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            LlmError::provider_unavailable(&format!("openai auth failed: {body}"))
        }
        StatusCode::TOO_MANY_REQUESTS => {
            LlmError::provider_unavailable(&format!("openai rate limited request: {body}"))
        }
        StatusCode::BAD_REQUEST => LlmError::schema(&format!("openai rejected request: {body}")),
        _ => LlmError::provider_unavailable(&format!(
            "openai returned {}: {}",
            status.as_u16(),
            body
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;
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

    fn sample_response() -> JsonValue {
        json!({
            "id": "chatcmpl-1",
            "model": "gpt-4o-mini",
            "choices": [{
                "index": 0,
                "finish_reason": "stop",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "hello there"}
                    ]
                }
            }],
            "usage": {
                "prompt_tokens": 12,
                "completion_tokens": 6,
                "total_tokens": 18
            }
        })
    }

    #[tokio::test]
    async fn chat_happy_path() {
        let server = MockServer::start().await;

        let response = ResponseTemplate::new(200).set_body_json(sample_response());
        Mock::given(method("POST"))
            .and(path(format!("/{CHAT_COMPLETIONS_PATH}")))
            .and(header("authorization", "Bearer test-key"))
            .respond_with(response)
            .mount(&server)
            .await;

        let cfg = OpenAiConfig::new("test-key")
            .unwrap()
            .with_base_url(&server.uri())
            .unwrap()
            .with_alias("gpt-4o", "gpt-4o-mini");
        let factory = OpenAiProviderFactory::new(cfg).unwrap();
        let model = factory
            .create_chat(
                "gpt-4o",
                &ProviderCfg {
                    name: "openai".into(),
                },
            )
            .unwrap();

        let req = sample_request("openai:gpt-4o");
        let response = model.chat(req, &StructOutPolicy::Off).await.unwrap();

        assert_eq!(aggregate_text(&response.message), "hello there");
        assert_eq!(response.usage.input_tokens, 12);
        assert_eq!(response.usage.output_tokens, 6);
        assert!(response.provider_meta.get("response_id").is_some());
    }

    #[tokio::test]
    async fn chat_stream_sse() {
        let server = MockServer::start().await;

        let body = concat!(
            "data: {\"id\":\"chatcmpl-1\",\"model\":\"gpt-4o-mini\",\"choices\":[{\"delta\":{\"content\":\"hello \"}}]}\n\n",
            "data: {\"id\":\"chatcmpl-1\",\"model\":\"gpt-4o-mini\",\"choices\":[{\"delta\":{\"content\":\"there\"}}]}\n\n",
            "data: {\"id\":\"chatcmpl-1\",\"model\":\"gpt-4o-mini\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":6}}\n\n",
            "data: [DONE]\n\n"
        );

        let response = ResponseTemplate::new(200)
            .set_body_string(body)
            .insert_header("content-type", "text/event-stream");

        Mock::given(method("POST"))
            .and(path(format!("/{CHAT_COMPLETIONS_PATH}")))
            .and(body_partial_json(json!({"stream": true})))
            .respond_with(response)
            .expect(1)
            .mount(&server)
            .await;

        let cfg = OpenAiConfig::new("key")
            .unwrap()
            .with_base_url(&server.uri())
            .unwrap();
        let factory = OpenAiProviderFactory::new(cfg).unwrap();
        let model = factory
            .create_chat(
                "gpt-4o-mini",
                &ProviderCfg {
                    name: "openai".into(),
                },
            )
            .unwrap();

        let req = sample_request("openai:gpt-4o-mini");
        let mut stream = model.chat_stream(req, &StructOutPolicy::Off).await.unwrap();
        let mut collected = Vec::new();
        while let Some(item) = stream.next().await {
            collected.push(item.unwrap());
        }

        let text: String = collected
            .iter()
            .filter_map(|delta| delta.text_delta.clone())
            .collect();
        assert_eq!(text, "hello there");

        let final_delta = collected
            .iter()
            .find(|delta| delta.finish.is_some())
            .expect("final delta");
        assert_eq!(final_delta.finish, Some(FinishReason::Stop));
        let usage = final_delta.usage_partial.as_ref().expect("usage");
        assert_eq!(usage.input_tokens, 12);
        assert_eq!(usage.output_tokens, 6);
    }
}
