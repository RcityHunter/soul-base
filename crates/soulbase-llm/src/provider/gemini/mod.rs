use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use reqwest::{
    header::{HeaderMap, HeaderValue, CONTENT_TYPE},
    Client, StatusCode, Url,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    chat::{ChatModel, ChatRequest, ChatResponse, ResponseKind, ToolSpec},
    errors::LlmError,
    jsonsafe::{enforce_json, StructOutPolicy},
    model::{ContentSegment, FinishReason, Message, Role, ToolCallProposal, Usage},
    provider::{ChatBoxStream, DynChatModel, ProviderCaps, ProviderCfg, ProviderFactory, Registry},
};
use soulbase_types::prelude::Id;

const DEFAULT_BASE_URL: &str = "https://generativelanguage.googleapis.com/";
const DEFAULT_VERSION: &str = "v1beta";

#[derive(Clone, Debug)]
pub struct GeminiConfig {
    pub api_key: String,
    pub base_url: Url,
    pub api_version: String,
    pub model_aliases: HashMap<String, String>,
    pub request_timeout: Duration,
    pub max_concurrent_requests: usize,
}

impl GeminiConfig {
    pub fn new(api_key: impl Into<String>) -> Result<Self, LlmError> {
        let base_url = Url::parse(DEFAULT_BASE_URL)
            .map_err(|err| LlmError::unknown(&format!("gemini base url parse failed: {err}")))?;
        Ok(Self {
            api_key: api_key.into(),
            base_url,
            api_version: DEFAULT_VERSION.to_string(),
            model_aliases: HashMap::new(),
            request_timeout: Duration::from_secs(30),
            max_concurrent_requests: 8,
        })
    }

    pub fn with_base_url(mut self, base: impl AsRef<str>) -> Result<Self, LlmError> {
        self.base_url = Url::parse(base.as_ref())
            .map_err(|err| LlmError::unknown(&format!("gemini base url parse failed: {err}")))?;
        if !self.base_url.path().ends_with('/') {
            self.base_url
                .set_path(&format!("{}/", self.base_url.path().trim_end_matches('/')));
        }
        Ok(self)
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.api_version = version.into();
        self
    }

    pub fn with_alias(mut self, alias: impl Into<String>, target: impl Into<String>) -> Self {
        self.model_aliases.insert(alias.into(), target.into());
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
}

struct GeminiShared {
    client: Client,
    config: GeminiConfig,
    limiter: Arc<Semaphore>,
}

impl GeminiShared {
    fn resolve_model(&self, requested: &str) -> String {
        self.config
            .model_aliases
            .get(requested)
            .cloned()
            .unwrap_or_else(|| requested.to_string())
    }

    fn endpoint(&self, model: &str, path: &str) -> Result<Url, LlmError> {
        let version = self.config.api_version.trim_end_matches('/');
        let joined = format!("{version}/models/{model}:{path}");
        let mut base =
            self.config.base_url.join(&joined).map_err(|err| {
                LlmError::unknown(&format!("gemini endpoint build failed: {err}"))
            })?;
        base.query_pairs_mut()
            .append_pair("key", &self.config.api_key);
        Ok(base)
    }

    async fn acquire(&self) -> Result<OwnedSemaphorePermit, LlmError> {
        self.limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| LlmError::unknown(&format!("gemini limiter closed: {err}")))
    }
}

pub struct GeminiProviderFactory {
    shared: Arc<GeminiShared>,
}

impl GeminiProviderFactory {
    pub fn new(config: GeminiConfig) -> Result<Self, LlmError> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let client = Client::builder()
            .default_headers(headers)
            .timeout(config.request_timeout)
            .build()
            .map_err(|err| LlmError::unknown(&format!("gemini client build failed: {err}")))?;

        let max_concurrency = config.max_concurrent_requests;
        Ok(Self {
            shared: Arc::new(GeminiShared {
                client,
                limiter: Arc::new(Semaphore::new(max_concurrency)),
                config,
            }),
        })
    }

    pub fn install(self, registry: &mut Registry) {
        registry.register(Box::new(self));
    }
}

#[async_trait]
impl ProviderFactory for GeminiProviderFactory {
    fn name(&self) -> &'static str {
        "gemini"
    }

    fn caps(&self) -> ProviderCaps {
        ProviderCaps {
            chat: true,
            stream: false,
            tools: true,
            embeddings: false,
            rerank: false,
            multimodal: false,
            json_schema: true,
        }
    }

    fn create_chat(&self, model: &str, _cfg: &ProviderCfg) -> Option<Box<DynChatModel>> {
        let resolved = self.shared.resolve_model(model);
        Some(Box::new(GeminiChatModel {
            model: resolved,
            shared: self.shared.clone(),
        }))
    }
}

struct GeminiChatModel {
    model: String,
    shared: Arc<GeminiShared>,
}

#[async_trait]
impl ChatModel for GeminiChatModel {
    type Stream = ChatBoxStream;

    async fn chat(
        &self,
        req: ChatRequest,
        enforce: &StructOutPolicy,
    ) -> Result<ChatResponse, LlmError> {
        let request = build_generate_request(&req)?;
        let _permit = self.shared.acquire().await?;
        let endpoint = self
            .shared
            .endpoint(&self.model, "generateContent")
            .map_err(|err| LlmError::unknown(&format!("gemini endpoint error: {err:?}")))?;

        let response = self
            .shared
            .client
            .post(endpoint)
            .json(&request)
            .send()
            .await
            .map_err(|err| {
                LlmError::provider_unavailable(&format!("gemini request error: {err}"))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".into());
            return Err(map_http_error(status, &body));
        }

        let payload = response
            .json::<GenerateContentResponse>()
            .await
            .map_err(|err| {
                LlmError::provider_unavailable(&format!("gemini response decode: {err}"))
            })?;

        build_chat_response(&req, enforce, payload)
    }

    async fn chat_stream(
        &self,
        _req: ChatRequest,
        _enforce: &StructOutPolicy,
    ) -> Result<Self::Stream, LlmError> {
        Err(LlmError::provider_unavailable(
            "gemini streaming not implemented",
        ))
    }
}

#[derive(Serialize)]
struct GenerateContentRequest {
    contents: Vec<GenerateContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system_instruction: Option<GenerateContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<GeminiTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_config: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    safety_settings: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation_config: Option<Value>,
}

#[derive(Serialize)]
struct GenerateContent {
    role: String,
    parts: Vec<ContentPart>,
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct ContentPart {
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    function_call: Option<FunctionCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    function_response: Option<FunctionResponse>,
}

#[derive(Serialize)]
struct FunctionCall {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<Value>,
}

#[derive(Serialize)]
struct FunctionResponse {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    response: Option<Value>,
}

#[derive(Serialize)]
struct GeminiTool {
    #[serde(rename = "functionDeclarations")]
    function_declarations: Vec<GeminiFunctionDecl>,
}

#[derive(Serialize)]
struct GeminiFunctionDecl {
    name: String,
    description: Option<String>,
    #[serde(rename = "parameters")]
    parameters: Value,
}

#[derive(Deserialize)]
struct GenerateContentResponse {
    #[serde(default)]
    candidates: Vec<GenerateCandidate>,
    #[serde(rename = "usageMetadata")]
    usage_metadata: Option<UsageMetadata>,
}

#[derive(Deserialize)]
struct GenerateCandidate {
    content: Option<GenerateContentBlock>,
    #[serde(rename = "finishReason")]
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct GenerateContentBlock {
    parts: Vec<GeneratePart>,
}

#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct GeneratePart {
    text: Option<String>,
    function_call: Option<FunctionCallPayload>,
    function_response: Option<FunctionResponsePayload>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FunctionCallPayload {
    name: String,
    #[serde(default)]
    args: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FunctionResponsePayload {
    name: String,
    #[serde(default)]
    response: Value,
}

#[derive(Deserialize)]
struct UsageMetadata {
    #[serde(rename = "promptTokenCount")]
    prompt_token_count: Option<u32>,
    #[serde(rename = "candidatesTokenCount")]
    candidates_token_count: Option<u32>,
}

fn build_generate_request(req: &ChatRequest) -> Result<GenerateContentRequest, LlmError> {
    let mut system_segments = Vec::<String>::new();
    let mut contents = Vec::<GenerateContent>::new();

    for message in &req.messages {
        match message.role {
            Role::System => {
                let text = collect_text_segments(message)?;
                if !text.is_empty() {
                    system_segments.push(text);
                }
            }
            Role::User => contents.push(GenerateContent {
                role: "user".into(),
                parts: build_text_parts(message)?,
            }),
            Role::Assistant => contents.push(GenerateContent {
                role: "model".into(),
                parts: build_assistant_parts(message)?,
            }),
            Role::Tool => contents.push(GenerateContent {
                role: "user".into(),
                parts: build_tool_parts(message)?,
            }),
        }
    }

    if contents.is_empty() {
        return Err(LlmError::schema(
            "gemini request requires at least one content message",
        ));
    }

    let system_instruction = if system_segments.is_empty() {
        None
    } else {
        Some(GenerateContent {
            role: "system".into(),
            parts: system_segments
                .into_iter()
                .map(|text| ContentPart {
                    text: Some(text),
                    ..ContentPart::default()
                })
                .collect(),
        })
    };

    let tools = if req.tool_specs.is_empty() {
        None
    } else {
        let declarations = req
            .tool_specs
            .iter()
            .map(to_function_declaration)
            .collect::<Result<Vec<_>, LlmError>>()?;
        Some(vec![GeminiTool {
            function_declarations: declarations,
        }])
    };

    let response_mime = match req.response_format.as_ref().map(|fmt| &fmt.kind) {
        Some(ResponseKind::Json) | Some(ResponseKind::JsonSchema) => {
            Some(json!({ "responseMimeType": "application/json" }))
        }
        _ => None,
    };

    Ok(GenerateContentRequest {
        contents,
        system_instruction,
        tools,
        tool_config: None,
        safety_settings: None,
        generation_config: response_mime,
    })
}

fn build_chat_response(
    request: &ChatRequest,
    enforce: &StructOutPolicy,
    payload: GenerateContentResponse,
) -> Result<ChatResponse, LlmError> {
    let mut candidates = payload.candidates;
    let candidate = candidates
        .pop()
        .ok_or_else(|| LlmError::provider_unavailable("gemini returned no candidates"))?;

    let content = candidate
        .content
        .ok_or_else(|| LlmError::provider_unavailable("gemini candidate missing content"))?;

    let mut segments = Vec::new();
    let mut tool_calls = Vec::new();
    let mut aggregated = String::new();

    for part in content.parts {
        if let Some(text) = part.text {
            aggregated.push_str(&text);
            segments.push(ContentSegment::Text { text });
        }

        if let Some(function_call) = part.function_call {
            tool_calls.push(ToolCallProposal {
                name: function_call.name,
                call_id: Id::new_random(),
                arguments: function_call.args,
            });
        }

        if part.function_response.is_some() {
            // Currently ignored, but we keep the branch for completeness.
        }
    }

    if let Some(fmt) = request.response_format.as_ref() {
        match fmt.kind {
            ResponseKind::Json => {
                let _ = enforce_json(&aggregated, enforce)?;
            }
            ResponseKind::JsonSchema => {
                let value = enforce_json(&aggregated, enforce)?;
                fmt.validate_schema(&value)?;
            }
            ResponseKind::Text => {}
        }
    }

    if segments.is_empty() && tool_calls.is_empty() {
        segments.push(ContentSegment::Text {
            text: aggregated.clone(),
        });
    }

    let usage = payload
        .usage_metadata
        .map(|usage| Usage {
            input_tokens: usage.prompt_token_count.unwrap_or_default(),
            output_tokens: usage.candidates_token_count.unwrap_or_default(),
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

    let finish = map_finish_reason(candidate.finish_reason.as_deref());
    let provider_meta = json!({
        "provider": "gemini",
        "finish_reason": candidate.finish_reason,
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
    let mut out = String::new();
    for segment in &message.segments {
        match segment {
            ContentSegment::Text { text } => out.push_str(text),
            _ => {
                return Err(LlmError::schema(
                    "gemini only supports text segments for system prompts",
                ))
            }
        }
    }
    Ok(out)
}

fn build_text_parts(message: &Message) -> Result<Vec<ContentPart>, LlmError> {
    let mut parts = Vec::new();
    for segment in &message.segments {
        match segment {
            ContentSegment::Text { text } => parts.push(ContentPart {
                text: Some(text.clone()),
                ..ContentPart::default()
            }),
            _ => {
                return Err(LlmError::schema(
                    "gemini currently supports text segments only",
                ))
            }
        }
    }
    if parts.is_empty() {
        return Err(LlmError::schema(
            "gemini message requires at least one text segment",
        ));
    }
    Ok(parts)
}

fn build_assistant_parts(message: &Message) -> Result<Vec<ContentPart>, LlmError> {
    let mut parts = Vec::new();
    for segment in &message.segments {
        match segment {
            ContentSegment::Text { text } => parts.push(ContentPart {
                text: Some(text.clone()),
                ..ContentPart::default()
            }),
            _ => {
                return Err(LlmError::schema(
                    "gemini assistant playback supports text segments only",
                ))
            }
        }
    }
    for call in &message.tool_calls {
        parts.push(ContentPart {
            function_call: Some(FunctionCall {
                name: call.name.clone(),
                args: Some(call.arguments.clone()),
            }),
            ..ContentPart::default()
        });
    }
    if parts.is_empty() {
        parts.push(ContentPart {
            text: Some(String::new()),
            ..ContentPart::default()
        });
    }
    Ok(parts)
}

fn build_tool_parts(message: &Message) -> Result<Vec<ContentPart>, LlmError> {
    let payload = message
        .segments
        .iter()
        .find_map(|segment| match segment {
            ContentSegment::Text { text } => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default();

    let call = message.tool_calls.get(0).ok_or_else(|| {
        LlmError::schema("gemini tool result message requires tool call metadata")
    })?;

    let response_json = if payload.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&payload).unwrap_or(Value::String(payload))
    };

    Ok(vec![ContentPart {
        function_response: Some(FunctionResponse {
            name: call.name.clone(),
            response: Some(response_json),
        }),
        ..ContentPart::default()
    }])
}

fn to_function_declaration(spec: &ToolSpec) -> Result<GeminiFunctionDecl, LlmError> {
    Ok(GeminiFunctionDecl {
        name: spec.id.clone(),
        description: Some(spec.description.clone()),
        parameters: spec.input_schema.clone(),
    })
}

fn map_finish_reason(reason: Option<&str>) -> FinishReason {
    match reason {
        Some("FINISH_REASON_UNSPECIFIED") | Some("STOP") => FinishReason::Stop,
        Some("MAX_TOKENS") => FinishReason::Length,
        Some("SAFETY") => FinishReason::Safety,
        Some("RECITATION") => FinishReason::Other("recitation".into()),
        Some("OTHER") => FinishReason::Other("other".into()),
        _ => FinishReason::Stop,
    }
}

fn build_http_error(status: StatusCode, body: &str) -> LlmError {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            LlmError::provider_unavailable(&format!("gemini auth failed: {body}"))
        }
        StatusCode::TOO_MANY_REQUESTS => {
            LlmError::provider_unavailable(&format!("gemini rate limited request: {body}"))
        }
        StatusCode::BAD_REQUEST => LlmError::schema(&format!("gemini rejected request: {body}")),
        _ => LlmError::provider_unavailable(&format!(
            "gemini returned {}: {}",
            status.as_u16(),
            body
        )),
    }
}

fn map_http_error(status: StatusCode, body: &str) -> LlmError {
    build_http_error(status, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_partial_json, header, method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_request(model_id: &str) -> ChatRequest {
        ChatRequest {
            model_id: model_id.to_string(),
            messages: vec![
                Message {
                    role: Role::System,
                    segments: vec![ContentSegment::Text {
                        text: "You are a helpful assistant.".into(),
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
            temperature: None,
            top_p: None,
            max_tokens: None,
            stop: vec![],
            seed: None,
            frequency_penalty: None,
            presence_penalty: None,
            logit_bias: serde_json::Map::new(),
            response_format: None,
            idempotency_key: None,
            cache_hint: None,
            allow_sensitive: false,
            metadata: Value::Null,
        }
    }

    fn sample_response() -> Value {
        json!({
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{
                        "text": "Hello there!"
                    }]
                },
                "finishReason": "STOP"
            }],
            "usageMetadata": {
                "promptTokenCount": 8,
                "candidatesTokenCount": 6
            }
        })
    }

    #[tokio::test]
    async fn chat_happy_path() {
        let server = MockServer::start().await;

        let response = ResponseTemplate::new(200).set_body_json(sample_response());
        Mock::given(method("POST"))
            .and(path_regex(
                r"/v1beta/models/gemini-1\.5-flash:generateContent.*",
            ))
            .and(header(CONTENT_TYPE.as_str(), "application/json"))
            .and(body_partial_json(json!({"contents":[{"role":"user"}]})))
            .respond_with(response)
            .mount(&server)
            .await;

        let cfg = GeminiConfig::new("test-key")
            .unwrap()
            .with_base_url(server.uri())
            .unwrap()
            .with_alias("flash", "gemini-1.5-flash");

        let factory = GeminiProviderFactory::new(cfg).unwrap();
        let mut registry = Registry::new();
        factory.install(&mut registry);

        let model = registry.chat("gemini:flash").expect("gemini provider");
        let request = sample_request("gemini:flash");
        let response = model
            .chat(request, &StructOutPolicy::StrictReject)
            .await
            .expect("chat succeeds");

        assert_eq!(response.message.segments.len(), 1);
        assert_eq!(
            response.message.segments[0],
            ContentSegment::Text {
                text: "Hello there!".into()
            }
        );
        assert_eq!(response.usage.input_tokens, 8);
        assert_eq!(response.finish, FinishReason::Stop);
    }
}
