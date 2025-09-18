use crate::errors::LlmError;
use crate::jsonsafe::StructOutPolicy;
use crate::model::*;
use futures_core::Stream;
use serde::{Deserialize, Serialize};

#[cfg(feature = "schema_json")]
type MaybeSchema = schemars::schema::RootSchema;
#[cfg(not(feature = "schema_json"))]
type MaybeSchema = serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ResponseKind {
    Text,
    Json,
    JsonSchema,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponseFormat {
    pub kind: ResponseKind,
    #[serde(default)]
    pub json_schema: Option<MaybeSchema>,
    #[serde(default)]
    pub strict: bool,
}

/// A read-only tool description derived from a soulbase-tools manifest entry.
/// The manifest payload is sanitized upstream; this type simply relays the
/// subset required for proposal generation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolSpec {
    pub manifest: soulbase_tools::prelude::ToolManifest,
    #[serde(default)]
    pub provider_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatRequest {
    pub model_id: String,
    pub messages: Vec<Message>,
    #[serde(default)]
    pub tool_specs: Vec<ToolSpec>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub stop: Vec<String>,
    #[serde(default)]
    pub seed: Option<u64>,
    #[serde(default)]
    pub frequency_penalty: Option<f32>,
    #[serde(default)]
    pub presence_penalty: Option<f32>,
    #[serde(default)]
    pub logit_bias: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub response_format: Option<ResponseFormat>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub cache_hint: Option<String>,
    #[serde(default)]
    pub allow_sensitive: bool,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChatResponse {
    pub model_id: String,
    pub message: Message,
    pub usage: Usage,
    #[serde(default)]
    pub cost: Option<crate::model::Cost>,
    pub finish: FinishReason,
    #[serde(default)]
    pub provider_meta: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatDelta {
    #[serde(default)]
    pub text_delta: Option<String>,
    #[serde(default)]
    pub tool_call_delta: Option<ToolCallProposal>,
    #[serde(default)]
    pub usage_partial: Option<Usage>,
    #[serde(default)]
    pub finish: Option<FinishReason>,
    #[serde(default)]
    pub first_token_ms: Option<u32>,
}

#[async_trait::async_trait]
pub trait ChatModel: Send + Sync {
    type Stream: Stream<Item = Result<ChatDelta, LlmError>> + Unpin + Send + 'static;

    async fn chat(
        &self,
        req: ChatRequest,
        enforce: &StructOutPolicy,
    ) -> Result<ChatResponse, LlmError>;
    async fn chat_stream(
        &self,
        req: ChatRequest,
        enforce: &StructOutPolicy,
    ) -> Result<Self::Stream, LlmError>;
}
