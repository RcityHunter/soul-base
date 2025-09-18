use crate::errors::LlmError;
use crate::jsonsafe::StructOutPolicy;
use crate::model::*;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use soulbase_tools::prelude::{SafetyClass, SideEffect, ToolManifest};

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCapabilityBrief {
    pub domain: String,
    pub action: String,
    pub resource: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolSpec {
    pub id: String,
    pub version: String,
    pub display_name: String,
    pub description: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub input_schema: serde_json::Value,
    #[serde(default)]
    pub capabilities: Vec<ToolCapabilityBrief>,
    #[serde(default)]
    pub safety_class: Option<SafetyClass>,
    #[serde(default)]
    pub side_effect: Option<SideEffect>,
    #[serde(default)]
    pub provider_id: Option<String>,
}

impl ToolSpec {
    pub fn from_manifest(manifest: &ToolManifest) -> Self {
        let input_schema =
            serde_json::to_value(&manifest.input_schema).unwrap_or_else(|_| serde_json::json!({}));
        let capabilities = manifest
            .capabilities
            .iter()
            .map(|cap| ToolCapabilityBrief {
                domain: cap.domain.clone(),
                action: cap.action.clone(),
                resource: cap.resource.clone(),
            })
            .collect();

        Self {
            id: manifest.id.0.clone(),
            version: manifest.version.clone(),
            display_name: manifest.display_name.clone(),
            description: manifest.description.clone(),
            tags: manifest.tags.clone(),
            input_schema,
            capabilities,
            safety_class: Some(manifest.safety_class),
            side_effect: Some(manifest.side_effect),
            provider_id: None,
        }
    }
}

impl From<&ToolManifest> for ToolSpec {
    fn from(manifest: &ToolManifest) -> Self {
        ToolSpec::from_manifest(manifest)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use soulbase_tools::manifest::SchemaDoc;
    use soulbase_tools::prelude::{
        CapabilityDecl, ConcurrencyKind, ConsentPolicy, IdempoKind, Limits, SafetyClass,
        SideEffect, ToolId,
    };

    fn schema_from_json(value: serde_json::Value) -> SchemaDoc {
        serde_json::from_value(value).expect("schema")
    }

    fn sample_manifest() -> ToolManifest {
        ToolManifest {
            id: ToolId("demo.tool".into()),
            version: "1.0.0".into(),
            display_name: "Demo Tool".into(),
            description: "Sample tool for testing".into(),
            tags: vec!["demo".into()],
            input_schema: schema_from_json(json!({ "type": "object" })),
            output_schema: schema_from_json(json!({ "type": "string" })),
            scopes: vec![],
            capabilities: vec![CapabilityDecl {
                domain: "net.http".into(),
                action: "get".into(),
                resource: "example.com".into(),
                attrs: json!({}),
            }],
            side_effect: SideEffect::Network,
            safety_class: SafetyClass::Medium,
            consent: ConsentPolicy {
                required: false,
                max_ttl_ms: Some(30_000),
            },
            limits: Limits {
                timeout_ms: 10_000,
                max_bytes_in: 128,
                max_bytes_out: 256,
                max_files: 0,
                max_depth: 1,
                max_concurrency: 1,
            },
            idempotency: IdempoKind::Keyed,
            concurrency: ConcurrencyKind::Parallel,
        }
    }

    #[test]
    fn tool_spec_from_manifest_is_compact() {
        let manifest = sample_manifest();
        let spec = ToolSpec::from_manifest(&manifest);
        assert_eq!(spec.id, "demo.tool");
        assert_eq!(spec.display_name, "Demo Tool");
        assert_eq!(spec.capabilities.len(), 1);
        let payload = serde_json::to_value(&spec).expect("serialize spec");
        assert!(payload.get("manifest").is_none());
        assert_eq!(payload.get("id").unwrap(), "demo.tool");
        assert_eq!(payload.get("input_schema").unwrap()["type"], "object");
    }
}
