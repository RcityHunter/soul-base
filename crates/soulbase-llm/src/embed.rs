use crate::errors::LlmError;
use crate::model::Usage;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmbedItem {
    pub id: String,
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmbedRequest {
    pub model_id: String,
    pub items: Vec<EmbedItem>,
    pub normalize: bool,
    pub pooling: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub enum VectorDType {
    F32,
}

#[derive(Clone, Debug, Serialize)]
pub struct EmbedResponse {
    pub dim: u32,
    pub dtype: VectorDType,
    pub vectors: Vec<Vec<f32>>,
    pub usage: Usage,
    #[serde(default)]
    pub cost: Option<crate::model::Cost>,
    #[serde(default)]
    pub provider_meta: serde_json::Value,
}

#[async_trait::async_trait]
pub trait EmbedModel: Send + Sync {
    async fn embed(&self, req: EmbedRequest) -> Result<EmbedResponse, LlmError>;
}
