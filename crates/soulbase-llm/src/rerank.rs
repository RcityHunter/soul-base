use crate::errors::LlmError;
use crate::model::Usage;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RerankRequest {
    pub model_id: String,
    pub query: String,
    pub candidates: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RerankResponse {
    pub scores: Vec<f32>,
    pub ordering: Vec<usize>,
    pub usage: Usage,
    #[serde(default)]
    pub cost: Option<crate::model::Cost>,
    #[serde(default)]
    pub provider_meta: serde_json::Value,
}

#[async_trait::async_trait]
pub trait RerankModel: Send + Sync {
    async fn rerank(&self, req: RerankRequest) -> Result<RerankResponse, LlmError>;
}
