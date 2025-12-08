//! 向量嵌入模型

use serde::{Deserialize, Serialize};
use soulbase_types::prelude::TenantId;

use crate::Entity;

/// 向量嵌入块 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmbeddingChunk {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub chunk_id: String,
    pub content: String,
    pub embedding: Vec<f32>,
    pub source_type: String,
    pub source_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub journey_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub created_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl Entity for EmbeddingChunk {
    const TABLE: &'static str = "embedding_chunk";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

/// 向量搜索查询参数
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorSearchQuery {
    pub query_embedding: Vec<f32>,
    pub top_k: u16,
    pub threshold: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub journey_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

/// 向量搜索结果
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorSearchResult {
    pub chunk_id: String,
    pub content: Option<String>,
    pub source_type: String,
    pub source_id: String,
    pub journey_id: Option<String>,
    pub score: f64,
    pub metadata: Option<serde_json::Value>,
}
