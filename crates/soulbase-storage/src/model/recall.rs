use serde::{Deserialize, Serialize};
use soulbase_types::prelude::TenantId;

use crate::model::Entity;

#[cfg(feature = "surreal")]
use crate::surreal::schema::TABLE_RECALL_CHUNK;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecallChunk {
    pub id: String,
    pub tenant: TenantId,
    pub chunk_id: String,
    pub journey_id: String,
    #[serde(default)]
    pub source_event_id: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    pub content: String,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    pub version: i32,
    pub created_at: i64,
    #[serde(default)]
    pub expires_at: Option<i64>,
}

impl Entity for RecallChunk {
    #[cfg(feature = "surreal")]
    const TABLE: &'static str = TABLE_RECALL_CHUNK;
    #[cfg(not(feature = "surreal"))]
    const TABLE: &'static str = "recall_chunk";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}
