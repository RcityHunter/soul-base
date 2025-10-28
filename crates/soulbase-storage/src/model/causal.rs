use serde::{Deserialize, Serialize};
use soulbase_types::prelude::TenantId;

use crate::model::Entity;

#[cfg(feature = "surreal")]
use crate::surreal::schema::TABLE_CAUSAL_EDGE;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CausalEdge {
    pub id: String,
    pub tenant: TenantId,
    pub edge_id: String,
    pub source_event_id: String,
    pub target_event_id: String,
    pub relation: String,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub evidence: Option<serde_json::Value>,
    pub created_at: i64,
}

impl Entity for CausalEdge {
    #[cfg(feature = "surreal")]
    const TABLE: &'static str = TABLE_CAUSAL_EDGE;
    #[cfg(not(feature = "surreal"))]
    const TABLE: &'static str = "causal_edge";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}
