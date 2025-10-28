use serde::{Deserialize, Serialize};
use soulbase_types::prelude::TenantId;

use crate::model::Entity;

#[cfg(feature = "surreal")]
use crate::surreal::schema::TABLE_AWARENESS_EVENT;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AwarenessEvent {
    pub id: String,
    pub tenant: TenantId,
    pub journey_id: String,
    pub kind: String,
    pub state: String,
    #[serde(default)]
    pub score: Option<f64>,
    #[serde(default)]
    pub traits: Option<Vec<String>>,
    #[serde(default)]
    pub evidence: Option<serde_json::Value>,
    pub triggered_at: i64,
    #[serde(default)]
    pub resolved_at: Option<i64>,
    pub updated_at: i64,
}

impl Entity for AwarenessEvent {
    #[cfg(feature = "surreal")]
    const TABLE: &'static str = TABLE_AWARENESS_EVENT;
    #[cfg(not(feature = "surreal"))]
    const TABLE: &'static str = "awareness_event";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}
