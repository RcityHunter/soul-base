use serde::{Deserialize, Serialize};
use soulbase_types::prelude::TenantId;

use crate::model::Entity;

#[cfg(feature = "surreal")]
use crate::surreal::schema::TABLE_TIMELINE_EVENT;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub id: String,
    pub tenant: TenantId,
    pub journey_id: String,
    #[serde(default)]
    pub thread_id: Option<String>,
    pub event_id: String,
    pub role: String,
    #[serde(default)]
    pub stage: Option<String>,
    pub occurred_at: i64,
    pub ingested_at: i64,
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
    #[serde(default)]
    pub metrics: Option<serde_json::Value>,
    #[serde(default)]
    pub vector_ref: Option<String>,
}

impl Entity for TimelineEvent {
    #[cfg(feature = "surreal")]
    const TABLE: &'static str = TABLE_TIMELINE_EVENT;
    #[cfg(not(feature = "surreal"))]
    const TABLE: &'static str = "timeline_event";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}
