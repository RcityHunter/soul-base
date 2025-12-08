//! DFR 决策指纹模型

use serde::{Deserialize, Serialize};
use soulbase_types::prelude::TenantId;

use crate::Entity;

/// DFR 决策指纹 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DfrFingerprint {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub fingerprint_id: String,
    pub decision_type: String,
    #[serde(default)]
    pub feature_vector: Vec<f32>,
    #[serde(default)]
    pub confidence: f32,
    pub created_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl Entity for DfrFingerprint {
    const TABLE: &'static str = "dfr_fingerprint";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

/// DFR 决策详情 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DfrDecision {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub decision_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint_id: Option<String>,
    pub decision_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_context: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    #[serde(default)]
    pub confidence: f32,
    #[serde(default)]
    pub latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    pub created_at_ms: i64,
}

impl Entity for DfrDecision {
    const TABLE: &'static str = "dfr_decision";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}
