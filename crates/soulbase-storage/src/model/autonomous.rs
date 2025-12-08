//! 自主延续会话模型

use serde::{Deserialize, Serialize};
use soulbase_types::prelude::TenantId;

use crate::Entity;

/// 自主延续会话 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AutonomousSession {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub session_id: String,
    #[serde(default = "default_status")]
    pub status: String,
    #[serde(default = "default_max_cycles")]
    pub max_cycles: i32,
    #[serde(default = "default_timeout")]
    pub timeout_ms: i64,
    #[serde(default)]
    pub cycles_executed: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_reason: Option<String>,
    #[serde(default)]
    pub scenario_stack: Vec<serde_json::Value>,
    #[serde(default)]
    pub total_cost_usd: f64,
    #[serde(default)]
    pub total_tokens: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

fn default_status() -> String {
    "pending".to_string()
}

fn default_max_cycles() -> i32 {
    100
}

fn default_timeout() -> i64 {
    3600000
}

impl Entity for AutonomousSession {
    const TABLE: &'static str = "autonomous_session";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}
