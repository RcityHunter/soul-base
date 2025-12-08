//! 演化事件模型

use serde::{Deserialize, Serialize};
use soulbase_types::prelude::TenantId;

use crate::Entity;

/// 群体演化事件 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvolutionGroupEvent {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub event_id: String,
    pub event_type: String,
    pub description: String,
    #[serde(default)]
    pub participants: Vec<String>,
    #[serde(default = "default_impact_scope")]
    pub impact_scope: String,
    #[serde(default = "default_magnitude")]
    pub magnitude: f64,
    pub occurred_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

fn default_impact_scope() -> String {
    "local".to_string()
}

fn default_magnitude() -> f64 {
    0.5
}

impl Entity for EvolutionGroupEvent {
    const TABLE: &'static str = "evolution_group_event";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

/// AI 个体演化事件 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvolutionAiEvent {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub record_id: String,
    pub ai_id: String,
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_state: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_state: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<String>,
    pub occurred_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl Entity for EvolutionAiEvent {
    const TABLE: &'static str = "evolution_ai";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

/// 关系演化事件 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvolutionRelationship {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub record_id: String,
    pub source_id: String,
    pub target_id: String,
    pub relationship_type: String,
    pub event_type: String,
    #[serde(default)]
    pub strength_before: f64,
    #[serde(default)]
    pub strength_after: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<String>,
    pub occurred_at_ms: i64,
}

impl Entity for EvolutionRelationship {
    const TABLE: &'static str = "evolution_relationship";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}
