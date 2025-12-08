//! 元认知分析模型

use serde::{Deserialize, Serialize};
use soulbase_types::prelude::TenantId;

use crate::Entity;

/// 元认知分析结果 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetacognitionAnalysis {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub analysis_id: String,
    pub mode: String,
    #[serde(default = "default_true")]
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default)]
    pub execution_time_ms: u64,
    pub created_at_ms: i64,
}

fn default_true() -> bool {
    true
}

impl Entity for MetacognitionAnalysis {
    const TABLE: &'static str = "metacognition_analysis";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

/// 元认知洞见 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetacognitionInsight {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub insight_id: String,
    pub analysis_id: String,
    pub insight_type: String,
    pub title: String,
    pub description: String,
    #[serde(default)]
    pub confidence: f32,
    #[serde(default)]
    pub importance: f32,
    #[serde(default)]
    pub related_entities: Vec<String>,
    #[serde(default)]
    pub suggested_actions: Vec<String>,
    pub created_at_ms: i64,
}

impl Entity for MetacognitionInsight {
    const TABLE: &'static str = "metacognition_insight";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

/// 思维模式 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThinkingPattern {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub pattern_id: String,
    pub pattern_type: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub frequency: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_occurrence_ms: Option<i64>,
    #[serde(default)]
    pub examples: Vec<String>,
    #[serde(default)]
    pub confidence: f32,
    pub created_at_ms: i64,
}

impl Entity for ThinkingPattern {
    const TABLE: &'static str = "thinking_pattern";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

/// AC 性能画像 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcPerformanceProfile {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub ac_id: String,
    #[serde(default)]
    pub total_duration_ms: u64,
    #[serde(default)]
    pub inference_time_ms: u64,
    #[serde(default)]
    pub tool_execution_time_ms: u64,
    #[serde(default)]
    pub context_assembly_time_ms: u64,
    #[serde(default)]
    pub tokens_input: u64,
    #[serde(default)]
    pub tokens_output: u64,
    #[serde(default)]
    pub cost_usd: f64,
    #[serde(default)]
    pub ic_count: u32,
    #[serde(default)]
    pub tool_calls: u32,
    #[serde(default)]
    pub cache_hits: u32,
    #[serde(default)]
    pub cache_misses: u32,
    #[serde(default)]
    pub anomalies: Vec<String>,
    pub created_at_ms: i64,
}

impl Entity for AcPerformanceProfile {
    const TABLE: &'static str = "ac_performance_profile";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

/// 决策审计 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecisionAudit {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub audit_id: String,
    pub decision_id: String,
    pub decision_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    #[serde(default)]
    pub confidence: f32,
    #[serde(default)]
    pub alternatives_considered: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub risk_assessment: Option<String>,
    #[serde(default)]
    pub human_approval_required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub human_approved: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approved_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approved_at_ms: Option<i64>,
    pub created_at_ms: i64,
}

impl Entity for DecisionAudit {
    const TABLE: &'static str = "decision_audit";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}
