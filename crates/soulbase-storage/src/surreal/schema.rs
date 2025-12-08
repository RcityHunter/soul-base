use crate::spi::migrate::MigrationScript;
use sha2::{Digest, Sha256};

pub const TABLE_TIMELINE_EVENT: &str = "timeline_event";
pub const TABLE_CAUSAL_EDGE: &str = "causal_edge";
pub const TABLE_RECALL_CHUNK: &str = "recall_chunk";
pub const TABLE_AWARENESS_EVENT: &str = "awareness_event";
pub const TABLE_VECTOR_MANIFEST: &str = "vector_manifest";
pub const TABLE_LLM_TOOL_PLAN: &str = "llm_tool_plan";
pub const TABLE_LLM_EXPLAIN: &str = "llm_explain";

// ACE 新增表
pub const TABLE_TIMESERIES_METRIC: &str = "timeseries_metric";
pub const TABLE_EMBEDDING_CHUNK: &str = "embedding_chunk";
pub const TABLE_EVOLUTION_GROUP_EVENT: &str = "evolution_group_event";
pub const TABLE_EVOLUTION_AI: &str = "evolution_ai";
pub const TABLE_EVOLUTION_RELATIONSHIP: &str = "evolution_relationship";
pub const TABLE_METACOGNITION_ANALYSIS: &str = "metacognition_analysis";
pub const TABLE_METACOGNITION_INSIGHT: &str = "metacognition_insight";
pub const TABLE_THINKING_PATTERN: &str = "thinking_pattern";
pub const TABLE_AC_PERFORMANCE_PROFILE: &str = "ac_performance_profile";
pub const TABLE_DECISION_AUDIT: &str = "decision_audit";
pub const TABLE_DFR_FINGERPRINT: &str = "dfr_fingerprint";
pub const TABLE_DFR_DECISION: &str = "dfr_decision";
pub const TABLE_AUTONOMOUS_SESSION: &str = "autonomous_session";

const MIGRATION_VERSION_CORE: &str = "0001_timeline_causal_recall_awareness";
const MIGRATION_VERSION_LLM: &str = "0002_llm_plan_explain";
const MIGRATION_VERSION_ACE: &str = "0003_ace_timeseries_evolution_metacognition";

const MIGRATION_CORE_UP: &str = r#"
DEFINE TABLE timeline_event SCHEMAFULL;
DEFINE FIELD tenant ON timeline_event TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD journey_id ON timeline_event TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD thread_id ON timeline_event TYPE option<string>;
DEFINE FIELD event_id ON timeline_event TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD role ON timeline_event TYPE string;
DEFINE FIELD stage ON timeline_event TYPE option<string>;
DEFINE FIELD occurred_at ON timeline_event TYPE int;
DEFINE FIELD ingested_at ON timeline_event TYPE int;
DEFINE FIELD payload ON timeline_event TYPE option<object>;
DEFINE FIELD metrics ON timeline_event TYPE option<object>;
DEFINE FIELD vector_ref ON timeline_event TYPE option<string>;
DEFINE INDEX uniq_timeline_event ON TABLE timeline_event FIELDS tenant, event_id UNIQUE;
DEFINE INDEX idx_timeline_journey ON TABLE timeline_event FIELDS tenant, journey_id, occurred_at;
DEFINE INDEX idx_timeline_thread ON TABLE timeline_event FIELDS tenant, thread_id, occurred_at;

DEFINE TABLE causal_edge SCHEMAFULL;
DEFINE FIELD tenant ON causal_edge TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD edge_id ON causal_edge TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD source_event_id ON causal_edge TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD target_event_id ON causal_edge TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD relation ON causal_edge TYPE string;
DEFINE FIELD confidence ON causal_edge TYPE option<number>;
DEFINE FIELD evidence ON causal_edge TYPE option<object>;
DEFINE FIELD created_at ON causal_edge TYPE int;
DEFINE INDEX uniq_causal_edge ON TABLE causal_edge FIELDS tenant, edge_id UNIQUE;
DEFINE INDEX idx_causal_relation ON TABLE causal_edge FIELDS tenant, source_event_id, target_event_id;
DEFINE INDEX idx_causal_target ON TABLE causal_edge FIELDS tenant, target_event_id;

DEFINE TABLE recall_chunk SCHEMAFULL;
DEFINE FIELD tenant ON recall_chunk TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD chunk_id ON recall_chunk TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD journey_id ON recall_chunk TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD source_event_id ON recall_chunk TYPE option<string>;
DEFINE FIELD label ON recall_chunk TYPE option<string>;
DEFINE FIELD content ON recall_chunk TYPE string;
DEFINE FIELD summary ON recall_chunk TYPE option<string>;
DEFINE FIELD embedding ON recall_chunk TYPE array<float>;
DEFINE FIELD metadata ON recall_chunk TYPE option<object>;
DEFINE FIELD version ON recall_chunk TYPE int DEFAULT 1;
DEFINE FIELD created_at ON recall_chunk TYPE int;
DEFINE FIELD expires_at ON recall_chunk TYPE option<int>;
DEFINE INDEX uniq_recall_chunk ON TABLE recall_chunk FIELDS tenant, chunk_id UNIQUE;
DEFINE INDEX idx_recall_journey ON TABLE recall_chunk FIELDS tenant, journey_id, created_at;
DEFINE INDEX idx_recall_source ON TABLE recall_chunk FIELDS tenant, source_event_id;
DEFINE INDEX idx_recall_vector ON TABLE recall_chunk FIELDS embedding SEARCH HNSW DIMENSION 1536;

DEFINE TABLE awareness_event SCHEMAFULL;
DEFINE FIELD tenant ON awareness_event TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD awareness_id ON awareness_event TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD journey_id ON awareness_event TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD kind ON awareness_event TYPE string;
DEFINE FIELD state ON awareness_event TYPE string;
DEFINE FIELD score ON awareness_event TYPE option<number>;
DEFINE FIELD traits ON awareness_event TYPE option<array<string>>;
DEFINE FIELD evidence ON awareness_event TYPE option<object>;
DEFINE FIELD triggered_at ON awareness_event TYPE int;
DEFINE FIELD resolved_at ON awareness_event TYPE option<int>;
DEFINE FIELD updated_at ON awareness_event TYPE int;
DEFINE INDEX uniq_awareness_event ON TABLE awareness_event FIELDS tenant, awareness_id UNIQUE;
DEFINE INDEX idx_awareness_timeline ON TABLE awareness_event FIELDS tenant, journey_id, triggered_at;

DEFINE TABLE vector_manifest SCHEMAFULL;
DEFINE FIELD tenant ON vector_manifest TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD index_id ON vector_manifest TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD description ON vector_manifest TYPE option<string>;
DEFINE FIELD metric ON vector_manifest TYPE string DEFAULT 'cosine';
DEFINE FIELD dims ON vector_manifest TYPE int ASSERT $value > 0;
DEFINE FIELD provider ON vector_manifest TYPE option<string>;
DEFINE FIELD runtime ON vector_manifest TYPE option<string>;
DEFINE FIELD config ON vector_manifest TYPE option<object>;
DEFINE FIELD created_at ON vector_manifest TYPE int;
DEFINE FIELD updated_at ON vector_manifest TYPE int;
DEFINE INDEX uniq_vector_manifest ON TABLE vector_manifest FIELDS tenant, index_id UNIQUE;
"#;

const MIGRATION_CORE_DOWN: &str = r#"
REMOVE TABLE vector_manifest;
REMOVE TABLE awareness_event;
REMOVE TABLE recall_chunk;
REMOVE TABLE causal_edge;
REMOVE TABLE timeline_event;
"#;

const MIGRATION_LLM_UP: &str = r#"
DEFINE TABLE llm_tool_plan SCHEMAFULL;
DEFINE FIELD tenant ON llm_tool_plan TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD plan_id ON llm_tool_plan TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD step_count ON llm_tool_plan TYPE int ASSERT $value >= 0;
DEFINE FIELD steps ON llm_tool_plan TYPE array<object>;
DEFINE FIELD created_at ON llm_tool_plan TYPE int;
DEFINE FIELD updated_at ON llm_tool_plan TYPE int;
DEFINE INDEX uniq_llm_plan ON TABLE llm_tool_plan FIELDS tenant, plan_id UNIQUE;
DEFINE INDEX idx_llm_plan_tenant ON TABLE llm_tool_plan FIELDS tenant;

DEFINE TABLE llm_explain SCHEMAFULL;
DEFINE FIELD tenant ON llm_explain TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD explain_id ON llm_explain TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD provider ON llm_explain TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD fingerprint ON llm_explain TYPE option<string>;
DEFINE FIELD plan_id ON llm_explain TYPE option<string>;
DEFINE FIELD metadata ON llm_explain TYPE object;
DEFINE FIELD created_at ON llm_explain TYPE int;
DEFINE INDEX uniq_llm_explain ON TABLE llm_explain FIELDS tenant, explain_id UNIQUE;
DEFINE INDEX idx_llm_explain_plan ON TABLE llm_explain FIELDS tenant, plan_id;
"#;

const MIGRATION_LLM_DOWN: &str = r#"
REMOVE TABLE llm_explain;
REMOVE TABLE llm_tool_plan;
"#;

const MIGRATION_ACE_UP: &str = r#"
-- 时序指标表
DEFINE TABLE timeseries_metric SCHEMAFULL;
DEFINE FIELD tenant ON timeseries_metric TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD metric_id ON timeseries_metric TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD name ON timeseries_metric TYPE string;
DEFINE FIELD value ON timeseries_metric TYPE number;
DEFINE FIELD timestamp_ms ON timeseries_metric TYPE int;
DEFINE FIELD tags ON timeseries_metric TYPE option<object>;
DEFINE FIELD session_id ON timeseries_metric TYPE option<string>;
DEFINE FIELD ac_id ON timeseries_metric TYPE option<string>;
DEFINE INDEX uniq_timeseries_metric ON TABLE timeseries_metric FIELDS tenant, metric_id UNIQUE;
DEFINE INDEX idx_timeseries_name_time ON TABLE timeseries_metric FIELDS tenant, name, timestamp_ms;
DEFINE INDEX idx_timeseries_session ON TABLE timeseries_metric FIELDS tenant, session_id, timestamp_ms;

-- 向量嵌入表
DEFINE TABLE embedding_chunk SCHEMAFULL;
DEFINE FIELD tenant ON embedding_chunk TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD chunk_id ON embedding_chunk TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD content ON embedding_chunk TYPE string;
DEFINE FIELD embedding ON embedding_chunk TYPE array<float>;
DEFINE FIELD source_type ON embedding_chunk TYPE string;
DEFINE FIELD source_id ON embedding_chunk TYPE string;
DEFINE FIELD journey_id ON embedding_chunk TYPE option<string>;
DEFINE FIELD session_id ON embedding_chunk TYPE option<string>;
DEFINE FIELD created_at_ms ON embedding_chunk TYPE int;
DEFINE FIELD metadata ON embedding_chunk TYPE option<object>;
DEFINE INDEX uniq_embedding_chunk ON TABLE embedding_chunk FIELDS tenant, chunk_id UNIQUE;
DEFINE INDEX idx_embedding_source ON TABLE embedding_chunk FIELDS tenant, source_type, source_id;
DEFINE INDEX idx_embedding_journey ON TABLE embedding_chunk FIELDS tenant, journey_id;
DEFINE INDEX idx_embedding_vector ON TABLE embedding_chunk FIELDS embedding SEARCH HNSW DIMENSION 1536;

-- 群体演化事件表
DEFINE TABLE evolution_group_event SCHEMAFULL;
DEFINE FIELD tenant ON evolution_group_event TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD event_id ON evolution_group_event TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD event_type ON evolution_group_event TYPE string;
DEFINE FIELD description ON evolution_group_event TYPE string;
DEFINE FIELD participants ON evolution_group_event TYPE array<string> DEFAULT [];
DEFINE FIELD impact_scope ON evolution_group_event TYPE string DEFAULT 'local';
DEFINE FIELD magnitude ON evolution_group_event TYPE number DEFAULT 0.5;
DEFINE FIELD occurred_at_ms ON evolution_group_event TYPE int;
DEFINE FIELD metadata ON evolution_group_event TYPE option<object>;
DEFINE INDEX uniq_evolution_group ON TABLE evolution_group_event FIELDS tenant, event_id UNIQUE;
DEFINE INDEX idx_evolution_group_type ON TABLE evolution_group_event FIELDS tenant, event_type;
DEFINE INDEX idx_evolution_group_time ON TABLE evolution_group_event FIELDS tenant, occurred_at_ms;

-- AI 个体演化表
DEFINE TABLE evolution_ai SCHEMAFULL;
DEFINE FIELD tenant ON evolution_ai TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD record_id ON evolution_ai TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD ai_id ON evolution_ai TYPE string;
DEFINE FIELD event_type ON evolution_ai TYPE string;
DEFINE FIELD from_state ON evolution_ai TYPE option<object>;
DEFINE FIELD to_state ON evolution_ai TYPE option<object>;
DEFINE FIELD trigger ON evolution_ai TYPE option<string>;
DEFINE FIELD occurred_at_ms ON evolution_ai TYPE int;
DEFINE FIELD metadata ON evolution_ai TYPE option<object>;
DEFINE INDEX uniq_evolution_ai ON TABLE evolution_ai FIELDS tenant, record_id UNIQUE;
DEFINE INDEX idx_evolution_ai_id ON TABLE evolution_ai FIELDS tenant, ai_id;
DEFINE INDEX idx_evolution_ai_time ON TABLE evolution_ai FIELDS tenant, occurred_at_ms;

-- 关系演化表
DEFINE TABLE evolution_relationship SCHEMAFULL;
DEFINE FIELD tenant ON evolution_relationship TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD record_id ON evolution_relationship TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD source_id ON evolution_relationship TYPE string;
DEFINE FIELD target_id ON evolution_relationship TYPE string;
DEFINE FIELD relationship_type ON evolution_relationship TYPE string;
DEFINE FIELD event_type ON evolution_relationship TYPE string;
DEFINE FIELD strength_before ON evolution_relationship TYPE number DEFAULT 0.0;
DEFINE FIELD strength_after ON evolution_relationship TYPE number DEFAULT 0.0;
DEFINE FIELD trigger ON evolution_relationship TYPE option<string>;
DEFINE FIELD occurred_at_ms ON evolution_relationship TYPE int;
DEFINE INDEX uniq_evolution_rel ON TABLE evolution_relationship FIELDS tenant, record_id UNIQUE;
DEFINE INDEX idx_evolution_rel_source ON TABLE evolution_relationship FIELDS tenant, source_id;
DEFINE INDEX idx_evolution_rel_target ON TABLE evolution_relationship FIELDS tenant, target_id;

-- 元认知分析表
DEFINE TABLE metacognition_analysis SCHEMAFULL;
DEFINE FIELD tenant ON metacognition_analysis TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD analysis_id ON metacognition_analysis TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD mode ON metacognition_analysis TYPE string;
DEFINE FIELD success ON metacognition_analysis TYPE bool DEFAULT true;
DEFINE FIELD data ON metacognition_analysis TYPE option<object>;
DEFINE FIELD summary ON metacognition_analysis TYPE option<string>;
DEFINE FIELD execution_time_ms ON metacognition_analysis TYPE int DEFAULT 0;
DEFINE FIELD created_at_ms ON metacognition_analysis TYPE int;
DEFINE INDEX uniq_metacognition_analysis ON TABLE metacognition_analysis FIELDS tenant, analysis_id UNIQUE;
DEFINE INDEX idx_metacognition_mode ON TABLE metacognition_analysis FIELDS tenant, mode;

-- 元认知洞见表
DEFINE TABLE metacognition_insight SCHEMAFULL;
DEFINE FIELD tenant ON metacognition_insight TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD insight_id ON metacognition_insight TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD analysis_id ON metacognition_insight TYPE string;
DEFINE FIELD insight_type ON metacognition_insight TYPE string;
DEFINE FIELD title ON metacognition_insight TYPE string;
DEFINE FIELD description ON metacognition_insight TYPE string;
DEFINE FIELD confidence ON metacognition_insight TYPE number DEFAULT 0.0;
DEFINE FIELD importance ON metacognition_insight TYPE number DEFAULT 0.0;
DEFINE FIELD related_entities ON metacognition_insight TYPE array<string> DEFAULT [];
DEFINE FIELD suggested_actions ON metacognition_insight TYPE array<string> DEFAULT [];
DEFINE FIELD created_at_ms ON metacognition_insight TYPE int;
DEFINE INDEX uniq_metacognition_insight ON TABLE metacognition_insight FIELDS tenant, insight_id UNIQUE;
DEFINE INDEX idx_metacognition_insight_analysis ON TABLE metacognition_insight FIELDS tenant, analysis_id;

-- 思维模式表
DEFINE TABLE thinking_pattern SCHEMAFULL;
DEFINE FIELD tenant ON thinking_pattern TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD pattern_id ON thinking_pattern TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD pattern_type ON thinking_pattern TYPE string;
DEFINE FIELD name ON thinking_pattern TYPE string;
DEFINE FIELD description ON thinking_pattern TYPE option<string>;
DEFINE FIELD frequency ON thinking_pattern TYPE int DEFAULT 0;
DEFINE FIELD last_occurrence_ms ON thinking_pattern TYPE option<int>;
DEFINE FIELD examples ON thinking_pattern TYPE array<string> DEFAULT [];
DEFINE FIELD confidence ON thinking_pattern TYPE number DEFAULT 0.0;
DEFINE FIELD created_at_ms ON thinking_pattern TYPE int;
DEFINE INDEX uniq_thinking_pattern ON TABLE thinking_pattern FIELDS tenant, pattern_id UNIQUE;
DEFINE INDEX idx_thinking_pattern_type ON TABLE thinking_pattern FIELDS tenant, pattern_type;

-- AC 性能画像表
DEFINE TABLE ac_performance_profile SCHEMAFULL;
DEFINE FIELD tenant ON ac_performance_profile TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD ac_id ON ac_performance_profile TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD total_duration_ms ON ac_performance_profile TYPE int DEFAULT 0;
DEFINE FIELD inference_time_ms ON ac_performance_profile TYPE int DEFAULT 0;
DEFINE FIELD tool_execution_time_ms ON ac_performance_profile TYPE int DEFAULT 0;
DEFINE FIELD context_assembly_time_ms ON ac_performance_profile TYPE int DEFAULT 0;
DEFINE FIELD tokens_input ON ac_performance_profile TYPE int DEFAULT 0;
DEFINE FIELD tokens_output ON ac_performance_profile TYPE int DEFAULT 0;
DEFINE FIELD cost_usd ON ac_performance_profile TYPE number DEFAULT 0.0;
DEFINE FIELD ic_count ON ac_performance_profile TYPE int DEFAULT 0;
DEFINE FIELD tool_calls ON ac_performance_profile TYPE int DEFAULT 0;
DEFINE FIELD cache_hits ON ac_performance_profile TYPE int DEFAULT 0;
DEFINE FIELD cache_misses ON ac_performance_profile TYPE int DEFAULT 0;
DEFINE FIELD anomalies ON ac_performance_profile TYPE array<string> DEFAULT [];
DEFINE FIELD created_at_ms ON ac_performance_profile TYPE int;
DEFINE INDEX uniq_ac_performance ON TABLE ac_performance_profile FIELDS tenant, ac_id UNIQUE;

-- 决策审计表
DEFINE TABLE decision_audit SCHEMAFULL;
DEFINE FIELD tenant ON decision_audit TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD audit_id ON decision_audit TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD decision_id ON decision_audit TYPE string;
DEFINE FIELD decision_type ON decision_audit TYPE string;
DEFINE FIELD input_summary ON decision_audit TYPE option<string>;
DEFINE FIELD output_summary ON decision_audit TYPE option<string>;
DEFINE FIELD rationale ON decision_audit TYPE option<string>;
DEFINE FIELD confidence ON decision_audit TYPE number DEFAULT 0.0;
DEFINE FIELD alternatives_considered ON decision_audit TYPE int DEFAULT 0;
DEFINE FIELD risk_assessment ON decision_audit TYPE option<string>;
DEFINE FIELD human_approval_required ON decision_audit TYPE bool DEFAULT false;
DEFINE FIELD human_approved ON decision_audit TYPE option<bool>;
DEFINE FIELD approved_by ON decision_audit TYPE option<string>;
DEFINE FIELD approved_at_ms ON decision_audit TYPE option<int>;
DEFINE FIELD created_at_ms ON decision_audit TYPE int;
DEFINE INDEX uniq_decision_audit ON TABLE decision_audit FIELDS tenant, audit_id UNIQUE;
DEFINE INDEX idx_decision_audit_decision ON TABLE decision_audit FIELDS tenant, decision_id;

-- DFR 指纹表
DEFINE TABLE dfr_fingerprint SCHEMAFULL;
DEFINE FIELD tenant ON dfr_fingerprint TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD fingerprint_id ON dfr_fingerprint TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD decision_type ON dfr_fingerprint TYPE string;
DEFINE FIELD feature_vector ON dfr_fingerprint TYPE array<float> DEFAULT [];
DEFINE FIELD confidence ON dfr_fingerprint TYPE number DEFAULT 0.0;
DEFINE FIELD created_at_ms ON dfr_fingerprint TYPE int;
DEFINE FIELD metadata ON dfr_fingerprint TYPE option<object>;
DEFINE INDEX uniq_dfr_fingerprint ON TABLE dfr_fingerprint FIELDS tenant, fingerprint_id UNIQUE;
DEFINE INDEX idx_dfr_fingerprint_type ON TABLE dfr_fingerprint FIELDS tenant, decision_type;
DEFINE INDEX idx_dfr_fingerprint_vector ON TABLE dfr_fingerprint FIELDS feature_vector SEARCH HNSW DIMENSION 128;

-- DFR 决策表
DEFINE TABLE dfr_decision SCHEMAFULL;
DEFINE FIELD tenant ON dfr_decision TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD decision_id ON dfr_decision TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD fingerprint_id ON dfr_decision TYPE option<string>;
DEFINE FIELD decision_type ON dfr_decision TYPE string;
DEFINE FIELD input_context ON dfr_decision TYPE option<object>;
DEFINE FIELD output ON dfr_decision TYPE option<object>;
DEFINE FIELD confidence ON dfr_decision TYPE number DEFAULT 0.0;
DEFINE FIELD latency_ms ON dfr_decision TYPE int DEFAULT 0;
DEFINE FIELD outcome ON dfr_decision TYPE option<string>;
DEFINE FIELD created_at_ms ON dfr_decision TYPE int;
DEFINE INDEX uniq_dfr_decision ON TABLE dfr_decision FIELDS tenant, decision_id UNIQUE;
DEFINE INDEX idx_dfr_decision_fingerprint ON TABLE dfr_decision FIELDS tenant, fingerprint_id;
DEFINE INDEX idx_dfr_decision_type ON TABLE dfr_decision FIELDS tenant, decision_type;

-- 自主会话表 (使用 SCHEMALESS 允许 metadata 存储任意嵌套 JSON)
DEFINE TABLE autonomous_session SCHEMALESS;
DEFINE FIELD tenant ON autonomous_session TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD session_id ON autonomous_session TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD status ON autonomous_session TYPE string DEFAULT 'pending';
DEFINE FIELD max_cycles ON autonomous_session TYPE int DEFAULT 100;
DEFINE FIELD timeout_ms ON autonomous_session TYPE int DEFAULT 3600000;
DEFINE FIELD cycles_executed ON autonomous_session TYPE int DEFAULT 0;
DEFINE FIELD started_at_ms ON autonomous_session TYPE option<int>;
DEFINE FIELD ended_at_ms ON autonomous_session TYPE option<int>;
DEFINE FIELD end_reason ON autonomous_session TYPE option<string>;
DEFINE FIELD scenario_stack ON autonomous_session TYPE array DEFAULT [];
DEFINE FIELD total_cost_usd ON autonomous_session TYPE number DEFAULT 0.0;
DEFINE FIELD total_tokens ON autonomous_session TYPE int DEFAULT 0;
-- metadata 字段在 SCHEMALESS 模式下可以存储任意嵌套 JSON
DEFINE INDEX uniq_autonomous_session ON TABLE autonomous_session FIELDS tenant, session_id UNIQUE;
DEFINE INDEX idx_autonomous_session_status ON TABLE autonomous_session FIELDS tenant, status;
"#;

const MIGRATION_ACE_DOWN: &str = r#"
REMOVE TABLE autonomous_session;
REMOVE TABLE dfr_decision;
REMOVE TABLE dfr_fingerprint;
REMOVE TABLE decision_audit;
REMOVE TABLE ac_performance_profile;
REMOVE TABLE thinking_pattern;
REMOVE TABLE metacognition_insight;
REMOVE TABLE metacognition_analysis;
REMOVE TABLE evolution_relationship;
REMOVE TABLE evolution_ai;
REMOVE TABLE evolution_group_event;
REMOVE TABLE embedding_chunk;
REMOVE TABLE timeseries_metric;
"#;

pub fn core_migration() -> MigrationScript {
    MigrationScript {
        version: MIGRATION_VERSION_CORE.to_string(),
        up_sql: MIGRATION_CORE_UP.trim().to_string(),
        down_sql: MIGRATION_CORE_DOWN.trim().to_string(),
        checksum: checksum(MIGRATION_CORE_UP, MIGRATION_CORE_DOWN),
    }
}

pub fn llm_migration() -> MigrationScript {
    MigrationScript {
        version: MIGRATION_VERSION_LLM.to_string(),
        up_sql: MIGRATION_LLM_UP.trim().to_string(),
        down_sql: MIGRATION_LLM_DOWN.trim().to_string(),
        checksum: checksum(MIGRATION_LLM_UP, MIGRATION_LLM_DOWN),
    }
}

pub fn ace_migration() -> MigrationScript {
    MigrationScript {
        version: MIGRATION_VERSION_ACE.to_string(),
        up_sql: MIGRATION_ACE_UP.trim().to_string(),
        down_sql: MIGRATION_ACE_DOWN.trim().to_string(),
        checksum: checksum(MIGRATION_ACE_UP, MIGRATION_ACE_DOWN),
    }
}

pub fn migrations() -> Vec<MigrationScript> {
    vec![core_migration(), llm_migration(), ace_migration()]
}

fn checksum(up: &str, down: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(up.as_bytes());
    hasher.update([0x00]);
    hasher.update(down.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_stable() {
        let migration = core_migration();
        assert!(migration.checksum.starts_with("sha256:"));
        assert!(migration.up_sql.contains("DEFINE TABLE timeline_event"));
        assert!(migration.down_sql.contains("REMOVE TABLE timeline_event"));
    }

    #[test]
    fn llm_checksum_stable() {
        let migration = llm_migration();
        assert!(migration.checksum.starts_with("sha256:"));
        assert!(migration.up_sql.contains("DEFINE TABLE llm_tool_plan"));
        assert!(migration.up_sql.contains("DEFINE TABLE llm_explain"));
        assert!(migration.down_sql.contains("REMOVE TABLE llm_tool_plan"));
    }
}
