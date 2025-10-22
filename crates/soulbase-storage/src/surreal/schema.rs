#![cfg(feature = "surreal")]

use crate::spi::migrate::MigrationScript;
use sha2::{Digest, Sha256};

pub const TABLE_TIMELINE_EVENT: &str = "timeline_event";
pub const TABLE_CAUSAL_EDGE: &str = "causal_edge";
pub const TABLE_RECALL_CHUNK: &str = "recall_chunk";
pub const TABLE_AWARENESS_EVENT: &str = "awareness_event";
pub const TABLE_VECTOR_MANIFEST: &str = "vector_manifest";
pub const TABLE_LLM_TOOL_PLAN: &str = "llm_tool_plan";
pub const TABLE_LLM_EXPLAIN: &str = "llm_explain";

const MIGRATION_VERSION_CORE: &str = "0001_timeline_causal_recall_awareness";
const MIGRATION_VERSION_LLM: &str = "0002_llm_plan_explain";

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

pub fn migrations() -> Vec<MigrationScript> {
    vec![core_migration(), llm_migration()]
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
