#![cfg(feature = "surreal")]

use serde_json::{json, Value};
use soulbase_storage::surreal::{schema_migrations, SurrealConfig, SurrealDatastore};
use soulbase_storage::{Datastore, Migrator, QueryExecutor};

#[tokio::test]
async fn surreal_core_migration_applies() {
    let datastore = SurrealDatastore::connect(SurrealConfig::default())
        .await
        .expect("connect surreal");
    let migrator = datastore.migrator();
    let scripts = schema_migrations();
    migrator
        .apply_up(&scripts)
        .await
        .expect("apply core migration");

    let session = datastore.session().await.expect("session");
    let tenant = "tenant-schema";

    session
        .query(
            "CREATE timeline_event:test_timeline SET \
             tenant = $tenant, journey_id = 'journey-1', thread_id = 'thread-1', \
             event_id = 'evt-1', role = 'User', occurred_at = 1, ingested_at = 1 RETURN NONE",
            json!({ "tenant": tenant }),
        )
        .await
        .expect("insert timeline event");

    session
        .query(
            "CREATE causal_edge:test_edge SET \
             tenant = $tenant, edge_id = 'edge-1', source_event_id = 'evt-1', \
             target_event_id = 'evt-2', relation = 'follows', confidence = 0.9, created_at = 1 RETURN NONE",
            json!({ "tenant": tenant }),
        )
        .await
        .expect("insert causal edge");

    let mut recall_params = json!({ "tenant": tenant });
    recall_params["embedding"] = Value::Array(
        (0..1536)
            .map(|_| Value::from(0.0_f64))
            .collect::<Vec<Value>>(),
    );
    session
        .query(
            "CREATE recall_chunk:test_chunk SET \
             tenant = $tenant, chunk_id = 'chunk-1', journey_id = 'journey-1', \
             content = 'hello world', embedding = $embedding, created_at = 1 RETURN NONE",
            recall_params,
        )
        .await
        .expect("insert recall chunk");

    session
        .query(
            "CREATE awareness_event:test_awareness SET \
             tenant = $tenant, awareness_id = 'aware-1', journey_id = 'journey-1', \
             kind = 'topic', state = 'Raised', triggered_at = 1, updated_at = 1 RETURN NONE",
            json!({ "tenant": tenant }),
        )
        .await
        .expect("insert awareness event");

    session
        .query(
            "CREATE vector_manifest:test_manifest SET \
             tenant = $tenant, index_id = 'idx-1', metric = 'cosine', dims = 1536, \
             created_at = 1, updated_at = 1 RETURN NONE",
            json!({ "tenant": tenant }),
        )
        .await
        .expect("insert vector manifest");

    session
        .query(
            "CREATE llm_tool_plan:test_plan SET \
             tenant = $tenant, plan_id = 'plan-1', step_count = 0, steps = [], \
             created_at = 1, updated_at = 1 RETURN NONE",
            json!({ "tenant": tenant }),
        )
        .await
        .expect("insert llm tool plan");

    session
        .query(
            "CREATE llm_explain:test_explain SET \
             tenant = $tenant, explain_id = 'exp-1', provider = 'openai', metadata = {}, \
             created_at = 1 RETURN NONE",
            json!({ "tenant": tenant }),
        )
        .await
        .expect("insert llm explain");

    let timeline = session
        .query(
            "SELECT event_id FROM timeline_event WHERE tenant = $tenant",
            json!({ "tenant": tenant }),
        )
        .await
        .expect("select timeline");
    assert!(!timeline.rows.is_empty());

    datastore.shutdown().await.expect("shutdown");
}
