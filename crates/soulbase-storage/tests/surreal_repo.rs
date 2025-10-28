#![cfg(feature = "surreal")]

use serde_json::json;
use soulbase_storage::prelude::*;
use soulbase_storage::surreal::{schema_migrations, SurrealConfig, SurrealDatastore};
use soulbase_types::prelude::TenantId;

fn tenant() -> TenantId {
    TenantId("tenant-repo".into())
}

fn sample_timeline(tenant: &TenantId) -> TimelineEvent {
    TimelineEvent {
        id: "timeline_event:evt-1".into(),
        tenant: tenant.clone(),
        journey_id: "journey-1".into(),
        thread_id: Some("thread-1".into()),
        event_id: "evt-1".into(),
        role: "user".into(),
        stage: Some("ingest".into()),
        occurred_at: 1,
        ingested_at: 2,
        payload: None,
        metrics: None,
        vector_ref: None,
    }
}

fn sample_awareness(tenant: &TenantId) -> AwarenessEvent {
    AwarenessEvent {
        id: "awareness_event:aware-1".into(),
        tenant: tenant.clone(),
        journey_id: "journey-1".into(),
        kind: "topic".into(),
        state: "raised".into(),
        score: Some(0.42),
        traits: Some(vec!["latent".into()]),
        evidence: None,
        triggered_at: 10,
        resolved_at: None,
        updated_at: 10,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeline_repo_roundtrip() {
    let datastore = SurrealDatastore::connect(SurrealConfig::default())
        .await
        .expect("connect surreal");
    datastore
        .migrator()
        .apply_up(&schema_migrations())
        .await
        .expect("apply migrations");

    let tenant = tenant();
    let repo = SurrealRepository::<TimelineEvent>::new(&datastore);

    let event = sample_timeline(&tenant);
    repo.create(&tenant, &event).await.expect("create timeline");

    let fetched = repo
        .get(&tenant, &event.id)
        .await
        .expect("get timeline")
        .expect("timeline exists");
    assert_eq!(fetched.event_id, "evt-1");

    let page = repo
        .select(&tenant, QueryParams::default())
        .await
        .expect("select timeline");
    assert_eq!(page.items.len(), 1);
    let stats = page.meta.expect("timeline stats");
    let indices = stats.indices_used.unwrap_or_default();
    assert!(
        indices.contains(&"idx_timeline_journey".to_string())
            || indices.contains(&"idx_timeline_thread".to_string())
    );

    let scan_page = repo
        .select(
            &tenant,
            QueryParams {
                filter: json!({"unknown": "value"}),
                ..Default::default()
            },
        )
        .await
        .expect("select with unknown filter");
    let scan_stats = scan_page.meta.expect("scan stats");
    assert!(scan_stats.degradation_reason.is_some());

    let explain = repo
        .select(
            &tenant,
            QueryParams {
                filter: json!({"journey_id": "journey-1"}),
                ..Default::default()
            },
        )
        .await
        .expect("select explain");
    let plan_stats = explain.meta.expect("plan stats");
    let plan_indices = plan_stats.indices_used.unwrap_or_default();
    assert!(
        plan_indices.iter().any(|idx| idx.contains("journey")),
        "plan indices: {:?}",
        plan_indices
    );

    let updated = repo
        .upsert(
            &tenant,
            &event.id,
            json!({"payload": {"summary": "ok"}, "ingested_at": 3}),
            None,
        )
        .await
        .expect("upsert timeline");
    assert_eq!(
        updated
            .payload
            .unwrap()
            .get("summary")
            .and_then(|v| v.as_str()),
        Some("ok")
    );

    repo.delete(&tenant, &event.id)
        .await
        .expect("delete timeline");
    assert!(repo.get(&tenant, &event.id).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn awareness_repo_supports_basic_crud() {
    let datastore = SurrealDatastore::connect(SurrealConfig::default())
        .await
        .expect("connect surreal");
    datastore
        .migrator()
        .apply_up(&schema_migrations())
        .await
        .expect("apply migrations");

    let tenant = tenant();
    let repo = SurrealRepository::<AwarenessEvent>::new(&datastore);
    let model = sample_awareness(&tenant);

    repo.create(&tenant, &model)
        .await
        .expect("create awareness");

    let fetched = repo
        .get(&tenant, &model.id)
        .await
        .expect("get awareness")
        .expect("awareness exists");
    assert_eq!(fetched.kind, "topic");

    repo.delete(&tenant, &model.id)
        .await
        .expect("delete awareness");
    assert!(repo.get(&tenant, &model.id).await.unwrap().is_none());
}
