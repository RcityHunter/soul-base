use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::json;
use soulbase_storage::prelude::*;
use soulbase_storage::surreal::{schema_migrations, SurrealConfig, SurrealDatastore};
use soulbase_tx::model::{MsgId, OutboxMessage, OutboxStatus};
use soulbase_tx::prelude::{DeadKind, Dispatcher, RetryPolicy, SurrealTxStores};
use soulbase_types::prelude::TenantId;
use tokio::time::Duration;

#[derive(Default, Clone)]
struct TestTransport {
    delivered: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
}

#[async_trait::async_trait]
impl soulbase_tx::outbox::Transport for TestTransport {
    async fn send(&self, topic: &str, payload: &serde_json::Value) -> Result<(), soulbase_tx::errors::TxError> {
        self.delivered
            .lock()
            .push((topic.to_string(), payload.clone()));
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_to_end_pipeline_records_event_and_dispatches_outbox() {
    let datastore = Arc::new(
        SurrealDatastore::connect(SurrealConfig::default())
            .await
            .expect("connect surreal"),
    );
    datastore
        .migrator()
        .apply_up(&schema_migrations())
        .await
        .expect("apply migrations");

    let tenant = TenantId("tenant-e2e".into());

    let timeline_repo = TimelineEventRepo::new(&datastore);
    timeline_repo
        .create(
            &tenant,
            &TimelineEvent {
                id: "timeline_event:e2e-1".into(),
                tenant: tenant.clone(),
                journey_id: "journey-1".into(),
                thread_id: Some("thread-1".into()),
                event_id: "evt-1".into(),
                role: "user".into(),
                stage: Some("ingest".into()),
                occurred_at: 1,
                ingested_at: 1,
                payload: Some(json!({"input": "hello"})),
                metrics: None,
                vector_ref: None,
            },
        )
        .await
        .expect("insert timeline");

    let fetched = timeline_repo
        .get(&tenant, "timeline_event:e2e-1")
        .await
        .expect("get timeline")
        .expect("timeline exists");
    assert_eq!(fetched.event_id, "evt-1");

    let stores = soulbase_tx::prelude::SurrealTxStores::new(datastore.clone());
    let outbox_store = stores.outbox();
    let dead_store = stores.dead();

    let transport = TestTransport::default();
    let dispatcher = Dispatcher {
        transport: transport.clone(),
        store: outbox_store.clone(),
        dead: dead_store.clone(),
        worker_id: "e2e-worker".into(),
        lease_ms: 30_000,
        batch: 10,
        group_by_key: true,
        backoff: Box::new(RetryPolicy::default()),
    };

    let now = chrono::Utc::now().timestamp_millis();
    let message = OutboxMessage::new(
        tenant.clone(),
        MsgId("msg-1".into()),
        "tool.invoke".into(),
        json!({"tenant": tenant.0, "tool_id": "search.web", "result": "ok"}),
        now,
    );
    outbox_store.enqueue(message).await.expect("enqueue");

    dispatcher
        .tick(&tenant, now)
        .await
        .expect("dispatch outbox");

    {
        let locked = transport.delivered.lock();
        assert_eq!(locked.len(), 1);
        assert_eq!(locked[0].0, "tool.invoke");
        assert_eq!(
            locked[0].1.get("tool_id").and_then(|v| v.as_str()),
            Some("search.web")
        );
    }

    let status = outbox_store
        .status(&tenant, &MsgId("msg-1".into()))
        .await
        .expect("status")
        .expect("message status");
    assert!(matches!(status, OutboxStatus::Delivered));

    tokio::time::sleep(Duration::from_millis(10)).await;

    let dead_letters = dead_store
        .list(&tenant, DeadKind::Outbox, 10)
        .await
        .expect("dead letters");
    assert!(dead_letters.is_empty());
}
