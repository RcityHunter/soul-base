use soulbase_tx::backoff::RetryPolicy;
use soulbase_tx::memory::{
    InMemoryDeadStore, InMemoryIdempoStore, InMemoryOutboxStore, InMemorySagaStore,
};
use soulbase_tx::model::{
    DeadLetter, DeadLetterRef, MsgId, OutboxMessage, SagaDefinition, SagaState, SagaStepDef,
};
use soulbase_tx::outbox::{DeadStore, Dispatcher, OutboxStore, OutboxTransport};
use soulbase_tx::prelude::*;
use soulbase_tx::replay::ReplayService;
use soulbase_tx::saga::{SagaOrchestrator, SagaParticipant};
use soulbase_tx::util::now_ms;
use soulbase_types::prelude::TenantId;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone)]
struct MockTransport {
    fail_first: Arc<AtomicBool>,
    delivered: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl OutboxTransport for MockTransport {
    async fn deliver(&self, _tenant: &TenantId, _message: &OutboxMessage) -> Result<(), TxError> {
        if self.fail_first.swap(false, Ordering::SeqCst) {
            Err(TxError::provider_unavailable("fail"))
        } else {
            self.delivered.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
}

fn build_message(tenant: &TenantId, id: &str, channel: &str) -> OutboxMessage {
    OutboxMessage::new(
        tenant.clone(),
        MsgId(id.to_string()),
        channel.to_string(),
        serde_json::json!({ "hello": "world" }),
        now_ms(),
    )
}

#[tokio::test]
async fn outbox_dispatcher_flow() {
    let tenant = TenantId("tenant-outbox".into());
    let outbox = InMemoryOutboxStore::default();
    let dead_store = InMemoryDeadStore::default();
    let transport = MockTransport {
        fail_first: Arc::new(AtomicBool::new(true)),
        delivered: Arc::new(AtomicUsize::new(0)),
    };

    let msg = build_message(&tenant, "msg-1", "channel");
    outbox.enqueue(msg).await.unwrap();

    let dispatcher = Dispatcher {
        transport: transport.clone(),
        store: outbox.clone(),
        dead: dead_store.clone(),
        worker_id: "worker-1".into(),
        max_attempts: 3,
        lease_ms: 200,
        batch: 10,
        backoff: RetryPolicy::default(),
    };

    dispatcher.tick(&tenant, now_ms()).await.unwrap();
    // first attempt failed but message requeued
    let status = outbox
        .status(&tenant, &MsgId("msg-1".into()))
        .await
        .unwrap();
    assert!(matches!(status, Some(OutboxStatus::Pending)));

    let later = now_ms() + 10_000;
    dispatcher.tick(&tenant, later).await.unwrap();
    assert_eq!(transport.delivered.load(Ordering::SeqCst), 1);
    let status = outbox
        .status(&tenant, &MsgId("msg-1".into()))
        .await
        .unwrap();
    assert!(matches!(status, Some(OutboxStatus::Delivered)));
    assert!(dead_store
        .load(&tenant, &MsgId("msg-1".into()))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn idempotency_flow() {
    let store = InMemoryIdempoStore::default();
    let tenant = TenantId("tenant-idempo".into());
    let first = store
        .check_and_put(&tenant, "request-1", "hash", 1_000)
        .await
        .unwrap();
    assert!(first.is_none());
    store
        .finish(&tenant, "request-1", "digest-123")
        .await
        .unwrap();
    let replay = store
        .check_and_put(&tenant, "request-1", "hash", 1_000)
        .await
        .unwrap();
    assert_eq!(replay, Some("digest-123".into()));
}

#[derive(Clone)]
struct TestParticipant {
    fail_second: bool,
}

#[async_trait::async_trait]
impl SagaParticipant for TestParticipant {
    async fn execute(&self, uri: &str, _saga: &SagaInstance) -> Result<bool, TxError> {
        if self.fail_second && uri == "step-b" {
            Ok(false)
        } else {
            Ok(true)
        }
    }

    async fn compensate(&self, _uri: &str, _saga: &SagaInstance) -> Result<bool, TxError> {
        Ok(true)
    }
}

#[tokio::test]
async fn saga_success_and_cancel() {
    let tenant = TenantId("tenant-saga".into());
    let store_success = InMemorySagaStore::default();
    let orchestrator = SagaOrchestrator {
        store: store_success.clone(),
        participant: TestParticipant { fail_second: false },
    };

    let def = SagaDefinition {
        name: "happy".into(),
        steps: vec![
            SagaStepDef {
                name: "A".into(),
                action_uri: "step-a".into(),
                compensate_uri: Some("undo-a".into()),
                timeout_ms: 60_000,
                idempotent: true,
            },
            SagaStepDef {
                name: "B".into(),
                action_uri: "step-b".into(),
                compensate_uri: Some("undo-b".into()),
                timeout_ms: 60_000,
                idempotent: true,
            },
        ],
    };

    let saga_id = orchestrator
        .start(&tenant, &def, Some(now_ms()))
        .await
        .unwrap();
    orchestrator.tick(&saga_id).await.unwrap();
    orchestrator.tick(&saga_id).await.unwrap();
    let saga = store_success.load(&saga_id).await.unwrap().unwrap();
    assert!(matches!(saga.state, SagaState::Completed));

    let failing_store = InMemorySagaStore::default();
    let failing_orchestrator = SagaOrchestrator {
        store: failing_store.clone(),
        participant: TestParticipant { fail_second: true },
    };
    let failing_id = failing_orchestrator
        .start(&tenant, &def, Some(now_ms()))
        .await
        .unwrap();
    failing_orchestrator.tick(&failing_id).await.unwrap();
    failing_orchestrator.tick(&failing_id).await.unwrap();
    failing_orchestrator.tick(&failing_id).await.unwrap();
    let saga2 = failing_store.load(&failing_id).await.unwrap().unwrap();
    assert!(matches!(
        saga2.state,
        SagaState::Compensating | SagaState::Cancelled | SagaState::Failed
    ));
}

#[tokio::test]
async fn dead_letter_replay() {
    let tenant = TenantId("tenant-replay".into());
    let outbox = InMemoryOutboxStore::default();
    let dead = InMemoryDeadStore::default();
    let msg = build_message(&tenant, "msg-dead", "channel");
    outbox.enqueue(msg.clone()).await.unwrap();
    outbox.dead(&tenant, &msg.id, "error").await.unwrap();
    dead.push(DeadLetter {
        reference: DeadLetterRef {
            tenant: tenant.clone(),
            id: msg.id.clone(),
        },
        last_error: Some("error".into()),
        stored_at: now_ms(),
    })
    .await
    .unwrap();

    let replay = ReplayService::new(outbox.clone(), dead.clone());
    replay
        .replay(&DeadLetterRef {
            tenant: tenant.clone(),
            id: msg.id.clone(),
        })
        .await
        .unwrap();

    let status = outbox.status(&tenant, &msg.id).await.unwrap();
    assert!(matches!(status, Some(OutboxStatus::Pending)));
    assert!(dead.load(&tenant, &msg.id).await.unwrap().is_none());
}
