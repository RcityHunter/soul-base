#![cfg(feature = "surreal")]

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use soulbase_storage::surreal::SurrealConfig;
use soulbase_storage::Migrator;
use soulbase_tx::prelude::*;
use soulbase_tx::util::now_ms;
use soulbase_types::prelude::{Id, TenantId};

async fn setup() -> (
    SurrealTxStores,
    Arc<soulbase_storage::surreal::SurrealDatastore>,
) {
    let config = SurrealConfig::default();
    let datastore = Arc::new(
        soulbase_storage::surreal::SurrealDatastore::connect(config)
            .await
            .expect("connect surreal"),
    );
    let scripts = surreal_migrations();
    datastore
        .migrator()
        .apply_up(&scripts)
        .await
        .expect("apply migrations");
    (SurrealTxStores::new(datastore.clone()), datastore)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outbox_flow_round_trip() {
    let (stores, _datastore) = setup().await;
    let outbox = stores.outbox();

    let tenant = TenantId("tenant-1".into());
    let msg_id = MsgId("msg-1".into());
    let now = now_ms();
    let mut message = OutboxMessage::new(
        tenant.clone(),
        msg_id.clone(),
        "events:ping".into(),
        json!({"hello": "world"}),
        now,
    );
    message.dispatch_key = Some("key-1".into());
    message.envelope_id = Some(Id("env-1".into()));

    outbox.enqueue(message).await.expect("enqueue");

    let leased = outbox
        .lease_batch(&tenant, "worker-A", now, 5000, 5, true)
        .await
        .expect("lease batch");
    assert_eq!(leased.len(), 1);
    let leased_msg = &leased[0];
    assert_eq!(leased_msg.id, msg_id);
    match leased_msg.status {
        OutboxStatus::Leased { ref worker, .. } => assert_eq!(worker, "worker-A"),
        _ => panic!("expected leased status"),
    }
    assert_eq!(leased_msg.attempts, 1);

    outbox
        .nack_backoff(&tenant, &msg_id, now + 10_000, "temporary failure")
        .await
        .expect("nack backoff");
    let status = outbox
        .status(&tenant, &msg_id)
        .await
        .expect("status after nack")
        .expect("status present");
    assert!(matches!(status, OutboxStatus::Pending));

    outbox
        .lease_batch(&tenant, "worker-B", now + 11_000, 2000, 5, false)
        .await
        .expect("lease second");
    outbox.ack_done(&tenant, &msg_id).await.expect("ack done");
    let status = outbox
        .status(&tenant, &msg_id)
        .await
        .expect("status after ack")
        .expect("status present");
    assert!(matches!(status, OutboxStatus::Delivered));

    outbox
        .dead_letter(&tenant, &msg_id, "delivery failed")
        .await
        .expect("dead letter");
    let status = outbox
        .status(&tenant, &msg_id)
        .await
        .expect("status after dead")
        .expect("status present");
    assert!(matches!(status, OutboxStatus::Dead));

    outbox.requeue(&tenant, &msg_id).await.expect("requeue");
    let status = outbox
        .status(&tenant, &msg_id)
        .await
        .expect("status after requeue")
        .expect("status present");
    assert!(matches!(status, OutboxStatus::Pending));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_letter_store_round_trip() {
    let (stores, _datastore) = setup().await;
    let dead = stores.dead();
    let tenant = TenantId("tenant-2".into());
    let reference = DeadLetterRef {
        tenant: tenant.clone(),
        kind: DeadKind::Outbox,
        id: MsgId("dead-1".into()),
    };
    let letter = DeadLetter {
        reference: reference.clone(),
        last_error: Some("boom".into()),
        stored_at: now_ms(),
        note: None,
    };

    dead.record(letter.clone()).await.expect("record dead");

    let refs = dead
        .list(&tenant, DeadKind::Outbox, 10)
        .await
        .expect("list dead");
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0], reference);

    let inspected = dead
        .inspect(&reference)
        .await
        .expect("inspect dead")
        .expect("letter present");
    assert_eq!(inspected.last_error.as_deref(), Some("boom"));

    dead.quarantine(&reference, "manual review")
        .await
        .expect("quarantine");
    let inspected = dead
        .inspect(&reference)
        .await
        .expect("inspect after quarantine")
        .expect("letter present");
    assert_eq!(inspected.note.as_deref(), Some("manual review"));

    dead.delete(&reference).await.expect("delete dead");
    let refs = dead
        .list(&tenant, DeadKind::Outbox, 10)
        .await
        .expect("list after delete");
    assert!(refs.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotency_store_behaviour() {
    let (stores, _datastore) = setup().await;
    let idempo = stores.idempo();
    let tenant = TenantId("tenant-3".into());
    let key = "operation-42";
    let hash = "hash-a";

    let existing = idempo
        .check_and_put(&tenant, key, hash, 1_000)
        .await
        .expect("check and put");
    assert!(existing.is_none());

    idempo
        .finish(&tenant, key, hash, "digest-ok")
        .await
        .expect("finish success");

    let existing = idempo
        .check_and_put(&tenant, key, hash, 1_000)
        .await
        .expect("replay success");
    assert_eq!(existing.as_deref(), Some("digest-ok"));

    let key_fail = "operation-err";
    idempo
        .check_and_put(&tenant, key_fail, hash, 500)
        .await
        .expect("check fail");
    idempo
        .fail(&tenant, key_fail, hash, "boom", 500)
        .await
        .expect("mark failed");

    let err = idempo
        .check_and_put(&tenant, key_fail, hash, 500)
        .await
        .expect_err("locked after failure");
    let dev_msg = err.into_inner().message_dev.unwrap();
    assert!(dev_msg.contains("temporarily locked"));

    tokio::time::sleep(Duration::from_millis(550)).await;
    idempo.purge_expired(now_ms()).await.expect("purge expired");
    let existing = idempo
        .check_and_put(&tenant, key_fail, hash, 500)
        .await
        .expect("reinsert after purge");
    assert!(existing.is_none());
}
