#![cfg(feature = "surreal")]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use soulbase_storage::surreal::SurrealConfig;
use soulbase_storage::Migrator;
use soulbase_tx::prelude::*;
use soulbase_tx::replay::ReplayService;
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
async fn outbox_grouping_respects_dispatch_keys() {
    let (stores, _datastore) = setup().await;
    let outbox = stores.outbox();
    let tenant = TenantId("tenant-group".into());
    let now = now_ms();

    for idx in 0..3 {
        let mut msg = OutboxMessage::new(
            tenant.clone(),
            MsgId(format!("msg-shared-{idx}")),
            "events:batch".into(),
            json!({ "seq": idx }),
            now,
        );
        msg.dispatch_key = Some("key-shared".into());
        outbox.enqueue(msg).await.expect("enqueue shared");
    }

    for idx in 0..2 {
        let mut msg = OutboxMessage::new(
            tenant.clone(),
            MsgId(format!("msg-unique-{idx}")),
            "events:batch".into(),
            json!({ "unique": idx }),
            now,
        );
        msg.dispatch_key = Some(format!("key-unique-{idx}"));
        outbox.enqueue(msg).await.expect("enqueue unique");
    }

    let leased_group = outbox
        .lease_batch(&tenant, "worker-group", now, 5_000, 5, true)
        .await
        .expect("lease with grouping");
    assert_eq!(leased_group.len(), 3, "grouping should lease one per key");
    let mut keys = HashSet::new();
    for msg in &leased_group {
        let key = msg
            .dispatch_key
            .clone()
            .expect("dispatch key present when grouping");
        assert!(
            keys.insert(key),
            "duplicate key returned with grouping enabled"
        );
    }

    for msg in leased_group.iter() {
        outbox
            .nack_backoff(&tenant, &msg.id, now, "retry")
            .await
            .expect("reset leased");
    }

    let leased_all = outbox
        .lease_batch(&tenant, "worker-all", now + 1_000, 5_000, 5, false)
        .await
        .expect("lease without grouping");
    assert_eq!(leased_all.len(), 5, "expected full batch without grouping");
    let unique_ids: HashSet<_> = leased_all.iter().map(|msg| msg.id.0.clone()).collect();
    assert_eq!(
        unique_ids.len(),
        5,
        "all leased messages should be distinct"
    );
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
async fn replay_service_restores_pending_message() {
    let (stores, _datastore) = setup().await;
    let outbox = stores.outbox();
    let dead = stores.dead();
    let tenant = TenantId("tenant-replay".into());
    let msg_id = MsgId("msg-replay".into());
    let now = now_ms();

    let mut message = OutboxMessage::new(
        tenant.clone(),
        msg_id.clone(),
        "events:replay".into(),
        json!({ "foo": "bar" }),
        now,
    );
    message.dispatch_key = Some("key-replay".into());
    outbox.enqueue(message).await.expect("enqueue message");

    outbox
        .dead_letter(&tenant, &msg_id, "failure")
        .await
        .expect("mark dead");
    let reference = DeadLetterRef {
        tenant: tenant.clone(),
        kind: DeadKind::Outbox,
        id: msg_id.clone(),
    };
    dead.record(DeadLetter {
        reference: reference.clone(),
        last_error: Some("failure".into()),
        stored_at: now,
        note: None,
    })
    .await
    .expect("record dead letter");

    let replay = ReplayService::new(outbox.clone(), dead.clone());
    replay
        .replay(&reference)
        .await
        .expect("replay should succeed");

    let status = outbox
        .status(&tenant, &msg_id)
        .await
        .expect("status after replay")
        .expect("message present");
    assert!(matches!(status, OutboxStatus::Pending));
    let dead_letter = dead.inspect(&reference).await.expect("inspect");
    assert!(dead_letter.is_none());
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
