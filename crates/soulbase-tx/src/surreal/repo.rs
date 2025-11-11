#![cfg(feature = "surreal")]
#![allow(clippy::result_large_err)]

use crate::errors::TxError;
use crate::idempo::IdempoStore;
use crate::model::{
    DeadKind, DeadLetter, DeadLetterRef, IdempoState, MsgId, OutboxMessage, OutboxStatus,
};
use crate::outbox::{DeadStore, OutboxStore};
use crate::surreal::mapper;
use crate::surreal::schema;
use crate::util::now_ms;
use async_trait::async_trait;
use serde_json::{Value, json};
use soulbase_storage::prelude::{
    Datastore, MigrationScript, QueryExecutor, Session, StorageError, Transaction,
};
use soulbase_storage::surreal::{SurrealDatastore, SurrealSession};
use soulbase_types::prelude::TenantId;
use std::collections::HashSet;
use std::sync::Arc;

pub fn migrations() -> Vec<MigrationScript> {
    schema::migrations()
}

fn map_storage(err: StorageError) -> TxError {
    TxError::from(err.into_inner())
}

fn map_result<T>(res: Result<T, StorageError>) -> Result<T, TxError> {
    res.map_err(map_storage)
}

#[derive(Clone)]
pub struct SurrealOutboxStore {
    datastore: Arc<SurrealDatastore>,
}

impl SurrealOutboxStore {
    pub fn new(datastore: Arc<SurrealDatastore>) -> Self {
        Self { datastore }
    }

    async fn session(&self) -> Result<SurrealSession, TxError> {
        map_result(self.datastore.session().await)
    }

    fn select_limit(batch: u32, group_by_key: bool) -> u32 {
        if !group_by_key {
            return batch.max(1);
        }
        batch.saturating_mul(4).max(batch.max(1))
    }
}

#[async_trait]
impl OutboxStore for SurrealOutboxStore {
    async fn enqueue(&self, msg: OutboxMessage) -> Result<(), TxError> {
        let session = self.session().await?;
        let payload = mapper::encode_outbox(&msg);
        map_result(
            session
                .query(
                    "UPSERT tx_outbox CONTENT $data RETURN NONE",
                    json!({"data": payload}),
                )
                .await,
        )?;
        Ok(())
    }

    async fn lease_batch(
        &self,
        tenant: &TenantId,
        worker: &str,
        now_ms: i64,
        lease_ms: u64,
        batch: u32,
        group_by_key: bool,
    ) -> Result<Vec<OutboxMessage>, TxError> {
        if batch == 0 {
            return Ok(Vec::new());
        }
        let session = self.session().await?;
        let mut tx = map_result(session.begin().await)?;

        let limit = Self::select_limit(batch, group_by_key);
        let candidates_outcome = map_result(
            tx.query(
                r#"SELECT msg_id, tenant, channel, payload, attempts, status, visible_at,
                             lease_worker, lease_until, last_error, dispatch_key, envelope_id,
                             created_at, updated_at
                   FROM tx_outbox
                   WHERE tenant = $tenant AND (
                    (status = "Pending" AND visible_at <= $now)
                    OR (status = "Leased" AND lease_until != NONE AND lease_until <= $now)
                   )
                   ORDER BY visible_at ASC, created_at ASC
                   LIMIT $limit"#,
                json!({
                    "tenant": tenant.0,
                    "now": now_ms,
                    "limit": limit,
                }),
            )
            .await,
        )?;

        let mut candidates = mapper::decode_outbox_list(Value::Array(candidates_outcome.rows))
            .map_err(|err| TxError::internal(&format!("decode outbox candidate: {err}")))?;

        if candidates.is_empty() {
            map_result(tx.rollback().await)?;
            return Ok(Vec::new());
        }

        let mut selected = Vec::new();
        let mut seen_keys = HashSet::new();
        for candidate in candidates.drain(..) {
            if group_by_key {
                if let Some(key) = &candidate.dispatch_key {
                    if !seen_keys.insert(key.clone()) {
                        continue;
                    }
                }
            }
            selected.push(candidate);
            if selected.len() as u32 >= batch {
                break;
            }
        }

        if selected.is_empty() {
            map_result(tx.rollback().await)?;
            return Ok(Vec::new());
        }

        let lease_until = now_ms + lease_ms as i64;
        let mut leased = Vec::with_capacity(selected.len());

        for mut candidate in selected {
            candidate.attempts = candidate.attempts.saturating_add(1);
            candidate.status = OutboxStatus::Leased {
                worker: worker.to_string(),
                lease_until,
            };
            candidate.updated_at = now_ms;
            map_result(
                tx.query(
                    r#"UPDATE tx_outbox SET
                        attempts = $attempts,
                        status = "Leased",
                        lease_worker = $worker,
                        lease_until = $lease_until,
                        updated_at = $now
                       WHERE tenant = $tenant AND msg_id = $msg_id
                       RETURN NONE"#,
                    json!({
                        "attempts": candidate.attempts,
                        "worker": worker,
                        "lease_until": lease_until,
                        "now": now_ms,
                        "tenant": tenant.0,
                        "msg_id": candidate.id.0,
                    }),
                )
                .await,
            )?;
            leased.push(candidate);
        }

        map_result(tx.commit().await)?;
        Ok(leased)
    }

    async fn ack_done(&self, tenant: &TenantId, id: &MsgId) -> Result<(), TxError> {
        let session = self.session().await?;
        map_result(
            session
                .query(
                    r#"UPDATE tx_outbox SET
                        status = "Delivered",
                        lease_worker = NONE,
                        lease_until = NONE,
                        updated_at = $now,
                        last_error = NONE
                      WHERE tenant = $tenant AND msg_id = $msg_id
                      RETURN NONE"#,
                    json!({
                        "tenant": tenant.0,
                        "msg_id": id.0,
                        "now": now_ms(),
                    }),
                )
                .await,
        )?;
        Ok(())
    }

    async fn nack_backoff(
        &self,
        tenant: &TenantId,
        id: &MsgId,
        next_visible_at: i64,
        error: &str,
    ) -> Result<(), TxError> {
        let session = self.session().await?;
        map_result(
            session
                .query(
                    r#"UPDATE tx_outbox SET
                        status = "Pending",
                        visible_at = $visible,
                        lease_worker = NONE,
                        lease_until = NONE,
                        last_error = $error,
                        updated_at = $now
                      WHERE tenant = $tenant AND msg_id = $msg_id
                      RETURN NONE"#,
                    json!({
                        "tenant": tenant.0,
                        "msg_id": id.0,
                        "visible": next_visible_at,
                        "error": error,
                        "now": now_ms(),
                    }),
                )
                .await,
        )?;
        Ok(())
    }

    async fn dead_letter(&self, tenant: &TenantId, id: &MsgId, error: &str) -> Result<(), TxError> {
        let session = self.session().await?;
        map_result(
            session
                .query(
                    r#"UPDATE tx_outbox SET
                        status = "Dead",
                        lease_worker = NONE,
                        lease_until = NONE,
                        last_error = $error,
                        updated_at = $now
                      WHERE tenant = $tenant AND msg_id = $msg_id
                      RETURN NONE"#,
                    json!({
                        "tenant": tenant.0,
                        "msg_id": id.0,
                        "error": error,
                        "now": now_ms(),
                    }),
                )
                .await,
        )?;
        Ok(())
    }

    async fn requeue(&self, tenant: &TenantId, id: &MsgId) -> Result<(), TxError> {
        let session = self.session().await?;
        let now = now_ms();
        map_result(
            session
                .query(
                    r#"UPDATE tx_outbox SET
                        status = "Pending",
                        visible_at = $visible,
                        lease_worker = NONE,
                        lease_until = NONE,
                        last_error = NONE,
                        updated_at = $now
                      WHERE tenant = $tenant AND msg_id = $msg_id
                      RETURN NONE"#,
                    json!({
                        "tenant": tenant.0,
                        "msg_id": id.0,
                        "visible": now,
                        "now": now,
                    }),
                )
                .await,
        )?;
        Ok(())
    }

    async fn status(
        &self,
        tenant: &TenantId,
        id: &MsgId,
    ) -> Result<Option<crate::model::OutboxStatus>, TxError> {
        let session = self.session().await?;
        let outcome = map_result(
            session
                .query(
                    "SELECT msg_id, tenant, channel, payload, attempts, status, visible_at, lease_worker, lease_until, last_error, dispatch_key, envelope_id, created_at, updated_at FROM tx_outbox WHERE tenant = $tenant AND msg_id = $msg_id LIMIT 1",
                    json!({
                        "tenant": tenant.0,
                        "msg_id": id.0,
                    }),
                )
                .await,
        )?;
        let mut messages = mapper::decode_outbox_list(Value::Array(outcome.rows))
            .map_err(|err| TxError::internal(&format!("decode outbox status: {err}")))?;
        Ok(messages.pop().map(|msg| msg.status))
    }
}

#[derive(Clone)]
pub struct SurrealDeadStore {
    datastore: Arc<SurrealDatastore>,
}

impl SurrealDeadStore {
    pub fn new(datastore: Arc<SurrealDatastore>) -> Self {
        Self { datastore }
    }

    async fn session(&self) -> Result<SurrealSession, TxError> {
        map_result(self.datastore.session().await)
    }
}

#[async_trait]
impl DeadStore for SurrealDeadStore {
    async fn record(&self, letter: DeadLetter) -> Result<(), TxError> {
        let session = self.session().await?;
        let payload = mapper::encode_dead(&letter);
        map_result(
            session
                .query(
                    "UPSERT tx_dead_letter CONTENT $data RETURN NONE",
                    json!({"data": payload}),
                )
                .await,
        )?;
        Ok(())
    }

    async fn list(
        &self,
        tenant: &TenantId,
        kind: DeadKind,
        limit: u32,
    ) -> Result<Vec<DeadLetterRef>, TxError> {
        let session = self.session().await?;
        let outcome = map_result(
            session
                .query(
                    "SELECT letter_id, tenant, kind, last_error, note, stored_at FROM tx_dead_letter WHERE tenant = $tenant AND kind = $kind ORDER BY stored_at DESC LIMIT $limit",
                    json!({
                        "tenant": tenant.0,
                        "kind": mapper::encode_dead_kind(&kind),
                        "limit": limit.max(1),
                    }),
                )
                .await,
        )?;
        let letters = mapper::decode_dead_list(Value::Array(outcome.rows))
            .map_err(|err| TxError::internal(&format!("decode dead-letter list: {err}")))?;
        Ok(letters.into_iter().map(|entry| entry.reference).collect())
    }

    async fn inspect(&self, reference: &DeadLetterRef) -> Result<Option<DeadLetter>, TxError> {
        let session = self.session().await?;
        let outcome = map_result(
            session
                .query(
                    "SELECT letter_id, tenant, kind, last_error, note, stored_at FROM tx_dead_letter WHERE tenant = $tenant AND kind = $kind AND letter_id = $letter_id LIMIT 1",
                    mapper::encode_dead_ref(reference),
                )
                .await,
        )?;
        let mut letters = mapper::decode_dead_list(Value::Array(outcome.rows))
            .map_err(|err| TxError::internal(&format!("decode dead-letter inspect: {err}")))?;
        Ok(letters.pop())
    }

    async fn delete(&self, reference: &DeadLetterRef) -> Result<(), TxError> {
        let session = self.session().await?;
        map_result(
            session
                .query(
                    "DELETE tx_dead_letter WHERE tenant = $tenant AND kind = $kind AND letter_id = $letter_id",
                    mapper::encode_dead_ref(reference),
                )
                .await,
        )?;
        Ok(())
    }

    async fn quarantine(&self, reference: &DeadLetterRef, note: &str) -> Result<(), TxError> {
        let session = self.session().await?;
        let mut params = mapper::encode_dead_ref(reference);
        if let Value::Object(ref mut map) = params {
            map.insert("note".into(), Value::String(note.to_string()));
            map.insert("now".into(), Value::from(now_ms()));
        }
        map_result(
            session
                .query(
                    "UPDATE tx_dead_letter SET note = $note, stored_at = $now WHERE tenant = $tenant AND kind = $kind AND letter_id = $letter_id RETURN NONE",
                    params,
                )
                .await,
        )?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct SurrealIdempoStore {
    datastore: Arc<SurrealDatastore>,
}

impl SurrealIdempoStore {
    pub fn new(datastore: Arc<SurrealDatastore>) -> Self {
        Self { datastore }
    }

    async fn session(&self) -> Result<SurrealSession, TxError> {
        map_result(self.datastore.session().await)
    }
}

#[async_trait]
impl IdempoStore for SurrealIdempoStore {
    async fn check_and_put(
        &self,
        tenant: &TenantId,
        key: &str,
        hash: &str,
        ttl_ms: i64,
    ) -> Result<Option<String>, TxError> {
        let session = self.session().await?;
        let mut tx = map_result(session.begin().await)?;
        let now = now_ms();

        let outcome = map_result(
            tx.query(
                "SELECT tenant, key, hash, state, digest, error, expires_at, updated_at FROM tx_idempo WHERE tenant = $tenant AND key = $key LIMIT 1",
                json!({"tenant": tenant.0, "key": key}),
            )
            .await,
        )?;
        let records = outcome.rows;

        if let Some(item) = records.into_iter().find(|v| !v.is_null()) {
            let record = mapper::decode_idempo(item)
                .map_err(|err| TxError::internal(&format!("decode idempotency record: {err}")))?;
            let state = mapper::idempo_state_from_record(&record);
            match &state {
                IdempoState::Succeeded { hash: h, digest } => {
                    if h != hash {
                        map_result(tx.rollback().await)?;
                        return Err(TxError::conflict("idempotency hash mismatch"));
                    }
                    map_result(tx.commit().await)?;
                    return Ok(Some(digest.clone()));
                }
                IdempoState::InFlight {
                    hash: h,
                    expires_at,
                } => {
                    if h != hash {
                        map_result(tx.rollback().await)?;
                        return Err(TxError::conflict("idempotency hash mismatch"));
                    }
                    if *expires_at > now {
                        map_result(tx.commit().await)?;
                        return Ok(None);
                    }
                }
                IdempoState::Failed {
                    hash: h,
                    expires_at,
                    ..
                } => {
                    if h != hash {
                        map_result(tx.rollback().await)?;
                        return Err(TxError::conflict("idempotency hash mismatch"));
                    }
                    if *expires_at > now {
                        map_result(tx.rollback().await)?;
                        return Err(TxError::bad_request(
                            "idempotency record is temporarily locked after failure",
                        ));
                    }
                }
            }

            let new_state = IdempoState::InFlight {
                hash: hash.to_string(),
                expires_at: now + ttl_ms,
            };
            let payload = mapper::encode_idempo_record(tenant, key, new_state, now);
            map_result(
            tx.query(
                "UPDATE tx_idempo SET hash = $data.hash, state = $data.state, digest = $data.digest, error = $data.error, expires_at = $data.expires_at, updated_at = $data.updated_at WHERE tenant = $data.tenant AND key = $data.key RETURN NONE",
                json!({"data": payload}),
            )
            .await,
        )?;
            map_result(tx.commit().await)?;
            return Ok(None);
        }

        let state = IdempoState::InFlight {
            hash: hash.to_string(),
            expires_at: now + ttl_ms,
        };
        let payload = mapper::encode_idempo_record(tenant, key, state, now);
        map_result(
            tx.query(
                "UPSERT tx_idempo CONTENT $data RETURN NONE",
                json!({"data": payload}),
            )
            .await,
        )?;
        map_result(tx.commit().await)?;
        Ok(None)
    }

    async fn finish(
        &self,
        tenant: &TenantId,
        key: &str,
        hash: &str,
        digest: &str,
    ) -> Result<(), TxError> {
        let session = self.session().await?;
        let now = now_ms();
        let outcome = map_result(
            session
                .query(
                    "SELECT tenant, key, hash, state, digest, error, expires_at, updated_at FROM tx_idempo WHERE tenant = $tenant AND key = $key LIMIT 1",
                    json!({"tenant": tenant.0, "key": key}),
                )
                .await,
        )?;
        let records = outcome.rows;
        let item = records
            .into_iter()
            .find(|v| !v.is_null())
            .ok_or_else(|| TxError::not_found("idempotency entry not found"))?;
        let record = mapper::decode_idempo(item)
            .map_err(|err| TxError::internal(&format!("decode idempotency record: {err}")))?;
        if record.hash != hash {
            return Err(TxError::conflict("idempotency hash mismatch"));
        }
        map_result(
            session
                .query(
                    "UPDATE tx_idempo SET state = 'Succeeded', digest = $digest, error = NONE, hash = $hash, expires_at = NONE, updated_at = $now WHERE tenant = $tenant AND key = $key RETURN NONE",
                    json!({
                        "tenant": tenant.0,
                        "key": key,
                        "hash": hash,
                        "digest": digest,
                        "now": now,
                    }),
                )
                .await,
        )?;
        Ok(())
    }

    async fn fail(
        &self,
        tenant: &TenantId,
        key: &str,
        hash: &str,
        error: &str,
        ttl_ms: i64,
    ) -> Result<(), TxError> {
        let session = self.session().await?;
        let now = now_ms();
        let outcome = map_result(
            session
                .query(
                    "SELECT tenant, key, hash, state, digest, error, expires_at, updated_at FROM tx_idempo WHERE tenant = $tenant AND key = $key LIMIT 1",
                    json!({"tenant": tenant.0, "key": key}),
                )
                .await,
        )?;
        let records = match Value::Array(outcome.rows) {
            Value::Array(items) => items,
            Value::Null => Vec::new(),
            other => vec![other],
        };
        let item = records
            .into_iter()
            .find(|v| !v.is_null())
            .ok_or_else(|| TxError::not_found("idempotency entry not found"))?;
        let record = mapper::decode_idempo(item)
            .map_err(|err| TxError::internal(&format!("decode idempotency record: {err}")))?;
        if record.hash != hash {
            return Err(TxError::conflict("idempotency hash mismatch"));
        }
        map_result(
            session
                .query(
                    "UPDATE tx_idempo SET state = 'Failed', error = $error, hash = $hash, digest = NONE, expires_at = $expires, updated_at = $now WHERE tenant = $tenant AND key = $key RETURN NONE",
                    json!({
                        "tenant": tenant.0,
                        "key": key,
                        "hash": hash,
                        "error": error,
                        "expires": now + ttl_ms,
                        "now": now,
                    }),
                )
                .await,
        )?;
        Ok(())
    }

    async fn purge_expired(&self, now_ms: i64) -> Result<(), TxError> {
        let session = self.session().await?;
        map_result(
            session
                .query(
                    "DELETE tx_idempo WHERE expires_at != NONE AND expires_at <= $now",
                    json!({"now": now_ms}),
                )
                .await,
        )?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct SurrealTxStores {
    datastore: Arc<SurrealDatastore>,
}

impl SurrealTxStores {
    pub fn new(datastore: Arc<SurrealDatastore>) -> Self {
        Self { datastore }
    }

    pub fn outbox(&self) -> SurrealOutboxStore {
        SurrealOutboxStore::new(self.datastore.clone())
    }

    pub fn dead(&self) -> SurrealDeadStore {
        SurrealDeadStore::new(self.datastore.clone())
    }

    pub fn idempo(&self) -> SurrealIdempoStore {
        SurrealIdempoStore::new(self.datastore.clone())
    }
}
