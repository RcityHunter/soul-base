use crate::backoff::BackoffPolicy;
use crate::errors::TxError;
use crate::model::{DeadKind, DeadLetter, DeadLetterRef, MsgId, OutboxMessage};
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;
#[async_trait]
pub trait OutboxStore: Send + Sync {
    async fn enqueue(&self, msg: OutboxMessage) -> Result<(), TxError>;
    async fn lease_batch(
        &self,
        tenant: &TenantId,
        worker: &str,
        now_ms: i64,
        lease_ms: u64,
        batch: u32,
        group_by_key: bool,
    ) -> Result<Vec<OutboxMessage>, TxError>;
    async fn ack_done(&self, tenant: &TenantId, id: &MsgId) -> Result<(), TxError>;
    async fn nack_backoff(
        &self,
        tenant: &TenantId,
        id: &MsgId,
        next_visible_at: i64,
        error: &str,
    ) -> Result<(), TxError>;
    async fn dead_letter(&self, tenant: &TenantId, id: &MsgId, error: &str) -> Result<(), TxError>;
    async fn requeue(&self, tenant: &TenantId, id: &MsgId) -> Result<(), TxError>;
    async fn status(
        &self,
        tenant: &TenantId,
        id: &MsgId,
    ) -> Result<Option<crate::model::OutboxStatus>, TxError>;
}
#[async_trait]
pub trait DeadStore: Send + Sync {
    async fn record(&self, letter: DeadLetter) -> Result<(), TxError>;
    async fn list(
        &self,
        tenant: &TenantId,
        kind: DeadKind,
        limit: u32,
    ) -> Result<Vec<DeadLetterRef>, TxError>;
    async fn inspect(&self, reference: &DeadLetterRef) -> Result<Option<DeadLetter>, TxError>;
    async fn delete(&self, reference: &DeadLetterRef) -> Result<(), TxError>;
    async fn quarantine(&self, reference: &DeadLetterRef, note: &str) -> Result<(), TxError>;
}
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, topic: &str, payload: &serde_json::Value) -> Result<(), TxError>;
}
pub struct Dispatcher<T, S, D>
where
    T: Transport,
    S: OutboxStore,
    D: DeadStore,
{
    pub transport: T,
    pub store: S,
    pub dead: D,
    pub worker_id: String,
    pub lease_ms: u64,
    pub batch: u32,
    pub group_by_key: bool,
    pub backoff: Box<dyn BackoffPolicy + Send + Sync>,
}
impl<T, S, D> Dispatcher<T, S, D>
where
    T: Transport,
    S: OutboxStore,
    D: DeadStore,
{
    pub async fn tick(&self, tenant: &TenantId, now_ms: i64) -> Result<(), TxError> {
        let messages = self
            .store
            .lease_batch(
                tenant,
                &self.worker_id,
                now_ms,
                self.lease_ms,
                self.batch,
                self.group_by_key,
            )
            .await?;
        for msg in messages {
            match self.transport.send(&msg.channel, &msg.payload).await {
                Ok(_) => {
                    self.store.ack_done(tenant, &msg.id).await?;
                }
                Err(err) => {
                    let err_obj = err.into_inner();
                    let err_msg = err_obj
                        .message_dev
                        .clone()
                        .unwrap_or_else(|| err_obj.message_user.clone());
                    let attempts = msg.attempts;
                    if !self.backoff.allowed(attempts) {
                        self.store.dead_letter(tenant, &msg.id, &err_msg).await?;
                        let dead_letter = DeadLetter {
                            reference: DeadLetterRef {
                                tenant: tenant.clone(),
                                kind: DeadKind::Outbox,
                                id: msg.id.clone(),
                            },
                            last_error: Some(err_msg),
                            stored_at: now_ms,
                            note: None,
                        };
                        self.dead.record(dead_letter).await?;
                    } else {
                        let next_attempt_at = self.backoff.next_after(now_ms, attempts);
                        self.store
                            .nack_backoff(tenant, &msg.id, next_attempt_at, &err_msg)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }
}
