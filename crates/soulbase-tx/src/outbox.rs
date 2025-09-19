use crate::backoff::Backoff;
use crate::errors::TxError;
use crate::model::{DeadLetter, DeadLetterRef, MsgId, OutboxMessage, OutboxStatus};
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;

#[async_trait]
pub trait OutboxStore: Send + Sync {
    async fn enqueue(&self, msg: OutboxMessage) -> Result<(), TxError>;
    async fn lease_batch(
        &self,
        tenant: &TenantId,
        now_ms: i64,
        worker: &str,
        batch: usize,
        lease_ms: i64,
    ) -> Result<Vec<OutboxMessage>, TxError>;
    async fn ack(&self, tenant: &TenantId, id: &MsgId) -> Result<(), TxError>;
    async fn retry(
        &self,
        tenant: &TenantId,
        id: &MsgId,
        error: &str,
        next_visible_at: i64,
    ) -> Result<(), TxError>;
    async fn dead(&self, tenant: &TenantId, id: &MsgId, error: &str) -> Result<(), TxError>;
    async fn requeue(&self, tenant: &TenantId, id: &MsgId) -> Result<(), TxError>;
    async fn status(&self, tenant: &TenantId, id: &MsgId) -> Result<Option<OutboxStatus>, TxError>;
}

#[async_trait]
pub trait DeadStore: Send + Sync {
    async fn push(&self, letter: DeadLetter) -> Result<(), TxError>;
    async fn load(&self, tenant: &TenantId, id: &MsgId) -> Result<Option<DeadLetter>, TxError>;
    async fn delete(&self, tenant: &TenantId, id: &MsgId) -> Result<(), TxError>;
}

#[async_trait]
pub trait DeadLetterReplay: Send + Sync {
    async fn replay(&self, letter: &DeadLetterRef) -> Result<(), TxError>;
}

#[async_trait]
pub trait OutboxTransport: Send + Sync {
    async fn deliver(&self, tenant: &TenantId, message: &OutboxMessage) -> Result<(), TxError>;
}

pub struct Dispatcher<T, S, D, B>
where
    T: OutboxTransport,
    S: OutboxStore,
    D: DeadStore,
    B: Backoff,
{
    pub transport: T,
    pub store: S,
    pub dead: D,
    pub worker_id: String,
    pub max_attempts: u32,
    pub lease_ms: i64,
    pub batch: usize,
    pub backoff: B,
}

impl<T, S, D, B> Dispatcher<T, S, D, B>
where
    T: OutboxTransport,
    S: OutboxStore,
    D: DeadStore,
    B: Backoff,
{
    pub async fn tick(&self, tenant: &TenantId, now_ms: i64) -> Result<(), TxError> {
        let messages = self
            .store
            .lease_batch(tenant, now_ms, &self.worker_id, self.batch, self.lease_ms)
            .await?;

        for msg in messages {
            let attempts = msg.attempts;
            match self.transport.deliver(tenant, &msg).await {
                Ok(_) => {
                    self.store.ack(tenant, &msg.id).await?;
                }
                Err(err) => {
                    let err_obj = err.into_inner();
                    let err_msg = err_obj
                        .message_dev
                        .clone()
                        .unwrap_or_else(|| err_obj.message_user.clone());
                    let max = self.max_attempts.min(self.backoff.max_attempts());
                    if attempts >= max {
                        self.store.dead(tenant, &msg.id, &err_msg).await?;
                        let dead_letter = DeadLetter {
                            reference: DeadLetterRef {
                                tenant: tenant.clone(),
                                id: msg.id.clone(),
                            },
                            last_error: Some(err_msg),
                            stored_at: now_ms,
                        };
                        self.dead.push(dead_letter).await?;
                    } else {
                        let delay = self.backoff.next_delay_ms(attempts);
                        self.store
                            .retry(tenant, &msg.id, &err_msg, now_ms + delay)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }
}
