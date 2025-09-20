use crate::errors::TxError;
use crate::model::{MsgId, OutboxMessage, OutboxStatus};
use crate::outbox::OutboxStore;
use crate::util::now_ms;
use async_trait::async_trait;
use parking_lot::RwLock;
use soulbase_types::prelude::TenantId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct InMemoryOutboxStore {
    inner: Arc<RwLock<HashMap<(String, String), OutboxMessage>>>,
}

impl InMemoryOutboxStore {
    fn key(tenant: &TenantId, id: &MsgId) -> (String, String) {
        (tenant.0.clone(), id.0.clone())
    }
}

#[async_trait]
impl OutboxStore for InMemoryOutboxStore {
    async fn enqueue(&self, msg: OutboxMessage) -> Result<(), TxError> {
        let key = Self::key(&msg.tenant, &msg.id);
        let mut guard = self.inner.write();
        guard.insert(key, msg);
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
        let mut guard = self.inner.write();
        let mut leased = Vec::new();
        let mut seen_keys = HashSet::new();
        for message in guard.values_mut() {
            if message.tenant != *tenant {
                continue;
            }
            let ready = match &message.status {
                OutboxStatus::Pending => message.visible_at <= now_ms,
                OutboxStatus::Leased { lease_until, .. } => *lease_until <= now_ms,
                _ => false,
            };
            if !ready {
                continue;
            }
            if group_by_key {
                if let Some(dispatch_key) = &message.dispatch_key {
                    if !seen_keys.insert(dispatch_key.clone()) {
                        continue;
                    }
                }
            }
            message.status = OutboxStatus::Leased {
                worker: worker.to_string(),
                lease_until: now_ms + lease_ms as i64,
            };
            message.attempts = message.attempts.saturating_add(1);
            message.updated_at = now_ms;
            leased.push(message.clone());
            if leased.len() as u32 >= batch {
                break;
            }
        }
        Ok(leased)
    }

    async fn ack_done(&self, tenant: &TenantId, id: &MsgId) -> Result<(), TxError> {
        let key = Self::key(tenant, id);
        let mut guard = self.inner.write();
        if let Some(message) = guard.get_mut(&key) {
            message.status = OutboxStatus::Delivered;
            message.updated_at = now_ms();
            Ok(())
        } else {
            Err(TxError::not_found("outbox message not found"))
        }
    }

    async fn nack_backoff(
        &self,
        tenant: &TenantId,
        id: &MsgId,
        next_visible_at: i64,
        error: &str,
    ) -> Result<(), TxError> {
        let key = Self::key(tenant, id);
        let mut guard = self.inner.write();
        if let Some(message) = guard.get_mut(&key) {
            message.status = OutboxStatus::Pending;
            message.visible_at = next_visible_at;
            message.last_error = Some(error.to_string());
            message.updated_at = now_ms();
            Ok(())
        } else {
            Err(TxError::not_found("outbox message not found"))
        }
    }

    async fn dead_letter(&self, tenant: &TenantId, id: &MsgId, error: &str) -> Result<(), TxError> {
        let key = Self::key(tenant, id);
        let mut guard = self.inner.write();
        if let Some(message) = guard.get_mut(&key) {
            message.status = OutboxStatus::Dead;
            message.last_error = Some(error.to_string());
            message.updated_at = now_ms();
            Ok(())
        } else {
            Err(TxError::not_found("outbox message not found"))
        }
    }

    async fn requeue(&self, tenant: &TenantId, id: &MsgId) -> Result<(), TxError> {
        let key = Self::key(tenant, id);
        let mut guard = self.inner.write();
        if let Some(message) = guard.get_mut(&key) {
            message.status = OutboxStatus::Pending;
            message.visible_at = now_ms();
            message.last_error = None;
            message.updated_at = now_ms();
            Ok(())
        } else {
            Err(TxError::not_found("outbox message not found"))
        }
    }

    async fn status(&self, tenant: &TenantId, id: &MsgId) -> Result<Option<OutboxStatus>, TxError> {
        let key = Self::key(tenant, id);
        let guard = self.inner.read();
        Ok(guard.get(&key).map(|m| m.status.clone()))
    }
}
