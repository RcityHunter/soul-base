use crate::errors::TxError;
use crate::model::{DeadLetter, DeadLetterRef, MsgId};
use crate::outbox::DeadStore;
use async_trait::async_trait;
use parking_lot::RwLock;
use soulbase_types::prelude::TenantId;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct InMemoryDeadStore {
    inner: Arc<RwLock<HashMap<(String, String), DeadLetter>>>,
}

impl InMemoryDeadStore {
    fn key(tenant: &TenantId, id: &MsgId) -> (String, String) {
        (tenant.0.clone(), id.0.clone())
    }
}

#[async_trait]
impl DeadStore for InMemoryDeadStore {
    async fn push(&self, letter: DeadLetter) -> Result<(), TxError> {
        let key = Self::key(&letter.reference.tenant, &letter.reference.id);
        self.inner.write().insert(key, letter);
        Ok(())
    }

    async fn load(&self, tenant: &TenantId, id: &MsgId) -> Result<Option<DeadLetter>, TxError> {
        let key = Self::key(tenant, id);
        Ok(self.inner.read().get(&key).cloned())
    }

    async fn delete(&self, tenant: &TenantId, id: &MsgId) -> Result<(), TxError> {
        let key = Self::key(tenant, id);
        self.inner.write().remove(&key);
        Ok(())
    }
}

#[derive(Default, Clone)]
pub struct InMemoryDeadReplay<S>
where
    S: crate::outbox::OutboxStore,
{
    pub outbox: S,
    pub dead: InMemoryDeadStore,
}

impl<S> InMemoryDeadReplay<S>
where
    S: crate::outbox::OutboxStore,
{
    pub fn new(outbox: S, dead: InMemoryDeadStore) -> Self {
        Self { outbox, dead }
    }
}

#[async_trait]
impl<S> crate::outbox::DeadLetterReplay for InMemoryDeadReplay<S>
where
    S: crate::outbox::OutboxStore,
{
    async fn replay(&self, reference: &DeadLetterRef) -> Result<(), TxError> {
        let tenant = &reference.tenant;
        let id = &reference.id;
        if self.dead.load(tenant, id).await?.is_none() {
            return Err(TxError::not_found("dead-letter missing"));
        }
        self.outbox.requeue(tenant, id).await?;
        self.dead.delete(tenant, id).await?;
        Ok(())
    }
}
