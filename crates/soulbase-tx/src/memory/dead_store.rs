use crate::errors::TxError;
use crate::model::{DeadKind, DeadLetter, DeadLetterRef};
use crate::outbox::DeadStore;
use async_trait::async_trait;
use parking_lot::RwLock;
use soulbase_types::prelude::TenantId;
use std::collections::HashMap;
use std::sync::Arc;

type DeadStoreKey = (String, DeadKind, String);

#[derive(Default, Clone)]
pub struct InMemoryDeadStore {
    inner: Arc<RwLock<HashMap<DeadStoreKey, DeadLetter>>>,
}

impl InMemoryDeadStore {
    fn key(reference: &DeadLetterRef) -> DeadStoreKey {
        (
            reference.tenant.0.clone(),
            reference.kind.clone(),
            reference.id.0.clone(),
        )
    }
}

#[async_trait]
impl DeadStore for InMemoryDeadStore {
    async fn record(&self, letter: DeadLetter) -> Result<(), TxError> {
        let key = Self::key(&letter.reference);
        self.inner.write().insert(key, letter);
        Ok(())
    }

    async fn list(
        &self,
        tenant: &TenantId,
        kind: DeadKind,
        limit: u32,
    ) -> Result<Vec<DeadLetterRef>, TxError> {
        let guard = self.inner.read();
        let mut refs: Vec<_> = guard
            .values()
            .filter(|letter| letter.reference.tenant == *tenant && letter.reference.kind == kind)
            .map(|letter| letter.reference.clone())
            .collect();
        refs.sort_by_key(|r| (r.tenant.0.clone(), r.id.0.clone()));
        if refs.len() as u32 > limit {
            refs.truncate(limit as usize);
        }
        Ok(refs)
    }

    async fn inspect(&self, reference: &DeadLetterRef) -> Result<Option<DeadLetter>, TxError> {
        Ok(self.inner.read().get(&Self::key(reference)).cloned())
    }

    async fn delete(&self, reference: &DeadLetterRef) -> Result<(), TxError> {
        self.inner.write().remove(&Self::key(reference));
        Ok(())
    }

    async fn quarantine(&self, reference: &DeadLetterRef, note: &str) -> Result<(), TxError> {
        let mut guard = self.inner.write();
        if let Some(letter) = guard.get_mut(&Self::key(reference)) {
            letter.note = Some(note.to_string());
            Ok(())
        } else {
            Err(TxError::not_found("dead-letter not found"))
        }
    }
}
