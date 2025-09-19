use crate::errors::TxError;
use crate::model::{SagaId, SagaInstance};
use crate::saga::SagaStore;
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct InMemorySagaStore {
    inner: Arc<RwLock<HashMap<String, SagaInstance>>>,
}

#[async_trait]
impl SagaStore for InMemorySagaStore {
    async fn insert(&self, saga: SagaInstance) -> Result<SagaInstance, TxError> {
        let mut guard = self.inner.write();
        guard.insert(saga.id.0.clone(), saga.clone());
        Ok(saga)
    }

    async fn update(&self, saga: SagaInstance) -> Result<(), TxError> {
        let mut guard = self.inner.write();
        guard.insert(saga.id.0.clone(), saga);
        Ok(())
    }

    async fn load(&self, id: &SagaId) -> Result<Option<SagaInstance>, TxError> {
        Ok(self.inner.read().get(&id.0).cloned())
    }
}
