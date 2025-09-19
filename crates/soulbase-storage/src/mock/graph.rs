use super::datastore::MockDatastore;
use crate::errors::StorageError;
use crate::model::Entity;
use crate::spi::graph::GraphStore;
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;

#[derive(Clone)]
pub struct InMemoryGraph {
    store: MockDatastore,
    table: &'static str,
}

impl InMemoryGraph {
    pub fn new<E: Entity>(store: &MockDatastore) -> Self {
        Self {
            store: store.clone(),
            table: E::TABLE,
        }
    }
}

#[async_trait]
impl<E> GraphStore<E> for InMemoryGraph
where
    E: Entity,
{
    async fn relate(
        &self,
        tenant: &TenantId,
        from: &str,
        label: &str,
        to: &str,
        props: serde_json::Value,
    ) -> Result<(), StorageError> {
        self.store
            .relate(self.table, tenant, from, label, to, props);
        Ok(())
    }

    async fn out(
        &self,
        tenant: &TenantId,
        from: &str,
        label: &str,
        limit: usize,
    ) -> Result<Vec<E>, StorageError> {
        let mut out = Vec::new();
        for (to, _props) in self.store.out(self.table, tenant, from, label) {
            if let Some(val) = self.store.fetch(self.table, tenant, &to) {
                let entity: E = serde_json::from_value(val)
                    .map_err(|e| StorageError::internal(&e.to_string()))?;
                out.push(entity);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    async fn detach(
        &self,
        tenant: &TenantId,
        from: &str,
        label: &str,
        to: &str,
    ) -> Result<(), StorageError> {
        self.store.detach(self.table, tenant, from, label, to);
        Ok(())
    }
}
