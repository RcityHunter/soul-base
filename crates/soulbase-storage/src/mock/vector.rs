use super::datastore::MockDatastore;
use crate::errors::StorageError;
use crate::model::Entity;
use crate::spi::vector::VectorIndex;
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct InMemoryVector<E: Entity> {
    store: MockDatastore,
    table: &'static str,
    _marker: PhantomData<E>,
}

impl<E: Entity> InMemoryVector<E> {
    pub fn new(store: &MockDatastore) -> Self {
        Self {
            store: store.clone(),
            table: E::TABLE,
            _marker: PhantomData,
        }
    }
}

fn distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

#[async_trait]
impl<E> VectorIndex<E> for InMemoryVector<E>
where
    E: Entity + Send + Sync,
{
    async fn upsert_vec(
        &self,
        tenant: &TenantId,
        id: &str,
        vector: &[f32],
    ) -> Result<(), StorageError> {
        self.store
            .upsert_vector(self.table, tenant, id, vector.to_vec());
        Ok(())
    }

    async fn delete_vec(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError> {
        self.store.remove_vector(self.table, tenant, id);
        Ok(())
    }

    async fn knn(
        &self,
        tenant: &TenantId,
        vector: &[f32],
        k: usize,
        _filter: Option<serde_json::Value>,
    ) -> Result<Vec<(E, f32)>, StorageError> {
        let mut results = Vec::new();
        for (id, stored_vec) in self.store.iter_vectors(self.table, tenant) {
            if stored_vec.len() != vector.len() {
                continue;
            }
            if let Some(raw) = self.store.fetch(self.table, tenant, &id) {
                let entity: E = serde_json::from_value(raw)
                    .map_err(|e| StorageError::internal(&e.to_string()))?;
                let dist = distance(vector, &stored_vec);
                results.push((entity, dist));
            }
        }
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k);
        Ok(results)
    }
}
