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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct Doc {
        id: String,
        tenant: TenantId,
    }

    impl Entity for Doc {
        const TABLE: &'static str = "doc";

        fn id(&self) -> &str {
            &self.id
        }

        fn tenant(&self) -> &TenantId {
            &self.tenant
        }
    }

    #[test]
    fn distance_is_euclidean() {
        let dist = super::distance(&[0.0, 0.0], &[3.0, 4.0]);
        assert!((dist - 5.0).abs() < f32::EPSILON);
    }

    #[tokio::test]
    async fn knn_orders_by_distance_and_skips_dimension_mismatch() {
        let store = MockDatastore::new();
        let vectors: InMemoryVector<Doc> = InMemoryVector::new(&store);
        let tenant = TenantId("tenant-vector".into());

        store.store(
            "doc",
            &tenant,
            "a",
            json!({"id": "a", "tenant": tenant.0.clone()}),
        );
        store.store(
            "doc",
            &tenant,
            "b",
            json!({"id": "b", "tenant": tenant.0.clone()}),
        );

        vectors
            .upsert_vec(&tenant, "a", &[1.0, 0.0, 0.0])
            .await
            .unwrap();
        vectors
            .upsert_vec(&tenant, "b", &[0.0, 1.0, 0.0])
            .await
            .unwrap();
        store.upsert_vector("doc", &tenant, "extra", vec![1.0, 2.0]);

        let hits = vectors
            .knn(&tenant, &[1.0, 0.1, 0.0], 2, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].0.id, "a");
        assert!(hits[0].1 < hits[1].1);
    }

    #[tokio::test]
    async fn delete_removes_vectors() {
        let store = MockDatastore::new();
        let vectors: InMemoryVector<Doc> = InMemoryVector::new(&store);
        let tenant = TenantId("tenant-vector".into());

        store.store(
            "doc",
            &tenant,
            "a",
            json!({"id": "a", "tenant": tenant.0.clone()}),
        );

        vectors
            .upsert_vec(&tenant, "a", &[1.0, 0.0])
            .await
            .unwrap();
        vectors.delete_vec(&tenant, "a").await.unwrap();
        assert!(store.get_vector("doc", &tenant, "a").is_none());
    }
}
