use crate::errors::StorageError;
use crate::model::Entity;
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;

#[async_trait]
pub trait VectorIndex<E: Entity>: Send + Sync {
    async fn upsert_vec(
        &self,
        tenant: &TenantId,
        id: &str,
        vector: &[f32],
    ) -> Result<(), StorageError>;
    async fn delete_vec(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError>;
    async fn knn(
        &self,
        tenant: &TenantId,
        vector: &[f32],
        k: usize,
        filter: Option<serde_json::Value>,
    ) -> Result<Vec<(E, f32)>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

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

    #[derive(Clone, Default)]
    struct MemoryVector {
        vectors: Arc<Mutex<HashMap<(String, String), Vec<f32>>>>,
        entities: Arc<Mutex<HashMap<(String, String), Doc>>>,
    }

    impl MemoryVector {
        fn new() -> Self {
            Self::default()
        }

        fn key(tenant: &TenantId, id: &str) -> (String, String) {
            (tenant.0.clone(), id.to_string())
        }

        fn insert_entity(&self, entity: Doc) {
            let key = Self::key(&entity.tenant, &entity.id);
            self.entities.lock().unwrap().insert(key, entity);
        }
    }

    #[async_trait]
    impl VectorIndex<Doc> for MemoryVector {
        async fn upsert_vec(
            &self,
            tenant: &TenantId,
            id: &str,
            vector: &[f32],
        ) -> Result<(), StorageError> {
            self.vectors
                .lock()
                .unwrap()
                .insert(Self::key(tenant, id), vector.to_vec());
            Ok(())
        }

        async fn delete_vec(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError> {
            self.vectors
                .lock()
                .unwrap()
                .remove(&Self::key(tenant, id));
            Ok(())
        }

        async fn knn(
            &self,
            tenant: &TenantId,
            vector: &[f32],
            k: usize,
            _filter: Option<serde_json::Value>,
        ) -> Result<Vec<(Doc, f32)>, StorageError> {
            let vectors = self.vectors.lock().unwrap();
            let entities = self.entities.lock().unwrap();
            let mut results = Vec::new();

            for (key, stored_vec) in vectors.iter() {
                if &key.0 != &tenant.0 {
                    continue;
                }
                if stored_vec.len() != vector.len() {
                    continue;
                }
                if let Some(entity) = entities.get(key) {
                    let dist = stored_vec
                        .iter()
                        .zip(vector.iter())
                        .map(|(a, b)| (a - b).powi(2))
                        .sum::<f32>()
                        .sqrt();
                    results.push((entity.clone(), dist));
                }
            }

            results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            results.truncate(k);
            Ok(results)
        }
    }

    fn tenant() -> TenantId {
        TenantId("tenant-vector-spi".into())
    }

    #[tokio::test]
    async fn knn_orders_results_and_truncates() {
        let index = MemoryVector::new();
        let tenant = tenant();
        index.insert_entity(Doc {
            id: "a".into(),
            tenant: tenant.clone(),
        });
        index.insert_entity(Doc {
            id: "b".into(),
            tenant: tenant.clone(),
        });

        index
            .upsert_vec(&tenant, "a", &[1.0, 0.0, 0.0])
            .await
            .unwrap();
        index
            .upsert_vec(&tenant, "b", &[0.0, 1.0, 0.0])
            .await
            .unwrap();

        let hits = index
            .knn(&tenant, &[1.0, 0.2, 0.0], 1, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0.id, "a");
    }

    #[tokio::test]
    async fn delete_removes_vector_entry() {
        let index = MemoryVector::new();
        let tenant = tenant();
        index.insert_entity(Doc {
            id: "a".into(),
            tenant: tenant.clone(),
        });
        index.upsert_vec(&tenant, "a", &[1.0, 0.0]).await.unwrap();
        index.delete_vec(&tenant, "a").await.unwrap();
        let hits = index
            .knn(&tenant, &[1.0, 0.0], 1, None)
            .await
            .unwrap();
        assert!(hits.is_empty());
    }
}
