use super::datastore::MockDatastore;
use crate::errors::StorageError;
use crate::model::Entity;
use crate::spi::graph::GraphStore;
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct InMemoryGraph<E: Entity> {
    store: MockDatastore,
    table: &'static str,
    _marker: PhantomData<E>,
}

impl<E: Entity> InMemoryGraph<E> {
    pub fn new(store: &MockDatastore) -> Self {
        Self {
            store: store.clone(),
            table: E::TABLE,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<E> GraphStore<E> for InMemoryGraph<E>
where
    E: Entity + Send + Sync,
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

    #[tokio::test]
    async fn relate_and_detach_flow() {
        let store = MockDatastore::new();
        let graph: InMemoryGraph<Doc> = InMemoryGraph::new(&store);
        let tenant = TenantId("tenant-graph".into());

        for id in ["a", "b", "c"] {
            store.store(
                "doc",
                &tenant,
                id,
                json!({"id": id, "tenant": tenant.0.clone()}),
            );
        }

        graph
            .relate(&tenant, "a", "edge", "b", json!({"rank": 1}))
            .await
            .unwrap();
        graph
            .relate(&tenant, "a", "edge", "c", json!({"rank": 2}))
            .await
            .unwrap();

        let out = graph.out(&tenant, "a", "edge", 1).await.unwrap();
        assert_eq!(out.len(), 1);

        graph
            .detach(&tenant, "a", "edge", "b")
            .await
            .unwrap();
        let remaining = graph.out(&tenant, "a", "edge", 10).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, "c");
    }
}
