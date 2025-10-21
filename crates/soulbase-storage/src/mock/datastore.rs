use crate::errors::StorageError;
use crate::spi::datastore::Datastore;
use async_trait::async_trait;
use parking_lot::RwLock;
use soulbase_types::prelude::TenantId;
use std::collections::HashMap;
use std::sync::Arc;

use super::session::MockSession;

#[derive(Clone, Default)]
pub struct MockDatastore {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    records: RwLock<HashMap<String, HashMap<String, serde_json::Value>>>,
    edges: RwLock<HashMap<String, Vec<EdgeRecord>>>,
    vectors: RwLock<HashMap<String, Vec<f32>>>,
}

#[derive(Clone, Debug)]
struct EdgeRecord {
    to: String,
    props: serde_json::Value,
}

impl MockDatastore {
    pub fn new() -> Self {
        Self::default()
    }

    fn table_key(table: &str, tenant: &TenantId) -> String {
        format!("{}::{}", table, tenant.0)
    }

    fn edge_key(table: &str, tenant: &TenantId, from: &str, label: &str) -> String {
        format!("{}::{}::{}::{}", table, tenant.0, from, label)
    }

    fn vector_key(table: &str, tenant: &TenantId, id: &str) -> String {
        format!("{}::{}::{}", table, tenant.0, id)
    }

    pub fn store(&self, table: &str, tenant: &TenantId, id: &str, value: serde_json::Value) {
        let key = Self::table_key(table, tenant);
        let mut map = self.inner.records.write();
        map.entry(key).or_default().insert(id.to_string(), value);
    }

    pub fn fetch(&self, table: &str, tenant: &TenantId, id: &str) -> Option<serde_json::Value> {
        let key = Self::table_key(table, tenant);
        self.inner
            .records
            .read()
            .get(&key)
            .and_then(|m| m.get(id).cloned())
    }

    pub fn remove(&self, table: &str, tenant: &TenantId, id: &str) -> Option<serde_json::Value> {
        let key = Self::table_key(table, tenant);
        self.inner
            .records
            .write()
            .get_mut(&key)
            .and_then(|m| m.remove(id))
    }

    pub fn list(&self, table: &str, tenant: &TenantId) -> Vec<serde_json::Value> {
        let key = Self::table_key(table, tenant);
        self.inner
            .records
            .read()
            .get(&key)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn relate(
        &self,
        table: &str,
        tenant: &TenantId,
        from: &str,
        label: &str,
        to: &str,
        props: serde_json::Value,
    ) {
        let key = Self::edge_key(table, tenant, from, label);
        let mut edges = self.inner.edges.write();
        edges.entry(key).or_default().push(EdgeRecord {
            to: to.to_string(),
            props,
        });
    }

    pub fn out(
        &self,
        table: &str,
        tenant: &TenantId,
        from: &str,
        label: &str,
    ) -> Vec<(String, serde_json::Value)> {
        let key = Self::edge_key(table, tenant, from, label);
        self.inner
            .edges
            .read()
            .get(&key)
            .map(|edges| {
                edges
                    .iter()
                    .map(|e| (e.to.clone(), e.props.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn detach(&self, table: &str, tenant: &TenantId, from: &str, label: &str, to: &str) {
        let key = Self::edge_key(table, tenant, from, label);
        if let Some(bucket) = self.inner.edges.write().get_mut(&key) {
            bucket.retain(|edge| edge.to != to);
        }
    }

    pub fn upsert_vector(&self, table: &str, tenant: &TenantId, id: &str, vector: Vec<f32>) {
        let key = Self::vector_key(table, tenant, id);
        self.inner.vectors.write().insert(key, vector);
    }

    pub fn get_vector(&self, table: &str, tenant: &TenantId, id: &str) -> Option<Vec<f32>> {
        let key = Self::vector_key(table, tenant, id);
        self.inner.vectors.read().get(&key).cloned()
    }

    pub fn remove_vector(&self, table: &str, tenant: &TenantId, id: &str) {
        let key = Self::vector_key(table, tenant, id);
        self.inner.vectors.write().remove(&key);
    }

    pub fn iter_vectors(&self, table: &str, tenant: &TenantId) -> Vec<(String, Vec<f32>)> {
        let prefix = format!("{}::{}::", table, tenant.0);
        self.inner
            .vectors
            .read()
            .iter()
            .filter_map(|(k, v)| {
                if k.starts_with(&prefix) {
                    let id = k[prefix.len()..].to_string();
                    Some((id, v.clone()))
                } else {
                    None
                }
            })
            .collect()
    }
}

#[async_trait]
impl Datastore for MockDatastore {
    type Session = MockSession;

    async fn session(&self) -> Result<Self::Session, StorageError> {
        Ok(MockSession::new(self.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use soulbase_types::prelude::TenantId;

    fn tenant() -> TenantId {
        TenantId("tenant-mock".into())
    }

    #[tokio::test]
    async fn records_cycle_through_store_fetch_remove() {
        let store = MockDatastore::new();
        let tenant = tenant();
        store.store("doc", &tenant, "id-1", json!({"field": 1}));

        assert_eq!(
            store.fetch("doc", &tenant, "id-1").unwrap(),
            json!({"field": 1})
        );
        assert_eq!(store.list("doc", &tenant).len(), 1);

        let removed = store.remove("doc", &tenant, "id-1").unwrap();
        assert_eq!(removed, json!({"field": 1}));
        assert!(store.fetch("doc", &tenant, "id-1").is_none());
    }

    #[tokio::test]
    async fn relations_attach_and_detach() {
        let store = MockDatastore::new();
        let tenant = tenant();
        store.store("doc", &tenant, "from", json!({"id": "from"}));
        store.store("doc", &tenant, "to", json!({"id": "to"}));

        store.relate(
            "doc",
            &tenant,
            "from",
            "likes",
            "to",
            json!({"weight": 2}),
        );

        let outbound = store.out("doc", &tenant, "from", "likes");
        assert_eq!(outbound.len(), 1);
        assert_eq!(outbound[0].0, "to");
        assert_eq!(outbound[0].1, json!({"weight": 2}));

        store.detach("doc", &tenant, "from", "likes", "to");
        assert!(store.out("doc", &tenant, "from", "likes").is_empty());
    }

    #[tokio::test]
    async fn vectors_support_upsert_iteration_and_remove() {
        let store = MockDatastore::new();
        let tenant = tenant();

        store.upsert_vector("vec", &tenant, "v1", vec![1.0, 0.0]);
        store.upsert_vector("vec", &tenant, "v2", vec![0.0, 1.0]);

        let iterated = store.iter_vectors("vec", &tenant);
        assert_eq!(iterated.len(), 2);

        assert_eq!(store.get_vector("vec", &tenant, "v1"), Some(vec![1.0, 0.0]));

        store.remove_vector("vec", &tenant, "v1");
        assert!(store.get_vector("vec", &tenant, "v1").is_none());
    }
}
