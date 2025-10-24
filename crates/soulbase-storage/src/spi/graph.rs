use crate::errors::StorageError;
use crate::model::Entity;
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;

#[async_trait]
pub trait GraphStore<E: Entity>: Send + Sync {
    async fn relate(
        &self,
        tenant: &TenantId,
        from: &str,
        label: &str,
        to: &str,
        props: serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn out(
        &self,
        tenant: &TenantId,
        from: &str,
        label: &str,
        limit: usize,
    ) -> Result<Vec<E>, StorageError>;
    async fn detach(
        &self,
        tenant: &TenantId,
        from: &str,
        label: &str,
        to: &str,
    ) -> Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
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
    struct MemoryGraph {
        nodes: Arc<Mutex<HashMap<(String, String), Doc>>>,
        edges: Arc<Mutex<HashMap<(String, String, String), Vec<(String, serde_json::Value)>>>>,
    }

    impl MemoryGraph {
        fn new() -> Self {
            Self::default()
        }

        fn node_key(tenant: &TenantId, id: &str) -> (String, String) {
            (tenant.0.clone(), id.to_string())
        }

        fn edge_key(tenant: &TenantId, from: &str, label: &str) -> (String, String, String) {
            (tenant.0.clone(), from.to_string(), label.to_string())
        }

        fn insert_node(&self, node: Doc) {
            self.nodes
                .lock()
                .unwrap()
                .insert(Self::node_key(&node.tenant, &node.id), node);
        }
    }

    #[async_trait]
    impl GraphStore<Doc> for MemoryGraph {
        async fn relate(
            &self,
            tenant: &TenantId,
            from: &str,
            label: &str,
            to: &str,
            props: serde_json::Value,
        ) -> Result<(), StorageError> {
            let mut edges = self.edges.lock().unwrap();
            edges
                .entry(Self::edge_key(tenant, from, label))
                .or_default()
                .push((to.to_string(), props));
            Ok(())
        }

        async fn out(
            &self,
            tenant: &TenantId,
            from: &str,
            label: &str,
            limit: usize,
        ) -> Result<Vec<Doc>, StorageError> {
            let edges = self.edges.lock().unwrap();
            let nodes = self.nodes.lock().unwrap();
            let mut out = Vec::new();
            if let Some(neighbors) = edges.get(&Self::edge_key(tenant, from, label)) {
                for (to, _) in neighbors.iter().take(limit) {
                    if let Some(node) = nodes.get(&Self::node_key(tenant, to)) {
                        out.push(node.clone());
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
            let mut edges = self.edges.lock().unwrap();
            if let Some(entries) = edges.get_mut(&Self::edge_key(tenant, from, label)) {
                entries.retain(|(target, _)| target != to);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn relate_out_and_detach_flow() {
        let graph = MemoryGraph::new();
        let tenant = TenantId("tenant-graph-spi".into());
        for id in ["a", "b", "c"] {
            graph.insert_node(Doc {
                id: id.into(),
                tenant: tenant.clone(),
            });
        }

        graph
            .relate(&tenant, "a", "likes", "b", json!({"weight": 1}))
            .await
            .unwrap();
        graph
            .relate(&tenant, "a", "likes", "c", json!({"weight": 2}))
            .await
            .unwrap();

        let out = graph.out(&tenant, "a", "likes", 10).await.unwrap();
        assert_eq!(out.len(), 2);

        graph
            .detach(&tenant, "a", "likes", "b")
            .await
            .unwrap();
        let remaining = graph.out(&tenant, "a", "likes", 10).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, "c");
    }
}
