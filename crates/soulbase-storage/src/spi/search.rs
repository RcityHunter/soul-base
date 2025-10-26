use crate::errors::StorageError;
use crate::model::Page;
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;

#[async_trait]
pub trait SearchStore: Send + Sync {
    async fn search(
        &self,
        tenant: &TenantId,
        query: &str,
        limit: usize,
    ) -> Result<Page<serde_json::Value>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Arc;

    #[derive(Clone)]
    struct MemorySearch {
        tenant: TenantId,
        items: Arc<Vec<serde_json::Value>>,
    }

    impl MemorySearch {
        fn new(tenant: TenantId, items: Vec<serde_json::Value>) -> Self {
            Self {
                tenant,
                items: Arc::new(items),
            }
        }
    }

    #[async_trait]
    impl SearchStore for MemorySearch {
        async fn search(
            &self,
            tenant: &TenantId,
            query: &str,
            limit: usize,
        ) -> Result<Page<serde_json::Value>, StorageError> {
            if tenant != &self.tenant {
                return Ok(Page { items: Vec::new(), next: None });
            }
            let normalized = query.trim().to_lowercase();
            let mut hits = Vec::new();
            for item in self.items.iter() {
                if hits.len() >= limit {
                    break;
                }
                if normalized.is_empty()
                    || item
                        .to_string()
                        .to_lowercase()
                        .contains(&normalized)
                {
                    hits.push(item.clone());
                }
            }
            Ok(Page { items: hits, next: None })
        }
    }

    #[tokio::test]
    async fn search_applies_tenant_and_query_filter() {
        let tenant = TenantId("tenant-search-spi".into());
        let items = vec![
            json!({"id": 1, "title": "Hello"}),
            json!({"id": 2, "title": "world"}),
            json!({"id": 3, "title": "HELLO again"}),
        ];
        let store = MemorySearch::new(tenant.clone(), items);

        let page = store
            .search(&tenant, "hello", 2)
            .await
            .expect("search ok");
        assert_eq!(page.items.len(), 2);
        assert!(page.items.iter().all(|item| item.to_string().to_lowercase().contains("hello")));

        let other = store
            .search(&TenantId("other".into()), "hello", 2)
            .await
            .expect("other tenant");
        assert!(other.items.is_empty());
    }
}
