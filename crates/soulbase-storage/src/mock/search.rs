use super::datastore::MockDatastore;
use crate::errors::StorageError;
use crate::model::{Entity, Page};
use crate::spi::search::SearchStore;
use async_trait::async_trait;
use serde_json::Value;
use soulbase_types::prelude::TenantId;

#[derive(Clone)]
pub struct InMemorySearch {
    store: MockDatastore,
    table: &'static str,
}

impl InMemorySearch {
    pub fn new<E: Entity>(store: &MockDatastore) -> Self {
        Self {
            store: store.clone(),
            table: E::TABLE,
        }
    }
}

#[async_trait]
impl SearchStore for InMemorySearch {
    async fn search(
        &self,
        tenant: &TenantId,
        query: &str,
        limit: usize,
    ) -> Result<Page<Value>, StorageError> {
        let mut hits = Vec::new();
        let matcher = query.trim().to_lowercase();
        for value in self.store.list(self.table, tenant) {
            if hits.len() >= limit {
                break;
            }
            if matcher.is_empty() {
                hits.push(value);
                continue;
            }
            if value.to_string().to_lowercase().contains(&matcher) {
                hits.push(value);
            }
        }
        Ok(Page {
            items: hits,
            next: None,
        })
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
    async fn search_is_case_insensitive_and_limited() {
        let store = MockDatastore::new();
        let tenant = TenantId("tenant-search".into());
        for (id, title) in [("a", "Hello World"), ("b", "Other"), ("c", "HELLO AGAIN")] {
            store.store(
                "doc",
                &tenant,
                id,
                json!({"id": id, "tenant": tenant.0.clone(), "title": title}),
            );
        }

        let search = InMemorySearch::new::<Doc>(&store);
        let page = search.search(&tenant, "hello", 1).await.unwrap();
        assert_eq!(page.items.len(), 1);
        assert!(page.items[0].to_string().to_lowercase().contains("hello"));

        let page_all = search.search(&tenant, " ", 10).await.unwrap();
        assert_eq!(page_all.items.len(), 3);
    }
}
