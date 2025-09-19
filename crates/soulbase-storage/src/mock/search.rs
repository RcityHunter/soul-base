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
