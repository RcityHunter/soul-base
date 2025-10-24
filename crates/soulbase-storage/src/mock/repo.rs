use super::datastore::MockDatastore;
use crate::errors::StorageError;
use crate::model::{Entity, Page, QueryParams};
use crate::spi::repo::Repository;
use async_trait::async_trait;
use serde_json::{Map, Value};
use soulbase_types::prelude::TenantId;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct InMemoryRepository<E: Entity> {
    store: MockDatastore,
    table: &'static str,
    _marker: PhantomData<E>,
}

impl<E: Entity> InMemoryRepository<E> {
    pub fn new(store: &MockDatastore) -> Self {
        Self {
            store: store.clone(),
            table: E::TABLE,
            _marker: PhantomData,
        }
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
        title: String,
        count: u32,
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

    fn tenant() -> TenantId {
        TenantId("tenant-repo".into())
    }

    #[test]
    fn merge_patch_overwrites_nested_values() {
        let mut base = json!({"a": {"b": 1, "c": 2}});
        let patch = json!({"a": {"b": 3}});
        merge_patch(&mut base, &patch);
        assert_eq!(base, json!({"a": {"b": 3, "c": 2}}));
    }

    #[test]
    fn matches_filter_honors_missing_keys() {
        let value = json!({"tenant": "t", "id": "1"});
        let filter = json!({"tenant": "t", "id": "missing"});
        assert!(!matches_filter(&value, &filter));
        assert!(matches_filter(&value, &json!({"tenant": "t"})));
    }

    #[tokio::test]
    async fn create_checks_for_conflicts_and_tenant() {
        let store = MockDatastore::new();
        let tenant = tenant();
        let repo: InMemoryRepository<Doc> = InMemoryRepository::new(&store);

        let doc = Doc {
            id: "doc-1".into(),
            tenant: tenant.clone(),
            title: "hello".into(),
            count: 1,
        };

        repo.create(&tenant, &doc).await.expect("first insert");
        let duplicate = repo.create(&tenant, &doc).await.expect_err("conflict");
        assert!(duplicate.to_string().contains("entity already exists"));

        let other_tenant = TenantId("other".into());
        let mismatch = Doc { tenant: other_tenant.clone(), ..doc.clone() };
        let err = repo.create(&tenant, &mismatch).await.expect_err("tenant mismatch");
        assert!(err.to_string().contains("tenant mismatch"));
    }

    #[tokio::test]
    async fn upsert_merges_patch_and_normalizes_fields() {
        let store = MockDatastore::new();
        let repo: InMemoryRepository<Doc> = InMemoryRepository::new(&store);
        let tenant = tenant();

        let updated = repo
            .upsert(
                &tenant,
                "doc-2",
                json!({"title": "new", "count": 5}),
                None,
            )
            .await
            .expect("upsert");
        assert_eq!(updated.id, "doc-2");
        assert_eq!(updated.tenant, tenant);
        assert_eq!(updated.title, "new");
        assert_eq!(updated.count, 5);

        let patched = repo
            .upsert(
                &tenant,
                "doc-2",
                json!({"count": 7}),
                None,
            )
            .await
            .expect("patch existing");
        assert_eq!(patched.count, 7);
    }

    #[tokio::test]
    async fn select_respects_filter_and_limit() {
        let store = MockDatastore::new();
        let repo: InMemoryRepository<Doc> = InMemoryRepository::new(&store);
        let tenant = tenant();

        for idx in 0..3 {
            let doc = Doc {
                id: format!("doc-{}", idx),
                tenant: tenant.clone(),
                title: if idx % 2 == 0 { "even".into() } else { "odd".into() },
                count: idx,
            };
            repo.create(&tenant, &doc).await.unwrap();
        }

        let params = QueryParams {
            filter: json!({"title": "even"}),
            limit: Some(1),
            ..Default::default()
        };
        let page = repo.select(&tenant, params).await.unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].title, "even");
    }

    #[tokio::test]
    async fn delete_errors_when_missing() {
        let store = MockDatastore::new();
        let repo: InMemoryRepository<Doc> = InMemoryRepository::new(&store);
        let tenant = tenant();
        let err = repo.delete(&tenant, "missing").await.expect_err("not found");
        assert!(err.to_string().contains("entity not found"));
    }
}

fn merge_patch(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(target_map), Value::Object(patch_map)) => {
            for (k, v) in patch_map {
                merge_patch(target_map.entry(k).or_insert(Value::Null), v);
            }
        }
        (slot, value) => {
            *slot = value.clone();
        }
    }
}

fn matches_filter(value: &Value, filter: &Value) -> bool {
    match (value, filter) {
        (Value::Object(data), Value::Object(filter_map)) => {
            filter_map.iter().all(|(k, expected)| {
                data.get(k)
                    .map(|actual| actual == expected)
                    .unwrap_or(false)
            })
        }
        (_, Value::Null) => true,
        (_, Value::Object(map)) if map.is_empty() => true,
        _ => true,
    }
}

#[async_trait]
impl<E> Repository<E> for InMemoryRepository<E>
where
    E: Entity + Send + Sync,
{
    async fn create(&self, tenant: &TenantId, entity: &E) -> Result<(), StorageError> {
        if entity.tenant() != tenant {
            return Err(StorageError::bad_request("tenant mismatch"));
        }
        if self.store.fetch(self.table, tenant, entity.id()).is_some() {
            return Err(StorageError::conflict("entity already exists"));
        }
        let value =
            serde_json::to_value(entity).map_err(|e| StorageError::internal(&e.to_string()))?;
        self.store.store(self.table, tenant, entity.id(), value);
        Ok(())
    }

    async fn upsert(
        &self,
        tenant: &TenantId,
        id: &str,
        patch: Value,
        _session: Option<&str>,
    ) -> Result<E, StorageError> {
        let mut base = self
            .store
            .fetch(self.table, tenant, id)
            .unwrap_or_else(|| Value::Object(Map::new()));
        merge_patch(&mut base, &patch);
        let mut map = base.as_object().cloned().unwrap_or_default();
        map.insert("id".into(), Value::String(id.to_string()));
        map.insert("tenant".into(), Value::String(tenant.0.clone()));
        let normalized = Value::Object(map);
        let entity: E = serde_json::from_value(normalized.clone())
            .map_err(|e| StorageError::internal(&e.to_string()))?;
        self.store.store(self.table, tenant, id, normalized);
        Ok(entity)
    }

    async fn get(&self, tenant: &TenantId, id: &str) -> Result<Option<E>, StorageError> {
        let value = self.store.fetch(self.table, tenant, id);
        Ok(match value {
            Some(val) => Some(
                serde_json::from_value(val).map_err(|e| StorageError::internal(&e.to_string()))?,
            ),
            None => None,
        })
    }

    async fn select(
        &self,
        tenant: &TenantId,
        params: QueryParams,
    ) -> Result<Page<E>, StorageError> {
        let values = self.store.list(self.table, tenant);
        let mut items = Vec::new();
        let limit = params.limit.unwrap_or(u32::MAX) as usize;
        for value in values {
            if !matches_filter(&value, &params.filter) {
                continue;
            }
            let entity: E = serde_json::from_value(value)
                .map_err(|e| StorageError::internal(&e.to_string()))?;
            items.push(entity);
            if items.len() >= limit {
                break;
            }
        }
        Ok(Page { items, next: None })
    }

    async fn delete(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError> {
        self.store
            .remove(self.table, tenant, id)
            .ok_or_else(|| StorageError::not_found("entity not found"))?;
        Ok(())
    }
}
