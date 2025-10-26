use crate::errors::StorageError;
use crate::model::{Entity, Page, QueryParams};
use async_trait::async_trait;
use soulbase_types::prelude::TenantId;

#[async_trait]
pub trait Repository<E: Entity>: Send + Sync {
    async fn create(&self, tenant: &TenantId, entity: &E) -> Result<(), StorageError>;
    async fn upsert(
        &self,
        tenant: &TenantId,
        id: &str,
        patch: serde_json::Value,
        session: Option<&str>,
    ) -> Result<E, StorageError>;
    async fn get(&self, tenant: &TenantId, id: &str) -> Result<Option<E>, StorageError>;
    async fn select(&self, tenant: &TenantId, params: QueryParams)
        -> Result<Page<E>, StorageError>;
    async fn delete(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError>;
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
        title: String,
        value: i32,
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
    struct MemoryRepo {
        items: Arc<Mutex<HashMap<(String, String), Doc>>>,
    }

    impl MemoryRepo {
        fn new() -> Self {
            Self::default()
        }

        fn key(tenant: &TenantId, id: &str) -> (String, String) {
            (tenant.0.clone(), id.to_string())
        }
    }

    #[async_trait]
    impl Repository<Doc> for MemoryRepo {
        async fn create(&self, tenant: &TenantId, entity: &Doc) -> Result<(), StorageError> {
            if entity.tenant() != tenant {
                return Err(StorageError::bad_request("tenant mismatch"));
            }
            let mut guard = self.items.lock().unwrap();
            let key = Self::key(tenant, entity.id());
            if guard.contains_key(&key) {
                return Err(StorageError::conflict("entity already exists"));
            }
            guard.insert(key, entity.clone());
            Ok(())
        }

        async fn upsert(
            &self,
            tenant: &TenantId,
            id: &str,
            patch: serde_json::Value,
            _session: Option<&str>,
        ) -> Result<Doc, StorageError> {
            let mut guard = self.items.lock().unwrap();
            let key = Self::key(tenant, id);
            let mut entity = guard
                .get(&key)
                .cloned()
                .unwrap_or_else(|| Doc {
                    id: id.to_string(),
                    tenant: tenant.clone(),
                    title: String::new(),
                    value: 0,
                });

            if let Some(title) = patch.get("title").and_then(|v| v.as_str()) {
                entity.title = title.to_string();
            }
            if let Some(val) = patch.get("value").and_then(|v| v.as_i64()) {
                entity.value = val as i32;
            }
            guard.insert(key, entity.clone());
            Ok(entity)
        }

        async fn get(&self, tenant: &TenantId, id: &str) -> Result<Option<Doc>, StorageError> {
            let guard = self.items.lock().unwrap();
            Ok(guard.get(&Self::key(tenant, id)).cloned())
        }

        async fn select(
            &self,
            tenant: &TenantId,
            params: QueryParams,
        ) -> Result<Page<Doc>, StorageError> {
            let guard = self.items.lock().unwrap();
            let limit = params.limit.unwrap_or(u32::MAX) as usize;
            let value_filter = params
                .filter
                .get("value")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32);

            let mut items = Vec::new();
            for doc in guard.values() {
                if doc.tenant != *tenant {
                    continue;
                }
                if let Some(filter_val) = value_filter {
                    if doc.value != filter_val {
                        continue;
                    }
                }
                items.push(doc.clone());
                if items.len() >= limit {
                    break;
                }
            }

            Ok(Page { items, next: None })
        }

        async fn delete(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError> {
            let mut guard = self.items.lock().unwrap();
            guard
                .remove(&Self::key(tenant, id))
                .map(|_| ())
                .ok_or_else(|| StorageError::not_found("entity not found"))
        }
    }

    fn tenant() -> TenantId {
        TenantId("tenant-spi".into())
    }

    #[tokio::test]
    async fn create_and_get_roundtrip() {
        let repo = MemoryRepo::new();
        let tenant = tenant();
        let doc = Doc {
            id: "doc-1".into(),
            tenant: tenant.clone(),
            title: "hello".into(),
            value: 1,
        };
        repo.create(&tenant, &doc).await.expect("create");
        let fetched = repo.get(&tenant, &doc.id).await.unwrap().unwrap();
        assert_eq!(fetched, doc);
    }

    #[tokio::test]
    async fn create_detects_conflict() {
        let repo = MemoryRepo::new();
        let tenant = tenant();
        let doc = Doc {
            id: "doc-1".into(),
            tenant: tenant.clone(),
            title: "hello".into(),
            value: 1,
        };
        repo.create(&tenant, &doc).await.unwrap();
        let err = repo.create(&tenant, &doc).await.expect_err("conflict");
        assert!(err.to_string().contains("entity already exists"));
    }

    #[tokio::test]
    async fn upsert_merges_patch_fields() {
        let repo = MemoryRepo::new();
        let tenant = tenant();
        repo.create(
            &tenant,
            &Doc {
                id: "doc-1".into(),
                tenant: tenant.clone(),
                title: "old".into(),
                value: 0,
            },
        )
        .await
        .unwrap();

        let updated = repo
            .upsert(
                &tenant,
                "doc-1",
                json!({"title": "new", "value": 42}),
                None,
            )
            .await
            .unwrap();
        assert_eq!(updated.title, "new");
        assert_eq!(updated.value, 42);
    }

    #[tokio::test]
    async fn select_applies_filter_and_limit() {
        let repo = MemoryRepo::new();
        let tenant = tenant();
        for value in 0..3 {
            let doc = Doc {
                id: format!("doc-{value}"),
                tenant: tenant.clone(),
                title: format!("title-{value}"),
                value,
            };
            repo.create(&tenant, &doc).await.unwrap();
        }

        let params = QueryParams {
            filter: json!({"value": 1}),
            limit: Some(1),
            ..Default::default()
        };
        let page = repo.select(&tenant, params).await.unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].value, 1);
    }

    #[tokio::test]
    async fn delete_errors_when_missing() {
        let repo = MemoryRepo::new();
        let tenant = tenant();
        let err = repo.delete(&tenant, "missing").await.expect_err("not found");
        assert!(err.to_string().contains("entity not found"));
    }
}
