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
