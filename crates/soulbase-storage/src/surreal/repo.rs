use super::datastore::SurrealDatastore;
use crate::errors::StorageError;
use crate::model::{Entity, Page, QueryParams};
use crate::spi::datastore::Datastore;
use crate::spi::query::QueryExecutor;
use crate::spi::repo::Repository;
use async_trait::async_trait;
use serde_json::{json, Map, Value};
use soulbase_types::prelude::TenantId;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct SurrealRepository<E: Entity> {
    datastore: SurrealDatastore,
    table: &'static str,
    _marker: PhantomData<E>,
}

impl<E: Entity> SurrealRepository<E> {
    pub fn new(datastore: &SurrealDatastore) -> Self {
        Self {
            datastore: datastore.clone(),
            table: E::TABLE,
            _marker: PhantomData,
        }
    }

    fn record_key(&self, id: &str) -> String {
        let prefix = format!("{}:", self.table);
        id.strip_prefix(&prefix).unwrap_or(id).to_string()
    }

    async fn run_query(&self, statement: &str, params: Value) -> Result<Vec<Value>, StorageError> {
        let session = self.datastore.session().await?;
        let value = session.query(statement, params).await?;
        match value {
            Value::Array(rows) => Ok(rows),
            other => Err(StorageError::internal(&format!(
                "surreal query expected array rows, got {other}"
            ))),
        }
    }

    async fn ensure_not_exists(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError> {
        let statement = r#"
            SELECT id
            FROM type::thing($table, $key)
            WHERE tenant = $tenant
            LIMIT 1;
        "#;
        let rows = self
            .run_query(
                statement,
                json!({
                    "table": self.table,
                    "key": self.record_key(id),
                    "tenant": tenant.0,
                }),
            )
            .await?;
        if rows.is_empty() {
            Ok(())
        } else {
            Err(StorageError::conflict("entity already exists"))
        }
    }

    fn normalize_row(mut row: Value, fallback_table: &str) -> Value {
        if let Value::Object(ref mut map) = row {
            if let Some(id_value) = map.get_mut("id") {
                if let Value::Object(id_obj) = id_value {
                    if let Some(Value::Object(thing_obj)) = id_obj.get("Thing") {
                        let tb = thing_obj
                            .get("tb")
                            .and_then(Value::as_str)
                            .unwrap_or(fallback_table);
                        if let Some(Value::String(record_id)) = thing_obj.get("id") {
                            *id_value = Value::String(format!("{tb}:{record_id}"));
                        }
                    }
                }
            }
            if let Some(Value::String(id_str)) = map.get_mut("id") {
                if id_str.contains('⟨') || id_str.contains('⟩') {
                    *id_str = id_str.replace('⟨', "").replace('⟩', "");
                }
            }
        }
        row
    }

    fn deserialize_entity(row: Value, table: &str) -> Result<E, StorageError> {
        let normalized = Self::normalize_row(row, table);
        serde_json::from_value(normalized)
            .map_err(|err| StorageError::internal(&format!("deserialize entity: {err}")))
    }

    fn build_filter_clause(&self, tenant: &TenantId, params: &QueryParams) -> (String, Value) {
        let mut conditions = vec!["tenant = $tenant".to_string()];
        let mut bindings = Map::new();
        bindings.insert("tenant".into(), Value::String(tenant.0.clone()));

        if let Value::Object(filter_map) = &params.filter {
            for (key, value) in filter_map {
                let bind_key = format!("filter_{}", key.replace('.', "_"));
                conditions.push(format!("{key} = ${bind_key}"));
                bindings.insert(bind_key, value.clone());
            }
        }

        let mut clause = format!("WHERE {}", conditions.join(" AND "));
        if let Some(order) = &params.order_by {
            clause.push_str(&format!(" ORDER BY {}", order));
        }
        if let Some(limit) = params.limit {
            clause.push_str(&format!(" LIMIT {}", limit));
        }

        (clause, Value::Object(bindings))
    }
}

#[async_trait]
impl<E> Repository<E> for SurrealRepository<E>
where
    E: Entity + Send + Sync,
{
    async fn create(&self, tenant: &TenantId, entity: &E) -> Result<(), StorageError> {
        if entity.tenant() != tenant {
            return Err(StorageError::bad_request("tenant mismatch"));
        }
        self.ensure_not_exists(tenant, entity.id()).await?;

        let mut data = serde_json::to_value(entity)
            .map_err(|err| StorageError::internal(&format!("serialize entity: {err}")))?;
        if let Value::Object(ref mut map) = data {
            map.remove("id");
        }
        let statement = r#"
            CREATE type::thing($table, $key) CONTENT $data RETURN NONE;
        "#;
        self.run_query(
            statement,
            json!({
                "table": self.table,
                "key": self.record_key(entity.id()),
                "data": data,
            }),
        )
        .await?;
        Ok(())
    }

    async fn upsert(
        &self,
        tenant: &TenantId,
        id: &str,
        patch: Value,
        _session: Option<&str>,
    ) -> Result<E, StorageError> {
        let mut content = match patch {
            Value::Object(map) => map,
            _ => return Err(StorageError::bad_request("upsert patch must be an object")),
        };
        content.insert("tenant".into(), Value::String(tenant.0.clone()));
        content.remove("id");

        let statement = r#"
            UPSERT type::thing($table, $key) CONTENT $data RETURN NONE;
        "#;

        self.run_query(
            statement,
            json!({
                "table": self.table,
                "key": self.record_key(id),
                "data": Value::Object(content),
            }),
        )
        .await?;

        self.get(tenant, id)
            .await?
            .ok_or_else(|| StorageError::internal("upsert fetch failed"))
    }

    async fn get(&self, tenant: &TenantId, id: &str) -> Result<Option<E>, StorageError> {
        let statement = r#"
            SELECT *, type::string(id) AS id
            FROM type::thing($table, $key)
            WHERE tenant = $tenant
            LIMIT 1;
        "#;
        let rows = self
            .run_query(
                statement,
                json!({
                    "table": self.table,
                    "key": self.record_key(id),
                    "tenant": tenant.0,
                }),
            )
            .await?;

        if let Some(row) = rows.into_iter().next() {
            Ok(Some(Self::deserialize_entity(row, self.table)?))
        } else {
            Ok(None)
        }
    }

    async fn select(
        &self,
        tenant: &TenantId,
        params: QueryParams,
    ) -> Result<Page<E>, StorageError> {
        let (clause, bindings) = self.build_filter_clause(tenant, &params);
        let statement = format!(
            "SELECT *, type::string(id) AS id FROM {} {}",
            self.table, clause
        );
        let rows = self.run_query(&statement, bindings).await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(Self::deserialize_entity(row, self.table)?);
        }
        Ok(Page { items, next: None })
    }

    async fn delete(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError> {
        if self.get(tenant, id).await?.is_none() {
            return Err(StorageError::not_found("entity not found"));
        }

        let statement = r#"
            DELETE type::thing($table, $key);
        "#;
        self.run_query(
            statement,
            json!({
                "table": self.table,
                "key": self.record_key(id),
            }),
        )
        .await?;
        Ok(())
    }
}
