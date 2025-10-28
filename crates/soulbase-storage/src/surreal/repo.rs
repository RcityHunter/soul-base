use super::datastore::SurrealDatastore;
use crate::errors::StorageError;
use crate::model::{
    awareness::AwarenessEvent, causal::CausalEdge, recall::RecallChunk, timeline::TimelineEvent,
    vector_manifest::VectorManifest, Entity, Page, QueryParams,
};
use crate::spi::datastore::Datastore;
use crate::spi::query::{QueryExecutor, QueryStats};
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

    async fn run_query(
        &self,
        statement: &str,
        params: Value,
    ) -> Result<(Vec<Value>, QueryStats), StorageError> {
        let session = self.datastore.session().await?;
        let outcome = session.query(statement, params).await?;
        Ok((outcome.rows, outcome.stats))
    }

    async fn ensure_not_exists(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError> {
        let statement = r#"
            SELECT id
            FROM type::thing($table, $key)
            WHERE tenant = $tenant
            LIMIT 1;
        "#;
        let (rows, _stats) = self
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
                    *id_str = id_str.chars().filter(|c| *c != '⟨' && *c != '⟩').collect();
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

    fn build_filter_clause(
        &self,
        tenant: &TenantId,
        params: &QueryParams,
    ) -> (String, Value, Vec<String>, bool) {
        let mut conditions = vec!["tenant = $tenant".to_string()];
        let mut bindings = Map::new();
        bindings.insert("tenant".into(), Value::String(tenant.0.clone()));

        let mut hinted_indices = Vec::new();
        let mut has_filter = false;
        if let Value::Object(filter_map) = &params.filter {
            has_filter = !filter_map.is_empty();
            for (key, value) in filter_map {
                let bind_key = format!("filter_{}", key.replace('.', "_"));
                conditions.push(format!("{key} = ${bind_key}"));
                bindings.insert(bind_key, value.clone());
            }
            hinted_indices.extend(self.infer_indices(filter_map));
        }

        let mut clause = format!("WHERE {}", conditions.join(" AND "));
        if let Some(order) = &params.order_by {
            clause.push_str(&format!(" ORDER BY {}", order));
        }
        if let Some(limit) = params.limit {
            clause.push_str(&format!(" LIMIT {}", limit));
        }

        (clause, Value::Object(bindings), hinted_indices, has_filter)
    }

    fn infer_indices(&self, filter_map: &Map<String, Value>) -> Vec<String> {
        let mut indices = Vec::new();
        let has_field = |key: &str| filter_map.get(key).filter(|v| !v.is_null()).is_some();
        match self.table {
            crate::surreal::schema::TABLE_TIMELINE_EVENT => {
                if has_field("event_id") {
                    indices.push("uniq_timeline_event".into());
                }
                if has_field("journey_id") {
                    indices.push("idx_timeline_journey".into());
                }
                if has_field("thread_id") {
                    indices.push("idx_timeline_thread".into());
                }
            }
            crate::surreal::schema::TABLE_CAUSAL_EDGE => {
                if has_field("edge_id") {
                    indices.push("uniq_causal_edge".into());
                }
                if has_field("source_event_id") {
                    indices.push("idx_causal_relation".into());
                }
                if has_field("target_event_id") {
                    indices.push("idx_causal_target".into());
                }
            }
            crate::surreal::schema::TABLE_RECALL_CHUNK => {
                if has_field("chunk_id") {
                    indices.push("uniq_recall_chunk".into());
                }
                if has_field("journey_id") {
                    indices.push("idx_recall_journey".into());
                }
                if has_field("source_event_id") {
                    indices.push("idx_recall_source".into());
                }
            }
            crate::surreal::schema::TABLE_AWARENESS_EVENT => {
                if has_field("awareness_id") {
                    indices.push("uniq_awareness_event".into());
                }
                if has_field("journey_id") {
                    indices.push("idx_awareness_timeline".into());
                }
            }
            crate::surreal::schema::TABLE_VECTOR_MANIFEST => {
                if has_field("index_id") {
                    indices.push("uniq_vector_manifest".into());
                }
            }
            crate::surreal::schema::TABLE_LLM_TOOL_PLAN => {
                if has_field("plan_id") {
                    indices.push("uniq_llm_plan".into());
                }
            }
            crate::surreal::schema::TABLE_LLM_EXPLAIN => {
                if has_field("explain_id") {
                    indices.push("uniq_llm_explain".into());
                }
                if has_field("plan_id") {
                    indices.push("idx_llm_explain_plan".into());
                }
            }
            _ => {}
        }
        indices
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
        let _ = self
            .run_query(
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
        let (rows, _stats) = self
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
        let (clause, bindings, hinted_indices, has_filter) =
            self.build_filter_clause(tenant, &params);
        let statement = format!(
            "SELECT *, type::string(id) AS id FROM {} {}",
            self.table, clause
        );
        let (rows, mut stats) = self.run_query(&statement, bindings).await?;
        if !hinted_indices.is_empty() {
            stats.indices_used = Some(hinted_indices);
        }
        if has_filter && stats.indices_used.is_none() && stats.degradation_reason.is_none() {
            stats.degradation_reason =
                Some("no supporting index detected for filter; potential scan".into());
        }
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(Self::deserialize_entity(row, self.table)?);
        }
        Ok(Page {
            items,
            next: None,
            meta: Some(stats),
        })
    }

    async fn delete(&self, tenant: &TenantId, id: &str) -> Result<(), StorageError> {
        if self.get(tenant, id).await?.is_none() {
            return Err(StorageError::not_found("entity not found"));
        }

        let statement = r#"
            DELETE type::thing($table, $key);
        "#;
        let _ = self
            .run_query(
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

pub type TimelineEventRepo = SurrealRepository<TimelineEvent>;
pub type CausalEdgeRepo = SurrealRepository<CausalEdge>;
pub type RecallChunkRepo = SurrealRepository<RecallChunk>;
pub type AwarenessEventRepo = SurrealRepository<AwarenessEvent>;
pub type VectorManifestRepo = SurrealRepository<VectorManifest>;
