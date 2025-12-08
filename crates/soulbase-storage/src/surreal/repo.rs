use super::datastore::SurrealDatastore;
use crate::errors::StorageError;
use crate::model::{
    awareness::AwarenessEvent,
    autonomous::AutonomousSession,
    causal::CausalEdge,
    dfr::{DfrDecision, DfrFingerprint},
    embedding::EmbeddingChunk,
    evolution::{EvolutionAiEvent, EvolutionGroupEvent, EvolutionRelationship},
    metacognition::{
        AcPerformanceProfile, DecisionAudit, MetacognitionAnalysis, MetacognitionInsight,
        ThinkingPattern,
    },
    recall::RecallChunk,
    timeline::TimelineEvent,
    timeseries::TimeseriesMetric,
    vector_manifest::VectorManifest,
    Entity, Page, QueryParams,
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
    ) -> (String, Value, Vec<String>, bool, bool) {
        let mut conditions = vec!["tenant = $tenant".to_string()];
        let mut bindings = Map::new();
        bindings.insert("tenant".into(), Value::String(tenant.0.clone()));

        let mut hinted_indices = Vec::new();
        let mut has_filter = false;
        let mut filter_has_index = false;
        if let Value::Object(filter_map) = &params.filter {
            has_filter = !filter_map.is_empty();
            for (key, value) in filter_map {
                let bind_key = format!("filter_{}", key.replace('.', "_"));
                conditions.push(format!("{key} = ${bind_key}"));
                bindings.insert(bind_key, value.clone());
            }
            let filter_indices = self.infer_indices(filter_map);
            if !filter_indices.is_empty() {
                filter_has_index = true;
                hinted_indices.extend(filter_indices);
            }
        }

        let mut clause = format!("WHERE {}", conditions.join(" AND "));
        let order_clause = params
            .order_by
            .as_deref()
            .or_else(|| self.default_order_clause());
        if let Some(order) = order_clause {
            clause.push_str(&format!(" ORDER BY {}", order));
            hinted_indices.extend(self.infer_indices_from_order(order));
        }
        if let Some(limit) = params.limit {
            clause.push_str(&format!(" LIMIT {}", limit));
        }

        (
            clause,
            Value::Object(bindings),
            hinted_indices,
            has_filter,
            filter_has_index,
        )
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
            // ACE 新增表索引推断
            crate::surreal::schema::TABLE_TIMESERIES_METRIC => {
                if has_field("metric_id") {
                    indices.push("uniq_timeseries_metric".into());
                }
                if has_field("name") {
                    indices.push("idx_timeseries_name_time".into());
                }
                if has_field("session_id") {
                    indices.push("idx_timeseries_session".into());
                }
            }
            crate::surreal::schema::TABLE_EMBEDDING_CHUNK => {
                if has_field("chunk_id") {
                    indices.push("uniq_embedding_chunk".into());
                }
                if has_field("source_type") || has_field("source_id") {
                    indices.push("idx_embedding_source".into());
                }
                if has_field("journey_id") {
                    indices.push("idx_embedding_journey".into());
                }
            }
            crate::surreal::schema::TABLE_EVOLUTION_GROUP_EVENT => {
                if has_field("event_id") {
                    indices.push("uniq_evolution_group".into());
                }
                if has_field("event_type") {
                    indices.push("idx_evolution_group_type".into());
                }
            }
            crate::surreal::schema::TABLE_EVOLUTION_AI => {
                if has_field("record_id") {
                    indices.push("uniq_evolution_ai".into());
                }
                if has_field("ai_id") {
                    indices.push("idx_evolution_ai_id".into());
                }
            }
            crate::surreal::schema::TABLE_EVOLUTION_RELATIONSHIP => {
                if has_field("record_id") {
                    indices.push("uniq_evolution_rel".into());
                }
                if has_field("source_id") {
                    indices.push("idx_evolution_rel_source".into());
                }
                if has_field("target_id") {
                    indices.push("idx_evolution_rel_target".into());
                }
            }
            crate::surreal::schema::TABLE_METACOGNITION_ANALYSIS => {
                if has_field("analysis_id") {
                    indices.push("uniq_metacognition_analysis".into());
                }
                if has_field("mode") {
                    indices.push("idx_metacognition_mode".into());
                }
            }
            crate::surreal::schema::TABLE_METACOGNITION_INSIGHT => {
                if has_field("insight_id") {
                    indices.push("uniq_metacognition_insight".into());
                }
                if has_field("analysis_id") {
                    indices.push("idx_metacognition_insight_analysis".into());
                }
            }
            crate::surreal::schema::TABLE_THINKING_PATTERN => {
                if has_field("pattern_id") {
                    indices.push("uniq_thinking_pattern".into());
                }
                if has_field("pattern_type") {
                    indices.push("idx_thinking_pattern_type".into());
                }
            }
            crate::surreal::schema::TABLE_AC_PERFORMANCE_PROFILE => {
                if has_field("ac_id") {
                    indices.push("uniq_ac_performance".into());
                }
            }
            crate::surreal::schema::TABLE_DECISION_AUDIT => {
                if has_field("audit_id") {
                    indices.push("uniq_decision_audit".into());
                }
                if has_field("decision_id") {
                    indices.push("idx_decision_audit_decision".into());
                }
            }
            crate::surreal::schema::TABLE_DFR_FINGERPRINT => {
                if has_field("fingerprint_id") {
                    indices.push("uniq_dfr_fingerprint".into());
                }
                if has_field("decision_type") {
                    indices.push("idx_dfr_fingerprint_type".into());
                }
            }
            crate::surreal::schema::TABLE_DFR_DECISION => {
                if has_field("decision_id") {
                    indices.push("uniq_dfr_decision".into());
                }
                if has_field("fingerprint_id") {
                    indices.push("idx_dfr_decision_fingerprint".into());
                }
                if has_field("decision_type") {
                    indices.push("idx_dfr_decision_type".into());
                }
            }
            crate::surreal::schema::TABLE_AUTONOMOUS_SESSION => {
                if has_field("session_id") {
                    indices.push("uniq_autonomous_session".into());
                }
                if has_field("status") {
                    indices.push("idx_autonomous_session_status".into());
                }
            }
            _ => {}
        }
        indices
    }

    fn infer_indices_from_order(&self, order: &str) -> Vec<String> {
        let mut indices = Vec::new();
        let lower = order.to_ascii_lowercase();
        match self.table {
            crate::surreal::schema::TABLE_TIMELINE_EVENT => {
                if lower.contains("journey_id") && lower.contains("occurred_at") {
                    indices.push("idx_timeline_journey".into());
                } else if lower.contains("thread_id") {
                    indices.push("idx_timeline_thread".into());
                }
            }
            crate::surreal::schema::TABLE_AWARENESS_EVENT => {
                if lower.contains("journey_id") && lower.contains("triggered_at") {
                    indices.push("idx_awareness_timeline".into());
                }
            }
            crate::surreal::schema::TABLE_RECALL_CHUNK => {
                if lower.contains("journey_id") && lower.contains("created_at") {
                    indices.push("idx_recall_journey".into());
                }
            }
            _ => {}
        }
        indices
    }

    fn default_order_clause(&self) -> Option<&'static str> {
        match self.table {
            crate::surreal::schema::TABLE_TIMELINE_EVENT => Some("journey_id, occurred_at"),
            crate::surreal::schema::TABLE_AWARENESS_EVENT => Some("journey_id, triggered_at"),
            crate::surreal::schema::TABLE_RECALL_CHUNK => Some("journey_id, created_at"),
            _ => None,
        }
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

        let mut payload = serde_json::to_value(entity)
            .map_err(|err| StorageError::internal(&format!("serialize entity: {err}")))?;
        let key = self.record_key(entity.id());
        match &mut payload {
            Value::Object(map) => {
                map.insert("tenant".into(), Value::String(tenant.0.clone()));
                map.remove("id");
            }
            other => {
                *other = json!({
                    "tenant": tenant.0.clone(),
                    "value": other.clone(),
                });
            }
        }

        let statement = r#"
            CREATE type::thing($table, $key) CONTENT $data RETURN NONE;
        "#;
        let _ = self
            .run_query(
                statement,
                json!({
                    "table": self.table,
                    "key": key,
                    "data": payload,
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
        if patch.get("tenant").is_some() {
            return Err(StorageError::bad_request("tenant field cannot be updated"));
        }
        if patch.get("id").is_some() {
            return Err(StorageError::bad_request("id field cannot be updated"));
        }

        let patch_map = match patch {
            Value::Object(map) if !map.is_empty() => map,
            Value::Object(_) => {
                return self
                    .get(tenant, id)
                    .await?
                    .ok_or_else(|| StorageError::internal("upsert fetch failed"))
            }
            _ => return Err(StorageError::bad_request("upsert patch must be an object")),
        };

        let mut assignments = Vec::new();
        let mut bindings = Map::new();
        bindings.insert("table".into(), Value::String(self.table.to_string()));
        bindings.insert("key".into(), Value::String(self.record_key(id)));
        bindings.insert("tenant".into(), Value::String(tenant.0.clone()));

        let mut json_patches = Vec::new();
        for (idx, (key, value)) in patch_map.into_iter().enumerate() {
            let bind_key = format!("patch_{idx}");
            match value {
                Value::Object(_) | Value::Array(_) => {
                    json_patches.push(json!({
                        "op": "add",
                        "path": format!("/{}", key),
                        "value": value,
                    }));
                }
                other => {
                    assignments.push(format!("{key} = ${bind_key}"));
                    bindings.insert(bind_key, other);
                }
            }
        }
        assignments.push("tenant = $tenant".to_string());

        let statement = format!(
            "UPDATE type::thing($table, $key) SET {} WHERE tenant = $tenant RETURN NONE;",
            assignments.join(", ")
        );

        let (rows, _stats) = self.run_query(&statement, Value::Object(bindings)).await?;

        let mut current = if let Some(row) = rows.into_iter().next() {
            Self::deserialize_entity(row, self.table)?
        } else if let Some(entity) = self.get(tenant, id).await? {
            entity
        } else {
            return Err(StorageError::not_found("entity not found for upsert"));
        };

        if !json_patches.is_empty() {
            self.run_query(
                "UPDATE type::thing($table, $key) PATCH $ops RETURN NONE;",
                json!({
                    "table": self.table,
                    "key": self.record_key(id),
                    "ops": json_patches,
                }),
            )
            .await?;
            current = self
                .get(tenant, id)
                .await?
                .ok_or_else(|| StorageError::internal("upsert fetch failed"))?;
        }

        Ok(current)
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
        let (clause, bindings, hinted_indices, has_filter, filter_has_index) =
            self.build_filter_clause(tenant, &params);
        let statement = format!(
            "SELECT *, type::string(id) AS id FROM {} {}",
            self.table, clause
        );
        let (rows, mut stats) = self.run_query(&statement, bindings).await?;
        if !hinted_indices.is_empty() {
            stats.indices_used = Some(hinted_indices);
        }
        if has_filter && !filter_has_index && stats.degradation_reason.is_none() {
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

    async fn upsert_or_create(
        &self,
        tenant: &TenantId,
        id: &str,
        patch: Value,
    ) -> Result<(), StorageError> {
        // Check if record exists
        if self.get(tenant, id).await?.is_some() {
            // Record exists, update it
            self.upsert(tenant, id, patch, None).await.map(|_| ())
        } else {
            // Record doesn't exist, create it
            // Use patch as content for creation
            let mut content = match patch {
                Value::Object(map) => map,
                _ => return Err(StorageError::bad_request("patch must be an object")),
            };

            // Ensure tenant field is set correctly
            content.insert("tenant".into(), Value::String(tenant.0.clone()));

            let key = self.record_key(id);
            let statement = r#"
                CREATE type::thing($table, $key) CONTENT $data RETURN NONE;
            "#;
            let _ = self
                .run_query(
                    statement,
                    json!({
                        "table": self.table,
                        "key": key,
                        "data": Value::Object(content),
                    }),
                )
                .await?;
            Ok(())
        }
    }
}

pub type TimelineEventRepo = SurrealRepository<TimelineEvent>;
pub type CausalEdgeRepo = SurrealRepository<CausalEdge>;
pub type RecallChunkRepo = SurrealRepository<RecallChunk>;
pub type AwarenessEventRepo = SurrealRepository<AwarenessEvent>;
pub type VectorManifestRepo = SurrealRepository<VectorManifest>;

// ACE 新增 Repository 类型别名
pub type TimeseriesMetricRepo = SurrealRepository<TimeseriesMetric>;
pub type EmbeddingChunkRepo = SurrealRepository<EmbeddingChunk>;
pub type EvolutionGroupEventRepo = SurrealRepository<EvolutionGroupEvent>;
pub type EvolutionAiEventRepo = SurrealRepository<EvolutionAiEvent>;
pub type EvolutionRelationshipRepo = SurrealRepository<EvolutionRelationship>;
pub type MetacognitionAnalysisRepo = SurrealRepository<MetacognitionAnalysis>;
pub type MetacognitionInsightRepo = SurrealRepository<MetacognitionInsight>;
pub type ThinkingPatternRepo = SurrealRepository<ThinkingPattern>;
pub type AcPerformanceProfileRepo = SurrealRepository<AcPerformanceProfile>;
pub type DecisionAuditRepo = SurrealRepository<DecisionAudit>;
pub type DfrFingerprintRepo = SurrealRepository<DfrFingerprint>;
pub type DfrDecisionRepo = SurrealRepository<DfrDecision>;
pub type AutonomousSessionRepo = SurrealRepository<AutonomousSession>;
