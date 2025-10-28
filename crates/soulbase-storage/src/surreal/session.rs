use super::{
    binder::QueryBinder, errors::map_surreal_error, observe::record_backend, tx::SurrealTransaction,
};
use crate::errors::StorageError;
use crate::spi::query::{QueryExecutor, QueryOutcome, QueryStats};
use crate::spi::session::Session;
use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Instant;
use surrealdb::{engine::any::Any, Surreal};

#[derive(Clone)]
pub struct SurrealSession {
    client: Arc<Surreal<Any>>,
}

impl SurrealSession {
    pub(crate) fn new(client: Arc<Surreal<Any>>) -> Self {
        Self { client }
    }

    pub fn client(&self) -> &Surreal<Any> {
        &self.client
    }

    async fn collect_indices(
        &self,
        statement: &str,
        bindings: &[(String, Value)],
    ) -> Option<Vec<String>> {
        let trimmed = statement.trim().trim_end_matches(';');
        let lower = trimmed.to_ascii_lowercase();
        if !lower.starts_with("select") {
            return None;
        }
        let explain_stmt = format!("EXPLAIN FOR {}", trimmed);
        let mut prepared = self.client.query(explain_stmt);
        for (key, value) in bindings {
            prepared = prepared.bind((key.clone(), value.clone()));
        }
        let mut response = prepared.await.ok()?;
        let mut plan_rows = response.take::<Vec<Value>>(0).ok()?;
        let plan = plan_rows.pop()?;
        let mut indices = Vec::new();
        collect_indices_recursive(&plan, &mut indices);
        indices.sort();
        indices.dedup();
        indices.sort();
        indices.dedup();
        if indices.is_empty() {
            None
        } else {
            Some(indices)
        }
    }
}

#[async_trait]
impl QueryExecutor for SurrealSession {
    async fn query(&self, statement: &str, params: Value) -> Result<QueryOutcome, StorageError> {
        let bindings = QueryBinder::into_bindings(params.clone());
        let mut prepared = self.client.query(statement);
        for (key, value) in bindings.iter() {
            prepared = prepared.bind((key.clone(), value.clone()));
        }
        let started = Instant::now();
        let mut response = prepared
            .await
            .map_err(|err| map_surreal_error(err, "surreal query execute"))?;
        let rows: Vec<Value> = match response.take::<Vec<Value>>(0) {
            Ok(rows) => rows,
            Err(err) => {
                let msg = err.to_string();
                if msg.contains("found None") {
                    Vec::new()
                } else {
                    return Err(map_surreal_error(err, "surreal query read"));
                }
            }
        };
        let rows_json = rows;
        let latency = started.elapsed();
        let plan_indices = self.collect_indices(statement, &bindings).await;
        let tenant_guard = statement.to_ascii_lowercase().contains("where")
            && statement.to_ascii_lowercase().contains("tenant");
        let stats = build_query_stats(statement, tenant_guard, plan_indices);
        if stats.full_scan {
            let reason = stats
                .degradation_reason
                .clone()
                .unwrap_or_else(|| "surreal query triggered full scan".into());
            return Err(StorageError::bad_request(&reason));
        }
        record_backend("surreal.query", latency, rows_json.len(), None);
        Ok(QueryOutcome {
            rows: rows_json,
            stats,
        })
    }
}

fn build_query_stats(
    statement: &str,
    tenant_guard: bool,
    plan_indices: Option<Vec<String>>,
) -> QueryStats {
    let hash = Sha256::digest(statement.as_bytes());
    let query_hash = format!("sha256:{:x}", hash);
    let mut stats = QueryStats {
        indices_used: plan_indices,
        query_hash: Some(query_hash),
        degradation_reason: None,
        full_scan: !tenant_guard,
    };
    if stats.full_scan {
        stats.degradation_reason = Some("tenant filter missing; full scan detected".to_string());
    }
    stats
}

fn collect_indices_recursive(value: &Value, indices: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(index)) = map.get("index") {
                indices.push(index.clone());
            }
            for child in map.values() {
                collect_indices_recursive(child, indices);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_indices_recursive(item, indices);
            }
        }
        _ => {}
    }
}

#[async_trait]
impl Session for SurrealSession {
    type Tx = SurrealTransaction;

    async fn begin(&self) -> Result<Self::Tx, StorageError> {
        let started = Instant::now();
        self.client
            .query("BEGIN TRANSACTION")
            .await
            .map_err(|err| map_surreal_error(err, "surreal begin"))?;
        record_backend("surreal.tx.begin", started.elapsed(), 0, None);
        Ok(SurrealTransaction::new(self.client.clone()))
    }
}
