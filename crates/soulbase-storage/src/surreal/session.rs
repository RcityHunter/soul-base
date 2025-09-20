use super::{
    binder::QueryBinder, errors::map_surreal_error, mapper::SurrealMapper, observe::record_backend,
    tx::SurrealTransaction,
};
use crate::errors::StorageError;
use crate::spi::query::QueryExecutor;
use crate::spi::session::Session;
use async_trait::async_trait;
use serde_json::Value;
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
}

#[async_trait]
impl QueryExecutor for SurrealSession {
    async fn query(&self, statement: &str, params: Value) -> Result<Value, StorageError> {
        let mut prepared = self.client.query(statement);
        for (key, value) in QueryBinder::into_bindings(params) {
            prepared = prepared.bind((key, value));
        }
        let started = Instant::now();
        let mut response = prepared
            .await
            .map_err(|err| map_surreal_error(err, "surreal query execute"))?;
        let rows: Vec<surrealdb::sql::Value> = match response.take(0) {
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
        let rows_json: Vec<Value> = rows.into_iter().map(SurrealMapper::to_json).collect();
        let latency = started.elapsed();
        record_backend("surreal.query", latency, rows_json.len(), None);
        Ok(Value::Array(rows_json))
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
