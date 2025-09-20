use super::{
    binder::QueryBinder, errors::map_surreal_error, mapper::SurrealMapper, observe::record_backend,
};
use crate::errors::StorageError;
use crate::spi::query::QueryExecutor;
use crate::spi::tx::Transaction;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use surrealdb::{engine::any::Any, Surreal};

#[derive(Clone)]
pub struct SurrealTransaction {
    client: Arc<Surreal<Any>>,
    active: Arc<AtomicBool>,
}

impl SurrealTransaction {
    pub(crate) fn new(client: Arc<Surreal<Any>>) -> Self {
        Self {
            client,
            active: Arc::new(AtomicBool::new(true)),
        }
    }

    fn ensure_active(&self) -> Result<(), StorageError> {
        if self.active.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(StorageError::bad_request("transaction already closed"))
        }
    }

    fn close(&self) {
        self.active.store(false, Ordering::SeqCst);
    }
}

#[async_trait]
impl QueryExecutor for SurrealTransaction {
    async fn query(&self, statement: &str, params: Value) -> Result<Value, StorageError> {
        self.ensure_active()?;
        let mut prepared = self.client.query(statement);
        for (key, value) in QueryBinder::into_bindings(params) {
            prepared = prepared.bind((key, value));
        }
        let started = Instant::now();
        let mut response = prepared
            .await
            .map_err(|err| map_surreal_error(err, "surreal tx query"))?;
        let rows: Vec<surrealdb::sql::Value> = match response.take(0) {
            Ok(rows) => rows,
            Err(err) => {
                let msg = err.to_string();
                if msg.contains("found None") {
                    Vec::new()
                } else {
                    return Err(map_surreal_error(err, "surreal tx query read"));
                }
            }
        };
        let rows_json: Vec<Value> = rows.into_iter().map(SurrealMapper::to_json).collect();
        record_backend("surreal.tx.query", started.elapsed(), rows_json.len(), None);
        Ok(Value::Array(rows_json))
    }
}

#[async_trait]
impl Transaction for SurrealTransaction {
    async fn commit(&mut self) -> Result<(), StorageError> {
        self.ensure_active()?;
        let started = Instant::now();
        self.client
            .query("COMMIT TRANSACTION")
            .await
            .map_err(|err| map_surreal_error(err, "surreal commit"))?;
        record_backend("surreal.tx.commit", started.elapsed(), 0, None);
        self.close();
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), StorageError> {
        self.ensure_active()?;
        let started = Instant::now();
        self.client
            .query("CANCEL TRANSACTION")
            .await
            .map_err(|err| map_surreal_error(err, "surreal rollback"))?;
        record_backend("surreal.tx.rollback", started.elapsed(), 0, None);
        self.close();
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }
}
