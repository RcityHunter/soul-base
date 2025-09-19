use super::datastore::MockDatastore;
use crate::errors::StorageError;
use crate::spi::query::QueryExecutor;
use crate::spi::tx::Transaction;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct MockTransaction {
    store: MockDatastore,
    closed: Arc<AtomicBool>,
}

impl MockTransaction {
    pub(crate) fn new(store: MockDatastore) -> Self {
        Self {
            store,
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn datastore(&self) -> &MockDatastore {
        &self.store
    }

    fn ensure_open(&self) -> Result<(), StorageError> {
        if self.closed.load(Ordering::SeqCst) {
            Err(StorageError::bad_request("transaction already closed"))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl QueryExecutor for MockTransaction {
    async fn query(&self, _statement: &str, _params: Value) -> Result<Value, StorageError> {
        Err(StorageError::bad_request(
            "mock transaction does not support raw queries; use typed adapters",
        ))
    }
}

#[async_trait]
impl Transaction for MockTransaction {
    async fn commit(&mut self) -> Result<(), StorageError> {
        self.ensure_open()?;
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), StorageError> {
        self.ensure_open()?;
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn is_active(&self) -> bool {
        !self.closed.load(Ordering::SeqCst)
    }
}
