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

#[cfg(test)]
mod tests {
    use super::*;
    use soulbase_types::prelude::TenantId;

    fn new_tx() -> MockTransaction {
        let store = MockDatastore::new();
        store.store("doc", &TenantId("tenant".into()), "id", serde_json::Value::Null);
        MockTransaction::new(store)
    }

    #[tokio::test]
    async fn commit_marks_transaction_closed() {
        let mut tx = new_tx();
        assert!(tx.is_active());
        tx.commit().await.expect("commit");
        assert!(!tx.is_active());
        let err = tx.commit().await.expect_err("second commit fails");
        assert!(err.to_string().contains("transaction already closed"));
    }

    #[tokio::test]
    async fn rollback_behaves_like_commit() {
        let mut tx = new_tx();
        tx.rollback().await.expect("rollback");
        assert!(!tx.is_active());
        let err = tx.rollback().await.expect_err("second rollback fails");
        assert!(err.to_string().contains("transaction already closed"));
    }

    #[tokio::test]
    async fn query_is_not_supported() {
        let tx = new_tx();
        let err = tx
            .query("SELECT * FROM doc", serde_json::Value::Null)
            .await
            .expect_err("query should fail");
        assert!(err
            .to_string()
            .contains("mock transaction does not support raw queries"));
    }
}
