use crate::errors::StorageError;
use crate::spi::query::QueryExecutor;
use crate::spi::tx::Transaction;
use async_trait::async_trait;

#[async_trait]
pub trait Session: QueryExecutor {
    type Tx: Transaction;

    async fn begin(&self) -> Result<Self::Tx, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spi::query::QueryExecutor;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Clone)]
    struct StubSession;

    #[derive(Clone)]
    struct StubTx {
        active: Arc<AtomicBool>,
    }

    #[async_trait]
    impl QueryExecutor for StubSession {
        async fn query(
            &self,
            statement: &str,
            params: serde_json::Value,
        ) -> Result<serde_json::Value, StorageError> {
            Ok(json!({"statement": statement, "params": params}))
        }
    }

    #[async_trait]
    impl Session for StubSession {
        type Tx = StubTx;

        async fn begin(&self) -> Result<Self::Tx, StorageError> {
            Ok(StubTx {
                active: Arc::new(AtomicBool::new(true)),
            })
        }
    }

    #[async_trait]
    impl QueryExecutor for StubTx {
        async fn query(
            &self,
            statement: &str,
            params: serde_json::Value,
        ) -> Result<serde_json::Value, StorageError> {
            Ok(json!({"statement": statement, "params": params, "scope": "tx"}))
        }
    }

    #[async_trait]
    impl Transaction for StubTx {
        async fn commit(&mut self) -> Result<(), StorageError> {
            self.active.store(false, Ordering::SeqCst);
            Ok(())
        }

        async fn rollback(&mut self) -> Result<(), StorageError> {
            self.active.store(false, Ordering::SeqCst);
            Ok(())
        }

        fn is_active(&self) -> bool {
            self.active.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn begin_yields_active_transaction() {
        let session = StubSession;
        let mut tx = session.begin().await.expect("begin tx");
        assert!(tx.is_active());
        let payload = tx
            .query("PING", json!({"x": 1}))
            .await
            .expect("tx query");
        assert_eq!(payload["scope"], "tx");
        tx.rollback().await.expect("rollback");
        assert!(!tx.is_active());
    }
}
