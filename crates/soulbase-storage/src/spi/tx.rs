use crate::errors::StorageError;
use crate::spi::query::QueryExecutor;
use async_trait::async_trait;

#[async_trait]
pub trait Transaction: QueryExecutor {
    async fn commit(&mut self) -> Result<(), StorageError>;
    async fn rollback(&mut self) -> Result<(), StorageError>;
    fn is_active(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct StubTx {
        active: Arc<AtomicBool>,
    }

    #[async_trait]
    impl QueryExecutor for StubTx {
        async fn query(
            &self,
            statement: &str,
            _params: serde_json::Value,
        ) -> Result<serde_json::Value, StorageError> {
            Ok(json!({"statement": statement}))
        }
    }

    #[async_trait]
    impl Transaction for StubTx {
        async fn commit(&mut self) -> Result<(), StorageError> {
            if !self.active.swap(false, Ordering::SeqCst) {
                return Err(StorageError::bad_request("transaction already closed"));
            }
            Ok(())
        }

        async fn rollback(&mut self) -> Result<(), StorageError> {
            if !self.active.swap(false, Ordering::SeqCst) {
                return Err(StorageError::bad_request("transaction already closed"));
            }
            Ok(())
        }

        fn is_active(&self) -> bool {
            self.active.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn commit_and_rollback_toggle_activity() {
        let mut tx = StubTx {
            active: Arc::new(AtomicBool::new(true)),
        };
        assert!(tx.is_active());
        tx.commit().await.expect("commit");
        assert!(!tx.is_active());
        let err = tx.commit().await.expect_err("second commit fails");
        assert!(err.to_string().contains("already closed"));

        let mut tx2 = StubTx {
            active: Arc::new(AtomicBool::new(true)),
        };
        tx2.rollback().await.expect("rollback");
        assert!(!tx2.is_active());
        let err = tx2.rollback().await.expect_err("second rollback fails");
        assert!(err.to_string().contains("already closed"));
    }
}
