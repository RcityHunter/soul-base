use crate::errors::StorageError;
use crate::spi::session::Session;
use async_trait::async_trait;

#[async_trait]
pub trait Datastore: Send + Sync {
    type Session: Session;

    async fn session(&self) -> Result<Self::Session, StorageError>;

    async fn shutdown(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spi::query::QueryExecutor;
    use crate::spi::tx::Transaction;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone)]
    struct StubDatastore {
        sessions: Arc<AtomicUsize>,
        session_queries: Arc<AtomicUsize>,
        tx_queries: Arc<AtomicUsize>,
    }

    impl StubDatastore {
        fn new() -> Self {
            Self {
                sessions: Arc::new(AtomicUsize::new(0)),
                session_queries: Arc::new(AtomicUsize::new(0)),
                tx_queries: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn session_count(&self) -> usize {
            self.sessions.load(Ordering::SeqCst)
        }

        fn session_query_count(&self) -> usize {
            self.session_queries.load(Ordering::SeqCst)
        }

        fn tx_query_count(&self) -> usize {
            self.tx_queries.load(Ordering::SeqCst)
        }
    }

    #[derive(Clone)]
    struct StubSession {
        session_queries: Arc<AtomicUsize>,
        tx_queries: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct StubTx {
        tx_queries: Arc<AtomicUsize>,
        active: Arc<AtomicBool>,
    }

    #[async_trait]
    impl QueryExecutor for StubSession {
        async fn query(
            &self,
            statement: &str,
            params: serde_json::Value,
        ) -> Result<serde_json::Value, StorageError> {
            self.session_queries.fetch_add(1, Ordering::SeqCst);
            Ok(json!({"origin": "session", "statement": statement, "params": params}))
        }
    }

    #[async_trait]
    impl Session for StubSession {
        type Tx = StubTx;

        async fn begin(&self) -> Result<Self::Tx, StorageError> {
            Ok(StubTx {
                tx_queries: self.tx_queries.clone(),
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
            self.tx_queries.fetch_add(1, Ordering::SeqCst);
            Ok(json!({"origin": "tx", "statement": statement, "params": params}))
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

    #[async_trait]
    impl Datastore for StubDatastore {
        type Session = StubSession;

        async fn session(&self) -> Result<Self::Session, StorageError> {
            self.sessions.fetch_add(1, Ordering::SeqCst);
            Ok(StubSession {
                session_queries: self.session_queries.clone(),
                tx_queries: self.tx_queries.clone(),
            })
        }
    }

    #[tokio::test]
    async fn shutdown_uses_default_ok() {
        let store = StubDatastore::new();
        store.shutdown().await.expect("default shutdown succeeds");
    }

    #[tokio::test]
    async fn session_and_transaction_round_trip() {
        let store = StubDatastore::new();
        let session = store.session().await.expect("obtain session");
        assert_eq!(store.session_count(), 1);

        let reply = session
            .query("SELECT 1", json!({"arg": 1}))
            .await
            .expect("session query");
        assert_eq!(reply["origin"], "session");
        assert_eq!(store.session_query_count(), 1);

        let mut tx = session.begin().await.expect("begin transaction");
        assert!(tx.is_active());

        let tx_reply = tx
            .query("SELECT 2", json!({"arg": 2}))
            .await
            .expect("tx query");
        assert_eq!(tx_reply["origin"], "tx");
        assert_eq!(store.tx_query_count(), 1);

        tx.commit().await.expect("commit");
        assert!(!tx.is_active());
    }
}
