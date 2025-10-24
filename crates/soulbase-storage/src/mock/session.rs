use super::datastore::MockDatastore;
use super::tx::MockTransaction;
use crate::errors::StorageError;
use crate::spi::query::QueryExecutor;
use crate::spi::session::Session;
use async_trait::async_trait;
use serde_json::Value;

#[derive(Clone)]
pub struct MockSession {
    store: MockDatastore,
}

impl MockSession {
    pub fn new(store: MockDatastore) -> Self {
        Self { store }
    }

    pub fn datastore(&self) -> &MockDatastore {
        &self.store
    }
}

#[async_trait]
impl QueryExecutor for MockSession {
    async fn query(&self, _statement: &str, _params: Value) -> Result<Value, StorageError> {
        Err(StorageError::bad_request(
            "mock session does not support raw queries; use typed adapters",
        ))
    }
}

#[async_trait]
impl Session for MockSession {
    type Tx = MockTransaction;

    async fn begin(&self) -> Result<Self::Tx, StorageError> {
        Ok(MockTransaction::new(self.store.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spi::tx::Transaction;
    use soulbase_types::prelude::TenantId;

    #[tokio::test]
    async fn begin_returns_active_transaction() {
        let store = MockDatastore::new();
        let tenant = TenantId("tenant".into());
        store.store("doc", &tenant, "id", serde_json::Value::Null);
        let session = MockSession::new(store.clone());

        let tx = session.begin().await.expect("start transaction");
        assert!(tx.is_active());
        assert_eq!(
            tx.datastore().fetch("doc", &tenant, "id"),
            Some(serde_json::Value::Null)
        );
    }

    #[tokio::test]
    async fn query_rejects_raw_statements() {
        let session = MockSession::new(MockDatastore::new());
        let err = session
            .query("SELECT * FROM doc", serde_json::Value::Null)
            .await
            .expect_err("query should fail");
        assert!(err.to_string().contains("mock session does not support raw queries"));
    }
}
