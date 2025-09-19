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
