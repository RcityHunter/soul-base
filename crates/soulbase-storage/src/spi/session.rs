use crate::errors::StorageError;
use crate::spi::query::QueryExecutor;
use crate::spi::tx::Transaction;
use async_trait::async_trait;

#[async_trait]
pub trait Session: QueryExecutor {
    type Tx: Transaction;

    async fn begin(&self) -> Result<Self::Tx, StorageError>;
}
