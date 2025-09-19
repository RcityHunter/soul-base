use crate::errors::StorageError;
use crate::spi::query::QueryExecutor;
use async_trait::async_trait;

#[async_trait]
pub trait Transaction: QueryExecutor {
    async fn commit(&mut self) -> Result<(), StorageError>;
    async fn rollback(&mut self) -> Result<(), StorageError>;
    fn is_active(&self) -> bool;
}
