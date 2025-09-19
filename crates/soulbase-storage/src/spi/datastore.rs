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
