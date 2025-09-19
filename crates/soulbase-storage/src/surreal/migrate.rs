use crate::errors::StorageError;
use crate::spi::migrate::{MigrationScript, Migrator};
use async_trait::async_trait;

pub struct SurrealMigrator;

#[async_trait]
impl Migrator for SurrealMigrator {
    async fn current_version(&self) -> Result<String, StorageError> {
        Err(StorageError::internal("surreal migrator not implemented"))
    }

    async fn apply_up(&self, _scripts: &[MigrationScript]) -> Result<(), StorageError> {
        Err(StorageError::internal("surreal migrator not implemented"))
    }

    async fn apply_down(&self, _scripts: &[MigrationScript]) -> Result<(), StorageError> {
        Err(StorageError::internal("surreal migrator not implemented"))
    }
}
