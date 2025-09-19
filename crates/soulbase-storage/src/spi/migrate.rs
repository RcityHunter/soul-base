use crate::errors::StorageError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationScript {
    pub version: String,
    pub up_sql: String,
    pub down_sql: String,
    pub checksum: String,
}

#[async_trait]
pub trait Migrator: Send + Sync {
    async fn current_version(&self) -> Result<String, StorageError>;
    async fn apply_up(&self, scripts: &[MigrationScript]) -> Result<(), StorageError>;
    async fn apply_down(&self, scripts: &[MigrationScript]) -> Result<(), StorageError>;
}
