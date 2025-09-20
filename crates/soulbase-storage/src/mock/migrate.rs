use crate::errors::StorageError;
use crate::spi::migrate::{MigrationScript, Migrator};
use async_trait::async_trait;
use parking_lot::RwLock;

#[derive(Clone)]
pub struct InMemoryMigrator {
    state: std::sync::Arc<RwLock<String>>,
}

impl InMemoryMigrator {
    pub fn new() -> Self {
        Self {
            state: std::sync::Arc::new(RwLock::new("none".to_string())),
        }
    }
}

impl Default for InMemoryMigrator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Migrator for InMemoryMigrator {
    async fn current_version(&self) -> Result<String, StorageError> {
        Ok(self.state.read().clone())
    }

    async fn apply_up(&self, scripts: &[MigrationScript]) -> Result<(), StorageError> {
        if let Some(last) = scripts.last() {
            *self.state.write() = last.version.clone();
        }
        Ok(())
    }

    async fn apply_down(&self, _scripts: &[MigrationScript]) -> Result<(), StorageError> {
        *self.state.write() = "none".to_string();
        Ok(())
    }
}
