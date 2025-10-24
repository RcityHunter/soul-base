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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn migrator_tracks_versions_and_resets() {
        let migrator = InMemoryMigrator::new();
        assert_eq!(migrator.current_version().await.unwrap(), "none");

        let scripts = vec![MigrationScript {
            version: "2024-01-01__init".into(),
            up_sql: "DEFINE TABLE doc".into(),
            down_sql: "REMOVE TABLE doc".into(),
            checksum: "sha256:test".into(),
        }];

        migrator.apply_up(&scripts).await.unwrap();
        assert_eq!(
            migrator.current_version().await.unwrap(),
            "2024-01-01__init"
        );

        migrator.apply_down(&scripts).await.unwrap();
        assert_eq!(migrator.current_version().await.unwrap(), "none");
    }
}
