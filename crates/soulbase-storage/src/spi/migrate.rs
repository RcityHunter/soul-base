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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct RecordingMigrator {
        version: Arc<Mutex<String>>,
        applied: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl Migrator for RecordingMigrator {
        async fn current_version(&self) -> Result<String, StorageError> {
            Ok(self.version.lock().unwrap().clone())
        }

        async fn apply_up(&self, scripts: &[MigrationScript]) -> Result<(), StorageError> {
            if let Some(last) = scripts.last() {
                *self.version.lock().unwrap() = last.version.clone();
            }
            let mut applied = self.applied.lock().unwrap();
            for script in scripts {
                applied.push(format!("up:{}", script.version));
            }
            Ok(())
        }

        async fn apply_down(&self, scripts: &[MigrationScript]) -> Result<(), StorageError> {
            if !scripts.is_empty() {
                *self.version.lock().unwrap() = "none".into();
            }
            let mut applied = self.applied.lock().unwrap();
            for script in scripts {
                applied.push(format!("down:{}", script.version));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn migrator_records_versions_and_calls() {
        let migrator = RecordingMigrator::default();
        assert_eq!(migrator.current_version().await.unwrap(), "");

        let scripts = vec![MigrationScript {
            version: "2025-init".into(),
            up_sql: "DEFINE TABLE doc".into(),
            down_sql: "REMOVE TABLE doc".into(),
            checksum: "sha256:test".into(),
        }];

        migrator.apply_up(&scripts).await.unwrap();
        assert_eq!(migrator.current_version().await.unwrap(), "2025-init");

        migrator.apply_down(&scripts).await.unwrap();
        assert_eq!(migrator.current_version().await.unwrap(), "none");

        let calls = migrator.applied.lock().unwrap().clone();
        assert_eq!(calls, vec!["up:2025-init".to_string(), "down:2025-init".to_string()]);
    }

    #[test]
    fn migration_script_serializes_roundtrip() {
        let script = MigrationScript {
            version: "2024-test".into(),
            up_sql: "SELECT 1".into(),
            down_sql: "SELECT 0".into(),
            checksum: "sha256:abc".into(),
        };
        let json = serde_json::to_string(&script).expect("serialize");
        let parsed: MigrationScript = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.version, script.version);
        assert_eq!(parsed.checksum, script.checksum);
    }
}
