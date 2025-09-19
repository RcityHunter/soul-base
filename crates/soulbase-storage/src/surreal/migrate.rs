#![cfg(feature = "surreal")]

use super::errors::map_surreal_error;
use super::observe::record_backend;
use crate::errors::StorageError;
use crate::spi::migrate::{MigrationScript, Migrator};
use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Instant;
use surrealdb::{engine::any::Any, Surreal};

const MIGRATION_TABLE: &str = "storage_migrations";

#[derive(Clone)]
pub struct SurrealMigrator {
    client: Arc<Surreal<Any>>,
    table: String,
}

impl SurrealMigrator {
    pub(crate) fn new(client: Arc<Surreal<Any>>) -> Self {
        Self {
            client,
            table: MIGRATION_TABLE.to_string(),
        }
    }

    async fn ensure_table(&self) -> Result<(), StorageError> {
        let stmt = format!("DEFINE TABLE {} SCHEMALESS", self.table);
        match self.client.query(stmt).await {
            Ok(_) => Ok(()),
            Err(err) => {
                let msg = err.to_string().to_lowercase();
                if msg.contains("already") && msg.contains("defined") {
                    Ok(())
                } else {
                    Err(map_surreal_error(err, "surreal migrate define"))
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct MigrationRow {
    version: String,
}

#[async_trait]
impl Migrator for SurrealMigrator {
    async fn current_version(&self) -> Result<String, StorageError> {
        self.ensure_table().await?;
        let started = Instant::now();
        let mut response = self
            .client
            .query(format!(
                "SELECT version FROM {} ORDER BY applied_at DESC LIMIT 1",
                self.table
            ))
            .await
            .map_err(|err| map_surreal_error(err, "surreal migrate current"))?;
        let rows: Vec<MigrationRow> = response
            .take(0)
            .map_err(|err| map_surreal_error(err, "surreal migrate current read"))?;
        record_backend(
            "surreal.migrate.current",
            started.elapsed(),
            rows.len(),
            None,
        );
        Ok(rows
            .into_iter()
            .next()
            .map(|row| row.version)
            .unwrap_or_else(|| "none".to_string()))
    }

    async fn apply_up(&self, scripts: &[MigrationScript]) -> Result<(), StorageError> {
        if scripts.is_empty() {
            return Ok(());
        }
        self.ensure_table().await?;
        let started = Instant::now();
        self.client
            .query("BEGIN TRANSACTION")
            .await
            .map_err(|err| map_surreal_error(err, "surreal migrate begin"))?;
        for script in scripts {
            self.client
                .query(&script.up_sql)
                .await
                .map_err(|err| map_surreal_error(err, "surreal migrate up"))?;
            let mut writer = self.client.query(format!(
                "CREATE {} SET version = $version, checksum = $checksum, applied_at = time::now()",
                self.table
            ));
            writer = writer.bind(("version", script.version.clone()));
            writer = writer.bind(("checksum", script.checksum.clone()));
            writer
                .await
                .map_err(|err| map_surreal_error(err, "surreal migrate audit"))?;
        }
        self.client
            .query("COMMIT TRANSACTION")
            .await
            .map_err(|err| map_surreal_error(err, "surreal migrate commit"))?;
        record_backend("surreal.migrate.up", started.elapsed(), scripts.len(), None);
        Ok(())
    }

    async fn apply_down(&self, scripts: &[MigrationScript]) -> Result<(), StorageError> {
        if scripts.is_empty() {
            return Ok(());
        }
        self.ensure_table().await?;
        let started = Instant::now();
        self.client
            .query("BEGIN TRANSACTION")
            .await
            .map_err(|err| map_surreal_error(err, "surreal migrate begin"))?;
        for script in scripts.iter().rev() {
            self.client
                .query(&script.down_sql)
                .await
                .map_err(|err| map_surreal_error(err, "surreal migrate down"))?;
            let mut writer = self
                .client
                .query(format!("DELETE {} WHERE version = $version", self.table));
            writer = writer.bind(("version", script.version.clone()));
            writer
                .await
                .map_err(|err| map_surreal_error(err, "surreal migrate delete"))?;
        }
        self.client
            .query("COMMIT TRANSACTION")
            .await
            .map_err(|err| map_surreal_error(err, "surreal migrate commit"))?;
        record_backend(
            "surreal.migrate.down",
            started.elapsed(),
            scripts.len(),
            None,
        );
        Ok(())
    }
}
