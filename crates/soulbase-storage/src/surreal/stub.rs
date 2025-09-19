use crate::errors::StorageError;
use crate::spi::datastore::Datastore;
use crate::spi::migrate::{MigrationScript, Migrator};
use crate::spi::query::QueryExecutor;
use crate::spi::session::Session;
use crate::spi::tx::Transaction;
use async_trait::async_trait;
use serde_json::Value;

#[derive(Clone, Default)]
pub struct SurrealDatastore;

#[async_trait]
impl Datastore for SurrealDatastore {
    type Session = SurrealSession;

    async fn session(&self) -> Result<Self::Session, StorageError> {
        Err(StorageError::unavailable(
            "Surreal adapter is unavailable; enable the `surreal` feature",
        ))
    }
}

#[derive(Clone, Default)]
pub struct SurrealSession;

#[async_trait]
impl QueryExecutor for SurrealSession {
    async fn query(&self, _statement: &str, _params: Value) -> Result<Value, StorageError> {
        Err(StorageError::unavailable(
            "Surreal query cannot run without the `surreal` feature",
        ))
    }
}

#[async_trait]
impl Session for SurrealSession {
    type Tx = SurrealTransaction;

    async fn begin(&self) -> Result<Self::Tx, StorageError> {
        Err(StorageError::unavailable(
            "Surreal transactions require the `surreal` feature",
        ))
    }
}

#[derive(Clone, Default)]
pub struct SurrealTransaction;

#[async_trait]
impl QueryExecutor for SurrealTransaction {
    async fn query(&self, _statement: &str, _params: Value) -> Result<Value, StorageError> {
        Err(StorageError::unavailable(
            "Surreal transaction query requires the `surreal` feature",
        ))
    }
}

#[async_trait]
impl Transaction for SurrealTransaction {
    async fn commit(&mut self) -> Result<(), StorageError> {
        Err(StorageError::unavailable(
            "Surreal transaction commit requires the `surreal` feature",
        ))
    }

    async fn rollback(&mut self) -> Result<(), StorageError> {
        Err(StorageError::unavailable(
            "Surreal transaction rollback requires the `surreal` feature",
        ))
    }

    fn is_active(&self) -> bool {
        false
    }
}

#[derive(Clone, Default)]
pub struct SurrealMigrator;

#[async_trait]
impl Migrator for SurrealMigrator {
    async fn current_version(&self) -> Result<String, StorageError> {
        Err(StorageError::unavailable(
            "Surreal migrator requires the `surreal` feature",
        ))
    }

    async fn apply_up(&self, _scripts: &[MigrationScript]) -> Result<(), StorageError> {
        Err(StorageError::unavailable(
            "Surreal migrator requires the `surreal` feature",
        ))
    }

    async fn apply_down(&self, _scripts: &[MigrationScript]) -> Result<(), StorageError> {
        Err(StorageError::unavailable(
            "Surreal migrator requires the `surreal` feature",
        ))
    }
}

#[derive(Clone, Default)]
pub struct SurrealMapper;

impl SurrealMapper {
    pub fn hydrate<T: serde::de::DeserializeOwned>(value: Value) -> serde_json::Result<T> {
        serde_json::from_value(value)
    }
}
