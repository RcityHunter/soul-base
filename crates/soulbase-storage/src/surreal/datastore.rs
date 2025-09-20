use super::{
    config::SurrealConfig, errors::map_surreal_error, migrate::SurrealMigrator,
    session::SurrealSession,
};
use crate::errors::StorageError;
use crate::spi::datastore::Datastore;
use crate::spi::health::HealthCheck;
use async_trait::async_trait;
use std::sync::Arc;
use surrealdb::engine::any::{connect, Any};
use surrealdb::{opt::auth, Surreal};

#[derive(Clone)]
pub struct SurrealDatastore {
    client: Arc<Surreal<Any>>,
    config: SurrealConfig,
    is_mem: bool,
}

impl SurrealDatastore {
    pub async fn connect(config: SurrealConfig) -> Result<Self, StorageError> {
        let is_mem = config.endpoint.starts_with("mem://");
        let client = connect(config.endpoint.as_str())
            .await
            .map_err(|err| map_surreal_error(err, "surreal connect"))?;

        if !is_mem {
            if let (Some(username), Some(password)) =
                (config.username.clone(), config.password.clone())
            {
                client
                    .signin(auth::Root {
                        username: &username,
                        password: &password,
                    })
                    .await
                    .map_err(|err| map_surreal_error(err, "surreal signin"))?;
            }
        }

        client
            .use_ns(config.namespace.as_str())
            .use_db(config.database.as_str())
            .await
            .map_err(|err| map_surreal_error(err, "surreal use namespace"))?;

        Ok(Self {
            client: Arc::new(client),
            config,
            is_mem,
        })
    }

    pub fn config(&self) -> &SurrealConfig {
        &self.config
    }

    pub fn migrator(&self) -> SurrealMigrator {
        SurrealMigrator::new(self.client.clone())
    }
}

#[async_trait]
impl Datastore for SurrealDatastore {
    type Session = SurrealSession;

    async fn session(&self) -> Result<Self::Session, StorageError> {
        Ok(SurrealSession::new(self.client.clone()))
    }

    async fn shutdown(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

#[async_trait]
impl HealthCheck for SurrealDatastore {
    async fn ping(&self) -> Result<(), StorageError> {
        if self.is_mem {
            return Ok(());
        }
        let mut response = self
            .client
            .query("RETURN true")
            .await
            .map_err(|err| map_surreal_error(err, "surreal ping"))?;
        let ok: Option<bool> = response
            .take(0)
            .map_err(|err| map_surreal_error(err, "surreal ping read"))?;
        if ok.unwrap_or(false) {
            Ok(())
        } else {
            Err(StorageError::provider_unavailable(
                "surreal ping returned false",
            ))
        }
    }
}
