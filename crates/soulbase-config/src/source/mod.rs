use crate::{
    errors::ConfigError,
    model::{ConfigMap, ProvenanceEntry},
    watch::WatchTx,
};
use async_trait::async_trait;

pub mod cli;
pub mod env;
pub mod file;

#[derive(Clone, Debug)]
pub struct SourceSnapshot {
    pub map: ConfigMap,
    pub provenance: Vec<ProvenanceEntry>,
}

#[async_trait]
pub trait Source: Send + Sync {
    fn id(&self) -> &'static str;
    async fn load(&self) -> Result<SourceSnapshot, ConfigError>;
    fn supports_watch(&self) -> bool {
        false
    }
    async fn watch(&self, _tx: WatchTx) -> Result<(), ConfigError> {
        Ok(())
    }
}
