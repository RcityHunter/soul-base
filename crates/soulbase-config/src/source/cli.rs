use super::*;
use crate::{
    access,
    errors::ConfigError,
    model::{KeyPath, Layer, ProvenanceEntry},
};
use chrono::Utc;

pub struct CliArgsSource {
    pub args: Vec<String>,
}

#[async_trait::async_trait]
impl Source for CliArgsSource {
    fn id(&self) -> &'static str {
        "cli"
    }

    async fn load(&self) -> Result<SourceSnapshot, ConfigError> {
        let mut map = serde_json::Map::new();
        let mut provenance = Vec::new();

        for arg in &self.args {
            if let Some((key, value)) = arg.strip_prefix("--").and_then(|s| s.split_once('=')) {
                access::set_path(&mut map, key, serde_json::Value::String(value.to_string()));
                provenance.push(ProvenanceEntry {
                    key: KeyPath(key.into()),
                    source_id: self.id().to_string(),
                    layer: Layer::Cli,
                    version: None,
                    ts_ms: Utc::now().timestamp_millis(),
                });
            }
        }

        Ok(SourceSnapshot { map, provenance })
    }
}
