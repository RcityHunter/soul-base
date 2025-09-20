use super::*;
use crate::model::{KeyPath, Layer, ProvenanceEntry};
use crate::{access, errors::ConfigError};
use chrono::Utc;

pub struct EnvSource {
    pub prefix: String,
    pub separator: String,
}

#[async_trait::async_trait]
impl Source for EnvSource {
    fn id(&self) -> &'static str {
        "env"
    }

    async fn load(&self) -> Result<SourceSnapshot, ConfigError> {
        let mut map = serde_json::Map::new();
        let mut provenance = Vec::new();

        for (key, value) in std::env::vars() {
            if !key.starts_with(&self.prefix) {
                continue;
            }
            let trimmed = key.trim_start_matches(&self.prefix);
            let trimmed = trimmed.trim_start_matches(&self.separator);
            if trimmed.is_empty() {
                continue;
            }
            let normalized = trimmed
                .split(&self.separator)
                .filter(|seg| !seg.is_empty())
                .map(|seg| seg.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join(".");

            access::set_path(&mut map, &normalized, serde_json::Value::String(value));
            provenance.push(ProvenanceEntry {
                key: KeyPath(normalized),
                source_id: self.id().to_string(),
                layer: Layer::Env,
                version: None,
                ts_ms: Utc::now().timestamp_millis(),
            });
        }

        Ok(SourceSnapshot { map, provenance })
    }
}
