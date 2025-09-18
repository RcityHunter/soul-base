use super::*;
use crate::model::{Layer, ProvenanceEntry};
use crate::{
    errors::{io_provider_unavailable, schema_invalid, ConfigError},
    model::KeyPath,
};
use chrono::Utc;
use serde_json::{Map, Value};
use std::path::PathBuf;

pub struct FileSource {
    pub paths: Vec<PathBuf>,
}

#[async_trait::async_trait]
impl Source for FileSource {
    fn id(&self) -> &'static str {
        "file"
    }

    async fn load(&self) -> Result<SourceSnapshot, ConfigError> {
        let mut merged = Map::new();
        let mut provenance = Vec::new();

        for path in &self.paths {
            let content = std::fs::read_to_string(path)
                .map_err(|e| io_provider_unavailable("read file", &e.to_string()))?;
            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
            let mut value = match ext {
                "json" => serde_json::from_str::<Value>(&content)
                    .map_err(|e| schema_invalid("json parse", &e.to_string()))?,
                "yml" | "yaml" => {
                    #[cfg(feature = "yaml")]
                    {
                        serde_yaml::from_str::<Value>(&content)
                            .map_err(|e| schema_invalid("yaml parse", &e.to_string()))?
                    }
                    #[cfg(not(feature = "yaml"))]
                    {
                        Value::Null
                    }
                }
                "toml" => {
                    #[cfg(feature = "toml")]
                    {
                        let parsed: toml::Value = toml::from_str(&content)
                            .map_err(|e| schema_invalid("toml parse", &e.to_string()))?;
                        serde_json::to_value(parsed)
                            .map_err(|e| schema_invalid("toml convert", &e.to_string()))?
                    }
                    #[cfg(not(feature = "toml"))]
                    {
                        Value::Null
                    }
                }
                _ => Value::Null,
            };

            if let Value::Object(obj) = value.take() {
                merge_into(&mut merged, obj);
                provenance.push(ProvenanceEntry {
                    key: KeyPath("**".into()),
                    source_id: self.id().to_string(),
                    layer: Layer::File,
                    version: None,
                    ts_ms: Utc::now().timestamp_millis(),
                });
            }
        }

        Ok(SourceSnapshot {
            map: merged,
            provenance,
        })
    }
}

fn merge_into(dst: &mut Map<String, Value>, src: Map<String, Value>) {
    for (key, value) in src {
        match (dst.get_mut(&key), value) {
            (Some(Value::Object(dst_obj)), Value::Object(src_obj)) => {
                merge_object(dst_obj, src_obj)
            }
            (_, v) => {
                dst.insert(key, v);
            }
        }
    }
}

fn merge_object(dst: &mut Map<String, Value>, src: Map<String, Value>) {
    for (key, value) in src {
        match (dst.get_mut(&key), value) {
            (Some(Value::Object(dst_obj)), Value::Object(src_obj)) => {
                merge_object(dst_obj, src_obj)
            }
            (_, v) => {
                dst.insert(key, v);
            }
        }
    }
}
