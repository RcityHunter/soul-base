use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyPath(pub String);

impl KeyPath {
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('.')
    }
}

pub type ConfigValue = serde_json::Value;
pub type ConfigMap = serde_json::Map<String, serde_json::Value>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReloadClass {
    BootOnly,
    HotReloadSafe,
    HotReloadRisky,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProvenanceEntry {
    pub key: KeyPath,
    pub source_id: String,
    pub layer: Layer,
    pub version: Option<String>,
    pub ts_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Layer {
    Defaults,
    File,
    RemoteKV,
    Env,
    Cli,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotVersion(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksum(pub String);
