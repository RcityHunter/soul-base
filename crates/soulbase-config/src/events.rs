use crate::model::{KeyPath, ReloadClass, SnapshotVersion};
use serde::{Deserialize, Serialize};
use serde_json::Map;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigUpdateEvent {
    pub from_version: Option<SnapshotVersion>,
    pub to_version: SnapshotVersion,
    pub changed_keys: Vec<KeyPath>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_diff: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance_summary: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hotreload_class_summary: Option<Vec<(KeyPath, ReloadClass)>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigErrorEvent {
    pub phase: String,
    pub code: &'static str,
    pub message_user: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Map<String, serde_json::Value>>,
}
