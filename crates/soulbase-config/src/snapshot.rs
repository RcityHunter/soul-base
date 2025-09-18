use crate::model::{Checksum, KeyPath, NamespaceId, ProvenanceEntry, SnapshotVersion};
use crate::{access, errors::ConfigError};
use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    version: SnapshotVersion,
    checksum: Checksum,
    issued_at_ms: i64,
    tree: serde_json::Value,
    provenance: Vec<ProvenanceEntry>,
    reload_policy: Option<String>,
}

impl ConfigSnapshot {
    pub fn from_tree(
        tree: serde_json::Value,
        version: SnapshotVersion,
        provenance: Vec<ProvenanceEntry>,
        reload_policy: Option<String>,
    ) -> Self {
        let bytes = serde_json::to_vec(&tree).expect("serialize snapshot");
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let checksum = STANDARD_NO_PAD.encode(hasher.finalize());

        Self {
            version,
            checksum: Checksum(checksum),
            issued_at_ms: Utc::now().timestamp_millis(),
            tree,
            provenance,
            reload_policy,
        }
    }

    pub fn version(&self) -> &SnapshotVersion {
        &self.version
    }
    pub fn checksum(&self) -> &Checksum {
        &self.checksum
    }
    pub fn issued_at_ms(&self) -> i64 {
        self.issued_at_ms
    }
    pub fn tree(&self) -> &serde_json::Value {
        &self.tree
    }
    pub fn provenance(&self) -> &[ProvenanceEntry] {
        &self.provenance
    }
    pub fn reload_policy(&self) -> Option<&str> {
        self.reload_policy.as_deref()
    }

    pub fn get_raw(&self, path: &KeyPath) -> Option<&serde_json::Value> {
        access::get_path(&self.tree, &path.0)
    }

    pub fn get<T: serde::de::DeserializeOwned>(&self, path: &KeyPath) -> Result<T, ConfigError> {
        let value = self
            .get_raw(path)
            .ok_or_else(|| crate::errors::schema_invalid("missing", &path.0))?;
        serde_json::from_value(value.clone())
            .map_err(|e| crate::errors::schema_invalid("type", &e.to_string()))
    }

    pub fn ns(&self, namespace: &NamespaceId) -> serde_json::Value {
        access::get_path(&self.tree, &namespace.0)
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}))
    }
}
