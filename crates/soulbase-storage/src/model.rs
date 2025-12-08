use serde::{Deserialize, Serialize};
use soulbase_types::prelude::TenantId;

use crate::spi::query::QueryStats;

pub trait Entity: Sized + serde::de::DeserializeOwned + Serialize + Send + Sync {
    const TABLE: &'static str;
    fn id(&self) -> &str;
    fn tenant(&self) -> &TenantId;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<QueryStats>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryParams {
    pub filter: serde_json::Value,
    #[serde(default)]
    pub order_by: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub cursor: Option<String>,
}

impl Default for QueryParams {
    fn default() -> Self {
        Self {
            filter: serde_json::json!({}),
            order_by: None,
            limit: None,
            cursor: None,
        }
    }
}

/// Sort direction for query results (backward compatibility)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sort {
    pub field: String,
    pub descending: bool,
}

impl Sort {
    pub fn ascending(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            descending: false,
        }
    }

    pub fn descending(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            descending: true,
        }
    }
}

/// Build a record ID from table, tenant, and suffix (backward compatibility)
pub fn make_record_id(table: &str, tenant: &TenantId, suffix: &str) -> String {
    format!("{}:{}:{}", table, tenant.0, suffix)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationVersion {
    pub version: String,
    pub checksum: String,
}

pub mod awareness;
pub mod autonomous;
pub mod causal;
pub mod dfr;
pub mod embedding;
pub mod evolution;
pub mod metacognition;
pub mod recall;
pub mod timeseries;
pub mod timeline;
pub mod vector_manifest;
