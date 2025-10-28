use serde::{Deserialize, Serialize};
use soulbase_types::prelude::TenantId;

use crate::model::Entity;

#[cfg(feature = "surreal")]
use crate::surreal::schema::TABLE_VECTOR_MANIFEST;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorManifest {
    pub id: String,
    pub tenant: TenantId,
    pub index_id: String,
    #[serde(default)]
    pub description: Option<String>,
    pub metric: String,
    pub dims: i32,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub runtime: Option<String>,
    #[serde(default)]
    pub config: Option<serde_json::Value>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Entity for VectorManifest {
    #[cfg(feature = "surreal")]
    const TABLE: &'static str = TABLE_VECTOR_MANIFEST;
    #[cfg(not(feature = "surreal"))]
    const TABLE: &'static str = "vector_manifest";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}
