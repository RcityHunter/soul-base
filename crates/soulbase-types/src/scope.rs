use crate::time::Timestamp;
use serde::{Deserialize, Serialize};
#[cfg(feature = "schema")]
use schemars::JsonSchema;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct Scope {
    pub resource: String,
    pub action: String,
    #[serde(default)]
    pub attrs: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct Consent {
    #[serde(default)]
    pub scopes: Vec<Scope>,
    pub expires_at: Option<Timestamp>,
    #[serde(default)]
    pub purpose: Option<String>,
}
