use crate::{id::Id, tenant::TenantId};
#[cfg(feature = "schema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub enum SubjectKind {
    User,
    Service,
    Agent,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct Subject {
    pub kind: SubjectKind,
    pub subject_id: Id,
    pub tenant: TenantId,
    #[serde(default)]
    pub claims: serde_json::Map<String, serde_json::Value>,
}
