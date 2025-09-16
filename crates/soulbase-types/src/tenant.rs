use serde::{Deserialize, Serialize};
#[cfg(feature = "schema")]
use schemars::JsonSchema;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct TenantId(pub String);
