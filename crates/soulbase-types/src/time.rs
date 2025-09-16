use serde::{Deserialize, Serialize};
#[cfg(feature = "schema")]
use schemars::JsonSchema;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct Timestamp(pub i64); // ms since epoch, UTC
