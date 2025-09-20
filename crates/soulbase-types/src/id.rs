#[cfg(feature = "schema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct Id(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct CausationId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct CorrelationId(pub String);

impl Id {
    #[cfg(feature = "uuid")]
    pub fn new_random() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    #[cfg(not(feature = "uuid"))]
    pub fn new_random() -> Self {
        // Non-uuid builds use a simple random suffix. Production can override.
        Self(format!("id_{}", nanoid::nanoid!()))
    }
}
