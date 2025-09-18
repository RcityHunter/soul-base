#[cfg(feature = "schema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct TraceContext {
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    #[serde(default)]
    pub baggage: serde_json::Map<String, serde_json::Value>,
}
