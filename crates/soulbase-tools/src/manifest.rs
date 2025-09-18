use serde::{Deserialize, Serialize};

#[cfg(feature = "schema_json")]
use schemars::schema::RootSchema;

#[cfg(feature = "schema_json")]
pub type SchemaDoc = RootSchema;
#[cfg(not(feature = "schema_json"))]
pub type SchemaDoc = serde_json::Value;

use crate::errors::ToolError;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ToolId(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SafetyClass {
    Low,
    Medium,
    High,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SideEffect {
    None,
    Read,
    Write,
    Network,
    Filesystem,
    Browser,
    Process,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsentPolicy {
    pub required: bool,
    #[serde(default)]
    pub max_ttl_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Limits {
    pub timeout_ms: u64,
    pub max_bytes_in: u64,
    pub max_bytes_out: u64,
    pub max_files: u64,
    pub max_depth: u32,
    pub max_concurrency: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CapabilityDecl {
    pub domain: String,
    pub action: String,
    pub resource: String,
    #[serde(default)]
    pub attrs: serde_json::Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdempoKind {
    None,
    Keyed,
    Global,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConcurrencyKind {
    Parallel,
    Queue,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolManifest {
    pub id: ToolId,
    pub version: String,
    pub display_name: String,
    pub description: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub input_schema: SchemaDoc,
    pub output_schema: SchemaDoc,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub capabilities: Vec<CapabilityDecl>,
    pub side_effect: SideEffect,
    pub safety_class: SafetyClass,
    pub consent: ConsentPolicy,
    pub limits: Limits,
    pub idempotency: IdempoKind,
    pub concurrency: ConcurrencyKind,
}

impl ToolManifest {
    pub fn validate_input(&self, value: &serde_json::Value) -> Result<(), ToolError> {
        validate_schema(&self.input_schema, value)
    }

    pub fn validate_output(&self, value: &serde_json::Value) -> Result<(), ToolError> {
        validate_schema(&self.output_schema, value)
    }

    pub fn fingerprint(&self) -> u64 {
        use ahash::AHasher;
        use std::hash::Hasher;

        let mut hasher = AHasher::default();
        let repr = serde_json::json!({
            "id": &self.id.0,
            "version": &self.version,
            "caps": &self.capabilities,
            "limits": {
                "timeout_ms": self.limits.timeout_ms,
                "max_bytes_in": self.limits.max_bytes_in,
                "max_bytes_out": self.limits.max_bytes_out,
                "max_files": self.limits.max_files,
                "max_depth": self.limits.max_depth,
                "max_concurrency": self.limits.max_concurrency,
            },
        });
        let bytes = serde_json::to_vec(&repr).unwrap_or_default();
        hasher.write(&bytes);
        hasher.finish()
    }
}

#[cfg(feature = "schema_json")]
fn validate_schema(schema: &SchemaDoc, value: &serde_json::Value) -> Result<(), ToolError> {
    use jsonschema::{Draft, JSONSchema};

    let schema_json = serde_json::to_value(schema)
        .map_err(|e| ToolError::schema(&format!("schema serialize: {e}")))?;
    let compiled = JSONSchema::options()
        .with_draft(Draft::Draft7)
        .compile(&schema_json)
        .map_err(|e| ToolError::schema(&format!("schema compile: {e}")))?;
    if let Err(errors) = compiled.validate(value) {
        let first = errors
            .into_iter()
            .next()
            .map(|err| err.to_string())
            .unwrap_or_else(|| "schema validation failed".to_string());
        return Err(ToolError::schema(&first));
    }
    Ok(())
}

#[cfg(not(feature = "schema_json"))]
fn validate_schema(_: &SchemaDoc, _: &serde_json::Value) -> Result<(), ToolError> {
    Ok(())
}
