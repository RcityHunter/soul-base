use serde_json::Value;

use crate::errors::CryptoError;

use super::json::canonicalize_to_string;

pub trait Canonicalizer: Send + Sync {
    fn canonical_json(&self, value: &Value) -> Result<Vec<u8>, CryptoError>;

    fn canonical_json_string(&self, value: &Value) -> Result<String, CryptoError> {
        let bytes = self.canonical_json(value)?;
        String::from_utf8(bytes).map_err(|err| {
            CryptoError::canonical(&format!("canonical output was not valid UTF-8: {err}"))
        })
    }

    fn canonicalize_str(&self, raw_json: &str) -> Result<Vec<u8>, CryptoError> {
        let value: Value = serde_json::from_str(raw_json)
            .map_err(|err| CryptoError::canonical(&format!("invalid json: {err}")))?;
        self.canonical_json(&value)
    }
}

#[derive(Default, Clone, Copy)]
pub struct JsonCanonicalizer;

impl Canonicalizer for JsonCanonicalizer {
    fn canonical_json(&self, value: &Value) -> Result<Vec<u8>, CryptoError> {
        let rendered = canonicalize_to_string(value)?;
        Ok(rendered.into_bytes())
    }
}
