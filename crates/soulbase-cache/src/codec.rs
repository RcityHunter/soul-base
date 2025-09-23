use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::errors::CacheError;

pub trait Codec: Send + Sync + 'static {
    fn encode<T: Serialize + Send + Sync>(&self, value: &T) -> Result<Bytes, CacheError>;
    fn decode<T: DeserializeOwned + Send + Sync>(&self, bytes: &Bytes) -> Result<T, CacheError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    fn encode<T: Serialize + Send + Sync>(&self, value: &T) -> Result<Bytes, CacheError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| CacheError::schema(&format!("encode json: {e}")))?;
        Ok(Bytes::from(bytes))
    }

    fn decode<T: DeserializeOwned + Send + Sync>(&self, bytes: &Bytes) -> Result<T, CacheError> {
        serde_json::from_slice(bytes).map_err(|e| CacheError::schema(&format!("decode json: {e}")))
    }
}
