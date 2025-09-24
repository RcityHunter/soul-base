use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

use crate::errors::CryptoError;

pub fn encode(data: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(data)
}

pub fn decode(input: &str) -> Result<Vec<u8>, CryptoError> {
    URL_SAFE_NO_PAD
        .decode(input)
        .map_err(|err| CryptoError::canonical(&format!("base64url decode failed: {err}")))
}
