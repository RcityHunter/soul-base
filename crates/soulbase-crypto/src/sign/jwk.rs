use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::base64url;
use crate::errors::CryptoError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JwkPublicKey {
    pub kid: String,
    pub kty: String,
    pub crv: String,
    pub x: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<i64>,
}

impl JwkPublicKey {
    pub fn from_verifying_key(
        kid: impl Into<String>,
        verifying_key: &VerifyingKey,
        expires_at_ms: Option<i64>,
    ) -> Self {
        Self {
            kid: kid.into(),
            kty: "OKP".to_string(),
            crv: "Ed25519".to_string(),
            x: base64url::encode(verifying_key.as_bytes()),
            exp: expires_at_ms,
        }
    }

    pub fn to_verifying_key(&self) -> Result<VerifyingKey, CryptoError> {
        let bytes = base64url::decode(&self.x)?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| CryptoError::canonical("invalid ed25519 public key length"))?;
        VerifyingKey::from_bytes(&arr)
            .map_err(|err| CryptoError::canonical(&format!("invalid ed25519 public key: {err}")))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JwkPrivateKey {
    pub kid: String,
    pub kty: String,
    pub crv: String,
    pub x: String,
    pub d: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<i64>,
}

impl JwkPrivateKey {
    pub fn from_keys(
        kid: impl Into<String>,
        secret: &Zeroizing<[u8; 32]>,
        verifying: &VerifyingKey,
        expires_at_ms: Option<i64>,
    ) -> Self {
        Self {
            kid: kid.into(),
            kty: "OKP".to_string(),
            crv: "Ed25519".to_string(),
            x: base64url::encode(verifying.as_bytes()),
            d: base64url::encode(secret.as_ref()),
            exp: expires_at_ms,
        }
    }

    pub fn to_signing_key(&self) -> Result<SigningKey, CryptoError> {
        let secret = base64url::decode(&self.d)?;
        let arr: [u8; 32] = secret
            .try_into()
            .map_err(|_| CryptoError::canonical("invalid ed25519 secret length"))?;
        Ok(SigningKey::from_bytes(&arr))
    }
}
