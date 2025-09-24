use soulbase_errors::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct CryptoError(pub Box<ErrorObj>);

impl CryptoError {
    pub fn into_inner(self) -> ErrorObj {
        *self.0
    }

    pub fn canonical(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Canonicalisation failed for provided payload.")
                .dev_msg(msg),
        )
    }

    pub fn digest(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Digest computation failed.")
                .dev_msg(msg),
        )
    }

    pub fn unsupported(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Requested algorithm is not supported.")
                .dev_msg(msg),
        )
    }

    pub fn signature_invalid(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::AUTH_FORBIDDEN)
                .user_msg("Signature verification failed.")
                .dev_msg(msg),
        )
    }

    pub fn keystore_unavailable(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::PROVIDER_UNAVAILABLE)
                .user_msg("Keystore provider unavailable.")
                .dev_msg(msg),
        )
    }

    pub fn keystore_forbidden(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::AUTH_FORBIDDEN)
                .user_msg("Key access denied or key revoked.")
                .dev_msg(msg),
        )
    }

    pub fn aead(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::AUTH_FORBIDDEN)
                .user_msg("Unable to decrypt or verify AEAD payload.")
                .dev_msg(msg),
        )
    }

    pub fn hkdf(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("HKDF derivation failed.")
                .dev_msg(msg),
        )
    }

    pub fn unknown(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::UNKNOWN_INTERNAL)
                .user_msg("Internal crypto error.")
                .dev_msg(msg),
        )
    }

    fn from_builder(builder: ErrorBuilder) -> Self {
        CryptoError(Box::new(builder.build()))
    }
}

impl From<ErrorObj> for CryptoError {
    fn from(value: ErrorObj) -> Self {
        CryptoError(Box::new(value))
    }
}
