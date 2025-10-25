use soulbase_config::errors::ConfigError;
use soulbase_errors::prelude::*;

#[cfg(feature = "s3")]
use soulbase_blob::errors::BlobError;
#[cfg(feature = "redis")]
use soulbase_cache::errors::CacheError;
#[cfg(feature = "kafka")]
use soulbase_tx::errors::TxError;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct InfraError(pub ErrorObj);

impl InfraError {
    pub fn config(detail: &str) -> Self {
        InfraError(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Infrastructure configuration is invalid.")
                .dev_msg(detail)
                .build(),
        )
    }

    pub fn provider_unavailable(detail: &str) -> Self {
        InfraError(
            ErrorBuilder::new(codes::PROVIDER_UNAVAILABLE)
                .user_msg("External infrastructure is unavailable.")
                .dev_msg(detail)
                .build(),
        )
    }

    pub fn feature_disabled(feature: &str, detail: &str) -> Self {
        InfraError(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Required capability is disabled.")
                .dev_msg(format!("feature '{feature}' is disabled: {detail}"))
                .build(),
        )
    }
}

impl From<InfraError> for ErrorObj {
    fn from(value: InfraError) -> Self {
        value.0
    }
}

impl From<ErrorObj> for InfraError {
    fn from(value: ErrorObj) -> Self {
        InfraError(value)
    }
}

impl From<ConfigError> for InfraError {
    fn from(err: ConfigError) -> Self {
        InfraError(err.into_inner())
    }
}

#[cfg(feature = "redis")]
impl From<CacheError> for InfraError {
    fn from(err: CacheError) -> Self {
        InfraError(err.into_inner())
    }
}

#[cfg(feature = "s3")]
impl From<BlobError> for InfraError {
    fn from(err: BlobError) -> Self {
        InfraError(err.into_inner())
    }
}

#[cfg(feature = "kafka")]
impl From<TxError> for InfraError {
    fn from(err: TxError) -> Self {
        InfraError(err.into_inner())
    }
}
