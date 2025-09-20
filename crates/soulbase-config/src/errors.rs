use soulbase_errors::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct ConfigError(pub Box<ErrorObj>);

impl ConfigError {
    pub fn into_inner(self) -> ErrorObj {
        *self.0
    }
}

pub fn schema_invalid(phase: &str, detail: &str) -> ConfigError {
    ConfigError(Box::new(
        ErrorBuilder::new(codes::SCHEMA_VALIDATION)
            .user_msg("Configuration is invalid.")
            .dev_msg(format!("{phase}: {detail}"))
            .build(),
    ))
}

pub fn io_provider_unavailable(phase: &str, detail: &str) -> ConfigError {
    ConfigError(Box::new(
        ErrorBuilder::new(codes::PROVIDER_UNAVAILABLE)
            .user_msg("Configuration source is unavailable.")
            .dev_msg(format!("{phase}: {detail}"))
            .build(),
    ))
}
