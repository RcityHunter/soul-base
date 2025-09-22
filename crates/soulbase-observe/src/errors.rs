use soulbase_errors::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct ObserveError(pub Box<ErrorObj>);

impl ObserveError {
    pub fn into_inner(self) -> ErrorObj {
        *self.0
    }

    pub fn internal(msg: &str) -> Self {
        Self(Box::new(
            ErrorBuilder::new(codes::UNKNOWN_INTERNAL)
                .user_msg("Observe pipeline internal error.")
                .dev_msg(msg)
                .build(),
        ))
    }

    pub fn unavailable(msg: &str) -> Self {
        Self(Box::new(
            ErrorBuilder::new(codes::PROVIDER_UNAVAILABLE)
                .user_msg("Observe exporter unavailable.")
                .dev_msg(msg)
                .build(),
        ))
    }

    pub fn bad_request(msg: &str) -> Self {
        Self(Box::new(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Observe request invalid.")
                .dev_msg(msg)
                .build(),
        ))
    }
}

impl From<ErrorObj> for ObserveError {
    fn from(value: ErrorObj) -> Self {
        Self(Box::new(value))
    }
}
