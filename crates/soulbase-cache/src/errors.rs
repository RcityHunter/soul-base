use soulbase_errors::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct CacheError(pub Box<ErrorObj>);

impl CacheError {
    pub fn into_inner(self) -> ErrorObj {
        *self.0
    }

    pub fn provider_unavailable(msg: &str) -> Self {
        Self(Box::new(
            ErrorBuilder::new(codes::PROVIDER_UNAVAILABLE)
                .user_msg("Cache backend unavailable.")
                .dev_msg(msg)
                .build(),
        ))
    }

    pub fn schema(msg: &str) -> Self {
        Self(Box::new(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Cache codec error.")
                .dev_msg(msg)
                .build(),
        ))
    }

    pub fn unknown(msg: &str) -> Self {
        Self(Box::new(
            ErrorBuilder::new(codes::UNKNOWN_INTERNAL)
                .user_msg("Cache internal error.")
                .dev_msg(msg)
                .build(),
        ))
    }
}

impl From<ErrorObj> for CacheError {
    fn from(value: ErrorObj) -> Self {
        Self(Box::new(value))
    }
}
