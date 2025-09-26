use soulbase_errors::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct NetError(pub Box<ErrorObj>);

impl NetError {
    pub fn into_inner(self) -> ErrorObj {
        *self.0
    }

    pub fn provider_unavailable(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::PROVIDER_UNAVAILABLE)
                .user_msg("Upstream unavailable.")
                .dev_msg(msg),
        )
    }

    pub fn timeout(phase: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::LLM_TIMEOUT)
                .user_msg(format!("{phase} timeout"))
                .dev_msg(phase),
        )
    }

    pub fn forbidden(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::AUTH_FORBIDDEN)
                .user_msg("Forbidden.")
                .dev_msg(msg),
        )
    }

    pub fn schema(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Invalid request.")
                .dev_msg(msg),
        )
    }

    pub fn unknown(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::UNKNOWN_INTERNAL)
                .user_msg("Internal error.")
                .dev_msg(msg),
        )
    }

    fn from_builder(builder: ErrorBuilder) -> Self {
        NetError(Box::new(builder.build()))
    }
}

impl From<ErrorObj> for NetError {
    fn from(value: ErrorObj) -> Self {
        NetError(Box::new(value))
    }
}
