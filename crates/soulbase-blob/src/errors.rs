use soulbase_errors::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct BlobError(pub Box<ErrorObj>);

impl BlobError {
    pub fn into_inner(self) -> ErrorObj {
        *self.0
    }

    pub fn provider_unavailable(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::PROVIDER_UNAVAILABLE)
                .user_msg("Blob backend unavailable.")
                .dev_msg(msg),
        )
    }

    pub fn not_found(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::STORAGE_NOT_FOUND)
                .user_msg("Object not found.")
                .dev_msg(msg),
        )
    }

    pub fn forbidden(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::AUTH_FORBIDDEN)
                .user_msg("Access denied.")
                .dev_msg(msg),
        )
    }

    pub fn schema(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Invalid blob request.")
                .dev_msg(msg),
        )
    }

    pub fn unknown(msg: &str) -> Self {
        Self::from_builder(
            ErrorBuilder::new(codes::UNKNOWN_INTERNAL)
                .user_msg("Internal blob error.")
                .dev_msg(msg),
        )
    }

    fn from_builder(builder: ErrorBuilder) -> Self {
        BlobError(Box::new(builder.build()))
    }
}

impl From<ErrorObj> for BlobError {
    fn from(value: ErrorObj) -> Self {
        BlobError(Box::new(value))
    }
}
