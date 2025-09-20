use soulbase_errors::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct StorageError(pub Box<ErrorObj>);

impl StorageError {
    pub fn into_inner(self) -> ErrorObj {
        *self.0
    }

    pub fn not_found(msg: &str) -> Self {
        StorageError(Box::new(
            ErrorBuilder::new(codes::STORAGE_NOT_FOUND)
                .user_msg("Resource not found.")
                .dev_msg(msg)
                .build(),
        ))
    }

    pub fn conflict(msg: &str) -> Self {
        StorageError(Box::new(
            ErrorBuilder::new(codes::STORAGE_CONFLICT)
                .user_msg("Storage conflict occurred.")
                .dev_msg(msg)
                .build(),
        ))
    }

    pub fn unavailable(msg: &str) -> Self {
        StorageError(Box::new(
            ErrorBuilder::new(codes::STORAGE_UNAVAILABLE)
                .user_msg("Storage backend unavailable.")
                .dev_msg(msg)
                .build(),
        ))
    }

    pub fn bad_request(msg: &str) -> Self {
        StorageError(Box::new(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Invalid storage request.")
                .dev_msg(msg)
                .build(),
        ))
    }

    pub fn provider_unavailable(msg: &str) -> Self {
        StorageError(Box::new(
            ErrorBuilder::new(codes::PROVIDER_UNAVAILABLE)
                .user_msg("Upstream provider unavailable.")
                .dev_msg(msg)
                .build(),
        ))
    }

    pub fn internal(msg: &str) -> Self {
        StorageError(Box::new(
            ErrorBuilder::new(codes::UNKNOWN_INTERNAL)
                .user_msg("Storage internal error.")
                .dev_msg(msg)
                .build(),
        ))
    }
}
