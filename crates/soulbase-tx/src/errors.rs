use soulbase_errors::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct TxError(pub ErrorObj);

impl TxError {
    pub fn into_inner(self) -> ErrorObj {
        self.0
    }

    pub fn provider_unavailable(msg: &str) -> Self {
        TxError(
            ErrorBuilder::new(codes::PROVIDER_UNAVAILABLE)
                .user_msg("Upstream is unavailable. Please retry later.")
                .dev_msg(msg)
                .build(),
        )
    }

    pub fn conflict(msg: &str) -> Self {
        TxError(
            ErrorBuilder::new(codes::STORAGE_CONFLICT)
                .user_msg("Concurrent modification detected.")
                .dev_msg(msg)
                .build(),
        )
    }

    pub fn not_found(msg: &str) -> Self {
        TxError(
            ErrorBuilder::new(codes::STORAGE_NOT_FOUND)
                .user_msg("Requested record not found.")
                .dev_msg(msg)
                .build(),
        )
    }

    pub fn bad_request(msg: &str) -> Self {
        TxError(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Invalid transaction request.")
                .dev_msg(msg)
                .build(),
        )
    }

    pub fn internal(msg: &str) -> Self {
        TxError(
            ErrorBuilder::new(codes::UNKNOWN_INTERNAL)
                .user_msg("Internal transaction error.")
                .dev_msg(msg)
                .build(),
        )
    }
}

impl From<ErrorObj> for TxError {
    fn from(value: ErrorObj) -> Self {
        TxError(value)
    }
}
