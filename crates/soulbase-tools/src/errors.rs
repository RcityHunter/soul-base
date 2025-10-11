use soulbase_auth::errors::AuthError;
use soulbase_errors::prelude::*;
use soulbase_sandbox::errors::SandboxError;
use thiserror::Error;

#[cfg(feature = "registry_surreal")]
use soulbase_storage::prelude::StorageError;
#[cfg(feature = "outbox")]
use soulbase_tx::errors::TxError;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct ToolError(pub Box<ErrorObj>);

impl ToolError {
    pub fn into_inner(self) -> ErrorObj {
        *self.0
    }

    pub fn schema(msg: &str) -> Self {
        ToolError(Box::new(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Tool arguments failed validation.")
                .dev_msg(msg)
                .build(),
        ))
    }

    pub fn not_found(tool_id: &str) -> Self {
        ToolError(Box::new(
            ErrorBuilder::new(codes::STORAGE_NOT_FOUND)
                .user_msg("Tool is not registered.")
                .dev_msg(format!("tool not found: {tool_id}"))
                .build(),
        ))
    }

    pub fn policy(msg: &str) -> Self {
        ToolError(Box::new(
            ErrorBuilder::new(codes::POLICY_DENY_TOOL)
                .user_msg("Tool invocation denied by policy.")
                .dev_msg(msg)
                .build(),
        ))
    }

    pub fn sandbox(err: SandboxError) -> Self {
        ToolError(Box::new(err.into_inner()))
    }

    pub fn unknown(msg: &str) -> Self {
        ToolError(Box::new(
            ErrorBuilder::new(codes::UNKNOWN_INTERNAL)
                .user_msg("Tool invocation failed.")
                .dev_msg(msg)
                .build(),
        ))
    }
}

impl From<AuthError> for ToolError {
    fn from(err: AuthError) -> Self {
        ToolError(Box::new(err.into_inner()))
    }
}

impl From<SandboxError> for ToolError {
    fn from(err: SandboxError) -> Self {
        ToolError::sandbox(err)
    }
}

#[cfg(feature = "registry_surreal")]
impl From<StorageError> for ToolError {
    fn from(err: StorageError) -> Self {
        ToolError(Box::new(err.into_inner()))
    }
}

#[cfg(feature = "outbox")]
impl From<TxError> for ToolError {
    fn from(err: TxError) -> Self {
        ToolError(Box::new(
            ErrorBuilder::new(codes::UNKNOWN_INTERNAL)
                .user_msg("Tool outbox dispatch failed.")
                .dev_msg(format!("outbox: {}", err))
                .build(),
        ))
    }
}
