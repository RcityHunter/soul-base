use soulbase_errors::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct SandboxError(pub Box<ErrorObj>);

impl SandboxError {
    pub fn into_inner(self) -> ErrorObj {
        *self.0
    }

    pub fn permission(reason: &str) -> Self {
        SandboxError(Box::new(
            ErrorBuilder::new(codes::SANDBOX_PERMISSION_DENY)
                .user_msg("Operation denied by sandbox policy.")
                .dev_msg(reason)
                .build(),
        ))
    }

    pub fn quota_budget(reason: &str) -> Self {
        SandboxError(Box::new(
            ErrorBuilder::new(codes::QUOTA_BUDGET)
                .user_msg("Sandbox budget exceeded.")
                .dev_msg(reason)
                .build(),
        ))
    }

    pub fn expired() -> Self {
        SandboxError(Box::new(
            ErrorBuilder::new(codes::POLICY_DENY_TOOL)
                .user_msg("Grant expired.")
                .dev_msg("grant expired")
                .build(),
        ))
    }

    pub fn internal(msg: &str) -> Self {
        SandboxError(Box::new(
            ErrorBuilder::new(codes::UNKNOWN_INTERNAL)
                .user_msg("Sandbox internal error.")
                .dev_msg(msg)
                .build(),
        ))
    }
}
