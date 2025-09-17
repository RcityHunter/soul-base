use soulbase_errors::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct AuthError(pub ErrorObj);

impl AuthError {
    pub fn into_inner(self) -> ErrorObj {
        self.0
    }
}

pub fn unauthenticated(msg: &str) -> AuthError {
    AuthError(
        ErrorBuilder::new(codes::AUTH_UNAUTHENTICATED)
            .user_msg("Please sign in.")
            .dev_msg(msg)
            .build(),
    )
}

pub fn forbidden(msg: &str) -> AuthError {
    AuthError(
        ErrorBuilder::new(codes::AUTH_FORBIDDEN)
            .user_msg("Forbidden.")
            .dev_msg(msg)
            .build(),
    )
}

pub fn rate_limited() -> AuthError {
    AuthError(
        ErrorBuilder::new(codes::QUOTA_RATELIMIT)
            .user_msg("Too many requests. Please retry later.")
            .build(),
    )
}

pub fn budget_exceeded() -> AuthError {
    AuthError(
        ErrorBuilder::new(codes::QUOTA_BUDGET)
            .user_msg("Budget exceeded.")
            .build(),
    )
}

pub fn policy_deny(msg: &str) -> AuthError {
    AuthError(
        ErrorBuilder::new(codes::POLICY_DENY_TOOL)
            .user_msg("Operation denied by policy.")
            .dev_msg(msg)
            .build(),
    )
}

pub fn provider_unavailable(msg: &str) -> AuthError {
    AuthError(
        ErrorBuilder::new(codes::PROVIDER_UNAVAILABLE)
            .user_msg("Upstream provider unavailable.")
            .dev_msg(msg)
            .build(),
    )
}
