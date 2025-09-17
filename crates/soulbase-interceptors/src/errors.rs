use soulbase_errors::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct InterceptError(pub ErrorObj);

impl InterceptError {
    pub fn into_inner(self) -> ErrorObj {
        self.0
    }

    pub fn internal(msg: &str) -> Self {
        InterceptError(
            ErrorBuilder::new(codes::UNKNOWN_INTERNAL)
                .user_msg("Internal error. Please retry later.")
                .dev_msg(msg)
                .build(),
        )
    }

    pub fn schema(msg: &str) -> Self {
        InterceptError(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Invalid request.")
                .dev_msg(msg)
                .build(),
        )
    }

    pub fn from_error(err: ErrorObj) -> Self {
        InterceptError(err)
    }

    pub fn from_public(code: ErrorCode, msg: &str) -> Self {
        InterceptError(ErrorBuilder::new(code).user_msg(msg).build())
    }

    pub fn deny_policy(reason: &str) -> Self {
        InterceptError(
            ErrorBuilder::new(codes::POLICY_DENY_TOOL)
                .user_msg("Operation denied by policy.")
                .dev_msg(reason)
                .build(),
        )
    }
}

pub fn to_http_response(err: &InterceptError) -> (u16, serde_json::Value) {
    let obj = &err.0;
    let public = obj.to_public();
    (
        obj.http_status,
        serde_json::json!({
            "code": public.code,
            "message": public.message,
            "correlation_id": public.correlation_id
        }),
    )
}
