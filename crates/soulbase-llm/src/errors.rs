use soulbase_errors::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{0:?}")]
pub struct LlmError(pub Box<ErrorObj>);

impl LlmError {
    pub fn into_inner(self) -> ErrorObj {
        *self.0
    }
    pub fn provider_unavailable(msg: &str) -> Self {
        LlmError(Box::new(
            ErrorBuilder::new(codes::PROVIDER_UNAVAILABLE)
                .user_msg("Model provider is unavailable. Please retry later.")
                .dev_msg(msg)
                .build(),
        ))
    }
    pub fn timeout(msg: &str) -> Self {
        LlmError(Box::new(
            ErrorBuilder::new(codes::LLM_TIMEOUT)
                .user_msg("Model did not respond in time.")
                .dev_msg(msg)
                .build(),
        ))
    }
    pub fn context_overflow(msg: &str) -> Self {
        LlmError(Box::new(
            ErrorBuilder::new(codes::LLM_CONTEXT_OVERFLOW)
                .user_msg("Input exceeds model context window.")
                .dev_msg(msg)
                .build(),
        ))
    }
    pub fn schema(msg: &str) -> Self {
        LlmError(Box::new(
            ErrorBuilder::new(codes::SCHEMA_VALIDATION)
                .user_msg("Model output failed schema validation.")
                .dev_msg(msg)
                .build(),
        ))
    }
    pub fn unknown(msg: &str) -> Self {
        LlmError(Box::new(
            ErrorBuilder::new(codes::UNKNOWN_INTERNAL)
                .user_msg("Internal error.")
                .dev_msg(msg)
                .build(),
        ))
    }
}
