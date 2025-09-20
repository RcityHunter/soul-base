#[cfg(any(feature = "wrap-reqwest", feature = "wrap-sqlx"))]
use crate::model::{ErrorBuilder, ErrorObj};

#[cfg(any(feature = "wrap-reqwest", feature = "wrap-sqlx"))]
use crate::code::codes;

#[cfg(feature = "wrap-reqwest")]
use crate::code::ErrorCode;

#[cfg(feature = "wrap-reqwest")]
impl From<reqwest::Error> for ErrorObj {
    fn from(e: reqwest::Error) -> Self {
        let code: ErrorCode = if e.is_timeout() {
            codes::LLM_TIMEOUT
        } else {
            codes::PROVIDER_UNAVAILABLE
        };
        ErrorBuilder::new(code)
            .user_msg("Upstream provider is unavailable. Please retry later.")
            .dev_msg(format!("reqwest: {e}"))
            .meta_kv("provider", serde_json::json!("http"))
            .build()
    }
}

#[cfg(feature = "wrap-sqlx")]
impl From<sqlx::Error> for ErrorObj {
    fn from(e: sqlx::Error) -> Self {
        use sqlx::Error::*;

        let (code, user_msg) = match e {
            RowNotFound => (codes::STORAGE_NOT_FOUND, "Resource not found."),
            _ => (
                codes::PROVIDER_UNAVAILABLE,
                "Database is unavailable. Please retry later.",
            ),
        };

        ErrorBuilder::new(code)
            .user_msg(user_msg)
            .dev_msg(format!("sqlx: {e}"))
            .meta_kv("provider", serde_json::json!("db"))
            .build()
    }
}
