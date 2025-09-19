use crate::{kind::ErrorKind, retry::RetryClass, severity::Severity};
use once_cell::sync::Lazy;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ErrorCode(pub &'static str);

impl Serialize for ErrorCode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0)
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(ErrorCode(Box::leak(s.into_boxed_str())))
    }
}

#[derive(Clone, Debug)]
pub struct CodeSpec {
    pub code: ErrorCode,
    pub kind: ErrorKind,
    pub http_status: u16,
    pub grpc_status: Option<i32>,
    pub retryable: RetryClass,
    pub severity: Severity,
    pub default_user_msg: &'static str,
}

pub mod codes {
    use super::ErrorCode;

    pub const AUTH_UNAUTHENTICATED: ErrorCode = ErrorCode("AUTH.UNAUTHENTICATED");
    pub const AUTH_FORBIDDEN: ErrorCode = ErrorCode("AUTH.FORBIDDEN");
    pub const SCHEMA_VALIDATION: ErrorCode = ErrorCode("SCHEMA.VALIDATION_FAILED");
    pub const QUOTA_RATELIMIT: ErrorCode = ErrorCode("QUOTA.RATE_LIMITED");
    pub const QUOTA_BUDGET: ErrorCode = ErrorCode("QUOTA.BUDGET_EXCEEDED");
    pub const POLICY_DENY_TOOL: ErrorCode = ErrorCode("POLICY.DENY_TOOL");
    pub const LLM_TIMEOUT: ErrorCode = ErrorCode("LLM.TIMEOUT");
    pub const LLM_CONTEXT_OVERFLOW: ErrorCode = ErrorCode("LLM.CONTEXT_OVERFLOW");
    pub const PROVIDER_UNAVAILABLE: ErrorCode = ErrorCode("PROVIDER.UNAVAILABLE");
    pub const STORAGE_NOT_FOUND: ErrorCode = ErrorCode("STORAGE.NOT_FOUND");
    pub const STORAGE_CONFLICT: ErrorCode = ErrorCode("STORAGE.CONFLICT");
    pub const STORAGE_UNAVAILABLE: ErrorCode = ErrorCode("STORAGE.UNAVAILABLE");
    pub const UNKNOWN_INTERNAL: ErrorCode = ErrorCode("UNKNOWN.INTERNAL");
    pub const SANDBOX_PERMISSION_DENY: ErrorCode = ErrorCode("SANDBOX.PERMISSION_DENY");
}

const fn grpc(code: i32) -> Option<i32> {
    Some(code)
}

pub static REGISTRY: Lazy<HashMap<&'static str, CodeSpec>> = Lazy::new(|| {
    use codes::*;

    let mut map = HashMap::new();
    let mut add = |spec: CodeSpec| {
        let key = spec.code.0;
        if map.insert(key, spec).is_some() {
            panic!("duplicate error code: {}", key);
        }
    };

    add(CodeSpec {
        code: AUTH_UNAUTHENTICATED,
        kind: ErrorKind::Auth,
        http_status: 401,
        grpc_status: grpc(16),
        retryable: RetryClass::Permanent,
        severity: Severity::Warn,
        default_user_msg: "Please sign in.",
    });

    add(CodeSpec {
        code: AUTH_FORBIDDEN,
        kind: ErrorKind::Auth,
        http_status: 403,
        grpc_status: grpc(7),
        retryable: RetryClass::Permanent,
        severity: Severity::Warn,
        default_user_msg: "You don't have permission to perform this action.",
    });

    add(CodeSpec {
        code: SCHEMA_VALIDATION,
        kind: ErrorKind::Schema,
        http_status: 422,
        grpc_status: grpc(3),
        retryable: RetryClass::Permanent,
        severity: Severity::Warn,
        default_user_msg: "Your request is invalid. Please check inputs.",
    });

    add(CodeSpec {
        code: QUOTA_RATELIMIT,
        kind: ErrorKind::RateLimit,
        http_status: 429,
        grpc_status: grpc(8),
        retryable: RetryClass::Transient,
        severity: Severity::Warn,
        default_user_msg: "Too many requests. Please retry later.",
    });

    add(CodeSpec {
        code: QUOTA_BUDGET,
        kind: ErrorKind::QosBudgetExceeded,
        http_status: 429,
        grpc_status: grpc(8),
        retryable: RetryClass::Permanent,
        severity: Severity::Warn,
        default_user_msg: "Budget exceeded.",
    });

    add(CodeSpec {
        code: POLICY_DENY_TOOL,
        kind: ErrorKind::PolicyDeny,
        http_status: 403,
        grpc_status: grpc(7),
        retryable: RetryClass::Permanent,
        severity: Severity::Warn,
        default_user_msg: "Tool usage is not allowed by policy.",
    });

    add(CodeSpec {
        code: LLM_TIMEOUT,
        kind: ErrorKind::LlmError,
        http_status: 503,
        grpc_status: grpc(14),
        retryable: RetryClass::Transient,
        severity: Severity::Error,
        default_user_msg: "The model did not respond in time. Please try again.",
    });

    add(CodeSpec {
        code: LLM_CONTEXT_OVERFLOW,
        kind: ErrorKind::LlmError,
        http_status: 400,
        grpc_status: grpc(11),
        retryable: RetryClass::Permanent,
        severity: Severity::Warn,
        default_user_msg: "Input is too long for the model.",
    });

    add(CodeSpec {
        code: PROVIDER_UNAVAILABLE,
        kind: ErrorKind::Provider,
        http_status: 503,
        grpc_status: grpc(14),
        retryable: RetryClass::Transient,
        severity: Severity::Error,
        default_user_msg: "Upstream provider is unavailable. Please retry later.",
    });

    add(CodeSpec {
        code: STORAGE_NOT_FOUND,
        kind: ErrorKind::NotFound,
        http_status: 404,
        grpc_status: grpc(5),
        retryable: RetryClass::Permanent,
        severity: Severity::Info,
        default_user_msg: "Resource not found.",
    });
    add(CodeSpec {
        code: STORAGE_CONFLICT,
        kind: ErrorKind::Conflict,
        http_status: 409,
        grpc_status: grpc(10),
        retryable: RetryClass::Transient,
        severity: Severity::Warn,
        default_user_msg: "The resource is currently locked. Please retry.",
    });

    add(CodeSpec {
        code: STORAGE_UNAVAILABLE,
        kind: ErrorKind::Storage,
        http_status: 503,
        grpc_status: grpc(14),
        retryable: RetryClass::Transient,
        severity: Severity::Error,
        default_user_msg: "Storage backend is unavailable. Please retry later.",
    });
    add(CodeSpec {
        code: UNKNOWN_INTERNAL,
        kind: ErrorKind::Unknown,
        http_status: 500,
        grpc_status: grpc(2),
        retryable: RetryClass::Transient,
        severity: Severity::Critical,
        default_user_msg: "Internal error. Please retry later.",
    });

    add(CodeSpec {
        code: SANDBOX_PERMISSION_DENY,
        kind: ErrorKind::Sandbox,
        http_status: 403,
        grpc_status: grpc(7),
        retryable: RetryClass::Permanent,
        severity: Severity::Warn,
        default_user_msg: "Operation denied by sandbox policy.",
    });

    map
});

pub fn spec_of(code: ErrorCode) -> &'static CodeSpec {
    REGISTRY.get(code.0).expect("unregistered ErrorCode")
}
