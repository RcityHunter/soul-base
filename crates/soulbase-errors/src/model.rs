use crate::{
    code::{spec_of, ErrorCode},
    kind::ErrorKind,
    retry::RetryClass,
    severity::Severity,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use soulbase_types::trace::TraceContext;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CauseEntry {
    pub code: String,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Map<String, Value>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorObj {
    pub code: ErrorCode,
    pub kind: ErrorKind,
    pub message_user: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_dev: Option<String>,
    pub http_status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc_status: Option<i32>,
    pub retryable: RetryClass,
    pub severity: Severity,
    #[serde(default)]
    pub meta: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause_chain: Option<Vec<CauseEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace: Option<TraceContext>,
}

pub struct ErrorBuilder {
    code: ErrorCode,
    message_user: Option<String>,
    message_dev: Option<String>,
    meta: Map<String, Value>,
    cause_chain: Vec<CauseEntry>,
    correlation_id: Option<String>,
    trace: Option<TraceContext>,
}

impl ErrorBuilder {
    pub fn new(code: ErrorCode) -> Self {
        Self {
            code,
            message_user: None,
            message_dev: None,
            meta: Map::new(),
            cause_chain: Vec::new(),
            correlation_id: None,
            trace: None,
        }
    }

    pub fn user_msg(mut self, message: impl Into<String>) -> Self {
        self.message_user = Some(message.into());
        self
    }

    pub fn dev_msg(mut self, message: impl Into<String>) -> Self {
        self.message_dev = Some(message.into());
        self
    }

    pub fn meta_kv(mut self, key: impl Into<String>, value: Value) -> Self {
        self.meta.insert(key.into(), value);
        self
    }

    pub fn cause(mut self, cause: CauseEntry) -> Self {
        self.cause_chain.push(cause);
        self
    }

    pub fn correlation(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    pub fn trace(mut self, trace: TraceContext) -> Self {
        self.trace = Some(trace);
        self
    }

    pub fn build(self) -> ErrorObj {
        let spec = spec_of(self.code);
        ErrorObj {
            code: self.code,
            kind: spec.kind,
            message_user: self
                .message_user
                .unwrap_or_else(|| spec.default_user_msg.to_string()),
            message_dev: self.message_dev,
            http_status: spec.http_status,
            grpc_status: spec.grpc_status,
            retryable: spec.retryable,
            severity: spec.severity,
            meta: self.meta,
            cause_chain: if self.cause_chain.is_empty() {
                None
            } else {
                Some(self.cause_chain)
            },
            correlation_id: self.correlation_id,
            trace: self.trace,
        }
    }
}
