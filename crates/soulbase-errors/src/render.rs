use crate::{
    kind::ErrorKind,
    model::{CauseEntry, ErrorObj},
    retry::RetryClass,
    severity::Severity,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Serialize, Deserialize)]
pub struct PublicErrorView {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuditErrorView {
    pub code: &'static str,
    pub kind: &'static str,
    pub http_status: u16,
    pub retryable: &'static str,
    pub severity: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_dev: Option<String>,
    pub meta: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause_chain: Option<Vec<CauseEntry>>,
}

impl ErrorObj {
    pub fn to_public(&self) -> PublicErrorView {
        PublicErrorView {
            code: self.code.0,
            message: self.message_user.clone(),
            correlation_id: self.correlation_id.clone(),
        }
    }

    pub fn to_audit(&self) -> AuditErrorView {
        AuditErrorView {
            code: self.code.0,
            kind: ErrorKind::as_str(self.kind),
            http_status: self.http_status,
            retryable: RetryClass::as_str(self.retryable),
            severity: Severity::as_str(self.severity),
            message_dev: self.message_dev.clone(),
            meta: self.meta.clone(),
            cause_chain: self.cause_chain.clone(),
        }
    }
}
