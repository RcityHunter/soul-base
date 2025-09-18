use crate::manifest::ToolId;
use serde::{Deserialize, Serialize};
use soulbase_types::prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolRegistered {
    pub tool_id: ToolId,
    pub tenant: TenantId,
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolInvokeBegin {
    pub envelope_id: Id,
    pub tenant: TenantId,
    pub subject_id: Id,
    pub tool_id: ToolId,
    pub call_id: Id,
    pub profile_hash: String,
    pub args_digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolInvokeEnd {
    pub envelope_id: Id,
    pub status: String,
    pub error_code: Option<&'static str>,
    pub budget_used_bytes_in: u64,
    pub budget_used_bytes_out: u64,
    pub output_digest: String,
}
