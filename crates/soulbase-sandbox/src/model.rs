use ahash::AHasher;
use serde::{Deserialize, Serialize};
use soulbase_types::prelude::*;
use std::collections::HashMap;
use std::hash::Hasher;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Capability {
    FsRead {
        path: String,
    },
    FsWrite {
        path: String,
    },
    FsList {
        path: String,
    },
    NetHttp {
        host: String,
        port: Option<u16>,
        scheme: Option<String>,
        methods: Vec<String>,
    },
    TmpUse,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SafetyClass {
    Low,
    Medium,
    High,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SideEffect {
    Read,
    Write,
    Network,
    Execute,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolManifestLite {
    pub name: String,
    pub permissions: Vec<Capability>,
    pub safety_class: SafetyClass,
    pub side_effect: SideEffect,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Budget {
    pub calls: i64,
    pub bytes_in: i64,
    pub bytes_out: i64,
    pub duration_ms: i64,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            calls: i64::MAX,
            bytes_in: i64::MAX,
            bytes_out: i64::MAX,
            duration_ms: i64::MAX,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Grant {
    pub tenant: TenantId,
    pub subject_id: Id,
    pub tool_name: String,
    pub call_id: Id,
    pub capabilities: Vec<Capability>,
    pub expires_at: i64,
    pub budget: Budget,
    pub decision_key_fingerprint: String,
}

#[derive(Clone, Debug)]
pub struct Profile {
    pub tenant: TenantId,
    pub subject_id: Id,
    pub tool_name: String,
    pub call_id: Id,
    pub capabilities: Vec<Capability>,
    pub policy: crate::config::PolicyConfig,
    pub expires_at: i64,
    pub budget: Budget,
    pub safety_class: SafetyClass,
    pub side_effect: SideEffect,
    pub manifest_name: String,
    pub decision_key_fingerprint: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExecOp {
    FsRead {
        path: String,
        offset: Option<u64>,
        len: Option<u64>,
    },
    FsWrite {
        path: String,
        contents_b64: String,
    },
    FsList {
        path: String,
    },
    NetHttp {
        method: String,
        url: String,
        headers: HashMap<String, String>,
        body_b64: Option<String>,
    },
    TmpAlloc {
        size_bytes: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecResult {
    pub ok: bool,
    pub out: serde_json::Value,
}

impl ExecResult {
    pub fn success(out: serde_json::Value) -> Self {
        Self { ok: true, out }
    }

    pub fn failure(out: serde_json::Value) -> Self {
        Self { ok: false, out }
    }
}

impl ExecOp {
    pub fn kind_name(&self) -> &'static str {
        match self {
            ExecOp::FsRead { .. } => "fs_read",
            ExecOp::FsWrite { .. } => "fs_write",
            ExecOp::FsList { .. } => "fs_list",
            ExecOp::NetHttp { .. } => "net_http",
            ExecOp::TmpAlloc { .. } => "tmp_alloc",
        }
    }
}

pub fn capability_fingerprint(caps: &[Capability]) -> u64 {
    let mut hasher = AHasher::default();
    let serialized = serde_json::to_vec(caps).unwrap_or_default();
    hasher.write(&serialized);
    hasher.finish()
}
