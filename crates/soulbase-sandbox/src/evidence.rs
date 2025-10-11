use crate::model::{ExecOp, ExecResult};
use chrono::Utc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use soulbase_types::prelude::Id;

pub struct MemoryEvidence {
    pub begins: Mutex<Vec<EvidenceRecord>>,
    pub ends: Mutex<Vec<EvidenceRecord>>,
}

impl MemoryEvidence {
    pub fn new() -> Self {
        Self {
            begins: Mutex::new(Vec::new()),
            ends: Mutex::new(Vec::new()),
        }
    }

    pub fn record_begin(&self, env_id: &Id, op: &ExecOp) {
        let mut guard = self.begins.lock();
        guard.push(EvidenceRecord::begin(env_id.clone(), op));
    }

    pub fn record_end(&self, env_id: &Id, op: &ExecOp, success: bool, details: Value) {
        let mut guard = self.ends.lock();
        guard.push(EvidenceRecord::end(env_id.clone(), op, success, details));
    }
}

impl Default for MemoryEvidence {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceRecord {
    pub env_id: Id,
    pub op_kind: String,
    pub timestamp_ms: i64,
    pub success: Option<bool>,
    #[serde(default)]
    pub details: Value,
}

impl EvidenceRecord {
    fn begin(env_id: Id, op: &ExecOp) -> Self {
        Self {
            env_id,
            op_kind: op.kind_name().into(),
            timestamp_ms: Utc::now().timestamp_millis(),
            success: None,
            details: summarize_op(op),
        }
    }

    fn end(env_id: Id, op: &ExecOp, success: bool, details: Value) -> Self {
        Self {
            env_id,
            op_kind: op.kind_name().into(),
            timestamp_ms: Utc::now().timestamp_millis(),
            success: Some(success),
            details,
        }
    }
}

pub trait EvidenceSink: Send + Sync {
    fn record_begin(&self, env_id: &Id, op: &ExecOp);
    fn record_end(&self, env_id: &Id, op: &ExecOp, success: bool, details: Value);
}

impl EvidenceSink for MemoryEvidence {
    fn record_begin(&self, env_id: &Id, op: &ExecOp) {
        MemoryEvidence::record_begin(self, env_id, op);
    }

    fn record_end(&self, env_id: &Id, op: &ExecOp, success: bool, details: Value) {
        MemoryEvidence::record_end(self, env_id, op, success, details);
    }
}

fn summarize_op(op: &ExecOp) -> Value {
    match op {
        ExecOp::FsRead { path, offset, len } => json!({
            "path": path,
            "offset": offset,
            "len": len,
        }),
        ExecOp::FsWrite { path, .. } => json!({
            "path": path,
        }),
        ExecOp::FsList { path } => json!({ "path": path }),
        ExecOp::NetHttp { method, url, .. } => json!({
            "method": method,
            "url": url,
        }),
        ExecOp::TmpAlloc { size_bytes } => json!({
            "size_bytes": size_bytes,
        }),
        ExecOp::ProcSpawn { program, args, .. } => json!({
            "program": program,
            "args": args,
        }),
    }
}

fn sanitize_value(value: &Value, depth: usize) -> Value {
    const MAX_DEPTH: usize = 3;
    const MAX_ENTRIES: usize = 16;
    const MAX_STRING: usize = 256;

    if depth >= MAX_DEPTH {
        return Value::String("<truncated>".into());
    }

    match value {
        Value::String(s) => {
            if s.len() > MAX_STRING {
                Value::String(format!("{}…", &s[..MAX_STRING]))
            } else {
                Value::String(s.clone())
            }
        }
        Value::Array(items) => {
            let truncated = items
                .iter()
                .take(MAX_ENTRIES)
                .map(|v| sanitize_value(v, depth + 1))
                .collect();
            Value::Array(truncated)
        }
        Value::Object(map) => {
            let mut sanitized = serde_json::Map::new();
            for (idx, (k, v)) in map.iter().enumerate() {
                if idx >= MAX_ENTRIES {
                    break;
                }
                sanitized.insert(k.clone(), sanitize_value(v, depth + 1));
            }
            Value::Object(sanitized)
        }
        _ => value.clone(),
    }
}

pub fn summarize_exec_output(op: &ExecOp, result: &ExecResult) -> Value {
    json!({
        "op": op.kind_name(),
        "ok": result.ok,
        "out": sanitize_value(&result.out, 0),
    })
}
