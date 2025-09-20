use crate::model::ExecOp;
use chrono::Utc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
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

    pub fn record_end(&self, env_id: &Id, op: &ExecOp, result: bool) {
        let mut guard = self.ends.lock();
        guard.push(EvidenceRecord::end(env_id.clone(), op, result));
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
}

impl EvidenceRecord {
    fn begin(env_id: Id, op: &ExecOp) -> Self {
        Self {
            env_id,
            op_kind: op.kind_name().into(),
            timestamp_ms: Utc::now().timestamp_millis(),
            success: None,
        }
    }

    fn end(env_id: Id, op: &ExecOp, success: bool) -> Self {
        Self {
            env_id,
            op_kind: op.kind_name().into(),
            timestamp_ms: Utc::now().timestamp_millis(),
            success: Some(success),
        }
    }
}

pub trait EvidenceSink: Send + Sync {
    fn record_begin(&self, env_id: &Id, op: &ExecOp);
    fn record_end(&self, env_id: &Id, op: &ExecOp, result: bool);
}

impl EvidenceSink for MemoryEvidence {
    fn record_begin(&self, env_id: &Id, op: &ExecOp) {
        MemoryEvidence::record_begin(self, env_id, op);
    }

    fn record_end(&self, env_id: &Id, op: &ExecOp, result: bool) {
        MemoryEvidence::record_end(self, env_id, op, result);
    }
}
