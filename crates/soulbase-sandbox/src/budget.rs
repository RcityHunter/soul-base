use crate::errors::SandboxError;
use crate::model::{Budget, ExecOp, Profile};
use base64::Engine as _;
use parking_lot::Mutex;

pub struct MemoryBudget {
    limits: Budget,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    calls_used: i64,
    bytes_in_used: i64,
}

impl MemoryBudget {
    pub fn new(limits: Budget) -> Self {
        Self {
            limits,
            state: Mutex::new(State::default()),
        }
    }

    pub fn check_and_consume(&self, profile: &Profile, op: &ExecOp) -> Result<(), SandboxError> {
        if profile.expires_at < chrono::Utc::now().timestamp_millis() {
            return Err(SandboxError::expired());
        }

        let mut state = self.state.lock();
        state.calls_used += 1;
        if self.limits.calls != i64::MAX && state.calls_used > self.limits.calls {
            return Err(SandboxError::quota_budget("call limit exceeded"));
        }

        let bytes_in = match op {
            ExecOp::FsRead { len, .. } => len.map(|l| l as i64).unwrap_or(0),
            ExecOp::FsWrite { contents_b64, .. } => decode_length(contents_b64) as i64,
            ExecOp::NetHttp { body_b64, .. } => body_b64
                .as_ref()
                .map(|b| decode_length(b) as i64)
                .unwrap_or(0),
            ExecOp::ProcSpawn { stdin_b64, .. } => stdin_b64
                .as_ref()
                .map(|s| decode_length(s) as i64)
                .unwrap_or(0),
            _ => 0,
        };
        state.bytes_in_used += bytes_in;
        if self.limits.bytes_in != i64::MAX && state.bytes_in_used > self.limits.bytes_in {
            return Err(SandboxError::quota_budget("bytes_in limit exceeded"));
        }

        Ok(())
    }
}

fn decode_length(encoded: &str) -> usize {
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map(|bytes| bytes.len())
        .unwrap_or(0)
}
