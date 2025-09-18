use crate::errors::SandboxError;
use crate::model::{Budget, ExecOp, Profile};
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
            ExecOp::NetHttp { body_b64, .. } => {
                body_b64.as_ref().map(|b| b.len() as i64).unwrap_or(0)
            }
            _ => 0,
        };
        state.bytes_in_used += bytes_in;
        if self.limits.bytes_in != i64::MAX && state.bytes_in_used > self.limits.bytes_in {
            return Err(SandboxError::quota_budget("bytes_in limit exceeded"));
        }

        Ok(())
    }
}
