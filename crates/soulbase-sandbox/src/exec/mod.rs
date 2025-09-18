mod fs;
mod net;

use crate::errors::SandboxError;
use crate::guard::{PolicyGuard, PolicyGuardDefault};
use crate::model::{ExecOp, ExecResult, Profile};
use crate::{budget::MemoryBudget, evidence::EvidenceSink};
use serde_json::json;
use soulbase_types::prelude::Id;

pub use fs::FsExecutor;
pub use net::NetExecutor;

pub struct Sandbox {
    fs: FsExecutor,
    net: NetExecutor,
    guard: PolicyGuardDefault,
}

impl Sandbox {
    pub fn minimal() -> Self {
        Self {
            fs: FsExecutor::new(),
            net: NetExecutor::new(),
            guard: PolicyGuardDefault,
        }
    }

    pub async fn run<Evid, Budg>(
        &self,
        profile: &Profile,
        env_id: &Id,
        evidence: &Evid,
        budget: &Budg,
        op: ExecOp,
    ) -> Result<ExecResult, SandboxError>
    where
        Evid: EvidenceSink,
        Budg: BudgetTracker,
    {
        self.guard.validate(profile, &op).await?;
        budget.check_and_consume(profile, &op)?;
        evidence.record_begin(env_id, &op);
        let result = match &op {
            ExecOp::FsRead { .. } | ExecOp::FsWrite { .. } | ExecOp::FsList { .. } => {
                self.fs.execute(profile, &op).await
            }
            ExecOp::NetHttp { .. } => self.net.execute(profile, &op).await,
            ExecOp::TmpAlloc { size_bytes } => Ok(ExecResult::success(json!({
                "simulated": true,
                "size_bytes": size_bytes,
                "tool": profile.tool_name,
            }))),
        };
        match result {
            Ok(res) => {
                evidence.record_end(env_id, &op, res.ok);
                Ok(res)
            }
            Err(err) => {
                evidence.record_end(env_id, &op, false);
                Err(err)
            }
        }
    }
}

pub trait BudgetTracker {
    fn check_and_consume(&self, profile: &Profile, op: &ExecOp) -> Result<(), SandboxError>;
}

impl BudgetTracker for MemoryBudget {
    fn check_and_consume(&self, profile: &Profile, op: &ExecOp) -> Result<(), SandboxError> {
        MemoryBudget::check_and_consume(self, profile, op)
    }
}
