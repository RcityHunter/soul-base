mod fs;
mod net;
#[cfg(feature = "exec-proc")]
mod proc;
#[cfg(feature = "observe")]
mod telemetry;

use crate::errors::SandboxError;
use crate::guard::{PolicyGuard, PolicyGuardDefault};
use crate::model::{ExecOp, ExecResult, Profile};
use crate::{budget::MemoryBudget, evidence::EvidenceSink};
use serde_json::json;
use soulbase_types::prelude::Id;

#[cfg(feature = "observe")]
use self::telemetry::SandboxTelemetry;
#[cfg(feature = "observe")]
use soulbase_observe::prelude::{Logger, MeterRegistry};
#[cfg(feature = "observe")]
use std::sync::Arc;
#[cfg(feature = "observe")]
use std::time::Instant;

pub use fs::FsExecutor;
pub use net::NetExecutor;
#[cfg(feature = "exec-proc")]
pub use proc::ProcExecutor;

pub struct Sandbox {
    fs: FsExecutor,
    net: NetExecutor,
    #[cfg(feature = "exec-proc")]
    proc_exec: ProcExecutor,
    guard: PolicyGuardDefault,
    #[cfg(feature = "observe")]
    telemetry: Option<SandboxTelemetry>,
}

impl Sandbox {
    pub fn minimal() -> Self {
        Self {
            fs: FsExecutor::new(),
            net: NetExecutor::new(),
            #[cfg(feature = "exec-proc")]
            proc_exec: ProcExecutor::new(),
            guard: PolicyGuardDefault,
            #[cfg(feature = "observe")]
            telemetry: None,
        }
    }

    #[cfg(feature = "observe")]
    pub fn with_telemetry(mut self, meter: MeterRegistry, logger: Arc<dyn Logger>) -> Self {
        self.telemetry = Some(SandboxTelemetry::new(meter, logger));
        self
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
        #[cfg(feature = "observe")]
        let started = Instant::now();

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
                "degradation": {
                    "reason": "tmp.alloc.simulated",
                },
            }))),
            ExecOp::ProcSpawn { .. } => {
                #[cfg(feature = "exec-proc")]
                {
                    self.proc_exec.execute(profile, &op).await
                }

                #[cfg(not(feature = "exec-proc"))]
                {
                    Err(SandboxError::permission("process execution disabled"))
                }
            }
        };
        match result {
            Ok(res) => {
                #[cfg(feature = "observe")]
                if let Some(tel) = &self.telemetry {
                    let degraded = is_degraded(&res);
                    let latency_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                    tel.record_success(profile, &op, &res, latency_ms, degraded)
                        .await;
                }

                let summary = crate::evidence::summarize_exec_output(&op, &res);
                evidence.record_end(env_id, &op, res.ok, summary);
                Ok(res)
            }
            Err(err) => {
                #[cfg(feature = "observe")]
                let err_code = err.0.code.0;
                #[cfg(feature = "observe")]
                let err_dev = err.0.message_dev.clone();
                #[cfg(feature = "observe")]
                let latency_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                evidence.record_end(
                    env_id,
                    &op,
                    false,
                    json!({
                        "op": op.kind_name(),
                        "ok": false,
                    }),
                );
                #[cfg(feature = "observe")]
                if let Some(tel) = &self.telemetry {
                    tel.record_failure(profile, &op, err_code, err_dev, latency_ms)
                        .await;
                }
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

#[cfg(feature = "observe")]
fn is_degraded(result: &ExecResult) -> bool {
    result
        .out
        .get("simulated")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        || result.out.get("degradation").is_some()
}
