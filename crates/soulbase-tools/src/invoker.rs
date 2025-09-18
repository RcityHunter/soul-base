use crate::errors::ToolError;
use crate::mapping::plan_ops;
use crate::preflight::{
    build_grant, compose_policy, manifest_to_lite, Preflight, PreflightOutput, ToolCall,
};
use crate::registry::{AvailableSpec, ToolRegistry};
use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::json;
use soulbase_auth::{prelude::Obligation, AuthFacade};
use soulbase_sandbox::prelude::{
    ExecOp, MemoryBudget, MemoryEvidence, PolicyConfig, PolicyGuard, PolicyGuardDefault,
    ProfileBuilder, ProfileBuilderDefault, Sandbox,
};
use soulbase_types::prelude::Id;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvokeStatus {
    Ok,
    Denied,
    Error,
}

#[derive(Clone, Debug)]
pub struct InvokeRequest {
    pub spec: AvailableSpec,
    pub call: ToolCall,
    pub profile_hash: String,
    pub obligations: Vec<Obligation>,
    pub planned_ops: Vec<ExecOp>,
}

#[derive(Clone, Debug)]
pub struct InvokeResult {
    pub status: InvokeStatus,
    pub error_code: Option<&'static str>,
    pub output: Option<serde_json::Value>,
    pub evidence_ref: Option<Id>,
}

#[async_trait]
pub trait Invoker: Send + Sync {
    async fn preflight(&self, call: &ToolCall) -> Result<PreflightOutput, ToolError>;
    async fn invoke(&self, request: InvokeRequest) -> Result<InvokeResult, ToolError>;
}

pub struct InvokerImpl {
    registry: Arc<dyn ToolRegistry>,
    auth: Arc<AuthFacade>,
    sandbox: Sandbox,
    policy_base: PolicyConfig,
    guard: PolicyGuardDefault,
    idem: Mutex<HashMap<(String, String, String), serde_json::Value>>,
}

impl InvokerImpl {
    pub fn new(
        registry: Arc<dyn ToolRegistry>,
        auth: Arc<AuthFacade>,
        sandbox: Sandbox,
        policy_base: PolicyConfig,
    ) -> Self {
        Self {
            registry,
            auth,
            sandbox,
            policy_base,
            guard: PolicyGuardDefault,
            idem: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Invoker for InvokerImpl {
    async fn preflight(&self, call: &ToolCall) -> Result<PreflightOutput, ToolError> {
        let pre = Preflight {
            registry: self.registry.as_ref(),
            auth: self.auth.as_ref(),
            policy: &self.policy_base,
            guard: &self.guard,
        };
        pre.run(call).await
    }

    async fn invoke(&self, mut request: InvokeRequest) -> Result<InvokeResult, ToolError> {
        if let Some(key) = &request.call.idempotency_key {
            if let Some(hit) = self
                .idem
                .lock()
                .get(&(
                    request.call.tenant.0.clone(),
                    request.spec.manifest.id.0.clone(),
                    key.clone(),
                ))
                .cloned()
            {
                return Ok(InvokeResult {
                    status: InvokeStatus::Ok,
                    error_code: None,
                    output: Some(hit),
                    evidence_ref: None,
                });
            }
        }

        // Re-plan if caller did not include ops.
        if request.planned_ops.is_empty() {
            request.planned_ops = plan_ops(&request.spec.manifest, &request.call.args)?;
        }

        let policy = compose_policy(self.policy_base.clone(), &request.spec.manifest);
        let grant = build_grant(&request.spec.manifest, &request.call);
        let manifest_lite = manifest_to_lite(&request.spec.manifest);
        let profile = ProfileBuilderDefault
            .build(&grant, &manifest_lite, &policy)
            .await?;

        for op in &request.planned_ops {
            self.guard.validate(&profile, op).await?;
        }

        let evidence = MemoryEvidence::new();
        let budget = MemoryBudget::new(grant.budget.clone());
        let env_id = Id(format!("env_{}", request.call.call_id.0));
        let mut aggregated = Vec::new();
        for op in &request.planned_ops {
            let result = self
                .sandbox
                .run(&profile, &env_id, &evidence, &budget, op.clone())
                .await?;
            aggregated.push(adapt_output(op, result.out.clone(), &request.call.args));
        }

        let output = if aggregated.len() == 1 {
            aggregated.pop().unwrap()
        } else {
            json!({ "results": aggregated })
        };

        request.spec.manifest.validate_output(&output)?;

        if let Some(key) = &request.call.idempotency_key {
            self.idem.lock().insert(
                (
                    request.call.tenant.0.clone(),
                    request.spec.manifest.id.0.clone(),
                    key.clone(),
                ),
                output.clone(),
            );
        }

        Ok(InvokeResult {
            status: InvokeStatus::Ok,
            error_code: None,
            output: Some(output),
            evidence_ref: Some(env_id),
        })
    }
}

fn adapt_output(
    op: &ExecOp,
    mut value: serde_json::Value,
    _args: &serde_json::Value,
) -> serde_json::Value {
    match op {
        ExecOp::NetHttp { url, .. } => {
            if let Some(obj) = value.as_object_mut() {
                obj.entry("url")
                    .or_insert_with(|| serde_json::Value::String(url.clone()));
                obj.entry("simulated")
                    .or_insert(serde_json::Value::Bool(true));
            }
            value
        }
        ExecOp::FsRead { path, .. } => {
            if let Some(obj) = value.as_object_mut() {
                obj.entry("request_path")
                    .or_insert_with(|| serde_json::Value::String(path.clone()));
            }
            value
        }
        _ => value,
    }
}
