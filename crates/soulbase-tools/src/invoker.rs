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
use soulbase_sandbox::evidence::EvidenceRecord;
use soulbase_sandbox::prelude::{
    ExecOp, MemoryBudget, MemoryEvidence, PolicyConfig, PolicyGuard, PolicyGuardDefault,
    ProfileBuilder, ProfileBuilderDefault, Sandbox,
};
use soulbase_types::prelude::Id;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(feature = "outbox")]
use chrono::Utc;
#[cfg(feature = "observe")]
use std::time::Instant;

#[cfg(feature = "observe")]
use crate::observe::{ctx_for_tool, log_invocation, LogInvocationParams};
#[cfg(feature = "observe")]
use soulbase_observe::prelude::{Logger, Meter, MeterRegistry, MetricKind, MetricSpec};
#[cfg(feature = "outbox")]
use soulbase_tx::{
    model::{MsgId, OutboxMessage},
    outbox::OutboxStore,
};

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
    pub error_code: Option<String>,
    pub output: Option<serde_json::Value>,
    pub evidence_ref: Option<Id>,
    pub degradation: Option<Vec<String>>,
    pub evidence: InvokeEvidence,
}

#[async_trait]
pub trait Invoker: Send + Sync {
    async fn preflight(&self, call: &ToolCall) -> Result<PreflightOutput, ToolError>;
    async fn invoke(&self, request: InvokeRequest) -> Result<InvokeResult, ToolError>;
}

#[derive(Clone, Debug, Default)]
pub struct InvokeEvidence {
    pub begins: Vec<EvidenceRecord>,
    pub ends: Vec<EvidenceRecord>,
}

impl InvokeEvidence {
    pub fn empty() -> Self {
        Self::default()
    }
}

pub struct InvokerImpl {
    registry: Arc<dyn ToolRegistry>,
    auth: Arc<AuthFacade>,
    sandbox: Sandbox,
    policy_base: PolicyConfig,
    guard: PolicyGuardDefault,
    idem: Mutex<HashMap<(String, String, String), serde_json::Value>>,
    #[cfg(feature = "outbox")]
    outbox: Option<Arc<dyn OutboxStore>>,
    #[cfg(feature = "observe")]
    meter: Arc<dyn Meter>,
    #[cfg(feature = "observe")]
    logger: Arc<dyn Logger>,
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
            #[cfg(feature = "outbox")]
            outbox: None,
            #[cfg(feature = "observe")]
            meter: Arc::new(MeterRegistry::default()),
            #[cfg(feature = "observe")]
            logger: Arc::new(soulbase_observe::prelude::NoopLogger),
        }
    }

    #[cfg(feature = "observe")]
    pub fn with_observe(mut self, meter: Arc<dyn Meter>, logger: Arc<dyn Logger>) -> Self {
        self.meter = meter;
        self.logger = logger;
        self
    }

    #[cfg(feature = "outbox")]
    pub fn with_outbox(mut self, outbox: Arc<dyn OutboxStore>) -> Self {
        self.outbox = Some(outbox);
        self
    }
}

#[cfg(feature = "observe")]
const TOOL_INVOCATIONS_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_tools_invocations_total",
    kind: MetricKind::Counter,
    help: "Total tool invocations",
    buckets_ms: None,
    stable_labels: &[],
};

#[cfg(feature = "observe")]
const TOOL_INVOCATIONS_FAIL_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_tools_invocations_fail_total",
    kind: MetricKind::Counter,
    help: "Failed tool invocations",
    buckets_ms: None,
    stable_labels: &[],
};

#[cfg(feature = "observe")]
const TOOL_INVOCATION_LATENCY_MS: MetricSpec = MetricSpec {
    name: "soulbase_tools_invocation_latency_ms",
    kind: MetricKind::Gauge,
    help: "Last tool invocation latency (ms)",
    buckets_ms: None,
    stable_labels: &[],
};

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
                    degradation: None,
                    evidence: InvokeEvidence::empty(),
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
        #[cfg(feature = "observe")]
        let ctx = ctx_for_tool(&request.call.tenant.0, &request.spec.manifest.id, "invoke");
        #[cfg(feature = "observe")]
        let started = Instant::now();
        let budget = MemoryBudget::new(grant.budget.clone());
        let env_id = Id(format!("env_{}", request.call.call_id.0));
        let mut aggregated = Vec::new();
        let mut degrade_reasons: Vec<String> = Vec::new();
        for op in &request.planned_ops {
            match self
                .sandbox
                .run(&profile, &env_id, &evidence, &budget, op.clone())
                .await
            {
                Ok(result) => {
                    if let Some(reason) = extract_degradation(&result.out) {
                        if !degrade_reasons.contains(&reason) {
                            degrade_reasons.push(reason);
                        }
                    }
                    aggregated.push(adapt_output(op, result.out.clone(), &request.call.args));
                }
                Err(err) => {
                    #[cfg(feature = "observe")]
                    {
                        let latency_ms =
                            started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                        self.meter.counter(&TOOL_INVOCATIONS_TOTAL).inc(1);
                        self.meter.counter(&TOOL_INVOCATIONS_FAIL_TOTAL).inc(1);
                        self.meter
                            .gauge(&TOOL_INVOCATION_LATENCY_MS)
                            .set(latency_ms);
                        log_invocation(
                            self.logger.as_ref(),
                            &ctx,
                            LogInvocationParams {
                                tenant: &request.call.tenant.0,
                                tool: &request.spec.manifest.id,
                                status: "error",
                                code: Some(err.0.code.0),
                                latency_ms,
                                degradation: None,
                            },
                        )
                        .await;
                    }
                    return Err(err.into());
                }
            }
        }

        let output = if aggregated.len() == 1 {
            let mut value = aggregated.pop().unwrap();
            if !degrade_reasons.is_empty() {
                if let Some(obj) = value.as_object_mut() {
                    obj.insert(
                        "degradation".into(),
                        json!({ "reasons": degrade_reasons.clone() }),
                    );
                }
            }
            value
        } else {
            let mut combined = json!({ "results": aggregated });
            if !degrade_reasons.is_empty() {
                if let Some(obj) = combined.as_object_mut() {
                    obj.insert(
                        "degradation".into(),
                        json!({ "reasons": degrade_reasons.clone() }),
                    );
                }
            }
            combined
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

        #[cfg(feature = "observe")]
        {
            let latency_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            self.meter.counter(&TOOL_INVOCATIONS_TOTAL).inc(1);
            self.meter
                .gauge(&TOOL_INVOCATION_LATENCY_MS)
                .set(latency_ms);
            let degradation = if degrade_reasons.is_empty() {
                None
            } else {
                Some(degrade_reasons.join(","))
            };
            let status = if degradation.is_some() {
                "degraded"
            } else {
                "ok"
            };
            log_invocation(
                self.logger.as_ref(),
                &ctx,
                LogInvocationParams {
                    tenant: &request.call.tenant.0,
                    tool: &request.spec.manifest.id,
                    status,
                    code: None,
                    latency_ms,
                    degradation: degradation.as_deref(),
                },
            )
            .await;
        }

        #[cfg(feature = "outbox")]
        if let Some(outbox) = &self.outbox {
            let now_ms = Utc::now().timestamp_millis();
            let mut payload = json!({
                "tenant": request.call.tenant.0.clone(),
                "tool_id": request.spec.manifest.id.0.clone(),
                "call_id": request.call.call_id.0.clone(),
                "status": "ok",
                "profile_hash": request.profile_hash.clone(),
                "output": output.clone(),
            });
            if !degrade_reasons.is_empty() {
                if let Some(obj) = payload.as_object_mut() {
                    obj.insert(
                        "degradation".into(),
                        json!({ "reasons": degrade_reasons.clone() }),
                    );
                }
            }
            let msg = OutboxMessage::new(
                request.call.tenant.clone(),
                MsgId(format!(
                    "tool:{}:{}:{}",
                    request.call.tenant.0, request.spec.manifest.id.0, request.call.call_id.0
                )),
                "tool.invoke".into(),
                payload,
                now_ms,
            );
            outbox.enqueue(msg).await?;
        }

        let evidence_snapshot = InvokeEvidence {
            begins: evidence.begins.lock().clone(),
            ends: evidence.ends.lock().clone(),
        };

        Ok(InvokeResult {
            status: InvokeStatus::Ok,
            error_code: None,
            output: Some(output),
            evidence_ref: Some(env_id),
            degradation: if degrade_reasons.is_empty() {
                None
            } else {
                Some(degrade_reasons)
            },
            evidence: evidence_snapshot,
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

fn extract_degradation(value: &serde_json::Value) -> Option<String> {
    value.get("degradation").and_then(|deg| {
        if let Some(obj) = deg.as_object() {
            if let Some(reason) = obj.get("reason").and_then(|r| r.as_str()) {
                Some(reason.to_string())
            } else if let Some(reasons) = obj.get("reasons").and_then(|r| r.as_array()) {
                Some(
                    reasons
                        .iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(","),
                )
            } else {
                None
            }
        } else {
            deg.as_str().map(|s| s.to_string())
        }
    })
}
