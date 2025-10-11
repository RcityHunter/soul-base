#[cfg(feature = "observe")]
use crate::evidence::summarize_exec_output;
#[cfg(feature = "observe")]
use crate::model::{ExecOp, ExecResult, Profile};
#[cfg(feature = "observe")]
use serde_json::json;
#[cfg(feature = "observe")]
use soulbase_observe::prelude::{
    LogBuilder, LogLevel, Logger, Meter, MeterRegistry, MetricKind, MetricSpec, NoopRedactor,
    ObserveCtx,
};
#[cfg(feature = "observe")]
use std::sync::Arc;

#[cfg(feature = "observe")]
const SANDBOX_EXEC_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_sandbox_exec_total",
    kind: MetricKind::Counter,
    help: "Total sandbox executions",
    buckets_ms: None,
    stable_labels: &[],
};

#[cfg(feature = "observe")]
const SANDBOX_EXEC_FAIL_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_sandbox_exec_fail_total",
    kind: MetricKind::Counter,
    help: "Sandbox execution failures",
    buckets_ms: None,
    stable_labels: &[],
};

#[cfg(feature = "observe")]
const SANDBOX_EXEC_DEGRADED_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_sandbox_exec_degraded_total",
    kind: MetricKind::Counter,
    help: "Sandbox executions that ran in degraded or simulated mode",
    buckets_ms: None,
    stable_labels: &[],
};

#[cfg(feature = "observe")]
const SANDBOX_EXEC_LATENCY_MS: MetricSpec = MetricSpec {
    name: "soulbase_sandbox_exec_latency_ms",
    kind: MetricKind::Gauge,
    help: "Last sandbox execution latency (ms)",
    buckets_ms: None,
    stable_labels: &[],
};

#[cfg(feature = "observe")]
#[derive(Clone)]
pub(crate) struct SandboxTelemetry {
    meter: MeterRegistry,
    logger: Arc<dyn Logger>,
}

#[cfg(feature = "observe")]
impl SandboxTelemetry {
    pub fn new(meter: MeterRegistry, logger: Arc<dyn Logger>) -> Self {
        Self { meter, logger }
    }

    fn ctx(&self, profile: &Profile, op: &ExecOp) -> ObserveCtx {
        let mut ctx = ObserveCtx::for_tenant(profile.tenant.0.clone());
        ctx.resource = Some(profile.tool_name.clone());
        ctx.action = Some(op.kind_name().to_string());
        ctx.subject_kind = Some("tool_invocation".into());
        ctx
    }

    pub async fn record_success(
        &self,
        profile: &Profile,
        op: &ExecOp,
        result: &ExecResult,
        latency_ms: u64,
        degraded: bool,
    ) {
        self.meter.counter(&SANDBOX_EXEC_TOTAL).inc(1);
        self.meter.gauge(&SANDBOX_EXEC_LATENCY_MS).set(latency_ms);
        if degraded {
            self.meter.counter(&SANDBOX_EXEC_DEGRADED_TOTAL).inc(1);
        }

        let ctx = self.ctx(profile, op);
        let redactor = NoopRedactor::default();
        let summary = summarize_exec_output(op, result);
        let degradation_reason = result
            .out
            .get("degradation")
            .and_then(|value| value.get("reason"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut builder = LogBuilder::new(
            if degraded {
                LogLevel::Warn
            } else {
                LogLevel::Info
            },
            "sandbox exec succeeded",
        )
        .label("tool", profile.tool_name.clone())
        .label("op", op.kind_name())
        .field("latency_ms", json!(latency_ms))
        .field("result", summary);

        if degraded {
            builder = builder.field("degraded", json!(true));
        }
        if let Some(reason) = degradation_reason.as_ref() {
            builder = builder.field("degradation_reason", json!(reason));
        }

        let event = builder.finish(&ctx, &redactor);
        self.logger.log(&ctx, event).await;
    }

    pub async fn record_failure(
        &self,
        profile: &Profile,
        op: &ExecOp,
        code: &str,
        message: Option<String>,
        latency_ms: u64,
    ) {
        self.meter.counter(&SANDBOX_EXEC_TOTAL).inc(1);
        self.meter.counter(&SANDBOX_EXEC_FAIL_TOTAL).inc(1);
        self.meter.gauge(&SANDBOX_EXEC_LATENCY_MS).set(latency_ms);

        let ctx = self.ctx(profile, op);
        let redactor = NoopRedactor::default();
        let mut builder = LogBuilder::new(LogLevel::Error, "sandbox exec failed")
            .label("tool", profile.tool_name.clone())
            .label("op", op.kind_name())
            .field("latency_ms", json!(latency_ms))
            .field("code", json!(code));

        if let Some(msg) = message {
            builder = builder.field("message", json!(msg));
        }

        let event = builder.finish(&ctx, &redactor);
        self.logger.log(&ctx, event).await;
    }
}
