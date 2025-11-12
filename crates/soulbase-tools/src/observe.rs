use crate::manifest::ToolId;
use std::collections::BTreeMap;

#[cfg(feature = "observe")]
use soulbase_observe::prelude::{LogBuilder, LogLevel, Logger, NoopRedactor, ObserveCtx};

pub fn labels(tenant: &str, tool: &ToolId, code: Option<&str>) -> BTreeMap<&'static str, String> {
    let mut map = BTreeMap::new();
    map.insert("tenant", tenant.to_string());
    map.insert("tool_id", tool.0.clone());
    if let Some(c) = code {
        map.insert("code", c.to_string());
    }
    map
}

#[cfg(feature = "observe")]
pub fn ctx_for_tool(tenant: &str, tool: &ToolId, action: &str) -> ObserveCtx {
    let mut ctx = ObserveCtx::for_tenant(tenant.to_string());
    ctx.resource = Some(tool.0.clone());
    ctx.action = Some(action.to_string());
    ctx.subject_kind = Some("tool_invocation".into());
    ctx
}

#[cfg(feature = "observe")]
pub struct LogInvocationParams<'a> {
    pub tenant: &'a str,
    pub tool: &'a ToolId,
    pub status: &'a str,
    pub code: Option<&'a str>,
    pub latency_ms: u64,
    pub degradation: Option<&'a str>,
}

#[cfg(feature = "observe")]
pub async fn log_invocation(
    logger: &dyn Logger,
    ctx: &ObserveCtx,
    params: LogInvocationParams<'_>,
) {
    let LogInvocationParams {
        tenant,
        tool,
        status,
        code,
        latency_ms,
        degradation,
    } = params;

    let level = match status {
        "error" => LogLevel::Error,
        "degraded" => LogLevel::Warn,
        _ => LogLevel::Info,
    };

    let mut builder = LogBuilder::new(level, "tool invocation finished")
        .label("tenant", tenant.to_string())
        .label("tool_id", tool.0.clone())
        .label("status", status.to_string())
        .field("latency_ms", latency_ms.into());
    if let Some(code) = code {
        builder = builder.field("code", code.into());
    }
    if let Some(reason) = degradation {
        builder = builder.field("degradation", reason.into());
    }
    let event = builder.finish(ctx, &NoopRedactor);
    logger.log(ctx, event).await;
}
