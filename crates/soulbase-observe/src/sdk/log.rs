use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;

use crate::ctx::ObserveCtx;
use crate::model::{LogEvent, LogLevel};
use crate::pipeline::redactor::Redactor;

#[async_trait]
pub trait Logger: Send + Sync {
    async fn log(&self, ctx: &ObserveCtx, event: LogEvent);
}

/// A [`Logger`] implementation that simply drops events.
#[derive(Default)]
pub struct NoopLogger;

#[async_trait]
impl Logger for NoopLogger {
    async fn log(&self, _ctx: &ObserveCtx, _event: LogEvent) {}
}

pub struct LogBuilder {
    ts_ms: Option<i64>,
    level: LogLevel,
    msg: String,
    labels: BTreeMap<&'static str, String>,
    fields: serde_json::Map<String, Value>,
}

impl LogBuilder {
    pub fn new(level: LogLevel, msg: impl Into<String>) -> Self {
        Self {
            ts_ms: None,
            level,
            msg: msg.into(),
            labels: BTreeMap::new(),
            fields: serde_json::Map::new(),
        }
    }

    pub fn at(mut self, timestamp_ms: i64) -> Self {
        self.ts_ms = Some(timestamp_ms);
        self
    }

    pub fn label(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.labels.insert(key, value.into());
        self
    }

    pub fn field(mut self, key: &str, value: Value) -> Self {
        self.fields.insert(key.to_string(), value);
        self
    }

    pub fn finish(self, ctx: &ObserveCtx, redactor: &dyn Redactor) -> LogEvent {
        let ts_ms = self.ts_ms.unwrap_or_else(|| Utc::now().timestamp_millis());
        let mut labels = self.labels;
        labels
            .entry("tenant")
            .or_insert_with(|| redactor.redact_label("tenant", &ctx.tenant));
        if let Some(route) = ctx.route_id.as_ref() {
            labels.insert("route_id", redactor.redact_label("route_id", route));
        }
        if let Some(resource) = ctx.resource.as_ref() {
            labels.insert("resource", redactor.redact_label("resource", resource));
        }
        if let Some(action) = ctx.action.as_ref() {
            labels.insert("action", redactor.redact_label("action", action));
        }
        if let Some(code) = ctx.code.as_ref() {
            labels.insert("code", code.clone());
        }

        let mut fields = self.fields;
        if let Some(subject) = ctx.subject_kind.as_ref() {
            fields.insert(
                "subject_kind".into(),
                Value::String(redactor.redact_field("subject_kind", subject)),
            );
        }
        if let Some(version) = ctx.config_version.as_ref() {
            fields.insert("config_version".into(), Value::String(version.clone()));
        }
        if let Some(checksum) = ctx.config_checksum.as_ref() {
            fields.insert("config_checksum".into(), Value::String(checksum.clone()));
        }

        LogEvent {
            ts_ms,
            level: self.level,
            msg: self.msg,
            labels,
            fields,
        }
    }
}
