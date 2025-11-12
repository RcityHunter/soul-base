use std::sync::Arc;

use async_trait::async_trait;
use http::HeaderValue;
use serde_json::json;

use crate::errors::NetError;
use crate::interceptors::Interceptor;
use crate::types::{NetRequest, NetResponse};
use soulbase_observe::prelude::{
    LogBuilder, LogLevel, Logger, Meter, MetricKind, MetricSpec, NoopRedactor,
};

const NET_REQUESTS_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_net_requests_total",
    kind: MetricKind::Counter,
    help: "Total outbound HTTP requests",
    buckets_ms: None,
    stable_labels: &[],
};

const NET_REQUEST_FAILURE_TOTAL: MetricSpec = MetricSpec {
    name: "soulbase_net_requests_failure_total",
    kind: MetricKind::Counter,
    help: "Failed outbound HTTP requests",
    buckets_ms: None,
    stable_labels: &[],
};

const NET_REQUEST_LATENCY_MS: MetricSpec = MetricSpec {
    name: "soulbase_net_request_latency_ms",
    kind: MetricKind::Gauge,
    help: "Latency of the last outbound HTTP request (ms)",
    buckets_ms: None,
    stable_labels: &[],
};

#[derive(Clone)]
pub struct ObserveInterceptor {
    logger: Arc<dyn Logger>,
    meter: Arc<dyn Meter>,
    emit_headers: bool,
}

impl ObserveInterceptor {
    pub fn new(meter: Arc<dyn Meter>, logger: Arc<dyn Logger>) -> Self {
        Self {
            logger,
            meter,
            emit_headers: false,
        }
    }

    pub fn emit_headers(mut self, emit: bool) -> Self {
        self.emit_headers = emit;
        self
    }
}

#[async_trait]
impl Interceptor for ObserveInterceptor {
    async fn before_send(&self, request: &mut NetRequest) -> Result<(), NetError> {
        self.meter.counter(&NET_REQUESTS_TOTAL).inc(1);
        if self.emit_headers {
            if let Some(ctx) = request.observe_ctx.as_ref() {
                if !ctx.tenant.is_empty() {
                    if let Ok(value) = HeaderValue::from_str(&ctx.tenant) {
                        request.headers.insert("x-soul-tenant", value);
                    }
                }
                if let Some(action) = ctx.action.as_ref() {
                    if let Ok(value) = HeaderValue::from_str(action) {
                        request.headers.insert("x-soul-action", value);
                    }
                }
                if let Some(route) = ctx.route_id.as_ref() {
                    if let Ok(value) = HeaderValue::from_str(route) {
                        request.headers.insert("x-soul-route", value);
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_success(
        &self,
        request: &NetRequest,
        response: &NetResponse,
    ) -> Result<(), NetError> {
        self.meter
            .gauge(&NET_REQUEST_LATENCY_MS)
            .set(response.elapsed.as_millis() as u64);

        if let Some(ctx) = request.observe_ctx.as_ref() {
            let event = LogBuilder::new(LogLevel::Info, "net request succeeded")
                .label("method", request.method.as_str().to_string())
                .label("status", response.status.as_u16().to_string())
                .label(
                    "host",
                    request.url.host_str().unwrap_or_default().to_string(),
                )
                .field("elapsed_ms", json!(response.elapsed.as_millis()))
                .finish(ctx, &NoopRedactor);
            self.logger.log(ctx, event).await;
        }
        Ok(())
    }

    async fn on_error(&self, request: &NetRequest, error: &NetError) -> Result<(), NetError> {
        self.meter.counter(&NET_REQUEST_FAILURE_TOTAL).inc(1);
        if let Some(ctx) = request.observe_ctx.as_ref() {
            let event = LogBuilder::new(LogLevel::Error, "net request failed")
                .label("method", request.method.as_str().to_string())
                .label(
                    "host",
                    request.url.host_str().unwrap_or_default().to_string(),
                )
                .field("code", json!(error.0.code.0))
                .field("message", json!(error.0.message_user))
                .finish(ctx, &NoopRedactor);
            self.logger.log(ctx, event).await;
        }
        Ok(())
    }
}
