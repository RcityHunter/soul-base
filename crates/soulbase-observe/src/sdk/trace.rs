use std::time::Instant;

use async_trait::async_trait;

use crate::model::SpanCtx;
use crate::ObserveError;

#[async_trait]
pub trait Tracer: Send + Sync {
    async fn start_span(&self, name: &str, ctx: &SpanCtx) -> Result<SpanRecorder, ObserveError>;
}

#[derive(Clone, Default)]
pub struct NoopTracer;

#[async_trait]
impl Tracer for NoopTracer {
    async fn start_span(&self, _name: &str, ctx: &SpanCtx) -> Result<SpanRecorder, ObserveError> {
        Ok(SpanRecorder::new(ctx.clone()))
    }
}

#[derive(Clone, Debug)]
pub struct SpanRecorder {
    ctx: SpanCtx,
    start: Instant,
}

impl SpanRecorder {
    pub fn new(ctx: SpanCtx) -> Self {
        Self {
            ctx,
            start: Instant::now(),
        }
    }

    pub fn context(&self) -> &SpanCtx {
        &self.ctx
    }

    pub fn finish(self) -> SpanResult {
        SpanResult {
            ctx: self.ctx,
            elapsed_ms: self.start.elapsed().as_millis() as u64,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SpanResult {
    pub ctx: SpanCtx,
    pub elapsed_ms: u64,
}
