use crate::context::{InterceptContext, ProtoRequest, ProtoResponse, ResilienceConfig};
use crate::errors::InterceptError;
use crate::stages::{Stage, StageOutcome};
use async_trait::async_trait;
use std::time::Duration;
use tokio::time::timeout;

pub struct ResilienceStage {
    config: ResilienceConfig,
}

impl ResilienceStage {
    pub fn new(timeout: Duration, max_retries: usize, backoff: Duration) -> Self {
        Self {
            config: ResilienceConfig {
                timeout,
                max_retries,
                backoff,
            },
        }
    }
}

#[async_trait]
impl Stage for ResilienceStage {
    async fn handle(
        &self,
        cx: &mut InterceptContext,
        _req: &mut dyn ProtoRequest,
        _rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError> {
        cx.resilience = self.config;
        Ok(StageOutcome::Continue)
    }
}

pub async fn run_with_resilience<Fut>(
    config: ResilienceConfig,
    fut: Fut,
) -> Result<serde_json::Value, InterceptError>
where
    Fut: std::future::Future<Output = Result<serde_json::Value, InterceptError>>,
{
    let result = timeout(config.timeout, fut).await;
    match result {
        Ok(res) => res,
        Err(_) => Err(InterceptError::internal("handler timeout")),
    }
}
