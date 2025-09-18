use crate::context::{InterceptContext, ProtoRequest, ProtoResponse};
use crate::errors::InterceptError;
use crate::stages::resilience::run_with_resilience;
use async_trait::async_trait;
use futures::future::BoxFuture;

#[async_trait]
pub trait Stage: Send + Sync {
    async fn handle(
        &self,
        cx: &mut InterceptContext,
        req: &mut dyn ProtoRequest,
        rsp: &mut dyn ProtoResponse,
    ) -> Result<StageOutcome, InterceptError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageOutcome {
    Continue,
    ShortCircuit,
}

pub struct InterceptorChain {
    stages: Vec<Box<dyn Stage>>,
}

impl InterceptorChain {
    pub fn new(stages: Vec<Box<dyn Stage>>) -> Self {
        Self { stages }
    }

    pub async fn run_with_handler<F>(
        &self,
        mut cx: InterceptContext,
        req: &mut dyn ProtoRequest,
        rsp: &mut dyn ProtoResponse,
        handler: F,
    ) -> Result<(), InterceptError>
    where
        F: for<'a> FnOnce(
                &'a mut InterceptContext,
                &'a mut dyn ProtoRequest,
            ) -> BoxFuture<'a, Result<serde_json::Value, InterceptError>>
            + Send,
    {
        let mut handler_opt = Some(handler);
        for stage in &self.stages {
            match stage.handle(&mut cx, req, rsp).await? {
                StageOutcome::Continue => {}
                StageOutcome::ShortCircuit => return Ok(()),
            }
        }

        let handler = handler_opt.take().expect("handler consumed");
        let config = cx.resilience;
        let fut = handler(&mut cx, req);
        let body = run_with_resilience(config, fut).await?;
        rsp.set_status(200);
        rsp.write_json(&body).await?;
        Ok(())
    }
}

pub mod authn_map;
pub mod authz_quota;
pub mod context_init;
pub mod error_norm;
pub mod obligations;
pub mod resilience;
pub mod response_stamp;
pub mod route_policy;
pub mod schema_guard;
pub mod tenant_guard;
