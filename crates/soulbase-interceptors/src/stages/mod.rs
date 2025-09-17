use crate::context::{InterceptContext, ProtoRequest, ProtoResponse};
use crate::errors::InterceptError;
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
        F: for<'a> FnOnce(&'a mut InterceptContext, &'a mut dyn ProtoRequest) -> BoxFuture<'a, Result<serde_json::Value, InterceptError>> + Send,
    {
        for stage in &self.stages {
            match stage.handle(&mut cx, req, rsp).await? {
                StageOutcome::Continue => {}
                StageOutcome::ShortCircuit => return Ok(()),
            }
        }

        let body = handler(&mut cx, req).await?;
        rsp.set_status(200);
        rsp.write_json(&body).await?;
        Ok(())
    }
}

pub mod context_init;
pub mod route_policy;
pub mod authn_map;
pub mod authz_quota;
pub mod schema_guard;
pub mod obligations;
pub mod response_stamp;
