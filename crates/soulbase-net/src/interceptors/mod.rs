use std::sync::Arc;

use async_trait::async_trait;

use crate::errors::NetError;
use crate::types::NetRequest;

pub type InterceptorObject = Arc<dyn Interceptor>;

#[async_trait]
pub trait Interceptor: Send + Sync {
    async fn before_send(&self, _request: &mut NetRequest) -> Result<(), NetError> {
        Ok(())
    }
}

pub mod sandbox_guard;
pub mod trace_ua;
