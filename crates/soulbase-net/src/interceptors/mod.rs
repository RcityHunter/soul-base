use std::sync::Arc;

use async_trait::async_trait;

use crate::errors::NetError;
use crate::types::{NetRequest, NetResponse};

pub type InterceptorObject = Arc<dyn Interceptor>;

#[async_trait]
pub trait Interceptor: Send + Sync {
    async fn before_send(&self, _request: &mut NetRequest) -> Result<(), NetError> {
        Ok(())
    }

    async fn on_success(
        &self,
        _request: &NetRequest,
        _response: &NetResponse,
    ) -> Result<(), NetError> {
        Ok(())
    }

    async fn on_error(&self, _request: &NetRequest, _error: &NetError) -> Result<(), NetError> {
        Ok(())
    }
}

#[cfg(feature = "auth")]
pub mod authz;
#[cfg(feature = "observe")]
pub mod observe;
pub mod rate_limit;
pub mod sandbox_guard;
pub mod sign;
pub mod trace_ua;
