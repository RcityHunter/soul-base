use async_trait::async_trait;
use http::header::{HeaderName, HeaderValue};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::errors::NetError;
use crate::interceptors::Interceptor;
use crate::types::NetRequest;

#[derive(Clone, Default)]
pub struct TraceUa {
    pub user_agent: String,
}

#[async_trait]
impl Interceptor for TraceUa {
    async fn before_send(&self, request: &mut NetRequest) -> Result<(), NetError> {
        if request.trace_id.is_none() {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros();
            request.trace_id = Some(format!("trace-{ts}"));
        }
        if !self.user_agent.is_empty() {
            if let Ok(value) = HeaderValue::from_str(&self.user_agent) {
                request
                    .headers
                    .insert(HeaderName::from_static("user-agent"), value);
            }
        }
        Ok(())
    }
}
