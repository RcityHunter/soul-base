use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::errors::NetError;
use crate::interceptors::Interceptor;
use crate::types::NetRequest;

#[derive(Clone, Debug)]
pub enum RateLimitScope {
    Global,
    Host,
    #[cfg(feature = "observe")]
    Tenant,
}

#[derive(Clone, Debug)]
pub struct RateLimitConfig {
    pub burst: u32,
    pub refill: Duration,
    pub scope: RateLimitScope,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            burst: 20,
            refill: Duration::from_secs(1),
            scope: RateLimitScope::Global,
        }
    }
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

#[derive(Clone)]
pub struct RateLimitInterceptor {
    config: RateLimitConfig,
    buckets: Arc<Mutex<HashMap<String, Bucket>>>,
}

impl RateLimitInterceptor {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn key(&self, request: &NetRequest) -> String {
        match &self.config.scope {
            RateLimitScope::Global => "global".into(),
            RateLimitScope::Host => request
                .url
                .host_str()
                .map(|h| h.to_string())
                .unwrap_or_else(|| "unknown".into()),
            #[cfg(feature = "observe")]
            RateLimitScope::Tenant => request
                .observe_ctx
                .as_ref()
                .map(|ctx| ctx.tenant.clone())
                .unwrap_or_else(|| "unknown".into()),
        }
    }
}

#[async_trait]
impl Interceptor for RateLimitInterceptor {
    async fn before_send(&self, request: &mut NetRequest) -> Result<(), NetError> {
        let key = self.key(request);
        let mut guard = self.buckets.lock();
        let burst = self.config.burst.max(1) as f64;
        let now = Instant::now();
        let bucket = guard.entry(key).or_insert(Bucket {
            tokens: burst,
            last: now,
        });

        let elapsed = now.saturating_duration_since(bucket.last);
        if elapsed >= self.config.refill {
            let refill_slots = (elapsed.as_secs_f64() / self.config.refill.as_secs_f64()) * burst;
            bucket.tokens = (bucket.tokens + refill_slots).min(burst);
            bucket.last = now;
        }

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            Err(NetError::rate_limited("outbound rate limit exceeded"))
        }
    }
}
