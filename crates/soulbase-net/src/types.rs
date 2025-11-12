use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use serde_json::Value;
use url::Url;

use crate::policy::RetryDecision;
#[cfg(feature = "auth")]
use soulbase_auth::model::{Action, AuthnInput, ResourceUrn};
#[cfg(feature = "observe")]
use soulbase_observe::prelude::ObserveCtx;
#[cfg(feature = "auth")]
use soulbase_types::prelude::Consent;

#[derive(Clone, Debug, Default)]
pub struct TimeoutCfg {
    pub connect: Option<Duration>,
    pub overall: Option<Duration>,
    pub read: Option<Duration>,
    pub write: Option<Duration>,
}

#[derive(Clone, Debug, Default)]
pub enum Body {
    #[default]
    Empty,
    Bytes(Bytes),
    Json(Value),
}

impl Body {
    pub fn as_bytes(&self) -> Option<Bytes> {
        match self {
            Body::Empty => Some(Bytes::new()),
            Body::Bytes(b) => Some(b.clone()),
            Body::Json(val) => serde_json::to_vec(val).ok().map(Bytes::from),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NetRequest {
    pub method: Method,
    pub url: Url,
    pub headers: HeaderMap,
    pub body: Body,
    pub timeout: TimeoutCfg,
    pub idempotent: bool,
    pub trace_id: Option<String>,
    pub retry_decision: Option<RetryDecision>,
    #[cfg(feature = "auth")]
    pub auth: Option<NetAuthContext>,
    #[cfg(feature = "observe")]
    pub observe_ctx: Option<ObserveCtx>,
}

impl Default for NetRequest {
    fn default() -> Self {
        Self {
            method: Method::GET,
            url: Url::parse("http://127.0.0.1/").expect("static url"),
            headers: HeaderMap::new(),
            body: Body::Empty,
            timeout: TimeoutCfg::default(),
            idempotent: true,
            trace_id: None,
            retry_decision: None,
            #[cfg(feature = "auth")]
            auth: None,
            #[cfg(feature = "observe")]
            observe_ctx: None,
        }
    }
}

impl NetRequest {
    pub fn set_trace_id<T: Into<String>>(&mut self, id: T) {
        self.trace_id = Some(id.into());
    }

    #[cfg(feature = "auth")]
    pub fn with_auth(mut self, ctx: NetAuthContext) -> Self {
        self.auth = Some(ctx);
        self
    }

    #[cfg(feature = "observe")]
    pub fn with_observe_ctx(mut self, ctx: ObserveCtx) -> Self {
        self.observe_ctx = Some(ctx);
        self
    }
}

#[derive(Clone, Debug)]
pub struct NetResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub elapsed: Duration,
}

impl NetResponse {
    pub fn new(status: StatusCode, headers: HeaderMap, body: Bytes, elapsed: Duration) -> Self {
        Self {
            status,
            headers,
            body,
            elapsed,
        }
    }
}

#[cfg(feature = "auth")]
#[derive(Clone, Debug)]
pub struct NetAuthContext {
    pub input: AuthnInput,
    pub resource: ResourceUrn,
    pub action: Action,
    pub attrs: Value,
    pub consent: Option<Consent>,
    pub correlation_id: Option<String>,
}

#[cfg(feature = "auth")]
impl NetAuthContext {
    pub fn new(input: AuthnInput, resource: ResourceUrn, action: Action) -> Self {
        Self {
            input,
            resource,
            action,
            attrs: Value::Object(Default::default()),
            consent: None,
            correlation_id: None,
        }
    }

    pub fn with_attrs(mut self, attrs: Value) -> Self {
        self.attrs = attrs;
        self
    }

    pub fn with_consent(mut self, consent: Consent) -> Self {
        self.consent = Some(consent);
        self
    }

    pub fn with_correlation_id<T: Into<String>>(mut self, id: T) -> Self {
        self.correlation_id = Some(id.into());
        self
    }
}
